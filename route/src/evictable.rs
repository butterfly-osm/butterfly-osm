//! #409/#410 — thread-agnostic eviction of per-thread query scratch.
//!
//! ## The problem this replaces
//!
//! The hot query paths keep large scratch arenas in `thread_local!`
//! storage for O(1) generation-stamped reuse across queries
//! (`CchQueryState` ~185 MB, `PhastSlots` ~80 MB/mode, bucket-M2M
//! `SearchState` ~60 MB on the 5.1 M-node Belgium foot graph). The
//! original #400 idle-compactor freed those via `rayon::broadcast`,
//! which only runs on the **global rayon pool**. But `/route` and
//! `/isochrone` execute *inline on Tokio runtime worker threads*
//! (handlers are async, deliberately not `spawn_blocking` — see
//! `route.rs`), and `/table` / `/trip` / `/transit_bulk` run on Tokio's
//! `spawn_blocking` pool. None of those are rayon workers, so their
//! thread-local scratch was **never reclaimed** — the real 27 → 48 Gi
//! steady-state growth under mixed traffic.
//!
//! ## The mechanism
//!
//! Each former `thread_local! { RefCell<Option<T>> }` becomes an
//! [`EvictableCell<T>`]. On first touch on a thread it allocates an
//! `Arc<EvictableInner<T>>` and registers a `Weak` to it in a single
//! process-global registry. The idle-compactor walks that registry —
//! **thread-agnostic**, so it reaches Tokio workers, the blocking pool,
//! and rayon workers alike — and frees the scratch of any cell idle
//! longer than the threshold.
//!
//! ## Why it is correct and cheap
//!
//! - The whole query runs inside `with_or_init`, holding the cell's
//!   `parking_lot::Mutex` for the query's lifetime — the same "borrowed
//!   for the whole search" shape the old `RefCell` had.
//! - The compactor uses `try_lock`: if a query holds the cell it fails
//!   and skips (an in-flight thread is by definition not idle). It can
//!   therefore never block a live query and never free in-use scratch —
//!   no use-after-free, no deadlock (it locks one cell at a time, never
//!   nested, and never blocks).
//! - Hot-path cost is one relaxed `AtomicU64` store + one uncontended
//!   `parking_lot::Mutex` lock per query (~3-5 ns), paid once per query,
//!   not per node. Against a 3.7 ms `/route` p50 it is unmeasurable.
//! - The registry holds `Weak`s, so a reaped thread's cell drops and is
//!   pruned on the next sweep — no leak, no pinning of freed scratch.

use parking_lot::Mutex;
use std::cell::OnceCell;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock, Weak};
use std::time::{Duration, Instant};

/// Process monotonic clock baseline. Cells and the compactor both
/// measure idleness against this, so the units always agree.
fn now_ms() -> u64 {
    static START: OnceLock<Instant> = OnceLock::new();
    START.get_or_init(Instant::now).elapsed().as_millis() as u64
}

/// Type-erased view the compactor walks without knowing `T`.
trait Evictable: Send + Sync {
    /// Free the scratch iff idle ≥ `threshold_ms`. Returns `true` if it
    /// freed a resident arena. Uses `try_lock` and never blocks.
    fn try_evict(&self, threshold_ms: u64, now: u64) -> bool;
}

struct EvictableInner<T: Send> {
    slot: Mutex<Option<T>>,
    last_touch_ms: AtomicU64,
}

impl<T: Send + 'static> Evictable for EvictableInner<T> {
    fn try_evict(&self, threshold_ms: u64, now: u64) -> bool {
        // Cheap atomic pre-check before touching the heavy mutex.
        let last = self.last_touch_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) < threshold_ms {
            return false;
        }
        // Non-blocking: if a query holds the cell, skip it — an active
        // thread is not idle, and we must never block the hot path.
        if let Some(mut guard) = self.slot.try_lock()
            && guard.take().is_some()
        {
            return true;
        }
        false
    }
}

/// The process-global registry of evictable cells (one `Weak` per
/// (cell, thread) instance). Guarded by a `Mutex` only on the cold
/// registration path and during the periodic compactor sweep — never
/// on the query hot path.
fn registry() -> &'static Mutex<Vec<Weak<dyn Evictable>>> {
    static REGISTRY: OnceLock<Mutex<Vec<Weak<dyn Evictable>>>> = OnceLock::new();
    REGISTRY.get_or_init(|| Mutex::new(Vec::new()))
}

/// A registry-backed replacement for `thread_local! { RefCell<Option<T>> }`.
///
/// Construct one per `thread_local!` slot via the `const` [`Self::new`].
/// Access the scratch through [`Self::with_or_init`], which lazily
/// builds the arena, registers it for cross-thread eviction on first
/// use, stamps the last-touch time, and hands a `&mut T` to the closure
/// for the duration of the query.
pub struct EvictableCell<T: Send + 'static> {
    // Per-thread (the cell lives in `thread_local!`), so non-`Sync`
    // interior mutability is fine. The Arc is shared with the registry.
    inner: OnceCell<Arc<EvictableInner<T>>>,
}

impl<T: Send + 'static> EvictableCell<T> {
    pub const fn new() -> Self {
        Self {
            inner: OnceCell::new(),
        }
    }

    /// Run `f` with a mutable reference to this thread's scratch,
    /// constructing it via `init` if absent (first use, or after the
    /// compactor evicted an idle arena). Holds the cell lock for the
    /// duration of `f` — i.e. the whole query — mirroring the old
    /// `RefCell` borrow lifetime.
    /// Test-only: the `Weak<dyn Evictable>` for this cell's inner arena
    /// (must have been initialised via `with_or_init` first), so a test can
    /// assert registry pruning by identity instead of a racy global count.
    #[cfg(test)]
    fn weak_for_test(&self) -> std::sync::Weak<dyn Evictable> {
        let arc = self.inner.get().expect("cell not initialised");
        let dyn_arc: Arc<dyn Evictable> = arc.clone();
        Arc::downgrade(&dyn_arc)
    }

    #[inline]
    pub fn with_or_init<R>(&self, init: impl FnOnce() -> T, f: impl FnOnce(&mut T) -> R) -> R {
        let arc = self.inner.get_or_init(|| {
            let inner = Arc::new(EvictableInner {
                slot: Mutex::new(None),
                last_touch_ms: AtomicU64::new(now_ms()),
            });
            // Register a Weak so the compactor can reach this cell on
            // any thread; the Weak is pruned once the owning thread dies.
            let dyn_arc: Arc<dyn Evictable> = inner.clone();
            registry().lock().push(Arc::downgrade(&dyn_arc));
            inner
        });
        arc.last_touch_ms.store(now_ms(), Ordering::Relaxed);
        let mut guard = arc.slot.lock();
        let scratch = guard.get_or_insert_with(init);
        f(scratch)
    }
}

impl<T: Send + 'static> Default for EvictableCell<T> {
    fn default() -> Self {
        Self::new()
    }
}

/// Walk the registry once and free every cell idle ≥ `threshold`.
///
/// Pruning dead `Weak`s and freeing idle cells happen in two phases so
/// the registry lock is never held while a per-cell `try_lock` is
/// attempted: first snapshot the live strong handles (pruning dead
/// entries under a brief lock), then evict outside the lock. Returns
/// the number of arenas freed. Called by the idle-compactor each tick.
pub fn evict_idle(threshold: Duration) -> usize {
    let threshold_ms = threshold.as_millis() as u64;
    let now = now_ms();

    // Phase 1: snapshot live handles + prune dead Weaks under the lock.
    let live: Vec<Arc<dyn Evictable>> = {
        let mut reg = registry().lock();
        let mut out = Vec::with_capacity(reg.len());
        reg.retain(|w| match w.upgrade() {
            Some(strong) => {
                out.push(strong);
                true
            }
            None => false, // owning thread died — drop the Weak
        });
        out
    };

    // Phase 2: try to evict each, holding no registry lock.
    let mut freed = 0usize;
    for cell in live {
        if cell.try_evict(threshold_ms, now) {
            freed += 1;
        }
    }
    freed
}

/// Test-only: does the registry still hold this exact Weak? Count-independent
/// (identity by pointer), so it is robust to concurrent registrations.
#[cfg(test)]
fn registry_contains_test(target: &std::sync::Weak<dyn Evictable>) -> bool {
    let reg = registry().lock();
    reg.iter().any(|w| std::sync::Weak::ptr_eq(w, target))
}

/// Number of registered (live + not-yet-pruned) cells. Test/observability.
pub fn registered_len() -> usize {
    registry().lock().len()
}

#[cfg(test)]
mod tests {
    use super::*;

    // The registry is process-global, so count-sensitive assertions
    // must not run concurrently with each other. Serialize them and
    // start each from a pruned clean slate (all test cells are local
    // variables that drop at end-of-test, leaving dead Weaks that a
    // sweep reclaims).
    static SERIAL: Mutex<()> = Mutex::new(());

    #[test]
    fn registers_once_per_cell() {
        let _g = SERIAL.lock();
        let _ = evict_idle(Duration::ZERO); // prune residue from other tests
        let before = registered_len();
        let cell: EvictableCell<Vec<u8>> = EvictableCell::new();
        cell.with_or_init(|| vec![1, 2, 3], |v| v.push(4));
        cell.with_or_init(Vec::new, |v| assert_eq!(v.len(), 4));
        // Exactly one registration despite two touches.
        assert_eq!(registered_len(), before + 1);
    }

    #[test]
    fn evicts_idle_and_reinits() {
        let _g = SERIAL.lock();
        let cell: EvictableCell<Vec<u8>> = EvictableCell::new();
        cell.with_or_init(|| vec![9; 100], |v| assert_eq!(v.len(), 100));
        // threshold 0 → immediately idle (we are not inside with_or_init
        // now, so the cell lock is free and the take() succeeds).
        let freed = evict_idle(Duration::from_millis(0));
        assert!(freed >= 1, "expected at least this cell to be freed");
        // Next touch must re-run init (the arena was taken).
        let mut reinit = false;
        cell.with_or_init(
            || {
                reinit = true;
                Vec::new()
            },
            |_| {},
        );
        assert!(reinit, "init must run again after eviction");
    }

    #[test]
    fn busy_cell_is_skipped() {
        let _g = SERIAL.lock();
        let cell: EvictableCell<Vec<u8>> = EvictableCell::new();
        // Hold the cell lock for the whole closure and try to evict from
        // within — try_lock must fail, so the arena survives.
        cell.with_or_init(
            || vec![7; 10],
            |_v| {
                // Our cell is locked here; the in-flight try_lock skips it.
                let _ = evict_idle(Duration::from_millis(0));
            },
        );
        // After the closure the lock is free; the value persisted across
        // the in-closure eviction attempt (init must NOT run again).
        cell.with_or_init(
            || panic!("must not re-init: arena should have survived the busy-skip"),
            |v| assert_eq!(v.len(), 10),
        );
    }

    #[test]
    fn dead_thread_cell_is_pruned() {
        let _g = SERIAL.lock();
        // NOTE: `registered_len()` is a GLOBAL count; other (non-evictable)
        // tests in the parallel suite register live cells via PHAST /
        // mode-slots, so asserting it returns to an exact pre-spawn baseline
        // is racy (was a real flake). Instead assert the ACTUAL invariant on
        // OUR OWN cell, deterministically: the dead thread drops its Arc, and
        // a sweep prunes the now-dead Weak out of the registry.
        let _ = evict_idle(Duration::ZERO);
        // Thread creates + registers a cell and hands us back a Weak to it.
        let (tx, rx) = std::sync::mpsc::channel::<std::sync::Weak<dyn Evictable>>();
        std::thread::spawn(move || {
            let cell: EvictableCell<Vec<u8>> = EvictableCell::new();
            cell.with_or_init(|| vec![0; 10], |_| {});
            tx.send(cell.weak_for_test()).unwrap();
        })
        .join()
        .unwrap();
        let weak = rx.recv().unwrap();
        // Thread died -> its Arc dropped -> our Weak can no longer upgrade.
        assert!(
            weak.upgrade().is_none(),
            "dead thread must drop its cell's Arc"
        );
        // The sweep's retain() must remove that dead Weak from the registry.
        // Bounded retry closes the narrow window where a *concurrent* test
        // thread dies between the sweep and the check (its dead Weak is not
        // ours; a re-sweep clears it). We assert OUR weak is absent, which is
        // count-independent and thus robust to concurrent live registrations.
        let mut pruned = false;
        for _ in 0..5 {
            let _ = evict_idle(Duration::ZERO);
            if !registry_contains_test(&weak) {
                pruned = true;
                break;
            }
        }
        assert!(
            pruned,
            "sweep must prune the dead thread's Weak from the registry"
        );
    }
}
