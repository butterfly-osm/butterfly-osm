//! #400/#409/#410 — lean-at-rest: free per-thread query scratch when idle.
//!
//! After a heavy /route, /table or /isochrone query the executing
//! thread retains its per-thread state (`CchQueryState`, `PhastSlots`,
//! bucket-M2M `SearchState`, etc.) — tens to hundreds of MB per thread,
//! never released for the rest of the process lifetime. The user wants
//! the server lean at rest so the OS can reclaim that RAM during idle.
//!
//! ## Mechanism
//!
//! A background thread sleeps `poll_interval`, then calls
//! [`crate::server::evictable::evict_idle`], which walks a process-
//! global registry of [`crate::server::evictable::EvictableCell`]s —
//! one per (scratch cell, thread). Each cell carries its own last-touch
//! timestamp (stamped on the query hot path) and a `try_lock`-guarded
//! slot; a cell idle longer than `threshold` is freed cross-thread.
//!
//! ## Why a registry, not `rayon::broadcast` (#409/#410)
//!
//! The previous design broadcast a drop closure across the **rayon
//! pool** only. But `/route` and `/isochrone` run *inline on Tokio
//! runtime workers*, and small-N `/table` / `/trip` / `/transit_bulk`
//! run on Tokio's `spawn_blocking` pool — none are rayon workers, so
//! their scratch was never reclaimed (the real 27 → 48 Gi steady-state
//! growth). The registry is thread-agnostic and reaches them all.
//!
//! Container mmap pages stay out of scope — the kernel evicts those
//! itself under memory pressure.

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

/// Sentinel — running compactor handle. Holding a clone keeps the
/// background thread alive; dropping all clones signals shutdown.
#[derive(Clone)]
pub struct IdleCompactor {
    stop: Arc<AtomicBool>,
}

impl IdleCompactor {
    /// Spawn the background thread. `threshold` is how long a worker
    /// must be idle before its caches are dropped; `poll_interval` is
    /// how often the compactor wakes up to check. A reasonable choice
    /// is `poll_interval = threshold / 4` so freshly-idle workers get
    /// evicted within a quarter of `threshold`.
    ///
    /// `regions` is the multi-region registry; the compactor walks
    /// every loaded region's mode slots once per tick and evicts any
    /// that have been idle longer than `threshold` (#402). Per-mode
    /// eviction frees the whole mode definition (~1-4 GB on Belgium)
    /// rather than just the per-worker scratch (~80 MB).
    ///
    /// Setting `threshold = 0` disables the compactor (returns a
    /// no-op handle).
    pub fn start(
        threshold: Duration,
        poll_interval: Duration,
        regions: Arc<crate::server::regions::RegionsState>,
    ) -> Self {
        let stop = Arc::new(AtomicBool::new(false));
        if threshold.is_zero() {
            tracing::info!("#400 idle-compactor: disabled (threshold=0)");
            return Self { stop };
        }
        let stop_clone = Arc::clone(&stop);
        tracing::info!(
            threshold_secs = threshold.as_secs(),
            poll_interval_secs = poll_interval.as_secs(),
            "#400 idle-compactor: spawning background poller"
        );
        std::thread::Builder::new()
            .name("idle-compactor".into())
            .spawn(move || run(stop_clone, threshold, poll_interval, regions))
            .expect("idle-compactor thread spawn");
        Self { stop }
    }
}

impl Drop for IdleCompactor {
    fn drop(&mut self) {
        // Only the last Arc drop signals shutdown.
        if Arc::strong_count(&self.stop) == 1 {
            self.stop.store(true, Ordering::Relaxed);
        }
    }
}

fn run(
    stop: Arc<AtomicBool>,
    threshold: Duration,
    poll_interval: Duration,
    regions: Arc<crate::server::regions::RegionsState>,
) {
    // Resolve the poll-interval into a small unit so we can wake up
    // promptly on shutdown without polling tightly.
    let sleep_step = Duration::from_secs(1).min(poll_interval);
    let mut elapsed = Duration::ZERO;
    while !stop.load(Ordering::Relaxed) {
        std::thread::sleep(sleep_step);
        elapsed += sleep_step;
        if elapsed < poll_interval {
            continue;
        }
        elapsed = Duration::ZERO;
        evict_all_workers(threshold);
        evict_idle_modes(threshold, &regions);
    }
}

fn evict_all_workers(threshold: Duration) {
    // #409/#410: a single thread-agnostic registry walk. Replaces the
    // old `rayon::broadcast`, which only reached the rayon pool — it
    // could not free the query/PHAST/bucket scratch that lives on Tokio
    // runtime + spawn_blocking threads (where `/route`, `/isochrone`,
    // and small-N `/table` actually execute). The registry holds a Weak
    // to every per-thread cell regardless of pool, so this frees them
    // all and prunes cells whose owning thread has died.
    let freed = crate::server::evictable::evict_idle(threshold);
    if freed > 0 {
        tracing::info!(
            cells_freed = freed,
            "#409 idle-compactor: freed idle per-thread query scratch"
        );
    }
}

/// #402 per-mode eviction. Walks every loaded region's mode slots
/// and asks each to evict if idle > threshold. Mode loading is
/// re-driven by the next `get_mode` call (single-flight under the
/// per-slot write lock). Mode size on Belgium: ~1-4 GB per mode.
fn evict_idle_modes(threshold: Duration, regions: &Arc<crate::server::regions::RegionsState>) {
    let threshold_ms = threshold.as_millis() as u64;
    let mut evicted = 0usize;
    for region in regions.iter_regions() {
        // Skip Pending regions — nothing loaded to evict.
        let state = match region.state_loaded() {
            Some(s) => s,
            None => continue,
        };
        for mode_idx in 0..state.modes.len() {
            if state.try_evict_mode_if_idle(mode_idx, threshold_ms) {
                evicted += 1;
            }
        }
    }
    if evicted > 0 {
        tracing::info!(
            modes = evicted,
            "#402 idle-compactor: evicted cold mode slots"
        );
    }
}
