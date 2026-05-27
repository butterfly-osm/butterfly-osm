//! #400 — lean-at-rest: drop per-worker thread-local caches when idle.
//!
//! After a heavy /table or /isochrone query, every active rayon worker
//! retains its per-thread state (`SearchState`, `PhastState`, etc.) —
//! tens to hundreds of MB per worker, never released for the rest of
//! the process lifetime. The user wants the server lean at rest so the
//! OS can give the RAM to other processes during idle periods.
//!
//! ## Mechanism
//!
//! A background thread sleeps `poll_interval`, then issues a
//! `rayon::broadcast` so a closure runs on every rayon worker. Each
//! worker checks its own thread-local `LAST_TOUCH` timestamp and, if
//! it hasn't been touched in `threshold`, drops its caches.
//!
//! Workers update `LAST_TOUCH` at the entry of every routing function
//! they execute (see e.g.
//! `crate::matrix::bucket_ch::touch_idle_marker`). The check is
//! cheap (one thread-local read + one timestamp compare) and only
//! fires when the compactor wakes up — not on every query.
//!
//! The drop side per module:
//! - `matrix::bucket_ch::try_drop_idle_state` — bucket-M2M
//!   SearchStates + bucket-items Vecs + 2-channel equivalents
//!   (~80 MB / worker on Belgium)
//! - `server::query::try_drop_idle_state` — /route CchQueryState
//!   (~60 MB / worker)
//! - `server::isochrone_handler::try_drop_idle_phast` — PHAST
//!   forward + reverse states per mode (~30-60 MB / worker per
//!   mode populated)
//! - `transit::raptor::try_drop_idle_state` — RAPTOR scratch
//!   (small relative to others)
//!
//! ## Out of scope (still RAM after compact)
//!
//! - `SEQ_STATE_LAT` / `SEQUENTIAL_ENGINE` (small-N fast-path
//!   thread-locals on the handler thread, not on rayon workers).
//!   `rayon::broadcast` reaches rayon workers only; handler-thread
//!   eviction would need a separate mechanism. ~60 MB total —
//!   accepted for v1.
//! - Container mmap pages — kernel evicts those itself under
//!   memory pressure; nothing to do here.

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
    /// Setting `threshold = 0` disables the compactor (returns a
    /// no-op handle).
    pub fn start(threshold: Duration, poll_interval: Duration) -> Self {
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
            .spawn(move || run(stop_clone, threshold, poll_interval))
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

fn run(stop: Arc<AtomicBool>, threshold: Duration, poll_interval: Duration) {
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
    }
}

fn evict_all_workers(threshold: Duration) {
    // `rayon::broadcast` runs `op` once on every worker thread in the
    // global rayon pool. The closure must be `Sync + Fn(BroadcastContext) -> R`.
    let drops_bucket = std::sync::atomic::AtomicUsize::new(0);
    let drops_query = std::sync::atomic::AtomicUsize::new(0);
    let drops_phast = std::sync::atomic::AtomicUsize::new(0);
    let drops_raptor = std::sync::atomic::AtomicUsize::new(0);
    rayon::broadcast(|_| {
        if crate::matrix::bucket_ch::try_drop_idle_state(threshold) {
            drops_bucket.fetch_add(1, Ordering::Relaxed);
        }
        if crate::server::query::try_drop_idle_state(threshold) {
            drops_query.fetch_add(1, Ordering::Relaxed);
        }
        if crate::server::isochrone_handler::try_drop_idle_phast(threshold) {
            drops_phast.fetch_add(1, Ordering::Relaxed);
        }
        if crate::transit::raptor::try_drop_idle_state(threshold) {
            drops_raptor.fetch_add(1, Ordering::Relaxed);
        }
    });
    let total = drops_bucket.load(Ordering::Relaxed)
        + drops_query.load(Ordering::Relaxed)
        + drops_phast.load(Ordering::Relaxed)
        + drops_raptor.load(Ordering::Relaxed);
    if total > 0 {
        tracing::info!(
            bucket = drops_bucket.load(Ordering::Relaxed),
            query = drops_query.load(Ordering::Relaxed),
            phast = drops_phast.load(Ordering::Relaxed),
            raptor = drops_raptor.load(Ordering::Relaxed),
            "#400 idle-compactor: dropped per-worker caches"
        );
    }
}
