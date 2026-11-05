//! Memory bounds for the serve (#655).
//!
//! Every query engine keeps its search scratch in thread-local cells
//! (`crate::evictable`), sized by the mode's node count: a bucket-M2M lane
//! holds two `SearchState`s (12 B per node each), a 2-channel lane two
//! `SearchState2`s (16 B per node each), a PHAST lane one `PhastState`,
//! a tree-PHAST lane one `TreeScratch`, a P2P query one `CchQueryState`.
//! The cells live on whichever thread ran the work: every rayon worker and
//! every runtime thread that ever computed. Sized by the CORE count, the
//! rayon pool (#631: cores − 2) made that scratch proportional to the host:
//! on a 96-thread host, a 1054² foot matrix allocated 94 lanes of scratch
//! and OOM-killed a 64 GiB pod (anon 55.7 GB) — the first such request
//! that pod ever saw came from the post-deploy gate itself.
//!
//! Three bounds, all derived from the memory the process actually has:
//!
//! 1. **The limit is the cgroup's**, not the host's (`memory_limit_bytes`):
//!    `MemTotal` capped by cgroup v2 `memory.max` / v1
//!    `memory.limit_in_bytes`. The RSS budget (80 % of it, `--rss-budget-gb`
//!    / `BUTTERFLY_RSS_BUDGET_GB`) used to be 80 % of the host's RAM — 200
//!    GB inside a 64 GiB pod, i.e. no budget at all.
//! 2. **The rayon pool is sized by a scratch budget** (`rayon_workers`):
//!    `min(cores − 2, scratch_budget / bytes of the largest per-lane
//!    state)`, where the scratch budget is `BUTTERFLY_SCRATCH_BUDGET_GB` or
//!    30 % of the RSS budget. One parallel request can then never allocate
//!    more scratch than the budget, whatever the host's core count.
//! 3. **Admission** (`admit`): a heavy request states how many bytes it
//!    will need (`need_*`: output cells plus one scratch state per lane it
//!    will occupy); if the current RSS plus that need exceeds the RSS
//!    budget, idle scratch is evicted first, and if it still does not fit
//!    the request is refused — `413 Payload Too Large` /
//!    `RESOURCE_EXHAUSTED` when nothing else is running (it can never fit:
//!    split it), `503 Service Unavailable` + `Retry-After` / `UNAVAILABLE`
//!    while other heavy work holds the memory. Never a crash.
//!
//! The per-node constants are pinned by tests next to each state struct
//! (`heap_bytes()` ≤ the formula, and the formula is within 25 % of it), so
//! a field added to a scratch struct fails a test instead of silently
//! shrinking the bound.

use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use axum::Json;
use axum::http::{StatusCode, header};
use axum::response::{IntoResponse, Response};

use super::types::ErrorResponse;

// ---------------------------------------------------------------------------
// The limit the process lives under
// ---------------------------------------------------------------------------

/// Parse a cgroup memory limit file: a byte count, or `max` (v2) / a value
/// ≥ 2^62 (v1 "unlimited") for no limit.
pub fn parse_cgroup_limit(s: &str) -> Option<u64> {
    let t = s.trim();
    if t == "max" {
        return None;
    }
    let v: u64 = t.parse().ok()?;
    if v >= (1u64 << 62) { None } else { Some(v) }
}

/// `MemTotal` from a `/proc/meminfo` body, in bytes.
pub fn parse_meminfo_total(s: &str) -> Option<u64> {
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

/// The smallest of the limits that apply, or `None` when none is readable.
pub fn smallest_limit(candidates: impl IntoIterator<Item = Option<u64>>) -> Option<u64> {
    candidates.into_iter().flatten().min()
}

/// The memory this process may use: the host's `MemTotal` capped by the
/// cgroup limit (v2 `memory.max`, v1 `memory.limit_in_bytes`).
/// `BUTTERFLY_MEMORY_LIMIT_BYTES` overrides (tests, odd hosts). 8 GiB when
/// nothing is readable.
pub fn memory_limit_bytes() -> u64 {
    const FALLBACK: u64 = 8 << 30;
    if let Ok(s) = std::env::var("BUTTERFLY_MEMORY_LIMIT_BYTES")
        && let Ok(v) = s.trim().parse::<u64>()
        && v > 0
    {
        return v;
    }
    let meminfo = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|s| parse_meminfo_total(&s));
    let cg_v2 = std::fs::read_to_string("/sys/fs/cgroup/memory.max")
        .ok()
        .and_then(|s| parse_cgroup_limit(&s));
    let cg_v1 = std::fs::read_to_string("/sys/fs/cgroup/memory/memory.limit_in_bytes")
        .ok()
        .and_then(|s| parse_cgroup_limit(&s));
    smallest_limit([meminfo, cg_v2, cg_v1]).unwrap_or(FALLBACK)
}

/// Resident set size of this process (`VmRSS`), in bytes.
pub fn vm_rss_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    for line in s.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            let kb: u64 = rest.split_whitespace().next()?.parse().ok()?;
            return Some(kb * 1024);
        }
    }
    None
}

// ---------------------------------------------------------------------------
// Scratch sizes
// ---------------------------------------------------------------------------

pub use crate::scratch::{Scratch, largest_parallel_lane_bytes, scratch_bytes};

// ---------------------------------------------------------------------------
// Budgets and pool sizes
// ---------------------------------------------------------------------------

/// The scratch budget: `BUTTERFLY_SCRATCH_BUDGET_GB`, else 30 % of the RSS
/// budget (itself 80 % of the memory limit).
pub fn scratch_budget_bytes() -> u64 {
    if let Ok(s) = std::env::var("BUTTERFLY_SCRATCH_BUDGET_GB")
        && let Ok(v) = s.trim().parse::<f64>()
        && v.is_finite()
        && v > 0.0
    {
        return (v * (1u64 << 30) as f64) as u64;
    }
    super::rss_budget_bytes() * 3 / 10
}

/// Rayon workers for a host of `cores` threads serving a graph of at most
/// `n_nodes_max` nodes under `scratch_budget` bytes of scratch:
/// `min(cores − 2, budget / largest lane)`, never below 2 (#631 keeps two
/// cores for the async runtime; two lanes keep the matrices parallel).
pub fn rayon_workers(cores: usize, n_nodes_max: usize, scratch_budget: u64) -> usize {
    let by_cores = cores.saturating_sub(2);
    let lane = largest_parallel_lane_bytes(n_nodes_max).max(1);
    let by_budget = (scratch_budget / lane) as usize;
    by_cores.min(by_budget).max(2)
}

/// Runtime worker threads: `BUTTERFLY_TOKIO_WORKERS`, else `min(cores, 16)`.
/// REST compute runs on these threads (`block_in_place`), each keeping its
/// own scratch: 96 of them on a 96-thread host was the second multiplier.
pub fn tokio_workers(cores: usize) -> usize {
    if let Ok(s) = std::env::var("BUTTERFLY_TOKIO_WORKERS")
        && let Ok(v) = s.trim().parse::<usize>()
        && v > 0
    {
        return v;
    }
    cores.clamp(2, 16)
}

/// Blocking-pool threads (`spawn_blocking`, `block_in_place` hand-offs):
/// `BUTTERFLY_BLOCKING_THREADS`, else 64 (tokio's default is 512, each a
/// potential scratch holder for its 10 s keep-alive).
pub fn blocking_threads() -> usize {
    if let Ok(s) = std::env::var("BUTTERFLY_BLOCKING_THREADS")
        && let Ok(v) = s.trim().parse::<usize>()
        && v > 0
    {
        return v;
    }
    64
}

/// The runtime the serve runs on: bounded worker and blocking pools.
pub fn runtime() -> std::io::Result<tokio::runtime::Runtime> {
    let cores = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .worker_threads(tokio_workers(cores))
        .max_blocking_threads(blocking_threads())
        .thread_name("tokio-rt-worker")
        .build()
}

/// The node count of the largest edge-based graph a data source holds —
/// what every per-lane scratch is sized by. Read from the 12-byte header
/// of the `ebg.nodes` table (container section `shared/ebg.nodes`, or any
/// region's `…/ebg.nodes`; a step directory's `step4/ebg.nodes`) without
/// loading anything. `None` when the source cannot be read that way.
pub fn max_ebg_nodes(source: &super::DataSource<'_>) -> Option<u32> {
    use std::io::{Read, Seek, SeekFrom};
    fn n_nodes_from_header(header: &[u8; 12]) -> u32 {
        u32::from_le_bytes([header[8], header[9], header[10], header[11]])
    }
    match source {
        super::DataSource::Container(path) => {
            let c = crate::formats::butterfly_dat::Container::open(path).ok()?;
            let mut f = std::fs::File::open(path).ok()?;
            let mut best: Option<u32> = None;
            for entry in c.sections_with_prefix("") {
                if !(entry.name == "shared/ebg.nodes" || entry.name.ends_with("/ebg.nodes")) {
                    continue;
                }
                f.seek(SeekFrom::Start(entry.offset)).ok()?;
                let mut header = [0u8; 12];
                f.read_exact(&mut header).ok()?;
                let n = n_nodes_from_header(&header);
                best = Some(best.map_or(n, |b| b.max(n)));
            }
            best
        }
        super::DataSource::Directory(dir) => {
            let candidates = [dir.join("step4").join("ebg.nodes"), dir.join("ebg.nodes")];
            for p in candidates {
                if let Ok(mut f) = std::fs::File::open(&p) {
                    let mut header = [0u8; 12];
                    if f.read_exact(&mut header).is_ok() {
                        return Some(n_nodes_from_header(&header));
                    }
                }
            }
            None
        }
    }
}

// ---------------------------------------------------------------------------
// Need estimates
// ---------------------------------------------------------------------------

/// Lanes a parallel request over `n_items` items will occupy: at most one
/// per rayon worker, at least one.
pub fn lanes(n_items: usize) -> usize {
    n_items.min(rayon::current_num_threads()).max(1)
}

/// Bytes a many-to-many matrix will need: the dense cell buffers (two
/// u32 channels + a mask byte per cell, however sparse the output) plus one
/// bucket lane per worker it reaches.
pub fn need_matrix(
    n_sources: usize,
    n_targets: usize,
    lanes: usize,
    n_nodes: usize,
    two_channel: bool,
) -> u64 {
    let cells = (n_sources as u64) * (n_targets as u64);
    let kind = if two_channel {
        Scratch::BucketM2M2ch
    } else {
        Scratch::BucketM2M
    };
    cells * 9 + lanes as u64 * scratch_bytes(kind, n_nodes)
}

/// Bytes an isochrone batch will need: one PHAST lane per worker plus the
/// stamped tiles and rings (~2 MB per origin, measured on Belgium).
pub fn need_isochrone(n_origins: usize, lanes: usize, n_nodes: usize) -> u64 {
    lanes as u64 * scratch_bytes(Scratch::Phast, n_nodes) + n_origins as u64 * (2 << 20)
}

/// Bytes a route batch will need: one P2P query state per worker plus the
/// polylines (~16 KB per pair).
pub fn need_route_batch(n_pairs: usize, workers: usize, n_nodes: usize) -> u64 {
    workers as u64 * scratch_bytes(Scratch::CchQuery, n_nodes) + n_pairs as u64 * (16 << 10)
}

/// Bytes an edges batch / flow exchange will need: one tree lane per worker
/// plus the per-edge rows (~24 B × ~400 edges per pair).
pub fn need_edges(n_pairs: usize, lanes: usize, n_nodes: usize) -> u64 {
    lanes as u64 * scratch_bytes(Scratch::Tree, n_nodes) + n_pairs as u64 * 24 * 400
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

static HEAVY_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// Bytes the admitted requests declared and may not have allocated yet:
/// two requests admitted back to back against the same low RSS would
/// otherwise both allocate their lanes. Counted until each permit drops
/// (conservative while a request's allocation is already in the RSS).
static RESERVED_BYTES: AtomicU64 = AtomicU64::new(0);

/// Held by an admitted heavy request for its whole life (streams included):
/// while any permit exists, a refusal is `Busy` (retry), not `TooLarge`,
/// and the request's declared bytes stay reserved.
#[derive(Debug)]
pub struct HeavyPermit {
    need: u64,
}

impl Drop for HeavyPermit {
    fn drop(&mut self) {
        HEAVY_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
        RESERVED_BYTES.fetch_sub(self.need, Ordering::AcqRel);
    }
}

/// Heavy requests currently admitted.
pub fn heavy_in_flight() -> usize {
    HEAVY_IN_FLIGHT.load(Ordering::Acquire)
}

/// Bytes reserved by the heavy requests in flight.
pub fn reserved_bytes() -> u64 {
    RESERVED_BYTES.load(Ordering::Acquire)
}

/// Why a request was not admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// Nothing else is running and it still does not fit: it never will on
    /// this server — split the request.
    TooLarge { need: u64, headroom: u64 },
    /// It would fit once the heavy work in flight is done.
    Busy {
        need: u64,
        headroom: u64,
        in_flight: usize,
    },
}

/// Seconds a `Busy` caller should wait before retrying.
pub const RETRY_AFTER_SECS: u64 = 5;

fn gib(b: u64) -> f64 {
    b as f64 / (1u64 << 30) as f64
}

impl Refused {
    pub fn message(&self) -> String {
        match self {
            Refused::TooLarge { need, headroom } => format!(
                "request needs about {:.1} GiB of memory, this server has {:.1} GiB free: split it (fewer origins × destinations per call) (#655)",
                gib(*need),
                gib(*headroom)
            ),
            Refused::Busy {
                need,
                headroom,
                in_flight,
            } => format!(
                "request needs about {:.1} GiB of memory, {:.1} GiB are free while {} heavy request(s) run: retry in {} s (#655)",
                gib(*need),
                gib(*headroom),
                in_flight,
                RETRY_AFTER_SECS
            ),
        }
    }

    /// REST: 413 (TooLarge) or 503 + `Retry-After` (Busy), JSON error body.
    pub fn into_rest_response(self) -> Response {
        let body = Json(ErrorResponse::new(self.message()));
        match self {
            Refused::TooLarge { .. } => (StatusCode::PAYLOAD_TOO_LARGE, body).into_response(),
            Refused::Busy { .. } => (
                StatusCode::SERVICE_UNAVAILABLE,
                [(header::RETRY_AFTER, RETRY_AFTER_SECS.to_string())],
                body,
            )
                .into_response(),
        }
    }

    /// Flight: `RESOURCE_EXHAUSTED` (TooLarge) or `UNAVAILABLE` (Busy).
    pub fn into_status(self) -> tonic::Status {
        match self {
            Refused::TooLarge { .. } => tonic::Status::resource_exhausted(self.message()),
            Refused::Busy { .. } => tonic::Status::unavailable(self.message()),
        }
    }
}

/// The decision, with every input injected (tests): `rss` is re-read after
/// `evict` freed idle scratch, `in_flight` / `reserved` are the heavy
/// count and the bytes the in-flight requests declared at the time.
pub fn admit_with(
    need: u64,
    budget: u64,
    mut rss: impl FnMut() -> u64,
    evict: impl FnOnce() -> usize,
    in_flight: usize,
    reserved: u64,
) -> Result<(), Refused> {
    let fits = |rss: u64| rss.saturating_add(reserved).saturating_add(need) <= budget;
    let mut cur = rss();
    if !fits(cur) {
        // Idle scratch of earlier requests is resident but reusable or
        // free-able: drop it before deciding.
        let _ = evict();
        cur = rss();
    }
    if fits(cur) {
        return Ok(());
    }
    let headroom = budget.saturating_sub(cur).saturating_sub(reserved);
    if in_flight > 0 {
        Err(Refused::Busy {
            need,
            headroom,
            in_flight,
        })
    } else {
        Err(Refused::TooLarge { need, headroom })
    }
}

/// Admit a heavy request needing `need` bytes against the RSS budget, or
/// refuse it. The permit must be held until the request's last byte is out.
pub fn admit(need: u64) -> Result<HeavyPermit, Refused> {
    let budget = super::rss_budget_bytes();
    let in_flight = heavy_in_flight();
    let rss = || vm_rss_bytes().unwrap_or(0);
    let evict = || crate::evictable::evict_idle(Duration::from_secs(1));
    match admit_with(need, budget, rss, evict, in_flight, reserved_bytes()) {
        Ok(()) => {
            HEAVY_IN_FLIGHT.fetch_add(1, Ordering::AcqRel);
            RESERVED_BYTES.fetch_add(need, Ordering::AcqRel);
            Ok(HeavyPermit { need })
        }
        Err(r) => {
            tracing::warn!(need_gib = gib(need), reason = %r.message(), "heavy request refused (#655)");
            Err(r)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cgroup_limit_parses_bytes_and_max() {
        assert_eq!(parse_cgroup_limit("68719476736\n"), Some(64 << 30));
        assert_eq!(parse_cgroup_limit("max\n"), None);
        assert_eq!(parse_cgroup_limit("9223372036854771712"), None); // v1 unlimited
        assert_eq!(parse_cgroup_limit("garbage"), None);
    }

    #[test]
    fn meminfo_total_is_read_in_bytes() {
        let s = "MemTotal:       263567444 kB\nMemFree:  12 kB\n";
        assert_eq!(parse_meminfo_total(s), Some(263_567_444 * 1024));
        assert_eq!(parse_meminfo_total("MemFree: 1 kB"), None);
    }

    #[test]
    fn the_limit_is_the_smallest_that_applies() {
        assert_eq!(
            smallest_limit([Some(251 << 30), Some(64 << 30), None]),
            Some(64 << 30)
        );
        assert_eq!(smallest_limit([None, None]), None);
    }

    #[test]
    fn rayon_workers_follow_the_budget_not_the_cores() {
        // Belgium foot, 5.25 M nodes: a 2-channel lane is 173 MB.
        let n = 5_250_000;
        assert_eq!(largest_parallel_lane_bytes(n), 33 * 5_250_000);
        // 96-thread host, 15 GiB of scratch: 92 lanes fit, cores allow 94.
        assert_eq!(rayon_workers(96, n, 15 << 30), 92);
        assert_eq!(rayon_workers(96, n, 20 << 30), 94);
        // 2 GiB of scratch: 12 lanes, whatever the core count.
        assert_eq!(rayon_workers(96, n, 2 << 30), 12);
        assert_eq!(rayon_workers(20, n, 2 << 30), 12);
        // Never below 2.
        assert_eq!(rayon_workers(96, n, 1 << 20), 2);
        assert_eq!(rayon_workers(2, n, 15 << 30), 2);
    }

    #[test]
    fn need_counts_cells_and_lanes() {
        let n = 1_000_000;
        // 1054² two-channel over 94 lanes: 10 MB of cells + 94 × 32 MB.
        let need = need_matrix(1054, 1054, 94, n, true);
        assert_eq!(need, 1054 * 1054 * 9 + 94 * 33 * 1_000_000);
        assert!(need_matrix(10, 10, 1, n, false) < need_matrix(10, 10, 1, n, true));
        assert!(need_isochrone(100, 8, n) > need_isochrone(1, 8, n));
    }

    #[test]
    fn admission_fits_fits_after_eviction_or_refuses() {
        let budget = 100;
        // Fits outright: no eviction asked.
        let mut evicted = 0;
        assert_eq!(
            admit_with(
                30,
                budget,
                || 60,
                || {
                    evicted += 1;
                    0
                },
                0,
                0
            ),
            Ok(())
        );
        assert_eq!(evicted, 0);
        // Fits once idle scratch is evicted.
        let rss = std::cell::Cell::new(90u64);
        assert_eq!(
            admit_with(
                30,
                budget,
                || rss.get(),
                || {
                    rss.set(60);
                    3
                },
                0,
                0
            ),
            Ok(())
        );
        // Still too big, nothing else running: TooLarge with the headroom.
        assert_eq!(
            admit_with(50, budget, || 80, || 0, 0, 0),
            Err(Refused::TooLarge {
                need: 50,
                headroom: 20
            })
        );
        // Still too big while heavy work runs: Busy.
        assert_eq!(
            admit_with(50, budget, || 80, || 0, 2, 0),
            Err(Refused::Busy {
                need: 50,
                headroom: 20,
                in_flight: 2
            })
        );
    }

    #[test]
    fn permits_count_heavy_work_in_flight_and_its_bytes() {
        let before = heavy_in_flight();
        let reserved_before = reserved_bytes();
        {
            let _p = admit(0).expect("zero bytes always fit");
            assert_eq!(heavy_in_flight(), before + 1);
        }
        assert_eq!(heavy_in_flight(), before);
        assert_eq!(reserved_bytes(), reserved_before);
    }

    #[test]
    fn reserved_bytes_of_requests_in_flight_count_against_the_budget() {
        // 60 resident + 30 reserved by an admitted request + 30 asked: no.
        assert_eq!(
            admit_with(30, 100, || 60, || 0, 1, 30),
            Err(Refused::Busy {
                need: 30,
                headroom: 10,
                in_flight: 1
            })
        );
        // Without the reservation it would have fit.
        assert_eq!(admit_with(30, 100, || 60, || 0, 1, 0), Ok(()));
    }

    #[test]
    fn refusals_map_to_413_and_503_with_retry_after() {
        let r = Refused::TooLarge {
            need: 3 << 30,
            headroom: 1 << 30,
        }
        .into_rest_response();
        assert_eq!(r.status(), StatusCode::PAYLOAD_TOO_LARGE);
        let r = Refused::Busy {
            need: 3 << 30,
            headroom: 1 << 30,
            in_flight: 1,
        }
        .into_rest_response();
        assert_eq!(r.status(), StatusCode::SERVICE_UNAVAILABLE);
        assert_eq!(
            r.headers().get(header::RETRY_AFTER).unwrap(),
            &RETRY_AFTER_SECS.to_string()
        );
        let s = Refused::TooLarge {
            need: 1,
            headroom: 0,
        }
        .into_status();
        assert_eq!(s.code(), tonic::Code::ResourceExhausted);
        let s = Refused::Busy {
            need: 1,
            headroom: 0,
            in_flight: 1,
        }
        .into_status();
        assert_eq!(s.code(), tonic::Code::Unavailable);
    }

    #[test]
    fn tokio_pools_are_bounded() {
        assert_eq!(tokio_workers(96), 16);
        assert_eq!(tokio_workers(8), 8);
        assert_eq!(tokio_workers(1), 2);
        assert_eq!(blocking_threads(), 64);
    }
}
