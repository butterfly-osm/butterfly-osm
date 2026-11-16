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
//! 3. **Admission** (`admit`): a heavy request states what it will need
//!    (`Need`: its output plus one lane of scratch per worker it would
//!    like). It is granted as many of those lanes as the RSS budget leaves
//!    room for — idle scratch evicted first — and runs on a request-scoped
//!    pool of that size when that is fewer than the shared pool's workers
//!    (its scratch goes away with the request); the shared pool is never
//!    shrunk for it. When not even one lane fits it is refused — `413
//!    Payload Too Large` / `RESOURCE_EXHAUSTED` when nothing else is
//!    running (it can never fit: split it), `503 Service Unavailable` +
//!    `Retry-After` / `UNAVAILABLE` while other heavy work holds the
//!    memory. Never a crash.
//!
//! The per-node constants are pinned by tests next to each state struct
//! (`heap_bytes()` ≤ the formula, and the formula is within 25 % of it), so
//! a field added to a scratch struct fails a test instead of silently
//! shrinking the bound.

use std::sync::Arc;
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

/// The memory this process holds that the kernel cannot reclaim, from a
/// `/proc/<pid>/status` body: `RssAnon` (heap, stacks, query scratch) plus
/// `VmLck` (pages pinned with `mlock`, i.e. `--lock-container`), in bytes.
///
/// `VmRSS` also counts `RssFile`: the clean, file-backed pages of the
/// mmapped graph container, which the kernel drops under pressure instead
/// of OOM-killing. Budgeting against `VmRSS` charged ~13 GB of reclaimable
/// page cache to the requests (refusals with plenty of real headroom) while
/// it said nothing about anon growth.
pub fn parse_status_unreclaimable(status: &str) -> Option<u64> {
    let mut anon: Option<u64> = None;
    let mut locked: u64 = 0;
    for line in status.lines() {
        let kb = |rest: &str| -> Option<u64> { rest.split_whitespace().next()?.parse().ok() };
        if let Some(rest) = line.strip_prefix("RssAnon:") {
            anon = kb(rest);
        } else if let Some(rest) = line.strip_prefix("VmLck:") {
            locked = kb(rest).unwrap_or(0);
        }
    }
    anon.map(|a| (a + locked) * 1024)
}

/// Unreclaimable bytes of this process (`RssAnon + VmLck`); falls back to
/// `VmRSS` where `RssAnon` is not reported (pre-4.5 kernels).
pub fn unreclaimable_bytes() -> Option<u64> {
    let s = std::fs::read_to_string("/proc/self/status").ok()?;
    parse_status_unreclaimable(&s).or_else(|| vm_rss_bytes())
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

/// Lanes a parallel request over `n_items` items would like: at most one
/// per rayon worker, at least one.
pub fn lanes(n_items: usize) -> usize {
    n_items.min(rayon::current_num_threads()).max(1)
}

/// What a heavy request will allocate: a fixed part (its output) plus one
/// lane of scratch per worker it reaches. Admission grants as many of the
/// `lanes` wanted as the memory allows (at least one) and the request runs
/// on that many — the pool everybody shares is never shrunk for it.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Need {
    /// Bytes independent of the parallelism (cells, rows, polygons).
    pub fixed: u64,
    /// Bytes of one lane of scratch.
    pub lane: u64,
    /// Lanes the request would like.
    pub lanes: usize,
}

/// Bytes per cell of the REST `/table` JSON body on top of the matrix
/// itself (#661): the duration and distance grids as `Option<f64>` rows
/// (2 × 16 B) plus the serialized text (~2 × 12 B), with margin.
pub const REST_TABLE_BYTES_PER_CELL: u64 = 64;

/// Peak unreclaimable bytes per CCH edge of one recustomization (#661).
pub const RECUSTOMIZE_BYTES_PER_EDGE: u64 = 64;

impl Need {
    /// A many-to-many matrix: two u32 channels + a mask byte per cell
    /// (however sparse the output) plus one bucket lane per worker.
    pub fn matrix(
        n_sources: usize,
        n_targets: usize,
        lanes: usize,
        n_nodes: usize,
        two_channel: bool,
    ) -> Need {
        let cells = (n_sources as u64) * (n_targets as u64);
        let kind = if two_channel {
            Scratch::BucketM2M2ch
        } else {
            Scratch::BucketM2M
        };
        Need {
            fixed: cells * 9,
            lane: scratch_bytes(kind, n_nodes),
            lanes,
        }
    }

    /// A REST `/table` (#661): the matrix above plus what the REST path
    /// adds per request — the JSON body (two `Option<f64>` grids built and
    /// serialized: [`REST_TABLE_BYTES_PER_CELL`]), the prefix-sum buckets
    /// and stamps sized by the graph (12 B per node, 16 counted), and the
    /// snapped endpoints with their seed sets. A grid of at most 100 cells
    /// runs on the calling thread's sequential engine: one lane, not one
    /// per worker.
    pub fn table_rest(
        n_sources: usize,
        n_targets: usize,
        lanes: usize,
        n_nodes: usize,
        two_channel: bool,
    ) -> Need {
        let cells = (n_sources as u64) * (n_targets as u64);
        let lanes = if cells <= 100 { 1 } else { lanes };
        let mut need = Need::matrix(n_sources, n_targets, lanes, n_nodes, two_channel);
        need.fixed = need
            .fixed
            .saturating_add(cells * REST_TABLE_BYTES_PER_CELL)
            .saturating_add(n_nodes as u64 * 16)
            .saturating_add((n_sources + n_targets) as u64 * (64 << 10));
        need
    }

    /// An isochrone batch: one PHAST lane per worker plus the stamped tiles
    /// and rings (~2 MB per origin, measured).
    pub fn isochrone(n_origins: usize, lanes: usize, n_nodes: usize) -> Need {
        Need {
            fixed: n_origins as u64 * (2 << 20),
            lane: scratch_bytes(Scratch::Phast, n_nodes),
            lanes,
        }
    }

    /// A route batch: one P2P query state per worker thread plus the
    /// polylines (~16 KB per pair). Its workers are its own (not rayon).
    pub fn route_batch(n_pairs: usize, workers: usize, n_nodes: usize) -> Need {
        Need {
            fixed: n_pairs as u64 * (16 << 10),
            lane: scratch_bytes(Scratch::CchQuery, n_nodes),
            lanes: workers.max(1),
        }
    }

    /// An edges batch / flow exchange: one tree lane per worker plus the
    /// per-edge rows (~24 B × ~400 edges per pair).
    pub fn edges(n_pairs: usize, lanes: usize, n_nodes: usize) -> Need {
        Need {
            fixed: n_pairs as u64 * 24 * 400,
            lane: scratch_bytes(Scratch::Tree, n_nodes),
            lanes,
        }
    }

    /// An avoid/exclude recustomization over `n_cch_edges` CCH edges (up +
    /// down): the stored weight set (time and distance, triangle-relaxed
    /// arrays plus their flat copies) and the transient arrays of the
    /// customization. Bytes per edge measured (#661), with margin.
    pub fn recustomize(n_cch_edges: usize) -> Need {
        Need {
            fixed: n_cch_edges as u64 * RECUSTOMIZE_BYTES_PER_EDGE,
            lane: 0,
            lanes: 1,
        }
    }

    /// Bytes at `lanes` lanes.
    pub fn bytes(&self, lanes: usize) -> u64 {
        self.fixed
            .saturating_add(self.lane.saturating_mul(lanes as u64))
    }
}

// ---------------------------------------------------------------------------
// Admission
// ---------------------------------------------------------------------------

static HEAVY_IN_FLIGHT: AtomicUsize = AtomicUsize::new(0);
/// Bytes the admitted requests declared and may not have allocated yet:
/// two requests admitted back to back against the same low reading would
/// otherwise both allocate their lanes. Counted until each permit drops
/// (conservative while a request's allocation is already resident).
static RESERVED_BYTES: AtomicU64 = AtomicU64::new(0);
/// Requests queued in [`admit_wait`].
static WAITING: AtomicUsize = AtomicUsize::new(0);
/// Unreclaimable bytes once the serve is ready (graph loaded, nothing in
/// flight): what no request can ever free. A request that does not fit in
/// `budget − baseline` never will (413); one that fits there is only busy.
static BASELINE_BYTES: AtomicU64 = AtomicU64::new(0);
/// Check-then-reserve is one step: without it two admissions read the same
/// memory and both take the same headroom.
static ADMIT_LOCK: parking_lot::Mutex<()> = parking_lot::Mutex::new(());
/// Last idle-scratch eviction asked by admission (ms since process start):
/// a queue of waiters must not walk the scratch registry hundreds of times
/// a second.
static LAST_EVICT_MS: AtomicU64 = AtomicU64::new(0);

fn released() -> &'static tokio::sync::Notify {
    static N: std::sync::OnceLock<tokio::sync::Notify> = std::sync::OnceLock::new();
    N.get_or_init(tokio::sync::Notify::new)
}

fn now_ms() -> u64 {
    static START: std::sync::OnceLock<std::time::Instant> = std::sync::OnceLock::new();
    START
        .get_or_init(std::time::Instant::now)
        .elapsed()
        .as_millis() as u64
}

fn env_usize(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|s| s.trim().parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(default)
}

/// Hard cap on heavy requests running at once, whatever the estimates say:
/// `BUTTERFLY_MAX_HEAVY`, default 8. An estimate can be wrong; a count
/// cannot. Each matrix already fans out over the whole rayon pool, so more
/// concurrent requests add memory, not throughput.
pub fn max_heavy() -> usize {
    env_usize("BUTTERFLY_MAX_HEAVY", 8)
}

/// How long a request waits in the admission queue before a 503:
/// `BUTTERFLY_ADMISSION_WAIT_MS`, default 15 s (under the 120 s request
/// timeout, above a typical matrix).
pub fn admission_wait() -> Duration {
    Duration::from_millis(env_usize("BUTTERFLY_ADMISSION_WAIT_MS", 15_000) as u64)
}

/// Requests that may queue at once: `BUTTERFLY_ADMISSION_QUEUE`, default
/// 128. Beyond, an immediate 503.
pub fn max_waiting() -> usize {
    env_usize("BUTTERFLY_ADMISSION_QUEUE", 128)
}

/// Record the unreclaimable floor once the serve is ready (see
/// [`BASELINE_BYTES`]).
pub fn set_baseline_now() {
    if let Some(b) = unreclaimable_bytes() {
        BASELINE_BYTES.store(b, Ordering::Release);
        tracing::info!(
            baseline_gib = gib(b),
            budget_gib = gib(super::rss_budget_bytes()),
            max_heavy = max_heavy(),
            "admission baseline (unreclaimable memory at ready) (#661)"
        );
    }
}

/// Held by an admitted heavy request for its whole life (streams included):
/// while any permit exists, a refusal is `Busy` (retry), not `TooLarge`,
/// and the request's declared bytes stay reserved.
#[derive(Debug)]
pub struct HeavyPermit {
    reserved: u64,
}

impl Drop for HeavyPermit {
    fn drop(&mut self) {
        HEAVY_IN_FLIGHT.fetch_sub(1, Ordering::AcqRel);
        RESERVED_BYTES.fetch_sub(self.reserved, Ordering::AcqRel);
        released().notify_waiters();
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

/// Requests waiting in the admission queue.
pub fn waiting() -> usize {
    WAITING.load(Ordering::Acquire)
}

/// Why a request was not admitted.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Refused {
    /// Even with nothing else running it would not fit: split it.
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
                "request needs about {:.1} GiB of memory and this server can give one request at most {:.1} GiB: split it (fewer origins × destinations per call), bound it (radius_km, max_minutes), or use the Arrow Flight matrix action for bulk matrices (#655, #661)",
                gib(*need),
                gib(*headroom)
            ),
            Refused::Busy {
                need,
                headroom,
                in_flight,
            } => format!(
                "server busy: request needs about {:.1} GiB of memory, {:.1} GiB are free while {} heavy request(s) run: retry in {} s (#655)",
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

/// What admission knows about the process when it decides.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Ledger {
    /// Unreclaimable bytes budget (80 % of the cgroup limit by default).
    pub budget: u64,
    /// Heavy requests running.
    pub in_flight: usize,
    /// Bytes the running requests declared.
    pub reserved: u64,
    /// Hard cap on running heavy requests.
    pub max_heavy: usize,
    /// Unreclaimable bytes at ready (0 = unknown).
    pub baseline: u64,
}

/// Admit `need` with ALL the lanes it wants, or refuse it, with every input
/// injected (tests): `used` is the unreclaimable memory, re-read after
/// `evict` freed idle scratch (asked only when it does not fit).
///
/// #661: no partial grant any more. A partial grant ran the request on a
/// request-scoped pool of fresh threads, each allocating its own scratch
/// (and churning allocator arenas as the pool died): under a burst every
/// admitted request added its own lanes on top of the shared pool's, the
/// multiplier behind the measured ~10× underestimate. A request now runs
/// on the shared pool, whose per-thread scratch is reused, and reserves
/// the cold-thread upper bound for its lanes until it ends.
pub fn admit_lanes_with(
    need: &Need,
    ledger: Ledger,
    mut used: impl FnMut() -> u64,
    evict: impl FnOnce() -> usize,
) -> Result<usize, Refused> {
    let lanes = need.lanes.max(1);
    let want = need.bytes(lanes);
    // Never fits, whatever runs: the budget left above the boot floor.
    let ceiling = ledger.budget.saturating_sub(ledger.baseline);
    if ledger.baseline > 0 && want > ceiling {
        return Err(Refused::TooLarge {
            need: want,
            headroom: ceiling,
        });
    }
    let free = |used: u64| -> u64 {
        ledger
            .budget
            .saturating_sub(used)
            .saturating_sub(ledger.reserved)
    };
    let mut cur = used();
    if ledger.in_flight >= ledger.max_heavy {
        return Err(Refused::Busy {
            need: want,
            headroom: free(cur),
            in_flight: ledger.in_flight,
        });
    }
    if free(cur) < want {
        // Idle scratch of earlier requests is resident but free-able: drop
        // it before refusing.
        let _ = evict();
        cur = used();
    }
    if free(cur) >= want {
        return Ok(lanes);
    }
    if ledger.in_flight == 0 && ledger.baseline == 0 {
        // No floor known yet: nothing runs and it does not fit.
        return Err(Refused::TooLarge {
            need: want,
            headroom: free(cur),
        });
    }
    Err(Refused::Busy {
        need: want,
        headroom: free(cur),
        in_flight: ledger.in_flight,
    })
}

/// An admitted heavy request: its permit, the lanes it may use, and the
/// pool to run on when that is fewer than the shared pool's workers (a
/// request-scoped pool: its threads, and the scratch they keep, go away
/// with the request).
pub struct Admitted {
    pub permit: HeavyPermit,
    pub lanes: usize,
    pub pool: Option<Arc<rayon::ThreadPool>>,
}

/// Run `f` on the request's pool when it has one (every `par_iter` inside
/// then uses that pool), on the shared pool otherwise.
///
/// `install` runs `f` on one of the pool's own threads, which has no Tokio
/// context: the streamed producers `f` starts (`spawn_blocking`) would
/// panic with "there is no reactor running". The caller's runtime handle,
/// when there is one, is entered on that thread for the duration of `f`.
pub fn with_pool<R: Send>(
    pool: &Option<Arc<rayon::ThreadPool>>,
    f: impl FnOnce() -> R + Send,
) -> R {
    match pool {
        Some(p) => {
            let handle = tokio::runtime::Handle::try_current().ok();
            p.install(move || {
                let _runtime = handle.as_ref().map(|h| h.enter());
                f()
            })
        }
        None => f(),
    }
}

/// A request-scoped pool of `lanes` workers when that is fewer than the
/// shared pool offers; `None` to use the shared pool.
pub fn request_pool(lanes: usize) -> Option<Arc<rayon::ThreadPool>> {
    if lanes >= rayon::current_num_threads() {
        return None;
    }
    match rayon::ThreadPoolBuilder::new()
        .num_threads(lanes.max(1))
        .thread_name(|i| format!("bf-lane-{i}"))
        .build()
    {
        Ok(p) => Some(Arc::new(p)),
        Err(e) => {
            tracing::warn!(error = %e, lanes, "request pool not built; running on the shared pool (#655)");
            None
        }
    }
}

/// One admission attempt, no logging of refusals (the caller decides).
fn try_admit(need: Need) -> Result<Admitted, Refused> {
    let _g = ADMIT_LOCK.lock();
    let ledger = Ledger {
        budget: super::rss_budget_bytes(),
        in_flight: heavy_in_flight(),
        reserved: reserved_bytes(),
        max_heavy: max_heavy(),
        baseline: BASELINE_BYTES.load(Ordering::Acquire),
    };
    let used = || unreclaimable_bytes().unwrap_or(0);
    let evict = || {
        let now = now_ms();
        let last = LAST_EVICT_MS.load(Ordering::Relaxed);
        if now.saturating_sub(last) < 200 {
            return 0;
        }
        LAST_EVICT_MS.store(now, Ordering::Relaxed);
        crate::evictable::evict_idle(Duration::from_secs(1))
    };
    let lanes = admit_lanes_with(&need, ledger, used, evict)?;
    let reserved = need.bytes(lanes);
    HEAVY_IN_FLIGHT.fetch_add(1, Ordering::AcqRel);
    RESERVED_BYTES.fetch_add(reserved, Ordering::AcqRel);
    drop(_g);
    // #661: the shared pool, always (see `admit_lanes_with`).
    Ok(Admitted {
        permit: HeavyPermit { reserved },
        lanes,
        pool: None,
    })
}

fn count_refusal(r: &Refused) {
    let kind = match r {
        Refused::TooLarge { .. } => "too_large",
        Refused::Busy { .. } => "busy",
    };
    metrics::counter!("butterfly_admission_refused_total", "reason" => kind).increment(1);
    tracing::warn!(need_gib = gib(match r { Refused::TooLarge { need, .. } | Refused::Busy { need, .. } => *need }), reason = %r.message(), "heavy request refused (#655, #661)");
}

/// Admit a heavy request now with as many of its wanted lanes as fit (at
/// least one), or refuse it. The permit must be held until the request's
/// last byte is out. Synchronous callers (Flight producers) use this; the
/// REST handlers queue with [`admit_wait`].
pub fn admit(need: Need) -> Result<Admitted, Refused> {
    try_admit(need).inspect_err(count_refusal)
}

/// Decrements [`WAITING`] however the wait ends (admitted, refused, the
/// client gone and the future dropped).
struct WaitGuard;
impl Drop for WaitGuard {
    fn drop(&mut self) {
        WAITING.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Admit a heavy request, queueing for at most [`admission_wait`] while the
/// memory or the heavy slots are taken (woken as soon as a permit drops,
/// re-checked every 250 ms for memory freed otherwise). A request that can
/// never fit is refused at once (`TooLarge` → 413); a full queue or the
/// deadline gives `Busy` (503 + `Retry-After`). Back-pressure, never an
/// OOM kill.
pub async fn admit_wait(need: Need) -> Result<Admitted, Refused> {
    match try_admit(need) {
        Ok(a) => return Ok(a),
        Err(r @ Refused::TooLarge { .. }) => {
            count_refusal(&r);
            return Err(r);
        }
        Err(Refused::Busy { .. }) => {}
    }
    if WAITING.fetch_add(1, Ordering::AcqRel) >= max_waiting() {
        WAITING.fetch_sub(1, Ordering::AcqRel);
        let r = Refused::Busy {
            need: need.bytes(1),
            headroom: 0,
            in_flight: heavy_in_flight(),
        };
        count_refusal(&r);
        return Err(r);
    }
    let _guard = WaitGuard;
    let t0 = std::time::Instant::now();
    let deadline = t0 + admission_wait();
    loop {
        let notified = released().notified();
        tokio::pin!(notified);
        notified.as_mut().enable();
        match try_admit(need) {
            Ok(a) => {
                metrics::histogram!("butterfly_admission_wait_seconds")
                    .record(t0.elapsed().as_secs_f64());
                return Ok(a);
            }
            Err(r @ Refused::TooLarge { .. }) => {
                count_refusal(&r);
                return Err(r);
            }
            Err(r) => {
                let now = std::time::Instant::now();
                if now >= deadline {
                    count_refusal(&r);
                    return Err(r);
                }
                let slice = (deadline - now).min(Duration::from_millis(250));
                let _ = tokio::time::timeout(slice, notified).await;
            }
        }
    }
}

/// The prefix of a weights-resolution error that is a busy refusal, not a
/// bad request (see [`weights_error_response`]).
pub const BUSY_PREFIX: &str = "server busy:";

/// Admit from synchronous code already off the async scheduler
/// (`block_in_place`): the same queue semantics as [`admit_wait`], polled
/// every 100 ms. Used where the allocation happens deep inside a sync
/// path (an avoid-polygon recustomization).
pub fn admit_blocking(need: Need) -> Result<Admitted, Refused> {
    let deadline = std::time::Instant::now() + admission_wait();
    loop {
        match try_admit(need) {
            Ok(a) => return Ok(a),
            Err(r @ Refused::TooLarge { .. }) => {
                count_refusal(&r);
                return Err(r);
            }
            Err(r) => {
                if std::time::Instant::now() >= deadline {
                    count_refusal(&r);
                    return Err(r);
                }
                std::thread::sleep(Duration::from_millis(100));
            }
        }
    }
}

/// A weights-resolution error as a REST response: a busy refusal is
/// `503` + `Retry-After`, anything else stays `400`.
pub fn weights_error_response(e: String) -> Response {
    if e.starts_with(BUSY_PREFIX) {
        (
            StatusCode::SERVICE_UNAVAILABLE,
            [(header::RETRY_AFTER, RETRY_AFTER_SECS.to_string())],
            Json(ErrorResponse::new(e)),
        )
            .into_response()
    } else {
        (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response()
    }
}

/// Admission gauges, sampled on `/metrics` scrape.
pub fn export_gauges() {
    metrics::gauge!("butterfly_admission_in_flight").set(heavy_in_flight() as f64);
    metrics::gauge!("butterfly_admission_waiting").set(waiting() as f64);
    metrics::gauge!("butterfly_admission_reserved_bytes").set(reserved_bytes() as f64);
    metrics::gauge!("butterfly_admission_budget_bytes").set(super::rss_budget_bytes() as f64);
    metrics::gauge!("butterfly_admission_baseline_bytes")
        .set(BASELINE_BYTES.load(Ordering::Acquire) as f64);
    if let Some(u) = unreclaimable_bytes() {
        metrics::gauge!("butterfly_process_unreclaimable_bytes").set(u as f64);
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
        // 1054² two-channel over 94 lanes: 10 MB of cells + 94 × 33 MB.
        let need = Need::matrix(1054, 1054, 94, n, true);
        assert_eq!(need.bytes(94), 1054 * 1054 * 9 + 94 * 33 * 1_000_000);
        assert!(
            Need::matrix(10, 10, 1, n, false).bytes(1) < Need::matrix(10, 10, 1, n, true).bytes(1)
        );
        assert!(Need::isochrone(100, 8, n).bytes(8) > Need::isochrone(1, 8, n).bytes(8));
    }

    fn ledger(budget: u64, in_flight: usize, reserved: u64) -> Ledger {
        Ledger {
            budget,
            in_flight,
            reserved,
            max_heavy: 8,
            baseline: 0,
        }
    }

    #[test]
    fn admission_is_all_lanes_or_nothing() {
        let need = Need {
            fixed: 10,
            lane: 10,
            lanes: 8,
        };
        // 100 budget, 0 used: 90 needed, fits → all 8 lanes, no eviction.
        let mut evicted = 0;
        assert_eq!(
            admit_lanes_with(
                &need,
                ledger(100, 0, 0),
                || 0,
                || {
                    evicted += 1;
                    0
                }
            ),
            Ok(8)
        );
        assert_eq!(evicted, 0, "no eviction when it fits");
        // 60 used, one request in flight: 40 free < 90 → Busy, never a partial grant.
        assert_eq!(
            admit_lanes_with(&need, ledger(100, 1, 0), || 60, || 0),
            Err(Refused::Busy {
                need: 90,
                headroom: 40,
                in_flight: 1
            })
        );
        // Eviction frees room: admitted with all lanes.
        let used = std::cell::Cell::new(60u64);
        assert_eq!(
            admit_lanes_with(
                &need,
                ledger(100, 1, 0),
                || used.get(),
                || {
                    used.set(0);
                    3
                }
            ),
            Ok(8)
        );
        // Reserved bytes of requests in flight count.
        assert!(matches!(
            admit_lanes_with(&need, ledger(100, 1, 20), || 0, || 0),
            Err(Refused::Busy { headroom: 80, .. })
        ));
        // No floor known, nothing in flight, does not fit: TooLarge.
        assert_eq!(
            admit_lanes_with(&need, ledger(100, 0, 0), || 95, || 0),
            Err(Refused::TooLarge {
                need: 90,
                headroom: 5
            })
        );
    }

    #[test]
    fn rest_table_counts_the_json_and_runs_small_grids_on_one_lane() {
        let n = 5_730_722;
        // 3 × 30 = 90 cells: sequential engine, one lane.
        assert_eq!(Need::table_rest(3, 30, 88, n, true).lanes, 1);
        // 1500 × 3: every worker may take a target → 88 lanes.
        let t = Need::table_rest(1500, 3, 88, n, true);
        assert_eq!(t.lanes, 88);
        // 1000 × 1000 adds ≥ 64 MB of JSON over the bare matrix.
        let bare = Need::matrix(1000, 1000, 88, n, true).fixed;
        assert!(Need::table_rest(1000, 1000, 88, n, true).fixed >= bare + 64_000_000);
    }

    #[test]
    fn the_hard_cap_holds_whatever_the_estimate() {
        // A zero-byte estimate (the 10× underestimate of #661 taken to the
        // limit) still cannot put more than max_heavy requests in flight.
        let need = Need {
            fixed: 0,
            lane: 0,
            lanes: 1,
        };
        let mut l = ledger(1 << 40, 8, 0);
        assert!(matches!(
            admit_lanes_with(&need, l, || 0, || 0),
            Err(Refused::Busy { in_flight: 8, .. })
        ));
        l.in_flight = 7;
        assert_eq!(admit_lanes_with(&need, l, || 0, || 0), Ok(1));
    }

    #[test]
    fn too_large_only_when_it_never_fits_above_the_floor() {
        let need = Need {
            fixed: 30,
            lane: 10,
            lanes: 4,
        };
        // 70 B wanted. Budget 100, floor 20: it fits above the floor, so with
        // 80 used it is only busy, even with nothing in flight (memory held by
        // caches or not yet returned is not the request's fault).
        let mut l = ledger(100, 0, 0);
        l.baseline = 20;
        assert!(matches!(
            admit_lanes_with(&need, l, || 80, || 0),
            Err(Refused::Busy { .. })
        ));
        // Floor 40: 70 > 60 → TooLarge at once, before reading the memory.
        l.baseline = 40;
        assert_eq!(
            admit_lanes_with(&need, l, || panic!("not read"), || 0),
            Err(Refused::TooLarge {
                need: 70,
                headroom: 60
            })
        );
    }

    #[test]
    fn unreclaimable_is_anon_plus_locked_never_file() {
        // A measured boot-complete reading: 32.1 GB VmRSS = 19.0 anon + 13.1 file.
        let status = "VmRSS:\t32100000 kB\nRssAnon:\t19000000 kB\nRssFile:\t13100000 kB\nRssShmem:\t0 kB\nVmLck:\t0 kB\n";
        assert_eq!(parse_status_unreclaimable(status), Some(19_000_000 * 1024));
        // --lock-container pins file pages: they cannot be reclaimed either.
        let locked = "RssAnon:\t19000000 kB\nRssFile:\t13100000 kB\nVmLck:\t10000000 kB\n";
        assert_eq!(parse_status_unreclaimable(locked), Some(29_000_000 * 1024));
        assert_eq!(parse_status_unreclaimable("VmRSS: 1 kB\n"), None);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_waiter_is_admitted_when_a_permit_drops() {
        // Fill the heavy slots with zero-byte permits, queue one more, free one.
        let zero = Need {
            fixed: 0,
            lane: 0,
            lanes: 1,
        };
        let held: Vec<_> = (0..max_heavy()).filter_map(|_| admit(zero).ok()).collect();
        if heavy_in_flight() < max_heavy() {
            return; // another test holds slots concurrently; nothing to assert
        }
        let waiter = tokio::spawn(admit_wait(zero));
        tokio::time::sleep(Duration::from_millis(100)).await;
        assert!(!waiter.is_finished(), "queued, not refused");
        drop(held);
        let got = tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("woken by the permit drop")
            .expect("joined");
        assert!(got.is_ok());
    }

    #[test]
    fn permits_count_heavy_work_in_flight_and_its_bytes() {
        let before = heavy_in_flight();
        let reserved_before = reserved_bytes();
        {
            let adm = admit(Need {
                fixed: 0,
                lane: 0,
                lanes: 1,
            })
            .expect("zero bytes always fit");
            assert_eq!(heavy_in_flight(), before + 1);
            assert_eq!(adm.lanes, 1);
            drop(adm);
        }
        assert_eq!(heavy_in_flight(), before);
        assert_eq!(reserved_bytes(), reserved_before);
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_request_pool_keeps_the_runtime_context_for_spawned_producers() {
        // The streamed producers call `spawn_blocking` from inside the
        // pool: without the entered handle this panicked ("there is no
        // reactor running") on the first tiled matrix.
        let pool = request_pool(1);
        if pool.is_none() {
            return; // a 1-thread shared pool: nothing to scope
        }
        let joined = tokio::task::spawn_blocking(move || {
            with_pool(&pool, || {
                let h = tokio::task::spawn_blocking(|| 41 + 1);
                tokio::runtime::Handle::current()
                    .block_on(h)
                    .expect("joined")
            })
        })
        .await
        .expect("outer");
        assert_eq!(joined, 42);
    }

    #[test]
    fn a_request_pool_has_the_granted_lanes_and_the_shared_pool_is_untouched() {
        let shared = rayon::current_num_threads();
        let pool = request_pool(2);
        if shared > 2 {
            let p = pool
                .as_ref()
                .expect("fewer lanes than the shared pool → a pool");
            assert_eq!(with_pool(&pool, rayon::current_num_threads), 2);
            assert_eq!(p.current_num_threads(), 2);
        } else {
            assert!(pool.is_none());
        }
        assert_eq!(rayon::current_num_threads(), shared);
        assert!(request_pool(shared + 10).is_none());
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
