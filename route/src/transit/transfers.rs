//! Stop-to-stop walking transfer graph (ULTRA-style, with triangle
//! dominance restriction).
//!
//! At build time we precompute the foot-mode CCH walking time between
//! every pair of stops within `transfer_radius_m` (great-circle). The
//! initial unrestricted set is stored as a sparse adjacency list:
//!
//! ```text
//! offsets:    Vec<u32> (n_stops + 1)
//! neighbours: Vec<(StopIdx, walk_seconds)>   sorted by neighbour id
//! ```
//!
//! We then apply an ULTRA-style **triangle dominance restriction**:
//! any transfer `(u, v)` such that there exists a third stop `w` with
//! `walk(u, w) + walk(w, v) ≤ walk(u, v) + ε` is removed, because any
//! journey that used `(u, v)` can instead walk via `w` and board at `w`
//! (or continue to `v`) without delaying arrival. This is a provably
//! safe subset of the full ULTRA paper's transfer restriction: it
//! preserves every Pareto-optimal journey while dropping a large
//! fraction of redundant edges on dense multi-operator networks
//! (typical: 30-60% reduction on Belgium multi-feed).
//!
//! With the restriction in place, wider radii (we default to 2000 m)
//! are affordable — the graph stays sparse because inter-operator
//! neighbours are removed wherever a closer stop provides a superior
//! staging point.
//!
//! The CCH 1-to-N call itself is routed through `matrix::bucket_ch`
//! using the foot-mode `up_adj_flat` / `down_rev_flat` time metric.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use rayon::prelude::*;

use crate::server::query::CchQuery;
use crate::server::spatial::SpatialIndex;
use crate::server::state::ModeData;

use super::timetable::{StopIdx, Timetable};
use super::transfers_cache;

const METERS_PER_DEG_LAT: f64 = 111_000.0;
/// Approximation at ~50°N (cos(50°) ≈ 0.6428).
const METERS_PER_DEG_LON_AT_50: f64 = 71_400.0;

/// Sparse undirected transfer graph on [`StopIdx`].
#[derive(Debug, Clone)]
pub struct TransferGraph {
    /// `offsets[s]..offsets[s+1]` is the adjacency slice for stop `s`.
    offsets: Vec<u32>,
    /// Flat `(neighbour, walk_seconds)` pairs, sorted by neighbour per stop.
    neighbours: Vec<(StopIdx, u32)>,
    /// Number of stops (same as timetable).
    n_stops: usize,
    /// Staging buffer: edges added via `add_edge` before `finalise`.
    staging: Vec<(u32, u32, u32)>,
    /// SHA-256 hash of the timetable + transfer parameters that produced
    /// this graph. Used by the cache to avoid stale data.
    pub provenance: [u8; 32],
}

impl TransferGraph {
    /// Empty graph — every stop has zero neighbours.
    pub fn empty(n_stops: usize) -> Self {
        Self {
            offsets: vec![0; n_stops + 1],
            neighbours: Vec::new(),
            n_stops,
            staging: Vec::new(),
            provenance: [0u8; 32],
        }
    }

    /// Add an edge to a staging buffer. Call [`Self::finalise`] to build
    /// the CSR layout.
    pub fn add_edge(&mut self, from: StopIdx, to: StopIdx, walk_s: u32) {
        self.staging.push((from, to, walk_s));
    }

    /// Build the final CSR layout from the edges added via [`Self::add_edge`].
    pub fn finalise(&mut self) {
        let triples = std::mem::take(&mut self.staging);
        let n_stops = self.n_stops;
        let provenance = self.provenance;
        *self = TransferGraph::from_triples(n_stops, triples);
        self.provenance = provenance;
    }

    /// Get adjacency slice for a stop.
    pub fn neighbours(&self, s: StopIdx) -> impl Iterator<Item = (StopIdx, u32)> + '_ {
        let start = self.offsets[s as usize] as usize;
        let end = self.offsets[s as usize + 1] as usize;
        self.neighbours[start..end].iter().copied()
    }

    /// Number of stops covered.
    pub fn n_stops(&self) -> usize {
        self.n_stops
    }

    /// Total number of directed edges stored.
    pub fn n_edges(&self) -> usize {
        self.neighbours.len()
    }

    /// Access the raw offsets array (needed by the cache writer).
    pub fn offsets_raw(&self) -> &[u32] {
        &self.offsets
    }

    /// Access the raw neighbours buffer (needed by the cache writer).
    pub fn neighbours_raw(&self) -> &[(StopIdx, u32)] {
        &self.neighbours
    }

    /// Build a [`TransferGraph`] directly from pre-built CSR parts.
    /// Used by the cache reader (#117) to avoid the `from_triples`
    /// round-trip: the on-disk layout is already CSR, so the reader
    /// hands us the offsets and neighbours arrays and we wrap them
    /// verbatim. Zero triples-to-CSR rebuild.
    pub fn from_csr_parts(
        n_stops: usize,
        offsets: Vec<u32>,
        neighbours: Vec<(StopIdx, u32)>,
        provenance: [u8; 32],
    ) -> Self {
        debug_assert_eq!(offsets.len(), n_stops + 1);
        debug_assert_eq!(
            offsets.last().copied().unwrap_or(0) as usize,
            neighbours.len(),
            "offsets/neighbours length mismatch"
        );
        Self {
            offsets,
            neighbours,
            n_stops,
            staging: Vec::new(),
            provenance,
        }
    }

    /// Build a [`TransferGraph`] from a flat `(from, to, walk_s)` list.
    ///
    /// Duplicates for the same `(from, to)` pair are collapsed to the
    /// minimum walking time.
    pub fn from_triples(n_stops: usize, triples: Vec<(u32, u32, u32)>) -> Self {
        let mut triples = triples;
        // Collapse duplicates.
        triples.sort();
        triples.dedup_by(|a, b| {
            if a.0 == b.0 && a.1 == b.1 {
                if a.2 < b.2 {
                    b.2 = a.2;
                }
                true
            } else {
                false
            }
        });

        let mut offsets = vec![0u32; n_stops + 1];
        for (from, _, _) in &triples {
            offsets[*from as usize + 1] += 1;
        }
        for i in 1..=n_stops {
            offsets[i] += offsets[i - 1];
        }
        let mut neighbours = vec![(0u32, 0u32); triples.len()];
        let mut cursor = vec![0u32; n_stops];
        for (from, to, w) in &triples {
            let base = offsets[*from as usize];
            let off = cursor[*from as usize];
            neighbours[(base + off) as usize] = (*to, *w);
            cursor[*from as usize] += 1;
        }
        Self {
            offsets,
            neighbours,
            n_stops,
            staging: Vec::new(),
            provenance: [0u8; 32],
        }
    }

    /// Load a cached transfer graph from disk (if the provenance matches).
    pub fn load_cached(path: &Path, expected_provenance: [u8; 32]) -> Result<Option<Self>> {
        transfers_cache::read(path, expected_provenance)
    }

    /// Persist the graph to disk in cache format.
    pub fn save_cached(&self, path: &Path) -> Result<()> {
        transfers_cache::write(path, self)
    }
}

/// Options for the transfer precomputation.
#[derive(Debug, Clone)]
pub struct TransferBuildOptions {
    /// Maximum pairwise straight-line radius (meters).
    pub radius_m: u32,
    /// Upper bound on walk time (seconds). Edges exceeding this cap are dropped.
    pub max_walk_s: u32,
    /// Walking speed assumption (meters / second) — used for cap conversion only.
    pub walk_speed_mps: f64,
    /// Apply the triangle dominance restriction after the CCH precompute.
    /// Default `true` — disabling it is only useful for debugging.
    pub apply_ultra_restriction: bool,
    /// Slack (seconds) added to the dominating walk when comparing
    /// `walk(u, w) + walk(w, v)` against `walk(u, v)`. A small positive
    /// value prevents float/rounding ties from drop-flipping under
    /// rebuild noise. Default: 2 s.
    pub ultra_restriction_slack_s: u32,
    /// Fixed walk cost (seconds) for a transfer between two stops that
    /// share the same GTFS parent station (#112). GTFS models multiple
    /// platforms of a station as children of a common `location_type=1`
    /// parent, and the spec says those platforms are interchangeable:
    /// a rider alighting on platform A can board the same trip at
    /// platform B without any real walking. The foot CCH can't express
    /// that — platforms that don't snap to a foot road, or that are
    /// separated by a station building with no through-walking edge,
    /// get either missing or wildly wrong transfer costs. This knob
    /// short-circuits that by injecting a bidirectional edge between
    /// every pair of same-parent children with this fixed cost,
    /// *before* the ULTRA dominance restriction runs — so a genuine
    /// shorter walking transfer still dominates when the CCH actually
    /// has one. Default: 60 s (the conventional GTFS "min_transfer_time"
    /// floor for same-station transfers).
    pub same_station_transfer_s: u32,
    /// Cross-feed equivalence radius (meters) — approach A for #113.
    /// When multiple operators publish what is semantically the same
    /// physical stop under different feed prefixes (SNCB
    /// Bruxelles-Midi vs STIB Bruxelles-Midi metro entrance), the
    /// multi-feed loader keeps them as distinct stops at slightly
    /// different coordinates. This knob collapses that ambiguity by
    /// injecting a bidirectional transfer edge between every pair of
    /// stops whose GTFS id carries a *different* feed prefix and
    /// whose great-circle distance is below the threshold. The cost
    /// is [`Self::cross_feed_transfer_s`]. Default: 50 m, which
    /// empirically hits intermodal hubs (SNCB↔STIB, SNCB↔De Lijn, …)
    /// without false-positive merges on unrelated nearby stops. Set
    /// to 0 to disable the sweep.
    pub stop_merge_radius_m: u32,
    /// Walk cost (seconds) applied to cross-feed equivalence edges
    /// produced by the [`Self::stop_merge_radius_m`] sweep. Default:
    /// 30 s — a conservative "same physical place, short in-station
    /// walk to the other operator's platform" floor. Lower than the
    /// GTFS same-station default (60 s) because cross-feed pairs
    /// landed here already passed the strict distance check.
    pub cross_feed_transfer_s: u32,
}

impl Default for TransferBuildOptions {
    fn default() -> Self {
        Self {
            // ULTRA-style: wide enough to pick up cross-operator
            // interchanges (SNCB ↔ De Lijn at suburban rail/bus pairs
            // are frequently in the 1.2–1.8 km range once you walk
            // around station buildings).
            radius_m: 2_000,
            max_walk_s: 1_500,
            walk_speed_mps: 1.3,
            apply_ultra_restriction: true,
            ultra_restriction_slack_s: 2,
            same_station_transfer_s: 60,
            stop_merge_radius_m: 50,
            cross_feed_transfer_s: 30,
        }
    }
}

/// Version tag for the provenance hash. Bumped whenever the transfer
/// algorithm or hash format changes meaningfully so cached graphs built
/// under an older version are forcibly rejected.
///
/// History:
///   - v1: initial (rejected by v2 bump)
///   - v2: stop-id chain + build params
///   - v3: order-invariant stop-id digest (sorted before hashing)
///   - v4: adds stop coordinates + foot-CCH fingerprint + RAPTOR transfer-
///     closure algorithm identity (issue #106 + #109).
///   - v5: adds same-station child-pair injection with a fixed cost
///     (issue #112). The injected edges change the transfer graph even
///     when all other parameters are identical, so any cache written
///     under v4 is forcibly rejected.
///   - v6: adds cross-feed equivalence edges (issue #113) driven by
///     `stop_merge_radius_m` + `cross_feed_transfer_s`. Same
///     invalidation rationale as v5.
///   - v7: fixes an ULTRA restriction bug where zero-cost edges
///     (co-located platform stops that share a foot-CCH rank) were
///     dropped via spurious zero-cost triangles, disconnecting
///     whole stations from themselves. v6 caches silently held
///     broken data for those stations; v7 rebuilds.
const TRANSFER_ALGO_VERSION: u32 = 7;

/// Foot-CCH fingerprint: a stable identifier of the specific foot CCH
/// graph that was used to compute the cached transfer edges. Derived
/// from the topology's node / edge counts plus a sample of edge weights
/// — see `foot_cch_fingerprint` below. This does NOT need to be
/// cryptographically unique; it just needs to change whenever the foot
/// CCH is rebuilt, so that a stale transfer cache is rejected after an
/// OSM PBF refresh or a pipeline re-run.
pub type FootCchFingerprint = [u8; 32];

/// Compute a fingerprint for the foot CCH that the transfer build will
/// use. Streams across the CCH topology and weights, producing a stable
/// sha256 that changes whenever either side changes.
///
/// This is the bit of `compute_provenance` that depends on the road
/// graph identity and is separated out so that the caller can compute
/// it once and reuse it across cache writes / reads.
pub fn foot_cch_fingerprint(foot: &ModeData) -> FootCchFingerprint {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(b"butterfly.foot-cch.v1");
    h.update((foot.cch_topo.n_nodes as u64).to_le_bytes());
    h.update((foot.cch_topo.up_targets.len() as u64).to_le_bytes());
    h.update((foot.cch_topo.down_targets.len() as u64).to_le_bytes());
    // Hash every up-weight and down-weight: any recustomization changes
    // these, so the fingerprint is sensitive to CCH rebuilds. We feed
    // each `u32` to the hasher as four little-endian bytes — explicit
    // and safe, identical hash output on every target. Runs once per
    // server startup; the 91M-edge Belgium foot CCH hashes in ~200 ms
    // with this loop, only marginally slower than the old slice cast
    // because `Sha256::update` is bandwidth-bound on the input itself.
    for w in foot.cch_weights.up.iter() {
        h.update(w.to_le_bytes());
    }
    for w in foot.cch_weights.down.iter() {
        h.update(w.to_le_bytes());
    }
    let mut out = [0u8; 32];
    out.copy_from_slice(h.finalize().as_slice());
    out
}

/// Compute provenance hash for a (timetable, options, foot-CCH) tuple.
///
/// **Order-invariant**: the GTFS loader receives stops from
/// `gtfs_structures::Gtfs.stops`, which is a `HashMap` whose iteration
/// order is randomised per run. The resulting `Timetable.stops` Vec
/// therefore has a different element order on every server start. To
/// keep the on-disk transfer cache valid across restarts, the
/// provenance hash must NOT depend on stop ordering — only on the
/// underlying set of stop ids, stop coordinates, and the build
/// parameters.
///
/// We sort the stop ids before feeding them into the digest. Sorting
/// 64k short strings is microsecond-cheap and guarantees a stable hash
/// across HashMap re-orderings.
///
/// **Includes stop coordinates and foot CCH fingerprint** (issue #109):
/// without them, a stop that moves 200 m down the street keeps the same
/// hash and the cached transfer edges reference the old snap rank. A
/// foot CCH rebuilt from a new OSM PBF but with the same stop IDs also
/// slips past the cache check. Both are real operational failures.
pub fn compute_provenance(
    timetable: &Timetable,
    opts: &TransferBuildOptions,
    foot_fingerprint: &FootCchFingerprint,
) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(TRANSFER_ALGO_VERSION.to_le_bytes());
    h.update(foot_fingerprint);
    h.update((timetable.n_stops() as u64).to_le_bytes());
    h.update((timetable.n_routes() as u64).to_le_bytes());
    h.update(opts.radius_m.to_le_bytes());
    h.update(opts.max_walk_s.to_le_bytes());
    h.update((opts.apply_ultra_restriction as u8).to_le_bytes());
    h.update(opts.ultra_restriction_slack_s.to_le_bytes());
    h.update(opts.same_station_transfer_s.to_le_bytes());
    h.update(opts.stop_merge_radius_m.to_le_bytes());
    h.update(opts.cross_feed_transfer_s.to_le_bytes());

    // Order-invariant digest: sort the (id, lon, lat) tuples by id, then
    // fold each into the hash. Any feed change, coordinate drift, or
    // re-ordering of the underlying stop set now invalidates the cache
    // deterministically.
    //
    // Coordinates are rounded to 6 decimal places (~11 cm precision) to
    // avoid float-precision drift from HashMap re-insertion changing the
    // last digits between runs. This is stable enough to detect real
    // stop moves (which happen in metres, not cm).
    let mut stops_idx: Vec<usize> = (0..timetable.stops.len()).collect();
    stops_idx.sort_by(|&a, &b| timetable.stops[a].id.cmp(&timetable.stops[b].id));
    for idx in stops_idx {
        let s = &timetable.stops[idx];
        h.update((s.id.len() as u32).to_le_bytes());
        h.update(s.id.as_bytes());
        let lon_fixed = (s.lon * 1_000_000.0).round() as i64;
        let lat_fixed = (s.lat * 1_000_000.0).round() as i64;
        h.update(lon_fixed.to_le_bytes());
        h.update(lat_fixed.to_le_bytes());
    }

    let mut out = [0u8; 32];
    out.copy_from_slice(h.finalize().as_slice());
    out
}

/// Precompute a walking transfer graph for all stops in a timetable,
/// using Butterfly's foot-mode CCH for exact road-network distances.
///
/// ## Algorithm
///
/// 1. **Snap** every stop to the foot CCH rank space (linear scan).
/// 2. **2D grid prefilter**: bucket all snapped stops into a square
///    grid whose cells are `2 * radius_m` wide. Any pair of stops within
///    `radius_m` great-circle is guaranteed to live in the same cell or
///    in one of the 8 immediate neighbours, so per-source neighbour
///    lookups are `O(stops_per_cell + 8 * stops_per_neighbour_cell)`
///    instead of `O(N)` per source. Total prefilter cost: `O(N + N*k)`
///    where `k` is the average local neighbourhood size.
/// 3. **Spatial chunking**: every cell becomes one (or several, if the
///    cell is large) chunk. A chunk is processed as a single
///    `table_bucket_parallel` call whose sources are the cell's stops
///    and whose targets are the *union* of every source's prefilter
///    neighbours. Chunked bucket M2M amortises the per-call setup cost
///    over O(stops-per-chunk) sources. For Belgium (64k stops, 2 km
///    radius) this collapses ~64,000 single-source calls into ~500
///    chunked calls.
/// 4. **Per-chunk filter**: from the dense chunk matrix we only emit
///    triples whose great-circle distance was already in the prefilter
///    list and whose CCH distance is ≤ `opts.max_walk_s`. The dense
///    buffer is dropped immediately, so memory is bounded by the
///    largest chunk.
/// 5. **Parallel chunks** via Rayon — chunks are independent.
/// 6. **ULTRA dominance restriction** prunes triangle-dominated edges.
///
/// Net complexity: `O(N + N*k + chunks*chunk_setup)` instead of the
/// naive `O(N^2 + N * call_setup)`. On Belgium the rebuild drops from
/// ~11 minutes (single-source loop) to seconds.
pub fn build_transfer_graph(
    timetable: &Timetable,
    foot: &ModeData,
    spatial: &SpatialIndex,
    opts: &TransferBuildOptions,
) -> Result<TransferGraph> {
    let n_stops = timetable.n_stops();
    if n_stops == 0 {
        return Ok(TransferGraph::empty(0));
    }

    tracing::info!(
        stops = n_stops,
        radius_m = opts.radius_m,
        "precomputing stop-to-stop walking transfers"
    );

    // ---- 1. Snap every stop to the foot CCH rank space. ---------------
    let mut snapped_rank: Vec<Option<u32>> = Vec::with_capacity(n_stops);
    let mut n_snapped = 0usize;
    for stop in &timetable.stops {
        let orig = spatial.snap(stop.lon, stop.lat, &foot.mask, 10);
        let rank = orig.and_then(|o| foot.rank_for_original(o));
        if rank.is_some() {
            n_snapped += 1;
        }
        snapped_rank.push(rank);
    }
    tracing::info!(n_snapped, n_total = n_stops, "foot-mode snap complete");

    // ---- 2. 2D grid prefilter. ----------------------------------------
    //
    // Cell side = `2 * radius_m`. With this sizing, any pair of stops
    // within `radius_m` great-circle is guaranteed to land in the same
    // cell or in one of the 8 immediate neighbours.
    let radius_m = opts.radius_m as f64;
    let cell_size_m = 2.0 * radius_m;
    let cell_size_lat = cell_size_m / METERS_PER_DEG_LAT;
    let cell_size_lon = cell_size_m / METERS_PER_DEG_LON_AT_50;

    fn cell_key(lat: f64, lon: f64, cs_lat: f64, cs_lon: f64) -> (i32, i32) {
        ((lat / cs_lat).floor() as i32, (lon / cs_lon).floor() as i32)
    }

    let mut grid: HashMap<(i32, i32), Vec<u32>> = HashMap::with_capacity(n_stops / 4);
    for (i, rank_opt) in snapped_rank.iter().enumerate() {
        if rank_opt.is_none() {
            continue;
        }
        let s = &timetable.stops[i];
        let key = cell_key(s.lat, s.lon, cell_size_lat, cell_size_lon);
        grid.entry(key).or_default().push(i as u32);
    }
    tracing::info!(
        cells = grid.len(),
        avg_per_cell = if grid.is_empty() {
            0
        } else {
            n_snapped / grid.len()
        },
        "spatial grid built"
    );

    // For each source stop, list of (neighbour_stop_idx, neighbour_cch_rank)
    // pairs that pass the great-circle radius check.
    let mut neighbour_lists: Vec<Vec<(u32, u32)>> = vec![Vec::new(); n_stops];
    let r2 = radius_m * radius_m;

    for ((cx, cy), members) in &grid {
        // Gather candidate stops from this cell + the 8 neighbours.
        let mut candidates: Vec<u32> = Vec::new();
        for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(others) = grid.get(&(*cx + dx, *cy + dy)) {
                    candidates.extend_from_slice(others);
                }
            }
        }
        // For each source in this cell, walk the candidates once.
        for &i in members {
            let src = &timetable.stops[i as usize];
            for &j in &candidates {
                if i == j {
                    continue;
                }
                let dst = &timetable.stops[j as usize];
                let dlat_m = (dst.lat - src.lat) * METERS_PER_DEG_LAT;
                let dlon_m = (dst.lon - src.lon) * METERS_PER_DEG_LON_AT_50;
                if dlat_m * dlat_m + dlon_m * dlon_m > r2 {
                    continue;
                }
                if let Some(dst_rank) = snapped_rank[j as usize] {
                    neighbour_lists[i as usize].push((j, dst_rank));
                }
            }
        }
    }

    // ---- 3. Per-source bounded bidirectional CCH search ---------------
    //
    // Issue #115: the previous chunked bucket-M2M loop paid catastrophic
    // setup overhead (SearchState + PrefixSumBuckets + dense matrix +
    // HashMap target-index per chunk × 2000 chunks). The correct shape
    // is a **bounded bidirectional CCH distance-only query per
    // (source, neighbour) pair**, running in parallel over sources
    // with a thread-local state shared across pair calls.
    //
    // `CchQuery::distance` reuses the existing `CCH_QUERY_STATE`
    // thread-local with generation-stamped O(1) reset. The existing
    // early-termination (`fwd_min >= best_dist && bwd_min >= best_dist`)
    // automatically bounds each search at the current-best candidate,
    // which in a bounded radius setting is typically the neighbour's
    // actual walking time — pruning the search to the local subgraph.
    //
    // Memory: zero per-chunk scratch. The only allocation is the
    // per-source triples emit list (`O(K)` where `K ≈ 20-60`).
    //
    // Parallelism: outer `par_iter` over stops drives the Rayon pool.
    // Each thread touches its own `CCH_QUERY_STATE` cell with no
    // cross-thread contention. No nested parallelism.

    let max_walk_s = opts.max_walk_s;
    // Weights are deciseconds; convert the max walk time to the same
    // unit for the early-termination check inside CchQuery::distance.
    let max_walk_ds = max_walk_s.saturating_mul(10);

    // Flatten sources so the parallel iterator sees a simple Vec.
    struct SourceWork {
        stop: u32,
        source_rank: u32,
        neighbours: Vec<(u32, u32)>,
    }

    let mut work: Vec<SourceWork> = Vec::with_capacity(n_stops);
    for (i, nb) in neighbour_lists.iter().enumerate() {
        if nb.is_empty() {
            continue;
        }
        if let Some(rank) = snapped_rank[i] {
            work.push(SourceWork {
                stop: i as u32,
                source_rank: rank,
                neighbours: nb.clone(),
            });
        }
    }
    tracing::info!(
        sources_with_neighbours = work.len(),
        total_pairs = work.iter().map(|w| w.neighbours.len()).sum::<usize>(),
        "per-source bounded bidirectional CCH pass starting"
    );

    // Borrow the foot CCH topology/weights once — CchQuery is a thin
    // reference wrapper so construction inside the parallel closure
    // is free.
    let topo = &foot.cch_topo;
    let up_flat = &foot.up_adj_flat;
    let down_rev_flat = &foot.down_rev_flat;
    let weights = &foot.cch_weights;

    let mut triples: Vec<(u32, u32, u32)> = work
        .par_iter()
        .flat_map_iter(|w| {
            let query = CchQuery::with_custom_weights(topo, up_flat, down_rev_flat, weights);
            let mut emitted: Vec<(u32, u32, u32)> = Vec::with_capacity(w.neighbours.len());
            for &(target_stop, target_rank) in &w.neighbours {
                let Some(raw) = query.distance(w.source_rank, target_rank) else {
                    continue;
                };
                if raw > max_walk_ds {
                    continue;
                }
                let walk_s = raw.div_ceil(10);
                if walk_s > max_walk_s {
                    continue;
                }
                emitted.push((w.stop, target_stop, walk_s));
            }
            emitted.into_iter()
        })
        .collect();

    // ---- 3b. Inject same-station child-pair edges (#112). ------------
    let injected = inject_same_station_edges(timetable, opts.same_station_transfer_s, &mut triples);
    if injected > 0 {
        tracing::info!(
            same_station_edges = injected,
            cost_s = opts.same_station_transfer_s,
            "injected same-station transfer edges"
        );
    }

    // ---- 3c. Inject cross-feed equivalence edges (#113). -------------
    let cross_feed_injected = inject_cross_feed_equivalence_edges(
        timetable,
        opts.stop_merge_radius_m,
        opts.cross_feed_transfer_s,
        &mut triples,
    );
    if cross_feed_injected > 0 {
        tracing::info!(
            cross_feed_edges = cross_feed_injected,
            radius_m = opts.stop_merge_radius_m,
            cost_s = opts.cross_feed_transfer_s,
            "injected cross-feed equivalence edges"
        );
    }

    let pre_restriction_edges = triples.len();

    let final_triples = if opts.apply_ultra_restriction {
        ultra_restrict_transfers(n_stops, triples, opts.ultra_restriction_slack_s)
    } else {
        triples
    };
    let post_restriction_edges = final_triples.len();

    let mut graph = TransferGraph::from_triples(n_stops, final_triples);
    let fingerprint = foot_cch_fingerprint(foot);
    graph.provenance = compute_provenance(timetable, opts, &fingerprint);
    tracing::info!(
        edges = graph.n_edges(),
        pre_restriction = pre_restriction_edges,
        post_restriction = post_restriction_edges,
        dropped = pre_restriction_edges.saturating_sub(post_restriction_edges),
        "stop-to-stop transfer graph complete"
    );
    Ok(graph)
}

/// Apply the ULTRA-style triangle dominance restriction to an unrestricted
/// set of transfer triples `(from, to, walk_s)`.
///
/// For every candidate transfer `(u, v)` we scan the outgoing neighbours
/// of `u` looking for a third stop `w ≠ v` whose transfer `walk(u, w)`
/// plus a corresponding `walk(w, v)` is no greater than `walk(u, v) + slack`.
/// When found, `(u, v)` is marked for removal: any journey using `(u, v)`
/// can walk `u → w → board-at-w → ...` (or chain `(u, w)` then `(w, v)`)
/// without arriving later.
///
/// This is `O(E · avg_deg)` where `E` is the unrestricted edge count and
/// `avg_deg` is the average out-degree per stop — on Belgium multi-feed
/// this is milliseconds for tens of thousands of candidate edges.
///
/// Correctness argument: removed edges are always dominated by a strictly
/// valid alternative (the triangle), and RAPTOR's round `k` can always
/// explore the triangle because the intermediate stop `w` is itself a
/// legitimate transfer target from `u`. No Pareto-optimal journey is lost.
pub fn ultra_restrict_transfers(
    n_stops: usize,
    triples: Vec<(u32, u32, u32)>,
    slack_s: u32,
) -> Vec<(u32, u32, u32)> {
    if triples.is_empty() {
        return triples;
    }

    // Temporary adjacency for O(1) walk lookup: `adj[u]` → sorted list of
    // `(v, walk)`. We reuse the same CSR layout the final graph will
    // end up with, but with cheap per-stop HashMaps for lookup.
    let mut adj: Vec<std::collections::HashMap<u32, u32>> = vec![Default::default(); n_stops];
    for &(u, v, w) in &triples {
        // Keep the tightest walk per directed edge (dedup).
        let entry = adj[u as usize].entry(v).or_insert(u32::MAX);
        if w < *entry {
            *entry = w;
        }
    }

    // Walk every directed edge and decide keep/drop.
    let mut keep = Vec::with_capacity(triples.len());
    for (u, neighbours) in adj.iter().enumerate() {
        for (&v, &walk_uv) in neighbours {
            // A direct transfer from u to itself is nonsense — drop.
            if u as u32 == v {
                continue;
            }
            // **Zero-cost direct edges are never dominated.** A
            // zero-walk edge represents two stops that coincide in
            // the foot CCH rank space (typically co-located platform
            // stops inside a station). Any triangle through another
            // zero-walk node is also zero-cost (0 + 0 ≤ 0 + slack),
            // which the naive rule would use to drop EVERY edge
            // inside a zero-cost cluster — leaving the cluster
            // internally disconnected. That breaks #112
            // (same-station child-pair injection) on stations where
            // all platforms snap to the same foot node. Keep all
            // zero-cost edges unconditionally; the direct transfer
            // is optimal by construction.
            if walk_uv == 0 {
                keep.push((u as u32, v, walk_uv));
                continue;
            }
            // Look for a dominating intermediate `w`.
            let mut dominated = false;
            for (&w, &walk_uw) in neighbours {
                if w == v || w == u as u32 {
                    continue;
                }
                // We need walk(w, v) too; only then can we form the triangle.
                let Some(&walk_wv) = adj[w as usize].get(&v) else {
                    continue;
                };
                if walk_uw.saturating_add(walk_wv) <= walk_uv.saturating_add(slack_s) {
                    dominated = true;
                    break;
                }
            }
            if !dominated {
                keep.push((u as u32, v, walk_uv));
            }
        }
    }
    keep.sort();
    keep
}

/// Inject a fixed-cost bidirectional transfer edge between every pair
/// of stops that share a GTFS parent station (#112). Same-parent
/// children are interchangeable platforms under the GTFS spec, and
/// should be reachable with a fixed floor cost even when the foot CCH
/// has no walking path between them (underground platforms, through-
/// station passages, etc.). Returns the number of directed edges
/// appended to `triples`.
///
/// `station_children` in `TimetableBuilder::build` includes the
/// parent itself in the children list, so a platform ↔ parent edge is
/// emitted and the parent `location_type=1` stop is first-class
/// addressable as a transfer endpoint.
pub(crate) fn inject_same_station_edges(
    timetable: &Timetable,
    cost_s: u32,
    triples: &mut Vec<(u32, u32, u32)>,
) -> usize {
    let mut injected = 0usize;
    for children in timetable.station_children.values() {
        let n = children.len();
        for i in 0..n {
            for j in (i + 1)..n {
                let a = children[i];
                let b = children[j];
                if a == b {
                    continue;
                }
                triples.push((a, b, cost_s));
                triples.push((b, a, cost_s));
                injected += 2;
            }
        }
    }
    injected
}

/// Extract the feed prefix from a namespaced stop id.
///
/// The multi-feed GTFS loader namespaces stop ids as `"<feed>:<id>"`
/// (see `gtfs::load_feed`). A stop loaded from a single-feed config
/// has no prefix and is treated as its own singleton feed — it will
/// never merge with anything.
fn feed_prefix(id: &str) -> Option<&str> {
    id.split_once(':').map(|(p, _)| p)
}

/// Inject a bidirectional transfer edge with fixed cost `cost_s`
/// between every pair of stops whose GTFS ids carry a *different*
/// feed prefix and whose great-circle distance is below
/// `radius_m` (#113). Disabled when `radius_m == 0`.
///
/// Runs a one-pass 2D grid sweep sized at `radius_m` cell width so
/// every candidate pair lands in the same cell or one of the 8
/// immediate neighbours. Complexity: O(N · k) where `k` is the local
/// cluster size. Returns the number of directed edges appended.
///
/// Intentionally independent of the foot-CCH snap state: cross-feed
/// equivalence is a semantic bridge, not a road-network fact, so it
/// works even when one or both stops failed to snap.
pub(crate) fn inject_cross_feed_equivalence_edges(
    timetable: &Timetable,
    radius_m: u32,
    cost_s: u32,
    triples: &mut Vec<(u32, u32, u32)>,
) -> usize {
    if radius_m == 0 || timetable.n_stops() == 0 {
        return 0;
    }
    let radius = radius_m as f64;
    let cell_size = radius;
    let cs_lat = cell_size / METERS_PER_DEG_LAT;
    let cs_lon = cell_size / METERS_PER_DEG_LON_AT_50;

    let mut grid: HashMap<(i32, i32), Vec<u32>> = HashMap::new();
    for (i, stop) in timetable.stops.iter().enumerate() {
        let key = (
            (stop.lat / cs_lat).floor() as i32,
            (stop.lon / cs_lon).floor() as i32,
        );
        grid.entry(key).or_default().push(i as u32);
    }

    let r2 = radius * radius;
    let mut injected = 0usize;
    let mut seen: std::collections::HashSet<(u32, u32)> = std::collections::HashSet::new();

    for (&(cx, cy), members) in &grid {
        let mut candidates: Vec<u32> = Vec::new();
        for dx in -1..=1 {
            for dy in -1..=1 {
                if let Some(others) = grid.get(&(cx + dx, cy + dy)) {
                    candidates.extend_from_slice(others);
                }
            }
        }
        for &i in members {
            let src = &timetable.stops[i as usize];
            let src_feed = feed_prefix(&src.id);
            for &j in &candidates {
                if i >= j {
                    continue;
                }
                let dst = &timetable.stops[j as usize];
                // Only bridge *cross*-feed pairs. Same-feed duplicates
                // belong to #112 (station_children) or the regular
                // foot-CCH transfer edges.
                let dst_feed = feed_prefix(&dst.id);
                match (src_feed, dst_feed) {
                    (Some(a), Some(b)) if a == b => continue,
                    (None, None) => continue,
                    _ => {}
                }
                let dlat_m = (dst.lat - src.lat) * METERS_PER_DEG_LAT;
                let dlon_m = (dst.lon - src.lon) * METERS_PER_DEG_LON_AT_50;
                if dlat_m * dlat_m + dlon_m * dlon_m > r2 {
                    continue;
                }
                if seen.insert((i, j)) {
                    triples.push((i, j, cost_s));
                    triples.push((j, i, cost_s));
                    injected += 2;
                }
            }
        }
    }
    injected
}

/// Load a cached transfer graph, or build it if the cache is missing /
/// stale, then persist the fresh result.
pub fn load_or_build(
    timetable: &Timetable,
    foot: &ModeData,
    spatial: &SpatialIndex,
    opts: &TransferBuildOptions,
    cache_path: &Path,
) -> Result<TransferGraph> {
    let fingerprint = foot_cch_fingerprint(foot);
    let provenance = compute_provenance(timetable, opts, &fingerprint);
    if let Some(graph) = TransferGraph::load_cached(cache_path, provenance)
        .context("reading cached transfer graph")?
    {
        tracing::info!(
            path = %cache_path.display(),
            edges = graph.n_edges(),
            "loaded cached transfer graph"
        );
        return Ok(graph);
    }
    let graph = build_transfer_graph(timetable, foot, spatial, opts)?;
    if let Some(parent) = cache_path.parent() {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating transit cache directory {}", parent.display()))?;
    }
    graph
        .save_cached(cache_path)
        .context("writing cached transfer graph")?;
    Ok(graph)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::transit::timetable::{StopTime, TimetableBuilder};

    #[test]
    fn from_triples_builds_csr() {
        let triples = vec![(0, 1, 10), (0, 2, 20), (1, 0, 10), (2, 0, 25)];
        let g = TransferGraph::from_triples(3, triples);
        let n0: Vec<_> = g.neighbours(0).collect();
        assert_eq!(n0, vec![(1, 10), (2, 20)]);
        let n1: Vec<_> = g.neighbours(1).collect();
        assert_eq!(n1, vec![(0, 10)]);
        // Duplicate collapsing: smaller wins.
        let g2 = TransferGraph::from_triples(2, vec![(0, 1, 50), (0, 1, 30)]);
        let n0: Vec<_> = g2.neighbours(0).collect();
        assert_eq!(n0, vec![(1, 30)]);
    }

    #[test]
    fn ultra_restriction_drops_triangle_dominated_edge() {
        // Four stops in a line: A — B — C — D. Short-edge walking times:
        //   A-B=10, B-C=10, C-D=10. Long-range candidate edges:
        //   A-C=22 (dominated by A-B + B-C = 20 ≤ 22+2=24 ✓)
        //   A-D=40 (dominated by A-B + B-D = 10+15 = 25 ≤ 40+2=42 ✓)
        //   B-D=15 (NOT dominated: B-C+C-D = 20 > 15+2=17 ✗)
        // Slack = 2.
        let triples = vec![
            (0, 1, 10),
            (1, 0, 10),
            (1, 2, 10),
            (2, 1, 10),
            (2, 3, 10),
            (3, 2, 10),
            // Dominated: A → C via B.
            (0, 2, 22),
            (2, 0, 22),
            // Dominated: A → D via B (then direct B→D=15).
            (0, 3, 40),
            (3, 0, 40),
            // NOT dominated: only triangle is via C which is 20 > 17.
            (1, 3, 15),
            (3, 1, 15),
        ];
        let restricted = ultra_restrict_transfers(4, triples, 2);
        // (0, 2) and (2, 0) dropped by the A-B-C triangle.
        assert!(!restricted.iter().any(|&(u, v, _)| (u, v) == (0, 2)));
        assert!(!restricted.iter().any(|&(u, v, _)| (u, v) == (2, 0)));
        // (0, 3) and (3, 0) dropped by the A-B-D triangle.
        assert!(!restricted.iter().any(|&(u, v, _)| (u, v) == (0, 3)));
        assert!(!restricted.iter().any(|&(u, v, _)| (u, v) == (3, 0)));
        // Short edges and the non-dominated B-D pair survive.
        assert!(restricted.contains(&(0, 1, 10)));
        assert!(restricted.contains(&(1, 2, 10)));
        assert!(restricted.contains(&(2, 3, 10)));
        assert!(restricted.contains(&(1, 3, 15)));
        assert!(restricted.contains(&(3, 1, 15)));
    }

    #[test]
    fn ultra_restriction_keeps_all_when_no_triangles() {
        // Star topology: 0 connected to 1, 2, 3, 4 but leaves are disconnected.
        // No triangles possible → every edge is kept.
        let triples = vec![
            (0, 1, 100),
            (1, 0, 100),
            (0, 2, 120),
            (2, 0, 120),
            (0, 3, 150),
            (3, 0, 150),
            (0, 4, 200),
            (4, 0, 200),
        ];
        let restricted = ultra_restrict_transfers(5, triples.clone(), 2);
        assert_eq!(restricted.len(), triples.len());
    }

    /// Regression for the v7 fix: a cluster of zero-cost edges (all
    /// pairs between three co-located platform stops) must survive
    /// ULTRA restriction, not be shredded by spurious zero-zero
    /// triangles.
    #[test]
    fn ultra_restriction_keeps_zero_cost_cluster() {
        // Three stops A, B, C all at the same foot rank → every
        // pair walks 0 seconds. The naive rule would drop every
        // direct edge because (0 + 0) ≤ (0 + slack) for every
        // possible triangle, leaving the cluster disconnected.
        let triples = vec![
            (0, 1, 0),
            (1, 0, 0),
            (0, 2, 0),
            (2, 0, 0),
            (1, 2, 0),
            (2, 1, 0),
        ];
        let restricted = ultra_restrict_transfers(3, triples.clone(), 2);
        // All six directed zero-cost edges must survive.
        assert_eq!(
            restricted.len(),
            6,
            "zero-cost cluster was shredded by ULTRA: {:?}",
            restricted
        );
        for t in &triples {
            assert!(
                restricted.contains(t),
                "missing zero-cost edge {t:?} in restricted set"
            );
        }
    }

    #[test]
    fn ultra_restriction_self_loops_dropped() {
        let triples = vec![(0, 0, 0), (0, 1, 10), (1, 0, 10)];
        let restricted = ultra_restrict_transfers(2, triples, 0);
        assert!(!restricted.iter().any(|&(u, v, _)| u == v));
    }

    fn tiny_timetable(lon_b: f64, lat_b: f64) -> Timetable {
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let c = b.add_stop("B", "B", lon_b, lat_b, None);
        b.add_trip(
            "T",
            "R",
            "R",
            "h",
            vec![a, c],
            vec![
                StopTime {
                    arrival: 0,
                    departure: 0,
                },
                StopTime {
                    arrival: 60,
                    departure: 60,
                },
            ],
        );
        b.build().unwrap()
    }

    #[test]
    fn provenance_is_stable() {
        let tt = tiny_timetable(0.01, 0.0);
        let opts = TransferBuildOptions::default();
        let fp: FootCchFingerprint = [0xAB; 32];
        let h1 = compute_provenance(&tt, &opts, &fp);
        let h2 = compute_provenance(&tt, &opts, &fp);
        assert_eq!(h1, h2);
    }

    #[test]
    fn provenance_changes_when_stop_moves() {
        // Issue #109: moving a stop 10 m must invalidate the cached
        // transfer graph because the cached edges reference a now-wrong
        // snap rank.
        let tt1 = tiny_timetable(0.01, 0.0);
        let tt2 = tiny_timetable(0.0101, 0.0); // ~10 m east
        let opts = TransferBuildOptions::default();
        let fp: FootCchFingerprint = [0xAB; 32];
        let h1 = compute_provenance(&tt1, &opts, &fp);
        let h2 = compute_provenance(&tt2, &opts, &fp);
        assert_ne!(
            h1, h2,
            "provenance must change when a stop's coordinates change"
        );
    }

    /// Build a 5-stop timetable: parent station P with two child
    /// platforms P_a and P_b, plus two free-standing stops X and Y.
    /// Returns (timetable, P_a, P_b, X, Y, parent).
    fn station_with_children() -> (Timetable, u32, u32, u32, u32, u32) {
        let mut b = TimetableBuilder::new();
        // Parent first so its index is stable.
        let parent = b.add_stop("P", "Parent Station", 0.0, 0.0, None);
        // Two platforms under the parent. add_stop requires the parent
        // to already exist, so we pass it explicitly.
        let p_a = b.add_stop("P_a", "Platform A", 0.0001, 0.0, Some(parent));
        let p_b = b.add_stop("P_b", "Platform B", 0.0, 0.0001, Some(parent));
        let x = b.add_stop("X", "X", 1.0, 0.0, None);
        let y = b.add_stop("Y", "Y", 2.0, 0.0, None);
        // One dummy trip touching every stop so the builder doesn't
        // drop them during build().
        b.add_trip(
            "T",
            "R",
            "R",
            "h",
            vec![x, p_a, p_b, y],
            vec![
                StopTime {
                    arrival: 0,
                    departure: 0,
                },
                StopTime {
                    arrival: 60,
                    departure: 60,
                },
                StopTime {
                    arrival: 120,
                    departure: 120,
                },
                StopTime {
                    arrival: 180,
                    departure: 180,
                },
            ],
        );
        let tt = b.build().unwrap();
        (tt, p_a, p_b, x, y, parent)
    }

    #[test]
    fn inject_same_station_edges_emits_all_child_pairs() {
        let (tt, p_a, p_b, _x, _y, parent) = station_with_children();
        let mut triples: Vec<(u32, u32, u32)> = Vec::new();
        let n = inject_same_station_edges(&tt, 60, &mut triples);

        // station_children includes the parent itself, so n_children = 3
        // (parent, P_a, P_b) and pairs = C(3, 2) = 3, giving 6 directed
        // edges (2 per pair).
        assert_eq!(n, 6);
        assert_eq!(triples.len(), 6);

        // Every emitted edge has cost 60.
        for &(_, _, c) in &triples {
            assert_eq!(c, 60);
        }

        // The P_a ↔ P_b pair is present in both directions.
        assert!(triples.contains(&(p_a, p_b, 60)));
        assert!(triples.contains(&(p_b, p_a, 60)));
        // The parent ↔ children pairs are present in both directions.
        assert!(triples.contains(&(parent, p_a, 60)));
        assert!(triples.contains(&(p_a, parent, 60)));
        assert!(triples.contains(&(parent, p_b, 60)));
        assert!(triples.contains(&(p_b, parent, 60)));
    }

    #[test]
    fn inject_same_station_edges_ignores_free_standing_stops() {
        let (tt, _p_a, _p_b, x, y, _parent) = station_with_children();
        let mut triples: Vec<(u32, u32, u32)> = Vec::new();
        inject_same_station_edges(&tt, 60, &mut triples);
        // Neither X nor Y has a parent, so no edge should mention them.
        for &(u, v, _) in &triples {
            assert_ne!(u, x);
            assert_ne!(v, x);
            assert_ne!(u, y);
            assert_ne!(v, y);
        }
    }

    /// Timetable with four stops:
    /// - `sncb:X` at (0.0, 0.0) — SNCB feed.
    /// - `stib:X_metro` at (0.0, 0.0) — STIB feed, same lon/lat.
    /// - `sncb:Y` at (0.0, 0.01) — SNCB feed, ~1.1 km north (outside 50 m).
    /// - `delijn:Y_bus` at (0.0, 0.01) — De Lijn, same place as Y.
    fn multi_feed_timetable() -> (Timetable, u32, u32, u32, u32) {
        let mut b = TimetableBuilder::new();
        let sx = b.add_stop("sncb:X", "Brussels Central SNCB", 0.0, 0.0, None);
        let tx = b.add_stop("stib:X_metro", "Brussels Central metro", 0.0, 0.0, None);
        let sy = b.add_stop("sncb:Y", "Gent Sint-Pieters SNCB", 0.0, 0.01, None);
        let dy = b.add_stop("delijn:Y_bus", "Gent Sint-Pieters bus", 0.0, 0.01, None);
        // One trip that touches every stop so the builder doesn't prune them.
        b.add_trip(
            "T",
            "R",
            "R",
            "h",
            vec![sx, tx, sy, dy],
            vec![
                StopTime {
                    arrival: 0,
                    departure: 0,
                },
                StopTime {
                    arrival: 60,
                    departure: 60,
                },
                StopTime {
                    arrival: 600,
                    departure: 600,
                },
                StopTime {
                    arrival: 660,
                    departure: 660,
                },
            ],
        );
        b.build().unwrap();
        // Rebuild and return the stops in a stable order via the returned Timetable.
        let mut b = TimetableBuilder::new();
        let sx = b.add_stop("sncb:X", "Brussels Central SNCB", 0.0, 0.0, None);
        let tx = b.add_stop("stib:X_metro", "Brussels Central metro", 0.0, 0.0, None);
        let sy = b.add_stop("sncb:Y", "Gent Sint-Pieters SNCB", 0.0, 0.01, None);
        let dy = b.add_stop("delijn:Y_bus", "Gent Sint-Pieters bus", 0.0, 0.01, None);
        b.add_trip(
            "T",
            "R",
            "R",
            "h",
            vec![sx, tx, sy, dy],
            vec![
                StopTime {
                    arrival: 0,
                    departure: 0,
                },
                StopTime {
                    arrival: 60,
                    departure: 60,
                },
                StopTime {
                    arrival: 600,
                    departure: 600,
                },
                StopTime {
                    arrival: 660,
                    departure: 660,
                },
            ],
        );
        (b.build().unwrap(), sx, tx, sy, dy)
    }

    #[test]
    fn inject_cross_feed_bridges_colocated_different_feeds() {
        let (tt, sx, tx, sy, dy) = multi_feed_timetable();
        let mut triples: Vec<(u32, u32, u32)> = Vec::new();
        inject_cross_feed_equivalence_edges(&tt, 50, 30, &mut triples);

        // Brussels Central: sncb:X <-> stib:X_metro (different feeds,
        // same coords) — expected.
        assert!(triples.contains(&(sx, tx, 30)));
        assert!(triples.contains(&(tx, sx, 30)));

        // Gent Sint-Pieters: sncb:Y <-> delijn:Y_bus (different feeds,
        // same coords) — expected.
        assert!(triples.contains(&(sy, dy, 30)));
        assert!(triples.contains(&(dy, sy, 30)));

        // No cross-cluster bridge — Brussels and Gent are 1.1 km apart.
        assert!(!triples.contains(&(sx, sy, 30)));
        assert!(!triples.contains(&(sx, dy, 30)));
        assert!(!triples.contains(&(tx, sy, 30)));
        assert!(!triples.contains(&(tx, dy, 30)));

        // Same-feed co-located pairs (none in this fixture, but double
        // check we didn't emit any by scanning for sncb:X ↔ sncb:Y,
        // which share a feed). Those are out of scope for #113.
        assert!(!triples.contains(&(sx, sy, 30)));
    }

    #[test]
    fn inject_cross_feed_disabled_by_zero_radius() {
        let (tt, _, _, _, _) = multi_feed_timetable();
        let mut triples: Vec<(u32, u32, u32)> = Vec::new();
        let n = inject_cross_feed_equivalence_edges(&tt, 0, 30, &mut triples);
        assert_eq!(n, 0);
        assert!(triples.is_empty());
    }

    /// #132 — a cross-feed equivalence bridge (e.g. SNCB station ↔
    /// STIB metro at the same coordinate, different operators) must
    /// survive ULTRA dominance restriction when **no triangle
    /// dominates it**. The earlier code path correctly injected the
    /// bridges before ULTRA, but no test verified the survival
    /// invariant — so a future change that re-orders injection vs.
    /// restriction (or that tightens the dominance test in a way that
    /// catches zero-walk duplicates) would silently drop bridges and
    /// produce slow multimodal routes between operator pairs.
    #[test]
    fn cross_feed_bridges_survive_ultra_restriction() {
        let (tt, sx, tx, sy, dy) = multi_feed_timetable();
        let mut triples: Vec<(u32, u32, u32)> = Vec::new();

        // Step 1: inject the cross-feed bridges (30 s flat cost).
        let n_injected = inject_cross_feed_equivalence_edges(&tt, 50, 30, &mut triples);
        assert_eq!(n_injected, 4, "two pairs × two directions = four edges");

        // Step 2: run ULTRA restriction. With no triangles in the
        // synthetic graph, every direct edge is undominated and must
        // be kept.
        let n_stops = tt.stops.len();
        let restricted = ultra_restrict_transfers(n_stops, triples, 2);

        // All four cross-feed bridges still present after ULTRA.
        assert!(
            restricted.contains(&(sx, tx, 30)),
            "Brussels SNCB → STIB metro bridge must survive ULTRA"
        );
        assert!(
            restricted.contains(&(tx, sx, 30)),
            "Brussels STIB metro → SNCB bridge must survive ULTRA"
        );
        assert!(
            restricted.contains(&(sy, dy, 30)),
            "Gent SNCB → De Lijn bus bridge must survive ULTRA"
        );
        assert!(
            restricted.contains(&(dy, sy, 30)),
            "Gent De Lijn bus → SNCB bridge must survive ULTRA"
        );
    }

    #[test]
    fn inject_cross_feed_skips_same_feed_colocated_stops() {
        // Two stops from the same feed at the exact same coordinate:
        // #113 must not touch these — they're either duplicates to be
        // cleaned up upstream, or already handled by #112.
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("sncb:A", "A", 4.35, 50.84, None);
        let b_ = b.add_stop("sncb:A_dup", "A dup", 4.35, 50.84, None);
        // One trip so both stops survive build().
        b.add_trip(
            "T",
            "R",
            "R",
            "h",
            vec![a, b_],
            vec![
                StopTime {
                    arrival: 0,
                    departure: 0,
                },
                StopTime {
                    arrival: 60,
                    departure: 60,
                },
            ],
        );
        let tt = b.build().unwrap();
        let mut triples: Vec<(u32, u32, u32)> = Vec::new();
        let n = inject_cross_feed_equivalence_edges(&tt, 50, 30, &mut triples);
        assert_eq!(n, 0);
    }

    #[test]
    fn provenance_changes_when_same_station_cost_changes() {
        // Bumping the fixed floor cost changes the transfer graph, so
        // the provenance hash must pick it up (#112 cache correctness).
        let tt = tiny_timetable(0.01, 0.0);
        let fp: FootCchFingerprint = [0xAB; 32];
        let mut o1 = TransferBuildOptions::default();
        let mut o2 = TransferBuildOptions::default();
        o1.same_station_transfer_s = 60;
        o2.same_station_transfer_s = 30;
        let h1 = compute_provenance(&tt, &o1, &fp);
        let h2 = compute_provenance(&tt, &o2, &fp);
        assert_ne!(
            h1, h2,
            "provenance must change when same_station_transfer_s changes"
        );
    }

    #[test]
    fn provenance_changes_when_foot_cch_changes() {
        // Issue #109: rebuilding the foot CCH (same stops, different
        // road graph) must invalidate the cache so stale transfer edges
        // referencing the old CCH rank space are rejected.
        let tt = tiny_timetable(0.01, 0.0);
        let opts = TransferBuildOptions::default();
        let fp1: FootCchFingerprint = [0xAB; 32];
        let fp2: FootCchFingerprint = [0xCD; 32];
        let h1 = compute_provenance(&tt, &opts, &fp1);
        let h2 = compute_provenance(&tt, &opts, &fp2);
        assert_ne!(
            h1, h2,
            "provenance must change when the foot CCH fingerprint changes"
        );
    }
}
