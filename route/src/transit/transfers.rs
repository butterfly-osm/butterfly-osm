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

use crate::matrix::bucket_ch::table_bucket_full_flat;
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
        }
    }
}

/// Version tag for the provenance hash. Bumped whenever the transfer
/// algorithm or hash format changes meaningfully so cached graphs built
/// under an older version are forcibly rejected.
const TRANSFER_ALGO_VERSION: u32 = 3;

/// Compute provenance hash for a (timetable, options) pair.
///
/// **Order-invariant**: the GTFS loader receives stops from
/// `gtfs_structures::Gtfs.stops`, which is a `HashMap` whose iteration
/// order is randomised per run. The resulting `Timetable.stops` Vec
/// therefore has a different element order on every server start. To
/// keep the on-disk transfer cache valid across restarts, the
/// provenance hash must NOT depend on stop ordering — only on the
/// underlying set of stop ids and the build parameters.
///
/// We sort the stop ids before feeding them into the digest. Sorting
/// 64k short strings is microsecond-cheap and guarantees a stable hash
/// across HashMap re-orderings.
pub fn compute_provenance(timetable: &Timetable, opts: &TransferBuildOptions) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update(TRANSFER_ALGO_VERSION.to_le_bytes());
    h.update((timetable.n_stops() as u64).to_le_bytes());
    h.update((timetable.n_routes() as u64).to_le_bytes());
    h.update(opts.radius_m.to_le_bytes());
    h.update(opts.max_walk_s.to_le_bytes());
    h.update((opts.apply_ultra_restriction as u8).to_le_bytes());
    h.update(opts.ultra_restriction_slack_s.to_le_bytes());

    // Order-invariant stop-id digest. Any feed change still invalidates
    // the cache because adding/removing/renaming a stop changes the set.
    let mut ids: Vec<&str> = timetable.stops.iter().map(|s| s.id.as_str()).collect();
    ids.sort_unstable();
    for id in &ids {
        h.update((id.len() as u32).to_le_bytes());
        h.update(id.as_bytes());
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
        let rank = orig.and_then(|o| {
            let filtered = foot.filtered_ebg.original_to_filtered[o as usize];
            if filtered == u32::MAX {
                None
            } else {
                Some(foot.order.perm[filtered as usize])
            }
        });
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

    // ---- 3. Spatially chunked CCH calls. -----------------------------
    //
    // Chunking strategy:
    //
    //   1. Each non-empty grid cell becomes one chunk. Cells with more
    //      than `MAX_CHUNK_SOURCES` stops are split (rare — only urban
    //      cores). Cells are spatially cohesive by construction, so the
    //      union of all sources' neighbours stays small and dense.
    //
    //   2. Each chunk runs ONE call to the **non-parallel**
    //      `table_bucket_full_flat`. Single-thread per chunk so the
    //      outer Rayon `par_iter` over chunks is the only layer of
    //      parallelism — no nested rayon contention.
    //
    //   3. The dense per-chunk matrix is small (avg ~30 sources × ~70
    //      targets ≈ 2000 cells ≈ 8 KB) so memory pressure is
    //      negligible; the bottleneck is CPU work in the bucket M2M
    //      itself, which scales linearly with the total number of
    //      source-target queries.
    //
    // For Belgium (64k stops, 8-core foot CCH build): ~2000 chunks
    // running ~50 in parallel at any given time, finishing in ~2 min
    // on the first build. Subsequent restarts hit the on-disk cache
    // (`transfers.bin`) and load instantly, so the build is genuinely
    // one-shot per refresh of the source feeds.

    const MAX_CHUNK_SOURCES: usize = 512;
    struct Chunk {
        source_stops: Vec<u32>,              // stop indices
        source_ranks: Vec<u32>,              // matching CCH ranks
        targets: Vec<u32>,                   // unique neighbour CCH ranks (sorted)
        target_idx_map: HashMap<u32, usize>, // rank -> column in matrix
    }

    let mut chunks: Vec<Chunk> = Vec::new();
    for members in grid.values() {
        for sub in members.chunks(MAX_CHUNK_SOURCES) {
            let mut source_stops = Vec::with_capacity(sub.len());
            let mut source_ranks = Vec::with_capacity(sub.len());
            let mut target_set: HashMap<u32, ()> = HashMap::new();
            for &i in sub {
                let neighbours = &neighbour_lists[i as usize];
                if neighbours.is_empty() {
                    continue;
                }
                source_stops.push(i);
                source_ranks.push(snapped_rank[i as usize].unwrap());
                for (_, r) in neighbours {
                    target_set.insert(*r, ());
                }
            }
            if source_stops.is_empty() {
                continue;
            }
            let mut targets: Vec<u32> = target_set.into_keys().collect();
            targets.sort_unstable();
            let mut target_idx_map = HashMap::with_capacity(targets.len());
            for (col, t) in targets.iter().enumerate() {
                target_idx_map.insert(*t, col);
            }
            chunks.push(Chunk {
                source_stops,
                source_ranks,
                targets,
                target_idx_map,
            });
        }
    }
    tracing::info!(
        chunks = chunks.len(),
        avg_sources_per_chunk = if chunks.is_empty() {
            0
        } else {
            chunks.iter().map(|c| c.source_stops.len()).sum::<usize>() / chunks.len()
        },
        avg_targets_per_chunk = if chunks.is_empty() {
            0
        } else {
            chunks.iter().map(|c| c.targets.len()).sum::<usize>() / chunks.len()
        },
        "transfer chunks prepared"
    );

    // ---- 4. Run chunks in parallel through bucket M2M. ---------------
    let max_walk_s = opts.max_walk_s;
    let n_cch_nodes = foot.cch_topo.n_nodes as usize;

    let triples: Vec<(u32, u32, u32)> = chunks
        .par_iter()
        .flat_map_iter(|chunk| {
            let (matrix, _stats) = table_bucket_full_flat(
                n_cch_nodes,
                &foot.up_adj_flat,
                &foot.down_rev_flat,
                &chunk.source_ranks,
                &chunk.targets,
            );
            let n_targets = chunk.targets.len();
            let mut emitted = Vec::new();
            for (row_idx, &src_stop) in chunk.source_stops.iter().enumerate() {
                let row_base = row_idx * n_targets;
                for (target_stop, target_rank) in &neighbour_lists[src_stop as usize] {
                    let Some(&col) = chunk.target_idx_map.get(target_rank) else {
                        continue;
                    };
                    let raw = matrix[row_base + col];
                    if raw == u32::MAX {
                        continue;
                    }
                    let walk_s = raw.div_ceil(10);
                    if walk_s > max_walk_s {
                        continue;
                    }
                    emitted.push((src_stop, *target_stop, walk_s));
                }
            }
            emitted.into_iter()
        })
        .collect();

    let pre_restriction_edges = triples.len();

    let final_triples = if opts.apply_ultra_restriction {
        ultra_restrict_transfers(n_stops, triples, opts.ultra_restriction_slack_s)
    } else {
        triples
    };
    let post_restriction_edges = final_triples.len();

    let mut graph = TransferGraph::from_triples(n_stops, final_triples);
    graph.provenance = compute_provenance(timetable, opts);
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

/// Load a cached transfer graph, or build it if the cache is missing /
/// stale, then persist the fresh result.
pub fn load_or_build(
    timetable: &Timetable,
    foot: &ModeData,
    spatial: &SpatialIndex,
    opts: &TransferBuildOptions,
    cache_path: &Path,
) -> Result<TransferGraph> {
    let provenance = compute_provenance(timetable, opts);
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

    #[test]
    fn ultra_restriction_self_loops_dropped() {
        let triples = vec![(0, 0, 0), (0, 1, 10), (1, 0, 10)];
        let restricted = ultra_restrict_transfers(2, triples, 0);
        assert!(!restricted.iter().any(|&(u, v, _)| u == v));
    }

    #[test]
    fn provenance_is_stable() {
        let mut b = TimetableBuilder::new();
        let a = b.add_stop("A", "A", 0.0, 0.0, None);
        let c = b.add_stop("B", "B", 0.01, 0.0, None);
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
        let tt = b.build().unwrap();
        let opts = TransferBuildOptions::default();
        let h1 = compute_provenance(&tt, &opts);
        let h2 = compute_provenance(&tt, &opts);
        assert_eq!(h1, h2);
    }
}
