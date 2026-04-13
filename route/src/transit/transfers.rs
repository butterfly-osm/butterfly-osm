//! Stop-to-stop walking transfer graph (ULTRA-style).
//!
//! At load time we precompute the foot-mode CCH walking time between every
//! pair of stops within `transfer_radius_m` (great-circle). The result is a
//! sparse adjacency list:
//!
//! ```text
//! offsets:  Vec<u32> (n_stops + 1)
//! neighbours: Vec<(StopIdx, walk_seconds)>   sorted by neighbour id
//! ```
//!
//! The radius prefilter uses a simple lon/lat equirectangular heuristic
//! (fast enough for Belgium's ~600 SNCB stops — O(N^2) prefilter is ~360K
//! pair checks, milliseconds).
//!
//! The actual CCH 1-to-N call is routed through `matrix::bucket_ch` using
//! the foot-mode `up_adj_flat` / `down_rev_flat` time metric.

use std::path::Path;

use anyhow::{Context, Result};

use crate::matrix::bucket_ch::table_bucket_parallel;
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
pub struct TransferBuildOptions {
    /// Maximum pairwise straight-line radius (meters).
    pub radius_m: u32,
    /// Upper bound on walk time (seconds). Edges exceeding this cap are dropped.
    pub max_walk_s: u32,
    /// Walking speed assumption (meters / second) — used for cap conversion only.
    pub walk_speed_mps: f64,
}

impl Default for TransferBuildOptions {
    fn default() -> Self {
        Self {
            radius_m: 1_000,
            max_walk_s: 900,
            walk_speed_mps: 1.3,
        }
    }
}

/// Compute provenance hash for a (timetable, options) pair.
pub fn compute_provenance(timetable: &Timetable, opts: &TransferBuildOptions) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut h = Sha256::new();
    h.update((timetable.n_stops() as u64).to_le_bytes());
    h.update((timetable.n_routes() as u64).to_le_bytes());
    h.update(opts.radius_m.to_le_bytes());
    h.update(opts.max_walk_s.to_le_bytes());
    // Stop id chain: include every stop's GTFS id so any feed change
    // invalidates the cache.
    for stop in &timetable.stops {
        h.update((stop.id.len() as u32).to_le_bytes());
        h.update(stop.id.as_bytes());
    }
    h.finalize().into()
}

/// Precompute a walking transfer graph for all stops in a timetable,
/// using Butterfly's foot-mode CCH for exact road-network distances.
///
/// For each stop, we snap it to the foot graph, then call 1-to-N CCH
/// with all neighbour stops within `opts.radius_m` as targets.
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

    // Snap every stop to the foot CCH (rank-space).
    // snapped_rank[i] = Some(rank) if stop i could snap.
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

    // Precompute a nearest-neighbours table using the spherical filter.
    // For Belgian SNCB (~600 stops) this is ~O(N^2) ≈ 360K checks.
    let radius_m = opts.radius_m as f64;
    let radius_deg_lat = radius_m / METERS_PER_DEG_LAT;
    let radius_deg_lon = radius_m / METERS_PER_DEG_LON_AT_50;

    let mut neighbour_lists: Vec<Vec<(u32, u32)>> = vec![Vec::new(); n_stops]; // (target_stop, target_rank)

    for i in 0..n_stops {
        if snapped_rank[i].is_none() {
            continue;
        }
        let src_lat = timetable.stops[i].lat;
        let src_lon = timetable.stops[i].lon;
        for (j, dst_rank_opt) in snapped_rank.iter().enumerate() {
            if i == j {
                continue;
            }
            let dj = &timetable.stops[j];
            if (dj.lat - src_lat).abs() > radius_deg_lat
                || (dj.lon - src_lon).abs() > radius_deg_lon
            {
                continue;
            }
            // Exact elliptical check.
            let dlat_m = (dj.lat - src_lat) * METERS_PER_DEG_LAT;
            let dlon_m = (dj.lon - src_lon) * METERS_PER_DEG_LON_AT_50;
            if dlat_m * dlat_m + dlon_m * dlon_m > radius_m * radius_m {
                continue;
            }
            if let Some(dst_rank) = *dst_rank_opt {
                neighbour_lists[i].push((j as u32, dst_rank));
            }
        }
    }

    // Run 1-to-N CCH for each source stop. We batch multiple sources into
    // a parallel `table_bucket_parallel` call for throughput by grouping
    // sources with overlapping target sets is complex; for the sizes we
    // care about a simple per-source loop using the parallel matrix API
    // (one row at a time) is plenty fast and avoids correctness subtleties.
    //
    // For each row we pass `sources=[src_rank]` and `targets=[all target ranks]`.
    let mut triples: Vec<(u32, u32, u32)> = Vec::new();
    let max_walk_s = opts.max_walk_s;
    let n_cch_nodes = foot.cch_topo.n_nodes as usize;

    for i in 0..n_stops {
        let Some(src_rank) = snapped_rank[i] else {
            continue;
        };
        let neighbours = &neighbour_lists[i];
        if neighbours.is_empty() {
            continue;
        }
        let sources_buf = [src_rank];
        let targets_buf: Vec<u32> = neighbours.iter().map(|(_, r)| *r).collect();
        let (matrix, _stats) = table_bucket_parallel(
            n_cch_nodes,
            &foot.up_adj_flat,
            &foot.down_rev_flat,
            &sources_buf,
            &targets_buf,
        );
        // matrix shape is [n_sources=1, n_targets=k], row-major.
        for (k, (target_stop, _rank)) in neighbours.iter().enumerate() {
            let raw = matrix[k];
            if raw == u32::MAX {
                continue;
            }
            // Weights are deciseconds; convert to whole seconds, rounding up
            // so we never understate walking time (safer for RAPTOR).
            let walk_s = raw.div_ceil(10);
            if walk_s > max_walk_s {
                continue;
            }
            triples.push((i as u32, *target_stop, walk_s));
        }
    }

    let mut graph = TransferGraph::from_triples(n_stops, triples);
    graph.provenance = compute_provenance(timetable, opts);
    tracing::info!(
        edges = graph.n_edges(),
        "stop-to-stop transfer graph complete"
    );
    Ok(graph)
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
