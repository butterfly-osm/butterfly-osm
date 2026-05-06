//! Map matching — snap GPS traces to road network using HMM + Viterbi
//!
//! Algorithm: Hidden Markov Model with Viterbi decoding (Newson & Krumm 2009)
//! - Emission probability: Gaussian on perpendicular GPS-to-edge distance
//! - Transition probability: exponential on |route_distance - great_circle_distance|
//! - Route distance: sum of physical edge lengths (length_mm) along the fastest path

use std::sync::Arc;

use crate::profile_abi::Mode;
use crate::server::edge_geom::EdgeGeometry;

use super::cross_region::solve_cross_region;
use super::overlay::OverlayCluster;
use super::query::CchQuery;
use super::regions::RegionsState;
use super::state::{CchWeights, ModeData, ServerState};

/// Maximum candidates per GPS observation (after perpendicular-distance reranking)
const MAX_CANDIDATES: usize = 8;

// Candidate selection uses midpoint distance from the spatial index (topologically reliable).
// Perpendicular projection is only used to refine the snap position and emission probability.

/// Default GPS noise standard deviation (meters)
const DEFAULT_GPS_SIGMA: f64 = 10.0;

/// Transition model parameter beta (meters)
/// Controls how much route/GC distance mismatch is penalized.
/// Newson & Krumm suggest beta = median(|d_route - d_gc|) from training data.
/// For urban areas with one-way streets, beta ~ 20-50m is typical.
const BETA: f64 = 30.0;

/// Gap distance (meters) — if consecutive GPS points are this far apart, break the trace
const GAP_THRESHOLD_M: f64 = 2000.0;

/// Approximate meters per degree at Belgian latitudes
const METERS_PER_DEG_LAT: f64 = 111_000.0;
const METERS_PER_DEG_LON_AT_50: f64 = 71_400.0;

/// Negative infinity for log-probabilities
const NEG_INF: f64 = f64::NEG_INFINITY;

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

/// A single candidate match for a GPS observation
#[derive(Debug, Clone)]
struct Candidate {
    ebg_id: u32,
    snapped_lon: f64,
    snapped_lat: f64,
    distance_m: f64, // perpendicular distance from GPS point to edge
}

/// Result of map matching a GPS trace
#[derive(Debug)]
pub struct MatchResult {
    /// Matched sub-routes (trace may be split at gaps)
    pub matchings: Vec<Matching>,
    /// Per-observation tracepoint info (None if observation couldn't be matched)
    pub tracepoints: Vec<Option<Tracepoint>>,
}

/// A continuous matched sub-route
#[derive(Debug)]
pub struct Matching {
    /// Ordered EBG edge IDs forming the matched path
    pub ebg_path: Vec<u32>,
    /// Total duration in deciseconds
    pub duration_ds: u32,
    /// Confidence score (0.0 to 1.0) — average emission probability
    pub confidence: f64,
    /// Region index in [`super::regions::RegionsState::regions`] this
    /// matching's `ebg_path` lives in. EBG ids are region-local: each
    /// loaded region has its own ebg-id space starting at 0. The
    /// caller MUST resolve geometry / road names against this
    /// region's per-region `ServerState`. For single-region traces
    /// this is always 0; cross-region traces produce one [`Matching`]
    /// per contiguous same-region run.
    pub region_idx: usize,
}

/// Matched position for a single GPS observation
#[derive(Debug, Clone)]
pub struct Tracepoint {
    pub lon: f64,
    pub lat: f64,
    pub ebg_id: u32,
    /// Index into matchings array
    pub matchings_index: usize,
    /// Index within the matching's waypoint sequence
    pub waypoint_index: usize,
}

// ---------------------------------------------------------------------------
// Core algorithm
// ---------------------------------------------------------------------------

/// Match a GPS trace to the road network
///
/// Returns None if no observations could be matched.
pub fn map_match(
    state: &ServerState,
    mode: Mode,
    coordinates: &[(f64, f64)], // (lon, lat)
    gps_accuracy: Option<f64>,
    snap_mask: Option<&[u64]>,
    exclude_weights: Option<&CchWeights>,
) -> Option<MatchResult> {
    let n = coordinates.len();
    if n < 2 {
        return None;
    }

    let mode_data = state.get_mode(mode);
    let sigma = gps_accuracy.unwrap_or(DEFAULT_GPS_SIGMA).max(1.0);
    let mask = snap_mask.unwrap_or(&mode_data.mask);
    let weights = exclude_weights.unwrap_or(&mode_data.cch_weights);

    // Step 1: Generate candidates for each observation
    let candidates: Vec<Vec<Candidate>> = coordinates
        .iter()
        .map(|&(lon, lat)| generate_candidates(state, mode.0, mask, lon, lat))
        .collect();

    // Step 2: Find segments (split at gaps or unmatched observations)
    let segments = find_segments(coordinates, &candidates);

    if segments.is_empty() {
        return None;
    }

    // Step 3: Run Viterbi on each segment
    let mut matchings = Vec::new();
    let mut tracepoints = vec![None; n];

    for segment in &segments {
        let seg_coords: Vec<(f64, f64)> = segment.iter().map(|&i| coordinates[i]).collect();
        let seg_candidates: Vec<&Vec<Candidate>> =
            segment.iter().map(|&i| &candidates[i]).collect();

        if let Some(matched_indices) = viterbi(
            mode_data,
            &state.ebg_nodes,
            &seg_coords,
            &seg_candidates,
            sigma,
            weights,
        ) {
            // Build EBG path from matched candidate sequence
            let matching_idx = matchings.len();
            let ebg_path =
                build_matched_path(mode_data, &seg_candidates, &matched_indices, weights);

            let duration_ds = ebg_path
                .iter()
                .map(|&eid| mode_data.node_weights[eid as usize])
                .filter(|&w| w != u32::MAX)
                .sum::<u32>();

            // Compute confidence as average emission probability
            let avg_emission: f64 = matched_indices
                .iter()
                .enumerate()
                .map(|(t, &c_idx)| {
                    let dist = seg_candidates[t][c_idx].distance_m;
                    emission_prob(dist, sigma)
                })
                .sum::<f64>()
                / matched_indices.len() as f64;

            matchings.push(Matching {
                ebg_path,
                duration_ds,
                confidence: avg_emission.exp(), // Convert from log to [0,1]
                region_idx: 0, // single-region path: caller's region is implicit
            });

            // Fill tracepoints
            for (seg_pos, &obs_idx) in segment.iter().enumerate() {
                let c_idx = matched_indices[seg_pos];
                let cand = &seg_candidates[seg_pos][c_idx];
                tracepoints[obs_idx] = Some(Tracepoint {
                    lon: cand.snapped_lon,
                    lat: cand.snapped_lat,
                    ebg_id: cand.ebg_id,
                    matchings_index: matching_idx,
                    waypoint_index: seg_pos,
                });
            }
        }
    }

    if matchings.is_empty() {
        return None;
    }

    Some(MatchResult {
        matchings,
        tracepoints,
    })
}

// ---------------------------------------------------------------------------
// Candidate generation
// ---------------------------------------------------------------------------

fn generate_candidates(
    state: &ServerState,
    mode_idx: u8,
    mask: &[u64],
    lon: f64,
    lat: f64,
) -> Vec<Candidate> {
    // Use midpoint-based selection (topologically reliable), then refine with perpendicular projection
    let hits =
        state
            .snap_index
            .snap_k_with_info_filtered(lon, lat, mode_idx, MAX_CANDIDATES, Some(mask));

    hits.into_iter()
        .filter_map(|(ebg_id, _midpoint_lon, _midpoint_lat, _midpoint_dist)| {
            // Compute perpendicular projection for better snap position and emission distance
            let (proj_lon, proj_lat, proj_dist) =
                project_onto_edge(lon, lat, ebg_id, &state.ebg_nodes, &state.edge_geom);
            // Filter out candidates with no valid projection (fallback returns INFINITY)
            if proj_dist.is_infinite() {
                return None;
            }
            Some(Candidate {
                ebg_id,
                snapped_lon: proj_lon,
                snapped_lat: proj_lat,
                distance_m: proj_dist,
            })
        })
        .collect()
}

/// Project a point onto the nearest position on an edge's polyline.
/// Returns (projected_lon, projected_lat, distance_in_meters).
fn project_onto_edge(
    lon: f64,
    lat: f64,
    ebg_id: u32,
    ebg_nodes: &crate::formats::EbgNodes,
    edge_geom: &EdgeGeometry,
) -> (f64, f64, f64) {
    let node = &ebg_nodes.nodes[ebg_id as usize];
    let polyline = edge_geom.polyline(node.geom_idx);
    let n_pts = polyline.len();
    if n_pts == 0 {
        return fallback_midpoint();
    }

    if n_pts == 1 {
        let (pt_lon, pt_lat) = polyline.at(0);
        let d = great_circle_m(lon, lat, pt_lon, pt_lat);
        return (pt_lon, pt_lat, d);
    }

    // Find closest point on any segment of the polyline
    let (mut best_lon, mut best_lat) = polyline.at(0);
    let mut best_dist = f64::INFINITY;

    for i in 0..n_pts - 1 {
        let (ax, ay) = polyline.at(i);
        let (bx, by) = polyline.at(i + 1);

        let (px, py) = project_point_onto_segment(lon, lat, ax, ay, bx, by);
        let d = great_circle_m(lon, lat, px, py);
        if d < best_dist {
            best_dist = d;
            best_lon = px;
            best_lat = py;
        }
    }

    (best_lon, best_lat, best_dist)
}

/// Project point P onto line segment AB. Returns the closest point on AB.
fn project_point_onto_segment(px: f64, py: f64, ax: f64, ay: f64, bx: f64, by: f64) -> (f64, f64) {
    // Scale lon to approximate equal-area at Belgian latitudes
    let scale_x = METERS_PER_DEG_LON_AT_50;
    let scale_y = METERS_PER_DEG_LAT;

    let dx = (bx - ax) * scale_x;
    let dy = (by - ay) * scale_y;
    let len_sq = dx * dx + dy * dy;

    if len_sq < 1e-12 {
        // Degenerate segment — return endpoint A
        return (ax, ay);
    }

    // t = dot(P-A, B-A) / |B-A|²  clamped to [0, 1]
    let t = (((px - ax) * scale_x * dx + (py - ay) * scale_y * dy) / len_sq).clamp(0.0, 1.0);

    (ax + t * (bx - ax), ay + t * (by - ay))
}

/// Fallback when polyline is unavailable
fn fallback_midpoint() -> (f64, f64, f64) {
    (0.0, 0.0, f64::INFINITY) // Infinite distance = unusable candidate
}

// ---------------------------------------------------------------------------
// Trace segmentation
// ---------------------------------------------------------------------------

/// Split trace into continuous segments.
/// Breaks at: observations with no candidates, or large GPS gaps.
fn find_segments(coordinates: &[(f64, f64)], candidates: &[Vec<Candidate>]) -> Vec<Vec<usize>> {
    let n = coordinates.len();
    let mut segments = Vec::new();
    let mut current_segment: Vec<usize> = Vec::new();

    for i in 0..n {
        if candidates[i].is_empty() {
            // No candidates — break segment
            if current_segment.len() >= 2 {
                segments.push(std::mem::take(&mut current_segment));
            } else {
                current_segment.clear();
            }
            continue;
        }

        // Check for large gap
        if let Some(&prev_idx) = current_segment.last() {
            let gc_dist = great_circle_m(
                coordinates[prev_idx].0,
                coordinates[prev_idx].1,
                coordinates[i].0,
                coordinates[i].1,
            );
            if gc_dist > GAP_THRESHOLD_M {
                if current_segment.len() >= 2 {
                    segments.push(std::mem::take(&mut current_segment));
                } else {
                    current_segment.clear();
                }
            }
        }

        current_segment.push(i);
    }

    // Flush last segment
    if current_segment.len() >= 2 {
        segments.push(current_segment);
    }

    segments
}

// ---------------------------------------------------------------------------
// HMM probabilities
// ---------------------------------------------------------------------------

/// Log emission probability: Gaussian on GPS-to-road distance
/// P(obs | state) ∝ exp(-d² / (2σ²))
fn emission_prob(distance_m: f64, sigma: f64) -> f64 {
    -(distance_m * distance_m) / (2.0 * sigma * sigma)
}

/// Log transition probability: exponential on |route_dist - gc_dist|
/// P(transition) ∝ exp(-|d_route - d_gc| / β)
fn transition_prob(route_dist_m: f64, gc_dist_m: f64) -> f64 {
    let diff = (route_dist_m - gc_dist_m).abs();
    -diff / BETA
}

// ---------------------------------------------------------------------------
// Viterbi algorithm
// ---------------------------------------------------------------------------

/// Run Viterbi decoding on a segment.
/// Returns the index of the best candidate at each time step, or None if decoding fails.
fn viterbi(
    mode_data: &ModeData,
    ebg_nodes: &crate::formats::EbgNodes,
    coordinates: &[(f64, f64)],
    candidates: &[&Vec<Candidate>],
    sigma: f64,
    cch_weights: &CchWeights,
) -> Option<Vec<usize>> {
    let n_obs = coordinates.len();
    if n_obs < 2 {
        return None;
    }

    // Viterbi trellis: log_prob[t][c] = best log-probability to reach candidate c at time t
    let mut log_prob: Vec<Vec<f64>> = Vec::with_capacity(n_obs);
    let mut predecessor: Vec<Vec<Option<usize>>> = Vec::with_capacity(n_obs);

    // Initialize t=0
    let init_probs: Vec<f64> = candidates[0]
        .iter()
        .map(|c| emission_prob(c.distance_m, sigma))
        .collect();
    log_prob.push(init_probs);
    predecessor.push(vec![None; candidates[0].len()]);

    // Forward pass
    for t in 1..n_obs {
        let n_curr = candidates[t].len();
        let n_prev = candidates[t - 1].len();

        if n_curr == 0 {
            return None; // Should not happen (pre-filtered in find_segments)
        }

        let gc_dist = great_circle_m(
            coordinates[t - 1].0,
            coordinates[t - 1].1,
            coordinates[t].0,
            coordinates[t].1,
        );

        // Compute transition costs: shortest-path distance from each prev candidate
        // to each current candidate
        let transition_dists = compute_transition_distances(
            mode_data,
            ebg_nodes,
            candidates[t - 1],
            candidates[t],
            cch_weights,
        );

        let mut curr_probs = vec![NEG_INF; n_curr];
        let mut curr_pred = vec![None; n_curr];

        for c in 0..n_curr {
            let emit = emission_prob(candidates[t][c].distance_m, sigma);

            for p in 0..n_prev {
                if log_prob[t - 1][p] == NEG_INF {
                    continue;
                }

                let route_dist_m = transition_dists[p * n_curr + c];
                if route_dist_m == f64::INFINITY {
                    continue; // No path found
                }

                let trans = transition_prob(route_dist_m, gc_dist);
                let total = log_prob[t - 1][p] + trans + emit;

                if total > curr_probs[c] {
                    curr_probs[c] = total;
                    curr_pred[c] = Some(p);
                }
            }
        }

        log_prob.push(curr_probs);
        predecessor.push(curr_pred);
    }

    // Backtrack: find best final state
    let last_probs = &log_prob[n_obs - 1];
    let best_final = last_probs
        .iter()
        .enumerate()
        .filter(|&(_, p)| *p != NEG_INF)
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(idx, _)| idx)?;

    // Trace back
    let mut path = vec![0usize; n_obs];
    path[n_obs - 1] = best_final;

    for t in (1..n_obs).rev() {
        path[t - 1] = predecessor[t][path[t]]?;
    }

    Some(path)
}

// ---------------------------------------------------------------------------
// Transition distance computation
// ---------------------------------------------------------------------------

/// Compute shortest-path distances (meters) from each candidate in `from` to each in `to`.
/// Returns flat array of size from.len() * to.len() (row-major: from[i] → to[j] at i*n_to+j).
///
/// Uses TIME-based CCH P2P to find the fastest path, then sums the physical edge lengths
/// (length_mm from ebg_nodes) along the unpacked path. This gives exact route distance
/// without depending on distance CCH weights.
fn compute_transition_distances(
    mode_data: &ModeData,
    ebg_nodes: &crate::formats::EbgNodes,
    from: &[Candidate],
    to: &[Candidate],
    cch_weights: &CchWeights,
) -> Vec<f64> {
    let n_from = from.len();
    let n_to = to.len();
    let mut result = vec![f64::INFINITY; n_from * n_to];

    let query = CchQuery::with_custom_weights(
        &mode_data.cch_topo,
        &mode_data.up_adj_flat,
        &mode_data.down_rev_flat,
        cch_weights,
    );

    for (i, from_cand) in from.iter().enumerate() {
        let src_rank = mode_data.orig_to_rank[from_cand.ebg_id as usize];
        if src_rank == u32::MAX {
            continue;
        }

        for (j, to_cand) in to.iter().enumerate() {
            let dst_rank = mode_data.orig_to_rank[to_cand.ebg_id as usize];
            if dst_rank == u32::MAX {
                continue;
            }

            if let Some(qr) = query.query(src_rank, dst_rank) {
                // Unpack CCH path to get EBG edge sequence
                let rank_path = super::unpack::unpack_path(
                    &mode_data.cch_topo,
                    cch_weights,
                    &qr.forward_parent,
                    &qr.backward_parent,
                    src_rank,
                    dst_rank,
                    qr.meeting_node,
                );

                // Sum physical edge lengths (length_mm) along the path.
                // NOTE: This sums full edge lengths including the first and last edges,
                // even though the snap point may be partway along them. The error is
                // bounded by one edge length (~50-200m) and is consistent across all
                // candidate pairs, so Viterbi selection is minimally affected.
                let total_mm: u64 = rank_path
                    .iter()
                    .map(|&rank| {
                        let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
                        let original_id = mode_data.filtered_to_original[filtered_id as usize];
                        ebg_nodes.nodes[original_id as usize].length_mm as u64
                    })
                    .sum();

                result[i * n_to + j] = total_mm as f64 / 1000.0; // mm → m
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Path reconstruction
// ---------------------------------------------------------------------------

/// Build the EBG path by connecting matched candidates with shortest paths.
fn build_matched_path(
    mode_data: &ModeData,
    candidates: &[&Vec<Candidate>],
    matched_indices: &[usize],
    cch_weights: &CchWeights,
) -> Vec<u32> {
    let n_obs = matched_indices.len();
    let mut full_path: Vec<u32> = Vec::new();

    for t in 0..n_obs {
        let cand = &candidates[t][matched_indices[t]];

        if t == 0 {
            full_path.push(cand.ebg_id);
            continue;
        }

        let prev_cand = &candidates[t - 1][matched_indices[t - 1]];
        if prev_cand.ebg_id == cand.ebg_id {
            continue; // Same edge, no path needed
        }

        // Find path between consecutive matched edges
        let sub_path = find_path_between(mode_data, prev_cand.ebg_id, cand.ebg_id, cch_weights);
        match sub_path {
            Some(path) => {
                // Skip first element (duplicate of previous edge)
                for &eid in path.iter().skip(1) {
                    full_path.push(eid);
                }
            }
            None => {
                // No path found — gap in topology. Include target edge to
                // preserve snap point, but geometry will have a discontinuity.
                tracing::warn!(
                    from_ebg = prev_cand.ebg_id,
                    to_ebg = cand.ebg_id,
                    obs = t,
                    "map match: no path between consecutive matched edges, inserting gap"
                );
                full_path.push(cand.ebg_id);
            }
        }
    }

    // Deduplicate consecutive edges
    full_path.dedup();
    full_path
}

/// Find the EBG edge path between two edges using CCH P2P + unpack.
fn find_path_between(
    mode_data: &ModeData,
    from_ebg: u32,
    to_ebg: u32,
    cch_weights: &CchWeights,
) -> Option<Vec<u32>> {
    let src_rank = mode_data.orig_to_rank[from_ebg as usize];
    let dst_rank = mode_data.orig_to_rank[to_ebg as usize];

    if src_rank == u32::MAX || dst_rank == u32::MAX {
        return None;
    }

    let query = CchQuery::with_custom_weights(
        &mode_data.cch_topo,
        &mode_data.up_adj_flat,
        &mode_data.down_rev_flat,
        cch_weights,
    );
    let result = query.query(src_rank, dst_rank)?;

    let rank_path = super::unpack::unpack_path(
        &mode_data.cch_topo,
        cch_weights,
        &result.forward_parent,
        &result.backward_parent,
        src_rank,
        dst_rank,
        result.meeting_node,
    );

    let ebg_path: Vec<u32> = rank_path
        .iter()
        .map(|&rank| {
            let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            mode_data.filtered_to_original[filtered_id as usize]
        })
        .collect();

    Some(ebg_path)
}

// ---------------------------------------------------------------------------
// Geometry helpers
// ---------------------------------------------------------------------------

/// Approximate great-circle distance in meters
fn great_circle_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    let dlat = (lat2 - lat1) * METERS_PER_DEG_LAT;
    let dlon = (lon2 - lon1) * METERS_PER_DEG_LON_AT_50;
    (dlat * dlat + dlon * dlon).sqrt()
}

// ===========================================================================
// Cross-region map matching (#194)
// ===========================================================================
//
// When a GPS trace crosses a region boundary, candidates for sample i
// can be in region A while candidates for sample i+1 are in region B.
// The single-region [`map_match`] above can't compute the transition
// distance because each region has its own CCH and the source/target
// CCH ranks live in disjoint address spaces.
//
// [`map_match_multi_region`] handles the multi-region case. The shape
// mirrors the single-region path: per-sample candidates, segmentation
// at gaps, then Viterbi. The only difference is in
// [`compute_transition_distances_multi`], which dispatches each
// candidate-pair transition to either:
//   - single-region CCH P2P (same region) — same code path as
//     [`compute_transition_distances`] above, just behind a region
//     index check, or
//   - [`solve_cross_region`] (different regions) — runs the access
//     CCH leg in the source region, looks up the prebuilt overlay
//     matrix between representative borders, and runs the egress CCH
//     leg in the destination region. Returns total cost in mode
//     units.
//
// Path reconstruction at cross-region boundaries inserts a "border
// crossing" anchor: the matched sequence is split into one [`Matching`]
// per contiguous same-region run. Edges in different regions cannot be
// concatenated into one `ebg_path` because EBG ids are region-local.
// The handler stitches them back together via two-leg geometry +
// border representative as anchor.
//
// Performance:
//   - Pure single-region trace: hits the existing [`map_match`]
//     fast-path with zero cross-region overhead.
//   - Cross-region transition: ~10x slower than a single-region
//     transition (1 CCH access + matrix lookup + 1 CCH egress vs.
//     1 CCH P2P).
//   - For a 10-point trace where 2 transitions cross regions, total
//     wall-clock is ~5 s + 2 × ~50 ms = ~5.1 s, dominated by the
//     single-region transitions.

/// One candidate match for a GPS observation in a known region.
///
/// Same as [`Candidate`] but tagged with the region index in
/// [`RegionsState::regions`]. Cross-region candidate pairs route their
/// transition cost through [`solve_cross_region`] instead of a single-
/// region CCH P2P.
#[derive(Debug, Clone)]
struct RegionCandidate {
    region_idx: usize,
    ebg_id: u32,
    snapped_lon: f64,
    snapped_lat: f64,
    distance_m: f64,
}

/// Map-match a GPS trace that may span multiple regions via the
/// cross-region overlay (#194).
///
/// Returns `None` if no observations could be matched.
///
/// Fast-path: when every sample's candidates fall in a single region,
/// delegates to the existing single-region [`map_match`] with zero
/// cross-region overhead. The single-region perf budget (~5 s for a
/// 10-point trace) is preserved exactly.
///
/// Cross-region path: when samples span two or more regions, runs a
/// region-aware Viterbi where transitions across region boundaries
/// route through [`solve_cross_region`]. Result `matchings` are split
/// at region boundaries (one [`Matching`] per contiguous same-region
/// run); cross-region transitions appear as adjacent matchings with
/// the border representative implicit between them.
///
/// `mode_name` must be loaded in every region the trace touches; if
/// any region lacks the mode the function returns `None`.
pub fn map_match_multi_region(
    regions: &RegionsState,
    mode_name: &str,
    coordinates: &[(f64, f64)],
    gps_accuracy: Option<f64>,
) -> Option<MatchResult> {
    let n = coordinates.len();
    if n < 2 {
        return None;
    }

    let sigma = gps_accuracy.unwrap_or(DEFAULT_GPS_SIGMA).max(1.0);

    // ---- Step 1: per-sample multi-region candidate generation ------
    //
    // For each GPS sample, collect candidates from every region whose
    // snap_index returns hits. A sample near the border can have
    // candidates from BOTH adjacent regions.
    let candidates: Vec<Vec<RegionCandidate>> = coordinates
        .iter()
        .map(|&(lon, lat)| generate_candidates_multi(regions, mode_name, lon, lat))
        .collect();

    // ---- Step 2: fast-path detection -------------------------------
    //
    // If every non-empty candidate set lives in a single region, fall
    // through to the existing single-region path. Cross-region
    // routing is only invoked when at least one sample has candidates
    // in a different region than another sample.
    let single_region = single_region_id(&candidates);
    if let Some(idx) = single_region {
        // Fast-path: one region, defer to the existing single-region
        // implementation. No cross-region overhead.
        let entry = &regions.regions[idx];
        let mode_idx = match entry.state.mode_lookup.get(mode_name) {
            Some(&m) => m,
            None => return None,
        };
        return map_match(
            &entry.state,
            Mode(mode_idx),
            coordinates,
            gps_accuracy,
            None,
            None,
        );
    }

    // ---- Step 3: cross-region path ---------------------------------
    //
    // Need an overlay to compute cross-region transitions. Without
    // one, treat as no-match (caller decides whether to 404 or fall
    // back to per-region single matching).
    let overlay = regions.overlay.as_ref()?;

    // Resolve mode for each region we'll touch.
    let mode_indices: Vec<u8> = regions
        .regions
        .iter()
        .map(|r| r.state.mode_lookup.get(mode_name).copied().unwrap_or(u8::MAX))
        .collect();

    // ---- Step 4: segmentation (gaps + unmatched samples) -----------
    let segments = find_segments_multi(coordinates, &candidates);
    if segments.is_empty() {
        return None;
    }

    // ---- Step 5: Viterbi per segment, region-aware -----------------
    let mut matchings: Vec<Matching> = Vec::new();
    let mut tracepoints: Vec<Option<Tracepoint>> = vec![None; n];

    for segment in &segments {
        let seg_coords: Vec<(f64, f64)> = segment.iter().map(|&i| coordinates[i]).collect();
        let seg_candidates: Vec<&Vec<RegionCandidate>> =
            segment.iter().map(|&i| &candidates[i]).collect();

        let matched_indices = match viterbi_multi(
            regions,
            &mode_indices,
            mode_name,
            overlay,
            &seg_coords,
            &seg_candidates,
            sigma,
        ) {
            Some(v) => v,
            None => continue,
        };

        // Split matched sequence into per-region runs and build one
        // Matching per run. Cross-region transitions become matching
        // boundaries; the border crossing is implicit between adjacent
        // matchings (caller's geometry assembly inserts a border
        // anchor if it has the overlay).
        let runs = split_into_region_runs(&seg_candidates, &matched_indices);
        for (run_start, run_end) in runs.iter() {
            // run is [run_start, run_end] inclusive within the segment.
            let run_region_idx = seg_candidates[*run_start][matched_indices[*run_start]].region_idx;
            let entry = &regions.regions[run_region_idx];
            let m_idx = mode_indices[run_region_idx];
            if m_idx == u8::MAX {
                continue;
            }
            let mode_data = entry.state.get_mode(Mode(m_idx));

            // Materialise the (single-region) Candidate slice for this
            // run so we can reuse build_matched_path verbatim.
            let run_cands: Vec<Vec<Candidate>> = (*run_start..=*run_end)
                .map(|t| {
                    seg_candidates[t]
                        .iter()
                        .filter(|c| c.region_idx == run_region_idx)
                        .map(|c| Candidate {
                            ebg_id: c.ebg_id,
                            snapped_lon: c.snapped_lon,
                            snapped_lat: c.snapped_lat,
                            distance_m: c.distance_m,
                        })
                        .collect()
                })
                .collect();
            // Recompute matched indices in the projected (single-
            // region) candidate vectors. We pick the same physical
            // candidate (ebg_id) we matched in the multi-region pass.
            let run_matched: Vec<usize> = (*run_start..=*run_end)
                .map(|t| {
                    let target_ebg = seg_candidates[t][matched_indices[t]].ebg_id;
                    run_cands[t - *run_start]
                        .iter()
                        .position(|c| c.ebg_id == target_ebg)
                        .unwrap_or(0)
                })
                .collect();

            let run_cand_refs: Vec<&Vec<Candidate>> = run_cands.iter().collect();
            let ebg_path = build_matched_path(
                mode_data,
                &run_cand_refs,
                &run_matched,
                &mode_data.cch_weights,
            );

            let duration_ds = ebg_path
                .iter()
                .map(|&eid| mode_data.node_weights[eid as usize])
                .filter(|&w| w != u32::MAX)
                .sum::<u32>();

            // Average emission for this run.
            let avg_emission: f64 = (*run_start..=*run_end)
                .map(|t| {
                    let dist = seg_candidates[t][matched_indices[t]].distance_m;
                    emission_prob(dist, sigma)
                })
                .sum::<f64>()
                / ((*run_end - *run_start + 1) as f64);

            let matching_idx = matchings.len();

            matchings.push(Matching {
                ebg_path,
                duration_ds,
                confidence: avg_emission.exp(),
                region_idx: run_region_idx,
            });

            for (waypoint_index, t) in (*run_start..=*run_end).enumerate() {
                let cand = &seg_candidates[t][matched_indices[t]];
                let obs_idx = segment[t];
                tracepoints[obs_idx] = Some(Tracepoint {
                    lon: cand.snapped_lon,
                    lat: cand.snapped_lat,
                    ebg_id: cand.ebg_id,
                    matchings_index: matching_idx,
                    waypoint_index,
                });
            }
        }
    }

    if matchings.is_empty() {
        return None;
    }

    Some(MatchResult {
        matchings,
        tracepoints,
    })
}

/// Generate candidates for a GPS sample by querying every loaded
/// region's snap_index. Tags each candidate with its region index so
/// the Viterbi transition step knows which CCH to use.
fn generate_candidates_multi(
    regions: &RegionsState,
    mode_name: &str,
    lon: f64,
    lat: f64,
) -> Vec<RegionCandidate> {
    let mut out: Vec<RegionCandidate> = Vec::new();
    for (region_idx, entry) in regions.regions.iter().enumerate() {
        let mode_idx = match entry.state.mode_lookup.get(mode_name) {
            Some(&m) => m,
            None => continue,
        };
        let mode_data = entry.state.get_mode(Mode(mode_idx));
        let mask = &mode_data.mask;
        let hits = entry.state.snap_index.snap_k_with_info_filtered(
            lon,
            lat,
            mode_idx,
            MAX_CANDIDATES,
            Some(mask),
        );
        for (ebg_id, _mlon, _mlat, _mdist) in hits {
            let (proj_lon, proj_lat, proj_dist) =
                project_onto_edge(lon, lat, ebg_id, &entry.state.ebg_nodes, &entry.state.edge_geom);
            if proj_dist.is_infinite() {
                continue;
            }
            out.push(RegionCandidate {
                region_idx,
                ebg_id,
                snapped_lon: proj_lon,
                snapped_lat: proj_lat,
                distance_m: proj_dist,
            });
        }
    }
    // Cap total candidates to bound Viterbi cost: a sample near a
    // border can produce up to MAX_CANDIDATES * n_regions candidates,
    // which inflates transition compute. Sort by distance ascending
    // and keep the closest 2*MAX_CANDIDATES (room for both regions on
    // a border sample).
    out.sort_by(|a, b| {
        a.distance_m
            .partial_cmp(&b.distance_m)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    out.truncate(MAX_CANDIDATES * 2);
    out
}

/// If every non-empty candidate set lives in the same region, return
/// that region's index. Otherwise return `None`.
fn single_region_id(candidates: &[Vec<RegionCandidate>]) -> Option<usize> {
    let mut seen: Option<usize> = None;
    for cs in candidates.iter() {
        for c in cs.iter() {
            match seen {
                None => seen = Some(c.region_idx),
                Some(idx) if idx == c.region_idx => {}
                Some(_) => return None,
            }
        }
    }
    seen
}

/// Same logic as [`find_segments`] but parameterised on
/// [`RegionCandidate`].
fn find_segments_multi(
    coordinates: &[(f64, f64)],
    candidates: &[Vec<RegionCandidate>],
) -> Vec<Vec<usize>> {
    let n = coordinates.len();
    let mut segments = Vec::new();
    let mut current_segment: Vec<usize> = Vec::new();

    for i in 0..n {
        if candidates[i].is_empty() {
            if current_segment.len() >= 2 {
                segments.push(std::mem::take(&mut current_segment));
            } else {
                current_segment.clear();
            }
            continue;
        }

        if let Some(&prev_idx) = current_segment.last() {
            let gc_dist = great_circle_m(
                coordinates[prev_idx].0,
                coordinates[prev_idx].1,
                coordinates[i].0,
                coordinates[i].1,
            );
            if gc_dist > GAP_THRESHOLD_M {
                if current_segment.len() >= 2 {
                    segments.push(std::mem::take(&mut current_segment));
                } else {
                    current_segment.clear();
                }
            }
        }

        current_segment.push(i);
    }

    if current_segment.len() >= 2 {
        segments.push(current_segment);
    }
    segments
}

/// Region-aware Viterbi. Same shape as [`viterbi`] but transition
/// distances are computed via [`compute_transition_distances_multi`]
/// which dispatches to single-region CCH or [`solve_cross_region`]
/// per candidate pair.
#[allow(clippy::too_many_arguments)]
fn viterbi_multi(
    regions: &RegionsState,
    mode_indices: &[u8],
    mode_name: &str,
    overlay: &OverlayCluster,
    coordinates: &[(f64, f64)],
    candidates: &[&Vec<RegionCandidate>],
    sigma: f64,
) -> Option<Vec<usize>> {
    let n_obs = coordinates.len();
    if n_obs < 2 {
        return None;
    }

    let mut log_prob: Vec<Vec<f64>> = Vec::with_capacity(n_obs);
    let mut predecessor: Vec<Vec<Option<usize>>> = Vec::with_capacity(n_obs);

    let init_probs: Vec<f64> = candidates[0]
        .iter()
        .map(|c| emission_prob(c.distance_m, sigma))
        .collect();
    log_prob.push(init_probs);
    predecessor.push(vec![None; candidates[0].len()]);

    for t in 1..n_obs {
        let n_curr = candidates[t].len();
        let n_prev = candidates[t - 1].len();

        if n_curr == 0 {
            return None;
        }

        let gc_dist = great_circle_m(
            coordinates[t - 1].0,
            coordinates[t - 1].1,
            coordinates[t].0,
            coordinates[t].1,
        );

        let transition_dists = compute_transition_distances_multi(
            regions,
            mode_indices,
            mode_name,
            overlay,
            candidates[t - 1],
            candidates[t],
        );

        let mut curr_probs = vec![NEG_INF; n_curr];
        let mut curr_pred = vec![None; n_curr];

        for c in 0..n_curr {
            let emit = emission_prob(candidates[t][c].distance_m, sigma);
            for p in 0..n_prev {
                if log_prob[t - 1][p] == NEG_INF {
                    continue;
                }
                let route_dist_m = transition_dists[p * n_curr + c];
                if route_dist_m == f64::INFINITY {
                    continue;
                }
                let trans = transition_prob(route_dist_m, gc_dist);
                let total = log_prob[t - 1][p] + trans + emit;
                if total > curr_probs[c] {
                    curr_probs[c] = total;
                    curr_pred[c] = Some(p);
                }
            }
        }

        log_prob.push(curr_probs);
        predecessor.push(curr_pred);
    }

    let last_probs = &log_prob[n_obs - 1];
    let best_final = last_probs
        .iter()
        .enumerate()
        .filter(|&(_, p)| *p != NEG_INF)
        .max_by(|a, b| a.1.partial_cmp(b.1).unwrap_or(std::cmp::Ordering::Equal))
        .map(|(idx, _)| idx)?;

    let mut path = vec![0usize; n_obs];
    path[n_obs - 1] = best_final;
    for t in (1..n_obs).rev() {
        path[t - 1] = predecessor[t][path[t]]?;
    }
    Some(path)
}

/// Compute per-pair transition distances (meters) in the multi-region
/// setting. For same-region pairs, runs single-region CCH P2P + length
/// sum. For cross-region pairs, runs [`solve_cross_region`] and sums
/// physical lengths from both legs.
///
/// The cross-region path is ~10× slower than the same-region path so
/// we group `from`/`to` by `(from.region_idx, to.region_idx)` and
/// reuse the per-region CCH query state where possible.
#[allow(clippy::too_many_arguments)]
fn compute_transition_distances_multi(
    regions: &RegionsState,
    mode_indices: &[u8],
    mode_name: &str,
    overlay: &OverlayCluster,
    from: &[RegionCandidate],
    to: &[RegionCandidate],
) -> Vec<f64> {
    let n_from = from.len();
    let n_to = to.len();
    let mut result = vec![f64::INFINITY; n_from * n_to];

    // Group by (from_region, to_region) to amortise CchQuery
    // construction (negligible compared to the search itself but keeps
    // the inner loop tidy).
    for (i, fc) in from.iter().enumerate() {
        let from_region = fc.region_idx;
        let from_mode_idx = mode_indices[from_region];
        if from_mode_idx == u8::MAX {
            continue;
        }
        let from_state: &Arc<ServerState> = &regions.regions[from_region].state;
        let from_mode_data = from_state.get_mode(Mode(from_mode_idx));
        let src_rank = from_mode_data.orig_to_rank[fc.ebg_id as usize];
        if src_rank == u32::MAX {
            continue;
        }

        for (j, tc) in to.iter().enumerate() {
            let to_region = tc.region_idx;
            let to_mode_idx = mode_indices[to_region];
            if to_mode_idx == u8::MAX {
                continue;
            }

            if from_region == to_region {
                // Same region: single CCH P2P, sum length_mm.
                let to_mode_data = from_mode_data; // same region
                let dst_rank = to_mode_data.orig_to_rank[tc.ebg_id as usize];
                if dst_rank == u32::MAX {
                    continue;
                }
                let query = CchQuery::with_custom_weights(
                    &from_mode_data.cch_topo,
                    &from_mode_data.up_adj_flat,
                    &from_mode_data.down_rev_flat,
                    &from_mode_data.cch_weights,
                );
                if let Some(qr) = query.query(src_rank, dst_rank) {
                    let rank_path = super::unpack::unpack_path(
                        &from_mode_data.cch_topo,
                        &from_mode_data.cch_weights,
                        &qr.forward_parent,
                        &qr.backward_parent,
                        src_rank,
                        dst_rank,
                        qr.meeting_node,
                    );
                    let total_mm: u64 = rank_path
                        .iter()
                        .map(|&rank| {
                            let filtered_id =
                                from_mode_data.cch_topo.rank_to_filtered[rank as usize];
                            let original_id =
                                from_mode_data.filtered_to_original[filtered_id as usize];
                            from_state.ebg_nodes.nodes[original_id as usize].length_mm as u64
                        })
                        .sum();
                    result[i * n_to + j] = total_mm as f64 / 1000.0;
                }
            } else {
                // Different regions: cross-region solve. Returns total
                // cost in mode units (deciseconds). For HMM transition
                // probability we want PHYSICAL DISTANCE, so we unpack
                // both legs and sum length_mm, plus add the haversine
                // border-crossing distance from src_border_ebg to
                // dst_border_ebg.
                let to_state: &Arc<ServerState> = &regions.regions[to_region].state;
                let to_mode_data = to_state.get_mode(Mode(to_mode_idx));
                let dst_rank = to_mode_data.orig_to_rank[tc.ebg_id as usize];
                if dst_rank == u32::MAX {
                    continue;
                }
                let from_id = &regions.regions[from_region].id;
                let to_id = &regions.regions[to_region].id;
                let solution = match solve_cross_region(
                    from_state,
                    from_id,
                    src_rank,
                    to_state,
                    to_id,
                    dst_rank,
                    mode_name,
                    overlay,
                ) {
                    Some(s) => s,
                    None => continue,
                };

                // Distance from src snap → src border representative
                // (within from_region).
                let src_border_rank = *from_mode_data
                    .orig_to_rank
                    .get(solution.src_border_ebg as usize)
                    .unwrap_or(&u32::MAX);
                let dst_border_rank = *to_mode_data
                    .orig_to_rank
                    .get(solution.dst_border_ebg as usize)
                    .unwrap_or(&u32::MAX);
                if src_border_rank == u32::MAX || dst_border_rank == u32::MAX {
                    continue;
                }

                let (_pts1, src_dist_m) = super::route::leg_points_and_distance(
                    from_state,
                    Mode(from_mode_idx),
                    src_rank,
                    src_border_rank,
                );
                let (_pts2, dst_dist_m) = super::route::leg_points_and_distance(
                    to_state,
                    Mode(to_mode_idx),
                    dst_border_rank,
                    dst_rank,
                );

                // Border crossing physical distance — straight-line
                // haversine between the two representative borders.
                let border_m = {
                    let src_reps = overlay.region_representatives(from_id);
                    let dst_reps = overlay.region_representatives(to_id);
                    match (
                        src_reps.get(solution.src_border_idx as usize),
                        dst_reps.get(solution.dst_border_idx as usize),
                    ) {
                        (Some(sb), Some(db)) => {
                            crate::nbg::haversine_distance(sb.lat, sb.lon, db.lat, db.lon)
                        }
                        _ => 0.0,
                    }
                };

                result[i * n_to + j] = src_dist_m + border_m + dst_dist_m;
            }
        }
    }

    result
}

/// Split a matched candidate sequence into contiguous same-region
/// runs. Each run is `(run_start, run_end)` inclusive within the
/// segment. Cross-region transitions become run boundaries.
fn split_into_region_runs(
    candidates: &[&Vec<RegionCandidate>],
    matched_indices: &[usize],
) -> Vec<(usize, usize)> {
    let n = matched_indices.len();
    let mut runs: Vec<(usize, usize)> = Vec::new();
    if n == 0 {
        return runs;
    }
    let mut run_start = 0usize;
    let mut prev_region = candidates[0][matched_indices[0]].region_idx;
    for t in 1..n {
        let cur_region = candidates[t][matched_indices[t]].region_idx;
        if cur_region != prev_region {
            runs.push((run_start, t - 1));
            run_start = t;
            prev_region = cur_region;
        }
    }
    runs.push((run_start, n - 1));
    runs
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_emission_prob_closer_is_better() {
        let p1 = emission_prob(5.0, 10.0);
        let p2 = emission_prob(20.0, 10.0);
        assert!(p1 > p2, "Closer point should have higher log-probability");
    }

    #[test]
    fn test_emission_prob_zero_distance() {
        let p = emission_prob(0.0, 10.0);
        assert_eq!(p, 0.0, "Zero distance should give log-prob 0");
    }

    #[test]
    fn test_transition_prob_matching_distances() {
        let p = transition_prob(100.0, 100.0);
        assert_eq!(p, 0.0, "Equal route/GC distances should give log-prob 0");
    }

    #[test]
    fn test_transition_prob_mismatch_penalized() {
        let p1 = transition_prob(100.0, 100.0); // perfect match
        let p2 = transition_prob(200.0, 100.0); // 100m mismatch
        assert!(p1 > p2, "Mismatched distances should be penalized");
    }

    #[test]
    fn test_great_circle_m() {
        // Brussels to nearby point ~1km east
        let d = great_circle_m(4.35, 50.85, 4.364, 50.85);
        assert!(d > 900.0 && d < 1100.0, "Should be ~1000m, got {}", d);
    }

    #[test]
    fn test_great_circle_m_zero() {
        let d = great_circle_m(4.35, 50.85, 4.35, 50.85);
        assert_eq!(d, 0.0);
    }

    #[test]
    fn test_find_segments_no_candidates() {
        let coords = vec![(4.0, 50.0), (4.1, 50.1), (4.2, 50.2)];
        let cands: Vec<Vec<Candidate>> = vec![vec![], vec![], vec![]];
        let segments = find_segments(&coords, &cands);
        assert!(
            segments.is_empty(),
            "Should produce no segments when no candidates"
        );
    }

    #[test]
    fn test_find_segments_continuous() {
        let coords = vec![(4.0, 50.0), (4.001, 50.0), (4.002, 50.0)];
        let cands: Vec<Vec<Candidate>> = coords
            .iter()
            .enumerate()
            .map(|(i, _)| {
                vec![Candidate {
                    ebg_id: i as u32,
                    snapped_lon: coords[i].0,
                    snapped_lat: coords[i].1,
                    distance_m: 5.0,
                }]
            })
            .collect();
        let segments = find_segments(&coords, &cands);
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0], vec![0, 1, 2]);
    }

    #[test]
    fn test_find_segments_gap_split() {
        // Point 0,1 close together, then huge gap to point 2,3
        let coords = vec![
            (4.0, 50.0),
            (4.001, 50.0),
            (4.1, 50.0), // ~7km gap from point 1
            (4.101, 50.0),
        ];
        let cands: Vec<Vec<Candidate>> = coords
            .iter()
            .enumerate()
            .map(|(i, _)| {
                vec![Candidate {
                    ebg_id: i as u32,
                    snapped_lon: coords[i].0,
                    snapped_lat: coords[i].1,
                    distance_m: 5.0,
                }]
            })
            .collect();
        let segments = find_segments(&coords, &cands);
        assert_eq!(segments.len(), 2, "Should split at gap");
        assert_eq!(segments[0], vec![0, 1]);
        assert_eq!(segments[1], vec![2, 3]);
    }

    // -- Cross-region helpers (#194) ------------------------------

    fn rc(region_idx: usize, ebg_id: u32) -> RegionCandidate {
        RegionCandidate {
            region_idx,
            ebg_id,
            snapped_lon: 0.0,
            snapped_lat: 0.0,
            distance_m: 0.0,
        }
    }

    #[test]
    fn test_single_region_id_all_one_region() {
        let cands = vec![vec![rc(0, 1), rc(0, 2)], vec![rc(0, 3)]];
        assert_eq!(single_region_id(&cands), Some(0));
    }

    #[test]
    fn test_single_region_id_mixed_returns_none() {
        let cands = vec![vec![rc(0, 1), rc(1, 2)], vec![rc(0, 3)]];
        assert_eq!(single_region_id(&cands), None);
    }

    #[test]
    fn test_single_region_id_empty_first_then_one() {
        let cands = vec![vec![], vec![rc(1, 5), rc(1, 6)]];
        assert_eq!(single_region_id(&cands), Some(1));
    }

    #[test]
    fn test_single_region_id_all_empty() {
        let cands: Vec<Vec<RegionCandidate>> = vec![vec![], vec![]];
        assert_eq!(single_region_id(&cands), None);
    }

    #[test]
    fn test_split_into_region_runs_single_run() {
        // Two samples both matched into region 0.
        let candidates = vec![
            vec![rc(0, 1)],
            vec![rc(0, 2), rc(0, 3)],
            vec![rc(0, 4)],
        ];
        let cand_refs: Vec<&Vec<RegionCandidate>> = candidates.iter().collect();
        let matched = vec![0, 0, 0];
        let runs = split_into_region_runs(&cand_refs, &matched);
        assert_eq!(runs, vec![(0, 2)]);
    }

    #[test]
    fn test_split_into_region_runs_cross_region() {
        // Sample 0 -> region 0, sample 1 -> region 1.
        let candidates = vec![
            vec![rc(0, 1), rc(1, 2)],
            vec![rc(0, 3), rc(1, 4)],
            vec![rc(1, 5)],
        ];
        let cand_refs: Vec<&Vec<RegionCandidate>> = candidates.iter().collect();
        // matched[0]=0 -> region 0; matched[1]=1 -> region 1; matched[2]=0 -> region 1
        let matched = vec![0, 1, 0];
        let runs = split_into_region_runs(&cand_refs, &matched);
        assert_eq!(runs, vec![(0, 0), (1, 2)]);
    }

    #[test]
    fn test_split_into_region_runs_alternating() {
        // 0 -> 1 -> 0 (zigzag)
        let candidates = vec![vec![rc(0, 1)], vec![rc(1, 2)], vec![rc(0, 3)]];
        let cand_refs: Vec<&Vec<RegionCandidate>> = candidates.iter().collect();
        let matched = vec![0, 0, 0];
        let runs = split_into_region_runs(&cand_refs, &matched);
        assert_eq!(runs, vec![(0, 0), (1, 1), (2, 2)]);
    }

    #[test]
    fn test_find_segments_multi_continuous() {
        let coords = vec![(4.0, 50.0), (4.001, 50.0), (4.002, 50.0)];
        let cands = vec![vec![rc(0, 1)], vec![rc(0, 2)], vec![rc(0, 3)]];
        let segs = find_segments_multi(&coords, &cands);
        assert_eq!(segs, vec![vec![0, 1, 2]]);
    }

    #[test]
    fn test_find_segments_multi_gap_split() {
        let coords = vec![(4.0, 50.0), (4.001, 50.0), (4.1, 50.0), (4.101, 50.0)];
        let cands = vec![
            vec![rc(0, 1)],
            vec![rc(0, 2)],
            vec![rc(0, 3)],
            vec![rc(0, 4)],
        ];
        let segs = find_segments_multi(&coords, &cands);
        assert_eq!(segs.len(), 2);
        assert_eq!(segs[0], vec![0, 1]);
        assert_eq!(segs[1], vec![2, 3]);
    }
}
