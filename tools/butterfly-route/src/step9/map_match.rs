//! Map matching — snap GPS traces to road network using HMM + Viterbi
//!
//! Algorithm: Hidden Markov Model with Viterbi decoding (Newson & Krumm 2009)
//! - Emission probability: Gaussian on perpendicular GPS-to-edge distance
//! - Transition probability: exponential on |route_distance - great_circle_distance|
//! - Route distance: sum of physical edge lengths (length_mm) along the fastest path

use crate::formats::NbgGeo;
use crate::profile_abi::Mode;

use super::query::CchQuery;
use super::state::{ModeData, ServerState};

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
) -> Option<MatchResult> {
    let n = coordinates.len();
    if n < 2 {
        return None;
    }

    let mode_data = state.get_mode(mode);
    let sigma = gps_accuracy.unwrap_or(DEFAULT_GPS_SIGMA).max(1.0);

    // Step 1: Generate candidates for each observation
    let candidates: Vec<Vec<Candidate>> = coordinates
        .iter()
        .map(|&(lon, lat)| generate_candidates(state, mode_data, lon, lat))
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
        ) {
            // Build EBG path from matched candidate sequence
            let matching_idx = matchings.len();
            let ebg_path =
                build_matched_path(state, mode, mode_data, &seg_candidates, &matched_indices);

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
    mode_data: &ModeData,
    lon: f64,
    lat: f64,
) -> Vec<Candidate> {
    // Use midpoint-based selection (topologically reliable), then refine with perpendicular projection
    let hits = state
        .spatial_index
        .snap_k_with_info(lon, lat, &mode_data.mask, MAX_CANDIDATES);

    hits.into_iter()
        .map(|(ebg_id, _midpoint_lon, _midpoint_lat, _midpoint_dist)| {
            // Compute perpendicular projection for better snap position and emission distance
            let (proj_lon, proj_lat, proj_dist) =
                project_onto_edge(lon, lat, ebg_id, &state.ebg_nodes, &state.nbg_geo);
            Candidate {
                ebg_id,
                snapped_lon: proj_lon,
                snapped_lat: proj_lat,
                distance_m: proj_dist,
            }
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
    nbg_geo: &NbgGeo,
) -> (f64, f64, f64) {
    let node = &ebg_nodes.nodes[ebg_id as usize];
    let geom_idx = node.geom_idx as usize;

    if geom_idx >= nbg_geo.polylines.len() {
        // Fallback: use edge endpoints from nbg_geo
        return fallback_midpoint(ebg_id, ebg_nodes, nbg_geo);
    }

    let polyline = &nbg_geo.polylines[geom_idx];
    let n_pts = polyline.lat_fxp.len();
    if n_pts == 0 {
        return fallback_midpoint(ebg_id, ebg_nodes, nbg_geo);
    }

    if n_pts == 1 {
        let pt_lon = polyline.lon_fxp[0] as f64 / 1e7;
        let pt_lat = polyline.lat_fxp[0] as f64 / 1e7;
        let d = great_circle_m(lon, lat, pt_lon, pt_lat);
        return (pt_lon, pt_lat, d);
    }

    // Find closest point on any segment of the polyline
    let mut best_lon = polyline.lon_fxp[0] as f64 / 1e7;
    let mut best_lat = polyline.lat_fxp[0] as f64 / 1e7;
    let mut best_dist = f64::INFINITY;

    for i in 0..n_pts - 1 {
        let ax = polyline.lon_fxp[i] as f64 / 1e7;
        let ay = polyline.lat_fxp[i] as f64 / 1e7;
        let bx = polyline.lon_fxp[i + 1] as f64 / 1e7;
        let by = polyline.lat_fxp[i + 1] as f64 / 1e7;

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
fn fallback_midpoint(
    _ebg_id: u32,
    _ebg_nodes: &crate::formats::EbgNodes,
    _nbg_geo: &NbgGeo,
) -> (f64, f64, f64) {
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
        let transition_dists =
            compute_transition_distances(mode_data, ebg_nodes, candidates[t - 1], candidates[t]);

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
        .filter(|(_, &p)| p != NEG_INF)
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
) -> Vec<f64> {
    let n_from = from.len();
    let n_to = to.len();
    let mut result = vec![f64::INFINITY; n_from * n_to];

    let query = CchQuery::with_custom_weights(
        &mode_data.cch_topo,
        &mode_data.down_rev,
        &mode_data.cch_weights,
    );

    for (i, from_cand) in from.iter().enumerate() {
        let src_filtered = mode_data.filtered_ebg.original_to_filtered[from_cand.ebg_id as usize];
        if src_filtered == u32::MAX {
            continue;
        }
        let src_rank = mode_data.order.perm[src_filtered as usize];

        for (j, to_cand) in to.iter().enumerate() {
            let dst_filtered = mode_data.filtered_ebg.original_to_filtered[to_cand.ebg_id as usize];
            if dst_filtered == u32::MAX {
                continue;
            }
            let dst_rank = mode_data.order.perm[dst_filtered as usize];

            if let Some(qr) = query.query(src_rank, dst_rank) {
                // Unpack CCH path to get EBG edge sequence
                let rank_path = super::unpack::unpack_path(
                    &mode_data.cch_topo,
                    &qr.forward_parent,
                    &qr.backward_parent,
                    src_rank,
                    dst_rank,
                    qr.meeting_node,
                );

                // Sum physical edge lengths (length_mm) along the path
                let total_mm: u64 = rank_path
                    .iter()
                    .map(|&rank| {
                        let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
                        let original_id =
                            mode_data.filtered_ebg.filtered_to_original[filtered_id as usize];
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
    state: &ServerState,
    mode: Mode,
    mode_data: &ModeData,
    candidates: &[&Vec<Candidate>],
    matched_indices: &[usize],
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
        let sub_path = find_path_between(state, mode, mode_data, prev_cand.ebg_id, cand.ebg_id);
        match sub_path {
            Some(path) => {
                // Skip first element (duplicate of previous edge)
                for &eid in path.iter().skip(1) {
                    full_path.push(eid);
                }
            }
            None => {
                // No path found — just append the target edge
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
    state: &ServerState,
    mode: Mode,
    mode_data: &ModeData,
    from_ebg: u32,
    to_ebg: u32,
) -> Option<Vec<u32>> {
    let src_filtered = mode_data.filtered_ebg.original_to_filtered[from_ebg as usize];
    let dst_filtered = mode_data.filtered_ebg.original_to_filtered[to_ebg as usize];

    if src_filtered == u32::MAX || dst_filtered == u32::MAX {
        return None;
    }

    let src_rank = mode_data.order.perm[src_filtered as usize];
    let dst_rank = mode_data.order.perm[dst_filtered as usize];

    let query = CchQuery::new(state, mode);
    let result = query.query(src_rank, dst_rank)?;

    let rank_path = super::unpack::unpack_path(
        &mode_data.cch_topo,
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
            mode_data.filtered_ebg.filtered_to_original[filtered_id as usize]
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
}
