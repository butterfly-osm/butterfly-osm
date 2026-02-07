//! TSP/Trip optimization module
//!
//! Provides a trip endpoint that takes a set of waypoints and returns the
//! optimized visiting order that minimizes total travel time. Uses a
//! nearest-neighbor greedy heuristic followed by 2-opt local improvement.
//!
//! The TSP solver operates on a precomputed N×N distance matrix from the
//! bucket M2M algorithm.

use axum::{
    extract::State,
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use crate::matrix::bucket_ch::table_bucket_full_flat;
use crate::profile_abi::Mode;

use super::state::ServerState;

// ============ TSP Solver (pure algorithm) ============

/// Result of TSP optimization
#[derive(Debug, Clone)]
pub struct TspSolution {
    /// Indices into original waypoints, in optimized visit order
    pub order: Vec<usize>,
    /// Total travel cost along the tour (in raw matrix units)
    pub total_cost: u64,
    /// Percentage improvement from 2-opt over the greedy solution
    pub improvement_pct: f64,
}

/// Solve TSP using multi-start nearest-neighbor greedy + 2-opt local improvement.
///
/// `matrix` is a flat N×N cost matrix where `matrix[i * n + j]` = cost from waypoint i to j.
/// Costs are u32 values; u32::MAX means unreachable.
///
/// If `round_trip` is true, the tour returns to the starting point.
/// If false, it is an open path from the first visited to the last.
///
/// The algorithm tries nearest-neighbor from every possible starting waypoint,
/// applies 2-opt improvement to each, and returns the best result.
/// For N <= 100 this is O(N^3 * iterations) which completes in microseconds.
///
/// Returns the optimized visit order as indices into the original waypoint list.
pub fn solve_tsp(matrix: &[u32], n: usize, round_trip: bool) -> TspSolution {
    // Trivial cases
    if n == 0 {
        return TspSolution {
            order: vec![],
            total_cost: 0,
            improvement_pct: 0.0,
        };
    }
    if n == 1 {
        return TspSolution {
            order: vec![0],
            total_cost: 0,
            improvement_pct: 0.0,
        };
    }
    if n == 2 {
        let cost_01 = cost(matrix, n, 0, 1);
        let total = if round_trip {
            let cost_10 = cost(matrix, n, 1, 0);
            cost_01.saturating_add(cost_10)
        } else {
            cost_01
        };
        return TspSolution {
            order: vec![0, 1],
            total_cost: total as u64,
            improvement_pct: 0.0,
        };
    }

    // Multi-start nearest-neighbor: try starting from every node, keep best.
    // This is critical for asymmetric matrices where a greedy choice from one
    // start can be catastrophically bad while another start finds near-optimal.
    //
    // For round trips: all starts are equivalent (tour forms a cycle), so
    // we try all N starts and rotate the best to begin at waypoint 0.
    //
    // For open paths: the departure point is fixed at waypoint 0, so we only
    // run greedy from start=0 but still apply 2-opt improvement.
    let mut best_order: Vec<usize> = Vec::new();
    let mut best_cost: u64 = u64::MAX;
    let mut best_greedy_cost: u64 = u64::MAX;

    if round_trip {
        for start in 0..n {
            let greedy_order = nearest_neighbor_greedy(matrix, n, start);
            let greedy_cost = tour_cost(matrix, n, &greedy_order, round_trip);
            let (mut opt_order, opt_cost) = two_opt_improve(matrix, n, greedy_order, greedy_cost, round_trip);

            // Rotate so waypoint 0 is first (cosmetic — same cycle cost)
            if let Some(pos) = opt_order.iter().position(|&x| x == 0) {
                opt_order.rotate_left(pos);
            }

            if opt_cost < best_cost || best_order.is_empty() {
                best_cost = opt_cost;
                best_order = opt_order;
                best_greedy_cost = greedy_cost;
            }
        }
    } else {
        // Open path: fixed start at waypoint 0
        let greedy_order = nearest_neighbor_greedy(matrix, n, 0);
        let greedy_cost = tour_cost(matrix, n, &greedy_order, round_trip);
        let (opt_order, opt_cost) = two_opt_improve(matrix, n, greedy_order, greedy_cost, round_trip);
        best_order = opt_order;
        best_cost = opt_cost;
        best_greedy_cost = greedy_cost;
    }

    let improvement_pct = if best_greedy_cost > 0 && best_greedy_cost != u64::MAX {
        (1.0 - best_cost as f64 / best_greedy_cost as f64) * 100.0
    } else {
        0.0
    };

    TspSolution {
        order: best_order,
        total_cost: best_cost,
        improvement_pct,
    }
}

/// Look up cost from waypoint i to waypoint j in the flat matrix.
/// Returns u64::MAX if the raw value is u32::MAX (unreachable).
#[inline]
fn cost(matrix: &[u32], n: usize, i: usize, j: usize) -> u64 {
    let v = matrix[i * n + j];
    if v == u32::MAX {
        u64::MAX / 2 // Use a large but non-overflowing sentinel
    } else {
        v as u64
    }
}

/// Nearest-neighbor greedy heuristic. Start from the given waypoint,
/// always visit the nearest unvisited waypoint.
fn nearest_neighbor_greedy(matrix: &[u32], n: usize, start: usize) -> Vec<usize> {
    let mut visited = vec![false; n];
    let mut order = Vec::with_capacity(n);

    let mut current = start;
    visited[current] = true;
    order.push(current);

    for _ in 1..n {
        let mut best_next = usize::MAX;
        let mut best_cost = u64::MAX;

        for j in 0..n {
            if !visited[j] {
                let c = cost(matrix, n, current, j);
                if c < best_cost {
                    best_cost = c;
                    best_next = j;
                }
            }
        }

        if best_next == usize::MAX {
            // All remaining nodes are unreachable from current.
            // Add them in order as a fallback.
            for j in 0..n {
                if !visited[j] {
                    visited[j] = true;
                    order.push(j);
                }
            }
            break;
        }

        visited[best_next] = true;
        order.push(best_next);
        current = best_next;
    }

    order
}

/// Calculate the total cost of a tour.
/// If `round_trip`, includes the cost from the last waypoint back to the first.
fn tour_cost(matrix: &[u32], n: usize, order: &[usize], round_trip: bool) -> u64 {
    if order.len() <= 1 {
        return 0;
    }

    let mut total: u64 = 0;
    for w in order.windows(2) {
        let c = cost(matrix, n, w[0], w[1]);
        total = total.saturating_add(c);
    }

    if round_trip {
        let c = cost(matrix, n, *order.last().unwrap(), order[0]);
        total = total.saturating_add(c);
    }

    total
}

/// Combined 2-opt + Or-opt local improvement.
///
/// Phase 1 (2-opt): Repeatedly try reversing segments of the tour.
/// Phase 2 (Or-opt / node relocation): Try removing each node and reinserting
/// it at the best position. This handles asymmetric cases where segment
/// reversal cannot improve the tour.
///
/// For an asymmetric matrix (directed graph), we must re-evaluate the full
/// tour cost after each move, since cost(A->B) != cost(B->A) in general.
fn two_opt_improve(
    matrix: &[u32],
    n: usize,
    mut order: Vec<usize>,
    mut current_cost: u64,
    round_trip: bool,
) -> (Vec<usize>, u64) {
    let len = order.len();
    if len <= 3 {
        // For 3 nodes, try all permutations (only 6 total, 2 with fixed start)
        if len == 3 {
            return brute_force_3(matrix, n, order, current_cost, round_trip);
        }
        return (order, current_cost);
    }

    // Cap outer iterations to avoid pathological cases
    let max_iterations = 100;
    let mut iteration = 0;

    loop {
        let mut improved = false;
        iteration += 1;

        // --- 2-opt: try reversing segments ---
        for i in 0..len - 1 {
            for j in i + 2..len {
                // Skip reversing entire tour in round_trip (same cycle backwards)
                if round_trip && i == 0 && j == len - 1 {
                    continue;
                }

                // Try reversing the segment order[i+1..=j]
                order[i + 1..=j].reverse();
                let new_cost = tour_cost(matrix, n, &order, round_trip);

                if new_cost < current_cost {
                    current_cost = new_cost;
                    improved = true;
                } else {
                    // Revert
                    order[i + 1..=j].reverse();
                }
            }
        }

        // --- Or-opt: try relocating each node to a better position ---
        // For open paths, skip relocating order[0] (fixed start).
        let relocate_start = if round_trip { 0 } else { 1 };

        for remove_pos in relocate_start..len {
            let node = order[remove_pos];

            // Remove node from current position
            let mut candidate = Vec::with_capacity(len);
            for (idx, &v) in order.iter().enumerate() {
                if idx != remove_pos {
                    candidate.push(v);
                }
            }

            // Try inserting at every valid position
            let insert_start = if round_trip { 0 } else { 1 };
            for insert_pos in insert_start..candidate.len() + 1 {
                // Skip if this would reconstruct the original order
                if insert_pos == remove_pos {
                    continue;
                }

                let mut trial = Vec::with_capacity(len);
                for &v in candidate.iter().take(insert_pos) {
                    trial.push(v);
                }
                trial.push(node);
                for &v in candidate.iter().skip(insert_pos) {
                    trial.push(v);
                }

                let trial_cost = tour_cost(matrix, n, &trial, round_trip);
                if trial_cost < current_cost {
                    order = trial;
                    current_cost = trial_cost;
                    improved = true;
                    break; // restart Or-opt from this new order
                }
            }

            // If we improved, break out to restart the outer loop
            if improved {
                break;
            }
        }

        if !improved || iteration >= max_iterations {
            break;
        }
    }

    (order, current_cost)
}

/// For exactly 3 nodes, try both orderings with fixed start at order[0].
fn brute_force_3(
    matrix: &[u32],
    n: usize,
    order: Vec<usize>,
    current_cost: u64,
    round_trip: bool,
) -> (Vec<usize>, u64) {
    let a = order[0];
    let b = order[1];
    let c = order[2];

    // Two possible orderings with fixed start: [a,b,c] and [a,c,b]
    let alt_order = vec![a, c, b];
    let alt_cost = tour_cost(matrix, n, &alt_order, round_trip);

    if alt_cost < current_cost {
        (alt_order, alt_cost)
    } else {
        (order, current_cost)
    }
}

// ============ Trip Handler ============

/// Request for trip/TSP optimization
#[derive(Debug, Deserialize)]
pub struct TripRequest {
    /// Waypoint coordinates [[lon, lat], ...] - 2 to 100 waypoints
    pub coordinates: Vec<[f64; 2]>,
    /// Transport mode: "car", "bike", or "foot"
    #[serde(default = "default_mode")]
    pub mode: String,
    /// Whether to return to start (default: true)
    #[serde(default = "default_true")]
    pub round_trip: bool,
    /// Annotations to return: "duration" (default), "distance", "duration,distance"
    #[serde(default = "default_annotations")]
    pub annotations: String,
}

fn default_mode() -> String {
    "car".to_string()
}

fn default_true() -> bool {
    true
}

fn default_annotations() -> String {
    "duration".to_string()
}

/// Response for trip endpoint
#[derive(Debug, Serialize)]
pub struct TripResponse {
    /// Status code
    pub code: String,
    /// Original waypoints with their position in the optimized trip
    pub waypoints: Vec<TripWaypoint>,
    /// The optimized trip(s). Currently always a single trip.
    pub trips: Vec<Trip>,
}

/// A waypoint in the trip response
#[derive(Debug, Serialize)]
pub struct TripWaypoint {
    /// Snapped location [lon, lat]
    pub location: [f64; 2],
    /// This waypoint's position in the optimized visit order
    pub waypoint_index: usize,
    /// Index into the trips array (always 0 for single trip)
    pub trips_index: usize,
    /// Road name (empty for now)
    pub name: String,
}

/// A complete optimized trip
#[derive(Debug, Serialize)]
pub struct Trip {
    /// Legs connecting consecutive waypoints in the optimized order
    pub legs: Vec<TripLeg>,
    /// Total trip duration in seconds
    pub duration: f64,
    /// Total trip distance in meters (if distance annotation requested, else 0)
    pub distance: f64,
    /// Optimization weight (same as duration)
    pub weight: f64,
    /// Weight metric name
    pub weight_name: String,
    /// Percentage improvement from 2-opt over greedy
    pub improvement_pct: f64,
    // NOTE: Route geometry concatenation across legs is a follow-up.
    // For now, individual leg geometries are not included.
    // To add geometry, compute P2P route for each consecutive pair in
    // the optimized order and concatenate the polylines.
}

/// A leg connecting two consecutive waypoints in the trip
#[derive(Debug, Serialize)]
pub struct TripLeg {
    /// Leg duration in seconds
    pub duration: f64,
    /// Leg distance in meters
    pub distance: f64,
    /// Summary (empty for now)
    pub summary: String,
}

/// Handler for POST /trip endpoint
pub async fn trip_handler(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<TripRequest>,
) -> impl IntoResponse {
    // Validate mode
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response()
        }
    };

    // Validate waypoint count
    let n = req.coordinates.len();
    if n < 2 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "At least 2 waypoints required"
            })),
        )
            .into_response();
    }
    if n > 100 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "Maximum 100 waypoints supported"
            })),
        )
            .into_response();
    }

    // Parse annotations
    let annotations: Vec<&str> = req.annotations.split(',').map(|s| s.trim()).collect();
    let want_duration = annotations.contains(&"duration") || (!annotations.contains(&"distance"));
    let want_distance = annotations.contains(&"distance");

    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // Snap all coordinates and convert to rank space
    let mut ranks: Vec<u32> = Vec::with_capacity(n);
    let mut snapped_locations: Vec<[f64; 2]> = Vec::with_capacity(n);
    let mut valid: Vec<bool> = Vec::with_capacity(n);

    for &[lon, lat] in &req.coordinates {
        if let Some(orig_id) = state.spatial_index.snap(lon, lat, &mode_data.mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                let rank = mode_data.order.perm[filtered as usize];
                ranks.push(rank);
                valid.push(true);
                // Get snapped location
                let snapped = get_node_location(&state, orig_id);
                snapped_locations.push(snapped);
            } else {
                ranks.push(0);
                valid.push(false);
                snapped_locations.push([lon, lat]);
            }
        } else {
            ranks.push(0);
            valid.push(false);
            snapped_locations.push([lon, lat]);
        }
    }

    // Check that all waypoints snapped successfully
    for (i, &v) in valid.iter().enumerate() {
        if !v {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "code": "NoSegment",
                    "message": format!(
                        "Could not snap waypoint {} ([{}, {}]) to road network for mode '{}'",
                        i, req.coordinates[i][0], req.coordinates[i][1], req.mode
                    )
                })),
            )
                .into_response();
        }
    }

    // Compute N×N duration matrix (for TSP optimization, always on time)
    let (duration_matrix, _stats) = table_bucket_full_flat(
        n_nodes,
        &mode_data.up_adj_flat,
        &mode_data.down_rev_flat,
        &ranks,
        &ranks,
    );

    // Run TSP solver on the duration matrix
    let tsp_result = solve_tsp(&duration_matrix, n, req.round_trip);

    // Compute distance matrix if requested (for reporting leg distances)
    let distance_matrix = if want_distance {
        let (dist_mat, _) = table_bucket_full_flat(
            n_nodes,
            &mode_data.up_adj_flat_dist,
            &mode_data.down_rev_flat_dist,
            &ranks,
            &ranks,
        );
        Some(dist_mat)
    } else {
        None
    };

    // Build legs from the optimized order
    let order = &tsp_result.order;
    let mut legs: Vec<TripLeg> = Vec::with_capacity(if req.round_trip { order.len() } else { order.len() - 1 });
    let mut total_duration_ds: u64 = 0;
    let mut total_distance_mm: u64 = 0;

    let leg_count = if req.round_trip { order.len() } else { order.len() - 1 };

    for leg_idx in 0..leg_count {
        let from = order[leg_idx];
        let to = order[(leg_idx + 1) % order.len()];

        let dur_ds = duration_matrix[from * n + to];
        let dur_s = if dur_ds == u32::MAX {
            f64::NAN
        } else {
            total_duration_ds += dur_ds as u64;
            dur_ds as f64 / 10.0 // deciseconds -> seconds
        };

        let dist_m = if let Some(ref dm) = distance_matrix {
            let d = dm[from * n + to];
            if d == u32::MAX {
                f64::NAN
            } else {
                total_distance_mm += d as u64;
                d as f64 / 1000.0 // millimeters -> meters
            }
        } else {
            0.0
        };

        legs.push(TripLeg {
            duration: dur_s,
            distance: dist_m,
            summary: String::new(),
        });
    }

    // Build waypoint response: for each original waypoint, where does it appear in the trip?
    // Create a reverse mapping: original_index -> position_in_optimized_order
    let mut waypoint_index_map = vec![0usize; n];
    for (position, &original_idx) in order.iter().enumerate() {
        waypoint_index_map[original_idx] = position;
    }

    let waypoints: Vec<TripWaypoint> = (0..n)
        .map(|i| TripWaypoint {
            location: snapped_locations[i],
            waypoint_index: waypoint_index_map[i],
            trips_index: 0,
            name: String::new(),
        })
        .collect();

    let trip = Trip {
        legs,
        duration: total_duration_ds as f64 / 10.0,
        distance: total_distance_mm as f64 / 1000.0,
        weight: total_duration_ds as f64 / 10.0,
        weight_name: "duration".to_string(),
        improvement_pct: tsp_result.improvement_pct,
    };

    Json(TripResponse {
        code: "Ok".to_string(),
        waypoints,
        trips: vec![trip],
    })
    .into_response()
}

/// Parse mode string into Mode enum
fn parse_mode(s: &str) -> Result<Mode, String> {
    match s.to_lowercase().as_str() {
        "car" => Ok(Mode::Car),
        "bike" => Ok(Mode::Bike),
        "foot" => Ok(Mode::Foot),
        _ => Err(format!("Invalid mode: {}. Use car, bike, or foot.", s)),
    }
}

/// Get snapped location [lon, lat] for an original EBG node
fn get_node_location(state: &ServerState, node_id: u32) -> [f64; 2] {
    let node = &state.ebg_nodes.nodes[node_id as usize];
    let edge_idx = node.geom_idx as usize;
    if edge_idx < state.nbg_geo.polylines.len() {
        let polyline = &state.nbg_geo.polylines[edge_idx];
        if !polyline.lon_fxp.is_empty() {
            return [
                polyline.lon_fxp[0] as f64 / 1e7,
                polyline.lat_fxp[0] as f64 / 1e7,
            ];
        }
    }
    [0.0, 0.0]
}

// ============ Tests ============

#[cfg(test)]
mod tests {
    use super::*;

    /// Helper: build a flat N×N matrix from a 2D cost specification.
    /// Input is Vec<Vec<u32>> where outer = rows (from), inner = cols (to).
    fn make_matrix(costs: &[&[u32]]) -> Vec<u32> {
        let n = costs.len();
        let mut flat = vec![0u32; n * n];
        for i in 0..n {
            assert_eq!(costs[i].len(), n, "Matrix must be square");
            for j in 0..n {
                flat[i * n + j] = costs[i][j];
            }
        }
        flat
    }

    #[test]
    fn test_tsp_trivial_empty() {
        let matrix: Vec<u32> = vec![];
        let result = solve_tsp(&matrix, 0, true);
        assert!(result.order.is_empty());
        assert_eq!(result.total_cost, 0);
    }

    #[test]
    fn test_tsp_trivial_single() {
        let matrix = vec![0u32];
        let result = solve_tsp(&matrix, 1, true);
        assert_eq!(result.order, vec![0]);
        assert_eq!(result.total_cost, 0);
    }

    #[test]
    fn test_tsp_trivial_two() {
        // 2 waypoints: A->B = 10, B->A = 20
        let matrix = make_matrix(&[
            &[0, 10],
            &[20, 0],
        ]);

        // Round trip: 10 + 20 = 30
        let result_rt = solve_tsp(&matrix, 2, true);
        assert_eq!(result_rt.order, vec![0, 1]);
        assert_eq!(result_rt.total_cost, 30);

        // Open trip: just 0->1 = 10
        let result_open = solve_tsp(&matrix, 2, false);
        assert_eq!(result_open.order, vec![0, 1]);
        assert_eq!(result_open.total_cost, 10);
    }

    #[test]
    fn test_tsp_greedy_3() {
        // 3 waypoints arranged so greedy from 0 gives optimal
        //   0->1 = 5, 0->2 = 20
        //   1->0 = 5, 1->2 = 3
        //   2->0 = 20, 2->1 = 3
        //
        // Greedy from 0: go to 1 (cost 5), then 2 (cost 3) = 5+3 = 8
        // Alternative: 0->2->1 = 20+3 = 23  (worse)
        // Round trip: 8 + 20 = 28  (return 2->0)
        let matrix = make_matrix(&[
            &[0,  5, 20],
            &[5,  0,  3],
            &[20, 3,  0],
        ]);

        let result = solve_tsp(&matrix, 3, false);
        assert_eq!(result.order, vec![0, 1, 2]);
        assert_eq!(result.total_cost, 8);

        let result_rt = solve_tsp(&matrix, 3, true);
        assert_eq!(result_rt.order, vec![0, 1, 2]);
        assert_eq!(result_rt.total_cost, 28);
    }

    #[test]
    fn test_tsp_2opt_improvement() {
        // 4 waypoints where greedy gives a suboptimal order.
        // Layout: a "crossing" pattern where greedy picks a crossing tour
        // and 2-opt should uncross it.
        //
        // Greedy from 0: 0->1 (1) -> 1->3 (1) -> 3->2 (10) = 12
        // Optimal:       0->1 (1) -> 1->2 (2) -> 2->3 (1)   = 4
        //
        // Matrix (asymmetric):
        //      0    1    2    3
        //  0 [ 0,   1,  10,  10]
        //  1 [10,   0,   2,   1]
        //  2 [10,  10,   0,   1]
        //  3 [ 1,  10,  10,   0]
        let matrix = make_matrix(&[
            &[ 0,  1, 10, 10],
            &[10,  0,  2,  1],
            &[10, 10,  0,  1],
            &[ 1, 10, 10,  0],
        ]);

        let result = solve_tsp(&matrix, 4, false);
        // The optimal open path is 0->1->2->3 with cost 1+2+1 = 4
        let expected_cost = 4u64;
        assert_eq!(result.total_cost, expected_cost,
            "Expected cost {}, got {}. Order: {:?}", expected_cost, result.total_cost, result.order);
        // Verify order gives the expected cost
        assert_eq!(tour_cost(&matrix, 4, &result.order, false), expected_cost);
    }

    #[test]
    fn test_tsp_round_trip_vs_open() {
        // Verify that round_trip=true adds return-to-start cost
        // and that it can differ from open path cost.
        //
        // 3 waypoints with asymmetric costs:
        //   0->1 = 2, 1->2 = 3, 2->0 = 100
        //   (return is expensive)
        let matrix = make_matrix(&[
            &[  0,   2, 50],
            &[ 50,   0,  3],
            &[100, 50,  0],
        ]);

        let open = solve_tsp(&matrix, 3, false);
        let round = solve_tsp(&matrix, 3, true);

        // Open: 0->1->2 = 2+3 = 5
        assert_eq!(open.total_cost, 5, "Open cost: expected 5, got {}", open.total_cost);

        // Round: 0->1->2->0 = 2+3+100 = 105
        assert_eq!(round.total_cost, 105, "Round trip cost: expected 105, got {}", round.total_cost);

        // Round trip cost must be >= open cost
        assert!(round.total_cost >= open.total_cost);
    }

    #[test]
    fn test_tsp_unreachable() {
        // Some pairs are unreachable (u32::MAX).
        // The solver should still find the best feasible tour using reachable edges.
        //
        //      0    1    2
        //  0 [ 0,   5, MAX]
        //  1 [ 5,   0,   3]
        //  2 [MAX,  3,   0]
        //
        // Only feasible open path through all 3: 0->1->2 (cost 5+3 = 8)
        // Greedy from 0: go to 1 (5), then 2 (3) = 8
        let matrix = make_matrix(&[
            &[       0,        5, u32::MAX],
            &[       5,        0,        3],
            &[u32::MAX,        3,        0],
        ]);

        let result = solve_tsp(&matrix, 3, false);
        assert_eq!(result.order, vec![0, 1, 2]);
        assert_eq!(result.total_cost, 8);

        // Round trip: 0->1->2->0, but 2->0 is MAX.
        // The tour still must visit all nodes. Cost = 5 + 3 + sentinel.
        let result_rt = solve_tsp(&matrix, 3, true);
        assert_eq!(result_rt.order.len(), 3);
        // All waypoints visited
        let mut visited: Vec<usize> = result_rt.order.clone();
        visited.sort();
        assert_eq!(visited, vec![0, 1, 2]);
    }

    #[test]
    fn test_tsp_asymmetric() {
        // Verify the algorithm works correctly with asymmetric matrices
        // where d(A->B) != d(B->A).
        //
        // 4 cities with strongly asymmetric costs:
        //       0    1    2    3
        //  0 [  0,  10,  50,   1]
        //  1 [100,   0,   1,  50]
        //  2 [ 50, 100,   0,   1]
        //  3 [  1,  50, 100,   0]
        //
        // Optimal open: 0->3->... or 0->1->2->3 = 10+1+1 = 12
        let matrix = make_matrix(&[
            &[  0,  10,  50,   1],
            &[100,   0,   1,  50],
            &[ 50, 100,   0,   1],
            &[  1,  50, 100,   0],
        ]);

        let result = solve_tsp(&matrix, 4, false);

        // Verify all nodes visited exactly once
        let mut visited: Vec<usize> = result.order.clone();
        visited.sort();
        assert_eq!(visited, vec![0, 1, 2, 3]);

        // The optimal open tour is 0->1->2->3 = 10+1+1 = 12
        assert_eq!(result.total_cost, 12,
            "Expected optimal cost 12 for asymmetric case, got {}. Order: {:?}",
            result.total_cost, result.order);

        // Verify the cost matches what tour_cost computes
        assert_eq!(tour_cost(&matrix, 4, &result.order, false), result.total_cost);
    }

    #[test]
    fn test_tsp_all_unreachable_except_sequential() {
        // Only sequential transitions are reachable: 0->1, 1->2, 2->3
        // Everything else is MAX.
        let max = u32::MAX;
        let matrix = make_matrix(&[
            &[  0,   5, max, max],
            &[max,   0,   3, max],
            &[max, max,   0,   7],
            &[max, max, max,   0],
        ]);

        let result = solve_tsp(&matrix, 4, false);
        assert_eq!(result.order, vec![0, 1, 2, 3]);
        assert_eq!(result.total_cost, 15); // 5 + 3 + 7
    }

    #[test]
    fn test_tsp_2opt_uncrosses() {
        // Classic 2-opt test: 4 points in a square where greedy creates
        // a crossing path and 2-opt should uncross it.
        //
        // Points at corners of a square:
        //   0=(0,0)  1=(10,0)  2=(10,10)  3=(0,10)
        //
        // Symmetric distances (Euclidean-like):
        // d(0,1) = d(1,0) = 10  (side)
        // d(1,2) = d(2,1) = 10  (side)
        // d(2,3) = d(3,2) = 10  (side)
        // d(3,0) = d(0,3) = 10  (side)
        // d(0,2) = d(2,0) = 14  (diagonal)
        // d(1,3) = d(3,1) = 14  (diagonal)
        //
        // Greedy from 0: 0->1 (10), 1->2 (10), 2->3 (10). Cost open = 30.
        // This is already optimal for a square (perimeter traversal).
        // Round trip: 30 + 10 = 40 (all sides).
        //
        // A crossing tour: 0->2->1->3 = 14+10+14 = 38 open, +10 = 48 round trip
        // 2-opt should prefer the non-crossing tour.
        let matrix = make_matrix(&[
            &[ 0, 10, 14, 10],
            &[10,  0, 10, 14],
            &[14, 10,  0, 10],
            &[10, 14, 10,  0],
        ]);

        let result_rt = solve_tsp(&matrix, 4, true);
        // Optimal round trip around the square is 40
        assert_eq!(result_rt.total_cost, 40,
            "Expected round trip cost 40, got {}. Order: {:?}",
            result_rt.total_cost, result_rt.order);
    }

    #[test]
    fn test_tour_cost_function() {
        let matrix = make_matrix(&[
            &[0,  5, 10],
            &[5,  0,  3],
            &[10, 3,  0],
        ]);

        // Open: 0->1->2 = 5+3 = 8
        assert_eq!(tour_cost(&matrix, 3, &[0, 1, 2], false), 8);
        // Round: 0->1->2->0 = 5+3+10 = 18
        assert_eq!(tour_cost(&matrix, 3, &[0, 1, 2], true), 18);
        // Different order open: 0->2->1 = 10+3 = 13
        assert_eq!(tour_cost(&matrix, 3, &[0, 2, 1], false), 13);
    }
}
