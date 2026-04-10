//! Debug compare endpoint (unmounted in production)

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    Json,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;

use super::query::CchQuery;
use super::state::ServerState;
use super::types::{parse_mode, ErrorResponse};

// ============ Types ============

#[derive(Debug, Deserialize)]
#[allow(dead_code)]
pub struct DebugCompareRequest {
    src_lon: f64,
    src_lat: f64,
    dst_lon: f64,
    dst_lat: f64,
    mode: String,
}

#[derive(Debug, Serialize)]
#[allow(dead_code)]
pub struct DebugCompareResponse {
    cch_distance: Option<u32>,
    dijkstra_distance: Option<u32>,
    cch_meeting_rank: Option<u32>,
    src_rank: u32,
    dst_rank: u32,
    src_filtered: u32,
    dst_filtered: u32,
    cch_fwd_settled: usize,
    cch_bwd_settled: usize,
    dijkstra_settled: usize,
}

// ============ Handler ============

/// Debug endpoint comparing CCH query with plain Dijkstra on filtered EBG
/// Not mounted in production router; retained for development use.
#[allow(dead_code)]
pub async fn debug_compare(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<DebugCompareRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let mode_data = state.get_mode(mode);

    // Snap source and destination
    let src_orig = match state
        .spatial_index
        .snap(req.src_lon, req.src_lat, &mode_data.mask, 10)
    {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Cannot snap source".to_string(),
                }),
            )
                .into_response()
        }
    };
    let dst_orig = match state
        .spatial_index
        .snap(req.dst_lon, req.dst_lat, &mode_data.mask, 10)
    {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Cannot snap dest".to_string(),
                }),
            )
                .into_response()
        }
    };

    // Convert to filtered space
    let src_filtered = mode_data.filtered_ebg.original_to_filtered[src_orig as usize];
    let dst_filtered = mode_data.filtered_ebg.original_to_filtered[dst_orig as usize];

    if src_filtered == u32::MAX || dst_filtered == u32::MAX {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Node not accessible".to_string(),
            }),
        )
            .into_response();
    }

    let _n = mode_data.cch_topo.n_nodes as usize;
    let perm = &mode_data.order.perm;

    // Get ranks
    let src_rank = perm[src_filtered as usize];
    let dst_rank = perm[dst_filtered as usize];

    // ========== Validate down_rev structure ==========
    eprintln!("\nValidating down_rev structure...");
    match super::query::validate_down_rev(&mode_data.cch_topo, &mode_data.down_rev, perm) {
        Ok(()) => eprintln!("  down_rev validation passed"),
        Err(e) => eprintln!("  down_rev validation FAILED: {}", e),
    }

    // ========== Run CCH Query ==========
    eprintln!("\nCCH BIDIR QUERY DEBUG:");
    let query = CchQuery::new(&state, mode);
    let cch_result = query.query_with_debug(src_rank, dst_rank, true);
    let cch_distance = cch_result.as_ref().map(|r| r.distance);
    let cch_meeting_rank = cch_result.as_ref().map(|r| perm[r.meeting_node as usize]);
    let cch_fwd_settled = 0usize; // We'd need to modify query to track this
    let cch_bwd_settled = 0usize;

    // ========== Run Plain Dijkstra on Filtered EBG ==========
    // This uses ALL edges in the filtered EBG with proper weights
    let dijkstra_result = run_filtered_dijkstra(
        &mode_data.filtered_ebg,
        &mode_data.node_weights,
        src_filtered,
        dst_filtered,
    );
    let dijkstra_distance = dijkstra_result.0;
    let dijkstra_settled = dijkstra_result.1;

    // ========== Run Plain Dijkstra on CCH UP+DOWN graphs ==========
    // If this gives a better result than CCH query, the query algorithm is wrong
    let (cch_dijkstra_distance, cch_dijkstra_settled, cch_path) = run_cch_dijkstra_with_path(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        perm,
        src_filtered,
        dst_filtered,
    );

    // Check edge counts from source
    let src_up_start = mode_data.cch_topo.up_offsets[src_filtered as usize] as usize;
    let src_up_end = mode_data.cch_topo.up_offsets[src_filtered as usize + 1] as usize;
    let src_down_start = mode_data.cch_topo.down_offsets[src_filtered as usize] as usize;
    let src_down_end = mode_data.cch_topo.down_offsets[src_filtered as usize + 1] as usize;

    let tgt_up_incoming = count_incoming_up_edges(&mode_data.cch_topo, dst_filtered);
    let tgt_down_incoming = mode_data.down_rev.offsets[dst_filtered as usize + 1]
        - mode_data.down_rev.offsets[dst_filtered as usize];

    eprintln!("DEBUG COMPARE:");
    eprintln!(
        "  Source {} (rank {}): {} UP edges, {} DOWN edges",
        src_filtered,
        src_rank,
        src_up_end - src_up_start,
        src_down_end - src_down_start
    );
    eprintln!(
        "  Target {} (rank {}): {} incoming UP edges, {} incoming DOWN edges",
        dst_filtered, dst_rank, tgt_up_incoming, tgt_down_incoming
    );
    eprintln!("  CCH query: {:?}", cch_distance);
    eprintln!("  CCH Dijkstra (UP+DOWN): {:?}", cch_dijkstra_distance);
    eprintln!("  Plain Dijkstra (filtered EBG): {:?}", dijkstra_distance);

    // Verify down_rev structure: sample entries from target
    eprintln!("\n  Down_rev entries for target {}:", dst_filtered);
    let tgt_rev_start = mode_data.down_rev.offsets[dst_filtered as usize] as usize;
    let tgt_rev_end = mode_data.down_rev.offsets[dst_filtered as usize + 1] as usize;
    for i in tgt_rev_start..tgt_rev_end.min(tgt_rev_start + 5) {
        let src_node = mode_data.down_rev.sources[i];
        let edge_idx = mode_data.down_rev.edge_idx[i] as usize;
        let src_rank = perm[src_node as usize];
        let weight = mode_data.cch_weights.down[edge_idx];
        eprintln!(
            "    {} (rank {}) -> {} with weight {} (edge_idx {})",
            src_node, src_rank, dst_filtered, weight, edge_idx
        );
    }

    // Verify by looking at down edges TO target directly
    eprintln!("\n  Direct DOWN edges to target {}:", dst_filtered);
    let mut found = 0;
    for src_node in 0..mode_data.cch_topo.n_nodes {
        let start = mode_data.cch_topo.down_offsets[src_node as usize] as usize;
        let end = mode_data.cch_topo.down_offsets[src_node as usize + 1] as usize;
        for i in start..end {
            if mode_data.cch_topo.down_targets[i] == dst_filtered {
                let weight = mode_data.cch_weights.down[i];
                eprintln!(
                    "    {} (rank {}) -> {} with weight {} (edge_idx {})",
                    src_node, perm[src_node as usize], dst_filtered, weight, i
                );
                found += 1;
                if found >= 5 {
                    break;
                }
            }
        }
        if found >= 5 {
            break;
        }
    }

    // Run separate UP-only and DOWN-only searches to verify
    eprintln!("\n  Running separate UP-only Dijkstra from source...");
    let up_only_dist =
        run_up_only_dijkstra(&mode_data.cch_topo, &mode_data.cch_weights, src_filtered);
    let fwd_reachable = up_only_dist.iter().filter(|&&d| d != u32::MAX).count();
    eprintln!("    Reachable nodes via UP-only: {}", fwd_reachable);
    eprintln!(
        "    dist_up[target={}] = {:?}",
        dst_filtered,
        if up_only_dist[dst_filtered as usize] == u32::MAX {
            "UNREACHABLE".to_string()
        } else {
            up_only_dist[dst_filtered as usize].to_string()
        }
    );

    eprintln!("\n  Running separate DOWN-only Dijkstra to target...");
    let down_only_dist = run_down_only_to_target(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &mode_data.down_rev,
        dst_filtered,
    );
    let bwd_reachable = down_only_dist.iter().filter(|&&d| d != u32::MAX).count();
    eprintln!(
        "    Reachable nodes via DOWN-only to target: {}",
        bwd_reachable
    );
    eprintln!(
        "    dist_down[source={}] = {:?}",
        src_filtered,
        if down_only_dist[src_filtered as usize] == u32::MAX {
            "UNREACHABLE".to_string()
        } else {
            down_only_dist[src_filtered as usize].to_string()
        }
    );

    // Find best meeting point manually
    let mut best_meet = u32::MAX;
    let mut best_meet_node = u32::MAX;
    for node in 0..mode_data.cch_topo.n_nodes {
        let d_up = up_only_dist[node as usize];
        let d_down = down_only_dist[node as usize];
        if d_up != u32::MAX && d_down != u32::MAX {
            let total = d_up.saturating_add(d_down);
            if total < best_meet {
                best_meet = total;
                best_meet_node = node;
            }
        }
    }
    eprintln!(
        "    Best meeting: node {} with dist_up={} + dist_down={} = {}",
        best_meet_node,
        up_only_dist
            .get(best_meet_node as usize)
            .unwrap_or(&u32::MAX),
        down_only_dist
            .get(best_meet_node as usize)
            .unwrap_or(&u32::MAX),
        best_meet
    );

    // Analyze the CCH Dijkstra path and verify edge weights
    if let Some(reported) = cch_dijkstra_distance.filter(|_| !cch_path.is_empty()) {
        eprintln!("\n  CCH Dijkstra path analysis ({} nodes):", cch_path.len());

        // Verify path weights sum to distance
        let mut weight_sum = 0u32;
        let mut edge_details: Vec<String> = Vec::new();
        for i in 0..cch_path.len() - 1 {
            let u = cch_path[i] as usize;
            let v = cch_path[i + 1];
            let rank_u = perm[u];
            let rank_v = perm[v as usize];

            // Find the edge u->v in UP or DOWN graph
            let (edge_type, weight) = if rank_u < rank_v {
                // Should be in UP graph
                let start = mode_data.cch_topo.up_offsets[u] as usize;
                let end = mode_data.cch_topo.up_offsets[u + 1] as usize;
                let mut found_weight = None;
                for idx in start..end {
                    if mode_data.cch_topo.up_targets[idx] == v {
                        found_weight = Some(mode_data.cch_weights.up[idx]);
                        break;
                    }
                }
                ("UP", found_weight)
            } else {
                // Should be in DOWN graph
                let start = mode_data.cch_topo.down_offsets[u] as usize;
                let end = mode_data.cch_topo.down_offsets[u + 1] as usize;
                let mut found_weight = None;
                for idx in start..end {
                    if mode_data.cch_topo.down_targets[idx] == v {
                        found_weight = Some(mode_data.cch_weights.down[idx]);
                        break;
                    }
                }
                ("DOWN", found_weight)
            };

            match weight {
                Some(w) if w != u32::MAX => {
                    weight_sum = weight_sum.saturating_add(w);
                    if edge_details.len() < 5 {
                        edge_details.push(format!("  {}->{} ({}, w={})", u, v, edge_type, w));
                    }
                }
                Some(_) => {
                    edge_details.push(format!("  {}->{} ({}, w=MAX - BLOCKED!)", u, v, edge_type));
                }
                None => {
                    edge_details.push(format!("  {}->{} ({}, NOT FOUND!)", u, v, edge_type));
                }
            }
        }
        eprintln!("    Reported distance: {}", reported);
        eprintln!("    Sum of edge weights: {}", weight_sum);
        if weight_sum != reported {
            eprintln!(
                "    WEIGHT MISMATCH! Diff = {}",
                weight_sum as i64 - reported as i64
            );
        } else {
            eprintln!("    Weights match");
        }

        // Show first few edges
        if !edge_details.is_empty() {
            eprintln!("    First edges:");
            for detail in &edge_details {
                eprintln!("    {}", detail);
            }
        }

        // Count transitions
        let mut peaks = 0;
        let mut valleys = 0;
        let mut prev_rank = perm[cch_path[0] as usize];
        let mut going_up = true;
        for i in 1..cch_path.len() {
            let curr_rank = perm[cch_path[i] as usize];
            if going_up && curr_rank < prev_rank {
                peaks += 1;
                going_up = false;
            } else if !going_up && curr_rank > prev_rank {
                valleys += 1;
                going_up = true;
            } else if curr_rank > prev_rank {
                going_up = true;
            } else if curr_rank < prev_rank {
                going_up = false;
            }
            prev_rank = curr_rank;
        }
        eprintln!(
            "    Peaks (up->down): {}, Valleys (down->up): {}",
            peaks, valleys
        );

        // For each valley, check if there's an UP shortcut that bypasses it
        if valleys > 0 {
            eprintln!("\n    Checking shortcuts at valleys:");
            prev_rank = perm[cch_path[0] as usize];
            let mut _going_up2 = true;
            let mut valley_count = 0;
            for i in 1..cch_path.len().saturating_sub(1) {
                let curr_rank = perm[cch_path[i] as usize];
                let next_rank = perm[cch_path[i + 1] as usize];

                let was_going_down = prev_rank > curr_rank;
                let now_going_up = curr_rank < next_rank;

                if was_going_down && now_going_up && valley_count < 3 {
                    valley_count += 1;
                    let prev_node = cch_path[i - 1] as usize;
                    let curr_node = cch_path[i] as usize;
                    let next_node = cch_path[i + 1] as usize;

                    let down_edge_weight = find_edge_weight(
                        &mode_data.cch_topo,
                        &mode_data.cch_weights,
                        prev_node,
                        curr_node as u32,
                        perm,
                    );
                    let up_edge_weight = find_edge_weight(
                        &mode_data.cch_topo,
                        &mode_data.cch_weights,
                        curr_node,
                        next_node as u32,
                        perm,
                    );
                    let valley_cost = down_edge_weight
                        .unwrap_or(u32::MAX)
                        .saturating_add(up_edge_weight.unwrap_or(u32::MAX));

                    let direct_up = if perm[prev_node] < perm[next_node] {
                        let start = mode_data.cch_topo.up_offsets[prev_node] as usize;
                        let end = mode_data.cch_topo.up_offsets[prev_node + 1] as usize;
                        let mut found_val = None;
                        for idx in start..end {
                            if mode_data.cch_topo.up_targets[idx] == next_node as u32 {
                                found_val = Some((
                                    mode_data.cch_weights.up[idx],
                                    mode_data.cch_topo.up_is_shortcut[idx],
                                ));
                                break;
                            }
                        }
                        found_val
                    } else {
                        None
                    };

                    eprintln!(
                        "      Valley {}: {} (rank {}) -> {} (rank {}) -> {} (rank {})",
                        valley_count,
                        prev_node,
                        perm[prev_node],
                        curr_node,
                        curr_rank,
                        next_node,
                        perm[next_node]
                    );
                    eprintln!(
                        "        Through valley: {} + {} = {}",
                        down_edge_weight.unwrap_or(0),
                        up_edge_weight.unwrap_or(0),
                        valley_cost
                    );
                    match direct_up {
                        Some((w, is_shortcut)) => {
                            eprintln!("        Direct UP edge: w={}, shortcut={}", w, is_shortcut);
                            if w <= valley_cost {
                                eprintln!("        Shortcut is cheaper or equal - should be used!");
                            } else {
                                eprintln!(
                                    "        Shortcut is more expensive (diff={})",
                                    w as i64 - valley_cost as i64
                                );
                                if is_shortcut {
                                    let start = mode_data.cch_topo.up_offsets[prev_node] as usize;
                                    let end = mode_data.cch_topo.up_offsets[prev_node + 1] as usize;
                                    for idx in start..end {
                                        if mode_data.cch_topo.up_targets[idx] == next_node as u32 {
                                            let middle = mode_data.cch_topo.up_middle[idx];
                                            let middle_rank = perm[middle as usize];
                                            eprintln!(
                                                "        Shortcut middle: {} (rank {})",
                                                middle, middle_rank
                                            );
                                            eprintln!(
                                                "        Valley middle:   {} (rank {})",
                                                curr_node, curr_rank
                                            );
                                            let w_um = find_edge_weight(
                                                &mode_data.cch_topo,
                                                &mode_data.cch_weights,
                                                prev_node,
                                                middle,
                                                perm,
                                            );
                                            let w_mv = find_edge_weight(
                                                &mode_data.cch_topo,
                                                &mode_data.cch_weights,
                                                middle as usize,
                                                next_node as u32,
                                                perm,
                                            );
                                            eprintln!("        Shortcut path: w({}->{}){:?} + w({}->{}){:?}",
                                                      prev_node, middle, w_um, middle, next_node, w_mv);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        None => eprintln!("        No direct UP edge exists"),
                    }
                }

                if curr_rank > prev_rank {
                    _going_up2 = true;
                } else if curr_rank < prev_rank {
                    _going_up2 = false;
                }
                prev_rank = curr_rank;
            }
        }
    }

    Json(serde_json::json!({
        "cch_distance": cch_distance,
        "cch_dijkstra_distance": cch_dijkstra_distance,
        "dijkstra_distance": dijkstra_distance,
        "cch_meeting_rank": cch_meeting_rank,
        "src_rank": src_rank,
        "dst_rank": dst_rank,
        "src_filtered": src_filtered,
        "dst_filtered": dst_filtered,
        "cch_fwd_settled": cch_fwd_settled,
        "cch_bwd_settled": cch_bwd_settled,
        "cch_dijkstra_settled": cch_dijkstra_settled,
        "dijkstra_settled": dijkstra_settled,
    }))
    .into_response()
}

// ============ Debug Helper Functions ============

/// Count incoming UP edges to a node (edges v -> u where rank(v) < rank(u))
#[allow(dead_code)]
fn count_incoming_up_edges(topo: &crate::formats::CchTopo, u: u32) -> usize {
    let n = topo.n_nodes as usize;
    let mut count = 0;
    for v in 0..n {
        let start = topo.up_offsets[v] as usize;
        let end = topo.up_offsets[v + 1] as usize;
        for i in start..end {
            if topo.up_targets[i] == u {
                count += 1;
            }
        }
    }
    count
}

/// Find edge weight in CCH graph
#[allow(dead_code)]
fn find_edge_weight(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    from: usize,
    to: u32,
    perm: &[u32],
) -> Option<u32> {
    let rank_from = perm[from];
    let rank_to = perm[to as usize];

    if rank_from < rank_to {
        let start = topo.up_offsets[from] as usize;
        let end = topo.up_offsets[from + 1] as usize;
        for idx in start..end {
            if topo.up_targets[idx] == to {
                return Some(weights.up[idx]);
            }
        }
    } else {
        let start = topo.down_offsets[from] as usize;
        let end = topo.down_offsets[from + 1] as usize;
        for idx in start..end {
            if topo.down_targets[idx] == to {
                return Some(weights.down[idx]);
            }
        }
    }
    None
}

/// Run Dijkstra on CCH UP+DOWN and return the path
#[allow(dead_code)]
fn run_cch_dijkstra_with_path(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    _perm: &[u32],
    src: u32,
    dst: u32,
) -> (Option<u32>, usize, Vec<u32>) {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut parent = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
    let mut settled = 0usize;

    dist[src as usize] = 0;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        settled += 1;

        if u == dst {
            let mut path = Vec::new();
            let mut curr = dst;
            while curr != u32::MAX {
                path.push(curr);
                curr = parent[curr as usize];
            }
            path.reverse();
            return (Some(d), settled, path);
        }

        let up_start = topo.up_offsets[u as usize] as usize;
        let up_end = topo.up_offsets[u as usize + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i];
            let w = weights.up[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                parent[v as usize] = u;
                pq.push(v, Reverse(new_dist));
            }
        }

        let down_start = topo.down_offsets[u as usize] as usize;
        let down_end = topo.down_offsets[u as usize + 1] as usize;
        for i in down_start..down_end {
            let v = topo.down_targets[i];
            let w = weights.down[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                parent[v as usize] = u;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    let path = if dist[dst as usize] != u32::MAX {
        let mut p = Vec::new();
        let mut curr = dst;
        while curr != u32::MAX {
            p.push(curr);
            curr = parent[curr as usize];
        }
        p.reverse();
        p
    } else {
        vec![]
    };

    (
        if dist[dst as usize] == u32::MAX {
            None
        } else {
            Some(dist[dst as usize])
        },
        settled,
        path,
    )
}

/// Run Dijkstra using only UP edges from source
#[allow(dead_code)]
fn run_up_only_dijkstra(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    src: u32,
) -> Vec<u32> {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();

    dist[src as usize] = 0;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        let up_start = topo.up_offsets[u as usize] as usize;
        let up_end = topo.up_offsets[u as usize + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i];
            let w = weights.up[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    dist
}

/// Run reverse Dijkstra using only DOWN edges to reach target
/// Returns dist[node] = shortest DOWN-only path from node to target
#[allow(dead_code)]
fn run_down_only_to_target(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    down_rev: &super::state::DownReverseAdj,
    dst: u32,
) -> Vec<u32> {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();

    dist[dst as usize] = 0;
    pq.push(dst, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        let start = down_rev.offsets[u as usize] as usize;
        let end = down_rev.offsets[u as usize + 1] as usize;
        for i in start..end {
            let x = down_rev.sources[i];
            let edge_idx = down_rev.edge_idx[i] as usize;
            let w = weights.down[edge_idx];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[x as usize] {
                dist[x as usize] = new_dist;
                pq.push(x, Reverse(new_dist));
            }
        }
    }

    dist
}

/// Run plain Dijkstra on filtered EBG (without CCH, using node weights only - no turn costs)
#[allow(dead_code)]
fn run_filtered_dijkstra(
    filtered_ebg: &crate::formats::FilteredEbg,
    node_weights: &[u32],
    src: u32,
    dst: u32,
) -> (Option<u32>, usize) {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = filtered_ebg.n_filtered_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
    let mut settled = 0usize;

    dist[src as usize] = 0;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        settled += 1;

        if u == dst {
            return (Some(d), settled);
        }

        let start = filtered_ebg.offsets[u as usize] as usize;
        let end = filtered_ebg.offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = filtered_ebg.heads[i];
            let v_orig = filtered_ebg.filtered_to_original[v as usize];
            let w = node_weights[v_orig as usize];

            if w == 0 {
                continue;
            }

            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    (
        if dist[dst as usize] == u32::MAX {
            None
        } else {
            Some(dist[dst as usize])
        },
        settled,
    )
}
