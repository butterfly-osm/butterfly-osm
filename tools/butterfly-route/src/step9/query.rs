//! CCH query algorithm - bidirectional Dijkstra on hierarchy

use priority_queue::PriorityQueue;
use std::cmp::Reverse;

use crate::formats::CchTopo;
use crate::profile_abi::Mode;

use super::state::{CchWeights, DownReverseAdj, ServerState};

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub distance: u32,        // Total distance in deciseconds
    pub meeting_node: u32,    // Node where forward/backward meet
    pub forward_parent: Vec<(u32, u32)>,  // (node, edge_idx) pairs for path
    pub backward_parent: Vec<(u32, u32)>, // (node, edge_idx) pairs for path
}

/// Bidirectional CCH query
pub struct CchQuery<'a> {
    topo: &'a CchTopo,
    down_rev: &'a DownReverseAdj,
    weights: &'a CchWeights,
    n_nodes: usize,
}

impl<'a> CchQuery<'a> {
    pub fn new(state: &'a ServerState, mode: Mode) -> Self {
        let mode_data = state.get_mode(mode);
        Self {
            topo: &mode_data.cch_topo,
            down_rev: &mode_data.down_rev,
            weights: &mode_data.cch_weights,
            n_nodes: mode_data.cch_topo.n_nodes as usize,
        }
    }

    /// Run bidirectional query from source to target
    pub fn query(&self, source: u32, target: u32) -> Option<QueryResult> {
        if source == target {
            return Some(QueryResult {
                distance: 0,
                meeting_node: source,
                forward_parent: vec![],
                backward_parent: vec![],
            });
        }

        let n = self.n_nodes;

        // Debug: check source/target connectivity
        let src_up_start = self.topo.up_offsets[source as usize] as usize;
        let src_up_end = self.topo.up_offsets[source as usize + 1] as usize;
        let src_up_count = src_up_end - src_up_start;
        let src_up_reachable = (src_up_start..src_up_end)
            .filter(|&i| self.weights.up[i] != u32::MAX)
            .count();

        let tgt_down_rev_start = self.down_rev.offsets[target as usize] as usize;
        let tgt_down_rev_end = self.down_rev.offsets[target as usize + 1] as usize;
        let tgt_down_rev_count = tgt_down_rev_end - tgt_down_rev_start;
        let tgt_down_rev_reachable = (tgt_down_rev_start..tgt_down_rev_end)
            .filter(|&i| {
                let orig_idx = self.down_rev.edge_idx[i] as usize;
                self.weights.down[orig_idx] != u32::MAX
            })
            .count();

        eprintln!("DEBUG: src={} has {} UP edges ({} reachable), target={} has {} incoming DOWN edges ({} reachable)",
            source, src_up_count, src_up_reachable, target, tgt_down_rev_count, tgt_down_rev_reachable);

        // Distance arrays
        let mut dist_fwd = vec![u32::MAX; n];
        let mut dist_bwd = vec![u32::MAX; n];

        // Parent tracking for path reconstruction
        let mut parent_fwd: Vec<Option<(u32, u32)>> = vec![None; n]; // (parent_node, edge_idx)
        let mut parent_bwd: Vec<Option<(u32, u32)>> = vec![None; n];

        // Priority queues (min-heap via Reverse)
        let mut pq_fwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
        let mut pq_bwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();

        // Initialize
        dist_fwd[source as usize] = 0;
        dist_bwd[target as usize] = 0;
        pq_fwd.push(source, Reverse(0));
        pq_bwd.push(target, Reverse(0));

        // Best meeting point
        let mut best_dist = u32::MAX;
        let mut meeting_node = u32::MAX;

        // Alternating search
        while !pq_fwd.is_empty() || !pq_bwd.is_empty() {
            // Forward step - search UP graph
            if let Some((u, Reverse(d))) = pq_fwd.pop() {
                if d > dist_fwd[u as usize] {
                    continue; // Stale entry
                }

                // Check if we can improve meeting point
                if dist_bwd[u as usize] != u32::MAX {
                    let total = d.saturating_add(dist_bwd[u as usize]);
                    if total < best_dist {
                        best_dist = total;
                        meeting_node = u;
                    }
                }

                // Pruning: if we can't improve, stop
                if d >= best_dist {
                    continue;
                }

                // Relax UP edges
                let start = self.topo.up_offsets[u as usize] as usize;
                let end = self.topo.up_offsets[u as usize + 1] as usize;

                for i in start..end {
                    let v = self.topo.up_targets[i];
                    let w = self.weights.up[i];

                    if w == u32::MAX {
                        continue; // Unreachable edge
                    }

                    let new_dist = d.saturating_add(w);
                    if new_dist < dist_fwd[v as usize] {
                        dist_fwd[v as usize] = new_dist;
                        parent_fwd[v as usize] = Some((u, i as u32));
                        pq_fwd.push(v, Reverse(new_dist));
                    }
                }
            }

            // Backward step - use REVERSE of DOWN graph
            // In directed CCH, backward search traverses DOWN edges in reverse:
            // - DOWN edge x→y (rank(x) > rank(y)) with weight w means "x can reach y with cost w"
            // - For backward search: if y can reach target, then x can too via x→y
            // - We iterate incoming edges to u (edges x→u) and update dist_bwd[x]
            if let Some((u, Reverse(d))) = pq_bwd.pop() {
                if d > dist_bwd[u as usize] {
                    continue;
                }

                // Check meeting point
                if dist_fwd[u as usize] != u32::MAX {
                    let total = d.saturating_add(dist_fwd[u as usize]);
                    if total < best_dist {
                        best_dist = total;
                        meeting_node = u;
                    }
                }

                if d >= best_dist {
                    continue;
                }

                // Relax reverse DOWN edges (incoming edges x→u in the DOWN graph)
                // For each x that has a DOWN edge x→u:
                //   dist_bwd[x] = min(dist_bwd[x], down_weight[x→u] + dist_bwd[u])
                let start = self.down_rev.offsets[u as usize] as usize;
                let end = self.down_rev.offsets[u as usize + 1] as usize;

                for i in start..end {
                    let x = self.down_rev.sources[i];       // source node of edge x→u
                    let orig_idx = self.down_rev.edge_idx[i] as usize; // index in down_weights
                    let w = self.weights.down[orig_idx];

                    if w == u32::MAX {
                        continue;
                    }

                    let new_dist = d.saturating_add(w);
                    if new_dist < dist_bwd[x as usize] {
                        dist_bwd[x as usize] = new_dist;
                        parent_bwd[x as usize] = Some((u, orig_idx as u32));
                        pq_bwd.push(x, Reverse(new_dist));
                    }
                }
            }

            // Early termination check
            let min_fwd = pq_fwd.peek().map(|(_, Reverse(d))| *d).unwrap_or(u32::MAX);
            let min_bwd = pq_bwd.peek().map(|(_, Reverse(d))| *d).unwrap_or(u32::MAX);
            if min_fwd.min(min_bwd) >= best_dist {
                break;
            }
        }

        if best_dist == u32::MAX {
            // Debug: count how far each search got
            let fwd_reached = dist_fwd.iter().filter(|&&d| d != u32::MAX).count();
            let bwd_reached = dist_bwd.iter().filter(|&&d| d != u32::MAX).count();
            eprintln!("DEBUG: No route found. Forward reached {} nodes, backward reached {} nodes",
                fwd_reached, bwd_reached);
            return None;
        }

        // Reconstruct path
        let forward_parent = reconstruct_path(&parent_fwd, source, meeting_node);
        let backward_parent = reconstruct_path(&parent_bwd, target, meeting_node);

        Some(QueryResult {
            distance: best_dist,
            meeting_node,
            forward_parent,
            backward_parent,
        })
    }
}

/// Reconstruct path from parent pointers
fn reconstruct_path(
    parent: &[Option<(u32, u32)>],
    start: u32,
    end: u32,
) -> Vec<(u32, u32)> {
    let mut path = Vec::new();
    let mut current = end;

    while current != start {
        if let Some((prev, edge_idx)) = parent[current as usize] {
            path.push((current, edge_idx));
            current = prev;
        } else {
            break;
        }
    }

    path.reverse();
    path
}

/// One-to-many query for distance matrix
pub fn query_one_to_many(
    state: &ServerState,
    mode: Mode,
    source: u32,
    targets: &[u32],
) -> Vec<Option<u32>> {
    let query = CchQuery::new(state, mode);
    targets
        .iter()
        .map(|&t| query.query(source, t).map(|r| r.distance))
        .collect()
}
