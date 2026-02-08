//! CCH query algorithm - bidirectional Dijkstra on hierarchy

use priority_queue::PriorityQueue;
use std::cmp::Reverse;

use crate::formats::CchTopo;
use crate::profile_abi::Mode;

use super::state::{CchWeights, DownReverseAdj, ServerState};

/// Validate backward adjacency invariant:
/// For every entry in down_rev, the reversed edge must be "upward in reversed graph"
/// i.e., for DOWN edge x→u (rank[x] >= rank[u]), reversed edge u→x has rank[u] <= rank[x]
pub fn validate_down_rev(
    topo: &CchTopo,
    down_rev: &DownReverseAdj,
    perm: &[u32],
) -> Result<(), String> {
    let n_nodes = topo.n_nodes as usize;
    let mut violations = 0;

    for u in 0..n_nodes {
        let start = down_rev.offsets[u] as usize;
        let end = down_rev.offsets[u + 1] as usize;

        for i in start..end {
            let x = down_rev.sources[i] as usize;
            let edge_idx = down_rev.edge_idx[i] as usize;

            // Verify this is a valid DOWN edge x→u
            let rank_x = perm[x];
            let rank_u = perm[u];

            // DOWN edge: rank[x] >= rank[u]
            // Reversed: u→x should be upward (rank[u] <= rank[x])
            if rank_x < rank_u {
                violations += 1;
                if violations <= 5 {
                    eprintln!(
                        "  down_rev violation: edge {}→{} has rank {} < {} (should be >=)",
                        x, u, rank_x, rank_u
                    );
                }
            }

            // Verify edge_idx points to valid DOWN edge
            if edge_idx >= topo.down_targets.len() {
                return Err(format!(
                    "Invalid edge_idx {} >= {}",
                    edge_idx,
                    topo.down_targets.len()
                ));
            }

            // Verify the target of this DOWN edge is actually u
            let stored_target = topo.down_targets[edge_idx];
            if stored_target != u as u32 {
                violations += 1;
                if violations <= 5 {
                    eprintln!(
                        "  down_rev target mismatch: edge_idx {} has target {}, expected {}",
                        edge_idx, stored_target, u
                    );
                }
            }
        }
    }

    if violations > 0 {
        Err(format!("{} down_rev violations found", violations))
    } else {
        Ok(())
    }
}

/// Query result
#[derive(Debug, Clone)]
pub struct QueryResult {
    pub distance: u32,
    pub meeting_node: u32,
    pub forward_parent: Vec<(u32, u32)>,
    pub backward_parent: Vec<(u32, u32)>,
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

    /// Create a query with custom weights (for alternative routes with penalties)
    pub fn with_custom_weights(
        topo: &'a CchTopo,
        down_rev: &'a DownReverseAdj,
        weights: &'a CchWeights,
    ) -> Self {
        Self {
            topo,
            down_rev,
            weights,
            n_nodes: topo.n_nodes as usize,
        }
    }

    /// Run bidirectional query from source to target
    pub fn query(&self, source: u32, target: u32) -> Option<QueryResult> {
        self.query_with_debug(source, target, false)
    }

    /// Run bidirectional query with optional debug output
    pub fn query_with_debug(&self, source: u32, target: u32, debug: bool) -> Option<QueryResult> {
        if source == target {
            return Some(QueryResult {
                distance: 0,
                meeting_node: source,
                forward_parent: vec![],
                backward_parent: vec![],
            });
        }

        let n = self.n_nodes;

        // Distance arrays
        let mut dist_fwd = vec![u32::MAX; n];
        let mut dist_bwd = vec![u32::MAX; n];

        // Parent tracking for path reconstruction
        let mut parent_fwd: Vec<Option<(u32, u32)>> = vec![None; n];
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

        // Debug counters
        let mut fwd_settled = 0usize;
        let mut bwd_settled = 0usize;
        let mut fwd_relaxed = 0usize;
        let mut bwd_relaxed = 0usize;

        // Run both searches to completion (no early termination for now)
        // This helps diagnose if the issue is in termination logic
        while !pq_fwd.is_empty() || !pq_bwd.is_empty() {
            // Forward step - search UP graph
            if let Some((u, Reverse(d))) = pq_fwd.pop() {
                if d > dist_fwd[u as usize] {
                    continue; // Stale entry
                }

                fwd_settled += 1;

                // Check meeting point when settling a node
                if dist_bwd[u as usize] != u32::MAX {
                    let total = d.saturating_add(dist_bwd[u as usize]);
                    if total < best_dist {
                        best_dist = total;
                        meeting_node = u;
                        if debug {
                            eprintln!(
                                "  FWD meet at {}: dist_fwd={}, dist_bwd={}, total={}",
                                u, d, dist_bwd[u as usize], total
                            );
                        }
                    }
                }

                // Relax UP edges
                let start = self.topo.up_offsets[u as usize] as usize;
                let end = self.topo.up_offsets[u as usize + 1] as usize;

                for i in start..end {
                    let v = self.topo.up_targets[i];
                    let w = self.weights.up[i];

                    if w == u32::MAX {
                        continue;
                    }

                    fwd_relaxed += 1;
                    let new_dist = d.saturating_add(w);
                    if new_dist < dist_fwd[v as usize] {
                        dist_fwd[v as usize] = new_dist;
                        parent_fwd[v as usize] = Some((u, i as u32));
                        pq_fwd.push(v, Reverse(new_dist));

                        // Check meeting when updating
                        if dist_bwd[v as usize] != u32::MAX {
                            let total = new_dist.saturating_add(dist_bwd[v as usize]);
                            if total < best_dist {
                                best_dist = total;
                                meeting_node = v;
                                if debug {
                                    eprintln!("  FWD meet at {} (via edge): dist_fwd={}, dist_bwd={}, total={}", v, new_dist, dist_bwd[v as usize], total);
                                }
                            }
                        }
                    }
                }
            }

            // Backward step - traverse reversed DOWN edges (= upward in reversed graph)
            if let Some((u, Reverse(d))) = pq_bwd.pop() {
                if d > dist_bwd[u as usize] {
                    continue;
                }

                bwd_settled += 1;

                // Check meeting point
                if dist_fwd[u as usize] != u32::MAX {
                    let total = d.saturating_add(dist_fwd[u as usize]);
                    if total < best_dist {
                        best_dist = total;
                        meeting_node = u;
                        if debug {
                            eprintln!(
                                "  BWD meet at {}: dist_fwd={}, dist_bwd={}, total={}",
                                u, dist_fwd[u as usize], d, total
                            );
                        }
                    }
                }

                // Relax reverse DOWN edges
                // down_rev[u] contains sources x of DOWN edges x→u
                // We update dist_bwd[x] = dist_bwd[u] + weight[x→u]
                let start = self.down_rev.offsets[u as usize] as usize;
                let end = self.down_rev.offsets[u as usize + 1] as usize;

                for i in start..end {
                    let x = self.down_rev.sources[i];
                    let edge_idx = self.down_rev.edge_idx[i] as usize;
                    let w = self.weights.down[edge_idx];

                    if w == u32::MAX {
                        continue;
                    }

                    bwd_relaxed += 1;
                    let new_dist = d.saturating_add(w);
                    if new_dist < dist_bwd[x as usize] {
                        dist_bwd[x as usize] = new_dist;
                        parent_bwd[x as usize] = Some((u, edge_idx as u32));
                        pq_bwd.push(x, Reverse(new_dist));

                        // Check meeting when updating
                        if dist_fwd[x as usize] != u32::MAX {
                            let total = new_dist.saturating_add(dist_fwd[x as usize]);
                            if total < best_dist {
                                best_dist = total;
                                meeting_node = x;
                                if debug {
                                    eprintln!("  BWD meet at {} (via edge): dist_fwd={}, dist_bwd={}, total={}", x, dist_fwd[x as usize], new_dist, total);
                                }
                            }
                        }
                    }
                }
            }
        }

        if debug {
            eprintln!(
                "  Search stats: fwd_settled={}, bwd_settled={}, fwd_relaxed={}, bwd_relaxed={}",
                fwd_settled, bwd_settled, fwd_relaxed, bwd_relaxed
            );
            eprintln!(
                "  Final: best_dist={}, meeting_node={}",
                best_dist, meeting_node
            );
        }

        if best_dist == u32::MAX {
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
fn reconstruct_path(parent: &[Option<(u32, u32)>], start: u32, end: u32) -> Vec<(u32, u32)> {
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
