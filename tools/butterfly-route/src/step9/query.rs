//! CCH query algorithm - bidirectional Dijkstra on hierarchy
//!
//! Uses thread-local generation-stamped state to eliminate O(|V|)
//! allocation per query. Distance and parent arrays are allocated
//! once per thread and reused across queries via version stamping.

use std::cell::RefCell;
use std::cmp::Reverse;

use priority_queue::PriorityQueue;

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

// =============================================================================
// THREAD-LOCAL CCH QUERY STATE (eliminates ~80MB allocation per query)
// =============================================================================

/// Thread-local CCH query state with generation stamping.
/// Eliminates O(|V|) initialization per query by using version stamps.
///
/// For Belgium (~5M EBG nodes), this avoids allocating:
/// - 2 × 20MB distance arrays
/// - 2 × 40MB parent arrays
///
/// per query. Instead, these are allocated once per thread and reused.
struct CchQueryState {
    /// Forward distance array (persistent across queries)
    dist_fwd: Vec<u32>,
    /// Backward distance array
    dist_bwd: Vec<u32>,
    /// Forward parent: packed (prev_node, edge_idx)
    parent_fwd: Vec<(u32, u32)>,
    /// Backward parent: packed (prev_node, edge_idx)
    parent_bwd: Vec<(u32, u32)>,
    /// Version stamp per node for forward search
    gen_fwd: Vec<u32>,
    /// Version stamp per node for backward search
    gen_bwd: Vec<u32>,
    /// Current generation (incremented per query)
    current_gen: u32,
    /// Forward priority queue (reused across queries)
    pq_fwd: PriorityQueue<u32, Reverse<u32>>,
    /// Backward priority queue
    pq_bwd: PriorityQueue<u32, Reverse<u32>>,
}

impl CchQueryState {
    fn new(n_nodes: usize) -> Self {
        Self {
            dist_fwd: vec![u32::MAX; n_nodes],
            dist_bwd: vec![u32::MAX; n_nodes],
            parent_fwd: vec![(u32::MAX, 0); n_nodes],
            parent_bwd: vec![(u32::MAX, 0); n_nodes],
            gen_fwd: vec![0; n_nodes],
            gen_bwd: vec![0; n_nodes],
            current_gen: 0,
            pq_fwd: PriorityQueue::new(),
            pq_bwd: PriorityQueue::new(),
        }
    }

    /// Start a new query (O(1) instead of O(n))
    #[inline]
    fn start_query(&mut self) {
        self.current_gen = self.current_gen.wrapping_add(1);
        if self.current_gen == 0 {
            // Overflow — reset all versions (rare, every ~4B queries)
            self.gen_fwd.iter_mut().for_each(|v| *v = 0);
            self.gen_bwd.iter_mut().for_each(|v| *v = 0);
            self.current_gen = 1;
        }
        self.pq_fwd.clear();
        self.pq_bwd.clear();
    }

    // Forward distance accessors
    #[inline]
    fn get_fwd(&self, node: usize) -> u32 {
        if self.gen_fwd[node] == self.current_gen {
            self.dist_fwd[node]
        } else {
            u32::MAX
        }
    }

    #[inline]
    fn set_fwd(&mut self, node: usize, dist: u32, parent: (u32, u32)) {
        self.dist_fwd[node] = dist;
        self.parent_fwd[node] = parent;
        self.gen_fwd[node] = self.current_gen;
    }

    // Backward distance accessors
    #[inline]
    fn get_bwd(&self, node: usize) -> u32 {
        if self.gen_bwd[node] == self.current_gen {
            self.dist_bwd[node]
        } else {
            u32::MAX
        }
    }

    #[inline]
    fn set_bwd(&mut self, node: usize, dist: u32, parent: (u32, u32)) {
        self.dist_bwd[node] = dist;
        self.parent_bwd[node] = parent;
        self.gen_bwd[node] = self.current_gen;
    }
}

thread_local! {
    /// Single thread-local CCH query state. Re-initializes when n_nodes changes.
    static CCH_QUERY_STATE: RefCell<Option<CchQueryState>> = const { RefCell::new(None) };
}

/// Reconstruct path from generation-stamped parent arrays
fn reconstruct_path_versioned(
    parent: &[(u32, u32)],
    gen: &[u32],
    current_gen: u32,
    start: u32,
    end: u32,
) -> Vec<(u32, u32)> {
    let mut path = Vec::new();
    let mut current = end;

    while current != start {
        if gen[current as usize] == current_gen {
            let (prev, edge_idx) = parent[current as usize];
            path.push((current, edge_idx));
            current = prev;
        } else {
            break;
        }
    }

    path.reverse();
    path
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

        CCH_QUERY_STATE.with(|cell| {
            let mut state_opt = cell.borrow_mut();

            // Initialize or reinitialize if n_nodes changed
            let state = state_opt.get_or_insert_with(|| CchQueryState::new(n));
            if state.dist_fwd.len() != n {
                *state = CchQueryState::new(n);
            }

            // Start new query (O(1) instead of O(n) memset)
            state.start_query();

            // Initialize source and target
            state.set_fwd(source as usize, 0, (source, 0));
            state.set_bwd(target as usize, 0, (target, 0));
            state.pq_fwd.push(source, Reverse(0));
            state.pq_bwd.push(target, Reverse(0));

            // Best meeting point
            let mut best_dist = u32::MAX;
            let mut meeting_node = u32::MAX;

            // Debug counters
            let mut fwd_settled = 0usize;
            let mut bwd_settled = 0usize;
            let mut fwd_relaxed = 0usize;
            let mut bwd_relaxed = 0usize;

            // Bidirectional search with early termination
            while !state.pq_fwd.is_empty() || !state.pq_bwd.is_empty() {
                // Early termination: if both queue minimums exceed best_dist, stop
                let fwd_min = state
                    .pq_fwd
                    .peek()
                    .map(|(_, &Reverse(d))| d)
                    .unwrap_or(u32::MAX);
                let bwd_min = state
                    .pq_bwd
                    .peek()
                    .map(|(_, &Reverse(d))| d)
                    .unwrap_or(u32::MAX);
                if fwd_min >= best_dist && bwd_min >= best_dist {
                    break;
                }

                // Forward step — search UP graph
                if let Some((u, Reverse(d))) = state.pq_fwd.pop() {
                    if d > state.get_fwd(u as usize) {
                        // Stale entry — skip
                    } else {
                        fwd_settled += 1;

                        // Check meeting point when settling a node
                        let bwd_d = state.get_bwd(u as usize);
                        if bwd_d != u32::MAX {
                            let total = d.saturating_add(bwd_d);
                            if total < best_dist {
                                best_dist = total;
                                meeting_node = u;
                                if debug {
                                    eprintln!(
                                        "  FWD meet at {}: dist_fwd={}, dist_bwd={}, total={}",
                                        u, d, bwd_d, total
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
                            if new_dist < state.get_fwd(v as usize) {
                                state.set_fwd(v as usize, new_dist, (u, i as u32));
                                state.pq_fwd.push(v, Reverse(new_dist));

                                // Check meeting when updating
                                let bwd_v = state.get_bwd(v as usize);
                                if bwd_v != u32::MAX {
                                    let total = new_dist.saturating_add(bwd_v);
                                    if total < best_dist {
                                        best_dist = total;
                                        meeting_node = v;
                                        if debug {
                                            eprintln!("  FWD meet at {} (via edge): dist_fwd={}, dist_bwd={}, total={}", v, new_dist, bwd_v, total);
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Backward step — traverse reversed DOWN edges (= upward in reversed graph)
                if let Some((u, Reverse(d))) = state.pq_bwd.pop() {
                    if d > state.get_bwd(u as usize) {
                        // Stale — skip
                    } else {
                        bwd_settled += 1;

                        // Check meeting point
                        let fwd_d = state.get_fwd(u as usize);
                        if fwd_d != u32::MAX {
                            let total = d.saturating_add(fwd_d);
                            if total < best_dist {
                                best_dist = total;
                                meeting_node = u;
                                if debug {
                                    eprintln!(
                                        "  BWD meet at {}: dist_fwd={}, dist_bwd={}, total={}",
                                        u, fwd_d, d, total
                                    );
                                }
                            }
                        }

                        // Relax reverse DOWN edges
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
                            if new_dist < state.get_bwd(x as usize) {
                                state.set_bwd(x as usize, new_dist, (u, edge_idx as u32));
                                state.pq_bwd.push(x, Reverse(new_dist));

                                // Check meeting when updating
                                let fwd_x = state.get_fwd(x as usize);
                                if fwd_x != u32::MAX {
                                    let total = new_dist.saturating_add(fwd_x);
                                    if total < best_dist {
                                        best_dist = total;
                                        meeting_node = x;
                                        if debug {
                                            eprintln!("  BWD meet at {} (via edge): dist_fwd={}, dist_bwd={}, total={}", x, fwd_x, new_dist, total);
                                        }
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

            // Reconstruct paths using generation-stamped parent arrays
            let forward_parent = reconstruct_path_versioned(
                &state.parent_fwd,
                &state.gen_fwd,
                state.current_gen,
                source,
                meeting_node,
            );
            let backward_parent = reconstruct_path_versioned(
                &state.parent_bwd,
                &state.gen_bwd,
                state.current_gen,
                target,
                meeting_node,
            );

            Some(QueryResult {
                distance: best_dist,
                meeting_node,
                forward_parent,
                backward_parent,
            })
        })
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::cch_topo::CchTopo;

    /// Build a minimal 5-node CCH graph for testing.
    ///
    /// Graph (rank = node id for simplicity):
    ///
    ///   UP edges:
    ///     0 → 2 (w=10)
    ///     1 → 2 (w=3)
    ///     2 → 3 (w=7)
    ///     2 → 4 (w=5)
    ///
    ///   DOWN edges (mirror):
    ///     2 → 0 (w=10)
    ///     2 → 1 (w=3)
    ///     3 → 2 (w=7)
    ///     4 → 2 (w=5)
    fn build_test_cch() -> (CchTopo, CchWeights, DownReverseAdj) {
        let n_nodes = 5u32;

        let up_offsets = vec![0u64, 1, 2, 4, 4, 4];
        let up_targets = vec![2u32, 2, 3, 4];
        let up_is_shortcut = vec![false; 4];
        let up_middle = vec![u32::MAX; 4];

        let down_offsets = vec![0u64, 0, 0, 2, 3, 4];
        let down_targets = vec![0u32, 1, 2, 2];
        let down_is_shortcut = vec![false; 4];
        let down_middle = vec![u32::MAX; 4];

        let rank_to_filtered: Vec<u32> = (0..n_nodes).collect();

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: 4,
            inputs_sha: [0u8; 32],
            up_offsets,
            up_targets,
            up_is_shortcut,
            up_middle,
            down_offsets,
            down_targets,
            down_is_shortcut,
            down_middle,
            rank_to_filtered,
        };

        let weights = CchWeights {
            up: vec![10u32, 3, 7, 5],
            down: vec![10u32, 3, 7, 5],
        };

        // Build DownReverseAdj from topo
        // DOWN edges: 2→0 (idx=0), 2→1 (idx=1), 3→2 (idx=2), 4→2 (idx=3)
        // Reversed (target → sources):
        //   node 0: incoming from 2 (edge_idx=0)
        //   node 1: incoming from 2 (edge_idx=1)
        //   node 2: incoming from 3 (edge_idx=2), 4 (edge_idx=3)
        //   node 3: none
        //   node 4: none
        let down_rev = DownReverseAdj {
            offsets: vec![0u64, 1, 2, 4, 4, 4],
            sources: vec![2u32, 2, 3, 4],
            edge_idx: vec![0u32, 1, 2, 3],
        };

        (topo, weights, down_rev)
    }

    #[test]
    fn test_same_node_query() {
        let (topo, weights, down_rev) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &down_rev, &weights);

        let result = query.query(0, 0).expect("same-node query should succeed");
        assert_eq!(result.distance, 0);
        assert_eq!(result.meeting_node, 0);
        assert!(result.forward_parent.is_empty());
        assert!(result.backward_parent.is_empty());
    }

    #[test]
    fn test_basic_shortest_path() {
        let (topo, weights, down_rev) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &down_rev, &weights);

        // Path 0 → 1: 0→UP→2→DOWN→1, cost = 10 + 3 = 13
        let result = query.query(0, 1).expect("path should exist");
        assert_eq!(result.distance, 13);

        // Path 1 → 0: 1→UP→2→DOWN→0, cost = 3 + 10 = 13
        let result = query.query(1, 0).expect("path should exist");
        assert_eq!(result.distance, 13);

        // Path 0 → 2: 0→UP→2, cost = 10
        let result = query.query(0, 2).expect("path should exist");
        assert_eq!(result.distance, 10);
    }

    #[test]
    fn test_multi_hop_path() {
        let (topo, weights, down_rev) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &down_rev, &weights);

        // Path 0 → 3: 0→UP→2→UP→3, cost = 10 + 7 = 17
        let result = query.query(0, 3).expect("path should exist");
        assert_eq!(result.distance, 17);

        // Path 0 → 4: 0→UP→2→UP→4, cost = 10 + 5 = 15
        let result = query.query(0, 4).expect("path should exist");
        assert_eq!(result.distance, 15);

        // Path 1 → 3: 1→UP→2→UP→3, cost = 3 + 7 = 10
        let result = query.query(1, 3).expect("path should exist");
        assert_eq!(result.distance, 10);
    }

    #[test]
    fn test_thread_local_state_reuse() {
        // Run many queries to verify thread-local state is correctly reused
        // across queries (generation stamping doesn't leak stale data)
        let (topo, weights, down_rev) = build_test_cch();
        let query = CchQuery::with_custom_weights(&topo, &down_rev, &weights);

        for _ in 0..100 {
            assert_eq!(query.query(0, 1).unwrap().distance, 13);
            assert_eq!(query.query(1, 0).unwrap().distance, 13);
            assert_eq!(query.query(0, 3).unwrap().distance, 17);
            assert_eq!(query.query(1, 4).unwrap().distance, 8); // 3 + 5
            assert_eq!(query.query(0, 0).unwrap().distance, 0);
        }
    }

    #[test]
    fn test_reconstruct_path_versioned_basic() {
        let parent = vec![(u32::MAX, 0), (0, 42), (1, 99)];
        let gen = vec![5, 5, 5];

        // Path from 0 to 2: 0 → 1 (edge 42) → 2 (edge 99)
        let path = reconstruct_path_versioned(&parent, &gen, 5, 0, 2);
        assert_eq!(path.len(), 2);
        assert_eq!(path[0], (1, 42));
        assert_eq!(path[1], (2, 99));
    }

    #[test]
    fn test_reconstruct_path_versioned_stale_gen() {
        let parent = vec![(u32::MAX, 0), (0, 42), (1, 99)];
        let gen = vec![5, 5, 3]; // Node 2 has stale generation

        // Should stop at node 2 because gen[2] != current_gen
        let path = reconstruct_path_versioned(&parent, &gen, 5, 0, 2);
        assert!(path.is_empty()); // Can't trace back from node 2
    }

    #[test]
    fn test_reconstruct_path_versioned_single_step() {
        let parent = vec![(u32::MAX, 0), (0, 7)];
        let gen = vec![1, 1];

        let path = reconstruct_path_versioned(&parent, &gen, 1, 0, 1);
        assert_eq!(path.len(), 1);
        assert_eq!(path[0], (1, 7));
    }

    #[test]
    fn test_no_path_between_disconnected_nodes() {
        // Build a graph where node 3 and 4 have UP edges to nowhere reachable from below
        // Actually in our test graph everything connects through node 2.
        // Let's make a 6-node graph with two components.
        let n_nodes = 6u32;

        let up_offsets = vec![0u64, 1, 2, 2, 3, 4, 4];
        let up_targets = vec![2u32, 2, 5, 5]; // 0→2, 1→2, 3→5, 4→5
        let up_is_shortcut = vec![false; 4];
        let up_middle = vec![u32::MAX; 4];

        let down_offsets = vec![0u64, 0, 0, 2, 2, 2, 4];
        let down_targets = vec![0u32, 1, 3, 4]; // 2→0, 2→1, 5→3, 5→4
        let down_is_shortcut = vec![false; 4];
        let down_middle = vec![u32::MAX; 4];

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: 4,
            inputs_sha: [0u8; 32],
            up_offsets,
            up_targets,
            up_is_shortcut,
            up_middle,
            down_offsets,
            down_targets,
            down_is_shortcut,
            down_middle,
            rank_to_filtered: (0..n_nodes).collect(),
        };

        let weights = CchWeights {
            up: vec![10, 3, 7, 5],
            down: vec![10, 3, 7, 5],
        };

        let down_rev = DownReverseAdj {
            offsets: vec![0u64, 1, 2, 2, 3, 4, 4],
            sources: vec![2u32, 2, 5, 5],
            edge_idx: vec![0u32, 1, 2, 3],
        };

        let query = CchQuery::with_custom_weights(&topo, &down_rev, &weights);

        // Same component works
        assert_eq!(query.query(0, 1).unwrap().distance, 13);
        // Path 3→4: 3→UP→5→DOWN→4, cost = 7 + 5 = 12
        assert_eq!(query.query(3, 4).unwrap().distance, 12);

        // Cross-component: no path
        assert!(query.query(0, 3).is_none());
        assert!(query.query(3, 1).is_none());
    }
}
