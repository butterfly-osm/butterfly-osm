//! PHAST (PHAst Shortest-path Trees) for efficient one-to-many queries
//!
//! Two-phase algorithm:
//! 1. Upward phase: PQ-based Dijkstra using only UP edges from origin
//! 2. Downward phase: Linear scan in reverse rank order, relaxing DOWN edges
//!
//! The downward phase is O(n) with no priority queue, making it much faster
//! than naive Dijkstra for large reachable sets.

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::formats::{CchTopo, CchWeights, OrderEbg};

/// PHAST query engine
pub struct PhastEngine {
    /// CCH topology
    topo: CchTopo,
    /// CCH weights
    weights: CchWeights,
    /// Node ordering (perm[node] = rank)
    perm: Vec<u32>,
    /// Inverse ordering (inv_perm[rank] = node)
    inv_perm: Vec<u32>,
    /// Number of nodes
    n_nodes: usize,
}

/// PHAST query result
#[derive(Debug)]
pub struct PhastResult {
    /// Distance from origin to each node (u32::MAX = unreachable)
    pub dist: Vec<u32>,
    /// Number of nodes with finite distance
    pub n_reachable: usize,
    /// Statistics
    pub stats: PhastStats,
}

/// PHAST statistics
#[derive(Debug, Default)]
pub struct PhastStats {
    pub upward_pq_pushes: usize,
    pub upward_pq_pops: usize,
    pub upward_relaxations: usize,
    pub upward_settled: usize,
    pub downward_relaxations: usize,
    pub downward_improved: usize,
    pub upward_time_ms: u64,
    pub downward_time_ms: u64,
    pub total_time_ms: u64,
}

impl PhastEngine {
    /// Create PHAST engine from loaded CCH data
    pub fn new(topo: CchTopo, weights: CchWeights, order: OrderEbg) -> Self {
        let n_nodes = topo.n_nodes as usize;
        let perm = order.perm;

        // Build inverse permutation: inv_perm[rank] = node
        let mut inv_perm = vec![0u32; n_nodes];
        for (node, &rank) in perm.iter().enumerate() {
            inv_perm[rank as usize] = node as u32;
        }

        Self {
            topo,
            weights,
            perm,
            inv_perm,
            n_nodes,
        }
    }

    /// Load PHAST engine from file paths
    pub fn load(
        topo_path: &std::path::Path,
        weights_path: &std::path::Path,
        order_path: &std::path::Path,
    ) -> anyhow::Result<Self> {
        use crate::formats::{CchTopoFile, CchWeightsFile, OrderEbgFile};

        let topo = CchTopoFile::read(topo_path)?;
        let weights = CchWeightsFile::read(weights_path)?;
        let order = OrderEbgFile::read(order_path)?;

        Ok(Self::new(topo, weights, order))
    }

    /// Run PHAST query from origin
    ///
    /// Returns distances to ALL nodes (not bounded by threshold).
    /// For isochrones, filter the result by threshold afterwards.
    pub fn query(&self, origin: u32) -> PhastResult {
        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        // Distance array
        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[origin as usize] = 0;

        // ============================================================
        // Phase 1: Upward search (PQ-based, UP edges only)
        // ============================================================
        let upward_start = std::time::Instant::now();

        // Priority queue: (distance, node)
        let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        pq.push(Reverse((0, origin)));
        stats.upward_pq_pushes += 1;

        while let Some(Reverse((d, u))) = pq.pop() {
            stats.upward_pq_pops += 1;

            // Skip if stale
            if d > dist[u as usize] {
                continue;
            }

            stats.upward_settled += 1;

            // Relax UP edges only
            let up_start = self.topo.up_offsets[u as usize] as usize;
            let up_end = self.topo.up_offsets[u as usize + 1] as usize;

            for i in up_start..up_end {
                let v = self.topo.up_targets[i];
                let w = self.weights.up[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d.saturating_add(w);
                stats.upward_relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    pq.push(Reverse((new_dist, v)));
                    stats.upward_pq_pushes += 1;
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // ============================================================
        // Phase 2: Downward scan (linear, DOWN edges only)
        // ============================================================
        // Process nodes in DECREASING rank order (highest rank first)
        // This ensures when we process a node, all higher-rank predecessors
        // have already been processed.
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = self.inv_perm[rank];
            let d_u = dist[u as usize];

            // Skip unreachable nodes
            if d_u == u32::MAX {
                continue;
            }

            // Relax DOWN edges
            let down_start = self.topo.down_offsets[u as usize] as usize;
            let down_end = self.topo.down_offsets[u as usize + 1] as usize;

            for i in down_start..down_end {
                let v = self.topo.down_targets[i];
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    stats.downward_improved += 1;
                }
            }
        }

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        // Count reachable nodes
        let n_reachable = dist.iter().filter(|&&d| d != u32::MAX).count();

        PhastResult {
            dist,
            n_reachable,
            stats,
        }
    }

    /// Run bounded PHAST query (stop downward scan when beyond threshold)
    ///
    /// This is an optimization: we still do full upward, but skip downward
    /// relaxations that would exceed the threshold.
    pub fn query_bounded(&self, origin: u32, threshold: u32) -> PhastResult {
        // Use active-set gating for better performance
        self.query_active_set(origin, threshold)
    }

    /// Run PHAST with active-set gating (rPHAST-lite)
    ///
    /// Key optimization: maintains a bitset of "active" nodes that might
    /// contribute to paths within threshold. Skips nodes not in active set
    /// during downward scan, dramatically reducing work for bounded queries.
    ///
    /// Active set propagates: if node u is active and edge u→v improves dist[v]
    /// to within threshold, v becomes active too.
    pub fn query_active_set(&self, origin: u32, threshold: u32) -> PhastResult {
        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[origin as usize] = 0;

        // Active set bitset: nodes that might contribute to paths ≤ threshold
        // Using u64 words for efficient bit operations
        let n_words = (self.n_nodes + 63) / 64;
        let mut active = vec![0u64; n_words];

        // Mark origin as active
        let origin_idx = origin as usize;
        active[origin_idx / 64] |= 1u64 << (origin_idx % 64);

        // Phase 1: Upward search with active set tracking
        let upward_start = std::time::Instant::now();

        let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        pq.push(Reverse((0, origin)));
        stats.upward_pq_pushes += 1;

        while let Some(Reverse((d, u))) = pq.pop() {
            stats.upward_pq_pops += 1;

            if d > dist[u as usize] {
                continue;
            }

            stats.upward_settled += 1;

            let up_start = self.topo.up_offsets[u as usize] as usize;
            let up_end = self.topo.up_offsets[u as usize + 1] as usize;

            for i in up_start..up_end {
                let v = self.topo.up_targets[i];
                let w = self.weights.up[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d.saturating_add(w);
                stats.upward_relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    pq.push(Reverse((new_dist, v)));
                    stats.upward_pq_pushes += 1;

                    // Only mark v as active if upward distance is within threshold
                    // Nodes with upward_dist > threshold can't contribute to paths ≤ threshold
                    // because the total path is upward_dist + downward_dist
                    if new_dist <= threshold {
                        let v_idx = v as usize;
                        active[v_idx / 64] |= 1u64 << (v_idx % 64);
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // Phase 2: Downward scan with active-set gating
        let downward_start = std::time::Instant::now();

        // Count nodes skipped due to active-set gating
        let mut nodes_skipped = 0usize;

        for rank in (0..self.n_nodes).rev() {
            let u = self.inv_perm[rank];
            let u_idx = u as usize;

            // Check if node is in active set
            let is_active = (active[u_idx / 64] >> (u_idx % 64)) & 1 != 0;
            if !is_active {
                nodes_skipped += 1;
                continue;
            }

            let d_u = dist[u_idx];

            // Skip if unreachable (shouldn't happen if active, but defensive)
            if d_u == u32::MAX {
                continue;
            }

            let down_start = self.topo.down_offsets[u_idx] as usize;
            let down_end = self.topo.down_offsets[u_idx + 1] as usize;

            for i in down_start..down_end {
                let v = self.topo.down_targets[i];
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    stats.downward_improved += 1;

                    // If new distance is within threshold, mark v as active
                    // This allows paths to propagate through v in later iterations
                    // Note: since we process in decreasing rank order, v has lower rank
                    // and will be processed later, so marking it active now is useful
                    if new_dist <= threshold {
                        let v_idx = v as usize;
                        active[v_idx / 64] |= 1u64 << (v_idx % 64);
                    }
                }
            }
        }

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        // The nodes_skipped count shows how effective active-set gating was
        // (available via stats if we add it, but not critical for correctness)

        let n_reachable = dist.iter().filter(|&&d| d <= threshold).count();

        PhastResult {
            dist,
            n_reachable,
            stats,
        }
    }

    /// Run bounded PHAST without active-set gating (for comparison/validation)
    pub fn query_bounded_naive(&self, origin: u32, threshold: u32) -> PhastResult {
        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[origin as usize] = 0;

        // Phase 1: Upward
        let upward_start = std::time::Instant::now();

        let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        pq.push(Reverse((0, origin)));
        stats.upward_pq_pushes += 1;

        while let Some(Reverse((d, u))) = pq.pop() {
            stats.upward_pq_pops += 1;

            if d > dist[u as usize] {
                continue;
            }

            stats.upward_settled += 1;

            let up_start = self.topo.up_offsets[u as usize] as usize;
            let up_end = self.topo.up_offsets[u as usize + 1] as usize;

            for i in up_start..up_end {
                let v = self.topo.up_targets[i];
                let w = self.weights.up[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d.saturating_add(w);
                stats.upward_relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    pq.push(Reverse((new_dist, v)));
                    stats.upward_pq_pushes += 1;
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // Phase 2: Downward (no active-set gating)
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = self.inv_perm[rank];
            let d_u = dist[u as usize];

            if d_u == u32::MAX {
                continue;
            }

            let down_start = self.topo.down_offsets[u as usize] as usize;
            let down_end = self.topo.down_offsets[u as usize + 1] as usize;

            for i in down_start..down_end {
                let v = self.topo.down_targets[i];
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    stats.downward_improved += 1;
                }
            }
        }

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        let n_reachable = dist.iter().filter(|&&d| d <= threshold).count();

        PhastResult {
            dist,
            n_reachable,
            stats,
        }
    }

    /// Get number of nodes
    pub fn n_nodes(&self) -> usize {
        self.n_nodes
    }

    /// Extract frontier edges for isochrone polygonization
    pub fn extract_frontier(&self, dist: &[u32], threshold: u32) -> Vec<super::FrontierEdge> {
        let mut frontier = Vec::new();

        for u in 0..self.n_nodes {
            let d_u = dist[u];
            if d_u > threshold {
                continue;
            }

            // Check UP edges
            let up_start = self.topo.up_offsets[u] as usize;
            let up_end = self.topo.up_offsets[u + 1] as usize;

            for i in up_start..up_end {
                let v = self.topo.up_targets[i];
                let w = self.weights.up[i];
                if w != u32::MAX {
                    let d_v = dist[v as usize];
                    if d_v > threshold {
                        frontier.push(super::FrontierEdge {
                            src: u as u32,
                            dst: v,
                            dist_src: d_u,
                            dist_dst: d_v,
                            weight: w,
                        });
                    }
                }
            }

            // Check DOWN edges
            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            for i in down_start..down_end {
                let v = self.topo.down_targets[i];
                let w = self.weights.down[i];
                if w != u32::MAX {
                    let d_v = dist[v as usize];
                    if d_v > threshold {
                        frontier.push(super::FrontierEdge {
                            src: u as u32,
                            dst: v,
                            dist_src: d_u,
                            dist_dst: d_v,
                            weight: w,
                        });
                    }
                }
            }
        }

        frontier
    }
}

#[cfg(test)]
mod tests {
    // Tests will validate PHAST vs naive Dijkstra
}
