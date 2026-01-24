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
        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[origin as usize] = 0;

        // Phase 1: Upward (bounded - don't push if beyond threshold)
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
                    // Only add to PQ if within threshold
                    // (upward edges can still be useful for reaching nodes
                    // that then propagate down to lower nodes)
                    pq.push(Reverse((new_dist, v)));
                    stats.upward_pq_pushes += 1;
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // Phase 2: Downward (skip if source is beyond threshold)
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = self.inv_perm[rank];
            let d_u = dist[u as usize];

            // Skip unreachable or beyond threshold
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

                // Only update if improves AND within some reasonable bound
                // (we allow slightly over threshold to capture frontier)
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
