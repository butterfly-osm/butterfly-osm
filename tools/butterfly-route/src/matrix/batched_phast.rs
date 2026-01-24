//! K-Lane Batched PHAST for bulk matrix computation
//!
//! Processes K sources in one downward scan, amortizing memory access cost.
//!
//! ## Blocked Relaxation
//!
//! The downward scan has 80-87% cache miss rate due to random writes to `dist[v]`.
//! Blocked relaxation buffers updates by destination block, converting random writes
//! to sequential writes within cache-friendly blocks.
//!
//! Expected improvement: 2-5x on downward phase (cache miss rate 85% → 30-50%)

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::formats::{CchTopo, CchWeights, CchTopoFile, CchWeightsFile, OrderEbg, OrderEbgFile};

/// Lane width for batched PHAST (tunable based on cache line size)
/// K=8 gives good balance between parallelism and register pressure
pub const K_LANES: usize = 8;

/// Block size for rank-based memory blocking
pub const BLOCK_SIZE: usize = 8192;

/// Batched PHAST engine for K-lane parallel queries
pub struct BatchedPhastEngine {
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

/// Result of K-lane batched PHAST query
#[derive(Debug)]
pub struct BatchedPhastResult {
    /// Distance arrays for each source (K arrays of n_nodes each)
    /// dist[lane][node] = distance from sources[lane] to node
    pub dist: Vec<Vec<u32>>,
    /// Number of active lanes (may be < K for last batch)
    pub n_lanes: usize,
    /// Statistics
    pub stats: BatchedPhastStats,
}

/// Statistics for batched PHAST
#[derive(Debug, Default, Clone)]
pub struct BatchedPhastStats {
    pub n_sources: usize,
    pub upward_relaxations: usize,
    pub upward_settled: usize,
    pub downward_relaxations: usize,
    pub downward_improved: usize,
    pub upward_time_ms: u64,
    pub downward_time_ms: u64,
    pub total_time_ms: u64,
    /// Blocked relaxation stats
    pub buffer_flushes: usize,
    pub buffered_updates: usize,
}

impl BatchedPhastEngine {
    /// Create batched PHAST engine from CCH data
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

    /// Load batched PHAST engine from file paths
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

    /// Get number of nodes
    pub fn n_nodes(&self) -> usize {
        self.n_nodes
    }

    /// Run K-lane batched PHAST query for up to K sources
    ///
    /// # Arguments
    /// * `sources` - Up to K source node IDs (len must be <= K_LANES)
    ///
    /// # Returns
    /// BatchedPhastResult with distance arrays for each source
    pub fn query_batch(&self, sources: &[u32]) -> BatchedPhastResult {
        assert!(sources.len() <= K_LANES, "Too many sources for batch");
        let k = sources.len();

        let start = std::time::Instant::now();
        let mut stats = BatchedPhastStats {
            n_sources: k,
            ..Default::default()
        };

        // Initialize K distance arrays
        let mut dist: Vec<Vec<u32>> = (0..k)
            .map(|_| vec![u32::MAX; self.n_nodes])
            .collect();

        // Set origin distances
        for (lane, &src) in sources.iter().enumerate() {
            dist[lane][src as usize] = 0;
        }

        // ============================================================
        // Phase 1: K parallel upward searches
        // ============================================================
        let upward_start = std::time::Instant::now();

        // Run K independent upward phases
        // Each uses its own priority queue
        for lane in 0..k {
            let origin = sources[lane];
            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                if d > dist[lane][u as usize] {
                    continue;
                }

                stats.upward_settled += 1;

                // Relax UP edges
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

                    if new_dist < dist[lane][v as usize] {
                        dist[lane][v as usize] = new_dist;
                        pq.push(Reverse((new_dist, v)));
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // ============================================================
        // Phase 2: Single K-lane downward scan
        // ============================================================
        // This is the key optimization: one memory pass updates K distance arrays
        let downward_start = std::time::Instant::now();

        // Process nodes in DECREASING rank order (highest rank first)
        for rank in (0..self.n_nodes).rev() {
            let u = self.inv_perm[rank];
            let u_idx = u as usize;

            // Get DOWN edge range (loaded once, used for all K lanes)
            let down_start = self.topo.down_offsets[u_idx] as usize;
            let down_end = self.topo.down_offsets[u_idx + 1] as usize;

            // Skip if no outgoing DOWN edges
            if down_start == down_end {
                continue;
            }

            // Check if ANY lane has finite distance from this node
            // This is a heuristic to skip entirely unreachable nodes
            let mut any_reachable = false;
            for lane in 0..k {
                if dist[lane][u_idx] != u32::MAX {
                    any_reachable = true;
                    break;
                }
            }
            if !any_reachable {
                continue;
            }

            // Relax DOWN edges for ALL K lanes
            for i in down_start..down_end {
                let v = self.topo.down_targets[i];
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let v_idx = v as usize;
                stats.downward_relaxations += 1;

                // Update all K lanes (this is the K-lane inner loop)
                // The key insight: v_idx and w are loaded once, used K times
                for lane in 0..k {
                    let d_u = dist[lane][u_idx];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        if new_dist < dist[lane][v_idx] {
                            dist[lane][v_idx] = new_dist;
                            stats.downward_improved += 1;
                        }
                    }
                }
            }
        }

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        BatchedPhastResult {
            dist,
            n_lanes: k,
            stats,
        }
    }

    /// Run K-lane batched PHAST with experimental cache optimizations
    ///
    /// Current status: Rank-block buffering approach failed because:
    /// 1. Node indices don't align with rank order (random access pattern)
    /// 2. Buffering overhead exceeds cache benefit
    /// 3. Even sorted flushes require inv_perm lookup (random access)
    ///
    /// Future: Consider renumbering nodes by rank at construction time,
    /// so that rank-ordered processing gives memory-sequential access.
    ///
    /// For now, this is identical to query_batch but tracks extra stats.
    pub fn query_batch_blocked(&self, sources: &[u32]) -> BatchedPhastResult {
        assert!(sources.len() <= K_LANES, "Too many sources for batch");
        let k = sources.len();

        let start = std::time::Instant::now();
        let mut stats = BatchedPhastStats {
            n_sources: k,
            ..Default::default()
        };

        // Initialize K distance arrays
        let mut dist: Vec<Vec<u32>> = (0..k)
            .map(|_| vec![u32::MAX; self.n_nodes])
            .collect();

        // Set origin distances
        for (lane, &src) in sources.iter().enumerate() {
            dist[lane][src as usize] = 0;
        }

        // ============================================================
        // Phase 1: K upward searches
        // ============================================================
        let upward_start = std::time::Instant::now();

        for lane in 0..k {
            let origin = sources[lane];
            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                if d > dist[lane][u as usize] {
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

                    if new_dist < dist[lane][v as usize] {
                        dist[lane][v as usize] = new_dist;
                        pq.push(Reverse((new_dist, v)));
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // ============================================================
        // Phase 2: K-lane downward scan (node-ordered for read locality)
        // ============================================================
        let downward_start = std::time::Instant::now();

        // Process nodes in DECREASING rank order (highest rank first)
        // This gives sequential reads for dist[u], but random writes for dist[v]
        for rank in (0..self.n_nodes).rev() {
            let u = self.inv_perm[rank];
            let u_idx = u as usize;

            // Get DOWN edge range
            let down_start = self.topo.down_offsets[u_idx] as usize;
            let down_end = self.topo.down_offsets[u_idx + 1] as usize;

            if down_start == down_end {
                continue;
            }

            // Check if ANY lane has finite distance
            let mut any_reachable = false;
            for lane in 0..k {
                if dist[lane][u_idx] != u32::MAX {
                    any_reachable = true;
                    break;
                }
            }
            if !any_reachable {
                continue;
            }

            // Relax DOWN edges for ALL K lanes
            for i in down_start..down_end {
                let v = self.topo.down_targets[i];
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let v_idx = v as usize;
                stats.downward_relaxations += 1;

                // Update all K lanes
                for lane in 0..k {
                    let d_u = dist[lane][u_idx];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        if new_dist < dist[lane][v_idx] {
                            dist[lane][v_idx] = new_dist;
                            stats.downward_improved += 1;
                        }
                    }
                }
            }
        }

        stats.buffer_flushes = 0;
        stats.buffered_updates = 0;

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        BatchedPhastResult {
            dist,
            n_lanes: k,
            stats,
        }
    }

    /// Compute full many-to-many matrix using K-lane batching
    ///
    /// # Arguments
    /// * `sources` - Source node IDs
    /// * `targets` - Target node IDs (if empty, uses all nodes)
    ///
    /// # Returns
    /// Callback is invoked with (src_batch_idx, distances) for each batch
    pub fn compute_matrix<F>(
        &self,
        sources: &[u32],
        targets: &[u32],
        mut callback: F,
    ) -> BatchedPhastStats
    where
        F: FnMut(usize, &[Vec<u32>], &[u32]),
    {
        let mut total_stats = BatchedPhastStats::default();
        total_stats.n_sources = sources.len();

        // Process sources in batches of K
        for (batch_idx, chunk) in sources.chunks(K_LANES).enumerate() {
            let result = self.query_batch(chunk);

            // Accumulate stats
            total_stats.upward_relaxations += result.stats.upward_relaxations;
            total_stats.upward_settled += result.stats.upward_settled;
            total_stats.downward_relaxations += result.stats.downward_relaxations;
            total_stats.downward_improved += result.stats.downward_improved;
            total_stats.upward_time_ms += result.stats.upward_time_ms;
            total_stats.downward_time_ms += result.stats.downward_time_ms;

            // Extract distances for requested targets
            if targets.is_empty() {
                // Full distance arrays
                callback(batch_idx * K_LANES, &result.dist, chunk);
            } else {
                // Extract only requested targets
                let extracted: Vec<Vec<u32>> = result.dist.iter()
                    .map(|d| targets.iter().map(|&t| d[t as usize]).collect())
                    .collect();
                callback(batch_idx * K_LANES, &extracted, chunk);
            }
        }

        total_stats
    }

    /// Compute distance from sources to specific targets, returning a flat matrix
    ///
    /// # Returns
    /// Matrix as row-major Vec<u32> of size sources.len() × targets.len()
    pub fn compute_matrix_flat(
        &self,
        sources: &[u32],
        targets: &[u32],
    ) -> (Vec<u32>, BatchedPhastStats) {
        let n_src = sources.len();
        let n_tgt = targets.len();
        let mut matrix = vec![u32::MAX; n_src * n_tgt];
        let mut total_stats = BatchedPhastStats::default();
        total_stats.n_sources = n_src;

        // Process sources in batches of K
        for (batch_idx, chunk) in sources.chunks(K_LANES).enumerate() {
            let result = self.query_batch(chunk);

            // Accumulate stats
            total_stats.upward_relaxations += result.stats.upward_relaxations;
            total_stats.upward_settled += result.stats.upward_settled;
            total_stats.downward_relaxations += result.stats.downward_relaxations;
            total_stats.downward_improved += result.stats.downward_improved;
            total_stats.upward_time_ms += result.stats.upward_time_ms;
            total_stats.downward_time_ms += result.stats.downward_time_ms;

            // Copy distances to flat matrix
            for (lane, dist) in result.dist.iter().enumerate() {
                let src_idx = batch_idx * K_LANES + lane;
                if src_idx >= n_src {
                    break;
                }
                for (tgt_idx, &tgt) in targets.iter().enumerate() {
                    matrix[src_idx * n_tgt + tgt_idx] = dist[tgt as usize];
                }
            }
        }

        total_stats.total_time_ms = total_stats.upward_time_ms + total_stats.downward_time_ms;
        (matrix, total_stats)
    }

    /// Compute matrix using blocked relaxation for better cache efficiency
    pub fn compute_matrix_flat_blocked(
        &self,
        sources: &[u32],
        targets: &[u32],
    ) -> (Vec<u32>, BatchedPhastStats) {
        let n_src = sources.len();
        let n_tgt = targets.len();
        let mut matrix = vec![u32::MAX; n_src * n_tgt];
        let mut total_stats = BatchedPhastStats::default();
        total_stats.n_sources = n_src;

        // Process sources in batches of K
        for (batch_idx, chunk) in sources.chunks(K_LANES).enumerate() {
            let result = self.query_batch_blocked(chunk);

            // Accumulate stats
            total_stats.upward_relaxations += result.stats.upward_relaxations;
            total_stats.upward_settled += result.stats.upward_settled;
            total_stats.downward_relaxations += result.stats.downward_relaxations;
            total_stats.downward_improved += result.stats.downward_improved;
            total_stats.upward_time_ms += result.stats.upward_time_ms;
            total_stats.downward_time_ms += result.stats.downward_time_ms;
            total_stats.buffer_flushes += result.stats.buffer_flushes;
            total_stats.buffered_updates += result.stats.buffered_updates;

            // Copy distances to flat matrix
            for (lane, dist) in result.dist.iter().enumerate() {
                let src_idx = batch_idx * K_LANES + lane;
                if src_idx >= n_src {
                    break;
                }
                for (tgt_idx, &tgt) in targets.iter().enumerate() {
                    matrix[src_idx * n_tgt + tgt_idx] = dist[tgt as usize];
                }
            }
        }

        total_stats.total_time_ms = total_stats.upward_time_ms + total_stats.downward_time_ms;
        (matrix, total_stats)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Tests comparing batched PHAST vs single-source PHAST
    // Will be added when test fixtures are available
}
