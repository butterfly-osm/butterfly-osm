//! K-Lane Batched PHAST for bulk matrix computation
//!
//! Processes K sources in one downward scan, amortizing memory access cost.
//!
//! ## Rank-Aligned CCH (Version 2)
//!
//! With rank-aligned CCH, node_id == rank. This means:
//! - No inv_perm lookup in downward scan (was the root cause of cache misses)
//! - dist[rank] is accessed sequentially for cache efficiency
//! - Expected 2-4x speedup from cache improvements
//!
//! ## K-Lane Batching
//!
//! K sources processed in one downward scan, amortizing memory access cost:
//! - Load edge data once (down_targets[i], down_weights[i])
//! - Update K distance arrays: dist[0][v], dist[1][v], ..., dist[K-1][v]
//! - Gives K× improvement over sequential single-source queries
//!
//! ## SoA Layout (Structure of Arrays)
//!
//! For better cache efficiency, distances are stored as:
//! - `dist[node * K + lane]` instead of `dist[lane][node]`
//! - All K distances for a node are in one cache line (K*4 = 32 bytes for K=8)
//! - Inner loop updates K consecutive u32s (autovectorizable)

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::formats::{CchTopo, CchTopoFile, CchWeights, CchWeightsFile};

/// Lane width for batched PHAST (tunable based on cache line size)
/// K=8 gives good balance between parallelism and register pressure
pub const K_LANES: usize = 8;

/// Block size for rank-based memory blocking
pub const BLOCK_SIZE: usize = 8192;

/// Batched PHAST engine for K-lane parallel queries
///
/// # Rank-Aligned CCH
///
/// With rank-aligned CCH (Version 2), all node IDs are rank positions.
/// The query sources must be rank positions (convert from filtered_id at API layer).
/// Use `rank_to_filtered()` to convert result node IDs back to filtered space.
pub struct BatchedPhastEngine {
    /// CCH topology (rank-aligned)
    topo: CchTopo,
    /// CCH weights
    weights: CchWeights,
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
    ///
    /// # Rank-Aligned CCH
    ///
    /// With rank-aligned CCH (Version 2), the order file is no longer needed.
    /// The rank_to_filtered mapping is stored in the topology itself.
    pub fn new(topo: CchTopo, weights: CchWeights) -> Self {
        let n_nodes = topo.n_nodes as usize;

        Self {
            topo,
            weights,
            n_nodes,
        }
    }

    /// Load batched PHAST engine from file paths
    ///
    /// Note: order_path is kept for backward compatibility but is no longer used.
    /// With rank-aligned CCH, the mapping is in the topology file.
    pub fn load(
        topo_path: &std::path::Path,
        weights_path: &std::path::Path,
        _order_path: &std::path::Path, // Unused with rank-aligned CCH
    ) -> anyhow::Result<Self> {
        let topo = CchTopoFile::read(topo_path)?;
        let weights = CchWeightsFile::read(weights_path)?;

        Ok(Self::new(topo, weights))
    }

    /// Get number of nodes
    pub fn n_nodes(&self) -> usize {
        self.n_nodes
    }

    /// Get the rank_to_filtered mapping for converting results to filtered space
    pub fn rank_to_filtered(&self) -> &[u32] {
        &self.topo.rank_to_filtered
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
        let mut dist: Vec<Vec<u32>> = (0..k).map(|_| vec![u32::MAX; self.n_nodes]).collect();

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
        //
        // RANK-ALIGNED CCH: node_id == rank, sequential memory access!
        // - dist[rank] accessed in order (was: dist[inv_perm[rank]] = random)
        // - offsets[rank] accessed in order
        // - Combined with K-lane batching = significant speedup
        let downward_start = std::time::Instant::now();

        // Process nodes in DECREASING rank order (highest rank first)
        for rank in (0..self.n_nodes).rev() {
            // With rank-aligned CCH: u = rank (no inv_perm lookup!)
            let u = rank;

            // Get DOWN edge range (loaded once, used for all K lanes)
            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            // Skip if no outgoing DOWN edges
            if down_start == down_end {
                continue;
            }

            // Check if ANY lane has finite distance from this node
            // This is a heuristic to skip entirely unreachable nodes
            let any_reachable = dist[..k].iter().any(|d| d[u] != u32::MAX);
            if !any_reachable {
                continue;
            }

            // Relax DOWN edges for ALL K lanes
            for i in down_start..down_end {
                // v is the target's rank (rank-aligned CCH)
                let v = self.topo.down_targets[i] as usize;
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                stats.downward_relaxations += 1;

                // Update all K lanes (this is the K-lane inner loop)
                // The key insight: v and w are loaded once, used K times
                for d_lane in &mut dist[..k] {
                    let d_u = d_lane[u];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        if new_dist < d_lane[v] {
                            d_lane[v] = new_dist;
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

    /// Run K-lane batched PHAST with early-stop and lane masking for bounded queries
    ///
    /// Key optimizations for small thresholds:
    /// 1. Early-stop upward phase: stop each lane when heap min > threshold
    /// 2. Track active blocks per lane: only process lanes with reachable nodes
    /// 3. Lane masking in downward: skip lanes that have no active work
    ///
    /// This makes batched competitive with single-source for small thresholds,
    /// while maintaining amortization benefits for large thresholds.
    pub fn query_batch_bounded(&self, sources: &[u32], threshold: u32) -> BatchedPhastResult {
        assert!(sources.len() <= K_LANES, "Too many sources for batch");
        let k = sources.len();

        let start = std::time::Instant::now();
        let mut stats = BatchedPhastStats {
            n_sources: k,
            ..Default::default()
        };

        // Initialize K distance arrays
        let mut dist: Vec<Vec<u32>> = (0..k).map(|_| vec![u32::MAX; self.n_nodes]).collect();

        // Set origin distances
        for (lane, &src) in sources.iter().enumerate() {
            dist[lane][src as usize] = 0;
        }

        // Track which lanes are still active (have nodes within threshold)
        let mut lane_active = [true; K_LANES];
        for la in &mut lane_active[k..K_LANES] {
            *la = false;
        }

        // Per-lane active block bitsets for downward gating
        const BLOCK_SIZE: usize = 512;
        let n_blocks = self.n_nodes.div_ceil(BLOCK_SIZE);
        let n_words = n_blocks.div_ceil(64);
        let mut active_blocks: Vec<Vec<u64>> = (0..k).map(|_| vec![0u64; n_words]).collect();

        // Mark origin blocks as active
        for (lane, &src) in sources.iter().enumerate() {
            let block = src as usize / BLOCK_SIZE;
            active_blocks[lane][block / 64] |= 1u64 << (block % 64);
        }

        // ============================================================
        // Phase 1: K upward searches with early-stop per lane
        // ============================================================
        let upward_start = std::time::Instant::now();

        for lane in 0..k {
            let origin = sources[lane];
            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                // Early stop: if current min distance exceeds threshold, this lane is done
                if d > threshold {
                    break;
                }

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

                        // Track active blocks if within threshold
                        if new_dist <= threshold {
                            let v_block = v as usize / BLOCK_SIZE;
                            active_blocks[lane][v_block / 64] |= 1u64 << (v_block % 64);
                        }
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // Count active blocks per lane and mark inactive lanes
        let mut lane_block_counts = [0usize; K_LANES];
        for lane in 0..k {
            lane_block_counts[lane] = active_blocks[lane]
                .iter()
                .map(|w| w.count_ones() as usize)
                .sum();
            if lane_block_counts[lane] == 0 {
                lane_active[lane] = false;
            }
        }

        // ============================================================
        // Phase 2: K-lane downward scan with lane masking
        // ============================================================
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = rank;
            let block = u / BLOCK_SIZE;
            let word_idx = block / 64;
            let bit = 1u64 << (block % 64);

            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            if down_start == down_end {
                continue;
            }

            // Check which lanes have this block active
            let mut any_lane_active = false;
            for lane in 0..k {
                if lane_active[lane] && (active_blocks[lane][word_idx] & bit) != 0 {
                    any_lane_active = true;
                    break;
                }
            }
            if !any_lane_active {
                continue;
            }

            // Relax DOWN edges for active lanes only
            for i in down_start..down_end {
                let v = self.topo.down_targets[i] as usize;
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                stats.downward_relaxations += 1;

                // Update only active lanes with this block active
                for lane in 0..k {
                    if !lane_active[lane] {
                        continue;
                    }
                    if (active_blocks[lane][word_idx] & bit) == 0 {
                        continue;
                    }

                    let d_u = dist[lane][u];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        if new_dist <= threshold && new_dist < dist[lane][v] {
                            dist[lane][v] = new_dist;
                            stats.downward_improved += 1;

                            // Mark target block as active
                            let v_block = v / BLOCK_SIZE;
                            active_blocks[lane][v_block / 64] |= 1u64 << (v_block % 64);
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

    /// Run K-lane batched PHAST with rank-aligned CCH
    ///
    /// With rank-aligned CCH (Version 2), this method achieves cache efficiency:
    /// - node_id == rank, so dist[rank] is sequential access
    /// - No inv_perm lookup needed (was the root cause of cache misses)
    /// - Expected 2-4x speedup vs non-rank-aligned version
    ///
    /// This method is now identical to query_batch (both use rank-aligned access).
    /// Kept as separate method for benchmarking and comparison with old results.
    pub fn query_batch_blocked(&self, sources: &[u32]) -> BatchedPhastResult {
        // With rank-aligned CCH, query_batch already has optimal cache access
        // This method is now equivalent - both benefit from sequential dist[rank] access
        self.query_batch(sources)
    }

    /// Run K-lane batched PHAST with SoA (Structure of Arrays) layout
    ///
    /// This is the most cache-efficient version:
    /// - Distances stored as `dist[node * K + lane]` (K distances per node contiguous)
    /// - All K distances for a node fit in one cache line
    /// - Inner loop is autovectorizable (K consecutive u32 updates)
    ///
    /// Expected 2-4x speedup over AoS layout (query_batch).
    pub fn query_batch_soa(&self, sources: &[u32]) -> BatchedPhastResult {
        assert!(sources.len() <= K_LANES, "Too many sources for batch");
        let k = sources.len();

        let start = std::time::Instant::now();
        let mut stats = BatchedPhastStats {
            n_sources: k,
            ..Default::default()
        };

        // Initialize SoA distance array: dist[node * K_LANES + lane]
        // All K distances for a node are contiguous (cache-line friendly)
        let mut dist_soa: Vec<u32> = vec![u32::MAX; self.n_nodes * K_LANES];

        // Set origin distances
        for (lane, &src) in sources.iter().enumerate() {
            dist_soa[src as usize * K_LANES + lane] = 0;
        }

        // ============================================================
        // Phase 1: K parallel upward searches (same as before)
        // ============================================================
        let upward_start = std::time::Instant::now();

        for (lane, &origin) in sources[..k].iter().enumerate() {
            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                let u_idx = u as usize * K_LANES + lane;
                if d > dist_soa[u_idx] {
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

                    let v_idx = v as usize * K_LANES + lane;
                    if new_dist < dist_soa[v_idx] {
                        dist_soa[v_idx] = new_dist;
                        pq.push(Reverse((new_dist, v)));
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // ============================================================
        // Phase 2: SoA K-lane downward scan
        // ============================================================
        // Key optimization: K distances for each node are contiguous
        // Loading dist[u*K..u*K+K] loads all K lanes in one cache line
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = rank;
            let u_base = u * K_LANES;

            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            if down_start == down_end {
                continue;
            }

            // Check if ANY lane has finite distance (fast path to skip unreachable)
            // With SoA, we can check K consecutive u32s
            let any_reachable = dist_soa[u_base..u_base + k].iter().any(|&d| d != u32::MAX);
            if !any_reachable {
                continue;
            }

            // Load all K distances for node u (one cache line)
            // This is the key win: all K values loaded together
            let du: [u32; K_LANES] = {
                let mut arr = [u32::MAX; K_LANES];
                arr[..k].copy_from_slice(&dist_soa[u_base..(k + u_base)]);
                arr
            };

            for i in down_start..down_end {
                let v = self.topo.down_targets[i] as usize;
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                stats.downward_relaxations += 1;

                let v_base = v * K_LANES;

                // SoA inner loop: update K consecutive u32s
                // This pattern is autovectorizable by LLVM
                for lane in 0..k {
                    let d_u = du[lane];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        let dv_ref = &mut dist_soa[v_base + lane];
                        if new_dist < *dv_ref {
                            *dv_ref = new_dist;
                            stats.downward_improved += 1;
                        }
                    }
                }
            }
        }

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        // Convert SoA back to AoS for result (to maintain API compatibility)
        let dist: Vec<Vec<u32>> = (0..k)
            .map(|lane| {
                (0..self.n_nodes)
                    .map(|node| dist_soa[node * K_LANES + lane])
                    .collect()
            })
            .collect();

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
        let mut total_stats = BatchedPhastStats {
            n_sources: sources.len(),
            ..Default::default()
        };

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
                let extracted: Vec<Vec<u32>> = result
                    .dist
                    .iter()
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
        let mut total_stats = BatchedPhastStats {
            n_sources: n_src,
            ..Default::default()
        };

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
        let mut total_stats = BatchedPhastStats {
            n_sources: n_src,
            ..Default::default()
        };

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

    /// Compute matrix using SoA layout for better cache efficiency
    ///
    /// Uses Structure of Arrays layout where K distances per node are contiguous.
    /// Extracts targets directly from SoA layout without full conversion.
    pub fn compute_matrix_flat_soa(
        &self,
        sources: &[u32],
        targets: &[u32],
    ) -> (Vec<u32>, BatchedPhastStats) {
        let n_src = sources.len();
        let n_tgt = targets.len();
        let mut matrix = vec![u32::MAX; n_src * n_tgt];
        let mut total_stats = BatchedPhastStats {
            n_sources: n_src,
            ..Default::default()
        };

        // Process sources in batches of K using SoA layout
        for (batch_idx, chunk) in sources.chunks(K_LANES).enumerate() {
            let result = self.query_batch_soa_raw(chunk);

            // Accumulate stats
            total_stats.upward_relaxations += result.stats.upward_relaxations;
            total_stats.upward_settled += result.stats.upward_settled;
            total_stats.downward_relaxations += result.stats.downward_relaxations;
            total_stats.downward_improved += result.stats.downward_improved;
            total_stats.upward_time_ms += result.stats.upward_time_ms;
            total_stats.downward_time_ms += result.stats.downward_time_ms;

            // Extract target distances directly from SoA layout (no full conversion)
            for (lane, &_src) in chunk.iter().enumerate() {
                let src_idx = batch_idx * K_LANES + lane;
                if src_idx >= n_src {
                    break;
                }
                for (tgt_idx, &tgt) in targets.iter().enumerate() {
                    // SoA access: dist[tgt * K_LANES + lane]
                    matrix[src_idx * n_tgt + tgt_idx] =
                        result.dist_soa[tgt as usize * K_LANES + lane];
                }
            }
        }

        total_stats.total_time_ms = total_stats.upward_time_ms + total_stats.downward_time_ms;
        (matrix, total_stats)
    }

    /// Raw SoA query that returns distances in SoA layout without conversion
    fn query_batch_soa_raw(&self, sources: &[u32]) -> BatchedPhastResultSoa {
        assert!(sources.len() <= K_LANES, "Too many sources for batch");
        let k = sources.len();

        let start = std::time::Instant::now();
        let mut stats = BatchedPhastStats {
            n_sources: k,
            ..Default::default()
        };

        // Initialize SoA distance array
        let mut dist_soa: Vec<u32> = vec![u32::MAX; self.n_nodes * K_LANES];

        // Set origin distances
        for (lane, &src) in sources.iter().enumerate() {
            dist_soa[src as usize * K_LANES + lane] = 0;
        }

        // Phase 1: K parallel upward searches
        let upward_start = std::time::Instant::now();

        for (lane, &origin) in sources[..k].iter().enumerate() {
            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                let u_idx = u as usize * K_LANES + lane;
                if d > dist_soa[u_idx] {
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

                    let v_idx = v as usize * K_LANES + lane;
                    if new_dist < dist_soa[v_idx] {
                        dist_soa[v_idx] = new_dist;
                        pq.push(Reverse((new_dist, v)));
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // Phase 2: SoA K-lane downward scan
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = rank;
            let u_base = u * K_LANES;

            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            if down_start == down_end {
                continue;
            }

            // Check if ANY lane has finite distance
            let any_reachable = dist_soa[u_base..u_base + k].iter().any(|&d| d != u32::MAX);
            if !any_reachable {
                continue;
            }

            // Load all K distances for node u
            let du: [u32; K_LANES] = {
                let mut arr = [u32::MAX; K_LANES];
                arr[..k].copy_from_slice(&dist_soa[u_base..(k + u_base)]);
                arr
            };

            for i in down_start..down_end {
                let v = self.topo.down_targets[i] as usize;
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                stats.downward_relaxations += 1;

                let v_base = v * K_LANES;

                // SoA inner loop: update K consecutive u32s
                for lane in 0..k {
                    let d_u = du[lane];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        let dv_ref = &mut dist_soa[v_base + lane];
                        if new_dist < *dv_ref {
                            *dv_ref = new_dist;
                            stats.downward_improved += 1;
                        }
                    }
                }
            }
        }

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        BatchedPhastResultSoa {
            dist_soa,
            n_lanes: k,
            stats,
        }
    }
}

/// Result of SoA query (raw layout, no conversion)
struct BatchedPhastResultSoa {
    /// Distance array in SoA layout: dist[node * K_LANES + lane]
    dist_soa: Vec<u32>,
    #[allow(dead_code)]
    n_lanes: usize,
    stats: BatchedPhastStats,
}

/// Block size for K-lane active gating
const KLANE_BLOCK_SIZE: usize = 4096;

impl BatchedPhastEngine {
    /// Run K-lane batched PHAST with block-level active gating for bounded queries
    ///
    /// This combines the benefits of:
    /// 1. K-lane batching (amortize memory access across K sources)
    /// 2. SoA layout (cache-friendly for vectorization)
    /// 3. Block-level active gating (skip inactive blocks entirely)
    /// 4. Lane masking (skip adjacency when all lanes are beyond threshold)
    /// 5. Adaptive switching (plain scan when most blocks are active)
    ///
    /// # Arguments
    /// * `sources` - Up to K source node IDs
    /// * `threshold` - Time threshold for bounded query (u32::MAX for unbounded)
    ///
    /// # Returns
    /// BatchedPhastResult with distances to all nodes within threshold
    pub fn query_batch_block_gated(&self, sources: &[u32], threshold: u32) -> BatchedPhastResult {
        assert!(sources.len() <= K_LANES, "Too many sources for batch");
        let k = sources.len();

        // For unbounded queries or small graphs, use plain batched PHAST
        if threshold == u32::MAX || self.n_nodes < KLANE_BLOCK_SIZE * 4 {
            return self.query_batch(sources);
        }

        // Heuristic: For large thresholds (>5 min), block gating usually doesn't help
        // because most of the graph becomes reachable. Skip the block tracking overhead
        // and use plain batched PHAST directly.
        // Threshold is in milliseconds: 5 min = 300,000 ms
        const LARGE_THRESHOLD_MS: u32 = 300_000;
        if threshold >= LARGE_THRESHOLD_MS {
            return self.query_batch(sources);
        }

        let start = std::time::Instant::now();
        let mut stats = BatchedPhastStats {
            n_sources: k,
            ..Default::default()
        };

        // Initialize SoA distance array: dist[node * K_LANES + lane]
        // All K distances for a node are contiguous (cache-line friendly)
        let mut dist_soa: Vec<u32> = vec![u32::MAX; self.n_nodes * K_LANES];

        // Set origin distances
        for (lane, &src) in sources.iter().enumerate() {
            dist_soa[src as usize * K_LANES + lane] = 0;
        }

        // Per-lane active block bitsets
        let n_blocks = self.n_nodes.div_ceil(KLANE_BLOCK_SIZE);
        let n_words = n_blocks.div_ceil(64);
        let mut active_blocks: Vec<Vec<u64>> = (0..k).map(|_| vec![0u64; n_words]).collect();

        // Mark origin blocks as active
        for (lane, &src) in sources.iter().enumerate() {
            let block = src as usize / KLANE_BLOCK_SIZE;
            active_blocks[lane][block / 64] |= 1u64 << (block % 64);
        }

        // ============================================================
        // Phase 1: K parallel upward searches with block tracking (SoA)
        // ============================================================
        let upward_start = std::time::Instant::now();

        for lane in 0..k {
            let origin = sources[lane];
            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                let u_idx = u as usize * K_LANES + lane;
                if d > dist_soa[u_idx] {
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

                    let v_idx = v as usize * K_LANES + lane;
                    if new_dist < dist_soa[v_idx] {
                        dist_soa[v_idx] = new_dist;
                        pq.push(Reverse((new_dist, v)));

                        // Mark block as active if within threshold
                        if new_dist <= threshold {
                            let v_block = v as usize / KLANE_BLOCK_SIZE;
                            active_blocks[lane][v_block / 64] |= 1u64 << (v_block % 64);
                        }
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // ============================================================
        // Compute combined active blocks (active if ANY lane has it active)
        // ============================================================
        let mut combined_active = vec![0u64; n_words];
        for ab in &active_blocks[..k] {
            for (i, &word) in ab.iter().enumerate() {
                combined_active[i] |= word;
            }
        }

        // Count active blocks for adaptive decision
        let active_block_count: usize = combined_active
            .iter()
            .map(|w| w.count_ones() as usize)
            .sum();
        let active_ratio = active_block_count as f64 / n_blocks as f64;

        // If most blocks are active (>30%), skip gating overhead
        const GATING_THRESHOLD: f64 = 0.30;

        // ============================================================
        // Phase 2: SoA downward scan (adaptive: plain or block-gated)
        // ============================================================
        let downward_start = std::time::Instant::now();

        if active_ratio > GATING_THRESHOLD {
            // High active ratio: run plain SoA downward scan (no gating overhead)
            // This path is identical to query_batch's downward phase
            for rank in (0..self.n_nodes).rev() {
                let u = rank;
                let u_base = u * K_LANES;

                let down_start = self.topo.down_offsets[u] as usize;
                let down_end = self.topo.down_offsets[u + 1] as usize;

                if down_start == down_end {
                    continue;
                }

                // Check if ANY lane has finite distance (fast path)
                let mut any_reachable = false;
                for lane in 0..k {
                    if dist_soa[u_base + lane] != u32::MAX {
                        any_reachable = true;
                        break;
                    }
                }
                if !any_reachable {
                    continue;
                }

                // Load all K distances for node u
                let du: [u32; K_LANES] = {
                    let mut arr = [u32::MAX; K_LANES];
                    arr[..k].copy_from_slice(&dist_soa[u_base..(k + u_base)]);
                    arr
                };

                for i in down_start..down_end {
                    let v = self.topo.down_targets[i] as usize;
                    let w = self.weights.down[i];

                    if w == u32::MAX {
                        continue;
                    }

                    stats.downward_relaxations += 1;

                    let v_base = v * K_LANES;

                    // SoA inner loop (autovectorizable)
                    for lane in 0..k {
                        let d_u = du[lane];
                        if d_u != u32::MAX {
                            let new_dist = d_u.saturating_add(w);
                            let dv_ref = &mut dist_soa[v_base + lane];
                            if new_dist < *dv_ref {
                                *dv_ref = new_dist;
                                stats.downward_improved += 1;
                            }
                        }
                    }
                }
            }
        } else {
            // Low active ratio: run block-gated SoA downward scan
            for block_idx in (0..n_blocks).rev() {
                // Check if ANY lane has this block active
                let is_active = (combined_active[block_idx / 64] >> (block_idx % 64)) & 1 != 0;
                if !is_active {
                    stats.buffer_flushes += 1; // Repurpose as blocks_skipped
                    continue;
                }
                stats.buffered_updates += 1; // Repurpose as blocks_processed

                let rank_start = block_idx * KLANE_BLOCK_SIZE;
                let rank_end = std::cmp::min((block_idx + 1) * KLANE_BLOCK_SIZE, self.n_nodes);

                for rank in (rank_start..rank_end).rev() {
                    let u = rank;
                    let u_base = u * K_LANES;

                    let down_start = self.topo.down_offsets[u] as usize;
                    let down_end = self.topo.down_offsets[u + 1] as usize;

                    if down_start == down_end {
                        continue;
                    }

                    // Load all K distances for node u AND compute lane mask in one pass
                    let mut du = [u32::MAX; K_LANES];
                    let mut lane_mask: u8 = 0;
                    for lane in 0..k {
                        let d_u = dist_soa[u_base + lane];
                        du[lane] = d_u;
                        if d_u != u32::MAX && d_u <= threshold {
                            lane_mask |= 1u8 << lane;
                        }
                    }

                    // Skip adjacency if no lanes are active
                    if lane_mask == 0 {
                        continue;
                    }

                    // Relax DOWN edges for active lanes only
                    for i in down_start..down_end {
                        let v = self.topo.down_targets[i] as usize;
                        let w = self.weights.down[i];

                        if w == u32::MAX {
                            continue;
                        }

                        stats.downward_relaxations += 1;

                        let v_base = v * K_LANES;

                        // Update only lanes in the mask
                        for lane in 0..k {
                            if (lane_mask >> lane) & 1 != 0 {
                                let d_u = du[lane];
                                let new_dist = d_u.saturating_add(w);
                                let dv_ref = &mut dist_soa[v_base + lane];
                                if new_dist < *dv_ref {
                                    *dv_ref = new_dist;
                                    stats.downward_improved += 1;

                                    // Mark destination block as active for this lane
                                    if new_dist <= threshold {
                                        let v_block = v / KLANE_BLOCK_SIZE;
                                        active_blocks[lane][v_block / 64] |= 1u64 << (v_block % 64);
                                        combined_active[v_block / 64] |= 1u64 << (v_block % 64);
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }

        stats.downward_time_ms = downward_start.elapsed().as_millis() as u64;
        stats.total_time_ms = start.elapsed().as_millis() as u64;

        // Convert SoA back to AoS for result (to maintain API compatibility)
        let dist: Vec<Vec<u32>> = (0..k)
            .map(|lane| {
                (0..self.n_nodes)
                    .map(|node| dist_soa[node * K_LANES + lane])
                    .collect()
            })
            .collect();

        BatchedPhastResult {
            dist,
            n_lanes: k,
            stats,
        }
    }

    /// Compute distance matrix with K-lane block-gated PHAST for bounded queries
    ///
    /// This is the bulk matrix computation path optimized for bounded isochrones.
    pub fn compute_matrix_block_gated(
        &self,
        sources: &[u32],
        targets: &[u32],
        threshold: u32,
    ) -> (Vec<u32>, BatchedPhastStats) {
        let n_src = sources.len();
        let n_tgt = targets.len();
        let mut matrix = vec![u32::MAX; n_src * n_tgt];
        let mut total_stats = BatchedPhastStats {
            n_sources: n_src,
            ..Default::default()
        };

        // Process sources in batches of K
        for (batch_idx, chunk) in sources.chunks(K_LANES).enumerate() {
            let result = self.query_batch_block_gated(chunk, threshold);

            // Accumulate stats
            total_stats.upward_relaxations += result.stats.upward_relaxations;
            total_stats.upward_settled += result.stats.upward_settled;
            total_stats.downward_relaxations += result.stats.downward_relaxations;
            total_stats.downward_improved += result.stats.downward_improved;
            total_stats.upward_time_ms += result.stats.upward_time_ms;
            total_stats.downward_time_ms += result.stats.downward_time_ms;
            total_stats.buffer_flushes += result.stats.buffer_flushes;
            total_stats.buffered_updates += result.stats.buffered_updates;

            // Extract target distances
            for (lane, _) in chunk.iter().enumerate() {
                let src_idx = batch_idx * K_LANES + lane;
                if src_idx >= n_src {
                    break;
                }
                for (tgt_idx, &tgt) in targets.iter().enumerate() {
                    matrix[src_idx * n_tgt + tgt_idx] = result.dist[lane][tgt as usize];
                }
            }
        }

        total_stats.total_time_ms = total_stats.upward_time_ms + total_stats.downward_time_ms;
        (matrix, total_stats)
    }
}

#[cfg(test)]
mod tests {
    // Tests comparing batched PHAST vs single-source PHAST
    // Will be added when test fixtures are available
}
