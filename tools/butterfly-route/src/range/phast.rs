//! PHAST (PHAst Shortest-path Trees) for efficient one-to-many queries
//!
//! Two-phase algorithm:
//! 1. Upward phase: PQ-based Dijkstra using only UP edges from origin
//! 2. Downward phase: Linear scan in reverse rank order, relaxing DOWN edges
//!
//! The downward phase is O(n) with no priority queue, making it much faster
//! than naive Dijkstra for large reachable sets.
//!
//! # Rank-Aligned CCH (Version 2)
//!
//! With rank-aligned CCH, node_id == rank. This means:
//! - No inv_perm lookup needed in downward scan
//! - `dist[rank]` is sequential access
//! - Significant cache efficiency improvement

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use crate::formats::{CchTopo, CchWeights};
use crate::matrix::bucket_ch::{DownReverseAdjFlat, UpAdjFlat};

/// PHAST query engine
///
/// # Rank-Aligned CCH
///
/// With rank-aligned CCH (Version 2), all node IDs are rank positions.
/// The query origin must be a rank position (convert from filtered_id at API layer).
/// Use `rank_to_filtered()` to convert result node IDs back to filtered space for geometry.
pub struct PhastEngine {
    /// CCH topology (rank-aligned)
    topo: CchTopo,
    /// CCH weights
    weights: CchWeights,
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
    /// Blocks skipped due to block-level gating
    pub blocks_skipped: usize,
    /// Blocks processed
    pub blocks_processed: usize,
    /// Nodes skipped within active blocks (du > threshold or INF)
    pub nodes_skipped_in_block: usize,
}

/// Block size for active gating (number of ranks per block)
///
/// Choice rationale:
/// - 4096 = good cache line alignment (4096 * 4 bytes = 16KB = fits in L1)
/// - ~600 blocks for 2.4M nodes → tiny bitset (~75 bytes)
/// - Large enough to skip meaningful work
/// - Small enough for fine-grained gating
pub const BLOCK_SIZE: usize = 4096;

impl PhastEngine {
    /// Create PHAST engine from loaded CCH data
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

    /// Load PHAST engine from file paths
    ///
    /// Note: order_path is kept for backward compatibility but is no longer used.
    /// With rank-aligned CCH, the mapping is in the topology file.
    pub fn load(
        topo_path: &std::path::Path,
        weights_path: &std::path::Path,
        _order_path: &std::path::Path,  // Unused with rank-aligned CCH
    ) -> anyhow::Result<Self> {
        use crate::formats::{CchTopoFile, CchWeightsFile};

        let topo = CchTopoFile::read(topo_path)?;
        let weights = CchWeightsFile::read(weights_path)?;

        Ok(Self::new(topo, weights))
    }

    /// Get the rank_to_filtered mapping for converting results to filtered space
    pub fn rank_to_filtered(&self) -> &[u32] {
        &self.topo.rank_to_filtered
    }

    /// Get total number of down-edges in the CCH
    pub fn total_down_edges(&self) -> usize {
        self.topo.down_targets.len()
    }

    /// Compute reachability metrics for a given distance array and threshold
    ///
    /// Returns (reachable_nodes, reachable_edges, total_nodes, total_edges)
    /// where reachable_edges are down-edges originating from nodes with dist <= threshold
    pub fn compute_reachability(&self, dist: &[u32], threshold: u32) -> (usize, usize, usize, usize) {
        let total_nodes = self.n_nodes;
        let total_edges = self.topo.down_targets.len();

        let mut reachable_nodes = 0usize;
        let mut reachable_edges = 0usize;

        for u in 0..self.n_nodes {
            if dist[u] != u32::MAX && dist[u] <= threshold {
                reachable_nodes += 1;
                // Count down-edges from this node
                let down_start = self.topo.down_offsets[u] as usize;
                let down_end = self.topo.down_offsets[u + 1] as usize;
                reachable_edges += down_end - down_start;
            }
        }

        (reachable_nodes, reachable_edges, total_nodes, total_edges)
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
        //
        // RANK-ALIGNED CCH: node_id == rank, so no inv_perm lookup needed!
        // dist[rank] is accessed sequentially for cache efficiency.
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            // With rank-aligned CCH: u = rank (no inv_perm lookup!)
            let u = rank;
            let d_u = dist[u];

            // Skip unreachable nodes
            if d_u == u32::MAX {
                continue;
            }

            // Relax DOWN edges (offsets indexed by rank in rank-aligned CCH)
            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            for i in down_start..down_end {
                // v is the target's rank (rank-aligned CCH)
                let v = self.topo.down_targets[i] as usize;
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[v] {
                    dist[v] = new_dist;
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

    /// Run bounded PHAST query with adaptive gating strategy
    ///
    /// Automatically chooses between block-gated and plain PHAST based on
    /// the estimated active block ratio after the upward phase.
    ///
    /// - If active_blocks / total_blocks > GATING_THRESHOLD, gating overhead
    ///   won't pay off, so we run plain PHAST.
    /// - Otherwise, we run block-gated PHAST for better performance.
    pub fn query_bounded(&self, origin: u32, threshold: u32) -> PhastResult {
        self.query_adaptive(origin, threshold)
    }

    /// Adaptive bounded PHAST that switches strategy based on active block ratio
    ///
    /// This avoids gating overhead when most of the graph is reachable.
    pub fn query_adaptive(&self, origin: u32, threshold: u32) -> PhastResult {
        // Threshold for switching: if >25% of blocks will be active, skip gating
        const GATING_THRESHOLD: f64 = 0.25;

        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[origin as usize] = 0;

        // Block tracking for adaptive decision
        let n_blocks = (self.n_nodes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let n_words = (n_blocks + 63) / 64;
        let mut active_blocks = vec![0u64; n_words];

        // Mark origin's block as active
        let origin_block = origin as usize / BLOCK_SIZE;
        active_blocks[origin_block / 64] |= 1u64 << (origin_block % 64);

        // ============================================================
        // Phase 1: Upward search (same for all variants)
        // ============================================================
        let upward_start = std::time::Instant::now();

        let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        pq.push(Reverse((0, origin)));
        stats.upward_pq_pushes += 1;

        while let Some(Reverse((d, u))) = pq.pop() {
            stats.upward_pq_pops += 1;

            // Early stop: if current min distance exceeds threshold, no point continuing
            // All remaining nodes in the heap have d >= current d, so they're all beyond threshold
            if d > threshold {
                break;
            }

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

                    // Track active blocks if within threshold
                    if new_dist <= threshold {
                        let v_block = v as usize / BLOCK_SIZE;
                        active_blocks[v_block / 64] |= 1u64 << (v_block % 64);
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // ============================================================
        // Adaptive decision: count active blocks and choose strategy
        // ============================================================
        let active_block_count: usize = active_blocks.iter()
            .map(|w| w.count_ones() as usize)
            .sum();
        let active_ratio = active_block_count as f64 / n_blocks as f64;

        // ============================================================
        // Phase 2: Downward scan (adaptive strategy)
        // ============================================================
        let downward_start = std::time::Instant::now();

        if active_ratio > GATING_THRESHOLD {
            // High active ratio: run plain downward scan (no gating overhead)
            for rank in (0..self.n_nodes).rev() {
                let u = rank;
                let d_u = dist[u];

                if d_u == u32::MAX {
                    continue;
                }

                let down_start = self.topo.down_offsets[u] as usize;
                let down_end = self.topo.down_offsets[u + 1] as usize;

                for i in down_start..down_end {
                    let v = self.topo.down_targets[i] as usize;
                    let w = self.weights.down[i];

                    if w == u32::MAX {
                        continue;
                    }

                    let new_dist = d_u.saturating_add(w);
                    stats.downward_relaxations += 1;

                    if new_dist < dist[v] {
                        dist[v] = new_dist;
                        stats.downward_improved += 1;
                    }
                }
            }
        } else {
            // Low active ratio: run block-gated downward scan
            for block_idx in (0..n_blocks).rev() {
                let is_active = (active_blocks[block_idx / 64] >> (block_idx % 64)) & 1 != 0;
                if !is_active {
                    stats.blocks_skipped += 1;
                    continue;
                }
                stats.blocks_processed += 1;

                let rank_start = block_idx * BLOCK_SIZE;
                let rank_end = std::cmp::min((block_idx + 1) * BLOCK_SIZE, self.n_nodes);

                for rank in (rank_start..rank_end).rev() {
                    let u = rank;
                    let d_u = dist[u];

                    if d_u == u32::MAX || d_u > threshold {
                        stats.nodes_skipped_in_block += 1;
                        continue;
                    }

                    let down_start = self.topo.down_offsets[u] as usize;
                    let down_end = self.topo.down_offsets[u + 1] as usize;

                    for i in down_start..down_end {
                        let v = self.topo.down_targets[i] as usize;
                        let w = self.weights.down[i];

                        if w == u32::MAX {
                            continue;
                        }

                        let new_dist = d_u.saturating_add(w);
                        stats.downward_relaxations += 1;

                        if new_dist < dist[v] {
                            dist[v] = new_dist;
                            stats.downward_improved += 1;

                            if new_dist <= threshold {
                                let v_block = v / BLOCK_SIZE;
                                active_blocks[v_block / 64] |= 1u64 << (v_block % 64);
                            }
                        }
                    }
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

            // Early stop: if current min distance exceeds threshold, no point continuing
            if d > threshold {
                break;
            }

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
        // RANK-ALIGNED CCH: node_id == rank, sequential memory access
        let downward_start = std::time::Instant::now();

        // Count nodes skipped due to active-set gating
        let mut nodes_skipped = 0usize;

        for rank in (0..self.n_nodes).rev() {
            // With rank-aligned CCH: u = rank (no inv_perm lookup!)
            let u = rank;

            // Check if node is in active set
            let is_active = (active[u / 64] >> (u % 64)) & 1 != 0;
            if !is_active {
                nodes_skipped += 1;
                continue;
            }

            let d_u = dist[u];

            // Skip if unreachable (shouldn't happen if active, but defensive)
            if d_u == u32::MAX {
                continue;
            }

            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            for i in down_start..down_end {
                // v is the target's rank (rank-aligned CCH)
                let v = self.topo.down_targets[i] as usize;
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[v] {
                    dist[v] = new_dist;
                    stats.downward_improved += 1;

                    // If new distance is within threshold, mark v as active
                    // This allows paths to propagate through v in later iterations
                    // Note: since we process in decreasing rank order, v has lower rank
                    // and will be processed later, so marking it active now is useful
                    if new_dist <= threshold {
                        active[v / 64] |= 1u64 << (v % 64);
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
        // RANK-ALIGNED CCH: node_id == rank, sequential memory access
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            // With rank-aligned CCH: u = rank (no inv_perm lookup!)
            let u = rank;
            let d_u = dist[u];

            if d_u == u32::MAX {
                continue;
            }

            let down_start = self.topo.down_offsets[u] as usize;
            let down_end = self.topo.down_offsets[u + 1] as usize;

            for i in down_start..down_end {
                // v is the target's rank (rank-aligned CCH)
                let v = self.topo.down_targets[i] as usize;
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[v] {
                    dist[v] = new_dist;
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

    /// Run PHAST with block-level active gating
    ///
    /// **Key optimization over per-node active set:**
    /// - Partitions ranks into blocks of BLOCK_SIZE (4096)
    /// - Maintains a tiny block-level bitset (~75 bytes for 2.4M nodes)
    /// - Outer loop iterates blocks in descending order
    /// - Skips entire inactive blocks (not just individual nodes)
    /// - Within active blocks, early-exit nodes with dist > threshold
    ///
    /// For bounded queries (isochrones), this can skip 80-95% of blocks,
    /// giving order-of-magnitude speedups for small thresholds.
    pub fn query_block_gated(&self, origin: u32, threshold: u32) -> PhastResult {
        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[origin as usize] = 0;

        // Block-level active set
        // A block is active if it contains any node with dist ≤ threshold
        // OR could receive updates ≤ threshold
        let n_blocks = (self.n_nodes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let n_words = (n_blocks + 63) / 64;
        let mut active_blocks = vec![0u64; n_words];

        // Mark origin's block as active
        let origin_block = origin as usize / BLOCK_SIZE;
        active_blocks[origin_block / 64] |= 1u64 << (origin_block % 64);

        // Phase 1: Upward search with block tracking
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

                    // Mark v's block as active if dist within threshold
                    // This ensures the block will be processed in downward phase
                    if new_dist <= threshold {
                        let v_block = v as usize / BLOCK_SIZE;
                        active_blocks[v_block / 64] |= 1u64 << (v_block % 64);
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // Phase 2: Block-gated downward scan
        // Process blocks in DECREASING order (highest block first)
        // Skip entirely inactive blocks
        let downward_start = std::time::Instant::now();

        // Process blocks from highest to lowest
        for block_idx in (0..n_blocks).rev() {
            // Check if block is active
            let is_active = (active_blocks[block_idx / 64] >> (block_idx % 64)) & 1 != 0;
            if !is_active {
                stats.blocks_skipped += 1;
                continue;
            }
            stats.blocks_processed += 1;

            // Calculate rank range for this block
            let rank_start = block_idx * BLOCK_SIZE;
            let rank_end = std::cmp::min((block_idx + 1) * BLOCK_SIZE, self.n_nodes);

            // Process nodes within block in decreasing rank order
            for rank in (rank_start..rank_end).rev() {
                // With rank-aligned CCH: u = rank
                let u = rank;
                let d_u = dist[u];

                // Early exit: skip nodes beyond threshold or unreachable
                // This is the "no-cache-miss" check - just load dist[u]
                if d_u == u32::MAX || d_u > threshold {
                    stats.nodes_skipped_in_block += 1;
                    continue;
                }

                // Relax DOWN edges
                let down_start = self.topo.down_offsets[u] as usize;
                let down_end = self.topo.down_offsets[u + 1] as usize;

                for i in down_start..down_end {
                    let v = self.topo.down_targets[i] as usize;
                    let w = self.weights.down[i];

                    if w == u32::MAX {
                        continue;
                    }

                    let new_dist = d_u.saturating_add(w);
                    stats.downward_relaxations += 1;

                    if new_dist < dist[v] {
                        dist[v] = new_dist;
                        stats.downward_improved += 1;

                        // Mark destination block as active if within threshold
                        // This ensures paths can propagate through v
                        if new_dist <= threshold {
                            let v_block = v / BLOCK_SIZE;
                            active_blocks[v_block / 64] |= 1u64 << (v_block % 64);
                        }
                    }
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

    /// Run reverse PHAST with block-level active gating (all-to-one)
    ///
    /// Computes shortest paths from ALL nodes to a single target (reverse isochrone).
    /// Answers: "From where can I reach `target` within `threshold` time?"
    ///
    /// Works by running PHAST on the reverse graph:
    /// - Upward phase: uses `down_rev_flat` (reverse DOWN edges = low-to-high in reverse graph)
    /// - Downward phase: uses `up_adj_flat` (original UP edges = high-to-low in reverse graph)
    ///
    /// The flat adjacencies are passed externally because they contain the SWAPPED
    /// direction data needed for reverse search (self's internal adjacencies are for
    /// the forward direction).
    ///
    /// Returns Vec<(rank, dist)> for all nodes with dist <= threshold.
    pub fn query_block_gated_reverse(
        &self,
        target: u32,
        threshold: u32,
        up_adj_flat: &UpAdjFlat,
        down_rev_flat: &DownReverseAdjFlat,
    ) -> PhastResult {
        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[target as usize] = 0;

        // Block-level active set
        let n_blocks = (self.n_nodes + BLOCK_SIZE - 1) / BLOCK_SIZE;
        let n_words = (n_blocks + 63) / 64;
        let mut active_blocks = vec![0u64; n_words];

        // Mark target's block as active
        let target_block = target as usize / BLOCK_SIZE;
        active_blocks[target_block / 64] |= 1u64 << (target_block % 64);

        // ============================================================
        // Phase 1: Upward search on REVERSE graph
        // Uses down_rev_flat: for each low-rank node, gives higher-rank
        // sources with DOWN weights (= UP edges in the reverse graph)
        // ============================================================
        let upward_start = std::time::Instant::now();

        let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        pq.push(Reverse((0, target)));
        stats.upward_pq_pushes += 1;

        while let Some(Reverse((d, u))) = pq.pop() {
            stats.upward_pq_pops += 1;

            if d > dist[u as usize] {
                continue;
            }

            stats.upward_settled += 1;

            // Relax reverse-UP edges via down_rev_flat
            // down_rev_flat[u].sources = higher-rank neighbors
            // down_rev_flat[u].weights = DOWN weights (= reverse graph UP weights)
            let edge_start = down_rev_flat.offsets[u as usize] as usize;
            let edge_end = down_rev_flat.offsets[u as usize + 1] as usize;

            for i in edge_start..edge_end {
                let v = down_rev_flat.sources[i];
                let w = down_rev_flat.weights[i];
                // No INF check needed - DownReverseAdjFlat is pre-filtered

                let new_dist = d.saturating_add(w);
                stats.upward_relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    pq.push(Reverse((new_dist, v)));
                    stats.upward_pq_pushes += 1;

                    // Mark v's block as active if dist within threshold
                    if new_dist <= threshold {
                        let v_block = v as usize / BLOCK_SIZE;
                        active_blocks[v_block / 64] |= 1u64 << (v_block % 64);
                    }
                }
            }
        }

        stats.upward_time_ms = upward_start.elapsed().as_millis() as u64;

        // ============================================================
        // Phase 2: Plain downward PULL scan on REVERSE graph
        // Uses up_adj_flat: for each low-rank node, gives higher-rank
        // targets with UP weights (= DOWN edges in the reverse graph)
        //
        // For each rank r (decreasing order):
        //   for (v, w) in up_adj_flat[r]:  // v has higher rank
        //     dist[r] = min(dist[r], dist[v] + w)
        //
        // NOTE: Block-gating is NOT used because PULL cannot propagate
        // block activation downward (unlike PUSH in forward PHAST).
        // ============================================================
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = rank;

            // PULL phase: check all higher-rank neighbors via up_adj_flat
            let up_start = up_adj_flat.offsets[u] as usize;
            let up_end = up_adj_flat.offsets[u + 1] as usize;

            for i in up_start..up_end {
                let v = up_adj_flat.targets[i] as usize; // v has higher rank
                let w = up_adj_flat.weights[i];

                let d_v = dist[v];
                if d_v == u32::MAX {
                    continue;
                }

                let new_dist = d_v.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[u] {
                    dist[u] = new_dist;
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

    /// Run bounded reverse PHAST with adaptive gating strategy (all-to-one)
    ///
    /// Automatically chooses between block-gated and plain reverse PHAST based on
    /// the estimated active block ratio after the upward phase.
    ///
    /// This is the reverse counterpart of `query_bounded` / `query_adaptive`.
    pub fn query_bounded_reverse(
        &self,
        target: u32,
        threshold: u32,
        up_adj_flat: &UpAdjFlat,
        down_rev_flat: &DownReverseAdjFlat,
    ) -> PhastResult {
        let start = std::time::Instant::now();
        let mut stats = PhastStats::default();

        let mut dist = vec![u32::MAX; self.n_nodes];
        dist[target as usize] = 0;

        // ============================================================
        // Phase 1: Upward search on REVERSE graph
        // ============================================================
        let upward_start = std::time::Instant::now();

        let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        pq.push(Reverse((0, target)));
        stats.upward_pq_pushes += 1;

        while let Some(Reverse((d, u))) = pq.pop() {
            stats.upward_pq_pops += 1;

            // Early stop: if current min distance exceeds threshold, no point continuing
            if d > threshold {
                break;
            }

            if d > dist[u as usize] {
                continue;
            }

            stats.upward_settled += 1;

            let edge_start = down_rev_flat.offsets[u as usize] as usize;
            let edge_end = down_rev_flat.offsets[u as usize + 1] as usize;

            for i in edge_start..edge_end {
                let v = down_rev_flat.sources[i];
                let w = down_rev_flat.weights[i];

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
        // Phase 2: Plain downward PULL scan on REVERSE graph
        // NOTE: Block-gating is NOT used because PULL cannot propagate
        // block activation downward (unlike PUSH in forward PHAST).
        // ============================================================
        let downward_start = std::time::Instant::now();

        for rank in (0..self.n_nodes).rev() {
            let u = rank;

            // PULL from higher-rank neighbors via up_adj_flat
            let up_start = up_adj_flat.offsets[u] as usize;
            let up_end = up_adj_flat.offsets[u + 1] as usize;

            for i in up_start..up_end {
                let v = up_adj_flat.targets[i] as usize;
                let w = up_adj_flat.weights[i];

                let d_v = dist[v];
                if d_v == u32::MAX {
                    continue;
                }

                let new_dist = d_v.saturating_add(w);
                stats.downward_relaxations += 1;

                if new_dist < dist[u] {
                    dist[u] = new_dist;
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
    use super::*;
    use crate::formats::{CchTopo, CchWeights};
    use crate::matrix::bucket_ch::{DownReverseAdjFlat, UpAdjFlat};

    /// Build a small CCH for testing reverse PHAST.
    ///
    /// Graph (5 nodes, rank-aligned so node_id == rank):
    ///
    ///   0 --10--> 2 --5--> 4
    ///   1 --3---> 2 --7--> 3
    ///
    /// Rank ordering: 0 < 1 < 2 < 3 < 4
    ///
    /// UP edges (low rank -> high rank):
    ///   0 -> 2 (weight 10)
    ///   1 -> 2 (weight 3)
    ///   2 -> 3 (weight 7)
    ///   2 -> 4 (weight 5)
    ///
    /// DOWN edges (high rank -> low rank):
    ///   2 -> 0 (weight 10)
    ///   2 -> 1 (weight 3)
    ///   3 -> 2 (weight 7)
    ///   4 -> 2 (weight 5)
    fn build_test_cch() -> (CchTopo, CchWeights) {
        let n_nodes = 5u32;

        // UP edges in CSR format (indexed by source rank)
        // Node 0: -> 2 (w=10)
        // Node 1: -> 2 (w=3)
        // Node 2: -> 3 (w=7), -> 4 (w=5)
        // Node 3: (none)
        // Node 4: (none)
        let up_offsets = vec![0u64, 1, 2, 4, 4, 4];
        let up_targets = vec![2u32, 2, 3, 4];
        let up_weights = vec![10u32, 3, 7, 5];
        let up_is_shortcut = vec![false; 4];
        let up_middle = vec![u32::MAX; 4];

        // DOWN edges in CSR format (indexed by source rank)
        // Node 0: (none)
        // Node 1: (none)
        // Node 2: -> 0 (w=10), -> 1 (w=3)
        // Node 3: -> 2 (w=7)
        // Node 4: -> 2 (w=5)
        let down_offsets = vec![0u64, 0, 0, 2, 3, 4];
        let down_targets = vec![0u32, 1, 2, 2];
        let down_weights = vec![10u32, 3, 7, 5];
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
            up: up_weights,
            down: down_weights,
        };

        (topo, weights)
    }

    #[test]
    fn test_reverse_phast_simple() {
        // Build test graph
        let (topo, weights) = build_test_cch();

        // Build flat adjacencies needed for reverse PHAST
        let up_adj_flat = UpAdjFlat::build(&topo, &weights);
        let down_rev_flat = DownReverseAdjFlat::build(&topo, &weights);

        let engine = PhastEngine::new(topo, weights);
        let threshold = u32::MAX - 1; // Effectively unbounded

        // Forward PHAST from node 0:
        //   d(0) = 0
        //   d(2) = 10 (via 0->2)
        //   d(3) = 17 (via 0->2->3)
        //   d(4) = 15 (via 0->2->4)
        //   d(1) = INF (no path 0->1 since edges only go upward then downward)
        // Wait -- with PHAST, the downward phase propagates from 2 down to 0 and 1.
        // Let me trace:
        //   Upward phase from 0: push(0,0). Pop (0,0).
        //     UP edges: 0->2 (w=10). dist[2]=10, push(10,2).
        //     Pop (10,2). UP edges: 2->3 (w=7), 2->4 (w=5).
        //     dist[3]=17, push(17,3). dist[4]=15, push(15,4).
        //     Pop (15,4). No UP edges.
        //     Pop (17,3). No UP edges.
        //   Downward phase (rank 4 down to 0):
        //     rank=4: dist[4]=15, DOWN: 4->2 (w=5). dist[2] = min(10, 15+5=20) = 10. No change.
        //     rank=3: dist[3]=17, DOWN: 3->2 (w=7). dist[2] = min(10, 17+7=24) = 10. No change.
        //     rank=2: dist[2]=10, DOWN: 2->0 (w=10), 2->1 (w=3).
        //       dist[0] = min(0, 10+10=20) = 0. No change.
        //       dist[1] = min(INF, 10+3=13) = 13.
        //     rank=1: dist[1]=13, no DOWN edges.
        //     rank=0: dist[0]=0, no DOWN edges.
        //
        // Forward PHAST result from 0: d=[0, 13, 10, 17, 15]

        let fwd_result = engine.query(0);
        assert_eq!(fwd_result.dist[0], 0);
        assert_eq!(fwd_result.dist[1], 13);
        assert_eq!(fwd_result.dist[2], 10);
        assert_eq!(fwd_result.dist[3], 17);
        assert_eq!(fwd_result.dist[4], 15);

        // Reverse PHAST to node 0 (all-to-one):
        // This should give d_rev[s] = shortest path from s to 0.
        //
        // In the original directed graph, paths TO node 0:
        //   From 0: d=0 (trivial)
        //   From 1: 1->2->0? Only if those edges exist in original direction.
        //     The original edges are: 0->2, 1->2, 2->3, 2->4 (UP) and 2->0, 2->1, 3->2, 4->2 (DOWN)
        //     But in the CCH, UP and DOWN represent the hierarchy, not the original direction.
        //     The CCH is bidirectional with separate UP/DOWN weights.
        //     d(1->0) via 1->2->0: UP(1->2, w=3) + DOWN(2->0, w=10) = 13
        //     But that's the forward PHAST path 0->1 reversed with same weights, so d_rev(1->0) = 13? Not necessarily.
        //     Actually in a CCH, the path s->t is: s goes UP to meeting node m, then m goes DOWN to t.
        //     For reverse PHAST to target 0:
        //       d_rev(s) = min path from s to 0
        //       = min over all m: UP_cost(s->m) + DOWN_cost(m->0)
        //     But the reverse PHAST on the REVERSE graph computes:
        //       Upward on reverse = using DOWN_reverse edges
        //       Downward on reverse = using UP edges
        //
        //     Let me trace reverse PHAST to target=0:
        //
        //     Upward phase (on reverse graph, starting from target=0):
        //       down_rev_flat[0]: for node 0, incoming DOWN edges. Looking at DOWN: 2->0 (w=10).
        //         So down_rev_flat[0].sources = [2], weights = [10].
        //       push(0, 0). Pop (0, 0).
        //         Iterate down_rev_flat[0]: v=2, w=10. dist[2] = 10. push(10, 2).
        //       Pop (10, 2).
        //         Iterate down_rev_flat[2]: for node 2, incoming DOWN edges.
        //         DOWN edges targeting 2: 3->2 (w=7) and 4->2 (w=5).
        //         So down_rev_flat[2].sources = [3, 4], weights = [7, 5].
        //         v=3, w=7. dist[3] = 17. push(17, 3).
        //         v=4, w=5. dist[4] = 15. push(15, 4).
        //       Pop (15, 4).
        //         Iterate down_rev_flat[4]: for node 4, incoming DOWN edges. None (no DOWN edge targets 4).
        //       Pop (17, 3).
        //         Iterate down_rev_flat[3]: for node 3, incoming DOWN edges. None.
        //
        //     After upward: dist = [0, INF, 10, 17, 15]
        //
        //     Downward phase (on reverse graph, using up_adj_flat):
        //       Process rank 4 down to 0:
        //       rank=4: up_adj_flat[4]: no UP edges from 4. dist[4] stays 15.
        //       rank=3: up_adj_flat[3]: no UP edges from 3. dist[3] stays 17.
        //       rank=2: up_adj_flat[2]: UP edges 2->3 (w=7), 2->4 (w=5).
        //         PULL from 3: dist[3]=17 + 7 = 24. dist[2]=10. No improvement.
        //         PULL from 4: dist[4]=15 + 5 = 20. dist[2]=10. No improvement.
        //       rank=1: up_adj_flat[1]: UP edge 1->2 (w=3).
        //         PULL from 2: dist[2]=10 + 3 = 13. dist[1]=INF. Improvement! dist[1] = 13.
        //       rank=0: up_adj_flat[0]: UP edge 0->2 (w=10).
        //         PULL from 2: dist[2]=10 + 10 = 20. dist[0]=0. No improvement.
        //
        //     Reverse PHAST result to 0: d_rev = [0, 13, 10, 17, 15]
        //
        //     This matches forward! Because in this symmetric graph, d(0->x) == d(x->0).

        let rev_result = engine.query_block_gated_reverse(0, threshold, &up_adj_flat, &down_rev_flat);
        assert_eq!(rev_result.dist[0], 0, "d_rev(0->0) should be 0");
        assert_eq!(rev_result.dist[1], 13, "d_rev(1->0) should be 13");
        assert_eq!(rev_result.dist[2], 10, "d_rev(2->0) should be 10");
        assert_eq!(rev_result.dist[3], 17, "d_rev(3->0) should be 17");
        assert_eq!(rev_result.dist[4], 15, "d_rev(4->0) should be 15");

        // For this symmetric graph, forward and reverse should match
        for node in 0..5 {
            assert_eq!(
                fwd_result.dist[node],
                rev_result.dist[node],
                "Forward from 0 to {} should equal reverse to 0 from {}",
                node, node
            );
        }
    }

    #[test]
    fn test_reverse_phast_vs_forward() {
        // Build test graph
        let (topo, weights) = build_test_cch();
        let up_adj_flat = UpAdjFlat::build(&topo, &weights);
        let down_rev_flat = DownReverseAdjFlat::build(&topo, &weights);
        let engine = PhastEngine::new(topo, weights);
        let threshold = u32::MAX - 1;

        // For every pair (s, t), verify:
        //   forward_phast(s).dist[t] == reverse_phast(t).dist[s]
        //
        // This must hold because forward PHAST(s) computes d(s->all),
        // and reverse PHAST(t) computes d(all->t).
        // So d_forward(s)[t] = d(s->t) = d_reverse(t)[s].

        for s in 0..5u32 {
            let fwd = engine.query(s);
            for t in 0..5u32 {
                let rev = engine.query_block_gated_reverse(t, threshold, &up_adj_flat, &down_rev_flat);
                assert_eq!(
                    fwd.dist[t as usize],
                    rev.dist[s as usize],
                    "d_fwd({}->{}) = {} should equal d_rev({}->{})[{}] = {}",
                    s, t, fwd.dist[t as usize],
                    t, s, s, rev.dist[s as usize]
                );
            }
        }
    }

    #[test]
    fn test_reverse_phast_bounded() {
        // Test that the bounded reverse PHAST correctly filters by threshold
        let (topo, weights) = build_test_cch();
        let up_adj_flat = UpAdjFlat::build(&topo, &weights);
        let down_rev_flat = DownReverseAdjFlat::build(&topo, &weights);
        let engine = PhastEngine::new(topo, weights);

        // Reverse PHAST to node 0 with threshold 12:
        // d_rev = [0, 13, 10, 17, 15]
        // Only nodes 0 (d=0) and 2 (d=10) should be within threshold 12
        let result = engine.query_bounded_reverse(0, 12, &up_adj_flat, &down_rev_flat);
        assert_eq!(result.n_reachable, 2, "Only nodes 0 and 2 should be reachable within threshold 12");

        // Node 0 should have dist 0
        assert_eq!(result.dist[0], 0);
        // Node 2 should have dist 10
        assert_eq!(result.dist[2], 10);
        // Node 1 should still be computed (dist 13 > 12) but counted as unreachable
        // The dist may or may not be exact beyond threshold, but n_reachable should be correct
    }

    #[test]
    fn test_reverse_phast_asymmetric() {
        // Build an ASYMMETRIC graph where d(s->t) != d(t->s)
        // to verify reverse PHAST is truly computing "all-to-one" not "one-to-all"
        //
        // 3 nodes, rank-aligned: 0 < 1 < 2
        //
        // UP edges:
        //   0 -> 2 (weight 10)
        //   1 -> 2 (weight 5)
        //
        // DOWN edges:
        //   2 -> 0 (weight 20)  <-- NOTE: asymmetric! UP 0->2 = 10, DOWN 2->0 = 20
        //   2 -> 1 (weight 3)   <-- NOTE: asymmetric! UP 1->2 = 5, DOWN 2->1 = 3

        let n_nodes = 3u32;

        let up_offsets = vec![0u64, 1, 2, 2];
        let up_targets = vec![2u32, 2];
        let up_weights = vec![10u32, 5];
        let up_is_shortcut = vec![false; 2];
        let up_middle = vec![u32::MAX; 2];

        let down_offsets = vec![0u64, 0, 0, 2];
        let down_targets = vec![0u32, 1];
        let down_weights = vec![20u32, 3];
        let down_is_shortcut = vec![false; 2];
        let down_middle = vec![u32::MAX; 2];

        let rank_to_filtered: Vec<u32> = (0..n_nodes).collect();

        let topo = CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: 2,
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
            up: up_weights,
            down: down_weights,
        };

        let up_adj_flat = UpAdjFlat::build(&topo, &weights);
        let down_rev_flat = DownReverseAdjFlat::build(&topo, &weights);
        let engine = PhastEngine::new(topo, weights);
        let threshold = u32::MAX - 1;

        // Forward PHAST from node 0:
        //   Upward: dist[0]=0, UP(0->2, w=10) => dist[2]=10
        //   Downward rank=2: DOWN(2->0, w=20) => dist[0]=min(0, 10+20)=0. DOWN(2->1, w=3) => dist[1]=13.
        //   Result: d_fwd(0) = [0, 13, 10]
        let fwd0 = engine.query(0);
        assert_eq!(fwd0.dist[0], 0);
        assert_eq!(fwd0.dist[1], 13); // 0->2->1: UP(10) + DOWN(3) = 13
        assert_eq!(fwd0.dist[2], 10); // 0->2: UP(10)

        // Forward PHAST from node 1:
        //   Upward: dist[1]=0, UP(1->2, w=5) => dist[2]=5
        //   Downward rank=2: DOWN(2->0, w=20) => dist[0]=25. DOWN(2->1, w=3) => dist[1]=min(0,8)=0.
        //   Result: d_fwd(1) = [25, 0, 5]
        let fwd1 = engine.query(1);
        assert_eq!(fwd1.dist[0], 25); // 1->2->0: UP(5) + DOWN(20) = 25
        assert_eq!(fwd1.dist[1], 0);
        assert_eq!(fwd1.dist[2], 5);  // 1->2: UP(5)

        // Reverse PHAST to node 0 (all-to-one):
        //   d_rev(s) = shortest path from s to 0
        //
        //   Upward on reverse (using down_rev_flat):
        //     Start: dist[0]=0
        //     down_rev_flat[0]: DOWN edges targeting 0 = (2->0, w=20). So sources=[2], weights=[20].
        //     dist[2] = 20. push(20, 2).
        //     Pop (20, 2). down_rev_flat[2]: DOWN edges targeting 2 = none (DOWN offsets: 2->0, 2->1, not targeting 2).
        //     Wait: DOWN edges: 2->0, 2->1. These target nodes 0 and 1. No DOWN edge targets node 2.
        //     But DOWN edges from nodes 3 and 4... we only have 3 nodes. So no edges.
        //     Actually there's no DOWN edge TO node 2 in this graph.
        //     Wait, I need to also check if any DOWN edge has target=2.
        //     DOWN targets are [0, 1]. Neither is 2. So down_rev_flat[2] is empty.
        //     Upward result: dist = [0, INF, 20]
        //
        //   Downward on reverse (PULL from up_adj_flat):
        //     rank=2: up_adj_flat[2] = empty (no UP from 2). dist[2]=20.
        //     rank=1: up_adj_flat[1] = [target=2, w=5]. PULL: dist[2]=20 + 5 = 25. dist[1]=INF -> 25.
        //     rank=0: up_adj_flat[0] = [target=2, w=10]. PULL: dist[2]=20 + 10 = 30. dist[0]=0 -> 0 (no change).
        //
        //   Reverse result: d_rev = [0, 25, 20]
        //
        //   d_rev(1->0) = 25 = d_fwd(1)[0] = 25. Matches!
        //   d_rev(2->0) = 20 = d_fwd(2)[0] should be checked:
        //     Forward PHAST from node 2:
        //       Upward: dist[2]=0. No UP edges from 2 (up_offsets[2]=2, up_offsets[3]=2, so empty).
        //       Downward rank=2: DOWN(2->0, w=20) => dist[0]=20. DOWN(2->1, w=3) => dist[1]=3.
        //     So d_fwd(2) = [20, 3, 0]. d_fwd(2)[0] = 20. Matches!

        let rev0 = engine.query_block_gated_reverse(0, threshold, &up_adj_flat, &down_rev_flat);
        assert_eq!(rev0.dist[0], 0, "d_rev(0->0) should be 0");
        assert_eq!(rev0.dist[1], 25, "d_rev(1->0) should be 25");
        assert_eq!(rev0.dist[2], 20, "d_rev(2->0) should be 20");

        // Verify asymmetry: d(0->1) = 13 but d(1->0) = 25
        assert_ne!(fwd0.dist[1], rev0.dist[1], "Graph should be asymmetric: d(0->1) != d(1->0)");

        // Cross-validate: for all (s,t) pairs, d_fwd(s)[t] == d_rev(t)[s]
        for t in 0..3u32 {
            let rev = engine.query_block_gated_reverse(t, threshold, &up_adj_flat, &down_rev_flat);
            for s in 0..3u32 {
                let fwd = engine.query(s);
                assert_eq!(
                    fwd.dist[t as usize],
                    rev.dist[s as usize],
                    "d_fwd({}->{}) = {} should equal d_rev({})[{}] = {}",
                    s, t, fwd.dist[t as usize], t, s, rev.dist[s as usize]
                );
            }
        }
    }

    #[test]
    fn test_reverse_phast_bounded_adaptive() {
        // Verify query_bounded_reverse gives same results as query_block_gated_reverse
        let (topo, weights) = build_test_cch();
        let up_adj_flat = UpAdjFlat::build(&topo, &weights);
        let down_rev_flat = DownReverseAdjFlat::build(&topo, &weights);
        let engine = PhastEngine::new(topo, weights);

        for target in 0..5u32 {
            let threshold = u32::MAX - 1;
            let block_result = engine.query_block_gated_reverse(target, threshold, &up_adj_flat, &down_rev_flat);
            let adaptive_result = engine.query_bounded_reverse(target, threshold, &up_adj_flat, &down_rev_flat);

            for node in 0..5 {
                assert_eq!(
                    block_result.dist[node],
                    adaptive_result.dist[node],
                    "Block-gated and adaptive reverse PHAST should agree for target={}, node={}",
                    target, node
                );
            }
        }
    }
}
