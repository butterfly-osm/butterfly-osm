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
