//! Bucket-based Many-to-Many CH Algorithm
//!
//! This module implements the classic "bucket" algorithm for computing distance matrices
//! on Contraction Hierarchies. Unlike PHAST which computes one-to-ALL distances,
//! this algorithm efficiently computes N×M matrices by:
//!
//! 1. Forward phase: Run upward search from each source, storing (source_id, dist) in buckets
//! 2. Backward phase: Run backward search from each target, joining with buckets
//!
//! Complexity: O(N × upward_search + M × backward_search + bucket_joins)
//! Much faster than PHAST for sparse matrices (small N, M relative to graph size).
//!
//! ## Optimizations
//!
//! - **Flat reverse adjacency**: Stores (source, weight) directly, eliminating edge_idx indirection
//! - **4-ary heap**: Better cache locality than binary heap (4 children per node)
//! - **Bucket prefix-sum layout**: O(1) lookup instead of O(log n) binary search
//! - **Version-stamped distances**: Amortized O(1) per-search initialization

use crate::formats::{CchTopo, CchWeights};
use crate::step9::state::DownReverseAdj;

// =============================================================================
// FLAT ADJACENCY STRUCTURES - Pre-filtered INF edges
// =============================================================================

/// Flat forward adjacency for UP edges with embedded weights
/// Filters out INF-weight edges at build time
#[derive(Clone)]
pub struct UpAdjFlat {
    pub offsets: Vec<u64>,   // n_nodes + 1
    pub targets: Vec<u32>,   // target node for edge
    pub weights: Vec<u32>,   // weight of edge (embedded)
}

impl UpAdjFlat {
    /// Build flat UP adjacency from topology and weights
    /// Filters out INF-weight edges to avoid checking in hot loop
    pub fn build(topo: &CchTopo, weights: &CchWeights) -> Self {
        let n_nodes = topo.n_nodes as usize;

        // First pass: count valid edges per node
        let mut counts = vec![0usize; n_nodes];
        for source in 0..n_nodes {
            let start = topo.up_offsets[source] as usize;
            let end = topo.up_offsets[source + 1] as usize;
            for i in start..end {
                if weights.up[i] != u32::MAX {
                    counts[source] += 1;
                }
            }
        }

        // Build offsets (prefix sum)
        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut offset = 0u64;
        for &count in &counts {
            offsets.push(offset);
            offset += count as u64;
        }
        offsets.push(offset);

        let total_edges = offset as usize;

        // Allocate arrays
        let mut targets = vec![0u32; total_edges];
        let mut flat_weights = vec![0u32; total_edges];

        // Second pass: fill in edges (skip INF)
        counts.fill(0);
        for source in 0..n_nodes {
            let start = topo.up_offsets[source] as usize;
            let end = topo.up_offsets[source + 1] as usize;

            for i in start..end {
                let w = weights.up[i];
                if w == u32::MAX {
                    continue;
                }
                let target = topo.up_targets[i];
                let pos = offsets[source] as usize + counts[source];
                targets[pos] = target;
                flat_weights[pos] = w;
                counts[source] += 1;
            }
        }

        Self {
            offsets,
            targets,
            weights: flat_weights,
        }
    }
}

/// Flat reverse adjacency for DOWN edges with embedded weights
/// Stores (source, weight) directly instead of (source, edge_idx)
/// This eliminates one memory indirection in the hot path
#[derive(Clone)]
pub struct DownReverseAdjFlat {
    pub offsets: Vec<u64>,   // n_nodes + 1
    pub sources: Vec<u32>,   // source node x for reverse edge
    pub weights: Vec<u32>,   // weight of edge x→y (embedded, not indirect)
}

impl DownReverseAdjFlat {
    /// Build flat reverse adjacency from topology and weights
    /// Filters out INF-weight edges to avoid checking in hot loop
    pub fn build(topo: &CchTopo, weights: &CchWeights) -> Self {
        let n_nodes = topo.n_nodes as usize;

        // First pass: count incoming VALID edges per node (skip INF weights)
        let mut counts = vec![0usize; n_nodes];
        for source in 0..n_nodes {
            let start = topo.down_offsets[source] as usize;
            let end = topo.down_offsets[source + 1] as usize;
            for i in start..end {
                if weights.down[i] != u32::MAX {
                    let target = topo.down_targets[i] as usize;
                    counts[target] += 1;
                }
            }
        }

        // Build offsets (prefix sum)
        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut offset = 0u64;
        for &count in &counts {
            offsets.push(offset);
            offset += count as u64;
        }
        offsets.push(offset);

        let total_edges = offset as usize;

        // Allocate arrays (only for valid edges)
        let mut sources = vec![0u32; total_edges];
        let mut flat_weights = vec![0u32; total_edges];

        // Second pass: fill in reverse edges with embedded weights (skip INF)
        counts.fill(0);

        for source in 0..n_nodes {
            let start = topo.down_offsets[source] as usize;
            let end = topo.down_offsets[source + 1] as usize;

            for i in start..end {
                let w = weights.down[i];
                if w == u32::MAX {
                    continue; // Skip INF edges
                }
                let target = topo.down_targets[i] as usize;
                let pos = offsets[target] as usize + counts[target];
                sources[pos] = source as u32;
                flat_weights[pos] = w;
                counts[target] += 1;
            }
        }

        Self {
            offsets,
            sources,
            weights: flat_weights,
        }
    }
}

/// Flat reverse adjacency for UP edges with embedded weights
/// For each node, stores incoming UP edges (nodes that have an UP edge TO this node)
/// Used for stall-on-demand optimization in forward search
#[derive(Clone)]
pub struct UpReverseAdjFlat {
    pub offsets: Vec<u64>,   // n_nodes + 1
    pub sources: Vec<u32>,   // source node p for reverse edge (p → u means p has UP edge to u)
    pub weights: Vec<u32>,   // weight of UP edge p→u
}

impl UpReverseAdjFlat {
    /// Build flat reverse adjacency for UP edges from topology and weights
    pub fn build(topo: &CchTopo, weights: &CchWeights) -> Self {
        let n_nodes = topo.n_nodes as usize;

        // First pass: count incoming UP edges per node (skip INF weights)
        let mut counts = vec![0usize; n_nodes];
        for source in 0..n_nodes {
            let start = topo.up_offsets[source] as usize;
            let end = topo.up_offsets[source + 1] as usize;
            for i in start..end {
                if weights.up[i] != u32::MAX {
                    let target = topo.up_targets[i] as usize;
                    counts[target] += 1;
                }
            }
        }

        // Build offsets (prefix sum)
        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut offset = 0u64;
        for &count in &counts {
            offsets.push(offset);
            offset += count as u64;
        }
        offsets.push(offset);

        let total_edges = offset as usize;

        // Allocate arrays
        let mut sources = vec![0u32; total_edges];
        let mut flat_weights = vec![0u32; total_edges];

        // Second pass: fill in reverse edges with embedded weights
        counts.fill(0);

        for source in 0..n_nodes {
            let start = topo.up_offsets[source] as usize;
            let end = topo.up_offsets[source + 1] as usize;

            for i in start..end {
                let w = weights.up[i];
                if w == u32::MAX {
                    continue;
                }
                let target = topo.up_targets[i] as usize;
                let pos = offsets[target] as usize + counts[target];
                sources[pos] = source as u32;
                flat_weights[pos] = w;
                counts[target] += 1;
            }
        }

        Self {
            offsets,
            sources,
            weights: flat_weights,
        }
    }
}

// =============================================================================
// 4-ARY HEAP WITH DECREASE-KEY (OSRM-style)
// =============================================================================

const ARITY: usize = 4;
const INVALID_HANDLE: u32 = u32::MAX;

/// 4-ary min-heap with decrease-key support
/// Mirrors OSRM's DAryHeap implementation
struct DAryHeap {
    /// Heap array: (weight, index into inserted_nodes)
    heap: Vec<(u32, u32)>,
}

impl DAryHeap {
    fn new(capacity: usize) -> Self {
        Self {
            heap: Vec::with_capacity(capacity),
        }
    }

    #[inline]
    fn size(&self) -> usize {
        self.heap.len()
    }

    #[inline]
    fn is_empty(&self) -> bool {
        self.heap.is_empty()
    }

    #[inline]
    fn clear(&mut self) {
        self.heap.clear();
    }

    #[inline]
    fn top(&self) -> (u32, u32) {
        self.heap[0]
    }

    /// Insert new element and return its handle
    #[inline]
    fn push(&mut self, weight: u32, index: u32, handles: &mut [u32]) {
        let pos = self.heap.len();
        self.heap.push((weight, index));
        self.heapify_up(pos, handles);
    }

    /// Decrease key at given handle
    #[inline]
    fn decrease(&mut self, handle: u32, weight: u32, index: u32, handles: &mut [u32]) {
        let pos = handle as usize;
        debug_assert!(
            pos < self.heap.len(),
            "decrease: handle {} out of bounds (heap len {}), index/node {}",
            pos, self.heap.len(), index
        );
        self.heap[pos] = (weight, index);
        self.heapify_up(pos, handles);
    }

    /// Pop minimum element
    #[inline]
    fn pop(&mut self, handles: &mut [u32]) -> Option<(u32, u32)> {
        if self.heap.is_empty() {
            return None;
        }
        let result = self.heap[0];
        if self.heap.len() == 1 {
            self.heap.pop();
            return Some(result);
        }
        // Swap last element to front and heapify down
        let last_idx = self.heap.len() - 1;
        self.heap.swap(0, last_idx);
        // Update handle for element that moved to position 0
        handles[self.heap[0].1 as usize] = 0;
        self.heap.pop();
        if !self.heap.is_empty() {
            self.heapify_down(0, handles);
        }
        Some(result)
    }

    #[inline]
    fn parent(index: usize) -> usize {
        (index - 1) / ARITY
    }

    #[inline]
    fn kth_child(index: usize, k: usize) -> usize {
        ARITY * index + k + 1
    }

    #[inline]
    fn heapify_up(&mut self, mut pos: usize, handles: &mut [u32]) {
        let item = self.heap[pos];
        while pos > 0 {
            let parent_pos = Self::parent(pos);
            if item.0 >= self.heap[parent_pos].0 {
                break;
            }
            // Move parent down
            let parent_item = self.heap[parent_pos];
            self.heap[pos] = parent_item;
            handles[parent_item.1 as usize] = pos as u32;
            pos = parent_pos;
        }
        self.heap[pos] = item;
        handles[item.1 as usize] = pos as u32;
    }

    #[inline]
    fn heapify_down(&mut self, mut pos: usize, handles: &mut [u32]) {
        let item = self.heap[pos];
        let len = self.heap.len();
        loop {
            let first_child = Self::kth_child(pos, 0);
            if first_child >= len {
                break;
            }
            // Find minimum child
            let mut min_child = first_child;
            let mut min_weight = self.heap[first_child].0;
            for k in 1..ARITY {
                let child = Self::kth_child(pos, k);
                if child >= len {
                    break;
                }
                if self.heap[child].0 < min_weight {
                    min_child = child;
                    min_weight = self.heap[child].0;
                }
            }
            if item.0 <= min_weight {
                break;
            }
            // Move min child up
            let child_item = self.heap[min_child];
            self.heap[pos] = child_item;
            handles[child_item.1 as usize] = pos as u32;
            pos = min_child;
        }
        self.heap[pos] = item;
        handles[item.1 as usize] = pos as u32;
    }
}

// =============================================================================
// SEARCH STATE - OSRM-style with DecreaseKey
// =============================================================================

/// Entry tracking node state with version stamp
#[derive(Clone, Copy)]
#[repr(C)]
struct NodeEntry {
    dist: u32,
    version: u32,
}

/// Reusable search state with 4-ary heap and decrease-key
struct SearchState {
    /// Per-node state: distance + version
    entries: Vec<NodeEntry>,
    current_version: u32,
    /// 4-ary min-heap with decrease-key
    heap: DAryHeap,
    /// Handles array: node → position in heap (SINGLE source of truth for handles)
    /// INVALID_HANDLE means node is not in heap (never inserted or already settled)
    handles: Vec<u32>,
    /// Counters for profiling
    pushes: usize,
    pops: usize,
    stale_pops: usize, // Should always be 0 with decrease-key
}

impl SearchState {
    fn new(n_nodes: usize, heap_capacity: usize) -> Self {
        Self {
            entries: vec![NodeEntry { dist: u32::MAX, version: 0 }; n_nodes],
            current_version: 0,
            heap: DAryHeap::new(heap_capacity),
            handles: vec![INVALID_HANDLE; n_nodes],
            pushes: 0,
            pops: 0,
            stale_pops: 0,
        }
    }

    #[inline]
    fn start_search(&mut self) {
        self.current_version = self.current_version.wrapping_add(1);
        if self.current_version == 0 {
            // Version overflow - reset all entries
            for e in &mut self.entries {
                e.dist = u32::MAX;
                e.version = 0;
            }
            // Also need to reset handles since we're starting fresh
            for h in &mut self.handles {
                *h = INVALID_HANDLE;
            }
            self.current_version = 1;
        }
        self.heap.clear();
    }

    #[inline]
    fn get_dist(&self, node: u32) -> u32 {
        let e = &self.entries[node as usize];
        if e.version == self.current_version {
            e.dist
        } else {
            u32::MAX
        }
    }

    /// Relax an edge: insert new or decrease-key existing
    #[inline]
    fn relax(&mut self, node: u32, dist: u32) -> bool {
        let e = &mut self.entries[node as usize];

        if e.version == self.current_version {
            // Node already seen this search
            if dist < e.dist {
                // Better path found - decrease key
                e.dist = dist;
                let handle = self.handles[node as usize];
                if handle != INVALID_HANDLE && (handle as usize) < self.heap.size() {
                    // Node is still in heap - decrease key
                    self.heap.decrease(handle, dist, node, &mut self.handles);
                    self.pushes += 1;
                }
                // Note: if handle == INVALID_HANDLE, node was already settled
                return true;
            }
            return false;
        }

        // First time seeing this node in current search
        // Reset handle to ensure no stale value is used
        self.handles[node as usize] = INVALID_HANDLE;
        e.dist = dist;
        e.version = self.current_version;
        self.heap.push(dist, node, &mut self.handles);
        self.pushes += 1;
        true
    }

    #[inline]
    fn pop(&mut self) -> Option<(u32, u32)> {
        if let Some((dist, node)) = self.heap.pop(&mut self.handles) {
            self.pops += 1;
            // Mark as settled (handle becomes INVALID_HANDLE after pop in heapify_down)
            self.handles[node as usize] = INVALID_HANDLE;
            return Some((dist, node));
        }
        None
    }
}

// =============================================================================
// BUCKET LAYOUT - Prefix-sum for O(1) lookup with reusable buffers
// =============================================================================

/// Bucket item (8 bytes, aligned for fast access)
#[derive(Clone, Copy)]
#[repr(C)]
struct BucketEntry {
    dist: u32,
    source_idx: u16,
    _pad: u16,
}

/// Reusable prefix-sum bucket structure with version stamping
/// - O(1) lookup per node (no binary search)
/// - No clearing between queries (stamp-based reset)
/// - Buffers reused across all queries
struct PrefixSumBuckets {
    n_nodes: usize,
    /// Count of items per node (stamped)
    counts: Vec<u32>,
    /// Version stamps for counts (avoid clearing)
    count_stamps: Vec<u32>,
    /// Current stamp for this build
    current_stamp: u32,
    /// Offsets into items array (n_nodes + 1)
    offsets: Vec<u32>,
    /// Flat array of bucket entries
    items: Vec<BucketEntry>,
    /// Temporary storage for nodes that have items (for offset building)
    active_nodes: Vec<u32>,
}

impl PrefixSumBuckets {
    fn new(n_nodes: usize) -> Self {
        Self {
            n_nodes,
            counts: vec![0; n_nodes],
            count_stamps: vec![0; n_nodes],
            current_stamp: 0,
            offsets: vec![0; n_nodes + 1],
            items: Vec::new(),
            active_nodes: Vec::new(),
        }
    }

    /// Build buckets from collected items - O(items) time, no per-node clearing
    fn build(&mut self, raw_items: &[(u32, u16, u32)]) {
        // Increment stamp (wrapping is fine, we compare equality)
        self.current_stamp = self.current_stamp.wrapping_add(1);
        if self.current_stamp == 0 {
            // Stamp overflow - must clear
            self.count_stamps.fill(0);
            self.current_stamp = 1;
        }

        self.active_nodes.clear();

        // First pass: count items per node (stamp-based, no clearing)
        for &(node, _, _) in raw_items {
            let n = node as usize;
            if self.count_stamps[n] != self.current_stamp {
                // First time seeing this node in this build
                self.count_stamps[n] = self.current_stamp;
                self.counts[n] = 0;
                self.active_nodes.push(node);
            }
            self.counts[n] += 1;
        }

        // Build offsets only for active nodes (sparse)
        // First, set all offsets to 0 for active nodes
        let mut total = 0u32;
        for &node in &self.active_nodes {
            let n = node as usize;
            self.offsets[n] = total;
            total += self.counts[n];
        }

        // Resize items if needed
        let total_items = total as usize;
        if self.items.len() < total_items {
            self.items.resize(total_items, BucketEntry { dist: 0, source_idx: 0, _pad: 0 });
        }

        // Reset counts for second pass (reuse as write cursors)
        for &node in &self.active_nodes {
            self.counts[node as usize] = 0;
        }

        // Second pass: place items
        for &(node, source_idx, dist) in raw_items {
            let n = node as usize;
            let pos = self.offsets[n] + self.counts[n];
            self.items[pos as usize] = BucketEntry { dist, source_idx, _pad: 0 };
            self.counts[n] += 1;
        }

        // Store end offset for last active node
        // (We'll use counts[n] == items in bucket for end calculation)
    }

    /// Get bucket entries for a node - O(k) where k is bucket size
    #[inline]
    fn get(&self, node: u32) -> &[BucketEntry] {
        let n = node as usize;
        if self.count_stamps[n] != self.current_stamp {
            // Node has no items in current build
            return &[];
        }
        let start = self.offsets[n] as usize;
        let len = self.counts[n] as usize;
        &self.items[start..start + len]
    }

    fn total_items(&self) -> usize {
        self.active_nodes.iter()
            .map(|&n| self.counts[n as usize] as usize)
            .sum()
    }

    fn n_nodes_with_buckets(&self) -> usize {
        self.active_nodes.len()
    }
}

/// Sorted bucket layout with binary search (legacy, for comparison)
struct SortedBuckets {
    items: Vec<(u32, u16, u32)>, // (node, source_idx, dist)
}

impl SortedBuckets {
    fn build(mut items: Vec<(u32, u16, u32)>) -> Self {
        items.sort_unstable_by_key(|&(node, _, _)| node);
        Self { items }
    }

    /// Create from already-sorted items (for parallel sort case)
    fn from_sorted(items: Vec<(u32, u16, u32)>) -> Self {
        Self { items }
    }

    #[inline]
    fn get(&self, node: u32) -> impl Iterator<Item = (u16, u32)> + '_ {
        let start = self.items.partition_point(|&(n, _, _)| n < node);
        let end = self.items.partition_point(|&(n, _, _)| n <= node);
        self.items[start..end].iter().map(|&(_, s, d)| (s, d))
    }

    /// Get bucket items as a slice (for parallel join)
    #[inline]
    fn get_slice(&self, node: u32) -> &[(u32, u16, u32)] {
        let start = self.items.partition_point(|&(n, _, _)| n < node);
        let end = self.items.partition_point(|&(n, _, _)| n <= node);
        &self.items[start..end]
    }

    fn total_items(&self) -> usize {
        self.items.len()
    }

    fn n_nodes_with_buckets(&self) -> usize {
        if self.items.is_empty() { return 0; }
        let mut count = 1;
        let mut prev = self.items[0].0;
        for &(n, _, _) in &self.items[1..] {
            if n != prev { count += 1; prev = n; }
        }
        count
    }

    /// Consume self and return the items buffer for reuse
    fn into_items(self) -> Vec<(u32, u16, u32)> {
        self.items
    }
}

// =============================================================================
// PUBLIC API
// =============================================================================

/// Statistics from bucket many-to-many computation
#[derive(Debug, Default, Clone)]
pub struct BucketM2MStats {
    pub n_sources: usize,
    pub n_targets: usize,
    pub forward_visited: usize,
    pub backward_visited: usize,
    pub bucket_items: usize,
    pub bucket_nodes: usize,
    pub join_operations: usize,
    pub skipped_joins: usize,  // Bucket entries skipped due to bound-aware pruning
    pub forward_time_ms: u64,
    pub sort_time_ms: u64,
    pub backward_time_ms: u64,
    /// Total relaxations
    pub heap_pushes: usize,
    /// Total settlements (no stale with decrease-key)
    pub heap_pops: usize,
    /// Stale pops (always 0 with decrease-key heap)
    pub stale_pops: usize,
}

/// Compute many-to-many distance matrix using optimized bucket algorithm
///
/// Uses the correct directed-graph formulation:
///   d(s → t) = min over m: d(s → m) + d(m → t)
///
/// Optimizations:
/// - Flat reverse adjacency (no edge_idx indirection)
/// - 4-ary heap with decrease-key (no stale entries)
/// - Prefix-sum bucket layout (O(1) lookup)
/// - Version-stamped distances (O(1) per-search init)
pub fn table_bucket(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev: &DownReverseAdj,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    // Build flat reverse adjacency with embedded weights
    let down_rev_flat = DownReverseAdjFlat::build(topo, weights);

    table_bucket_optimized(topo, weights, &down_rev_flat, sources, targets)
}

/// Optimized version using pre-built flat reverse adjacency
pub fn table_bucket_optimized(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    let n_nodes = topo.n_nodes as usize;
    let n_sources = sources.len();
    let n_targets = targets.len();

    let mut matrix = vec![u32::MAX; n_sources * n_targets];

    if n_sources == 0 || n_targets == 0 {
        return (matrix, BucketM2MStats::default());
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // Estimate for pre-allocation
    let avg_visited = (n_nodes / 400).max(500).min(20000);

    // Single reusable search state
    let mut state = SearchState::new(n_nodes, avg_visited);

    // ========== PHASE 1: Forward searches from SOURCES (UP edges) ==========
    let forward_start = std::time::Instant::now();

    // Collect bucket items: (node, source_idx, dist)
    let mut bucket_items: Vec<(u32, u16, u32)> = Vec::with_capacity(n_sources * avg_visited);

    for (source_idx, &source) in sources.iter().enumerate() {
        if source as usize >= n_nodes {
            continue;
        }
        forward_fill_buckets_opt(
            topo,
            &weights.up,
            source_idx as u16,
            source,
            &mut state,
            &mut bucket_items,
        );
    }

    stats.forward_visited = bucket_items.len();
    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

    // ========== PHASE 2: Sort buckets for binary search ==========
    let sort_start = std::time::Instant::now();
    let buckets = SortedBuckets::build(bucket_items);
    stats.bucket_items = buckets.total_items();
    stats.bucket_nodes = buckets.n_nodes_with_buckets();
    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

    // ========== PHASE 3: Backward searches from TARGETS ==========
    let backward_start = std::time::Instant::now();

    for (target_idx, &target) in targets.iter().enumerate() {
        if target as usize >= n_nodes {
            continue;
        }

        let (visited, joins) = backward_join_opt(
            down_rev_flat,
            target,
            &buckets,
            &mut matrix,
            n_targets,
            target_idx,
            &mut state,
        );

        stats.backward_visited += visited;
        stats.join_operations += joins;
    }

    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    // Collect stats
    stats.heap_pushes = state.pushes;
    stats.heap_pops = state.pops;
    stats.stale_pops = state.stale_pops;

    (matrix, stats)
}

/// Reusable M2M engine to avoid per-call allocations
pub struct BucketM2MEngine {
    n_nodes: usize,
    state: SearchState,
    bucket_items: Vec<(u32, u16, u32)>,
}

impl BucketM2MEngine {
    /// Create a new engine for the given graph size
    pub fn new(n_nodes: usize) -> Self {
        let avg_visited = (n_nodes / 400).max(500).min(20000);
        Self {
            n_nodes,
            state: SearchState::new(n_nodes, avg_visited),
            bucket_items: Vec::with_capacity(avg_visited * 100),
        }
    }

    /// Compute distance matrix using pre-allocated state
    pub fn compute(
        &mut self,
        topo: &CchTopo,
        weights: &CchWeights,
        down_rev_flat: &DownReverseAdjFlat,
        sources: &[u32],
        targets: &[u32],
    ) -> (Vec<u32>, BucketM2MStats) {
        let n_sources = sources.len();
        let n_targets = targets.len();

        let mut matrix = vec![u32::MAX; n_sources * n_targets];

        if n_sources == 0 || n_targets == 0 {
            return (matrix, BucketM2MStats::default());
        }

        let mut stats = BucketM2MStats {
            n_sources,
            n_targets,
            ..Default::default()
        };

        // Clear bucket items (reuse allocation)
        self.bucket_items.clear();

        // Reset counters for this computation
        self.state.pushes = 0;
        self.state.pops = 0;
        self.state.stale_pops = 0;

        // ========== PHASE 1: Forward searches from SOURCES (UP edges) ==========
        let forward_start = std::time::Instant::now();

        for (source_idx, &source) in sources.iter().enumerate() {
            if source as usize >= self.n_nodes {
                continue;
            }
            forward_fill_buckets_opt(
                topo,
                &weights.up,
                source_idx as u16,
                source,
                &mut self.state,
                &mut self.bucket_items,
            );
        }

        stats.forward_visited = self.bucket_items.len();
        stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

        // ========== PHASE 2: Sort buckets for binary search ==========
        let sort_start = std::time::Instant::now();
        let bucket_items = std::mem::take(&mut self.bucket_items);
        let buckets = SortedBuckets::build(bucket_items);
        stats.bucket_items = buckets.total_items();
        stats.bucket_nodes = buckets.n_nodes_with_buckets();
        stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

        // ========== PHASE 3: Backward searches from TARGETS ==========
        let backward_start = std::time::Instant::now();

        for (target_idx, &target) in targets.iter().enumerate() {
            if target as usize >= self.n_nodes {
                continue;
            }

            let (visited, joins) = backward_join_opt(
                down_rev_flat,
                target,
                &buckets,
                &mut matrix,
                n_targets,
                target_idx,
                &mut self.state,
            );

            stats.backward_visited += visited;
            stats.join_operations += joins;
        }

        stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

        // Restore bucket_items for reuse
        self.bucket_items = buckets.into_items();

        // Collect stats
        stats.heap_pushes = self.state.pushes;
        stats.heap_pops = self.state.pops;
        stats.stale_pops = self.state.stale_pops;

        (matrix, stats)
    }

    /// Compute using pre-built flat UP adjacency (no INF checks in forward loop)
    pub fn compute_flat(
        &mut self,
        up_adj_flat: &UpAdjFlat,
        down_rev_flat: &DownReverseAdjFlat,
        sources: &[u32],
        targets: &[u32],
    ) -> (Vec<u32>, BucketM2MStats) {
        let n_sources = sources.len();
        let n_targets = targets.len();

        let mut matrix = vec![u32::MAX; n_sources * n_targets];

        if n_sources == 0 || n_targets == 0 {
            return (matrix, BucketM2MStats::default());
        }

        let mut stats = BucketM2MStats {
            n_sources,
            n_targets,
            ..Default::default()
        };

        // Clear bucket items (reuse allocation)
        self.bucket_items.clear();

        // Reset counters for this computation
        self.state.pushes = 0;
        self.state.pops = 0;
        self.state.stale_pops = 0;

        // ========== PHASE 1: Forward searches from SOURCES (UP edges, pre-filtered) ==========
        let forward_start = std::time::Instant::now();

        for (source_idx, &source) in sources.iter().enumerate() {
            if source as usize >= self.n_nodes {
                continue;
            }
            forward_fill_buckets_flat(
                up_adj_flat,
                source_idx as u16,
                source,
                &mut self.state,
                &mut self.bucket_items,
            );
        }

        stats.forward_visited = self.bucket_items.len();
        stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

        // ========== PHASE 2: Sort buckets for binary search ==========
        let sort_start = std::time::Instant::now();
        let bucket_items = std::mem::take(&mut self.bucket_items);
        let buckets = SortedBuckets::build(bucket_items);
        stats.bucket_items = buckets.total_items();
        stats.bucket_nodes = buckets.n_nodes_with_buckets();
        stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

        // ========== PHASE 3: Backward searches from TARGETS ==========
        let backward_start = std::time::Instant::now();

        for (target_idx, &target) in targets.iter().enumerate() {
            if target as usize >= self.n_nodes {
                continue;
            }

            let (visited, joins) = backward_join_opt(
                down_rev_flat,
                target,
                &buckets,
                &mut matrix,
                n_targets,
                target_idx,
                &mut self.state,
            );

            stats.backward_visited += visited;
            stats.join_operations += joins;
        }

        stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

        // Restore bucket_items for reuse
        self.bucket_items = buckets.into_items();

        // Collect stats
        stats.heap_pushes = self.state.pushes;
        stats.heap_pops = self.state.pops;
        stats.stale_pops = self.state.stale_pops;

        (matrix, stats)
    }

    /// Compute with stall-on-demand optimization in forward search
    /// Returns (matrix, stats, total_stalls, total_non_stalls)
    pub fn compute_with_stall(
        &mut self,
        up_adj_flat: &UpAdjFlat,
        up_rev_flat: &UpReverseAdjFlat,
        down_rev_flat: &DownReverseAdjFlat,
        sources: &[u32],
        targets: &[u32],
    ) -> (Vec<u32>, BucketM2MStats, usize, usize) {
        let n_sources = sources.len();
        let n_targets = targets.len();

        let mut matrix = vec![u32::MAX; n_sources * n_targets];

        if n_sources == 0 || n_targets == 0 {
            return (matrix, BucketM2MStats::default(), 0, 0);
        }

        let mut stats = BucketM2MStats {
            n_sources,
            n_targets,
            ..Default::default()
        };

        // Clear bucket items (reuse allocation)
        self.bucket_items.clear();

        // Reset counters for this computation
        self.state.pushes = 0;
        self.state.pops = 0;
        self.state.stale_pops = 0;

        // ========== PHASE 1: Forward searches with stall-on-demand ==========
        let forward_start = std::time::Instant::now();

        let mut total_stalls = 0usize;
        let mut total_non_stalls = 0usize;

        for (source_idx, &source) in sources.iter().enumerate() {
            if source as usize >= self.n_nodes {
                continue;
            }
            let (stalls, non_stalls) = forward_fill_buckets_with_stall(
                up_adj_flat,
                up_rev_flat,
                source_idx as u16,
                source,
                &mut self.state,
                &mut self.bucket_items,
            );
            total_stalls += stalls;
            total_non_stalls += non_stalls;
        }

        stats.forward_visited = self.bucket_items.len();
        stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

        // ========== PHASE 2: Sort buckets for binary search ==========
        let sort_start = std::time::Instant::now();
        let bucket_items = std::mem::take(&mut self.bucket_items);
        let buckets = SortedBuckets::build(bucket_items);
        stats.bucket_items = buckets.total_items();
        stats.bucket_nodes = buckets.n_nodes_with_buckets();
        stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

        // ========== PHASE 3: Backward searches from TARGETS ==========
        let backward_start = std::time::Instant::now();

        for (target_idx, &target) in targets.iter().enumerate() {
            if target as usize >= self.n_nodes {
                continue;
            }

            let (visited, joins) = backward_join_opt(
                down_rev_flat,
                target,
                &buckets,
                &mut matrix,
                n_targets,
                target_idx,
                &mut self.state,
            );

            stats.backward_visited += visited;
            stats.join_operations += joins;
        }

        stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

        // Restore bucket_items for reuse
        self.bucket_items = buckets.into_items();

        // Collect stats
        stats.heap_pushes = self.state.pushes;
        stats.heap_pops = self.state.pops;
        stats.stale_pops = self.state.stale_pops;

        (matrix, stats, total_stalls, total_non_stalls)
    }
}

/// Fully optimized version using pre-built flat adjacencies for both directions
pub fn table_bucket_full_flat(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    let n_sources = sources.len();
    let n_targets = targets.len();

    let mut matrix = vec![u32::MAX; n_sources * n_targets];

    if n_sources == 0 || n_targets == 0 {
        return (matrix, BucketM2MStats::default());
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // Estimate for pre-allocation
    let avg_visited = (n_nodes / 400).max(500).min(20000);

    // Single reusable search state
    let mut state = SearchState::new(n_nodes, avg_visited);

    // ========== PHASE 1: Forward searches from SOURCES (UP edges) ==========
    let forward_start = std::time::Instant::now();

    // Collect bucket items: (node, source_idx, dist)
    let mut bucket_items: Vec<(u32, u16, u32)> = Vec::with_capacity(n_sources * avg_visited);

    for (source_idx, &source) in sources.iter().enumerate() {
        if source as usize >= n_nodes {
            continue;
        }
        forward_fill_buckets_flat(
            up_adj_flat,
            source_idx as u16,
            source,
            &mut state,
            &mut bucket_items,
        );
    }

    stats.forward_visited = bucket_items.len();
    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

    // ========== PHASE 2: Sort buckets for binary search ==========
    let sort_start = std::time::Instant::now();
    let buckets = SortedBuckets::build(bucket_items);
    stats.bucket_items = buckets.total_items();
    stats.bucket_nodes = buckets.n_nodes_with_buckets();
    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

    // ========== PHASE 3: Backward searches from TARGETS ==========
    let backward_start = std::time::Instant::now();

    for (target_idx, &target) in targets.iter().enumerate() {
        if target as usize >= n_nodes {
            continue;
        }

        let (visited, joins) = backward_join_opt(
            down_rev_flat,
            target,
            &buckets,
            &mut matrix,
            n_targets,
            target_idx,
            &mut state,
        );

        stats.backward_visited += visited;
        stats.join_operations += joins;
    }

    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    // Collect stats
    stats.heap_pushes = state.pushes;
    stats.heap_pops = state.pops;
    stats.stale_pops = state.stale_pops;

    (matrix, stats)
}

/// Forward search using flat UP adjacency (no INF check in hot loop)
fn forward_fill_buckets_flat(
    up_adj_flat: &UpAdjFlat,
    source_idx: u16,
    source: u32,
    state: &mut SearchState,
    bucket_items: &mut Vec<(u32, u16, u32)>,
) {
    state.start_search();
    state.relax(source, 0);

    while let Some((d, u)) = state.pop() {
        bucket_items.push((u, source_idx, d));

        // Relax UP edges (no INF check - pre-filtered)
        let start = up_adj_flat.offsets[u as usize] as usize;
        let end = up_adj_flat.offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = up_adj_flat.targets[i];
            let w = up_adj_flat.weights[i];
            let new_dist = d.saturating_add(w);
            state.relax(v, new_dist);
        }
    }
}

/// Forward search with stall-on-demand optimization
/// When we pop node u with distance du, check if there exists a settled node p
/// with incoming UP edge (p → u) where dp + w(p→u) < du.
/// If so, we can "stall" u (skip relaxing its outgoing edges).
/// Returns (stalls, non_stalls) for instrumentation
fn forward_fill_buckets_with_stall(
    up_adj_flat: &UpAdjFlat,
    up_rev_flat: &UpReverseAdjFlat,
    source_idx: u16,
    source: u32,
    state: &mut SearchState,
    bucket_items: &mut Vec<(u32, u16, u32)>,
) -> (usize, usize) {
    state.start_search();
    state.relax(source, 0);

    let mut stalls = 0usize;
    let mut non_stalls = 0usize;

    while let Some((du, u)) = state.pop() {
        bucket_items.push((u, source_idx, du));

        // Stall-on-demand check: can we reach u more cheaply via an incoming UP edge?
        let mut should_stall = false;
        let in_start = up_rev_flat.offsets[u as usize] as usize;
        let in_end = up_rev_flat.offsets[u as usize + 1] as usize;

        for i in in_start..in_end {
            let p = up_rev_flat.sources[i];
            let w = up_rev_flat.weights[i];
            let dp = state.get_dist(p);
            // If p is settled (dp < INF) and dp + w < du, we can stall u
            if dp != u32::MAX && dp.saturating_add(w) < du {
                should_stall = true;
                break;
            }
        }

        if should_stall {
            stalls += 1;
            continue;  // Skip relaxing outgoing edges
        }

        non_stalls += 1;

        // Relax UP edges (no INF check - pre-filtered)
        let start = up_adj_flat.offsets[u as usize] as usize;
        let end = up_adj_flat.offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = up_adj_flat.targets[i];
            let w = up_adj_flat.weights[i];
            let new_dist = du.saturating_add(w);
            state.relax(v, new_dist);
        }
    }

    (stalls, non_stalls)
}

/// Forward search from source using UP edges, collecting bucket items
fn forward_fill_buckets_opt(
    topo: &CchTopo,
    weights_up: &[u32],
    source_idx: u16,
    source: u32,
    state: &mut SearchState,
    bucket_items: &mut Vec<(u32, u16, u32)>,
) {
    state.start_search();
    state.relax(source, 0);

    while let Some((d, u)) = state.pop() {
        bucket_items.push((u, source_idx, d));

        // Relax UP edges
        let start = topo.up_offsets[u as usize] as usize;
        let end = topo.up_offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = topo.up_targets[i];
            let w = weights_up[i];

            if w == u32::MAX {
                continue;
            }

            let new_dist = d.saturating_add(w);
            state.relax(v, new_dist);
        }
    }
}

/// Backward search from target using flat reverse adjacency, joining with buckets
fn backward_join_opt(
    down_rev_flat: &DownReverseAdjFlat,
    target: u32,
    buckets: &SortedBuckets,
    matrix: &mut [u32],
    n_targets: usize,
    target_idx: usize,
    state: &mut SearchState,
) -> (usize, usize) {  // (visited, joins)
    state.start_search();
    state.relax(target, 0);

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, u)) = state.pop() {
        visited += 1;

        // Binary search bucket lookup
        for (source_idx, bucket_dist) in buckets.get(u) {
            let total = bucket_dist.saturating_add(d);
            let cell = source_idx as usize * n_targets + target_idx;
            if total < matrix[cell] {
                matrix[cell] = total;
            }
            joins += 1;
        }

        // Relax reversed DOWN edges using flat adjacency (no edge_idx indirection!)
        let edge_start = down_rev_flat.offsets[u as usize] as usize;
        let edge_end = down_rev_flat.offsets[u as usize + 1] as usize;

        for i in edge_start..edge_end {
            let x = down_rev_flat.sources[i];
            let w = down_rev_flat.weights[i];
            let new_dist = d.saturating_add(w);
            state.relax(x, new_dist);
        }
    }

    (visited, joins)
}

/// Backward search from target using flat reverse adjacency, joining with prefix-sum buckets
/// O(1) bucket lookup instead of O(log n) binary search
fn backward_join_prefix(
    down_rev_flat: &DownReverseAdjFlat,
    target: u32,
    buckets: &PrefixSumBuckets,
    matrix: &mut [u32],
    n_targets: usize,
    target_idx: usize,
    state: &mut SearchState,
) -> (usize, usize) {
    state.start_search();
    state.relax(target, 0);

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, u)) = state.pop() {
        visited += 1;

        // O(1) prefix-sum bucket lookup (no binary search)
        let bucket_entries = buckets.get(u);
        for entry in bucket_entries {
            let total = entry.dist.saturating_add(d);
            let cell = entry.source_idx as usize * n_targets + target_idx;
            if total < matrix[cell] {
                matrix[cell] = total;
            }
            joins += 1;
        }

        // Relax reversed DOWN edges using flat adjacency (no edge_idx indirection!)
        // INF edges already filtered during build, so no need to check here
        let edge_start = down_rev_flat.offsets[u as usize] as usize;
        let edge_end = down_rev_flat.offsets[u as usize + 1] as usize;

        for i in edge_start..edge_end {
            let x = down_rev_flat.sources[i];
            let w = down_rev_flat.weights[i]; // Direct access, no indirection!
            let new_dist = d.saturating_add(w);
            state.relax(x, new_dist);
        }
    }

    (visited, joins)
}

// =============================================================================
// PARALLEL BUCKET M2M
// =============================================================================

use rayon::prelude::*;

/// Parallel bucket M2M computation
/// Uses rayon to parallelize both forward and backward phases
pub fn table_bucket_parallel(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    let n_sources = sources.len();
    let n_targets = targets.len();

    if n_sources == 0 || n_targets == 0 {
        return (vec![u32::MAX; n_sources * n_targets], BucketM2MStats::default());
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // ========== PHASE 1: Parallel forward searches from SOURCES ==========
    let forward_start = std::time::Instant::now();

    // Each source produces its own bucket items
    let bucket_chunks: Vec<Vec<(u32, u16, u32)>> = sources
        .par_iter()
        .enumerate()
        .filter_map(|(source_idx, &source)| {
            if source as usize >= n_nodes {
                return None;
            }

            // Thread-local search state
            let avg_visited = (n_nodes / 400).max(500).min(20000);
            let mut state = SearchState::new(n_nodes, avg_visited);
            let mut bucket_items = Vec::with_capacity(avg_visited);

            forward_fill_buckets_flat(
                up_adj_flat,
                source_idx as u16,
                source,
                &mut state,
                &mut bucket_items,
            );

            Some(bucket_items)
        })
        .collect();

    // Merge all bucket chunks
    let mut bucket_items: Vec<(u32, u16, u32)> = bucket_chunks.into_iter().flatten().collect();
    stats.forward_visited = bucket_items.len();
    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;

    // ========== PHASE 2: Sort buckets (parallel sort) ==========
    let sort_start = std::time::Instant::now();
    bucket_items.par_sort_unstable_by_key(|(node, _, _)| *node);
    let buckets = SortedBuckets::from_sorted(bucket_items);
    stats.bucket_items = buckets.total_items();
    stats.bucket_nodes = buckets.n_nodes_with_buckets();
    stats.sort_time_ms = sort_start.elapsed().as_millis() as u64;

    // ========== PHASE 3: Parallel backward searches from TARGETS ==========
    let backward_start = std::time::Instant::now();

    // Pre-allocate matrix
    let matrix: Vec<std::sync::atomic::AtomicU32> = (0..n_sources * n_targets)
        .map(|_| std::sync::atomic::AtomicU32::new(u32::MAX))
        .collect();

    // Parallel backward phase - each target can run independently
    let (total_visited, total_joins): (usize, usize) = targets
        .par_iter()
        .enumerate()
        .filter(|(_, &target)| (target as usize) < n_nodes)
        .map(|(target_idx, &target)| {
            // Thread-local search state
            let avg_visited = (n_nodes / 400).max(500).min(20000);
            let mut state = SearchState::new(n_nodes, avg_visited);

            backward_join_parallel(
                down_rev_flat,
                target,
                &buckets,
                &matrix,
                n_targets,
                target_idx,
                &mut state,
            )
        })
        .reduce(|| (0, 0), |(v1, j1), (v2, j2)| (v1 + v2, j1 + j2));

    stats.backward_visited = total_visited;
    stats.join_operations = total_joins;
    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    // Convert atomic matrix to regular Vec
    let result_matrix: Vec<u32> = matrix
        .into_iter()
        .map(|a| a.into_inner())
        .collect();

    (result_matrix, stats)
}

/// Backward join for parallel execution (uses atomic matrix)
fn backward_join_parallel(
    down_rev_flat: &DownReverseAdjFlat,
    target: u32,
    buckets: &SortedBuckets,
    matrix: &[std::sync::atomic::AtomicU32],
    n_targets: usize,
    target_idx: usize,
    state: &mut SearchState,
) -> (usize, usize) {
    use std::sync::atomic::Ordering;

    state.start_search();
    state.relax(target, 0);

    let mut visited = 0usize;
    let mut joins = 0usize;

    while let Some((d, u)) = state.pop() {
        visited += 1;

        // Join with buckets at this node
        for (source_idx, dist_to_source) in buckets.get(u) {
            joins += 1;
            let total_dist = dist_to_source.saturating_add(d);
            let idx = source_idx as usize * n_targets + target_idx;

            // Atomic min update
            matrix[idx].fetch_min(total_dist, Ordering::Relaxed);
        }

        // Relax DOWN-reverse edges
        let edge_start = down_rev_flat.offsets[u as usize] as usize;
        let edge_end = down_rev_flat.offsets[u as usize + 1] as usize;

        for i in edge_start..edge_end {
            let x = down_rev_flat.sources[i];
            let w = down_rev_flat.weights[i];
            let new_dist = d.saturating_add(w);
            state.relax(x, new_dist);
        }
    }

    (visited, joins)
}

// =============================================================================
// LEGACY API - For compatibility with existing code
// =============================================================================

pub struct BucketArena {
    items: Vec<(u32, u16, u32)>,
}

impl BucketArena {
    pub fn new(_n_nodes: usize, _n_sources: usize, _avg_visited_per_source: usize) -> Self {
        Self { items: Vec::new() }
    }

    #[inline]
    pub fn push(&mut self, node: u32, source_idx: u16, dist: u32) -> bool {
        self.items.push((node, source_idx, dist));
        true
    }

    #[inline]
    pub fn get(&self, _node: u32) -> &[(u16, u32)] {
        &[]
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn total_items(&self) -> usize {
        self.items.len()
    }
}
