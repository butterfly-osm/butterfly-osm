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
//! ## Key Optimizations
//!
//! - **Sparse buckets**: HashMap storage, no fixed capacity limits
//! - **Parallel backward phase**: rayon parallel processing of targets
//! - **Search versioning**: Avoid O(N) dist array initialization per search
//! - **Cache-friendly columns**: Local target column, merged at end

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use rayon::prelude::*;
use rustc_hash::FxHashMap;

use crate::formats::{CchTopo, CchWeights};
use crate::step9::state::DownReverseAdj;

/// Thread-local search state with versioning
///
/// Instead of zeroing the entire dist array (O(N)) for each search,
/// we use a version counter. A node is "unvisited" if its version
/// doesn't match the current search version.
struct VersionedSearchState {
    dist: Vec<u32>,
    version: Vec<u32>,
    current_version: u32,
    visited: Vec<u32>,
    pq: BinaryHeap<Reverse<(u32, u32)>>,
}

impl VersionedSearchState {
    fn new(n_nodes: usize, avg_visited: usize) -> Self {
        Self {
            dist: vec![0; n_nodes],
            version: vec![0; n_nodes],
            current_version: 0,
            visited: Vec::with_capacity(avg_visited),
            pq: BinaryHeap::with_capacity(avg_visited),
        }
    }

    /// Start a new search, incrementing version instead of zeroing dist
    #[inline]
    fn start_search(&mut self) {
        self.current_version = self.current_version.wrapping_add(1);
        // Handle version wrap-around (every 4B searches, reset everything)
        if self.current_version == 0 {
            self.version.fill(0);
            self.current_version = 1;
        }
        self.visited.clear();
        self.pq.clear();
    }

    /// Get distance to node, returning u32::MAX if node not visited in current search
    #[inline]
    fn get_dist(&self, node: u32) -> u32 {
        let idx = node as usize;
        if self.version[idx] == self.current_version {
            self.dist[idx]
        } else {
            u32::MAX
        }
    }

    /// Set distance to node in current search
    #[inline]
    fn set_dist(&mut self, node: u32, d: u32) {
        let idx = node as usize;
        self.dist[idx] = d;
        self.version[idx] = self.current_version;
    }
}

/// Sparse bucket storage using HashMap
///
/// Only nodes that are actually visited have buckets allocated.
/// Each bucket can hold all sources without overflow.
pub struct SparseBuckets {
    /// Map from node ID to list of (source_idx, distance) pairs
    buckets: FxHashMap<u32, Vec<(u16, u32)>>,
    /// Total number of items stored
    total_items: usize,
}

// SparseBuckets is Sync because HashMap is Sync when keys and values are Sync
unsafe impl Sync for SparseBuckets {}

impl SparseBuckets {
    /// Create empty sparse buckets
    pub fn new() -> Self {
        Self {
            buckets: FxHashMap::default(),
            total_items: 0,
        }
    }

    /// Create with pre-allocated capacity for expected number of visited nodes
    pub fn with_capacity(expected_nodes: usize) -> Self {
        Self {
            buckets: FxHashMap::with_capacity_and_hasher(expected_nodes, Default::default()),
            total_items: 0,
        }
    }

    /// Push an entry into a node's bucket
    #[inline]
    pub fn push(&mut self, node: u32, source_idx: u16, dist: u32) {
        self.buckets
            .entry(node)
            .or_insert_with(Vec::new)
            .push((source_idx, dist));
        self.total_items += 1;
    }

    /// Get all entries in a node's bucket
    #[inline]
    pub fn get(&self, node: u32) -> &[(u16, u32)] {
        self.buckets.get(&node).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Total number of items stored
    pub fn total_items(&self) -> usize {
        self.total_items
    }

    /// Number of nodes with buckets
    pub fn num_buckets(&self) -> usize {
        self.buckets.len()
    }
}

impl Default for SparseBuckets {
    fn default() -> Self {
        Self::new()
    }
}

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
    pub forward_time_ms: u64,
    pub backward_time_ms: u64,
}

/// Compute many-to-many distance matrix using bucket algorithm
///
/// Uses the correct directed-graph formulation:
///   d(s → t) = min over m: d(s → m) + d(m → t)
///
/// - Source phase: forward UP search computes d(s → m)
/// - Target phase: reverse search (via DownReverseAdj) computes d(m → t)
///
/// # Arguments
/// * `topo` - CCH topology (up/down graphs)
/// * `weights` - CCH edge weights
/// * `down_rev` - Reverse adjacency for backward/target search
/// * `sources` - Source node IDs (in CCH/filtered space)
/// * `targets` - Target node IDs (in CCH/filtered space)
///
/// # Returns
/// Row-major matrix of distances (n_sources × n_targets), u32::MAX for unreachable
pub fn table_bucket(
    topo: &CchTopo,
    weights: &CchWeights,
    down_rev: &DownReverseAdj,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, BucketM2MStats) {
    let n_nodes = topo.n_nodes as usize;
    let n_sources = sources.len();
    let n_targets = targets.len();

    // Initialize result matrix
    let mut matrix = vec![u32::MAX; n_sources * n_targets];

    if n_sources == 0 || n_targets == 0 {
        return (matrix, BucketM2MStats::default());
    }

    let mut stats = BucketM2MStats {
        n_sources,
        n_targets,
        ..Default::default()
    };

    // Estimate visited nodes per source (typical CH search visits ~0.1% of graph)
    let avg_visited = (n_nodes / 400).max(500).min(20000);
    let expected_bucket_nodes = n_sources * avg_visited;

    // Create sparse buckets
    let mut buckets = SparseBuckets::with_capacity(expected_bucket_nodes);

    // Pre-allocate search structures for reuse (forward phase only)
    let mut fwd_dist = vec![u32::MAX; n_nodes];
    let mut fwd_visited: Vec<u32> = Vec::with_capacity(avg_visited);
    let mut fwd_pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::with_capacity(avg_visited);

    // ========== Forward Phase: Populate buckets (sequential) ==========
    let forward_start = std::time::Instant::now();

    for (source_idx, &source) in sources.iter().enumerate() {
        if source as usize >= n_nodes {
            continue;
        }

        let visited = forward_search_with_buckets(
            topo,
            &weights.up,
            source_idx as u16,
            source,
            &mut buckets,
            &mut fwd_dist,
            &mut fwd_visited,
            &mut fwd_pq,
        );
        stats.forward_visited += visited;
    }

    stats.forward_time_ms = forward_start.elapsed().as_millis() as u64;
    stats.bucket_items = buckets.total_items();
    stats.bucket_nodes = buckets.num_buckets();

    // ========== Backward Phase: Join with buckets (parallel) ==========
    let backward_start = std::time::Instant::now();

    // Use thread-local versioned search state to avoid O(N) initialization per search
    thread_local! {
        static SEARCH_STATE: std::cell::RefCell<Option<VersionedSearchState>> = const { std::cell::RefCell::new(None) };
    }

    // Process targets in parallel, each computing a local column
    let results: Vec<(usize, Vec<u32>, usize, usize)> = targets
        .par_iter()
        .enumerate()
        .filter_map(|(target_idx, &target)| {
            if target as usize >= n_nodes {
                return None;
            }

            // Get or create thread-local search state
            SEARCH_STATE.with(|cell| {
                let mut state_opt = cell.borrow_mut();
                if state_opt.is_none() {
                    *state_opt = Some(VersionedSearchState::new(n_nodes, avg_visited));
                }
                let state = state_opt.as_mut().unwrap();

                // Local column for cache-friendly updates
                let mut target_column = vec![u32::MAX; n_sources];

                let (visited, joins) = backward_search_versioned(
                    &weights.down,
                    down_rev,
                    target,
                    &buckets,
                    &mut target_column,
                    state,
                );

                Some((target_idx, target_column, visited, joins))
            })
        })
        .collect();

    // Merge results back into the matrix
    for (target_idx, target_column, visited, joins) in results {
        stats.backward_visited += visited;
        stats.join_operations += joins;

        // Copy column into row-major matrix
        for (source_idx, &dist) in target_column.iter().enumerate() {
            if dist != u32::MAX {
                matrix[source_idx * n_targets + target_idx] = dist;
            }
        }
    }

    stats.backward_time_ms = backward_start.elapsed().as_millis() as u64;

    (matrix, stats)
}

/// Forward search from a source, populating buckets
///
/// Runs Dijkstra on the UP graph, storing (source_idx, dist) in bucket[v]
/// for each visited node v.
///
/// Uses pre-allocated dist/visited/pq to avoid allocations.
fn forward_search_with_buckets(
    topo: &CchTopo,
    weights_up: &[u32],
    source_idx: u16,
    source: u32,
    buckets: &mut SparseBuckets,
    dist: &mut [u32],
    visited_nodes: &mut Vec<u32>,
    pq: &mut BinaryHeap<Reverse<(u32, u32)>>,
) -> usize {
    // Reset only previously visited nodes
    for &v in visited_nodes.iter() {
        dist[v as usize] = u32::MAX;
    }
    visited_nodes.clear();
    pq.clear();

    let mut visited_count = 0usize;

    dist[source as usize] = 0;
    pq.push(Reverse((0, source)));

    while let Some(Reverse((d, u))) = pq.pop() {
        if d > dist[u as usize] {
            continue; // Stale entry
        }

        visited_count += 1;
        visited_nodes.push(u);

        // Store in bucket
        buckets.push(u, source_idx, d);

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
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(Reverse((new_dist, v)));
            }
        }
    }

    visited_count
}

/// Reverse search from a target using versioned search state
///
/// Uses VersionedSearchState to avoid O(N) dist array reset per search.
/// This is the main performance optimization for the backward phase.
fn backward_search_versioned(
    weights_down: &[u32],
    down_rev: &DownReverseAdj,
    target: u32,
    buckets: &SparseBuckets,
    target_column: &mut [u32],
    state: &mut VersionedSearchState,
) -> (usize, usize) {
    // Start new search (O(1) instead of O(N))
    state.start_search();

    let mut visited_count = 0usize;
    let mut joins = 0usize;

    state.set_dist(target, 0);
    state.pq.push(Reverse((0, target)));

    while let Some(Reverse((d, u))) = state.pq.pop() {
        let current_dist = state.get_dist(u);
        if d > current_dist {
            continue;
        }

        visited_count += 1;

        // Join with bucket[u]
        let bucket = buckets.get(u);
        for &(source_idx, d_s_to_m) in bucket {
            let total = d_s_to_m.saturating_add(d);
            let cell = source_idx as usize;
            if total < target_column[cell] {
                target_column[cell] = total;
            }
            joins += 1;
        }

        // Relax REVERSED DOWN edges
        let start = down_rev.offsets[u as usize] as usize;
        let end = down_rev.offsets[u as usize + 1] as usize;

        for i in start..end {
            let x = down_rev.sources[i];
            let edge_idx = down_rev.edge_idx[i] as usize;
            let w = weights_down[edge_idx];

            if w == u32::MAX {
                continue;
            }

            let new_dist = d.saturating_add(w);
            let old_dist = state.get_dist(x);
            if new_dist < old_dist {
                state.set_dist(x, new_dist);
                state.pq.push(Reverse((new_dist, x)));
            }
        }
    }

    (visited_count, joins)
}

// ============= Legacy BucketArena for API compatibility =============

/// Bucket arena - flat storage for all bucket entries (legacy, kept for API compatibility)
pub struct BucketArena {
    sparse: SparseBuckets,
}

impl BucketArena {
    pub fn new(n_nodes: usize, n_sources: usize, avg_visited_per_source: usize) -> Self {
        let expected = n_sources * avg_visited_per_source / 10;
        Self {
            sparse: SparseBuckets::with_capacity(expected.max(1000)),
        }
    }

    #[inline]
    pub fn push(&mut self, node: u32, source_idx: u16, dist: u32) -> bool {
        self.sparse.push(node, source_idx, dist);
        true
    }

    #[inline]
    pub fn get(&self, node: u32) -> &[(u16, u32)] {
        self.sparse.get(node)
    }

    pub fn clear(&mut self) {
        self.sparse = SparseBuckets::new();
    }

    pub fn total_items(&self) -> usize {
        self.sparse.total_items()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sparse_buckets_basic() {
        let mut buckets = SparseBuckets::new();

        // Push some entries
        buckets.push(5, 0, 100);
        buckets.push(5, 1, 200);
        buckets.push(10, 2, 50);

        // Retrieve
        let bucket5 = buckets.get(5);
        assert_eq!(bucket5.len(), 2);
        assert_eq!(bucket5[0], (0, 100));
        assert_eq!(bucket5[1], (1, 200));

        let bucket10 = buckets.get(10);
        assert_eq!(bucket10.len(), 1);
        assert_eq!(bucket10[0], (2, 50));

        // Empty bucket
        let bucket0 = buckets.get(0);
        assert_eq!(bucket0.len(), 0);

        assert_eq!(buckets.total_items(), 3);
        assert_eq!(buckets.num_buckets(), 2);
    }

    #[test]
    fn test_sparse_buckets_many_sources() {
        let mut buckets = SparseBuckets::new();

        // Many sources converging at same node (simulating CH behavior)
        for source_idx in 0..100u16 {
            buckets.push(999, source_idx, source_idx as u32 * 10);
        }

        let bucket = buckets.get(999);
        assert_eq!(bucket.len(), 100);
        assert_eq!(buckets.total_items(), 100);
    }
}
