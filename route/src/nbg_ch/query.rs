//! Optimized NBG CH bucket M2M query
//!
//! Key optimizations:
//! 1. Flat adjacency structure (cache-friendly)
//! 2. Version-stamped distances (O(1) reset)
//! 3. Sorted buckets with binary search
//! 4. Reusable search state (zero allocation per query)

use std::cmp::Reverse;
use std::collections::BinaryHeap;

use super::NbgChTopo;

/// Flat UP adjacency for cache-friendly access
pub struct FlatUpAdj {
    pub offsets: Vec<u64>,
    pub targets: Vec<u32>,
    pub weights: Vec<u32>,
}

impl FlatUpAdj {
    pub fn from_topo(topo: &NbgChTopo) -> Self {
        Self {
            offsets: topo.up_offsets.clone(),
            targets: topo.up_heads.clone(),
            weights: topo.up_weights.clone(),
        }
    }

    #[inline(always)]
    pub fn neighbors(&self, node: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let start = self.offsets[node as usize] as usize;
        let end = self.offsets[node as usize + 1] as usize;
        (start..end).map(move |i| (self.targets[i], self.weights[i]))
    }
}

/// Version-stamped distance entry (8 bytes, cache-line friendly)
#[derive(Clone, Copy)]
struct DistEntry {
    dist: u32,
    version: u32,
}

/// Reusable search state with version stamping
pub struct SearchState {
    dist: Vec<DistEntry>,
    version: u32,
    heap: BinaryHeap<Reverse<(u32, u32)>>,
}

impl SearchState {
    pub fn new(n_nodes: usize) -> Self {
        Self {
            dist: vec![
                DistEntry {
                    dist: u32::MAX,
                    version: 0
                };
                n_nodes
            ],
            version: 0,
            heap: BinaryHeap::with_capacity(1024),
        }
    }

    #[inline(always)]
    fn reset(&mut self) {
        self.version = self.version.wrapping_add(1);
        if self.version == 0 {
            // Version wrapped, need full reset
            for entry in &mut self.dist {
                entry.version = 0;
            }
            self.version = 1;
        }
        self.heap.clear();
    }

    #[inline(always)]
    fn get_dist(&self, node: u32) -> u32 {
        let entry = &self.dist[node as usize];
        if entry.version == self.version {
            entry.dist
        } else {
            u32::MAX
        }
    }

    #[inline(always)]
    fn set_dist(&mut self, node: u32, dist: u32) {
        self.dist[node as usize] = DistEntry {
            dist,
            version: self.version,
        };
    }
}

/// Sorted bucket structure for O(log n) lookup
pub struct SortedBuckets {
    // Flat array of (node, source_idx, dist) sorted by node
    items: Vec<(u32, u32, u32)>,
}

impl Default for SortedBuckets {
    fn default() -> Self {
        Self::new()
    }
}

impl SortedBuckets {
    pub fn new() -> Self {
        Self { items: Vec::new() }
    }

    pub fn clear(&mut self) {
        self.items.clear();
    }

    pub fn add(&mut self, node: u32, source_idx: u32, dist: u32) {
        self.items.push((node, source_idx, dist));
    }

    pub fn sort(&mut self) {
        self.items.sort_unstable_by_key(|(node, _, _)| *node);
    }

    /// Get all bucket entries for a node using binary search
    #[inline(always)]
    pub fn get(&self, node: u32) -> &[(u32, u32, u32)] {
        // Binary search for first occurrence
        let start = self.items.partition_point(|(n, _, _)| *n < node);
        let end = self.items[start..].partition_point(|(n, _, _)| *n == node) + start;
        &self.items[start..end]
    }
}

/// Optimized bucket M2M engine
pub struct NbgBucketM2M {
    n_nodes: usize,
    up_adj: FlatUpAdj,
}

impl NbgBucketM2M {
    pub fn new(topo: &NbgChTopo) -> Self {
        Self {
            n_nodes: topo.n_nodes as usize,
            up_adj: FlatUpAdj::from_topo(topo),
        }
    }

    /// Compute distance matrix with optimizations
    pub fn compute(&self, sources: &[u32], targets: &[u32]) -> (Vec<u32>, NbgM2MStats) {
        let n_sources = sources.len();
        let n_targets = targets.len();

        let start_time = std::time::Instant::now();

        // Reusable state
        let mut state = SearchState::new(self.n_nodes);
        let mut buckets = SortedBuckets::new();

        // Phase 1: Forward searches from sources
        let mut fwd_visited = 0u64;
        for (src_idx, &source) in sources.iter().enumerate() {
            fwd_visited +=
                self.forward_search(source, src_idx as u32, &mut state, &mut buckets) as u64;
        }

        let fwd_time = start_time.elapsed().as_micros();

        // Sort buckets once
        buckets.sort();

        let sort_time = start_time.elapsed().as_micros() - fwd_time;

        // Phase 2: Backward searches from targets
        let mut matrix = vec![u32::MAX; n_sources * n_targets];
        let mut bwd_visited = 0u64;
        let mut joins = 0u64;

        for (tgt_idx, &target) in targets.iter().enumerate() {
            let (visited, j) = self.backward_search(
                target,
                tgt_idx,
                &buckets,
                &mut matrix,
                n_targets,
                &mut state,
            );
            bwd_visited += visited as u64;
            joins += j;
        }

        let total_time = start_time.elapsed().as_micros();

        let stats = NbgM2MStats {
            n_sources,
            n_targets,
            fwd_visited,
            bwd_visited,
            joins,
            fwd_time_us: fwd_time as u64,
            sort_time_us: sort_time as u64,
            bwd_time_us: (total_time - fwd_time - sort_time) as u64,
            total_time_us: total_time as u64,
        };

        (matrix, stats)
    }

    #[inline(never)]
    fn forward_search(
        &self,
        source: u32,
        src_idx: u32,
        state: &mut SearchState,
        buckets: &mut SortedBuckets,
    ) -> usize {
        state.reset();
        state.set_dist(source, 0);
        state.heap.push(Reverse((0, source)));

        let mut visited = 0usize;

        while let Some(Reverse((d, u))) = state.heap.pop() {
            // Skip stale entries
            if d > state.get_dist(u) {
                continue;
            }

            // Add to bucket
            buckets.add(u, src_idx, d);
            visited += 1;

            // Relax UP edges
            for (v, w) in self.up_adj.neighbors(u) {
                let new_dist = d.saturating_add(w);
                if new_dist < state.get_dist(v) {
                    state.set_dist(v, new_dist);
                    state.heap.push(Reverse((new_dist, v)));
                }
            }
        }

        visited
    }

    #[inline(never)]
    fn backward_search(
        &self,
        target: u32,
        tgt_idx: usize,
        buckets: &SortedBuckets,
        matrix: &mut [u32],
        n_targets: usize,
        state: &mut SearchState,
    ) -> (usize, u64) {
        state.reset();
        state.set_dist(target, 0);
        state.heap.push(Reverse((0, target)));

        let mut visited = 0usize;
        let mut joins = 0u64;

        while let Some(Reverse((d, u))) = state.heap.pop() {
            // Skip stale entries
            if d > state.get_dist(u) {
                continue;
            }

            visited += 1;

            // Join with bucket (binary search)
            let bucket_entries = buckets.get(u);
            for &(_, src_idx, src_dist) in bucket_entries {
                let total = src_dist.saturating_add(d);
                let idx = src_idx as usize * n_targets + tgt_idx;
                if total < matrix[idx] {
                    matrix[idx] = total;
                }
                joins += 1;
            }

            // Relax UP edges
            for (v, w) in self.up_adj.neighbors(u) {
                let new_dist = d.saturating_add(w);
                if new_dist < state.get_dist(v) {
                    state.set_dist(v, new_dist);
                    state.heap.push(Reverse((new_dist, v)));
                }
            }
        }

        (visited, joins)
    }
}

#[derive(Debug, Clone)]
pub struct NbgM2MStats {
    pub n_sources: usize,
    pub n_targets: usize,
    pub fwd_visited: u64,
    pub bwd_visited: u64,
    pub joins: u64,
    pub fwd_time_us: u64,
    pub sort_time_us: u64,
    pub bwd_time_us: u64,
    pub total_time_us: u64,
}

// Keep old interface for compatibility
#[allow(dead_code)]
pub struct NbgChQuery<'a> {
    topo: &'a NbgChTopo,
    state: SearchState,
}

impl<'a> NbgChQuery<'a> {
    pub fn new(topo: &'a NbgChTopo) -> Self {
        Self {
            topo,
            state: SearchState::new(topo.n_nodes as usize),
        }
    }
}
