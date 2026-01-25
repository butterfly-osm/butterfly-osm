//! NBG CH bidirectional query
//!
//! Simple bidirectional Dijkstra on NBG CH for distance queries.

use std::collections::BinaryHeap;
use std::cmp::Reverse;

use super::NbgChTopo;

/// Query engine for NBG CH
pub struct NbgChQuery<'a> {
    topo: &'a NbgChTopo,
    // Reusable search state
    fwd_dist: Vec<u32>,
    bwd_dist: Vec<u32>,
    fwd_version: Vec<u32>,
    bwd_version: Vec<u32>,
    current_version: u32,
}

impl<'a> NbgChQuery<'a> {
    pub fn new(topo: &'a NbgChTopo) -> Self {
        let n = topo.n_nodes as usize;
        Self {
            topo,
            fwd_dist: vec![u32::MAX; n],
            bwd_dist: vec![u32::MAX; n],
            fwd_version: vec![0; n],
            bwd_version: vec![0; n],
            current_version: 0,
        }
    }

    /// Compute shortest distance from source to target
    pub fn distance(&mut self, source: u32, target: u32) -> u32 {
        self.current_version += 1;
        let v = self.current_version;

        // Initialize
        self.fwd_dist[source as usize] = 0;
        self.fwd_version[source as usize] = v;
        self.bwd_dist[target as usize] = 0;
        self.bwd_version[target as usize] = v;

        let mut fwd_heap: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
        let mut bwd_heap: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();

        fwd_heap.push(Reverse((0, source)));
        bwd_heap.push(Reverse((0, target)));

        let mut best = u32::MAX;
        let mut fwd_settled = 0u32;
        let mut bwd_settled = 0u32;

        // Alternating bidirectional search
        while !fwd_heap.is_empty() || !bwd_heap.is_empty() {
            // Forward step
            if let Some(Reverse((d, u))) = fwd_heap.pop() {
                if self.fwd_version[u as usize] != v || d > self.fwd_dist[u as usize] {
                    continue;
                }

                // Check meeting point
                if self.bwd_version[u as usize] == v {
                    let total = d.saturating_add(self.bwd_dist[u as usize]);
                    best = best.min(total);
                }

                // Stall check: if we've settled enough and current dist > best/2, stop
                fwd_settled += 1;
                if d > best / 2 {
                    // Can safely stop forward search
                }

                // Relax UP edges
                let start = self.topo.up_offsets[u as usize] as usize;
                let end = self.topo.up_offsets[u as usize + 1] as usize;

                for i in start..end {
                    let v_node = self.topo.up_heads[i];
                    let w = self.topo.up_weights[i];
                    let new_dist = d.saturating_add(w);

                    let old_dist = if self.fwd_version[v_node as usize] == v {
                        self.fwd_dist[v_node as usize]
                    } else {
                        u32::MAX
                    };

                    if new_dist < old_dist {
                        self.fwd_dist[v_node as usize] = new_dist;
                        self.fwd_version[v_node as usize] = v;
                        fwd_heap.push(Reverse((new_dist, v_node)));
                    }
                }
            }

            // Backward step
            if let Some(Reverse((d, u))) = bwd_heap.pop() {
                if self.bwd_version[u as usize] != v || d > self.bwd_dist[u as usize] {
                    continue;
                }

                // Check meeting point
                if self.fwd_version[u as usize] == v {
                    let total = d.saturating_add(self.fwd_dist[u as usize]);
                    best = best.min(total);
                }

                bwd_settled += 1;
                if d > best / 2 {
                    // Can safely stop backward search
                }

                // Relax UP edges (backward search goes UP in CH)
                let start = self.topo.up_offsets[u as usize] as usize;
                let end = self.topo.up_offsets[u as usize + 1] as usize;

                for i in start..end {
                    let v_node = self.topo.up_heads[i];
                    let w = self.topo.up_weights[i];
                    let new_dist = d.saturating_add(w);

                    let old_dist = if self.bwd_version[v_node as usize] == v {
                        self.bwd_dist[v_node as usize]
                    } else {
                        u32::MAX
                    };

                    if new_dist < old_dist {
                        self.bwd_dist[v_node as usize] = new_dist;
                        self.bwd_version[v_node as usize] = v;
                        bwd_heap.push(Reverse((new_dist, v_node)));
                    }
                }
            }

            // Termination: both heaps exhausted or best found
            if fwd_heap.is_empty() && bwd_heap.is_empty() {
                break;
            }
        }

        best
    }
}

/// Bucket-based many-to-many on NBG CH
pub struct NbgBucketM2M<'a> {
    topo: &'a NbgChTopo,
}

impl<'a> NbgBucketM2M<'a> {
    pub fn new(topo: &'a NbgChTopo) -> Self {
        Self { topo }
    }

    /// Compute distance matrix
    pub fn compute(&self, sources: &[u32], targets: &[u32]) -> (Vec<u32>, NbgM2MStats) {
        let n_nodes = self.topo.n_nodes as usize;
        let n_sources = sources.len();
        let n_targets = targets.len();

        let start = std::time::Instant::now();

        // Phase 1: Forward search from sources, fill buckets
        let mut buckets: Vec<Vec<(u16, u32)>> = vec![Vec::new(); n_nodes]; // (source_idx, dist)

        let mut fwd_visited = 0u64;
        for (src_idx, &source) in sources.iter().enumerate() {
            let visited = self.forward_search(source, src_idx as u16, &mut buckets);
            fwd_visited += visited as u64;
        }

        let fwd_time = start.elapsed().as_millis();

        // Phase 2: Backward search from targets, join with buckets
        let mut matrix = vec![u32::MAX; n_sources * n_targets];
        let mut bwd_visited = 0u64;
        let mut joins = 0u64;

        for (tgt_idx, &target) in targets.iter().enumerate() {
            let (visited, j) = self.backward_search(target, tgt_idx, &buckets, &mut matrix, n_targets);
            bwd_visited += visited as u64;
            joins += j;
        }

        let total_time = start.elapsed().as_millis();

        let stats = NbgM2MStats {
            n_sources,
            n_targets,
            fwd_visited,
            bwd_visited,
            joins,
            fwd_time_ms: fwd_time as u64,
            total_time_ms: total_time as u64,
        };

        (matrix, stats)
    }

    fn forward_search(&self, source: u32, src_idx: u16, buckets: &mut [Vec<(u16, u32)>]) -> usize {
        let n_nodes = self.topo.n_nodes as usize;
        let mut dist = vec![u32::MAX; n_nodes];
        let mut heap: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();

        dist[source as usize] = 0;
        heap.push(Reverse((0, source)));
        let mut visited = 0;

        while let Some(Reverse((d, u))) = heap.pop() {
            if d > dist[u as usize] {
                continue;
            }

            // Add to bucket
            buckets[u as usize].push((src_idx, d));
            visited += 1;

            // Relax UP edges
            let start = self.topo.up_offsets[u as usize] as usize;
            let end = self.topo.up_offsets[u as usize + 1] as usize;

            for i in start..end {
                let v = self.topo.up_heads[i];
                let w = self.topo.up_weights[i];
                let new_dist = d.saturating_add(w);

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    heap.push(Reverse((new_dist, v)));
                }
            }
        }

        visited
    }

    fn backward_search(
        &self,
        target: u32,
        tgt_idx: usize,
        buckets: &[Vec<(u16, u32)>],
        matrix: &mut [u32],
        n_targets: usize,
    ) -> (usize, u64) {
        let n_nodes = self.topo.n_nodes as usize;
        let mut dist = vec![u32::MAX; n_nodes];
        let mut heap: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();

        dist[target as usize] = 0;
        heap.push(Reverse((0, target)));
        let mut visited = 0;
        let mut joins = 0u64;

        while let Some(Reverse((d, u))) = heap.pop() {
            if d > dist[u as usize] {
                continue;
            }

            visited += 1;

            // Join with bucket
            for &(src_idx, src_dist) in &buckets[u as usize] {
                let total = src_dist.saturating_add(d);
                let idx = src_idx as usize * n_targets + tgt_idx;
                if total < matrix[idx] {
                    matrix[idx] = total;
                }
                joins += 1;
            }

            // Relax UP edges (backward goes UP in CH)
            let start = self.topo.up_offsets[u as usize] as usize;
            let end = self.topo.up_offsets[u as usize + 1] as usize;

            for i in start..end {
                let v = self.topo.up_heads[i];
                let w = self.topo.up_weights[i];
                let new_dist = d.saturating_add(w);

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    heap.push(Reverse((new_dist, v)));
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
    pub fwd_time_ms: u64,
    pub total_time_ms: u64,
}
