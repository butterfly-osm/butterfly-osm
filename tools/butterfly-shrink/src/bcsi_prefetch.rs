//! BCSI chunk-to-block preplanning and prefetching
//! 
//! Maps chunks of node IDs to their BCSI blocks, enabling:
//! - Sequential I/O instead of random
//! - Block deduplication 
//! - Prefetching and parallel decompression

use crate::bcsi::{BcsiReader, TopIndexEntry, BcsiPayload};
use butterfly_common::Result;
use std::collections::BTreeMap;
use std::sync::Arc;

/// Planner for optimizing BCSI lookups
pub struct BcsiPrefetchPlanner {
    top_index: Arc<Vec<TopIndexEntry>>,
}

impl BcsiPrefetchPlanner {
    pub fn new(top_index: Arc<Vec<TopIndexEntry>>) -> Self {
        Self { top_index }
    }
    
    /// Plan optimal block access for a chunk of node IDs
    pub fn plan_chunk(&self, node_ids: &[i64]) -> ChunkPlan {
        // Map each node to its block
        let mut block_to_nodes: BTreeMap<usize, Vec<i64>> = BTreeMap::new();
        
        for &node_id in node_ids {
            let block_idx = self.find_block_index(node_id);
            block_to_nodes.entry(block_idx)
                .or_insert_with(Vec::new)
                .push(node_id);
        }
        
        // Sort nodes within each block for cache-friendly access
        for nodes in block_to_nodes.values_mut() {
            nodes.sort_unstable();
        }
        
        // Build access sequence
        let access_sequence: Vec<BlockAccess> = block_to_nodes
            .into_iter()
            .map(|(block_idx, nodes)| BlockAccess {
                block_idx,
                node_ids: nodes,
            })
            .collect();
        
        ChunkPlan {
            total_nodes: node_ids.len(),
            unique_blocks: access_sequence.len(),
            access_sequence,
        }
    }
    
    /// Find which block contains a node ID
    fn find_block_index(&self, node_id: i64) -> usize {
        // Binary search the top index
        match self.top_index.binary_search_by_key(&node_id, |e| e.first_key) {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    0  // Before first block
                } else {
                    idx - 1  // In previous block
                }
            }
        }
    }
}

/// Plan for accessing a chunk of nodes
#[derive(Debug)]
pub struct ChunkPlan {
    pub total_nodes: usize,
    pub unique_blocks: usize,
    pub access_sequence: Vec<BlockAccess>,
}

/// Single block access descriptor
#[derive(Debug)]
pub struct BlockAccess {
    pub block_idx: usize,
    pub node_ids: Vec<i64>,  // Sorted node IDs in this block
}

impl ChunkPlan {
    /// Calculate efficiency metrics
    pub fn efficiency_ratio(&self) -> f64 {
        if self.total_nodes == 0 {
            1.0
        } else {
            self.unique_blocks as f64 / self.total_nodes as f64
        }
    }
    
    /// Estimate I/O operations saved
    pub fn io_ops_saved(&self) -> usize {
        if self.total_nodes > self.unique_blocks {
            self.total_nodes - self.unique_blocks
        } else {
            0
        }
    }
}

/// Optimized BCSI reader with prefetching
pub struct BcsiPrefetchReader {
    reader: BcsiReader,
    planner: BcsiPrefetchPlanner,
    stats: LookupStats,
}

#[derive(Default, Debug)]
pub struct LookupStats {
    pub total_lookups: usize,
    pub unique_blocks_accessed: usize,
    pub cache_hits: usize,
    pub io_operations: usize,
}

impl BcsiPrefetchReader {
    pub fn new(reader: BcsiReader, top_index: Arc<Vec<TopIndexEntry>>) -> Self {
        Self {
            reader,
            planner: BcsiPrefetchPlanner::new(top_index),
            stats: LookupStats::default(),
        }
    }
    
    /// Perform optimized lookups for a chunk
    pub fn lookup_chunk(&mut self, node_ids: &[i64]) -> Result<Vec<(i64, BcsiPayload)>> {
        // Plan the access sequence
        let plan = self.planner.plan_chunk(node_ids);
        
        // Update stats
        self.stats.total_lookups += plan.total_nodes;
        self.stats.unique_blocks_accessed += plan.unique_blocks;
        
        // Prefetch blocks (could be async in future)
        self.prefetch_blocks(&plan)?;
        
        // Execute lookups in optimal order
        let mut results = Vec::with_capacity(node_ids.len());
        
        for block_access in &plan.access_sequence {
            // Process all nodes in this block together
            for &node_id in &block_access.node_ids {
                if let Some(payload) = self.reader.lookup(node_id)? {
                    results.push((node_id, payload));
                }
            }
            self.stats.io_operations += 1;
        }
        
        // Sort results back to original order if needed
        results.sort_unstable_by_key(|(id, _)| *id);
        
        Ok(results)
    }
    
    /// Prefetch blocks for upcoming access
    fn prefetch_blocks(&mut self, _plan: &ChunkPlan) -> Result<()> {
        // In current implementation, just ensure blocks are cached
        // Future: could use io_uring or separate thread for true async
        
        // For now, this is a no-op as the reader will cache on demand
        // But the structure is here for future optimization
        
        Ok(())
    }
    
    pub fn stats(&self) -> &LookupStats {
        &self.stats
    }
    
    pub fn efficiency_ratio(&self) -> f64 {
        if self.stats.total_lookups == 0 {
            1.0
        } else {
            self.stats.io_operations as f64 / self.stats.total_lookups as f64
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_chunk_planning() {
        // Create mock top index
        let top_index = vec![
            TopIndexEntry { first_key: 0, file_offset: 0 },
            TopIndexEntry { first_key: 1000, file_offset: 1024 },
            TopIndexEntry { first_key: 2000, file_offset: 2048 },
            TopIndexEntry { first_key: 3000, file_offset: 3072 },
        ];
        
        let planner = BcsiPrefetchPlanner::new(Arc::new(top_index));
        
        // Test with nodes that span multiple blocks
        let node_ids = vec![500, 1500, 2500, 501, 1501, 2501];
        let plan = planner.plan_chunk(&node_ids);
        
        assert_eq!(plan.total_nodes, 6);
        assert_eq!(plan.unique_blocks, 3);  // Blocks 0, 1, 2
        assert_eq!(plan.io_ops_saved(), 3);  // 6 lookups -> 3 I/Os
        
        // Check nodes are grouped by block
        assert_eq!(plan.access_sequence[0].node_ids, vec![500, 501]);
        assert_eq!(plan.access_sequence[1].node_ids, vec![1500, 1501]);
        assert_eq!(plan.access_sequence[2].node_ids, vec![2500, 2501]);
    }
}