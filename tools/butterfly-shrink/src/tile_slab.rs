//! Single-slab tile queue implementation for optimal memory usage
//! 
//! Instead of per-way Vec allocations, uses a single contiguous slab
//! for all node references, with compact indexing.

use std::mem;

/// Single-slab tile queue with exact byte accounting
pub struct TileQueueSlab {
    // Single slab for ALL node refs (no per-way Vecs!)
    refs_slab: Vec<i64>,
    
    // Compact way indexing: (way_id, start_offset, ref_count, has_highway)
    way_headers: Vec<WayHeader>,
    
    // Tag storage (only highway-related tags)
    // Using u16 indices into a tag dictionary would save more but adds complexity
    way_tags: Vec<CompactTags>,
    
    // Unique nodes for deduplication (built on flush)
    unique_nodes: Vec<i64>,
    unique_nodes_sorted: bool,
    
    // Exact byte accounting
    allocated_bytes: usize,
}

#[repr(C)]
#[derive(Clone, Copy)]
struct WayHeader {
    way_id: i64,        // 8 bytes
    refs_start: u32,    // 4 bytes - offset into refs_slab
    refs_count: u16,    // 2 bytes - number of refs (max 65535)
    flags: u8,          // 1 byte - bit 0: has_highway
    _padding: u8,       // 1 byte - alignment
}

/// Compact tag storage
struct CompactTags {
    highway: Option<Box<str>>,  // Only store if present
    oneway: Option<Box<str>>,
    access: Option<Box<str>>,
}

impl TileQueueSlab {
    /// Create new tile queue with pre-allocation hints
    pub fn new() -> Self {
        Self::with_capacity(1000, 10000)  // 1K ways, 10K refs typical
    }
    
    pub fn with_capacity(ways: usize, refs: usize) -> Self {
        Self {
            refs_slab: Vec::with_capacity(refs),
            way_headers: Vec::with_capacity(ways),
            way_tags: Vec::with_capacity(ways),
            unique_nodes: Vec::new(),  // Built on demand
            unique_nodes_sorted: false,
            allocated_bytes: 0,
        }
    }
    
    /// Add a way to the queue
    pub fn add_way(&mut self, way_id: i64, refs: &[i64], tags: &[(&str, &str)]) -> bool {
        // Check if we can fit this way
        let refs_start = self.refs_slab.len();
        if refs_start > u32::MAX as usize || refs.len() > u16::MAX as usize {
            return false;  // Would overflow compact indices
        }
        
        // Extract relevant tags
        let mut highway = None;
        let mut oneway = None;
        let mut access = None;
        let mut has_highway = false;
        
        for (k, v) in tags {
            match *k {
                "highway" => {
                    highway = Some((*v).into());
                    has_highway = true;
                }
                "oneway" => oneway = Some((*v).into()),
                "access" => access = Some((*v).into()),
                _ => {}  // Ignore other tags
            }
        }
        
        // Add to slab
        self.refs_slab.extend_from_slice(refs);
        
        // Create compact header
        let header = WayHeader {
            way_id,
            refs_start: refs_start as u32,
            refs_count: refs.len() as u16,
            flags: if has_highway { 1 } else { 0 },
            _padding: 0,
        };
        self.way_headers.push(header);
        
        // Store tags
        self.way_tags.push(CompactTags {
            highway,
            oneway,
            access,
        });
        
        // Mark unique nodes as needing rebuild
        self.unique_nodes_sorted = false;
        
        // Update accounting
        self.update_allocated_bytes();
        
        true
    }
    
    /// Build unique nodes list (called before lookups)
    pub fn build_unique_nodes(&mut self) {
        if self.unique_nodes_sorted {
            return;
        }
        
        // Clear and reserve
        self.unique_nodes.clear();
        self.unique_nodes.reserve(self.refs_slab.len() / 3);  // Estimate 3x reuse
        
        // Copy all refs
        self.unique_nodes.extend_from_slice(&self.refs_slab);
        
        // Sort and dedup in-place
        self.unique_nodes.sort_unstable();
        self.unique_nodes.dedup();
        
        // Shrink if overallocated
        if self.unique_nodes.capacity() > self.unique_nodes.len() * 2 {
            self.unique_nodes.shrink_to_fit();
        }
        
        self.unique_nodes_sorted = true;
        self.update_allocated_bytes();
    }
    
    /// Get unique nodes (must call build_unique_nodes first!)
    pub fn unique_nodes(&self) -> &[i64] {
        debug_assert!(self.unique_nodes_sorted, "Must build unique nodes first");
        &self.unique_nodes
    }
    
    /// Iterate over ways
    pub fn ways(&self) -> impl Iterator<Item = (i64, &[i64], bool)> + '_ {
        self.way_headers.iter().map(move |header| {
            let refs = &self.refs_slab[header.refs_start as usize..
                                       (header.refs_start + header.refs_count as u32) as usize];
            (header.way_id, refs, header.flags & 1 != 0)
        })
    }
    
    /// Get tags for a way by index
    pub fn get_tags(&self, idx: usize) -> Vec<(&str, &str)> {
        let tags = &self.way_tags[idx];
        let mut result = Vec::with_capacity(3);
        
        if let Some(ref highway) = tags.highway {
            result.push(("highway", highway.as_ref()));
        }
        if let Some(ref oneway) = tags.oneway {
            result.push(("oneway", oneway.as_ref()));
        }
        if let Some(ref access) = tags.access {
            result.push(("access", access.as_ref()));
        }
        
        result
    }
    
    /// Clear the queue and release memory properly
    pub fn clear(&mut self) {
        // Don't just clear - actually release capacity
        self.refs_slab = Vec::with_capacity(10000);
        self.way_headers = Vec::with_capacity(1000);
        self.way_tags = Vec::with_capacity(1000);
        self.unique_nodes = Vec::new();
        self.unique_nodes_sorted = false;
        self.allocated_bytes = self.calculate_bytes();
    }
    
    /// Calculate exact memory usage
    fn calculate_bytes(&self) -> usize {
        // Slab capacity
        self.refs_slab.capacity() * mem::size_of::<i64>() +
        // Headers capacity  
        self.way_headers.capacity() * mem::size_of::<WayHeader>() +
        // Tags - estimate based on actual strings
        self.way_tags.iter().map(|t| {
            24 + // Vec overhead
            t.highway.as_ref().map(|s| s.len() + 24).unwrap_or(0) +
            t.oneway.as_ref().map(|s| s.len() + 24).unwrap_or(0) +
            t.access.as_ref().map(|s| s.len() + 24).unwrap_or(0)
        }).sum::<usize>() +
        // Unique nodes capacity
        self.unique_nodes.capacity() * mem::size_of::<i64>() +
        // Struct overhead
        mem::size_of::<Self>()
    }
    
    fn update_allocated_bytes(&mut self) {
        self.allocated_bytes = self.calculate_bytes();
    }
    
    pub fn allocated_bytes(&self) -> usize {
        self.allocated_bytes
    }
    
    pub fn way_count(&self) -> usize {
        self.way_headers.len()
    }
    
    pub fn should_flush(&self) -> bool {
        const MAX_WAYS: usize = 50_000;
        const MAX_NODES: usize = 400_000;
        const MAX_BYTES: usize = 80_000_000;
        
        self.way_headers.len() >= MAX_WAYS ||
        self.refs_slab.len() >= MAX_NODES ||
        self.allocated_bytes >= MAX_BYTES
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_slab_memory_efficiency() {
        let mut queue = TileQueueSlab::new();
        
        // Add 1000 ways with 100 refs each
        for i in 0..1000 {
            let refs: Vec<i64> = (0..100).map(|j| i * 1000 + j).collect();
            let tags = vec![("highway", "residential")];
            queue.add_way(i, &refs, &tags);
        }
        
        // Build unique nodes
        queue.build_unique_nodes();
        
        // Check memory usage
        let bytes = queue.allocated_bytes();
        let ways = queue.way_count();
        
        // Should be much less than naive approach
        // Naive: 1000 ways * (24 bytes Vec + 100 * 8) = 824KB just for refs
        // Our approach: 100K * 8 + 1000 * 16 = 816KB total
        assert!(bytes < 1_000_000, "Memory usage too high: {}", bytes);
        assert_eq!(ways, 1000);
    }
}