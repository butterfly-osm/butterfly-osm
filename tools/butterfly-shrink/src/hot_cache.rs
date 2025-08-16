//! Hot-node micro-cache for frequently accessed nodes
//! 
//! Direct-mapped cache for O(1) lookups of hot intersection nodes

use std::sync::atomic::{AtomicU64, Ordering};

/// Compact cache entry (16 bytes)
#[repr(C)]
#[derive(Clone, Copy, Default)]
struct CacheEntry {
    orig_id: i64,  // Original node ID (key)
    rep_id: i64,   // Representative ID (value)
}

/// Direct-mapped hot node cache
/// 
/// Uses low bits of node ID for indexing (no hash needed)
/// Fixed size, O(1) operations, cache-friendly
pub struct HotNodeCache {
    entries: Vec<CacheEntry>,
    mask: usize,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
}

impl HotNodeCache {
    /// Create cache with power-of-2 entries
    pub fn new(size_bits: u8) -> Self {
        let size = 1 << size_bits;
        Self {
            entries: vec![CacheEntry::default(); size],
            mask: size - 1,
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
        }
    }
    
    /// Create cache with 128K entries (~2MB)
    pub fn new_128k() -> Self {
        Self::new(17)  // 2^17 = 131,072 entries
    }
    
    /// Create cache with 64K entries (~1MB)
    pub fn new_64k() -> Self {
        Self::new(16)  // 2^16 = 65,536 entries
    }
    
    /// Lookup a node in the cache
    #[inline]
    pub fn get(&self, orig_id: i64) -> Option<i64> {
        let idx = (orig_id as usize) & self.mask;
        let entry = unsafe { 
            // Safe because idx is always < entries.len() due to mask
            self.entries.get_unchecked(idx)
        };
        
        if entry.orig_id == orig_id {
            self.hits.fetch_add(1, Ordering::Relaxed);
            Some(entry.rep_id)
        } else {
            self.misses.fetch_add(1, Ordering::Relaxed);
            None
        }
    }
    
    /// Insert a mapping into the cache
    #[inline]
    pub fn insert(&mut self, orig_id: i64, rep_id: i64) {
        let idx = (orig_id as usize) & self.mask;
        let entry = unsafe {
            // Safe because idx is always < entries.len() due to mask
            self.entries.get_unchecked_mut(idx)
        };
        
        if entry.orig_id != 0 && entry.orig_id != orig_id {
            self.evictions.fetch_add(1, Ordering::Relaxed);
        }
        
        *entry = CacheEntry { orig_id, rep_id };
    }
    
    /// Clear the cache
    pub fn clear(&mut self) {
        self.entries.fill(CacheEntry::default());
        self.hits.store(0, Ordering::Relaxed);
        self.misses.store(0, Ordering::Relaxed);
        self.evictions.store(0, Ordering::Relaxed);
    }
    
    /// Get cache statistics
    pub fn stats(&self) -> CacheStats {
        CacheStats {
            hits: self.hits.load(Ordering::Relaxed),
            misses: self.misses.load(Ordering::Relaxed),
            evictions: self.evictions.load(Ordering::Relaxed),
            size: self.entries.len(),
            bytes: self.entries.len() * std::mem::size_of::<CacheEntry>(),
        }
    }
    
    /// Calculate hit rate
    pub fn hit_rate(&self) -> f64 {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total = hits + misses;
        
        if total == 0 {
            0.0
        } else {
            hits as f64 / total as f64
        }
    }
}

#[derive(Debug)]
pub struct CacheStats {
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub size: usize,
    pub bytes: usize,
}

impl CacheStats {
    pub fn hit_rate(&self) -> f64 {
        let total = self.hits + self.misses;
        if total == 0 {
            0.0
        } else {
            self.hits as f64 / total as f64
        }
    }
}

/// Thread-safe wrapper for shared hot cache
pub struct SharedHotCache {
    cache: parking_lot::RwLock<HotNodeCache>,
}

impl SharedHotCache {
    pub fn new(size_bits: u8) -> Self {
        Self {
            cache: parking_lot::RwLock::new(HotNodeCache::new(size_bits)),
        }
    }
    
    pub fn new_128k() -> Self {
        Self::new(17)
    }
    
    /// Try to get from cache (read lock)
    pub fn get(&self, orig_id: i64) -> Option<i64> {
        self.cache.read().get(orig_id)
    }
    
    /// Insert into cache (write lock)
    pub fn insert(&self, orig_id: i64, rep_id: i64) {
        self.cache.write().insert(orig_id, rep_id);
    }
    
    /// Batch insert (single write lock)
    pub fn insert_batch(&self, mappings: &[(i64, i64)]) {
        let mut cache = self.cache.write();
        for &(orig_id, rep_id) in mappings {
            cache.insert(orig_id, rep_id);
        }
    }
    
    pub fn stats(&self) -> CacheStats {
        self.cache.read().stats()
    }
    
    pub fn clear(&self) {
        self.cache.write().clear();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_hot_cache_basic() {
        let mut cache = HotNodeCache::new(10);  // 1024 entries
        
        // Insert some mappings
        cache.insert(1234, 5678);
        cache.insert(2345, 6789);
        
        // Test hits
        assert_eq!(cache.get(1234), Some(5678));
        assert_eq!(cache.get(2345), Some(6789));
        
        // Test miss
        assert_eq!(cache.get(9999), None);
        
        // Check stats
        let stats = cache.stats();
        assert_eq!(stats.hits, 2);
        assert_eq!(stats.misses, 1);
    }
    
    #[test]
    fn test_cache_collision() {
        let mut cache = HotNodeCache::new(4);  // 16 entries
        
        // These will collide (same low 4 bits)
        cache.insert(0x10, 100);
        cache.insert(0x20, 200);  // Same slot as 0x10
        
        // 0x10 should be evicted
        assert_eq!(cache.get(0x10), None);
        assert_eq!(cache.get(0x20), Some(200));
        
        let stats = cache.stats();
        assert_eq!(stats.evictions, 1);
    }
    
    #[test]
    fn test_memory_size() {
        let cache = HotNodeCache::new_128k();
        let stats = cache.stats();
        
        // 128K entries * 16 bytes = 2MB
        assert_eq!(stats.size, 131072);
        assert_eq!(stats.bytes, 2097152);
    }
}