//! Simple LRU cache for node ID mappings

use std::collections::HashMap;

/// A simple LRU cache implementation for i64 -> i64 mappings
/// This is a simplified implementation using HashMap for now
pub struct LruCache {
    map: HashMap<i64, i64>,
    capacity: usize,
    access_order: Vec<i64>,
}

impl LruCache {
    /// Create a new LRU cache with the specified capacity
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::with_capacity(capacity),
            capacity,
            access_order: Vec::with_capacity(capacity),
        }
    }

    /// Get a value from the cache, moving it to the front
    pub fn get(&mut self, key: i64) -> Option<i64> {
        if let Some(&value) = self.map.get(&key) {
            // Move to front (remove and push to end)
            if let Some(pos) = self.access_order.iter().position(|&k| k == key) {
                self.access_order.remove(pos);
            }
            self.access_order.push(key);
            Some(value)
        } else {
            None
        }
    }

    /// Insert a key-value pair, evicting the least recently used if necessary
    pub fn put(&mut self, key: i64, value: i64) {
        if self.map.contains_key(&key) {
            // Update existing
            self.map.insert(key, value);
            // Move to front
            if let Some(pos) = self.access_order.iter().position(|&k| k == key) {
                self.access_order.remove(pos);
            }
            self.access_order.push(key);
        } else {
            // Insert new
            if self.map.len() >= self.capacity {
                // Evict LRU
                if let Some(lru_key) = self.access_order.first().copied() {
                    self.map.remove(&lru_key);
                    self.access_order.remove(0);
                }
            }
            
            self.map.insert(key, value);
            self.access_order.push(key);
        }
    }

    /// Get the current number of items in the cache
    pub fn len(&self) -> usize {
        self.map.len()
    }

    /// Check if the cache is empty
    pub fn is_empty(&self) -> bool {
        self.map.is_empty()
    }

    /// Get the capacity of the cache
    pub fn capacity(&self) -> usize {
        self.capacity
    }

    /// Get cache hit ratio as a percentage
    pub fn hit_ratio(&self) -> f64 {
        // This would need separate tracking in a real implementation
        0.0
    }
}

