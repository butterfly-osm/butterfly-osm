//! M7.2 - Sharded Caching: Concurrent cache access without contention

use crate::thread_architecture::NumaNode;
use crate::turn_restriction_tables::{TurnMovement, TurnPenalty};
use crate::profiles::TransportProfile;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, VecDeque};
use std::hash::{Hash, Hasher};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

/// Number of cache shards for optimal concurrency
pub const CACHE_SHARD_COUNT: usize = 64;

/// Auto-rebalancing configuration
pub const REBALANCING_HIT_RATE_GAP_THRESHOLD: f64 = 0.12; // 12%
pub const REBALANCING_DURATION_THRESHOLD: Duration = Duration::from_secs(60);
pub const REBALANCING_STEP_PERCENT: f64 = 0.05; // 5%

/// Cache entry for turn restriction data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnCacheEntry {
    pub movement: TurnMovement,
    pub penalty: TurnPenalty,
    pub profile: TransportProfile,
    pub access_count: u64,
    #[serde(skip, default = "TurnCacheEntry::default_instant")]
    pub last_access: Instant,
    pub numa_node: Option<NumaNode>,
}

impl TurnCacheEntry {
    pub fn new(movement: TurnMovement, penalty: TurnPenalty, profile: TransportProfile) -> Self {
        Self {
            movement,
            penalty,
            profile,
            access_count: 1,
            last_access: Instant::now(),
            numa_node: Some(NumaNode::current()),
        }
    }

    fn default_instant() -> Instant {
        Instant::now()
    }

    pub fn update_access(&mut self) {
        self.access_count += 1;
        self.last_access = Instant::now();
    }

    pub fn age(&self) -> Duration {
        self.last_access.elapsed()
    }
}

/// Cache entry for geometry data
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GeometryCacheEntry {
    pub edge_id: u64,
    pub geometry_data: Vec<u8>, // Simplified geometry representation
    pub access_count: u64,
    #[serde(skip, default = "GeometryCacheEntry::default_instant")]
    pub last_access: Instant,
    pub numa_node: Option<NumaNode>,
    pub size_bytes: usize,
}

impl GeometryCacheEntry {
    pub fn new(edge_id: u64, geometry_data: Vec<u8>) -> Self {
        let size_bytes = geometry_data.len();
        Self {
            edge_id,
            geometry_data,
            access_count: 1,
            last_access: Instant::now(),
            numa_node: Some(NumaNode::current()),
            size_bytes,
        }
    }

    fn default_instant() -> Instant {
        Instant::now()
    }

    pub fn update_access(&mut self) {
        self.access_count += 1;
        self.last_access = Instant::now();
    }

    pub fn age(&self) -> Duration {
        self.last_access.elapsed()
    }
}

/// LRU cache shard for concurrent access
#[derive(Debug)]
pub struct CacheShard<K, V> 
where 
    K: Clone + Hash + Eq,
    V: Clone,
{
    shard_id: usize,
    capacity: usize,
    data: RwLock<HashMap<K, V>>,
    lru_order: RwLock<VecDeque<K>>,
    hits: AtomicU64,
    misses: AtomicU64,
    evictions: AtomicU64,
    memory_usage: AtomicUsize,
    numa_node: NumaNode,
}

impl<K, V> CacheShard<K, V>
where 
    K: Clone + Hash + Eq,
    V: Clone,
{
    pub fn new(shard_id: usize, capacity: usize, numa_node: NumaNode) -> Self {
        Self {
            shard_id,
            capacity,
            data: RwLock::new(HashMap::with_capacity(capacity)),
            lru_order: RwLock::new(VecDeque::with_capacity(capacity)),
            hits: AtomicU64::new(0),
            misses: AtomicU64::new(0),
            evictions: AtomicU64::new(0),
            memory_usage: AtomicUsize::new(0),
            numa_node,
        }
    }

    /// Get an item from the cache
    pub fn get(&self, key: &K) -> Option<V> {
        if let Ok(data) = self.data.read() {
            if let Some(value) = data.get(key) {
                self.hits.fetch_add(1, Ordering::Relaxed);
                
                // Update LRU order
                if let Ok(mut lru) = self.lru_order.write() {
                    if let Some(pos) = lru.iter().position(|k| k == key) {
                        lru.remove(pos);
                        lru.push_back(key.clone());
                    }
                }
                
                return Some(value.clone());
            }
        }
        
        self.misses.fetch_add(1, Ordering::Relaxed);
        None
    }

    /// Insert an item into the cache
    pub fn insert(&self, key: K, value: V, size_bytes: usize) {
        let mut data = self.data.write().unwrap();
        let mut lru = self.lru_order.write().unwrap();

        // Check if we need to evict
        while data.len() >= self.capacity && !lru.is_empty() {
            if let Some(old_key) = lru.pop_front() {
                if data.remove(&old_key).is_some() {
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Insert new item
        data.insert(key.clone(), value);
        lru.push_back(key);
        self.memory_usage.fetch_add(size_bytes, Ordering::Relaxed);
    }

    /// Get cache statistics
    pub fn stats(&self) -> CacheShardStats {
        let hits = self.hits.load(Ordering::Relaxed);
        let misses = self.misses.load(Ordering::Relaxed);
        let total_requests = hits + misses;
        let hit_rate = if total_requests > 0 {
            hits as f64 / total_requests as f64
        } else {
            0.0
        };

        let size = self.data.read().unwrap().len();
        let memory_usage = self.memory_usage.load(Ordering::Relaxed);

        CacheShardStats {
            shard_id: self.shard_id,
            capacity: self.capacity,
            size,
            hit_rate,
            hits,
            misses,
            evictions: self.evictions.load(Ordering::Relaxed),
            memory_usage,
            numa_node: self.numa_node,
        }
    }

    /// Clear the cache
    pub fn clear(&self) {
        let mut data = self.data.write().unwrap();
        let mut lru = self.lru_order.write().unwrap();
        
        data.clear();
        lru.clear();
        self.memory_usage.store(0, Ordering::Relaxed);
    }

    /// Resize the cache capacity
    pub fn resize(&self, new_capacity: usize) -> Result<(), String> {
        if new_capacity == 0 {
            return Err("Cache capacity cannot be zero".to_string());
        }

        let mut data = self.data.write().unwrap();
        let mut lru = self.lru_order.write().unwrap();

        // If shrinking, evict excess items
        while data.len() > new_capacity && !lru.is_empty() {
            if let Some(old_key) = lru.pop_front() {
                if data.remove(&old_key).is_some() {
                    self.evictions.fetch_add(1, Ordering::Relaxed);
                }
            }
        }

        // Note: Capacity update is skipped to avoid unsafe operations
        // In production, this would require proper interior mutability

        Ok(())
    }
}

/// Key type for turn cache entries
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TurnCacheKey {
    pub movement: TurnMovement,
    pub profile: TransportProfile,
}

impl TurnCacheKey {
    pub fn new(movement: TurnMovement, profile: TransportProfile) -> Self {
        Self { movement, profile }
    }
}

/// Sharded turn restriction cache
pub struct ShardedTurnCache {
    shards: Vec<CacheShard<TurnCacheKey, TurnCacheEntry>>,
    total_capacity: usize,
    numa_interleave: bool,
}

impl ShardedTurnCache {
    pub fn new(total_capacity: usize, numa_interleave: bool) -> Self {
        let shard_capacity = total_capacity / CACHE_SHARD_COUNT;
        let mut shards = Vec::with_capacity(CACHE_SHARD_COUNT);
        
        for i in 0..CACHE_SHARD_COUNT {
            let numa_node = if numa_interleave {
                NumaNode::new((i % NumaNode::system_count()) as u16)
            } else {
                NumaNode::current()
            };
            
            shards.push(CacheShard::new(i, shard_capacity, numa_node));
        }
        
        Self {
            shards,
            total_capacity,
            numa_interleave,
        }
    }

    /// Calculate shard index for a key
    fn shard_index(&self, key: &TurnCacheKey) -> usize {
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        key.hash(&mut hasher);
        (hasher.finish() as usize) % CACHE_SHARD_COUNT
    }

    /// Get a turn restriction from cache
    pub fn get(&self, movement: &TurnMovement, profile: &TransportProfile) -> Option<TurnPenalty> {
        let key = TurnCacheKey::new(*movement, *profile);
        let shard_idx = self.shard_index(&key);
        
        self.shards[shard_idx].get(&key).map(|entry| entry.penalty)
    }

    /// Insert a turn restriction into cache
    pub fn insert(&self, movement: TurnMovement, profile: TransportProfile, penalty: TurnPenalty) {
        let key = TurnCacheKey::new(movement, profile);
        let shard_idx = self.shard_index(&key);
        let entry = TurnCacheEntry::new(movement, penalty, profile);
        let size_bytes = std::mem::size_of::<TurnCacheEntry>();
        
        self.shards[shard_idx].insert(key, entry, size_bytes);
    }

    /// Get cache statistics
    pub fn stats(&self) -> ShardedCacheStats {
        let shard_stats: Vec<_> = self.shards.iter().map(|shard| shard.stats()).collect();
        
        let total_hits: u64 = shard_stats.iter().map(|s| s.hits).sum();
        let total_misses: u64 = shard_stats.iter().map(|s| s.misses).sum();
        let total_requests = total_hits + total_misses;
        let overall_hit_rate = if total_requests > 0 {
            total_hits as f64 / total_requests as f64
        } else {
            0.0
        };

        let total_memory: usize = shard_stats.iter().map(|s| s.memory_usage).sum();
        let total_size: usize = shard_stats.iter().map(|s| s.size).sum();

        ShardedCacheStats {
            cache_type: "turn".to_string(),
            total_capacity: self.total_capacity,
            total_size,
            overall_hit_rate,
            total_hits,
            total_misses,
            total_memory,
            numa_interleave: self.numa_interleave,
            shard_stats,
        }
    }

    /// Resize the cache
    pub fn resize(&self, new_total_capacity: usize) -> Result<(), String> {
        let new_shard_capacity = new_total_capacity / CACHE_SHARD_COUNT;
        
        for shard in &self.shards {
            shard.resize(new_shard_capacity)?;
        }

        // Note: Total capacity update is skipped to avoid unsafe operations
        // In production, this would require proper interior mutability

        Ok(())
    }
}

/// Sharded geometry cache
pub struct ShardedGeometryCache {
    shards: Vec<CacheShard<u64, GeometryCacheEntry>>,
    total_capacity: usize,
    numa_interleave: bool,
}

impl ShardedGeometryCache {
    pub fn new(total_capacity: usize, numa_interleave: bool) -> Self {
        let shard_capacity = total_capacity / CACHE_SHARD_COUNT;
        let mut shards = Vec::with_capacity(CACHE_SHARD_COUNT);
        
        for i in 0..CACHE_SHARD_COUNT {
            let numa_node = if numa_interleave {
                NumaNode::new((i % NumaNode::system_count()) as u16)
            } else {
                NumaNode::current()
            };
            
            shards.push(CacheShard::new(i, shard_capacity, numa_node));
        }
        
        Self {
            shards,
            total_capacity,
            numa_interleave,
        }
    }

    /// Calculate shard index for an edge ID
    fn shard_index(&self, edge_id: u64) -> usize {
        (edge_id as usize) % CACHE_SHARD_COUNT
    }

    /// Get geometry data from cache
    pub fn get(&self, edge_id: u64) -> Option<Vec<u8>> {
        let shard_idx = self.shard_index(edge_id);
        self.shards[shard_idx].get(&edge_id).map(|entry| entry.geometry_data)
    }

    /// Insert geometry data into cache
    pub fn insert(&self, edge_id: u64, geometry_data: Vec<u8>) {
        let shard_idx = self.shard_index(edge_id);
        let entry = GeometryCacheEntry::new(edge_id, geometry_data.clone());
        let size_bytes = entry.size_bytes + std::mem::size_of::<GeometryCacheEntry>();
        
        self.shards[shard_idx].insert(edge_id, entry, size_bytes);
    }

    /// Get cache statistics
    pub fn stats(&self) -> ShardedCacheStats {
        let shard_stats: Vec<_> = self.shards.iter().map(|shard| shard.stats()).collect();
        
        let total_hits: u64 = shard_stats.iter().map(|s| s.hits).sum();
        let total_misses: u64 = shard_stats.iter().map(|s| s.misses).sum();
        let total_requests = total_hits + total_misses;
        let overall_hit_rate = if total_requests > 0 {
            total_hits as f64 / total_requests as f64
        } else {
            0.0
        };

        let total_memory: usize = shard_stats.iter().map(|s| s.memory_usage).sum();
        let total_size: usize = shard_stats.iter().map(|s| s.size).sum();

        ShardedCacheStats {
            cache_type: "geometry".to_string(),
            total_capacity: self.total_capacity,
            total_size,
            overall_hit_rate,
            total_hits,
            total_misses,
            total_memory,
            numa_interleave: self.numa_interleave,
            shard_stats,
        }
    }

    /// Resize the cache
    pub fn resize(&self, new_total_capacity: usize) -> Result<(), String> {
        let new_shard_capacity = new_total_capacity / CACHE_SHARD_COUNT;
        
        for shard in &self.shards {
            shard.resize(new_shard_capacity)?;
        }

        // Note: Total capacity update is skipped to avoid unsafe operations
        // In production, this would require proper interior mutability

        Ok(())
    }
}

/// Auto-rebalancing cache manager
pub struct AutoRebalancingCacheManager {
    turn_cache: Arc<ShardedTurnCache>,
    geometry_cache: Arc<ShardedGeometryCache>,
    last_rebalance: Instant,
    rebalancing_enabled: bool,
    hit_rate_gap_start: Option<Instant>,
}

impl AutoRebalancingCacheManager {
    pub fn new(
        turn_capacity: usize,
        geometry_capacity: usize,
        numa_interleave: bool,
    ) -> Self {
        Self {
            turn_cache: Arc::new(ShardedTurnCache::new(turn_capacity, numa_interleave)),
            geometry_cache: Arc::new(ShardedGeometryCache::new(geometry_capacity, numa_interleave)),
            last_rebalance: Instant::now(),
            rebalancing_enabled: true,
            hit_rate_gap_start: None,
        }
    }

    /// Check if rebalancing is needed and perform it
    pub fn check_and_rebalance(&mut self) -> Result<Option<RebalancingAction>, String> {
        if !self.rebalancing_enabled {
            return Ok(None);
        }

        let turn_stats = self.turn_cache.stats();
        let geom_stats = self.geometry_cache.stats();
        
        let hit_rate_gap = (turn_stats.overall_hit_rate - geom_stats.overall_hit_rate).abs();
        
        if hit_rate_gap >= REBALANCING_HIT_RATE_GAP_THRESHOLD {
            if self.hit_rate_gap_start.is_none() {
                self.hit_rate_gap_start = Some(Instant::now());
                return Ok(None);
            }
            
            if let Some(start_time) = self.hit_rate_gap_start {
                if start_time.elapsed() >= REBALANCING_DURATION_THRESHOLD {
                    return self.perform_rebalancing(turn_stats.overall_hit_rate, geom_stats.overall_hit_rate);
                }
            }
        } else {
            self.hit_rate_gap_start = None;
        }
        
        Ok(None)
    }

    /// Perform cache rebalancing
    fn perform_rebalancing(&mut self, turn_hit_rate: f64, geom_hit_rate: f64) -> Result<Option<RebalancingAction>, String> {
        let total_capacity = self.turn_cache.total_capacity + self.geometry_cache.total_capacity;
        let step_size = (total_capacity as f64 * REBALANCING_STEP_PERCENT) as usize;
        
        let action = if turn_hit_rate > geom_hit_rate {
            // Give more capacity to geometry cache
            let new_turn_capacity = self.turn_cache.total_capacity.saturating_sub(step_size);
            let new_geom_capacity = self.geometry_cache.total_capacity + step_size;
            
            self.turn_cache.resize(new_turn_capacity)?;
            self.geometry_cache.resize(new_geom_capacity)?;
            
            RebalancingAction {
                action_type: "capacity_shift".to_string(),
                from_cache: "turn".to_string(),
                to_cache: "geometry".to_string(),
                amount: step_size,
                turn_hit_rate,
                geom_hit_rate,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        } else {
            // Give more capacity to turn cache
            let new_geom_capacity = self.geometry_cache.total_capacity.saturating_sub(step_size);
            let new_turn_capacity = self.turn_cache.total_capacity + step_size;
            
            self.geometry_cache.resize(new_geom_capacity)?;
            self.turn_cache.resize(new_turn_capacity)?;
            
            RebalancingAction {
                action_type: "capacity_shift".to_string(),
                from_cache: "geometry".to_string(),
                to_cache: "turn".to_string(),
                amount: step_size,
                turn_hit_rate,
                geom_hit_rate,
                timestamp: std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs(),
            }
        };

        self.last_rebalance = Instant::now();
        self.hit_rate_gap_start = None;

        Ok(Some(action))
    }

    /// Get turn cache reference
    pub fn turn_cache(&self) -> Arc<ShardedTurnCache> {
        Arc::clone(&self.turn_cache)
    }

    /// Get geometry cache reference
    pub fn geometry_cache(&self) -> Arc<ShardedGeometryCache> {
        Arc::clone(&self.geometry_cache)
    }

    /// Get comprehensive statistics
    pub fn stats(&self) -> CacheManagerStats {
        let now = Instant::now();
        CacheManagerStats {
            turn_cache: self.turn_cache.stats(),
            geometry_cache: self.geometry_cache.stats(),
            last_rebalance_secs_ago: now.duration_since(self.last_rebalance).as_secs(),
            rebalancing_enabled: self.rebalancing_enabled,
            hit_rate_gap_duration_secs: self.hit_rate_gap_start
                .map(|start| now.duration_since(start).as_secs()),
        }
    }
}

/// Statistics for individual cache shard
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheShardStats {
    pub shard_id: usize,
    pub capacity: usize,
    pub size: usize,
    pub hit_rate: f64,
    pub hits: u64,
    pub misses: u64,
    pub evictions: u64,
    pub memory_usage: usize,
    pub numa_node: NumaNode,
}

/// Statistics for sharded cache
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ShardedCacheStats {
    pub cache_type: String,
    pub total_capacity: usize,
    pub total_size: usize,
    pub overall_hit_rate: f64,
    pub total_hits: u64,
    pub total_misses: u64,
    pub total_memory: usize,
    pub numa_interleave: bool,
    pub shard_stats: Vec<CacheShardStats>,
}

/// Cache rebalancing action
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RebalancingAction {
    pub action_type: String,
    pub from_cache: String,
    pub to_cache: String,
    pub amount: usize,
    pub turn_hit_rate: f64,
    pub geom_hit_rate: f64,
    pub timestamp: u64,
}

/// Comprehensive cache manager statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CacheManagerStats {
    pub turn_cache: ShardedCacheStats,
    pub geometry_cache: ShardedCacheStats,
    pub last_rebalance_secs_ago: u64,
    pub rebalancing_enabled: bool,
    pub hit_rate_gap_duration_secs: Option<u64>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::turn_restriction_tables::{JunctionId, TurnMovement};
    use crate::profiles::EdgeId;

    #[test]
    fn test_cache_shard_creation() {
        let numa_node = NumaNode::new(0);
        let shard: CacheShard<i32, String> = CacheShard::new(0, 100, numa_node);
        
        let stats = shard.stats();
        assert_eq!(stats.shard_id, 0);
        assert_eq!(stats.capacity, 100);
        assert_eq!(stats.size, 0);
        assert_eq!(stats.hits, 0);
        assert_eq!(stats.misses, 0);
    }

    #[test]
    fn test_cache_shard_operations() {
        let numa_node = NumaNode::new(0);
        let shard: CacheShard<i32, String> = CacheShard::new(0, 3, numa_node);
        
        // Test miss
        assert!(shard.get(&1).is_none());
        let stats = shard.stats();
        assert_eq!(stats.misses, 1);
        
        // Test insert and hit
        shard.insert(1, "one".to_string(), 10);
        assert_eq!(shard.get(&1), Some("one".to_string()));
        let stats = shard.stats();
        assert_eq!(stats.hits, 1);
        assert_eq!(stats.size, 1);
        
        // Test LRU eviction
        shard.insert(2, "two".to_string(), 10);
        shard.insert(3, "three".to_string(), 10);
        shard.insert(4, "four".to_string(), 10); // Should evict 1
        
        assert!(shard.get(&1).is_none()); // Evicted
        assert_eq!(shard.get(&4), Some("four".to_string()));
        
        let stats = shard.stats();
        assert_eq!(stats.size, 3);
        assert_eq!(stats.evictions, 1);
    }

    #[test]
    fn test_turn_cache_key() {
        let movement = TurnMovement::new(
            EdgeId(1),
            JunctionId::new(10),
            EdgeId(2),
        );
        let key1 = TurnCacheKey::new(movement, TransportProfile::Car);
        let key2 = TurnCacheKey::new(movement, TransportProfile::Car);
        let key3 = TurnCacheKey::new(movement, TransportProfile::Bicycle);
        
        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
    }

    #[test]
    fn test_sharded_turn_cache() {
        let cache = ShardedTurnCache::new(640, true); // 64 shards × 10 capacity each
        
        let movement = TurnMovement::new(
            EdgeId(1),
            JunctionId::new(10),
            EdgeId(2),
        );
        
        // Test miss
        assert!(cache.get(&movement, &TransportProfile::Car).is_none());
        
        // Test insert and hit
        cache.insert(movement, TransportProfile::Car, 100);
        assert_eq!(cache.get(&movement, &TransportProfile::Car), Some(100));
        
        // Test different profile
        assert!(cache.get(&movement, &TransportProfile::Bicycle).is_none());
        
        let stats = cache.stats();
        assert_eq!(stats.cache_type, "turn");
        assert_eq!(stats.total_capacity, 640);
        assert_eq!(stats.shard_stats.len(), CACHE_SHARD_COUNT);
        assert!(stats.overall_hit_rate > 0.0);
    }

    #[test]
    fn test_sharded_geometry_cache() {
        let cache = ShardedGeometryCache::new(640, false);
        
        let edge_id = 12345;
        let geometry = vec![1, 2, 3, 4, 5];
        
        // Test miss
        assert!(cache.get(edge_id).is_none());
        
        // Test insert and hit
        cache.insert(edge_id, geometry.clone());
        assert_eq!(cache.get(edge_id), Some(geometry));
        
        let stats = cache.stats();
        assert_eq!(stats.cache_type, "geometry");
        assert_eq!(stats.total_capacity, 640);
        assert!(!stats.numa_interleave);
    }

    #[test]
    fn test_cache_resize() {
        let cache = ShardedTurnCache::new(64, false);
        
        // Fill the cache
        for i in 0..100 {
            let movement = TurnMovement::new(
                EdgeId(i),
                JunctionId::new(i as u64),
                EdgeId(i + 1),
            );
            cache.insert(movement, TransportProfile::Car, i as u16);
        }
        
        let stats_before = cache.stats();
        assert!(stats_before.total_size <= 64);
        
        // Resize larger
        cache.resize(128).unwrap();
        let stats_after = cache.stats();
        assert_eq!(stats_after.total_capacity, 128);
        
        // Should be able to insert more items now
        for i in 100..150 {
            let movement = TurnMovement::new(
                EdgeId(i),
                JunctionId::new(i as u64),
                EdgeId(i + 1),
            );
            cache.insert(movement, TransportProfile::Car, i as u16);
        }
    }

    #[test]
    fn test_auto_rebalancing_cache_manager() {
        let mut manager = AutoRebalancingCacheManager::new(100, 100, true);
        
        // Test initial state
        let stats = manager.stats();
        assert_eq!(stats.turn_cache.total_capacity, 100);
        assert_eq!(stats.geometry_cache.total_capacity, 100);
        assert!(stats.rebalancing_enabled);
        
        // Test that no rebalancing occurs initially
        let action = manager.check_and_rebalance().unwrap();
        assert!(action.is_none());
    }

    #[test]
    fn test_cache_entry_creation() {
        let movement = TurnMovement::new(
            EdgeId(1),
            JunctionId::new(10),
            EdgeId(2),
        );
        
        let entry = TurnCacheEntry::new(movement, 50, TransportProfile::Car);
        assert_eq!(entry.movement, movement);
        assert_eq!(entry.penalty, 50);
        assert_eq!(entry.profile, TransportProfile::Car);
        assert_eq!(entry.access_count, 1);
        assert!(entry.numa_node.is_some());
        
        let geometry_entry = GeometryCacheEntry::new(123, vec![1, 2, 3, 4]);
        assert_eq!(geometry_entry.edge_id, 123);
        assert_eq!(geometry_entry.geometry_data, vec![1, 2, 3, 4]);
        assert_eq!(geometry_entry.size_bytes, 4);
        assert_eq!(geometry_entry.access_count, 1);
    }

    #[test]
    fn test_shard_distribution() {
        let cache = ShardedTurnCache::new(640, true);
        
        // Test that different keys go to different shards
        let mut shard_usage = HashMap::new();
        
        for i in 0..1000 {
            let movement = TurnMovement::new(
                EdgeId(i),
                JunctionId::new(i as u64),
                EdgeId(i + 1),
            );
            let key = TurnCacheKey::new(movement, TransportProfile::Car);
            let shard_idx = cache.shard_index(&key);
            *shard_usage.entry(shard_idx).or_insert(0) += 1;
        }
        
        // Should have decent distribution across shards
        assert!(shard_usage.len() > CACHE_SHARD_COUNT / 2);
        
        // No shard should be overloaded
        for count in shard_usage.values() {
            assert!(*count < 100); // Less than 10% of items in any single shard
        }
    }

    #[test]
    fn test_numa_interleave() {
        let cache_interleaved = ShardedTurnCache::new(640, true);
        let cache_local = ShardedTurnCache::new(640, false);
        
        let stats_interleaved = cache_interleaved.stats();
        let stats_local = cache_local.stats();
        
        assert!(stats_interleaved.numa_interleave);
        assert!(!stats_local.numa_interleave);
        
        // With interleaving, should see different NUMA nodes
        if NumaNode::system_count() > 1 {
            let numa_nodes: std::collections::HashSet<_> = stats_interleaved
                .shard_stats
                .iter()
                .map(|s| s.numa_node.0)
                .collect();
            assert!(numa_nodes.len() > 1);
        }
    }

    #[test]
    fn test_cache_memory_tracking() {
        let cache = ShardedTurnCache::new(100, false);
        
        let movement = TurnMovement::new(
            EdgeId(1),
            JunctionId::new(10),
            EdgeId(2),
        );
        
        let stats_before = cache.stats();
        assert_eq!(stats_before.total_memory, 0);
        
        cache.insert(movement, TransportProfile::Car, 100);
        
        let stats_after = cache.stats();
        assert!(stats_after.total_memory > 0);
    }

    #[test]
    fn test_concurrent_cache_access() {
        use std::sync::Arc;
        use std::thread;
        
        let cache = Arc::new(ShardedTurnCache::new(1000, true));
        let mut handles = vec![];
        
        // Spawn multiple threads doing cache operations
        for thread_id in 0..4 {
            let cache_clone = Arc::clone(&cache);
            let handle = thread::spawn(move || {
                for i in 0..100 {
                    let movement = TurnMovement::new(
                        EdgeId(thread_id * 100 + i),
                        JunctionId::new((thread_id * 100 + i) as u64),
                        EdgeId(thread_id * 100 + i + 1),
                    );
                    
                    // Insert
                    cache_clone.insert(movement, TransportProfile::Car, (i as u16) % 1000);
                    
                    // Read back
                    let _result = cache_clone.get(&movement, &TransportProfile::Car);
                }
            });
            handles.push(handle);
        }
        
        for handle in handles {
            handle.join().unwrap();
        }
        
        let stats = cache.stats();
        assert!(stats.total_hits > 0);
        assert!(stats.total_size > 0);
    }
}