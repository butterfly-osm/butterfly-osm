//! Way batching system for efficient MultiGet operations

use crate::cache::LruCache;
use crate::config::Config;
use crate::db::NodeIndex;
use crate::telemetry::{Telemetry, Timer};
use crate::writer::PbfWriter;
use anyhow::{Context, Result};
use std::collections::{HashMap, HashSet};
use std::sync::Arc;

/// Configuration for batching behavior
#[derive(Debug, Clone)]
pub struct BatchConfig {
    pub max_ways: usize,           // Max ways per batch (e.g., 50k)
    pub max_unique_nodes: usize,   // Max unique node IDs per batch (e.g., 1.5M)
    pub cache_capacity: usize,     // LRU cache size (e.g., 2-4M entries)
    pub enable_tile_bucketing: bool,  // Enable spatial tile bucketing
    pub tile_grid_degrees: f64,    // Tile size in degrees (e.g., 0.1)
    pub max_tiles_in_memory: usize,   // Max tiles to keep in memory
}

impl BatchConfig {
    pub fn from_config(config: &Config) -> Self {
        // Calculate max unique nodes based on memory limit
        // Assume each unique node costs ~24 bytes (8 bytes ID + 16 bytes overhead)
        let memory_bytes = config.batch_memory_limit_mb * 1024 * 1024;
        let max_unique_nodes = (memory_bytes / 24).min(1_500_000); // Cap at 1.5M for safety
        
        Self {
            max_ways: config.batch_ways,
            max_unique_nodes,
            cache_capacity: config.lru_cache_size,
            enable_tile_bucketing: config.enable_tile_bucketing,
            tile_grid_degrees: config.tile_grid_degrees,
            max_tiles_in_memory: config.max_tiles_in_memory,
        }
    }
    
    /// Auto-tune batch size based on MultiGet performance
    pub fn autotune(&mut self, multiget_latency_ms: f64, hit_ratio: f64) {
        // If MultiGet is fast and cache hit ratio is high, increase batch size
        // If MultiGet is slow or cache hit ratio is low, decrease batch size
        
        let latency_factor = if multiget_latency_ms < 10.0 {
            1.2 // Fast MultiGet, increase batch size by 20%
        } else if multiget_latency_ms > 50.0 {
            0.8 // Slow MultiGet, decrease batch size by 20%
        } else {
            1.0 // Normal latency, no change
        };
        
        let hit_ratio_factor = if hit_ratio > 0.8 {
            1.1 // High cache hit ratio, can increase batch size
        } else if hit_ratio < 0.5 {
            0.9 // Low cache hit ratio, decrease batch size
        } else {
            1.0 // Normal hit ratio, no change
        };
        
        let adjustment_factor = latency_factor * hit_ratio_factor;
        
        // Apply adjustment with bounds
        self.max_ways = ((self.max_ways as f64 * adjustment_factor) as usize)
            .max(1000)      // Minimum 1k ways per batch
            .min(100_000);  // Maximum 100k ways per batch
            
        log::debug!(
            "Autotuned batch size to {} (latency: {:.1}ms, hit_ratio: {:.2})",
            self.max_ways, multiget_latency_ms, hit_ratio
        );
    }
}

impl Default for BatchConfig {
    fn default() -> Self {
        Self::from_config(&Config::default())
    }
}

/// A batch of ways to be processed together
pub struct WayBatch {
    ways: Vec<WayData>,
    unique_node_ids: HashSet<i64>,
    total_size_estimate: usize,
}

#[derive(Debug, Clone)]
pub struct WayData {
    pub id: i64,
    pub refs: Vec<i64>,
    pub tags: HashMap<String, String>,
    pub tile_key: Option<(i32, i32)>,  // Tile coordinates for bucketing
}

impl From<&osmpbf::Way<'_>> for WayData {
    fn from(way: &osmpbf::Way<'_>) -> Self {
        Self {
            id: way.id(),
            refs: way.refs().collect(),
            tags: way.tags().map(|(k, v)| (k.to_string(), v.to_string())).collect(),
            tile_key: None,  // Will be computed when first node is resolved
        }
    }
}

/// Tile-based queue for spatial bucketing
struct TileQueue {
    tile_key: (i32, i32),
    ways: Vec<WayData>,
    unique_nodes: HashSet<i64>,
    size_estimate: usize,
}

impl TileQueue {
    fn new(tile_key: (i32, i32)) -> Self {
        Self {
            tile_key,
            ways: Vec::new(),
            unique_nodes: HashSet::new(),
            size_estimate: 0,
        }
    }
    
    fn add_way(&mut self, mut way: WayData) {
        way.tile_key = Some(self.tile_key);
        for &node_id in &way.refs {
            self.unique_nodes.insert(node_id);
        }
        self.size_estimate += 100 + way.refs.len() * 8;
        self.ways.push(way);
    }
    
    fn should_flush(&self, max_ways: usize, max_nodes: usize) -> bool {
        self.ways.len() >= max_ways || 
        self.unique_nodes.len() >= max_nodes ||
        self.size_estimate >= 32 * 1024 * 1024  // 32MB per tile
    }
}

/// Batching processor for ways with integrated caching and MultiGet
pub struct WayBatcher {
    config: BatchConfig,
    current_batch: WayBatch,
    tile_queues: HashMap<(i32, i32), TileQueue>,  // Tile-based queues
    cache: LruCache,
    telemetry: Telemetry,
    total_ways_written: u64,
}

impl WayBatcher {
    pub fn new(config: BatchConfig, telemetry: Telemetry) -> Self {
        Self {
            cache: LruCache::new(config.cache_capacity),
            current_batch: WayBatch::new(),
            tile_queues: HashMap::new(),
            config,
            telemetry,
            total_ways_written: 0,
        }
    }
    
    /// Compute tile key from coordinates
    #[allow(dead_code)]
    fn compute_tile_key(&self, lat_nano: i64, lon_nano: i64) -> (i32, i32) {
        let lat = lat_nano as f64 / 1e9;
        let lon = lon_nano as f64 / 1e9;
        let tile_x = (lon / self.config.tile_grid_degrees).floor() as i32;
        let tile_y = (lat / self.config.tile_grid_degrees).floor() as i32;
        (tile_x, tile_y)
    }

    /// Add a way to the current batch, processing if batch is full
    pub fn add_way(
        &mut self, 
        way: &osmpbf::Way,
        highway_tags: &HashSet<String>,
        node_index: &Arc<NodeIndex>,
        pbf_writer: &mut PbfWriter,
    ) -> Result<()> {
        // Convert way to owned data
        let mut way_data = WayData::from(way);
        
        // Filter by highway tags first
        let mut highway_tag = None;
        let mut _oneway_tag = None;
        
        for (key, value) in &way_data.tags {
            match key.as_str() {
                "highway" => {
                    if highway_tags.contains(value) {
                        highway_tag = Some(value.clone());
                    } else {
                        log::trace!("Skipping way {} - highway tag '{}' not in preset", way_data.id, value);
                        return Ok(());
                    }
                }
                "oneway" => {
                    _oneway_tag = Some(value.clone());
                }
                _ => {}
            }
        }
        
        // Skip if no matching highway tag
        if highway_tag.is_none() {
            log::trace!("Skipping way {} - no highway tag found", way_data.id);
            return Ok(());
        }
        
        if self.config.enable_tile_bucketing && !way_data.refs.is_empty() {
            // Tile bucketing enabled: compute tile from first node
            let first_node_id = way_data.refs[0];
            
            // Try cache first
            if let Some(rep_id) = self.cache.get(first_node_id) {
                // Get the representative node's position (assuming it's stored)
                if let Ok(Some(rep_id)) = node_index.get(rep_id) {
                    // For now, use a simple hash-based tile assignment
                    // In production, we'd look up the actual coordinates
                    let tile_key = ((rep_id / 1000000) as i32, (rep_id / 10000000) as i32);
                    way_data.tile_key = Some(tile_key);
                    
                    // Add to tile queue
                    let tile_queue = self.tile_queues.entry(tile_key)
                        .or_insert_with(|| TileQueue::new(tile_key));
                    tile_queue.add_way(way_data);
                    
                    // Check if this tile should be flushed
                    if tile_queue.should_flush(self.config.max_ways / 4, self.config.max_unique_nodes / 4) {
                        self.flush_tile(tile_key, node_index, pbf_writer)?;
                    }
                    
                    // Check if we have too many tiles in memory
                    if self.tile_queues.len() > self.config.max_tiles_in_memory {
                        self.flush_largest_tile(node_index, pbf_writer)?;
                    }
                    
                    return Ok(());
                }
            }
        }
        
        // Fall back to non-tiled batching
        for &node_id in &way_data.refs {
            self.current_batch.unique_node_ids.insert(node_id);
        }
        
        self.current_batch.ways.push(way_data);
        self.current_batch.total_size_estimate += 100;
        
        // Check if batch is full
        if self.should_flush_batch() {
            let batch_ways = self.flush_batch(node_index, pbf_writer)?;
            self.total_ways_written += batch_ways;
        }
        
        Ok(())
    }

    /// Check if the current batch should be flushed
    fn should_flush_batch(&self) -> bool {
        self.current_batch.ways.len() >= self.config.max_ways ||
        self.current_batch.unique_node_ids.len() >= self.config.max_unique_nodes ||
        self.current_batch.total_size_estimate >= 64 * 1024 * 1024 // 64MB
    }

    /// Flush the current batch by processing all ways
    pub fn flush_batch(
        &mut self,
        node_index: &Arc<NodeIndex>,
        pbf_writer: &mut PbfWriter,
    ) -> Result<u64> {
        if self.current_batch.ways.is_empty() {
            return Ok(0);
        }

        let timer = Timer::new();
        
        // Collect unique node IDs, sorted for better locality
        let mut unique_nodes: Vec<i64> = self.current_batch.unique_node_ids.iter().cloned().collect();
        unique_nodes.sort_unstable();
        
        // Check cache first for all nodes
        let mut cache_hits = 0;
        let mut cache_misses = Vec::new();
        let mut cached_mappings = HashMap::new();
        
        for &node_id in &unique_nodes {
            if let Some(rep_id) = self.cache.get(node_id) {
                cached_mappings.insert(node_id, rep_id);
                cache_hits += 1;
                self.telemetry.record_cache_hit();
            } else {
                cache_misses.push(node_id);
                self.telemetry.record_cache_miss();
            }
        }
        
        // MultiGet for cache misses
        let mut db_mappings = HashMap::new();
        if !cache_misses.is_empty() {
            let multiget_timer = Timer::new();
            db_mappings = node_index.multi_get(&cache_misses)
                .context("Failed to perform MultiGet on node index")?;
            
            // Record telemetry
            self.telemetry.record_multiget(cache_misses.len(), multiget_timer.elapsed());
            
            // Fill cache with fetched mappings
            for (&orig_id, &rep_id) in &db_mappings {
                self.cache.put(orig_id, rep_id);
            }
        }
        
        // Combine cached and fetched mappings
        let mut all_mappings = cached_mappings;
        all_mappings.extend(db_mappings);
        
        // Process each way in the batch
        let mut ways_written = 0;
        
        for way_data in &self.current_batch.ways {
            // Remap node references
            let mut remapped_refs = Vec::new();
            let mut missing_nodes = Vec::new();
            
            for &node_ref in &way_data.refs {
                if let Some(&representative_id) = all_mappings.get(&node_ref) {
                    remapped_refs.push(representative_id);
                } else {
                    missing_nodes.push(node_ref);
                }
            }
            
            // Handle missing nodes (should be rare)
            if !missing_nodes.is_empty() {
                log::warn!("Way {} has {} missing node references: {:?}", 
                    way_data.id, missing_nodes.len(), &missing_nodes[..missing_nodes.len().min(5)]);
                continue; // Skip this way
            }
            
            // Remove consecutive duplicates
            remapped_refs.dedup();
            
            // Skip ways with < 2 nodes
            if remapped_refs.len() < 2 {
                log::debug!("Skipping way {} with {} nodes after deduplication", 
                    way_data.id, remapped_refs.len());
                continue;
            }
            
            // Write way to PBF with minimal tags
            let mut output_tags = HashMap::new();
            if let Some(highway) = way_data.tags.get("highway") {
                output_tags.insert("highway".to_string(), highway.clone());
            }
            if let Some(oneway) = way_data.tags.get("oneway") {
                output_tags.insert("oneway".to_string(), oneway.clone());
            }
            
            pbf_writer.write_way(way_data.id, &remapped_refs, &output_tags)
                .context("Failed to write way to PBF")?;
            
            ways_written += 1;
            
            log::debug!("Processed way {} with {} -> {} nodes", 
                way_data.id, way_data.refs.len(), remapped_refs.len());
        }
        
        // Record batch telemetry
        self.telemetry.record_batch(self.current_batch.ways.len(), self.current_batch.unique_node_ids.len());
        
        // Autotune batch size based on performance
        let avg_multiget_latency = self.telemetry.avg_multiget_latency_ms();
        let cache_hit_ratio = self.telemetry.cache_hit_ratio();
        self.config.autotune(avg_multiget_latency, cache_hit_ratio);
        
        log::debug!("Processed batch: {} ways ({} written), {} unique nodes, {} cache hits, {} cache misses, took {}ms",
            self.current_batch.ways.len(),
            ways_written,
            self.current_batch.unique_node_ids.len(),
            cache_hits,
            cache_misses.len(),
            timer.elapsed().as_millis()
        );
        
        // Clear the batch
        self.current_batch = WayBatch::new();
        
        Ok(ways_written as u64)
    }

    /// Flush a specific tile
    fn flush_tile(
        &mut self,
        tile_key: (i32, i32),
        node_index: &Arc<NodeIndex>,
        pbf_writer: &mut PbfWriter,
    ) -> Result<()> {
        if let Some(tile_queue) = self.tile_queues.remove(&tile_key) {
            log::debug!("Flushing tile {:?} with {} ways", tile_key, tile_queue.ways.len());
            
            // Convert tile queue to batch and flush
            let mut batch = WayBatch {
                ways: tile_queue.ways,
                unique_node_ids: tile_queue.unique_nodes,
                total_size_estimate: tile_queue.size_estimate,
            };
            
            // Swap with current batch temporarily
            std::mem::swap(&mut self.current_batch, &mut batch);
            let ways_written = self.flush_batch(node_index, pbf_writer)?;
            std::mem::swap(&mut self.current_batch, &mut batch);
            
            self.total_ways_written += ways_written;
        }
        Ok(())
    }
    
    /// Flush the largest tile to free memory
    fn flush_largest_tile(
        &mut self,
        node_index: &Arc<NodeIndex>,
        pbf_writer: &mut PbfWriter,
    ) -> Result<()> {
        // Find the largest tile
        let largest_tile = self.tile_queues
            .iter()
            .max_by_key(|(_, queue)| queue.ways.len())
            .map(|(key, _)| *key);
            
        if let Some(tile_key) = largest_tile {
            self.flush_tile(tile_key, node_index, pbf_writer)?;
        }
        Ok(())
    }
    
    /// Flush all remaining tiles
    pub fn flush_all_tiles(
        &mut self,
        node_index: &Arc<NodeIndex>,
        pbf_writer: &mut PbfWriter,
    ) -> Result<()> {
        let tile_keys: Vec<_> = self.tile_queues.keys().cloned().collect();
        for tile_key in tile_keys {
            self.flush_tile(tile_key, node_index, pbf_writer)?;
        }
        Ok(())
    }

    /// Get total ways written across all batches
    pub fn total_ways_written(&self) -> u64 {
        self.total_ways_written
    }

    /// Get cache statistics
    pub fn cache_stats(&self) -> (usize, usize, f64) {
        let len = self.cache.len();
        let capacity = self.cache.capacity();
        let hit_ratio = self.cache.hit_ratio();
        (len, capacity, hit_ratio)
    }
}

impl WayBatch {
    fn new() -> Self {
        Self {
            ways: Vec::new(),
            unique_node_ids: HashSet::new(),
            total_size_estimate: 0,
        }
    }
}