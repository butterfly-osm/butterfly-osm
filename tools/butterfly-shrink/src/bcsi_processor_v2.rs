//! BCSI processor v2 with single-slab, prefetch planning, and hot cache
//! 
//! Optimized for speed while maintaining <4GB memory usage

use crate::bcsi::{BcsiReader, TopIndexEntry, compute_tile_id};
use crate::bcsi_prefetch::BcsiPrefetchReader;
use crate::hot_cache::SharedHotCache;
use crate::tile_slab::TileQueueSlab;
use crate::writer::PbfWriter;
use crate::config::Config;
use crate::progress::Progress;
use butterfly_common::{Error, Result};
use osmpbf::{Element, ElementReader};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Instant;

// Memory budget constants (4GB total) - OPTIMIZED FOR SPEED
const BCSI_CACHE_SIZE: usize = 2_000_000_000;     // 2.0 GB for BCSI block cache
const TILE_STAGING_SIZE: usize = 1_500_000_000;   // 1.5 GB for tile queues
const HOT_CACHE_SIZE_BITS: u8 = 17;               // 128K entries = ~2MB

// Processing constants - OPTIMIZED
const MAX_ACTIVE_TILES: usize = 16;         

/// Memory tracker with atomic operations
struct MemoryTracker {
    tile_bytes: AtomicUsize,
    peak_bytes: AtomicUsize,
}

impl MemoryTracker {
    fn new() -> Self {
        Self {
            tile_bytes: AtomicUsize::new(0),
            peak_bytes: AtomicUsize::new(0),
        }
    }
    
    fn try_allocate(&self, bytes: usize) -> bool {
        let mut current = self.tile_bytes.load(Ordering::Acquire);
        loop {
            if current + bytes > TILE_STAGING_SIZE {
                return false;
            }
            match self.tile_bytes.compare_exchange_weak(
                current,
                current + bytes,
                Ordering::Release,
                Ordering::Acquire,
            ) {
                Ok(new) => {
                    self.update_peak(new);
                    return true;
                }
                Err(c) => current = c,
            }
        }
    }
    
    fn release(&self, bytes: usize) {
        self.tile_bytes.fetch_sub(bytes, Ordering::Release);
    }
    
    fn update_peak(&self, new_bytes: usize) {
        let mut peak = self.peak_bytes.load(Ordering::Relaxed);
        while new_bytes > peak {
            match self.peak_bytes.compare_exchange_weak(
                peak,
                new_bytes,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(p) => peak = p,
            }
        }
    }
    
    fn peak_mb(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed) / 1_000_000
    }
}

/// BCSI Processor V2 - optimized for speed
pub struct BcsiProcessorV2 {
    config: Config,
    temp_dir: PathBuf,
    memory_tracker: Arc<MemoryTracker>,
    hot_cache: Arc<SharedHotCache>,
}

impl BcsiProcessorV2 {
    pub fn new(config: Config) -> Result<Self> {
        let temp_dir = std::env::temp_dir().join(format!("butterfly-bcsi-v2-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).map_err(Error::IoError)?;
        
        Ok(Self {
            config,
            temp_dir,
            memory_tracker: Arc::new(MemoryTracker::new()),
            hot_cache: Arc::new(SharedHotCache::new(HOT_CACHE_SIZE_BITS)),
        })
    }
    
    /// Process PBF with optimized pipeline
    pub fn process(&mut self, input_path: &Path, output_path: &Path) -> Result<ProcessStats> {
        let start = Instant::now();
        log::info!("Starting BCSI processor V2 (optimized for speed)");
        
        // Phase 1: Process nodes (same as before)
        let (bcsi_path, top_index, node_stats) = self.process_nodes(input_path, output_path)?;
        
        // Phase 2: Process ways with optimizations
        let way_stats = self.process_ways_v2(input_path, output_path, &bcsi_path, top_index)?;
        
        // Cleanup
        self.cleanup()?;
        
        let elapsed = start.elapsed();
        log::info!("Processing complete in {:.2}s", elapsed.as_secs_f64());
        log::info!("Peak tile memory: {} MB", self.memory_tracker.peak_mb());
        log::info!("Hot cache hit rate: {:.1}%", self.hot_cache.stats().hit_rate() * 100.0);
        
        Ok(ProcessStats {
            total_nodes: node_stats.total_nodes,
            rep_nodes: node_stats.rep_nodes,
            total_ways: way_stats.total_ways,
            written_ways: way_stats.written_ways,
            elapsed_secs: elapsed.as_secs_f64(),
        })
    }
    
    /// Phase 1: Process nodes (reuse existing implementation)
    fn process_nodes(&self, input_path: &Path, output_path: &Path) 
        -> Result<(PathBuf, Vec<TopIndexEntry>, NodeStats)> {
        // Delegate to existing efficient implementation
        let processor = crate::bcsi_processor::BcsiProcessor::new(self.config.clone())?;
        let (bcsi_path, top_index, stats) = processor.process_nodes(input_path, output_path)?;
        Ok((bcsi_path, top_index, NodeStats {
            total_nodes: stats.total_nodes,
            rep_nodes: stats.rep_nodes,
        }))
    }
    
    /// Phase 2: Process ways with all optimizations
    fn process_ways_v2(
        &self,
        input_path: &Path,
        output_path: &Path,
        bcsi_path: &Path,
        top_index: Vec<TopIndexEntry>,
    ) -> Result<WayStats> {
        log::info!("Phase 2: Processing ways with V2 optimizations");
        let start = Instant::now();
        let progress = Progress::new("Phase 2: Ways");
        
        // Create optimized BCSI reader with prefetch planner
        let bcsi_reader = BcsiReader::new(bcsi_path, top_index.clone(), BCSI_CACHE_SIZE)?;
        let mut bcsi = BcsiPrefetchReader::new(bcsi_reader, Arc::new(top_index));
        
        // Tile queues using single-slab architecture
        let mut tile_queues: HashMap<u32, TileQueueSlab> = HashMap::new();
        
        // Output writer
        let mut writer = PbfWriter::append(output_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to append: {}", e)))?;
        
        let mut total_ways = 0u64;
        let mut written_ways = 0u64;
        
        // Read ways
        let reader = ElementReader::from_path(input_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to open input: {}", e)))?;
        
        reader.for_each(|element| {
            if let Element::Way(way) = element {
                total_ways += 1;
                
                let way_refs: Vec<i64> = way.refs().collect();
                
                // Find first node that exists in BCSI (try all nodes if needed)
                let mut tile_id = None;
                for &node_id in &way_refs {
                    // Try hot cache first
                    if let Some(rep_id) = self.hot_cache.get(node_id) {
                        // Hot cache hit!
                        tile_id = Some(compute_tile_id(rep_id, rep_id, self.config.grid_size_m));
                        break;
                    }
                    
                    // Try BCSI lookup
                    if let Ok(results) = bcsi.lookup_chunk(&[node_id]) {
                        if let Some((_, payload)) = results.first() {
                            // Add to hot cache for future
                            self.hot_cache.insert(node_id, payload.rep_id);
                            tile_id = Some(payload.tile_id);
                            break;
                        }
                    }
                }
                
                // Skip way if no nodes found in BCSI
                let tile_id = match tile_id {
                    Some(id) => id,
                    None => return,  // No nodes in this way are representatives
                };
                
                // Check if we can allocate memory for this way
                let estimated_bytes = way_refs.len() * 8 + 100;
                if !self.memory_tracker.try_allocate(estimated_bytes) {
                    // Need to flush a tile first
                    if let Some((&flush_id, _)) = tile_queues.iter()
                        .max_by_key(|(_, q)| q.allocated_bytes()) {
                        if let Some(mut queue) = tile_queues.remove(&flush_id) {
                            let bytes = queue.allocated_bytes();
                            let flushed = self.flush_tile_v2(&mut queue, &mut bcsi, &mut writer).unwrap_or(0);
                            written_ways += flushed;
                            self.memory_tracker.release(bytes);
                        }
                    }
                    
                    // Try again
                    if !self.memory_tracker.try_allocate(estimated_bytes) {
                        return;  // Still can't allocate
                    }
                }
                
                // Extract tags
                let tags: Vec<(&str, &str)> = way.tags()
                    .map(|(k, v)| (k, v))
                    .collect();
                
                // Add to tile queue
                let queue = tile_queues.entry(tile_id)
                    .or_insert_with(|| TileQueueSlab::new());
                
                if !queue.add_way(way.id(), &way_refs, &tags) {
                    // Queue full or would overflow indices
                    let bytes = queue.allocated_bytes();
                    let mut queue_owned = std::mem::replace(queue, TileQueueSlab::new());
                    let flushed = self.flush_tile_v2(&mut queue_owned, &mut bcsi, &mut writer).unwrap_or(0);
                    written_ways += flushed;
                    self.memory_tracker.release(bytes);
                    
                    // Add to fresh queue
                    queue.add_way(way.id(), &way_refs, &tags);
                }
                
                // Check flush conditions
                if queue.should_flush() {
                    let bytes = queue.allocated_bytes();
                    let mut queue_owned = std::mem::replace(queue, TileQueueSlab::new());
                    let flushed = self.flush_tile_v2(&mut queue_owned, &mut bcsi, &mut writer).unwrap_or(0);
                    written_ways += flushed;
                    self.memory_tracker.release(bytes);
                }
                
                // Limit active tiles
                if tile_queues.len() >= MAX_ACTIVE_TILES {
                    // Flush smallest tile
                    if let Some((&flush_id, _)) = tile_queues.iter()
                        .min_by_key(|(_, q)| q.way_count()) {
                        if let Some(mut queue) = tile_queues.remove(&flush_id) {
                            let bytes = queue.allocated_bytes();
                            let flushed = self.flush_tile_v2(&mut queue, &mut bcsi, &mut writer).unwrap_or(0);
                            written_ways += flushed;
                            self.memory_tracker.release(bytes);
                        }
                    }
                }
                
                if total_ways % 100_000 == 0 {
                    progress.set_total(total_ways);
                    progress.set(total_ways);
                }
            }
        })
        .map_err(|e| Error::InvalidInput(format!("Failed to read ways: {}", e)))?;
        
        // Flush remaining tiles
        for (_, mut queue) in tile_queues {
            let bytes = queue.allocated_bytes();
            let flushed = self.flush_tile_v2(&mut queue, &mut bcsi, &mut writer)?;
            written_ways += flushed;
            self.memory_tracker.release(bytes);
        }
        
        writer.finalize().map_err(|e| Error::InvalidInput(format!("Failed to finalize: {}", e)))?;
        progress.finish();
        
        let elapsed = start.elapsed();
        log::info!("Phase 2 complete: {} ways → {} written in {:.2}s",
            total_ways, written_ways, elapsed.as_secs_f64());
        log::info!("BCSI efficiency: {:.3}", bcsi.efficiency_ratio());
        
        Ok(WayStats {
            total_ways,
            written_ways,
        })
    }
    
    /// Flush a tile with optimized lookups
    fn flush_tile_v2(
        &self,
        queue: &mut TileQueueSlab,
        bcsi: &mut BcsiPrefetchReader,
        writer: &mut PbfWriter,
    ) -> Result<u64> {
        if queue.way_count() == 0 {
            return Ok(0);
        }
        
        // Build unique nodes list
        queue.build_unique_nodes();
        let unique_nodes = queue.unique_nodes();
        
        // Check hot cache first, collect misses
        let mut cache_misses = Vec::new();
        let mut cached_mappings = Vec::new();
        
        for &node_id in unique_nodes {
            if let Some(rep_id) = self.hot_cache.get(node_id) {
                cached_mappings.push((node_id, rep_id));
            } else {
                cache_misses.push(node_id);
            }
        }
        
        // Perform optimized BCSI lookups for misses
        let bcsi_results = if !cache_misses.is_empty() {
            bcsi.lookup_chunk(&cache_misses)?
        } else {
            Vec::new()
        };
        
        // Convert results and update hot cache
        let bcsi_mappings: Vec<(i64, i64)> = bcsi_results.iter()
            .map(|&(orig_id, payload)| (orig_id, payload.rep_id))
            .collect();
        self.hot_cache.insert_batch(&bcsi_mappings);
        
        // Build complete mapping
        let mut id_map: Vec<(i64, i64)> = Vec::with_capacity(unique_nodes.len());
        id_map.extend(cached_mappings);
        id_map.extend(bcsi_mappings);
        id_map.sort_unstable_by_key(|&(k, _)| k);
        
        // Write ways
        let mut written = 0u64;
        
        for (idx, (way_id, refs, has_highway)) in queue.ways().enumerate() {
            if !has_highway {
                continue;  // No highway tag at all
            }
            
            // Check if highway value matches our filter
            let tags = queue.get_tags(idx);
            let highway_matches = tags.iter().any(|(k, v)| {
                *k == "highway" && self.config.highway_tags.iter().any(|t| *v == t.as_str())
            });
            
            if !highway_matches {
                continue;  // Highway type not in our list
            }
            
            // Remap using binary search
            let mut remapped = Vec::with_capacity(refs.len());
            for &node_id in refs {
                let rep_id = match id_map.binary_search_by_key(&node_id, |&(k, _)| k) {
                    Ok(idx) => id_map[idx].1,
                    Err(_) => node_id,
                };
                remapped.push(rep_id);
            }
            
            // Dedup consecutive
            remapped.dedup();
            
            if remapped.len() >= 2 {
                let tags = queue.get_tags(idx);
                writer.write_way_simple(way_id, remapped, tags)
                    .map_err(|e| Error::InvalidInput(format!("Failed to write way: {}", e)))?;
                written += 1;
            }
        }
        
        Ok(written)
    }
    
    fn cleanup(&self) -> Result<()> {
        log::info!("Cleaning up temporary files");
        std::fs::remove_dir_all(&self.temp_dir).ok();
        Ok(())
    }
}

// Stats structures
pub struct ProcessStats {
    pub total_nodes: u64,
    pub rep_nodes: u64,
    pub total_ways: u64,
    pub written_ways: u64,
    pub elapsed_secs: f64,
}

struct NodeStats {
    total_nodes: u64,
    rep_nodes: u64,
}

struct WayStats {
    total_ways: u64,
    written_ways: u64,
}