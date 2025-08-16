//! Two-pass processor for butterfly-shrink
//! 
//! Pass 1: Node deduplication with grid snapping and index building
//! Pass 2: Way processing with tile-based batching

use crate::config::Config;
use crate::memory::MemoryWatchdog;
use crate::telemetry::Telemetry;
use crate::writer::PbfWriter;
use butterfly_common::{Error, Result};
use osmpbf::{Element, ElementReader};
use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, DB};
use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

/// Column family names
const CF_ORIG_TO_REP: &str = "orig_to_rep";  // orig_node_id -> rep_id
const CF_REP_TO_TILE: &str = "rep_to_tile";   // rep_id -> tile_id

/// Tile ID calculation from coordinates
fn compute_tile_id(lat_nano: i64, lon_nano: i64, tile_km: f64) -> u32 {
    // Convert nanodegrees to degrees
    let lat = lat_nano as f64 / 1e9;
    let lon = lon_nano as f64 / 1e9;
    
    // Tile size in degrees (approximate)
    let tile_deg = tile_km / 111.0;
    
    // Compute tile indices
    let lat_idx = ((lat + 90.0) / tile_deg) as i32;
    let lon_idx = ((lon + 180.0) / tile_deg) as i32;
    
    // Combine into single 32-bit ID using Morton encoding
    let mut tile_id = 0u32;
    for i in 0..16 {
        tile_id |= ((lat_idx >> i) & 1) as u32;
        tile_id <<= 1;
        tile_id |= ((lon_idx >> i) & 1) as u32;
        if i < 15 {
            tile_id <<= 1;
        }
    }
    tile_id
}

/// Two-pass processor
pub struct TwoPassProcessor {
    config: Config,
    telemetry: Arc<Telemetry>,
    memory_watchdog: std::sync::Mutex<MemoryWatchdog>,
    db_path: String,
}

impl TwoPassProcessor {
    pub fn new(config: Config) -> Result<Self> {
        let telemetry = Arc::new(Telemetry::new());
        let memory_watchdog = std::sync::Mutex::new(MemoryWatchdog::new((config.max_memory_mb as f64 * 1.1) as usize));
        
        // Create temp directory for RocksDB
        let db_path = format!(
            "/tmp/butterfly-shrink-{}/node_index",
            uuid::Uuid::new_v4()
        );
        std::fs::create_dir_all(&db_path).map_err(Error::IoError)?;
        
        Ok(Self {
            config,
            telemetry,
            memory_watchdog,
            db_path,
        })
    }
    
    /// Pass 1: Process nodes, build index
    pub fn pass1_nodes(&self, input_path: &Path, output_path: &Path) -> Result<Pass1Stats> {
        log::info!("Pass 1: Processing nodes with grid snapping...");
        let start = Instant::now();
        
        // Check initial memory
        let mem_action = self.memory_watchdog.lock().unwrap().check();
        let rss_mb = self.memory_watchdog.lock().unwrap().current_rss_mb();
        log::info!("Initial memory: {:.1}MB, action: {:?}", rss_mb, mem_action);
        
        // Create RocksDB with two column families
        let mut cf_opts = Options::default();
        cf_opts.set_compression_type(rocksdb::DBCompressionType::Zstd);
        
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new(CF_ORIG_TO_REP, cf_opts.clone()),
            ColumnFamilyDescriptor::new(CF_REP_TO_TILE, cf_opts.clone()),
        ];
        
        let mut db_opts = Options::default();
        db_opts.create_if_missing(true);
        db_opts.create_missing_column_families(true);
        
        // Write-heavy optimization
        db_opts.set_write_buffer_size(64 * 1024 * 1024); // 64MB memtables
        db_opts.set_max_write_buffer_number(3);
        db_opts.set_target_file_size_base(64 * 1024 * 1024);
        db_opts.set_disable_auto_compactions(false);
        db_opts.set_max_background_jobs(4);
        
        let db = DB::open_cf_descriptors(&db_opts, &self.db_path, cf_descriptors)
            .map_err(|e| Error::InvalidInput(format!("Failed to open database: {}", e)))?;
        
        let cf_orig = db.cf_handle(CF_ORIG_TO_REP)
            .ok_or_else(|| Error::InvalidInput("CF1 not found".to_string()))?;
        let cf_tile = db.cf_handle(CF_REP_TO_TILE)
            .ok_or_else(|| Error::InvalidInput("CF2 not found".to_string()))?;
        
        // Track deduplicated nodes
        let mut seen_cells = HashSet::new();
        let mut rep_nodes = Vec::new();
        let mut cell_to_rep: HashMap<(i64, i64), i64> = HashMap::new(); // O(1) lookup
        let mut total_nodes = 0u64;
        let mut rep_count = 0u64;
        
        // Open input PBF
        let reader = ElementReader::from_path(input_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to open input: {e}")))?;
        
        // Create output writer
        let mut writer = PbfWriter::new(output_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to create writer: {}", e)))?;
        writer.write_header()
            .map_err(|e| Error::InvalidInput(format!("Failed to write header: {}", e)))?;
        
        // Process all nodes
        let batch = std::cell::RefCell::new(WriteBatch::default());
        let mut batch_size = 0;
        
        reader.for_each(|element| {
            match element {
                Element::Node(node) => {
                    total_nodes += 1;
                    log::debug!("Processing node {}: ({}, {})", node.id(), node.lat(), node.lon());
                
                    // Snap to grid
                    let (lat_nano, lon_nano) = crate::snap_coordinate(
                        node.lat(),
                        node.lon(),
                        self.config.grid_size_m as f64,
                    );
                    
                    // Check if this cell already has a representative
                    let cell_key = (lat_nano, lon_nano);
                    let is_rep = seen_cells.insert(cell_key);
                    
                    let rep_id = if is_rep {
                        // This is the representative for this cell
                        rep_count += 1;
                        let rep_id = node.id();
                        
                        // Store mapping for O(1) lookup
                        cell_to_rep.insert(cell_key, rep_id);
                        
                        // Write to output
                        rep_nodes.push((rep_id, lat_nano, lon_nano));
                        
                        // Compute tile ID
                        let tile_id = compute_tile_id(lat_nano, lon_nano, 2.0); // 2km tiles
                        
                        // Add to batch: rep_id -> tile_id
                        batch.borrow_mut().put_cf(
                            cf_tile,
                            rep_id.to_le_bytes(),
                            tile_id.to_le_bytes(),
                        );
                        batch_size += 1;
                        
                        rep_id
                    } else {
                        // O(1) lookup instead of O(N) search
                        *cell_to_rep.get(&cell_key).unwrap_or(&node.id())
                    };
                    
                    // Add to batch: orig_id -> rep_id
                    batch.borrow_mut().put_cf(
                        cf_orig,
                        node.id().to_le_bytes(),
                        rep_id.to_le_bytes(),
                    );
                    batch_size += 1;
                    
                    // Flush batch periodically (larger batches for better performance)
                    if batch_size >= 100_000 {
                        let b = batch.replace(WriteBatch::default());
                        if let Err(e) = db.write(b) {
                            log::error!("Failed to write batch: {}", e);
                        }
                        batch_size = 0;
                        
                        // Check memory periodically
                        if total_nodes % 100_000 == 0 {
                            let mem_action = self.memory_watchdog.lock().unwrap().check();
                            if mem_action != crate::memory::MemoryAction::Continue {
                                let rss_mb = self.memory_watchdog.lock().unwrap().current_rss_mb();
                                log::warn!("Memory pressure during Pass 1: {:?} at {:.1}MB", mem_action, rss_mb);
                            }
                        }
                    }
                }
                Element::DenseNode(dense_node) => {
                    // Process dense node (contains ID, lat, lon directly)
                    total_nodes += 1;
                    log::debug!("Processing dense node {}: ({}, {})", dense_node.id(), dense_node.lat(), dense_node.lon());
                    
                    // Snap to grid  
                    let (lat_nano, lon_nano) = crate::snap_coordinate(
                        dense_node.lat(),
                        dense_node.lon(),
                        self.config.grid_size_m as f64,
                    );
                    
                    // Check if this cell already has a representative
                    let cell_key = (lat_nano, lon_nano);
                    let is_rep = seen_cells.insert(cell_key);
                    
                    let rep_id = if is_rep {
                        // This is the representative for this cell
                        rep_count += 1;
                        let rep_id = dense_node.id();
                        
                        // Store mapping for O(1) lookup
                        cell_to_rep.insert(cell_key, rep_id);
                        
                        // Write to output
                        rep_nodes.push((rep_id, lat_nano, lon_nano));
                        
                        // Compute tile ID
                        let tile_id = compute_tile_id(lat_nano, lon_nano, 2.0); // 2km tiles
                        
                        // Add to batch: rep_id -> tile_id
                        batch.borrow_mut().put_cf(
                            cf_tile,
                            rep_id.to_le_bytes(),
                            tile_id.to_le_bytes(),
                        );
                        batch_size += 1;
                        
                        rep_id
                    } else {
                        // O(1) lookup instead of O(N) search
                        *cell_to_rep.get(&cell_key).unwrap_or(&dense_node.id())
                    };
                    
                    // Add to batch: orig_id -> rep_id
                    batch.borrow_mut().put_cf(
                        cf_orig,
                        dense_node.id().to_le_bytes(),
                        rep_id.to_le_bytes(),
                    );
                    batch_size += 1;
                    
                    // Flush batch periodically (larger batches for better performance)
                    if batch_size >= 100_000 {
                        let b = batch.replace(WriteBatch::default());
                        if let Err(e) = db.write(b) {
                            log::error!("Failed to write batch: {}", e);
                        }
                        batch_size = 0;
                        
                        // Check memory periodically
                        if total_nodes % 100_000 == 0 {
                            let mem_action = self.memory_watchdog.lock().unwrap().check();
                            if mem_action != crate::memory::MemoryAction::Continue {
                                let rss_mb = self.memory_watchdog.lock().unwrap().current_rss_mb();
                                log::warn!("Memory pressure during Pass 1: {:?} at {:.1}MB", mem_action, rss_mb);
                            }
                        }
                    }
                }
                _ => {} // Ignore ways and relations in pass 1
            }
        })
        .map_err(|e| Error::InvalidInput(format!("Failed to read nodes: {e}")))?;
        
        // Final batch flush
        if batch_size > 0 {
            db.write(batch.into_inner()).map_err(|e| Error::InvalidInput(format!("Failed to write final batch: {}", e)))?;
        }
        
        // Write all representative nodes to output
        log::info!("Writing {} representative nodes to output...", rep_count);
        for (id, lat_nano, lon_nano) in rep_nodes {
            writer.write_node_nano(id, lat_nano, lon_nano, &[])
                .map_err(|e| Error::InvalidInput(format!("Failed to write node: {}", e)))?;
        }
        writer.finalize()
            .map_err(|e| Error::InvalidInput(format!("Failed to finalize writer: {}", e)))?;
        
        // Compact the database for optimal read performance
        log::info!("Compacting database for Pass 2...");
        db.compact_range_cf(cf_orig, None::<&[u8]>, None::<&[u8]>);
        db.compact_range_cf(cf_tile, None::<&[u8]>, None::<&[u8]>);
        
        // Close database
        drop(db);
        
        let elapsed = start.elapsed();
        
        // Report telemetry
        self.telemetry.print_stats();
        
        log::info!(
            "Pass 1 complete: {} nodes → {} representatives in {:.2}s ({:.1}M nodes/s)",
            total_nodes,
            rep_count,
            elapsed.as_secs_f64(),
            total_nodes as f64 / elapsed.as_secs_f64() / 1e6
        );
        
        Ok(Pass1Stats {
            total_nodes,
            rep_nodes: rep_count,
            elapsed_secs: elapsed.as_secs_f64(),
        })
    }
    
    /// Pass 2: Process ways with tile-based batching
    pub fn pass2_ways(&self, input_path: &Path, output_path: &Path) -> Result<Pass2Stats> {
        log::info!("Pass 2: Processing ways with tile-based batching...");
        let start = Instant::now();
        
        // Open RocksDB in read-optimized mode
        let mut db_opts = Options::default();
        db_opts.set_use_direct_reads(true);
        
        // Read-optimized settings
        let cache_size = (self.config.max_memory_mb as usize * 40 / 100) * 1024 * 1024; // 40% for DB
        let mut block_opts = rocksdb::BlockBasedOptions::default();
        block_opts.set_block_cache(&rocksdb::Cache::new_lru_cache(cache_size));
        block_opts.set_block_size(16 * 1024); // 16KB blocks
        block_opts.set_cache_index_and_filter_blocks(true);
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        db_opts.set_block_based_table_factory(&block_opts);
        
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new(CF_ORIG_TO_REP, db_opts.clone()),
            ColumnFamilyDescriptor::new(CF_REP_TO_TILE, db_opts.clone()),
        ];
        
        let db = DB::open_cf_descriptors(&db_opts, &self.db_path, cf_descriptors)
            .map_err(|e| Error::InvalidInput(format!("Failed to open database: {}", e)))?;
        
        let cf_orig = db.cf_handle(CF_ORIG_TO_REP)
            .ok_or_else(|| Error::InvalidInput("CF1 not found".to_string()))?;
        let cf_tile = db.cf_handle(CF_REP_TO_TILE)
            .ok_or_else(|| Error::InvalidInput("CF2 not found".to_string()))?;
        
        // Tile queues (K=32 active tiles)
        const MAX_ACTIVE_TILES: usize = 32;
        let mut tile_queues: HashMap<u32, TileQueue> = HashMap::new();
        
        // Memory budget for tile queues (30% of total)
        let queue_budget_mb = self.config.max_memory_mb as usize * 30 / 100;
        let per_tile_budget_mb = queue_budget_mb / MAX_ACTIVE_TILES;
        
        // Open input PBF again
        let reader = ElementReader::from_path(input_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to open input: {e}")))?;
        
        // Append to existing output (already has nodes)
        let mut writer = PbfWriter::append(output_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to open output for appending: {}", e)))?;
        
        let mut total_ways = 0u64;
        let mut written_ways = 0u64;
        
        // Process all ways
        reader.for_each(|element| {
            if let Element::Way(way) = element {
                total_ways += 1;
                // Way counting is done through total_ways
                
                // Check memory periodically
                if total_ways % 10_000 == 0 {
                    let mem_action = self.memory_watchdog.lock().unwrap().check();
                    if mem_action != crate::memory::MemoryAction::Continue {
                        let rss_mb = self.memory_watchdog.lock().unwrap().current_rss_mb();
                        log::warn!("Memory pressure during Pass 2: {:?} at {:.1}MB", mem_action, rss_mb);
                    }
                }
                
                // Get first node to determine tile
                let way_refs: Vec<i64> = way.refs().collect();
                if let Some(&first_node_id) = way_refs.first() {
                    // Look up representative
                    if let Ok(Some(rep_bytes)) = db.get_cf(cf_orig, first_node_id.to_le_bytes()) {
                        let rep_id = i64::from_le_bytes(rep_bytes.try_into().unwrap());
                        
                        // Look up tile
                        if let Ok(Some(tile_bytes)) = db.get_cf(cf_tile, rep_id.to_le_bytes()) {
                            let tile_id = u32::from_le_bytes(tile_bytes.try_into().unwrap());
                            
                            // Add to tile queue
                            let queue = tile_queues.entry(tile_id)
                                .or_insert_with(|| TileQueue::new(per_tile_budget_mb));
                            
                            // Convert tags to owned strings
                            let tags: Vec<(String, String)> = way.tags()
                                .map(|(k, v)| (k.to_string(), v.to_string()))
                                .collect();
                            
                            queue.add_way(way.id(), way_refs.clone(), tags);
                            
                            // Check if queue needs flushing
                            if queue.should_flush() {
                                match self.flush_tile_queue(
                                    queue,
                                    &db,
                                    cf_orig,
                                    &mut writer,
                                ) {
                                    Ok(batch_ways) => {
                                        written_ways += batch_ways;
                                        queue.reset();
                                    }
                                    Err(e) => {
                                        log::error!("Failed to flush tile queue: {}", e);
                                    }
                                }
                            }
                        }
                    }
                }
                
                // If too many active tiles, flush oldest
                if tile_queues.len() > MAX_ACTIVE_TILES {
                    // Find tile with most ways and flush it
                    if let Some((&tile_id, _)) = tile_queues.iter()
                        .max_by_key(|(_, q)| q.way_count()) {
                        
                        if let Some(mut queue) = tile_queues.remove(&tile_id) {
                            match self.flush_tile_queue(
                                &mut queue,
                                &db,
                                cf_orig,
                                &mut writer,
                            ) {
                                Ok(batch_ways) => {
                                    written_ways += batch_ways;
                                }
                                Err(e) => {
                                    log::error!("Failed to flush tile queue: {}", e);
                                }
                            }
                        }
                    }
                }
            }
        })
        .map_err(|e| Error::InvalidInput(format!("Failed to read ways: {e}")))?;
        
        // Flush remaining tiles
        for (_, mut queue) in tile_queues {
            if queue.way_count() > 0 {
                let batch_ways = self.flush_tile_queue(
                    &mut queue,
                    &db,
                    cf_orig,
                    &mut writer,
                )?;
                written_ways += batch_ways;
            }
        }
        
        writer.finalize()
            .map_err(|e| Error::InvalidInput(format!("Failed to finalize writer: {}", e)))?;
        
        let elapsed = start.elapsed();
        
        // Report telemetry
        self.telemetry.print_stats();
        
        log::info!(
            "Pass 2 complete: {} ways → {} written in {:.2}s ({:.1}k ways/s)",
            total_ways,
            written_ways,
            elapsed.as_secs_f64(),
            total_ways as f64 / elapsed.as_secs_f64() / 1000.0
        );
        
        Ok(Pass2Stats {
            total_ways,
            written_ways,
            elapsed_secs: elapsed.as_secs_f64(),
        })
    }
    
    fn flush_tile_queue(
        &self,
        queue: &mut TileQueue,
        db: &DB,
        cf_orig: &ColumnFamily,
        writer: &mut PbfWriter,
    ) -> Result<u64> {
        // Collect all unique node IDs
        let unique_ids = queue.get_unique_node_ids();
        
        // MultiGet in chunks
        let mut id_map = HashMap::new();
        for chunk in unique_ids.chunks(200_000) {
            let keys: Vec<_> = chunk.iter()
                .map(|id| id.to_le_bytes().to_vec())
                .collect();
            
            let results = db.multi_get_cf(
                keys.iter().map(|k| (cf_orig, k.as_slice())).collect::<Vec<_>>()
            );
            
            for (i, result) in results.into_iter().enumerate() {
                if let Ok(Some(value)) = result {
                    let orig_id = chunk[i];
                    let rep_id = i64::from_le_bytes(value.try_into().unwrap());
                    id_map.insert(orig_id, rep_id);
                }
            }
        }
        
        // Write remapped ways
        let mut written = 0u64;
        for (way_id, node_refs, tags) in queue.drain_ways() {
            // Remap node references
            let remapped: Vec<i64> = node_refs.iter()
                .map(|&id| id_map.get(&id).copied().unwrap_or(id))
                .collect();
            
            // Remove consecutive duplicates
            let mut deduped = Vec::new();
            let mut prev = None;
            for &node in &remapped {
                if prev != Some(node) {
                    deduped.push(node);
                    prev = Some(node);
                }
            }
            
            // Only write if way has at least 2 nodes
            if deduped.len() >= 2 {
                // Convert tags to &str references
                let tag_refs: Vec<(&str, &str)> = tags.iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                writer.write_way_simple(way_id, deduped, tag_refs)
                    .map_err(|e| Error::InvalidInput(format!("Failed to write way: {}", e)))?;
                written += 1;
            }
        }
        
        Ok(written)
    }
}

/// Tile queue for batching ways
struct TileQueue {
    ways: Vec<(i64, Vec<i64>, Vec<(String, String)>)>,
    unique_node_ids: HashSet<i64>,
    estimated_bytes: usize,
    max_bytes: usize,
}

impl TileQueue {
    fn new(max_mb: usize) -> Self {
        Self {
            ways: Vec::new(),
            unique_node_ids: HashSet::new(),
            estimated_bytes: 0,
            max_bytes: max_mb * 1024 * 1024,
        }
    }
    
    fn add_way(&mut self, id: i64, refs: Vec<i64>, tags: Vec<(String, String)>) {
        // Track unique nodes
        for &node_id in &refs {
            self.unique_node_ids.insert(node_id);
        }
        
        // Estimate memory
        let way_bytes = 8 + refs.len() * 8 + tags.len() * 32;
        self.estimated_bytes += way_bytes;
        
        self.ways.push((id, refs, tags));
    }
    
    fn should_flush(&self) -> bool {
        self.ways.len() >= 50_000 ||
        self.unique_node_ids.len() >= 1_500_000 ||
        self.estimated_bytes >= self.max_bytes
    }
    
    fn way_count(&self) -> usize {
        self.ways.len()
    }
    
    fn get_unique_node_ids(&self) -> Vec<i64> {
        self.unique_node_ids.iter().copied().collect()
    }
    
    fn drain_ways(&mut self) -> Vec<(i64, Vec<i64>, Vec<(String, String)>)> {
        std::mem::take(&mut self.ways)
    }
    
    fn reset(&mut self) {
        self.ways.clear();
        self.unique_node_ids.clear();
        self.estimated_bytes = 0;
    }
}

pub struct Pass1Stats {
    pub total_nodes: u64,
    pub rep_nodes: u64,
    pub elapsed_secs: f64,
}

pub struct Pass2Stats {
    pub total_ways: u64,
    pub written_ways: u64,
    pub elapsed_secs: f64,
}