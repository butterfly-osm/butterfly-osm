//! Main processing pipeline for PBF shrinking

use crate::batch::{BatchConfig, WayBatcher};
use crate::config::Config;
use crate::db::{NodeIndex, PhaseMode};
use crate::snap_coordinate;
use crate::telemetry::{Telemetry, Timer};
use crate::writer::PbfWriter;
use anyhow::{Context, Result};
use butterfly_common::Error;
use osmpbf::{Element, ElementReader};
use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Debug, Clone, PartialEq)]
enum ProcessingPhase {
    Nodes,
    Ways,
    Relations,
}

/// Statistics for the processing run
#[derive(Debug, Default, Clone)]
pub struct Stats {
    pub input_nodes: u64,
    pub output_nodes: u64,
    pub input_ways: u64,
    pub output_ways: u64,
    pub input_relations: u64,
    pub output_relations: u64,
    pub dropped_ways: u64,
    pub failed_restrictions: u64,
    pub grid_cells: u64,
}

/// Main processor for shrinking PBF files
pub struct Processor {
    config: Config,
    db_path: PathBuf,
    node_index: Option<Arc<NodeIndex>>,
    stats: Stats,
    telemetry: Telemetry,
    batch_config: BatchConfig,
}

impl Processor {
    pub fn new(config: Config, db_path: PathBuf) -> Self {
        Self {
            config,
            db_path,
            node_index: None,
            stats: Stats::default(),
            telemetry: Telemetry::new(),
            batch_config: BatchConfig::default(),
        }
    }
    
    pub fn with_batch_config(mut self, batch_config: BatchConfig) -> Self {
        self.batch_config = batch_config;
        self
    }
    
    /// Process a PBF file
    pub fn process(&mut self, input: &Path, output: &Path) -> Result<Stats> {
        // Check if input file exists
        if !input.exists() {
            return Err(anyhow::anyhow!("Input file not found: {}", input.display()));
        }
        
        // Open input reader
        let reader = ElementReader::from_path(input)
            .map_err(|e| Error::InvalidInput(format!("Failed to open PBF: {}", e)))?;
        
        // For now, we'll implement single-threaded processing
        // Parallel processing will be added in the next iteration
        self.process_single_threaded(reader, input, output)?;
        
        Ok(self.stats.clone())
    }
    
    /// Optimized processing with batching and caching
    fn process_single_threaded<R: std::io::Read + Send>(
        &mut self,
        reader: ElementReader<R>,
        _input: &Path,
        output: &Path,
    ) -> Result<()> {
        let mut grid_cells = HashMap::new();
        
        // Open NodeIndex in write-optimized mode for node ingestion
        log::info!("Opening RocksDB in write-optimized mode for node ingestion...");
        let node_index = NodeIndex::new_with_mode(&self.db_path, PhaseMode::WriteHeavy, self.config.db_cache_mb)
            .context("Failed to open NodeIndex for writes")?;
        self.node_index = Some(Arc::new(node_index));
        
        // Create PBF writer
        let mut pbf_writer = PbfWriter::new(output)
            .context("Failed to create PBF writer")?;
        pbf_writer.write_header()
            .context("Failed to write PBF header")?;
        
        // Create way batcher with LRU cache
        let mut way_batcher = WayBatcher::new(self.batch_config.clone(), self.telemetry.clone());
        let highway_tags: HashSet<String> = self.config.highway_tags.iter().cloned().collect();
        
        // Process elements in streaming fashion with phase separation
        log::info!("Processing PBF elements in streaming mode...");
        let mut processing_phase = ProcessingPhase::Nodes;
        let node_timer = Timer::new();
        let mut way_timer = Timer::default();
        let mut relation_timer = Timer::default();
        let mut switch_error: Option<anyhow::Error> = None;
        
        let read_result = reader.for_each(|element| {
            match element {
                Element::Node(node) => {
                    if processing_phase != ProcessingPhase::Nodes {
                        // Start way processing phase
                        self.telemetry.record_node_ingest_time(node_timer.elapsed());
                        log::info!("Phase 1 complete: {} nodes processed in {}ms", 
                            self.stats.input_nodes, node_timer.elapsed().as_millis());
                        
                        // Switch to read-optimized mode for way processing
                        if switch_error.is_none() {
                            if let Err(e) = self.switch_to_read_mode() {
                                log::error!("Failed to switch to read mode: {}", e);
                                switch_error = Some(e);
                                return;
                            }
                        }
                        
                        processing_phase = ProcessingPhase::Ways;
                        way_timer = Timer::new();
                        log::info!("Phase 2: Processing ways with batching...");
                    }
                    
                    if let Err(e) = self.process_node(node.id(), node.lat(), node.lon(), &mut grid_cells, &mut pbf_writer) {
                        log::error!("Failed to process node {}: {}", node.id(), e);
                    }
                }
                Element::DenseNode(dense_node) => {
                    if processing_phase != ProcessingPhase::Nodes {
                        // Start way processing phase  
                        self.telemetry.record_node_ingest_time(node_timer.elapsed());
                        log::info!("Phase 1 complete: {} nodes processed in {}ms", 
                            self.stats.input_nodes, node_timer.elapsed().as_millis());
                        
                        // Switch to read-optimized mode for way processing
                        if switch_error.is_none() {
                            if let Err(e) = self.switch_to_read_mode() {
                                log::error!("Failed to switch to read mode: {}", e);
                                switch_error = Some(e);
                                return;
                            }
                        }
                        
                        processing_phase = ProcessingPhase::Ways;
                        way_timer = Timer::new();
                        log::info!("Phase 2: Processing ways with batching...");
                    }
                    
                    if let Err(e) = self.process_node(dense_node.id(), dense_node.lat(), dense_node.lon(), &mut grid_cells, &mut pbf_writer) {
                        log::error!("Failed to process dense node {}: {}", dense_node.id(), e);
                    }
                }
                Element::Way(way) => {
                    if processing_phase == ProcessingPhase::Nodes {
                        // Start way processing phase
                        self.telemetry.record_node_ingest_time(node_timer.elapsed());
                        log::info!("Phase 1 complete: {} nodes processed in {}ms", 
                            self.stats.input_nodes, node_timer.elapsed().as_millis());
                        
                        // Switch to read-optimized mode for way processing
                        if switch_error.is_none() {
                            if let Err(e) = self.switch_to_read_mode() {
                                log::error!("Failed to switch to read mode: {}", e);
                                switch_error = Some(e);
                                return;
                            }
                        }
                        
                        processing_phase = ProcessingPhase::Ways;
                        way_timer = Timer::new();
                        log::info!("Phase 2: Processing ways with batching...");
                    } else if processing_phase == ProcessingPhase::Relations {
                        // This shouldn't happen in a well-formed PBF, but handle it
                        log::warn!("Found way after relations started - PBF may not be properly sorted");
                    }
                    
                    self.stats.input_ways += 1;
                    if let Some(ref node_index) = self.node_index {
                        if let Err(e) = way_batcher.add_way(&way, &highway_tags, node_index, &mut pbf_writer) {
                            log::error!("Failed to process way {}: {}", way.id(), e);
                        }
                    }
                }
                Element::Relation(relation) => {
                    if processing_phase == ProcessingPhase::Ways {
                        // Start relation processing phase
                        // Flush all remaining tiles first
                        if let Some(ref node_index) = self.node_index {
                            if let Err(e) = way_batcher.flush_all_tiles(node_index, &mut pbf_writer) {
                                log::error!("Failed to flush tiles: {}", e);
                            }
                            
                            // Then flush any remaining unbucketed ways
                            match way_batcher.flush_batch(node_index, &mut pbf_writer) {
                                Ok(final_batch_ways) => {
                                    self.stats.output_ways = way_batcher.total_ways_written() + final_batch_ways;
                                }
                                Err(e) => {
                                    log::error!("Failed to flush final way batch: {}", e);
                                    self.stats.output_ways = way_batcher.total_ways_written();
                                }
                            }
                        }
                        
                        self.telemetry.record_way_remap_time(way_timer.elapsed());
                        log::info!("Phase 2 complete: {} ways processed in {}ms", 
                            self.stats.input_ways, way_timer.elapsed().as_millis());
                        
                        processing_phase = ProcessingPhase::Relations;
                        relation_timer = Timer::new();
                        log::info!("Phase 3: Processing relations...");
                    } else if processing_phase == ProcessingPhase::Nodes {
                        // This shouldn't happen
                        log::warn!("Found relation before ways - PBF may not be properly sorted");
                    }
                    
                    self.stats.input_relations += 1;
                    if let Err(e) = self.process_relation(&relation, &mut pbf_writer) {
                        log::error!("Failed to process relation {}: {}", relation.id(), e);
                    }
                }
            }
        });
        
        // Handle any remaining phase completions
        match processing_phase {
            ProcessingPhase::Nodes => {
                self.telemetry.record_node_ingest_time(node_timer.elapsed());
                log::info!("Processing complete: only nodes found");
            }
            ProcessingPhase::Ways => {
                // Flush all remaining tiles first
                if let Some(ref node_index) = self.node_index {
                    if let Err(e) = way_batcher.flush_all_tiles(node_index, &mut pbf_writer) {
                        log::error!("Failed to flush tiles: {}", e);
                    }
                    
                    // Then flush any remaining unbucketed ways
                    if let Ok(final_batch_ways) = way_batcher.flush_batch(node_index, &mut pbf_writer) {
                        self.stats.output_ways = way_batcher.total_ways_written() + final_batch_ways;
                    } else {
                        self.stats.output_ways = way_batcher.total_ways_written();
                    }
                }
                
                self.telemetry.record_way_remap_time(way_timer.elapsed());
                log::info!("Phase 2 complete: {} ways processed in {}ms", 
                    self.stats.input_ways, way_timer.elapsed().as_millis());
            }
            ProcessingPhase::Relations => {
                self.telemetry.record_relation_pass_time(relation_timer.elapsed());
                log::info!("Phase 3 complete: {} relations processed in {}ms", 
                    self.stats.input_relations, relation_timer.elapsed().as_millis());
            }
        }
        
        // Check for switch errors first
        if let Some(err) = switch_error {
            return Err(err);
        }
        
        // Check for PBF reading errors
        read_result.map_err(|e| Error::InvalidInput(format!("Failed to read PBF: {}", e)))?;
        
        self.stats.grid_cells = grid_cells.len() as u64;
        
        // Finalize PBF file
        pbf_writer.finalize()
            .context("Failed to finalize PBF file")?;
        
        // Print telemetry
        self.telemetry.print_stats();
        
        log::info!("Processing complete: {} nodes -> {} nodes, {} ways -> {} ways ({} cells)",
            self.stats.input_nodes,
            self.stats.output_nodes,
            self.stats.input_ways,
            self.stats.output_ways,
            self.stats.grid_cells
        );
        
        Ok(())
    }
    
    /// Process a single node with immediate RocksDB storage
    fn process_node(
        &mut self,
        node_id: i64,
        lat: f64,
        lon: f64,
        grid_cells: &mut HashMap<(i64, i64), i64>,
        pbf_writer: &mut PbfWriter,
    ) -> Result<()> {
        self.stats.input_nodes += 1;
        
        // Snap coordinates
        let (lat_nano, lon_nano) = snap_coordinate(lat, lon, self.config.grid_size_m);
        let grid_key = (lat_nano, lon_nano);
        
        // Check if this grid cell already has a representative
        let representative_id = if let Some(&rep_id) = grid_cells.get(&grid_key) {
            // Map to existing representative
            rep_id
        } else {
            // This node becomes the representative
            grid_cells.insert(grid_key, node_id);
            self.stats.output_nodes += 1;
            node_id
        };
        
        // Store mapping in RocksDB immediately
        if let Some(ref node_index) = self.node_index {
            node_index.put(node_id, representative_id)
                .context("Failed to store node mapping")?;
            self.telemetry.increment_puts(1);
        } else {
            return Err(anyhow::anyhow!("NodeIndex not initialized"));
        }
        
        // Write snapped node to output PBF if it's a representative
        if representative_id == node_id {
            let snapped_lat = lat_nano as f64 / 1e9;
            let snapped_lon = lon_nano as f64 / 1e9;
            let tags = HashMap::new(); // Nodes don't need tags for routing
            pbf_writer.write_node(representative_id, snapped_lat, snapped_lon, &tags)
                .context("Failed to write node to PBF")?;
        }
        
        Ok(())
    }
    
    
    /// Process a single relation (turn restrictions)
    fn process_relation(&mut self, relation: &osmpbf::Relation, pbf_writer: &mut PbfWriter) -> Result<()> {
        self.stats.input_relations += 1;
        
        // Only process turn restrictions if enabled
        if !self.config.keep_turn_restrictions {
            return Ok(());
        }
        
        // Check if this is a turn restriction
        let mut is_restriction = false;
        for (key, value) in relation.tags() {
            if key == "type" && value == "restriction" {
                is_restriction = true;
                break;
            }
        }
        
        if !is_restriction {
            return Ok(());
        }
        
        // TODO: Process turn restrictions with node remapping
        // For now, just count them and write placeholder
        self.stats.output_relations += 1;
        
        let tags: HashMap<String, String> = relation.tags().map(|(k, v)| (k.to_string(), v.to_string())).collect();
        let members = Vec::new(); // TODO: Implement proper member remapping
        
        pbf_writer.write_relation(relation.id(), &members, &tags)
            .context("Failed to write relation to PBF")?;
            
        log::debug!("Found turn restriction: {}", relation.id());
        Ok(())
    }
    
    /// Switch from write-optimized to read-optimized mode at phase boundary
    fn switch_to_read_mode(&mut self) -> Result<()> {
        log::info!("Switching to read-optimized mode for way processing...");
        let start = std::time::Instant::now();
        
        // First compact the existing DB
        if let Some(ref node_index) = self.node_index {
            log::info!("Compacting database for optimal read performance...");
            node_index.compact_for_reads()
                .context("Failed to compact RocksDB")?;
        }
        
        // Close the write-optimized DB
        log::info!("Closing write-optimized database...");
        self.node_index = None;
        
        // Small delay to ensure files are released
        std::thread::sleep(std::time::Duration::from_millis(100));
        
        // Reopen in read-optimized mode
        log::info!("Reopening database in read-optimized mode...");
        let node_index = NodeIndex::new_with_mode(&self.db_path, PhaseMode::ReadOptimized, self.config.db_cache_mb)
            .context("Failed to reopen NodeIndex for reads")?;
        self.node_index = Some(Arc::new(node_index));
        
        let elapsed = start.elapsed();
        log::info!("Mode switch completed in {:.2}s", elapsed.as_secs_f64());
        
        Ok(())
    }
    
    /// Get processing statistics
    pub fn stats(&self) -> &Stats {
        &self.stats
    }
}

/// Check if the temporary directory is on tmpfs
pub fn check_tmpfs(path: &Path) -> bool {
    #[cfg(target_os = "linux")]
    {
        use std::fs;
        use std::io::Read;
        
        if let Ok(mut file) = fs::File::open("/proc/mounts") {
            let mut contents = String::new();
            if file.read_to_string(&mut contents).is_ok() {
                for line in contents.lines() {
                    let parts: Vec<&str> = line.split_whitespace().collect();
                    if parts.len() >= 3 {
                        let mount_point = parts[1];
                        let fs_type = parts[2];
                        
                        if fs_type == "tmpfs" && path.starts_with(mount_point) {
                            return true;
                        }
                    }
                }
            }
        }
    }
    
    #[cfg(target_os = "macos")]
    {
        use std::process::Command;
        
        if let Ok(output) = Command::new("mount").output() {
            if let Ok(stdout) = String::from_utf8(output.stdout) {
                for line in stdout.lines() {
                    if line.contains("tmpfs") || line.contains("ramfs") {
                        let parts: Vec<&str> = line.split_whitespace().collect();
                        if parts.len() >= 3 {
                            let mount_point = parts[2];
                            if path.starts_with(mount_point) {
                                return true;
                            }
                        }
                    }
                }
            }
        }
    }
    
    false
}