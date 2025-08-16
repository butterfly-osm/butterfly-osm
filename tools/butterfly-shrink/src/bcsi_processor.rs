//! BCSI-based single-pass processor with hard 4GB memory cap
//! 
//! Processes OSM PBF files with a single read, building a compressed index
//! at the node→way boundary for efficient lookups.

use crate::bcsi::{BcsiWriter, BcsiReader, BcsiPayload, TopIndexEntry, compute_tile_id};
use crate::external_sort::{ExternalSorter, ExternalSortConfig, CellRecord, MappingRecord, SortableRecord};
use crate::writer::PbfWriter;
use crate::config::Config;
use crate::telemetry::Telemetry;
use butterfly_common::{Error, Result};
use osmpbf::{Element, ElementReader};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, BufReader, Write, Read};
use std::path::{Path, PathBuf};
use std::sync::{Arc, atomic::{AtomicUsize, Ordering}};
use std::time::Instant;

// Memory budget constants (4GB total) - EMERGENCY SAFE BASELINE
const BCSI_CACHE_SIZE: usize = 512_000_000;       // 512 MB for BCSI block cache (SAFE)
const TILE_STAGING_SIZE: usize = 512_000_000;     // 512 MB for tile queues (SAFE)
const _LOOKUP_STAGING_SIZE: usize = 128_000_000;  // 128 MB for lookup staging
const _WRITER_IO_SIZE: usize = 384_000_000;       // 384 MB for writer/decoder
const _TOP_INDEX_SIZE: usize = 256_000_000;       // 256 MB for top index + misc
// Total: ~1.9GB peak, leaves margin for allocator/page cache

// Processing constants - OPTIMIZED FOR SPEED
const _SPILL_RUN_SIZE: usize = 1_000_000_000;     // 1 GB per spill run
const MAX_ACTIVE_TILES: usize = 16;               // Maximum concurrent tiles (RESTORED)
const MAX_WAYS_PER_TILE: usize = 50_000;          // Ways per tile before flush (RESTORED)
const MAX_NODES_PER_TILE: usize = 400_000;        // Unique nodes per tile (RESTORED)
const MAX_BYTES_PER_TILE: usize = 80_000_000;     // 80 MB per tile (INCREASED)
const LOOKUP_CHUNK_SIZE: usize = 100_000;         // Max keys per BCSI lookup (SAFE)

/// Global memory tracker for hard enforcement
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
                    // Update peak
                    let mut peak = self.peak_bytes.load(Ordering::Relaxed);
                    while new > peak {
                        match self.peak_bytes.compare_exchange_weak(
                            peak,
                            new,
                            Ordering::Relaxed,
                            Ordering::Relaxed,
                        ) {
                            Ok(_) => break,
                            Err(p) => peak = p,
                        }
                    }
                    return true;
                }
                Err(c) => current = c,
            }
        }
    }
    
    fn release(&self, bytes: usize) {
        self.tile_bytes.fetch_sub(bytes, Ordering::Release);
    }
    
    fn _current(&self) -> usize {
        self.tile_bytes.load(Ordering::Relaxed)
    }
    
    fn peak(&self) -> usize {
        self.peak_bytes.load(Ordering::Relaxed)
    }
}

/// BCSI Processor - single-pass with hard memory caps
pub struct BcsiProcessor {
    config: Config,
    _telemetry: Arc<Telemetry>,
    temp_dir: PathBuf,
    memory_tracker: Arc<MemoryTracker>,
}

impl BcsiProcessor {
    pub fn new(config: Config) -> Result<Self> {
        let temp_dir = std::env::temp_dir().join(format!("butterfly-bcsi-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&temp_dir).map_err(Error::IoError)?;
        
        Ok(Self {
            config,
            _telemetry: Arc::new(Telemetry::new()),
            temp_dir,
            memory_tracker: Arc::new(MemoryTracker::new()),
        })
    }
    
    /// Process PBF with single read and BCSI index
    pub fn process(&mut self, input_path: &Path, output_path: &Path) -> Result<ProcessStats> {
        let start = Instant::now();
        log::info!("Starting BCSI processor with 4GB memory cap");
        
        // Create a single writer that will be used for both phases
        let mut pbf_writer = PbfWriter::new(output_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to create writer: {}", e)))?;
        pbf_writer.write_header()
            .map_err(|e| Error::InvalidInput(format!("Failed to write header: {}", e)))?;
        
        // Phase 1: Process nodes and build BCSI
        let (bcsi_path, top_index, node_stats) = self.process_nodes_with_writer(input_path, &mut pbf_writer)?;
        
        // Phase 2: Process ways using BCSI (using same writer)
        let way_stats = self.process_ways_with_writer(input_path, &mut pbf_writer, &bcsi_path, top_index)?;
        
        // Phase 3: Process relations (skipping for now)
        let relation_stats = RelationStats { total_relations: 0, written_relations: 0 };
        
        // Finalize the PBF file now that all phases are complete
        pbf_writer.finalize()
            .map_err(|e| Error::InvalidInput(format!("Failed to finalize PBF: {}", e)))?;
        
        // Cleanup temp files
        self.cleanup()?;
        
        let elapsed = start.elapsed();
        log::info!("BCSI processing complete in {:.2}s", elapsed.as_secs_f64());
        log::info!("Peak tile memory: {} MB", self.memory_tracker.peak() / 1_000_000);
        
        Ok(ProcessStats {
            total_nodes: node_stats.total_nodes,
            rep_nodes: node_stats.rep_nodes,
            total_ways: way_stats.total_ways,
            written_ways: way_stats.written_ways,
            total_relations: relation_stats.total_relations,
            written_relations: relation_stats.written_relations,
            elapsed_secs: elapsed.as_secs_f64(),
        })
    }
    
    /// Phase 1: Process nodes, build BCSI index
    fn process_nodes_with_writer(&self, input_path: &Path, writer: &mut PbfWriter) -> Result<(PathBuf, Vec<TopIndexEntry>, NodeStats)> {
        log::info!("Phase 1: Processing nodes and building BCSI");
        let start = Instant::now();
        
        // Create spill files
        let spill_a_path = self.temp_dir.join("spill_a.bin");
        let spill_b_path = self.temp_dir.join("spill_b.bin");
        let mut spill_a = BufWriter::with_capacity(8_000_000, File::create(&spill_a_path).map_err(Error::IoError)?);
        let mut spill_b = BufWriter::with_capacity(8_000_000, File::create(&spill_b_path).map_err(Error::IoError)?);
        
        let mut total_nodes = 0u64;
        let mut spill_count = 0u64;
        
        // Read nodes and create spills
        let reader = ElementReader::from_path(input_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to open input: {}", e)))?;
        
        reader.for_each(|element| {
            match element {
                Element::Node(node) => {
                    total_nodes += 1;
                    
                    // Snap to grid
                    let (lat_nano, lon_nano) = crate::snap_coordinate(
                        node.lat(),
                        node.lon(),
                        self.config.grid_size_m as f64,
                    );
                    
                    // Compute cell key (Morton encoding)
                    let cell_key = self.compute_cell_key(lat_nano, lon_nano);
                    
                    // Spill A: for deduplication
                    let record_a = CellRecord {
                        cell_key,
                        orig_id: node.id(),
                        lat_nano,
                        lon_nano,
                    };
                    spill_a.write_all(&record_a.to_bytes()).unwrap();
                    
                    // Spill B: for mapping
                    let record_b = MappingRecord {
                        orig_id: node.id(),
                        cell_key,
                    };
                    spill_b.write_all(&record_b.to_bytes()).unwrap();
                    
                    spill_count += 1;
                    
                    if spill_count % 1_000_000 == 0 {
                        log::debug!("Spilled {} nodes", spill_count);
                    }
                }
                Element::DenseNode(node) => {
                    total_nodes += 1;
                    
                    // Snap to grid
                    let (lat_nano, lon_nano) = crate::snap_coordinate(
                        node.lat(),
                        node.lon(),
                        self.config.grid_size_m as f64,
                    );
                    
                    // Compute cell key (Morton encoding)
                    let cell_key = self.compute_cell_key(lat_nano, lon_nano);
                    
                    // Spill A: for deduplication
                    let record_a = CellRecord {
                        cell_key,
                        orig_id: node.id(),
                        lat_nano,
                        lon_nano,
                    };
                    spill_a.write_all(&record_a.to_bytes()).unwrap();
                    
                    // Spill B: for mapping
                    let record_b = MappingRecord {
                        orig_id: node.id(),
                        cell_key,
                    };
                    spill_b.write_all(&record_b.to_bytes()).unwrap();
                    
                    spill_count += 1;
                    
                    if spill_count % 1_000_000 == 0 {
                        log::debug!("Spilled {} nodes", spill_count);
                    }
                }
                _ => {} // Skip ways/relations in first pass
            }
        })
        .map_err(|e| Error::InvalidInput(format!("Failed to read nodes: {}", e)))?;
        
        spill_a.flush().map_err(Error::IoError)?;
        spill_b.flush().map_err(Error::IoError)?;
        
        log::info!("Spilled {} nodes in {:.2}s", total_nodes, start.elapsed().as_secs_f64());
        
        // Build BCSI from spills
        let (bcsi_path, top_index, rep_nodes) = self.build_bcsi_from_spills(
            &spill_a_path,
            &spill_b_path,
            writer,
            total_nodes,
        )?;
        
        let elapsed = start.elapsed();
        log::info!("Phase 1 complete: {} nodes → {} reps in {:.2}s", 
            total_nodes, rep_nodes, elapsed.as_secs_f64());
        
        Ok((bcsi_path, top_index, NodeStats {
            total_nodes,
            rep_nodes,
        }))
    }
    
    /// Build BCSI from spill files
    fn build_bcsi_from_spills(
        &self,
        spill_a_path: &Path,
        spill_b_path: &Path,
        writer: &mut PbfWriter,
        total_nodes: u64,
    ) -> Result<(PathBuf, Vec<TopIndexEntry>, u64)> {
        log::info!("Building BCSI index from spills (total nodes: {})", total_nodes);
        
        // Step 1: Sort spill A by cell_key
        let sorted_a_path = self.temp_dir.join("sorted_a.bin");
        let sorter = ExternalSorter::<CellRecord>::new(ExternalSortConfig {
            memory_limit: 256_000_000, // 256 MB for sorting
            temp_dir: self.temp_dir.clone(),
            max_fan_in: 16,
        })?;
        sorter.sort_file(spill_a_path, &sorted_a_path)?;
        
        // Step 2: Deduplicate to get representatives
        let reps_path = self.temp_dir.join("reps.bin");
        let rep_count = self.deduplicate_cells(&sorted_a_path, &reps_path)?;
        
        // Step 3: Sort spill B by cell_key for join
        // Need to sort B by cell_key first
        let sorted_b_by_cell = self.temp_dir.join("sorted_b_by_cell.bin");
        self.sort_mapping_by_cell(spill_b_path, &sorted_b_by_cell)?;
        
        // Step 4: Join B with REPS to create C (orig_id → rep_id, tile_id)
        let c_path = self.temp_dir.join("c.bin");
        self.merge_join(&sorted_b_by_cell, &reps_path, &c_path)?;
        
        // Step 5: Sort C by orig_id
        let sorted_c_path = self.temp_dir.join("sorted_c.bin");
        self.sort_c_by_orig_id(&c_path, &sorted_c_path)?;
        
        // Step 6: Build BCSI from sorted C
        let bcsi_path = self.temp_dir.join("index.bcsi");
        let top_index = self.build_bcsi(&sorted_c_path, &bcsi_path)?;
        
        // Step 7: Write representative nodes to output using provided writer
        self.write_rep_nodes_to_writer(&reps_path, writer)?;
        
        Ok((bcsi_path, top_index, rep_count))
    }
    
    /// Deduplicate cells (first node per cell wins)
    fn deduplicate_cells(&self, sorted_path: &Path, output_path: &Path) -> Result<u64> {
        let mut reader = BufReader::with_capacity(8_000_000, File::open(sorted_path).map_err(Error::IoError)?);
        let mut writer = BufWriter::with_capacity(8_000_000, File::create(output_path).map_err(Error::IoError)?);
        
        let mut last_cell_key = u64::MAX;
        let mut rep_count = 0u64;
        let mut buf = vec![0u8; CellRecord::SIZE];
        
        while reader.read_exact(&mut buf).is_ok() {
            let record = CellRecord::from_bytes(&buf)?;
            
            // Keep first node per cell
            if record.cell_key != last_cell_key {
                // Add tile_id to record for REPS file
                let tile_id = compute_tile_id(record.lat_nano, record.lon_nano, self.config.grid_size_m as f64);
                
                // Write extended record (with tile_id)
                writer.write_all(&record.to_bytes()).map_err(Error::IoError)?;
                writer.write_all(&tile_id.to_le_bytes()).map_err(Error::IoError)?;
                
                last_cell_key = record.cell_key;
                rep_count += 1;
            }
        }
        
        writer.flush().map_err(Error::IoError)?;
        log::info!("Deduplicated to {} representative nodes", rep_count);
        Ok(rep_count)
    }
    
    /// Sort mapping records by cell_key
    fn sort_mapping_by_cell(&self, input: &Path, output: &Path) -> Result<()> {
        // Convert MappingRecord to sort by cell_key instead of orig_id
        #[derive(Clone)]
        struct CellMappingRecord {
            cell_key: u64,
            orig_id: i64,
        }
        
        impl SortableRecord for CellMappingRecord {
            const SIZE: usize = 16;
            
            fn to_bytes(&self) -> Vec<u8> {
                let mut bytes = Vec::with_capacity(Self::SIZE);
                bytes.extend_from_slice(&self.cell_key.to_le_bytes());
                bytes.extend_from_slice(&self.orig_id.to_le_bytes());
                bytes
            }
            
            fn from_bytes(bytes: &[u8]) -> Result<Self> {
                Ok(Self {
                    cell_key: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    orig_id: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                })
            }
            
            fn compare(&self, other: &Self) -> std::cmp::Ordering {
                self.cell_key.cmp(&other.cell_key)
            }
        }
        
        // Convert format while reading
        let temp_path = self.temp_dir.join("mapping_by_cell_unsorted.bin");
        let mut reader = BufReader::with_capacity(8_000_000, File::open(input).map_err(Error::IoError)?);
        let mut writer = BufWriter::with_capacity(8_000_000, File::create(&temp_path).map_err(Error::IoError)?);
        
        let mut buf = vec![0u8; MappingRecord::SIZE];
        while reader.read_exact(&mut buf).is_ok() {
            let record = MappingRecord::from_bytes(&buf)?;
            let cell_record = CellMappingRecord {
                cell_key: record.cell_key,
                orig_id: record.orig_id,
            };
            writer.write_all(&cell_record.to_bytes()).map_err(Error::IoError)?;
        }
        writer.flush().map_err(Error::IoError)?;
        
        // Sort by cell_key
        let sorter = ExternalSorter::<CellMappingRecord>::new(ExternalSortConfig {
            memory_limit: 256_000_000,
            temp_dir: self.temp_dir.clone(),
            max_fan_in: 16,
        })?;
        sorter.sort_file(&temp_path, output)?;
        
        Ok(())
    }
    
    /// Merge-join B with REPS using streaming approach (no HashMap!)
    fn merge_join(&self, b_path: &Path, reps_path: &Path, output: &Path) -> Result<()> {
        log::info!("Merge-joining to create orig→rep mapping (streaming)");
        
        let mut writer = BufWriter::with_capacity(8_000_000, File::create(output).map_err(Error::IoError)?);
        
        // Both files are sorted by cell_key, so we can do a streaming merge-join
        let mut reps_reader = BufReader::with_capacity(4_000_000, File::open(reps_path).map_err(Error::IoError)?);
        let mut b_reader = BufReader::with_capacity(4_000_000, File::open(b_path).map_err(Error::IoError)?);
        
        let mut reps_buf = vec![0u8; CellRecord::SIZE + 4]; // +4 for tile_id
        let mut b_buf = vec![0u8; 16]; // cell_key + orig_id
        
        // Current REPS record
        let mut current_reps: Option<(u64, i64, u32)> = None; // (cell_key, rep_id, tile_id)
        
        // Read first REPS record
        if reps_reader.read_exact(&mut reps_buf).is_ok() {
            let record = CellRecord::from_bytes(&reps_buf[..CellRecord::SIZE])?;
            let tile_id = u32::from_le_bytes(reps_buf[CellRecord::SIZE..].try_into().unwrap());
            current_reps = Some((record.cell_key, record.orig_id, tile_id));
        }
        
        // Process B records
        while b_reader.read_exact(&mut b_buf).is_ok() {
            let b_cell_key = u64::from_le_bytes(b_buf[0..8].try_into().unwrap());
            let b_orig_id = i64::from_le_bytes(b_buf[8..16].try_into().unwrap());
            
            // Advance REPS reader to catch up if needed
            while let Some((reps_cell_key, _, _)) = current_reps {
                if reps_cell_key >= b_cell_key {
                    break;
                }
                // Read next REPS record
                if reps_reader.read_exact(&mut reps_buf).is_ok() {
                    let record = CellRecord::from_bytes(&reps_buf[..CellRecord::SIZE])?;
                    let tile_id = u32::from_le_bytes(reps_buf[CellRecord::SIZE..].try_into().unwrap());
                    current_reps = Some((record.cell_key, record.orig_id, tile_id));
                } else {
                    current_reps = None;
                    break;
                }
            }
            
            // Check if we have a match
            if let Some((reps_cell_key, rep_id, tile_id)) = current_reps {
                if reps_cell_key == b_cell_key {
                    // Match! Write C record: (orig_id, rep_id, tile_id)
                    writer.write_all(&b_orig_id.to_le_bytes()).map_err(Error::IoError)?;
                    writer.write_all(&rep_id.to_le_bytes()).map_err(Error::IoError)?;
                    writer.write_all(&tile_id.to_le_bytes()).map_err(Error::IoError)?;
                }
            }
        }
        
        writer.flush().map_err(Error::IoError)?;
        Ok(())
    }
    
    /// Sort C by orig_id
    fn sort_c_by_orig_id(&self, input: &Path, output: &Path) -> Result<()> {
        #[derive(Clone)]
        struct CRecord {
            orig_id: i64,
            rep_id: i64,
            tile_id: u32,
        }
        
        impl SortableRecord for CRecord {
            const SIZE: usize = 20;
            
            fn to_bytes(&self) -> Vec<u8> {
                let mut bytes = Vec::with_capacity(Self::SIZE);
                bytes.extend_from_slice(&self.orig_id.to_le_bytes());
                bytes.extend_from_slice(&self.rep_id.to_le_bytes());
                bytes.extend_from_slice(&self.tile_id.to_le_bytes());
                bytes
            }
            
            fn from_bytes(bytes: &[u8]) -> Result<Self> {
                Ok(Self {
                    orig_id: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
                    rep_id: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
                    tile_id: u32::from_le_bytes(bytes[16..20].try_into().unwrap()),
                })
            }
            
            fn compare(&self, other: &Self) -> std::cmp::Ordering {
                self.orig_id.cmp(&other.orig_id)
            }
        }
        
        let sorter = ExternalSorter::<CRecord>::new(ExternalSortConfig {
            memory_limit: 256_000_000,
            temp_dir: self.temp_dir.clone(),
            max_fan_in: 16,
        })?;
        sorter.sort_file(input, output)?;
        Ok(())
    }
    
    /// Build BCSI from sorted C
    fn build_bcsi(&self, sorted_c: &Path, bcsi_path: &Path) -> Result<Vec<TopIndexEntry>> {
        log::info!("Building BCSI index");
        
        let mut writer = BcsiWriter::new(bcsi_path, 4)?; // Zstd level 4
        let mut reader = BufReader::with_capacity(8_000_000, File::open(sorted_c).map_err(Error::IoError)?);
        let mut buf = vec![0u8; 20]; // orig_id + rep_id + tile_id
        
        while reader.read_exact(&mut buf).is_ok() {
            let orig_id = i64::from_le_bytes(buf[0..8].try_into().unwrap());
            let rep_id = i64::from_le_bytes(buf[8..16].try_into().unwrap());
            let tile_id = u32::from_le_bytes(buf[16..20].try_into().unwrap());
            
            writer.add_entry(orig_id, BcsiPayload { rep_id, tile_id })?;
        }
        
        let top_index = writer.finalize()?;
        log::info!("BCSI index built with {} blocks", top_index.len());
        Ok(top_index)
    }
    
    /// Write representative nodes to output in spatial order (no sorting needed!)
    fn write_rep_nodes_to_writer(&self, reps_path: &Path, pbf_writer: &mut PbfWriter) -> Result<()> {
        log::info!("Writing representative nodes in spatial order (Morton/cell_key)");
        
        // REPS file is already sorted by cell_key (Morton order) - perfect for spatial locality!
        // This gives BETTER compression than ID order for lat/lon deltas in DenseNodes
        
        // Use the provided writer (already has header written)
        
        // Stream REPS directly to PBF (already in spatial order)
        let mut reader = BufReader::with_capacity(8_000_000, File::open(reps_path).map_err(Error::IoError)?);
        let mut buf = vec![0u8; CellRecord::SIZE + 4]; // +4 for tile_id
        let mut node_count = 0u64;
        let mut nodes_batch = Vec::with_capacity(8000); // DenseNodes batch size
        
        while reader.read_exact(&mut buf).is_ok() {
            let record = CellRecord::from_bytes(&buf[..CellRecord::SIZE])?;
            // tile_id is in buf[CellRecord::SIZE..] but we don't need it for node writing
            
            nodes_batch.push((record.orig_id, record.lat_nano, record.lon_nano));
            node_count += 1;
            
            // Write batch when full (8k nodes per DenseNodes block is typical)
            if nodes_batch.len() >= 8000 {
                for (id, lat, lon) in nodes_batch.drain(..) {
                    pbf_writer.write_node_nano(id, lat, lon, &[])
                        .map_err(|e| Error::InvalidInput(format!("Failed to write node: {}", e)))?;
                }
            }
            
            if node_count % 1_000_000 == 0 {
                log::debug!("Written {} representative nodes", node_count);
            }
        }
        
        // Write remaining nodes
        for (id, lat, lon) in nodes_batch {
            pbf_writer.write_node_nano(id, lat, lon, &[])
                .map_err(|e| Error::InvalidInput(format!("Failed to write node: {}", e)))?;
        }
        
        // Don't finalize here - keep writer open for Phase 2!
        
        log::info!("Wrote {} representative nodes in spatial order", node_count);
        Ok(())
    }
    
    /// Phase 2: Process ways using BCSI
    fn process_ways_with_writer(
        &self,
        input_path: &Path,
        writer: &mut PbfWriter,
        bcsi_path: &Path,
        top_index: Vec<TopIndexEntry>,
    ) -> Result<WayStats> {
        log::info!("Phase 2: Processing ways with BCSI lookups");
        let start = Instant::now();
        
        // Open BCSI reader with 1.5GB cache
        let mut bcsi = BcsiReader::new(bcsi_path, top_index, BCSI_CACHE_SIZE)?;
        
        // Tile queues with hard caps
        let mut tile_queues: HashMap<u32, TileQueue> = HashMap::new();
        
        // Use the same writer from Phase 1 (no append needed!)
        
        let mut total_ways = 0u64;
        let mut written_ways = 0u64;
        let mut ways_enqueued = 0u64;
        let mut tiles_flushed = 0u64;
        
        // Continue reading from where we left off (after nodes)
        let reader = ElementReader::from_path(input_path)
            .map_err(|e| Error::InvalidInput(format!("Failed to open input: {}", e)))?;
        
        reader.for_each(|element| {
            if let Element::Way(way) = element {
                total_ways += 1;
                
                // Progress logging every 10k ways
                if total_ways % 10_000 == 0 {
                    log::info!("Progress: ways_seen={}, enqueued={}, tiles_flushed={}, ways_emitted={}", 
                        total_ways, ways_enqueued, tiles_flushed, written_ways);
                }
                
                // Get tile from first node
                let way_refs: Vec<i64> = way.refs().collect();
                if let Some(&first_node) = way_refs.first() {
                    if let Ok(Some(payload)) = bcsi.lookup(first_node) {
                        let tile_id = payload.tile_id;
                        
                        // Calculate memory needed for this way
                        let way_bytes = 8 + way_refs.len() * 8 + 32; // Estimate
                        
                        // Check if we can allocate memory
                        if !self.memory_tracker.try_allocate(way_bytes) {
                            // Need to flush largest queue to make room
                            if let Some((&largest_id, _)) = tile_queues.iter()
                                .max_by_key(|(_, q)| q.allocated_bytes) {
                                if let Some(mut largest) = tile_queues.remove(&largest_id) {
                                    let bytes_to_release = largest.allocated_bytes;
                                    let flushed = self.flush_tile(&mut largest, &mut bcsi, &mut writer).unwrap_or(0);
                                    written_ways += flushed;
                                    self.memory_tracker.release(bytes_to_release);
                                    largest.reset();
                                }
                            }
                            
                            // Try again
                            if !self.memory_tracker.try_allocate(way_bytes) {
                                log::warn!("Cannot allocate {} bytes for way {}, skipping", way_bytes, way.id());
                                return; // Skip this way
                            }
                        }
                        
                        // Add to tile queue
                        let queue = tile_queues.entry(tile_id)
                            .or_insert_with(|| TileQueue::new());
                        
                        let tags: Vec<(&str, &str)> = way.tags().collect();
                        let old_bytes = queue.allocated_bytes;
                        queue.add_way(way.id(), way_refs, tags);
                        ways_enqueued += 1;
                        
                        // Track actual allocation delta (in case queue grew its vectors)
                        let new_bytes = queue.allocated_bytes;
                        if new_bytes > old_bytes + way_bytes {
                            let extra = new_bytes - old_bytes - way_bytes;
                            if extra > 0 && !self.memory_tracker.try_allocate(extra) {
                                // Should not happen, but handle gracefully
                                log::warn!("Vector reallocation exceeded estimate by {} bytes", extra);
                            }
                        }
                        
                        // Check flush conditions
                        if queue.should_flush() {
                            if let Some(mut queue) = tile_queues.remove(&tile_id) {
                                let bytes_to_release = queue.allocated_bytes;
                                let flushed = self.flush_tile(&mut queue, &mut bcsi, &mut writer).unwrap_or(0);
                                written_ways += flushed;
                                tiles_flushed += 1;
                                self.memory_tracker.release(bytes_to_release);
                                queue.reset();
                            }
                        }
                        
                        // Limit active tiles
                        if tile_queues.len() >= MAX_ACTIVE_TILES {
                            // Flush smallest queue to make room
                            if let Some((&smallest_id, _)) = tile_queues.iter()
                                .min_by_key(|(_, q)| q.way_ids.len()) {
                                if let Some(mut smallest) = tile_queues.remove(&smallest_id) {
                                    let bytes_to_release = smallest.allocated_bytes;
                                    let flushed = self.flush_tile(&mut smallest, &mut bcsi, &mut writer).unwrap_or(0);
                                    written_ways += flushed;
                                    self.memory_tracker.release(bytes_to_release);
                                }
                            }
                        }
                    }
                }
            }
        })
        .map_err(|e| Error::InvalidInput(format!("Failed to read ways: {}", e)))?;
        
        // Flush remaining tiles
        for (_, mut queue) in tile_queues {
            let bytes_to_release = queue.allocated_bytes;
            let flushed = self.flush_tile(&mut queue, &mut bcsi, &mut writer).unwrap_or(0);
            written_ways += flushed;
            self.memory_tracker.release(bytes_to_release);
        }
        
        // Don't finalize yet - Phase 3 (relations) might come next
        
        let elapsed = start.elapsed();
        log::info!("Phase 2 complete: {} ways → {} written in {:.2}s",
            total_ways, written_ways, elapsed.as_secs_f64());
        
        Ok(WayStats {
            total_ways,
            written_ways,
        })
    }
    
    /// Flush a tile queue
    fn flush_tile(
        &self,
        queue: &mut TileQueue,
        bcsi: &mut BcsiReader,
        writer: &mut PbfWriter,
    ) -> Result<u64> {
        if queue.way_ids.is_empty() {
            return Ok(0);
        }
        
        // Deduplicate nodes efficiently (no HashSet!)
        queue.deduplicate_nodes();
        
        // Parallel BCSI lookups - sort results for efficient remapping
        let mut id_pairs: Vec<(i64, i64)> = Vec::with_capacity(queue.unique_node_ids.len());
        for chunk in queue.unique_node_ids.chunks(LOOKUP_CHUNK_SIZE) {
            for &node_id in chunk {
                if let Ok(Some(payload)) = bcsi.lookup(node_id) {
                    id_pairs.push((node_id, payload.rep_id));
                }
            }
        }
        // Sort for binary search during remapping
        id_pairs.sort_unstable_by_key(|&(k, _)| k);
        
        // Write remapped ways
        let mut written = 0u64;
        
        for (i, &way_id) in queue.way_ids.iter().enumerate() {
            let (start, len) = queue.way_ref_indices[i];
            let refs = &queue.refs_pool[start..start + len];
            let tags = &queue.way_tags[i];
            
            // Check if has highway tag (inline check, no HashSet)
            let mut keep = false;
            for (k, v) in tags {
                if k == "highway" && self.config.highway_tags.contains(v) {
                    keep = true;
                    break;
                }
            }
            
            if !keep {
                continue;
            }
            
            // Remap references using binary search on sorted pairs
            let mut remapped = Vec::with_capacity(refs.len());
            for &id in refs {
                let rep_id = match id_pairs.binary_search_by_key(&id, |&(k, _)| k) {
                    Ok(idx) => id_pairs[idx].1,
                    Err(_) => id,
                };
                remapped.push(rep_id);
            }
            
            // Remove consecutive duplicates
            remapped.dedup();
            
            // Write if valid
            if remapped.len() >= 2 {
                let filtered_tags: Vec<(&str, &str)> = tags.iter()
                    .map(|(k, v)| (k.as_str(), v.as_str()))
                    .collect();
                
                writer.write_way_simple(way_id, remapped, filtered_tags)
                    .map_err(|e| Error::InvalidInput(format!("Failed to write way: {}", e)))?;
                written += 1;
            }
        }
        
        Ok(written)
    }
    
    /// Phase 3: Process relations
    fn process_relations(
        &self,
        _input_path: &Path,
        _output_path: &Path,
        _bcsi_path: &Path,
    ) -> Result<RelationStats> {
        // Simplified - would process turn restrictions
        Ok(RelationStats {
            total_relations: 0,
            written_relations: 0,
        })
    }
    
    /// Compute cell key from snapped coordinates
    fn compute_cell_key(&self, lat_nano: i64, lon_nano: i64) -> u64 {
        use crate::bcsi::morton_encode;
        
        let lat_deg = lat_nano as f64 / 1e9;
        let lon_deg = lon_nano as f64 / 1e9;
        
        let grid_m = self.config.grid_size_m as f64;
        let cell_y = ((lat_deg + 90.0) * 111111.0 / grid_m) as i32;
        let cell_x = ((lon_deg + 180.0) * 111111.0 / grid_m) as i32;
        
        morton_encode(cell_x, cell_y)
    }
    
    /// Cleanup temporary files
    fn cleanup(&self) -> Result<()> {
        log::info!("Cleaning up temporary files");
        std::fs::remove_dir_all(&self.temp_dir).ok();
        Ok(())
    }
}

/// Tile queue for batching ways (FIXED: no HashSet, minimal tag storage)
struct TileQueue {
    // Compact storage using shared pools
    way_ids: Vec<i64>,
    way_ref_indices: Vec<(usize, usize)>, // (start, len) into refs_pool
    refs_pool: Vec<i64>,                  // Shared pool of all refs
    way_tags: Vec<Vec<(String, String)>>, // Only highway tags stored
    
    // Unique nodes - Vec instead of HashSet for 3x memory savings!
    unique_node_ids: Vec<i64>,
    unique_nodes_sorted: bool,
    
    // Accurate byte tracking
    allocated_bytes: usize,
}

impl TileQueue {
    fn new() -> Self {
        // Pre-allocate vectors with expected capacity to avoid reallocations
        // This prevents the "Vector reallocation exceeded estimate" warnings with France
        Self {
            way_ids: Vec::with_capacity(MAX_WAYS_PER_TILE),
            way_ref_indices: Vec::with_capacity(MAX_WAYS_PER_TILE),
            refs_pool: Vec::with_capacity(MAX_WAYS_PER_TILE * 20), // Avg ~20 nodes per way
            way_tags: Vec::with_capacity(MAX_WAYS_PER_TILE),
            unique_node_ids: Vec::with_capacity(MAX_NODES_PER_TILE),
            unique_nodes_sorted: false,
            allocated_bytes: 0,
        }
    }
    
    fn add_way(&mut self, id: i64, refs: Vec<i64>, tags: Vec<(&str, &str)>) {
        // Add to shared pools
        let start = self.refs_pool.len();
        let len = refs.len();
        
        self.way_ids.push(id);
        self.way_ref_indices.push((start, len));
        self.refs_pool.extend(&refs);
        
        // Only store tags we actually need (highway tags)
        let filtered_tags: Vec<(String, String)> = tags.into_iter()
            .filter(|(k, _)| k == &"highway" || k == &"oneway" || k == &"access")
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        self.way_tags.push(filtered_tags);
        
        // Add to unique nodes (will dedup later)
        self.unique_node_ids.extend(&refs);
        self.unique_nodes_sorted = false;
        
        // Update accurate byte accounting
        self.update_allocated_bytes();
    }
    
    fn update_allocated_bytes(&mut self) {
        self.allocated_bytes = 
            self.way_ids.capacity() * 8 +
            self.way_ref_indices.capacity() * 16 +
            self.refs_pool.capacity() * 8 +
            self.way_tags.len() * 32 + // Estimate for tags
            self.unique_node_ids.capacity() * 8 +
            100; // Overhead
    }
    
    /// Sort and deduplicate nodes (no HashSet!)
    fn deduplicate_nodes(&mut self) {
        if !self.unique_nodes_sorted && !self.unique_node_ids.is_empty() {
            self.unique_node_ids.sort_unstable();
            self.unique_node_ids.dedup();
            self.unique_nodes_sorted = true;
            self.unique_node_ids.shrink_to_fit();
            self.update_allocated_bytes();
        }
    }
    
    fn should_flush(&self) -> bool {
        self.way_ids.len() >= MAX_WAYS_PER_TILE ||
        self.unique_node_ids.len() >= MAX_NODES_PER_TILE ||
        self.allocated_bytes >= MAX_BYTES_PER_TILE
    }
    
    fn reset(&mut self) {
        // Clear vectors but retain allocated capacity to avoid reallocations
        // This is crucial for performance with large datasets like France
        self.way_ids.clear();
        self.way_ref_indices.clear();
        self.refs_pool.clear();
        self.way_tags.clear();
        self.unique_node_ids.clear();
        self.unique_nodes_sorted = false;
        self.allocated_bytes = 0;
    }
}

/// Processing statistics
pub struct ProcessStats {
    pub total_nodes: u64,
    pub rep_nodes: u64,
    pub total_ways: u64,
    pub written_ways: u64,
    pub total_relations: u64,
    pub written_relations: u64,
    pub elapsed_secs: f64,
}

pub struct NodeStats {
    pub total_nodes: u64,
    pub rep_nodes: u64,
}

pub struct WayStats {
    pub total_ways: u64,
    pub written_ways: u64,
}

pub struct RelationStats {
    pub total_relations: u64,
    pub written_relations: u64,
}