//! External sort with fixed memory buffers and k-way merge
//! 
//! Provides memory-bounded sorting for large datasets that don't fit in RAM.
//! All operations use fixed-size buffers to guarantee memory caps.

use std::cmp::Ordering;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::marker::PhantomData;
use std::path::{Path, PathBuf};
use butterfly_common::{Error, Result};

/// Fixed-size buffer for external sort operations
const SORT_BUFFER_SIZE: usize = 256 * 1024 * 1024; // 256 MB per run
const MERGE_FAN_IN: usize = 16; // Max concurrent merge streams
const READ_BUFFER_SIZE: usize = 8 * 1024 * 1024; // 8 MB per reader

/// External sort configuration with hard memory limits
pub struct ExternalSortConfig {
    /// Maximum memory for in-memory sorting (bytes)
    pub memory_limit: usize,
    /// Directory for temporary files
    pub temp_dir: PathBuf,
    /// Maximum fan-in for k-way merge
    pub max_fan_in: usize,
}

impl Default for ExternalSortConfig {
    fn default() -> Self {
        Self {
            memory_limit: SORT_BUFFER_SIZE,
            temp_dir: std::env::temp_dir().join(format!("butterfly-sort-{}", uuid::Uuid::new_v4())),
            max_fan_in: MERGE_FAN_IN,
        }
    }
}

/// Trait for fixed-size sortable records
pub trait SortableRecord: Sized + Clone {
    /// Size of record in bytes (must be constant)
    const SIZE: usize;
    
    /// Serialize to fixed-size byte array
    fn to_bytes(&self) -> Vec<u8>;
    
    /// Deserialize from byte slice
    fn from_bytes(bytes: &[u8]) -> Result<Self>;
    
    /// Comparison function for sorting
    fn compare(&self, other: &Self) -> Ordering;
}

/// External sorter with fixed memory guarantees
pub struct ExternalSorter<T: SortableRecord> {
    config: ExternalSortConfig,
    _phantom: PhantomData<T>,
}

impl<T: SortableRecord> ExternalSorter<T> {
    pub fn new(config: ExternalSortConfig) -> Result<Self> {
        // Create temp directory
        std::fs::create_dir_all(&config.temp_dir)
            .map_err(|e| Error::IoError(e))?;
        
        Ok(Self {
            config,
            _phantom: PhantomData,
        })
    }
    
    /// Sort a file of fixed-size records with bounded memory
    pub fn sort_file(&self, input: &Path, output: &Path) -> Result<()> {
        // Phase 1: Create sorted runs
        let run_files = self.create_sorted_runs(input)?;
        
        // Phase 2: K-way merge runs into output
        self.merge_runs(&run_files, output)?;
        
        // Cleanup temp files
        for run_file in &run_files {
            std::fs::remove_file(run_file).ok();
        }
        
        Ok(())
    }
    
    /// Create sorted runs from input file
    fn create_sorted_runs(&self, input: &Path) -> Result<Vec<PathBuf>> {
        let mut run_files = Vec::new();
        let mut run_index = 0;
        
        let file = File::open(input).map_err(Error::IoError)?;
        let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE, file);
        
        // Calculate records per run based on memory limit
        let records_per_run = self.config.memory_limit / T::SIZE;
        let mut buffer = Vec::with_capacity(records_per_run);
        let mut read_buffer = vec![0u8; T::SIZE];
        
        loop {
            // Fill buffer up to memory limit
            buffer.clear();
            for _ in 0..records_per_run {
                match reader.read_exact(&mut read_buffer) {
                    Ok(()) => {
                        let record = T::from_bytes(&read_buffer)?;
                        buffer.push(record);
                    }
                    Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => {
                        break;
                    }
                    Err(e) => return Err(Error::IoError(e)),
                }
            }
            
            if buffer.is_empty() {
                break;
            }
            
            // Sort buffer in memory
            buffer.sort_by(|a, b| a.compare(b));
            
            // Write sorted run to temp file
            let run_path = self.config.temp_dir.join(format!("run_{:06}.tmp", run_index));
            let mut writer = BufWriter::with_capacity(
                READ_BUFFER_SIZE,
                File::create(&run_path).map_err(Error::IoError)?
            );
            
            for record in &buffer {
                writer.write_all(&record.to_bytes()).map_err(Error::IoError)?;
            }
            writer.flush().map_err(Error::IoError)?;
            
            run_files.push(run_path);
            run_index += 1;
            
            log::debug!("Created sorted run {} with {} records", run_index, buffer.len());
        }
        
        Ok(run_files)
    }
    
    /// K-way merge sorted runs into output file
    fn merge_runs(&self, run_files: &[PathBuf], output: &Path) -> Result<()> {
        if run_files.is_empty() {
            // Create empty output file
            File::create(output).map_err(Error::IoError)?;
            return Ok(());
        }
        
        if run_files.len() == 1 {
            // Single run, just rename
            std::fs::rename(&run_files[0], output).map_err(Error::IoError)?;
            return Ok(());
        }
        
        // Multi-way merge with bounded fan-in
        let mut current_runs = run_files.to_vec();
        let mut merge_pass = 0;
        
        while current_runs.len() > 1 {
            let mut next_runs = Vec::new();
            
            // Process runs in groups of max_fan_in
            for chunk in current_runs.chunks(self.config.max_fan_in) {
                let output_path = if chunk.len() == current_runs.len() && current_runs.len() <= self.config.max_fan_in {
                    // Final merge
                    output.to_path_buf()
                } else {
                    // Intermediate merge
                    let path = self.config.temp_dir.join(format!("merge_{}_{}.tmp", merge_pass, next_runs.len()));
                    next_runs.push(path.clone());
                    path
                };
                
                self.k_way_merge(chunk, &output_path)?;
            }
            
            // Clean up merged runs
            for run in &current_runs {
                if run != output {
                    std::fs::remove_file(run).ok();
                }
            }
            
            current_runs = next_runs;
            merge_pass += 1;
        }
        
        Ok(())
    }
    
    /// K-way merge implementation with fixed buffers
    fn k_way_merge(&self, inputs: &[PathBuf], output: &Path) -> Result<()> {
        use std::collections::BinaryHeap;
        
        #[derive(Clone)]
        struct HeapEntry<T: SortableRecord> {
            record: T,
            source_idx: usize,
        }
        
        impl<T: SortableRecord> Ord for HeapEntry<T> {
            fn cmp(&self, other: &Self) -> Ordering {
                // Reverse for min-heap
                other.record.compare(&self.record)
                    .then_with(|| other.source_idx.cmp(&self.source_idx))
            }
        }
        
        impl<T: SortableRecord> PartialOrd for HeapEntry<T> {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                Some(self.cmp(other))
            }
        }
        
        impl<T: SortableRecord> Eq for HeapEntry<T> {}
        impl<T: SortableRecord> PartialEq for HeapEntry<T> {
            fn eq(&self, other: &Self) -> bool {
                self.record.compare(&other.record) == Ordering::Equal
            }
        }
        
        // Open all input files with bounded buffers
        let mut readers: Vec<BufReader<File>> = Vec::new();
        let mut heap = BinaryHeap::new();
        
        for (idx, path) in inputs.iter().enumerate() {
            let file = File::open(path).map_err(Error::IoError)?;
            let mut reader = BufReader::with_capacity(READ_BUFFER_SIZE / inputs.len(), file);
            
            // Read first record from each file
            let mut buf = vec![0u8; T::SIZE];
            if reader.read_exact(&mut buf).is_ok() {
                let record = T::from_bytes(&buf)?;
                heap.push(HeapEntry { record, source_idx: idx });
            }
            
            readers.push(reader);
        }
        
        // Open output with bounded buffer
        let mut writer = BufWriter::with_capacity(
            READ_BUFFER_SIZE,
            File::create(output).map_err(Error::IoError)?
        );
        
        // Merge loop
        let mut read_buf = vec![0u8; T::SIZE];
        while let Some(HeapEntry { record, source_idx }) = heap.pop() {
            // Write minimum record
            writer.write_all(&record.to_bytes()).map_err(Error::IoError)?;
            
            // Read next record from same source
            if readers[source_idx].read_exact(&mut read_buf).is_ok() {
                let next_record = T::from_bytes(&read_buf)?;
                heap.push(HeapEntry { 
                    record: next_record, 
                    source_idx 
                });
            }
        }
        
        writer.flush().map_err(Error::IoError)?;
        Ok(())
    }
}

// Example record types for node processing

/// Record A: for deduplication by cell (LEGACY - kept for compatibility)
#[derive(Clone, Debug)]
pub struct CellRecord {
    pub cell_key: u64,
    pub orig_id: i64,
    pub lat_nano: i64,
    pub lon_nano: i64,
}

impl SortableRecord for CellRecord {
    const SIZE: usize = 32; // 4 * 8 bytes
    
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SIZE);
        bytes.extend_from_slice(&self.cell_key.to_le_bytes());
        bytes.extend_from_slice(&self.orig_id.to_le_bytes());
        bytes.extend_from_slice(&self.lat_nano.to_le_bytes());
        bytes.extend_from_slice(&self.lon_nano.to_le_bytes());
        bytes
    }
    
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::SIZE {
            return Err(Error::InvalidInput("Invalid record size".to_string()));
        }
        Ok(Self {
            cell_key: u64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            orig_id: i64::from_le_bytes(bytes[8..16].try_into().unwrap()),
            lat_nano: i64::from_le_bytes(bytes[16..24].try_into().unwrap()),
            lon_nano: i64::from_le_bytes(bytes[24..32].try_into().unwrap()),
        })
    }
    
    fn compare(&self, other: &Self) -> Ordering {
        self.cell_key.cmp(&other.cell_key)
            .then_with(|| self.orig_id.cmp(&other.orig_id))
    }
}

/// Record B: for mapping orig_id to cell
#[derive(Clone, Debug)]
pub struct MappingRecord {
    pub orig_id: i64,
    pub cell_key: u64,
}

impl SortableRecord for MappingRecord {
    const SIZE: usize = 16; // 2 * 8 bytes
    
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(Self::SIZE);
        bytes.extend_from_slice(&self.orig_id.to_le_bytes());
        bytes.extend_from_slice(&self.cell_key.to_le_bytes());
        bytes
    }
    
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() != Self::SIZE {
            return Err(Error::InvalidInput("Invalid record size".to_string()));
        }
        Ok(Self {
            orig_id: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            cell_key: u64::from_le_bytes(bytes[8..16].try_into().unwrap()),
        })
    }
    
    fn compare(&self, other: &Self) -> Ordering {
        self.orig_id.cmp(&other.orig_id)
    }
}

