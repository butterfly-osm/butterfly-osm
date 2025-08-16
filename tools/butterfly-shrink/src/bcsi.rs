//! Block-Compressed Sorted Index (BCSI)
//! 
//! A read-only, compressed index optimized for point lookups with tile-based locality.
//! Fixed memory usage with hard caps, no unbounded growth.

use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write, Seek, SeekFrom};
use std::path::Path;
use std::sync::Arc;
use butterfly_common::{Error, Result};
use zstd;

/// Fixed block size (number of entries per block)
pub const BLOCK_ENTRIES: usize = 65536; // 64K entries per block

/// BCSI payload: rep_id (8 bytes) + tile_id (4 bytes)
#[derive(Clone, Copy, Debug)]
pub struct BcsiPayload {
    pub rep_id: i64,
    pub tile_id: u32,
}

impl BcsiPayload {
    const SIZE: usize = 12;
    
    fn to_bytes(&self) -> [u8; Self::SIZE] {
        let mut bytes = [0u8; Self::SIZE];
        bytes[0..8].copy_from_slice(&self.rep_id.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.tile_id.to_le_bytes());
        bytes
    }
    
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < Self::SIZE {
            return Err(Error::InvalidInput("Invalid payload size".to_string()));
        }
        Ok(Self {
            rep_id: i64::from_le_bytes(bytes[0..8].try_into().unwrap()),
            tile_id: u32::from_le_bytes(bytes[8..12].try_into().unwrap()),
        })
    }
}

/// Top-level index entry
#[derive(Clone, Debug)]
pub struct TopIndexEntry {
    pub first_key: i64,    // First orig_node_id in block
    pub file_offset: u64,  // Byte offset in BCSI file
}

/// BCSI Writer - builds the index from sorted input
pub struct BcsiWriter {
    file: BufWriter<File>,
    top_index: Vec<TopIndexEntry>,
    current_block: Vec<(i64, BcsiPayload)>,
    current_offset: u64,
    compression_level: i32,
}

impl BcsiWriter {
    pub fn new(path: &Path, compression_level: i32) -> Result<Self> {
        let file = File::create(path).map_err(Error::IoError)?;
        Ok(Self {
            file: BufWriter::with_capacity(8 * 1024 * 1024, file), // 8MB buffer
            top_index: Vec::new(),
            current_block: Vec::with_capacity(BLOCK_ENTRIES),
            current_offset: 0,
            compression_level,
        })
    }
    
    /// Add an entry (must be called with sorted orig_node_ids)
    pub fn add_entry(&mut self, orig_id: i64, payload: BcsiPayload) -> Result<()> {
        // Verify sorted order
        if let Some((last_id, _)) = self.current_block.last() {
            if orig_id <= *last_id {
                return Err(Error::InvalidInput(format!(
                    "BCSI entries must be sorted: {} <= {}",
                    orig_id, last_id
                )));
            }
        }
        
        self.current_block.push((orig_id, payload));
        
        // Flush block when full
        if self.current_block.len() >= BLOCK_ENTRIES {
            self.flush_block()?;
        }
        
        Ok(())
    }
    
    /// Flush current block to disk (compressed)
    fn flush_block(&mut self) -> Result<()> {
        if self.current_block.is_empty() {
            return Ok(());
        }
        
        // Record first key and offset for top index
        let first_key = self.current_block[0].0;
        self.top_index.push(TopIndexEntry {
            first_key,
            file_offset: self.current_offset,
        });
        
        // Delta-encode keys and pack into bytes
        let mut encoded = Vec::new();
        let mut prev_key = 0i64;
        
        for (key, payload) in &self.current_block {
            // Delta encode key
            let delta = *key - prev_key;
            prev_key = *key;
            
            // Varint encode delta
            Self::write_varint(&mut encoded, delta as u64)?;
            
            // Write fixed-size payload
            encoded.extend_from_slice(&payload.to_bytes());
        }
        
        // Compress block with zstd
        let compressed = zstd::encode_all(&encoded[..], self.compression_level)
            .map_err(|e| Error::InvalidInput(format!("Compression failed: {}", e)))?;
        
        // Write block size and compressed data
        let block_size = compressed.len() as u32;
        self.file.write_all(&block_size.to_le_bytes()).map_err(Error::IoError)?;
        self.file.write_all(&compressed).map_err(Error::IoError)?;
        
        self.current_offset += 4 + block_size as u64;
        self.current_block.clear();
        
        Ok(())
    }
    
    /// Finalize the index and write top index
    pub fn finalize(mut self) -> Result<Vec<TopIndexEntry>> {
        // Flush any remaining entries
        if !self.current_block.is_empty() {
            self.flush_block()?;
        }
        
        self.file.flush().map_err(Error::IoError)?;
        Ok(self.top_index)
    }
    
    /// Write varint-encoded value
    fn write_varint(buf: &mut Vec<u8>, mut value: u64) -> Result<()> {
        while value >= 0x80 {
            buf.push((value as u8) | 0x80);
            value >>= 7;
        }
        buf.push(value as u8);
        Ok(())
    }
}

/// BCSI Reader - provides fast lookups with bounded memory
pub struct BcsiReader {
    file: File,
    top_index: Arc<Vec<TopIndexEntry>>,
    block_cache: BlockCache,
}

impl BcsiReader {
    /// Create reader with specified cache size in bytes
    pub fn new(path: &Path, top_index: Vec<TopIndexEntry>, cache_size_bytes: usize) -> Result<Self> {
        let file = File::open(path).map_err(Error::IoError)?;
        
        // Calculate max cached blocks based on average block size
        // Assume ~500KB average compressed, ~800KB decompressed
        let max_cached_blocks = (cache_size_bytes / (800 * 1024)).max(2);
        
        Ok(Self {
            file,
            top_index: Arc::new(top_index),
            block_cache: BlockCache::new(max_cached_blocks),
        })
    }
    
    /// Lookup orig_node_id → (rep_id, tile_id)
    pub fn lookup(&mut self, orig_id: i64) -> Result<Option<BcsiPayload>> {
        // Binary search top index to find block
        let block_idx = match self.top_index.binary_search_by_key(&orig_id, |e| e.first_key) {
            Ok(idx) => idx,
            Err(idx) => {
                if idx == 0 {
                    return Ok(None); // Key is before first block
                }
                idx - 1
            }
        };
        
        if block_idx >= self.top_index.len() {
            return Ok(None);
        }
        
        // Load block (from cache or disk)
        let block = self.load_block(block_idx)?;
        
        // Binary search within block
        match block.binary_search_by_key(&orig_id, |(k, _)| *k) {
            Ok(idx) => Ok(Some(block[idx].1)),
            Err(_) => Ok(None),
        }
    }
    
    /// Load block from cache or disk
    fn load_block(&mut self, block_idx: usize) -> Result<Arc<Vec<(i64, BcsiPayload)>>> {
        // Check cache first
        if let Some(block) = self.block_cache.get(block_idx) {
            return Ok(block);
        }
        
        // Load from disk
        let entry = &self.top_index[block_idx];
        self.file.seek(SeekFrom::Start(entry.file_offset)).map_err(Error::IoError)?;
        
        // Read block size
        let mut size_buf = [0u8; 4];
        self.file.read_exact(&mut size_buf).map_err(Error::IoError)?;
        let block_size = u32::from_le_bytes(size_buf) as usize;
        
        // Read compressed block
        let mut compressed = vec![0u8; block_size];
        self.file.read_exact(&mut compressed).map_err(Error::IoError)?;
        
        // Decompress
        let decompressed = zstd::decode_all(&compressed[..])
            .map_err(|e| Error::InvalidInput(format!("Decompression failed: {}", e)))?;
        
        // Decode entries
        let mut entries = Vec::with_capacity(BLOCK_ENTRIES);
        let mut cursor = 0;
        let mut prev_key = 0i64;
        
        while cursor < decompressed.len() {
            // Read varint delta
            let (delta, bytes_read) = Self::read_varint(&decompressed[cursor..])?;
            cursor += bytes_read;
            
            // Reconstruct key
            let key = prev_key + delta as i64;
            prev_key = key;
            
            // Read payload
            if cursor + BcsiPayload::SIZE > decompressed.len() {
                break;
            }
            let payload = BcsiPayload::from_bytes(&decompressed[cursor..cursor + BcsiPayload::SIZE])?;
            cursor += BcsiPayload::SIZE;
            
            entries.push((key, payload));
        }
        
        let block = Arc::new(entries);
        self.block_cache.insert(block_idx, block.clone());
        Ok(block)
    }
    
    /// Read varint-encoded value
    fn read_varint(buf: &[u8]) -> Result<(u64, usize)> {
        let mut value = 0u64;
        let mut shift = 0;
        let mut bytes_read = 0;
        
        for &byte in buf {
            bytes_read += 1;
            value |= ((byte & 0x7F) as u64) << shift;
            if byte & 0x80 == 0 {
                return Ok((value, bytes_read));
            }
            shift += 7;
            if shift >= 64 {
                return Err(Error::InvalidInput("Varint too large".to_string()));
            }
        }
        
        Err(Error::InvalidInput("Incomplete varint".to_string()))
    }
}

/// Fixed-size LRU block cache with O(1) lookups
struct BlockCache {
    map: HashMap<usize, Arc<Vec<(i64, BcsiPayload)>>>,
    max_entries: usize,
    next_slot: usize,
    // Track which blocks are in which slots for eviction
    slot_to_block: Vec<Option<usize>>,
}

impl BlockCache {
    fn new(max_entries: usize) -> Self {
        Self {
            map: HashMap::with_capacity(max_entries),
            max_entries,
            next_slot: 0,
            slot_to_block: vec![None; max_entries],
        }
    }
    
    fn get(&self, block_idx: usize) -> Option<Arc<Vec<(i64, BcsiPayload)>>> {
        // O(1) lookup instead of O(n) linear scan!
        self.map.get(&block_idx).cloned()
    }
    
    fn insert(&mut self, block_idx: usize, block: Arc<Vec<(i64, BcsiPayload)>>) {
        // Check if already in cache
        if self.map.contains_key(&block_idx) {
            return;
        }
        
        // Evict old block if necessary
        if let Some(old_block_idx) = self.slot_to_block[self.next_slot] {
            self.map.remove(&old_block_idx);
        }
        
        // Insert new block
        self.map.insert(block_idx, block);
        self.slot_to_block[self.next_slot] = Some(block_idx);
        self.next_slot = (self.next_slot + 1) % self.max_entries;
    }
}

/// Morton encoding for spatial tiles
pub fn morton_encode(x: i32, y: i32) -> u64 {
    let mut result = 0u64;
    for i in 0..32 {
        result |= ((x >> i) & 1) as u64;
        result <<= 1;
        result |= ((y >> i) & 1) as u64;
        if i < 31 {
            result <<= 1;
        }
    }
    result
}

/// Compute tile ID from snapped coordinates
pub fn compute_tile_id(lat_nano: i64, lon_nano: i64, grid_m: f64) -> u32 {
    // Tile size: ~2km, adjusted for grid
    let tile_factor = (2000.0 / grid_m).round().max(1.0) as i64;
    
    // Convert to grid cells
    let lat_deg = lat_nano as f64 / 1e9;
    let lon_deg = lon_nano as f64 / 1e9;
    
    // Grid cell coordinates
    let cell_y = ((lat_deg + 90.0) * 111111.0 / grid_m) as i64;
    let cell_x = ((lon_deg + 180.0) * 111111.0 / grid_m) as i64;
    
    // Tile coordinates
    let tile_y = (cell_y / tile_factor) as i32;
    let tile_x = (cell_x / tile_factor) as i32;
    
    morton_encode(tile_x, tile_y) as u32
}