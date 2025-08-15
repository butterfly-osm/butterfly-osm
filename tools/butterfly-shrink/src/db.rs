//! RocksDB integration for node index

use anyhow::{Context, Result};
use byteorder::{BigEndian, ByteOrder};
use rocksdb::{
    BlockBasedOptions, Cache, ColumnFamilyDescriptor, DBCompressionType, Options, ReadOptions, WriteBatch, DB,
};
use std::path::Path;
use std::collections::HashMap;

pub struct NodeIndex {
    db: DB,
    cell_cf: String,
    node_cf: String,
}

impl NodeIndex {
    pub fn new(path: &Path, cache_mb: usize) -> Result<Self> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        
        // Optimize for point lookups (per plan)
        opts.optimize_for_point_lookup(cache_mb as u64);
        
        // Block-based table options for point lookups
        let cache = Cache::new_lru_cache(cache_mb * 1024 * 1024);
        let mut block_opts = BlockBasedOptions::default();
        block_opts.set_block_cache(&cache);
        block_opts.set_bloom_filter(10.0, false);
        block_opts.set_block_size(8 * 1024); // 8KB blocks for point lookups
        block_opts.set_pin_l0_filter_and_index_blocks_in_cache(true);
        opts.set_block_based_table_factory(&block_opts);
        
        // Write buffer optimization
        opts.set_write_buffer_size(32 * 1024 * 1024); // 32MB
        opts.set_max_write_buffer_number(3);
        
        // Compression settings - use Zstd which is available
        opts.set_compression_type(DBCompressionType::Zstd);
        
        // Level compaction with dynamic level bytes
        opts.set_level_compaction_dynamic_level_bytes(true);
        
        // I/O optimization
        opts.set_bytes_per_sync(1024 * 1024); // 1MB
        
        // Parallelism
        let num_cores = num_cpus::get();
        opts.set_max_background_jobs((num_cores / 2).max(2) as i32);
        opts.increase_parallelism(num_cores as i32);
        
        // Open files optimization
        opts.set_max_open_files(-1);
        
        // Column families
        let node_cf_name = "nodes".to_string();
        let cell_cf_name = "cells".to_string();
        
        let cfs = vec![
            ColumnFamilyDescriptor::new(&node_cf_name, opts.clone()),
            ColumnFamilyDescriptor::new(&cell_cf_name, opts.clone()),
        ];
        
        let db = DB::open_cf_descriptors(&opts, path, cfs).context("Failed to open RocksDB with column families")?;
        
        Ok(Self { 
            db,
            node_cf: node_cf_name,
            cell_cf: cell_cf_name,
        })
    }
    
    /// Store a mapping from original node ID to representative node ID
    pub fn put(&self, original_id: i64, representative_id: i64) -> Result<()> {
        let cf = self.db.cf_handle(&self.node_cf).context("Failed to get node column family")?;
        let key = original_id.to_be_bytes();
        let value = representative_id.to_be_bytes();
        self.db.put_cf(cf, key, value)?;
        Ok(())
    }
    
    /// Store a grid cell mapping
    pub fn put_cell(&self, cell_key: (i64, i64), representative_id: i64) -> Result<()> {
        let cf = self.db.cf_handle(&self.cell_cf).context("Failed to get cell column family")?;
        let key = encode_cell_key(cell_key);
        let value = representative_id.to_be_bytes();
        self.db.put_cf(cf, key, value)?;
        Ok(())
    }
    
    /// Get representative ID for a grid cell
    pub fn get_cell(&self, cell_key: (i64, i64)) -> Result<Option<i64>> {
        let cf = self.db.cf_handle(&self.cell_cf).context("Failed to get cell column family")?;
        let key = encode_cell_key(cell_key);
        match self.db.get_cf(cf, key)? {
            Some(value) => {
                if value.len() == 8 {
                    Ok(Some(BigEndian::read_i64(&value)))
                } else {
                    anyhow::bail!("Invalid cell value size in database")
                }
            }
            None => Ok(None),
        }
    }
    
    /// Store multiple mappings in a batch
    pub fn put_batch(&self, mappings: &[(i64, i64)]) -> Result<()> {
        let cf = self.db.cf_handle(&self.node_cf).context("Failed to get node column family")?;
        let mut batch = WriteBatch::default();
        for (original_id, representative_id) in mappings {
            batch.put_cf(
                cf,
                original_id.to_be_bytes(),
                representative_id.to_be_bytes(),
            );
        }
        self.db.write(batch)?;
        Ok(())
    }
    
    /// Get the representative node ID for an original node ID
    pub fn get(&self, original_id: i64) -> Result<Option<i64>> {
        let cf = self.db.cf_handle(&self.node_cf).context("Failed to get node column family")?;
        let key = original_id.to_be_bytes();
        match self.db.get_cf(cf, key)? {
            Some(value) => {
                if value.len() == 8 {
                    Ok(Some(BigEndian::read_i64(&value)))
                } else {
                    anyhow::bail!("Invalid value size in database")
                }
            }
            None => Ok(None),
        }
    }

    /// Get multiple node mappings using batched MultiGet
    pub fn multi_get(&self, original_ids: &[i64]) -> Result<HashMap<i64, i64>> {
        self.multi_get_with_readahead(original_ids, 4)
    }
    
    /// Get multiple node mappings with configurable readahead
    pub fn multi_get_with_readahead(&self, original_ids: &[i64], readahead_mb: usize) -> Result<HashMap<i64, i64>> {
        if original_ids.is_empty() {
            return Ok(HashMap::new());
        }

        // Prepare keys for MultiGet
        let keys: Vec<Vec<u8>> = original_ids
            .iter()
            .map(|id| id.to_be_bytes().to_vec())
            .collect();

        // Create read options optimized for batch reads
        let mut read_opts = ReadOptions::default();
        read_opts.set_readahead_size(readahead_mb * 1024 * 1024); // Configurable readahead
        read_opts.fill_cache(true);

        // Get the node column family handle
        let cf = self.db.cf_handle(&self.node_cf).context("Failed to get node column family")?;
        
        // Perform MultiGet with column family
        let key_refs: Vec<&[u8]> = keys.iter().map(|k| k.as_slice()).collect();
        let cf_refs = vec![cf; key_refs.len()];
        let results = self.db.multi_get_cf_opt(cf_refs.into_iter().zip(key_refs), &read_opts);

        // Process results
        let mut mappings = HashMap::with_capacity(original_ids.len());
        
        for (i, result) in results.into_iter().enumerate() {
            match result? {
                Some(value) => {
                    if value.len() == 8 {
                        let representative_id = BigEndian::read_i64(&value);
                        mappings.insert(original_ids[i], representative_id);
                    } else {
                        anyhow::bail!("Invalid value size in database for key {}", original_ids[i]);
                    }
                }
                None => {
                    // Node not found - this should not happen in normal operation
                    // but we'll handle it gracefully
                    log::warn!("Node {} not found in index", original_ids[i]);
                }
            }
        }

        Ok(mappings)
    }
    
    /// Check if a node exists in the index
    pub fn contains(&self, original_id: i64) -> Result<bool> {
        let cf = self.db.cf_handle(&self.node_cf).context("Failed to get node column family")?;
        let key = original_id.to_be_bytes();
        self.db.get_cf(cf, key).map(|v| v.is_some())
            .context("Failed to check node existence")
    }
    
    /// Compact the database for optimal read performance
    pub fn compact_for_reads(&self) -> Result<()> {
        log::info!("Compacting RocksDB for optimal read performance...");
        
        // Flush all memtables to SST files
        self.db.flush()?;
        
        // Compact all levels to optimize SST layout
        self.db.compact_range::<&[u8], &[u8]>(None, None);
        
        log::info!("RocksDB compaction completed");
        Ok(())
    }
    
    /// Disable WAL for write-heavy phases
    pub fn set_disable_wal(&self, _disable: bool) -> Result<()> {
        // This would require write options to be passed to individual operations
        // For now, we'll configure this at open time
        Ok(())
    }
    
    /// Get database statistics
    pub fn stats(&self) -> String {
        self.db
            .property_value("rocksdb.stats")
            .ok()
            .flatten()
            .unwrap_or_else(|| "No stats available".to_string())
    }
    
    /// Get specific database properties for telemetry
    pub fn get_property(&self, name: &str) -> Option<u64> {
        self.db
            .property_value(name)
            .ok()
            .flatten()
            .and_then(|s| s.parse().ok())
    }
}

/// Encode a cell key (lat_cell, lon_cell) as a byte array for RocksDB storage
fn encode_cell_key(cell_key: (i64, i64)) -> Vec<u8> {
    let mut key = Vec::with_capacity(16);
    key.extend_from_slice(&cell_key.0.to_be_bytes());
    key.extend_from_slice(&cell_key.1.to_be_bytes());
    key
}