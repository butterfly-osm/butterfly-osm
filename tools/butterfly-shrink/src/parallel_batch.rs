//! Parallel batch processing with backpressure

use crate::batch::{BatchConfig, WayData};
use crate::db::NodeIndex;
use crate::parallel::BoundedWorkerPool;
use crate::telemetry::Telemetry;
use crate::writer::PbfWriter;
use anyhow::{Context, Result};
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use std::time::Instant;

/// Batch of ways ready for processing
pub struct WayBatchData {
    pub ways: Vec<WayData>,
    pub unique_node_ids: Vec<i64>,
    pub batch_id: u64,
}

/// Result of batch processing
pub struct BatchResult {
    pub batch_id: u64,
    pub ways_written: u64,
    pub mappings: HashMap<i64, i64>,
}

/// Parallel batch processor with backpressure
pub struct ParallelBatchProcessor {
    #[allow(dead_code)]
    config: BatchConfig,
    #[allow(dead_code)]
    telemetry: Telemetry,
    #[allow(dead_code)]
    node_index: Arc<NodeIndex>,
    #[allow(dead_code)]
    writer: Arc<Mutex<PbfWriter>>,
    pool: BoundedWorkerPool<WayBatchData, BatchResult>,
    batch_counter: u64,
    pending_batches: usize,
}

impl ParallelBatchProcessor {
    /// Create a new parallel batch processor
    pub fn new(
        config: BatchConfig,
        telemetry: Telemetry,
        node_index: Arc<NodeIndex>,
        writer: Arc<Mutex<PbfWriter>>,
        num_workers: usize,
    ) -> Result<Self> {
        // Create bounded worker pool with 2-batch depth per worker
        let queue_capacity = num_workers * 2;
        
        let config_clone = config.clone();
        let telemetry_clone = telemetry.clone();
        let node_index_clone = Arc::clone(&node_index);
        let writer_clone = Arc::clone(&writer);
        
        let pool = BoundedWorkerPool::new(
            num_workers,
            queue_capacity,
            move |batch_data: WayBatchData| -> Result<BatchResult> {
                Self::process_batch(
                    batch_data,
                    &config_clone,
                    &telemetry_clone,
                    &node_index_clone,
                    &writer_clone,
                )
            },
        )?;
        
        Ok(Self {
            config,
            telemetry,
            node_index,
            writer,
            pool,
            batch_counter: 0,
            pending_batches: 0,
        })
    }
    
    /// Process a single batch (runs in worker thread)
    fn process_batch(
        mut batch_data: WayBatchData,
        config: &BatchConfig,
        telemetry: &Telemetry,
        node_index: &Arc<NodeIndex>,
        writer: &Arc<Mutex<PbfWriter>>,
    ) -> Result<BatchResult> {
        let start = Instant::now();
        let batch_id = batch_data.batch_id;
        
        // Deduplicate and sort node IDs
        batch_data.unique_node_ids.sort_unstable();
        batch_data.unique_node_ids.dedup();
        
        // Perform MultiGet in chunks
        let mut all_mappings = HashMap::new();
        let chunk_size = config.max_multiget_keys;
        
        for chunk in batch_data.unique_node_ids.chunks(chunk_size) {
            let chunk_mappings = node_index.multi_get(chunk)
                .context("Failed to perform MultiGet on node index")?;
            all_mappings.extend(chunk_mappings);
        }
        
        // Record telemetry
        telemetry.record_multiget(batch_data.unique_node_ids.len(), start.elapsed());
        
        // Process ways and write to PBF
        let mut ways_written = 0u64;
        {
            let mut writer = writer.lock().unwrap();
            
            for way_data in &batch_data.ways {
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
                
                // Skip ways with missing nodes
                if !missing_nodes.is_empty() {
                    log::warn!("Way {} has {} missing node references", 
                        way_data.id, missing_nodes.len());
                    continue;
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
                
                writer.write_way(way_data.id, &remapped_refs, &output_tags)
                    .context("Failed to write way to PBF")?;
                
                ways_written += 1;
            }
        }
        
        // Record batch telemetry
        telemetry.record_batch(batch_data.ways.len(), batch_data.unique_node_ids.len());
        
        log::debug!(
            "Batch {} processed: {} ways ({} written), {} unique nodes in {}ms",
            batch_id,
            batch_data.ways.len(),
            ways_written,
            batch_data.unique_node_ids.len(),
            start.elapsed().as_millis()
        );
        
        Ok(BatchResult {
            batch_id,
            ways_written,
            mappings: all_mappings,
        })
    }
    
    /// Submit a batch for processing (blocks if queue is full)
    pub fn submit_batch(&mut self, ways: Vec<WayData>, unique_node_ids: Vec<i64>) -> Result<()> {
        let batch_data = WayBatchData {
            ways,
            unique_node_ids,
            batch_id: self.batch_counter,
        };
        
        self.batch_counter += 1;
        
        // Submit to pool (blocks if queue is full, providing backpressure)
        self.pool.submit(batch_data)?;
        self.pending_batches += 1;
        
        // Log backpressure status
        let pending_input = self.pool.pending_input();
        let pending_output = self.pool.pending_output();
        
        if pending_input > 0 {
            log::debug!(
                "Backpressure: {} batches in input queue, {} in output queue",
                pending_input, pending_output
            );
        }
        
        Ok(())
    }
    
    /// Try to submit a batch without blocking
    pub fn try_submit_batch(&mut self, ways: Vec<WayData>, unique_node_ids: Vec<i64>) -> Result<bool> {
        let batch_data = WayBatchData {
            ways,
            unique_node_ids,
            batch_id: self.batch_counter,
        };
        
        // Try to submit without blocking
        if self.pool.try_submit(batch_data)? {
            self.batch_counter += 1;
            self.pending_batches += 1;
            Ok(true)
        } else {
            // Queue is full, backpressure applied
            log::debug!("Batch submission blocked - queue full (backpressure)");
            Ok(false)
        }
    }
    
    /// Process completed batches (non-blocking)
    pub fn process_results(&mut self) -> Result<Vec<BatchResult>> {
        let mut results = Vec::new();
        
        // Collect all available results without blocking
        while let Some(result) = self.pool.try_receive()? {
            self.pending_batches = self.pending_batches.saturating_sub(1);
            results.push(result);
        }
        
        Ok(results)
    }
    
    /// Wait for all batches to complete
    pub fn wait_for_completion(&mut self) -> Result<Vec<BatchResult>> {
        let mut all_results = Vec::new();
        
        // Collect all pending results
        while self.pending_batches > 0 {
            let result = self.pool.receive()?;
            self.pending_batches -= 1;
            all_results.push(result);
        }
        
        Ok(all_results)
    }
    
    /// Shutdown the processor
    pub fn shutdown(self) -> Result<()> {
        self.pool.shutdown()
    }
    
    /// Get current backpressure status
    pub fn backpressure_status(&self) -> (usize, usize, usize) {
        (
            self.pool.pending_input(),
            self.pool.pending_output(),
            self.pending_batches,
        )
    }
}