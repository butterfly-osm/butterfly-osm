//! Parallel processing with bounded channels for backpressure

use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::Arc;
use std::thread;

/// Work item for parallel processing
pub enum WorkItem<T> {
    Process(T),
    Shutdown,
}

/// Bounded parallel processor with backpressure
pub struct BoundedProcessor<T: Send + 'static, R: Send + 'static> {
    workers: Vec<thread::JoinHandle<()>>,
    sender: Sender<WorkItem<T>>,
    receiver: Receiver<R>,
}

impl<T: Send + 'static, R: Send + 'static> BoundedProcessor<T, R> {
    /// Create a new bounded processor with tiny queue depth for immediate backpressure
    pub fn new<F>(num_workers: usize, queue_depth: usize, processor: F) -> Self
    where
        F: Fn(T) -> Result<R> + Send + Sync + 'static,
    {
        let processor = Arc::new(processor);
        
        // Create bounded channels with tiny depth (2-3 items)
        let (work_tx, work_rx) = bounded::<WorkItem<T>>(queue_depth);
        let (result_tx, result_rx) = bounded::<R>(queue_depth);
        
        let mut workers = Vec::with_capacity(num_workers);
        
        for worker_id in 0..num_workers {
            let work_rx = work_rx.clone();
            let result_tx = result_tx.clone();
            let processor = Arc::clone(&processor);
            
            let handle = thread::spawn(move || {
                loop {
                    match work_rx.recv() {
                        Ok(WorkItem::Process(item)) => {
                            match processor(item) {
                                Ok(result) => {
                                    if result_tx.send(result).is_err() {
                                        break; // Receiver dropped
                                    }
                                }
                                Err(e) => {
                                    log::error!("Worker {} processing error: {}", worker_id, e);
                                }
                            }
                        }
                        Ok(WorkItem::Shutdown) | Err(_) => {
                            break;
                        }
                    }
                }
                log::debug!("Worker {} shutting down", worker_id);
            });
            
            workers.push(handle);
        }
        
        Self {
            workers,
            sender: work_tx,
            receiver: result_rx,
        }
    }
    
    /// Submit work item (blocks if queue is full)
    pub fn submit(&self, item: T) -> Result<()> {
        self.sender.send(WorkItem::Process(item))
            .map_err(|_| anyhow::anyhow!("Failed to submit work"))
    }
    
    /// Try to submit work item (non-blocking)
    pub fn try_submit(&self, item: T) -> Result<bool> {
        match self.sender.try_send(WorkItem::Process(item)) {
            Ok(_) => Ok(true),
            Err(crossbeam_channel::TrySendError::Full(_)) => Ok(false),
            Err(_) => Err(anyhow::anyhow!("Channel disconnected")),
        }
    }
    
    /// Receive a result (blocks)
    pub fn receive(&self) -> Result<R> {
        self.receiver.recv()
            .map_err(|_| anyhow::anyhow!("No more results"))
    }
    
    /// Try to receive a result (non-blocking)
    pub fn try_receive(&self) -> Option<R> {
        self.receiver.try_recv().ok()
    }
    
    /// Shutdown all workers
    pub fn shutdown(self) -> Result<Vec<R>> {
        // Send shutdown signal to all workers
        for _ in 0..self.workers.len() {
            let _ = self.sender.send(WorkItem::Shutdown);
        }
        
        // Collect remaining results
        let mut results = Vec::new();
        while let Ok(result) = self.receiver.try_recv() {
            results.push(result);
        }
        
        // Wait for workers to finish
        for worker in self.workers {
            let _ = worker.join();
        }
        
        Ok(results)
    }
}

/// Simple batch processor with bounded depth
pub struct BatchProcessor {
    max_batch_size: usize,
    queue_depth: usize,
}

impl BatchProcessor {
    pub fn new(max_batch_size: usize) -> Self {
        Self {
            max_batch_size,
            queue_depth: 2, // Tiny queue depth for immediate backpressure
        }
    }
    
    /// Process items in batches with backpressure
    pub fn process_batched<T, R, F>(
        &self,
        items: Vec<T>,
        processor: F,
    ) -> Result<Vec<R>>
    where
        T: Send + 'static,
        R: Send + 'static,
        F: Fn(Vec<T>) -> Result<Vec<R>> + Send + Sync + 'static,
    {
        let num_workers = num_cpus::get().min(4); // Cap at 4 workers
        let bounded_proc = BoundedProcessor::new(num_workers, self.queue_depth, processor);
        
        let mut results = Vec::new();
        let mut batch = Vec::with_capacity(self.max_batch_size);
        
        for item in items {
            batch.push(item);
            
            if batch.len() >= self.max_batch_size {
                let current_batch = std::mem::replace(&mut batch, Vec::with_capacity(self.max_batch_size));
                bounded_proc.submit(current_batch)?;
                
                // Try to collect results to maintain flow
                while let Some(batch_results) = bounded_proc.try_receive() {
                    results.extend(batch_results);
                }
            }
        }
        
        // Submit final partial batch
        if !batch.is_empty() {
            bounded_proc.submit(batch)?;
        }
        
        // Shutdown and collect remaining results
        let final_results = bounded_proc.shutdown()?;
        for batch_results in final_results {
            results.extend(batch_results);
        }
        
        Ok(results)
    }
}