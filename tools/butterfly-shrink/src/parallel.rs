//! Parallel processing infrastructure

use anyhow::Result;
use crossbeam_channel::{bounded, Receiver, Sender};
use std::sync::Arc;
use std::thread;

/// Tagged element for order preservation
#[derive(Debug, Clone)]
pub struct TaggedElement<T> {
    pub sequence: u64,
    pub element: T,
}

/// Parallel processing pipeline
pub struct Pipeline<I, O> {
    input_tx: Sender<TaggedElement<I>>,
    output_rx: Receiver<TaggedElement<O>>,
    workers: Vec<thread::JoinHandle<()>>,
}

impl<I, O> Pipeline<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    /// Create a new pipeline with the specified number of workers
    pub fn new<F>(num_workers: usize, buffer_size: usize, processor: F) -> Self
    where
        F: Fn(I) -> Result<O> + Send + Sync + 'static,
    {
        let (input_tx, input_rx): (Sender<TaggedElement<I>>, Receiver<TaggedElement<I>>) = bounded(buffer_size);
        let (output_tx, output_rx): (Sender<TaggedElement<O>>, Receiver<TaggedElement<O>>) = bounded(buffer_size);
        
        let processor = Arc::new(processor);
        let mut workers = Vec::with_capacity(num_workers);
        
        for _ in 0..num_workers {
            let input_rx = input_rx.clone();
            let output_tx = output_tx.clone();
            let processor = Arc::clone(&processor);
            
            let handle = thread::spawn(move || {
                while let Ok(tagged) = input_rx.recv() {
                    let element: I = tagged.element;
                    match processor(element) {
                        Ok(result) => {
                            let output = TaggedElement {
                                sequence: tagged.sequence,
                                element: result,
                            };
                            if output_tx.send(output).is_err() {
                                break; // Output channel closed
                            }
                        }
                        Err(e) => {
                            log::error!("Processing error: {}", e);
                        }
                    }
                }
            });
            
            workers.push(handle);
        }
        
        Self {
            input_tx,
            output_rx,
            workers,
        }
    }
    
    /// Send an element to be processed
    pub fn send(&self, sequence: u64, element: I) -> Result<()> {
        let tagged = TaggedElement { sequence, element };
        self.input_tx
            .send(tagged)
            .map_err(|e| anyhow::anyhow!("Failed to send element: {}", e))
    }
    
    /// Receive a processed element
    pub fn recv(&self) -> Result<TaggedElement<O>> {
        self.output_rx
            .recv()
            .map_err(|e| anyhow::anyhow!("Failed to receive element: {}", e))
    }
    
    /// Try to receive a processed element without blocking
    pub fn try_recv(&self) -> Option<TaggedElement<O>> {
        self.output_rx.try_recv().ok()
    }
    
    /// Shutdown the pipeline gracefully
    pub fn shutdown(self) {
        drop(self.input_tx); // Close input channel
        for worker in self.workers {
            let _ = worker.join();
        }
    }
}

/// Reordering buffer for maintaining element order
pub struct ReorderBuffer<T> {
    buffer: Vec<Option<T>>,
    next_sequence: u64,
    window_size: usize,
}

impl<T> ReorderBuffer<T> {
    pub fn new(window_size: usize) -> Self {
        let mut buffer = Vec::with_capacity(window_size);
        for _ in 0..window_size {
            buffer.push(None);
        }
        Self {
            buffer,
            next_sequence: 0,
            window_size,
        }
    }
    
    /// Add an element to the buffer
    pub fn add(&mut self, sequence: u64, element: T) -> Vec<T> {
        if sequence < self.next_sequence {
            // Already processed, ignore
            return Vec::new();
        }
        
        let offset = (sequence - self.next_sequence) as usize;
        if offset >= self.window_size {
            log::warn!("Reorder buffer overflow: sequence {} too far ahead", sequence);
            return Vec::new();
        }
        
        self.buffer[offset] = Some(element);
        
        // Collect ready elements
        let mut ready = Vec::new();
        while !self.buffer.is_empty() && self.buffer[0].is_some() {
            ready.push(self.buffer.remove(0).unwrap());
            self.buffer.push(None);
            self.next_sequence += 1;
        }
        
        ready
    }
    
    /// Flush remaining elements (for shutdown)
    pub fn flush(self) -> Vec<T> {
        self.buffer
            .into_iter()
            .flatten()
            .collect()
    }
}

/// Simple bounded worker pool for way batch processing
pub struct BoundedWorkerPool<I, O> {
    input_tx: Sender<WorkItem<I>>,
    output_rx: Receiver<Result<O>>,
    _workers: Vec<thread::JoinHandle<()>>,
}

#[derive(Debug)]
enum WorkItem<T> {
    Process(T),
    Shutdown,
}

impl<I, O> BoundedWorkerPool<I, O>
where
    I: Send + 'static,
    O: Send + 'static,
{
    /// Create a bounded worker pool with fixed queue sizes
    pub fn new<F>(num_workers: usize, queue_capacity: usize, processor: F) -> Result<Self>
    where
        F: Fn(I) -> Result<O> + Send + Sync + 'static,
    {
        let (input_tx, input_rx) = bounded(queue_capacity);
        let (output_tx, output_rx) = bounded(queue_capacity);
        
        let processor = Arc::new(processor);
        let mut workers = Vec::with_capacity(num_workers);
        
        for worker_id in 0..num_workers {
            let input_rx = input_rx.clone();
            let output_tx = output_tx.clone();
            let processor = Arc::clone(&processor);
            
            let handle = thread::spawn(move || {
                log::debug!("Bounded worker {} started", worker_id);
                
                while let Ok(work_item) = input_rx.recv() {
                    match work_item {
                        WorkItem::Process(input) => {
                            let result = processor(input);
                            if output_tx.send(result).is_err() {
                                log::warn!("Worker {} output channel closed", worker_id);
                                break;
                            }
                        }
                        WorkItem::Shutdown => {
                            log::debug!("Worker {} shutting down", worker_id);
                            break;
                        }
                    }
                }
                
                log::debug!("Worker {} finished", worker_id);
            });
            
            workers.push(handle);
        }
        
        Ok(Self {
            input_tx,
            output_rx,
            _workers: workers,
        })
    }
    
    /// Submit work (blocks if queue is full)
    pub fn submit(&self, item: I) -> Result<()> {
        self.input_tx
            .send(WorkItem::Process(item))
            .map_err(|e| anyhow::anyhow!("Failed to submit work: {}", e))
    }
    
    /// Try to submit work without blocking
    pub fn try_submit(&self, item: I) -> Result<bool> {
        match self.input_tx.try_send(WorkItem::Process(item)) {
            Ok(()) => Ok(true),
            Err(crossbeam_channel::TrySendError::Full(_)) => Ok(false),
            Err(crossbeam_channel::TrySendError::Disconnected(_)) => {
                anyhow::bail!("Workers disconnected")
            }
        }
    }
    
    /// Receive result (blocks until available)
    pub fn receive(&self) -> Result<O> {
        self.output_rx
            .recv()
            .map_err(|e| anyhow::anyhow!("Failed to receive result: {}", e))?
            .map_err(|e| anyhow::anyhow!("Worker error: {}", e))
    }
    
    /// Try to receive result without blocking
    pub fn try_receive(&self) -> Result<Option<O>> {
        match self.output_rx.try_recv() {
            Ok(result) => Ok(Some(result.map_err(|e| anyhow::anyhow!("Worker error: {}", e))?)),
            Err(crossbeam_channel::TryRecvError::Empty) => Ok(None),
            Err(crossbeam_channel::TryRecvError::Disconnected) => {
                anyhow::bail!("Workers disconnected")
            }
        }
    }
    
    /// Shutdown all workers gracefully
    pub fn shutdown(&self) -> Result<()> {
        for _ in 0..self._workers.len() {
            self.input_tx
                .send(WorkItem::Shutdown)
                .map_err(|e| anyhow::anyhow!("Failed to shutdown worker: {}", e))?;
        }
        Ok(())
    }
    
    /// Get number of pending items in input queue
    pub fn pending_input(&self) -> usize {
        self.input_tx.len()
    }
    
    /// Get number of pending items in output queue  
    pub fn pending_output(&self) -> usize {
        self.output_rx.len()
    }
}