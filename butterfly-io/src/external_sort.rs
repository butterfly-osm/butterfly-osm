//! External sorter with memory throttling and k-way merge
//!
//! Provides disk-based sorting for datasets larger than available memory,
//! with RSS monitoring and automatic memory throttling.

use crate::format::BflyHeader;
use crate::loser_tree::{LoserTree, LoserTreeEntry};
use crate::token_bucket::WorkerAdmissionController;
use crate::{IoError, IoResult};
use std::fs::File;
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::Duration;

/// External sorter trait for disk-based sorting with memory management
pub trait ExternalSorter<T> {
    /// Add an item to the sorter
    fn push(&mut self, item: T) -> IoResult<()>;

    /// Finish adding items and begin sorting
    fn finish(self) -> IoResult<SortedIterator<T>>;

    /// Get current memory usage estimate
    fn memory_usage(&self) -> usize;
}

/// RSS memory monitor with 250ms sampling
pub struct RssMonitor {
    current_rss_kb: Arc<Mutex<u64>>,
    should_stop: Arc<AtomicBool>,
    _handle: thread::JoinHandle<()>,
}

impl RssMonitor {
    pub fn new() -> IoResult<Self> {
        let current_rss_kb = Arc::new(Mutex::new(0));
        let should_stop = Arc::new(AtomicBool::new(false));

        let rss_clone = current_rss_kb.clone();
        let stop_clone = should_stop.clone();

        let handle = thread::spawn(move || {
            while !stop_clone.load(Ordering::Relaxed) {
                if let Ok(rss) = read_rss_kb() {
                    *rss_clone.lock().unwrap() = rss;
                }
                thread::sleep(Duration::from_millis(250));
            }
        });

        Ok(Self {
            current_rss_kb,
            should_stop,
            _handle: handle,
        })
    }

    pub fn current_rss_mb(&self) -> u64 {
        *self.current_rss_kb.lock().unwrap() / 1024
    }
}

impl Drop for RssMonitor {
    fn drop(&mut self) {
        self.should_stop.store(true, Ordering::Relaxed);
    }
}

/// Read current RSS from /proc/self/statm
fn read_rss_kb() -> IoResult<u64> {
    let contents = std::fs::read_to_string("/proc/self/statm")?;
    let fields: Vec<&str> = contents.split_whitespace().collect();

    if fields.len() < 2 {
        return Err(IoError::InvalidFormat(
            "Invalid /proc/self/statm format".to_string(),
        ));
    }

    // Field 1 is RSS in pages, convert to KB (assuming 4KB pages)
    let rss_pages: u64 = fields[1]
        .parse()
        .map_err(|_| IoError::InvalidFormat("Invalid RSS value".to_string()))?;

    Ok(rss_pages * 4) // 4KB pages
}

/// Memory-throttled external sorter implementation with token bucket admission control
pub struct MemoryThrottledSorter<T> {
    buffer: Vec<T>,
    temp_dir: PathBuf,
    spill_files: Vec<PathBuf>,
    memory_limit_mb: u64,
    rss_monitor: RssMonitor,
    run_counter: usize,
    admission_controller: WorkerAdmissionController,
}

impl<T> MemoryThrottledSorter<T>
where
    T: Clone + Ord + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    pub fn new<P: AsRef<Path>>(temp_dir: P, memory_limit_mb: u64) -> IoResult<Self> {
        let temp_dir = temp_dir.as_ref().to_path_buf();
        std::fs::create_dir_all(&temp_dir)?;

        // Configure token bucket for admission control
        // Allow burst of 1000 items, refill at 100 items/sec to prevent overwhelming memory
        let admission_controller = WorkerAdmissionController::new(100, 1000, 1);

        Ok(Self {
            buffer: Vec::new(),
            temp_dir,
            spill_files: Vec::new(),
            memory_limit_mb,
            rss_monitor: RssMonitor::new()?,
            run_counter: 0,
            admission_controller,
        })
    }

    /// Spill current buffer to disk as a sorted run
    fn spill_to_disk(&mut self) -> IoResult<()> {
        if self.buffer.is_empty() {
            return Ok(());
        }

        // Sort current buffer
        self.buffer.sort();

        // Create spill file
        let spill_path = self
            .temp_dir
            .join(format!("run_{:04}.bfly", self.run_counter));
        self.run_counter += 1;

        // Serialize all items to build payload
        let mut payload = Vec::new();

        // Write number of items first
        payload.extend_from_slice(&(self.buffer.len() as u64).to_le_bytes());

        // Write each item with length prefix
        for item in &self.buffer {
            let serialized =
                bincode::serialize(item).map_err(|e| IoError::Serialization(e.to_string()))?;
            payload.extend_from_slice(&(serialized.len() as u32).to_le_bytes());
            payload.extend_from_slice(&serialized);
        }

        // Calculate payload hash using XXH3
        let payload_hash = xxhash_rust::xxh3::xxh3_64(&payload);

        // Create BFLY header
        let header = BflyHeader::new(payload.len() as u64, payload_hash);

        // Write to file with BFLY format
        let mut file = File::create(&spill_path)?;
        file.write_all(&header.to_bytes())?;
        file.write_all(&payload)?;
        file.flush()?;
        file.sync_all()?; // Grouped fsync for durability (as specified in M0.3)

        self.spill_files.push(spill_path);
        self.buffer.clear();

        Ok(())
    }
}

impl<T> ExternalSorter<T> for MemoryThrottledSorter<T>
where
    T: Clone + Ord + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
{
    fn push(&mut self, item: T) -> IoResult<()> {
        // Token bucket admission control - wait up to 1 second for admission
        if !self
            .admission_controller
            .admit_with_timeout(Duration::from_secs(1))
        {
            return Err(IoError::InvalidFormat(
                "Worker admission rate limit exceeded".to_string(),
            ));
        }

        self.buffer.push(item);

        // Check memory pressure (90% of limit)
        let current_rss_mb = self.rss_monitor.current_rss_mb();
        let threshold_mb = (self.memory_limit_mb as f64 * 0.9) as u64;

        if current_rss_mb > threshold_mb {
            self.spill_to_disk()?;
        }

        Ok(())
    }

    fn finish(mut self) -> IoResult<SortedIterator<T>> {
        // Spill any remaining buffer
        if !self.buffer.is_empty() {
            self.spill_to_disk()?;
        }

        SortedIterator::new(self.spill_files, self.temp_dir)
    }

    fn memory_usage(&self) -> usize {
        self.buffer.len() * std::mem::size_of::<T>()
    }
}

/// Iterator over sorted results using k-way merge with loser tree
pub struct SortedIterator<T> {
    loser_tree: LoserTree<T>,
    readers: Vec<RunReader<T>>,
    temp_dir: PathBuf,
}

struct RunReader<T> {
    reader: Box<dyn Iterator<Item = IoResult<T>>>,
}

impl<T> SortedIterator<T>
where
    T: Clone + Ord + serde::Serialize + for<'de> serde::Deserialize<'de> + 'static,
{
    fn new(spill_files: Vec<PathBuf>, temp_dir: PathBuf) -> IoResult<Self> {
        let mut readers = Vec::new();
        let mut initial_values = Vec::new();

        // Create readers for each spill file
        for (run_index, spill_file) in spill_files.iter().enumerate() {
            let reader = SpillFileIterator::new(spill_file)?;
            let mut run_reader = RunReader {
                reader: Box::new(reader),
            };

            // Get first item from each run for loser tree initialization
            if let Some(Ok(first_item)) = run_reader.reader.next() {
                initial_values.push(Some(LoserTreeEntry {
                    value: first_item,
                    run_index,
                }));
            } else {
                initial_values.push(None);
            }

            readers.push(run_reader);
        }

        // Initialize loser tree with first elements
        let mut loser_tree = LoserTree::new(spill_files.len());
        loser_tree.initialize(initial_values);

        Ok(Self {
            loser_tree,
            readers,
            temp_dir,
        })
    }
}

impl<T> Iterator for SortedIterator<T>
where
    T: Clone + Ord + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    type Item = IoResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(min_entry) = self.loser_tree.peek_min() {
            let result = min_entry.value.clone();
            let run_index = min_entry.run_index;

            // Try to get next item from the same run
            let next_entry = if let Some(next_result) = self.readers[run_index].reader.next() {
                match next_result {
                    Ok(next_item) => Some(LoserTreeEntry {
                        value: next_item,
                        run_index,
                    }),
                    Err(e) => return Some(Err(e)),
                }
            } else {
                None // This run is exhausted
            };

            // Update loser tree with next item from this run
            self.loser_tree
                .extract_min_and_replace(run_index, next_entry);

            Some(Ok(result))
        } else {
            None // All runs exhausted
        }
    }
}

impl<T> Drop for SortedIterator<T> {
    fn drop(&mut self) {
        // Clean up temp directory
        if let Err(e) = std::fs::remove_dir_all(&self.temp_dir) {
            eprintln!("Warning: Failed to clean up temp directory: {}", e);
        }
    }
}

/// Iterator for reading items from a spill file
struct SpillFileIterator<T> {
    reader: BufReader<std::fs::File>,
    remaining_items: usize,
    _phantom: std::marker::PhantomData<T>,
}

impl<T> SpillFileIterator<T>
where
    T: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    fn new(path: &Path) -> IoResult<Self> {
        let mut file = File::open(path)?;

        // Read and validate BFLY header
        let mut header_bytes = [0u8; 32];
        std::io::Read::read_exact(&mut file, &mut header_bytes)?;
        let header = BflyHeader::from_bytes(&header_bytes)?;
        header.validate()?;

        // Read item count from start of payload
        let mut count_bytes = [0u8; 8];
        std::io::Read::read_exact(&mut file, &mut count_bytes)?;
        let item_count = u64::from_le_bytes(count_bytes) as usize;

        // Create buffered reader positioned after header and item count
        let reader = BufReader::new(file);

        Ok(Self {
            reader,
            remaining_items: item_count,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<T> Iterator for SpillFileIterator<T>
where
    T: Clone + serde::Serialize + for<'de> serde::Deserialize<'de>,
{
    type Item = IoResult<T>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining_items == 0 {
            return None;
        }

        // Read item length (4 bytes)
        let mut len_bytes = [0u8; 4];
        if let Err(e) = std::io::Read::read_exact(&mut self.reader, &mut len_bytes) {
            return Some(Err(IoError::Io(e)));
        }
        let item_len = u32::from_le_bytes(len_bytes) as usize;

        // Read item data
        let mut item_data = vec![0u8; item_len];
        if let Err(e) = std::io::Read::read_exact(&mut self.reader, &mut item_data) {
            return Some(Err(IoError::Io(e)));
        }

        // Deserialize the item
        match bincode::deserialize(&item_data) {
            Ok(item) => {
                self.remaining_items -= 1;
                Some(Ok(item))
            }
            Err(e) => Some(Err(IoError::Serialization(e.to_string()))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    #[test]
    fn test_rss_monitor() {
        let monitor = RssMonitor::new().expect("Failed to create RSS monitor");
        // Give the monitor thread time to sample
        std::thread::sleep(std::time::Duration::from_millis(300));
        let rss_mb = monitor.current_rss_mb();
        // RSS should be at least a few MB for a running Rust process
        assert!(rss_mb >= 1, "RSS should be at least 1MB, got {}", rss_mb);
    }

    #[test]
    fn test_external_sorter_basic() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        let mut sorter =
            MemoryThrottledSorter::new(temp_dir.path(), 1024).expect("Failed to create sorter");

        // Add some test data
        for i in (0..100).rev() {
            sorter.push(i).expect("Failed to push item");
        }

        let iterator = sorter.finish().expect("Failed to finish sorting");
        let results: Result<Vec<_>, _> = iterator.collect();
        let sorted_items = results.expect("Failed to iterate results");

        // Verify sorted order
        for window in sorted_items.windows(2) {
            assert!(window[0] <= window[1], "Items not in sorted order");
        }
    }

    #[test]
    fn test_external_sorter_with_spilling() {
        let temp_dir = TempDir::new().expect("Failed to create temp dir");
        // Use very low memory limit to force spilling
        let mut sorter =
            MemoryThrottledSorter::new(temp_dir.path(), 1).expect("Failed to create sorter");

        // Add enough data to trigger spilling
        let test_data: Vec<i32> = (0..1000).rev().collect();
        for &item in &test_data {
            sorter.push(item).expect("Failed to push item");
        }

        let iterator = sorter.finish().expect("Failed to finish sorting");
        let results: Result<Vec<_>, _> = iterator.collect();
        let sorted_items = results.expect("Failed to iterate results");

        // Verify we got all items back
        assert_eq!(sorted_items.len(), 1000);

        // Verify sorted order
        for (i, &item) in sorted_items.iter().enumerate() {
            assert_eq!(item, i as i32, "Item {} should be {}", i, i);
        }
    }
}
