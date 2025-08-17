//! Compression with chunk size auditing and auto-adjustment

use crate::error::{IoError, IoResult};
use std::io::{Read, Write};

/// Chunk size auditor that monitors compression efficiency
pub struct ChunkSizeAuditor {
    target_chunk_size: usize,
    current_level: i32,
    samples: Vec<CompressionSample>,
    max_samples: usize,
}

#[derive(Debug)]
struct CompressionSample {
    uncompressed_size: usize,
    compressed_size: usize,
}

impl ChunkSizeAuditor {
    pub fn new(target_chunk_size: usize) -> Self {
        Self {
            target_chunk_size,
            current_level: 3, // zstd default
            samples: Vec::new(),
            max_samples: 50,
        }
    }
    
    /// Record a compression sample and potentially adjust level
    pub fn record_sample(&mut self, uncompressed: usize, compressed: usize) -> Option<i32> {
        
        self.samples.push(CompressionSample {
            uncompressed_size: uncompressed,
            compressed_size: compressed,
        });
        
        // Keep only recent samples
        if self.samples.len() > self.max_samples {
            self.samples.remove(0);
        }
        
        // Check if we need to adjust compression level
        if self.samples.len() >= 10 {
            let median_ratio = self.calculate_median_ratio();
            let median_compressed = (self.target_chunk_size as f64 * median_ratio) as usize;
            
            // If actual median is drifting >1 bucket from target, adjust
            let bucket_size = self.target_chunk_size / 4; // 64KB buckets for 256KB target
            
            if median_compressed < self.target_chunk_size - bucket_size {
                // Too compressed, reduce level
                if self.current_level > 1 {
                    self.current_level -= 1;
                    return Some(self.current_level);
                }
            } else if median_compressed > self.target_chunk_size + bucket_size {
                // Not compressed enough, increase level
                if self.current_level < 22 {
                    self.current_level += 1;
                    return Some(self.current_level);
                }
            }
        }
        
        None
    }
    
    fn calculate_median_ratio(&self) -> f64 {
        if self.samples.is_empty() {
            return 0.5; // Default ratio
        }
        
        // Calculate ratios from actual size data to ensure fields are used
        let mut ratios: Vec<f64> = self.samples.iter()
            .map(|s| s.compressed_size as f64 / s.uncompressed_size as f64)
            .collect();
        ratios.sort_by(|a, b| a.partial_cmp(b).unwrap());
        
        let mid = ratios.len() / 2;
        if ratios.len() % 2 == 0 {
            (ratios[mid - 1] + ratios[mid]) / 2.0
        } else {
            ratios[mid]
        }
    }
    
    pub fn current_level(&self) -> i32 {
        self.current_level
    }
}

/// Compressed chunk writer with TOC
pub struct CompressedWriter<W: Write> {
    writer: W,
    auditor: ChunkSizeAuditor,
    toc: Vec<ChunkEntry>,
    bytes_written: u64,
}

#[derive(Debug, Clone)]
pub struct ChunkEntry {
    pub offset: u64,
    pub compressed_size: u32,
    pub uncompressed_size: u32,
    pub checksum: u32,
}

impl<W: Write> CompressedWriter<W> {
    pub fn new(writer: W, target_chunk_size: usize) -> Self {
        Self {
            writer,
            auditor: ChunkSizeAuditor::new(target_chunk_size),
            toc: Vec::new(),
            bytes_written: 0,
        }
    }
    
    /// Write a chunk with compression and checksum
    pub fn write_chunk(&mut self, data: &[u8]) -> IoResult<()> {
        // Calculate XXH3 checksum
        let checksum = xxhash_rust::xxh3::xxh3_64(data) as u32;
        
        // Compress with current level
        let level = self.auditor.current_level();
        let compressed = zstd::encode_all(data, level)?;
        
        // Record sample and potentially adjust level for next chunk
        if let Some(new_level) = self.auditor.record_sample(data.len(), compressed.len()) {
            // Log the adjustment (in real implementation)
            #[cfg(debug_assertions)]
            eprintln!("Adjusted zstd level to {} (ratio drift detected)", new_level);
        }
        
        // Write compressed data
        self.writer.write_all(&compressed)?;
        
        // Record TOC entry
        let entry = ChunkEntry {
            offset: self.bytes_written,
            compressed_size: compressed.len() as u32,
            uncompressed_size: data.len() as u32,
            checksum,
        };
        
        self.toc.push(entry);
        self.bytes_written += compressed.len() as u64;
        
        Ok(())
    }
    
    /// Finish writing and return TOC
    pub fn finish(self) -> IoResult<(W, Vec<ChunkEntry>)> {
        Ok((self.writer, self.toc))
    }
}

/// Compressed chunk reader with TOC
pub struct CompressedReader<R: Read> {
    reader: R,
    toc: Vec<ChunkEntry>,
}

impl<R: Read> CompressedReader<R> {
    pub fn new(reader: R, toc: Vec<ChunkEntry>) -> Self {
        Self { reader, toc }
    }
    
    /// Read and decompress a chunk by index
    pub fn read_chunk(&mut self, index: usize) -> IoResult<Vec<u8>> {
        if index >= self.toc.len() {
            return Err(IoError::InvalidFormat(
                format!("Chunk index {} out of bounds", index)
            ));
        }
        
        let entry = &self.toc[index];
        
        // Read compressed data
        let mut compressed = vec![0u8; entry.compressed_size as usize];
        self.reader.read_exact(&mut compressed)?;
        
        // Decompress
        let decompressed = zstd::decode_all(&compressed[..])?;
        
        // Verify checksum
        let actual_checksum = xxhash_rust::xxh3::xxh3_64(&decompressed) as u32;
        if actual_checksum != entry.checksum {
            return Err(IoError::ChecksumMismatch {
                expected: entry.checksum,
                actual: actual_checksum,
            });
        }
        
        // Verify size
        if decompressed.len() != entry.uncompressed_size as usize {
            return Err(IoError::InvalidFormat(
                format!("Size mismatch: expected {}, got {}", 
                       entry.uncompressed_size, decompressed.len())
            ));
        }
        
        Ok(decompressed)
    }
    
    pub fn chunk_count(&self) -> usize {
        self.toc.len()
    }
}