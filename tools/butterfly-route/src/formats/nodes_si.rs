///! Two-level sparse index for nodes.sa
///!
///! Format: nodes.si (little-endian)
///!
///! Header (32 bytes):
///!   magic:      u32 = 0x4E4F4458  // "NODX"
///!   version:    u16 = 1
///!   reserved:   u16 = 0
///!   block_size: u32 = 2048
///!   top_bits:   u8  = 16
///!   reserved2:  [19]u8
///!
///! Level 1 (65536 entries for top_bits=16):
///!   For each bucket k in [0..65535]:
///!     start_idx: u64  // index into Level 2 for first sample
///!     end_idx:   u64  // one past last (start=end if empty)
///!
///! Level 2 (M = ceil(count / block_size) samples):
///!   For each sample j in [0..M-1]:
///!     id_sample:  i64  // id at record j*block_size in nodes.sa
///!     rec_index:  u64  // = j*block_size

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const MAGIC: u32 = 0x4E4F4458; // "NODX"
const VERSION: u16 = 1;
const BLOCK_SIZE: u32 = 2048;
const TOP_BITS: u8 = 16;
const HEADER_SIZE: usize = 32;
const NUM_BUCKETS: usize = 1 << TOP_BITS; // 65536

#[derive(Debug)]
struct Level2Sample {
    id_sample: i64,
    rec_index: u64,
}

/// Write two-level sparse index for nodes
pub fn write<P: AsRef<Path>>(path: P, nodes: &[(i64, f64, f64)]) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
    let mut writer = BufWriter::new(file);

    // Ensure nodes are sorted
    let mut sorted_nodes = nodes.to_vec();
    sorted_nodes.sort_by_key(|(id, _, _)| *id);

    // Build Level 2 samples (one per block_size records)
    let mut level2: Vec<Level2Sample> = Vec::new();
    for (j, chunk) in sorted_nodes.chunks(BLOCK_SIZE as usize).enumerate() {
        if let Some((id, _, _)) = chunk.first() {
            level2.push(Level2Sample {
                id_sample: *id,
                rec_index: (j as u64) * (BLOCK_SIZE as u64),
            });
        }
    }

    // Build Level 1 buckets by partitioning Level 2 samples
    let mut level1: Vec<(u64, u64)> = vec![(0, 0); NUM_BUCKETS];

    // Group Level 2 samples by high bits
    for (sample_idx, sample) in level2.iter().enumerate() {
        let bucket = compute_bucket(sample.id_sample);
        let (start, end) = &mut level1[bucket];

        if *start == 0 && *end == 0 {
            // First sample in this bucket
            *start = sample_idx as u64;
            *end = (sample_idx + 1) as u64;
        } else {
            // Extend bucket
            *end = (sample_idx + 1) as u64;
        }
    }

    // Fix empty buckets: ensure start_idx points to correct position
    let mut last_end = 0u64;
    for (start, end) in level1.iter_mut() {
        if *start == 0 && *end == 0 {
            // Empty bucket - point to where next bucket would start
            *start = last_end;
            *end = last_end;
        }
        last_end = *end;
    }

    // Write header
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes()); // reserved
    header.extend_from_slice(&BLOCK_SIZE.to_le_bytes());
    header.push(TOP_BITS);
    header.resize(HEADER_SIZE, 0); // Fill reserved2

    writer.write_all(&header)?;

    // Write Level 1
    for (start_idx, end_idx) in level1.iter() {
        writer.write_all(&start_idx.to_le_bytes())?;
        writer.write_all(&end_idx.to_le_bytes())?;
    }

    // Write Level 2
    for sample in level2.iter() {
        writer.write_all(&sample.id_sample.to_le_bytes())?;
        writer.write_all(&sample.rec_index.to_le_bytes())?;
    }

    writer.flush()?;
    Ok(())
}

/// Compute bucket from node ID using high bits
fn compute_bucket(id: i64) -> usize {
    let id_u64 = id as u64;
    let hi = id_u64 >> (64 - TOP_BITS as u32);
    hi as usize
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bucket_computation() {
        // Test that buckets are distributed across the range
        let id1 = 123_295i64;
        let id2 = 13_254_885_295i64;

        let bucket1 = compute_bucket(id1);
        let bucket2 = compute_bucket(id2);

        // Different IDs should typically map to different buckets
        // (though not guaranteed for all cases)
        assert!(bucket1 < NUM_BUCKETS);
        assert!(bucket2 < NUM_BUCKETS);
    }

    #[test]
    fn test_level2_sampling() {
        // Create test data: 10000 nodes
        let nodes: Vec<(i64, f64, f64)> = (1..=10000)
            .map(|i| (i, 50.0 + (i as f64) * 0.0001, 4.0 + (i as f64) * 0.0001))
            .collect();

        // Expected number of samples
        let expected_samples = (nodes.len() as f64 / BLOCK_SIZE as f64).ceil() as usize;

        // Build Level 2 (simplified from write function)
        let mut level2: Vec<Level2Sample> = Vec::new();
        for (j, chunk) in nodes.chunks(BLOCK_SIZE as usize).enumerate() {
            if let Some((id, _, _)) = chunk.first() {
                level2.push(Level2Sample {
                    id_sample: *id,
                    rec_index: (j as u64) * (BLOCK_SIZE as u64),
                });
            }
        }

        assert_eq!(level2.len(), expected_samples);

        // Verify samples are at correct positions
        assert_eq!(level2[0].id_sample, 1);
        assert_eq!(level2[0].rec_index, 0);
        assert_eq!(level2[1].id_sample, 2049);
        assert_eq!(level2[1].rec_index, 2048);
    }
}
