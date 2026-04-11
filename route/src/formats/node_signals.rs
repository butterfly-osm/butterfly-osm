//! Sorted array of OSM node IDs with traffic signals
//!
//! Format: node_signals.bin (little-endian, memory-mappable)
//!
//! Header (64 bytes):
//!   magic:        u32 = 0x53494753  // "SIGS"
//!   version:      u16 = 1
//!   reserved:     u16 = 0
//!   count:        u64
//!   created_unix: u64
//!   input_sha256: [32]u8
//!   reserved2:    [8]u8
//!
//! Body (count records, sorted strictly ascending):
//!   osm_node_id: i64
//!
//! Footer (16 bytes):
//!   body_crc64: u64
//!   file_crc64: u64
//!
//! Lookup: O(log n) binary search to check if a node has a traffic signal

use anyhow::{bail, Context, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc::Digest;

const MAGIC: u32 = 0x53494753; // "SIGS"
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 64;

/// Traffic signal nodes - sorted list of OSM node IDs
pub struct NodeSignals {
    /// Sorted list of OSM node IDs with traffic signals
    pub node_ids: Vec<i64>,
}

impl NodeSignals {
    /// Create from a list of node IDs (will be sorted)
    pub fn new(mut node_ids: Vec<i64>) -> Self {
        node_ids.sort_unstable();
        node_ids.dedup();
        Self { node_ids }
    }

    /// Check if a node has a traffic signal (O(log n))
    pub fn has_signal(&self, osm_node_id: i64) -> bool {
        self.node_ids.binary_search(&osm_node_id).is_ok()
    }

    /// Number of signal nodes
    pub fn len(&self) -> usize {
        self.node_ids.len()
    }

    /// Check if empty
    pub fn is_empty(&self) -> bool {
        self.node_ids.is_empty()
    }
}

/// File reader/writer for NodeSignals
pub struct NodeSignalsFile;

impl NodeSignalsFile {
    /// Write signal nodes to file
    pub fn write<P: AsRef<Path>>(
        path: P,
        signals: &NodeSignals,
        input_sha256: &[u8; 32],
    ) -> Result<()> {
        let file = File::create(path.as_ref())
            .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
        let mut writer = BufWriter::new(file);

        let created_unix = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        // Build header
        let mut header = Vec::with_capacity(HEADER_SIZE);
        header.extend_from_slice(&MAGIC.to_le_bytes());
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.extend_from_slice(&0u16.to_le_bytes()); // reserved
        header.extend_from_slice(&(signals.node_ids.len() as u64).to_le_bytes());
        header.extend_from_slice(&created_unix.to_le_bytes());
        header.extend_from_slice(input_sha256);
        header.resize(HEADER_SIZE, 0);

        writer.write_all(&header)?;

        // Write body and calculate CRC
        let mut body_digest = Digest::new();
        for &node_id in &signals.node_ids {
            let bytes = node_id.to_le_bytes();
            body_digest.update(&bytes);
            writer.write_all(&bytes)?;
        }
        let body_crc64 = body_digest.finalize();

        // Calculate file CRC
        let mut file_digest = Digest::new();
        file_digest.update(&header);
        for &node_id in &signals.node_ids {
            file_digest.update(&node_id.to_le_bytes());
        }
        let file_crc64 = file_digest.finalize();

        // Write footer
        writer.write_all(&body_crc64.to_le_bytes())?;
        writer.write_all(&file_crc64.to_le_bytes())?;

        writer.flush()?;
        Ok(())
    }

    /// Read signal nodes from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<NodeSignals> {
        let file = File::open(path.as_ref())
            .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;
        let mut reader = BufReader::new(file);

        // Read header
        let mut header = [0u8; HEADER_SIZE];
        reader
            .read_exact(&mut header)
            .context("Failed to read header")?;

        // Verify magic and version
        let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
        let version = u16::from_le_bytes(header[4..6].try_into().unwrap());

        if magic != MAGIC {
            bail!(
                "Invalid magic: expected 0x{:08X}, got 0x{:08X}",
                MAGIC,
                magic
            );
        }
        if version != VERSION {
            bail!("Unsupported version: expected {}, got {}", VERSION, version);
        }

        let count = u64::from_le_bytes(header[8..16].try_into().unwrap()) as usize;

        // Read body
        let mut node_ids = Vec::with_capacity(count);
        let mut body_digest = Digest::new();

        for _ in 0..count {
            let mut buf = [0u8; 8];
            reader
                .read_exact(&mut buf)
                .context("Failed to read node ID")?;
            body_digest.update(&buf);
            node_ids.push(i64::from_le_bytes(buf));
        }

        // Verify body CRC
        let mut footer = [0u8; 16];
        reader
            .read_exact(&mut footer)
            .context("Failed to read footer")?;

        let expected_body_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        let actual_body_crc = body_digest.finalize();

        if expected_body_crc != actual_body_crc {
            bail!(
                "Body CRC mismatch: expected 0x{:016X}, got 0x{:016X}",
                expected_body_crc,
                actual_body_crc
            );
        }

        Ok(NodeSignals { node_ids })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_roundtrip() -> Result<()> {
        let signals = NodeSignals::new(vec![100, 200, 50, 300, 200]); // Unsorted, with dupe

        // Should be sorted and deduped
        assert_eq!(signals.node_ids, vec![50, 100, 200, 300]);
        assert!(signals.has_signal(100));
        assert!(signals.has_signal(200));
        assert!(!signals.has_signal(150));

        let tmp = NamedTempFile::new()?;
        let sha = [0u8; 32];
        NodeSignalsFile::write(tmp.path(), &signals, &sha)?;

        let loaded = NodeSignalsFile::read(tmp.path())?;
        assert_eq!(loaded.node_ids, signals.node_ids);

        Ok(())
    }
}
