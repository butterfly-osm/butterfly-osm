//! cch.topo format - CCH shortcut topology (metric-independent)
//!
//! Stores which shortcuts exist, not their weights.
//! Weights are computed per-mode in Step 8 (customization).
//!
//! # Rank-Aligned Storage (Version 2)
//!
//! All node IDs in this format are RANK POSITIONS, not filtered node IDs.
//! This means: node_id = rank, where rank is the contraction order.
//!
//! Benefits:
//! - `offsets[rank]` gives edges directly (no inv_perm lookup)
//! - `dist[rank]` during PHAST is sequential memory access
//! - 2-4x speedup expected from cache efficiency
//!
//! For path unpacking and geometry lookup, use `rank_to_filtered` mapping.

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x43434854; // "CCHT"
const VERSION: u16 = 2; // Version 2: rank-aligned storage

/// A shortcut in the CCH
#[derive(Debug, Clone, Copy)]
pub struct Shortcut {
    pub target: u32, // Target node (rank position)
    pub middle: u32, // Middle node for unpacking (rank position)
}

/// CCH topology - stores the hierarchical graph structure
///
/// # Node ID Convention (Version 2)
///
/// All node IDs are RANK POSITIONS:
/// - `up_offsets[rank]` = start of edges for node at rank
/// - `up_targets[i]` = target rank position
/// - `up_middle[i]` = middle node rank position (for shortcut unpacking)
///
/// Use `rank_to_filtered[rank]` to convert back to filtered node IDs for:
/// - Geometry lookup (needs original EBG coordinates)
/// - Weight lookup (weights indexed by original arc)
#[derive(Debug, Clone)]
pub struct CchTopo {
    pub n_nodes: u32,
    pub n_shortcuts: u64,
    pub n_original_arcs: u64,
    pub inputs_sha: [u8; 32],

    // Upward graph in CSR format (indexed by rank)
    // For node at rank r, upward neighbors have rank > r
    pub up_offsets: Vec<u64>,      // n_nodes + 1, indexed by rank
    pub up_targets: Vec<u32>,      // Rank positions of targets
    pub up_is_shortcut: Vec<bool>, // true if this is a shortcut, false if original
    pub up_middle: Vec<u32>,       // Rank position of middle node (u32::MAX if original)

    // Downward graph in CSR format (indexed by rank)
    // For node at rank r, downward neighbors have rank < r
    pub down_offsets: Vec<u64>,
    pub down_targets: Vec<u32>,
    pub down_is_shortcut: Vec<bool>,
    pub down_middle: Vec<u32>,

    // Mapping from rank position to filtered node ID
    // rank_to_filtered[rank] = filtered_id
    // Used for geometry lookup and path unpacking
    pub rank_to_filtered: Vec<u32>,
}

pub struct CchTopoFile;

impl CchTopoFile {
    pub fn write<P: AsRef<Path>>(path: P, data: &CchTopo) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (76 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_nodes_bytes = data.n_nodes.to_le_bytes();
        let n_shortcuts_bytes = data.n_shortcuts.to_le_bytes();
        let n_original_bytes = data.n_original_arcs.to_le_bytes();
        let n_up_edges = data.up_offsets.last().copied().unwrap_or(0);
        let n_down_edges = data.down_offsets.last().copied().unwrap_or(0);
        let n_up_bytes = n_up_edges.to_le_bytes();
        let n_down_bytes = n_down_edges.to_le_bytes();

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_nodes_bytes)?;
        writer.write_all(&n_shortcuts_bytes)?;
        writer.write_all(&n_original_bytes)?;
        writer.write_all(&n_up_bytes)?;
        writer.write_all(&n_down_bytes)?;
        writer.write_all(&data.inputs_sha)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_nodes_bytes);
        crc_digest.update(&n_shortcuts_bytes);
        crc_digest.update(&n_original_bytes);
        crc_digest.update(&n_up_bytes);
        crc_digest.update(&n_down_bytes);
        crc_digest.update(&data.inputs_sha);

        // Up graph
        for &off in &data.up_offsets {
            let bytes = off.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        for &t in &data.up_targets {
            let bytes = t.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        for &is_sc in &data.up_is_shortcut {
            let byte = if is_sc { 1u8 } else { 0u8 };
            writer.write_all(&[byte])?;
            crc_digest.update(&[byte]);
        }
        for &m in &data.up_middle {
            let bytes = m.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Down graph
        for &off in &data.down_offsets {
            let bytes = off.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        for &t in &data.down_targets {
            let bytes = t.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        for &is_sc in &data.down_is_shortcut {
            let byte = if is_sc { 1u8 } else { 0u8 };
            writer.write_all(&[byte])?;
            crc_digest.update(&[byte]);
        }
        for &m in &data.down_middle {
            let bytes = m.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Rank to filtered mapping (Version 2)
        for &f in &data.rank_to_filtered {
            let bytes = f.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    pub fn read<P: AsRef<Path>>(path: P) -> Result<CchTopo> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (76 bytes)
        // magic(4) + version(2) + reserved(2) + n_nodes(4) + n_shortcuts(8) +
        // n_original(8) + n_up(8) + n_down(8) + sha256(32) = 76
        let mut header = vec![0u8; 76];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        if magic != MAGIC {
            anyhow::bail!(
                "Invalid magic: expected 0x{:08X}, got 0x{:08X}",
                MAGIC,
                magic
            );
        }

        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != VERSION {
            anyhow::bail!(
                "Unsupported CCH topology version: expected {}, got {}. Please rebuild with step7-contract.",
                VERSION, version
            );
        }

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_shortcuts = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let n_original_arcs = u64::from_le_bytes([
            header[20], header[21], header[22], header[23], header[24], header[25], header[26],
            header[27],
        ]);
        let n_up_edges = u64::from_le_bytes([
            header[28], header[29], header[30], header[31], header[32], header[33], header[34],
            header[35],
        ]);
        let n_down_edges = u64::from_le_bytes([
            header[36], header[37], header[38], header[39], header[40], header[41], header[42],
            header[43],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[44..76]);

        // Read up graph
        let mut up_offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            up_offsets.push(u64::from_le_bytes(buf));
        }

        let mut up_targets = Vec::with_capacity(n_up_edges as usize);
        for _ in 0..n_up_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            up_targets.push(u32::from_le_bytes(buf));
        }

        let mut up_is_shortcut = Vec::with_capacity(n_up_edges as usize);
        for _ in 0..n_up_edges {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            up_is_shortcut.push(buf[0] != 0);
        }

        let mut up_middle = Vec::with_capacity(n_up_edges as usize);
        for _ in 0..n_up_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            up_middle.push(u32::from_le_bytes(buf));
        }

        // Read down graph
        let mut down_offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            down_offsets.push(u64::from_le_bytes(buf));
        }

        let mut down_targets = Vec::with_capacity(n_down_edges as usize);
        for _ in 0..n_down_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            down_targets.push(u32::from_le_bytes(buf));
        }

        let mut down_is_shortcut = Vec::with_capacity(n_down_edges as usize);
        for _ in 0..n_down_edges {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            down_is_shortcut.push(buf[0] != 0);
        }

        let mut down_middle = Vec::with_capacity(n_down_edges as usize);
        for _ in 0..n_down_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            down_middle.push(u32::from_le_bytes(buf));
        }

        // Rank to filtered mapping (Version 2)
        let mut rank_to_filtered = Vec::with_capacity(n_nodes as usize);
        for _ in 0..n_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            rank_to_filtered.push(u32::from_le_bytes(buf));
        }

        // Verify CRC64 checksum
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in cch.topo: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(CchTopo {
            n_nodes,
            n_shortcuts,
            n_original_arcs,
            inputs_sha,
            up_offsets,
            up_targets,
            up_is_shortcut,
            up_middle,
            down_offsets,
            down_targets,
            down_is_shortcut,
            down_middle,
            rank_to_filtered,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write as IoWrite};
    use tempfile::NamedTempFile;

    fn make_test_topo() -> CchTopo {
        CchTopo {
            n_nodes: 4,
            n_shortcuts: 1,
            n_original_arcs: 3,
            inputs_sha: [0xCD; 32],
            up_offsets: vec![0, 1, 2, 3, 3],
            up_targets: vec![1, 2, 3],
            up_is_shortcut: vec![false, false, true],
            up_middle: vec![u32::MAX, u32::MAX, 1],
            down_offsets: vec![0, 0, 1, 2, 3],
            down_targets: vec![0, 1, 2],
            down_is_shortcut: vec![false, false, true],
            down_middle: vec![u32::MAX, u32::MAX, 1],
            rank_to_filtered: vec![10, 20, 30, 40],
        }
    }

    #[test]
    fn test_roundtrip() -> Result<()> {
        let data = make_test_topo();
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;
        let loaded = CchTopoFile::read(tmp.path())?;

        assert_eq!(loaded.n_nodes, 4);
        assert_eq!(loaded.n_shortcuts, 1);
        assert_eq!(loaded.n_original_arcs, 3);
        assert_eq!(loaded.inputs_sha, [0xCD; 32]);
        assert_eq!(loaded.up_targets, vec![1, 2, 3]);
        assert_eq!(loaded.up_is_shortcut, vec![false, false, true]);
        assert_eq!(loaded.up_middle[2], 1);
        assert_eq!(loaded.down_targets, vec![0, 1, 2]);
        assert_eq!(loaded.rank_to_filtered, vec![10, 20, 30, 40]);
        Ok(())
    }

    #[test]
    fn test_crc_detects_body_corruption() -> Result<()> {
        let data = make_test_topo();
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;

        // Corrupt a byte in the up_offsets section (just after the 76-byte header)
        {
            let mut file = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
            file.seek(SeekFrom::Start(80))?;
            file.write_all(&[0xFF])?;
        }

        let result = CchTopoFile::read(tmp.path());
        assert!(result.is_err(), "corrupted file should fail CRC check");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("CRC64 mismatch"),
            "error should mention CRC: {}",
            err_msg
        );
        Ok(())
    }
}
