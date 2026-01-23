///! cch.topo format - CCH shortcut topology (metric-independent)
///!
///! Stores which shortcuts exist, not their weights.
///! Weights are computed per-mode in Step 8 (customization).

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x43434854; // "CCHT"
const VERSION: u16 = 1;

/// A shortcut in the CCH
#[derive(Debug, Clone, Copy)]
pub struct Shortcut {
    pub target: u32,      // Target node (in original node IDs)
    pub middle: u32,      // Middle node for unpacking (contracted node)
}

/// CCH topology - stores the hierarchical graph structure
#[derive(Debug)]
pub struct CchTopo {
    pub n_nodes: u32,
    pub n_shortcuts: u64,
    pub n_original_arcs: u64,
    pub inputs_sha: [u8; 32],

    // Upward graph in CSR format (by rank)
    // For node with rank r, upward neighbors are nodes with rank > r
    pub up_offsets: Vec<u64>,      // n_nodes + 1
    pub up_targets: Vec<u32>,      // Original arcs + shortcuts going up
    pub up_is_shortcut: Vec<bool>, // true if this is a shortcut, false if original
    pub up_middle: Vec<u32>,       // Middle node for shortcuts (u32::MAX if original)

    // Downward graph in CSR format (by rank)
    // For node with rank r, downward neighbors are nodes with rank < r
    pub down_offsets: Vec<u64>,
    pub down_targets: Vec<u32>,
    pub down_is_shortcut: Vec<bool>,
    pub down_middle: Vec<u32>,
}

pub struct CchTopoFile;

impl CchTopoFile {
    pub fn write<P: AsRef<Path>>(path: P, data: &CchTopo) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (64 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_nodes_bytes = data.n_nodes.to_le_bytes();
        let n_shortcuts_bytes = data.n_shortcuts.to_le_bytes();
        let n_original_bytes = data.n_original_arcs.to_le_bytes();
        let n_up_edges = (data.up_offsets.last().copied().unwrap_or(0)) as u64;
        let n_down_edges = (data.down_offsets.last().copied().unwrap_or(0)) as u64;
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

        // Footer
        let body_crc = crc_digest.finalize();
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    pub fn read<P: AsRef<Path>>(path: P) -> Result<CchTopo> {
        let mut reader = BufReader::new(File::open(path)?);

        // Header (76 bytes)
        // magic(4) + version(2) + reserved(2) + n_nodes(4) + n_shortcuts(8) +
        // n_original(8) + n_up(8) + n_down(8) + sha256(32) = 76
        let mut header = vec![0u8; 76];
        reader.read_exact(&mut header)?;

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        if magic != MAGIC {
            anyhow::bail!("Invalid magic: expected 0x{:08X}, got 0x{:08X}", MAGIC, magic);
        }

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_shortcuts = u64::from_le_bytes([
            header[12], header[13], header[14], header[15],
            header[16], header[17], header[18], header[19],
        ]);
        let n_original_arcs = u64::from_le_bytes([
            header[20], header[21], header[22], header[23],
            header[24], header[25], header[26], header[27],
        ]);
        let n_up_edges = u64::from_le_bytes([
            header[28], header[29], header[30], header[31],
            header[32], header[33], header[34], header[35],
        ]);
        let n_down_edges = u64::from_le_bytes([
            header[36], header[37], header[38], header[39],
            header[40], header[41], header[42], header[43],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[44..76]);

        // Read up graph
        let mut up_offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            up_offsets.push(u64::from_le_bytes(buf));
        }

        let mut up_targets = Vec::with_capacity(n_up_edges as usize);
        for _ in 0..n_up_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            up_targets.push(u32::from_le_bytes(buf));
        }

        let mut up_is_shortcut = Vec::with_capacity(n_up_edges as usize);
        for _ in 0..n_up_edges {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            up_is_shortcut.push(buf[0] != 0);
        }

        let mut up_middle = Vec::with_capacity(n_up_edges as usize);
        for _ in 0..n_up_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            up_middle.push(u32::from_le_bytes(buf));
        }

        // Read down graph
        let mut down_offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            down_offsets.push(u64::from_le_bytes(buf));
        }

        let mut down_targets = Vec::with_capacity(n_down_edges as usize);
        for _ in 0..n_down_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            down_targets.push(u32::from_le_bytes(buf));
        }

        let mut down_is_shortcut = Vec::with_capacity(n_down_edges as usize);
        for _ in 0..n_down_edges {
            let mut buf = [0u8; 1];
            reader.read_exact(&mut buf)?;
            down_is_shortcut.push(buf[0] != 0);
        }

        let mut down_middle = Vec::with_capacity(n_down_edges as usize);
        for _ in 0..n_down_edges {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            down_middle.push(u32::from_le_bytes(buf));
        }

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
        })
    }
}
