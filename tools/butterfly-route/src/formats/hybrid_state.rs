//! hybrid_state.<mode> format - Hybrid state graph for per-mode CCH
//!
//! Stores the hybrid state graph with mixed node-states and edge-states.
//! Provides 2.62x state reduction vs full EBG while maintaining exact turn cost semantics.
//!
//! Used by Step 6/7/8 to build per-mode CCH hierarchies with dramatically fewer nodes.

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;
use crate::profile_abi::Mode;

const MAGIC: u32 = 0x48535447; // "HSTG" = Hybrid STate Graph
const VERSION: u16 = 1;

/// Hybrid state graph for a specific mode
#[derive(Debug)]
pub struct HybridState {
    pub mode: Mode,
    pub n_states: u32,
    pub n_node_states: u32,
    pub n_edge_states: u32,
    pub n_arcs: u64,
    pub n_nbg_nodes: u32,
    pub n_ebg_nodes: u32,
    pub inputs_sha: [u8; 32],

    // CSR in hybrid state space
    pub offsets: Vec<u64>, // n_states + 1
    pub targets: Vec<u32>, // n_arcs (hybrid state IDs)
    pub weights: Vec<u32>, // n_arcs (edge weight + turn cost in deciseconds)

    // State mappings
    // For states 0..n_node_states: maps to NBG node ID
    pub node_state_to_nbg: Vec<u32>, // n_node_states
    // For states n_node_states..n_states: maps to EBG node ID
    pub edge_state_to_ebg: Vec<u32>, // n_edge_states

    // Reverse mappings for coordinate lookup
    pub nbg_to_node_state: Vec<u32>, // n_nbg_nodes: nbg_id -> state_id (u32::MAX if complex)
    pub ebg_to_edge_state: Vec<u32>, // n_ebg_nodes: ebg_id -> state_id (u32::MAX if simple dest)

    // For coordinate lookup from EBG
    pub ebg_head_nbg: Vec<u32>, // n_ebg_nodes: ebg_id -> head_nbg (for coordinate lookup)
}

impl HybridState {
    /// Check if a state is a node-state
    #[inline]
    pub fn is_node_state(&self, state: u32) -> bool {
        state < self.n_node_states
    }

    /// Check if a state is an edge-state
    #[inline]
    pub fn is_edge_state(&self, state: u32) -> bool {
        state >= self.n_node_states
    }

    /// Get NBG node for a state (for coordinate lookup)
    #[inline]
    pub fn state_to_nbg(&self, state: u32) -> u32 {
        if state < self.n_node_states {
            self.node_state_to_nbg[state as usize]
        } else {
            let edge_idx = (state - self.n_node_states) as usize;
            let ebg_id = self.edge_state_to_ebg[edge_idx];
            self.ebg_head_nbg[ebg_id as usize]
        }
    }

    /// Get iterator over outgoing arcs from a state
    #[inline]
    pub fn out_arcs(&self, state: u32) -> impl Iterator<Item = (u32, u32)> + '_ {
        let start = self.offsets[state as usize] as usize;
        let end = self.offsets[state as usize + 1] as usize;
        (start..end).map(move |i| (self.targets[i], self.weights[i]))
    }
}

pub struct HybridStateFile;

impl HybridStateFile {
    /// Write hybrid state graph to file
    pub fn write<P: AsRef<Path>>(path: P, data: &HybridState) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path.as_ref())?);
        let mut crc_digest = crc::Digest::new();

        // Header (96 bytes)
        // magic(4) + version(2) + mode(1) + reserved(1) +
        // n_states(4) + n_node_states(4) + n_edge_states(4) +
        // n_arcs(8) + n_nbg(4) + n_ebg(4) + sha(32) + padding(28)
        let mut header = Vec::with_capacity(96);
        header.extend_from_slice(&MAGIC.to_le_bytes());
        header.extend_from_slice(&VERSION.to_le_bytes());
        header.push(data.mode as u8);
        header.push(0u8); // reserved
        header.extend_from_slice(&data.n_states.to_le_bytes());
        header.extend_from_slice(&data.n_node_states.to_le_bytes());
        header.extend_from_slice(&data.n_edge_states.to_le_bytes());
        header.extend_from_slice(&data.n_arcs.to_le_bytes());
        header.extend_from_slice(&data.n_nbg_nodes.to_le_bytes());
        header.extend_from_slice(&data.n_ebg_nodes.to_le_bytes());
        header.extend_from_slice(&data.inputs_sha);
        header.extend_from_slice(&[0u8; 28]); // padding to 96 bytes
        assert_eq!(header.len(), 96);

        writer.write_all(&header)?;
        crc_digest.update(&header);

        // CSR offsets
        for &off in &data.offsets {
            let bytes = off.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // CSR targets
        for &t in &data.targets {
            let bytes = t.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // CSR weights
        for &w in &data.weights {
            let bytes = w.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // node_state_to_nbg
        for &nbg in &data.node_state_to_nbg {
            let bytes = nbg.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // edge_state_to_ebg
        for &ebg in &data.edge_state_to_ebg {
            let bytes = ebg.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // nbg_to_node_state
        for &state in &data.nbg_to_node_state {
            let bytes = state.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // ebg_to_edge_state
        for &state in &data.ebg_to_edge_state {
            let bytes = state.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // ebg_head_nbg
        for &nbg in &data.ebg_head_nbg {
            let bytes = nbg.to_le_bytes();
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

    /// Read hybrid state graph from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<HybridState> {
        let mut reader = BufReader::new(
            File::open(path.as_ref())
                .with_context(|| format!("Failed to open {}", path.as_ref().display()))?,
        );

        // Read header
        let mut header = [0u8; 96];
        reader.read_exact(&mut header)?;

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        if magic != MAGIC {
            anyhow::bail!(
                "Invalid magic: expected 0x{:08X} (HSTG), got 0x{:08X}",
                MAGIC,
                magic
            );
        }

        let version = u16::from_le_bytes([header[4], header[5]]);
        if version != VERSION {
            anyhow::bail!("Unsupported version: {}", version);
        }

        let mode = match header[6] {
            0 => Mode::Car,
            1 => Mode::Bike,
            2 => Mode::Foot,
            m => anyhow::bail!("Invalid mode: {}", m),
        };

        let n_states = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_node_states = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
        let n_edge_states = u32::from_le_bytes([header[16], header[17], header[18], header[19]]);
        let n_arcs = u64::from_le_bytes([
            header[20], header[21], header[22], header[23], header[24], header[25], header[26],
            header[27],
        ]);
        let n_nbg_nodes = u32::from_le_bytes([header[28], header[29], header[30], header[31]]);
        let n_ebg_nodes = u32::from_le_bytes([header[32], header[33], header[34], header[35]]);

        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[36..68]);

        // Read CSR offsets
        let mut offsets = Vec::with_capacity(n_states as usize + 1);
        for _ in 0..=n_states {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            offsets.push(u64::from_le_bytes(buf));
        }

        // Read CSR targets
        let mut targets = Vec::with_capacity(n_arcs as usize);
        for _ in 0..n_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            targets.push(u32::from_le_bytes(buf));
        }

        // Read CSR weights
        let mut weights = Vec::with_capacity(n_arcs as usize);
        for _ in 0..n_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            weights.push(u32::from_le_bytes(buf));
        }

        // Read node_state_to_nbg
        let mut node_state_to_nbg = Vec::with_capacity(n_node_states as usize);
        for _ in 0..n_node_states {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            node_state_to_nbg.push(u32::from_le_bytes(buf));
        }

        // Read edge_state_to_ebg
        let mut edge_state_to_ebg = Vec::with_capacity(n_edge_states as usize);
        for _ in 0..n_edge_states {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            edge_state_to_ebg.push(u32::from_le_bytes(buf));
        }

        // Read nbg_to_node_state
        let mut nbg_to_node_state = Vec::with_capacity(n_nbg_nodes as usize);
        for _ in 0..n_nbg_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            nbg_to_node_state.push(u32::from_le_bytes(buf));
        }

        // Read ebg_to_edge_state
        let mut ebg_to_edge_state = Vec::with_capacity(n_ebg_nodes as usize);
        for _ in 0..n_ebg_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            ebg_to_edge_state.push(u32::from_le_bytes(buf));
        }

        // Read ebg_head_nbg
        let mut ebg_head_nbg = Vec::with_capacity(n_ebg_nodes as usize);
        for _ in 0..n_ebg_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            ebg_head_nbg.push(u32::from_le_bytes(buf));
        }

        Ok(HybridState {
            mode,
            n_states,
            n_node_states,
            n_edge_states,
            n_arcs,
            n_nbg_nodes,
            n_ebg_nodes,
            inputs_sha,
            offsets,
            targets,
            weights,
            node_state_to_nbg,
            edge_state_to_ebg,
            nbg_to_node_state,
            ebg_to_edge_state,
            ebg_head_nbg,
        })
    }
}
