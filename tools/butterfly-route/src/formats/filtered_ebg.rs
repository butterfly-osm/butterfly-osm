//! filtered_ebg.<mode> format - Mode-filtered EBG for per-mode CCH
//!
//! Stores the filtered subgraph containing only mode-accessible nodes and transitions.
//! Used by Step 6/7/8 to build per-mode CCH hierarchies.

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;
use crate::profile_abi::Mode;

const MAGIC: u32 = 0x46454247; // "FEBG" = Filtered EBG
const VERSION: u16 = 1;

/// Filtered EBG for a specific mode
#[derive(Debug)]
pub struct FilteredEbg {
    pub mode: Mode,
    pub n_filtered_nodes: u32,
    pub n_filtered_arcs: u64,
    pub n_original_nodes: u32,
    pub inputs_sha: [u8; 32],

    // CSR in filtered space
    pub offsets: Vec<u64>,          // n_filtered_nodes + 1
    pub heads: Vec<u32>,            // n_filtered_arcs (filtered node IDs)
    pub original_arc_idx: Vec<u32>, // n_filtered_arcs (original arc indices for turn penalties)

    // Node ID mappings
    pub filtered_to_original: Vec<u32>, // n_filtered_nodes: filtered_id -> original_id
    pub original_to_filtered: Vec<u32>, // n_original_nodes: original_id -> filtered_id (u32::MAX if not in filtered)
}

impl FilteredEbg {
    /// Build filtered EBG from original EBG and mode mask
    ///
    /// DEPRECATED: Use build_with_arc_filter instead to properly enforce turn restrictions.
    /// This function only checks node accessibility, not arc (turn) accessibility.
    #[allow(dead_code)]
    pub fn build(
        mode: Mode,
        ebg_offsets: &[u64],
        ebg_heads: &[u32],
        mask: &[u8],
        n_original_nodes: u32,
        inputs_sha: [u8; 32],
    ) -> Self {
        // Delegate to build_with_arc_filter with no arc filtering
        Self::build_with_arc_filter(
            mode,
            ebg_offsets,
            ebg_heads,
            mask,
            None, // No turn_idx
            None, // No arc_mode_masks
            n_original_nodes,
            inputs_sha,
        )
    }

    /// Build filtered EBG from original EBG with both node and arc filtering.
    ///
    /// This function filters arcs based on:
    /// 1. Source and target node accessibility (from node mask)
    /// 2. Arc (turn) accessibility for this mode (from turn table mode_mask)
    ///
    /// # Arguments
    ///
    /// * `mode` - The mode to filter for (Car, Bike, Foot)
    /// * `ebg_offsets` - CSR offsets for the original EBG
    /// * `ebg_heads` - CSR heads (target node IDs) for the original EBG
    /// * `mask` - Bitset of accessible nodes for this mode
    /// * `turn_idx` - For each arc, index into arc_mode_masks (None to skip arc filtering)
    /// * `arc_mode_masks` - Mode mask for each unique turn entry (None to skip arc filtering)
    /// * `n_original_nodes` - Number of nodes in original EBG
    /// * `inputs_sha` - SHA-256 of input files
    #[allow(clippy::too_many_arguments)]
    pub fn build_with_arc_filter(
        mode: Mode,
        ebg_offsets: &[u64],
        ebg_heads: &[u32],
        mask: &[u8],
        turn_idx: Option<&[u32]>,
        arc_mode_masks: Option<&[u8]>,
        n_original_nodes: u32,
        inputs_sha: [u8; 32],
    ) -> Self {
        let n_orig = n_original_nodes as usize;

        // Mode bit for checking arc accessibility
        let mode_bit = match mode {
            Mode::Car => 1u8,
            Mode::Bike => 2u8,
            Mode::Foot => 4u8,
        };

        // Helper to check node mask
        let is_node_accessible = |node: usize| -> bool {
            let byte_idx = node / 8;
            let bit_idx = node % 8;
            byte_idx < mask.len() && (mask[byte_idx] & (1 << bit_idx)) != 0
        };

        // Helper to check arc accessibility
        let is_arc_accessible = |arc_idx: usize| -> bool {
            match (turn_idx, arc_mode_masks) {
                (Some(tidx), Some(masks)) => {
                    let turn_entry_idx = tidx[arc_idx] as usize;
                    if turn_entry_idx < masks.len() {
                        (masks[turn_entry_idx] & mode_bit) != 0
                    } else {
                        true // Invalid index - allow (shouldn't happen)
                    }
                }
                _ => true, // No arc filtering - allow all
            }
        };

        // Build filtered_to_original: collect accessible nodes
        let mut filtered_to_original = Vec::new();
        for i in 0..n_orig {
            if is_node_accessible(i) {
                filtered_to_original.push(i as u32);
            }
        }
        let n_filtered = filtered_to_original.len();

        // Build original_to_filtered: reverse mapping
        let mut original_to_filtered = vec![u32::MAX; n_orig];
        for (filtered_id, &original_id) in filtered_to_original.iter().enumerate() {
            original_to_filtered[original_id as usize] = filtered_id as u32;
        }

        // Build filtered CSR
        let mut offsets = Vec::with_capacity(n_filtered + 1);
        let mut heads = Vec::new();
        let mut original_arc_idx = Vec::new();

        for &original_u in &filtered_to_original {
            offsets.push(heads.len() as u64);

            let start = ebg_offsets[original_u as usize] as usize;
            let end = ebg_offsets[original_u as usize + 1] as usize;

            for (arc_idx, &head) in ebg_heads.iter().enumerate().take(end).skip(start) {
                let original_v = head as usize;
                // Check BOTH node accessibility AND arc accessibility
                if is_node_accessible(original_v) && is_arc_accessible(arc_idx) {
                    let filtered_v = original_to_filtered[original_v];
                    heads.push(filtered_v);
                    original_arc_idx.push(arc_idx as u32);
                }
            }
        }
        offsets.push(heads.len() as u64);

        Self {
            mode,
            n_filtered_nodes: n_filtered as u32,
            n_filtered_arcs: heads.len() as u64,
            n_original_nodes,
            inputs_sha,
            offsets,
            heads,
            original_arc_idx,
            filtered_to_original,
            original_to_filtered,
        }
    }

    /// Get original node ID from filtered node ID
    #[inline]
    pub fn to_original(&self, filtered_id: u32) -> u32 {
        self.filtered_to_original[filtered_id as usize]
    }

    /// Get filtered node ID from original node ID (returns None if not accessible)
    #[inline]
    pub fn to_filtered(&self, original_id: u32) -> Option<u32> {
        let filtered = self.original_to_filtered[original_id as usize];
        if filtered == u32::MAX {
            None
        } else {
            Some(filtered)
        }
    }
}

pub struct FilteredEbgFile;

impl FilteredEbgFile {
    /// Write filtered EBG to file
    pub fn write<P: AsRef<Path>>(path: P, data: &FilteredEbg) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path.as_ref())?);
        let mut crc_digest = crc::Digest::new();

        // Header (64 bytes)
        // magic(4) + version(2) + mode(1) + reserved(1) + n_filtered(4) + n_arcs(8) + n_original(4) + sha(32) + padding(8)
        let header = [
            &MAGIC.to_le_bytes()[..],
            &VERSION.to_le_bytes()[..],
            &[data.mode as u8, 0u8][..],
            &data.n_filtered_nodes.to_le_bytes()[..],
            &data.n_filtered_arcs.to_le_bytes()[..],
            &data.n_original_nodes.to_le_bytes()[..],
            &data.inputs_sha[..],
            &[0u8; 8][..],
        ]
        .concat();

        writer.write_all(&header)?;
        crc_digest.update(&header);

        // Offsets
        for &off in &data.offsets {
            let bytes = off.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Heads
        for &h in &data.heads {
            let bytes = h.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Original arc indices
        for &idx in &data.original_arc_idx {
            let bytes = idx.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // filtered_to_original
        for &orig in &data.filtered_to_original {
            let bytes = orig.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // original_to_filtered
        for &filt in &data.original_to_filtered {
            let bytes = filt.to_le_bytes();
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

    /// Read filtered EBG from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<FilteredEbg> {
        let mut reader = BufReader::new(File::open(path.as_ref())?);
        let mut crc_digest = crc::Digest::new();

        // Read header
        let mut header = [0u8; 64];
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

        let mode = match header[6] {
            0 => Mode::Car,
            1 => Mode::Bike,
            2 => Mode::Foot,
            m => anyhow::bail!("Invalid mode: {}", m),
        };

        let n_filtered_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_filtered_arcs = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let n_original_nodes = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);

        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[24..56]);

        // Read offsets
        let mut offsets = Vec::with_capacity(n_filtered_nodes as usize + 1);
        for _ in 0..=n_filtered_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            offsets.push(u64::from_le_bytes(buf));
        }

        // Read heads
        let mut heads = Vec::with_capacity(n_filtered_arcs as usize);
        for _ in 0..n_filtered_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            heads.push(u32::from_le_bytes(buf));
        }

        // Read original_arc_idx
        let mut original_arc_idx = Vec::with_capacity(n_filtered_arcs as usize);
        for _ in 0..n_filtered_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            original_arc_idx.push(u32::from_le_bytes(buf));
        }

        // Read filtered_to_original
        let mut filtered_to_original = Vec::with_capacity(n_filtered_nodes as usize);
        for _ in 0..n_filtered_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            filtered_to_original.push(u32::from_le_bytes(buf));
        }

        // Read original_to_filtered
        let mut original_to_filtered = Vec::with_capacity(n_original_nodes as usize);
        for _ in 0..n_original_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            original_to_filtered.push(u32::from_le_bytes(buf));
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in filtered_ebg: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(FilteredEbg {
            mode,
            n_filtered_nodes,
            n_filtered_arcs,
            n_original_nodes,
            inputs_sha,
            offsets,
            heads,
            original_arc_idx,
            filtered_to_original,
            original_to_filtered,
        })
    }
}
