//! filtered_ebg.<mode> format - Mode-filtered EBG for per-mode CCH
//!
//! Stores the filtered subgraph containing only mode-accessible nodes and transitions.
//! Used by Step 6/7/8 to build per-mode CCH hierarchies.
//!
//! # Zero-copy reader (#152)
//!
//! Layout: header(64 bytes) | offsets((n_filt+1) × u64) | heads
//! (n_arcs × u32) | original_arc_idx (n_arcs × u32) | filtered_to_original
//! (n_filt × u32) | original_to_filtered (n_orig × u32) | footer(16 bytes).
//!
//! - The container guarantees 8-byte section alignment.
//! - The 64-byte header keeps the offsets u64 array u64-aligned.
//! - Every subsequent array is u32, which only needs 4-byte alignment;
//!   any cursor that has consumed a multiple of 4 bytes (which all
//!   prior arrays do) is sufficient.

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use super::crc;
use super::mmap::ArcCow;
use crate::profile_abi::Mode;

const MAGIC: u32 = 0x46454247; // "FEBG" = Filtered EBG
const VERSION: u16 = 1;
const HEADER_LEN: usize = 64;
const FOOTER_LEN: usize = 16;

/// Filtered EBG for a specific mode
#[derive(Debug)]
pub struct FilteredEbg {
    pub mode: Mode,
    pub n_filtered_nodes: u32,
    pub n_filtered_arcs: u64,
    pub n_original_nodes: u32,
    pub inputs_sha: [u8; 32],

    // CSR in filtered space.
    //
    // Each `ArcCow<T>` is heap-owned when built in memory or read from a
    // plain file, and is an `Arc<Mmap>`-backed zero-copy view when loaded
    // from a container section via [`FilteredEbgFile::read_from_mmap_unverified`].
    // Dropping the struct decrements the mmap's strong count so the kernel
    // can reclaim the pages once every consumer releases its handle (#296).
    pub offsets: ArcCow<u64>,          // n_filtered_nodes + 1
    pub heads: ArcCow<u32>,            // n_filtered_arcs (filtered node IDs)
    pub original_arc_idx: ArcCow<u32>, // n_filtered_arcs

    // Node ID mappings
    pub filtered_to_original: ArcCow<u32>, // n_filtered_nodes
    pub original_to_filtered: ArcCow<u32>, // n_original_nodes (u32::MAX if not in filtered)
}

impl FilteredEbg {
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
        let mode_bit = mode.bit();

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

        // No SCC filtering — keep all accessible nodes.
        // Dead-end stubs and one-way fragments remain routable.
        // The query handler returns "no route" for unreachable pairs.
        Self {
            mode,
            n_filtered_nodes: n_filtered as u32,
            n_filtered_arcs: heads.len() as u64,
            n_original_nodes,
            inputs_sha,
            offsets: ArcCow::from_vec(offsets),
            heads: ArcCow::from_vec(heads),
            original_arc_idx: ArcCow::from_vec(original_arc_idx),
            filtered_to_original: ArcCow::from_vec(filtered_to_original),
            original_to_filtered: ArcCow::from_vec(original_to_filtered),
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

// `largest_scc_filter` was removed when SCC filtering was disabled
// (see comment above on `Self::build_with_arc_filter`). All accessible
// nodes are kept; dead-end stubs and one-way fragments remain routable
// and the query handler returns "no route" for unreachable pairs.

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
            &[data.mode.0, 0u8][..],
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
        for &off in data.offsets.iter() {
            let bytes = off.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Heads
        for &h in data.heads.iter() {
            let bytes = h.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Original arc indices
        for &idx in data.original_arc_idx.iter() {
            let bytes = idx.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // filtered_to_original
        for &orig in data.filtered_to_original.iter() {
            let bytes = orig.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // original_to_filtered
        for &filt in data.original_to_filtered.iter() {
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
        Self::read_from_reader(BufReader::new(File::open(path.as_ref())?))
    }

    pub fn read_from_bytes(bytes: &[u8]) -> Result<FilteredEbg> {
        Self::read_from_reader(std::io::Cursor::new(bytes))
    }

    fn read_from_reader<R: Read>(mut reader: R) -> Result<FilteredEbg> {
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

        anyhow::ensure!(
            (header[6] as usize) < crate::profile_abi::MAX_MODES,
            "Invalid mode: {}",
            header[6]
        );
        let mode = Mode(header[6]);

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
            offsets: ArcCow::from_vec(offsets),
            heads: ArcCow::from_vec(heads),
            original_arc_idx: ArcCow::from_vec(original_arc_idx),
            filtered_to_original: ArcCow::from_vec(filtered_to_original),
            original_to_filtered: ArcCow::from_vec(original_to_filtered),
        })
    }

    /// Zero-copy reader for `'static` byte slices (mmap-backed
    /// container sections). Reinterprets the body arrays as borrowed
    /// slices into the mapping; CRC is verified before returning.
    ///
    /// Layout (#152):
    ///   header(64) | offsets((n_filt+1) × u64)
    ///             | heads(n_arcs × u32)
    ///             | original_arc_idx(n_arcs × u32)
    ///             | filtered_to_original(n_filt × u32)
    ///             | original_to_filtered(n_orig × u32)
    ///             | footer(16)
    ///
    /// 8-byte section alignment guaranteed by the container; the
    /// 64-byte header keeps the offsets u64 array aligned. Every
    /// subsequent u32 array only needs 4-byte alignment.
    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<FilteredEbg> {
        Self::read_from_bytes_zero_copy_with_cold(bytes).map(|(out, _)| out)
    }

    /// Zero-copy reader that additionally returns the byte range of
    /// the build-time-only cold prefix (`offsets`, `heads`,
    /// `original_arc_idx`). Callers (`server/state.rs`) can pass this
    /// range to `madvise(DONTNEED)` so the cold pages drop from RSS.
    ///
    /// Historically the returned slices were `Cow::Borrowed` into the
    /// mapping. After #296, this path copies into owned `Vec`s so the
    /// returned `FilteredEbg` does not pin the mapping; the byte range
    /// is still safe to `madvise(DONTNEED)` once the copies are made.
    /// Production zero-copy lives on [`Self::read_from_mmap_unverified`].
    ///
    /// Hot serve-time arrays (`filtered_to_original`,
    /// `original_to_filtered`) live AFTER the cold prefix and are
    /// never advised away.
    pub fn read_from_bytes_zero_copy_with_cold(
        bytes: &'static [u8],
    ) -> Result<(FilteredEbg, &'static [u8])> {
        anyhow::ensure!(
            bytes.len() >= HEADER_LEN + FOOTER_LEN,
            "filtered_ebg too short for header+footer: {} bytes",
            bytes.len()
        );
        debug_assert_eq!(
            bytes.as_ptr() as usize % 8,
            0,
            "filtered_ebg section start must be 8-byte aligned"
        );

        let header = &bytes[..HEADER_LEN];
        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        anyhow::ensure!(
            magic == MAGIC,
            "Invalid magic in filtered_ebg: expected 0x{:08X}, got 0x{:08X}",
            MAGIC,
            magic
        );
        let version = u16::from_le_bytes([header[4], header[5]]);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported filtered_ebg version {version}, expected {VERSION}",
        );
        anyhow::ensure!(
            (header[6] as usize) < crate::profile_abi::MAX_MODES,
            "Invalid mode in filtered_ebg: {}",
            header[6]
        );
        let mode = Mode(header[6]);

        let n_filtered_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_filtered_arcs = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let n_original_nodes = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[24..56]);

        let n_filt = n_filtered_nodes as usize;
        let n_orig = n_original_nodes as usize;
        let n_arcs = usize::try_from(n_filtered_arcs)
            .map_err(|_| anyhow::anyhow!("filtered_ebg n_arcs > usize::MAX"))?;

        let offsets_bytes = (n_filt + 1)
            .checked_mul(8)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg offsets size overflow"))?;
        let heads_bytes = n_arcs
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg heads size overflow"))?;
        let oai_bytes = heads_bytes;
        let f2o_bytes = n_filt
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg f2o size overflow"))?;
        let o2f_bytes = n_orig
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg o2f size overflow"))?;

        let off_start = HEADER_LEN;
        let off_end = off_start + offsets_bytes;
        let heads_end = off_end + heads_bytes;
        let oai_end = heads_end + oai_bytes;
        let f2o_end = oai_end + f2o_bytes;
        let o2f_end = f2o_end + o2f_bytes;
        anyhow::ensure!(
            bytes.len() == o2f_end + FOOTER_LEN,
            "filtered_ebg length mismatch: declared {}, expected body+footer {}",
            bytes.len(),
            o2f_end + FOOTER_LEN
        );

        let offsets: &'static [u64] = bytemuck::cast_slice(&bytes[off_start..off_end]);
        let heads: &'static [u32] = bytemuck::cast_slice(&bytes[off_end..heads_end]);
        let original_arc_idx: &'static [u32] = bytemuck::cast_slice(&bytes[heads_end..oai_end]);
        let filtered_to_original: &'static [u32] = bytemuck::cast_slice(&bytes[oai_end..f2o_end]);
        let original_to_filtered: &'static [u32] = bytemuck::cast_slice(&bytes[f2o_end..o2f_end]);

        // CRC over header + body
        let mut crc_digest = crc::Digest::new();
        crc_digest.update(header);
        crc_digest.update(&bytes[off_start..o2f_end]);
        let computed = crc_digest.finalize();
        let footer = &bytes[o2f_end..o2f_end + FOOTER_LEN];
        let stored = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed == stored,
            "CRC64 mismatch in filtered_ebg: computed 0x{:016X}, stored 0x{:016X}",
            computed,
            stored
        );

        let cold = &bytes[off_start..oai_end];
        // Legacy zero-copy path: kept for tests and the back-compat
        // `server/state.rs` fallback that still hands us `'static` bytes
        // (containers that haven't migrated to `Arc<Mmap>` plumbing).
        // We copy into owned `Vec`s so the on-disk → in-memory shape
        // matches the post-#296 `ArcCow<T>` field type; the
        // `Arc<Mmap>`-backed un-leak path is
        // [`Self::read_from_mmap_unverified`].
        let parsed = FilteredEbg {
            mode,
            n_filtered_nodes,
            n_filtered_arcs,
            n_original_nodes,
            inputs_sha,
            offsets: ArcCow::from_vec(offsets.to_vec()),
            heads: ArcCow::from_vec(heads.to_vec()),
            original_arc_idx: ArcCow::from_vec(original_arc_idx.to_vec()),
            filtered_to_original: ArcCow::from_vec(filtered_to_original.to_vec()),
            original_to_filtered: ArcCow::from_vec(original_to_filtered.to_vec()),
        };
        Ok((parsed, cold))
    }

    /// Production mmap-backed reader (#296). Holds an `Arc<Mmap>`
    /// clone for the returned struct's lifetime — when the struct
    /// drops, the strong count decreases. Once every clone drops, the
    /// `Mmap` drops, `munmap` fires, and the kernel reclaims the
    /// pages. This is the un-leak counterpart to
    /// [`Self::read_from_bytes_zero_copy_with_cold`].
    ///
    /// `byte_offset` and `byte_len` are the position and length of the
    /// section within the container, as recorded in the directory
    /// entry. CRC walking is the caller's responsibility (typically
    /// driven through the lazy CRC layer before this call) — the
    /// `_unverified` suffix matches the convention from the other
    /// mmap-backed readers.
    ///
    /// Layout (#152), reproduced for the offset arithmetic below:
    ///   header(64) | offsets((n_filt+1) × u64)
    ///             | heads(n_arcs × u32)
    ///             | original_arc_idx(n_arcs × u32)
    ///             | filtered_to_original(n_filt × u32)
    ///             | original_to_filtered(n_orig × u32)
    ///             | footer(16)
    ///
    /// Container guarantees 8-byte section alignment, so the offsets
    /// `u64` array is u64-aligned at `byte_offset + HEADER_LEN`. Every
    /// subsequent array is `u32`, only needing 4-byte alignment, which
    /// any cursor that has consumed a multiple of 4 bytes (all prior
    /// arrays do) satisfies.
    pub fn read_from_mmap_unverified(
        mmap: Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> Result<FilteredEbg> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "filtered_ebg section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let bytes = &mmap[byte_offset..byte_offset + byte_len];
        anyhow::ensure!(
            bytes.len() >= HEADER_LEN + FOOTER_LEN,
            "filtered_ebg too short for header+footer: {} bytes",
            bytes.len()
        );

        let header = &bytes[..HEADER_LEN];
        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        anyhow::ensure!(
            magic == MAGIC,
            "Invalid magic in filtered_ebg: expected 0x{:08X}, got 0x{:08X}",
            MAGIC,
            magic
        );
        let version = u16::from_le_bytes([header[4], header[5]]);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported filtered_ebg version {version}, expected {VERSION}",
        );
        anyhow::ensure!(
            (header[6] as usize) < crate::profile_abi::MAX_MODES,
            "Invalid mode in filtered_ebg: {}",
            header[6]
        );
        let mode = Mode(header[6]);

        let n_filtered_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_filtered_arcs = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let n_original_nodes = u32::from_le_bytes([header[20], header[21], header[22], header[23]]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[24..56]);

        let n_filt = n_filtered_nodes as usize;
        let n_orig = n_original_nodes as usize;
        let n_arcs = usize::try_from(n_filtered_arcs)
            .map_err(|_| anyhow::anyhow!("filtered_ebg n_arcs > usize::MAX"))?;

        let offsets_n = n_filt
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg offsets count overflow"))?;
        let offsets_bytes = offsets_n
            .checked_mul(8)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg offsets size overflow"))?;
        let heads_bytes = n_arcs
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg heads size overflow"))?;
        let oai_bytes = heads_bytes;
        let f2o_bytes = n_filt
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg f2o size overflow"))?;
        let o2f_bytes = n_orig
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg o2f size overflow"))?;

        // Container-absolute byte offsets of each sub-array.
        let offsets_off = byte_offset
            .checked_add(HEADER_LEN)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg offsets offset overflow"))?;
        let heads_off = offsets_off
            .checked_add(offsets_bytes)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg heads offset overflow"))?;
        let oai_off = heads_off
            .checked_add(heads_bytes)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg oai offset overflow"))?;
        let f2o_off = oai_off
            .checked_add(oai_bytes)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg f2o offset overflow"))?;
        let o2f_off = f2o_off
            .checked_add(f2o_bytes)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg o2f offset overflow"))?;
        let body_end = o2f_off
            .checked_add(o2f_bytes)
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg body end offset overflow"))?;

        let expected = HEADER_LEN
            .checked_add(offsets_bytes)
            .and_then(|n| n.checked_add(heads_bytes))
            .and_then(|n| n.checked_add(oai_bytes))
            .and_then(|n| n.checked_add(f2o_bytes))
            .and_then(|n| n.checked_add(o2f_bytes))
            .and_then(|n| n.checked_add(FOOTER_LEN))
            .ok_or_else(|| anyhow::anyhow!("filtered_ebg section size overflow"))?;
        anyhow::ensure!(
            byte_len == expected,
            "filtered_ebg length mismatch: declared {byte_len}, expected {expected}",
        );

        let offsets = ArcCow::<u64>::from_mmap(Arc::clone(&mmap), offsets_off, offsets_n)?;
        let heads = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), heads_off, n_arcs)?;
        let original_arc_idx = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), oai_off, n_arcs)?;
        let filtered_to_original = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), f2o_off, n_filt)?;
        let original_to_filtered = ArcCow::<u32>::from_mmap(mmap, o2f_off, n_orig)?;

        // Sanity: body_end matches what we computed independently from
        // `expected`. Cheap check; guards against arithmetic drift if
        // the layout ever changes.
        debug_assert_eq!(body_end, byte_offset + expected - FOOTER_LEN);

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
