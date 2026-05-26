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
//!
//! # Format version 4 (#151)
//!
//! v4 enables the zero-copy reader to mmap a per-mode topology straight
//! out of `butterfly.dat` without any heap copy of the body arrays. Two
//! changes vs v3:
//!
//! - The header is 80 bytes (was 76). The four extra bytes are a
//!   `reserved` u32 between `n_nodes` and `n_shortcuts`. They pad the
//!   header to a u64 boundary, so the first u64 array (`up_offsets`)
//!   starts u64-aligned regardless of where the section sits on disk.
//!
//! - Every variable-length `[u32]` array (`up_targets`, `up_middle`,
//!   `down_targets`, `down_middle`, `rank_to_filtered`) is followed by
//!   0 or 4 zero bytes so the section cursor advances to the next u64
//!   boundary. Without that, when `n_up_edges` happens to be odd (true
//!   for car/truck/foot on Belgium, false for bike) the trailing u64
//!   sections would land on a u32-aligned offset and the zero-copy
//!   reader's `bytemuck::cast_slice::<u8, u64>` would fail at runtime.
//!
//! Pad bytes are part of the body and are included in the CRC.
//!
//! v3/v4 files are not readable by this build: the writer of step7
//! emits v5 only, and the reader bails on the version mismatch with a
//! hint to rerun `butterfly-route step7-contract`. The serve-time RSS
//! win unlocked by zero-copy is large enough (≈ 3-5 GB on Belgium)
//! that supporting legacy layouts indefinitely is a footgun.
//!
//! # Format version 5 (#352)
//!
//! v5 packs the `up_middle` / `down_middle` rank arrays at the narrowest
//! width that holds the data — `u16` (`< 65 535` ranks), `u24`
//! (`< 16 777 215`), or `u32` otherwise. Width is picked per-direction.
//! The `u32::MAX` "no middle" sentinel maps to the per-width sentinel
//! (`u16::MAX`, `U24_SENTINEL`) on encode, same convention as
//! [`WeightArray`]. Belgium ranks max around 2.5 M filtered nodes per
//! mode so middles compress to u24 cleanly — ~140 MB saved across the
//! four-mode topology. Europe-scale (DE/FR/IT) car-mode might exceed
//! `2²⁴ = 16 777 216`; the picker falls back to u32 automatically.
//!
//! Header byte 12 carries the per-direction width codes:
//!   bits 0..=1 = up_middle width (00=u32, 01=u16, 10=u24)
//!   bits 2..=3 = down_middle width (same encoding)
//!   bits 4..=7 = reserved (reader rejects non-zero)
//!
//! Bytes 13..=15 stay reserved-zero (reader rejects non-zero).

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use super::bitset::BitsetField;
use super::cch_weights::{WeightArray, WeightWidth};
use super::crc;
use super::mmap::ArcCow;

const MAGIC: u32 = 0x43434854; // "CCHT"
const VERSION: u32 = 5; // Version 5 (#352): u16/u24/u32 width-picked middles via WeightArray
const HEADER_LEN: usize = 80;
const FOOTER_LEN: usize = 16;

const MIDDLE_WIDTH_CODE_U32: u8 = 0;
const MIDDLE_WIDTH_CODE_U16: u8 = 1;
const MIDDLE_WIDTH_CODE_U24: u8 = 2;
const MIDDLE_WIDTH_CODE_MASK: u8 = 0b0000_0011;
const MIDDLE_DOWN_SHIFT: u8 = 2;
const MIDDLE_BYTE12_KNOWN_MASK: u8 = 0b0000_1111;

/// Number of zero bytes to write after a `[u32]` slice of length `n` to
/// advance the section cursor to the next u64 boundary.
#[inline]
const fn u32_pad_to_u64(n: usize) -> usize {
    (n & 1) * 4
}

/// Number of zero pad bytes to advance from a packed middle data span
/// (`data_bytes`) to the next u64 boundary.
#[inline]
const fn middle_pad_to_u64(data_bytes: usize) -> usize {
    (8 - (data_bytes & 7)) & 7
}

fn pick_middle_width(slice: &[u32]) -> WeightWidth {
    let mut max_val: u32 = 0;
    for &v in slice {
        if v == u32::MAX {
            continue;
        }
        if v > max_val {
            max_val = v;
        }
    }
    if max_val < (u16::MAX as u32) {
        WeightWidth::U16
    } else if max_val < super::cch_weights::U24_SENTINEL {
        WeightWidth::U24
    } else {
        WeightWidth::U32
    }
}

#[inline]
const fn width_code_from_weight_width(w: WeightWidth) -> u8 {
    match w {
        WeightWidth::U16 => MIDDLE_WIDTH_CODE_U16,
        WeightWidth::U24 => MIDDLE_WIDTH_CODE_U24,
        WeightWidth::U32 => MIDDLE_WIDTH_CODE_U32,
    }
}

fn weight_width_from_code(code: u8) -> Result<WeightWidth> {
    match code {
        MIDDLE_WIDTH_CODE_U32 => Ok(WeightWidth::U32),
        MIDDLE_WIDTH_CODE_U16 => Ok(WeightWidth::U16),
        MIDDLE_WIDTH_CODE_U24 => Ok(WeightWidth::U24),
        _ => anyhow::bail!("Unknown cch.topo middle width code: {code}"),
    }
}

fn encode_middles_to_bytes(slice: &[u32], width: WeightWidth) -> Vec<u8> {
    match width {
        WeightWidth::U32 => {
            let mut out = Vec::with_capacity(4 * slice.len());
            for &v in slice {
                out.extend_from_slice(&v.to_le_bytes());
            }
            out
        }
        WeightWidth::U16 => {
            let mut out = Vec::with_capacity(2 * slice.len());
            for &v in slice {
                let v16: u16 = if v == u32::MAX { u16::MAX } else { v as u16 };
                out.extend_from_slice(&v16.to_le_bytes());
            }
            out
        }
        WeightWidth::U24 => {
            let mut out = Vec::with_capacity(3 * slice.len());
            for &v in slice {
                let packed: u32 = if v == u32::MAX {
                    super::cch_weights::U24_SENTINEL
                } else {
                    v
                };
                out.push((packed & 0xFF) as u8);
                out.push(((packed >> 8) & 0xFF) as u8);
                out.push(((packed >> 16) & 0xFF) as u8);
            }
            out
        }
    }
}

fn weight_array_from_bytes(bytes: Vec<u8>, n: usize, width: WeightWidth) -> WeightArray {
    debug_assert_eq!(
        bytes.len(),
        n * width.bytes_per_entry(),
        "byte/entry mismatch"
    );
    match width {
        WeightWidth::U32 => {
            let v: Vec<u32> = bytes
                .chunks_exact(4)
                .map(|c| u32::from_le_bytes(c.try_into().unwrap()))
                .collect();
            WeightArray::from_vec_u32(v)
        }
        WeightWidth::U16 => {
            let v: Vec<u16> = bytes
                .chunks_exact(2)
                .map(|c| u16::from_le_bytes(c.try_into().unwrap()))
                .collect();
            WeightArray::from_vec_u16(v)
        }
        WeightWidth::U24 => WeightArray::from_u24_bytes(bytes, n),
    }
}

fn mmap_weight_array(
    mmap: &Arc<memmap2::Mmap>,
    abs_byte_off: usize,
    n: usize,
    width: WeightWidth,
) -> Result<WeightArray> {
    match width {
        WeightWidth::U32 => {
            let arc = ArcCow::<u32>::from_mmap(Arc::clone(mmap), abs_byte_off, n)?;
            Ok(WeightArray::from_arccow_u32(arc))
        }
        WeightWidth::U16 => {
            let arc = ArcCow::<u16>::from_mmap(Arc::clone(mmap), abs_byte_off, n)?;
            Ok(WeightArray::from_arccow_u16(arc))
        }
        WeightWidth::U24 => {
            let arc = ArcCow::<u8>::from_mmap(Arc::clone(mmap), abs_byte_off, n * 3)?;
            Ok(WeightArray::from_arccow_u24(arc, n))
        }
    }
}

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

    // Upward graph in CSR format (indexed by rank).
    // For node at rank r, upward neighbors have rank > r.
    // `ArcCow` variants: `Owned(Vec<T>)` for writers / builders / unit
    // tests, `Mmap { Arc<Mmap>, .. }` for the zero-copy server boot
    // path (#296). The `Arc<Mmap>` strong-count keeps the mapping
    // alive while the struct lives; dropping the struct lets the
    // kernel reclaim the pages once the last clone drops.
    pub up_offsets: ArcCow<u64>, // n_nodes + 1, indexed by rank
    pub up_targets: ArcCow<u32>, // Rank positions of targets
    /// Bit-packed: bit `i` is true iff edge `i` is a shortcut. Use
    /// `up_is_shortcut.bit(i)` instead of indexing — see #147.
    pub up_is_shortcut: BitsetField,
    /// Rank position of middle node (`u32::MAX` if original). Stored as
    /// a width-picked [`WeightArray`] (u16/u24/u32) so the disk and
    /// mmap-resident representation tracks the rank space (#352). The
    /// `u32::MAX` "no middle" sentinel maps to the per-width sentinel
    /// (`u16::MAX`, `U24_SENTINEL`) via [`WeightArray::get`].
    pub up_middle: WeightArray,

    // Downward graph in CSR format (indexed by rank)
    // For node at rank r, downward neighbors have rank < r
    pub down_offsets: ArcCow<u64>,
    pub down_targets: ArcCow<u32>,
    pub down_is_shortcut: BitsetField,
    pub down_middle: WeightArray,

    // Mapping from rank position to filtered node ID
    // rank_to_filtered[rank] = filtered_id
    // Used for geometry lookup and path unpacking
    pub rank_to_filtered: ArcCow<u32>,
}

pub struct CchTopoFile;

impl CchTopoFile {
    /// Write a v4 cch.topo file.
    ///
    /// # Format (v4, #151)
    ///
    /// Header is 80 bytes (was 76 in v3), every variable-length `[u32]`
    /// array is padded with up to 4 zero bytes so the next `[u64]` array
    /// (or the file footer) starts at a u64 boundary. The padding bytes
    /// are part of the body and are included in the CRC. Combined with
    /// the container's per-section 8-byte alignment, this lets the
    /// zero-copy reader call `bytemuck::cast_slice` on every body array
    /// regardless of `n_up_edges` / `n_down_edges` parity.
    ///
    /// # Migration
    ///
    /// v4 is a hard cut: v3 step7 outputs are rejected with an error
    /// pointing at `step7-contract`. The serve-time RSS win unlocked by
    /// zero-copy is large enough (≈ 3-5 GB on Belgium) that supporting
    /// the legacy layout indefinitely is a footgun.
    pub fn write<P: AsRef<Path>>(path: P, data: &CchTopo) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Materialise middles as `Vec<u32>` once for the width picker
        // and body encoder. Avoids repeated traversals of mmap-backed
        // entries per direction.
        let up_middle_vec: Vec<u32> = data.up_middle.iter().collect();
        let down_middle_vec: Vec<u32> = data.down_middle.iter().collect();
        let up_width = pick_middle_width(&up_middle_vec);
        let down_width = pick_middle_width(&down_middle_vec);
        let byte12: u8 = width_code_from_weight_width(up_width)
            | (width_code_from_weight_width(down_width) << MIDDLE_DOWN_SHIFT);
        debug_assert_eq!(
            byte12 & !MIDDLE_BYTE12_KNOWN_MASK,
            0,
            "width-code encoding produced reserved-bit pollution"
        );

        // -------- Header (80 bytes, u64-aligned) -------------------------
        // [ 0.. 4]  magic "CCHT"
        // [ 4.. 8]  version u32 = 5
        // [ 8..12]  n_nodes u32
        // [12]      middle-width-codes byte (see MIDDLE_*_CODE constants)
        // [13..16]  reserved (zero, reader rejects non-zero)
        // [16..24]  n_shortcuts u64
        // [24..32]  n_original_arcs u64
        // [32..40]  n_up_edges u64
        // [40..48]  n_down_edges u64
        // [48..80]  sha256[32]
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let n_nodes_bytes = data.n_nodes.to_le_bytes();
        let mut reserved_bytes = [0u8; 4];
        reserved_bytes[0] = byte12;
        let n_shortcuts_bytes = data.n_shortcuts.to_le_bytes();
        let n_original_bytes = data.n_original_arcs.to_le_bytes();
        let n_up_edges = data.up_offsets.last().copied().unwrap_or(0);
        let n_down_edges = data.down_offsets.last().copied().unwrap_or(0);
        let n_up_bytes = n_up_edges.to_le_bytes();
        let n_down_bytes = n_down_edges.to_le_bytes();

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&n_nodes_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_shortcuts_bytes)?;
        writer.write_all(&n_original_bytes)?;
        writer.write_all(&n_up_bytes)?;
        writer.write_all(&n_down_bytes)?;
        writer.write_all(&data.inputs_sha)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&n_nodes_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_shortcuts_bytes);
        crc_digest.update(&n_original_bytes);
        crc_digest.update(&n_up_bytes);
        crc_digest.update(&n_down_bytes);
        crc_digest.update(&data.inputs_sha);

        // Helper that writes a u32 slice and emits up to 4 zero pad bytes
        // so the cursor advances to the next u64 boundary. Pad bytes are
        // part of the body CRC.
        let write_u32_padded = |writer: &mut BufWriter<File>,
                                crc_digest: &mut crc::Digest,
                                slice: &[u32]|
         -> Result<()> {
            for &v in slice {
                let bytes = v.to_le_bytes();
                writer.write_all(&bytes)?;
                crc_digest.update(&bytes);
            }
            let pad = u32_pad_to_u64(slice.len());
            if pad != 0 {
                let zeros = [0u8; 4];
                writer.write_all(&zeros[..pad])?;
                crc_digest.update(&zeros[..pad]);
            }
            Ok(())
        };

        // Helper for the variable-width middles. Encodes at the chosen
        // width then pads to the next u64 boundary so the subsequent
        // `[u64]` section starts u64-aligned. Pad bytes are part of the
        // body CRC.
        let write_middles = |writer: &mut BufWriter<File>,
                             crc_digest: &mut crc::Digest,
                             slice: &[u32],
                             width: WeightWidth|
         -> Result<()> {
            let bytes = encode_middles_to_bytes(slice, width);
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
            let pad = middle_pad_to_u64(bytes.len());
            if pad != 0 {
                let zeros = [0u8; 8];
                writer.write_all(&zeros[..pad])?;
                crc_digest.update(&zeros[..pad]);
            }
            Ok(())
        };

        // -------- Up graph ---------------------------------------------
        // Offsets are u64 — naturally aligned, no padding.
        for &off in data.up_offsets.iter() {
            let bytes = off.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        write_u32_padded(&mut writer, &mut crc_digest, &data.up_targets)?;
        // Bitset on disk (#90 phase 4): pack 64 booleans per u64 so a
        // 192M-edge Belgium build saves ~168 MB on this section alone.
        // u64 words are naturally aligned.
        let up_bits: &[u64] = data.up_is_shortcut.as_words();
        for &word in up_bits {
            let bytes = word.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        write_middles(&mut writer, &mut crc_digest, &up_middle_vec, up_width)?;

        // -------- Down graph -------------------------------------------
        for &off in data.down_offsets.iter() {
            let bytes = off.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        write_u32_padded(&mut writer, &mut crc_digest, &data.down_targets)?;
        let down_bits: &[u64] = data.down_is_shortcut.as_words();
        for &word in down_bits {
            let bytes = word.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }
        write_middles(&mut writer, &mut crc_digest, &down_middle_vec, down_width)?;

        // -------- Rank → filtered mapping ------------------------------
        // No further u64 sections follow, but the v4 format keeps the
        // padding rule so the body length is always a multiple of 8.
        // This keeps the file footer at a u64 boundary.
        write_u32_padded(&mut writer, &mut crc_digest, &data.rank_to_filtered)?;

        // -------- Footer (16 bytes: body_crc, file_crc) ---------------
        let body_crc = crc_digest.finalize();
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read from any `Path`. Convenience wrapper that buffers the file.
    pub fn read<P: AsRef<Path>>(path: P) -> Result<CchTopo> {
        Self::read_from_reader(BufReader::new(File::open(path)?))
    }

    /// Read directly from an in-memory byte slice (e.g. an mmap-backed
    /// section of a `butterfly.dat` container). Same byte format as the
    /// path API; CRC is checked here too.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<CchTopo> {
        Self::read_from_reader(std::io::Cursor::new(bytes))
    }

    fn read_from_reader<R: Read>(mut reader: R) -> Result<CchTopo> {
        let mut crc_digest = crc::Digest::new();

        // -------- Header (80 bytes, v5 #352) — see parse_header ---------
        let mut header = [0u8; HEADER_LEN];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);
        let (
            n_nodes,
            n_shortcuts,
            n_original_arcs,
            n_up_edges,
            n_down_edges,
            inputs_sha,
            up_width,
            down_width,
        ) = parse_header(&header)?;

        // Helper: read `n` little-endian u32s and consume the v4 padding
        // bytes (0 or 4) that follow if `n` is odd. Padding is part of
        // the body CRC.
        let read_u32_padded =
            |reader: &mut R, crc_digest: &mut crc::Digest, n: usize| -> Result<Vec<u32>> {
                let mut out = Vec::with_capacity(n);
                for _ in 0..n {
                    let mut buf = [0u8; 4];
                    reader.read_exact(&mut buf)?;
                    crc_digest.update(&buf);
                    out.push(u32::from_le_bytes(buf));
                }
                let pad = u32_pad_to_u64(n);
                if pad != 0 {
                    let mut zeros = [0u8; 4];
                    reader.read_exact(&mut zeros[..pad])?;
                    crc_digest.update(&zeros[..pad]);
                }
                Ok(out)
            };

        // Helper: read `n` middles at `width` plus 0..=7 trailing pad
        // bytes so the cursor lands on a u64 boundary.
        let read_middles = |reader: &mut R,
                            crc_digest: &mut crc::Digest,
                            n: usize,
                            width: WeightWidth|
         -> Result<WeightArray> {
            let data_bytes = n * width.bytes_per_entry();
            let mut buf = vec![0u8; data_bytes];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            let pad = middle_pad_to_u64(data_bytes);
            if pad != 0 {
                let mut zeros = [0u8; 8];
                reader.read_exact(&mut zeros[..pad])?;
                crc_digest.update(&zeros[..pad]);
            }
            Ok(weight_array_from_bytes(buf, n, width))
        };

        // -------- Up graph ---------------------------------------------
        let mut up_offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            up_offsets.push(u64::from_le_bytes(buf));
        }

        let up_targets = read_u32_padded(&mut reader, &mut crc_digest, n_up_edges)?;

        let n_up_words = n_up_edges.div_ceil(64);
        let mut up_bits = Vec::with_capacity(n_up_words);
        for _ in 0..n_up_words {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            up_bits.push(u64::from_le_bytes(buf));
        }
        let up_is_shortcut = BitsetField::from_owned_words(up_bits, n_up_edges);

        let up_middle = read_middles(&mut reader, &mut crc_digest, n_up_edges, up_width)?;

        // -------- Down graph -------------------------------------------
        let mut down_offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            down_offsets.push(u64::from_le_bytes(buf));
        }

        let down_targets = read_u32_padded(&mut reader, &mut crc_digest, n_down_edges)?;

        let n_down_words = n_down_edges.div_ceil(64);
        let mut down_bits = Vec::with_capacity(n_down_words);
        for _ in 0..n_down_words {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            down_bits.push(u64::from_le_bytes(buf));
        }
        let down_is_shortcut = BitsetField::from_owned_words(down_bits, n_down_edges);

        let down_middle = read_middles(&mut reader, &mut crc_digest, n_down_edges, down_width)?;

        // -------- Rank → filtered mapping ------------------------------
        let rank_to_filtered = read_u32_padded(&mut reader, &mut crc_digest, n_nodes as usize)?;

        // -------- Footer (CRC verification) ----------------------------
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; FOOTER_LEN];
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
            up_offsets: ArcCow::from_vec(up_offsets),
            up_targets: ArcCow::from_vec(up_targets),
            up_is_shortcut,
            up_middle,
            down_offsets: ArcCow::from_vec(down_offsets),
            down_targets: ArcCow::from_vec(down_targets),
            down_is_shortcut,
            down_middle,
            rank_to_filtered: ArcCow::from_vec(rank_to_filtered),
        })
    }

    /// Legacy `'static [u8]` reader — see #147 for the original
    /// zero-copy design and #296 for the un-leak follow-up.
    ///
    /// Historically this constructed a `CchTopo` whose numeric fields
    /// borrowed from the input bytes via `Cow::Borrowed`. With #296
    /// the field type is now [`ArcCow`], which only carries an
    /// `Arc<Mmap>` reference; the `'static` lifetime input has no
    /// safe way to express the same shape, so the body arrays are
    /// copied into owned `Vec`s here. The `*_is_shortcut` bitsets
    /// still borrow the on-disk packed `u64` words directly because
    /// `BitsetField` keeps its own `Cow<'static, [u64]>` — that's a
    /// separately-tracked migration.
    ///
    /// Production loaders should use
    /// [`Self::read_from_mmap_unverified`] instead, which is true
    /// zero-copy and lets the `Arc<Mmap>` drop when the struct does.
    ///
    /// CRC is verified before returning. Section start MUST be 8-byte
    /// aligned (the container writer guarantees this for every section).
    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<CchTopo> {
        Self::read_from_bytes_zero_copy_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes_zero_copy`] but elides the
    /// internal CRC walk over the body bytes.
    ///
    /// Caller MUST guarantee the bytes have already been verified — for
    /// example, the container loader (#160) drives `LazyContainer`
    /// verification before reaching this entry point. Skipping the
    /// per-format CRC here avoids paging the body in twice on the
    /// container load path.
    pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<CchTopo> {
        Self::read_from_bytes_zero_copy_inner(bytes, false)
    }

    fn read_from_bytes_zero_copy_inner(bytes: &'static [u8], verify: bool) -> Result<CchTopo> {
        anyhow::ensure!(
            bytes.len() >= HEADER_LEN + FOOTER_LEN,
            "cch.topo too short for header+footer: {} bytes",
            bytes.len()
        );

        // The container guarantees that every section starts at an
        // 8-byte file offset. Combined with the v4 header being 80 bytes
        // and every variable-length u32 array being padded to a u64
        // boundary, every body slice we cast to `&[u64]` here is
        // guaranteed u64-aligned regardless of n_up_edges / n_down_edges
        // parity. (Same goes for u32 slices, which need 4-byte alignment.)
        debug_assert_eq!(
            bytes.as_ptr() as usize % 8,
            0,
            "cch.topo bytes must be u64-aligned at section start \
             (container writer pads sections to 8-byte boundaries)"
        );

        // ----- Header (80 bytes, v5) -----
        let (
            n_nodes,
            n_shortcuts,
            n_original_arcs,
            n_up_edges,
            n_down_edges,
            inputs_sha,
            up_width,
            down_width,
        ) = parse_header(bytes)?;

        let n_offsets = (n_nodes as usize) + 1;
        let n_up_words = n_up_edges.div_ceil(64);
        let n_down_words = n_down_edges.div_ceil(64);
        let n_up_pad = u32_pad_to_u64(n_up_edges);
        let n_down_pad = u32_pad_to_u64(n_down_edges);
        let n_nodes_pad = u32_pad_to_u64(n_nodes as usize);
        let upm_data_bytes = n_up_edges * up_width.bytes_per_entry();
        let upm_pad = middle_pad_to_u64(upm_data_bytes);
        let dnm_data_bytes = n_down_edges * down_width.bytes_per_entry();
        let dnm_pad = middle_pad_to_u64(dnm_data_bytes);

        // ----- Layout (#352 v5):
        //   header(80)
        // | up_offsets(8 * n_offsets)
        // | up_targets(4 * n_up + n_up_pad)
        // | up_bits(8 * n_up_words)
        // | up_middle(upm_data_bytes + upm_pad)
        // | down_offsets(8 * n_offsets)
        // | down_targets(4 * n_down + n_down_pad)
        // | down_bits(8 * n_down_words)
        // | down_middle(dnm_data_bytes + dnm_pad)
        // | rank_to_filtered(4 * n_nodes + n_nodes_pad)
        // | footer(16)
        let mut cur = HEADER_LEN;

        let upo_end = cur + 8 * n_offsets;
        let up_offsets: &'static [u64] = bytemuck::cast_slice(&bytes[cur..upo_end]);
        cur = upo_end;

        let upt_end = cur + 4 * n_up_edges;
        let up_targets: &'static [u32] = bytemuck::cast_slice(&bytes[cur..upt_end]);
        cur = upt_end + n_up_pad;

        let upb_end = cur + 8 * n_up_words;
        let up_bits_words: &'static [u64] = bytemuck::cast_slice(&bytes[cur..upb_end]);
        cur = upb_end;

        let upm_end = cur + upm_data_bytes;
        let up_middle_bytes: &'static [u8] = &bytes[cur..upm_end];
        cur = upm_end + upm_pad;

        let dno_end = cur + 8 * n_offsets;
        let down_offsets: &'static [u64] = bytemuck::cast_slice(&bytes[cur..dno_end]);
        cur = dno_end;

        let dnt_end = cur + 4 * n_down_edges;
        let down_targets: &'static [u32] = bytemuck::cast_slice(&bytes[cur..dnt_end]);
        cur = dnt_end + n_down_pad;

        let dnb_end = cur + 8 * n_down_words;
        let down_bits_words: &'static [u64] = bytemuck::cast_slice(&bytes[cur..dnb_end]);
        cur = dnb_end;

        let dnm_end = cur + dnm_data_bytes;
        let down_middle_bytes: &'static [u8] = &bytes[cur..dnm_end];
        cur = dnm_end + dnm_pad;

        let rtf_end = cur + 4 * (n_nodes as usize);
        let rank_to_filtered: &'static [u32] = bytemuck::cast_slice(&bytes[cur..rtf_end]);
        cur = rtf_end + n_nodes_pad;

        // ----- CRC verification: all bytes before footer -----
        anyhow::ensure!(
            bytes.len() == cur + FOOTER_LEN,
            "cch.topo length mismatch: declared {}, expected body+footer {}",
            bytes.len(),
            cur + FOOTER_LEN
        );
        if verify {
            let body = &bytes[..cur];
            let computed_crc = {
                let mut d = crc::Digest::new();
                d.update(body);
                d.finalize()
            };
            let footer = &bytes[cur..cur + FOOTER_LEN];
            let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
            anyhow::ensure!(
                computed_crc == stored_crc,
                "CRC64 mismatch in cch.topo: computed 0x{:016X}, stored 0x{:016X}",
                computed_crc,
                stored_crc
            );
        }

        // Test-fixture path: the caller leaked a `Box<[u8]>` so the
        // `'static` bytes outlive the struct. Production loaders no
        // longer go through this — they use
        // [`Self::read_from_mmap_unverified`] which wires the
        // `Arc<Mmap>` strong-count into the returned `ArcCow` fields
        // so the mapping drops cleanly on eviction. We copy the body
        // arrays into owned `Vec`s here so #296 eviction is not
        // silently subverted by stray `'static` references from
        // test/legacy callers. The bitset still borrows its packed
        // words from the leaked `'static` slice; that's a small,
        // bounded amount of memory and `BitsetField` is the
        // separate-issue Cow type.
        Ok(CchTopo {
            n_nodes,
            n_shortcuts,
            n_original_arcs,
            inputs_sha,
            up_offsets: ArcCow::from_vec(up_offsets.to_vec()),
            up_targets: ArcCow::from_vec(up_targets.to_vec()),
            up_is_shortcut: BitsetField::from_borrowed_words(up_bits_words, n_up_edges),
            up_middle: weight_array_from_bytes(up_middle_bytes.to_vec(), n_up_edges, up_width),
            down_offsets: ArcCow::from_vec(down_offsets.to_vec()),
            down_targets: ArcCow::from_vec(down_targets.to_vec()),
            down_is_shortcut: BitsetField::from_borrowed_words(down_bits_words, n_down_edges),
            down_middle: weight_array_from_bytes(
                down_middle_bytes.to_vec(),
                n_down_edges,
                down_width,
            ),
            rank_to_filtered: ArcCow::from_vec(rank_to_filtered.to_vec()),
        })
    }

    /// Production mmap-backed reader (#296). Constructs a `CchTopo`
    /// whose body arrays are zero-copy `ArcCow::Mmap` views over the
    /// supplied `Arc<Mmap>`. Each returned field carries a clone of
    /// the `Arc`, so the mapping stays alive as long as the struct
    /// (or any clone of an individual field) does. Once the
    /// `ServerState` drops, the strong-count falls to zero,
    /// `Mmap::drop` calls `munmap`, and the kernel reclaims the
    /// pages — finally un-leaking the boot-time mapping (#296).
    ///
    /// `byte_offset` and `byte_len` are the position and length of
    /// the section inside the container, as recorded in the
    /// directory. CRC verification is the caller's responsibility
    /// (driven through the lazy CRC layer before this call).
    ///
    /// The bitset `is_shortcut` fields are NOT zero-copy here —
    /// `BitsetField` is itself a `Cow`-based abstraction
    /// (separately tracked) and currently cannot hold an `Arc<Mmap>`
    /// reference. We copy the packed u64 words into owned storage.
    /// The cost is bounded: each bitset is `ceil(n_edges / 64)`
    /// u64 words, i.e. ~1/256 of the u32 target array. For the
    /// Belgium build (~192 M edges) that's ~3 MB total across both
    /// directions vs the multi-GB u32/u64 arrays we keep zero-copy.
    pub fn read_from_mmap_unverified(
        mmap: Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> Result<CchTopo> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "cch.topo section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let bytes = &mmap[byte_offset..byte_offset + byte_len];

        // The container guarantees section starts are 8-byte aligned.
        // The v4 header is 80 bytes (u64-multiple) and every variable-
        // length u32 array is padded to the next u64 boundary, so all
        // u64 sub-slices stay u64-aligned and all u32 sub-slices stay
        // u32-aligned regardless of n_up_edges / n_down_edges parity.
        debug_assert_eq!(
            bytes.as_ptr() as usize % 8,
            0,
            "cch.topo bytes must be u64-aligned at section start \
             (container writer pads sections to 8-byte boundaries)"
        );

        let (
            n_nodes,
            n_shortcuts,
            n_original_arcs,
            n_up_edges,
            n_down_edges,
            inputs_sha,
            up_width,
            down_width,
        ) = parse_header(bytes)?;

        let n_offsets = (n_nodes as usize) + 1;
        let n_up_words = n_up_edges.div_ceil(64);
        let n_down_words = n_down_edges.div_ceil(64);
        let n_up_pad = u32_pad_to_u64(n_up_edges);
        let n_down_pad = u32_pad_to_u64(n_down_edges);
        let n_nodes_pad = u32_pad_to_u64(n_nodes as usize);
        let upm_data_bytes = n_up_edges * up_width.bytes_per_entry();
        let upm_pad = middle_pad_to_u64(upm_data_bytes);
        let dnm_data_bytes = n_down_edges * down_width.bytes_per_entry();
        let dnm_pad = middle_pad_to_u64(dnm_data_bytes);

        // Layout (#352 v5) — see `read_from_bytes_zero_copy_inner`.
        let mut cur = HEADER_LEN;

        let upo_off = byte_offset + cur;
        let upo_len_bytes = 8 * n_offsets;
        cur += upo_len_bytes;

        let upt_off = byte_offset + cur;
        let upt_len_bytes = 4 * n_up_edges;
        cur += upt_len_bytes + n_up_pad;

        let upb_off = byte_offset + cur;
        let upb_len_bytes = 8 * n_up_words;
        cur += upb_len_bytes;

        let upm_off = byte_offset + cur;
        cur += upm_data_bytes + upm_pad;

        let dno_off = byte_offset + cur;
        let dno_len_bytes = 8 * n_offsets;
        cur += dno_len_bytes;

        let dnt_off = byte_offset + cur;
        let dnt_len_bytes = 4 * n_down_edges;
        cur += dnt_len_bytes + n_down_pad;

        let dnb_off = byte_offset + cur;
        let dnb_len_bytes = 8 * n_down_words;
        cur += dnb_len_bytes;

        let dnm_off = byte_offset + cur;
        cur += dnm_data_bytes + dnm_pad;

        let rtf_off = byte_offset + cur;
        let rtf_len_bytes = 4 * (n_nodes as usize);
        cur += rtf_len_bytes + n_nodes_pad;

        let expected = cur + FOOTER_LEN;
        anyhow::ensure!(
            byte_len == expected,
            "cch.topo length mismatch: declared {byte_len}, expected body+footer {expected}",
        );

        // Build the zero-copy ArcCow views. Each call validates
        // alignment + bounds against the mmap; the hot path does no
        // further checks beyond bytemuck::cast_slice. Middles go
        // through `mmap_weight_array` to honour the width code.
        let up_offsets = ArcCow::<u64>::from_mmap(Arc::clone(&mmap), upo_off, n_offsets)?;
        let up_targets = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), upt_off, n_up_edges)?;
        let up_middle = mmap_weight_array(&mmap, upm_off, n_up_edges, up_width)?;
        let down_offsets = ArcCow::<u64>::from_mmap(Arc::clone(&mmap), dno_off, n_offsets)?;
        let down_targets = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), dnt_off, n_down_edges)?;
        let down_middle = mmap_weight_array(&mmap, dnm_off, n_down_edges, down_width)?;
        let rank_to_filtered =
            ArcCow::<u32>::from_mmap(Arc::clone(&mmap), rtf_off, n_nodes as usize)?;

        // Bitsets: BitsetField doesn't yet have an Arc<Mmap> variant
        // (separate issue), so copy the packed words into owned
        // storage. See doc comment for the bounded-cost rationale.
        let up_bits_bytes = &mmap[upb_off..upb_off + upb_len_bytes];
        let up_bits_words: &[u64] = bytemuck::cast_slice(up_bits_bytes);
        let up_is_shortcut = BitsetField::from_owned_words(up_bits_words.to_vec(), n_up_edges);

        let down_bits_bytes = &mmap[dnb_off..dnb_off + dnb_len_bytes];
        let down_bits_words: &[u64] = bytemuck::cast_slice(down_bits_bytes);
        let down_is_shortcut =
            BitsetField::from_owned_words(down_bits_words.to_vec(), n_down_edges);

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

/// `(n_nodes, n_shortcuts, n_original_arcs, n_up_edges, n_down_edges,
/// inputs_sha, up_middle_width, down_middle_width)` — the full set of
/// v5 cch.topo header fields. Named alias to keep clippy off the
/// 8-tuple in `parse_header`.
type HeaderFields = (u32, u64, u64, usize, usize, [u8; 32], WeightWidth, WeightWidth);

/// Parse the 80-byte v5 cch.topo header and return its fields.
/// Shared by the owned, zero-copy, and mmap-backed readers.
///
/// `n_up_edges` / `n_down_edges` are returned as `usize` because every
/// downstream consumer indexes by them.
fn parse_header(bytes: &[u8]) -> Result<HeaderFields> {
    anyhow::ensure!(
        bytes.len() >= HEADER_LEN,
        "cch.topo too short for header: {} bytes",
        bytes.len()
    );
    let h = &bytes[..HEADER_LEN];
    let magic = u32::from_le_bytes(h[0..4].try_into().unwrap());
    anyhow::ensure!(
        magic == MAGIC,
        "Invalid magic: expected 0x{:08X}, got 0x{:08X}",
        MAGIC,
        magic
    );
    let version = u32::from_le_bytes(h[4..8].try_into().unwrap());
    anyhow::ensure!(
        version == VERSION,
        "Unsupported cch.topo format version {}: this build only reads v{}. \
         Regenerate the per-mode topology with `butterfly-route step7-contract` \
         (the v5 layout packs CCH middles at u16/u24/u32 — picked per direction).",
        version,
        VERSION
    );
    let n_nodes = u32::from_le_bytes(h[8..12].try_into().unwrap());
    let byte12 = h[12];
    anyhow::ensure!(
        byte12 & !MIDDLE_BYTE12_KNOWN_MASK == 0,
        "cch.topo header byte 12 has unknown bits set: 0x{:02X} \
         (known mask 0x{:02X}); this build does not understand the format",
        byte12,
        MIDDLE_BYTE12_KNOWN_MASK
    );
    anyhow::ensure!(
        h[13] == 0 && h[14] == 0 && h[15] == 0,
        "cch.topo header bytes 13..=15 must be zero (reserved); got [{}, {}, {}]",
        h[13],
        h[14],
        h[15]
    );
    let up_width = weight_width_from_code(byte12 & MIDDLE_WIDTH_CODE_MASK)?;
    let down_width = weight_width_from_code((byte12 >> MIDDLE_DOWN_SHIFT) & MIDDLE_WIDTH_CODE_MASK)?;
    let n_shortcuts = u64::from_le_bytes(h[16..24].try_into().unwrap());
    let n_original_arcs = u64::from_le_bytes(h[24..32].try_into().unwrap());
    let n_up_edges = u64::from_le_bytes(h[32..40].try_into().unwrap()) as usize;
    let n_down_edges = u64::from_le_bytes(h[40..48].try_into().unwrap()) as usize;
    let mut inputs_sha = [0u8; 32];
    inputs_sha.copy_from_slice(&h[48..80]);
    Ok((
        n_nodes,
        n_shortcuts,
        n_original_arcs,
        n_up_edges,
        n_down_edges,
        inputs_sha,
        up_width,
        down_width,
    ))
}

#[cfg(test)]
mod unverified_path_tests {
    use super::*;

    /// Regression for #161 review items 3-4: the `_unverified` reader
    /// MUST skip the format's body CRC walk. We construct a valid
    /// section, then corrupt the trailing CRC bytes; the verified
    /// reader rejects, but the unverified reader returns the parsed
    /// data because the body itself is intact.
    #[test]
    fn read_from_bytes_zero_copy_unverified_ignores_trailing_crc() {
        // A minimal v4 header whose declared lengths are zero-sized
        // arrays — the simplest valid topology for this test.
        let header_len = HEADER_LEN;
        let mut bytes = vec![0u8; header_len + FOOTER_LEN];
        bytes[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        bytes[4..8].copy_from_slice(&VERSION.to_le_bytes());
        // n_nodes = 1 → one offsets entry (n_nodes + 1 = 2)
        bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
        // n_shortcuts, n_original_arcs = 0
        // n_up_edges = 0
        // n_down_edges = 0
        // Layout: header(80) + 2*u64 up_offsets + 2*u64 down_offsets +
        //         1*u32 rank_to_filtered + 4 pad + footer(16)
        let n_offsets = 2;
        let body_len = 8 * n_offsets + 8 * n_offsets + 4 + 4;
        let total = header_len + body_len + FOOTER_LEN;
        let mut bytes = vec![0u8; total];
        bytes[0..4].copy_from_slice(&MAGIC.to_le_bytes());
        bytes[4..8].copy_from_slice(&VERSION.to_le_bytes());
        bytes[8..12].copy_from_slice(&1u32.to_le_bytes());
        // n_up_edges = 0 (header[32..40]), n_down_edges = 0 (header[40..48])
        // Compute the CRC of the body so the verified path passes too,
        // for the baseline assertion.
        let body = &bytes[..header_len + body_len];
        let mut d = crc::Digest::new();
        d.update(body);
        let body_crc = d.finalize();
        bytes[header_len + body_len..header_len + body_len + 8]
            .copy_from_slice(&body_crc.to_le_bytes());
        // Leak so `'static` slice can be constructed.
        let leaked: &'static [u8] = Box::leak(bytes.clone().into_boxed_slice());
        // Verified path succeeds.
        assert!(
            CchTopoFile::read_from_bytes_zero_copy(leaked).is_ok(),
            "verified path should accept a clean section"
        );

        // Now corrupt the trailing CRC. The verified path must reject;
        // the unverified path must accept (the body bytes are still
        // valid; only the trailing CRC is bogus).
        let mut corrupted = bytes.clone();
        corrupted[header_len + body_len] ^= 0xFF;
        let leaked2: &'static [u8] = Box::leak(corrupted.into_boxed_slice());
        assert!(
            CchTopoFile::read_from_bytes_zero_copy(leaked2).is_err(),
            "verified path should reject corrupted CRC"
        );
        assert!(
            CchTopoFile::read_from_bytes_zero_copy_unverified(leaked2).is_ok(),
            "unverified path should ignore corrupted CRC"
        );
    }
}

/// Pack a `Vec<bool>` into a little-endian `Vec<u64>` bitset.
///
/// Bit `i` of word `i / 64` (LSB-first within each word) is set iff
/// `bools[i] == true`. The output length is `ceil(n / 64)` words.
#[cfg(test)]
fn pack_bools_to_bitset(bools: &[bool]) -> Vec<u64> {
    let n_words = bools.len().div_ceil(64);
    let mut out = vec![0u64; n_words];
    for (i, &b) in bools.iter().enumerate() {
        if b {
            out[i / 64] |= 1u64 << (i % 64);
        }
    }
    out
}

/// Inverse of `pack_bools_to_bitset`.
///
/// `n` is the original boolean count (the bitset may have trailing bits
/// up to a word boundary that are not part of the logical content).
#[cfg(test)]
fn unpack_bitset_to_bools(bits: &[u64], n: usize) -> Vec<bool> {
    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let w = bits[i / 64];
        out.push((w >> (i % 64)) & 1 == 1);
    }
    out
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
            up_offsets: vec![0u64, 1, 2, 3, 3].into(),
            up_targets: vec![1u32, 2, 3].into(),
            up_is_shortcut: BitsetField::from_bools(&[false, false, true]),
            up_middle: vec![u32::MAX, u32::MAX, 1].into(),
            down_offsets: vec![0u64, 0, 1, 2, 3].into(),
            down_targets: vec![0u32, 1, 2].into(),
            down_is_shortcut: BitsetField::from_bools(&[false, false, true]),
            down_middle: vec![u32::MAX, u32::MAX, 1].into(),
            rank_to_filtered: vec![10u32, 20, 30, 40].into(),
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
        assert_eq!(&loaded.up_targets[..], &[1u32, 2, 3]);
        assert_eq!(loaded.up_is_shortcut.len(), 3);
        assert!(!loaded.up_is_shortcut.bit(0));
        assert!(!loaded.up_is_shortcut.bit(1));
        assert!(loaded.up_is_shortcut.bit(2));
        assert_eq!(loaded.up_middle.get(2), 1);
        assert_eq!(&loaded.down_targets[..], &[0u32, 1, 2]);
        assert_eq!(&loaded.rank_to_filtered[..], &[10u32, 20, 30, 40]);
        Ok(())
    }

    #[test]
    fn test_bitset_pack_unpack_roundtrip() {
        // Empty
        assert_eq!(pack_bools_to_bitset(&[]), Vec::<u64>::new());
        assert_eq!(unpack_bitset_to_bools(&[], 0), Vec::<bool>::new());

        // Single word, partial.
        let pattern = vec![true, false, true, false, true, false, true, false];
        let bits = pack_bools_to_bitset(&pattern);
        assert_eq!(bits, vec![0b0101_0101u64]);
        assert_eq!(unpack_bitset_to_bools(&bits, pattern.len()), pattern);

        // Across a word boundary (n = 65).
        let pattern: Vec<bool> = (0..65).map(|i: i32| i % 3 == 0).collect();
        let bits = pack_bools_to_bitset(&pattern);
        assert_eq!(bits.len(), 2);
        assert_eq!(unpack_bitset_to_bools(&bits, 65), pattern);

        // Byte-equivalent layout: writing the disk format and reading it
        // back must reproduce the bool vector.
        let n = 192_000usize;
        let pattern: Vec<bool> = (0..n).map(|i| (i * 7919) % 13 == 0).collect();
        let bits = pack_bools_to_bitset(&pattern);
        let recovered = unpack_bitset_to_bools(&bits, n);
        assert_eq!(recovered, pattern);
        assert_eq!(bits.len(), n.div_ceil(64));
    }

    #[test]
    fn test_bitset_savings() {
        // Concrete sanity check: an N-bit vector takes N/8 bytes packed
        // vs N bytes unpacked. Belgium's ~192M-edge build saves about
        // 168 MB on this section alone (192M/8=24M packed vs 192M raw).
        let n = 192_112_840usize; // Belgium step-7 unified n_up + n_down
        let packed_bytes = n.div_ceil(64) * 8;
        let unpacked_bytes = n;
        let saved = unpacked_bytes - packed_bytes;
        assert!(
            saved > 165_000_000,
            "expected >165 MB savings, got {} bytes",
            saved
        );
    }

    #[test]
    fn test_crc_detects_body_corruption() -> Result<()> {
        let data = make_test_topo();
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;

        // Corrupt a byte at offset 88, inside up_offsets (which starts
        // at offset 80 after the v4 header).
        {
            let mut file = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
            file.seek(SeekFrom::Start(88))?;
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

    /// Synthesize a CchTopo whose `n_up` and `n_down` parities can be
    /// chosen independently. The CSR offsets, targets, middles and
    /// bitsets all stay self-consistent so a write→read round-trip is
    /// byte-for-byte stable. Parity matters for #151 because it controls
    /// whether the v4 writer emits a 4-byte pad after each variable-
    /// length u32 array.
    fn make_topo_with_parity(n_up: usize, n_down: usize, n_nodes: u32) -> CchTopo {
        let nn = n_nodes as usize;

        // Build up-CSR with `n_up` edges spread across `n_nodes` rows;
        // last row gets all the leftover edges.
        let mut up_offsets = Vec::with_capacity(nn + 1);
        let mut up_targets = Vec::with_capacity(n_up);
        for i in 0..nn {
            up_offsets.push(up_targets.len() as u64);
            if i + 1 == nn {
                for j in 0..n_up {
                    up_targets.push(((i + j + 1) as u32) % n_nodes);
                }
            }
        }
        up_offsets.push(up_targets.len() as u64);

        let mut down_offsets = Vec::with_capacity(nn + 1);
        let mut down_targets = Vec::with_capacity(n_down);
        for i in 0..nn {
            down_offsets.push(down_targets.len() as u64);
            if i + 1 == nn {
                for j in 0..n_down {
                    down_targets.push(((i + j + 2) as u32) % n_nodes);
                }
            }
        }
        down_offsets.push(down_targets.len() as u64);

        let up_bools: Vec<bool> = (0..n_up).map(|i| i % 3 == 0).collect();
        let down_bools: Vec<bool> = (0..n_down).map(|i| i % 5 == 0).collect();
        let up_middle: Vec<u32> = (0..n_up).map(|i| (i as u32).wrapping_mul(7)).collect();
        let down_middle: Vec<u32> = (0..n_down).map(|i| (i as u32).wrapping_mul(11)).collect();
        let rank_to_filtered: Vec<u32> = (0..n_nodes).map(|i| i * 13 + 1).collect();

        CchTopo {
            n_nodes,
            n_shortcuts: 0,
            n_original_arcs: (n_up + n_down) as u64,
            inputs_sha: [0xAB; 32],
            up_offsets: up_offsets.into(),
            up_targets: up_targets.into(),
            up_is_shortcut: BitsetField::from_bools(&up_bools),
            up_middle: up_middle.into(),
            down_offsets: down_offsets.into(),
            down_targets: down_targets.into(),
            down_is_shortcut: BitsetField::from_bools(&down_bools),
            down_middle: down_middle.into(),
            rank_to_filtered: rank_to_filtered.into(),
        }
    }

    /// Assert read-back equality field by field.
    fn assert_topo_eq(a: &CchTopo, b: &CchTopo) {
        assert_eq!(a.n_nodes, b.n_nodes);
        assert_eq!(a.n_shortcuts, b.n_shortcuts);
        assert_eq!(a.n_original_arcs, b.n_original_arcs);
        assert_eq!(a.inputs_sha, b.inputs_sha);
        assert_eq!(&a.up_offsets[..], &b.up_offsets[..]);
        assert_eq!(&a.up_targets[..], &b.up_targets[..]);
        assert_eq!(a.up_is_shortcut.len(), b.up_is_shortcut.len());
        for i in 0..a.up_is_shortcut.len() {
            assert_eq!(
                a.up_is_shortcut.bit(i),
                b.up_is_shortcut.bit(i),
                "up bit {i}"
            );
        }
        assert_eq!(a.up_middle.to_vec_u32(), b.up_middle.to_vec_u32());
        assert_eq!(&a.down_offsets[..], &b.down_offsets[..]);
        assert_eq!(&a.down_targets[..], &b.down_targets[..]);
        assert_eq!(a.down_is_shortcut.len(), b.down_is_shortcut.len());
        for i in 0..a.down_is_shortcut.len() {
            assert_eq!(
                a.down_is_shortcut.bit(i),
                b.down_is_shortcut.bit(i),
                "down bit {i}"
            );
        }
        assert_eq!(a.down_middle.to_vec_u32(), b.down_middle.to_vec_u32());
        assert_eq!(&a.rank_to_filtered[..], &b.rank_to_filtered[..]);
    }

    /// v4 acceptance: round-trip works for every combination of edge-
    /// count parities. v3 broke when `n_up` was odd because the trailing
    /// u64 sections landed at a u32-aligned (not u64-aligned) offset.
    #[test]
    fn test_v4_roundtrip_all_parities() -> Result<()> {
        for &n_up in &[0usize, 1, 2, 3, 4, 5, 7, 8] {
            for &n_down in &[0usize, 1, 2, 3, 4, 5, 7, 8] {
                let n_nodes = (n_up.max(n_down) + 1) as u32;
                let data = make_topo_with_parity(n_up, n_down, n_nodes);
                let tmp = NamedTempFile::new()?;
                CchTopoFile::write(tmp.path(), &data)?;
                let loaded = CchTopoFile::read(tmp.path())?;
                assert_topo_eq(&data, &loaded);
            }
        }
        Ok(())
    }

    /// Read the file as a leaked `&'static [u8]` and run the zero-copy
    /// reader over it. This is the path the server actually uses at boot.
    /// The zero-copy reader requires u64-aligned input — match exactly
    /// what `butterfly_dat`'s section padding gives us by leaking an
    /// 8-byte-aligned heap allocation.
    fn read_file_as_static_aligned(path: &Path) -> &'static [u8] {
        let raw = std::fs::read(path).expect("read file");
        // Allocate via Vec<u64> so the start pointer is u64-aligned, then
        // re-borrow as a u8 slice. Box::leak gives us 'static.
        let n_words = raw.len().div_ceil(8);
        let mut words = vec![0u64; n_words];
        let bytes_per_word = 8;
        // Safe byte-level copy via from_raw_parts_mut alternative:
        // iterate by chunks and pack bytes.
        for (i, chunk) in raw.chunks(bytes_per_word).enumerate() {
            let mut buf = [0u8; 8];
            buf[..chunk.len()].copy_from_slice(chunk);
            words[i] = u64::from_le_bytes(buf);
        }
        let leaked: &'static [u64] = Box::leak(words.into_boxed_slice());
        let bytes: &'static [u8] = bytemuck::cast_slice(leaked);
        // Truncate to the original byte length (the last u64 may have
        // trailing padding bytes from the chunked copy).
        &bytes[..raw.len()]
    }

    #[test]
    fn test_v4_zero_copy_all_parities() -> Result<()> {
        for &n_up in &[0usize, 1, 2, 3, 5, 8] {
            for &n_down in &[0usize, 1, 2, 3, 5, 8] {
                let n_nodes = (n_up.max(n_down) + 2) as u32;
                let data = make_topo_with_parity(n_up, n_down, n_nodes);
                let tmp = NamedTempFile::new()?;
                CchTopoFile::write(tmp.path(), &data)?;

                let static_bytes = read_file_as_static_aligned(tmp.path());
                let loaded = CchTopoFile::read_from_bytes_zero_copy(static_bytes)?;
                assert_topo_eq(&data, &loaded);
            }
        }
        Ok(())
    }

    /// v3 files (legacy 76-byte header) must be rejected with a clear
    /// migration message. The error body matters for ops: a server that
    /// silently corrupts v3 data would be a far worse bug than failing
    /// to start.
    #[test]
    fn test_v3_files_rejected_with_migration_hint() -> Result<()> {
        // Hand-write a minimally plausible v3 header (76 bytes) with
        // version=3, then trailing zeros + a bogus CRC. The reader must
        // bail on the version mismatch before it touches any body bytes.
        let mut buf = Vec::new();
        buf.extend_from_slice(&MAGIC.to_le_bytes()); // 4
        buf.extend_from_slice(&3u16.to_le_bytes()); // 2 (v3 used u16)
        buf.extend_from_slice(&0u16.to_le_bytes()); // 2 reserved
        buf.extend_from_slice(&0u32.to_le_bytes()); // 4 n_nodes
        buf.extend_from_slice(&0u64.to_le_bytes()); // 8 n_shortcuts
        buf.extend_from_slice(&0u64.to_le_bytes()); // 8 n_original
        buf.extend_from_slice(&0u64.to_le_bytes()); // 8 n_up
        buf.extend_from_slice(&0u64.to_le_bytes()); // 8 n_down
        buf.extend_from_slice(&[0u8; 32]); // 32 sha
        buf.extend_from_slice(&[0u8; 16]); // footer placeholder

        let tmp = NamedTempFile::new()?;
        std::fs::write(tmp.path(), &buf)?;

        let res = CchTopoFile::read(tmp.path());
        assert!(res.is_err(), "v3 file should be rejected outright");
        let msg = res.unwrap_err().to_string();
        assert!(
            msg.contains("version") && msg.contains("step7-contract"),
            "v3 rejection should hint at step7-contract regen, got: {msg}"
        );
        Ok(())
    }

    /// Sanity-check the v4 header math: 80 bytes, every field at the
    /// declared offset. If anyone perturbs the layout this test fires.
    #[test]
    fn test_v4_header_layout() -> Result<()> {
        let data = make_topo_with_parity(3, 3, 4); // n_up odd → triggers padding too
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;
        let raw = std::fs::read(tmp.path())?;

        assert!(raw.len() >= HEADER_LEN + FOOTER_LEN);
        assert_eq!(
            u32::from_le_bytes(raw[0..4].try_into().unwrap()),
            MAGIC,
            "magic at [0..4]"
        );
        assert_eq!(
            u32::from_le_bytes(raw[4..8].try_into().unwrap()),
            VERSION,
            "version u32 at [4..8]"
        );
        assert_eq!(
            u32::from_le_bytes(raw[8..12].try_into().unwrap()),
            data.n_nodes,
            "n_nodes at [8..12]"
        );
        // v5 (#352): byte 12 carries per-direction middle width codes
        // (bits 0..=3); bits 4..=7 of byte 12 and bytes 13..=15 stay
        // reserved-zero.
        assert_eq!(raw[12] & !MIDDLE_BYTE12_KNOWN_MASK, 0, "byte12 reserved bits");
        assert_eq!(raw[13], 0, "header[13] reserved");
        assert_eq!(raw[14], 0, "header[14] reserved");
        assert_eq!(raw[15], 0, "header[15] reserved");
        assert_eq!(
            u64::from_le_bytes(raw[16..24].try_into().unwrap()),
            data.n_shortcuts,
            "n_shortcuts at [16..24]"
        );
        assert_eq!(
            u64::from_le_bytes(raw[24..32].try_into().unwrap()),
            data.n_original_arcs,
            "n_original at [24..32]"
        );
        assert_eq!(
            u64::from_le_bytes(raw[32..40].try_into().unwrap()),
            data.up_offsets.last().copied().unwrap_or(0),
            "n_up at [32..40]"
        );
        assert_eq!(
            u64::from_le_bytes(raw[40..48].try_into().unwrap()),
            data.down_offsets.last().copied().unwrap_or(0),
            "n_down at [40..48]"
        );
        assert_eq!(&raw[48..80], &data.inputs_sha[..], "sha at [48..80]");
        Ok(())
    }

    /// v5 (#352): when middle ranks all fit in u16, the writer picks
    /// U16 and the round-trip preserves every entry — including the
    /// `u32::MAX` "no middle" sentinel via the per-width sentinel
    /// (`u16::MAX`).
    #[test]
    fn test_v5_picks_u16_for_small_ranks() -> Result<()> {
        let data = CchTopo {
            n_nodes: 4,
            n_shortcuts: 2,
            n_original_arcs: 2,
            inputs_sha: [0xAB; 32],
            up_offsets: vec![0u64, 1, 2, 3, 4].into(),
            up_targets: vec![1u32, 2, 3, 0].into(),
            up_is_shortcut: BitsetField::from_bools(&[false, true, false, true]),
            up_middle: WeightArray::from_vec_u32(vec![u32::MAX, 2, u32::MAX, 1]),
            down_offsets: vec![0u64, 0, 1, 2, 3].into(),
            down_targets: vec![0u32, 1, 2].into(),
            down_is_shortcut: BitsetField::from_bools(&[false, false, true]),
            down_middle: WeightArray::from_vec_u32(vec![u32::MAX, u32::MAX, 1]),
            rank_to_filtered: vec![10u32, 20, 30, 40].into(),
        };
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;
        let raw = std::fs::read(tmp.path())?;
        assert_eq!(
            raw[12] & MIDDLE_BYTE12_KNOWN_MASK,
            MIDDLE_WIDTH_CODE_U16 | (MIDDLE_WIDTH_CODE_U16 << MIDDLE_DOWN_SHIFT),
            "byte12 should encode u16 width for both directions"
        );
        let loaded = CchTopoFile::read(tmp.path())?;
        assert_eq!(
            loaded.up_middle.width(),
            crate::formats::cch_weights::WeightWidth::U16
        );
        assert_eq!(
            loaded.down_middle.width(),
            crate::formats::cch_weights::WeightWidth::U16
        );
        assert_eq!(loaded.up_middle.to_vec_u32(), vec![u32::MAX, 2, u32::MAX, 1]);
        assert_eq!(loaded.down_middle.to_vec_u32(), vec![u32::MAX, u32::MAX, 1]);
        Ok(())
    }

    /// v5 (#352): when middle ranks exceed `u16::MAX` but fit u24, the
    /// writer picks U24 — the typical Belgium-class build (~2.5 M
    /// filtered nodes per mode → u24 covers every entry).
    #[test]
    fn test_v5_picks_u24_for_belgium_class_ranks() -> Result<()> {
        let big_rank: u32 = 100_000; // exceeds u16::MAX = 65 535
        let data = CchTopo {
            n_nodes: 4,
            n_shortcuts: 2,
            n_original_arcs: 2,
            inputs_sha: [0u8; 32],
            up_offsets: vec![0u64, 1, 2, 3, 4].into(),
            up_targets: vec![1u32, 2, 3, 0].into(),
            up_is_shortcut: BitsetField::from_bools(&[true, true, false, false]),
            up_middle: WeightArray::from_vec_u32(vec![big_rank, big_rank + 5, u32::MAX, u32::MAX]),
            down_offsets: vec![0u64, 0, 1, 2, 3].into(),
            down_targets: vec![0u32, 1, 2].into(),
            down_is_shortcut: BitsetField::from_bools(&[false, true, true]),
            down_middle: WeightArray::from_vec_u32(vec![u32::MAX, big_rank, big_rank + 7]),
            rank_to_filtered: vec![10u32, 20, 30, 40].into(),
        };
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;
        let raw = std::fs::read(tmp.path())?;
        assert_eq!(
            raw[12] & MIDDLE_BYTE12_KNOWN_MASK,
            MIDDLE_WIDTH_CODE_U24 | (MIDDLE_WIDTH_CODE_U24 << MIDDLE_DOWN_SHIFT),
            "byte12 should encode u24 width for both directions"
        );
        let loaded = CchTopoFile::read(tmp.path())?;
        assert_eq!(
            loaded.up_middle.width(),
            crate::formats::cch_weights::WeightWidth::U24
        );
        assert_eq!(
            loaded.down_middle.width(),
            crate::formats::cch_weights::WeightWidth::U24
        );
        assert_eq!(
            loaded.up_middle.to_vec_u32(),
            vec![big_rank, big_rank + 5, u32::MAX, u32::MAX]
        );
        assert_eq!(
            loaded.down_middle.to_vec_u32(),
            vec![u32::MAX, big_rank, big_rank + 7]
        );
        Ok(())
    }

    /// v5 (#352): when middle ranks exceed `U24_SENTINEL`, the writer
    /// falls back to U32. Picks per-direction so one direction can be
    /// narrow even if the other overflows.
    #[test]
    fn test_v5_picks_u32_when_u24_overflows() -> Result<()> {
        let huge_rank = crate::formats::cch_weights::U24_SENTINEL + 100;
        let data = CchTopo {
            n_nodes: 4,
            n_shortcuts: 1,
            n_original_arcs: 3,
            inputs_sha: [0u8; 32],
            up_offsets: vec![0u64, 1, 2, 3, 4].into(),
            up_targets: vec![1u32, 2, 3, 0].into(),
            up_is_shortcut: BitsetField::from_bools(&[false, false, true, false]),
            up_middle: WeightArray::from_vec_u32(vec![u32::MAX, u32::MAX, huge_rank, u32::MAX]),
            down_offsets: vec![0u64, 0, 1, 2, 3].into(),
            down_targets: vec![0u32, 1, 2].into(),
            down_is_shortcut: BitsetField::from_bools(&[false, false, true]),
            down_middle: WeightArray::from_vec_u32(vec![u32::MAX, u32::MAX, 1]),
            rank_to_filtered: vec![10u32, 20, 30, 40].into(),
        };
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;
        let raw = std::fs::read(tmp.path())?;
        assert_eq!(
            raw[12] & MIDDLE_BYTE12_KNOWN_MASK,
            MIDDLE_WIDTH_CODE_U32 | (MIDDLE_WIDTH_CODE_U16 << MIDDLE_DOWN_SHIFT),
            "byte12 picker: up=U32, down=U16"
        );
        let loaded = CchTopoFile::read(tmp.path())?;
        assert_eq!(
            loaded.up_middle.width(),
            crate::formats::cch_weights::WeightWidth::U32
        );
        assert_eq!(
            loaded.down_middle.width(),
            crate::formats::cch_weights::WeightWidth::U16
        );
        assert_eq!(
            loaded.up_middle.to_vec_u32(),
            vec![u32::MAX, u32::MAX, huge_rank, u32::MAX]
        );
        assert_eq!(loaded.down_middle.to_vec_u32(), vec![u32::MAX, u32::MAX, 1]);
        Ok(())
    }

    /// v5 (#352): the reader rejects any non-zero in the reserved bits
    /// of byte 12 (bits 4..=7) or in bytes 13..=15.
    #[test]
    fn test_v5_rejects_reserved_bit_pollution() -> Result<()> {
        let data = make_test_topo();
        let tmp = NamedTempFile::new()?;
        CchTopoFile::write(tmp.path(), &data)?;
        let mut raw = std::fs::read(tmp.path())?;

        // Pollute bit 5 of byte 12 → reader rejects.
        let saved = raw[12];
        raw[12] |= 0b0010_0000;
        let mut tmp2 = NamedTempFile::new()?;
        std::io::Write::write_all(&mut tmp2, &raw)?;
        let err = CchTopoFile::read(tmp2.path()).expect_err("must reject");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("unknown bits"),
            "unexpected error message: {msg}"
        );
        raw[12] = saved;

        // Pollute byte 13 → reader rejects.
        raw[13] = 0xFF;
        let mut tmp3 = NamedTempFile::new()?;
        std::io::Write::write_all(&mut tmp3, &raw)?;
        let err = CchTopoFile::read(tmp3.path()).expect_err("must reject");
        let msg = format!("{err:#}");
        assert!(
            msg.contains("reserved"),
            "byte 13 pollution should be rejected: {msg}"
        );
        Ok(())
    }
}
