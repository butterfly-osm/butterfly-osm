//! Packed mmap-friendly snap index sections (#154).
//!
//! Three section types, all CRC-validated, magic+version-checked,
//! and laid out for zero-copy reads via `bytemuck::cast_slice`:
//!
//! - [`SnapPointsFile`] — `shared/snap_points`. Flat array of
//!   [`PackedPoint`] (16 B each), Hilbert-sorted within each grid
//!   cell. One entry per polyline vertex kept by the same 50 m
//!   dedup rule the legacy `SpatialIndex::build` used.
//! - [`SnapGridFile`] — `shared/snap_grid`. Uniform-grid CSR
//!   directory (`offsets[n_cells + 1]`) into the `snap_points` array.
//! - [`SnapMaskFile`] — `mode/<m>/snap_mask`. Per-sample bitmap
//!   (one bit per `snap_points` entry). Bit set ⇔ that sample is
//!   snap-eligible for the mode.
//!
//! See `route/docs/154-design.md` for the full design rationale.
//!
//! # On-disk layouts
//!
//! All headers are 40 bytes (u64-aligned). Bodies are u64-aligned by
//! virtue of the container's per-section pad. Footers are 16 bytes:
//! `body_crc : u64 || file_crc : u64`.

use anyhow::Result;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;

// ---------- Constants -------------------------------------------------------

/// Magic for `shared/snap_points`. ASCII "SNPP".
pub const SNAP_POINTS_MAGIC: u32 = 0x534E_5050;
/// Magic for `shared/snap_grid`. ASCII "SNGD".
pub const SNAP_GRID_MAGIC: u32 = 0x534E_4744;
/// Magic for `mode/<m>/snap_mask`. ASCII "SNMK".
pub const SNAP_MASK_MAGIC: u32 = 0x534E_4D4B;

const SNAP_VERSION: u16 = 1;
/// Common header size across all three section types. 40 bytes is the
/// next multiple of 8 that fits `snap_points` (magic 4 + version 2 +
/// _pad 2 + n_points 4 + 4×i32 bbox + cell_log2 1 + 7-byte pad). The
/// shorter `snap_mask` and `snap_grid` payloads simply carry extra
/// padding to reach 40 bytes — 8 wasted bytes per section is trivial.
const HEADER_SIZE: usize = 40;
const FOOTER_SIZE: usize = 16;

/// 16-byte sample record. Stored sequentially in `snap_points`.
#[repr(C)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, bytemuck::Pod, bytemuck::Zeroable)]
pub struct PackedPoint {
    /// Longitude in i32-e7 fixed point (1 unit ≈ 1.1 cm at the equator).
    pub lon_e7: i32,
    /// Latitude in i32-e7 fixed point.
    pub lat_e7: i32,
    /// Original EBG node id this sample belongs to.
    pub ebg_id: u32,
    /// Edge bearing, degrees (0=North, clockwise). Range [0, 360).
    pub bearing: u16,
    /// Reserved. Zero on write, ignored on read.
    pub _pad: u16,
}

const _: () = assert!(std::mem::size_of::<PackedPoint>() == 16);
const _: () = assert!(std::mem::align_of::<PackedPoint>() == 4);

// ---------- snap_points ----------------------------------------------------

/// Parsed `shared/snap_points` section.
#[derive(Debug, Clone)]
pub struct SnapPoints {
    pub n_points: u32,
    pub bbox_min_lon: i32,
    pub bbox_min_lat: i32,
    pub bbox_max_lon: i32,
    pub bbox_max_lat: i32,
    pub cell_log2: u8,
    /// Sample array. Borrowed when read zero-copy from a mmap'd
    /// container, owned when read from a plain file or built in memory.
    pub points: Cow<'static, [PackedPoint]>,
}

impl SnapPoints {
    #[inline]
    pub fn as_slice(&self) -> &[PackedPoint] {
        &self.points
    }

    #[inline]
    pub fn len(&self) -> usize {
        self.points.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.points.is_empty()
    }
}

pub struct SnapPointsFile;

impl SnapPointsFile {
    /// Encode to header || body || footer bytes.
    pub fn encode(p: &SnapPoints) -> Vec<u8> {
        let n = p.points.len();
        let body_len = n
            .checked_mul(std::mem::size_of::<PackedPoint>())
            .expect("snap_points body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header
        out.extend_from_slice(&SNAP_POINTS_MAGIC.to_le_bytes());
        out.extend_from_slice(&SNAP_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // _pad0
        out.extend_from_slice(&(n as u32).to_le_bytes());
        out.extend_from_slice(&p.bbox_min_lon.to_le_bytes());
        out.extend_from_slice(&p.bbox_min_lat.to_le_bytes());
        out.extend_from_slice(&p.bbox_max_lon.to_le_bytes());
        out.extend_from_slice(&p.bbox_max_lat.to_le_bytes());
        out.push(p.cell_log2);
        out.extend_from_slice(&[0u8; 11]); // _pad1 — pads to 40-byte HEADER_SIZE
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body
        let body_bytes: &[u8] = bytemuck::cast_slice(p.points.as_ref());
        out.extend_from_slice(body_bytes);

        // Footer
        let mut body_d = Digest::new();
        body_d.update(&out[HEADER_SIZE..HEADER_SIZE + body_len]);
        let body_crc = body_d.finalize();
        let mut file_d = Digest::new();
        file_d.update(&out[..HEADER_SIZE + body_len]);
        let file_crc = file_d.finalize();
        out.extend_from_slice(&body_crc.to_le_bytes());
        out.extend_from_slice(&file_crc.to_le_bytes());
        out
    }

    pub fn write<P: AsRef<Path>>(path: P, idx: &SnapPoints) -> Result<()> {
        let bytes = Self::encode(idx);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    /// Owning reader: copies the body into a `Vec<PackedPoint>`.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<SnapPoints> {
        let parsed = parse_snap_points_header_and_check(bytes, true)?;
        let body = parsed.body;
        let pts: &[PackedPoint] = bytemuck::cast_slice(body);
        Ok(SnapPoints {
            n_points: parsed.n_points,
            bbox_min_lon: parsed.bbox_min_lon,
            bbox_min_lat: parsed.bbox_min_lat,
            bbox_max_lon: parsed.bbox_max_lon,
            bbox_max_lat: parsed.bbox_max_lat,
            cell_log2: parsed.cell_log2,
            points: Cow::Owned(pts.to_vec()),
        })
    }

    /// Zero-copy reader for a `'static` byte slice (mmap-backed).
    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<SnapPoints> {
        Self::read_from_bytes_zero_copy_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes_zero_copy`] but elides the CRC
    /// walk over the body. Caller MUST guarantee the bytes are
    /// already verified upstream.
    pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<SnapPoints> {
        Self::read_from_bytes_zero_copy_inner(bytes, false)
    }

    fn read_from_bytes_zero_copy_inner(bytes: &'static [u8], verify: bool) -> Result<SnapPoints> {
        let parsed = parse_snap_points_header_and_check(bytes, verify)?;
        debug_assert_eq!(
            parsed.body.as_ptr() as usize % std::mem::align_of::<PackedPoint>(),
            0,
            "snap_points body must be aligned for PackedPoint"
        );
        let pts: &'static [PackedPoint] = bytemuck::cast_slice(parsed.body);
        Ok(SnapPoints {
            n_points: parsed.n_points,
            bbox_min_lon: parsed.bbox_min_lon,
            bbox_min_lat: parsed.bbox_min_lat,
            bbox_max_lon: parsed.bbox_max_lon,
            bbox_max_lat: parsed.bbox_max_lat,
            cell_log2: parsed.cell_log2,
            points: Cow::Borrowed(pts),
        })
    }
}

struct ParsedSnapPoints<'a> {
    n_points: u32,
    bbox_min_lon: i32,
    bbox_min_lat: i32,
    bbox_max_lon: i32,
    bbox_max_lat: i32,
    cell_log2: u8,
    body: &'a [u8],
}

fn parse_snap_points_header_and_check(
    bytes: &[u8],
    verify_crc: bool,
) -> Result<ParsedSnapPoints<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "snap_points too short: {} bytes",
        bytes.len()
    );
    let header = &bytes[..HEADER_SIZE];
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == SNAP_POINTS_MAGIC,
        "Invalid magic in snap_points: expected 0x{:08X}, got 0x{:08X}",
        SNAP_POINTS_MAGIC,
        magic
    );
    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == SNAP_VERSION,
        "Unsupported snap_points version {}, expected {}",
        version,
        SNAP_VERSION
    );
    let n_points = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let bbox_min_lon = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    let bbox_min_lat = i32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    let bbox_max_lon = i32::from_le_bytes([header[20], header[21], header[22], header[23]]);
    let bbox_max_lat = i32::from_le_bytes([header[24], header[25], header[26], header[27]]);
    let cell_log2 = header[28];

    let body_bytes = (n_points as usize)
        .checked_mul(std::mem::size_of::<PackedPoint>())
        .ok_or_else(|| anyhow::anyhow!("snap_points body byte overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_bytes + FOOTER_SIZE,
        "snap_points length mismatch: declared {}, actual {}",
        HEADER_SIZE + body_bytes + FOOTER_SIZE,
        bytes.len()
    );

    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_bytes];
    let footer = &bytes[HEADER_SIZE + body_bytes..];
    if verify_crc {
        verify_crcs(bytes, body, body_bytes, footer, "snap_points")?;
    }

    Ok(ParsedSnapPoints {
        n_points,
        bbox_min_lon,
        bbox_min_lat,
        bbox_max_lon,
        bbox_max_lat,
        cell_log2,
        body,
    })
}

// ---------- snap_grid ------------------------------------------------------

/// Parsed `shared/snap_grid` section. The body is the CSR `offsets`
/// array of length `n_cells_x * n_cells_y + 1`.
#[derive(Debug, Clone)]
pub struct SnapGrid {
    pub n_cells_x: u32,
    pub n_cells_y: u32,
    pub origin_x: i32,
    pub origin_y: i32,
    pub cell_log2: u8,
    /// `offsets[i]..offsets[i+1]` is the half-open range into
    /// `snap_points` for cell `i`.
    pub offsets: Cow<'static, [u32]>,
}

impl SnapGrid {
    #[inline]
    pub fn n_cells(&self) -> usize {
        self.n_cells_x as usize * self.n_cells_y as usize
    }
}

pub struct SnapGridFile;

impl SnapGridFile {
    pub fn encode(g: &SnapGrid) -> Vec<u8> {
        let expected_offsets = g.n_cells() + 1;
        assert_eq!(
            g.offsets.len(),
            expected_offsets,
            "snap_grid offsets must have n_cells_x * n_cells_y + 1 entries"
        );
        let body_len = expected_offsets
            .checked_mul(4)
            .expect("snap_grid body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header
        out.extend_from_slice(&SNAP_GRID_MAGIC.to_le_bytes());
        out.extend_from_slice(&SNAP_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // _pad0
        out.extend_from_slice(&g.n_cells_x.to_le_bytes());
        out.extend_from_slice(&g.n_cells_y.to_le_bytes());
        out.extend_from_slice(&g.origin_x.to_le_bytes());
        out.extend_from_slice(&g.origin_y.to_le_bytes());
        out.push(g.cell_log2);
        out.extend_from_slice(&[0u8; 15]); // _pad — pads to 40-byte HEADER_SIZE
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body
        let body_bytes: &[u8] = bytemuck::cast_slice(g.offsets.as_ref());
        out.extend_from_slice(body_bytes);

        // Footer
        let mut body_d = Digest::new();
        body_d.update(&out[HEADER_SIZE..HEADER_SIZE + body_len]);
        let body_crc = body_d.finalize();
        let mut file_d = Digest::new();
        file_d.update(&out[..HEADER_SIZE + body_len]);
        let file_crc = file_d.finalize();
        out.extend_from_slice(&body_crc.to_le_bytes());
        out.extend_from_slice(&file_crc.to_le_bytes());
        out
    }

    pub fn write<P: AsRef<Path>>(path: P, g: &SnapGrid) -> Result<()> {
        let bytes = Self::encode(g);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    pub fn read_from_bytes(bytes: &[u8]) -> Result<SnapGrid> {
        let parsed = parse_snap_grid_header_and_check(bytes, true)?;
        let off_slice: &[u32] = bytemuck::cast_slice(parsed.body);
        Ok(SnapGrid {
            n_cells_x: parsed.n_cells_x,
            n_cells_y: parsed.n_cells_y,
            origin_x: parsed.origin_x,
            origin_y: parsed.origin_y,
            cell_log2: parsed.cell_log2,
            offsets: Cow::Owned(off_slice.to_vec()),
        })
    }

    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<SnapGrid> {
        Self::read_from_bytes_zero_copy_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes_zero_copy`] but elides the CRC
    /// walk over the body. Caller MUST guarantee the bytes are
    /// already verified upstream.
    pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<SnapGrid> {
        Self::read_from_bytes_zero_copy_inner(bytes, false)
    }

    fn read_from_bytes_zero_copy_inner(bytes: &'static [u8], verify: bool) -> Result<SnapGrid> {
        let parsed = parse_snap_grid_header_and_check(bytes, verify)?;
        debug_assert_eq!(
            parsed.body.as_ptr() as usize % 4,
            0,
            "snap_grid body must be 4-byte aligned"
        );
        let off_slice: &'static [u32] = bytemuck::cast_slice(parsed.body);
        Ok(SnapGrid {
            n_cells_x: parsed.n_cells_x,
            n_cells_y: parsed.n_cells_y,
            origin_x: parsed.origin_x,
            origin_y: parsed.origin_y,
            cell_log2: parsed.cell_log2,
            offsets: Cow::Borrowed(off_slice),
        })
    }
}

struct ParsedSnapGrid<'a> {
    n_cells_x: u32,
    n_cells_y: u32,
    origin_x: i32,
    origin_y: i32,
    cell_log2: u8,
    body: &'a [u8],
}

fn parse_snap_grid_header_and_check(bytes: &[u8], verify_crc: bool) -> Result<ParsedSnapGrid<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "snap_grid too short: {} bytes",
        bytes.len()
    );
    let header = &bytes[..HEADER_SIZE];
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == SNAP_GRID_MAGIC,
        "Invalid magic in snap_grid: expected 0x{:08X}, got 0x{:08X}",
        SNAP_GRID_MAGIC,
        magic
    );
    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == SNAP_VERSION,
        "Unsupported snap_grid version {}, expected {}",
        version,
        SNAP_VERSION
    );
    let n_cells_x = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let n_cells_y = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    let origin_x = i32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    let origin_y = i32::from_le_bytes([header[20], header[21], header[22], header[23]]);
    let cell_log2 = header[24];

    let n_cells = (n_cells_x as usize)
        .checked_mul(n_cells_y as usize)
        .ok_or_else(|| anyhow::anyhow!("snap_grid cell count overflow"))?;
    let n_offsets = n_cells
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("snap_grid offsets count overflow"))?;
    let body_bytes = n_offsets
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("snap_grid body byte overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_bytes + FOOTER_SIZE,
        "snap_grid length mismatch: declared {}, actual {}",
        HEADER_SIZE + body_bytes + FOOTER_SIZE,
        bytes.len()
    );
    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_bytes];
    let footer = &bytes[HEADER_SIZE + body_bytes..];
    if verify_crc {
        verify_crcs(bytes, body, body_bytes, footer, "snap_grid")?;
    }

    Ok(ParsedSnapGrid {
        n_cells_x,
        n_cells_y,
        origin_x,
        origin_y,
        cell_log2,
        body,
    })
}

// ---------- snap_mask ------------------------------------------------------

/// Parsed `mode/<m>/snap_mask` section. The body is `[u64]` bits, one
/// per `snap_points` entry.
#[derive(Debug, Clone)]
pub struct SnapMask {
    pub mode: u8,
    pub n_points: u32,
    pub inputs_sha: [u8; 16],
    pub bits: Cow<'static, [u64]>,
}

impl SnapMask {
    /// Test whether sample index `i` is set.
    #[inline]
    pub fn is_set(&self, i: usize) -> bool {
        let word = i / 64;
        let bit = i % 64;
        word < self.bits.len() && (self.bits[word] & (1u64 << bit)) != 0
    }
}

pub struct SnapMaskFile;

impl SnapMaskFile {
    pub fn encode(m: &SnapMask) -> Vec<u8> {
        let n_words_expected = (m.n_points as usize).div_ceil(64);
        assert_eq!(
            m.bits.len(),
            n_words_expected,
            "snap_mask bits must be ceil(n_points / 64) words"
        );
        let body_len = n_words_expected
            .checked_mul(8)
            .expect("snap_mask body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header
        out.extend_from_slice(&SNAP_MASK_MAGIC.to_le_bytes());
        out.extend_from_slice(&SNAP_VERSION.to_le_bytes());
        out.push(m.mode);
        out.push(0u8); // _pad0
        out.extend_from_slice(&m.n_points.to_le_bytes());
        out.extend_from_slice(&(n_words_expected as u32).to_le_bytes());
        out.extend_from_slice(&m.inputs_sha);
        out.extend_from_slice(&[0u8; 8]); // _pad — pads to 40-byte HEADER_SIZE
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body
        let body_bytes: &[u8] = bytemuck::cast_slice(m.bits.as_ref());
        out.extend_from_slice(body_bytes);

        // Footer
        let mut body_d = Digest::new();
        body_d.update(&out[HEADER_SIZE..HEADER_SIZE + body_len]);
        let body_crc = body_d.finalize();
        let mut file_d = Digest::new();
        file_d.update(&out[..HEADER_SIZE + body_len]);
        let file_crc = file_d.finalize();
        out.extend_from_slice(&body_crc.to_le_bytes());
        out.extend_from_slice(&file_crc.to_le_bytes());
        out
    }

    pub fn write<P: AsRef<Path>>(path: P, m: &SnapMask) -> Result<()> {
        let bytes = Self::encode(m);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    pub fn read_from_bytes(bytes: &[u8]) -> Result<SnapMask> {
        let parsed = parse_snap_mask_header_and_check(bytes, true)?;
        let bits: &[u64] = bytemuck::cast_slice(parsed.body);
        Ok(SnapMask {
            mode: parsed.mode,
            n_points: parsed.n_points,
            inputs_sha: parsed.inputs_sha,
            bits: Cow::Owned(bits.to_vec()),
        })
    }

    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<SnapMask> {
        Self::read_from_bytes_zero_copy_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes_zero_copy`] but elides the CRC
    /// walk over the body. Caller MUST guarantee bytes are already
    /// verified upstream.
    pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<SnapMask> {
        Self::read_from_bytes_zero_copy_inner(bytes, false)
    }

    fn read_from_bytes_zero_copy_inner(bytes: &'static [u8], verify: bool) -> Result<SnapMask> {
        let parsed = parse_snap_mask_header_and_check(bytes, verify)?;
        debug_assert_eq!(
            parsed.body.as_ptr() as usize % 8,
            0,
            "snap_mask body must be 8-byte aligned"
        );
        let bits: &'static [u64] = bytemuck::cast_slice(parsed.body);
        Ok(SnapMask {
            mode: parsed.mode,
            n_points: parsed.n_points,
            inputs_sha: parsed.inputs_sha,
            bits: Cow::Borrowed(bits),
        })
    }
}

struct ParsedSnapMask<'a> {
    mode: u8,
    n_points: u32,
    inputs_sha: [u8; 16],
    body: &'a [u8],
}

fn parse_snap_mask_header_and_check(bytes: &[u8], verify_crc: bool) -> Result<ParsedSnapMask<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "snap_mask too short: {} bytes",
        bytes.len()
    );
    let header = &bytes[..HEADER_SIZE];
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == SNAP_MASK_MAGIC,
        "Invalid magic in snap_mask: expected 0x{:08X}, got 0x{:08X}",
        SNAP_MASK_MAGIC,
        magic
    );
    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == SNAP_VERSION,
        "Unsupported snap_mask version {}, expected {}",
        version,
        SNAP_VERSION
    );
    let mode = header[6];
    let n_points = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let n_words = u32::from_le_bytes([header[12], header[13], header[14], header[15]]) as usize;
    let mut inputs_sha = [0u8; 16];
    inputs_sha.copy_from_slice(&header[16..32]);

    let expected_words = (n_points as usize).div_ceil(64);
    anyhow::ensure!(
        n_words == expected_words,
        "snap_mask n_words {} != ceil(n_points / 64) {}",
        n_words,
        expected_words
    );
    let body_bytes = n_words
        .checked_mul(8)
        .ok_or_else(|| anyhow::anyhow!("snap_mask body byte overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_bytes + FOOTER_SIZE,
        "snap_mask length mismatch: declared {}, actual {}",
        HEADER_SIZE + body_bytes + FOOTER_SIZE,
        bytes.len()
    );
    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_bytes];
    let footer = &bytes[HEADER_SIZE + body_bytes..];
    if verify_crc {
        verify_crcs(bytes, body, body_bytes, footer, "snap_mask")?;
    }

    Ok(ParsedSnapMask {
        mode,
        n_points,
        inputs_sha,
        body,
    })
}

// ---------- shared CRC verifier --------------------------------------------

fn verify_crcs(
    full: &[u8],
    body: &[u8],
    body_len: usize,
    footer: &[u8],
    label: &'static str,
) -> Result<()> {
    let mut body_d = Digest::new();
    body_d.update(body);
    let computed_body = body_d.finalize();
    let mut file_d = Digest::new();
    file_d.update(&full[..HEADER_SIZE + body_len]);
    let computed_file = file_d.finalize();

    let stored_body = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let stored_file = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    anyhow::ensure!(
        computed_body == stored_body,
        "{} body CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
        label,
        computed_body,
        stored_body
    );
    anyhow::ensure!(
        computed_file == stored_file,
        "{} file CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
        label,
        computed_file,
        stored_file
    );
    Ok(())
}

// ---------- Tests -----------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_points() -> Vec<PackedPoint> {
        (0..100u32)
            .map(|i| PackedPoint {
                lon_e7: 30_000_000 + (i as i32) * 1000,
                lat_e7: 500_000_000 + (i as i32) * 500,
                ebg_id: i,
                bearing: (i * 3) as u16,
                _pad: 0,
            })
            .collect()
    }

    #[test]
    fn snap_points_roundtrip_owned() {
        let pts = sample_points();
        let original = SnapPoints {
            n_points: pts.len() as u32,
            bbox_min_lon: 25_000_000,
            bbox_min_lat: 494_000_000,
            bbox_max_lon: 65_000_000,
            bbox_max_lat: 516_000_000,
            cell_log2: 17,
            points: Cow::Owned(pts.clone()),
        };
        let bytes = SnapPointsFile::encode(&original);
        let parsed = SnapPointsFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.n_points as usize, pts.len());
        assert_eq!(parsed.bbox_min_lon, 25_000_000);
        assert_eq!(parsed.cell_log2, 17);
        assert_eq!(parsed.points.as_ref(), pts.as_slice());
    }

    #[test]
    fn snap_points_zero_copy_matches_owned() {
        let pts = sample_points();
        let original = SnapPoints {
            n_points: pts.len() as u32,
            bbox_min_lon: 0,
            bbox_min_lat: 0,
            bbox_max_lon: 1,
            bbox_max_lat: 1,
            cell_log2: 17,
            points: Cow::Owned(pts.clone()),
        };
        let bytes = SnapPointsFile::encode(&original);
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let owned = SnapPointsFile::read_from_bytes(leaked).expect("owned");
        let zc = SnapPointsFile::read_from_bytes_zero_copy(leaked).expect("zc");
        assert_eq!(owned.points.as_ref(), zc.points.as_ref());
        assert!(matches!(zc.points, Cow::Borrowed(_)));
    }

    #[test]
    fn snap_points_corruption_detected() {
        let pts = sample_points();
        let original = SnapPoints {
            n_points: pts.len() as u32,
            bbox_min_lon: 0,
            bbox_min_lat: 0,
            bbox_max_lon: 1,
            bbox_max_lat: 1,
            cell_log2: 17,
            points: Cow::Owned(pts),
        };
        let mut bytes = SnapPointsFile::encode(&original);
        bytes[HEADER_SIZE + 8] ^= 0xFF;
        let r = SnapPointsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("CRC mismatch"));
    }

    #[test]
    fn snap_points_bad_magic_rejected() {
        let pts = sample_points();
        let original = SnapPoints {
            n_points: pts.len() as u32,
            bbox_min_lon: 0,
            bbox_min_lat: 0,
            bbox_max_lon: 1,
            bbox_max_lat: 1,
            cell_log2: 17,
            points: Cow::Owned(pts),
        };
        let mut bytes = SnapPointsFile::encode(&original);
        bytes[0] ^= 0xFF;
        let r = SnapPointsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("Invalid magic"));
    }

    #[test]
    fn snap_grid_roundtrip() {
        let n_cells_x = 8u32;
        let n_cells_y = 5u32;
        let n_cells = (n_cells_x * n_cells_y) as usize;
        let mut offsets: Vec<u32> = (0..=n_cells as u32).collect();
        offsets[n_cells] = 100;
        let original = SnapGrid {
            n_cells_x,
            n_cells_y,
            origin_x: 25_000_000,
            origin_y: 494_000_000,
            cell_log2: 17,
            offsets: Cow::Owned(offsets.clone()),
        };
        let bytes = SnapGridFile::encode(&original);
        let parsed = SnapGridFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.n_cells_x, n_cells_x);
        assert_eq!(parsed.n_cells_y, n_cells_y);
        assert_eq!(parsed.cell_log2, 17);
        assert_eq!(parsed.offsets.as_ref(), offsets.as_slice());
    }

    #[test]
    fn snap_grid_zero_copy() {
        let n_cells_x = 4u32;
        let n_cells_y = 3u32;
        let offsets: Vec<u32> = (0..=(n_cells_x * n_cells_y)).collect();
        let original = SnapGrid {
            n_cells_x,
            n_cells_y,
            origin_x: 0,
            origin_y: 0,
            cell_log2: 17,
            offsets: Cow::Owned(offsets.clone()),
        };
        let bytes = SnapGridFile::encode(&original);
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let zc = SnapGridFile::read_from_bytes_zero_copy(leaked).expect("zc");
        assert_eq!(zc.offsets.as_ref(), offsets.as_slice());
        assert!(matches!(zc.offsets, Cow::Borrowed(_)));
    }

    #[test]
    fn snap_mask_roundtrip() {
        let n_points = 1000u32;
        let n_words = (n_points as usize).div_ceil(64);
        let mut bits = vec![0u64; n_words];
        for &i in &[0u32, 7, 63, 64, 65, 999] {
            let word = (i / 64) as usize;
            let bit = (i % 64) as usize;
            bits[word] |= 1u64 << bit;
        }
        let original = SnapMask {
            mode: 1,
            n_points,
            inputs_sha: [0xCD; 16],
            bits: Cow::Owned(bits.clone()),
        };
        let bytes = SnapMaskFile::encode(&original);
        let parsed = SnapMaskFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.mode, 1);
        assert_eq!(parsed.n_points, n_points);
        assert_eq!(parsed.inputs_sha, [0xCD; 16]);
        assert!(parsed.is_set(0));
        assert!(parsed.is_set(7));
        assert!(parsed.is_set(63));
        assert!(parsed.is_set(64));
        assert!(parsed.is_set(65));
        assert!(parsed.is_set(999));
        assert!(!parsed.is_set(1));
        assert!(!parsed.is_set(998));
    }

    #[test]
    fn snap_mask_zero_copy() {
        let n_points = 256u32;
        let n_words = (n_points as usize).div_ceil(64);
        let bits: Vec<u64> = (0..n_words as u64)
            .map(|i| i.wrapping_mul(0xDEAD_BEEF))
            .collect();
        let original = SnapMask {
            mode: 3,
            n_points,
            inputs_sha: [0; 16],
            bits: Cow::Owned(bits.clone()),
        };
        let bytes = SnapMaskFile::encode(&original);
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let zc = SnapMaskFile::read_from_bytes_zero_copy(leaked).expect("zc");
        assert_eq!(zc.bits.as_ref(), bits.as_slice());
        assert!(matches!(zc.bits, Cow::Borrowed(_)));
    }

    #[test]
    fn snap_mask_corruption_detected() {
        let n_points = 64u32;
        let bits = vec![0xFFFF_FFFF_FFFF_FFFFu64; 1];
        let original = SnapMask {
            mode: 0,
            n_points,
            inputs_sha: [0; 16],
            bits: Cow::Owned(bits),
        };
        let mut bytes = SnapMaskFile::encode(&original);
        bytes[HEADER_SIZE + 4] ^= 0xFF;
        let r = SnapMaskFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("CRC mismatch"));
    }

    #[test]
    fn empty_snap_points_roundtrip() {
        let original = SnapPoints {
            n_points: 0,
            bbox_min_lon: 0,
            bbox_min_lat: 0,
            bbox_max_lon: 0,
            bbox_max_lat: 0,
            cell_log2: 17,
            points: Cow::Owned(vec![]),
        };
        let bytes = SnapPointsFile::encode(&original);
        let parsed = SnapPointsFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.n_points, 0);
        assert!(parsed.points.is_empty());
    }

    #[test]
    fn snap_grid_offsets_length_check() {
        // Length sanity: encode panics if offsets.len() != n_cells + 1.
        let g = SnapGrid {
            n_cells_x: 2,
            n_cells_y: 2,
            origin_x: 0,
            origin_y: 0,
            cell_log2: 17,
            offsets: Cow::Owned(vec![0u32, 1, 2, 3, 4]), // n_cells=4 -> need 5 entries
        };
        let bytes = SnapGridFile::encode(&g);
        let parsed = SnapGridFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.offsets.len(), 5);
    }
}
