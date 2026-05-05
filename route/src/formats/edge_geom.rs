//! Flat mmap-friendly edge geometry sections (#155).
//!
//! Two section types, both CRC-validated, magic+version-checked, and laid
//! out for zero-copy reads via `bytemuck::cast_slice`:
//!
//! - [`EdgeGeomOffsetsFile`] — `shared/edge_geom_offsets`. CSR-style
//!   `[u32; n_edges + 1]` of cumulative point counts, indexing into the
//!   points body. `offsets[i]..offsets[i+1]` is the half-open range of
//!   vertex indices for edge `i`.
//! - [`EdgeGeomPointsFile`] — `shared/edge_geom_points`. Interleaved
//!   `[i32; 2 * n_points]` array of `(lon_e7, lat_e7)` pairs.
//!
//! Together they replace the heap-resident `Vec<PolyLine>` shape inside
//! `NbgGeo` on the serve path. See `route/docs/155-design.md` for the
//! design rationale.
//!
//! # On-disk layout
//!
//! All headers are 32 bytes (u64-aligned). Footers are 16 bytes:
//! `body_crc : u64 || file_crc : u64`.

use anyhow::Result;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;

// ---------- Constants -------------------------------------------------------

/// Magic for `shared/edge_geom_offsets`. ASCII "EGOF" (LE byte-order).
pub const EDGE_GEOM_OFFSETS_MAGIC: u32 = 0x464F_4745;
/// Magic for `shared/edge_geom_points`. ASCII "EGPT" (LE byte-order).
pub const EDGE_GEOM_POINTS_MAGIC: u32 = 0x5450_4745;

const EDGE_GEOM_VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 16;

// ---------- edge_geom_offsets ----------------------------------------------

/// Parsed `shared/edge_geom_offsets` section.
#[derive(Debug, Clone)]
pub struct EdgeGeomOffsets {
    pub n_edges: u32,
    pub n_points: u32,
    /// Cumulative point counts. `offsets[edge_id]..offsets[edge_id + 1]`
    /// indexes the half-open vertex range for edge `edge_id` in the
    /// `points` body. Length is `n_edges + 1`; `offsets[0] = 0`,
    /// `offsets[n_edges] = n_points`.
    pub offsets: Cow<'static, [u32]>,
}

impl EdgeGeomOffsets {
    #[inline]
    pub fn n_edges(&self) -> usize {
        self.n_edges as usize
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.n_edges == 0
    }
}

pub struct EdgeGeomOffsetsFile;

impl EdgeGeomOffsetsFile {
    pub fn encode(o: &EdgeGeomOffsets) -> Vec<u8> {
        let expected = (o.n_edges as usize)
            .checked_add(1)
            .expect("edge_geom_offsets length overflow");
        assert_eq!(
            o.offsets.len(),
            expected,
            "edge_geom_offsets length must be n_edges + 1 (got {} for n_edges={})",
            o.offsets.len(),
            o.n_edges
        );
        let body_len = expected
            .checked_mul(4)
            .expect("edge_geom_offsets body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header
        out.extend_from_slice(&EDGE_GEOM_OFFSETS_MAGIC.to_le_bytes());
        out.extend_from_slice(&EDGE_GEOM_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // _pad0
        out.extend_from_slice(&o.n_edges.to_le_bytes());
        out.extend_from_slice(&o.n_points.to_le_bytes());
        out.extend_from_slice(&[0u8; 16]); // _pad1 — pads to 32-byte HEADER_SIZE
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body — convert to LE bytes (bytemuck cast assumes host endian).
        for &v in o.offsets.as_ref() {
            out.extend_from_slice(&v.to_le_bytes());
        }

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

    pub fn write<P: AsRef<Path>>(path: P, o: &EdgeGeomOffsets) -> Result<()> {
        let bytes = Self::encode(o);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    /// Owning reader: copies the body into a `Vec<u32>`.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<EdgeGeomOffsets> {
        let parsed = parse_offsets_header_and_check(bytes, true)?;
        let mut offsets = Vec::with_capacity(parsed.expected_entries);
        for chunk in parsed.body.chunks_exact(4) {
            offsets.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        sanity_check_offsets(&offsets, parsed.n_points)?;
        Ok(EdgeGeomOffsets {
            n_edges: parsed.n_edges,
            n_points: parsed.n_points,
            offsets: Cow::Owned(offsets),
        })
    }

    /// Zero-copy reader for a `'static` byte slice (mmap-backed).
    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<EdgeGeomOffsets> {
        Self::read_from_bytes_zero_copy_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes_zero_copy`] but elides the CRC
    /// walk over the body. Caller MUST guarantee the bytes are
    /// already verified upstream.
    pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<EdgeGeomOffsets> {
        Self::read_from_bytes_zero_copy_inner(bytes, false)
    }

    fn read_from_bytes_zero_copy_inner(
        bytes: &'static [u8],
        verify: bool,
    ) -> Result<EdgeGeomOffsets> {
        let parsed = parse_offsets_header_and_check(bytes, verify)?;
        debug_assert_eq!(
            parsed.body.as_ptr() as usize % 4,
            0,
            "edge_geom_offsets body must be 4-byte aligned for &[u32]"
        );
        let off_slice: &'static [u32] = bytemuck::cast_slice(parsed.body);
        sanity_check_offsets(off_slice, parsed.n_points)?;
        Ok(EdgeGeomOffsets {
            n_edges: parsed.n_edges,
            n_points: parsed.n_points,
            offsets: Cow::Borrowed(off_slice),
        })
    }
}

struct ParsedOffsets<'a> {
    n_edges: u32,
    n_points: u32,
    expected_entries: usize,
    body: &'a [u8],
}

fn parse_offsets_header_and_check(bytes: &[u8], verify_crc: bool) -> Result<ParsedOffsets<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "edge_geom_offsets too short: {} bytes",
        bytes.len()
    );
    let header = &bytes[..HEADER_SIZE];
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == EDGE_GEOM_OFFSETS_MAGIC,
        "Invalid magic in edge_geom_offsets: expected 0x{:08X}, got 0x{:08X}",
        EDGE_GEOM_OFFSETS_MAGIC,
        magic
    );
    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == EDGE_GEOM_VERSION,
        "Unsupported edge_geom_offsets version {}, expected {}",
        version,
        EDGE_GEOM_VERSION
    );
    let n_edges = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let n_points = u32::from_le_bytes([header[12], header[13], header[14], header[15]]);

    let expected_entries = (n_edges as usize)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("edge_geom_offsets entry-count overflow"))?;
    let body_bytes = expected_entries
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("edge_geom_offsets body byte overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_bytes + FOOTER_SIZE,
        "edge_geom_offsets length mismatch: declared {}, actual {}",
        HEADER_SIZE + body_bytes + FOOTER_SIZE,
        bytes.len()
    );
    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_bytes];
    let footer = &bytes[HEADER_SIZE + body_bytes..];
    if verify_crc {
        verify_crcs(bytes, body, body_bytes, footer, "edge_geom_offsets")?;
    }

    Ok(ParsedOffsets {
        n_edges,
        n_points,
        expected_entries,
        body,
    })
}

fn sanity_check_offsets(offsets: &[u32], declared_n_points: u32) -> Result<()> {
    anyhow::ensure!(
        !offsets.is_empty(),
        "edge_geom_offsets sanity: empty offsets array"
    );
    anyhow::ensure!(
        offsets[0] == 0,
        "edge_geom_offsets sanity: offsets[0] = {}, must be 0",
        offsets[0]
    );
    let last = *offsets.last().unwrap();
    anyhow::ensure!(
        last == declared_n_points,
        "edge_geom_offsets sanity: offsets[n_edges] = {} but n_points = {}",
        last,
        declared_n_points
    );
    // Monotonic non-decreasing check (cheap and catches encoder bugs).
    for w in offsets.windows(2) {
        anyhow::ensure!(
            w[0] <= w[1],
            "edge_geom_offsets sanity: non-monotonic ({} > {})",
            w[0],
            w[1]
        );
    }
    Ok(())
}

// ---------- edge_geom_points -----------------------------------------------

/// Parsed `shared/edge_geom_points` section.
#[derive(Debug, Clone)]
pub struct EdgeGeomPoints {
    pub n_points: u32,
    pub bbox_min_lon: i32,
    pub bbox_min_lat: i32,
    pub bbox_max_lon: i32,
    pub bbox_max_lat: i32,
    /// Interleaved `(lon_e7, lat_e7)` pairs. Length = `2 * n_points`.
    pub points: Cow<'static, [i32]>,
}

pub struct EdgeGeomPointsFile;

impl EdgeGeomPointsFile {
    pub fn encode(p: &EdgeGeomPoints) -> Vec<u8> {
        let expected_i32s = (p.n_points as usize)
            .checked_mul(2)
            .expect("edge_geom_points i32 count overflow");
        assert_eq!(
            p.points.len(),
            expected_i32s,
            "edge_geom_points length must be 2 * n_points (got {} for n_points={})",
            p.points.len(),
            p.n_points
        );
        let body_len = expected_i32s
            .checked_mul(4)
            .expect("edge_geom_points body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header
        out.extend_from_slice(&EDGE_GEOM_POINTS_MAGIC.to_le_bytes());
        out.extend_from_slice(&EDGE_GEOM_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // _pad0
        out.extend_from_slice(&p.n_points.to_le_bytes());
        out.extend_from_slice(&p.bbox_min_lon.to_le_bytes());
        out.extend_from_slice(&p.bbox_min_lat.to_le_bytes());
        out.extend_from_slice(&p.bbox_max_lon.to_le_bytes());
        out.extend_from_slice(&p.bbox_max_lat.to_le_bytes());
        out.extend_from_slice(&[0u8; 4]); // _pad1 — pads to 32-byte HEADER_SIZE
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body
        for &v in p.points.as_ref() {
            out.extend_from_slice(&v.to_le_bytes());
        }

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

    pub fn write<P: AsRef<Path>>(path: P, p: &EdgeGeomPoints) -> Result<()> {
        let bytes = Self::encode(p);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    pub fn read_from_bytes(bytes: &[u8]) -> Result<EdgeGeomPoints> {
        let parsed = parse_points_header_and_check(bytes, true)?;
        let mut points = Vec::with_capacity(parsed.expected_i32s);
        for chunk in parsed.body.chunks_exact(4) {
            points.push(i32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(EdgeGeomPoints {
            n_points: parsed.n_points,
            bbox_min_lon: parsed.bbox_min_lon,
            bbox_min_lat: parsed.bbox_min_lat,
            bbox_max_lon: parsed.bbox_max_lon,
            bbox_max_lat: parsed.bbox_max_lat,
            points: Cow::Owned(points),
        })
    }

    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<EdgeGeomPoints> {
        Self::read_from_bytes_zero_copy_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes_zero_copy`] but elides the CRC
    /// walk over the body. Caller MUST guarantee the bytes are
    /// already verified upstream.
    pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<EdgeGeomPoints> {
        Self::read_from_bytes_zero_copy_inner(bytes, false)
    }

    fn read_from_bytes_zero_copy_inner(
        bytes: &'static [u8],
        verify: bool,
    ) -> Result<EdgeGeomPoints> {
        let parsed = parse_points_header_and_check(bytes, verify)?;
        debug_assert_eq!(
            parsed.body.as_ptr() as usize % 4,
            0,
            "edge_geom_points body must be 4-byte aligned for &[i32]"
        );
        let pts: &'static [i32] = bytemuck::cast_slice(parsed.body);
        Ok(EdgeGeomPoints {
            n_points: parsed.n_points,
            bbox_min_lon: parsed.bbox_min_lon,
            bbox_min_lat: parsed.bbox_min_lat,
            bbox_max_lon: parsed.bbox_max_lon,
            bbox_max_lat: parsed.bbox_max_lat,
            points: Cow::Borrowed(pts),
        })
    }
}

struct ParsedPoints<'a> {
    n_points: u32,
    bbox_min_lon: i32,
    bbox_min_lat: i32,
    bbox_max_lon: i32,
    bbox_max_lat: i32,
    expected_i32s: usize,
    body: &'a [u8],
}

fn parse_points_header_and_check(bytes: &[u8], verify_crc: bool) -> Result<ParsedPoints<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "edge_geom_points too short: {} bytes",
        bytes.len()
    );
    let header = &bytes[..HEADER_SIZE];
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == EDGE_GEOM_POINTS_MAGIC,
        "Invalid magic in edge_geom_points: expected 0x{:08X}, got 0x{:08X}",
        EDGE_GEOM_POINTS_MAGIC,
        magic
    );
    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == EDGE_GEOM_VERSION,
        "Unsupported edge_geom_points version {}, expected {}",
        version,
        EDGE_GEOM_VERSION
    );
    let n_points = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let bbox_min_lon = i32::from_le_bytes([header[12], header[13], header[14], header[15]]);
    let bbox_min_lat = i32::from_le_bytes([header[16], header[17], header[18], header[19]]);
    let bbox_max_lon = i32::from_le_bytes([header[20], header[21], header[22], header[23]]);
    let bbox_max_lat = i32::from_le_bytes([header[24], header[25], header[26], header[27]]);

    let expected_i32s = (n_points as usize)
        .checked_mul(2)
        .ok_or_else(|| anyhow::anyhow!("edge_geom_points i32-count overflow"))?;
    let body_bytes = expected_i32s
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("edge_geom_points body byte overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_bytes + FOOTER_SIZE,
        "edge_geom_points length mismatch: declared {}, actual {}",
        HEADER_SIZE + body_bytes + FOOTER_SIZE,
        bytes.len()
    );
    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_bytes];
    let footer = &bytes[HEADER_SIZE + body_bytes..];
    if verify_crc {
        verify_crcs(bytes, body, body_bytes, footer, "edge_geom_points")?;
    }

    Ok(ParsedPoints {
        n_points,
        bbox_min_lon,
        bbox_min_lat,
        bbox_max_lon,
        bbox_max_lat,
        expected_i32s,
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

    fn sample_offsets() -> EdgeGeomOffsets {
        // 4 edges with point counts: [3, 0, 5, 2]. Cumulative: [0, 3, 3, 8, 10].
        let offsets: Vec<u32> = vec![0, 3, 3, 8, 10];
        EdgeGeomOffsets {
            n_edges: 4,
            n_points: 10,
            offsets: Cow::Owned(offsets),
        }
    }

    fn sample_points() -> EdgeGeomPoints {
        // 10 points, interleaved lon_e7/lat_e7. Use distinguishable values.
        let pts: Vec<i32> = (0..20i32).map(|i| 30_000_000 + i * 100).collect();
        EdgeGeomPoints {
            n_points: 10,
            bbox_min_lon: 30_000_000,
            bbox_min_lat: 30_000_100,
            bbox_max_lon: 30_001_800,
            bbox_max_lat: 30_001_900,
            points: Cow::Owned(pts),
        }
    }

    #[test]
    fn offsets_roundtrip_owned() {
        let original = sample_offsets();
        let bytes = EdgeGeomOffsetsFile::encode(&original);
        let parsed = EdgeGeomOffsetsFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.n_edges, original.n_edges);
        assert_eq!(parsed.n_points, original.n_points);
        assert_eq!(parsed.offsets.as_ref(), original.offsets.as_ref());
    }

    #[test]
    fn offsets_zero_copy_matches_owned() {
        let original = sample_offsets();
        let bytes = EdgeGeomOffsetsFile::encode(&original);
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let owned = EdgeGeomOffsetsFile::read_from_bytes(leaked).expect("owned");
        let zc = EdgeGeomOffsetsFile::read_from_bytes_zero_copy(leaked).expect("zc");
        assert_eq!(owned.offsets.as_ref(), zc.offsets.as_ref());
        assert!(matches!(zc.offsets, Cow::Borrowed(_)));
    }

    #[test]
    fn offsets_corruption_detected() {
        let original = sample_offsets();
        let mut bytes = EdgeGeomOffsetsFile::encode(&original);
        bytes[HEADER_SIZE + 4] ^= 0xFF;
        let r = EdgeGeomOffsetsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("CRC mismatch"));
    }

    #[test]
    fn offsets_bad_magic_rejected_owning_path() {
        let original = sample_offsets();
        let mut bytes = EdgeGeomOffsetsFile::encode(&original);
        bytes[0] ^= 0xFF;
        let r = EdgeGeomOffsetsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("Invalid magic"));
    }

    #[test]
    fn offsets_bad_magic_rejected_zero_copy_path() {
        let original = sample_offsets();
        let mut bytes = EdgeGeomOffsetsFile::encode(&original);
        bytes[0] ^= 0xFF;
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let r = EdgeGeomOffsetsFile::read_from_bytes_zero_copy(leaked);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("Invalid magic"));
    }

    #[test]
    fn offsets_bad_version_rejected_both_paths() {
        let original = sample_offsets();
        let mut bytes = EdgeGeomOffsetsFile::encode(&original);
        // Stomp version (bytes 4..6).
        bytes[4] = 99;
        bytes[5] = 0;
        // Owning
        let r = EdgeGeomOffsetsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(
            r.unwrap_err()
                .to_string()
                .contains("Unsupported edge_geom_offsets version")
        );
        // Zero-copy
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let r = EdgeGeomOffsetsFile::read_from_bytes_zero_copy(leaked);
        assert!(r.is_err());
        assert!(
            r.unwrap_err()
                .to_string()
                .contains("Unsupported edge_geom_offsets version")
        );
    }

    #[test]
    fn offsets_non_monotonic_rejected() {
        // Hand-craft a body that's monotonic-violating but passes CRC.
        // Easiest: encode with valid offsets, manually build a forged
        // version with bad ordering and recompute CRCs.
        let bad = EdgeGeomOffsets {
            n_edges: 3,
            n_points: 10,
            offsets: Cow::Owned(vec![0, 5, 3, 10]), // non-monotonic at index 1->2
        };
        let bytes = EdgeGeomOffsetsFile::encode(&bad);
        let r = EdgeGeomOffsetsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("non-monotonic"));
    }

    #[test]
    fn offsets_bad_first_entry_rejected() {
        let bad = EdgeGeomOffsets {
            n_edges: 2,
            n_points: 5,
            offsets: Cow::Owned(vec![1, 3, 5]), // offsets[0] != 0
        };
        let bytes = EdgeGeomOffsetsFile::encode(&bad);
        let r = EdgeGeomOffsetsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("offsets[0]"));
    }

    #[test]
    fn offsets_last_entry_must_match_n_points() {
        let bad = EdgeGeomOffsets {
            n_edges: 2,
            n_points: 7, // mismatch with offsets[2] = 5
            offsets: Cow::Owned(vec![0, 3, 5]),
        };
        let bytes = EdgeGeomOffsetsFile::encode(&bad);
        let r = EdgeGeomOffsetsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("n_points"));
    }

    #[test]
    fn points_roundtrip_owned() {
        let original = sample_points();
        let bytes = EdgeGeomPointsFile::encode(&original);
        let parsed = EdgeGeomPointsFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.n_points, original.n_points);
        assert_eq!(parsed.bbox_min_lon, original.bbox_min_lon);
        assert_eq!(parsed.bbox_max_lat, original.bbox_max_lat);
        assert_eq!(parsed.points.as_ref(), original.points.as_ref());
    }

    #[test]
    fn points_zero_copy_matches_owned() {
        let original = sample_points();
        let bytes = EdgeGeomPointsFile::encode(&original);
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let owned = EdgeGeomPointsFile::read_from_bytes(leaked).expect("owned");
        let zc = EdgeGeomPointsFile::read_from_bytes_zero_copy(leaked).expect("zc");
        assert_eq!(owned.points.as_ref(), zc.points.as_ref());
        assert!(matches!(zc.points, Cow::Borrowed(_)));
    }

    #[test]
    fn points_corruption_detected() {
        let original = sample_points();
        let mut bytes = EdgeGeomPointsFile::encode(&original);
        bytes[HEADER_SIZE + 8] ^= 0xFF;
        let r = EdgeGeomPointsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("CRC mismatch"));
    }

    #[test]
    fn points_bad_magic_rejected_both_paths() {
        let original = sample_points();
        let mut bytes = EdgeGeomPointsFile::encode(&original);
        bytes[0] ^= 0xFF;
        let r = EdgeGeomPointsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("Invalid magic"));
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let r = EdgeGeomPointsFile::read_from_bytes_zero_copy(leaked);
        assert!(r.is_err());
        assert!(r.unwrap_err().to_string().contains("Invalid magic"));
    }

    #[test]
    fn points_bad_version_rejected_both_paths() {
        let original = sample_points();
        let mut bytes = EdgeGeomPointsFile::encode(&original);
        bytes[4] = 99;
        bytes[5] = 0;
        let r = EdgeGeomPointsFile::read_from_bytes(&bytes);
        assert!(r.is_err());
        assert!(
            r.unwrap_err()
                .to_string()
                .contains("Unsupported edge_geom_points version")
        );
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let r = EdgeGeomPointsFile::read_from_bytes_zero_copy(leaked);
        assert!(r.is_err());
        assert!(
            r.unwrap_err()
                .to_string()
                .contains("Unsupported edge_geom_points version")
        );
    }

    #[test]
    fn empty_geometry_roundtrip() {
        let off = EdgeGeomOffsets {
            n_edges: 0,
            n_points: 0,
            offsets: Cow::Owned(vec![0]),
        };
        let pts = EdgeGeomPoints {
            n_points: 0,
            bbox_min_lon: 0,
            bbox_min_lat: 0,
            bbox_max_lon: 0,
            bbox_max_lat: 0,
            points: Cow::Owned(vec![]),
        };
        let off_bytes = EdgeGeomOffsetsFile::encode(&off);
        let pts_bytes = EdgeGeomPointsFile::encode(&pts);
        let parsed_off = EdgeGeomOffsetsFile::read_from_bytes(&off_bytes).expect("off");
        let parsed_pts = EdgeGeomPointsFile::read_from_bytes(&pts_bytes).expect("pts");
        assert_eq!(parsed_off.n_edges, 0);
        assert_eq!(parsed_off.n_points, 0);
        assert_eq!(parsed_pts.n_points, 0);
        assert!(parsed_pts.points.is_empty());
    }

    #[test]
    fn truncated_offsets_rejected() {
        let original = sample_offsets();
        let bytes = EdgeGeomOffsetsFile::encode(&original);
        let truncated = &bytes[..bytes.len() - 8];
        let r = EdgeGeomOffsetsFile::read_from_bytes(truncated);
        assert!(r.is_err());
    }

    #[test]
    fn truncated_points_rejected() {
        let original = sample_points();
        let bytes = EdgeGeomPointsFile::encode(&original);
        let truncated = &bytes[..bytes.len() - 8];
        let r = EdgeGeomPointsFile::read_from_bytes(truncated);
        assert!(r.is_err());
    }
}
