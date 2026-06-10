//! Flat mmap-friendly per-edge OSM node ID chains (#460 follow-up).
//!
//! Companion to [`super::edge_geom`]: where `edge_geom_*` stores each NBG
//! edge's polyline *coordinates*, these sections store the same edge's OSM
//! node *IDs* — every intermediate geometry node along the underlying OSM
//! way, in the edge's canonical (u→v) direction. They are produced in step
//! 3 (`emit_edges` has the full per-way node chain in scope — the only
//! place the coord→id association is unambiguous) and carried through the
//! container so the serve path can expand any NBG edge to per-OSM-segment
//! `(osm_node_from, osm_node_to)` rows. Production motivation: at NBG
//! granularity ~49% of `edges_flow` mass keyed to node pairs absent from
//! per-segment reference tables (#460).
//!
//! Two section types, both CRC-validated, magic+version-checked, zero-copy
//! readable via `bytemuck::cast_slice`:
//!
//! - [`EdgeOsmOffsetsFile`] — `shared/edge_osm_offsets`. CSR-style
//!   `[u32; n_edges + 1]` of cumulative ID counts indexing into the ids
//!   body. `offsets[i]..offsets[i+1]` is the half-open range of id
//!   indices for edge `i`. Indexing space: NBG undirected edge ids — the
//!   SAME space as `edge_geom` (`EbgNode.geom_idx`), and per-edge counts
//!   are identical to the polyline's point counts (one id per vertex).
//! - [`EdgeOsmIdsFile`] — `shared/edge_osm_ids`. Flat `[i64; n_ids]` of
//!   OSM node IDs.
//!
//! The same encodings double as step-3 output files
//! (`nbg.edge_osm.offsets` / `nbg.edge_osm.ids`) which `pack` reads.
//!
//! # On-disk layout
//!
//! Headers are 32 bytes (u64-aligned). Footers are 16 bytes:
//! `body_crc : u64 || file_crc : u64`. All fields little-endian.

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::Arc;

use super::crc::Digest;
use super::mmap::ArcCow;

// ---------- Constants -------------------------------------------------------

/// Magic for `shared/edge_osm_offsets`. ASCII "EOOF" (LE byte-order).
pub const EDGE_OSM_OFFSETS_MAGIC: u32 = 0x464F_4F45;
/// Magic for `shared/edge_osm_ids`. ASCII "EOID" (LE byte-order).
pub const EDGE_OSM_IDS_MAGIC: u32 = 0x4449_4F45;

const EDGE_OSM_VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 16;

// ---------- edge_osm_offsets ------------------------------------------------

/// Parsed `shared/edge_osm_offsets` section.
#[derive(Debug, Clone)]
pub struct EdgeOsmOffsets {
    pub n_edges: u32,
    pub n_ids: u32,
    /// Cumulative ID counts. `offsets[edge_id]..offsets[edge_id + 1]`
    /// indexes the half-open id range for NBG edge `edge_id` in the ids
    /// body. Length is `n_edges + 1`; `offsets[0] = 0`,
    /// `offsets[n_edges] = n_ids`.
    pub offsets: ArcCow<u32>,
}

pub struct EdgeOsmOffsetsFile;

impl EdgeOsmOffsetsFile {
    pub fn encode(o: &EdgeOsmOffsets) -> Vec<u8> {
        let expected = (o.n_edges as usize)
            .checked_add(1)
            .expect("edge_osm_offsets length overflow");
        assert_eq!(
            o.offsets.len(),
            expected,
            "edge_osm_offsets length must be n_edges + 1 (got {} for n_edges={})",
            o.offsets.len(),
            o.n_edges
        );
        let body_len = expected
            .checked_mul(4)
            .expect("edge_osm_offsets body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header
        out.extend_from_slice(&EDGE_OSM_OFFSETS_MAGIC.to_le_bytes());
        out.extend_from_slice(&EDGE_OSM_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // _pad0
        out.extend_from_slice(&o.n_edges.to_le_bytes());
        out.extend_from_slice(&o.n_ids.to_le_bytes());
        out.extend_from_slice(&[0u8; 16]); // _pad1 — pads to 32-byte HEADER_SIZE
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body — LE bytes (bytemuck cast assumes host endian on read).
        for &v in o.offsets.as_slice() {
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

    pub fn write<P: AsRef<Path>>(path: P, o: &EdgeOsmOffsets) -> Result<()> {
        let bytes = Self::encode(o);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    /// Owning reader: copies the body into a `Vec<u32>`.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<EdgeOsmOffsets> {
        let parsed = parse_offsets_header_and_check(bytes, true)?;
        let mut offsets = Vec::with_capacity(parsed.expected_entries);
        for chunk in parsed.body.chunks_exact(4) {
            offsets.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        sanity_check_offsets(&offsets, parsed.n_ids)?;
        Ok(EdgeOsmOffsets {
            n_edges: parsed.n_edges,
            n_ids: parsed.n_ids,
            offsets: ArcCow::from_vec(offsets),
        })
    }

    pub fn read<P: AsRef<Path>>(path: P) -> Result<EdgeOsmOffsets> {
        let bytes = std::fs::read(path.as_ref())?;
        Self::read_from_bytes(&bytes)
    }

    /// Production mmap-backed reader. CRC walking is the caller's
    /// responsibility (driven through the lazy CRC layer).
    pub fn read_from_mmap_unverified(
        mmap: Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> Result<EdgeOsmOffsets> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "edge_osm_offsets section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let (n_edges, n_ids, expected_entries) = {
            let bytes = &mmap[byte_offset..byte_offset + byte_len];
            let parsed = parse_offsets_header_and_check(bytes, false)?;
            let off_slice: &[u32] = bytemuck::cast_slice(parsed.body);
            sanity_check_offsets(off_slice, parsed.n_ids)?;
            (parsed.n_edges, parsed.n_ids, parsed.expected_entries)
        };
        let body_byte_offset = byte_offset + HEADER_SIZE;
        let offsets = ArcCow::<u32>::from_mmap(mmap, body_byte_offset, expected_entries)?;
        Ok(EdgeOsmOffsets {
            n_edges,
            n_ids,
            offsets,
        })
    }
}

struct ParsedOffsets<'a> {
    n_edges: u32,
    n_ids: u32,
    expected_entries: usize,
    body: &'a [u8],
}

fn parse_offsets_header_and_check(bytes: &[u8], verify_crc: bool) -> Result<ParsedOffsets<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "edge_osm_offsets too short: {} bytes",
        bytes.len()
    );
    let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    anyhow::ensure!(
        magic == EDGE_OSM_OFFSETS_MAGIC,
        "edge_osm_offsets bad magic: {magic:#010x}"
    );
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    anyhow::ensure!(
        version == EDGE_OSM_VERSION,
        "edge_osm_offsets unsupported version: {version}"
    );
    let n_edges = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let n_ids = u32::from_le_bytes([bytes[12], bytes[13], bytes[14], bytes[15]]);
    let expected_entries = (n_edges as usize)
        .checked_add(1)
        .ok_or_else(|| anyhow::anyhow!("edge_osm_offsets n_edges overflow"))?;
    let body_len = expected_entries
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("edge_osm_offsets body length overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_len + FOOTER_SIZE,
        "edge_osm_offsets size mismatch: {} bytes, expected {}",
        bytes.len(),
        HEADER_SIZE + body_len + FOOTER_SIZE
    );
    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_len];
    if verify_crc {
        verify_footer(bytes, body_len, "edge_osm_offsets")?;
    }
    Ok(ParsedOffsets {
        n_edges,
        n_ids,
        expected_entries,
        body,
    })
}

fn sanity_check_offsets(offsets: &[u32], n_ids: u32) -> Result<()> {
    anyhow::ensure!(
        offsets.first() == Some(&0),
        "edge_osm_offsets must start at 0"
    );
    anyhow::ensure!(
        offsets.last() == Some(&n_ids),
        "edge_osm_offsets must end at n_ids ({n_ids})"
    );
    anyhow::ensure!(
        offsets.windows(2).all(|w| w[0] <= w[1]),
        "edge_osm_offsets must be non-decreasing"
    );
    Ok(())
}

// ---------- edge_osm_ids ----------------------------------------------------

/// Parsed `shared/edge_osm_ids` section.
#[derive(Debug, Clone)]
pub struct EdgeOsmIds {
    pub n_ids: u32,
    /// Flat OSM node IDs, one per polyline vertex, in each edge's
    /// canonical (u→v) direction.
    pub ids: ArcCow<i64>,
}

pub struct EdgeOsmIdsFile;

impl EdgeOsmIdsFile {
    pub fn encode(o: &EdgeOsmIds) -> Vec<u8> {
        assert_eq!(
            o.ids.len(),
            o.n_ids as usize,
            "edge_osm_ids length must equal n_ids"
        );
        let body_len = (o.n_ids as usize)
            .checked_mul(8)
            .expect("edge_osm_ids body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header
        out.extend_from_slice(&EDGE_OSM_IDS_MAGIC.to_le_bytes());
        out.extend_from_slice(&EDGE_OSM_VERSION.to_le_bytes());
        out.extend_from_slice(&0u16.to_le_bytes()); // _pad0
        out.extend_from_slice(&o.n_ids.to_le_bytes());
        out.extend_from_slice(&[0u8; 20]); // _pad1 — pads to 32-byte HEADER_SIZE
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body
        for &v in o.ids.as_slice() {
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

    pub fn write<P: AsRef<Path>>(path: P, o: &EdgeOsmIds) -> Result<()> {
        let bytes = Self::encode(o);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    /// Owning reader: copies the body into a `Vec<i64>`.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<EdgeOsmIds> {
        let parsed = parse_ids_header_and_check(bytes, true)?;
        let mut ids = Vec::with_capacity(parsed.n_ids as usize);
        for chunk in parsed.body.chunks_exact(8) {
            ids.push(i64::from_le_bytes([
                chunk[0], chunk[1], chunk[2], chunk[3], chunk[4], chunk[5], chunk[6], chunk[7],
            ]));
        }
        Ok(EdgeOsmIds {
            n_ids: parsed.n_ids,
            ids: ArcCow::from_vec(ids),
        })
    }

    pub fn read<P: AsRef<Path>>(path: P) -> Result<EdgeOsmIds> {
        let bytes = std::fs::read(path.as_ref())?;
        Self::read_from_bytes(&bytes)
    }

    /// Production mmap-backed reader. CRC walking is the caller's
    /// responsibility (driven through the lazy CRC layer). The ids body
    /// sits at `byte_offset + 32` — container sections are 8-aligned, so
    /// the `&[i64]` alignment requirement holds (checked again inside
    /// [`ArcCow::from_mmap`]).
    pub fn read_from_mmap_unverified(
        mmap: Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> Result<EdgeOsmIds> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "edge_osm_ids section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let n_ids = {
            let bytes = &mmap[byte_offset..byte_offset + byte_len];
            let parsed = parse_ids_header_and_check(bytes, false)?;
            parsed.n_ids
        };
        let body_byte_offset = byte_offset + HEADER_SIZE;
        let ids = ArcCow::<i64>::from_mmap(mmap, body_byte_offset, n_ids as usize)?;
        Ok(EdgeOsmIds { n_ids, ids })
    }
}

struct ParsedIds<'a> {
    n_ids: u32,
    body: &'a [u8],
}

fn parse_ids_header_and_check(bytes: &[u8], verify_crc: bool) -> Result<ParsedIds<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "edge_osm_ids too short: {} bytes",
        bytes.len()
    );
    let magic = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]);
    anyhow::ensure!(
        magic == EDGE_OSM_IDS_MAGIC,
        "edge_osm_ids bad magic: {magic:#010x}"
    );
    let version = u16::from_le_bytes([bytes[4], bytes[5]]);
    anyhow::ensure!(
        version == EDGE_OSM_VERSION,
        "edge_osm_ids unsupported version: {version}"
    );
    let n_ids = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
    let body_len = (n_ids as usize)
        .checked_mul(8)
        .ok_or_else(|| anyhow::anyhow!("edge_osm_ids body length overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_len + FOOTER_SIZE,
        "edge_osm_ids size mismatch: {} bytes, expected {}",
        bytes.len(),
        HEADER_SIZE + body_len + FOOTER_SIZE
    );
    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_len];
    if verify_crc {
        verify_footer(bytes, body_len, "edge_osm_ids")?;
    }
    Ok(ParsedIds { n_ids, body })
}

// ---------- shared footer verification --------------------------------------

fn verify_footer(bytes: &[u8], body_len: usize, what: &str) -> Result<()> {
    let footer = &bytes[HEADER_SIZE + body_len..];
    let stored_body_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let stored_file_crc = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    let mut body_d = Digest::new();
    body_d.update(&bytes[HEADER_SIZE..HEADER_SIZE + body_len]);
    anyhow::ensure!(
        body_d.finalize() == stored_body_crc,
        "{what} body CRC mismatch"
    );
    let mut file_d = Digest::new();
    file_d.update(&bytes[..HEADER_SIZE + body_len]);
    anyhow::ensure!(
        file_d.finalize() == stored_file_crc,
        "{what} file CRC mismatch"
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample() -> (EdgeOsmOffsets, EdgeOsmIds) {
        // 3 edges: [10,11,12], [12,13], [20,21,22,23]
        let offsets = EdgeOsmOffsets {
            n_edges: 3,
            n_ids: 9,
            offsets: ArcCow::from_vec(vec![0, 3, 5, 9]),
        };
        let ids = EdgeOsmIds {
            n_ids: 9,
            ids: ArcCow::from_vec(vec![10, 11, 12, 12, 13, 20, 21, 22, 23]),
        };
        (offsets, ids)
    }

    #[test]
    fn offsets_roundtrip() {
        let (o, _) = sample();
        let bytes = EdgeOsmOffsetsFile::encode(&o);
        let back = EdgeOsmOffsetsFile::read_from_bytes(&bytes).unwrap();
        assert_eq!(back.n_edges, 3);
        assert_eq!(back.n_ids, 9);
        assert_eq!(back.offsets.as_slice(), &[0, 3, 5, 9]);
    }

    #[test]
    fn ids_roundtrip() {
        let (_, i) = sample();
        let bytes = EdgeOsmIdsFile::encode(&i);
        let back = EdgeOsmIdsFile::read_from_bytes(&bytes).unwrap();
        assert_eq!(back.n_ids, 9);
        assert_eq!(back.ids.as_slice(), &[10, 11, 12, 12, 13, 20, 21, 22, 23]);
    }

    #[test]
    fn offsets_rejects_corruption() {
        let (o, _) = sample();
        let mut bytes = EdgeOsmOffsetsFile::encode(&o);
        let mid = HEADER_SIZE + 4;
        bytes[mid] ^= 0xFF;
        assert!(EdgeOsmOffsetsFile::read_from_bytes(&bytes).is_err());
    }

    #[test]
    fn ids_rejects_bad_magic() {
        let (_, i) = sample();
        let mut bytes = EdgeOsmIdsFile::encode(&i);
        bytes[0] ^= 0xFF;
        assert!(EdgeOsmIdsFile::read_from_bytes(&bytes).is_err());
    }

    #[test]
    fn ids_rejects_truncation() {
        let (_, i) = sample();
        let bytes = EdgeOsmIdsFile::encode(&i);
        assert!(EdgeOsmIdsFile::read_from_bytes(&bytes[..bytes.len() - 1]).is_err());
    }

    #[test]
    fn offsets_rejects_non_monotonic() {
        let o = EdgeOsmOffsets {
            n_edges: 2,
            n_ids: 4,
            offsets: ArcCow::from_vec(vec![0, 3, 4]),
        };
        let mut bytes = EdgeOsmOffsetsFile::encode(&o);
        // Swap entries 1 and 2 (3↔4 → [0,4,3] not non-decreasing… but
        // also recompute CRCs so the monotonicity check is what trips).
        let b = HEADER_SIZE;
        bytes[b + 4..b + 8].copy_from_slice(&4u32.to_le_bytes());
        bytes[b + 8..b + 12].copy_from_slice(&3u32.to_le_bytes());
        // Recompute footer
        let body_len = 3 * 4;
        let mut body_d = Digest::new();
        body_d.update(&bytes[HEADER_SIZE..HEADER_SIZE + body_len]);
        let body_crc = body_d.finalize();
        let mut file_d = Digest::new();
        file_d.update(&bytes[..HEADER_SIZE + body_len]);
        let file_crc = file_d.finalize();
        let f = HEADER_SIZE + body_len;
        bytes[f..f + 8].copy_from_slice(&body_crc.to_le_bytes());
        bytes[f + 8..f + 16].copy_from_slice(&file_crc.to_le_bytes());
        let err = EdgeOsmOffsetsFile::read_from_bytes(&bytes);
        assert!(err.is_err(), "non-monotonic offsets must be rejected");
    }
}
