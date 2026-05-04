//! Per-mode mapping sections (#153) — `orig_to_rank` + `filtered_to_original`.
//!
//! These are the **server-only** mapping arrays that let `ServerState`
//! drop `FilteredEbg` and `OrderEbg` from the container serve path.
//!
//! Both arrays are flat `[u32]`. A single shared on-disk format covers
//! both because they only differ in semantics:
//!
//! - `orig_to_rank` — indexed by original EBG node id, value =
//!   composed `perm[original_to_filtered[orig_id]]`. Sentinel
//!   `u32::MAX` when the original node is not in this mode's filtered
//!   subgraph (i.e. inaccessible). Length = `n_original_nodes`.
//! - `filtered_to_original` — indexed by filtered EBG node id, value =
//!   original EBG node id. Length = `n_filtered_nodes`. Same data as
//!   `FilteredEbg.filtered_to_original` today, packed standalone.
//!
//! ## On-disk layout
//!
//! ```text
//! header (32 bytes):
//!   magic   : u32 = 0x4D49 4458 = "MIDX" (Mode Index)
//!   version : u16 = 1
//!   kind    : u8  = 0 (orig_to_rank) | 1 (filtered_to_original)
//!   mode    : u8  = mode index
//!   count   : u32 = number of u32 entries
//!   _pad    : u32 = 0
//!   inputs_sha : [u8; 16]  // truncated SHA-256 of inputs
//! body:
//!   u32 entries[count]  // little-endian
//! footer (16 bytes):
//!   body_crc : u64
//!   file_crc : u64        // header || body
//! ```
//!
//! Header is 32 bytes — already u64-aligned. Container packing pads to
//! u64 (`butterfly_dat::ContainerWriter`) so the body `[u32]` is
//! naturally 4-byte aligned for `bytemuck::cast_slice`.

use anyhow::Result;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;

const MAGIC: u32 = 0x4D49_4458; // "MIDX"
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 32;
const FOOTER_SIZE: usize = 16;

/// Discriminates which mapping the section carries. Stored in the
/// header so a misnamed section is rejected with a useful error.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum ModeIndexKind {
    /// `orig_to_rank[orig_id]` → CCH rank, or `u32::MAX` if inaccessible.
    OrigToRank = 0,
    /// `filtered_to_original[filtered_id]` → original EBG node id.
    FilteredToOriginal = 1,
}

impl ModeIndexKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(Self::OrigToRank),
            1 => Some(Self::FilteredToOriginal),
            _ => None,
        }
    }
}

/// Parsed mapping section.
#[derive(Debug, Clone)]
pub struct ModeIndex {
    pub kind: ModeIndexKind,
    pub mode: u8,
    pub inputs_sha: [u8; 16],
    /// Flat `u32` array. Borrowed when read zero-copy from a mmap'd
    /// container, owned when read from a plain file or built in memory.
    pub data: Cow<'static, [u32]>,
}

impl ModeIndex {
    /// Borrowed slice view, regardless of the underlying ownership.
    #[inline]
    pub fn as_slice(&self) -> &[u32] {
        &self.data
    }

    /// Number of u32 entries.
    #[inline]
    pub fn len(&self) -> usize {
        self.data.len()
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }
}

/// File reader/writer for `ModeIndex` sections.
pub struct ModeIndexFile;

impl ModeIndexFile {
    /// Encode an index to its full on-disk byte representation
    /// (header || body || footer). Used by the pack writer.
    pub fn encode(idx: &ModeIndex) -> Vec<u8> {
        let count = idx.data.len();
        let body_len = count
            .checked_mul(4)
            .expect("ModeIndex body byte count overflow");
        let mut out = Vec::with_capacity(HEADER_SIZE + body_len + FOOTER_SIZE);

        // Header (32 bytes)
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.push(idx.kind as u8);
        out.push(idx.mode);
        out.extend_from_slice(&(count as u32).to_le_bytes());
        out.extend_from_slice(&0u32.to_le_bytes()); // _pad
        out.extend_from_slice(&idx.inputs_sha);
        debug_assert_eq!(out.len(), HEADER_SIZE);

        // Body
        for &v in idx.data.iter() {
            out.extend_from_slice(&v.to_le_bytes());
        }

        // Footer
        let mut body_digest = Digest::new();
        body_digest.update(&out[HEADER_SIZE..HEADER_SIZE + body_len]);
        let body_crc = body_digest.finalize();

        let mut file_digest = Digest::new();
        file_digest.update(&out[..HEADER_SIZE + body_len]);
        let file_crc = file_digest.finalize();

        out.extend_from_slice(&body_crc.to_le_bytes());
        out.extend_from_slice(&file_crc.to_le_bytes());
        out
    }

    /// Write a section to a standalone file (used by tests and any
    /// future tool that wants to inspect the format on disk).
    pub fn write<P: AsRef<Path>>(path: P, idx: &ModeIndex) -> Result<()> {
        let bytes = Self::encode(idx);
        let mut w = BufWriter::new(File::create(path.as_ref())?);
        w.write_all(&bytes)?;
        w.flush()?;
        Ok(())
    }

    /// Plain owning reader. Copies the body into a `Vec<u32>`.
    pub fn read_from_bytes(bytes: &[u8]) -> Result<ModeIndex> {
        let (kind, mode, inputs_sha, count, body) = parse_header_and_check(bytes)?;
        let mut v: Vec<u32> = Vec::with_capacity(count);
        for chunk in body.chunks_exact(4) {
            v.push(u32::from_le_bytes([chunk[0], chunk[1], chunk[2], chunk[3]]));
        }
        Ok(ModeIndex {
            kind,
            mode,
            inputs_sha,
            data: Cow::Owned(v),
        })
    }

    /// Zero-copy reader for `'static` byte slices (mmap-backed
    /// container sections). Reinterprets the body as `&'static [u32]`;
    /// CRCs are verified before returning.
    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<ModeIndex> {
        let (kind, mode, inputs_sha, _count, body) = parse_header_and_check(bytes)?;
        debug_assert_eq!(
            body.as_ptr() as usize % 4,
            0,
            "ModeIndex body must be 4-byte aligned"
        );
        let data: &'static [u32] = bytemuck::cast_slice(body);
        Ok(ModeIndex {
            kind,
            mode,
            inputs_sha,
            data: Cow::Borrowed(data),
        })
    }
}

/// Result of parsing a `ModeIndex` header: discriminator, mode byte,
/// inputs SHA, body element count, and the body byte slice.
type ParsedHeader<'a> = (ModeIndexKind, u8, [u8; 16], usize, &'a [u8]);

/// Parse the header, verify magic / version / CRCs, return the body
/// byte slice. Common to both readers.
fn parse_header_and_check(bytes: &[u8]) -> Result<ParsedHeader<'_>> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "ModeIndex too short for header+footer: {} bytes",
        bytes.len()
    );

    let header = &bytes[..HEADER_SIZE];
    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == MAGIC,
        "Invalid magic in ModeIndex: expected 0x{:08X}, got 0x{:08X}",
        MAGIC,
        magic
    );
    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == VERSION,
        "Unsupported ModeIndex version {}, expected {}",
        version,
        VERSION
    );
    let kind = ModeIndexKind::from_u8(header[6])
        .ok_or_else(|| anyhow::anyhow!("Invalid ModeIndex kind: {}", header[6]))?;
    let mode = header[7];
    let count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]) as usize;
    // header[12..16] reserved (zero)
    let mut inputs_sha = [0u8; 16];
    inputs_sha.copy_from_slice(&header[16..32]);

    let body_bytes = count
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("ModeIndex count * 4 overflow"))?;
    anyhow::ensure!(
        bytes.len() == HEADER_SIZE + body_bytes + FOOTER_SIZE,
        "ModeIndex length mismatch: declared header+body+footer = {}, actual = {}",
        HEADER_SIZE + body_bytes + FOOTER_SIZE,
        bytes.len()
    );

    let body = &bytes[HEADER_SIZE..HEADER_SIZE + body_bytes];

    // Verify CRCs.
    let mut body_digest = Digest::new();
    body_digest.update(body);
    let computed_body = body_digest.finalize();
    let mut file_digest = Digest::new();
    file_digest.update(&bytes[..HEADER_SIZE + body_bytes]);
    let computed_file = file_digest.finalize();

    let footer = &bytes[HEADER_SIZE + body_bytes..];
    let stored_body = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let stored_file = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    anyhow::ensure!(
        computed_body == stored_body,
        "ModeIndex body CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
        computed_body,
        stored_body
    );
    anyhow::ensure!(
        computed_file == stored_file,
        "ModeIndex file CRC mismatch: computed 0x{:016X}, stored 0x{:016X}",
        computed_file,
        stored_file
    );

    Ok((kind, mode, inputs_sha, count, body))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn roundtrip_owned() {
        let original = ModeIndex {
            kind: ModeIndexKind::OrigToRank,
            mode: 1,
            inputs_sha: [0xAB; 16],
            data: Cow::Owned(vec![0u32, 1, 2, u32::MAX, 4]),
        };
        let bytes = ModeIndexFile::encode(&original);
        let parsed = ModeIndexFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.kind, ModeIndexKind::OrigToRank);
        assert_eq!(parsed.mode, 1);
        assert_eq!(parsed.inputs_sha, [0xAB; 16]);
        assert_eq!(parsed.data.as_ref(), original.data.as_ref());
    }

    #[test]
    fn roundtrip_filtered_to_original() {
        let original = ModeIndex {
            kind: ModeIndexKind::FilteredToOriginal,
            mode: 2,
            inputs_sha: [0; 16],
            data: Cow::Owned((0..1000u32).collect()),
        };
        let bytes = ModeIndexFile::encode(&original);
        let parsed = ModeIndexFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.kind, ModeIndexKind::FilteredToOriginal);
        assert_eq!(parsed.mode, 2);
        assert_eq!(parsed.data.len(), 1000);
        for (i, &v) in parsed.data.iter().enumerate() {
            assert_eq!(v, i as u32);
        }
    }

    #[test]
    fn empty_roundtrip() {
        let original = ModeIndex {
            kind: ModeIndexKind::OrigToRank,
            mode: 0,
            inputs_sha: [0; 16],
            data: Cow::Owned(vec![]),
        };
        let bytes = ModeIndexFile::encode(&original);
        let parsed = ModeIndexFile::read_from_bytes(&bytes).expect("read");
        assert_eq!(parsed.data.len(), 0);
    }

    #[test]
    fn detect_corruption() {
        let original = ModeIndex {
            kind: ModeIndexKind::OrigToRank,
            mode: 0,
            inputs_sha: [0; 16],
            data: Cow::Owned(vec![1u32, 2, 3, 4]),
        };
        let mut bytes = ModeIndexFile::encode(&original);
        // Flip a body byte
        bytes[HEADER_SIZE + 4] ^= 0xFF;
        let res = ModeIndexFile::read_from_bytes(&bytes);
        assert!(res.is_err(), "corruption must fail CRC check");
        let err = res.unwrap_err().to_string();
        assert!(err.contains("CRC mismatch"), "unexpected error: {}", err);
    }

    #[test]
    fn reject_bad_magic() {
        let mut bytes = ModeIndexFile::encode(&ModeIndex {
            kind: ModeIndexKind::OrigToRank,
            mode: 0,
            inputs_sha: [0; 16],
            data: Cow::Owned(vec![0u32]),
        });
        bytes[0] ^= 0xFF;
        let res = ModeIndexFile::read_from_bytes(&bytes);
        assert!(res.is_err());
        assert!(res.unwrap_err().to_string().contains("Invalid magic"));
    }

    #[test]
    fn zero_copy_matches_owned() {
        let original = ModeIndex {
            kind: ModeIndexKind::FilteredToOriginal,
            mode: 3,
            inputs_sha: [0xDE; 16],
            data: Cow::Owned((0..256u32).map(|x| x.wrapping_mul(7)).collect()),
        };
        let bytes = ModeIndexFile::encode(&original);
        // Leak to get 'static lifetime for zero-copy reader.
        let leaked: &'static [u8] = Box::leak(bytes.into_boxed_slice());
        let owned = ModeIndexFile::read_from_bytes(leaked).expect("read owned");
        let zerocopy = ModeIndexFile::read_from_bytes_zero_copy(leaked).expect("read zero-copy");
        assert_eq!(owned.kind, zerocopy.kind);
        assert_eq!(owned.mode, zerocopy.mode);
        assert_eq!(owned.inputs_sha, zerocopy.inputs_sha);
        assert_eq!(owned.data.as_ref(), zerocopy.data.as_ref());
        assert!(matches!(zerocopy.data, Cow::Borrowed(_)));
    }
}
