//! ebg.nodes format - EBG node table (directed NBG edges)
//!
//! # Zero-copy reader (#152)
//!
//! The body is a flat array of fixed-size 24-byte records. `EbgNode`
//! is `#[repr(C)]` with six `u32` fields in declared order, so its
//! in-memory layout matches the on-disk record byte-for-byte. The
//! container guarantees 8-byte section alignment and the 64-byte
//! header keeps the body 4-byte-aligned at the section-relative
//! offset, so `bytemuck::cast_slice::<u8, EbgNode>` works from the
//! mmap with no heap copy.

use anyhow::Result;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4542474E; // "EBGN"
const VERSION: u16 = 1;
const HEADER_LEN: usize = 64;
const FOOTER_LEN: usize = 16;
const NODE_RECORD_LEN: usize = 24;

/// One EBG node record. `#[repr(C)]` + all-u32 fields makes this Pod
/// with no padding; on-disk layout is byte-identical to in-memory
/// layout, which is what the zero-copy reader (#152) relies on.
#[derive(Debug, Clone, Copy, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
pub struct EbgNode {
    pub tail_nbg: u32,    // compact NBG node id
    pub head_nbg: u32,    // compact NBG node id
    pub geom_idx: u32,    // index into nbg.geo record
    pub length_mm: u32,   // copy of nbg.geo.length_mm
    pub class_bits: u32,  // ferry, bridge, tunnel, roundabout, ford, etc.
    pub primary_way: u32, // lower 32 bits of first_osm_way_id
}

const _: () = assert!(std::mem::size_of::<EbgNode>() == NODE_RECORD_LEN);
const _: () = assert!(std::mem::align_of::<EbgNode>() == 4);

#[derive(Debug)]
pub struct EbgNodes {
    pub n_nodes: u32,
    pub created_unix: u64,
    pub inputs_sha: [u8; 32],
    /// Flat node array. Borrowed (zero-copy) when read from a
    /// `'static` byte slice (mmap-backed container section), owned
    /// otherwise. Indexed by `ebg_id`.
    pub nodes: Cow<'static, [EbgNode]>,
}

pub struct EbgNodesFile;

impl EbgNodesFile {
    /// Write EBG nodes to file
    pub fn write<P: AsRef<Path>>(path: P, data: &EbgNodes) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (64 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_nodes_bytes = data.n_nodes.to_le_bytes();
        let created_unix_bytes = data.created_unix.to_le_bytes();
        let padding = [0u8; 12]; // Pad to 64 bytes: 4+2+2+4+8+32 = 52, need 12 more

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_nodes_bytes)?;
        writer.write_all(&created_unix_bytes)?;
        writer.write_all(&data.inputs_sha)?;
        writer.write_all(&padding)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_nodes_bytes);
        crc_digest.update(&created_unix_bytes);
        crc_digest.update(&data.inputs_sha);
        crc_digest.update(&padding);

        // Body: n_nodes records (24 bytes each)
        for node in data.nodes.iter() {
            let tail_bytes = node.tail_nbg.to_le_bytes();
            let head_bytes = node.head_nbg.to_le_bytes();
            let geom_bytes = node.geom_idx.to_le_bytes();
            let length_bytes = node.length_mm.to_le_bytes();
            let class_bytes = node.class_bits.to_le_bytes();
            let way_bytes = node.primary_way.to_le_bytes();

            writer.write_all(&tail_bytes)?;
            writer.write_all(&head_bytes)?;
            writer.write_all(&geom_bytes)?;
            writer.write_all(&length_bytes)?;
            writer.write_all(&class_bytes)?;
            writer.write_all(&way_bytes)?;

            crc_digest.update(&tail_bytes);
            crc_digest.update(&head_bytes);
            crc_digest.update(&geom_bytes);
            crc_digest.update(&length_bytes);
            crc_digest.update(&class_bytes);
            crc_digest.update(&way_bytes);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        let file_crc = body_crc;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read EBG nodes from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<EbgNodes> {
        Self::read_from_reader(BufReader::new(File::open(path)?))
    }

    pub fn read_from_bytes(bytes: &[u8]) -> Result<EbgNodes> {
        Self::read_from_reader(std::io::Cursor::new(bytes))
    }

    fn read_from_reader<R: Read>(mut reader: R) -> Result<EbgNodes> {
        let mut crc_digest = crc::Digest::new();

        let mut header = vec![0u8; HEADER_LEN];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        anyhow::ensure!(
            magic == MAGIC,
            "Invalid magic in ebg.nodes: expected 0x{:08X}, got 0x{:08X}",
            MAGIC,
            magic
        );
        let version = u16::from_le_bytes([header[4], header[5]]);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported ebg.nodes version {version}, expected {VERSION}",
        );

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let created_unix = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[20..52]);

        // Read the body as one byte block, CRC it, then cast to
        // `&[EbgNode]` and copy into an owned Vec. The cast is
        // alignment-safe — the temporary buffer starts at an aligned
        // address, but we copy into a new Vec anyway since the bytes
        // here are not `'static`. Owned-path callers (e.g. directory
        // load) end up with the same semantics as before.
        let body_len = NODE_RECORD_LEN
            .checked_mul(n_nodes as usize)
            .ok_or_else(|| anyhow::anyhow!("ebg.nodes body size overflow for n_nodes={n_nodes}"))?;
        let mut body = vec![0u8; body_len];
        reader.read_exact(&mut body)?;
        crc_digest.update(&body);

        let mut nodes: Vec<EbgNode> = Vec::with_capacity(n_nodes as usize);
        // SAFETY-style note: bytemuck::pod_read_unaligned is safe and
        // handles arbitrary alignment of the source bytes.
        for chunk in body.chunks_exact(NODE_RECORD_LEN) {
            let arr: [u8; NODE_RECORD_LEN] =
                chunk.try_into().expect("chunks_exact yields full chunks");
            nodes.push(bytemuck::pod_read_unaligned::<EbgNode>(&arr));
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; FOOTER_LEN];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in ebg.nodes: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(EbgNodes {
            n_nodes,
            created_unix,
            inputs_sha,
            nodes: Cow::Owned(nodes),
        })
    }

    /// Zero-copy reader for `'static` byte slices (mmap-backed
    /// container sections). The body is reinterpreted as
    /// `&'static [EbgNode]` directly from the mapping — no heap
    /// allocation. CRC is verified before returning.
    ///
    /// The caller (the container) guarantees that the section bytes
    /// start at an 8-byte boundary; combined with the 64-byte header,
    /// the body slice starts at a 4-byte boundary, which matches
    /// `align_of::<EbgNode>() == 4`.
    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<EbgNodes> {
        anyhow::ensure!(
            bytes.len() >= HEADER_LEN + FOOTER_LEN,
            "ebg.nodes too short for header+footer: {} bytes",
            bytes.len()
        );
        debug_assert_eq!(
            bytes.as_ptr() as usize % 8,
            0,
            "ebg.nodes section start must be 8-byte aligned"
        );

        let header = &bytes[..HEADER_LEN];
        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        anyhow::ensure!(
            magic == MAGIC,
            "Invalid magic in ebg.nodes: expected 0x{:08X}, got 0x{:08X}",
            MAGIC,
            magic
        );
        let version = u16::from_le_bytes([header[4], header[5]]);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported ebg.nodes version {version}, expected {VERSION}",
        );

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let created_unix = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[20..52]);

        let body_len = NODE_RECORD_LEN
            .checked_mul(n_nodes as usize)
            .ok_or_else(|| anyhow::anyhow!("ebg.nodes body size overflow for n_nodes={n_nodes}"))?;
        let body_end = HEADER_LEN
            .checked_add(body_len)
            .ok_or_else(|| anyhow::anyhow!("ebg.nodes section size overflow"))?;
        anyhow::ensure!(
            bytes.len() == body_end + FOOTER_LEN,
            "ebg.nodes length mismatch: declared {}, expected body+footer {}",
            bytes.len(),
            body_end + FOOTER_LEN
        );

        let body = &bytes[HEADER_LEN..body_end];
        let footer = &bytes[body_end..body_end + FOOTER_LEN];

        // CRC over header + body, mirroring the legacy reader.
        let mut crc_digest = crc::Digest::new();
        crc_digest.update(header);
        crc_digest.update(body);
        let computed = crc_digest.finalize();
        let stored = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed == stored,
            "CRC64 mismatch in ebg.nodes: computed 0x{:016X}, stored 0x{:016X}",
            computed,
            stored
        );

        let nodes: &'static [EbgNode] = bytemuck::cast_slice(body);

        Ok(EbgNodes {
            n_nodes,
            created_unix,
            inputs_sha,
            nodes: Cow::Borrowed(nodes),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Seek, SeekFrom, Write as IoWrite};
    use tempfile::NamedTempFile;

    fn make_test_nodes() -> EbgNodes {
        EbgNodes {
            n_nodes: 3,
            created_unix: 1700000000,
            inputs_sha: [0xAB; 32],
            nodes: Cow::Owned(vec![
                EbgNode {
                    tail_nbg: 0,
                    head_nbg: 1,
                    geom_idx: 100,
                    length_mm: 5000,
                    class_bits: 0,
                    primary_way: 42,
                },
                EbgNode {
                    tail_nbg: 1,
                    head_nbg: 2,
                    geom_idx: 101,
                    length_mm: 3000,
                    class_bits: 1,
                    primary_way: 43,
                },
                EbgNode {
                    tail_nbg: 2,
                    head_nbg: 0,
                    geom_idx: 102,
                    length_mm: 7000,
                    class_bits: 0,
                    primary_way: 44,
                },
            ]),
        }
    }

    #[test]
    fn test_roundtrip() -> Result<()> {
        let data = make_test_nodes();
        let tmp = NamedTempFile::new()?;
        EbgNodesFile::write(tmp.path(), &data)?;
        let loaded = EbgNodesFile::read(tmp.path())?;

        assert_eq!(loaded.n_nodes, 3);
        assert_eq!(loaded.created_unix, 1700000000);
        assert_eq!(loaded.inputs_sha, [0xAB; 32]);
        assert_eq!(loaded.nodes.len(), 3);
        assert_eq!(loaded.nodes[0].tail_nbg, 0);
        assert_eq!(loaded.nodes[0].head_nbg, 1);
        assert_eq!(loaded.nodes[1].length_mm, 3000);
        assert_eq!(loaded.nodes[2].primary_way, 44);
        Ok(())
    }

    #[test]
    fn test_crc_detects_body_corruption() -> Result<()> {
        let data = make_test_nodes();
        let tmp = NamedTempFile::new()?;
        EbgNodesFile::write(tmp.path(), &data)?;

        // Corrupt a byte in the body (first node record, offset 64)
        {
            let mut file = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
            file.seek(SeekFrom::Start(64))?;
            file.write_all(&[0xFF])?;
        }

        let result = EbgNodesFile::read(tmp.path());
        assert!(result.is_err(), "corrupted file should fail CRC check");
        let err_msg = result.unwrap_err().to_string();
        assert!(
            err_msg.contains("CRC64 mismatch"),
            "error should mention CRC: {}",
            err_msg
        );
        Ok(())
    }

    #[test]
    fn test_crc_detects_header_corruption() -> Result<()> {
        let data = make_test_nodes();
        let tmp = NamedTempFile::new()?;
        EbgNodesFile::write(tmp.path(), &data)?;

        // Corrupt a byte in the header (inputs_sha area, offset 30)
        {
            let mut file = std::fs::OpenOptions::new().write(true).open(tmp.path())?;
            file.seek(SeekFrom::Start(30))?;
            file.write_all(&[0x00])?;
        }

        let result = EbgNodesFile::read(tmp.path());
        assert!(result.is_err(), "corrupted header should fail CRC check");
        Ok(())
    }
}
