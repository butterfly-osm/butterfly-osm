//! ebg.csr format - EBG adjacency (CSR over EBG nodes)
//!
//! # Zero-copy reader (#152)
//!
//! Layout: header(64 bytes) | offsets ((n_nodes+1) × u64) | heads
//! (n_arcs × u32) | turn_idx (n_arcs × u32) | footer(16 bytes).
//!
//! - The container guarantees 8-byte section alignment.
//! - The 64-byte header keeps the offsets array u64-aligned.
//! - The heads and turn_idx u32 arrays only need 4-byte alignment.
//!
//! No padding is required between arrays for the zero-copy reader to
//! cast each slice with `bytemuck::cast_slice`.

use anyhow::Result;
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x45424743; // "EBGC"
const VERSION: u16 = 1;
const HEADER_LEN: usize = 64;
const FOOTER_LEN: usize = 16;

#[derive(Debug)]
pub struct EbgCsr {
    pub n_nodes: u32,
    pub n_arcs: u64,
    pub created_unix: u64,
    pub inputs_sha: [u8; 32],
    /// CSR offsets array (`n_nodes + 1`). Borrowed (zero-copy) when
    /// loaded from a `'static` byte slice, owned otherwise.
    pub offsets: Cow<'static, [u64]>,
    /// CSR heads array (`n_arcs`).
    pub heads: Cow<'static, [u32]>,
    /// Turn-table index per arc (`n_arcs`).
    pub turn_idx: Cow<'static, [u32]>,
}

pub struct EbgCsrFile;

impl EbgCsrFile {
    /// Write EBG CSR to file
    pub fn write<P: AsRef<Path>>(path: P, data: &EbgCsr) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (64 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_nodes_bytes = data.n_nodes.to_le_bytes();
        let n_arcs_bytes = data.n_arcs.to_le_bytes();
        let created_unix_bytes = data.created_unix.to_le_bytes();
        let padding = [0u8; 4]; // Pad to 64 bytes: 4+2+2+4+8+8+32 = 60, need 4 more

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_nodes_bytes)?;
        writer.write_all(&n_arcs_bytes)?;
        writer.write_all(&created_unix_bytes)?;
        writer.write_all(&data.inputs_sha)?;
        writer.write_all(&padding)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_nodes_bytes);
        crc_digest.update(&n_arcs_bytes);
        crc_digest.update(&created_unix_bytes);
        crc_digest.update(&data.inputs_sha);
        crc_digest.update(&padding);

        // Offsets
        for &offset in data.offsets.iter() {
            let bytes = offset.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Heads
        for &head in data.heads.iter() {
            let bytes = head.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Turn indices
        for &idx in data.turn_idx.iter() {
            let bytes = idx.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        let file_crc = body_crc;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read EBG CSR from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<EbgCsr> {
        Self::read_from_reader(BufReader::new(File::open(path)?))
    }

    pub fn read_from_bytes(bytes: &[u8]) -> Result<EbgCsr> {
        Self::read_from_reader(std::io::Cursor::new(bytes))
    }

    fn read_from_reader<R: Read>(mut reader: R) -> Result<EbgCsr> {
        let mut crc_digest = crc::Digest::new();

        let mut header = vec![0u8; HEADER_LEN];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        anyhow::ensure!(
            magic == MAGIC,
            "Invalid magic in ebg.csr: expected 0x{:08X}, got 0x{:08X}",
            MAGIC,
            magic
        );
        let version = u16::from_le_bytes([header[4], header[5]]);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported ebg.csr version {version}, expected {VERSION}",
        );

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_arcs = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let created_unix = u64::from_le_bytes([
            header[20], header[21], header[22], header[23], header[24], header[25], header[26],
            header[27],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[28..60]);

        // Read offsets
        let mut offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            offsets.push(u64::from_le_bytes(buf));
        }

        // Read heads
        let mut heads = Vec::with_capacity(n_arcs as usize);
        for _ in 0..n_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            heads.push(u32::from_le_bytes(buf));
        }

        // Read turn indices
        let mut turn_idx = Vec::with_capacity(n_arcs as usize);
        for _ in 0..n_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            turn_idx.push(u32::from_le_bytes(buf));
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; FOOTER_LEN];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in ebg.csr: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(EbgCsr {
            n_nodes,
            n_arcs,
            created_unix,
            inputs_sha,
            offsets: Cow::Owned(offsets),
            heads: Cow::Owned(heads),
            turn_idx: Cow::Owned(turn_idx),
        })
    }

    /// Zero-copy reader for `'static` byte slices (mmap-backed
    /// container sections). Reinterprets the body arrays as borrowed
    /// slices into the mapping; CRC is verified before returning.
    ///
    /// Layout (#152):
    ///   header(64) | offsets((n_nodes+1) × u64)
    ///             | heads(n_arcs × u32)
    ///             | turn_idx(n_arcs × u32)
    ///             | footer(16)
    ///
    /// The 64-byte header keeps the offsets u64 array aligned. The
    /// heads/turn_idx u32 arrays only need 4-byte alignment which
    /// any cursor reaches naturally.
    pub fn read_from_bytes_zero_copy(bytes: &'static [u8]) -> Result<EbgCsr> {
        Self::read_from_bytes_zero_copy_inner(bytes, true)
    }

    /// Same as [`Self::read_from_bytes_zero_copy`] but elides the
    /// internal CRC walk over the body. Caller MUST guarantee the
    /// bytes have been verified upstream (e.g. via `LazyContainer`).
    pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<EbgCsr> {
        Self::read_from_bytes_zero_copy_inner(bytes, false)
    }

    fn read_from_bytes_zero_copy_inner(bytes: &'static [u8], verify: bool) -> Result<EbgCsr> {
        anyhow::ensure!(
            bytes.len() >= HEADER_LEN + FOOTER_LEN,
            "ebg.csr too short for header+footer: {} bytes",
            bytes.len()
        );
        debug_assert_eq!(
            bytes.as_ptr() as usize % 8,
            0,
            "ebg.csr section start must be 8-byte aligned"
        );

        let header = &bytes[..HEADER_LEN];
        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        anyhow::ensure!(
            magic == MAGIC,
            "Invalid magic in ebg.csr: expected 0x{:08X}, got 0x{:08X}",
            MAGIC,
            magic
        );
        let version = u16::from_le_bytes([header[4], header[5]]);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported ebg.csr version {version}, expected {VERSION}",
        );

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_arcs = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let created_unix = u64::from_le_bytes([
            header[20], header[21], header[22], header[23], header[24], header[25], header[26],
            header[27],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[28..60]);

        let n_offsets = (n_nodes as usize)
            .checked_add(1)
            .ok_or_else(|| anyhow::anyhow!("ebg.csr offsets count overflow"))?;
        let n_arcs_us =
            usize::try_from(n_arcs).map_err(|_| anyhow::anyhow!("ebg.csr n_arcs > usize::MAX"))?;

        let offsets_len = n_offsets
            .checked_mul(8)
            .ok_or_else(|| anyhow::anyhow!("ebg.csr offsets size overflow"))?;
        let heads_len = n_arcs_us
            .checked_mul(4)
            .ok_or_else(|| anyhow::anyhow!("ebg.csr heads size overflow"))?;
        let turn_len = heads_len;

        let off_start = HEADER_LEN;
        let off_end = off_start + offsets_len;
        let heads_end = off_end + heads_len;
        let turn_end = heads_end + turn_len;
        anyhow::ensure!(
            bytes.len() == turn_end + FOOTER_LEN,
            "ebg.csr length mismatch: declared {}, expected body+footer {}",
            bytes.len(),
            turn_end + FOOTER_LEN
        );

        let offsets: &'static [u64] = bytemuck::cast_slice(&bytes[off_start..off_end]);
        let heads: &'static [u32] = bytemuck::cast_slice(&bytes[off_end..heads_end]);
        let turn_idx: &'static [u32] = bytemuck::cast_slice(&bytes[heads_end..turn_end]);

        // CRC over header + body
        if verify {
            let mut crc_digest = crc::Digest::new();
            crc_digest.update(header);
            crc_digest.update(&bytes[off_start..turn_end]);
            let computed = crc_digest.finalize();
            let footer = &bytes[turn_end..turn_end + FOOTER_LEN];
            let stored = u64::from_le_bytes(footer[0..8].try_into().unwrap());
            anyhow::ensure!(
                computed == stored,
                "CRC64 mismatch in ebg.csr: computed 0x{:016X}, stored 0x{:016X}",
                computed,
                stored
            );
        }

        Ok(EbgCsr {
            n_nodes,
            n_arcs,
            created_unix,
            inputs_sha,
            offsets: Cow::Borrowed(offsets),
            heads: Cow::Borrowed(heads),
            turn_idx: Cow::Borrowed(turn_idx),
        })
    }
}
