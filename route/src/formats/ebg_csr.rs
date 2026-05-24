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
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use super::crc;
use super::mmap::ArcCow;

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
    /// CSR offsets array (`n_nodes + 1`). Owned when built or read
    /// from a plain file; Arc-backed mmap view when read from a
    /// container section. See [`ArcCow`] for the variant shape and
    /// the eviction story (#296).
    pub offsets: ArcCow<u64>,
    /// CSR heads array (`n_arcs`).
    pub heads: ArcCow<u32>,
    /// Turn-table index per arc (`n_arcs`).
    pub turn_idx: ArcCow<u32>,
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
            offsets: ArcCow::from_vec(offsets),
            heads: ArcCow::from_vec(heads),
            turn_idx: ArcCow::from_vec(turn_idx),
        })
    }

    /// Zero-copy reader for `'static` byte slices (test fixtures that
    /// leak a `Box<[u8]>`). Production loaders should use
    /// [`Self::read_from_mmap_unverified`] which keeps the
    /// `Arc<Mmap>` strong-count tied to the returned struct.
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

    /// Production mmap-backed reader (#296). Holds an `Arc<Mmap>`
    /// clone for the returned struct's lifetime — when the struct
    /// drops, the strong count decreases. Once all clones drop, the
    /// `Mmap` drops, `munmap` fires, and the kernel reclaims the
    /// pages.
    ///
    /// `byte_offset` and `byte_len` are the position and length of
    /// the section within the container, as recorded in the directory
    /// entry. CRC walking is the caller's responsibility (typically
    /// driven through the lazy CRC layer before this call).
    pub fn read_from_mmap_unverified(
        mmap: Arc<memmap2::Mmap>,
        byte_offset: usize,
        byte_len: usize,
    ) -> Result<EbgCsr> {
        anyhow::ensure!(
            byte_offset.saturating_add(byte_len) <= mmap.len(),
            "ebg.csr section out of bounds: off={byte_offset} len={byte_len} mmap_len={}",
            mmap.len()
        );
        let bytes = &mmap[byte_offset..byte_offset + byte_len];

        // Parse header to determine sub-array sizes within the body.
        let (n_nodes, n_arcs, created_unix, inputs_sha) = parse_header(bytes)?;

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

        // Section-relative offsets
        let off_start = HEADER_LEN;
        let off_end = off_start
            .checked_add(offsets_len)
            .ok_or_else(|| anyhow::anyhow!("ebg.csr offsets end overflow"))?;
        let heads_end = off_end
            .checked_add(heads_len)
            .ok_or_else(|| anyhow::anyhow!("ebg.csr heads end overflow"))?;
        let turn_end = heads_end
            .checked_add(turn_len)
            .ok_or_else(|| anyhow::anyhow!("ebg.csr turn end overflow"))?;
        let expected = turn_end
            .checked_add(FOOTER_LEN)
            .ok_or_else(|| anyhow::anyhow!("ebg.csr section size overflow"))?;
        anyhow::ensure!(
            byte_len == expected,
            "ebg.csr length mismatch: declared {byte_len}, expected {expected}",
        );

        // Container-absolute offsets for the mmap-backed ArcCow views.
        let offsets_byte_offset = byte_offset + off_start;
        let heads_byte_offset = byte_offset + off_end;
        let turn_byte_offset = byte_offset + heads_end;

        let offsets = ArcCow::<u64>::from_mmap(Arc::clone(&mmap), offsets_byte_offset, n_offsets)?;
        let heads = ArcCow::<u32>::from_mmap(Arc::clone(&mmap), heads_byte_offset, n_arcs_us)?;
        let turn_idx = ArcCow::<u32>::from_mmap(mmap, turn_byte_offset, n_arcs_us)?;

        Ok(EbgCsr {
            n_nodes,
            n_arcs,
            created_unix,
            inputs_sha,
            offsets,
            heads,
            turn_idx,
        })
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

        let (n_nodes, n_arcs, created_unix, inputs_sha) = parse_header(bytes)?;
        let header = &bytes[..HEADER_LEN];

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

        let offsets_slice: &[u64] = bytemuck::cast_slice(&bytes[off_start..off_end]);
        let heads_slice: &[u32] = bytemuck::cast_slice(&bytes[off_end..heads_end]);
        let turn_slice: &[u32] = bytemuck::cast_slice(&bytes[heads_end..turn_end]);

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

        // Test fixtures use this path — wrap the Vec'd copies in
        // `ArcCow::Owned`. The `bytes: &'static [u8]` lifetime here
        // means the caller leaked the buffer (typically `Box::leak`
        // in a #[cfg(test)] block); we don't carry that leak into
        // production storage. Production goes through
        // [`Self::read_from_mmap_unverified`].
        Ok(EbgCsr {
            n_nodes,
            n_arcs,
            created_unix,
            inputs_sha,
            offsets: ArcCow::from_vec(offsets_slice.to_vec()),
            heads: ArcCow::from_vec(heads_slice.to_vec()),
            turn_idx: ArcCow::from_vec(turn_slice.to_vec()),
        })
    }
}

/// Parse the 64-byte EBG CSR header and return the fixed fields.
/// Shared by the owned, zero-copy, and mmap-backed readers.
fn parse_header(bytes: &[u8]) -> Result<(u32, u64, u64, [u8; 32])> {
    anyhow::ensure!(
        bytes.len() >= HEADER_LEN + FOOTER_LEN,
        "ebg.csr too short for header+footer: {} bytes",
        bytes.len()
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
    Ok((n_nodes, n_arcs, created_unix, inputs_sha))
}
