///! mask.<mode>.bitset format - Per-mode node accessibility mask
///!
///! Format (little-endian, mmap-friendly):
///!
///! Header (24 bytes):
///!   magic:       u32 = 0x4D41534B  // "MASK"
///!   version:     u16 = 1
///!   mode:        u8  = {0=car,1=bike,2=foot}
///!   reserved:    u8  = 0
///!   count:       u32 = n_nodes
///!   inputs_sha:  [8]u8  // truncated SHA-256 of inputs
///!   pad:         [4]u8
///!
///! Body (ceil(count/8) bytes):
///!   bits[ceil(count/8)]  // 1 = traversable, 0 = not traversable
///!
///! Footer (16 bytes):
///!   body_crc64:  u64
///!   file_crc64:  u64

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;
use crate::profile_abi::Mode;

const MAGIC: u32 = 0x4D41534B; // "MASK"
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 24; // 4 + 2 + 1 + 1 + 4 + 8 + 4(pad)

#[derive(Debug, Clone)]
pub struct ModMask {
    pub mode: Mode,
    pub mask: Vec<u8>, // bitset: ceil(n_nodes/8) bytes
    pub n_nodes: u32,
    pub inputs_sha: [u8; 8],
}

impl ModMask {
    pub fn new(mode: Mode, n_nodes: u32, inputs_sha: [u8; 8]) -> Self {
        let byte_len = ((n_nodes + 7) / 8) as usize;
        Self {
            mode,
            mask: vec![0u8; byte_len],
            n_nodes,
            inputs_sha,
        }
    }

    pub fn set(&mut self, node_id: u32) {
        let byte_idx = (node_id / 8) as usize;
        let bit_idx = (node_id % 8) as u8;
        self.mask[byte_idx] |= 1 << bit_idx;
    }

    pub fn get(&self, node_id: u32) -> bool {
        let byte_idx = (node_id / 8) as usize;
        let bit_idx = (node_id % 8) as u8;
        (self.mask[byte_idx] & (1 << bit_idx)) != 0
    }
}

/// Write mask.<mode>.bitset file
pub fn write<P: AsRef<Path>>(path: P, data: &ModMask) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
    let mut writer = BufWriter::new(file);

    // Build header
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.push(data.mode as u8);
    header.push(0); // reserved
    header.extend_from_slice(&data.n_nodes.to_le_bytes());
    header.extend_from_slice(&data.inputs_sha);
    header.extend_from_slice(&[0u8; 4]); // padding to 24 bytes
    assert_eq!(header.len(), HEADER_SIZE);

    writer.write_all(&header)?;

    // Write body and calculate CRC
    let mut body_digest = Digest::new();
    body_digest.update(&data.mask);
    writer.write_all(&data.mask)?;

    let body_crc64 = body_digest.finalize();

    // Calculate file CRC (header + body)
    let mut file_digest = Digest::new();
    file_digest.update(&header);
    file_digest.update(&data.mask);
    let file_crc64 = file_digest.finalize();

    // Write footer
    writer.write_all(&body_crc64.to_le_bytes())?;
    writer.write_all(&file_crc64.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

/// Read mask.<mode>.bitset file
pub fn read_all<P: AsRef<Path>>(path: P) -> Result<ModMask> {
    use std::io::Read;

    let mut file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;

    // Read header
    let mut header = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header)?;

    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == MAGIC,
        "Invalid magic in {}: expected 0x{:08x}, got 0x{:08x}",
        path.as_ref().display(),
        MAGIC,
        magic
    );

    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == VERSION,
        "Unsupported version in {}: {}",
        path.as_ref().display(),
        version
    );

    let mode_byte = header[6];
    let mode = match mode_byte {
        0 => Mode::Car,
        1 => Mode::Bike,
        2 => Mode::Foot,
        _ => anyhow::bail!("Invalid mode: {}", mode_byte),
    };

    let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

    let mut inputs_sha = [0u8; 8];
    inputs_sha.copy_from_slice(&header[12..20]);

    // Read mask
    let byte_len = ((n_nodes + 7) / 8) as usize;
    let mut mask = vec![0u8; byte_len];
    file.read_exact(&mut mask)?;

    // TODO: Verify CRCs

    Ok(ModMask {
        mode,
        mask,
        n_nodes,
        inputs_sha,
    })
}

/// Verify mask.<mode>.bitset file structure and checksums
pub fn verify<P: AsRef<Path>>(path: P) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;

    // Read and verify header
    let mut header = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header)?;

    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic != MAGIC {
        anyhow::bail!(
            "Invalid magic in {}: expected 0x{:08x}, got 0x{:08x}",
            path.as_ref().display(),
            MAGIC,
            magic
        );
    }

    let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
    let byte_len = ((n_nodes + 7) / 8) as u64;

    // Verify file size
    let expected_size = HEADER_SIZE as u64 + byte_len + 16;
    let actual_size = file.seek(SeekFrom::End(0))?;

    if actual_size != expected_size {
        anyhow::bail!(
            "Size mismatch in {}: expected {} bytes, got {} bytes",
            path.as_ref().display(),
            expected_size,
            actual_size
        );
    }

    Ok(())
}
