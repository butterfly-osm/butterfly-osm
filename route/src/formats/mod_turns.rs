//! t.<mode>.u32 format - Per-mode turn penalties
//!
//! Format (little-endian, mmap-friendly):
//!
//! Header (32 bytes):
//!   magic:       u32 = 0x544D4F44  // "TMOD"
//!   version:     u16 = 2  // v2: seconds (was v1: deciseconds, #297)
//!   mode:        u8  = {0=car,1=bike,2=foot,...}
//!   reserved:    u8  = 0
//!   count:       u32 = n_arcs
//!   inputs_sha:  [16]u8  // truncated SHA-256 of inputs
//!
//! Body (count * u32):
//!   u32 penalty_s[count]  // seconds (0 = no penalty or mode not allowed)
//!
//! Footer (16 bytes):
//!   body_crc64:  u64
//!   file_crc64:  u64

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;
use crate::profile_abi::Mode;

const MAGIC: u32 = 0x544D4F44; // "TMOD"
const VERSION: u16 = 2;
const HEADER_SIZE: usize = 32; // 4 + 2 + 1 + 1 + 4 + 16 + 4(pad)

#[derive(Debug, Clone)]
pub struct ModTurns {
    pub mode: Mode,
    pub penalties: Vec<u32>, // seconds per arc (was deciseconds in v1, #297)
    pub inputs_sha: [u8; 16],
}

/// Write t.<mode>.u32 file
pub fn write<P: AsRef<Path>>(path: P, data: &ModTurns) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
    let mut writer = BufWriter::new(file);

    // Build header
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.push(data.mode.0);
    header.push(0); // reserved
    header.extend_from_slice(&(data.penalties.len() as u32).to_le_bytes());
    header.extend_from_slice(&data.inputs_sha);
    header.extend_from_slice(&[0u8; 4]); // padding to 32 bytes
    assert_eq!(header.len(), HEADER_SIZE);

    writer.write_all(&header)?;

    // Write body and calculate CRC
    let mut body_digest = Digest::new();
    for &penalty in &data.penalties {
        let bytes = penalty.to_le_bytes();
        body_digest.update(&bytes);
        writer.write_all(&bytes)?;
    }

    let body_crc64 = body_digest.finalize();

    // Calculate file CRC (header + body)
    let mut file_digest = Digest::new();
    file_digest.update(&header);
    for &penalty in &data.penalties {
        file_digest.update(&penalty.to_le_bytes());
    }
    let file_crc64 = file_digest.finalize();

    // Write footer
    writer.write_all(&body_crc64.to_le_bytes())?;
    writer.write_all(&file_crc64.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

/// Read t.<mode>.u32 file
pub fn read_all<P: AsRef<Path>>(path: P) -> Result<ModTurns> {
    let file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;
    read_all_from_reader(std::io::BufReader::new(file))
        .with_context(|| format!("reading turn penalties from {}", path.as_ref().display()))
}

/// Read `t.<mode>.u32` from an in-memory byte slice (e.g. a `.butterfly`
/// container `mode/<m>/node_weights.turn` section). Mirrors
/// [`crate::formats::mod_weights::read_all_from_bytes`]; used by the #433
/// serve-boot car recustomization, which re-reads the turn table the
/// build-time step8 folded into the served CCH weights.
pub fn read_all_from_bytes(bytes: &[u8]) -> Result<ModTurns> {
    read_all_from_reader(std::io::Cursor::new(bytes))
}

fn read_all_from_reader<R: std::io::Read>(mut file: R) -> Result<ModTurns> {
    // Read header
    let mut header = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header)?;

    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    anyhow::ensure!(
        magic == MAGIC,
        "Invalid magic: expected 0x{:08x}, got 0x{:08x}",
        MAGIC,
        magic
    );

    let version = u16::from_le_bytes([header[4], header[5]]);
    anyhow::ensure!(
        version == VERSION,
        "Unsupported t.<mode>.u32 version: {} (expected {}). \
         v1 stored deciseconds; re-run step 5 to regenerate as v2 (seconds, #297).",
        version,
        VERSION,
    );

    let mode_byte = header[6];
    anyhow::ensure!(
        (mode_byte as usize) < crate::profile_abi::MAX_MODES,
        "Invalid mode: {}",
        mode_byte
    );
    let mode = Mode(mode_byte);

    let count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

    let mut inputs_sha = [0u8; 16];
    inputs_sha.copy_from_slice(&header[12..28]);

    // Read penalties
    let mut body_digest = Digest::new();
    let mut penalties = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut bytes = [0u8; 4];
        file.read_exact(&mut bytes)?;
        body_digest.update(&bytes);
        penalties.push(u32::from_le_bytes(bytes));
    }

    // Verify CRCs
    let computed_body_crc = body_digest.finalize();

    let mut file_digest = Digest::new();
    file_digest.update(&header);
    for &p in &penalties {
        file_digest.update(&p.to_le_bytes());
    }
    let computed_file_crc = file_digest.finalize();

    let mut footer = [0u8; 16];
    file.read_exact(&mut footer)?;
    let stored_body_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let stored_file_crc = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    anyhow::ensure!(
        computed_body_crc == stored_body_crc && computed_file_crc == stored_file_crc,
        "CRC64 mismatch in t.mod.u32: body 0x{:016X}/0x{:016X}, file 0x{:016X}/0x{:016X}",
        computed_body_crc,
        stored_body_crc,
        computed_file_crc,
        stored_file_crc
    );

    Ok(ModTurns {
        mode,
        penalties,
        inputs_sha,
    })
}

/// Verify t.<mode>.u32 file structure and checksums
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

    let count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

    // Verify file size
    let expected_size = HEADER_SIZE as u64 + (count as u64 * 4) + 16;
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
