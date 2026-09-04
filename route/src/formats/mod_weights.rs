//! w.<mode>.u32 format - Per-mode node weights
//!
//! Format (little-endian, mmap-friendly):
//!
//! Header (32 bytes):
//!   magic:       u32 = 0x574D4F44  // "WMOD"
//!   version:     u16 = 2  // v2: seconds (was v1: deciseconds, #297)
//!   mode:        u8  = {0=car,1=bike,2=foot,...}
//!   reserved:    u8  = 0
//!   count:       u32 = n_nodes
//!   inputs_sha:  [16]u8  // truncated SHA-256 of inputs
//!
//! Body (count * u32):
//!   u32 weight_s[count]  // seconds (0 = inaccessible)
//!
//! Footer (16 bytes):
//!   body_crc64:  u64
//!   file_crc64:  u64

use anyhow::{Context, Result};
use std::borrow::Cow;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;
use crate::model::types::Mode;

const MAGIC: u32 = 0x574D4F44; // "WMOD"
const VERSION: u16 = 2;
const HEADER_SIZE: usize = 32; // 4 + 2 + 1 + 1 + 4 + 16 + 4(pad)
const FOOTER_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct ModWeights {
    pub mode: Mode,
    /// Per-node weights in seconds (#297 unit conversion — was
    /// deciseconds in v1, now seconds in v2).
    /// Always `Cow::Owned`: every reader walks the CRC and decodes
    /// into a fresh `Vec<u32>`.
    pub weights: Cow<'static, [u32]>,
    pub inputs_sha: [u8; 16],
}

/// Write w.<mode>.u32 file
pub fn write<P: AsRef<Path>>(path: P, data: &ModWeights) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
    let mut writer = BufWriter::new(file);

    // Build header
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.push(data.mode.0);
    header.push(0); // reserved
    header.extend_from_slice(&(data.weights.len() as u32).to_le_bytes());
    header.extend_from_slice(&data.inputs_sha);
    header.extend_from_slice(&[0u8; 4]); // padding to 32 bytes
    assert_eq!(header.len(), HEADER_SIZE);

    writer.write_all(&header)?;

    // Write body and calculate CRC
    let mut body_digest = Digest::new();
    for &weight in data.weights.iter() {
        let bytes = weight.to_le_bytes();
        body_digest.update(&bytes);
        writer.write_all(&bytes)?;
    }

    let body_crc64 = body_digest.finalize();

    // Calculate file CRC (header + body)
    let mut file_digest = Digest::new();
    file_digest.update(&header);
    for &weight in data.weights.iter() {
        file_digest.update(&weight.to_le_bytes());
    }
    let file_crc64 = file_digest.finalize();

    // Write footer
    writer.write_all(&body_crc64.to_le_bytes())?;
    writer.write_all(&file_crc64.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

/// Read w.<mode>.u32 file
pub fn read_all<P: AsRef<Path>>(path: P) -> Result<ModWeights> {
    let file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;
    read_all_from_reader(file).with_context(|| format!("reading {}", path.as_ref().display()))
}

/// Read w.<mode>.u32 from an in-memory byte slice (mmap-backed bundle).
pub fn read_all_from_bytes(bytes: &[u8]) -> Result<ModWeights> {
    read_all_from_reader(std::io::Cursor::new(bytes))
}

fn read_all_from_reader<R: std::io::Read>(mut file: R) -> Result<ModWeights> {
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
        "Unsupported w.<mode>.u32 version: {} (expected {}). \
         v1 used deciseconds; re-run step 5 to regenerate as v2 (seconds, #297).",
        version,
        VERSION,
    );

    let mode_byte = header[6];
    anyhow::ensure!(
        (mode_byte as usize) < crate::model::types::MAX_MODES,
        "Invalid mode: {}",
        mode_byte
    );
    let mode = Mode(mode_byte);

    let count = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);

    let mut inputs_sha = [0u8; 16];
    inputs_sha.copy_from_slice(&header[12..28]);

    // Read weights
    let mut body_digest = Digest::new();
    let mut weights = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut bytes = [0u8; 4];
        file.read_exact(&mut bytes)?;
        body_digest.update(&bytes);
        weights.push(u32::from_le_bytes(bytes));
    }

    // Verify CRCs
    let computed_body_crc = body_digest.finalize();

    let mut file_digest = Digest::new();
    file_digest.update(&header);
    for &w in &weights {
        file_digest.update(&w.to_le_bytes());
    }
    let computed_file_crc = file_digest.finalize();

    let mut footer = [0u8; 16];
    file.read_exact(&mut footer)?;
    let stored_body_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
    let stored_file_crc = u64::from_le_bytes(footer[8..16].try_into().unwrap());
    anyhow::ensure!(
        computed_body_crc == stored_body_crc && computed_file_crc == stored_file_crc,
        "CRC64 mismatch in w.mod.u32: body 0x{:016X}/0x{:016X}, file 0x{:016X}/0x{:016X}",
        computed_body_crc,
        stored_body_crc,
        computed_file_crc,
        stored_file_crc
    );

    Ok(ModWeights {
        mode,
        weights: Cow::Owned(weights),
        inputs_sha,
    })
}

/// Verify w.<mode>.u32 file structure and checksums
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
    let expected_size = HEADER_SIZE as u64 + (count as u64 * 4) + FOOTER_SIZE as u64;
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

#[cfg(test)]
mod tests {
    use super::*;

    /// Pack a valid `ModWeights` payload into bytes. Mirrors what
    /// `write()` produces minus the file I/O.
    fn build_bytes(mode: Mode, weights: &[u32], inputs_sha: &[u8; 16]) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_SIZE + weights.len() * 4 + FOOTER_SIZE);
        out.extend_from_slice(&MAGIC.to_le_bytes());
        out.extend_from_slice(&VERSION.to_le_bytes());
        out.push(mode.0);
        out.push(0);
        out.extend_from_slice(&(weights.len() as u32).to_le_bytes());
        out.extend_from_slice(inputs_sha);
        out.extend_from_slice(&[0u8; 4]);
        assert_eq!(out.len(), HEADER_SIZE);

        let mut body_digest = Digest::new();
        for &w in weights {
            let b = w.to_le_bytes();
            body_digest.update(&b);
            out.extend_from_slice(&b);
        }
        let body_crc = body_digest.finalize();

        let mut file_digest = Digest::new();
        file_digest.update(&out[..HEADER_SIZE]);
        for &w in weights {
            file_digest.update(&w.to_le_bytes());
        }
        let file_crc = file_digest.finalize();
        out.extend_from_slice(&body_crc.to_le_bytes());
        out.extend_from_slice(&file_crc.to_le_bytes());
        out
    }

    #[test]
    fn read_from_bytes_roundtrip() {
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30, 40, 50];
        let inputs_sha = [1u8; 16];
        let bytes = build_bytes(mode, &weights, &inputs_sha);

        let parsed = read_all_from_bytes(&bytes).expect("parse ok");
        assert_eq!(parsed.mode.0, 0);
        assert_eq!(parsed.inputs_sha, inputs_sha);
        assert_eq!(&parsed.weights[..], weights.as_slice());
    }

    #[test]
    fn read_from_bytes_fails_on_bad_magic() {
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30];
        let inputs_sha = [0u8; 16];
        let mut bytes = build_bytes(mode, &weights, &inputs_sha);
        // Corrupt the magic in-place.
        bytes[0] = 0xAA;

        let err = read_all_from_bytes(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("magic"), "unexpected error: {msg}");
    }

    #[test]
    fn read_from_bytes_fails_on_truncated_body() {
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30];
        let inputs_sha = [0u8; 16];
        let mut bytes = build_bytes(mode, &weights, &inputs_sha);
        // Truncate the footer so the declared size no longer matches.
        bytes.truncate(bytes.len() - 4);

        let err = read_all_from_bytes(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("failed to fill whole buffer"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn read_from_bytes_rejects_corrupted_crc() {
        // `read_all_from_bytes` is the only remaining byte-slice
        // reader and it always walks the CRC — a corrupted body CRC
        // must be rejected, not silently accepted.
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30];
        let inputs_sha = [0u8; 16];
        let mut bytes = build_bytes(mode, &weights, &inputs_sha);
        let body_end = HEADER_SIZE + weights.len() * 4;
        // Overwrite body CRC bytes (offsets body_end..body_end+8).
        for i in 0..8 {
            bytes[body_end + i] ^= 0xFF;
        }
        let err = read_all_from_bytes(&bytes).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("CRC64 mismatch"), "unexpected error: {msg}");
    }
}
