//! w.<mode>.u32 format - Per-mode node weights
//!
//! Format (little-endian, mmap-friendly):
//!
//! Header (32 bytes):
//!   magic:       u32 = 0x574D4F44  // "WMOD"
//!   version:     u16 = 1
//!   mode:        u8  = {0=car,1=bike,2=foot}
//!   reserved:    u8  = 0
//!   count:       u32 = n_nodes
//!   inputs_sha:  [16]u8  // truncated SHA-256 of inputs
//!
//! Body (count * u32):
//!   u32 weight_ds[count]  // deciseconds (0 = inaccessible)
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
use crate::profile_abi::Mode;

const MAGIC: u32 = 0x574D4F44; // "WMOD"
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 32; // 4 + 2 + 1 + 1 + 4 + 16 + 4(pad)
const FOOTER_SIZE: usize = 16;

#[derive(Debug, Clone)]
pub struct ModWeights {
    pub mode: Mode,
    /// Per-node weights in deciseconds.
    /// `Cow::Borrowed` for mmap-backed container reads (zero-copy);
    /// `Cow::Owned` for the legacy file-reader path.
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
    anyhow::ensure!(version == VERSION, "Unsupported version: {}", version);

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

/// #294: zero-copy read of `w.<mode>.u32` over `'static` mmap bytes.
/// Returns `ModWeights` with `weights: Cow::Borrowed(&[u32])` directly
/// over the input slice. Saves ~20 MB per mode on Belgium that the
/// owning Vec<u32> path would have copied onto the heap.
///
/// Skips the per-format CRC walk; caller MUST have verified the bytes
/// upstream (e.g. via `LazyContainer::verify_now`).
pub fn read_from_bytes_zero_copy_unverified(bytes: &'static [u8]) -> Result<ModWeights> {
    anyhow::ensure!(
        bytes.len() >= HEADER_SIZE + FOOTER_SIZE,
        "mod_weights too short: {} bytes",
        bytes.len()
    );
    // Container guarantees 8-byte alignment, but bytemuck::cast_slice
    // panics on misalignment — validate explicitly so misuse fails
    // with a typed error instead of an abort.
    anyhow::ensure!(
        (bytes.as_ptr() as usize).is_multiple_of(4),
        "mod_weights section must start 4-byte aligned (got addr 0x{:x})",
        bytes.as_ptr() as usize
    );

    let header = &bytes[..HEADER_SIZE];
    let magic = u32::from_le_bytes(header[0..4].try_into().unwrap());
    anyhow::ensure!(
        magic == MAGIC,
        "Invalid magic: expected 0x{:08x}, got 0x{:08x}",
        MAGIC,
        magic
    );

    let version = u16::from_le_bytes(header[4..6].try_into().unwrap());
    anyhow::ensure!(version == VERSION, "Unsupported version: {}", version);

    let mode_byte = header[6];
    anyhow::ensure!(
        (mode_byte as usize) < crate::profile_abi::MAX_MODES,
        "Invalid mode: {}",
        mode_byte
    );
    let mode = Mode(mode_byte);

    let count = u32::from_le_bytes(header[8..12].try_into().unwrap()) as usize;

    let mut inputs_sha = [0u8; 16];
    inputs_sha.copy_from_slice(&header[12..28]);

    // PR #319 Copilot review: a malicious/corrupt header `count` could
    // overflow `count * 4` or the addition, making `body_end` wrap and
    // potentially be less than `HEADER_SIZE`. Use checked arithmetic so
    // overflow returns a clean error instead of panicking on the slice.
    let body_len = count
        .checked_mul(4)
        .ok_or_else(|| anyhow::anyhow!("mod_weights count overflows: count={}", count))?;
    let body_end = HEADER_SIZE
        .checked_add(body_len)
        .ok_or_else(|| anyhow::anyhow!("mod_weights body_end overflows: count={}", count))?;
    let expected_total = body_end
        .checked_add(FOOTER_SIZE)
        .ok_or_else(|| anyhow::anyhow!("mod_weights expected_total overflows: count={}", count))?;
    anyhow::ensure!(
        bytes.len() == expected_total,
        "mod_weights size mismatch: got {}, expected {}",
        bytes.len(),
        expected_total
    );
    // `body_end >= HEADER_SIZE` is implied by `body_end =
    // HEADER_SIZE.checked_add(body_len)` having succeeded
    // (body_len is non-negative because it's a usize).

    let weights: &'static [u32] = bytemuck::cast_slice(&bytes[HEADER_SIZE..body_end]);

    Ok(ModWeights {
        mode,
        weights: Cow::Borrowed(weights),
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

    /// Leak the buffer so its byte slice is `'static`. Tests only.
    fn leak_static(buf: Vec<u8>) -> &'static [u8] {
        Box::leak(buf.into_boxed_slice())
    }

    #[test]
    fn zero_copy_roundtrip_returns_borrowed_view() {
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30, 40, 50];
        let inputs_sha = [1u8; 16];
        let bytes = build_bytes(mode, &weights, &inputs_sha);
        let bytes = leak_static(bytes);

        let parsed = read_from_bytes_zero_copy_unverified(bytes).expect("parse ok");
        assert_eq!(parsed.mode.0, 0);
        assert_eq!(parsed.inputs_sha, inputs_sha);
        assert_eq!(&parsed.weights[..], weights.as_slice());
        // Critically: the view must be Cow::Borrowed pointing into our
        // input bytes, not a fresh Vec copy.
        match &parsed.weights {
            std::borrow::Cow::Borrowed(_) => {} // ok
            std::borrow::Cow::Owned(_) => panic!("expected Cow::Borrowed view"),
        }
    }

    #[test]
    fn zero_copy_fails_on_bad_magic() {
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30];
        let inputs_sha = [0u8; 16];
        let mut bytes = build_bytes(mode, &weights, &inputs_sha);
        // Corrupt the magic in-place.
        bytes[0] = 0xAA;
        let bytes = leak_static(bytes);

        let err = read_from_bytes_zero_copy_unverified(bytes).unwrap_err();
        let msg = err.to_string();
        assert!(msg.contains("magic"), "unexpected error: {msg}");
    }

    #[test]
    fn zero_copy_fails_on_size_mismatch() {
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30];
        let inputs_sha = [0u8; 16];
        let mut bytes = build_bytes(mode, &weights, &inputs_sha);
        // Truncate the footer so file_size != expected.
        bytes.truncate(bytes.len() - 4);
        let bytes = leak_static(bytes);

        let err = read_from_bytes_zero_copy_unverified(bytes).unwrap_err();
        let msg = err.to_string();
        assert!(
            msg.contains("size mismatch") || msg.contains("too short"),
            "unexpected error: {msg}"
        );
    }

    #[test]
    fn zero_copy_unverified_ignores_corrupted_crc() {
        // The `_unverified` reader skips CRC validation; callers are
        // expected to have run LazyContainer's CRC check upstream.
        // This test asserts that corrupting the body CRC does NOT
        // cause the reader to fail — that's the documented contract.
        let mode = Mode(0);
        let weights = vec![10u32, 20, 30];
        let inputs_sha = [0u8; 16];
        let mut bytes = build_bytes(mode, &weights, &inputs_sha);
        let body_end = HEADER_SIZE + weights.len() * 4;
        // Overwrite body CRC bytes (offsets body_end..body_end+8).
        for i in 0..8 {
            bytes[body_end + i] ^= 0xFF;
        }
        let bytes = leak_static(bytes);
        let parsed = read_from_bytes_zero_copy_unverified(bytes).expect("ok despite CRC bits");
        assert_eq!(&parsed.weights[..], weights.as_slice());
    }
}
