///! way_attrs.<mode>.bin format - Per-mode way attributes
///!
///! Format (little-endian, mmap-friendly):
///!
///! Header (80 bytes):
///!   magic:       u32 = 0x57415941  // "WAYA"
///!   version:     u16 = 1
///!   mode:        u8  = {0=car,1=bike,2=foot}
///!   reserved:    u8  = 0
///!   count:       u64
///!   dict_k_sha:  [32]u8
///!   dict_v_sha:  [32]u8
///!
///! Body (count records, sorted by way_id):
///!   way_id:             i64
///!   flags:              u32  // access + oneway + class bits
///!   base_speed_mmps:    u32
///!   highway_class:      u16
///!   surface_class:      u16
///!   per_km_penalty_ds:  u16
///!   const_penalty_ds:   u32
///!   reserved:           [6]u8  // padding to 32 bytes
///!
///! Footer (16 bytes):
///!   body_crc64:  u64
///!   file_crc64:  u64

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;
use crate::profile_abi::{Mode, WayOutput};

const MAGIC: u32 = 0x57415941; // "WAYA"
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 80; // 4 + 2 + 1 + 1 + 8 + 32 + 32
const RECORD_SIZE: usize = 32; // 8 + 4 + 4 + 2 + 2 + 2 + 4 + 6(pad)

#[derive(Debug, Clone)]
pub struct WayAttr {
    pub way_id: i64,
    pub output: WayOutput,
}

/// Write way_attrs.<mode>.bin file
pub fn write<P: AsRef<Path>>(
    path: P,
    mode: Mode,
    attrs: &[WayAttr],
    dict_k_sha256: &[u8; 32],
    dict_v_sha256: &[u8; 32],
) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
    let mut writer = BufWriter::new(file);

    // Ensure attrs are sorted by way_id
    let mut sorted_attrs = attrs.to_vec();
    sorted_attrs.sort_by_key(|a| a.way_id);

    // Build header
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.push(mode as u8);
    header.push(0); // reserved
    header.extend_from_slice(&(sorted_attrs.len() as u64).to_le_bytes());
    header.extend_from_slice(dict_k_sha256);
    header.extend_from_slice(dict_v_sha256);
    assert_eq!(header.len(), HEADER_SIZE);

    writer.write_all(&header)?;

    // Write body and calculate CRC
    let mut body_digest = Digest::new();
    for attr in sorted_attrs.iter() {
        let record = encode_record(attr);
        body_digest.update(&record);
        writer.write_all(&record)?;
    }

    let body_crc64 = body_digest.finalize();

    // Calculate file CRC (header + body)
    let mut file_digest = Digest::new();
    file_digest.update(&header);
    for attr in sorted_attrs.iter() {
        let record = encode_record(attr);
        file_digest.update(&record);
    }
    let file_crc64 = file_digest.finalize();

    // Write footer
    writer.write_all(&body_crc64.to_le_bytes())?;
    writer.write_all(&file_crc64.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

/// Encode a single way_attrs record
fn encode_record(attr: &WayAttr) -> Vec<u8> {
    let mut record = Vec::with_capacity(RECORD_SIZE);

    // Encode flags: access + oneway + class_bits
    let mut flags = attr.output.class_bits;
    if attr.output.access_fwd {
        flags |= 1 << 0;
    }
    if attr.output.access_rev {
        flags |= 1 << 1;
    }
    flags |= (attr.output.oneway as u32) << 2;

    record.extend_from_slice(&attr.way_id.to_le_bytes());
    record.extend_from_slice(&flags.to_le_bytes());
    record.extend_from_slice(&attr.output.base_speed_mmps.to_le_bytes());
    record.extend_from_slice(&attr.output.highway_class.to_le_bytes());
    record.extend_from_slice(&attr.output.surface_class.to_le_bytes());
    record.extend_from_slice(&attr.output.per_km_penalty_ds.to_le_bytes());
    record.extend_from_slice(&attr.output.const_penalty_ds.to_le_bytes());
    record.extend_from_slice(&[0u8; 6]); // padding to 32 bytes

    assert_eq!(record.len(), RECORD_SIZE);
    record
}

/// Verify way_attrs file structure and checksums
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

    let count = u64::from_le_bytes([
        header[8], header[9], header[10], header[11],
        header[12], header[13], header[14], header[15],
    ]);

    // Verify file size
    let expected_size = HEADER_SIZE as u64 + (count * RECORD_SIZE as u64) + 16;
    let actual_size = file.seek(SeekFrom::End(0))?;

    if actual_size != expected_size {
        anyhow::bail!(
            "Size mismatch in {}: expected {} bytes, got {} bytes",
            path.as_ref().display(),
            expected_size,
            actual_size
        );
    }

    println!(
        "  ✓ {} verified ({} ways, {} bytes)",
        path.as_ref().display(),
        count,
        actual_size
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_record_size() {
        let attr = WayAttr {
            way_id: 12345,
            output: WayOutput::default(),
        };
        let record = encode_record(&attr);
        assert_eq!(record.len(), RECORD_SIZE);
    }

    #[test]
    fn test_flags_encoding() {
        let attr = WayAttr {
            way_id: 1,
            output: WayOutput {
                access_fwd: true,
                access_rev: false,
                oneway: 1,
                class_bits: 0,
                ..Default::default()
            },
        };
        let record = encode_record(&attr);
        let flags = u32::from_le_bytes([record[8], record[9], record[10], record[11]]);
        assert_eq!(flags & 0x1, 1); // access_fwd
        assert_eq!(flags & 0x2, 0); // access_rev
        assert_eq!((flags >> 2) & 0x3, 1); // oneway
    }
}
