///! Sorted array format for nodes - replaces sparse bitmap approach
///!
///! Format: nodes.sa (little-endian, memory-mappable, fixed 16-byte records)
///!
///! Header (128 bytes):
///!   magic:        u32 = 0x4E4F4453  // "NODS"
///!   version:      u16 = 1
///!   reserved:     u16 = 0
///!   count:        u64
///!   scale:        u32 = 10_000_000
///!   bbox_min_lat: i32
///!   bbox_min_lon: i32
///!   bbox_max_lat: i32
///!   bbox_max_lon: i32
///!   created_unix: u64
///!   input_sha256: [32]u8
///!   reserved2:    [60]u8
///!
///! Body (count records, sorted strictly ascending by id):
///!   id:      i64
///!   lat_fxp: i32
///!   lon_fxp: i32
///!
///! Footer (16 bytes):
///!   body_crc64: u64
///!   file_crc64: u64

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;

const MAGIC: u32 = 0x4E4F4453; // "NODS"
const VERSION: u16 = 1;
const SCALE: u32 = 10_000_000; // 1e-7 degrees
const HEADER_SIZE: usize = 128;
const RECORD_SIZE: usize = 16; // i64 + i32 + i32

/// Write nodes in sorted array format
pub fn write<P: AsRef<Path>>(
    path: P,
    nodes: &[(i64, f64, f64)],
    input_sha256: &[u8; 32],
) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
    let mut writer = BufWriter::new(file);

    // Ensure nodes are sorted
    let mut sorted_nodes = nodes.to_vec();
    sorted_nodes.sort_by_key(|(id, _, _)| *id);

    // Calculate bounding box in fixed-point
    let (bbox_min_lat, bbox_min_lon, bbox_max_lat, bbox_max_lon) = calculate_bbox(&sorted_nodes);

    // Get current timestamp
    let created_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // Write header (we'll calculate CRCs and update later)
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.extend_from_slice(&0u16.to_le_bytes()); // reserved
    header.extend_from_slice(&(sorted_nodes.len() as u64).to_le_bytes());
    header.extend_from_slice(&SCALE.to_le_bytes());
    header.extend_from_slice(&bbox_min_lat.to_le_bytes());
    header.extend_from_slice(&bbox_min_lon.to_le_bytes());
    header.extend_from_slice(&bbox_max_lat.to_le_bytes());
    header.extend_from_slice(&bbox_max_lon.to_le_bytes());
    header.extend_from_slice(&created_unix.to_le_bytes());
    header.extend_from_slice(input_sha256);
    header.resize(HEADER_SIZE, 0); // Fill reserved2

    writer.write_all(&header)?;

    // Write body and calculate CRC
    let mut body_digest = Digest::new();
    for (id, lat, lon) in sorted_nodes.iter() {
        let lat_fxp = (lat * SCALE as f64).round() as i32;
        let lon_fxp = (lon * SCALE as f64).round() as i32;

        let mut record = Vec::with_capacity(RECORD_SIZE);
        record.extend_from_slice(&id.to_le_bytes());
        record.extend_from_slice(&lat_fxp.to_le_bytes());
        record.extend_from_slice(&lon_fxp.to_le_bytes());

        body_digest.update(&record);
        writer.write_all(&record)?;
    }

    let body_crc64 = body_digest.finalize();

    // Calculate file CRC (header + body)
    let mut file_digest = Digest::new();
    file_digest.update(&header);
    for (id, lat, lon) in sorted_nodes.iter() {
        let lat_fxp = (lat * SCALE as f64).round() as i32;
        let lon_fxp = (lon * SCALE as f64).round() as i32;

        let mut record = Vec::with_capacity(RECORD_SIZE);
        record.extend_from_slice(&id.to_le_bytes());
        record.extend_from_slice(&lat_fxp.to_le_bytes());
        record.extend_from_slice(&lon_fxp.to_le_bytes());

        file_digest.update(&record);
    }
    let file_crc64 = file_digest.finalize();

    // Write footer
    writer.write_all(&body_crc64.to_le_bytes())?;
    writer.write_all(&file_crc64.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

fn calculate_bbox(nodes: &[(i64, f64, f64)]) -> (i32, i32, i32, i32) {
    if nodes.is_empty() {
        return (0, 0, 0, 0);
    }

    let mut min_lat = f64::MAX;
    let mut min_lon = f64::MAX;
    let mut max_lat = f64::MIN;
    let mut max_lon = f64::MIN;

    for (_, lat, lon) in nodes {
        min_lat = min_lat.min(*lat);
        min_lon = min_lon.min(*lon);
        max_lat = max_lat.max(*lat);
        max_lon = max_lon.max(*lon);
    }

    (
        (min_lat * SCALE as f64).round() as i32,
        (min_lon * SCALE as f64).round() as i32,
        (max_lat * SCALE as f64).round() as i32,
        (max_lon * SCALE as f64).round() as i32,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bbox_calculation() {
        let nodes = vec![
            (1, 50.8503, 4.3517),  // Brussels
            (2, 51.2194, 4.4025),  // Antwerp
            (3, 50.4501, 3.9520),  // Mons
        ];

        let (min_lat, min_lon, max_lat, max_lon) = calculate_bbox(&nodes);

        // Check that bbox contains all points (in fixed-point)
        assert!(min_lat <= 504501000);
        assert!(max_lat >= 512194000);
        assert!(min_lon <= 39520000);
        assert!(max_lon >= 44025000);
    }
}
