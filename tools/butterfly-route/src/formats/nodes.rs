///! nodes.bin format - sparse node coordinate storage with fixed-point encoding

use anyhow::Result;
use bit_vec::BitVec;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4E4F4445; // "NODE"
const VERSION: u16 = 1;
const SCALE: u32 = 10_000_000; // 1e-7 degrees
const IDX_KIND_SPARSE_BITMAP: u8 = 1;

#[repr(C)]
struct Header {
    magic: u32,
    version: u16,
    reserved: u16,
    count: u64,
    id_base: i64,
    id_stride: u64,
    scale: u32,
    bbox_min_lat: i32,
    bbox_min_lon: i32,
    bbox_max_lat: i32,
    bbox_max_lon: i32,
    idx_kind: u8,
    reserved2: [u8; 7],
}

impl Header {
    fn to_bytes(&self) -> Vec<u8> {
        let mut bytes = Vec::with_capacity(64);
        bytes.extend_from_slice(&self.magic.to_le_bytes());
        bytes.extend_from_slice(&self.version.to_le_bytes());
        bytes.extend_from_slice(&self.reserved.to_le_bytes());
        bytes.extend_from_slice(&self.count.to_le_bytes());
        bytes.extend_from_slice(&self.id_base.to_le_bytes());
        bytes.extend_from_slice(&self.id_stride.to_le_bytes());
        bytes.extend_from_slice(&self.scale.to_le_bytes());
        bytes.extend_from_slice(&self.bbox_min_lat.to_le_bytes());
        bytes.extend_from_slice(&self.bbox_min_lon.to_le_bytes());
        bytes.extend_from_slice(&self.bbox_max_lat.to_le_bytes());
        bytes.extend_from_slice(&self.bbox_max_lon.to_le_bytes());
        bytes.push(self.idx_kind);
        bytes.extend_from_slice(&self.reserved2);
        bytes
    }

    #[allow(dead_code)]
    fn from_bytes(bytes: &[u8]) -> Result<Self> {
        if bytes.len() < 64 {
            anyhow::bail!("Header too short");
        }

        let magic = u32::from_le_bytes(bytes[0..4].try_into()?);
        if magic != MAGIC {
            anyhow::bail!("Invalid magic number: {:08x}", magic);
        }

        let version = u16::from_le_bytes(bytes[4..6].try_into()?);
        if version != VERSION {
            anyhow::bail!("Unsupported version: {}", version);
        }

        Ok(Self {
            magic,
            version,
            reserved: u16::from_le_bytes(bytes[6..8].try_into()?),
            count: u64::from_le_bytes(bytes[8..16].try_into()?),
            id_base: i64::from_le_bytes(bytes[16..24].try_into()?),
            id_stride: u64::from_le_bytes(bytes[24..32].try_into()?),
            scale: u32::from_le_bytes(bytes[32..36].try_into()?),
            bbox_min_lat: i32::from_le_bytes(bytes[36..40].try_into()?),
            bbox_min_lon: i32::from_le_bytes(bytes[40..44].try_into()?),
            bbox_max_lat: i32::from_le_bytes(bytes[44..48].try_into()?),
            bbox_max_lon: i32::from_le_bytes(bytes[48..52].try_into()?),
            idx_kind: bytes[52],
            reserved2: bytes[53..60].try_into()?,
        })
    }
}

pub struct NodesFile;

impl NodesFile {
    /// Write nodes.bin file
    pub fn write<P: AsRef<Path>>(
        path: P,
        nodes: &[(i64, f64, f64)], // (id, lat, lon)
    ) -> Result<()> {
        if nodes.is_empty() {
            anyhow::bail!("Cannot write empty nodes file");
        }

        // Find ID range and bbox
        let mut min_id = i64::MAX;
        let mut max_id = i64::MIN;
        let mut min_lat = f64::MAX;
        let mut max_lat = f64::MIN;
        let mut min_lon = f64::MAX;
        let mut max_lon = f64::MIN;

        for (id, lat, lon) in nodes {
            min_id = min_id.min(*id);
            max_id = max_id.max(*id);
            min_lat = min_lat.min(*lat);
            max_lat = max_lat.max(*lat);
            min_lon = min_lon.min(*lon);
            max_lon = max_lon.max(*lon);
        }

        let id_stride = (max_id - min_id + 1) as u64;

        // Build sparse bitmap
        let mut bitmap = BitVec::from_elem(id_stride as usize, false);
        for (id, _, _) in nodes {
            let idx = (*id - min_id) as usize;
            bitmap.set(idx, true);
        }

        // Convert coordinates to fixed-point
        let mut coords = Vec::new();
        for (id, lat, lon) in nodes {
            let lat_fxp = (lat * SCALE as f64).round() as i32;
            let lon_fxp = (lon * SCALE as f64).round() as i32;
            coords.push((id, lat_fxp, lon_fxp));
        }

        // Sort by ID for determinism
        coords.sort_by_key(|(id, _, _)| *id);

        // Create header
        let header = Header {
            magic: MAGIC,
            version: VERSION,
            reserved: 0,
            count: nodes.len() as u64,
            id_base: min_id,
            id_stride,
            scale: SCALE,
            bbox_min_lat: (min_lat * SCALE as f64).round() as i32,
            bbox_min_lon: (min_lon * SCALE as f64).round() as i32,
            bbox_max_lat: (max_lat * SCALE as f64).round() as i32,
            bbox_max_lon: (max_lon * SCALE as f64).round() as i32,
            idx_kind: IDX_KIND_SPARSE_BITMAP,
            reserved2: [0; 7],
        };

        // Write file
        let file = File::create(path)?;
        let mut writer = BufWriter::new(file);
        let mut crc_digest = crc::Digest::new();

        // Write header
        let header_bytes = header.to_bytes();
        writer.write_all(&header_bytes)?;
        crc_digest.update(&header_bytes);

        // Write bitmap
        let bitmap_bytes = bitmap.to_bytes();
        writer.write_all(&bitmap_bytes)?;
        crc_digest.update(&bitmap_bytes);

        // Write coordinates
        let mut coords_digest = crc::Digest::new();
        for (_, lat_fxp, lon_fxp) in coords {
            let lat_bytes = lat_fxp.to_le_bytes();
            let lon_bytes = lon_fxp.to_le_bytes();
            writer.write_all(&lat_bytes)?;
            writer.write_all(&lon_bytes)?;
            coords_digest.update(&lat_bytes);
            coords_digest.update(&lon_bytes);
            crc_digest.update(&lat_bytes);
            crc_digest.update(&lon_bytes);
        }

        // Write footer
        let coords_crc = coords_digest.finalize();
        let file_crc = crc_digest.finalize();

        writer.write_all(&coords_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Verify checksums in nodes.bin file
    pub fn verify<P: AsRef<Path>>(path: P) -> Result<()> {
        let mut file = File::open(path)?;

        // Read entire file except footer
        let file_len = file.metadata()?.len();
        if file_len < 64 + 16 {
            anyhow::bail!("File too short");
        }

        let content_len = file_len - 16;
        let mut content = vec![0u8; content_len as usize];
        file.read_exact(&mut content)?;

        // Read footer
        let mut footer = [0u8; 16];
        file.read_exact(&mut footer)?;

        let _stored_coords_crc = u64::from_le_bytes(footer[0..8].try_into()?);
        let stored_file_crc = u64::from_le_bytes(footer[8..16].try_into()?);

        // Verify file CRC
        let computed_file_crc = crc::checksum(&content);
        if computed_file_crc != stored_file_crc {
            anyhow::bail!(
                "File CRC mismatch: expected {:016x}, got {:016x}",
                stored_file_crc,
                computed_file_crc
            );
        }

        println!("✓ nodes.bin CRC-64 verified");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_nodes_write_verify() {
        let nodes = vec![
            (100, 50.5, 4.5),
            (200, 50.6, 4.6),
            (150, 50.55, 4.55),
        ];

        let tmpfile = NamedTempFile::new().unwrap();
        NodesFile::write(tmpfile.path(), &nodes).unwrap();
        NodesFile::verify(tmpfile.path()).unwrap();
    }
}
