//! ebg.nodes format - EBG node table (directed NBG edges)

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4542474E; // "EBGN"
const VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct EbgNode {
    pub tail_nbg: u32,    // compact NBG node id
    pub head_nbg: u32,    // compact NBG node id
    pub geom_idx: u32,    // index into nbg.geo record
    pub length_mm: u32,   // copy of nbg.geo.length_mm
    pub class_bits: u32,  // ferry, bridge, tunnel, roundabout, ford, etc.
    pub primary_way: u32, // lower 32 bits of first_osm_way_id
}

#[derive(Debug)]
pub struct EbgNodes {
    pub n_nodes: u32,
    pub created_unix: u64,
    pub inputs_sha: [u8; 32],
    pub nodes: Vec<EbgNode>,
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
        for node in &data.nodes {
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
        let mut reader = BufReader::new(File::open(path)?);
        let mut crc_digest = crc::Digest::new();

        let mut header = vec![0u8; 64];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let created_unix = u64::from_le_bytes([
            header[12], header[13], header[14], header[15], header[16], header[17], header[18],
            header[19],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[20..52]);

        let mut nodes = Vec::with_capacity(n_nodes as usize);
        for _ in 0..n_nodes {
            let mut record = [0u8; 24];
            reader.read_exact(&mut record)?;
            crc_digest.update(&record);

            nodes.push(EbgNode {
                tail_nbg: u32::from_le_bytes([record[0], record[1], record[2], record[3]]),
                head_nbg: u32::from_le_bytes([record[4], record[5], record[6], record[7]]),
                geom_idx: u32::from_le_bytes([record[8], record[9], record[10], record[11]]),
                length_mm: u32::from_le_bytes([record[12], record[13], record[14], record[15]]),
                class_bits: u32::from_le_bytes([record[16], record[17], record[18], record[19]]),
                primary_way: u32::from_le_bytes([record[20], record[21], record[22], record[23]]),
            });
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
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
            nodes,
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
            nodes: vec![
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
            ],
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
