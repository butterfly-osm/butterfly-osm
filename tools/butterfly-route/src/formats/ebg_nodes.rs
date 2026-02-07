//! ebg.nodes format - EBG node table (directed NBG edges)

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write, Read, BufReader};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4542474E; // "EBGN"
const VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct EbgNode {
    pub tail_nbg: u32,      // compact NBG node id
    pub head_nbg: u32,      // compact NBG node id
    pub geom_idx: u32,      // index into nbg.geo record
    pub length_mm: u32,     // copy of nbg.geo.length_mm
    pub class_bits: u32,    // ferry, bridge, tunnel, roundabout, ford, etc.
    pub primary_way: u32,   // lower 32 bits of first_osm_way_id
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
        let mut header = vec![0u8; 64];
        reader.read_exact(&mut header)?;

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let created_unix = u64::from_le_bytes([
            header[12], header[13], header[14], header[15],
            header[16], header[17], header[18], header[19],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[20..52]);

        let mut nodes = Vec::with_capacity(n_nodes as usize);
        for _ in 0..n_nodes {
            let mut record = [0u8; 24];
            reader.read_exact(&mut record)?;

            nodes.push(EbgNode {
                tail_nbg: u32::from_le_bytes([record[0], record[1], record[2], record[3]]),
                head_nbg: u32::from_le_bytes([record[4], record[5], record[6], record[7]]),
                geom_idx: u32::from_le_bytes([record[8], record[9], record[10], record[11]]),
                length_mm: u32::from_le_bytes([record[12], record[13], record[14], record[15]]),
                class_bits: u32::from_le_bytes([record[16], record[17], record[18], record[19]]),
                primary_way: u32::from_le_bytes([record[20], record[21], record[22], record[23]]),
            });
        }

        Ok(EbgNodes {
            n_nodes,
            created_unix,
            inputs_sha,
            nodes,
        })
    }
}
