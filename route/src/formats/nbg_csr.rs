//! nbg.csr format - Compact CSR graph for undirected NBG topology

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4E424743; // "NBGC"
const VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct NbgCsr {
    pub n_nodes: u32,
    pub n_edges_und: u64,
    pub created_unix: u64,
    pub inputs_sha: [u8; 32],
    pub offsets: Vec<u64>,  // n_nodes + 1
    pub heads: Vec<u32>,    // 2 * n_edges_und
    pub edge_idx: Vec<u64>, // 2 * n_edges_und
}

pub struct NbgCsrFile;

impl NbgCsrFile {
    /// Write NBG CSR to file
    pub fn write<P: AsRef<Path>>(path: P, csr: &NbgCsr) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_nodes_bytes = csr.n_nodes.to_le_bytes();
        let n_edges_und_bytes = csr.n_edges_und.to_le_bytes();
        let created_unix_bytes = csr.created_unix.to_le_bytes();

        let padding = [0u8; 4]; // Pad to 64 bytes

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_nodes_bytes)?;
        writer.write_all(&n_edges_und_bytes)?;
        writer.write_all(&created_unix_bytes)?;
        writer.write_all(&csr.inputs_sha)?;
        writer.write_all(&padding)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_nodes_bytes);
        crc_digest.update(&n_edges_und_bytes);
        crc_digest.update(&created_unix_bytes);
        crc_digest.update(&csr.inputs_sha);
        crc_digest.update(&padding);

        // Offsets
        for &offset in &csr.offsets {
            let bytes = offset.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Heads
        for &head in &csr.heads {
            let bytes = head.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Edge indices
        for &idx in &csr.edge_idx {
            let bytes = idx.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        let file_crc = body_crc; // Simple approach for now
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read NBG CSR from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<NbgCsr> {
        use std::io::{BufReader, Read};

        let mut reader = BufReader::new(std::fs::File::open(path)?);
        let mut crc_digest = crc::Digest::new();

        let mut header = vec![0u8; 64];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_edges_und = u64::from_le_bytes([
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
        let mut heads = Vec::with_capacity((2 * n_edges_und) as usize);
        for _ in 0..(2 * n_edges_und) {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            heads.push(u32::from_le_bytes(buf));
        }

        // Read edge_idx
        let mut edge_idx = Vec::with_capacity((2 * n_edges_und) as usize);
        for _ in 0..(2 * n_edges_und) {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            edge_idx.push(u64::from_le_bytes(buf));
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in nbg.csr: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(NbgCsr {
            n_nodes,
            n_edges_und,
            created_unix,
            inputs_sha,
            offsets,
            heads,
            edge_idx,
        })
    }
}
