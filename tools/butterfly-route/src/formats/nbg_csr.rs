///! nbg.csr format - Compact CSR graph for undirected NBG topology

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
    pub offsets: Vec<u64>,      // n_nodes + 1
    pub heads: Vec<u32>,        // 2 * n_edges_und
    pub edge_idx: Vec<u64>,     // 2 * n_edges_und
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
}
