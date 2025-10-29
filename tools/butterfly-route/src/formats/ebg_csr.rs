///! ebg.csr format - EBG adjacency (CSR over EBG nodes)

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write, Read, BufReader};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x45424743; // "EBGC"
const VERSION: u16 = 1;

#[derive(Debug)]
pub struct EbgCsr {
    pub n_nodes: u32,
    pub n_arcs: u64,
    pub created_unix: u64,
    pub inputs_sha: [u8; 32],
    pub offsets: Vec<u64>,   // n_nodes + 1
    pub heads: Vec<u32>,     // n_arcs
    pub turn_idx: Vec<u32>,  // n_arcs - index into turn_table
}

pub struct EbgCsrFile;

impl EbgCsrFile {
    /// Write EBG CSR to file
    pub fn write<P: AsRef<Path>>(path: P, data: &EbgCsr) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (64 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_nodes_bytes = data.n_nodes.to_le_bytes();
        let n_arcs_bytes = data.n_arcs.to_le_bytes();
        let created_unix_bytes = data.created_unix.to_le_bytes();
        let padding = [0u8; 4]; // Pad to 64 bytes: 4+2+2+4+8+8+32 = 60, need 4 more

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_nodes_bytes)?;
        writer.write_all(&n_arcs_bytes)?;
        writer.write_all(&created_unix_bytes)?;
        writer.write_all(&data.inputs_sha)?;
        writer.write_all(&padding)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_nodes_bytes);
        crc_digest.update(&n_arcs_bytes);
        crc_digest.update(&created_unix_bytes);
        crc_digest.update(&data.inputs_sha);
        crc_digest.update(&padding);

        // Offsets
        for &offset in &data.offsets {
            let bytes = offset.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Heads
        for &head in &data.heads {
            let bytes = head.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Turn indices
        for &idx in &data.turn_idx {
            let bytes = idx.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        let file_crc = body_crc;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read EBG CSR from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<EbgCsr> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = vec![0u8; 64];
        reader.read_exact(&mut header)?;

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let n_arcs = u64::from_le_bytes([
            header[12], header[13], header[14], header[15],
            header[16], header[17], header[18], header[19],
        ]);
        let created_unix = u64::from_le_bytes([
            header[20], header[21], header[22], header[23],
            header[24], header[25], header[26], header[27],
        ]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[28..60]);

        // Read offsets
        let mut offsets = Vec::with_capacity((n_nodes + 1) as usize);
        for _ in 0..=n_nodes {
            let mut buf = [0u8; 8];
            reader.read_exact(&mut buf)?;
            offsets.push(u64::from_le_bytes(buf));
        }

        // Read heads
        let mut heads = Vec::with_capacity(n_arcs as usize);
        for _ in 0..n_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            heads.push(u32::from_le_bytes(buf));
        }

        // Read turn indices
        let mut turn_idx = Vec::with_capacity(n_arcs as usize);
        for _ in 0..n_arcs {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            turn_idx.push(u32::from_le_bytes(buf));
        }

        Ok(EbgCsr {
            n_nodes,
            n_arcs,
            created_unix,
            inputs_sha,
            offsets,
            heads,
            turn_idx,
        })
    }
}
