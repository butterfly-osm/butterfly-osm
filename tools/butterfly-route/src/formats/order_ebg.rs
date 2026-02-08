//! order.ebg format - CCH ordering permutation for EBG nodes

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4F524445; // "ORDE"
const VERSION: u16 = 1;

/// CCH ordering for EBG nodes
#[derive(Debug)]
pub struct OrderEbg {
    pub n_nodes: u32,
    pub inputs_sha: [u8; 32],
    pub perm: Vec<u32>,     // perm[old_id] = rank (elimination order)
    pub inv_perm: Vec<u32>, // inv_perm[rank] = old_id
}

pub struct OrderEbgFile;

impl OrderEbgFile {
    /// Write order.ebg to file
    pub fn write<P: AsRef<Path>>(path: P, data: &OrderEbg) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (48 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_nodes_bytes = data.n_nodes.to_le_bytes();
        let padding = [0u8; 4];

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_nodes_bytes)?;
        writer.write_all(&data.inputs_sha)?;
        writer.write_all(&padding)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_nodes_bytes);
        crc_digest.update(&data.inputs_sha);
        crc_digest.update(&padding);

        // perm array
        for &p in &data.perm {
            let bytes = p.to_le_bytes();
            writer.write_all(&bytes)?;
            crc_digest.update(&bytes);
        }

        // inv_perm array
        for &ip in &data.inv_perm {
            let bytes = ip.to_le_bytes();
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

    /// Read order.ebg from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<OrderEbg> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut crc_digest = crc::Digest::new();

        // Read header (48 bytes)
        let mut header = vec![0u8; 48];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        if magic != MAGIC {
            anyhow::bail!(
                "Invalid magic: expected 0x{:08X}, got 0x{:08X}",
                MAGIC,
                magic
            );
        }

        let n_nodes = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[12..44]);

        // Read perm
        let mut perm = Vec::with_capacity(n_nodes as usize);
        for _ in 0..n_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            perm.push(u32::from_le_bytes(buf));
        }

        // Read inv_perm
        let mut inv_perm = Vec::with_capacity(n_nodes as usize);
        for _ in 0..n_nodes {
            let mut buf = [0u8; 4];
            reader.read_exact(&mut buf)?;
            crc_digest.update(&buf);
            inv_perm.push(u32::from_le_bytes(buf));
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in order.ebg: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(OrderEbg {
            n_nodes,
            inputs_sha,
            perm,
            inv_perm,
        })
    }
}
