///! ebg.turn_table format - Deduplicated turn table with mode masks

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write, Read, BufReader};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x45424754; // "EBGT"
const VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum TurnKind {
    None = 0,
    Ban = 1,
    Only = 2,
    Penalty = 3,
}

impl From<u8> for TurnKind {
    fn from(value: u8) -> Self {
        match value {
            1 => TurnKind::Ban,
            2 => TurnKind::Only,
            3 => TurnKind::Penalty,
            _ => TurnKind::None,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TurnEntry {
    pub mode_mask: u8,          // bit0=car, bit1=bike, bit2=foot
    pub kind: TurnKind,
    pub has_time_dep: bool,
    pub penalty_ds_car: u32,    // deciseconds, 0 if N/A
    pub penalty_ds_bike: u32,
    pub penalty_ds_foot: u32,
    pub attrs_idx: u32,         // future use (e.g., turn classes); 0 for now
}

#[derive(Debug)]
pub struct TurnTable {
    pub n_entries: u32,
    pub inputs_sha: [u8; 32],
    pub entries: Vec<TurnEntry>,
}

pub struct TurnTableFile;

impl TurnTableFile {
    /// Write turn table to file
    pub fn write<P: AsRef<Path>>(path: P, data: &TurnTable) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (40 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_entries_bytes = data.n_entries.to_le_bytes();

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_entries_bytes)?;
        writer.write_all(&data.inputs_sha)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_entries_bytes);
        crc_digest.update(&data.inputs_sha);

        // Body: n_entries records (20 bytes each)
        for entry in &data.entries {
            let mode_byte = entry.mode_mask.to_le_bytes();
            let kind_byte = (entry.kind as u8).to_le_bytes();
            let time_dep_byte = (entry.has_time_dep as u8).to_le_bytes();
            let reserved_byte = 0u8.to_le_bytes();
            let car_bytes = entry.penalty_ds_car.to_le_bytes();
            let bike_bytes = entry.penalty_ds_bike.to_le_bytes();
            let foot_bytes = entry.penalty_ds_foot.to_le_bytes();
            let attrs_bytes = entry.attrs_idx.to_le_bytes();

            writer.write_all(&mode_byte)?;
            writer.write_all(&kind_byte)?;
            writer.write_all(&time_dep_byte)?;
            writer.write_all(&reserved_byte)?;
            writer.write_all(&car_bytes)?;
            writer.write_all(&bike_bytes)?;
            writer.write_all(&foot_bytes)?;
            writer.write_all(&attrs_bytes)?;

            crc_digest.update(&mode_byte);
            crc_digest.update(&kind_byte);
            crc_digest.update(&time_dep_byte);
            crc_digest.update(&reserved_byte);
            crc_digest.update(&car_bytes);
            crc_digest.update(&bike_bytes);
            crc_digest.update(&foot_bytes);
            crc_digest.update(&attrs_bytes);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        let file_crc = body_crc;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read turn table from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<TurnTable> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut header = vec![0u8; 44]; // magic(4) + version(2) + reserved(2) + n_entries(4) + inputs_sha(32)
        reader.read_exact(&mut header)?;

        let n_entries = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[12..44]);

        let mut entries = Vec::with_capacity(n_entries as usize);
        for _ in 0..n_entries {
            let mut record = [0u8; 20];
            reader.read_exact(&mut record)?;

            entries.push(TurnEntry {
                mode_mask: record[0],
                kind: TurnKind::from(record[1]),
                has_time_dep: record[2] != 0,
                penalty_ds_car: u32::from_le_bytes([record[4], record[5], record[6], record[7]]),
                penalty_ds_bike: u32::from_le_bytes([record[8], record[9], record[10], record[11]]),
                penalty_ds_foot: u32::from_le_bytes([record[12], record[13], record[14], record[15]]),
                attrs_idx: u32::from_le_bytes([record[16], record[17], record[18], record[19]]),
            });
        }

        Ok(TurnTable {
            n_entries,
            inputs_sha,
            entries,
        })
    }
}
