//! ebg.turn_table format - Deduplicated turn table with mode masks

use anyhow::Result;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

use super::crc;
use crate::profile_abi::MAX_MODES;

const MAGIC: u32 = 0x45424754; // "EBGT"
const VERSION: u16 = 3; // v3: penalty array values in seconds (was deciseconds in v2, #297)

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

/// Turn table entry with dynamic per-mode penalty array.
/// `penalty_s[i]` = penalty in seconds for mode with index i (post-#297;
/// v2 stored deciseconds, v3 stores seconds).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub struct TurnEntry {
    pub mode_mask: u8, // bit i = mode i accessible
    pub kind: TurnKind,
    pub has_time_dep: bool,
    pub penalty_s: [u32; MAX_MODES], // indexed by mode index
    pub attrs_idx: u32,              // future use; 0 for now
}

#[derive(Debug)]
pub struct TurnTable {
    pub n_entries: u32,
    pub inputs_sha: [u8; 32],
    pub entries: Vec<TurnEntry>,
}

pub struct TurnTableFile;

/// Record size: mode_mask(1) + kind(1) + time_dep(1) + reserved(1) + penalty_ds(4*MAX_MODES) + attrs_idx(4)
const RECORD_SIZE: usize = 4 + 4 * MAX_MODES + 4; // = 40 bytes

impl TurnTableFile {
    /// Write turn table to file (v2 format with dynamic penalty arrays)
    pub fn write<P: AsRef<Path>>(path: P, data: &TurnTable) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header (44 bytes): magic(4) + version(2) + reserved(2) + n_entries(4) + inputs_sha(32)
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

        // Body: n_entries records (RECORD_SIZE bytes each)
        for entry in &data.entries {
            let mut record = [0u8; RECORD_SIZE];
            record[0] = entry.mode_mask;
            record[1] = entry.kind as u8;
            record[2] = entry.has_time_dep as u8;
            // record[3] = reserved

            // Write penalty_s array: MAX_MODES u32s starting at offset 4
            for (i, &p) in entry.penalty_s.iter().enumerate() {
                let off = 4 + i * 4;
                record[off..off + 4].copy_from_slice(&p.to_le_bytes());
            }

            // attrs_idx at offset 4 + MAX_MODES*4
            let attrs_off = 4 + MAX_MODES * 4;
            record[attrs_off..attrs_off + 4].copy_from_slice(&entry.attrs_idx.to_le_bytes());

            writer.write_all(&record)?;
            crc_digest.update(&record);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read turn table from file (v2 format with dynamic penalty arrays)
    pub fn read<P: AsRef<Path>>(path: P) -> Result<TurnTable> {
        let mut reader = BufReader::new(File::open(path)?);
        let mut crc_digest = crc::Digest::new();

        let mut header = vec![0u8; 44]; // magic(4) + version(2) + reserved(2) + n_entries(4) + inputs_sha(32)
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
        anyhow::ensure!(
            magic == MAGIC,
            "Invalid magic in ebg.turn_table: expected 0x{:08X}, got 0x{:08X}",
            MAGIC,
            magic
        );

        let version = u16::from_le_bytes([header[4], header[5]]);
        anyhow::ensure!(
            version == VERSION,
            "Unsupported turn_table version: {} (expected {}). \
             v2 stored deciseconds; re-run step 4 to regenerate as v3 (seconds, #297).",
            version,
            VERSION
        );

        let n_entries = u32::from_le_bytes([header[8], header[9], header[10], header[11]]);
        let mut inputs_sha = [0u8; 32];
        inputs_sha.copy_from_slice(&header[12..44]);

        let mut entries = Vec::with_capacity(n_entries as usize);
        for _ in 0..n_entries {
            let mut record = [0u8; RECORD_SIZE];
            reader.read_exact(&mut record)?;
            crc_digest.update(&record);

            let mut penalty_s = [0u32; MAX_MODES];
            for (i, slot) in penalty_s.iter_mut().enumerate() {
                let off = 4 + i * 4;
                *slot = u32::from_le_bytes([
                    record[off],
                    record[off + 1],
                    record[off + 2],
                    record[off + 3],
                ]);
            }

            let attrs_off = 4 + MAX_MODES * 4;
            entries.push(TurnEntry {
                mode_mask: record[0],
                kind: TurnKind::from(record[1]),
                has_time_dep: record[2] != 0,
                penalty_s,
                attrs_idx: u32::from_le_bytes([
                    record[attrs_off],
                    record[attrs_off + 1],
                    record[attrs_off + 2],
                    record[attrs_off + 3],
                ]),
            });
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in ebg.turn_table: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(TurnTable {
            n_entries,
            inputs_sha,
            entries,
        })
    }
}
