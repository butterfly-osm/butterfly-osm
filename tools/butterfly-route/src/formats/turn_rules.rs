//! turn_rules.<mode>.bin format - Per-mode turn restrictions
//!
//! Format (little-endian):
//!
//! Header (80 bytes):
//!   magic:         u32 = 0x5455524E  // "TURN"
//!   version:       u16 = 1
//!   mode:          u8  = {0,1,2}
//!   reserved:      u8  = 0
//!   count:         u64
//!   rel_dict_k_sha: [32]u8
//!   rel_dict_v_sha: [32]u8
//!
//! Body (count records, sorted by via_node_id, from_way_id, to_way_id):
//!   via_node_id:   i64
//!   from_way_id:   i64
//!   to_way_id:     i64
//!   kind:          u8  // 0=None,1=Ban,2=Only,3=Penalty
//!   penalty_ds:    u32
//!   is_time_dep:   u8  // 0/1/2 (2=needs_expansion for via=way)
//!   reserved:      [6]u8
//!
//! Footer (16 bytes):
//!   body_crc64:    u64
//!   file_crc64:    u64

use anyhow::{Context, Result};
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc::Digest;
use crate::profile_abi::{Mode, TurnRuleKind};

const MAGIC: u32 = 0x5455524E; // "TURN"
const VERSION: u16 = 1;
const HEADER_SIZE: usize = 80; // 4 + 2 + 1 + 1 + 8 + 32 + 32
const RECORD_SIZE: usize = 36; // i64*3 + u8 + u32 + u8 + [6]u8 = 24 + 1 + 4 + 1 + 6

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct TurnRule {
    pub via_node_id: i64,
    pub from_way_id: i64,
    pub to_way_id: i64,
    pub kind: TurnRuleKind,
    pub penalty_ds: u32,
    pub is_time_dep: u8, // 0=static, 1=time-dependent, 2=needs expansion (via=way)
}

/// Write turn_rules.<mode>.bin file
pub fn write<P: AsRef<Path>>(
    path: P,
    mode: Mode,
    rules: &[TurnRule],
    rel_dict_k_sha256: &[u8; 32],
    rel_dict_v_sha256: &[u8; 32],
) -> Result<()> {
    let file = File::create(path.as_ref())
        .with_context(|| format!("Failed to create {}", path.as_ref().display()))?;
    let mut writer = BufWriter::new(file);

    // Sort rules by (via_node_id, from_way_id, to_way_id)
    let mut sorted_rules = rules.to_vec();
    sorted_rules.sort();

    // Build header
    let mut header = Vec::with_capacity(HEADER_SIZE);
    header.extend_from_slice(&MAGIC.to_le_bytes());
    header.extend_from_slice(&VERSION.to_le_bytes());
    header.push(mode as u8);
    header.push(0); // reserved
    header.extend_from_slice(&(sorted_rules.len() as u64).to_le_bytes());
    header.extend_from_slice(rel_dict_k_sha256);
    header.extend_from_slice(rel_dict_v_sha256);
    assert_eq!(header.len(), HEADER_SIZE);

    writer.write_all(&header)?;

    // Write body and calculate CRC
    let mut body_digest = Digest::new();
    for rule in sorted_rules.iter() {
        let record = encode_record(rule);
        body_digest.update(&record);
        writer.write_all(&record)?;
    }

    let body_crc64 = body_digest.finalize();

    // Calculate file CRC (header + body)
    let mut file_digest = Digest::new();
    file_digest.update(&header);
    for rule in sorted_rules.iter() {
        let record = encode_record(rule);
        file_digest.update(&record);
    }
    let file_crc64 = file_digest.finalize();

    // Write footer
    writer.write_all(&body_crc64.to_le_bytes())?;
    writer.write_all(&file_crc64.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

/// Encode a single turn rule record
fn encode_record(rule: &TurnRule) -> Vec<u8> {
    let mut record = Vec::with_capacity(RECORD_SIZE);

    record.extend_from_slice(&rule.via_node_id.to_le_bytes());
    record.extend_from_slice(&rule.from_way_id.to_le_bytes());
    record.extend_from_slice(&rule.to_way_id.to_le_bytes());
    record.push(rule.kind as u8);
    record.extend_from_slice(&rule.penalty_ds.to_le_bytes());
    record.push(rule.is_time_dep);
    record.extend_from_slice(&[0u8; 6]); // reserved

    assert_eq!(record.len(), RECORD_SIZE);
    record
}

/// Decode a single turn rule record
fn decode_record(record: &[u8]) -> Result<TurnRule> {
    anyhow::ensure!(record.len() >= RECORD_SIZE, "Record too small");

    let via_node_id = i64::from_le_bytes([
        record[0], record[1], record[2], record[3],
        record[4], record[5], record[6], record[7],
    ]);
    let from_way_id = i64::from_le_bytes([
        record[8], record[9], record[10], record[11],
        record[12], record[13], record[14], record[15],
    ]);
    let to_way_id = i64::from_le_bytes([
        record[16], record[17], record[18], record[19],
        record[20], record[21], record[22], record[23],
    ]);
    let kind_byte = record[24];
    let penalty_ds = u32::from_le_bytes([record[25], record[26], record[27], record[28]]);
    let is_time_dep = record[29];

    let kind = match kind_byte {
        0 => TurnRuleKind::None,
        1 => TurnRuleKind::Ban,
        2 => TurnRuleKind::Only,
        3 => TurnRuleKind::Penalty,
        _ => anyhow::bail!("Invalid turn rule kind: {}", kind_byte),
    };

    Ok(TurnRule {
        via_node_id,
        from_way_id,
        to_way_id,
        kind,
        penalty_ds,
        is_time_dep,
    })
}

/// Read all turn rules from file
pub fn read_all<P: AsRef<Path>>(path: P) -> Result<Vec<TurnRule>> {
    use std::io::Read;

    let mut file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;

    // Read header
    let mut header = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header)?;

    let count = u64::from_le_bytes([
        header[8], header[9], header[10], header[11],
        header[12], header[13], header[14], header[15],
    ]);

    let mut rules = Vec::with_capacity(count as usize);
    for _ in 0..count {
        let mut record = vec![0u8; RECORD_SIZE];
        file.read_exact(&mut record)?;
        rules.push(decode_record(&record)?);
    }

    Ok(rules)
}

/// Verify turn_rules file structure and checksums
pub fn verify<P: AsRef<Path>>(path: P) -> Result<()> {
    use std::io::{Read, Seek, SeekFrom};

    let mut file = File::open(path.as_ref())
        .with_context(|| format!("Failed to open {}", path.as_ref().display()))?;

    // Read and verify header
    let mut header = vec![0u8; HEADER_SIZE];
    file.read_exact(&mut header)?;

    let magic = u32::from_le_bytes([header[0], header[1], header[2], header[3]]);
    if magic != MAGIC {
        anyhow::bail!(
            "Invalid magic in {}: expected 0x{:08x}, got 0x{:08x}",
            path.as_ref().display(),
            MAGIC,
            magic
        );
    }

    let count = u64::from_le_bytes([
        header[8], header[9], header[10], header[11],
        header[12], header[13], header[14], header[15],
    ]);

    // Verify file size
    let expected_size = HEADER_SIZE as u64 + (count * RECORD_SIZE as u64) + 16;
    let actual_size = file.seek(SeekFrom::End(0))?;

    if actual_size != expected_size {
        anyhow::bail!(
            "Size mismatch in {}: expected {} bytes, got {} bytes",
            path.as_ref().display(),
            expected_size,
            actual_size
        );
    }

    println!(
        "  ✓ {} verified ({} rules, {} bytes)",
        path.as_ref().display(),
        count,
        actual_size
    );
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_record_size() {
        let rule = TurnRule {
            via_node_id: 123,
            from_way_id: 456,
            to_way_id: 789,
            kind: TurnRuleKind::Ban,
            penalty_ds: 0,
            is_time_dep: 0,
        };
        let record = encode_record(&rule);
        assert_eq!(record.len(), RECORD_SIZE);
    }

    #[test]
    fn test_turn_rule_ordering() {
        let mut rules = [
            TurnRule {
                via_node_id: 2,
                from_way_id: 1,
                to_way_id: 1,
                kind: TurnRuleKind::Ban,
                penalty_ds: 0,
                is_time_dep: 0,
            },
            TurnRule {
                via_node_id: 1,
                from_way_id: 2,
                to_way_id: 1,
                kind: TurnRuleKind::Ban,
                penalty_ds: 0,
                is_time_dep: 0,
            },
        ];
        rules.sort();
        assert_eq!(rules[0].via_node_id, 1);
        assert_eq!(rules[1].via_node_id, 2);
    }
}
