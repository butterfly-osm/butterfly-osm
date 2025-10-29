///! relations.raw format - turn restrictions and relevant relations

use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x52454C53; // "RELS"
const VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MemberKind {
    Node = 0,
    Way = 1,
    Relation = 2,
}

impl MemberKind {
    #[allow(dead_code)]
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(MemberKind::Node),
            1 => Some(MemberKind::Way),
            2 => Some(MemberKind::Relation),
            _ => None,
        }
    }
}

#[derive(Clone)]
pub struct Member {
    pub role: String,
    pub kind: MemberKind,
    pub ref_id: i64,
}

#[derive(Clone)]
pub struct Relation {
    pub id: i64,
    pub members: Vec<Member>,
    pub tags: Vec<(String, String)>,
}

pub struct RelationsFile;

impl RelationsFile {
    /// Write relations.raw file
    pub fn write<P: AsRef<Path>>(path: P, relations: &[Relation]) -> Result<()> {
        if relations.is_empty() {
            // Empty is OK for relations (some regions may have no restrictions)
            // Write empty file with just header and footer
            let file = File::create(path.as_ref())?;
            let mut writer = BufWriter::new(file);

            let mut header_bytes = Vec::new();
            header_bytes.extend_from_slice(&MAGIC.to_le_bytes());
            header_bytes.extend_from_slice(&VERSION.to_le_bytes());
            header_bytes.extend_from_slice(&0u16.to_le_bytes()); // reserved
            header_bytes.extend_from_slice(&0u64.to_le_bytes()); // count = 0
            header_bytes.extend_from_slice(&28u64.to_le_bytes()); // kdict_off (right after header)
            header_bytes.extend_from_slice(&28u64.to_le_bytes()); // vdict_off (same, empty dict)

            writer.write_all(&header_bytes)?;
            let crc = crc::checksum(&header_bytes);
            writer.write_all(&0u64.to_le_bytes())?; // rels_crc
            writer.write_all(&crc.to_le_bytes())?; // file_crc
            writer.flush()?;
            return Ok(());
        }

        // Build dictionaries (keys, values, roles)
        let mut keys = HashMap::new();
        let mut values = HashMap::new();

        for rel in relations {
            for (k, v) in &rel.tags {
                keys.insert(k.clone(), 0u32);
                values.insert(v.clone(), 0u32);
            }
            for member in &rel.members {
                values.insert(member.role.clone(), 0u32); // roles share value dict
            }
        }

        // Sort dictionaries lexicographically
        let mut sorted_keys: Vec<_> = keys.keys().cloned().collect();
        sorted_keys.sort();
        let key_dict: HashMap<String, u32> = sorted_keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), i as u32))
            .collect();

        let mut sorted_values: Vec<_> = values.keys().cloned().collect();
        sorted_values.sort();
        let value_dict: HashMap<String, u32> = sorted_values
            .iter()
            .enumerate()
            .map(|(i, v)| (v.clone(), i as u32))
            .collect();

        // Sort relations by ID
        let mut sorted_rels = relations.to_vec();
        sorted_rels.sort_by_key(|r| r.id);

        // Calculate offsets BEFORE writing anything
        let header_size = 28u64; // magic(4) + version(2) + reserved(2) + count(8) + kdict_off(8) + vdict_off(8)

        let mut body_size = 0u64;
        for rel in &sorted_rels {
            body_size += 8; // rel_id
            body_size += 2; // n_members
            body_size += 12 * rel.members.len() as u64; // members (role_id(2) + kind(1) + reserved(1) + ref(8))
            body_size += 2; // n_tags
            body_size += 8 * rel.tags.len() as u64; // tags (k_id + v_id)
        }

        let kdict_off = header_size + body_size;

        let mut kdict_size = 0u64;
        for key in &sorted_keys {
            kdict_size += 4; // k_id
            kdict_size += 2; // len
            kdict_size += key.len() as u64; // bytes
        }

        let vdict_off = kdict_off + kdict_size;

        // Now write everything with correct offsets from the start
        let file = File::create(path.as_ref())?;
        let mut writer = BufWriter::new(file);
        let mut crc_digest = crc::Digest::new();

        // Write header with CORRECT offsets
        let mut header_bytes = Vec::new();
        header_bytes.extend_from_slice(&MAGIC.to_le_bytes());
        header_bytes.extend_from_slice(&VERSION.to_le_bytes());
        header_bytes.extend_from_slice(&0u16.to_le_bytes()); // reserved
        header_bytes.extend_from_slice(&(sorted_rels.len() as u64).to_le_bytes());
        header_bytes.extend_from_slice(&kdict_off.to_le_bytes());
        header_bytes.extend_from_slice(&vdict_off.to_le_bytes());

        writer.write_all(&header_bytes)?;
        crc_digest.update(&header_bytes);

        // Write relations
        let mut rels_digest = crc::Digest::new();
        for rel in &sorted_rels {
            // rel_id
            let id_bytes = rel.id.to_le_bytes();
            writer.write_all(&id_bytes)?;
            rels_digest.update(&id_bytes);
            crc_digest.update(&id_bytes);

            // n_members
            let n_members = rel.members.len() as u16;
            let n_members_bytes = n_members.to_le_bytes();
            writer.write_all(&n_members_bytes)?;
            rels_digest.update(&n_members_bytes);
            crc_digest.update(&n_members_bytes);

            // members
            for member in &rel.members {
                let role_id = value_dict[&member.role] as u16;
                let kind = member.kind as u8;
                let reserved = 0u8;

                let role_bytes = role_id.to_le_bytes();
                writer.write_all(&role_bytes)?;
                writer.write_all(&[kind])?;
                writer.write_all(&[reserved])?;
                let ref_bytes = member.ref_id.to_le_bytes();
                writer.write_all(&ref_bytes)?;

                rels_digest.update(&role_bytes);
                rels_digest.update(&[kind]);
                rels_digest.update(&[reserved]);
                rels_digest.update(&ref_bytes);
                crc_digest.update(&role_bytes);
                crc_digest.update(&[kind]);
                crc_digest.update(&[reserved]);
                crc_digest.update(&ref_bytes);
            }

            // n_tags
            let n_tags = rel.tags.len() as u16;
            let n_tags_bytes = n_tags.to_le_bytes();
            writer.write_all(&n_tags_bytes)?;
            rels_digest.update(&n_tags_bytes);
            crc_digest.update(&n_tags_bytes);

            // tags
            for (k, v) in &rel.tags {
                let k_id = key_dict[k];
                let v_id = value_dict[v];
                let k_bytes = k_id.to_le_bytes();
                let v_bytes = v_id.to_le_bytes();
                writer.write_all(&k_bytes)?;
                writer.write_all(&v_bytes)?;
                rels_digest.update(&k_bytes);
                rels_digest.update(&v_bytes);
                crc_digest.update(&k_bytes);
                crc_digest.update(&v_bytes);
            }
        }

        // Write key dictionary
        for key in &sorted_keys {
            let k_id = key_dict[key];
            let len = key.len() as u16;
            writer.write_all(&k_id.to_le_bytes())?;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(key.as_bytes())?;
            crc_digest.update(&k_id.to_le_bytes());
            crc_digest.update(&len.to_le_bytes());
            crc_digest.update(key.as_bytes());
        }

        // Write value dictionary
        for value in &sorted_values {
            let v_id = value_dict[value];
            let len = value.len() as u16;
            writer.write_all(&v_id.to_le_bytes())?;
            writer.write_all(&len.to_le_bytes())?;
            writer.write_all(value.as_bytes())?;
            crc_digest.update(&v_id.to_le_bytes());
            crc_digest.update(&len.to_le_bytes());
            crc_digest.update(value.as_bytes());
        }

        // Write footer
        let rels_crc = rels_digest.finalize();
        let file_crc = crc_digest.finalize();
        writer.write_all(&rels_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Verify checksums in relations.raw file
    pub fn verify<P: AsRef<Path>>(path: P) -> Result<()> {
        let mut file = File::open(path)?;

        // Read entire file except footer
        let file_len = file.metadata()?.len();
        if file_len < 28 + 16 {
            anyhow::bail!("File too short");
        }

        let content_len = file_len - 16;
        let mut content = vec![0u8; content_len as usize];
        file.read_exact(&mut content)?;

        // Read footer
        let mut footer = [0u8; 16];
        file.read_exact(&mut footer)?;

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

        println!("✓ relations.raw CRC-64 verified");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_relations_write_verify() {
        let relations = vec![Relation {
            id: 100,
            members: vec![
                Member {
                    role: "from".to_string(),
                    kind: MemberKind::Way,
                    ref_id: 1,
                },
                Member {
                    role: "via".to_string(),
                    kind: MemberKind::Node,
                    ref_id: 2,
                },
                Member {
                    role: "to".to_string(),
                    kind: MemberKind::Way,
                    ref_id: 3,
                },
            ],
            tags: vec![
                ("type".to_string(), "restriction".to_string()),
                ("restriction".to_string(), "no_left_turn".to_string()),
            ],
        }];

        let tmpfile = NamedTempFile::new().unwrap();
        RelationsFile::write(tmpfile.path(), &relations).unwrap();
        RelationsFile::verify(tmpfile.path()).unwrap();
    }
}
