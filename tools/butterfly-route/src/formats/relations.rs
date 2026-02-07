//! relations.raw format - turn restrictions and relevant relations

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
        let header_size = 32u64; // magic(4) + version(2) + reserved(2) + count(8) + kdict_off(8) + vdict_off(8)

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

    /// Read relations.raw file and return relations with tags resolved from dictionaries
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Vec<Relation>> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < 32 + 16 {
            anyhow::bail!("File too short");
        }

        // Read header (32 bytes)
        let mut header = [0u8; 32];
        file.read_exact(&mut header)?;

        let magic = u32::from_le_bytes(header[0..4].try_into()?);
        if magic != MAGIC {
            anyhow::bail!("Invalid magic number: expected 0x{:08x}, got 0x{:08x}", MAGIC, magic);
        }

        let version = u16::from_le_bytes(header[4..6].try_into()?);
        if version != VERSION {
            anyhow::bail!("Unsupported version: {}", version);
        }

        let count = u64::from_le_bytes(header[8..16].try_into()?);
        let kdict_off = u64::from_le_bytes(header[16..24].try_into()?);
        let vdict_off = u64::from_le_bytes(header[24..32].try_into()?);

        // Read entire file
        let mut all_bytes = vec![0u8; file_len as usize];
        all_bytes[..32].copy_from_slice(&header);
        file.read_exact(&mut all_bytes[32..])?;

        // Read key dictionary
        let key_dict = Self::read_dict(&all_bytes, kdict_off as usize, vdict_off as usize)?;

        // Read value dictionary (includes roles)
        let val_dict = Self::read_dict(&all_bytes, vdict_off as usize, all_bytes.len() - 16)?;

        // Read relations from body (starts at offset 32)
        let mut relations = Vec::with_capacity(count as usize);
        let mut pos = 32usize;

        for _ in 0..count {
            // rel_id
            let rel_id = i64::from_le_bytes(all_bytes[pos..pos+8].try_into()?);
            pos += 8;

            // n_members
            let n_members = u16::from_le_bytes(all_bytes[pos..pos+2].try_into()?) as usize;
            pos += 2;

            // members
            let mut members = Vec::with_capacity(n_members);
            for _ in 0..n_members {
                let role_id = u16::from_le_bytes(all_bytes[pos..pos+2].try_into()?) as u32;
                let kind_byte = all_bytes[pos+2];
                let kind = MemberKind::from_u8(kind_byte)
                    .ok_or_else(|| anyhow::anyhow!("Invalid member kind: {}", kind_byte))?;
                // skip reserved byte at pos+3
                let ref_id = i64::from_le_bytes(all_bytes[pos+4..pos+12].try_into()?);
                pos += 12;

                let role = val_dict.get(&role_id)
                    .ok_or_else(|| anyhow::anyhow!("Role ID {} not in dictionary", role_id))?
                    .clone();

                members.push(Member {
                    role,
                    kind,
                    ref_id,
                });
            }

            // n_tags
            let n_tags = u16::from_le_bytes(all_bytes[pos..pos+2].try_into()?) as usize;
            pos += 2;

            // tags
            let mut tags = Vec::with_capacity(n_tags);
            for _ in 0..n_tags {
                let k_id = u32::from_le_bytes(all_bytes[pos..pos+4].try_into()?);
                let v_id = u32::from_le_bytes(all_bytes[pos+4..pos+8].try_into()?);
                pos += 8;

                let key = key_dict.get(&k_id)
                    .ok_or_else(|| anyhow::anyhow!("Key ID {} not in dictionary", k_id))?
                    .clone();
                let val = val_dict.get(&v_id)
                    .ok_or_else(|| anyhow::anyhow!("Value ID {} not in dictionary", v_id))?
                    .clone();
                tags.push((key, val));
            }

            relations.push(Relation {
                id: rel_id,
                members,
                tags,
            });
        }

        Ok(relations)
    }

    /// Read dictionaries from relations.raw and return them with their SHA-256 hashes
    #[allow(clippy::type_complexity)]
    pub fn read_dictionaries<P: AsRef<Path>>(path: P) -> Result<(HashMap<u32, String>, HashMap<u32, String>, [u8; 32], [u8; 32])> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < 32 + 16 {
            anyhow::bail!("File too short");
        }

        // Read header (32 bytes)
        let mut header = [0u8; 32];
        file.read_exact(&mut header)?;

        let kdict_off = u64::from_le_bytes(header[16..24].try_into()?);
        let vdict_off = u64::from_le_bytes(header[24..32].try_into()?);

        // Read entire file
        let mut all_bytes = vec![0u8; file_len as usize];
        all_bytes[..32].copy_from_slice(&header);
        file.read_exact(&mut all_bytes[32..])?;

        // Read key dictionary
        let key_dict = Self::read_dict(&all_bytes, kdict_off as usize, vdict_off as usize)?;

        // Read value dictionary
        let val_dict = Self::read_dict(&all_bytes, vdict_off as usize, all_bytes.len() - 16)?;

        // Compute SHA-256 of dictionaries
        let key_sha256 = Self::compute_dict_sha256(&key_dict);
        let val_sha256 = Self::compute_dict_sha256(&val_dict);

        Ok((key_dict, val_dict, key_sha256, val_sha256))
    }

    /// Read a dictionary from byte buffer
    fn read_dict(bytes: &[u8], start: usize, end: usize) -> Result<HashMap<u32, String>> {
        let mut dict = HashMap::new();
        let mut pos = start;

        while pos < end {
            if pos + 6 > end {
                break; // Not enough bytes for id(4) + len(2)
            }

            let id = u32::from_le_bytes(bytes[pos..pos+4].try_into()?);
            let len = u16::from_le_bytes(bytes[pos+4..pos+6].try_into()?) as usize;
            pos += 6;

            if pos + len > end {
                anyhow::bail!("Dictionary entry extends beyond bounds");
            }

            // Use from_utf8_lossy to handle malformed UTF-8 in OSM data
            let s = String::from_utf8_lossy(&bytes[pos..pos+len]).to_string();
            dict.insert(id, s);
            pos += len;
        }

        Ok(dict)
    }

    /// Compute SHA-256 of a dictionary
    fn compute_dict_sha256(dict: &HashMap<u32, String>) -> [u8; 32] {
        use sha2::{Sha256, Digest};

        let mut hasher = Sha256::new();
        let mut keys: Vec<_> = dict.keys().collect();
        keys.sort();

        for key in keys {
            hasher.update(key.to_le_bytes());
            hasher.update(dict[key].as_bytes());
        }

        let result = hasher.finalize();
        let mut hash = [0u8; 32];
        hash.copy_from_slice(&result);
        hash
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
