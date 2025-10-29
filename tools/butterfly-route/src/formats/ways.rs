///! ways.raw format - way geometry and tags with dictionary encoding

use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x57415953; // "WAYS"
const VERSION: u16 = 1;

#[derive(Clone)]
pub struct Way {
    pub id: i64,
    pub nodes: Vec<i64>,
    pub tags: Vec<(String, String)>,
}

pub struct WaysFile;

impl WaysFile {
    /// Write ways.raw file
    pub fn write<P: AsRef<Path>>(path: P, ways: &[Way]) -> Result<()> {
        if ways.is_empty() {
            anyhow::bail!("Cannot write empty ways file");
        }

        // Build dictionaries
        let mut keys = HashMap::new();
        let mut values = HashMap::new();

        for way in ways {
            for (k, v) in &way.tags {
                keys.insert(k.clone(), 0u32);
                values.insert(v.clone(), 0u32);
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

        // Sort ways by ID
        let mut sorted_ways = ways.to_vec();
        sorted_ways.sort_by_key(|w| w.id);

        // Calculate offsets BEFORE writing anything
        let header_size = 28u64; // magic(4) + version(2) + reserved(2) + count(8) + kdict_off(8) + vdict_off(8)

        let mut body_size = 0u64;
        for way in &sorted_ways {
            body_size += 8; // way_id
            body_size += 4; // n_nodes
            body_size += 8 * way.nodes.len() as u64; // nodes
            body_size += 2; // n_tags
            body_size += 8 * way.tags.len() as u64; // tags (k_id + v_id)
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
        header_bytes.extend_from_slice(&(sorted_ways.len() as u64).to_le_bytes());
        header_bytes.extend_from_slice(&kdict_off.to_le_bytes());
        header_bytes.extend_from_slice(&vdict_off.to_le_bytes());

        writer.write_all(&header_bytes)?;
        crc_digest.update(&header_bytes);

        // Write ways
        let mut ways_digest = crc::Digest::new();
        for way in &sorted_ways {
            // way_id
            let id_bytes = way.id.to_le_bytes();
            writer.write_all(&id_bytes)?;
            ways_digest.update(&id_bytes);
            crc_digest.update(&id_bytes);

            // n_nodes
            let n_nodes = way.nodes.len() as u32;
            let n_nodes_bytes = n_nodes.to_le_bytes();
            writer.write_all(&n_nodes_bytes)?;
            ways_digest.update(&n_nodes_bytes);
            crc_digest.update(&n_nodes_bytes);

            // nodes
            for node_id in &way.nodes {
                let node_bytes = node_id.to_le_bytes();
                writer.write_all(&node_bytes)?;
                ways_digest.update(&node_bytes);
                crc_digest.update(&node_bytes);
            }

            // n_tags
            let n_tags = way.tags.len() as u16;
            let n_tags_bytes = n_tags.to_le_bytes();
            writer.write_all(&n_tags_bytes)?;
            ways_digest.update(&n_tags_bytes);
            crc_digest.update(&n_tags_bytes);

            // tags
            for (k, v) in &way.tags {
                let k_id = key_dict[k];
                let v_id = value_dict[v];
                let k_bytes = k_id.to_le_bytes();
                let v_bytes = v_id.to_le_bytes();
                writer.write_all(&k_bytes)?;
                writer.write_all(&v_bytes)?;
                ways_digest.update(&k_bytes);
                ways_digest.update(&v_bytes);
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
        let ways_crc = ways_digest.finalize();
        let file_crc = crc_digest.finalize();
        writer.write_all(&ways_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Verify checksums in ways.raw file
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

        println!("✓ ways.raw CRC-64 verified");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::NamedTempFile;

    #[test]
    fn test_ways_write_verify() {
        let ways = vec![
            Way {
                id: 100,
                nodes: vec![1, 2, 3],
                tags: vec![
                    ("highway".to_string(), "residential".to_string()),
                    ("name".to_string(), "Main St".to_string()),
                ],
            },
            Way {
                id: 50,
                nodes: vec![4, 5],
                tags: vec![("highway".to_string(), "primary".to_string())],
            },
        ];

        let tmpfile = NamedTempFile::new().unwrap();
        WaysFile::write(tmpfile.path(), &ways).unwrap();
        WaysFile::verify(tmpfile.path()).unwrap();
    }
}
