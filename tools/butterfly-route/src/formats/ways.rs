//! ways.raw format - way geometry and tags with dictionary encoding

use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
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
        let header_size = 32u64; // magic(4) + version(2) + reserved(2) + count(8) + kdict_off(8) + vdict_off(8)

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

    /// Read ways.raw file and return ways with tags resolved from dictionaries
    pub fn read<P: AsRef<Path>>(path: P) -> Result<Vec<Way>> {
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
            anyhow::bail!(
                "Invalid magic number: expected 0x{:08x}, got 0x{:08x}",
                MAGIC,
                magic
            );
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

        // Read value dictionary
        let val_dict = Self::read_dict(&all_bytes, vdict_off as usize, all_bytes.len() - 16)?;

        // Read ways from body (starts at offset 32)
        let mut ways = Vec::with_capacity(count as usize);
        let mut pos = 32usize;

        for _ in 0..count {
            if pos + 8 > all_bytes.len() {
                anyhow::bail!(
                    "ways.raw: truncated file at offset {}, need {} bytes",
                    pos,
                    pos + 8
                );
            }
            // way_id
            let way_id = i64::from_le_bytes(all_bytes[pos..pos + 8].try_into()?);
            pos += 8;

            // n_nodes
            let n_nodes = u32::from_le_bytes(all_bytes[pos..pos + 4].try_into()?) as usize;
            pos += 4;

            // nodes
            let mut nodes = Vec::with_capacity(n_nodes);
            for _ in 0..n_nodes {
                let node_id = i64::from_le_bytes(all_bytes[pos..pos + 8].try_into()?);
                nodes.push(node_id);
                pos += 8;
            }

            // n_tags
            let n_tags = u16::from_le_bytes(all_bytes[pos..pos + 2].try_into()?) as usize;
            pos += 2;

            // tags
            let mut tags = Vec::with_capacity(n_tags);
            for _ in 0..n_tags {
                let k_id = u32::from_le_bytes(all_bytes[pos..pos + 4].try_into()?);
                let v_id = u32::from_le_bytes(all_bytes[pos + 4..pos + 8].try_into()?);
                pos += 8;

                let key = key_dict
                    .get(&k_id)
                    .ok_or_else(|| anyhow::anyhow!("Key ID {} not in dictionary", k_id))?
                    .clone();
                let val = val_dict
                    .get(&v_id)
                    .ok_or_else(|| anyhow::anyhow!("Value ID {} not in dictionary", v_id))?
                    .clone();
                tags.push((key, val));
            }

            ways.push(Way {
                id: way_id,
                nodes,
                tags,
            });
        }

        Ok(ways)
    }

    /// Read dictionaries from ways.raw and return them with their SHA-256 hashes
    #[allow(clippy::type_complexity)]
    pub fn read_dictionaries<P: AsRef<Path>>(
        path: P,
    ) -> Result<(
        HashMap<u32, String>,
        HashMap<u32, String>,
        [u8; 32],
        [u8; 32],
    )> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();

        if file_len < 32 + 16 {
            anyhow::bail!("File too short");
        }

        // Read header (32 bytes: magic(4) + version(2) + reserved(2) + count(8) + kdict_off(8) + vdict_off(8))
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

            let id = u32::from_le_bytes(bytes[pos..pos + 4].try_into()?);
            let len = u16::from_le_bytes(bytes[pos + 4..pos + 6].try_into()?) as usize;
            pos += 6;

            if pos + len > end {
                anyhow::bail!("Dictionary entry extends beyond bounds");
            }

            // Use from_utf8_lossy to handle malformed UTF-8 in OSM data
            let s = String::from_utf8_lossy(&bytes[pos..pos + len]).to_string();
            dict.insert(id, s);
            pos += len;
        }

        Ok(dict)
    }

    /// Stream ways from file without loading all into memory
    /// Yields (way_id, tag_key_ids, tag_val_ids, nodes)
    #[allow(clippy::type_complexity)]
    pub fn stream_ways<P: AsRef<Path>>(
        path: P,
    ) -> Result<impl Iterator<Item = Result<(i64, Vec<u32>, Vec<u32>, Vec<i64>)>>> {
        use std::io::{BufReader, Seek, SeekFrom};

        let mut file = File::open(path)?;
        let _file_len = file.metadata()?.len();

        // Read header
        let mut header = [0u8; 32];
        file.read_exact(&mut header)?;

        let count = u64::from_le_bytes(header[8..16].try_into()?);
        let kdict_off = u64::from_le_bytes(header[16..24].try_into()?);

        // Position at start of ways data
        file.seek(SeekFrom::Start(32))?;

        let reader = BufReader::with_capacity(1024 * 1024, file); // 1MB buffer

        Ok(WayStreamIterator {
            reader,
            remaining: count,
            _end_offset: kdict_off,
        })
    }

    /// Compute SHA-256 of a dictionary
    fn compute_dict_sha256(dict: &HashMap<u32, String>) -> [u8; 32] {
        use sha2::{Digest, Sha256};

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

/// Iterator for streaming ways from file
pub struct WayStreamIterator<R: Read> {
    reader: BufReader<R>,
    remaining: u64,
    _end_offset: u64,
}

impl<R: Read> Iterator for WayStreamIterator<R> {
    type Item = Result<(i64, Vec<u32>, Vec<u32>, Vec<i64>)>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        self.remaining -= 1;

        // Read way_id
        let mut buf8 = [0u8; 8];
        if let Err(e) = self.reader.read_exact(&mut buf8) {
            return Some(Err(e.into()));
        }
        let way_id = i64::from_le_bytes(buf8);

        // Read n_nodes
        let mut buf4 = [0u8; 4];
        if let Err(e) = self.reader.read_exact(&mut buf4) {
            return Some(Err(e.into()));
        }
        let n_nodes = u32::from_le_bytes(buf4) as usize;

        // Read nodes
        let mut nodes = Vec::with_capacity(n_nodes);
        for _ in 0..n_nodes {
            if let Err(e) = self.reader.read_exact(&mut buf8) {
                return Some(Err(e.into()));
            }
            nodes.push(i64::from_le_bytes(buf8));
        }

        // Read n_tags
        let mut buf2 = [0u8; 2];
        if let Err(e) = self.reader.read_exact(&mut buf2) {
            return Some(Err(e.into()));
        }
        let n_tags = u16::from_le_bytes(buf2) as usize;

        // Read tag IDs
        let mut keys = Vec::with_capacity(n_tags);
        let mut vals = Vec::with_capacity(n_tags);
        for _ in 0..n_tags {
            if let Err(e) = self.reader.read_exact(&mut buf4) {
                return Some(Err(e.into()));
            }
            let k_id = u32::from_le_bytes(buf4);

            if let Err(e) = self.reader.read_exact(&mut buf4) {
                return Some(Err(e.into()));
            }
            let v_id = u32::from_le_bytes(buf4);

            keys.push(k_id);
            vals.push(v_id);
        }

        Some(Ok((way_id, keys, vals, nodes)))
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
