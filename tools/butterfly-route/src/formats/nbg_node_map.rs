///! nbg.node_map format - OSM node ID to compact node ID mapping

use anyhow::Result;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Read, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4E42474D; // "NBGM"
const VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct NodeMapping {
    pub osm_node_id: i64,
    pub compact_id: u32,
}

pub struct NbgNodeMap {
    pub mappings: Vec<NodeMapping>,
}

pub struct NbgNodeMapFile;

impl NbgNodeMapFile {
    /// Write node map to file
    pub fn write<P: AsRef<Path>>(path: P, node_map: &NbgNodeMap) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Header
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let count = node_map.mappings.len() as u64;
        let count_bytes = count.to_le_bytes();

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&count_bytes)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&count_bytes);

        // Body (sorted by OSM node id)
        for mapping in &node_map.mappings {
            let osm_bytes = mapping.osm_node_id.to_le_bytes();
            let compact_bytes = mapping.compact_id.to_le_bytes();

            writer.write_all(&osm_bytes)?;
            writer.write_all(&compact_bytes)?;

            crc_digest.update(&osm_bytes);
            crc_digest.update(&compact_bytes);
        }

        // Footer
        let body_crc = crc_digest.finalize();
        let file_crc = body_crc;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read node map from file and build lookup HashMap
    pub fn read<P: AsRef<Path>>(path: P) -> Result<HashMap<i64, u32>> {
        let mut file = File::open(path)?;
        let mut header = [0u8; 16];
        file.read_exact(&mut header)?;

        let count = u64::from_le_bytes(header[8..16].try_into()?);

        let mut map = HashMap::with_capacity(count as usize);
        for _ in 0..count {
            let mut osm_bytes = [0u8; 8];
            let mut compact_bytes = [0u8; 4];
            file.read_exact(&mut osm_bytes)?;
            file.read_exact(&mut compact_bytes)?;

            let osm_id = i64::from_le_bytes(osm_bytes);
            let compact_id = u32::from_le_bytes(compact_bytes);
            map.insert(osm_id, compact_id);
        }

        Ok(map)
    }

    /// Read node map from file as NbgNodeMap struct
    pub fn read_map<P: AsRef<Path>>(path: P) -> Result<NbgNodeMap> {
        let mut file = File::open(path)?;
        let mut header = [0u8; 16];
        file.read_exact(&mut header)?;

        let count = u64::from_le_bytes(header[8..16].try_into()?);

        let mut mappings = Vec::with_capacity(count as usize);
        for compact_id in 0..count {
            let mut osm_bytes = [0u8; 8];
            let mut _compact_bytes = [0u8; 4];
            file.read_exact(&mut osm_bytes)?;
            file.read_exact(&mut _compact_bytes)?;

            let osm_node_id = i64::from_le_bytes(osm_bytes);
            mappings.push(NodeMapping {
                osm_node_id,
                compact_id: compact_id as u32,
            });
        }

        Ok(NbgNodeMap { mappings })
    }
}
