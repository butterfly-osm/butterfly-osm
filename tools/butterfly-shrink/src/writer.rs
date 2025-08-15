//! Production-quality PBF writer inspired by pbf-craft patterns

use crate::config::Config;
use anyhow::{Context, Result};
use byteorder::{BigEndian, WriteBytesExt};
use flate2::{write::ZlibEncoder, Compression};
use protobuf::Message;
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

// Use our own generated protobuf structures
use crate::proto::{fileformat, osmformat};


/// String table builder that manages unique strings and their indices
#[derive(Debug)]
struct StringTableBuilder {
    strings: Vec<String>,
    string_to_index: HashMap<String, u32>,
}

impl StringTableBuilder {
    fn new() -> Self {
        let mut builder = Self {
            strings: Vec::new(),
            string_to_index: HashMap::new(),
        };
        // Index 0 must be empty string per PBF spec
        builder.add_string("");
        builder
    }

    fn add_string(&mut self, s: &str) -> u32 {
        if let Some(&index) = self.string_to_index.get(s) {
            index
        } else {
            let index = self.strings.len() as u32;
            self.strings.push(s.to_string());
            self.string_to_index.insert(s.to_string(), index);
            index
        }
    }

    fn build(self) -> osmformat::StringTable {
        let mut string_table = osmformat::StringTable::new();
        for s in self.strings {
            string_table.s.push(s.into_bytes());
        }
        string_table
    }
}

/// Primitive block builder that accumulates elements and builds PBF blocks
#[derive(Debug)]
struct PrimitiveBuilder {
    string_table: StringTableBuilder,
    nodes: Vec<osmformat::Node>,
    ways: Vec<osmformat::Way>,
    relations: Vec<osmformat::Relation>,
    granularity: i32,
}

impl PrimitiveBuilder {
    fn new(granularity: i32) -> Self {
        Self {
            string_table: StringTableBuilder::new(),
            nodes: Vec::new(),
            ways: Vec::new(),
            relations: Vec::new(),
            granularity,
        }
    }

    fn add_node(&mut self, id: i64, lat: f64, lon: f64, tags: &HashMap<String, String>) {
        let mut node = osmformat::Node::new();
        node.set_id(id);
        
        // Convert coordinates to PBF format (nanodegrees / granularity)
        let lat_pbf = ((lat * 1e9) / self.granularity as f64).round() as i64;
        let lon_pbf = ((lon * 1e9) / self.granularity as f64).round() as i64;
        node.set_lat(lat_pbf);
        node.set_lon(lon_pbf);

        // Add tags
        for (key, value) in tags {
            let key_idx = self.string_table.add_string(key);
            let val_idx = self.string_table.add_string(value);
            node.keys.push(key_idx);
            node.vals.push(val_idx);
        }

        self.nodes.push(node);
    }

    fn add_way(&mut self, id: i64, refs: &[i64], tags: &HashMap<String, String>) {
        let mut way = osmformat::Way::new();
        way.set_id(id);

        // Delta encode node references (critical for PBF format)
        let mut last_ref = 0i64;
        for &node_ref in refs {
            way.refs.push(node_ref - last_ref);
            last_ref = node_ref;
        }

        // Add tags
        for (key, value) in tags {
            let key_idx = self.string_table.add_string(key);
            let val_idx = self.string_table.add_string(value);
            way.keys.push(key_idx);
            way.vals.push(val_idx);
        }

        self.ways.push(way);
    }

    fn add_relation(&mut self, id: i64, members: &[RelationMember], tags: &HashMap<String, String>) {
        let mut relation = osmformat::Relation::new();
        relation.set_id(id);

        // Delta encode member IDs and add members
        let mut last_memid = 0i64;
        for member in members {
            let role_idx = self.string_table.add_string(&member.role);
            relation.roles_sid.push(role_idx as i32);
            
            relation.memids.push(member.member_id - last_memid);
            last_memid = member.member_id;

            let member_type = match member.member_type.as_str() {
                "node" => osmformat::relation::MemberType::NODE,
                "way" => osmformat::relation::MemberType::WAY,
                "relation" => osmformat::relation::MemberType::RELATION,
                _ => osmformat::relation::MemberType::NODE,
            };
            relation.types.push(member_type.into());
        }

        // Add tags
        for (key, value) in tags {
            let key_idx = self.string_table.add_string(key);
            let val_idx = self.string_table.add_string(value);
            relation.keys.push(key_idx);
            relation.vals.push(val_idx);
        }

        self.relations.push(relation);
    }

    fn element_count(&self) -> usize {
        self.nodes.len() + self.ways.len() + self.relations.len()
    }

    fn is_empty(&self) -> bool {
        self.element_count() == 0
    }

    fn build(self) -> osmformat::PrimitiveBlock {
        let mut primitive_block = osmformat::PrimitiveBlock::new();
        primitive_block.stringtable = protobuf::MessageField::some(self.string_table.build());
        primitive_block.set_granularity(self.granularity);
        
        // Create primitive groups by element type
        if !self.nodes.is_empty() {
            let mut group = osmformat::PrimitiveGroup::new();
            group.nodes = self.nodes;
            primitive_block.primitivegroup.push(group);
        }

        if !self.ways.is_empty() {
            let mut group = osmformat::PrimitiveGroup::new();
            group.ways = self.ways;
            primitive_block.primitivegroup.push(group);
        }

        if !self.relations.is_empty() {
            let mut group = osmformat::PrimitiveGroup::new();
            group.relations = self.relations;
            primitive_block.primitivegroup.push(group);
        }

        primitive_block
    }
}

/// Relation member for PBF writing
#[derive(Debug, Clone)]
pub struct RelationMember {
    pub member_type: String, // "node", "way", "relation"
    pub member_id: i64,
    pub role: String,
}

/// Production-quality PBF writer using proven patterns
pub struct PbfWriter {
    writer: BufWriter<File>,
    builder: PrimitiveBuilder,
    granularity: i32,
    compression_level: Compression,
    max_block_elements: usize,
}

impl PbfWriter {
    /// Create a new PBF writer
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        Self::with_config(path, &Config::default())
    }
    
    /// Create a new PBF writer with custom configuration
    pub fn with_config<P: AsRef<Path>>(path: P, config: &Config) -> Result<Self> {
        let file = File::create(path.as_ref())
            .context("Failed to create output file")?;
        let buffer_size = config.pbf_block_size_kb * 1024;
        let writer = BufWriter::with_capacity(buffer_size, file);
        let granularity = 100; // 100 nanodegrees = ~1cm precision
        let builder = PrimitiveBuilder::new(granularity);
        let compression_level = Compression::new(config.zstd_level);
        // Calculate reasonable element count based on block size
        // Assuming ~100 bytes per element average, aim for 70% fill rate before compression
        let max_block_elements = ((config.pbf_block_size_kb * 1024 * 7) / (100 * 10)).max(1000).min(100_000);

        Ok(Self {
            writer,
            builder,
            granularity,
            compression_level,
            max_block_elements,
        })
    }

    /// Write PBF header block
    pub fn write_header(&mut self) -> Result<()> {
        let mut header_block = osmformat::HeaderBlock::new();
        header_block.set_writingprogram("butterfly-shrink v2.0.0".to_string());
        header_block.set_source("OpenStreetMap data processed by butterfly-shrink".to_string());

        self.write_blob("OSMHeader", &header_block)?;
        Ok(())
    }

    /// Write a blob with proper compression and header format
    fn write_blob<M: Message>(&mut self, blob_type: &str, message: &M) -> Result<()> {
        // Serialize the message
        let raw_data = message.write_to_bytes()
            .context("Failed to serialize protobuf message")?;

        // Create compressed blob
        let mut blob = fileformat::Blob::new();
        blob.set_raw_size(raw_data.len() as i32);

        // Compress data using zlib with configured level
        let mut encoder = ZlibEncoder::new(Vec::new(), self.compression_level);
        encoder.write_all(&raw_data)
            .context("Failed to write to zlib encoder")?;
        let compressed_data = encoder.finish()
            .context("Failed to finish zlib compression")?;

        // Use compressed data if it's actually smaller
        if compressed_data.len() < raw_data.len() {
            blob.set_zlib_data(compressed_data);
        } else {
            blob.set_raw(raw_data);
        }

        let blob_data = blob.write_to_bytes()
            .context("Failed to serialize blob")?;

        // Create blob header
        let mut blob_header = fileformat::BlobHeader::new();
        blob_header.set_type(blob_type.to_string());
        blob_header.set_datasize(blob_data.len() as i32);

        let header_data = blob_header.write_to_bytes()
            .context("Failed to serialize blob header")?;

        // Write: header_length (4 bytes big-endian) + header + blob
        self.writer.write_u32::<BigEndian>(header_data.len() as u32)
            .context("Failed to write header length")?;
        self.writer.write_all(&header_data)
            .context("Failed to write header data")?;
        self.writer.write_all(&blob_data)
            .context("Failed to write blob data")?;

        Ok(())
    }

    /// Write accumulated elements as a primitive block
    fn flush_elements(&mut self) -> Result<()> {
        if self.builder.is_empty() {
            return Ok(());
        }

        let element_count = self.builder.element_count();
        log::debug!("Flushing {} elements to primitive block", element_count);

        // Build the primitive block
        let old_builder = std::mem::replace(&mut self.builder, PrimitiveBuilder::new(self.granularity));
        let primitive_block = old_builder.build();

        // Write the block
        self.write_blob("OSMData", &primitive_block)?;

        Ok(())
    }

    /// Write a single node
    pub fn write_node(&mut self, id: i64, lat: f64, lon: f64, tags: &HashMap<String, String>) -> Result<()> {
        self.builder.add_node(id, lat, lon, tags);

        // Flush when we reach the batch limit
        if self.builder.element_count() >= self.max_block_elements {
            self.flush_elements()?;
        }

        Ok(())
    }

    /// Write a single way
    pub fn write_way(&mut self, id: i64, refs: &[i64], tags: &HashMap<String, String>) -> Result<()> {
        self.builder.add_way(id, refs, tags);

        // Flush when we reach the batch limit
        if self.builder.element_count() >= self.max_block_elements {
            self.flush_elements()?;
        }

        Ok(())
    }

    /// Write a single relation
    pub fn write_relation(&mut self, id: i64, members: &[RelationMember], tags: &HashMap<String, String>) -> Result<()> {
        self.builder.add_relation(id, members, tags);

        // Flush when we reach the batch limit
        if self.builder.element_count() >= self.max_block_elements {
            self.flush_elements()?;
        }

        Ok(())
    }

    /// Finalize and close the PBF file
    pub fn finalize(mut self) -> Result<()> {
        // Write any remaining elements
        self.flush_elements()?;

        self.writer.flush()
            .context("Failed to flush PBF file")?;

        log::info!("PBF file writing completed successfully");
        Ok(())
    }
}