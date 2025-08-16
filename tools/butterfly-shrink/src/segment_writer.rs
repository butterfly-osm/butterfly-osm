//! Segment-based PBF writer that avoids the append() problem
//! 
//! Writes nodes, ways, and relations to separate segment files,
//! then assembles them into a final PBF with proper structure.

use std::fs::File;
use std::io::{self, Write, BufWriter, BufReader, Read};
use std::path::{Path, PathBuf};
use byteorder::{BigEndian, WriteBytesExt};
use flate2::write::ZlibEncoder;
use flate2::Compression;
use anyhow::{Result, Context};
use protobuf::Message;
use crate::proto::{fileformat, osmformat};

/// Trait for writing PBF blocks to different sinks
pub trait BlockSink {
    fn write_blob(&mut self, blob_type: &str, blob_data: &[u8]) -> Result<()>;
}

/// Writes raw OSMData blocks to a segment file (no header)
pub struct SegmentSink {
    file: BufWriter<File>,
    bytes_written: u64,
}

impl SegmentSink {
    pub fn new(path: &Path) -> Result<Self> {
        let file = File::create(path)
            .with_context(|| format!("Failed to create segment file: {:?}", path))?;
        Ok(Self {
            file: BufWriter::with_capacity(8_000_000, file),
            bytes_written: 0,
        })
    }
    
    pub fn bytes_written(&self) -> u64 {
        self.bytes_written
    }
    
    pub fn flush(&mut self) -> Result<()> {
        self.file.flush().context("Failed to flush segment file")
    }
}

impl BlockSink for SegmentSink {
    fn write_blob(&mut self, blob_type: &str, blob_data: &[u8]) -> Result<()> {
        // Only accept OSMData blocks for segments
        if blob_type != "OSMData" {
            return Ok(()); // Silently skip headers in segments
        }
        
        // Create Blob wrapper with zlib compression
        let mut blob = fileformat::Blob::new();
        blob.set_raw_size(blob_data.len() as i32);
        
        let mut encoder = ZlibEncoder::new(Vec::new(), Compression::new(6));
        encoder.write_all(blob_data)?;
        let compressed = encoder.finish()?;
        blob.set_zlib_data(compressed);
        
        // Create BlobHeader
        let mut header = fileformat::BlobHeader::new();
        header.set_field_type(blob_type.to_string());
        header.set_datasize(blob.compute_size() as i32);
        
        // Encode header and blob
        let header_data = header.write_to_bytes()?;
        let blob_data = blob.write_to_bytes()?;
        
        // Write: header_length (4 bytes BE) + header + blob
        self.file.write_u32::<BigEndian>(header_data.len() as u32)?;
        self.file.write_all(&header_data)?;
        self.file.write_all(&blob_data)?;
        
        self.bytes_written += 4 + header_data.len() as u64 + blob_data.len() as u64;
        Ok(())
    }
}

/// Assembles segment files into a final PBF
pub struct PbfAssembler {
    temp_dir: PathBuf,
}

impl PbfAssembler {
    pub fn new(temp_dir: PathBuf) -> Self {
        Self { temp_dir }
    }
    
    /// Assemble segments into final PBF
    pub fn assemble(
        &self,
        output_path: &Path,
        bbox: Option<(f64, f64, f64, f64)>, // (min_lat, min_lon, max_lat, max_lon)
    ) -> Result<()> {
        log::info!("Assembling PBF from segments");
        
        let nodes_seg = self.temp_dir.join("nodes.seg");
        let ways_seg = self.temp_dir.join("ways.seg");
        let rels_seg = self.temp_dir.join("rels.seg");
        
        // Create temp output file
        let temp_output = output_path.with_extension("tmp.pbf");
        let mut output = BufWriter::with_capacity(
            16_000_000,
            File::create(&temp_output)?
        );
        
        // Write HeaderBlock
        self.write_header(&mut output, bbox)?;
        
        // Copy segments in order
        let mut copied = 0u64;
        
        if nodes_seg.exists() {
            log::info!("Copying nodes segment");
            copied += self.copy_segment(&nodes_seg, &mut output)?;
        }
        
        if ways_seg.exists() {
            log::info!("Copying ways segment");
            copied += self.copy_segment(&ways_seg, &mut output)?;
        }
        
        if rels_seg.exists() {
            log::info!("Copying relations segment");
            copied += self.copy_segment(&rels_seg, &mut output)?;
        }
        
        output.flush()?;
        drop(output);
        
        // Atomic rename
        std::fs::rename(&temp_output, output_path)
            .context("Failed to rename temp file to final output")?;
        
        log::info!("Assembly complete: {} bytes written", copied);
        Ok(())
    }
    
    /// Write PBF header block
    fn write_header(
        &self,
        output: &mut BufWriter<File>,
        bbox: Option<(f64, f64, f64, f64)>,
    ) -> Result<()> {
        let mut header_block = osmformat::HeaderBlock::new();
        
        // Set required features
        header_block.mut_required_features().push("OsmSchema-V0.6".to_string());
        header_block.mut_required_features().push("DenseNodes".to_string());
        
        // Set bbox if provided
        if let Some((min_lat, min_lon, max_lat, max_lon)) = bbox {
            let mut bbox = osmformat::HeaderBBox::new();
            bbox.set_left((min_lon * 1e9) as i64);
            bbox.set_right((max_lon * 1e9) as i64);
            bbox.set_top((max_lat * 1e9) as i64);
            bbox.set_bottom((min_lat * 1e9) as i64);
            header_block.set_bbox(bbox);
        }
        
        // Set generator
        header_block.set_writingprogram("butterfly-shrink v2.0.0".to_string());
        
        // Encode and write
        let header_data = header_block.write_to_bytes()?;
        
        // Create blob with header
        let mut blob = fileformat::Blob::new();
        blob.set_raw_size(header_data.len() as i32);
        blob.set_raw(header_data);
        
        // Create blob header
        let mut blob_header = fileformat::BlobHeader::new();
        blob_header.set_field_type("OSMHeader".to_string());
        blob_header.set_datasize(blob.compute_size() as i32);
        
        // Write to output
        let header_bytes = blob_header.write_to_bytes()?;
        let blob_bytes = blob.write_to_bytes()?;
        
        output.write_u32::<BigEndian>(header_bytes.len() as u32)?;
        output.write_all(&header_bytes)?;
        output.write_all(&blob_bytes)?;
        
        Ok(())
    }
    
    /// Copy a segment file to output
    fn copy_segment(&self, segment_path: &Path, output: &mut BufWriter<File>) -> Result<u64> {
        let mut input = BufReader::with_capacity(8_000_000, File::open(segment_path)?);
        let mut buffer = vec![0u8; 1_000_000];
        let mut total = 0u64;
        
        loop {
            let n = input.read(&mut buffer)?;
            if n == 0 {
                break;
            }
            output.write_all(&buffer[..n])?;
            total += n as u64;
        }
        
        Ok(total)
    }
}