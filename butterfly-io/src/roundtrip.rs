//! Round-trip tests for BFLY format

use crate::{BflyHeader, CompressedWriter, CompressedReader, IoResult};
use std::io::Cursor;

/// Test complete round-trip: header + compressed chunks + TOC
pub fn test_roundtrip(chunks: &[&[u8]]) -> IoResult<Vec<Vec<u8>>> {
    // Step 1: Create header with payload hash
    let total_payload: Vec<u8> = chunks.iter().flat_map(|c| c.iter()).copied().collect();
    let payload_hash = xxhash_rust::xxh3::xxh3_64(&total_payload);
    let header = BflyHeader::new(total_payload.len() as u64, payload_hash);
    
    // Step 2: Write everything to buffer
    let mut buffer = Vec::new();
    
    // Write header
    buffer.extend_from_slice(&header.to_bytes());
    
    // Write compressed chunks
    let cursor = Cursor::new(Vec::new());
    let mut writer = CompressedWriter::new(cursor, crate::CHUNK_SIZE);
    
    for chunk in chunks {
        writer.write_chunk(chunk)?;
    }
    
    let (cursor, toc) = writer.finish()?;
    let compressed_data = cursor.into_inner();
    buffer.extend_from_slice(&compressed_data);
    
    // Write TOC at end
    let toc_bytes = serialize_toc(&toc);
    buffer.extend_from_slice(&toc_bytes);
    
    // Step 3: Read everything back
    let mut cursor = Cursor::new(&buffer[..]);
    
    // Read header
    let mut header_bytes = [0u8; 32];
    std::io::Read::read_exact(&mut cursor, &mut header_bytes)?;
    let read_header = BflyHeader::from_bytes(&header_bytes)?;
    
    // Verify header matches
    assert_eq!(header.payload_size, read_header.payload_size);
    assert_eq!(header.payload_hash, read_header.payload_hash);
    
    // Read compressed data
    let compressed_size = compressed_data.len();
    let mut compressed_buf = vec![0u8; compressed_size];
    std::io::Read::read_exact(&mut cursor, &mut compressed_buf)?;
    
    // Read TOC
    let mut toc_buf = vec![0u8; toc_bytes.len()];
    std::io::Read::read_exact(&mut cursor, &mut toc_buf)?;
    let read_toc = deserialize_toc(&toc_buf)?;
    
    // Decompress chunks
    let mut reader = CompressedReader::new(Cursor::new(compressed_buf), read_toc);
    let mut result = Vec::new();
    
    for i in 0..chunks.len() {
        let chunk = reader.read_chunk(i)?;
        result.push(chunk);
    }
    
    Ok(result)
}

fn serialize_toc(toc: &[crate::ChunkEntry]) -> Vec<u8> {
    let mut bytes = Vec::new();
    bytes.extend_from_slice(&(toc.len() as u32).to_le_bytes());
    
    for entry in toc {
        bytes.extend_from_slice(&entry.offset.to_le_bytes());
        bytes.extend_from_slice(&entry.compressed_size.to_le_bytes());
        bytes.extend_from_slice(&entry.uncompressed_size.to_le_bytes());
        bytes.extend_from_slice(&entry.checksum.to_le_bytes());
    }
    
    bytes
}

fn deserialize_toc(bytes: &[u8]) -> IoResult<Vec<crate::ChunkEntry>> {
    if bytes.len() < 4 {
        return Err(crate::IoError::InvalidFormat("TOC too short".to_string()));
    }
    
    let count = u32::from_le_bytes([bytes[0], bytes[1], bytes[2], bytes[3]]) as usize;
    let expected_len = 4 + count * 20; // 4 bytes count + 20 bytes per entry
    
    if bytes.len() != expected_len {
        return Err(crate::IoError::InvalidFormat("Invalid TOC length".to_string()));
    }
    
    let mut entries = Vec::new();
    let mut offset = 4;
    
    for _ in 0..count {
        let entry_offset = u64::from_le_bytes([
            bytes[offset], bytes[offset+1], bytes[offset+2], bytes[offset+3],
            bytes[offset+4], bytes[offset+5], bytes[offset+6], bytes[offset+7]
        ]);
        let compressed_size = u32::from_le_bytes([
            bytes[offset+8], bytes[offset+9], bytes[offset+10], bytes[offset+11]
        ]);
        let uncompressed_size = u32::from_le_bytes([
            bytes[offset+12], bytes[offset+13], bytes[offset+14], bytes[offset+15]
        ]);
        let checksum = u32::from_le_bytes([
            bytes[offset+16], bytes[offset+17], bytes[offset+18], bytes[offset+19]
        ]);
        
        entries.push(crate::ChunkEntry {
            offset: entry_offset,
            compressed_size,
            uncompressed_size,
            checksum,
        });
        
        offset += 20;
    }
    
    Ok(entries)
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_roundtrip() {
        let chunks = vec![
            b"Hello, world!".as_slice(),
            b"This is a test chunk with more data.".as_slice(),
            b"Final chunk".as_slice(),
        ];
        
        let result = test_roundtrip(&chunks).expect("Round-trip failed");
        
        assert_eq!(result.len(), chunks.len());
        for (original, decoded) in chunks.iter().zip(result.iter()) {
            assert_eq!(*original, decoded.as_slice());
        }
    }
    
    #[test]
    fn test_large_chunk_roundtrip() {
        // Create a large chunk to test compression
        let large_data = vec![42u8; 10000];
        let chunks = vec![large_data.as_slice()];
        
        let result = test_roundtrip(&chunks).expect("Large chunk round-trip failed");
        
        assert_eq!(result.len(), 1);
        assert_eq!(result[0], large_data);
    }
}