//! Arrow IPC Streaming for Matrix Tiles
//!
//! Outputs distance matrices in Apache Arrow IPC format for efficient streaming
//! and consumption by analytics tools (DuckDB, Polars, pyarrow, etc.)
//!
//! ## Tile Schema
//!
//! Each tile contains a block of the distance matrix:
//! ```text
//! src_block_start: u32    // First source index in this tile
//! dst_block_start: u32    // First destination index in this tile
//! src_block_len: u16      // Number of sources in this tile
//! dst_block_len: u16      // Number of destinations in this tile
//! durations_ms: Binary    // Row-major packed u32 distances
//! ```
//!
//! ## Streaming Protocol
//!
//! 1. Writer sends Arrow schema
//! 2. Writer sends record batches as tiles complete
//! 3. Client can cancel via channel drop
//! 4. Backpressure via bounded channel

use std::sync::Arc;
use arrow::array::{ArrayRef, BinaryArray, UInt16Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use bytes::Bytes;

/// A single tile of the distance matrix
#[derive(Debug, Clone)]
pub struct MatrixTile {
    /// First source index in this tile
    pub src_block_start: u32,
    /// First destination index in this tile
    pub dst_block_start: u32,
    /// Number of sources in this tile
    pub src_block_len: u16,
    /// Number of destinations in this tile
    pub dst_block_len: u16,
    /// Row-major packed u32 distances (in milliseconds)
    /// Size = src_block_len × dst_block_len × 4 bytes
    pub durations_ms: Vec<u8>,
}

impl MatrixTile {
    /// Create a new tile from a slice of the distance matrix
    ///
    /// # Arguments
    /// * `src_start` - Starting source index
    /// * `dst_start` - Starting destination index
    /// * `distances` - 2D slice [src_offset..][dst_offset..] row-major u32
    pub fn from_distances(
        src_start: u32,
        dst_start: u32,
        distances: &[Vec<u32>],
    ) -> Self {
        let src_len = distances.len();
        let dst_len = if src_len > 0 { distances[0].len() } else { 0 };

        // Pack distances as bytes (little-endian u32)
        let mut bytes = Vec::with_capacity(src_len * dst_len * 4);
        for row in distances {
            for &d in row {
                bytes.extend_from_slice(&d.to_le_bytes());
            }
        }

        Self {
            src_block_start: src_start,
            dst_block_start: dst_start,
            src_block_len: src_len as u16,
            dst_block_len: dst_len as u16,
            durations_ms: bytes,
        }
    }

    /// Create a tile from a flat row-major matrix slice
    pub fn from_flat(
        src_start: u32,
        dst_start: u32,
        src_len: u16,
        dst_len: u16,
        flat_distances: &[u32],
    ) -> Self {
        assert_eq!(flat_distances.len(), (src_len as usize) * (dst_len as usize));

        let mut bytes = Vec::with_capacity(flat_distances.len() * 4);
        for &d in flat_distances {
            bytes.extend_from_slice(&d.to_le_bytes());
        }

        Self {
            src_block_start: src_start,
            dst_block_start: dst_start,
            src_block_len: src_len,
            dst_block_len: dst_len,
            durations_ms: bytes,
        }
    }
}

/// Arrow schema for matrix tiles
pub fn matrix_tile_schema() -> Schema {
    Schema::new(vec![
        Field::new("src_block_start", DataType::UInt32, false),
        Field::new("dst_block_start", DataType::UInt32, false),
        Field::new("src_block_len", DataType::UInt16, false),
        Field::new("dst_block_len", DataType::UInt16, false),
        Field::new("durations_ms", DataType::Binary, false),
    ])
}

/// Convert a batch of tiles to an Arrow RecordBatch
pub fn tiles_to_record_batch(tiles: &[MatrixTile]) -> anyhow::Result<RecordBatch> {
    let schema = Arc::new(matrix_tile_schema());

    let src_starts: ArrayRef = Arc::new(UInt32Array::from(
        tiles.iter().map(|t| t.src_block_start).collect::<Vec<_>>()
    ));
    let dst_starts: ArrayRef = Arc::new(UInt32Array::from(
        tiles.iter().map(|t| t.dst_block_start).collect::<Vec<_>>()
    ));
    let src_lens: ArrayRef = Arc::new(UInt16Array::from(
        tiles.iter().map(|t| t.src_block_len).collect::<Vec<_>>()
    ));
    let dst_lens: ArrayRef = Arc::new(UInt16Array::from(
        tiles.iter().map(|t| t.dst_block_len).collect::<Vec<_>>()
    ));
    let durations: ArrayRef = Arc::new(BinaryArray::from(
        tiles.iter().map(|t| t.durations_ms.as_slice()).collect::<Vec<_>>()
    ));

    let batch = RecordBatch::try_new(
        schema,
        vec![src_starts, dst_starts, src_lens, dst_lens, durations],
    )?;

    Ok(batch)
}

/// Arrow IPC stream writer for matrix tiles
pub struct ArrowMatrixWriter<W: std::io::Write> {
    writer: StreamWriter<W>,
    tiles_written: usize,
    bytes_written: usize,
}

impl<W: std::io::Write> ArrowMatrixWriter<W> {
    /// Create a new Arrow IPC stream writer
    pub fn new(inner: W) -> anyhow::Result<Self> {
        let schema = Arc::new(matrix_tile_schema());
        let writer = StreamWriter::try_new(inner, &schema)?;

        Ok(Self {
            writer,
            tiles_written: 0,
            bytes_written: 0,
        })
    }

    /// Write a batch of tiles to the stream
    pub fn write_tiles(&mut self, tiles: &[MatrixTile]) -> anyhow::Result<()> {
        if tiles.is_empty() {
            return Ok(());
        }

        let batch = tiles_to_record_batch(tiles)?;
        self.writer.write(&batch)?;

        self.tiles_written += tiles.len();
        for tile in tiles {
            self.bytes_written += tile.durations_ms.len();
        }

        Ok(())
    }

    /// Write a single tile
    pub fn write_tile(&mut self, tile: &MatrixTile) -> anyhow::Result<()> {
        self.write_tiles(&[tile.clone()])
    }

    /// Finish writing and close the stream
    pub fn finish(mut self) -> anyhow::Result<(usize, usize)> {
        self.writer.finish()?;
        Ok((self.tiles_written, self.bytes_written))
    }

    /// Get statistics
    pub fn stats(&self) -> (usize, usize) {
        (self.tiles_written, self.bytes_written)
    }
}

/// Serialize a single RecordBatch to bytes (for HTTP streaming)
pub fn record_batch_to_bytes(batch: &RecordBatch) -> anyhow::Result<Bytes> {
    let mut buf = Vec::new();
    {
        let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())?;
        writer.write(batch)?;
        writer.finish()?;
    }
    Ok(Bytes::from(buf))
}

/// Content type for Arrow IPC stream
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tile_creation() {
        let distances = vec![
            vec![0, 100, 200],
            vec![100, 0, 150],
        ];

        let tile = MatrixTile::from_distances(0, 0, &distances);

        assert_eq!(tile.src_block_start, 0);
        assert_eq!(tile.dst_block_start, 0);
        assert_eq!(tile.src_block_len, 2);
        assert_eq!(tile.dst_block_len, 3);
        assert_eq!(tile.durations_ms.len(), 2 * 3 * 4); // 2×3 u32s
    }

    #[test]
    fn test_arrow_writer() {
        let distances = vec![
            vec![0, 100, 200],
            vec![100, 0, 150],
        ];
        let tile = MatrixTile::from_distances(0, 0, &distances);

        let mut buf = Vec::new();
        let mut writer = ArrowMatrixWriter::new(&mut buf).unwrap();
        writer.write_tile(&tile).unwrap();
        let (tiles, bytes) = writer.finish().unwrap();

        assert_eq!(tiles, 1);
        assert_eq!(bytes, 2 * 3 * 4);
        assert!(!buf.is_empty()); // Arrow IPC bytes written
    }
}
