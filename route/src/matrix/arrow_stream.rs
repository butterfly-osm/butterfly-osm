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
//! This module owns the tile shape and its Arrow encoding only. The
//! stream itself is written by whoever holds the transport: the Flight
//! server builds its own `RecordBatch`es on the tonic side, and the
//! bench harness collects tiles and encodes them here.

use arrow::array::{ArrayRef, BinaryArray, UInt16Array, UInt32Array};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::sync::Arc;

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
        tiles.iter().map(|t| t.src_block_start).collect::<Vec<_>>(),
    ));
    let dst_starts: ArrayRef = Arc::new(UInt32Array::from(
        tiles.iter().map(|t| t.dst_block_start).collect::<Vec<_>>(),
    ));
    let src_lens: ArrayRef = Arc::new(UInt16Array::from(
        tiles.iter().map(|t| t.src_block_len).collect::<Vec<_>>(),
    ));
    let dst_lens: ArrayRef = Arc::new(UInt16Array::from(
        tiles.iter().map(|t| t.dst_block_len).collect::<Vec<_>>(),
    ));
    let durations: ArrayRef = Arc::new(BinaryArray::from(
        tiles
            .iter()
            .map(|t| t.durations_ms.as_slice())
            .collect::<Vec<_>>(),
    ));

    let batch = RecordBatch::try_new(
        schema,
        vec![src_starts, dst_starts, src_lens, dst_lens, durations],
    )?;

    Ok(batch)
}

/// Content type for Arrow IPC stream
pub const ARROW_STREAM_CONTENT_TYPE: &str = "application/vnd.apache.arrow.stream";

#[cfg(test)]
mod tests {
    use super::*;

    /// Field packing and the Arrow encoding in one assertion, on the
    /// surface that ships: `bench/main.rs` builds a `MatrixTile`
    /// literally and hands it to `tiles_to_record_batch`.
    #[test]
    fn tiles_to_record_batch_packs_tile_fields() {
        // 2 sources x 3 destinations, row-major little-endian u32.
        let flat: [u32; 6] = [0, 100, 200, 100, 0, 150];
        let mut durations_ms = Vec::with_capacity(flat.len() * 4);
        for d in flat {
            durations_ms.extend_from_slice(&d.to_le_bytes());
        }
        let tile = MatrixTile {
            src_block_start: 7,
            dst_block_start: 11,
            src_block_len: 2,
            dst_block_len: 3,
            durations_ms,
        };
        assert_eq!(tile.durations_ms.len(), 2 * 3 * 4);

        let batch = tiles_to_record_batch(std::slice::from_ref(&tile)).expect("record batch");
        assert_eq!(batch.schema().as_ref(), &matrix_tile_schema());
        assert_eq!(batch.num_rows(), 1);

        let col_u32 = |name: &str| -> u32 {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
                .expect("u32 column")
                .value(0)
        };
        let col_u16 = |name: &str| -> u16 {
            batch
                .column_by_name(name)
                .and_then(|c| c.as_any().downcast_ref::<UInt16Array>())
                .expect("u16 column")
                .value(0)
        };
        assert_eq!(col_u32("src_block_start"), 7);
        assert_eq!(col_u32("dst_block_start"), 11);
        assert_eq!(col_u16("src_block_len"), 2);
        assert_eq!(col_u16("dst_block_len"), 3);

        let bytes = batch
            .column_by_name("durations_ms")
            .and_then(|c| c.as_any().downcast_ref::<BinaryArray>())
            .expect("binary column")
            .value(0);
        assert_eq!(bytes, tile.durations_ms.as_slice());
    }
}
