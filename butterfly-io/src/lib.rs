//! Binary formats and I/O infrastructure for butterfly-osm
//!
//! This crate provides:
//! - BFLY binary format headers and chunked I/O
//! - High-performance aligned I/O operations
//! - Compression and checksums (zstd, CRC32, XXH3)

pub mod error;
pub mod format;
pub mod io;

pub use error::{IoError, IoResult};

/// BFLY format version
pub const BFLY_VERSION: u32 = 1;

/// Standard chunk size for I/O operations (4KB aligned)
pub const CHUNK_SIZE: usize = 4096;
