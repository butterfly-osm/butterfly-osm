//! Binary formats and I/O infrastructure for butterfly-osm
//!
//! This crate provides:
//! - BFLY binary format headers and chunked I/O
//! - High-performance aligned I/O operations
//! - Compression and checksums (zstd, CRC32, XXH3)

pub mod compression;
pub mod error;
pub mod external_sort;
pub mod format;
pub mod io;
pub mod loser_tree;
pub mod roundtrip;
pub mod token_bucket;

pub use compression::{ChunkSizeAuditor, CompressedWriter, CompressedReader, ChunkEntry};
pub use error::{IoError, IoResult};
pub use external_sort::{ExternalSorter, MemoryThrottledSorter, RssMonitor, SortedIterator};
pub use format::BflyHeader;
pub use io::AlignedIo;
pub use loser_tree::{LoserTree, LoserTreeEntry};
pub use token_bucket::{TokenBucket, WorkerAdmissionController, AdmissionStats};

/// BFLY format version
pub const BFLY_VERSION: u32 = 1;

/// Standard chunk size for I/O operations (4KB aligned)
pub const CHUNK_SIZE: usize = 4096;
