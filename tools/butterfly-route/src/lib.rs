//! Butterfly-Route: High-performance OSM routing engine
//!
//! Step 1: PBF Ingest - Convert OSM PBF to immutable, deterministic artifacts

pub mod formats;
pub mod ingest;
pub mod validate;
pub mod cli;

pub use formats::{NodesFile, WaysFile, RelationsFile};
pub use validate::LockFile;
