//! Core library modules for butterfly-dl
//!
//! This module contains the internal implementation details of the butterfly-dl library.

pub mod downloader;
pub mod source;
pub mod stream;

// Re-export main types for internal use
pub use downloader::Downloader;
pub use source::{resolve_output_filename, SourceConfig};
