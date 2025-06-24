//! Core library modules for butterfly-dl
//!
//! This module contains the internal implementation details of the butterfly-dl library.

pub mod error;
pub mod source;
pub mod stream;
pub mod downloader;

// Re-export main types for internal use
pub use error::{Error, Result};
pub use source::{DownloadSource, SourceConfig, resolve_source, resolve_output_filename};
pub use stream::{DownloadStream, DownloadOptions, ProgressCallback};
pub use downloader::Downloader;