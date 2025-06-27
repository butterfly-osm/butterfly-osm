//! # Butterfly-dl Library
//!
//! A high-performance, memory-efficient library for downloading OpenStreetMap data files
//! with intelligent source routing and minimal memory usage.
//!
//! ## Features
//!
//! - **Smart source routing**: S3 for planet files, HTTP for regional extracts
//! - **Memory efficient**: <1GB RAM usage regardless of file size
//! - **Streaming support**: Download directly to any AsyncWrite destination
//! - **Progress tracking**: Optional progress callbacks for custom UIs
//! - **Feature flags**: Optional S3 support to minimize dependencies
//!
//! ## Basic Usage
//!
//! ```rust,no_run
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Download to file with auto-generated filename
//!     butterfly_dl::get("europe/belgium", None).await?;
//!     
//!     // Download to specific file
//!     butterfly_dl::get("planet", Some("./planet.pbf")).await?;
//!     
//!     // Stream download
//!     let mut stream = butterfly_dl::get_stream("europe/monaco").await?;
//!     // Use stream with any AsyncRead-compatible code
//!     
//!     Ok(())
//! }
//! ```
//!
//! ## Progress Tracking
//!
//! ```rust,no_run
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     butterfly_dl::get_with_progress(
//!         "europe/belgium",
//!         Some("belgium.pbf"),
//!         |downloaded, total| {
//!             println!("Progress: {}/{} bytes", downloaded, total);
//!         }
//!     ).await?;
//!     
//!     Ok(())
//! }
//! ```

use std::sync::Arc;
use tokio::io::AsyncRead;

// Re-export core types that users might need
pub use crate::core::stream::{DownloadOptions, OverwriteBehavior};
pub use butterfly_common::{Error, Result};

// Internal modules
mod core;

// C-compatible FFI bindings (optional)
#[cfg(feature = "c-bindings")]
pub mod ffi;

/// Download a file to a destination
///
/// # Arguments
/// * `source` - Source identifier (e.g., "planet", "europe", "europe/belgium")
/// * `dest` - Optional destination file path. If None, auto-generates filename
///
/// # Examples
/// ```rust,no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// // Download with auto-generated filename
/// butterfly_dl::get("europe/belgium", None).await?;
///
/// // Download to specific file
/// butterfly_dl::get("planet", Some("./my-planet.pbf")).await?;
/// # Ok(())
/// # }
/// ```
pub async fn get(source: &str, dest: Option<&str>) -> Result<()> {
    let downloader = core::Downloader::new();
    let options = DownloadOptions::default();

    let file_path = match dest {
        Some(path) => path.to_string(),
        None => core::resolve_output_filename(source),
    };

    downloader
        .download_to_file(source, &file_path, &options)
        .await
}

/// Download and return a stream
///
/// Returns an AsyncRead stream that can be used with any compatible code.
/// The stream implements AsyncRead + Send + Unpin.
///
/// # Arguments
/// * `source` - Source identifier (e.g., "planet", "europe", "europe/belgium")
///
/// # Returns
/// * `impl AsyncRead + Send + Unpin` - Stream of the downloaded data
///
/// # Examples
/// ```rust,no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// use tokio::io::AsyncReadExt;
/// 
/// let mut stream = butterfly_dl::get_stream("europe/monaco").await?;
/// let mut buffer = Vec::new();
/// stream.read_to_end(&mut buffer).await?;
/// println!("Downloaded {} bytes", buffer.len());
/// # Ok(())
/// # }
/// ```
pub async fn get_stream(source: &str) -> Result<impl AsyncRead + Send + Unpin> {
    let downloader = core::Downloader::new();
    let options = DownloadOptions::default();

    let (stream, _total_size) = downloader.download_stream(source, &options).await?;
    Ok(stream)
}

/// Download with progress tracking
///
/// Downloads a file with a progress callback that receives (downloaded_bytes, total_bytes).
///
/// # Arguments
/// * `source` - Source identifier (e.g., "planet", "europe", "europe/belgium")
/// * `dest` - Optional destination file path. If None, auto-generates filename
/// * `progress` - Callback function that receives (downloaded, total) bytes
///
/// # Examples
/// ```rust,no_run
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// butterfly_dl::get_with_progress(
///     "europe/belgium",
///     Some("belgium.pbf"),
///     |downloaded, total| {
///         let percent = (downloaded as f64 / total as f64) * 100.0;
///         println!("Progress: {:.1}%", percent);
///     }
/// ).await?;
/// # Ok(())
/// # }
/// ```
pub async fn get_with_progress<F>(source: &str, dest: Option<&str>, progress: F) -> Result<()>
where
    F: Fn(u64, u64) + Send + Sync + 'static,
{
    let downloader = core::Downloader::new();
    let options = DownloadOptions {
        progress: Some(Arc::new(progress)),
        ..Default::default()
    };

    let file_path = match dest {
        Some(path) => path.to_string(),
        None => core::resolve_output_filename(source),
    };

    downloader
        .download_to_file(source, &file_path, &options)
        .await
}

/// Download with custom options
///
/// Provides full control over download options including buffer size,
/// connection limits, and progress tracking.
///
/// # Arguments
/// * `source` - Source identifier
/// * `dest` - Optional destination file path
/// * `options` - Download options
///
/// # Examples
/// ```rust,no_run
/// use butterfly_dl::{DownloadOptions, OverwriteBehavior};
/// use std::sync::Arc;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let options = DownloadOptions {
///     buffer_size: 128 * 1024, // 128KB buffer
///     max_connections: 8,      // Limit to 8 connections
///     overwrite: OverwriteBehavior::Force, // Overwrite without prompting
///     progress: Some(Arc::new(|downloaded, total| {
///         println!("Downloaded: {} / {}", downloaded, total);
///     })),
/// };
///
/// butterfly_dl::get_with_options("europe/belgium", None, options).await?;
/// # Ok(())
/// # }
/// ```
pub async fn get_with_options(
    source: &str,
    dest: Option<&str>,
    options: DownloadOptions,
) -> Result<()> {
    let downloader = core::Downloader::new();

    let file_path = match dest {
        Some(path) => path.to_string(),
        None => core::resolve_output_filename(source),
    };

    downloader
        .download_to_file(source, &file_path, &options)
        .await
}

/// Advanced API: Create a downloader with custom configuration
///
/// For advanced users who need to customize source URLs, S3 buckets, etc.
///
/// # Examples
/// ```rust,no_run
/// use butterfly_dl::{Downloader, SourceConfig};
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
/// let config = SourceConfig {
///     planet_http_url: "https://my-custom-mirror.org/planet.pbf".to_string(),
///     ..Default::default()
/// };
///
/// let downloader = Downloader::with_config(config);
/// // Use downloader methods...
/// # Ok(())
/// # }
/// ```
pub use core::{Downloader, SourceConfig};

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn test_get_with_tempfile() {
        let dir = tempdir().unwrap();
        let _file_path = dir.path().join("test.pbf");

        // This would fail in real test without network, but validates the API
        // get("europe/monaco", Some(file_path.to_str().unwrap())).await.unwrap();
    }

    #[test]
    fn test_resolve_output_filename() {
        assert_eq!(
            core::resolve_output_filename("planet"),
            "planet-latest.osm.pbf"
        );
        assert_eq!(
            core::resolve_output_filename("europe"),
            "europe-latest.osm.pbf"
        );
        assert_eq!(
            core::resolve_output_filename("europe/belgium"),
            "belgium-latest.osm.pbf"
        );
    }
}
