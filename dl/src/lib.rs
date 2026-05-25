//! # Butterfly-dl Library
//!
//! A high-performance, memory-efficient library for downloading OpenStreetMap data files
//! with intelligent source routing and minimal memory usage.
//!
//! ## Features
//!
//! - **Smart source routing**: via Geofabrik HTTP mirrors
//! - **Memory efficient**: <1GB RAM usage regardless of file size
//! - **Streaming support**: Download directly to any AsyncWrite destination
//! - **Progress tracking**: Optional progress callbacks for custom UIs
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
//! use butterfly_dl::DownloadOptions;
//! use std::sync::Arc;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     butterfly_dl::get_with_options(
//!         "europe/belgium",
//!         Some("belgium.pbf"),
//!         DownloadOptions {
//!             progress: Some(Arc::new(|downloaded, total| {
//!                 println!("Progress: {}/{} bytes", downloaded, total);
//!             })),
//!             ..Default::default()
//!         },
//!     ).await?;
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

/// Wrap a user-supplied progress callback so the library guarantees the
/// contract documented on `get_with_options`: monotonic, clamped to
/// `total`, single terminal call. Implemented via an `AtomicU64` for
/// the last-reported tally and an `AtomicBool` to suppress duplicate
/// terminal calls (#136).
fn clamp_progress_arc(
    callback: Arc<dyn Fn(u64, u64) + Send + Sync>,
) -> Arc<dyn Fn(u64, u64) + Send + Sync> {
    use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
    let last_reported = Arc::new(AtomicU64::new(0));
    let terminal_called = Arc::new(AtomicBool::new(false));
    Arc::new(move |downloaded, total| {
        let clamped = downloaded.min(total);
        let prev = last_reported.fetch_max(clamped, Ordering::Relaxed);
        let monotone = clamped.max(prev);
        if monotone == total && total > 0 && terminal_called.swap(true, Ordering::Relaxed) {
            return;
        }
        callback(monotone, total);
    })
}

/// Generic verified download primitive (#100). Consolidates HTTP
/// download logic that was previously duplicated across
/// `butterfly-dl` and `butterfly-route::transit::feeds`. Library
/// consumers call [`verified::download_verified`] with a
/// [`verified::VerifiedOptions`] bag; every verification default
/// (magic prefix, min bytes, sidecar) is filled automatically from
/// the target path's extension via
/// [`verified::VerifiedOptions::for_extension`].
pub mod verified;

/// Region-indexed parallel downloads (#100). One TOML per region
/// (`dl/regions/<name>.toml`) enumerates every file the region's
/// routing deployment needs; [`regions::fetch_region`] dispatches
/// one verified download per entry concurrently.
pub mod regions;

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
        .await?;
    maybe_write_sidecar(file_path.clone()).await;
    Ok(())
}

/// #230: write the `.sha256` sidecar when the destination has a
/// recognised extension. The parallel HTTP downloader doesn't compute
/// SHA inline (it would force single-connection streaming and lose
/// the range-parallelism win on big PBFs), so we do a second pass
/// over the just-written file. The page cache is hot — for a 6 GiB
/// file this is a ~2-second CPU-bound hash with no disk I/O.
/// Recognised extensions come from
/// [`verified::VerifiedOptions::for_extension`] (PBF, ZIP, GZ, XZ,
/// ZST, XML).
///
/// The hash + write run inside `tokio::task::spawn_blocking` so they
/// don't pin the async executor's I/O thread for the seconds it takes
/// on multi-GiB files. On a single-thread runtime that would freeze
/// the whole runtime; on a multi-thread runtime it would tie up one
/// worker. `spawn_blocking` parks the work on the dedicated blocking
/// pool where this kind of CPU+disk-bound task belongs.
async fn maybe_write_sidecar(file_path: String) {
    let res = tokio::task::spawn_blocking(move || -> std::io::Result<()> {
        let target = std::path::Path::new(&file_path);
        if !verified::VerifiedOptions::for_extension(target).sha256_sidecar {
            return Ok(());
        }
        let Some(sha) = verified::hash_file_if_exists(target) else {
            return Ok(());
        };
        if let Err(e) = verified::write_sidecar(target, sha) {
            // Sidecar write failure is non-fatal — the file is already
            // on disk. Warn so deployment pipelines that rely on the
            // sidecar can detect the issue.
            eprintln!("⚠ failed to write .sha256 sidecar: {e}");
        }
        Ok(())
    })
    .await;
    if let Err(join_err) = res {
        eprintln!("⚠ sidecar task join error: {join_err}");
    }
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
/// use tokio::io::AsyncReadExt;
///
/// # #[tokio::main]
/// # async fn main() -> Result<(), Box<dyn std::error::Error>> {
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

/// Download with custom options
///
/// Provides full control over download options including buffer size,
/// connection limits, and progress tracking.
///
/// # Progress callback contract
///
/// When `options.progress` is `Some(callback)`, the library wraps the
/// callback so consumers can rely on the following invariants:
///
/// - `total` is the size announced by the server in `Content-Length` and
///   does not change across calls within a single download.
/// - `downloaded` is monotonically non-decreasing across calls.
/// - `downloaded` is clamped so `downloaded <= total` is guaranteed.
/// - On successful completion, the callback is called with
///   `downloaded == total` **exactly once** before `get_with_options`
///   returns; duplicate terminal calls are suppressed by the wrapper.
/// - On error, no terminal call is guaranteed; the callback may stop
///   being invoked mid-download.
///
/// Callbacks should be cheap (they run on the I/O hot path) and must
/// not block — heavy work (UI repaint, IPC, network calls) should be
/// marshalled to a separate task.
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
    mut options: DownloadOptions,
) -> Result<()> {
    let downloader = core::Downloader::new();

    // Wrap any user-supplied progress callback so the documented
    // contract (monotonic, clamped, single-terminal) is enforced
    // by the library rather than left to caller discipline.
    if let Some(cb) = options.progress.take() {
        options.progress = Some(clamp_progress_arc(cb));
    }

    let file_path = match dest {
        Some(path) => path.to_string(),
        None => core::resolve_output_filename(source),
    };

    downloader
        .download_to_file(source, &file_path, &options)
        .await?;
    maybe_write_sidecar(file_path).await;
    Ok(())
}

/// Advanced API: Create a downloader with custom configuration
///
/// For advanced users who need to customize source URLs, mirror configuration, etc.
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
