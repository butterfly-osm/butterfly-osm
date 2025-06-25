//! Streaming implementations for butterfly-dl
//!
//! Provides AsyncRead implementations for different download sources.

use std::pin::Pin;
use std::task::{Context, Poll};
use std::sync::Arc;
use tokio::io::{AsyncRead, ReadBuf};
use futures::TryStreamExt;

/// A unified stream for HTTP sources
pub enum DownloadStream {
    /// HTTP stream using reqwest
    Http(Box<dyn AsyncRead + Send + Unpin>),
}

impl AsyncRead for DownloadStream {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        match &mut *self {
            DownloadStream::Http(stream) => Pin::new(stream).poll_read(cx, buf),
        }
    }
}

/// Progress callback function type
pub type ProgressCallback = Arc<dyn Fn(u64, u64) + Send + Sync>;

/// Overwrite behavior for existing files
#[derive(Debug, Clone, PartialEq)]
pub enum OverwriteBehavior {
    /// Prompt user for confirmation (default)
    Prompt,
    /// Force overwrite without prompting
    Force,
    /// Never overwrite, fail if file exists
    NeverOverwrite,
}

impl Default for OverwriteBehavior {
    fn default() -> Self {
        Self::Prompt
    }
}

/// Options for download operations
pub struct DownloadOptions {
    /// Optional progress callback
    pub progress: Option<ProgressCallback>,
    
    /// Buffer size for streaming operations
    pub buffer_size: usize,
    
    /// Maximum number of parallel connections for HTTP downloads
    pub max_connections: usize,
    
    /// Behavior when destination file already exists
    pub overwrite: OverwriteBehavior,
}

impl Default for DownloadOptions {
    fn default() -> Self {
        Self {
            progress: None,
            buffer_size: 64 * 1024, // 64KB
            max_connections: 16,
            overwrite: OverwriteBehavior::default(),
        }
    }
}

/// Creates a DownloadStream from an HTTP response
pub fn create_http_stream(response: reqwest::Response) -> DownloadStream {
    let stream = Box::new(tokio_util::io::StreamReader::new(
        response.bytes_stream().map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    ));
    DownloadStream::Http(stream)
}

