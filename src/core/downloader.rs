//! Core download functionality for butterfly-dl
//!
//! Provides high-performance, memory-efficient download implementations for S3 and HTTP sources.

use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncWrite, AsyncWriteExt, AsyncReadExt};
use futures::StreamExt;
use futures::TryStreamExt;
use reqwest::{Client, ClientBuilder};
use once_cell::sync::Lazy;

use crate::core::error::{Error, Result};
use crate::core::source::{DownloadSource, SourceConfig};
use crate::core::stream::{DownloadOptions, DownloadStream, create_http_stream};

#[cfg(feature = "s3")]
use aws_config::BehaviorVersion;
#[cfg(feature = "s3")]
use aws_sdk_s3::Client as S3Client;
#[cfg(feature = "s3")]
use crate::core::stream::create_s3_stream;

// Constants removed - unused in current implementation

/// Global HTTP client with optimizations
static GLOBAL_CLIENT: Lazy<Client> = Lazy::new(|| {
    ClientBuilder::new()
        .tcp_keepalive(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(20)
        .user_agent("butterfly-dl/0.1.0")
        .build()
        .expect("Failed to create HTTP client")
});

/// High-level downloader that handles all source types
pub struct Downloader {
    config: SourceConfig,
}

impl Default for Downloader {
    fn default() -> Self {
        Self::new()
    }
}

impl Downloader {
    /// Create a new downloader with default configuration
    pub fn new() -> Self {
        Self {
            config: SourceConfig::default(),
        }
    }

    /// Create a new downloader with custom configuration
    pub fn with_config(config: SourceConfig) -> Self {
        Self { config }
    }

    /// Download to a file destination
    pub async fn download_to_file(
        &self,
        source: &str,
        file_path: &str,
        options: &DownloadOptions,
    ) -> Result<()> {
        let download_source = crate::core::source::resolve_source(source, &self.config)?;
        
        match download_source {
            #[cfg(feature = "s3")]
            DownloadSource::S3 { bucket, key, region } => {
                self.download_s3_to_file(&bucket, &key, &region, file_path, options).await
            }
            DownloadSource::Http { url } => {
                self.download_http_to_file(&url, file_path, options).await
            }
        }
    }

    /// Download and return a stream
    pub async fn download_stream(
        &self,
        source: &str,
        options: &DownloadOptions,
    ) -> Result<(DownloadStream, u64)> {
        let download_source = crate::core::source::resolve_source(source, &self.config)?;
        
        match download_source {
            #[cfg(feature = "s3")]
            DownloadSource::S3 { bucket, key, region } => {
                self.create_s3_stream(&bucket, &key, &region, options).await
            }
            DownloadSource::Http { url } => {
                self.create_http_stream(&url, options).await
            }
        }
    }

    /// Download from S3 to file
    #[cfg(feature = "s3")]
    async fn download_s3_to_file(
        &self,
        bucket: &str,
        key: &str,
        region: &str,
        file_path: &str,
        options: &DownloadOptions,
    ) -> Result<()> {
        let (stream, total_size) = self.create_s3_stream(bucket, key, region, options).await?;
        let file = create_optimized_file(file_path, Some(total_size)).await?;
        
        self.stream_to_writer(stream, Box::new(file), total_size, options).await
    }

    /// Download from HTTP to file
    async fn download_http_to_file(
        &self,
        url: &str,
        file_path: &str,
        options: &DownloadOptions,
    ) -> Result<()> {
        let client = &*GLOBAL_CLIENT;
        
        // Get file size and check range support
        let head_response = client.head(url).send().await?;
        if !head_response.status().is_success() {
            return Err(Error::HttpError(format!("Failed to get file info: {}", head_response.status())));
        }
        
        let total_size = head_response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .ok_or_else(|| Error::HttpError("Could not determine file size".to_string()))?;
        
        let supports_ranges = head_response
            .headers()
            .get("accept-ranges")
            .map_or(false, |v| v.to_str().unwrap_or("") == "bytes");
        
        let file = create_optimized_file(file_path, Some(total_size)).await?;
        
        if !supports_ranges || total_size < 10 * 1024 * 1024 {
            // Single connection download
            let response = client.get(url).send().await?;
            let stream = create_http_stream(response);
            self.stream_to_writer(stream, Box::new(file), total_size, options).await
        } else {
            // Parallel download
            self.download_http_parallel(client, url, Box::new(file), total_size, options).await
        }
    }

    /// Create S3 stream
    #[cfg(feature = "s3")]
    async fn create_s3_stream(
        &self,
        bucket: &str,
        key: &str,
        region: &str,
        _options: &DownloadOptions,
    ) -> Result<(DownloadStream, u64)> {
        let config = aws_config::defaults(BehaviorVersion::latest())
            .region(aws_config::Region::new(region.to_string()))
            .load()
            .await;
        
        let s3_client = S3Client::new(&config);
        
        // Get object metadata
        let head_response = s3_client
            .head_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::S3Error(e.to_string()))?;
        
        let total_size = head_response.content_length().unwrap_or(0) as u64;
        
        // Get the object
        let response = s3_client
            .get_object()
            .bucket(bucket)
            .key(key)
            .send()
            .await
            .map_err(|e| Error::S3Error(e.to_string()))?;
        
        let stream = create_s3_stream(response.body);
        Ok((stream, total_size))
    }

    /// Create HTTP stream (single connection)
    async fn create_http_stream(
        &self,
        url: &str,
        _options: &DownloadOptions,
    ) -> Result<(DownloadStream, u64)> {
        let client = &*GLOBAL_CLIENT;
        
        let head_response = client.head(url).send().await?;
        if !head_response.status().is_success() {
            return Err(Error::HttpError(format!("Failed to get file info: {}", head_response.status())));
        }
        
        let total_size = head_response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        
        let response = client.get(url).send().await?;
        if !response.status().is_success() {
            return Err(Error::HttpError(format!("Failed to download: {}", response.status())));
        }
        
        let stream = create_http_stream(response);
        Ok((stream, total_size))
    }

    /// Stream data to a writer with progress tracking
    async fn stream_to_writer(
        &self,
        mut stream: DownloadStream,
        mut writer: Box<dyn AsyncWrite + Send + Unpin>,
        total_size: u64,
        options: &DownloadOptions,
    ) -> Result<()> {
        let mut buffer = vec![0u8; options.buffer_size];
        let mut downloaded = 0u64;
        
        loop {
            let bytes_read = stream.read(&mut buffer).await?;
            if bytes_read == 0 {
                break;
            }
            
            writer.write_all(&buffer[..bytes_read]).await?;
            downloaded += bytes_read as u64;
            
            if let Some(ref progress) = options.progress {
                progress(downloaded, total_size);
            }
        }
        
        writer.flush().await?;
        Ok(())
    }

    /// Download HTTP with parallel range requests
    async fn download_http_parallel(
        &self,
        client: &Client,
        url: &str,
        mut writer: Box<dyn AsyncWrite + Send + Unpin>,
        total_size: u64,
        options: &DownloadOptions,
    ) -> Result<()> {
        let connections = calculate_optimal_connections(total_size, options.max_connections);
        let chunk_size = total_size / connections as u64;
        
        // Generate ranges
        let ranges: Vec<(u64, u64)> = (0..connections)
            .map(|i| {
                let start = i as u64 * chunk_size;
                let end = if i == connections - 1 {
                    total_size - 1
                } else {
                    start + chunk_size - 1
                };
                (start, end)
            })
            .collect();
        
        let downloaded_bytes = Arc::new(AtomicU64::new(0));
        
        // Progress tracking
        let progress_handle = if let Some(progress_fn) = options.progress.clone() {
            let downloaded_clone = Arc::clone(&downloaded_bytes);
            Some(tokio::spawn(async move {
                while downloaded_clone.load(Ordering::Relaxed) < total_size {
                    let current = downloaded_clone.load(Ordering::Relaxed);
                    progress_fn(current, total_size);
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                progress_fn(total_size, total_size);
            }))
        } else {
            None
        };
        
        // Ring buffer for ordering chunks
        let mut ring_buffer: Vec<Option<Vec<u8>>> = vec![None; ranges.len()];
        let mut next_chunk_to_write = 0;
        
        // Download chunks with controlled concurrency
        let stream = futures::stream::iter(ranges.into_iter().enumerate())
            .map(|(idx, (start, end))| {
                let client = client.clone();
                let url = url.to_string();
                let downloaded_bytes = Arc::clone(&downloaded_bytes);
                
                async move {
                    let range_header = format!("bytes={}-{}", start, end);
                    let response = client
                        .get(&url)
                        .header("Range", range_header)
                        .send()
                        .await?;
                    
                    if !response.status().is_success() && response.status().as_u16() != 206 {
                        return Err(Error::HttpError(format!("Range request failed: {}", response.status())));
                    }
                    
                    // Stream chunk data
                    let mut chunk_data = Vec::new();
                    let mut stream = response.bytes_stream();
                    
                    while let Some(bytes_chunk) = stream.try_next().await? {
                        chunk_data.extend_from_slice(&bytes_chunk);
                        downloaded_bytes.fetch_add(bytes_chunk.len() as u64, Ordering::Relaxed);
                    }
                    
                    Ok::<(usize, Vec<u8>), Error>((idx, chunk_data))
                }
            })
            .buffer_unordered(connections);
        
        tokio::pin!(stream);
        
        // Collect and write chunks in order
        while let Some(result) = stream.next().await {
            let (idx, data) = result?;
            ring_buffer[idx] = Some(data);
            
            // Write sequential chunks
            while next_chunk_to_write < ring_buffer.len() && ring_buffer[next_chunk_to_write].is_some() {
                if let Some(chunk) = ring_buffer[next_chunk_to_write].take() {
                    writer.write_all(&chunk).await?;
                }
                next_chunk_to_write += 1;
            }
        }
        
        writer.flush().await?;
        
        if let Some(handle) = progress_handle {
            handle.abort();
        }
        
        Ok(())
    }
}

/// Calculate optimal number of connections based on file size and limits
fn calculate_optimal_connections(file_size: u64, max_connections: usize) -> usize {
    let cpu_count = num_cpus::get();
    
    let base_connections = match file_size {
        size if size <= 10 * 1024 * 1024 => 2,       // <= 10MB: 2 connections
        size if size <= 100 * 1024 * 1024 => 4,      // <= 100MB: 4 connections  
        size if size <= 512 * 1024 * 1024 => 8,      // <= 512MB: 8 connections
        size if size <= 1024 * 1024 * 1024 => 12,    // <= 1GB: 12 connections
        _ => 16,                                      // > 1GB: 16 connections
    };
    
    std::cmp::min(base_connections, std::cmp::min(max_connections, cpu_count * 2))
}

/// Create an optimized file for large downloads with optional Direct I/O
async fn create_optimized_file(path: &str, size_hint: Option<u64>) -> Result<tokio::fs::File> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        
        // Try Direct I/O for large files (>1GB) on Unix systems
        if let Some(size) = size_hint {
            if size > 1024 * 1024 * 1024 {
                match std::fs::OpenOptions::new()
                    .write(true)
                    .create(true)
                    .truncate(true)
                    .custom_flags(libc::O_DIRECT)
                    .open(path)
                {
                    Ok(file) => {
                        return Ok(tokio::fs::File::from_std(file));
                    },
                    Err(_) => {
                        // Direct I/O failed, fall back to standard I/O
                    }
                }
            }
        }
    }
    
    // Fallback to standard file creation
    tokio::fs::File::create(path).await.map_err(Into::into)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_calculate_optimal_connections() {
        assert_eq!(calculate_optimal_connections(5 * 1024 * 1024, 16), 2);     // 5MB
        assert_eq!(calculate_optimal_connections(50 * 1024 * 1024, 16), 4);    // 50MB
        assert_eq!(calculate_optimal_connections(200 * 1024 * 1024, 16), 8);   // 200MB
        assert_eq!(calculate_optimal_connections(2 * 1024 * 1024 * 1024, 16), 16); // 2GB
    }

    #[test]
    fn test_calculate_optimal_connections_with_limit() {
        assert_eq!(calculate_optimal_connections(2 * 1024 * 1024 * 1024, 8), 8); // Limited to 8
    }
}