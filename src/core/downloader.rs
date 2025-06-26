//! Core download functionality for butterfly-dl
//!
//! Provides high-performance, memory-efficient download implementations for HTTP sources.

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
use crate::core::stream::{DownloadOptions, DownloadStream, OverwriteBehavior, create_http_stream};

/// Maximum number of retry attempts for network errors
const MAX_RETRY_ATTEMPTS: u32 = 3;

/// Base delay for exponential backoff (in milliseconds)
const BASE_RETRY_DELAY_MS: u64 = 1000;

/// Global HTTP client with optimizations
static GLOBAL_CLIENT: Lazy<Client> = Lazy::new(|| {
    ClientBuilder::new()
        .tcp_keepalive(Duration::from_secs(60))
        .pool_idle_timeout(Duration::from_secs(90))
        .pool_max_idle_per_host(20)
        .timeout(Duration::from_secs(30))        // Overall request timeout
        .connect_timeout(Duration::from_secs(10)) // Connection timeout
        .user_agent(format!("butterfly-dl/{}", env!("BUTTERFLY_VERSION")))
        .build()
        .expect("Failed to create HTTP client")
});

/// Execute an operation with retry logic for network errors
async fn retry_on_network_error<F, Fut, T>(operation: F) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut attempt = 0;
    
    loop {
        match operation().await {
            Ok(result) => return Ok(result),
            Err(Error::NetworkError(msg)) if attempt < MAX_RETRY_ATTEMPTS => {
                attempt += 1;
                let delay = BASE_RETRY_DELAY_MS * (1 << (attempt - 1)); // Exponential backoff
                eprintln!("⚠️  Network error (attempt {attempt}): {msg}. Retrying in {delay}ms...");
                tokio::time::sleep(Duration::from_millis(delay)).await;
            }
            Err(e) => return Err(e), // Non-network errors or max retries exceeded
        }
    }
}

/// Check if destination file exists and handle overwrite behavior
async fn check_overwrite_permission(file_path: &str, behavior: &OverwriteBehavior) -> Result<bool> {
    // Check if file exists
    if !std::path::Path::new(file_path).exists() {
        return Ok(true); // File doesn't exist, proceed
    }
    
    match behavior {
        OverwriteBehavior::Force => {
            eprintln!("⚠️  Overwriting existing file: {file_path}");
            Ok(true)
        }
        OverwriteBehavior::NeverOverwrite => {
            Err(Error::IoError(std::io::Error::new(
                std::io::ErrorKind::AlreadyExists,
                format!("File already exists: {file_path} (use --force to overwrite)"),
            )))
        }
        OverwriteBehavior::Prompt => {
            eprintln!("⚠️  File already exists: {file_path}");
            eprint!("Overwrite? [y/N]: ");
            
            // Flush stderr to ensure prompt is displayed
            use std::io::Write;
            std::io::stderr().flush().map_err(Error::IoError)?;
            
            // Read user input from stdin
            let mut input = String::new();
            std::io::stdin().read_line(&mut input).map_err(Error::IoError)?;
            
            let response = input.trim().to_lowercase();
            match response.as_str() {
                "y" | "yes" => {
                    eprintln!("✅ Overwriting file");
                    Ok(true)
                }
                _ => {
                    eprintln!("❌ Download cancelled");
                    Err(Error::IoError(std::io::Error::new(
                        std::io::ErrorKind::Interrupted,
                        "Download cancelled by user",
                    )))
                }
            }
        }
    }
}

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
        // Check overwrite permission before starting download
        check_overwrite_permission(file_path, &options.overwrite).await?;
        
        let download_source = crate::core::source::resolve_source(source, &self.config)?;
        
        match download_source {
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
            DownloadSource::Http { url } => {
                self.create_http_stream(&url, options).await
            }
        }
    }

    /// Download from HTTP to file
    async fn download_http_to_file(
        &self,
        url: &str,
        file_path: &str,
        options: &DownloadOptions,
    ) -> Result<()> {
        let client = &*GLOBAL_CLIENT;
        
        // Get file size and check range support with retry
        let (total_size, supports_ranges) = retry_on_network_error(|| async {
            let head_response = client.head(url).send().await?;
            if !head_response.status().is_success() {
                return Err(create_helpful_http_error(url, head_response.status()));
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
                .is_some_and(|v| v.to_str().unwrap_or("") == "bytes");
                
            Ok((total_size, supports_ranges))
        }).await?;
        
        let file = create_optimized_file(file_path, Some(total_size)).await?;
        
        let optimal_connections = calculate_optimal_connections(total_size, options.max_connections);
        
        if !supports_ranges || optimal_connections == 1 {
            // Single connection download - resilient streaming
            self.download_single_resilient(client, url, Box::new(file), total_size, supports_ranges, options).await
        } else {
            // Parallel download - resilient chunks
            self.download_http_parallel_resilient(client, url, Box::new(file), total_size, options).await
        }
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
            return Err(create_helpful_http_error(url, head_response.status()));
        }
        
        let total_size = head_response
            .headers()
            .get("content-length")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .unwrap_or(0);
        
        let response = client.get(url).send().await?;
        if !response.status().is_success() {
            let status = response.status();
            return Err(Error::HttpError(format!("Failed to download: {status}")));
        }
        
        let stream = create_http_stream(response);
        Ok((stream, total_size))
    }

    /// Resilient single connection download with range resume capability
    async fn download_single_resilient(
        &self,
        client: &Client,
        url: &str,
        mut writer: Box<dyn AsyncWrite + Send + Unpin>,
        total_size: u64,
        supports_ranges: bool,
        options: &DownloadOptions,
    ) -> Result<()> {
        let mut downloaded = 0u64;
        
        while downloaded < total_size {
            let result = if downloaded == 0 {
                // Initial request
                retry_on_network_error(|| async {
                    let response = client.get(url).send().await?;
                    let stream = create_http_stream(response);
                    Ok(stream)
                }).await
            } else if supports_ranges {
                // Resume using range request
                retry_on_network_error(|| async {
                    let range_header = format!("bytes={downloaded}-");
                    let response = client.get(url).header("Range", range_header).send().await?;
                    let stream = create_http_stream(response);
                    Ok(stream)
                }).await
            } else {
                return Err(Error::NetworkError("Cannot resume download - server doesn't support ranges".to_string()));
            };
            
            match result {
                Ok(stream) => {
                    // Stream with resilient reading
                    match self.stream_to_writer_resilient(stream, &mut writer, total_size, &mut downloaded, options).await {
                        Ok(()) => break, // Download completed
                        Err(Error::NetworkError(_)) => {
                            eprintln!("⚠️  Stream interrupted at {downloaded} bytes, resuming...");
                            continue; // Retry from current position
                        }
                        Err(e) => return Err(e), // Non-network errors
                    }
                }
                Err(e) => return Err(e),
            }
        }
        
        writer.flush().await?;
        Ok(())
    }

    /// Resilient streaming - if stream fails, propagate error for retry at higher level
    async fn stream_to_writer_resilient(
        &self,
        mut stream: DownloadStream,
        writer: &mut Box<dyn AsyncWrite + Send + Unpin>,
        total_size: u64,
        downloaded: &mut u64,
        options: &DownloadOptions,
    ) -> Result<()> {
        let mut buffer = vec![0u8; options.buffer_size];
        
        loop {
            let bytes_read = stream.read(&mut buffer).await.map_err(|e| {
                Error::NetworkError(format!("Stream read error: {e}"))
            })?;
            
            if bytes_read == 0 {
                break;
            }
            
            writer.write_all(&buffer[..bytes_read]).await?;
            *downloaded += bytes_read as u64;
            
            if let Some(ref progress) = options.progress {
                progress(*downloaded, total_size);
            }
        }
        
        Ok(())
    }

    /// Resilient parallel download with per-chunk retry
    async fn download_http_parallel_resilient(
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
        
        // Download chunks with resilient retry per chunk
        let stream = futures::stream::iter(ranges.into_iter().enumerate())
            .map(|(idx, (start, end))| {
                let client = client.clone();
                let url = url.to_string();
                let downloaded_bytes = Arc::clone(&downloaded_bytes);
                
                async move {
                    // Retry entire chunk download on failure
                    retry_on_network_error(|| async {
                        let range_header = format!("bytes={start}-{end}");
                        let response = client
                            .get(&url)
                            .header("Range", range_header)
                            .send()
                            .await?;
                        
                        if !response.status().is_success() && response.status().as_u16() != 206 {
                            let status = response.status();
                            return Err(Error::HttpError(format!("Range request failed: {status}")));
                        }
                        
                        // Stream chunk data with resilient reading
                        let mut chunk_data = Vec::new();
                        let mut stream = response.bytes_stream();
                        
                        while let Some(bytes_chunk) = stream.try_next().await? {
                            chunk_data.extend_from_slice(&bytes_chunk);
                            downloaded_bytes.fetch_add(bytes_chunk.len() as u64, Ordering::Relaxed);
                        }
                        
                        Ok::<(usize, Vec<u8>), Error>((idx, chunk_data))
                    }).await
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
        size if size <= 1024 * 1024 => 1,            // <= 1MB: single connection (curl-like)
        size if size <= 10 * 1024 * 1024 => 2,       // <= 10MB: 2 connections
        size if size <= 100 * 1024 * 1024 => 4,      // <= 100MB: 4 connections  
        size if size <= 512 * 1024 * 1024 => 8,      // <= 512MB: 8 connections
        size if size <= 1024 * 1024 * 1024 => 12,    // <= 1GB: 12 connections
        _ => 16,                                      // > 1GB: 16 connections
    };
    
    std::cmp::min(base_connections, std::cmp::min(max_connections, cpu_count * 2))
}

/// Create an optimized file for large downloads with optional Direct I/O
async fn create_optimized_file(path: &str, _size_hint: Option<u64>) -> Result<tokio::fs::File> {
    #[cfg(unix)]
    {
        // Try Direct I/O for large files (>1GB) on Linux systems only
        // O_DIRECT is not available on macOS/BSD systems
        #[cfg(target_os = "linux")]
        {
            use std::os::unix::fs::OpenOptionsExt;
            
            if let Some(size) = _size_hint {
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
    }
    
    // Fallback to standard file creation
    tokio::fs::File::create(path).await.map_err(Into::into)
}

/// Create a helpful HTTP error with suggestions for common typos
fn create_helpful_http_error(url: &str, status: reqwest::StatusCode) -> Error {
    let mut message = format!("Failed to get file info: {status}");
    
    if status == reqwest::StatusCode::NOT_FOUND {
        // Extract source from URL patterns
        let source = if url.contains("planet.openstreetmap.org") {
            Some("planet".to_string())
        } else if url.contains("download.geofabrik.de") {
            // Extract the source from the URL pattern: https://download.geofabrik.de/{source}-latest.osm.pbf
            url.split("download.geofabrik.de/")
                .nth(1)
                .and_then(|after_domain| after_domain.strip_suffix("-latest.osm.pbf"))
                .map(|s| s.to_string())
        } else {
            None
        };
        
        if let Some(source) = source {
            if let Some(suggestion) = crate::core::error::suggest_correction(&source) {
                message = format!(
                    "Source '{source}' not found. Did you mean '{suggestion}'?"
                );
            } else {
                message = format!(
                    "Source '{source}' not found. Check the URL or try common sources like: planet, europe, asia"
                );
            }
        } else {
            // Generic fallback for unknown domains
            message = format!(
                "Source not found ({status}): {url}. Check the URL or try common sources like: planet, europe, asia"
            );
        }
    }
    
    Error::HttpError(message)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;
    use wiremock::{MockServer, Mock, ResponseTemplate};
    use wiremock::matchers::{method, path};
    use tempfile::NamedTempFile;

    #[test]
    fn test_calculate_optimal_connections() {
        let cpu_count = num_cpus::get();
        
        assert_eq!(calculate_optimal_connections(512 * 1024, 16), 1);        // 512KB: single connection
        assert_eq!(calculate_optimal_connections(5 * 1024 * 1024, 16), 2);   // 5MB: 2 connections
        assert_eq!(calculate_optimal_connections(50 * 1024 * 1024, 16), 4);  // 50MB: 4 connections
        assert_eq!(calculate_optimal_connections(200 * 1024 * 1024, 16), std::cmp::min(8, cpu_count * 2)); // 200MB: 8 connections (or CPU limit)
        assert_eq!(calculate_optimal_connections(2 * 1024 * 1024 * 1024, 16), std::cmp::min(16, cpu_count * 2)); // 2GB: 16 connections (or CPU limit)
    }

    #[test]
    fn test_calculate_optimal_connections_with_limit() {
        // Test with a large file (2GB) and max_connections limit of 8
        // Result should be limited by min(base_connections=16, max_connections=8, cpu_count*2)
        let result = calculate_optimal_connections(2 * 1024 * 1024 * 1024, 8);
        let cpu_count = num_cpus::get();
        let expected = std::cmp::min(8, cpu_count * 2); // Limited by either max_connections or CPU count
        assert_eq!(result, expected);
    }

    #[test]
    fn test_calculate_optimal_connections_small_files() {
        // Small files should use single connection for curl-like performance
        assert_eq!(calculate_optimal_connections(100 * 1024, 16), 1);     // 100KB
        assert_eq!(calculate_optimal_connections(500 * 1024, 16), 1);     // 500KB  
        assert_eq!(calculate_optimal_connections(1024 * 1024, 16), 1);    // 1MB (boundary)
        assert_eq!(calculate_optimal_connections(1024 * 1024 + 1, 16), 2); // 1MB + 1 byte
    }

    #[tokio::test]
    async fn test_resilient_download_with_network_failure() {
        // Create mock server
        let mock_server = MockServer::start().await;
        
        // Test data: 1KB file (will fit in single connection)
        let test_data = b"A".repeat(1024);
        let total_size = test_data.len() as u64;
        
        // Track how many times each endpoint is called
        let head_call_count = Arc::new(AtomicUsize::new(0));
        let get_call_count = Arc::new(AtomicUsize::new(0));
        
        // HEAD endpoint - always succeeds, returns file info
        let head_count_clone = Arc::clone(&head_call_count);
        Mock::given(method("HEAD"))
            .and(path("/test-file.pbf"))
            .respond_with(move |_: &wiremock::Request| {
                head_count_clone.fetch_add(1, Ordering::SeqCst);
                ResponseTemplate::new(200)
                    .insert_header("content-length", total_size.to_string().as_str())
                    .insert_header("accept-ranges", "bytes")
            })
            .mount(&mock_server)
            .await;
        
        // GET endpoint - succeeds on first call for basic functionality test
        let get_count_clone = Arc::clone(&get_call_count);
        let test_data_clone = test_data.clone();
        Mock::given(method("GET"))
            .and(path("/test-file.pbf"))
            .respond_with(move |_req: &wiremock::Request| {
                let call_num = get_count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                
                println!("GET call #{call_num}");
                
                // For basic test: succeed immediately
                println!("Call {call_num} - returning full data");
                ResponseTemplate::new(200)
                    .insert_header("content-length", total_size.to_string().as_str())
                    .set_body_raw(test_data_clone.clone(), "application/octet-stream")
            })
            .mount(&mock_server)
            .await;
        
        // Create temporary file for download
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        
        // Create downloader and options
        let downloader = Downloader::new();
        let options = DownloadOptions::default();
        
        let base_uri = mock_server.uri();
        let url = format!("{base_uri}/test-file.pbf");
        
        // Test the resilient download
        let result = downloader.download_http_to_file(&url, file_path, &options).await;
        
        // Verify success
        assert!(result.is_ok(), "Download should succeed: {result:?}");
        
        // Verify file content
        let downloaded_data = std::fs::read(file_path).unwrap();
        assert_eq!(downloaded_data, test_data, "Downloaded file should match original data");
        
        // Verify basic behavior
        let head_calls = head_call_count.load(Ordering::SeqCst);
        let get_calls = get_call_count.load(Ordering::SeqCst);
        
        println!("HEAD calls: {head_calls}, GET calls: {get_calls}");
        
        // For basic test: should have made 1 HEAD and 1 GET call
        assert_eq!(head_calls, 1, "Should have made 1 HEAD request");
        assert_eq!(get_calls, 1, "Should have made 1 GET request");
        
        println!("✅ Basic download test passed! Made {head_calls} HEAD and {get_calls} GET calls");
    }

    #[tokio::test]
    async fn test_retry_exponential_backoff() {
        use std::time::Instant;
        
        // Test that retry_on_network_error implements exponential backoff
        let start_time = Instant::now();
        let call_count = Arc::new(AtomicUsize::new(0));
        
        let result = retry_on_network_error(|| {
            let count_clone = Arc::clone(&call_count);
            async move {
                let call_num = count_clone.fetch_add(1, Ordering::SeqCst) + 1;
                
                if call_num <= 2 {
                    // Fail first 2 calls
                    Err(Error::NetworkError("Simulated network failure".to_string()))
                } else {
                    // Succeed on 3rd call
                    Ok("success")
                }
            }
        }).await;
        
        let elapsed = start_time.elapsed();
        let calls = call_count.load(Ordering::SeqCst);
        
        // Should succeed after 3 calls
        assert!(result.is_ok());
        assert_eq!(calls, 3);
        
        // Should have taken at least 3 seconds (1s + 2s delays)
        assert!(elapsed >= Duration::from_secs(3), "Should implement exponential backoff delays");
        
        println!("✅ Exponential backoff test passed! {calls} calls in {elapsed:?}");
    }

    #[tokio::test]
    async fn test_overwrite_behavior_force() {
        use tempfile::NamedTempFile;
        use crate::core::stream::OverwriteBehavior;
        
        // Create a temporary file that already exists
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        
        // Write some content to the file
        std::fs::write(file_path, "existing content").unwrap();
        assert!(std::path::Path::new(file_path).exists());
        
        // Test force overwrite
        let result = check_overwrite_permission(file_path, &OverwriteBehavior::Force).await;
        assert!(result.is_ok(), "Force overwrite should succeed");
        assert!(result.unwrap(), "Force overwrite should return true");
        
        println!("✅ Force overwrite test passed!");
    }

    #[tokio::test]
    async fn test_overwrite_behavior_never() {
        use tempfile::NamedTempFile;
        use crate::core::stream::OverwriteBehavior;
        
        // Create a temporary file that already exists
        let temp_file = NamedTempFile::new().unwrap();
        let file_path = temp_file.path().to_str().unwrap();
        
        // Write some content to the file
        std::fs::write(file_path, "existing content").unwrap();
        assert!(std::path::Path::new(file_path).exists());
        
        // Test never overwrite
        let result = check_overwrite_permission(file_path, &OverwriteBehavior::NeverOverwrite).await;
        assert!(result.is_err(), "Never overwrite should fail when file exists");
        
        // Check error message
        let error = result.unwrap_err();
        match error {
            crate::core::error::Error::IoError(io_err) => {
                assert_eq!(io_err.kind(), std::io::ErrorKind::AlreadyExists);
                assert!(io_err.to_string().contains("use --force to overwrite"));
            }
            _ => panic!("Expected IoError with AlreadyExists kind"),
        }
        
        println!("✅ Never overwrite test passed!");
    }

    #[tokio::test]
    async fn test_overwrite_behavior_new_file() {
        use tempfile::tempdir;
        use crate::core::stream::OverwriteBehavior;
        
        // Create path to non-existent file
        let temp_dir = tempdir().unwrap();
        let file_path = temp_dir.path().join("nonexistent.pbf");
        let file_path_str = file_path.to_str().unwrap();
        
        assert!(!std::path::Path::new(file_path_str).exists());
        
        // Test all behaviors with non-existent file (should all succeed)
        for behavior in [OverwriteBehavior::Force, OverwriteBehavior::NeverOverwrite, OverwriteBehavior::Prompt] {
            let result = check_overwrite_permission(file_path_str, &behavior).await;
            assert!(result.is_ok(), "All behaviors should succeed for non-existent file");
            assert!(result.unwrap(), "All behaviors should return true for non-existent file");
        }
        
        println!("✅ New file test passed!");
    }
}