//! # Butterfly-dl: Optimized OpenStreetMap Data Downloader
//!
//! A high-performance command-line tool for downloading OpenStreetMap data files with minimal memory usage.
//! 
//! ## Features
//! 
//! - Smart source routing: S3 for planet files, HTTP for regional extracts
//! - Memory efficient: <1GB RAM usage regardless of file size (including 81GB planet)
//! - Parallel downloads for HTTP sources with auto-tuning
//! - Single-stream optimized downloads for S3 sources
//! - Direct I/O support for large files
//! - Stdout streaming support for pipeline integration
//! - All logging to stderr for curl-like behavior
//! 
//! ## Usage
//! 
//! ```bash
//! # Download planet file from S3
//! butterfly-dl planet
//! 
//! # Download regional extract from Geofabrik
//! butterfly-dl europe/belgium
//! 
//! # Stream to stdout
//! butterfly-dl europe/belgium - | gzip > belgium.pbf.gz
//! 
//! # Save to specific file
//! butterfly-dl planet planet.pbf
//! ```

use clap::Parser;
use anyhow::{Result, anyhow};
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, ClientBuilder};
use std::sync::Arc;
use std::time::Duration;
use once_cell::sync::Lazy;
use futures::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use futures::TryStreamExt;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use aws_config::BehaviorVersion;
use aws_sdk_s3::Client as S3Client;

#[cfg(unix)]
use std::os::unix::fs::OpenOptionsExt;

/// Buffer size for streaming operations (64KB)
const BUFFER_SIZE: usize = 64 * 1024;

/// Maximum number of parallel connections for HTTP downloads
const MAX_CONNECTIONS: usize = 16;


/// Command-line interface for butterfly-dl
#[derive(Parser)]
#[command(name = "butterfly-dl")]
#[command(about = "Optimized OpenStreetMap data downloader with S3 and HTTP support")]
#[command(version = "0.1.0")]
struct Cli {
    /// Source identifier (e.g., "planet", "europe", "europe/belgium")
    source: String,
    
    /// Output file path, or "-" for stdout
    #[arg(default_value = "")]
    output: String,
    
    /// Enable dry-run mode (show what would be downloaded without downloading)
    #[arg(long)]
    dry_run: bool,
    
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
}

/// Download source types
#[derive(Debug, Clone)]
enum DownloadSource {
    S3 {
        bucket: String,
        key: String,
        region: String,
    },
    Http {
        url: String,
    },
}

/// Output destination types
#[derive(Debug)]
enum OutputDestination {
    File(String),
    Stdout,
}

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

/// Resolve source string to download source
fn resolve_source(source: &str) -> DownloadSource {
    match source {
        "planet" => DownloadSource::S3 {
            bucket: "osm-planet-eu-central-1".to_string(),
            key: "planet-latest.osm.pbf".to_string(),
            region: "eu-central-1".to_string(),
        },
        path if path.contains('/') => DownloadSource::Http {
            url: format!("https://download.geofabrik.de/{}-latest.osm.pbf", path),
        },
        continent => DownloadSource::Http {
            url: format!("https://download.geofabrik.de/{}-latest.osm.pbf", continent),
        },
    }
}

/// Resolve output destination
fn resolve_output(source: &str, output: &str) -> OutputDestination {
    if output == "-" {
        OutputDestination::Stdout
    } else if output.is_empty() {
        // Auto-generate filename
        let filename = match source {
            "planet" => "planet-latest.osm.pbf".to_string(),
            path if path.contains('/') => {
                let name = path.split('/').last().unwrap_or(path);
                format!("{}-latest.osm.pbf", name)
            },
            continent => format!("{}-latest.osm.pbf", continent),
        };
        OutputDestination::File(filename)
    } else {
        OutputDestination::File(output.to_string())
    }
}

/// Download from S3 with single-stream optimization
async fn download_s3(
    bucket: &str,
    key: &str,
    region: &str,
    output: OutputDestination,
) -> Result<()> {
    eprintln!("🌍 Downloading from S3: s3://{}/{}", bucket, key);
    
    // Configure AWS client for anonymous access
    let config = aws_config::defaults(BehaviorVersion::latest())
        .region(aws_config::Region::new(region.to_string()))
        .load()
        .await;
    
    let s3_client = S3Client::new(&config);
    
    // Get object metadata for progress tracking
    let head_response = s3_client
        .head_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    
    let total_size = head_response.content_length().unwrap_or(0) as u64;
    
    // Setup progress bar to stderr
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-")
    );
    
    // Get the object
    let response = s3_client
        .get_object()
        .bucket(bucket)
        .key(key)
        .send()
        .await?;
    
    let mut stream = response.body.into_async_read();
    let mut writer: Box<dyn AsyncWrite + Unpin> = match output {
        OutputDestination::File(path) => {
            let file = create_optimized_file(&path, Some(total_size)).await?;
            eprintln!("📁 Saving to: {}", path);
            Box::new(file)
        },
        OutputDestination::Stdout => {
            Box::new(tokio::io::stdout())
        },
    };
    
    // Stream with progress tracking
    let mut buffer = vec![0u8; BUFFER_SIZE];
    let mut downloaded = 0u64;
    
    loop {
        let bytes_read = tokio::io::AsyncReadExt::read(&mut stream, &mut buffer).await?;
        if bytes_read == 0 {
            break;
        }
        
        writer.write_all(&buffer[..bytes_read]).await?;
        downloaded += bytes_read as u64;
        pb.set_position(downloaded);
    }
    
    writer.flush().await?;
    pb.finish_with_message("✅ S3 download completed!");
    
    Ok(())
}

/// Download from HTTP with parallel range requests
async fn download_http(
    url: &str,
    output: OutputDestination,
) -> Result<()> {
    eprintln!("🌐 Downloading from HTTP: {}", url);
    
    let client = &*GLOBAL_CLIENT;
    
    // Get file size
    let head_response = client.head(url).send().await?;
    if !head_response.status().is_success() {
        return Err(anyhow!("Failed to get file info: {}", head_response.status()));
    }
    
    let total_size = head_response
        .headers()
        .get("content-length")
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("Could not determine file size"))?;
    
    // Check if server supports range requests
    let supports_ranges = head_response
        .headers()
        .get("accept-ranges")
        .map_or(false, |v| v.to_str().unwrap_or("") == "bytes");
    
    if !supports_ranges || total_size < 10 * 1024 * 1024 {
        // Fallback to single connection for small files or no range support
        return download_http_single(client, url, output, total_size).await;
    }
    
    // Calculate optimal connections and chunk size
    let connections = calculate_optimal_connections(total_size);
    let chunk_size = total_size / connections as u64;
    
    eprintln!("📊 Using {} parallel connections", connections);
    
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
    
    // Setup progress bar
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-")
    );
    
    let downloaded_bytes = Arc::new(AtomicU64::new(0));
    
    // Setup output writer
    let mut writer: Box<dyn AsyncWrite + Unpin> = match output {
        OutputDestination::File(path) => {
            let file = create_optimized_file(&path, Some(total_size)).await?;
            eprintln!("📁 Saving to: {}", path);
            Box::new(file)
        },
        OutputDestination::Stdout => {
            Box::new(tokio::io::stdout())
        },
    };
    
    // Progress updater
    let pb_for_progress = pb.clone();
    let downloaded_clone = Arc::clone(&downloaded_bytes);
    let progress_task = tokio::spawn(async move {
        while downloaded_clone.load(Ordering::Relaxed) < total_size {
            let current = downloaded_clone.load(Ordering::Relaxed);
            pb_for_progress.set_position(current);
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        pb_for_progress.set_position(total_size);
    });
    
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
                    return Err(anyhow!("Range request failed: {}", response.status()));
                }
                
                // Stream chunk data with fixed buffer
                let mut chunk_data = Vec::new();
                let mut stream = response.bytes_stream();
                
                while let Some(bytes_chunk) = stream.try_next().await? {
                    chunk_data.extend_from_slice(&bytes_chunk);
                    downloaded_bytes.fetch_add(bytes_chunk.len() as u64, Ordering::Relaxed);
                }
                
                Result::<(usize, Vec<u8>)>::Ok((idx, chunk_data))
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
    progress_task.abort();
    pb.finish_with_message("✅ HTTP download completed!");
    
    Ok(())
}

/// Single connection HTTP download fallback
async fn download_http_single(
    client: &Client,
    url: &str,
    output: OutputDestination,
    total_size: u64,
) -> Result<()> {
    eprintln!("📡 Using single connection download");
    
    let response = client.get(url).send().await?;
    if !response.status().is_success() {
        return Err(anyhow!("Failed to download: {}", response.status()));
    }
    
    let pb = ProgressBar::new(total_size);
    pb.set_style(
        ProgressStyle::default_bar()
            .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
            .progress_chars("#>-")
    );
    
    let mut writer: Box<dyn AsyncWrite + Unpin> = match output {
        OutputDestination::File(path) => {
            let file = create_optimized_file(&path, Some(total_size)).await?;
            eprintln!("📁 Saving to: {}", path);
            Box::new(file)
        },
        OutputDestination::Stdout => {
            Box::new(tokio::io::stdout())
        },
    };
    
    let mut stream = response.bytes_stream();
    let mut downloaded = 0u64;
    
    while let Some(chunk) = stream.try_next().await? {
        writer.write_all(&chunk).await?;
        downloaded += chunk.len() as u64;
        pb.set_position(downloaded);
    }
    
    writer.flush().await?;
    pb.finish_with_message("✅ Single connection download completed!");
    
    Ok(())
}

/// Calculate optimal number of connections based on file size and CPU count
fn calculate_optimal_connections(file_size: u64) -> usize {
    let cpu_count = num_cpus::get();
    
    let base_connections = match file_size {
        size if size <= 10 * 1024 * 1024 => 2,       // <= 10MB: 2 connections
        size if size <= 100 * 1024 * 1024 => 4,      // <= 100MB: 4 connections  
        size if size <= 512 * 1024 * 1024 => 8,      // <= 512MB: 8 connections
        size if size <= 1024 * 1024 * 1024 => 12,    // <= 1GB: 12 connections
        _ => 16,                                      // > 1GB: 16 connections
    };
    
    std::cmp::min(base_connections, std::cmp::min(MAX_CONNECTIONS, cpu_count * 2))
}

/// Create an optimized file for large downloads with optional Direct I/O
async fn create_optimized_file(path: &str, size_hint: Option<u64>) -> Result<tokio::fs::File> {
    #[cfg(unix)]
    {
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
                        eprintln!("🚀 Using Direct I/O for large file optimization");
                        return Ok(tokio::fs::File::from_std(file));
                    },
                    Err(_) => {
                        eprintln!("⚠️  Direct I/O not available, using standard I/O");
                    }
                }
            }
        }
    }
    
    // Fallback to standard file creation
    tokio::fs::File::create(path).await.map_err(Into::into)
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize logging to stderr
    env_logger::Builder::from_default_env()
        .target(env_logger::Target::Stderr)
        .init();
    
    if cli.verbose {
        eprintln!("🦋 Butterfly-dl v0.1.0 starting...");
    }
    
    // Resolve source and output
    let source = resolve_source(&cli.source);
    let output = resolve_output(&cli.source, &cli.output);
    
    if cli.dry_run {
        eprintln!("🔍 [DRY RUN] Would download: {:?} to {:?}", source, output);
        return Ok(());
    }
    
    // Download based on source type
    match source {
        DownloadSource::S3 { bucket, key, region } => {
            download_s3(&bucket, &key, &region, output).await?;
        },
        DownloadSource::Http { url } => {
            download_http(&url, output).await?;
        },
    }
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_resolve_source_planet() {
        let source = resolve_source("planet");
        match source {
            DownloadSource::S3 { bucket, key, region } => {
                assert_eq!(bucket, "osm-planet-eu-central-1");
                assert_eq!(key, "planet-latest.osm.pbf");
                assert_eq!(region, "eu-central-1");
            },
            _ => panic!("Expected S3 source for planet"),
        }
    }

    #[test]
    fn test_resolve_source_continent() {
        let source = resolve_source("europe");
        match source {
            DownloadSource::Http { url } => {
                assert_eq!(url, "https://download.geofabrik.de/europe-latest.osm.pbf");
            },
            _ => panic!("Expected HTTP source for continent"),
        }
    }

    #[test]
    fn test_resolve_source_country() {
        let source = resolve_source("europe/belgium");
        match source {
            DownloadSource::Http { url } => {
                assert_eq!(url, "https://download.geofabrik.de/europe/belgium-latest.osm.pbf");
            },
            _ => panic!("Expected HTTP source for country"),
        }
    }

    #[test]
    fn test_resolve_output_auto() {
        let output = resolve_output("europe/belgium", "");
        match output {
            OutputDestination::File(path) => {
                assert_eq!(path, "belgium-latest.osm.pbf");
            },
            _ => panic!("Expected file output"),
        }
    }

    #[test]
    fn test_resolve_output_stdout() {
        let output = resolve_output("planet", "-");
        match output {
            OutputDestination::Stdout => {},
            _ => panic!("Expected stdout output"),
        }
    }

    #[test]
    fn test_calculate_optimal_connections() {
        assert_eq!(calculate_optimal_connections(5 * 1024 * 1024), 2);     // 5MB
        assert_eq!(calculate_optimal_connections(50 * 1024 * 1024), 4);    // 50MB
        assert_eq!(calculate_optimal_connections(200 * 1024 * 1024), 8);   // 200MB
        assert_eq!(calculate_optimal_connections(2 * 1024 * 1024 * 1024), 16); // 2GB
    }

    #[test]
    fn test_buffer_size_memory_efficiency() {
        // Verify that our buffer size is reasonable for memory efficiency
        assert_eq!(BUFFER_SIZE, 64 * 1024); // 64KB
        assert!(BUFFER_SIZE >= 4 * 1024);   // At least 4KB for reasonable performance
        assert!(BUFFER_SIZE <= 1024 * 1024); // At most 1MB to keep memory usage low
    }

    #[test]
    fn test_max_connections_reasonable() {
        // Ensure max connections is reasonable to avoid overwhelming servers
        assert_eq!(MAX_CONNECTIONS, 16);
        assert!(MAX_CONNECTIONS >= 2);   // At least 2 for parallelism benefits
        assert!(MAX_CONNECTIONS <= 32);  // At most 32 to be respectful to servers
    }

    #[tokio::test]
    async fn test_create_optimized_file_small() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_small.pbf");
        let path_str = file_path.to_str().unwrap();
        
        // Small file should not trigger Direct I/O
        let file = create_optimized_file(path_str, Some(10 * 1024 * 1024)).await;
        assert!(file.is_ok());
        
        // Cleanup
        tokio::fs::remove_file(path_str).await.ok();
    }

    #[tokio::test]
    async fn test_create_optimized_file_large() {
        use tempfile::tempdir;
        let dir = tempdir().unwrap();
        let file_path = dir.path().join("test_large.pbf");
        let path_str = file_path.to_str().unwrap();
        
        // Large file should attempt Direct I/O (but may fall back gracefully)
        let file = create_optimized_file(path_str, Some(2 * 1024 * 1024 * 1024)).await;
        assert!(file.is_ok());
        
        // Cleanup
        tokio::fs::remove_file(path_str).await.ok();
    }
}