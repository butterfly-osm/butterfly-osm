//! # Geofabrik PBF Downloader
//!
//! A command-line tool and library for downloading OpenStreetMap PBF files from Geofabrik.
//! 
//! ## Features
//! 
//! - Download individual countries or entire continents
//! - Automatic region type validation (continent vs country)
//! - Progress bars for downloads
//! - Proper file organization with continent subdirectories
//! - Docker support with volume mounting
//! 
//! ## Usage
//! 
//! ```bash
//! # Download a country
//! geofabrik-downloader country monaco
//! 
//! # Download a continent  
//! geofabrik-downloader continent antarctica
//! 
//! # Download multiple regions
//! geofabrik-downloader countries monaco,andorra
//! geofabrik-downloader continents antarctica,australia-oceania
//! ```

use clap::{Parser, Subcommand};
use anyhow::{Result, anyhow};
use serde::Deserialize;
use std::fs;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::{Client, ClientBuilder};
use std::io::Write;
use log::{info, warn};
use std::fmt;
use std::sync::Arc;
use std::time::{SystemTime, Duration};
use once_cell::sync::Lazy;
use futures::StreamExt;
use std::sync::atomic::{AtomicU64, Ordering};
use futures::TryStreamExt;

/// Command-line interface for the Geofabrik PBF downloader
#[derive(Parser)]
#[command(name = "geofabrik-downloader")]
#[command(about = "Download OpenStreetMap PBF files from Geofabrik")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
    
    /// Enable dry-run mode (show what would be downloaded without downloading)
    #[arg(long, global = true)]
    dry_run: bool,
}

/// Available commands for downloading different types of regions
#[derive(Subcommand)]
enum Commands {
    /// Download a single country
    Country {
        /// Country name (e.g., monaco)
        name: String,
    },
    /// Download an entire continent
    Continent {
        /// Continent name (e.g., europe)
        name: String,
    },
    /// Download multiple countries
    Countries {
        /// Comma-separated list of countries
        names: String,
    },
    /// Download multiple continents
    Continents {
        /// Comma-separated list of continents
        names: String,
    },
    /// List available regions
    List {
        /// Filter by type: countries, continents, or all
        #[arg(value_enum, default_value = "all")]
        filter: ListFilter,
    },
}

/// Filter options for the list command
#[derive(clap::ValueEnum, Clone, Debug)]
enum ListFilter {
    /// Show all regions
    All,
    /// Show only countries
    Countries,
    /// Show only continents
    Continents,
}

/// Custom error types for better error handling
#[derive(Debug)]
pub enum GeofabrikError {
    /// Region not found in Geofabrik index
    RegionNotFound(String),
    /// Wrong region type (e.g., using country command for continent)
    WrongRegionType { region: String, expected: String, actual: String },
    /// HTTP request failed
    HttpError(reqwest::Error),
    /// File I/O error
    IoError(std::io::Error),
    /// No download URL available
    NoDownloadUrl(String),
    /// API response error
    ApiError(String),
}

impl fmt::Display for GeofabrikError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GeofabrikError::RegionNotFound(region) => {
                write!(f, "Region '{}' not found in Geofabrik index", region)
            }
            GeofabrikError::WrongRegionType { region, expected, actual } => {
                write!(f, "'{}' is a {}, not a {}. Use the {} command instead", 
                       region, actual, expected, actual)
            }
            GeofabrikError::HttpError(err) => write!(f, "HTTP request failed: {}", err),
            GeofabrikError::IoError(err) => write!(f, "File operation failed: {}", err),
            GeofabrikError::NoDownloadUrl(region) => {
                write!(f, "No PBF download URL available for '{}'", region)
            }
            GeofabrikError::ApiError(msg) => write!(f, "API error: {}", msg),
        }
    }
}

impl std::error::Error for GeofabrikError {}

impl From<reqwest::Error> for GeofabrikError {
    fn from(err: reqwest::Error) -> Self {
        GeofabrikError::HttpError(err)
    }
}

impl From<std::io::Error> for GeofabrikError {
    fn from(err: std::io::Error) -> Self {
        GeofabrikError::IoError(err)
    }
}

// Optimized defaults (convention over configuration with autotuning)
const MAX_CHUNK_SIZE: u64 = 16 * 1024 * 1024; // 16MB max
const MIN_CHUNK_SIZE: u64 = 2 * 1024 * 1024; // 2MB min
const ENABLE_PARALLEL_DOWNLOAD: bool = true;

// Global optimized HTTP client for connection reuse
static GLOBAL_CLIENT: Lazy<Client> = Lazy::new(|| {
    create_optimized_client().expect("Failed to create optimized HTTP client")
});

/// Create an optimized HTTP client with performance enhancements
fn create_optimized_client() -> Result<Client> {
    ClientBuilder::new()
        // Aggressive connection pooling for parallel downloads
        .pool_max_idle_per_host(32)
        .pool_idle_timeout(Duration::from_secs(120))
        // Enable compression but be selective
        .gzip(false)  // PBF files are already compressed
        .brotli(false) // Disable compression overhead
        // TCP optimizations for maximum throughput
        .tcp_nodelay(true)
        .tcp_keepalive(Duration::from_secs(30))
        // Aggressive timeouts for faster failure detection
        .timeout(Duration::from_secs(180))
        .connect_timeout(Duration::from_secs(10))
        // Use rustls for better performance than openssl
        .use_rustls_tls()
        // Optimized user agent
        .user_agent("geofabrik-downloader/0.1.0 (high-performance)")
        // Enable HTTP/2 adaptive window scaling
        .http2_adaptive_window(true)
        .http2_max_frame_size(Some(1024 * 1024)) // 1MB HTTP/2 frames
        .build()
        .map_err(|e| anyhow!("Failed to build HTTP client: {}", e))
}

/// Calculate optimal chunk size based on file size and connection count
fn calculate_optimal_chunk_size(file_size: u64, connections: u32) -> u64 {
    // Calculate chunk size that balances parallelism and efficiency
    let base_calculation = file_size / (connections as u64 * 4);
    
    // Clamp to optimal range based on research
    base_calculation.clamp(MIN_CHUNK_SIZE, MAX_CHUNK_SIZE)
}

/// Calculate optimal number of connections based on file size and system capabilities
fn calculate_optimal_connections(file_size: u64) -> u32 {
    // Get system info for intelligent scaling
    let cpu_count = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4) as u32;
    
    let base_connections = match file_size {
        size if size <= 10 * 1024 * 1024 => 2,       // <= 10MB: 2 connections
        size if size <= 100 * 1024 * 1024 => 4,      // <= 100MB: 4 connections  
        size if size <= 512 * 1024 * 1024 => 8,      // <= 512MB: 8 connections
        size if size <= 1024 * 1024 * 1024 => 12,    // <= 1GB: 12 connections
        size if size <= 2048 * 1024 * 1024 => 16,    // <= 2GB: 16 connections
        _ => 20,                                      // > 2GB: 20 connections
    };
    
    // Scale with CPU count but cap at reasonable limits
    let scaled = std::cmp::min(base_connections, cpu_count * 4);
    std::cmp::max(scaled, 2) // Minimum 2 connections
}

/// Check if a file exists and is newer than the renewal period
/// 
/// # Arguments
/// * `file_path` - Path to the file to check
/// * `renewal_period_days` - Number of days after which to consider file stale
/// 
/// # Returns
/// * `bool` - true if file exists and is fresh (newer than renewal period)
fn is_file_fresh(file_path: &str, renewal_period_days: u64) -> bool {
    if let Ok(metadata) = fs::metadata(file_path) {
        if let Ok(modified) = metadata.modified() {
            if let Ok(elapsed) = SystemTime::now().duration_since(modified) {
                let renewal_duration = Duration::from_secs(renewal_period_days * 24 * 60 * 60);
                return elapsed < renewal_duration;
            }
        }
    }
    false
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize logging
    env_logger::init();
    
    let cli = Cli::parse();
    info!("Starting Geofabrik downloader");

    match cli.command {
        Commands::Country { name } => {
            if cli.dry_run {
                println!("🔍 [DRY RUN] Would download country: {}", name);
            } else {
                println!("Downloading country: {}", name);
            }
            download_country(&name, cli.dry_run).await?;
        }
        Commands::Continent { name } => {
            if cli.dry_run {
                println!("🔍 [DRY RUN] Would download continent: {}", name);
            } else {
                println!("Downloading continent: {}", name);
            }
            download_continent(&name, cli.dry_run).await?;
        }
        Commands::Countries { names } => {
            let countries: Vec<&str> = names.split(',').map(|s| s.trim()).collect();
            if cli.dry_run {
                println!("🔍 [DRY RUN] Would download {} countries: {:?}", countries.len(), countries);
            } else {
                println!("Downloading {} countries: {:?}", countries.len(), countries);
            }
            for country in countries {
                download_country(country, cli.dry_run).await?;
            }
        }
        Commands::Continents { names } => {
            let continents: Vec<&str> = names.split(',').map(|s| s.trim()).collect();
            if cli.dry_run {
                println!("🔍 [DRY RUN] Would download {} continents: {:?}", continents.len(), continents);
            } else {
                println!("Downloading {} continents: {:?}", continents.len(), continents);
            }
            for continent in continents {
                download_continent(continent, cli.dry_run).await?;
            }
        }
        Commands::List { filter } => {
            list_regions(filter).await?;
        }
    }

    Ok(())
}

/// List available regions from Geofabrik
/// 
/// # Arguments
/// * `filter` - Filter to apply (all, countries, or continents)
/// 
/// # Returns
/// * `Result<()>` - Success or error details
async fn list_regions(filter: ListFilter) -> Result<()> {
    let client = &*GLOBAL_CLIENT;
    
    info!("Fetching Geofabrik index for listing regions with filter: {:?}", filter);
    println!("🔍 Fetching Geofabrik index...");
    let index = fetch_geofabrik_index(&client).await?;
    
    let mut countries = Vec::new();
    let mut continents = Vec::new();
    
    // Categorize regions
    for feature in &index.features {
        if let Some(_urls) = &feature.properties.urls {
            if feature.properties.parent.is_some() {
                // Has parent = country
                countries.push(&feature.properties);
            } else {
                // No parent = continent
                continents.push(&feature.properties);
            }
        }
    }
    
    // Sort by name
    countries.sort_by(|a, b| a.name.cmp(&b.name));
    continents.sort_by(|a, b| a.name.cmp(&b.name));
    
    match filter {
        ListFilter::All => {
            println!("\n📍 Available Continents ({}):", continents.len());
            for continent in &continents {
                println!("  {} ({})", continent.name, continent.id);
            }
            
            println!("\n🏴 Available Countries ({}):", countries.len());
            for country in &countries {
                let parent = country.parent.as_deref().unwrap_or("unknown");
                println!("  {} ({}) - in {}", country.name, country.id, parent);
            }
        }
        ListFilter::Continents => {
            println!("\n📍 Available Continents ({}):", continents.len());
            for continent in &continents {
                println!("  {} ({})", continent.name, continent.id);
            }
        }
        ListFilter::Countries => {
            println!("\n🏴 Available Countries ({}):", countries.len());
            for country in &countries {
                let parent = country.parent.as_deref().unwrap_or("unknown");
                println!("  {} ({}) - in {}", country.name, country.id, parent);
            }
        }
    }
    
    println!("\n💡 Usage examples:");
    println!("  geofabrik-downloader country monaco");
    println!("  geofabrik-downloader continent europe");
    println!("  geofabrik-downloader countries monaco,andorra");
    
    Ok(())
}

/// Root structure of the Geofabrik index JSON
#[derive(Deserialize, Debug)]
struct GeofabrikIndex {
    /// List of all available regions (countries and continents)
    features: Vec<Feature>,
}

/// A geographic region (country or continent) in the Geofabrik index
#[derive(Deserialize, Debug)]
struct Feature {
    /// Region metadata and download information
    properties: Properties,
}

/// Properties of a geographic region
#[derive(Deserialize, Debug)]
struct Properties {
    /// Unique identifier for the region (e.g., "monaco", "antarctica")
    id: String,
    /// Human-readable name (e.g., "Monaco", "Antarctica")
    name: String,
    /// Parent region identifier, if any (countries have continent parents)
    parent: Option<String>,
    /// Available download URLs for this region
    urls: Option<Urls>,
}

/// Download URLs for different file formats
#[derive(Deserialize, Debug)]
struct Urls {
    /// URL for the PBF (Protocol Buffer Format) file
    pbf: Option<String>,
}

/// Download a single country's PBF file
/// 
/// # Arguments
/// * `country` - Name or ID of the country to download
/// * `dry_run` - If true, only show what would be downloaded without downloading
/// 
/// # Returns
/// * `Result<()>` - Success or error details
async fn download_country(country: &str, dry_run: bool) -> Result<()> {
    let client = &*GLOBAL_CLIENT;
    
    info!("Starting country download: {} (dry_run: {})", country, dry_run);
    println!("🔍 Fetching Geofabrik index...");
    let index = fetch_geofabrik_index(&client).await?;
    
    let feature = find_region(&index, country)?;
    
    // Validate this is actually a country (has a parent)
    if feature.properties.parent.is_none() {
        warn!("Validation failed: '{}' is a continent, not a country", feature.properties.name);
        println!("⚠️  '{}' appears to be a continent, not a country.", feature.properties.name);
        println!("💡 Try: geofabrik-downloader continent {}", country);
        return Err(anyhow!("Use 'continent' command for continent-level downloads"));
    }
    
    if let Some(urls) = &feature.properties.urls {
        if let Some(pbf_url) = &urls.pbf {
            println!("📁 Found {} at: {}", feature.properties.name, pbf_url);
            if dry_run {
                let base_dir = if std::path::Path::new("/data").exists() { "/data" } else { "./data" };
                let continent = feature.properties.parent.as_deref().unwrap_or("unknown");
                let output_path = format!("{}/pbf/{}/{}.pbf", base_dir, continent, country);
                println!("📁 [DRY RUN] Would save to: {}", output_path);
            } else {
                download_pbf(client, country, pbf_url, &feature.properties.parent, false).await?;
            }
        } else {
            return Err(anyhow!("No PBF download available for {}", country));
        }
    } else {
        return Err(anyhow!("No download URLs found for {}", country));
    }
    
    Ok(())
}

/// Download a single continent's PBF file
/// 
/// # Arguments
/// * `continent` - Name or ID of the continent to download
/// * `dry_run` - If true, only show what would be downloaded without downloading
/// 
/// # Returns
/// * `Result<()>` - Success or error details
async fn download_continent(continent: &str, dry_run: bool) -> Result<()> {
    let client = &*GLOBAL_CLIENT;
    
    info!("Starting continent download: {} (dry_run: {})", continent, dry_run);
    println!("🔍 Fetching Geofabrik index...");
    let index = fetch_geofabrik_index(&client).await?;
    
    let feature = find_region(&index, continent)?;
    
    // Validate this is actually a continent (has no parent)
    if feature.properties.parent.is_some() {
        warn!("Validation failed: '{}' is a country/region, not a continent", feature.properties.name);
        println!("⚠️  '{}' appears to be a country/region, not a continent.", feature.properties.name);
        println!("💡 Try: geofabrik-downloader country {}", continent);
        return Err(anyhow!("Use 'country' command for country-level downloads"));
    }
    
    if let Some(urls) = &feature.properties.urls {
        if let Some(pbf_url) = &urls.pbf {
            println!("📁 Found {} at: {}", feature.properties.name, pbf_url);
            if dry_run {
                let base_dir = if std::path::Path::new("/data").exists() { "/data" } else { "./data" };
                let output_path = format!("{}/pbf/{}.pbf", base_dir, continent);
                println!("📁 [DRY RUN] Would save to: {}", output_path);
            } else {
                download_pbf(client, continent, pbf_url, &None, true).await?;
            }
        } else {
            return Err(anyhow!("No PBF download available for {}", continent));
        }
    } else {
        return Err(anyhow!("No download URLs found for {}", continent));
    }
    
    Ok(())
}

/// Fetch the Geofabrik index containing all available regions
/// 
/// # Arguments
/// * `client` - HTTP client for making the request
/// 
/// # Returns
/// * `Result<GeofabrikIndex>` - Parsed index or error
async fn fetch_geofabrik_index(client: &Client) -> Result<GeofabrikIndex> {
    let response = client
        .get("https://download.geofabrik.de/index-v1.json")
        .send()
        .await?;
    
    if !response.status().is_success() {
        return Err(anyhow!("Failed to fetch Geofabrik index: {}", response.status()));
    }
    
    let index: GeofabrikIndex = response.json().await?;
    Ok(index)
}

/// Find a region in the Geofabrik index by name or ID
/// 
/// # Arguments
/// * `index` - The Geofabrik index to search
/// * `region_name` - Name or ID of the region to find (case-insensitive)
/// 
/// # Returns
/// * `Result<&Feature>` - Reference to the found region or error
fn find_region<'a>(index: &'a GeofabrikIndex, region_name: &str) -> Result<&'a Feature> {
    let region_lower = region_name.to_lowercase();
    
    for feature in &index.features {
        if feature.properties.id.to_lowercase() == region_lower || 
           feature.properties.name.to_lowercase() == region_lower {
            return Ok(feature);
        }
    }
    
    Err(anyhow!("Region '{}' not found in Geofabrik index", region_name))
}

/// Download a file using optimized streaming with auto-tuned parallel connections and real-time progress
/// 
/// # Arguments
/// * `client` - HTTP client for downloading
/// * `url` - Download URL for the file
/// * `output_path` - Path where to save the file
/// 
/// # Returns
/// * `Result<()>` - Success or error
async fn download_with_optimized_streaming(
    client: &Client,
    url: &str,
    output_path: &str,
) -> Result<()> {
    // First, check if server supports range requests and get file size
    let head_response = client.head(url).send().await?;
    
    if !head_response.status().is_success() {
        return Err(anyhow!("HEAD request failed: {}", head_response.status()));
    }
    
    let total_size = head_response
        .headers()
        .get("content-length")
        .and_then(|h| h.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .ok_or_else(|| anyhow!("Could not determine file size"))?;
    
    // Check if server supports range requests
    let accepts_ranges = head_response
        .headers()
        .get("accept-ranges")
        .and_then(|h| h.to_str().ok())
        .unwrap_or("");
    
    if accepts_ranges != "bytes" {
        return Err(anyhow!("Server does not support range requests"));
    }
    
    // Auto-calculate optimal parameters
    let connections = calculate_optimal_connections(total_size);
    let chunk_size = calculate_optimal_chunk_size(total_size, connections);
    
    info!("File size: {} bytes, auto-tuned to {} connections with {}MB chunks", 
          total_size, connections, chunk_size / (1024 * 1024));
    
    // Calculate chunk ranges
    let effective_chunk_size = std::cmp::min(chunk_size, total_size / connections as u64);
    let mut ranges = Vec::new();
    let mut start = 0u64;
    
    while start < total_size {
        let end = std::cmp::min(start + effective_chunk_size - 1, total_size - 1);
        ranges.push((start, end));
        start = end + 1;
    }
    
    let actual_connections = std::cmp::min(connections as usize, ranges.len());
    info!("Downloading {} chunks using {} connections", ranges.len(), actual_connections);
    
    // Setup progress tracking with atomic counter for real-time updates
    let downloaded_bytes = Arc::new(AtomicU64::new(0));
    let pb = Arc::new(ProgressBar::new(total_size));
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({bytes_per_sec}, {eta}) {msg}")
        .unwrap_or_else(|_| ProgressStyle::default_bar())
        .progress_chars("#>-"));
    pb.set_message(format!("Auto-tuned: {} connections, {}MB chunks", 
                          actual_connections, chunk_size / (1024 * 1024)));
    
    // Enable steady tick for smoother visual updates
    pb.enable_steady_tick(std::time::Duration::from_millis(120));
    
    // Simple progress updater - just reflect actual progress
    let pb_clone = Arc::clone(&pb);
    let downloaded_clone = Arc::clone(&downloaded_bytes);
    let progress_task = tokio::spawn(async move {
        let mut last_bytes = 0u64;
        while downloaded_clone.load(Ordering::Relaxed) < total_size {
            let current_bytes = downloaded_clone.load(Ordering::Relaxed);
            if current_bytes != last_bytes {
                pb_clone.set_position(current_bytes);
                last_bytes = current_bytes;
            }
            tokio::time::sleep(tokio::time::Duration::from_millis(100)).await; // 10 FPS - enough for visual feedback
        }
        pb_clone.set_position(total_size);
    });
    
    // Pre-allocate file space to avoid fragmentation
    let file = fs::File::create(output_path)?;
    file.set_len(total_size)?; // Pre-allocate full file size
    let mut file = file;
    
    // Use pre-allocated chunk storage with capacity hints
    let mut chunks = Vec::with_capacity(ranges.len());
    for (start, end) in &ranges {
        let chunk_size = (end - start + 1) as usize;
        chunks.push(Vec::with_capacity(chunk_size)); // Pre-allocate exact size
    }
    let mut next_chunk_to_write = 0;
    
    // Stream chunks with controlled concurrency and real-time progress
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
                
                // High-performance streaming with pre-allocated buffer
                let expected_size = (end - start + 1) as usize;
                let mut chunk_data = Vec::with_capacity(expected_size);
                let mut stream = response.bytes_stream();
                
                while let Some(bytes_chunk) = stream.try_next().await? {
                    chunk_data.extend_from_slice(&bytes_chunk);
                    downloaded_bytes.fetch_add(bytes_chunk.len() as u64, Ordering::Relaxed);
                }
                
                // Shrink to actual size to save memory
                chunk_data.shrink_to_fit();
                
                Result::<(usize, Vec<u8>)>::Ok((idx, chunk_data))
            }
        })
        .buffer_unordered(actual_connections);
    
    tokio::pin!(stream);
    
    // Stream and write chunks maintaining order
    while let Some(result) = stream.next().await {
        let (idx, data) = result?;
        chunks[idx] = data;
        
        // Write completed sequential chunks immediately to save memory
        while next_chunk_to_write < chunks.len() && !chunks[next_chunk_to_write].is_empty() {
            file.write_all(&chunks[next_chunk_to_write])?;
            chunks[next_chunk_to_write].clear(); // Free memory
            next_chunk_to_write += 1;
        }
    }
    
    // Write any remaining chunks
    for i in next_chunk_to_write..chunks.len() {
        if !chunks[i].is_empty() {
            file.write_all(&chunks[i])?;
        }
    }
    
    // Stop progress updater and finalize
    progress_task.abort();
    pb.finish_with_message("Download completed!");
    
    info!("Optimized streaming download completed successfully with {} connections", actual_connections);
    Ok(())
}

/// Download a PBF file and save it to the appropriate directory
/// 
/// # Arguments
/// * `client` - HTTP client for downloading
/// * `region` - Name of the region being downloaded
/// * `url` - Download URL for the PBF file
/// * `parent` - Parent region (for organizing directory structure)
/// * `is_continent` - Whether this is a continent-level download
/// 
/// # Returns
/// * `Result<()>` - Success or error
async fn download_pbf(client: &Client, region: &str, url: &str, parent: &Option<String>, is_continent: bool) -> Result<()> {
    // Determine output path
    let base_dir = if std::path::Path::new("/data").exists() { "/data" } else { "./data" };
    
    let (output_dir, output_path) = if is_continent {
        // Continent files go in root pbf directory
        let output_dir = format!("{}/pbf", base_dir);
        let output_path = format!("{}/{}.pbf", output_dir, region);
        (output_dir, output_path)
    } else {
        // Country files go in continent subdirectory
        let continent = parent.as_deref().unwrap_or("unknown");
        let output_dir = format!("{}/pbf/{}", base_dir, continent);
        let output_path = format!("{}/{}.pbf", output_dir, region);
        (output_dir, output_path)
    };
    
    // Create directory if it doesn't exist
    fs::create_dir_all(&output_dir)?;
    
    // Check renewal period from environment variable
    let renewal_period_days = std::env::var("RENEW_PBF_PERIOD")
        .unwrap_or_else(|_| "7".to_string())
        .parse::<u64>()
        .unwrap_or(7);
    
    // Check if file exists and is fresh
    if is_file_fresh(&output_path, renewal_period_days) {
        println!("⏭️  {} already exists and is fresh (less than {} days old), skipping download", region, renewal_period_days);
        println!("📁 Existing file: {}", output_path);
        return Ok(());
    }
    
    println!("📥 Downloading {} to {}", region, output_path);
    
    // Use optimized streaming with auto-tuning (convention over configuration)
    if ENABLE_PARALLEL_DOWNLOAD {
        match download_with_optimized_streaming(client, url, &output_path).await {
            Ok(()) => {
                info!("Successfully downloaded {} using optimized streaming", region);
                println!("✅ Downloaded {} successfully using optimized streaming!", region);
                println!("📁 Saved to: {}", output_path);
                return Ok(());
            }
            Err(e) => {
                warn!("Optimized streaming download failed: {}, falling back to single connection", e);
            }
        }
    }
    
    // Fallback to single connection download
    info!("Using single-connection download for {}", region);
    let response = client.get(url).send().await?;
    
    if !response.status().is_success() {
        return Err(anyhow!("Failed to download {}: {}", url, response.status()));
    }
    
    let total_size = response.content_length().unwrap_or(0);
    
    let pb = ProgressBar::new(total_size);
    pb.set_style(ProgressStyle::default_bar()
        .template("{spinner:.green} [{elapsed_precise}] [{wide_bar:.cyan/blue}] {bytes}/{total_bytes} ({eta})")?
        .progress_chars("#>-"));
    
    let mut file = fs::File::create(&output_path)?;
    
    let bytes = response.bytes().await?;
    file.write_all(&bytes)?;
    let downloaded = bytes.len() as u64;
    pb.set_position(downloaded);
    
    pb.finish_with_message(format!("✅ Downloaded {} successfully!", region));
    println!("📁 Saved to: {}", output_path);
    
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json;

    /// Create a mock Geofabrik index for testing
    fn create_test_index() -> GeofabrikIndex {
        let json = r#"{
            "features": [
                {
                    "properties": {
                        "id": "monaco",
                        "name": "Monaco",
                        "parent": "europe",
                        "urls": {
                            "pbf": "https://download.geofabrik.de/europe/monaco-latest.osm.pbf"
                        }
                    }
                },
                {
                    "properties": {
                        "id": "antarctica",
                        "name": "Antarctica",
                        "parent": null,
                        "urls": {
                            "pbf": "https://download.geofabrik.de/antarctica-latest.osm.pbf"
                        }
                    }
                },
                {
                    "properties": {
                        "id": "europe",
                        "name": "Europe",
                        "parent": null,
                        "urls": {
                            "pbf": "https://download.geofabrik.de/europe-latest.osm.pbf"
                        }
                    }
                }
            ]
        }"#;
        
        serde_json::from_str(json).unwrap()
    }

    #[test]
    fn test_find_region_by_id() {
        let index = create_test_index();
        
        let result = find_region(&index, "monaco");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().properties.name, "Monaco");
    }

    #[test]
    fn test_find_region_by_name() {
        let index = create_test_index();
        
        let result = find_region(&index, "Monaco");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().properties.id, "monaco");
    }

    #[test]
    fn test_find_region_case_insensitive() {
        let index = create_test_index();
        
        let result = find_region(&index, "MONACO");
        assert!(result.is_ok());
        assert_eq!(result.unwrap().properties.name, "Monaco");
    }

    #[test]
    fn test_find_region_not_found() {
        let index = create_test_index();
        
        let result = find_region(&index, "nonexistent");
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("not found"));
    }

    #[test]
    fn test_continent_has_no_parent() {
        let index = create_test_index();
        
        let antarctica = find_region(&index, "antarctica").unwrap();
        assert!(antarctica.properties.parent.is_none());
        
        let europe = find_region(&index, "europe").unwrap();
        assert!(europe.properties.parent.is_none());
    }

    #[test]
    fn test_country_has_parent() {
        let index = create_test_index();
        
        let monaco = find_region(&index, "monaco").unwrap();
        assert!(monaco.properties.parent.is_some());
        assert_eq!(monaco.properties.parent.as_ref().unwrap(), "europe");
    }

    #[test]
    fn test_geofabrik_index_deserialization() {
        let json = r#"{
            "features": [
                {
                    "properties": {
                        "id": "test",
                        "name": "Test Region",
                        "parent": null,
                        "urls": {
                            "pbf": "https://example.com/test.pbf"
                        }
                    }
                }
            ]
        }"#;
        
        let index: GeofabrikIndex = serde_json::from_str(json).unwrap();
        assert_eq!(index.features.len(), 1);
        assert_eq!(index.features[0].properties.name, "Test Region");
    }

    #[test]
    fn test_list_filter_enum() {
        // Test that ListFilter enum works with clap
        use clap::ValueEnum;
        let variants = ListFilter::value_variants();
        assert_eq!(variants.len(), 3);
    }

    /// Integration tests for the full download workflow
    #[cfg(test)]
    mod integration_tests {
        use super::*;
        // use std::path::Path;
        // use tempfile::TempDir;

        #[tokio::test]
        async fn test_fetch_geofabrik_index_integration() {
            let client = &*GLOBAL_CLIENT;
            let result = fetch_geofabrik_index(&client).await;
            
            // Should successfully fetch real index
            assert!(result.is_ok());
            let index = result.unwrap();
            
            // Should have many features
            assert!(index.features.len() > 100);
            
            // Should contain known regions
            let monaco_found = index.features.iter().any(|f| f.properties.id == "monaco");
            assert!(monaco_found, "Monaco should be in the index");
            
            let europe_found = index.features.iter().any(|f| f.properties.id == "europe");
            assert!(europe_found, "Europe should be in the index");
        }

        #[tokio::test]
        async fn test_download_country_dry_run() {
            // Test dry run functionality
            let result = download_country("monaco", true).await;
            assert!(result.is_ok(), "Dry run should succeed without downloading");
        }

        #[tokio::test]
        async fn test_download_continent_dry_run() {
            // Test dry run functionality for continents
            let result = download_continent("antarctica", true).await;
            assert!(result.is_ok(), "Dry run should succeed without downloading");
        }

        #[tokio::test]
        async fn test_country_continent_validation() {
            // Test that country command rejects continents
            let result = download_country("europe", true).await;
            assert!(result.is_err(), "Should reject continent in country command");
            
            // Test that continent command rejects countries
            let result = download_continent("monaco", true).await;
            assert!(result.is_err(), "Should reject country in continent command");
        }

        #[tokio::test]
        async fn test_invalid_region() {
            // Test handling of non-existent regions
            let result = download_country("nonexistent-region", true).await;
            assert!(result.is_err(), "Should fail for non-existent region");
            
            let result = download_continent("nonexistent-continent", true).await;
            assert!(result.is_err(), "Should fail for non-existent continent");
        }

        #[tokio::test]
        async fn test_autotuning_defaults() {
            // Test that autotuning defaults are sane
            assert_eq!(MAX_CHUNK_SIZE, 16 * 1024 * 1024);
            assert_eq!(MIN_CHUNK_SIZE, 2 * 1024 * 1024);
            assert_eq!(ENABLE_PARALLEL_DOWNLOAD, true);
            
            // Test autotuning functions (results depend on CPU count, so test ranges)
            let small_connections = calculate_optimal_connections(5 * 1024 * 1024);
            assert!(small_connections >= 2 && small_connections <= 4); // 5MB -> 2-4 connections
            
            let medium_connections = calculate_optimal_connections(50 * 1024 * 1024);  
            assert!(medium_connections >= 4 && medium_connections <= 8); // 50MB -> 4-8 connections
            
            let large_connections = calculate_optimal_connections(500 * 1024 * 1024);
            assert!(large_connections >= 8 && large_connections <= 16); // 500MB -> 8-16 connections
            
            let xl_connections = calculate_optimal_connections(3 * 1024 * 1024 * 1024);
            assert!(xl_connections >= 16 && xl_connections <= 32); // 3GB -> 16-32 connections
            
            // Test chunk size calculation
            let chunk_size = calculate_optimal_chunk_size(100 * 1024 * 1024, 4);
            assert!(chunk_size >= MIN_CHUNK_SIZE);
            assert!(chunk_size <= MAX_CHUNK_SIZE);
        }

        #[test]
        fn test_file_freshness_check() {
            // Test that non-existent files are not fresh
            assert_eq!(is_file_fresh("/non/existent/file.pbf", 7), false);
            
            // Test with a real file (Cargo.toml should exist)
            // A file modified recently should be fresh for a 7-day period
            assert_eq!(is_file_fresh("Cargo.toml", 7), true);
            
            // A file should not be fresh for a 0-day period
            assert_eq!(is_file_fresh("Cargo.toml", 0), false);
        }

        #[tokio::test]
        async fn test_list_regions_integration() {
            // Test list functionality
            let result = list_regions(ListFilter::All).await;
            assert!(result.is_ok(), "List all should succeed");
            
            let result = list_regions(ListFilter::Countries).await;
            assert!(result.is_ok(), "List countries should succeed");
            
            let result = list_regions(ListFilter::Continents).await;
            assert!(result.is_ok(), "List continents should succeed");
        }
    }
}

