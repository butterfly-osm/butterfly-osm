//! # Butterfly-dl CLI
//!
//! Command-line interface for the butterfly-dl library.
//! Provides a curl-like interface for downloading OpenStreetMap data files.

use clap::Parser;
use butterfly_dl::Result;

mod cli;

/// Command-line interface for butterfly-dl
#[derive(Parser)]
#[command(name = "butterfly-dl")]
#[command(about = "Optimized OpenStreetMap data downloader with HTTP support")]
#[command(long_about = "Downloads single OpenStreetMap files efficiently:
  butterfly-dl planet              # Download planet file (81GB) from HTTP
  butterfly-dl europe              # Download Europe continent from HTTP
  butterfly-dl europe/belgium      # Download Belgium from HTTP
  butterfly-dl europe/monaco -     # Stream Monaco to stdout")]
#[command(version = env!("BUTTERFLY_VERSION"))]
struct Cli {
    /// Source to download: "planet" (HTTP), "europe" (continent), or "europe/belgium" (country/region)
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

/// Output destination types
#[derive(Debug)]
enum OutputDestination {
    File(String),
    Stdout,
}

/// Resolve output destination from CLI arguments
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

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    
    // Initialize logging to stderr
    env_logger::Builder::from_default_env()
        .target(env_logger::Target::Stderr)
        .init();
    
    if cli.verbose {
        eprintln!("🦋 Butterfly-dl v{} starting...", env!("BUTTERFLY_VERSION"));
    }
    
    // Resolve output destination
    let output = resolve_output(&cli.source, &cli.output);
    
    if cli.dry_run {
        eprintln!("🔍 [DRY RUN] Would download: {} to {:?}", cli.source, output);
        return Ok(());
    }
    
    // Handle different output destinations
    match output {
        OutputDestination::File(file_path) => {
            download_to_file(&cli.source, &file_path, cli.verbose).await?;
        }
        OutputDestination::Stdout => {
            download_to_stdout(&cli.source, cli.verbose).await?;
        }
    }
    
    Ok(())
}

/// Download to a file with progress bar
async fn download_to_file(source: &str, file_path: &str, verbose: bool) -> Result<()> {
    if verbose {
        // Show download source information
        show_download_info(source);
    }
    
    eprintln!("📁 Saving to: {}", file_path);
    
    // Create progress bar manager
    let progress_manager = cli::ProgressManager::new(0, &format!("🌐 Downloading {}", source));
    
    // Use library with progress callback
    butterfly_dl::get_with_progress(
        source,
        Some(file_path),
        {
            let pb = progress_manager.pb.clone();
            move |downloaded, total| {
                if pb.length().unwrap_or(0) != total {
                    pb.set_length(total);
                }
                pb.set_position(downloaded);
                if downloaded >= total {
                    pb.finish_with_message("✅ Download completed!");
                }
            }
        }
    ).await?;
    
    Ok(())
}

/// Download to stdout (no progress bar)
async fn download_to_stdout(source: &str, verbose: bool) -> Result<()> {
    if verbose {
        show_download_info(source);
        eprintln!("📡 Streaming to stdout");
    }
    
    // Get stream and pipe to stdout
    let mut stream = butterfly_dl::get_stream(source).await?;
    let mut stdout = tokio::io::stdout();
    
    tokio::io::copy(&mut stream, &mut stdout).await
        .map_err(|e| butterfly_dl::Error::IoError(e))?;
    
    Ok(())
}

/// Show information about the download source
fn show_download_info(source: &str) {
    match source {
        "planet" => {
            eprintln!("🌐 Downloading from HTTP: https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf");
        }
        path if path.contains('/') => {
            eprintln!("🌐 Downloading from HTTP: https://download.geofabrik.de/{}-latest.osm.pbf", path);
        }
        continent => {
            eprintln!("🌐 Downloading from HTTP: https://download.geofabrik.de/{}-latest.osm.pbf", continent);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

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
    fn test_resolve_output_custom_file() {
        let output = resolve_output("planet", "my-planet.pbf");
        match output {
            OutputDestination::File(path) => {
                assert_eq!(path, "my-planet.pbf");
            },
            _ => panic!("Expected file output"),
        }
    }
}