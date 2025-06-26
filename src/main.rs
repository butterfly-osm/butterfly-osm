//! # Butterfly-dl CLI
//!
//! Command-line interface for the butterfly-dl library.
//! Provides a curl-like interface for downloading OpenStreetMap data files.

use clap::Parser;
use butterfly_dl::{Result, OverwriteBehavior, DownloadOptions};
use log::error;

mod cli;

/// Command-line interface for butterfly-dl
#[derive(Parser)]
#[command(name = "butterfly-dl")]
#[command(about = "Optimized OpenStreetMap data downloader with HTTP support")]
#[command(long_about = "Downloads single OpenStreetMap files efficiently:
  butterfly-dl planet              # Download planet file (81GB) from HTTP
  butterfly-dl europe              # Download Europe continent from HTTP
  butterfly-dl europe/belgium      # Download Belgium from HTTP
  butterfly-dl europe/monaco -     # Stream Monaco to stdout

File Overwrite Behavior:
  By default, you'll be prompted if destination file exists
  --force                          # Overwrite without asking
  --no-clobber                     # Never overwrite, fail if file exists")]
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
    
    /// Force overwrite existing files without prompting
    #[arg(short, long)]
    force: bool,
    
    /// Never overwrite existing files (fail if destination exists)
    #[arg(long)]
    no_clobber: bool,
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
                let name = path.split('/').next_back().unwrap_or(path);
                format!("{name}-latest.osm.pbf")
            },
            continent => format!("{continent}-latest.osm.pbf"),
        };
        OutputDestination::File(filename)
    } else {
        OutputDestination::File(output.to_string())
    }
}

#[tokio::main]
async fn main() {
    if let Err(e) = run().await {
        error!("❌ Error: {e}");
        std::process::exit(1);
    }
}

async fn run() -> Result<()> {
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
        let source = &cli.source;
        eprintln!("🔍 [DRY RUN] Would download: {source} to {output:?}");
        return Ok(());
    }
    
    // Validate conflicting flags
    if cli.force && cli.no_clobber {
        eprintln!("❌ Error: --force and --no-clobber cannot be used together");
        std::process::exit(1);
    }
    
    // Handle different output destinations
    match output {
        OutputDestination::File(file_path) => {
            download_to_file(&cli.source, &file_path, cli.verbose, cli.force, cli.no_clobber).await?;
        }
        OutputDestination::Stdout => {
            download_to_stdout(&cli.source, cli.verbose).await?;
        }
    }
    
    Ok(())
}

/// Download to a file with progress bar
async fn download_to_file(source: &str, file_path: &str, verbose: bool, force: bool, no_clobber: bool) -> Result<()> {
    if verbose {
        // Show download source information
        show_download_info(source);
    }
    
    eprintln!("📁 Saving to: {file_path}");
    
    // Determine overwrite behavior from CLI flags
    let overwrite = if force {
        OverwriteBehavior::Force
    } else if no_clobber {
        OverwriteBehavior::NeverOverwrite
    } else {
        OverwriteBehavior::Prompt
    };
    
    // Create progress bar manager
    let progress_manager = cli::ProgressManager::new(0, &format!("🌐 Downloading {source}"));
    
    // Create download options with overwrite behavior
    let options = DownloadOptions {
        overwrite,
        progress: Some(std::sync::Arc::new({
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
        })),
        ..Default::default()
    };
    
    // Use library with custom options
    butterfly_dl::get_with_options(source, Some(file_path), options).await?;
    
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
        .map_err(butterfly_dl::Error::IoError)?;
    
    Ok(())
}

/// Show information about the download source
fn show_download_info(source: &str) {
    match source {
        "planet" => {
            eprintln!("🌐 Downloading from HTTP: https://planet.openstreetmap.org/pbf/planet-latest.osm.pbf");
        }
        path if path.contains('/') => {
            eprintln!("🌐 Downloading from HTTP: https://download.geofabrik.de/{path}-latest.osm.pbf");
        }
        continent => {
            eprintln!("🌐 Downloading from HTTP: https://download.geofabrik.de/{continent}-latest.osm.pbf");
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