use butterfly_shrink::{Config, Preset, Processor};
use butterfly_shrink::db::NodeIndex;
use butterfly_shrink::processor::check_tmpfs;
use clap::Parser;
use std::path::PathBuf;
use std::env;
use uuid::Uuid;

#[derive(Parser)]
#[command(
    author,
    version,
    about = "Single-pass routing-optimized PBF processor",
    long_about = "Transform OSM PBF files into minimal routing-ready versions"
)]
struct Cli {
    /// Input PBF file (or - for stdin)
    #[arg(value_name = "INPUT")]
    input: String,

    /// Output PBF file (or - for stdout)
    #[arg(value_name = "OUTPUT")]
    output: String,
    
    /// Grid resolution in meters (1, 2, 5, or 10)
    #[arg(short, long, default_value = "5")]
    grid: f64,
    
    /// Highway preset (car, bike, or foot)
    #[arg(short, long)]
    preset: Option<String>,
    
    /// Configuration file (YAML)
    #[arg(short, long)]
    config: Option<PathBuf>,
    
    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,
    
    /// Use Direct I/O (Unix only)
    #[arg(long)]
    direct_io: bool,
    
    /// Number of parallel workers
    #[arg(short = 'j', long)]
    workers: Option<usize>,
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();
    
    // Initialize logging
    let log_level = if cli.verbose { "debug" } else { "info" };
    env::set_var("RUST_LOG", log_level);
    env_logger::init();
    
    // Validate grid size
    if ![1.0, 2.0, 5.0, 10.0].contains(&cli.grid) {
        anyhow::bail!("Grid size must be 1, 2, 5, or 10 meters");
    }
    
    // Create configuration
    let mut config = if let Some(config_path) = &cli.config {
        let yaml_str = std::fs::read_to_string(config_path)?;
        let yaml_config: butterfly_shrink::config::YamlConfig = serde_yaml::from_str(&yaml_str)?;
        Config::from_yaml(&yaml_config)
    } else {
        Config::default()
    };
    
    // Apply CLI overrides
    config.grid_size_m = cli.grid;
    config.direct_io = cli.direct_io;
    
    if let Some(workers) = cli.workers {
        config.num_workers = workers;
    }
    
    // Apply preset
    if let Some(preset_str) = &cli.preset {
        let preset = match preset_str.as_str() {
            "car" => Preset::Car,
            "bike" => Preset::Bike,
            "foot" => Preset::Foot,
            _ => anyhow::bail!("Invalid preset: {}", preset_str),
        };
        config.apply_preset(preset);
    }
    
    // Setup temporary directory
    let tmp_base = env::var("TMPDIR").unwrap_or_else(|_| "/tmp".to_string());
    let tmp_dir = PathBuf::from(&tmp_base).join(format!("butterfly-shrink-{}", Uuid::new_v4()));
    std::fs::create_dir_all(&tmp_dir)?;
    
    // Check for tmpfs
    if check_tmpfs(&tmp_dir) {
        log::warn!("WARNING: TMPDIR appears to be tmpfs (RAM-backed)");
        log::warn!("  Current: {} (tmpfs filesystem detected)", tmp_base);
        log::warn!("  Action: export TMPDIR=/mnt/ssd/tmp");
    }
    
    // Create RocksDB index
    let db_path = tmp_dir.join("node_index");
    log::info!("Using RocksDB at {}", db_path.display());
    
    let node_index = NodeIndex::new(&db_path, config.rocksdb_cache_mb)?;
    
    // Process the file
    let input_path = if cli.input == "-" {
        anyhow::bail!("Stdin input not yet implemented");
    } else {
        PathBuf::from(&cli.input)
    };
    
    let output_path = if cli.output == "-" {
        anyhow::bail!("Stdout output not yet implemented");
    } else {
        PathBuf::from(&cli.output)
    };
    
    log::info!("Grid resolution: {}m", config.grid_size_m);
    log::info!("Highway preset: {:?}", cli.preset.as_deref().unwrap_or("car"));
    log::info!("Parallel workers: {}", config.num_workers);
    
    let mut processor = Processor::new(config, node_index);
    let stats = processor.process(&input_path, &output_path)?;
    
    // Print statistics
    println!("\nbutterfly-shrink statistics:");
    println!("  Input:  {} nodes, {} ways, {} relations",
        stats.input_nodes, stats.input_ways, stats.input_relations);
    println!("  Output: {} nodes, {} ways, {} relations",
        stats.output_nodes, stats.output_ways, stats.output_relations);
    
    if stats.input_nodes > 0 {
        let node_reduction = 100.0 - (stats.output_nodes as f64 / stats.input_nodes as f64 * 100.0);
        println!("  Reduction: {:.1}% nodes", node_reduction);
    }
    
    println!("  Grid cells: {}", stats.grid_cells);
    
    if stats.dropped_ways > 0 {
        println!("  Dropped ways: {}", stats.dropped_ways);
    }
    if stats.failed_restrictions > 0 {
        println!("  Failed restrictions: {}", stats.failed_restrictions);
    }
    
    // Cleanup
    std::fs::remove_dir_all(&tmp_dir)?;
    
    Ok(())
}
