use butterfly_shrink::{Config, Preset, Processor};
use butterfly_shrink::batch::BatchConfig;
use butterfly_shrink::processor::check_tmpfs;
use clap::Parser;
use std::path::PathBuf;
use std::env;
use uuid::Uuid;

// Use jemalloc as global allocator for better memory management
#[cfg(not(target_env = "msvc"))]
use tikv_jemallocator::Jemalloc;

#[cfg(not(target_env = "msvc"))]
#[global_allocator]
static GLOBAL: Jemalloc = Jemalloc;

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
    
    /// Enable two-pass mode for better memory efficiency
    #[arg(long)]
    two_pass: bool,
    
    /// Use BCSI index instead of RocksDB (experimental, single-pass with 4GB cap)
    #[arg(long)]
    bcsi: bool,
    
    /// Use emergency BCSI mode with strict memory controls (guaranteed <4GB)
    #[arg(long)]
    bcsi_emergency: bool,
    
    /// Debug element reading (counts elements without processing)
    #[arg(long)]
    debug_elements: bool,
    
    /// Number of parallel workers
    #[arg(short = 'j', long)]
    workers: Option<usize>,
    
    // Cache configuration
    /// LRU cache size in MB for node mappings (default: 128)
    #[arg(long, default_value = "128")]
    cache_mb: usize,
    
    /// RocksDB block cache size in MB (default: 128)
    #[arg(long, default_value = "128")]
    db_cache_mb: usize,
    
    // Batching configuration
    /// Maximum ways per batch (default: 50000)
    #[arg(long, default_value = "50000")]
    batch_ways: usize,
    
    /// Maximum unique node IDs per batch (default: 1500000)
    #[arg(long, default_value = "1500000")]
    batch_ids: usize,
    
    // Compression configuration
    /// Zstd compression level 1-22 (default: 6)
    #[arg(long, default_value = "6")]
    zstd: u32,
    
    /// PBF output block size in KB (default: 256)
    #[arg(long, default_value = "256")]
    block_size: usize,
    
    // Optional features
    /// Disable compaction at phase boundary
    #[arg(long)]
    no_compact: bool,
    
    /// Enable tile bucketing for ways (experimental)
    #[arg(long)]
    tile_bucket: bool,
    
    /// Disable autotuning
    #[arg(long)]
    no_autotune: bool,
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
    
    // Cache settings
    config.set_cache_mb(cli.cache_mb);
    config.db_cache_mb = cli.db_cache_mb;
    
    // Batching settings
    config.batch_ways = cli.batch_ways;
    config.batch_unique_nodes = cli.batch_ids;
    
    // Compression settings
    config.zstd_level = cli.zstd.min(22);
    config.pbf_block_size_kb = cli.block_size;
    
    // Optional features
    config.compact_after_nodes = !cli.no_compact;
    config.enable_tile_bucketing = cli.tile_bucket;
    config.enable_autotuning = !cli.no_autotune;
    
    if let Some(workers) = cli.workers {
        config.num_workers = workers;
    }
    
    // Validate configuration
    config.validate();
    
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
    
    // Prepare RocksDB path (will be opened by processor)
    let db_path = tmp_dir.join("node_index");
    log::info!("RocksDB will be created at {}", db_path.display());
    
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
    
    log::info!("Configuration:");
    log::info!("  Grid resolution: {}m", config.grid_size_m);
    log::info!("  Highway preset: {:?}", cli.preset.as_deref().unwrap_or("car"));
    log::info!("  Cache: {}MB LRU, {}MB RocksDB", config.cache_mb, config.db_cache_mb);
    log::info!("  Batching: {} ways, {} unique nodes", config.batch_ways, config.batch_unique_nodes);
    log::info!("  Compression: Zstd level {}", config.zstd_level);
    log::info!("  Features: compact={}, tile={}, autotune={}", 
        config.compact_after_nodes, config.enable_tile_bucketing, config.enable_autotuning);
    
    // Create batch configuration
    let batch_config = BatchConfig {
        max_ways: config.batch_ways,
        max_unique_nodes: config.batch_unique_nodes,
        cache_capacity: config.lru_cache_size,
        batch_memory_limit_mb: config.batch_memory_limit_mb,
        max_multiget_keys: config.max_multiget_keys,
        enable_tile_bucketing: config.enable_tile_bucketing,
        tile_grid_degrees: config.tile_grid_degrees,
        max_tiles_in_memory: config.max_tiles_in_memory,
    };
    
    // Choose between single-pass, two-pass, BCSI, or emergency mode
    if cli.bcsi_emergency {
        // Emergency BCSI mode - guaranteed <4GB with all fixes
        println!("Running in EMERGENCY BCSI mode (strict <4GB guarantee)...");
        println!("Features: shared slabs, serialized lookups, byte-accurate cache");
        let mut emergency_processor = butterfly_shrink::bcsi_processor_emergency::BcsiProcessorEmergency::new(config)?;
        let stats = emergency_processor.process(&input_path, &output_path)?;
        
        // Print statistics
        println!("\nbutterfly-shrink EMERGENCY statistics:");
        println!("  Nodes: {} → {} representatives", stats.total_nodes, stats.rep_nodes);
        println!("  Ways: {} → {} written", stats.total_ways, stats.written_ways);
        println!("  Relations: {} → {} written", stats.total_relations, stats.written_relations);
        println!("  Total time: {:.2}s", stats.elapsed_secs);
        
        if stats.total_nodes > 0 {
            let node_reduction = 100.0 - (stats.rep_nodes as f64 / stats.total_nodes as f64 * 100.0);
            println!("  Node reduction: {:.1}%", node_reduction);
        }
    } else if cli.bcsi {
        // BCSI mode - single-pass with hard 4GB memory cap
        println!("Running in BCSI mode (4GB memory cap)...");
        let mut bcsi_processor = butterfly_shrink::bcsi_processor::BcsiProcessor::new(config)?;
        let stats = bcsi_processor.process(&input_path, &output_path)?;
        
        // Print statistics
        println!("\nbutterfly-shrink BCSI statistics:");
        println!("  Nodes: {} → {} representatives", stats.total_nodes, stats.rep_nodes);
        println!("  Ways: {} → {} written", stats.total_ways, stats.written_ways);
        println!("  Relations: {} → {} written", stats.total_relations, stats.written_relations);
        println!("  Total time: {:.2}s", stats.elapsed_secs);
        
        if stats.total_nodes > 0 {
            let node_reduction = 100.0 - (stats.rep_nodes as f64 / stats.total_nodes as f64 * 100.0);
            println!("  Node reduction: {:.1}%", node_reduction);
        }
    } else if cli.two_pass {
        // Two-pass mode
        println!("Running in two-pass mode...");
        let two_pass_processor = butterfly_shrink::two_pass::TwoPassProcessor::new(config)?;
        
        // Pass 1: Nodes
        let pass1_stats = two_pass_processor.pass1_nodes(&input_path, &output_path)?;
        println!("Pass 1 complete: {} nodes → {} representatives", 
            pass1_stats.total_nodes, pass1_stats.rep_nodes);
        
        // Pass 2: Ways
        let pass2_stats = two_pass_processor.pass2_ways(&input_path, &output_path)?;
        println!("Pass 2 complete: {} ways → {} written", 
            pass2_stats.total_ways, pass2_stats.written_ways);
        
        // Print summary statistics
        println!("\nbutterfly-shrink two-pass statistics:");
        println!("  Nodes: {} → {} representatives", pass1_stats.total_nodes, pass1_stats.rep_nodes);
        println!("  Ways: {} → {} written", pass2_stats.total_ways, pass2_stats.written_ways);
    } else {
        // Single-pass mode (existing code)
        let mut processor = Processor::new(config, db_path)
            .with_batch_config(batch_config);
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
    }
    
    // Cleanup
    std::fs::remove_dir_all(&tmp_dir)?;
    
    Ok(())
}
