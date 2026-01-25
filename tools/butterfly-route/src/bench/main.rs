//! Benchmark harness for bulk performance testing
//!
//! Supports:
//! - Single isochrone benchmarks
//! - Batch isochrone benchmarks
//! - Matrix tile benchmarks
//!
//! Outputs: p50/p95/p99 times + detailed counters

use std::cmp::Reverse;
use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use rand::prelude::*;

use butterfly_route::range::phast::{PhastEngine, PhastStats};
use butterfly_route::range::frontier::FrontierExtractor;
use butterfly_route::range::contour::{generate_contour, GridConfig};
use butterfly_route::range::sparse_contour::{generate_sparse_contour, SparseContourConfig};
use butterfly_route::range::batched_isochrone::BatchedIsochroneEngine;
use butterfly_route::matrix::batched_phast::{BatchedPhastEngine, BatchedPhastStats, K_LANES};
use butterfly_route::formats::CchWeightsFile;
use butterfly_route::step9::state::DownReverseAdj;
use butterfly_route::matrix::bucket_ch::{table_bucket, table_bucket_optimized, table_bucket_full_flat, DownReverseAdjFlat, UpAdjFlat, UpReverseAdjFlat, BucketM2MStats, BucketM2MEngine};

#[derive(Parser)]
#[command(name = "butterfly-bench")]
#[command(about = "Benchmark harness for butterfly-route performance testing")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Benchmark single isochrone queries
    Isochrone {
        /// Data directory containing prebuilt Belgium CCH files
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode (car, bike, foot)
        #[arg(long, default_value = "bike")]
        mode: String,

        /// Threshold in milliseconds
        #[arg(long, default_value = "600000")]
        threshold_ms: u32,

        /// Number of random origins to test
        #[arg(long, default_value = "100")]
        n_origins: usize,

        /// Random seed for reproducibility
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Benchmark batch of isochrones
    IsochoneBatch {
        /// Data directory containing prebuilt Belgium CCH files
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "bike")]
        mode: String,

        /// Threshold in milliseconds
        #[arg(long, default_value = "600000")]
        threshold_ms: u32,

        /// Batch size (number of origins per batch)
        #[arg(long, default_value = "100")]
        batch_size: usize,

        /// Number of batches to run
        #[arg(long, default_value = "10")]
        n_batches: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Benchmark PHAST-only (no polygon generation)
    PhastOnly {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "bike")]
        mode: String,

        /// Number of queries
        #[arg(long, default_value = "1000")]
        n_queries: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Compare K-lane batched PHAST vs single-source PHAST
    BatchedPhast {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "bike")]
        mode: String,

        /// Number of sources to benchmark
        #[arg(long, default_value = "64")]
        n_sources: usize,

        /// Number of targets (0 = all nodes)
        #[arg(long, default_value = "1000")]
        n_targets: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Compare active-set gating vs naive bounded PHAST
    ActiveSet {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "car")]
        mode: String,

        /// Time threshold in milliseconds
        #[arg(long, default_value = "600000")]
        threshold_ms: u32,

        /// Number of queries
        #[arg(long, default_value = "50")]
        n_queries: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Compare K-lane batched isochrones vs single-source isochrones
    BatchedIsochrone {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "car")]
        mode: String,

        /// Time threshold in milliseconds
        #[arg(long, default_value = "120000")]
        threshold_ms: u32,

        /// Number of origins to process
        #[arg(long, default_value = "32")]
        n_origins: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Compare blocked vs non-blocked relaxation (cache efficiency test)
    BlockedRelaxation {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "car")]
        mode: String,

        /// Number of sources to benchmark
        #[arg(long, default_value = "64")]
        n_sources: usize,

        /// Number of targets (0 = all nodes)
        #[arg(long, default_value = "1000")]
        n_targets: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Compare block-gated vs active-set gated PHAST
    BlockGated {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "foot")]
        mode: String,

        /// Time threshold in milliseconds
        #[arg(long, default_value = "300000")]
        threshold_ms: u32,

        /// Number of queries
        #[arg(long, default_value = "100")]
        n_queries: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Benchmark adaptive gating strategy (switches based on active block ratio)
    Adaptive {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "foot")]
        mode: String,

        /// Comma-separated thresholds in milliseconds
        #[arg(long, default_value = "60000,300000,600000")]
        thresholds: String,

        /// Number of queries per threshold
        #[arg(long, default_value = "50")]
        n_queries: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Compare K-lane block-gated PHAST vs regular batched PHAST for bounded queries
    KlaneBounded {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "foot")]
        mode: String,

        /// Time threshold in milliseconds
        #[arg(long, default_value = "300000")]
        threshold_ms: u32,

        /// Number of batches to run
        #[arg(long, default_value = "8")]
        n_batches: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Analyze reachability fraction at various thresholds (for rPHAST decision)
    Reachability {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "foot")]
        mode: String,

        /// Comma-separated list of thresholds in ms (e.g., "60000,120000,300000,600000")
        #[arg(long, default_value = "60000,120000,300000,600000")]
        thresholds: String,

        /// Number of random origins to sample
        #[arg(long, default_value = "20")]
        n_origins: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Benchmark matrix tile streaming (tests the streaming compute path)
    MatrixStream {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "car")]
        mode: String,

        /// Number of sources
        #[arg(long, default_value = "1000")]
        n_sources: usize,

        /// Number of targets
        #[arg(long, default_value = "1000")]
        n_targets: usize,

        /// Source tile size (default 8 = K_LANES)
        #[arg(long, default_value = "8")]
        src_tile_size: usize,

        /// Destination tile size
        #[arg(long, default_value = "256")]
        dst_tile_size: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Benchmark bucket-based many-to-many CH (for sparse matrices)
    BucketM2M {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "car")]
        mode: String,

        /// Comma-separated matrix sizes to test (e.g., "10,25,50,100")
        #[arg(long, default_value = "10,25,50,100")]
        sizes: String,

        /// Use parallel implementation
        #[arg(long)]
        parallel: bool,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },

    /// Compare dense vs sparse contour generation
    ContourCompare {
        /// Data directory
        #[arg(long)]
        data_dir: PathBuf,

        /// Transport mode
        #[arg(long, default_value = "bike")]
        mode: String,

        /// Threshold in deciseconds
        #[arg(long, default_value = "6000")]
        threshold_ds: u32,

        /// Number of queries
        #[arg(long, default_value = "50")]
        n_queries: usize,

        /// Random seed
        #[arg(long, default_value = "42")]
        seed: u64,
    },
}

/// Aggregated statistics across multiple runs
#[derive(Default)]
struct AggregatedStats {
    upward_pq_pushes: u64,
    upward_pq_pops: u64,
    upward_relaxations: u64,
    upward_settled: u64,
    downward_relaxations: u64,
    downward_improved: u64,
    upward_time_ms: u64,
    downward_time_ms: u64,
    frontier_edges: u64,
    grid_cells: u64,
    contour_vertices: u64,
}

impl AggregatedStats {
    fn add(&mut self, phast: &PhastStats) {
        self.upward_pq_pushes += phast.upward_pq_pushes as u64;
        self.upward_pq_pops += phast.upward_pq_pops as u64;
        self.upward_relaxations += phast.upward_relaxations as u64;
        self.upward_settled += phast.upward_settled as u64;
        self.downward_relaxations += phast.downward_relaxations as u64;
        self.downward_improved += phast.downward_improved as u64;
        self.upward_time_ms += phast.upward_time_ms;
        self.downward_time_ms += phast.downward_time_ms;
    }

    fn avg(&self, n: usize) -> AggregatedStats {
        let n = n.max(1) as u64;
        AggregatedStats {
            upward_pq_pushes: self.upward_pq_pushes / n,
            upward_pq_pops: self.upward_pq_pops / n,
            upward_relaxations: self.upward_relaxations / n,
            upward_settled: self.upward_settled / n,
            downward_relaxations: self.downward_relaxations / n,
            downward_improved: self.downward_improved / n,
            upward_time_ms: self.upward_time_ms / n,
            downward_time_ms: self.downward_time_ms / n,
            frontier_edges: self.frontier_edges / n,
            grid_cells: self.grid_cells / n,
            contour_vertices: self.contour_vertices / n,
        }
    }
}

fn main() -> anyhow::Result<()> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Isochrone {
            data_dir,
            mode,
            threshold_ms,
            n_origins,
            seed,
        } => run_isochrone_bench(&data_dir, &mode, threshold_ms, n_origins, seed),

        Commands::IsochoneBatch {
            data_dir,
            mode,
            threshold_ms,
            batch_size,
            n_batches,
            seed,
        } => run_batch_bench(&data_dir, &mode, threshold_ms, batch_size, n_batches, seed),

        Commands::PhastOnly {
            data_dir,
            mode,
            n_queries,
            seed,
        } => run_phast_bench(&data_dir, &mode, n_queries, seed),

        Commands::BatchedPhast {
            data_dir,
            mode,
            n_sources,
            n_targets,
            seed,
        } => run_batched_phast_bench(&data_dir, &mode, n_sources, n_targets, seed),

        Commands::ActiveSet {
            data_dir,
            mode,
            threshold_ms,
            n_queries,
            seed,
        } => run_active_set_bench(&data_dir, &mode, threshold_ms, n_queries, seed),

        Commands::BatchedIsochrone {
            data_dir,
            mode,
            threshold_ms,
            n_origins,
            seed,
        } => run_batched_isochrone_bench(&data_dir, &mode, threshold_ms, n_origins, seed),

        Commands::BlockedRelaxation {
            data_dir,
            mode,
            n_sources,
            n_targets,
            seed,
        } => run_blocked_relaxation_bench(&data_dir, &mode, n_sources, n_targets, seed),

        Commands::BlockGated {
            data_dir,
            mode,
            threshold_ms,
            n_queries,
            seed,
        } => run_block_gated_bench(&data_dir, &mode, threshold_ms, n_queries, seed),

        Commands::Adaptive {
            data_dir,
            mode,
            thresholds,
            n_queries,
            seed,
        } => {
            let thresholds: Vec<u32> = thresholds
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            run_adaptive_bench(&data_dir, &mode, &thresholds, n_queries, seed)
        }

        Commands::KlaneBounded {
            data_dir,
            mode,
            threshold_ms,
            n_batches,
            seed,
        } => run_klane_bounded_bench(&data_dir, &mode, threshold_ms, n_batches, seed),

        Commands::Reachability {
            data_dir,
            mode,
            thresholds,
            n_origins,
            seed,
        } => {
            let thresholds: Vec<u32> = thresholds
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            run_reachability_analysis(&data_dir, &mode, &thresholds, n_origins, seed)
        }

        Commands::MatrixStream {
            data_dir,
            mode,
            n_sources,
            n_targets,
            src_tile_size,
            dst_tile_size,
            seed,
        } => run_matrix_stream_bench(&data_dir, &mode, n_sources, n_targets, src_tile_size, dst_tile_size, seed),

        Commands::BucketM2M {
            data_dir,
            mode,
            sizes,
            parallel,
            seed,
        } => {
            let sizes: Vec<usize> = sizes
                .split(',')
                .filter_map(|s| s.trim().parse().ok())
                .collect();
            run_bucket_m2m_bench(&data_dir, &mode, &sizes, parallel, seed)
        }
        Commands::ContourCompare {
            data_dir,
            mode,
            threshold_ds,
            n_queries,
            seed,
        } => run_contour_compare_bench(&data_dir, &mode, threshold_ds, n_queries, seed),
    }
}

fn run_isochrone_bench(
    data_dir: &PathBuf,
    mode: &str,
    threshold_ms: u32,
    n_origins: usize,
    seed: u64,
) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  ISOCHRONE BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Threshold: {} ms ({:.1} min)", threshold_ms, threshold_ms as f64 / 60000.0);
    println!("  Origins: {}", n_origins);
    println!("  Seed: {}", seed);
    println!();

    // Load data
    println!("[1/2] Loading data...");
    let load_start = Instant::now();

    let phast = load_phast(data_dir, mode)?;
    let extractor = load_extractor(data_dir, mode)?;
    let sparse_config = match mode {
        "car" => SparseContourConfig::for_car(),
        "bike" => SparseContourConfig::for_bike(),
        "foot" => SparseContourConfig::for_foot(),
        _ => SparseContourConfig::for_bike(),
    };

    println!("  ✓ Loaded in {:.1}s", load_start.elapsed().as_secs_f64());
    println!("  ✓ PHAST nodes: {}", phast.n_nodes());
    println!();

    // Convert ms to deciseconds (CCH weight units)
    // 1 decisecond = 100 milliseconds
    let threshold_ds = threshold_ms / 100;

    // Generate random origins
    let mut rng = StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_origins)
        .map(|_| rng.gen_range(0..phast.n_nodes() as u32))
        .collect();

    // Run benchmarks
    println!("[2/2] Running {} queries...", n_origins);

    let mut hist_total = Histogram::<u64>::new(3)?;
    let mut hist_phast = Histogram::<u64>::new(3)?;
    let mut hist_frontier = Histogram::<u64>::new(3)?;
    let mut hist_contour = Histogram::<u64>::new(3)?;
    let mut agg_stats = AggregatedStats::default();

    for (i, &origin) in origins.iter().enumerate() {
        let total_start = Instant::now();

        // PHAST (expects deciseconds)
        let phast_start = Instant::now();
        let result = phast.query_bounded(origin, threshold_ds);
        let phast_time = phast_start.elapsed();

        // Frontier extraction (expects deciseconds)
        let frontier_start = Instant::now();
        let segments = extractor.extract_reachable_segments(&result.dist, threshold_ds);
        let frontier_time = frontier_start.elapsed();

        // Contour generation (sparse boundary tracing)
        let contour_start = Instant::now();
        let contour = generate_sparse_contour(&segments, &sparse_config)?;
        let contour_time = contour_start.elapsed();

        let total_time = total_start.elapsed();

        // Record times
        hist_total.record(total_time.as_micros() as u64)?;
        hist_phast.record(phast_time.as_micros() as u64)?;
        hist_frontier.record(frontier_time.as_micros() as u64)?;
        hist_contour.record(contour_time.as_micros() as u64)?;

        // Aggregate stats
        agg_stats.add(&result.stats);
        agg_stats.frontier_edges += segments.len() as u64;
        agg_stats.grid_cells += contour.stats.total_cells_set as u64;
        agg_stats.contour_vertices += contour.stats.contour_vertices_after_simplify as u64;

        if (i + 1) % 10 == 0 || i == n_origins - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_origins);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    println!();
    println!();

    // Report results
    print_histogram_stats("Total", &hist_total);
    print_histogram_stats("PHAST", &hist_phast);
    print_histogram_stats("Frontier", &hist_frontier);
    print_histogram_stats("Contour", &hist_contour);

    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  COUNTERS (averages)");
    println!("───────────────────────────────────────────────────────────────");
    let avg = agg_stats.avg(n_origins);
    println!("  Upward PQ pushes:     {:>12}", format_number(avg.upward_pq_pushes));
    println!("  Upward PQ pops:       {:>12}", format_number(avg.upward_pq_pops));
    println!("  Upward relaxations:   {:>12}", format_number(avg.upward_relaxations));
    println!("  Upward settled:       {:>12}", format_number(avg.upward_settled));
    println!("  Downward relaxations: {:>12}", format_number(avg.downward_relaxations));
    println!("  Downward improved:    {:>12}", format_number(avg.downward_improved));
    println!("  Frontier segments:    {:>12}", format_number(avg.frontier_edges));
    println!("  Grid cells filled:    {:>12}", format_number(avg.grid_cells));
    println!("  Contour vertices:     {:>12}", format_number(avg.contour_vertices));
    println!();

    // Throughput
    let total_time_sec = hist_total.mean() as f64 / 1_000_000.0 * n_origins as f64;
    let throughput = n_origins as f64 / total_time_sec;
    println!("  Throughput: {:.1} isochrones/sec", throughput);
    println!();

    Ok(())
}

fn run_batch_bench(
    data_dir: &PathBuf,
    mode: &str,
    threshold_ms: u32,
    batch_size: usize,
    n_batches: usize,
    seed: u64,
) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  BATCH ISOCHRONE BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Threshold: {} ms", threshold_ms);
    println!("  Batch size: {}", batch_size);
    println!("  Batches: {}", n_batches);
    println!();

    // Load data
    println!("[1/2] Loading data...");
    let load_start = Instant::now();

    let phast = load_phast(data_dir, mode)?;
    let extractor = load_extractor(data_dir, mode)?;
    let grid_config = match mode {
        "car" => GridConfig::for_car(),
        "bike" => GridConfig::for_bike(),
        "foot" => GridConfig::for_foot(),
        _ => GridConfig::for_bike(),
    };

    println!("  ✓ Loaded in {:.1}s", load_start.elapsed().as_secs_f64());
    println!();

    // Run batches
    println!("[2/2] Running {} batches of {} origins...", n_batches, batch_size);

    let mut rng = StdRng::seed_from_u64(seed);
    let mut batch_times: Vec<Duration> = Vec::with_capacity(n_batches);
    let mut total_isochrones = 0usize;

    for batch in 0..n_batches {
        let origins: Vec<u32> = (0..batch_size)
            .map(|_| rng.gen_range(0..phast.n_nodes() as u32))
            .collect();

        let batch_start = Instant::now();

        for &origin in &origins {
            let result = phast.query_bounded(origin, threshold_ms);
            let segments = extractor.extract_reachable_segments(&result.dist, threshold_ms);
            let _ = generate_contour(&segments, &grid_config)?;
            total_isochrones += 1;
        }

        batch_times.push(batch_start.elapsed());
        println!("  Batch {}/{}: {:.1}s ({:.0} iso/s)",
            batch + 1, n_batches,
            batch_times.last().unwrap().as_secs_f64(),
            batch_size as f64 / batch_times.last().unwrap().as_secs_f64()
        );
    }

    // Summary
    println!();
    let total_time: Duration = batch_times.iter().sum();
    let throughput = total_isochrones as f64 / total_time.as_secs_f64();
    println!("───────────────────────────────────────────────────────────────");
    println!("  Total isochrones: {}", total_isochrones);
    println!("  Total time: {:.1}s", total_time.as_secs_f64());
    println!("  Throughput: {:.1} isochrones/sec", throughput);
    println!();

    Ok(())
}

fn run_phast_bench(
    data_dir: &PathBuf,
    mode: &str,
    n_queries: usize,
    seed: u64,
) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  PHAST-ONLY BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Queries: {}", n_queries);
    println!();

    // Load data
    println!("[1/2] Loading PHAST engine...");
    let load_start = Instant::now();
    let phast = load_phast(data_dir, mode)?;
    println!("  ✓ Loaded in {:.1}s ({} nodes)",
        load_start.elapsed().as_secs_f64(), phast.n_nodes());
    println!();

    // Generate random origins
    let mut rng = StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_queries)
        .map(|_| rng.gen_range(0..phast.n_nodes() as u32))
        .collect();

    // Run queries
    println!("[2/2] Running {} PHAST queries...", n_queries);

    let mut hist = Histogram::<u64>::new(3)?;
    let mut agg_stats = AggregatedStats::default();

    let benchmark_start = Instant::now();
    for (i, &origin) in origins.iter().enumerate() {
        let start = Instant::now();
        let result = phast.query(origin);
        hist.record(start.elapsed().as_micros() as u64)?;
        agg_stats.add(&result.stats);

        if (i + 1) % 100 == 0 || i == n_queries - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_queries);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    let total_time = benchmark_start.elapsed();
    println!();
    println!();

    print_histogram_stats("PHAST query", &hist);

    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  COUNTERS (averages)");
    println!("───────────────────────────────────────────────────────────────");
    let avg = agg_stats.avg(n_queries);
    println!("  Upward PQ pushes:     {:>12}", format_number(avg.upward_pq_pushes));
    println!("  Upward PQ pops:       {:>12}", format_number(avg.upward_pq_pops));
    println!("  Upward relaxations:   {:>12}", format_number(avg.upward_relaxations));
    println!("  Upward settled:       {:>12}", format_number(avg.upward_settled));
    println!("  Downward relaxations: {:>12}", format_number(avg.downward_relaxations));
    println!("  Downward improved:    {:>12}", format_number(avg.downward_improved));
    println!();
    println!("  Total time: {:.1}s", total_time.as_secs_f64());
    println!("  Throughput: {:.1} queries/sec", n_queries as f64 / total_time.as_secs_f64());
    println!();

    Ok(())
}

fn load_phast(data_dir: &PathBuf, mode: &str) -> anyhow::Result<PhastEngine> {
    // Support multiple directory layouts:
    // 1. All files in data_dir
    // 2. Split across step6/step7/step8 subdirectories
    // 3. Split across step6-belgium-fixed, step7-belgium-fixed, etc.

    // Prioritize rank-aligned (version 2) over older versions
    let topo_path = find_file(data_dir, &[
        format!("cch.{}.topo", mode),
        format!("step7-rank-aligned/cch.{}.topo", mode),
        format!("step7-belgium-fixed/cch.{}.topo", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.{}.topo", mode))?;

    let weights_path = find_file(data_dir, &[
        format!("cch.w.{}.u32", mode),
        format!("step8-rank-aligned/cch.w.{}.u32", mode),
        format!("step8-belgium-fixed/cch.w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.w.{}.u32", mode))?;

    let order_path = find_file(data_dir, &[
        format!("order.{}.ebg", mode),
        format!("step6/order.{}.ebg", mode),
        format!("step6-belgium-fixed/order.{}.ebg", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find order.{}.ebg", mode))?;

    PhastEngine::load(&topo_path, &weights_path, &order_path)
}

fn find_file(base: &PathBuf, candidates: &[String]) -> Option<PathBuf> {
    for candidate in candidates {
        let path = base.join(candidate);
        if path.exists() {
            return Some(path);
        }
        // Also try going up one level if base doesn't have the file
        let parent_path = base.parent().map(|p| p.join(candidate));
        if let Some(p) = parent_path {
            if p.exists() {
                return Some(p);
            }
        }
    }
    None
}

fn load_extractor(data_dir: &PathBuf, mode: &str) -> anyhow::Result<FrontierExtractor> {
    let filtered_path = find_file(data_dir, &[
        format!("filtered.{}.ebg", mode),
        format!("step5-debug/filtered.{}.ebg", mode),
        format!("belgium/step5-debug/filtered.{}.ebg", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find filtered.{}.ebg", mode))?;

    let ebg_nodes_path = find_file(data_dir, &[
        "ebg.nodes".to_string(),
        "step4/ebg.nodes".to_string(),
        "belgium/step4/ebg.nodes".to_string(),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find ebg.nodes"))?;

    let nbg_geo_path = find_file(data_dir, &[
        "nbg.geo".to_string(),
        "step3/nbg.geo".to_string(),
        "belgium/step3/nbg.geo".to_string(),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find nbg.geo"))?;

    let weights_path = find_file(data_dir, &[
        format!("w.{}.u32", mode),
        format!("step5-debug/w.{}.u32", mode),
        format!("belgium/step5-debug/w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find w.{}.u32", mode))?;

    FrontierExtractor::load(&filtered_path, &ebg_nodes_path, &nbg_geo_path, &weights_path)
}

fn print_histogram_stats(name: &str, hist: &Histogram<u64>) {
    println!("───────────────────────────────────────────────────────────────");
    println!("  {} timing (μs)", name);
    println!("───────────────────────────────────────────────────────────────");
    println!("    min:    {:>10.0}", hist.min() as f64);
    println!("    p50:    {:>10.0}", hist.value_at_quantile(0.50) as f64);
    println!("    p90:    {:>10.0}", hist.value_at_quantile(0.90) as f64);
    println!("    p95:    {:>10.0}", hist.value_at_quantile(0.95) as f64);
    println!("    p99:    {:>10.0}", hist.value_at_quantile(0.99) as f64);
    println!("    max:    {:>10.0}", hist.max() as f64);
    println!("    mean:   {:>10.1}", hist.mean());
    println!("    stdev:  {:>10.1}", hist.stdev());
}

fn format_number(n: u64) -> String {
    if n >= 1_000_000_000 {
        format!("{:.2}B", n as f64 / 1_000_000_000.0)
    } else if n >= 1_000_000 {
        format!("{:.2}M", n as f64 / 1_000_000.0)
    } else if n >= 1_000 {
        format!("{:.2}K", n as f64 / 1_000.0)
    } else {
        format!("{}", n)
    }
}

fn run_batched_phast_bench(
    data_dir: &PathBuf,
    mode: &str,
    n_sources: usize,
    n_targets: usize,
    seed: u64,
) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  K-LANE BATCHED PHAST BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Sources: {}", n_sources);
    println!("  Targets: {}", if n_targets == 0 { "all".to_string() } else { n_targets.to_string() });
    println!("  K-lanes: {}", K_LANES);
    println!();

    // Load data
    println!("[1/3] Loading engines...");
    let load_start = Instant::now();

    let single_phast = load_phast(data_dir, mode)?;
    let batched_phast = load_batched_phast(data_dir, mode)?;

    println!("  ✓ Loaded in {:.1}s ({} nodes)",
        load_start.elapsed().as_secs_f64(), single_phast.n_nodes());
    println!();

    // Generate random sources and targets
    let mut rng = StdRng::seed_from_u64(seed);
    let sources: Vec<u32> = (0..n_sources)
        .map(|_| rng.gen_range(0..single_phast.n_nodes() as u32))
        .collect();
    let targets: Vec<u32> = if n_targets == 0 {
        Vec::new() // All nodes
    } else {
        (0..n_targets)
            .map(|_| rng.gen_range(0..single_phast.n_nodes() as u32))
            .collect()
    };

    // Use targets if provided, otherwise use a sample of nodes for verification
    let verification_targets: Vec<u32> = if targets.is_empty() {
        // Sample 1000 random targets for verification when no targets specified
        (0..1000.min(single_phast.n_nodes()))
            .map(|_| rng.gen_range(0..single_phast.n_nodes() as u32))
            .collect()
    } else {
        targets.clone()
    };

    // ========== Single-source PHAST baseline ==========
    println!("[2/3] Running single-source PHAST baseline ({} queries)...", n_sources);

    let single_start = Instant::now();
    let mut single_results: Vec<Vec<u32>> = Vec::with_capacity(n_sources);

    for (i, &src) in sources.iter().enumerate() {
        let result = single_phast.query(src);
        single_results.push(verification_targets.iter().map(|&t| result.dist[t as usize]).collect());
        if (i + 1) % 10 == 0 || i == n_sources - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_sources);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    let single_time = single_start.elapsed();
    println!();
    println!("  ✓ Single-source: {:.2}s ({:.1} queries/sec)",
        single_time.as_secs_f64(),
        n_sources as f64 / single_time.as_secs_f64());
    println!();

    // ========== K-lane batched PHAST (AoS layout) ==========
    println!("[3/4] Running K-lane batched PHAST AoS ({} sources in {} batches)...",
        n_sources, (n_sources + K_LANES - 1) / K_LANES);

    let batched_start = Instant::now();
    let (batched_matrix, batched_stats) = batched_phast.compute_matrix_flat(&sources, &verification_targets);
    let batched_time = batched_start.elapsed();

    println!("  ✓ Batched AoS: {:.2}s ({:.1} queries/sec)",
        batched_time.as_secs_f64(),
        n_sources as f64 / batched_time.as_secs_f64());

    // ========== K-lane batched PHAST (SoA layout) ==========
    println!("[4/4] Running K-lane batched PHAST SoA ({} sources in {} batches)...",
        n_sources, (n_sources + K_LANES - 1) / K_LANES);

    let soa_start = Instant::now();
    let (soa_matrix, soa_stats) = batched_phast.compute_matrix_flat_soa(&sources, &verification_targets);
    let soa_time = soa_start.elapsed();

    println!("  ✓ Batched SoA: {:.2}s ({:.1} queries/sec)",
        soa_time.as_secs_f64(),
        n_sources as f64 / soa_time.as_secs_f64());
    println!();

    // ========== Verify correctness ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  CORRECTNESS VERIFICATION");
    println!("───────────────────────────────────────────────────────────────");

    let n_targets_actual = verification_targets.len();

    // Check AoS results
    let mut aos_mismatches = 0;
    for (src_idx, single_dist) in single_results.iter().enumerate() {
        for (tgt_idx, &expected) in single_dist.iter().enumerate() {
            let actual = batched_matrix[src_idx * n_targets_actual + tgt_idx];
            if expected != actual {
                aos_mismatches += 1;
            }
        }
    }

    // Check SoA results
    let mut soa_mismatches = 0;
    for (src_idx, single_dist) in single_results.iter().enumerate() {
        for (tgt_idx, &expected) in single_dist.iter().enumerate() {
            let actual = soa_matrix[src_idx * n_targets_actual + tgt_idx];
            if expected != actual {
                soa_mismatches += 1;
            }
        }
    }

    if aos_mismatches == 0 && soa_mismatches == 0 {
        println!("  ✅ AoS: All {} × {} = {} distances match!",
            n_sources, n_targets_actual, n_sources * n_targets_actual);
        println!("  ✅ SoA: All {} × {} = {} distances match!",
            n_sources, n_targets_actual, n_sources * n_targets_actual);
    } else {
        if aos_mismatches > 0 {
            println!("  ❌ AoS: {} mismatches", aos_mismatches);
        }
        if soa_mismatches > 0 {
            println!("  ❌ SoA: {} mismatches", soa_mismatches);
        }
    }
    println!();

    // ========== Performance comparison ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  PERFORMANCE COMPARISON");
    println!("───────────────────────────────────────────────────────────────");

    let aos_speedup = single_time.as_secs_f64() / batched_time.as_secs_f64();
    let soa_speedup = single_time.as_secs_f64() / soa_time.as_secs_f64();
    let soa_vs_aos = batched_time.as_secs_f64() / soa_time.as_secs_f64();

    println!("  Single-source time:    {:>8.2}s", single_time.as_secs_f64());
    println!("  K-lane AoS time:       {:>8.2}s ({:.2}x vs single)", batched_time.as_secs_f64(), aos_speedup);
    println!("  K-lane SoA time:       {:>8.2}s ({:.2}x vs single)", soa_time.as_secs_f64(), soa_speedup);
    println!("  SoA vs AoS speedup:    {:>8.2}x", soa_vs_aos);
    println!();

    println!("  AoS stats:");
    println!("    Upward time:          {:>12} ms", batched_stats.upward_time_ms);
    println!("    Downward time:        {:>12} ms", batched_stats.downward_time_ms);

    println!("  SoA stats:");
    println!("    Upward time:          {:>12} ms", soa_stats.upward_time_ms);
    println!("    Downward time:        {:>12} ms", soa_stats.downward_time_ms);
    println!();

    if soa_vs_aos > 1.3 {
        println!("  ✅ SoA layout provides significant cache benefit!");
    } else if soa_vs_aos > 1.0 {
        println!("  ℹ️  SoA slightly faster - may need SIMD to see full benefit");
    } else {
        println!("  ⚠️  SoA not faster - investigate memory layout");
    }
    println!();

    Ok(())
}

fn load_batched_phast(data_dir: &PathBuf, mode: &str) -> anyhow::Result<BatchedPhastEngine> {
    // Prioritize rank-aligned (version 2) over older versions
    let topo_path = find_file(data_dir, &[
        format!("cch.{}.topo", mode),
        format!("step7-rank-aligned/cch.{}.topo", mode),
        format!("step7-belgium-fixed/cch.{}.topo", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.{}.topo", mode))?;

    let weights_path = find_file(data_dir, &[
        format!("cch.w.{}.u32", mode),
        format!("step8-rank-aligned/cch.w.{}.u32", mode),
        format!("step8-belgium-fixed/cch.w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.w.{}.u32", mode))?;

    let order_path = find_file(data_dir, &[
        format!("order.{}.ebg", mode),
        format!("step6/order.{}.ebg", mode),
        format!("step6-belgium-fixed/order.{}.ebg", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find order.{}.ebg", mode))?;

    BatchedPhastEngine::load(&topo_path, &weights_path, &order_path)
}

fn run_active_set_bench(
    data_dir: &PathBuf,
    mode: &str,
    threshold_ms: u32,
    n_queries: usize,
    seed: u64,
) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  ACTIVE-SET GATING BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Threshold: {} ms ({:.1} min)", threshold_ms, threshold_ms as f64 / 60000.0);
    println!("  Queries: {}", n_queries);
    println!();

    // Load data
    println!("[1/3] Loading PHAST engine...");
    let load_start = Instant::now();
    let phast = load_phast(data_dir, mode)?;
    println!("  ✓ Loaded in {:.1}s ({} nodes)",
        load_start.elapsed().as_secs_f64(), phast.n_nodes());
    println!();

    // Generate random origins
    let mut rng = StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_queries)
        .map(|_| rng.gen_range(0..phast.n_nodes() as u32))
        .collect();

    // ========== Naive bounded PHAST ==========
    println!("[2/3] Running naive bounded PHAST...");

    let mut naive_times: Vec<Duration> = Vec::with_capacity(n_queries);
    let mut naive_relaxations: u64 = 0;
    let mut naive_reachable: u64 = 0;

    for (i, &origin) in origins.iter().enumerate() {
        let start = Instant::now();
        let result = phast.query_bounded_naive(origin, threshold_ms);
        naive_times.push(start.elapsed());
        naive_relaxations += result.stats.downward_relaxations as u64;
        naive_reachable += result.n_reachable as u64;

        if (i + 1) % 10 == 0 || i == n_queries - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_queries);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    println!();

    let naive_total: Duration = naive_times.iter().sum();
    let naive_avg_ms = naive_total.as_millis() as f64 / n_queries as f64;
    println!("  ✓ Naive: {:.1}ms avg, {:.1}M relaxations/query",
        naive_avg_ms,
        naive_relaxations as f64 / n_queries as f64 / 1_000_000.0);
    println!();

    // ========== Active-set gated PHAST ==========
    println!("[3/3] Running active-set gated PHAST...");

    let mut gated_times: Vec<Duration> = Vec::with_capacity(n_queries);
    let mut gated_relaxations: u64 = 0;
    let mut gated_reachable: u64 = 0;

    for (i, &origin) in origins.iter().enumerate() {
        let start = Instant::now();
        let result = phast.query_active_set(origin, threshold_ms);
        gated_times.push(start.elapsed());
        gated_relaxations += result.stats.downward_relaxations as u64;
        gated_reachable += result.n_reachable as u64;

        if (i + 1) % 10 == 0 || i == n_queries - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_queries);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    println!();

    let gated_total: Duration = gated_times.iter().sum();
    let gated_avg_ms = gated_total.as_millis() as f64 / n_queries as f64;
    println!("  ✓ Active-set: {:.1}ms avg, {:.1}M relaxations/query",
        gated_avg_ms,
        gated_relaxations as f64 / n_queries as f64 / 1_000_000.0);
    println!();

    // ========== Verification ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  CORRECTNESS VERIFICATION");
    println!("───────────────────────────────────────────────────────────────");

    // Verify that both methods produce same reachable count
    if naive_reachable == gated_reachable {
        println!("  ✅ Reachable counts match: {} total", naive_reachable);
    } else {
        println!("  ❌ Reachable count mismatch: naive={}, gated={}", naive_reachable, gated_reachable);
    }

    // Spot check a few queries for exact distance equality
    let mut spot_check_ok = true;
    for &origin in origins.iter().take(5) {
        let naive_result = phast.query_bounded_naive(origin, threshold_ms);
        let gated_result = phast.query_active_set(origin, threshold_ms);

        for (i, (&naive_d, &gated_d)) in naive_result.dist.iter().zip(gated_result.dist.iter()).enumerate() {
            if naive_d <= threshold_ms && gated_d <= threshold_ms && naive_d != gated_d {
                println!("  ❌ Distance mismatch at node {}: naive={}, gated={}", i, naive_d, gated_d);
                spot_check_ok = false;
                break;
            }
        }
    }
    if spot_check_ok {
        println!("  ✅ Spot check: 5 queries have matching distances");
    }
    println!();

    // ========== Performance comparison ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  PERFORMANCE COMPARISON");
    println!("───────────────────────────────────────────────────────────────");

    let time_speedup = naive_total.as_secs_f64() / gated_total.as_secs_f64();
    let relax_reduction = 1.0 - (gated_relaxations as f64 / naive_relaxations as f64);

    println!("  Naive avg time:        {:>8.1} ms", naive_avg_ms);
    println!("  Active-set avg time:   {:>8.1} ms", gated_avg_ms);
    println!("  Time speedup:          {:>8.2}x", time_speedup);
    println!();
    println!("  Naive relaxations:     {:>12}", format_number(naive_relaxations / n_queries as u64));
    println!("  Active-set relaxations:{:>12}", format_number(gated_relaxations / n_queries as u64));
    println!("  Relaxation reduction:  {:>8.1}%", relax_reduction * 100.0);
    println!();

    if time_speedup > 1.5 {
        println!("  ✅ Active-set gating provides significant speedup!");
    } else if time_speedup > 1.1 {
        println!("  ⚠️  Active-set gating provides modest speedup");
    } else {
        println!("  ⚠️  Active-set gating overhead may outweigh benefits for this threshold");
    }
    println!();

    Ok(())
}

fn run_batched_isochrone_bench(
    data_dir: &PathBuf,
    mode: &str,
    threshold_ms: u32,
    n_origins: usize,
    seed: u64,
) -> anyhow::Result<()> {
    use butterfly_route::profile_abi::Mode;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  K-LANE BATCHED ISOCHRONE BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Threshold: {} ms ({:.1} min)", threshold_ms, threshold_ms as f64 / 60000.0);
    println!("  Origins: {} (in batches of {})", n_origins, K_LANES);
    println!();

    // Parse mode
    let mode_enum = match mode.to_lowercase().as_str() {
        "car" => Mode::Car,
        "bike" => Mode::Bike,
        "foot" => Mode::Foot,
        _ => Mode::Car,
    };

    // ========== Load data ==========
    println!("[1/4] Loading single-source engine...");
    let single_start = Instant::now();
    let phast = load_phast(data_dir, mode)?;
    let extractor = load_extractor(data_dir, mode)?;
    let sparse_config = match mode {
        "car" => SparseContourConfig::for_car(),
        "bike" => SparseContourConfig::for_bike(),
        "foot" => SparseContourConfig::for_foot(),
        _ => SparseContourConfig::for_car(),
    };
    println!("  ✓ Loaded in {:.1}s ({} nodes)", single_start.elapsed().as_secs_f64(), phast.n_nodes());

    println!("\n[2/4] Loading K-lane batched engine...");
    let batched_start = Instant::now();

    // Find paths using the same discovery logic as single-source
    // Prioritize rank-aligned (version 2) over older versions
    let topo_path = find_file(data_dir, &[
        format!("cch.{}.topo", mode),
        format!("step7-rank-aligned/cch.{}.topo", mode),
        format!("step7/cch.{}.topo", mode),
        format!("step7-new/cch.{}.topo", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.{}.topo", mode))?;

    let weights_path = find_file(data_dir, &[
        format!("cch.w.{}.u32", mode),
        format!("step8-rank-aligned/cch.w.{}.u32", mode),
        format!("step8/cch.w.{}.u32", mode),
        format!("step8-new/cch.w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.w.{}.u32", mode))?;

    let order_path = find_file(data_dir, &[
        format!("order.{}.ebg", mode),
        format!("step6/order.{}.ebg", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find order.{}.ebg", mode))?;

    let filtered_path = find_file(data_dir, &[
        format!("filtered.{}.ebg", mode),
        format!("step5/filtered.{}.ebg", mode),
        format!("step5-debug/filtered.{}.ebg", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find filtered.{}.ebg", mode))?;

    let ebg_nodes_path = find_file(data_dir, &[
        "ebg.nodes".to_string(),
        "step4/ebg.nodes".to_string(),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find ebg.nodes"))?;

    let nbg_geo_path = find_file(data_dir, &[
        "nbg.geo".to_string(),
        "step3/nbg.geo".to_string(),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find nbg.geo"))?;

    let base_weights_path = find_file(data_dir, &[
        format!("w.{}.u32", mode),
        format!("step5/w.{}.u32", mode),
        format!("step5-debug/w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find w.{}.u32", mode))?;

    let batched_engine = BatchedIsochroneEngine::load(
        &topo_path,
        &weights_path,
        &order_path,
        &filtered_path,
        &ebg_nodes_path,
        &nbg_geo_path,
        &base_weights_path,
        mode_enum,
    )?;
    println!("  ✓ Loaded in {:.1}s", batched_start.elapsed().as_secs_f64());

    // Convert ms to deciseconds (CCH weight units)
    // 1 decisecond = 100 milliseconds
    let threshold_ds = threshold_ms / 100;

    // Generate random origins
    let mut rng = StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_origins)
        .map(|_| rng.gen_range(0..phast.n_nodes() as u32))
        .collect();

    // ========== Run single-source baseline ==========
    println!("\n[3/4] Running single-source isochrones...");
    let single_run_start = Instant::now();
    let mut single_vertices = 0usize;

    for (i, &origin) in origins.iter().enumerate() {
        let result = phast.query_bounded(origin, threshold_ds);
        let segments = extractor.extract_reachable_segments(&result.dist, threshold_ms);
        let contour = generate_sparse_contour(&segments, &sparse_config)?;
        single_vertices += contour.outer_ring.len();

        if (i + 1) % 10 == 0 || i + 1 == n_origins {
            print!("  Progress: {}/{}\r", i + 1, n_origins);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    let single_time = single_run_start.elapsed();
    println!("  ✓ Single: {:.2}s ({:.1} iso/s)",
        single_time.as_secs_f64(),
        n_origins as f64 / single_time.as_secs_f64());

    // ========== Run K-lane batched ==========
    println!("\n[4/4] Running K-lane batched isochrones...");
    let batched_run_start = Instant::now();
    let mut batched_vertices = 0usize;
    let mut n_batches = 0usize;

    for (batch_idx, chunk) in origins.chunks(K_LANES).enumerate() {
        let result = batched_engine.query_batch(chunk, threshold_ds)?;
        for contour in &result.contours {
            batched_vertices += contour.outer_ring.len();
        }
        n_batches += 1;

        if (batch_idx + 1) % 2 == 0 || batch_idx + 1 == (n_origins + K_LANES - 1) / K_LANES {
            print!("  Progress: {}/{} batches\r", batch_idx + 1, (n_origins + K_LANES - 1) / K_LANES);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    let batched_time = batched_run_start.elapsed();
    println!("  ✓ Batched: {:.2}s ({:.1} iso/s, {} batches)",
        batched_time.as_secs_f64(),
        n_origins as f64 / batched_time.as_secs_f64(),
        n_batches);

    // ========== Correctness verification ==========
    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  CORRECTNESS VERIFICATION");
    println!("───────────────────────────────────────────────────────────────");

    // Rough check: vertex counts should be similar
    let vertex_diff_pct = ((single_vertices as f64 - batched_vertices as f64).abs() / single_vertices as f64) * 100.0;
    if vertex_diff_pct < 5.0 {
        println!("  ✅ Vertex counts similar: {} (single) vs {} (batched), diff {:.1}%",
            single_vertices, batched_vertices, vertex_diff_pct);
    } else {
        println!("  ⚠️  Vertex counts differ: {} (single) vs {} (batched), diff {:.1}%",
            single_vertices, batched_vertices, vertex_diff_pct);
    }

    // ========== Performance comparison ==========
    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  PERFORMANCE COMPARISON");
    println!("───────────────────────────────────────────────────────────────");

    let single_throughput = n_origins as f64 / single_time.as_secs_f64();
    let batched_throughput = n_origins as f64 / batched_time.as_secs_f64();
    let speedup = batched_throughput / single_throughput;

    println!("  Single-source:");
    println!("    Time: {:.2}s for {} isochrones", single_time.as_secs_f64(), n_origins);
    println!("    Throughput: {:.1} iso/sec", single_throughput);
    println!();
    println!("  K-lane batched (K={}):", K_LANES);
    println!("    Time: {:.2}s for {} isochrones", batched_time.as_secs_f64(), n_origins);
    println!("    Throughput: {:.1} iso/sec", batched_throughput);
    println!();
    println!("  Speedup: {:.2}x", speedup);
    println!();

    if speedup > 2.0 {
        println!("  ✅ K-lane batching provides significant speedup!");
    } else if speedup > 1.2 {
        println!("  ✅ K-lane batching provides moderate speedup");
    } else {
        println!("  ⚠️  K-lane batching shows minimal benefit (may be I/O or contour dominated)");
    }
    println!();

    Ok(())
}

fn run_blocked_relaxation_bench(
    data_dir: &PathBuf,
    mode: &str,
    n_sources: usize,
    n_targets: usize,
    seed: u64,
) -> anyhow::Result<()> {
    use butterfly_route::matrix::batched_phast::BLOCK_SIZE;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  BLOCKED RELAXATION BENCHMARK (Cache Efficiency)");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Sources: {}", n_sources);
    println!("  Targets: {}", if n_targets == 0 { "all".to_string() } else { n_targets.to_string() });
    println!("  K-lanes: {}", K_LANES);
    println!("  Block size: {} nodes", BLOCK_SIZE);
    println!();

    // Load data
    println!("[1/4] Loading batched PHAST engine...");
    let load_start = Instant::now();
    let engine = load_batched_phast(data_dir, mode)?;
    println!("  ✓ Loaded in {:.1}s ({} nodes)", load_start.elapsed().as_secs_f64(), engine.n_nodes());
    println!();

    // Generate random sources and targets
    let mut rng = StdRng::seed_from_u64(seed);
    let sources: Vec<u32> = (0..n_sources)
        .map(|_| rng.gen_range(0..engine.n_nodes() as u32))
        .collect();
    let targets: Vec<u32> = if n_targets == 0 {
        // Sample 1000 targets for verification
        (0..1000.min(engine.n_nodes()))
            .map(|_| rng.gen_range(0..engine.n_nodes() as u32))
            .collect()
    } else {
        (0..n_targets)
            .map(|_| rng.gen_range(0..engine.n_nodes() as u32))
            .collect()
    };

    // ========== Non-blocked baseline ==========
    println!("[2/4] Running non-blocked K-lane PHAST...");
    let baseline_start = Instant::now();
    let (baseline_matrix, baseline_stats) = engine.compute_matrix_flat(&sources, &targets);
    let baseline_time = baseline_start.elapsed();
    println!("  ✓ Non-blocked: {:.2}s ({:.1} queries/sec)",
        baseline_time.as_secs_f64(),
        n_sources as f64 / baseline_time.as_secs_f64());
    println!("    Downward time: {}ms", baseline_stats.downward_time_ms);
    println!();

    // ========== Blocked relaxation ==========
    println!("[3/4] Running BLOCKED K-lane PHAST...");
    let blocked_start = Instant::now();
    let (blocked_matrix, blocked_stats) = engine.compute_matrix_flat_blocked(&sources, &targets);
    let blocked_time = blocked_start.elapsed();
    println!("  ✓ Blocked: {:.2}s ({:.1} queries/sec)",
        blocked_time.as_secs_f64(),
        n_sources as f64 / blocked_time.as_secs_f64());
    println!("    Downward time: {}ms", blocked_stats.downward_time_ms);
    println!("    Buffer flushes: {}", blocked_stats.buffer_flushes);
    println!("    Buffered updates: {}", format_number(blocked_stats.buffered_updates as u64));
    println!();

    // ========== Correctness verification ==========
    println!("[4/4] Verifying correctness...");
    println!("───────────────────────────────────────────────────────────────");
    println!("  CORRECTNESS VERIFICATION");
    println!("───────────────────────────────────────────────────────────────");

    let mut mismatches = 0;
    let mut max_diff: i64 = 0;
    let n_tgt = targets.len();

    for src_idx in 0..n_sources {
        for tgt_idx in 0..n_tgt {
            let expected = baseline_matrix[src_idx * n_tgt + tgt_idx];
            let actual = blocked_matrix[src_idx * n_tgt + tgt_idx];
            if expected != actual {
                mismatches += 1;
                let diff = (expected as i64 - actual as i64).abs();
                max_diff = max_diff.max(diff);
                if mismatches <= 3 {
                    println!("  Mismatch: src={} tgt={}: baseline={}, blocked={}",
                        src_idx, tgt_idx, expected, actual);
                }
            }
        }
    }

    if mismatches == 0 {
        println!("  ✅ All {} × {} = {} distances match!",
            n_sources, n_tgt, n_sources * n_tgt);
    } else {
        println!("  ❌ {} mismatches (max diff: {})", mismatches, max_diff);
    }
    println!();

    // ========== Performance comparison ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  PERFORMANCE COMPARISON");
    println!("───────────────────────────────────────────────────────────────");

    let speedup = baseline_time.as_secs_f64() / blocked_time.as_secs_f64();
    let downward_speedup = baseline_stats.downward_time_ms as f64 / blocked_stats.downward_time_ms.max(1) as f64;

    println!("  Non-blocked:");
    println!("    Total time:     {:>8.2}s", baseline_time.as_secs_f64());
    println!("    Downward time:  {:>8}ms", baseline_stats.downward_time_ms);
    println!("    Upward time:    {:>8}ms", baseline_stats.upward_time_ms);
    println!();
    println!("  Blocked:");
    println!("    Total time:     {:>8.2}s", blocked_time.as_secs_f64());
    println!("    Downward time:  {:>8}ms", blocked_stats.downward_time_ms);
    println!("    Upward time:    {:>8}ms", blocked_stats.upward_time_ms);
    println!();
    println!("  Total speedup:       {:>8.2}x", speedup);
    println!("  Downward speedup:    {:>8.2}x", downward_speedup);
    println!();

    // Analysis
    println!("───────────────────────────────────────────────────────────────");
    println!("  ANALYSIS");
    println!("───────────────────────────────────────────────────────────────");

    if speedup > 2.0 {
        println!("  ✅ Blocked relaxation provides significant speedup!");
        println!("     Cache miss rate likely improved substantially.");
    } else if speedup > 1.2 {
        println!("  ✅ Blocked relaxation provides moderate speedup.");
        println!("     Try tuning BLOCK_SIZE or MAX_BUFFER_ENTRIES.");
    } else if speedup > 0.9 {
        println!("  ⚠️  Blocked relaxation shows minimal benefit.");
        println!("     Buffer overhead may be too high for this graph.");
    } else {
        println!("  ❌ Blocked relaxation is SLOWER than baseline!");
        println!("     This is unexpected - investigate buffer/flush overhead.");
    }
    println!();

    // Suggest perf stat
    println!("  💡 For accurate cache analysis, run:");
    println!("     perf stat -e cache-misses,cache-references,LLC-load-misses butterfly-bench blocked-relaxation ...");
    println!();

    Ok(())
}

fn run_block_gated_bench(
    data_dir: &PathBuf,
    mode: &str,
    threshold_ms: u32,
    n_queries: usize,
    seed: u64,
) -> anyhow::Result<()> {
    use butterfly_route::range::phast::BLOCK_SIZE;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  BLOCK-GATED PHAST BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Threshold: {} ms ({:.1} min)", threshold_ms, threshold_ms as f64 / 60000.0);
    println!("  Queries: {}", n_queries);
    println!("  Block size: {} ranks", BLOCK_SIZE);
    println!();

    // Load data
    println!("[1/4] Loading PHAST engine...");
    let load_start = Instant::now();
    let phast = load_phast(data_dir, mode)?;
    let n_nodes = phast.n_nodes();
    let n_blocks = (n_nodes + BLOCK_SIZE - 1) / BLOCK_SIZE;
    println!("  ✓ Loaded in {:.1}s ({} nodes, {} blocks)",
        load_start.elapsed().as_secs_f64(), n_nodes, n_blocks);
    println!();

    // Generate random origins
    let mut rng = StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_queries)
        .map(|_| rng.gen_range(0..n_nodes as u32))
        .collect();

    // ========== Active-set gated PHAST ==========
    println!("[2/4] Running active-set gated PHAST...");

    let mut active_times: Vec<Duration> = Vec::with_capacity(n_queries);
    let mut active_relaxations: u64 = 0;
    let mut active_reachable: u64 = 0;

    for (i, &origin) in origins.iter().enumerate() {
        let start = Instant::now();
        let result = phast.query_active_set(origin, threshold_ms);
        active_times.push(start.elapsed());
        active_relaxations += result.stats.downward_relaxations as u64;
        active_reachable += result.n_reachable as u64;

        if (i + 1) % 10 == 0 || i == n_queries - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_queries);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    println!();

    let active_total: Duration = active_times.iter().sum();
    let active_avg_ms = active_total.as_millis() as f64 / n_queries as f64;
    println!("  ✓ Active-set: {:.1}ms avg", active_avg_ms);
    println!();

    // ========== Block-gated PHAST ==========
    println!("[3/4] Running block-gated PHAST...");

    let mut block_times: Vec<Duration> = Vec::with_capacity(n_queries);
    let mut block_relaxations: u64 = 0;
    let mut block_reachable: u64 = 0;
    let mut total_blocks_processed: u64 = 0;
    let mut total_blocks_skipped: u64 = 0;
    let mut total_nodes_skipped_in_block: u64 = 0;

    for (i, &origin) in origins.iter().enumerate() {
        let start = Instant::now();
        let result = phast.query_block_gated(origin, threshold_ms);
        block_times.push(start.elapsed());
        block_relaxations += result.stats.downward_relaxations as u64;
        block_reachable += result.n_reachable as u64;
        total_blocks_processed += result.stats.blocks_processed as u64;
        total_blocks_skipped += result.stats.blocks_skipped as u64;
        total_nodes_skipped_in_block += result.stats.nodes_skipped_in_block as u64;

        if (i + 1) % 10 == 0 || i == n_queries - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_queries);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    println!();

    let block_total: Duration = block_times.iter().sum();
    let block_avg_ms = block_total.as_millis() as f64 / n_queries as f64;
    println!("  ✓ Block-gated: {:.1}ms avg", block_avg_ms);
    println!();

    // ========== Verification ==========
    println!("[4/4] Verifying correctness...");

    let mut mismatches = 0;
    for &origin in origins.iter().take(10) {
        let active_result = phast.query_active_set(origin, threshold_ms);
        let block_result = phast.query_block_gated(origin, threshold_ms);

        for node in 0..n_nodes {
            let active_d = active_result.dist[node];
            let block_d = block_result.dist[node];
            let active_reach = active_d <= threshold_ms;
            let block_reach = block_d <= threshold_ms;

            if active_reach != block_reach || (active_reach && block_reach && active_d != block_d) {
                mismatches += 1;
            }
        }
    }

    if mismatches == 0 {
        println!("  ✅ All distances match (spot-checked 10 queries)");
    } else {
        println!("  ❌ {} mismatches found!", mismatches);
    }
    println!();

    // ========== Results ==========
    println!("═══════════════════════════════════════════════════════════════");
    println!("  RESULTS");
    println!("═══════════════════════════════════════════════════════════════");

    // Sort times for percentile calculation
    active_times.sort();
    block_times.sort();

    let active_p50 = active_times[n_queries / 2].as_millis();
    let active_p95 = active_times[n_queries * 95 / 100].as_millis();
    let active_p99 = active_times[n_queries * 99 / 100].as_millis();

    let block_p50 = block_times[n_queries / 2].as_millis();
    let block_p95 = block_times[n_queries * 95 / 100].as_millis();
    let block_p99 = block_times[n_queries * 99 / 100].as_millis();

    println!("  Active-set gating:");
    println!("    p50:  {:>6}ms", active_p50);
    println!("    p95:  {:>6}ms", active_p95);
    println!("    p99:  {:>6}ms", active_p99);
    println!("    Avg:  {:>6.1}ms", active_avg_ms);
    println!("    Relaxations/query: {:.2}M", active_relaxations as f64 / n_queries as f64 / 1_000_000.0);
    println!();

    println!("  Block-level gating:");
    println!("    p50:  {:>6}ms", block_p50);
    println!("    p95:  {:>6}ms", block_p95);
    println!("    p99:  {:>6}ms", block_p99);
    println!("    Avg:  {:>6.1}ms", block_avg_ms);
    println!("    Relaxations/query: {:.2}M", block_relaxations as f64 / n_queries as f64 / 1_000_000.0);
    println!("    Blocks processed/query: {:.0} / {} ({:.1}%)",
        total_blocks_processed as f64 / n_queries as f64,
        n_blocks,
        100.0 * total_blocks_processed as f64 / (n_queries as f64 * n_blocks as f64));
    println!("    Blocks skipped/query: {:.0} ({:.1}%)",
        total_blocks_skipped as f64 / n_queries as f64,
        100.0 * total_blocks_skipped as f64 / (n_queries as f64 * n_blocks as f64));
    println!("    Nodes skipped in active blocks/query: {:.0}",
        total_nodes_skipped_in_block as f64 / n_queries as f64);
    println!();

    // Speedup
    let speedup = active_avg_ms / block_avg_ms;
    let p95_speedup = active_p95 as f64 / block_p95 as f64;

    println!("───────────────────────────────────────────────────────────────");
    println!("  COMPARISON");
    println!("───────────────────────────────────────────────────────────────");
    println!("  Avg speedup:  {:.2}x", speedup);
    println!("  p95 speedup:  {:.2}x", p95_speedup);
    println!();

    // Reachable comparison
    let avg_reachable = active_reachable as f64 / n_queries as f64;
    println!("  Avg reachable nodes: {:.0} ({:.2}% of graph)",
        avg_reachable, 100.0 * avg_reachable / n_nodes as f64);
    println!();

    // Analysis
    if speedup > 1.2 {
        println!("  ✅ Block-level gating provides speedup!");
        println!("     Consider larger BLOCK_SIZE for even more benefit.");
    } else if speedup > 0.95 {
        println!("  ➡️  Block-level gating is roughly equivalent.");
        println!("     Benefit increases with smaller thresholds.");
    } else {
        println!("  ⚠️  Block-level gating is slightly slower.");
        println!("     Per-node gating is more efficient at this scale.");
    }
    println!();

    // Target check from todo
    println!("───────────────────────────────────────────────────────────────");
    println!("  TARGET CHECK (from todo_immediate.md)");
    println!("───────────────────────────────────────────────────────────────");

    let target_met = match (mode, threshold_ms) {
        ("foot", 300000) => block_p95 <= 40,  // foot 5min: p95 < 20-40ms
        ("bike", 600000) => block_p95 <= 80,  // bike 10min: p95 < 50-80ms
        _ => true,
    };

    if mode == "foot" && threshold_ms == 300000 {
        println!("  Foot 5min target: p95 < 20-40ms");
        println!("  Actual p95: {}ms → {}", block_p95, if target_met { "✅ MET" } else { "❌ NOT MET" });
    } else if mode == "bike" && threshold_ms == 600000 {
        println!("  Bike 10min target: p95 < 50-80ms");
        println!("  Actual p95: {}ms → {}", block_p95, if target_met { "✅ MET" } else { "❌ NOT MET" });
    } else {
        println!("  (No specific target for mode={}, threshold={}ms)", mode, threshold_ms);
    }
    println!();

    Ok(())
}

fn run_adaptive_bench(
    data_dir: &PathBuf,
    mode: &str,
    thresholds: &[u32],
    n_queries: usize,
    seed: u64,
) -> anyhow::Result<()> {
    use butterfly_route::range::phast::BLOCK_SIZE;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  ADAPTIVE GATING BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Thresholds: {:?}", thresholds);
    println!("  Queries per threshold: {}", n_queries);
    println!("  Block size: {} ranks", BLOCK_SIZE);
    println!("  Gating threshold: 25% active blocks");
    println!();

    // Load data
    println!("[1/2] Loading PHAST engine...");
    let load_start = Instant::now();
    let phast = load_phast(data_dir, mode)?;
    let n_nodes = phast.n_nodes();
    let n_blocks = (n_nodes + BLOCK_SIZE - 1) / BLOCK_SIZE;
    println!("  ✓ Loaded in {:.1}s ({} nodes, {} blocks)",
        load_start.elapsed().as_secs_f64(), n_nodes, n_blocks);
    println!();

    // Generate random origins
    let mut rng = StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_queries)
        .map(|_| rng.gen_range(0..n_nodes as u32))
        .collect();

    println!("[2/2] Running benchmarks...");
    println!();

    // Results table header
    println!("┌──────────┬──────────┬──────────┬──────────┬───────────┬───────────┬──────────┐");
    println!("│ Thresh   │ Method   │ p50 (ms) │ p95 (ms) │ Avg (ms)  │ Strategy  │ Reachable│");
    println!("├──────────┼──────────┼──────────┼──────────┼───────────┼───────────┼──────────┤");

    for &threshold_ms in thresholds {
        // Run plain PHAST (unbounded, then filter)
        let mut plain_times: Vec<Duration> = Vec::with_capacity(n_queries);
        for &origin in &origins {
            let start = Instant::now();
            let _ = phast.query(origin);
            plain_times.push(start.elapsed());
        }
        plain_times.sort();
        let plain_p50 = plain_times[n_queries / 2].as_millis();
        let plain_p95 = plain_times[n_queries * 95 / 100].as_millis();
        let plain_avg: f64 = plain_times.iter().map(|d| d.as_millis() as f64).sum::<f64>() / n_queries as f64;

        // Run adaptive PHAST
        let mut adaptive_times: Vec<Duration> = Vec::with_capacity(n_queries);
        let mut total_reachable: u64 = 0;
        let mut gated_count = 0usize;
        let mut ungated_count = 0usize;

        for &origin in &origins {
            let start = Instant::now();
            let result = phast.query_adaptive(origin, threshold_ms);
            adaptive_times.push(start.elapsed());
            total_reachable += result.n_reachable as u64;

            // Determine which strategy was used based on blocks_processed/skipped
            if result.stats.blocks_processed > 0 || result.stats.blocks_skipped > 0 {
                gated_count += 1;
            } else {
                ungated_count += 1;
            }
        }
        adaptive_times.sort();
        let adaptive_p50 = adaptive_times[n_queries / 2].as_millis();
        let adaptive_p95 = adaptive_times[n_queries * 95 / 100].as_millis();
        let adaptive_avg: f64 = adaptive_times.iter().map(|d| d.as_millis() as f64).sum::<f64>() / n_queries as f64;

        // Run block-gated PHAST (always gated)
        let mut gated_times: Vec<Duration> = Vec::with_capacity(n_queries);
        for &origin in &origins {
            let start = Instant::now();
            let _ = phast.query_block_gated(origin, threshold_ms);
            gated_times.push(start.elapsed());
        }
        gated_times.sort();
        let gated_p50 = gated_times[n_queries / 2].as_millis();
        let gated_p95 = gated_times[n_queries * 95 / 100].as_millis();
        let gated_avg: f64 = gated_times.iter().map(|d| d.as_millis() as f64).sum::<f64>() / n_queries as f64;

        let avg_reachable = total_reachable as f64 / n_queries as f64;
        let reachable_pct = 100.0 * avg_reachable / n_nodes as f64;

        let strategy_desc = if gated_count > ungated_count {
            format!("Gated {}", gated_count)
        } else {
            format!("Plain {}", ungated_count)
        };

        // Print results
        println!("│ {:>6}ms │ Plain    │ {:>8} │ {:>8} │ {:>9.1} │           │          │",
            threshold_ms, plain_p50, plain_p95, plain_avg);
        println!("│          │ Block    │ {:>8} │ {:>8} │ {:>9.1} │           │          │",
            gated_p50, gated_p95, gated_avg);
        println!("│          │ Adaptive │ {:>8} │ {:>8} │ {:>9.1} │ {:>9} │ {:>6.1}%  │",
            adaptive_p50, adaptive_p95, adaptive_avg, strategy_desc, reachable_pct);
        println!("├──────────┼──────────┼──────────┼──────────┼───────────┼───────────┼──────────┤");
    }

    println!("└──────────┴──────────┴──────────┴──────────┴───────────┴───────────┴──────────┘");
    println!();

    // Analysis
    println!("───────────────────────────────────────────────────────────────");
    println!("  ANALYSIS");
    println!("───────────────────────────────────────────────────────────────");
    println!("  - Plain PHAST: unbounded, full graph scan");
    println!("  - Block-gated: always uses block-level gating");
    println!("  - Adaptive: switches based on active block ratio (>25% → plain)");
    println!();
    println!("  Adaptive should match or beat both methods:");
    println!("  - For small T (few active blocks): uses gating → fast");
    println!("  - For large T (many active blocks): skips gating overhead → no regression");
    println!();

    Ok(())
}

fn run_klane_bounded_bench(
    data_dir: &PathBuf,
    mode: &str,
    threshold_ms: u32,
    n_batches: usize,
    seed: u64,
) -> anyhow::Result<()> {
    // Convert to deciseconds (CCH weight units)
    let threshold_ds = threshold_ms / 100;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  K-LANE BLOCK-GATED PHAST BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Threshold: {} ms = {} ds ({:.1} min)", threshold_ms, threshold_ds, threshold_ms as f64 / 60000.0);
    println!("  Batches: {} × {} lanes = {} total sources", n_batches, K_LANES, n_batches * K_LANES);
    println!();

    // Load batched PHAST engine
    println!("[1/3] Loading batched PHAST engine...");
    let load_start = Instant::now();
    let engine = load_batched_phast(data_dir, mode)?;
    let n_nodes = engine.n_nodes();
    println!("  ✓ Loaded in {:.1}s ({} nodes)", load_start.elapsed().as_secs_f64(), n_nodes);
    println!();

    // Generate random sources (K per batch)
    let mut rng = StdRng::seed_from_u64(seed);
    let sources: Vec<Vec<u32>> = (0..n_batches)
        .map(|_| (0..K_LANES).map(|_| rng.gen_range(0..n_nodes as u32)).collect())
        .collect();

    // ========== Regular batched PHAST (unbounded) ==========
    println!("[2/3] Running regular batched PHAST (unbounded)...");

    let mut regular_times: Vec<Duration> = Vec::with_capacity(n_batches);
    let mut regular_stats = BatchedPhastStats::default();

    for batch in &sources {
        let start = Instant::now();
        let result = engine.query_batch(batch);
        regular_times.push(start.elapsed());

        regular_stats.upward_relaxations += result.stats.upward_relaxations;
        regular_stats.downward_relaxations += result.stats.downward_relaxations;
        regular_stats.downward_improved += result.stats.downward_improved;
    }

    let regular_total: Duration = regular_times.iter().sum();
    let regular_avg = regular_total.as_millis() as f64 / n_batches as f64;
    let regular_per_query = regular_avg / K_LANES as f64;
    println!("  ✓ Regular: {:.1}ms/batch, {:.2}ms/query effective", regular_avg, regular_per_query);
    println!();

    // ========== K-lane block-gated PHAST (bounded) ==========
    println!("[3/3] Running K-lane block-gated PHAST (bounded T={} ds)...", threshold_ds);

    let mut gated_times: Vec<Duration> = Vec::with_capacity(n_batches);
    let mut gated_stats = BatchedPhastStats::default();

    for batch in &sources {
        let start = Instant::now();
        let result = engine.query_batch_block_gated(batch, threshold_ds);
        gated_times.push(start.elapsed());

        gated_stats.upward_relaxations += result.stats.upward_relaxations;
        gated_stats.downward_relaxations += result.stats.downward_relaxations;
        gated_stats.downward_improved += result.stats.downward_improved;
        gated_stats.buffer_flushes += result.stats.buffer_flushes;  // blocks_skipped
        gated_stats.buffered_updates += result.stats.buffered_updates;  // blocks_processed
    }

    let gated_total: Duration = gated_times.iter().sum();
    let gated_avg = gated_total.as_millis() as f64 / n_batches as f64;
    let gated_per_query = gated_avg / K_LANES as f64;
    println!("  ✓ K-lane gated: {:.1}ms/batch, {:.2}ms/query effective", gated_avg, gated_per_query);
    println!();

    // ========== Correctness check ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  CORRECTNESS CHECK");
    println!("───────────────────────────────────────────────────────────────");

    // Compare first batch results
    let regular_result = engine.query_batch(&sources[0]);
    let gated_result = engine.query_batch_block_gated(&sources[0], threshold_ds);

    let mut mismatches = 0;
    for lane in 0..K_LANES {
        for node in 0..n_nodes {
            let regular_d = regular_result.dist[lane][node];
            let gated_d = gated_result.dist[lane][node];

            // Both should agree on reachability within threshold (using deciseconds)
            let regular_reachable = regular_d <= threshold_ds;
            let gated_reachable = gated_d <= threshold_ds;

            if regular_reachable != gated_reachable {
                mismatches += 1;
            } else if regular_reachable && gated_reachable && regular_d != gated_d {
                mismatches += 1;
            }
        }
    }

    if mismatches == 0 {
        println!("  ✅ All distances within threshold match!");
    } else {
        println!("  ❌ {} mismatches found!", mismatches);
    }
    println!();

    // ========== Results ==========
    println!("═══════════════════════════════════════════════════════════════");
    println!("  RESULTS");
    println!("═══════════════════════════════════════════════════════════════");

    regular_times.sort();
    gated_times.sort();

    let regular_p50 = regular_times[n_batches / 2].as_millis();
    let regular_p95 = regular_times[n_batches * 95 / 100.max(1)].as_millis();

    let gated_p50 = gated_times[n_batches / 2].as_millis();
    let gated_p95 = gated_times[n_batches * 95 / 100.max(1)].as_millis();

    println!("  Regular batched (unbounded):");
    println!("    p50 batch:  {:>6}ms", regular_p50);
    println!("    p95 batch:  {:>6}ms", regular_p95);
    println!("    Avg batch:  {:>6.1}ms", regular_avg);
    println!("    Effective/query: {:>6.2}ms", regular_per_query);
    println!("    Relaxations/batch: {:.2}M", regular_stats.downward_relaxations as f64 / n_batches as f64 / 1_000_000.0);
    println!();

    println!("  K-lane block-gated (T={}ms):", threshold_ms);
    println!("    p50 batch:  {:>6}ms", gated_p50);
    println!("    p95 batch:  {:>6}ms", gated_p95);
    println!("    Avg batch:  {:>6.1}ms", gated_avg);
    println!("    Effective/query: {:>6.2}ms", gated_per_query);
    println!("    Relaxations/batch: {:.2}M", gated_stats.downward_relaxations as f64 / n_batches as f64 / 1_000_000.0);
    println!("    Blocks processed/batch: {:.0}", gated_stats.buffered_updates as f64 / n_batches as f64);
    println!("    Blocks skipped/batch: {:.0}", gated_stats.buffer_flushes as f64 / n_batches as f64);
    println!();

    // Speedup
    let speedup = regular_avg / gated_avg;
    println!("───────────────────────────────────────────────────────────────");
    println!("  COMPARISON");
    println!("───────────────────────────────────────────────────────────────");
    println!("  Batch speedup: {:.2}x", speedup);
    println!("  Effective per-query speedup: {:.2}x", regular_per_query / gated_per_query);
    println!();

    // Analysis
    if speedup > 1.5 {
        println!("  ✅ K-lane block-gated provides significant speedup!");
        println!("     Bounded queries benefit from lane masking and block skipping.");
    } else if speedup > 1.1 {
        println!("  ✅ K-lane block-gated provides moderate speedup.");
    } else if speedup > 0.95 {
        println!("  ➡️  K-lane block-gated is roughly equivalent.");
        println!("     Consider smaller threshold for more benefit.");
    } else {
        println!("  ⚠️  K-lane block-gated is slower (overhead not worth it).");
    }
    println!();

    Ok(())
}

/// Analyze reachability fraction at various thresholds
///
/// This helps decide whether rPHAST is worth implementing:
/// - If reachable fraction is high (>80-90%), rPHAST won't help much
/// - If reachable fraction is moderate (<60%), rPHAST preprocessing may be worth it
fn run_reachability_analysis(
    data_dir: &PathBuf,
    mode: &str,
    thresholds: &[u32],
    n_origins: usize,
    seed: u64,
) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  REACHABILITY ANALYSIS FOR rPHAST DECISION");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Thresholds: {:?}", thresholds.iter().map(|t| format!("{:.1}min", *t as f64 / 60000.0)).collect::<Vec<_>>());
    println!("  Origins sampled: {}", n_origins);
    println!();

    // Load PHAST engine
    println!("[1/2] Loading PHAST engine...");
    let load_start = std::time::Instant::now();
    let phast = load_phast(data_dir, mode)?;
    let n_nodes = phast.n_nodes();
    let total_edges = phast.total_down_edges();
    println!("  ✓ Loaded in {:.1}s", load_start.elapsed().as_secs_f64());
    println!("  Total nodes: {}", n_nodes);
    println!("  Total down-edges: {:.2}M", total_edges as f64 / 1_000_000.0);
    println!();

    // Generate random origins
    let mut rng = StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_origins)
        .map(|_| rng.gen_range(0..n_nodes as u32))
        .collect();

    // Run analysis
    println!("[2/2] Analyzing reachability...");
    println!();

    // Track statistics for each threshold
    struct ThresholdStats {
        threshold: u32,
        avg_reachable_nodes_pct: f64,
        avg_reachable_edges_pct: f64,
        min_reachable_nodes_pct: f64,
        max_reachable_nodes_pct: f64,
        min_reachable_edges_pct: f64,
        max_reachable_edges_pct: f64,
    }

    let mut all_stats: Vec<ThresholdStats> = Vec::new();

    for &threshold in thresholds {
        let mut node_pcts: Vec<f64> = Vec::with_capacity(n_origins);
        let mut edge_pcts: Vec<f64> = Vec::with_capacity(n_origins);

        for &origin in &origins {
            // Run unbounded PHAST query
            let result = phast.query(origin);

            // Compute reachability at this threshold
            let (reachable_nodes, reachable_edges, total_n, total_e) =
                phast.compute_reachability(&result.dist, threshold);

            node_pcts.push(reachable_nodes as f64 / total_n as f64 * 100.0);
            edge_pcts.push(reachable_edges as f64 / total_e as f64 * 100.0);
        }

        let avg_nodes = node_pcts.iter().sum::<f64>() / n_origins as f64;
        let avg_edges = edge_pcts.iter().sum::<f64>() / n_origins as f64;
        let min_nodes = node_pcts.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_nodes = node_pcts.iter().cloned().fold(f64::NEG_INFINITY, f64::max);
        let min_edges = edge_pcts.iter().cloned().fold(f64::INFINITY, f64::min);
        let max_edges = edge_pcts.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

        all_stats.push(ThresholdStats {
            threshold,
            avg_reachable_nodes_pct: avg_nodes,
            avg_reachable_edges_pct: avg_edges,
            min_reachable_nodes_pct: min_nodes,
            max_reachable_nodes_pct: max_nodes,
            min_reachable_edges_pct: min_edges,
            max_reachable_edges_pct: max_edges,
        });
    }

    // Print results table
    println!("───────────────────────────────────────────────────────────────");
    println!("  REACHABILITY RESULTS");
    println!("───────────────────────────────────────────────────────────────");
    println!();
    println!("┌──────────┬──────────────────────────────┬──────────────────────────────┐");
    println!("│ Threshold│     Reachable Nodes (%)      │     Reachable Edges (%)      │");
    println!("│          │   avg    (min   -   max)     │   avg    (min   -   max)     │");
    println!("├──────────┼──────────────────────────────┼──────────────────────────────┤");

    for stats in &all_stats {
        let threshold_str = format!("{:.1}min", stats.threshold as f64 / 60000.0);
        println!(
            "│ {:>8} │  {:>5.1}%  ({:>5.1}% - {:>5.1}%) │  {:>5.1}%  ({:>5.1}% - {:>5.1}%) │",
            threshold_str,
            stats.avg_reachable_nodes_pct,
            stats.min_reachable_nodes_pct,
            stats.max_reachable_nodes_pct,
            stats.avg_reachable_edges_pct,
            stats.min_reachable_edges_pct,
            stats.max_reachable_edges_pct,
        );
    }

    println!("└──────────┴──────────────────────────────┴──────────────────────────────┘");
    println!();

    // Decision guidance
    println!("───────────────────────────────────────────────────────────────");
    println!("  rPHAST DECISION GUIDANCE");
    println!("───────────────────────────────────────────────────────────────");
    println!();

    for stats in &all_stats {
        let threshold_str = format!("{:.1}min", stats.threshold as f64 / 60000.0);
        let edge_pct = stats.avg_reachable_edges_pct;

        print!("  {} T={}: ", mode, threshold_str);

        if edge_pct > 90.0 {
            println!("❌ rPHAST NOT recommended (>90% edges reachable)");
            println!("     → Use K-lane batching for throughput, accept full scan");
        } else if edge_pct > 70.0 {
            println!("⚠️  rPHAST marginal benefit ({:.1}% edges)", edge_pct);
            println!("     → K-lane batching likely better ROI");
        } else if edge_pct > 40.0 {
            println!("✅ rPHAST may help ({:.1}% edges)", edge_pct);
            println!("     → Consider rPHAST if throughput not sufficient");
        } else {
            println!("✅ rPHAST recommended ({:.1}% edges)", edge_pct);
            println!("     → Preprocessing could save significant work");
        }
    }

    println!();
    println!("  Key insight: rPHAST preprocessing is only worth it when the");
    println!("  reachable fraction is well below 100%. For large thresholds");
    println!("  where most of the graph is reachable, K-lane batching is the");
    println!("  better approach for throughput optimization.");
    println!();

    Ok(())
}

/// Benchmark matrix tile streaming (compute path only, no HTTP)
fn run_matrix_stream_bench(
    data_dir: &PathBuf,
    mode: &str,
    n_sources: usize,
    n_targets: usize,
    src_tile_size: usize,
    dst_tile_size: usize,
    seed: u64,
) -> anyhow::Result<()> {
    use butterfly_route::matrix::arrow_stream::{MatrixTile, tiles_to_record_batch};
    use arrow::ipc::writer::StreamWriter;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  MATRIX STREAMING BENCHMARK");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Sources: {}", n_sources);
    println!("  Targets: {}", n_targets);
    println!("  Src tile size: {}", src_tile_size);
    println!("  Dst tile size: {}", dst_tile_size);
    println!("  Matrix size: {}x{} = {} cells", n_sources, n_targets, n_sources * n_targets);
    println!("───────────────────────────────────────────────────────────────");
    println!();

    // Load CCH data (uses path search logic that handles different directory structures)
    println!("Loading CCH data...");
    let engine = load_batched_phast(data_dir, mode)?;
    let n_nodes = engine.n_nodes();
    println!("  ✓ {} nodes loaded", n_nodes);
    println!();

    // Generate random sources and targets
    let mut rng = StdRng::seed_from_u64(seed);
    let sources: Vec<u32> = (0..n_sources)
        .map(|_| rng.gen_range(0..n_nodes as u32))
        .collect();
    let targets: Vec<u32> = (0..n_targets)
        .map(|_| rng.gen_range(0..n_nodes as u32))
        .collect();

    println!("───────────────────────────────────────────────────────────────");
    println!("  STREAMING COMPUTE + SERIALIZATION");
    println!("───────────────────────────────────────────────────────────────");

    let start = Instant::now();
    let mut total_tiles = 0usize;
    let mut total_bytes = 0usize;
    let mut total_compute_ms = 0u64;
    let mut total_serialize_ms = 0u64;

    // Align src_tile_size to K_LANES
    let effective_src_tile = ((src_tile_size + K_LANES - 1) / K_LANES) * K_LANES;

    for src_batch_start in (0..n_sources).step_by(effective_src_tile) {
        let src_batch_end = (src_batch_start + effective_src_tile).min(n_sources);
        let batch_sources = &sources[src_batch_start..src_batch_end];

        // Compute distances for this batch
        let compute_start = Instant::now();
        let (matrix, _stats) = engine.compute_matrix_flat(batch_sources, &targets);
        total_compute_ms += compute_start.elapsed().as_millis() as u64;

        // Serialize tiles
        for dst_batch_start in (0..n_targets).step_by(dst_tile_size) {
            let dst_batch_end = (dst_batch_start + dst_tile_size).min(n_targets);
            let actual_src_len = batch_sources.len();
            let actual_dst_len = dst_batch_end - dst_batch_start;

            // Extract tile data
            let mut tile_data = Vec::with_capacity(actual_src_len * actual_dst_len * 4);
            for src_offset in 0..actual_src_len {
                for dst_offset in 0..actual_dst_len {
                    let d = matrix[src_offset * n_targets + dst_batch_start + dst_offset];
                    tile_data.extend_from_slice(&d.to_le_bytes());
                }
            }

            let tile = MatrixTile {
                src_block_start: src_batch_start as u32,
                dst_block_start: dst_batch_start as u32,
                src_block_len: actual_src_len as u16,
                dst_block_len: actual_dst_len as u16,
                durations_ms: tile_data,
            };

            // Serialize to Arrow IPC
            let serialize_start = Instant::now();
            let batch = tiles_to_record_batch(&[tile])?;
            let mut buf = Vec::new();
            {
                let mut writer = StreamWriter::try_new(&mut buf, batch.schema_ref())?;
                writer.write(&batch)?;
                writer.finish()?;
            }
            total_serialize_ms += serialize_start.elapsed().as_millis() as u64;
            total_bytes += buf.len();
            total_tiles += 1;
        }
    }

    let total_time = start.elapsed();

    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  RESULTS");
    println!("───────────────────────────────────────────────────────────────");
    println!("  Total tiles: {}", total_tiles);
    println!("  Total bytes: {:.2} MB", total_bytes as f64 / 1024.0 / 1024.0);
    println!("  Total time: {:?}", total_time);
    println!();
    println!("  Compute time: {}ms ({:.1}%)", total_compute_ms,
             total_compute_ms as f64 / total_time.as_millis() as f64 * 100.0);
    println!("  Serialize time: {}ms ({:.1}%)", total_serialize_ms,
             total_serialize_ms as f64 / total_time.as_millis() as f64 * 100.0);
    println!();

    let cells = n_sources * n_targets;
    let cells_per_sec = cells as f64 / total_time.as_secs_f64();
    let mb_per_sec = total_bytes as f64 / 1024.0 / 1024.0 / total_time.as_secs_f64();
    let tiles_per_sec = total_tiles as f64 / total_time.as_secs_f64();

    println!("  Throughput:");
    println!("    Cells/sec: {:.0}", cells_per_sec);
    println!("    MB/sec: {:.2}", mb_per_sec);
    println!("    Tiles/sec: {:.1}", tiles_per_sec);
    println!();

    // Effective queries/sec (each source is essentially a PHAST query)
    let queries_per_sec = n_sources as f64 / total_time.as_secs_f64();
    println!("  Effective PHAST queries/sec: {:.1}", queries_per_sec);
    println!();

    Ok(())
}

/// Benchmark bucket-based many-to-many CH algorithm
fn run_bucket_m2m_bench(
    data_dir: &PathBuf,
    mode: &str,
    sizes: &[usize],
    parallel: bool,
    seed: u64,
) -> anyhow::Result<()> {
    use butterfly_route::formats::CchTopoFile;
    use butterfly_route::matrix::bucket_ch::table_bucket_parallel;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  BUCKET MANY-TO-MANY CH BENCHMARK {}", if parallel { "(PARALLEL)" } else { "(SEQUENTIAL)" });
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Sizes: {:?}", sizes);
    println!("  Parallel: {}", parallel);
    println!("───────────────────────────────────────────────────────────────");
    println!();

    // Load CCH data
    let topo_path = find_file(data_dir, &[
        format!("cch.{}.topo", mode),
        format!("step7-rank-aligned/cch.{}.topo", mode),
        format!("step7/cch.{}.topo", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.{}.topo", mode))?;

    let weights_path = find_file(data_dir, &[
        format!("cch.w.{}.u32", mode),
        format!("step8-rank-aligned/cch.w.{}.u32", mode),
        format!("step8/cch.w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.w.{}.u32", mode))?;

    println!("Loading CCH topology from {:?}...", topo_path);
    let topo = CchTopoFile::read(&topo_path)?;
    let n_nodes = topo.n_nodes as usize;
    println!("  ✓ {} nodes, {} up edges, {} down edges",
             n_nodes, topo.up_targets.len(), topo.down_targets.len());

    println!("Loading CCH weights from {:?}...", weights_path);
    let weights = CchWeightsFile::read(&weights_path)?;
    let up_inf = weights.up.iter().filter(|&&w| w == u32::MAX).count();
    let down_inf = weights.down.iter().filter(|&&w| w == u32::MAX).count();
    println!("  ✓ {} up weights ({} INF = {:.1}%), {} down weights ({} INF = {:.1}%)",
             weights.up.len(), up_inf, 100.0 * up_inf as f64 / weights.up.len() as f64,
             weights.down.len(), down_inf, 100.0 * down_inf as f64 / weights.down.len() as f64);

    println!("Building UpAdjFlat (optimized forward adjacency)...");
    let up_adj_flat = UpAdjFlat::build(&topo, &weights);
    println!("  ✓ {} flat forward entries", up_adj_flat.targets.len());

    println!("Building DownReverseAdjFlat (optimized backward adjacency)...");
    let down_rev_flat = DownReverseAdjFlat::build(&topo, &weights);
    println!("  ✓ {} flat reverse entries", down_rev_flat.sources.len());

    println!("Building UpReverseAdjFlat (for stall-on-demand)...");
    let up_rev_flat = UpReverseAdjFlat::build(&topo, &weights);
    println!("  ✓ {} incoming UP entries", up_rev_flat.sources.len());

    println!("Building DownReverseAdj (for P2P validation)...");
    let down_rev = build_down_rev(&topo);
    println!("  ✓ {} reverse entries", down_rev.sources.len());
    println!();

    // Run benchmarks for each size
    println!("───────────────────────────────────────────────────────────────");
    println!("  BENCHMARK RESULTS");
    println!("───────────────────────────────────────────────────────────────");
    println!();
    println!("{:>8} {:>12} {:>10} {:>12} {:>10} {:>10} {:>10} {:>10}",
             "Size", "Cells", "Time(ms)", "Cells/sec", "Fwd Vis", "Bwd Vis", "Joins", "Stale%");
    println!("{}", "-".repeat(100));

    let mut rng = StdRng::seed_from_u64(seed);

    // Create reusable engine to avoid per-call allocations
    let mut engine = BucketM2MEngine::new(n_nodes);

    for &n in sizes {
        // Generate random sources and targets
        let sources: Vec<u32> = (0..n)
            .map(|_| rng.gen_range(0..n_nodes as u32))
            .collect();
        let targets: Vec<u32> = (0..n)
            .map(|_| rng.gen_range(0..n_nodes as u32))
            .collect();

        // Warmup run
        if parallel {
            let _ = table_bucket_parallel(n_nodes, &up_adj_flat, &down_rev_flat, &sources, &targets);
        } else {
            let _ = engine.compute(&topo, &weights, &down_rev_flat, &sources, &targets);
        }

        // Benchmark run (average of 3)
        let mut times = Vec::new();
        let mut last_stats = None;

        for _ in 0..3 {
            let start = Instant::now();
            let (_, stats) = if parallel {
                table_bucket_parallel(n_nodes, &up_adj_flat, &down_rev_flat, &sources, &targets)
            } else {
                engine.compute(&topo, &weights, &down_rev_flat, &sources, &targets)
            };
            times.push(start.elapsed());
            last_stats = Some(stats);
        }

        let avg_time = times.iter().map(|t| t.as_secs_f64()).sum::<f64>() / times.len() as f64;
        let avg_time_ms = avg_time * 1000.0;
        let cells = n * n;
        let cells_per_sec = cells as f64 / avg_time;

        let stats = last_stats.unwrap();
        let fwd_vis_avg = stats.forward_visited / n;
        let bwd_vis_avg = stats.backward_visited / n;

        let stale_pct = if stats.heap_pops > 0 {
            100.0 * stats.stale_pops as f64 / stats.heap_pops as f64
        } else {
            0.0
        };

        println!("{:>8} {:>12} {:>10.1} {:>12.0} {:>10} {:>10} {:>10} {:>9.1}%",
                 format!("{}×{}", n, n),
                 cells,
                 avg_time_ms,
                 cells_per_sec,
                 fwd_vis_avg,
                 bwd_vis_avg,
                 stats.join_operations,
                 stale_pct);
        println!("         Fwd: {}ms, Sort: {}ms, Bwd: {}ms",
                 stats.forward_time_ms, stats.sort_time_ms, stats.backward_time_ms);
    }

    // ========== Stall-on-Demand Benchmark ==========
    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  STALL-ON-DEMAND BENCHMARK");
    println!("───────────────────────────────────────────────────────────────");
    println!();
    println!("{:>8} {:>12} {:>10} {:>10} {:>10} {:>10} {:>10}",
             "Size", "Cells", "Time(ms)", "Stalls", "NonStall", "StallRate", "vs Base");
    println!("{}", "-".repeat(80));

    let mut rng2 = StdRng::seed_from_u64(seed + 1000);  // Different seed for variety

    for &n in sizes {
        // Generate random sources and targets
        let sources: Vec<u32> = (0..n)
            .map(|_| rng2.gen_range(0..n_nodes as u32))
            .collect();
        let targets: Vec<u32> = (0..n)
            .map(|_| rng2.gen_range(0..n_nodes as u32))
            .collect();

        // Baseline (without stall)
        let base_start = Instant::now();
        let (base_matrix, _) = engine.compute_flat(&up_adj_flat, &down_rev_flat, &sources, &targets);
        let base_time = base_start.elapsed();

        // With stall-on-demand
        let stall_start = Instant::now();
        let (stall_matrix, stats, stalls, non_stalls) = engine.compute_with_stall(
            &up_adj_flat, &up_rev_flat, &down_rev_flat, &sources, &targets
        );
        let stall_time = stall_start.elapsed();

        // Verify correctness
        let mut mismatches = 0;
        for i in 0..(n * n) {
            if base_matrix[i] != stall_matrix[i] {
                mismatches += 1;
            }
        }

        let stall_rate = if stalls + non_stalls > 0 {
            100.0 * stalls as f64 / (stalls + non_stalls) as f64
        } else {
            0.0
        };

        let speedup = base_time.as_secs_f64() / stall_time.as_secs_f64();

        println!("{:>8} {:>12} {:>10.1} {:>10} {:>10} {:>9.1}% {:>9.2}x",
                 format!("{}×{}", n, n),
                 n * n,
                 stall_time.as_secs_f64() * 1000.0,
                 stalls,
                 non_stalls,
                 stall_rate,
                 speedup);

        if mismatches > 0 {
            println!("         ⚠️  {} mismatches vs baseline!", mismatches);
        }
    }

    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  TARGET COMPARISON");
    println!("───────────────────────────────────────────────────────────────");
    println!("  OSRM benchmark (CH, Belgium, sequential):");
    println!("    10×10: 6ms, 25×25: 10ms, 50×50: 17ms, 100×100: 35ms");
    println!();

    // ========== Correctness Validation ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  CORRECTNESS VALIDATION (5×5 vs P2P)");
    println!("───────────────────────────────────────────────────────────────");

    // Use fresh random sources/targets for validation
    let mut val_rng = StdRng::seed_from_u64(12345);
    let val_sources: Vec<u32> = (0..5).map(|_| val_rng.gen_range(0..n_nodes as u32)).collect();
    let val_targets: Vec<u32> = (0..5).map(|_| val_rng.gen_range(0..n_nodes as u32)).collect();

    // Run bucket M2M (using the same engine as benchmarks)
    let (m2m_matrix, _) = engine.compute(&topo, &weights, &down_rev_flat, &val_sources, &val_targets);

    // Run P2P queries for comparison
    let mut mismatches = 0;
    let mut checked = 0;

    for (si, &s) in val_sources.iter().enumerate() {
        for (ti, &t) in val_targets.iter().enumerate() {
            let m2m_dist = m2m_matrix[si * 5 + ti];

            // Run P2P query using the same algorithm as the server
            let p2p_dist = run_p2p_query(&topo, &weights, &down_rev, s, t);

            checked += 1;
            if m2m_dist != p2p_dist {
                mismatches += 1;
                if mismatches <= 5 {
                    println!("  MISMATCH: s={}, t={}: M2M={}, P2P={}",
                             s, t,
                             if m2m_dist == u32::MAX { "INF".to_string() } else { m2m_dist.to_string() },
                             if p2p_dist == u32::MAX { "INF".to_string() } else { p2p_dist.to_string() });
                }
            }
        }
    }

    if mismatches == 0 {
        println!("  ✓ All {} queries match P2P results!", checked);
    } else {
        println!("  ✗ {} / {} mismatches found", mismatches, checked);
    }
    println!();

    Ok(())
}

/// Run a single P2P query using bidirectional Dijkstra (same as server)
/// Returns distance or u32::MAX if unreachable
fn run_p2p_query(
    topo: &butterfly_route::formats::CchTopo,
    weights: &butterfly_route::formats::CchWeights,
    down_rev: &DownReverseAdj,
    source: u32,
    target: u32,
) -> u32 {
    use std::collections::BinaryHeap;

    if source == target {
        return 0;
    }

    let n = topo.n_nodes as usize;
    let mut dist_fwd = vec![u32::MAX; n];
    let mut dist_bwd = vec![u32::MAX; n];
    let mut pq_fwd: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
    let mut pq_bwd: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();

    dist_fwd[source as usize] = 0;
    dist_bwd[target as usize] = 0;
    pq_fwd.push(Reverse((0, source)));
    pq_bwd.push(Reverse((0, target)));

    let mut best_dist = u32::MAX;

    while !pq_fwd.is_empty() || !pq_bwd.is_empty() {
        // Forward step - UP edges
        if let Some(Reverse((d, u))) = pq_fwd.pop() {
            if d > dist_fwd[u as usize] {
                continue;
            }

            // Check meeting
            if dist_bwd[u as usize] != u32::MAX {
                let total = d.saturating_add(dist_bwd[u as usize]);
                if total < best_dist {
                    best_dist = total;
                }
            }

            // Relax UP edges
            let start = topo.up_offsets[u as usize] as usize;
            let end = topo.up_offsets[u as usize + 1] as usize;
            for i in start..end {
                let v = topo.up_targets[i];
                let w = weights.up[i];
                if w == u32::MAX { continue; }
                let new_d = d.saturating_add(w);
                if new_d < dist_fwd[v as usize] {
                    dist_fwd[v as usize] = new_d;
                    pq_fwd.push(Reverse((new_d, v)));
                    if dist_bwd[v as usize] != u32::MAX {
                        let total = new_d.saturating_add(dist_bwd[v as usize]);
                        if total < best_dist { best_dist = total; }
                    }
                }
            }
        }

        // Backward step - reversed DOWN edges
        if let Some(Reverse((d, u))) = pq_bwd.pop() {
            if d > dist_bwd[u as usize] {
                continue;
            }

            // Check meeting
            if dist_fwd[u as usize] != u32::MAX {
                let total = d.saturating_add(dist_fwd[u as usize]);
                if total < best_dist {
                    best_dist = total;
                }
            }

            // Relax reversed DOWN edges
            let start = down_rev.offsets[u as usize] as usize;
            let end = down_rev.offsets[u as usize + 1] as usize;
            for i in start..end {
                let x = down_rev.sources[i];
                let edge_idx = down_rev.edge_idx[i] as usize;
                let w = weights.down[edge_idx];
                if w == u32::MAX { continue; }
                let new_d = d.saturating_add(w);
                if new_d < dist_bwd[x as usize] {
                    dist_bwd[x as usize] = new_d;
                    pq_bwd.push(Reverse((new_d, x)));
                    if dist_fwd[x as usize] != u32::MAX {
                        let total = new_d.saturating_add(dist_fwd[x as usize]);
                        if total < best_dist { best_dist = total; }
                    }
                }
            }
        }
    }

    best_dist
}

/// Build reverse adjacency for DOWN edges
///
/// For each node u, collects all nodes x that have DOWN edges x→u.
/// This enables reverse search: given target t, find all nodes that can reach t.
fn build_down_rev(topo: &butterfly_route::formats::CchTopo) -> DownReverseAdj {
    let n_nodes = topo.n_nodes as usize;

    // Count incoming DOWN edges for each node
    let mut counts = vec![0u32; n_nodes];
    for &target in &topo.down_targets {
        counts[target as usize] += 1;
    }

    // Build offsets
    let mut offsets = vec![0u64; n_nodes + 1];
    for i in 0..n_nodes {
        offsets[i + 1] = offsets[i] + counts[i] as u64;
    }

    let total_edges = offsets[n_nodes] as usize;
    let mut sources = vec![0u32; total_edges];
    let mut edge_idx = vec![0u32; total_edges];

    // Reset counts for filling
    counts.fill(0);

    // Fill reverse edges
    // For each source node src, iterate its outgoing DOWN edges
    for src in 0..n_nodes {
        let start = topo.down_offsets[src] as usize;
        let end = topo.down_offsets[src + 1] as usize;

        for i in start..end {
            let target = topo.down_targets[i] as usize;
            let pos = offsets[target] as usize + counts[target] as usize;
            sources[pos] = src as u32;
            edge_idx[pos] = i as u32;
            counts[target] += 1;
        }
    }

    DownReverseAdj { offsets, sources, edge_idx }
}

/// Compare dense vs sparse contour generation
fn run_contour_compare_bench(
    data_dir: &PathBuf,
    mode: &str,
    threshold_ds: u32,
    n_queries: usize,
    seed: u64,
) -> anyhow::Result<()> {
    println!("═══════════════════════════════════════════════════════════════");
    println!("  CONTOUR COMPARISON BENCHMARK (dense vs sparse)");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Mode: {}", mode);
    println!("  Threshold: {} ds ({:.1} min)", threshold_ds, threshold_ds as f64 / 600.0);
    println!("  Queries: {}", n_queries);

    // Load data
    println!("\n[1/3] Loading data...");
    let topo_path = data_dir.join(format!("step7/cch.{}.topo", mode));
    let weights_path = data_dir.join(format!("step8/cch.w.{}.u32", mode));
    let order_path = data_dir.join(format!("step6/order.{}.ebg", mode));

    let phast = PhastEngine::load(&topo_path, &weights_path, &order_path)?;
    let n_nodes = phast.n_nodes();
    println!("  ✓ Loaded PHAST ({} nodes)", n_nodes);

    let filtered_ebg_path = data_dir.join(format!("step5/filtered.{}.ebg", mode));
    let ebg_nodes_path = data_dir.join("step4/ebg.nodes");
    let nbg_geo_path = data_dir.join("step3/nbg.geo");
    let weights_base_path = data_dir.join(format!("step5/w.{}.u32", mode));

    let extractor = FrontierExtractor::load(
        &filtered_ebg_path,
        &ebg_nodes_path,
        &nbg_geo_path,
        &weights_base_path,
    )?;
    println!("  ✓ Loaded frontier extractor");

    // Config for both methods
    let dense_config = match mode {
        "car" => GridConfig::for_car(),
        "bike" => GridConfig::for_bike(),
        "foot" => GridConfig::for_foot(),
        _ => GridConfig::for_bike(),
    };
    let sparse_config = match mode {
        "car" => SparseContourConfig::for_car(),
        "bike" => SparseContourConfig::for_bike(),
        "foot" => SparseContourConfig::for_foot(),
        _ => SparseContourConfig::for_bike(),
    };

    // Generate random origins
    println!("\n[2/3] Generating {} random origins...", n_queries);
    let mut rng = rand::rngs::StdRng::seed_from_u64(seed);
    let origins: Vec<u32> = (0..n_queries)
        .map(|_| rng.gen_range(0..n_nodes as u32))
        .collect();

    // Run benchmarks
    println!("\n[3/3] Running comparison...\n");

    let mut dense_times: Vec<u64> = Vec::with_capacity(n_queries);
    let mut sparse_times: Vec<u64> = Vec::with_capacity(n_queries);
    let mut dense_tiles: Vec<usize> = Vec::with_capacity(n_queries);
    let mut sparse_tiles: Vec<usize> = Vec::with_capacity(n_queries);
    let mut vertex_diffs: Vec<i64> = Vec::with_capacity(n_queries);

    // Track sparse timing breakdown
    let mut sparse_stamp_times: Vec<u64> = Vec::with_capacity(n_queries);
    let mut sparse_morph_times: Vec<u64> = Vec::with_capacity(n_queries);
    let mut sparse_contour_times: Vec<u64> = Vec::with_capacity(n_queries);
    let mut sparse_simplify_times: Vec<u64> = Vec::with_capacity(n_queries);

    // Convert threshold_ds to threshold_ms for segment extraction
    let threshold_ms = threshold_ds * 100;

    for (i, &origin) in origins.iter().enumerate() {
        // Run PHAST + frontier (same for both)
        let result = phast.query_active_set(origin, threshold_ds);
        let segments = extractor.extract_reachable_segments(&result.dist, threshold_ms);

        if segments.is_empty() {
            continue;
        }

        // Dense contour
        let dense_start = Instant::now();
        let dense_result = generate_contour(&segments, &dense_config)?;
        let dense_time = dense_start.elapsed().as_micros() as u64;
        dense_times.push(dense_time);
        dense_tiles.push(dense_result.stats.grid_cols * dense_result.stats.grid_rows);

        // Sparse contour
        let sparse_start = Instant::now();
        let sparse_result = generate_sparse_contour(&segments, &sparse_config)?;
        let sparse_time = sparse_start.elapsed().as_micros() as u64;
        sparse_times.push(sparse_time);
        sparse_tiles.push(sparse_result.stats.active_tiles_after_morphology * 64 * 64);
        sparse_stamp_times.push(sparse_result.stats.stamp_time_us);
        sparse_morph_times.push(sparse_result.stats.morphology_time_us);
        sparse_contour_times.push(sparse_result.stats.contour_time_us);
        sparse_simplify_times.push(sparse_result.stats.simplify_time_us);

        // Track vertex difference
        let dense_verts = dense_result.stats.contour_vertices_after_simplify as i64;
        let sparse_verts = sparse_result.stats.contour_vertices_after_simplify as i64;
        vertex_diffs.push(dense_verts - sparse_verts);

        if (i + 1) % 10 == 0 || i == n_queries - 1 {
            print!("\r  Progress: {}/{}", i + 1, n_queries);
            std::io::Write::flush(&mut std::io::stdout())?;
        }
    }
    println!("\n");

    if dense_times.is_empty() {
        println!("  No valid queries (all origins had empty reachable sets)");
        return Ok(());
    }

    // Sort for percentiles
    dense_times.sort();
    sparse_times.sort();

    let n = dense_times.len();
    let p50 = n / 2;
    let p90 = n * 90 / 100;
    let p95 = n * 95 / 100;
    let p99 = (n * 99 / 100).min(n - 1);

    let dense_sum: u64 = dense_times.iter().sum();
    let sparse_sum: u64 = sparse_times.iter().sum();
    let dense_mean = dense_sum as f64 / n as f64;
    let sparse_mean = sparse_sum as f64 / n as f64;

    println!("───────────────────────────────────────────────────────────────");
    println!("  DENSE CONTOUR (μs)");
    println!("───────────────────────────────────────────────────────────────");
    println!("    min:  {:>12}", dense_times[0]);
    println!("    p50:  {:>12}", dense_times[p50]);
    println!("    p90:  {:>12}", dense_times[p90]);
    println!("    p95:  {:>12}", dense_times[p95]);
    println!("    p99:  {:>12}", dense_times[p99]);
    println!("    max:  {:>12}", dense_times[n - 1]);
    println!("    mean: {:>12.1}", dense_mean);

    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  SPARSE CONTOUR (μs)");
    println!("───────────────────────────────────────────────────────────────");
    println!("    min:  {:>12}", sparse_times[0]);
    println!("    p50:  {:>12}", sparse_times[p50]);
    println!("    p90:  {:>12}", sparse_times[p90]);
    println!("    p95:  {:>12}", sparse_times[p95]);
    println!("    p99:  {:>12}", sparse_times[p99]);
    println!("    max:  {:>12}", sparse_times[n - 1]);
    println!("    mean: {:>12.1}", sparse_mean);

    // Sparse timing breakdown
    sparse_stamp_times.sort();
    sparse_morph_times.sort();
    sparse_contour_times.sort();
    sparse_simplify_times.sort();

    let stamp_mean = sparse_stamp_times.iter().sum::<u64>() as f64 / n as f64;
    let morph_mean = sparse_morph_times.iter().sum::<u64>() as f64 / n as f64;
    let contour_mean = sparse_contour_times.iter().sum::<u64>() as f64 / n as f64;
    let simplify_mean = sparse_simplify_times.iter().sum::<u64>() as f64 / n as f64;

    println!();
    println!("  SPARSE TIMING BREAKDOWN (μs mean / p99):");
    println!("    Stamp:    {:>8.0} / {:>8}", stamp_mean, sparse_stamp_times[p99]);
    println!("    Morph:    {:>8.0} / {:>8}", morph_mean, sparse_morph_times[p99]);
    println!("    Contour:  {:>8.0} / {:>8}", contour_mean, sparse_contour_times[p99]);
    println!("    Simplify: {:>8.0} / {:>8}", simplify_mean, sparse_simplify_times[p99]);

    println!();
    println!("───────────────────────────────────────────────────────────────");
    println!("  COMPARISON");
    println!("───────────────────────────────────────────────────────────────");
    println!("    Speedup (mean):  {:.2}x", dense_mean / sparse_mean);
    println!("    Speedup (p50):   {:.2}x", dense_times[p50] as f64 / sparse_times[p50] as f64);
    println!("    Speedup (p99):   {:.2}x", dense_times[p99] as f64 / sparse_times[p99].max(1) as f64);

    let dense_tiles_mean: f64 = dense_tiles.iter().sum::<usize>() as f64 / n as f64;
    let sparse_tiles_mean: f64 = sparse_tiles.iter().sum::<usize>() as f64 / n as f64;
    println!();
    println!("    Dense grid cells (mean):  {:.0}", dense_tiles_mean);
    println!("    Sparse grid cells (mean): {:.0}", sparse_tiles_mean);
    println!("    Cell reduction:           {:.1}%", 100.0 * (1.0 - sparse_tiles_mean / dense_tiles_mean));

    let vertex_diff_mean: f64 = vertex_diffs.iter().sum::<i64>() as f64 / n as f64;
    println!();
    println!("    Vertex difference (mean): {:.1} (dense - sparse)", vertex_diff_mean);

    if sparse_mean < dense_mean {
        println!();
        println!("  ✅ SPARSE is {:.1}x FASTER on average!", dense_mean / sparse_mean);
    } else {
        println!();
        println!("  ⚠️  DENSE is faster (sparse overhead may dominate for small areas)");
    }

    Ok(())
}
