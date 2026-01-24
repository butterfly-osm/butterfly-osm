//! Benchmark harness for bulk performance testing
//!
//! Supports:
//! - Single isochrone benchmarks
//! - Batch isochrone benchmarks
//! - Matrix tile benchmarks
//!
//! Outputs: p50/p95/p99 times + detailed counters

use std::path::PathBuf;
use std::time::{Duration, Instant};

use clap::{Parser, Subcommand};
use hdrhistogram::Histogram;
use rand::prelude::*;

use butterfly_route::range::phast::{PhastEngine, PhastStats};
use butterfly_route::range::frontier::FrontierExtractor;
use butterfly_route::range::contour::{generate_contour, GridConfig};
use butterfly_route::matrix::batched_phast::{BatchedPhastEngine, K_LANES};

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
    let grid_config = match mode {
        "car" => GridConfig::for_car(),
        "bike" => GridConfig::for_bike(),
        "foot" => GridConfig::for_foot(),
        _ => GridConfig::for_bike(),
    };

    println!("  ✓ Loaded in {:.1}s", load_start.elapsed().as_secs_f64());
    println!("  ✓ PHAST nodes: {}", phast.n_nodes());
    println!();

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

        // PHAST
        let phast_start = Instant::now();
        let result = phast.query_bounded(origin, threshold_ms);
        let phast_time = phast_start.elapsed();

        // Frontier extraction
        let frontier_start = Instant::now();
        let segments = extractor.extract_reachable_segments(&result.dist, threshold_ms);
        let frontier_time = frontier_start.elapsed();

        // Contour generation
        let contour_start = Instant::now();
        let contour = generate_contour(&segments, &grid_config)?;
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
        agg_stats.grid_cells += contour.stats.filled_cells as u64;
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

    let topo_path = find_file(data_dir, &[
        format!("cch.{}.topo", mode),
        format!("step7-belgium-fixed/cch.{}.topo", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.{}.topo", mode))?;

    let weights_path = find_file(data_dir, &[
        format!("cch.w.{}.u32", mode),
        format!("step8-belgium-fixed/cch.w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.w.{}.u32", mode))?;

    let order_path = find_file(data_dir, &[
        format!("order.{}.ebg", mode),
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

    // ========== K-lane batched PHAST ==========
    println!("[3/3] Running K-lane batched PHAST ({} sources in {} batches)...",
        n_sources, (n_sources + K_LANES - 1) / K_LANES);

    let batched_start = Instant::now();
    let (batched_matrix, batched_stats) = batched_phast.compute_matrix_flat(&sources, &verification_targets);
    let batched_time = batched_start.elapsed();

    println!("  ✓ Batched PHAST: {:.2}s ({:.1} queries/sec)",
        batched_time.as_secs_f64(),
        n_sources as f64 / batched_time.as_secs_f64());
    println!();

    // ========== Verify correctness ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  CORRECTNESS VERIFICATION");
    println!("───────────────────────────────────────────────────────────────");

    let n_targets_actual = verification_targets.len();
    let mut mismatches = 0;
    let mut max_diff: i64 = 0;

    for (src_idx, single_dist) in single_results.iter().enumerate() {
        for (tgt_idx, &expected) in single_dist.iter().enumerate() {
            let actual = batched_matrix[src_idx * n_targets_actual + tgt_idx];
            if expected != actual {
                mismatches += 1;
                let diff = (expected as i64 - actual as i64).abs();
                max_diff = max_diff.max(diff);
                if mismatches <= 3 {
                    println!("  Mismatch: src={} tgt={}: expected={}, actual={}",
                        src_idx, tgt_idx, expected, actual);
                }
            }
        }
    }

    if mismatches == 0 {
        println!("  ✅ All {} × {} = {} distances match!",
            n_sources, n_targets_actual, n_sources * n_targets_actual);
    } else {
        println!("  ❌ {} mismatches (max diff: {})", mismatches, max_diff);
    }
    println!();

    // ========== Performance comparison ==========
    println!("───────────────────────────────────────────────────────────────");
    println!("  PERFORMANCE COMPARISON");
    println!("───────────────────────────────────────────────────────────────");

    let speedup = single_time.as_secs_f64() / batched_time.as_secs_f64();
    println!("  Single-source time:    {:>8.2}s", single_time.as_secs_f64());
    println!("  K-lane batched time:   {:>8.2}s", batched_time.as_secs_f64());
    println!("  Speedup:               {:>8.2}x", speedup);
    println!("  Expected speedup:      {:>8.2}x (K-lanes amortization)", K_LANES as f64 * 0.8);
    println!();
    println!("  Batched stats:");
    println!("    Upward relaxations:   {:>12}", format_number(batched_stats.upward_relaxations as u64));
    println!("    Downward relaxations: {:>12}", format_number(batched_stats.downward_relaxations as u64));
    println!("    Downward improved:    {:>12}", format_number(batched_stats.downward_improved as u64));
    println!("    Upward time:          {:>12} ms", batched_stats.upward_time_ms);
    println!("    Downward time:        {:>12} ms", batched_stats.downward_time_ms);
    println!();

    if speedup < 1.5 {
        println!("  ⚠️  Speedup lower than expected - check cache efficiency");
    } else if speedup >= K_LANES as f64 * 0.5 {
        println!("  ✅ Good speedup from K-lane batching!");
    }
    println!();

    Ok(())
}

fn load_batched_phast(data_dir: &PathBuf, mode: &str) -> anyhow::Result<BatchedPhastEngine> {
    let topo_path = find_file(data_dir, &[
        format!("cch.{}.topo", mode),
        format!("step7-belgium-fixed/cch.{}.topo", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.{}.topo", mode))?;

    let weights_path = find_file(data_dir, &[
        format!("cch.w.{}.u32", mode),
        format!("step8-belgium-fixed/cch.w.{}.u32", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find cch.w.{}.u32", mode))?;

    let order_path = find_file(data_dir, &[
        format!("order.{}.ebg", mode),
        format!("step6-belgium-fixed/order.{}.ebg", mode),
    ]).ok_or_else(|| anyhow::anyhow!("Cannot find order.{}.ebg", mode))?;

    BatchedPhastEngine::load(&topo_path, &weights_path, &order_path)
}
