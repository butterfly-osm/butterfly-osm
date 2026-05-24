//! Weight distribution profiler (#298).
//!
//! Loads a region container (or a step-tree directory) and emits a
//! deterministic, machine-readable JSON + human markdown report covering
//! the five measurements that gate #279 (lossless u24 + overflow encoding)
//! and #297 (cs → s, mm → m unit change):
//!
//!   A. Static distribution per (mode, metric, direction).
//!   B. Hot-query-weighted distribution (100 corpus OD + 10 000 random).
//!   C. Per-block range histograms (for per-block bit-width codec).
//!   D. Cumulative rounding sensitivity at the new units.
//!   E. Triangle relaxation tie rate at cs vs s precision.
//!
//! The profiler is read-only: it never mutates `ServerState` and uses the
//! existing `CchQuery::distance` serve-path for hot-query instrumentation
//! by way of a thread-local counter that the relaxation loop reads.
//!
//! All RNG draws use `StdRng::seed_from_u64(WEIGHT_PROFILE_SEED)` so the
//! same Belgium container produces bit-identical reports across runs.

use std::cmp::Reverse;
use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use priority_queue::PriorityQueue;
use rand::{RngExt, SeedableRng, rngs::StdRng};
use rayon::prelude::*;
use serde::Serialize;

use butterfly_route::matrix::bucket_ch::{DownReverseAdjFlat, UpAdjFlat};
use butterfly_route::server::state::ServerState;

/// Fixed seed for every RNG draw in this profiler (10 000 random OD
/// pairs, 1 000 rounding-sensitivity routes). Picked once; never
/// changed so two runs of the profiler on the same data emit
/// byte-identical JSON.
pub const WEIGHT_PROFILE_SEED: u64 = 0x0B07_7E_F1;

/// JSON schema version. Bump on any breaking layout change (added
/// fields are *not* breaking; removed/renamed fields *are*). The
/// consuming codec-evaluator tools key off this string.
pub const SCHEMA_VERSION: &str = "0.1";

// ---------- JSON schema ----------------------------------------------------

#[derive(Debug, Serialize)]
pub struct Report {
    pub version: String,
    pub region: String,
    pub git_sha: String,
    pub generated_at: String,
    pub seed: String,
    /// Mode → per-mode bundle. Keys are sorted by `serde_json`'s
    /// default BTreeMap ordering so the JSON is byte-deterministic.
    pub modes: BTreeMap<String, ModeReport>,
}

#[derive(Debug, Serialize)]
pub struct ModeReport {
    pub time: MetricReport,
    pub dist: MetricReport,
}

#[derive(Debug, Serialize)]
pub struct MetricReport {
    /// Section A — static distribution per direction (up + down).
    pub statik: DirectionPair<StaticStats>,
    /// Section C — per-block range histograms per direction.
    pub blocks: DirectionPair<BlockStats>,
    /// Section B — hot-query-weighted distribution (combined across
    /// directions because each bidirectional CCH query touches both
    /// the UP and DOWN graphs).
    pub hot: HotStats,
}

#[derive(Debug, Serialize)]
pub struct DirectionPair<T> {
    pub up: T,
    pub down: T,
}

// ---- Section A ------------------------------------------------------------

#[derive(Debug, Serialize, Default, Clone)]
pub struct StaticStats {
    pub n_edges: u64,
    pub n_inf: u64,
    pub min: u64,
    pub max: u64,
    pub p50: u64,
    pub p90: u64,
    pub p99: u64,
    pub p99_9: u64,
    pub p99_99: u64,
    pub distinct_count: u64,
    pub buckets_cs: BucketCounts,
    pub buckets_s: BucketCounts,
    pub buckets_ds: BucketCounts,
    /// Log-spaced histogram. Keys are the bucket upper bound or
    /// `"inf"` for the INF bucket. Values are edge counts.
    pub log_histogram: BTreeMap<String, u64>,
}

// ---- Section B ------------------------------------------------------------

#[derive(Debug, Serialize, Default, Clone)]
pub struct HotStats {
    /// Number of OD queries that ran (queries that fail to snap or
    /// reach are still counted into the denominator — they contribute
    /// 0 relaxations).
    pub n_queries_total: u64,
    pub n_queries_reached: u64,
    pub n_queries_unreachable: u64,
    /// Number of `relaxed_edges` visits across all queries. One edge
    /// can be relaxed multiple times (lazy reinsertion) and each
    /// visit is counted — this is the metric the codec actually
    /// cares about because each visit is a weight load from memory.
    pub n_relaxed_total: u64,
    /// Number of *distinct* edges visited at least once across all
    /// queries. This distinguishes "hot" edges (visited many times)
    /// from "cold" edges (visited once).
    pub n_unique_edges_visited: u64,
    /// Weighted overflow rates at each codec threshold, computed in
    /// the *same unit* as the codec target. `r = #relaxations of
    /// edges with weight > threshold / #relaxations total`. INF
    /// edges never appear here because the flats filter them out at
    /// build time (the bidirectional search never sees them).
    pub overflow_rates_cs: OverflowRates,
    pub overflow_rates_s: OverflowRates,
    pub overflow_rates_ds: OverflowRates,
    /// Same shape as Section A's log histogram, but bucketed by the
    /// relaxation count instead of the static edge count.
    pub log_histogram: BTreeMap<String, u64>,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct OverflowRates {
    /// `r_u8` = relaxations of edges with quantised weight > 255.
    pub u8: f64,
    /// `r_u12` = relaxations of edges with quantised weight > 4095.
    pub u12: f64,
    /// `r_u14` = relaxations of edges with quantised weight > 16383.
    pub u14: f64,
    /// `r_u16` = relaxations of edges with quantised weight > 65534
    /// (the largest u16 value that doesn't collide with a hypothetical
    /// 65 535 sentinel).
    pub u16: f64,
    /// `r_u24` = relaxations of edges with quantised weight > 16 777 214.
    pub u24: f64,
    /// Raw count of relaxations over the u16 threshold. Useful when
    /// the consumer wants to double-check the rate against
    /// `n_relaxed_total`.
    pub n_over_u16: u64,
    /// Raw count of relaxations over the u24 threshold.
    pub n_over_u24: u64,
}

// ---- Section C ------------------------------------------------------------

#[derive(Debug, Serialize, Default, Clone)]
pub struct BlockStats {
    /// Keyed by block size as a stringified integer (`"32"`, `"64"`,
    /// `"128"`). JSON consumers can deserialise either as a string
    /// key or pre-parse to an integer.
    pub by_block_size: BTreeMap<String, BlockSizeStats>,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct BlockSizeStats {
    pub block_size: u32,
    pub n_blocks: u64,
    /// Histogram of `bits_needed = ceil(log2(block_range + 1))`,
    /// where `block_range = max - min` over the non-INF weights in
    /// the block. Indexed by bit count from 0 up to 32. INF-only
    /// blocks are counted into `n_inf_blocks` and omitted from this
    /// histogram so the codec evaluator sees only real bit widths.
    pub bits_needed_histogram: Vec<u64>,
    /// Number of blocks that consist *entirely* of INF entries (and
    /// therefore have no meaningful range). These are encoded as a
    /// single sentinel by every reasonable codec.
    pub n_inf_blocks: u64,
    /// Distinct-value histogram per block. Indexed `[0]` = "blocks
    /// with 0 distinct" (== `n_inf_blocks`), `[i]` = number of blocks
    /// with exactly `i` distinct non-INF values, capped at `block_size`.
    pub distinct_per_block_histogram: Vec<u64>,
    /// Mean and max of `bits_needed` across non-INF blocks. Cheap
    /// summary so the JSON consumer doesn't have to re-derive from
    /// the histogram for the headline number.
    pub mean_bits: f64,
    pub max_bits: u32,
}

#[derive(Debug, Serialize, Default, Clone)]
pub struct BucketCounts {
    /// Edges with weight `≤ 65 534` (fits in `u16`, no sentinel
    /// collision). Computed in the same units as the codec target —
    /// raw cs for `buckets_cs`, raw/100 for `buckets_s`, raw/10 for
    /// `buckets_ds`. INF is excluded from this bucket.
    pub le_65534: u64,
    /// Edges that quantise to exactly `65535` at the target precision.
    /// These collide with a hypothetical `u16` sentinel and would
    /// need to overflow even though they fit numerically. INF
    /// excluded.
    pub eq_65535: u64,
    /// Edges with weight `≤ 16 777 214` (fits in `u24`, no sentinel
    /// collision). INF excluded.
    pub le_u24_max_minus_one: u64,
    /// Edges that are currently `u32::MAX` (the existing INF
    /// sentinel). Carried through unchanged for every codec.
    pub eq_inf: u64,
    /// Edges with weight strictly above `u24` max but below INF. These
    /// require the overflow escape in any u16/u24-based codec.
    pub gt_u24_max_minus_one: u64,
}

// ---------- entry point ----------------------------------------------------

/// Top-level entry point invoked by the `butterfly-bench weight-profile`
/// CLI handler in `main.rs`. Loads the requested region, walks every
/// measurement section, and writes `weight-profile.json` +
/// `weight-profile.md` under `output_dir`.
pub fn run_weight_profile(
    data_dir: &Path,
    output_dir: &Path,
    region: Option<&str>,
) -> Result<()> {
    std::fs::create_dir_all(output_dir)
        .with_context(|| format!("creating output directory {}", output_dir.display()))?;

    println!("═══════════════════════════════════════════════════════════════");
    println!("  WEIGHT DISTRIBUTION PROFILER (#298)");
    println!("═══════════════════════════════════════════════════════════════");
    println!("  Data dir: {}", data_dir.display());
    println!("  Output dir: {}", output_dir.display());
    println!("  Region: {}", region.unwrap_or("(directory tree)"));
    println!("  Seed: 0x{:016X}", WEIGHT_PROFILE_SEED);
    println!();

    // ---- Load ServerState ------------------------------------------------
    println!("[1/?] Loading ServerState from directory tree...");
    let load_start = std::time::Instant::now();
    let state = ServerState::load(data_dir, None)
        .with_context(|| format!("loading ServerState from {}", data_dir.display()))?;
    let load_elapsed = load_start.elapsed();
    println!(
        "  ✓ loaded {} modes in {:.1}s",
        state.mode_names.len(),
        load_elapsed.as_secs_f64()
    );
    for (i, name) in state.mode_names.iter().enumerate() {
        let mode_data = &state.modes[i];
        println!(
            "    - {}: {} CCH ranks, {} UP weights, {} DOWN weights",
            name,
            mode_data.cch_topo.n_nodes,
            mode_data.cch_weights.up.len(),
            mode_data.cch_weights.down.len()
        );
    }
    println!();

    // ---- Section A -------------------------------------------------------
    println!("[2/?] Section A: static distribution per (mode × metric × direction)...");
    let mut static_a: BTreeMap<String, (StaticStats, StaticStats, StaticStats, StaticStats)> =
        BTreeMap::new();
    for (i, mode_name) in state.mode_names.iter().enumerate() {
        let mode_data = &state.modes[i];
        let t_up = compute_static_stats(&mode_data.cch_weights.up);
        let t_dn = compute_static_stats(&mode_data.cch_weights.down);
        let d_up = compute_static_stats(&mode_data.cch_weights_dist.up);
        let d_dn = compute_static_stats(&mode_data.cch_weights_dist.down);
        println!(
            "  - {}: time up p99={} p99.9={} max={} inf={}; \
             dist up p99={} p99.9={} max={} inf={}",
            mode_name,
            t_up.p99,
            t_up.p99_9,
            t_up.max,
            t_up.n_inf,
            d_up.p99,
            d_up.p99_9,
            d_up.max,
            d_up.n_inf,
        );
        static_a.insert(mode_name.clone(), (t_up, t_dn, d_up, d_dn));
    }
    println!();

    // ---- Section C -------------------------------------------------------
    println!(
        "[3/?] Section C: per-block range histograms (block sizes 32, 64, 128)..."
    );
    const BLOCK_SIZES: &[usize] = &[32, 64, 128];
    let mut blocks_c: BTreeMap<String, (BlockStats, BlockStats, BlockStats, BlockStats)> =
        BTreeMap::new();
    for (i, mode_name) in state.mode_names.iter().enumerate() {
        let mode_data = &state.modes[i];
        let t_up = compute_block_stats(&mode_data.cch_weights.up, BLOCK_SIZES);
        let t_dn = compute_block_stats(&mode_data.cch_weights.down, BLOCK_SIZES);
        let d_up = compute_block_stats(&mode_data.cch_weights_dist.up, BLOCK_SIZES);
        let d_dn = compute_block_stats(&mode_data.cch_weights_dist.down, BLOCK_SIZES);
        let summary = |b: &BlockStats| -> String {
            let mut s = Vec::new();
            for sz in BLOCK_SIZES {
                let key = sz.to_string();
                if let Some(bs) = b.by_block_size.get(&key) {
                    s.push(format!(
                        "{}:mean={:.2}/max={}",
                        sz, bs.mean_bits, bs.max_bits
                    ));
                }
            }
            s.join(" ")
        };
        println!(
            "  - {} time up [{}], dist up [{}]",
            mode_name,
            summary(&t_up),
            summary(&d_up),
        );
        blocks_c.insert(mode_name.clone(), (t_up, t_dn, d_up, d_dn));
    }
    println!();

    // ---- Section B -------------------------------------------------------
    //
    // Generate OD pairs once (in WGS84) and reuse them across modes.
    // Snapping happens per mode because the mode mask determines which
    // edges are usable as source/dest.
    println!(
        "[4/?] Section B: hot-query-weighted distribution \
         (100 corpus + 10 000 random OD pairs)..."
    );
    let bbox = compute_bbox(&state);
    println!(
        "  bbox (lon, lat): [{:.4}, {:.4}] x [{:.4}, {:.4}]",
        bbox.min_lon, bbox.min_lat, bbox.max_lon, bbox.max_lat,
    );
    let od_pairs = generate_od_pairs(&bbox);
    println!(
        "  generated {} OD pairs (100 deterministic corpus + 10 000 RNG)",
        od_pairs.len()
    );

    let mut hot_b: BTreeMap<String, (HotStats, HotStats)> = BTreeMap::new();
    for (mode_idx, mode_name) in state.mode_names.iter().enumerate() {
        let mode_data = &state.modes[mode_idx];
        let snap_start = std::time::Instant::now();
        let snapped: Vec<(u32, u32)> = snap_od_pairs(&state, &od_pairs, mode_idx as u8);
        let n_snapped = snapped.len();
        println!(
            "  - {}: snapped {}/{} OD pairs in {:.1}s",
            mode_name,
            n_snapped,
            od_pairs.len(),
            snap_start.elapsed().as_secs_f64()
        );

        // Time metric: instrumented bidirectional search over the
        // standard time flats.
        let t_start = std::time::Instant::now();
        let n_nodes = mode_data.cch_topo.n_nodes as usize;
        let hot_time = run_instrumented_queries(
            n_nodes,
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            &snapped,
        );
        println!(
            "    time: {} queries → {} relaxations, {} unique edges \
             (u16 overflow {:.4}% cs, {:.4}% s; u24 {:.4}% cs, {:.4}% s) in {:.1}s",
            hot_time.n_queries_total,
            hot_time.n_relaxed_total,
            hot_time.n_unique_edges_visited,
            100.0 * hot_time.overflow_rates_cs.u16,
            100.0 * hot_time.overflow_rates_s.u16,
            100.0 * hot_time.overflow_rates_cs.u24,
            100.0 * hot_time.overflow_rates_s.u24,
            t_start.elapsed().as_secs_f64()
        );

        // Distance metric: same shape over the dist flats. Snapping
        // uses the same EBG-id → rank chain because the per-mode rank
        // permutation is shared with the time CCH.
        let d_start = std::time::Instant::now();
        let hot_dist = run_instrumented_queries(
            n_nodes,
            &mode_data.up_adj_flat_dist,
            &mode_data.down_rev_flat_dist,
            &snapped,
        );
        println!(
            "    dist: {} queries → {} relaxations, {} unique edges \
             (u16 overflow {:.4}% cs, {:.4}% s; u24 {:.4}% cs, {:.4}% s) in {:.1}s",
            hot_dist.n_queries_total,
            hot_dist.n_relaxed_total,
            hot_dist.n_unique_edges_visited,
            100.0 * hot_dist.overflow_rates_cs.u16,
            100.0 * hot_dist.overflow_rates_s.u16,
            100.0 * hot_dist.overflow_rates_cs.u24,
            100.0 * hot_dist.overflow_rates_s.u24,
            d_start.elapsed().as_secs_f64()
        );

        hot_b.insert(mode_name.clone(), (hot_time, hot_dist));
    }
    println!();

    // ---- Assemble per-mode report ---------------------------------------
    let mut modes_report: BTreeMap<String, ModeReport> = BTreeMap::new();
    for mode_name in state.mode_names.iter() {
        let (t_up, t_dn, d_up, d_dn) = static_a.remove(mode_name).expect("static A populated");
        let (tb_up, tb_dn, db_up, db_dn) =
            blocks_c.remove(mode_name).expect("blocks C populated");
        let (hot_time, hot_dist) = hot_b.remove(mode_name).expect("hot B populated");
        modes_report.insert(
            mode_name.clone(),
            ModeReport {
                time: MetricReport {
                    statik: DirectionPair { up: t_up, down: t_dn },
                    blocks: DirectionPair { up: tb_up, down: tb_dn },
                    hot: hot_time,
                },
                dist: MetricReport {
                    statik: DirectionPair { up: d_up, down: d_dn },
                    blocks: DirectionPair { up: db_up, down: db_dn },
                    hot: hot_dist,
                },
            },
        );
    }

    // ---- Render JSON (interim — only section A so far) -------------------
    let report = Report {
        version: SCHEMA_VERSION.to_string(),
        region: region.unwrap_or("").to_string(),
        git_sha: git_sha().unwrap_or_else(|| "unknown".to_string()),
        generated_at: chrono::Utc::now().to_rfc3339(),
        seed: format!("0x{:016X}", WEIGHT_PROFILE_SEED),
        modes: modes_report,
    };

    let json_path = output_dir.join("weight-profile.json");
    let json_str = serde_json::to_string_pretty(&report)
        .context("serialising weight-profile report to JSON")?;
    std::fs::write(&json_path, json_str)
        .with_context(|| format!("writing {}", json_path.display()))?;
    println!("  ✓ wrote {}", json_path.display());
    println!();

    // Subsequent commits land:
    //   - Section C: per-block range histograms (sizes 32/64/128).
    //   - Section B: hot-query-weighted overflow rates.
    //   - Section D: rounding sensitivity.
    //   - Section E: triangle-relaxation tie rate.
    //   - Final pass: render `weight-profile.md` + repo-tracked outputs.

    Ok(())
}

// ---------- Section A helpers ----------------------------------------------

/// Walk a `&[u32]` weight array and produce a full `StaticStats`. INF
/// (`u32::MAX`) entries are excluded from min/max/percentiles and from
/// every `BucketCounts` bucket *except* `eq_inf`. The `distinct_count`
/// is over the non-INF set; INF is counted via `n_inf`.
///
/// The percentile shape uses the standard linear-interpolation
/// definition on the *sorted non-INF set*: `p_k = sorted[floor(k *
/// (n-1) / 1)]` where index is computed in fixed-point so the result
/// is bit-identical across runs.
fn compute_static_stats(weights: &[u32]) -> StaticStats {
    let n_total = weights.len() as u64;
    let mut values: Vec<u32> = Vec::with_capacity(weights.len());
    let mut n_inf: u64 = 0;
    for &w in weights {
        if w == u32::MAX {
            n_inf += 1;
        } else {
            values.push(w);
        }
    }
    if values.is_empty() {
        // Edge case: every weight is INF (shouldn't happen in a built
        // CCH, but be defensive). Return all-zero stats plus the inf
        // bucket so downstream sees a consistent shape.
        let mut log_histogram = empty_log_histogram();
        log_histogram.insert("inf".to_string(), n_inf);
        return StaticStats {
            n_edges: n_total,
            n_inf,
            buckets_cs: BucketCounts {
                eq_inf: n_inf,
                ..Default::default()
            },
            buckets_s: BucketCounts {
                eq_inf: n_inf,
                ..Default::default()
            },
            buckets_ds: BucketCounts {
                eq_inf: n_inf,
                ..Default::default()
            },
            log_histogram,
            ..Default::default()
        };
    }
    values.sort_unstable();
    let n = values.len();

    // Percentile lookup. `percentile_index` clamps to [0, n-1] and
    // uses a deterministic integer index computed in fixed-point so
    // the answer never drifts with the platform's f64 rounding.
    let p50 = values[percentile_index(n, 50_000)] as u64;
    let p90 = values[percentile_index(n, 90_000)] as u64;
    let p99 = values[percentile_index(n, 99_000)] as u64;
    let p99_9 = values[percentile_index(n, 99_900)] as u64;
    let p99_99 = values[percentile_index(n, 99_990)] as u64;
    let min = values[0] as u64;
    let max = *values.last().unwrap() as u64;

    // Distinct-value count — the sorted vector lets us count in one
    // linear pass without allocating a HashSet.
    let mut distinct: u64 = 1;
    let mut last = values[0];
    for &v in values.iter().skip(1) {
        if v != last {
            distinct += 1;
            last = v;
        }
    }

    // Bucket counts at every target precision.
    let buckets_cs = compute_buckets(weights, 1);
    let buckets_s = compute_buckets(weights, 100);
    let buckets_ds = compute_buckets(weights, 10);

    // Log-spaced histogram (bucket edges 1, 10, 100, ..., 1e9).
    let mut log_histogram = empty_log_histogram();
    for &w in weights {
        let key = log_bucket_label(w);
        *log_histogram.entry(key).or_insert(0) += 1;
    }

    StaticStats {
        n_edges: n_total,
        n_inf,
        min,
        max,
        p50,
        p90,
        p99,
        p99_9,
        p99_99,
        distinct_count: distinct,
        buckets_cs,
        buckets_s,
        buckets_ds,
        log_histogram,
    }
}

/// Compute bucket counts for one precision target. `divisor` is the
/// factor applied per edge before bucketing (1 = raw cs, 10 = ds, 100
/// = s). INF is preserved as INF — never divided — and counted into
/// `eq_inf`. The division uses `round-half-to-even` ("banker's
/// rounding") so the result is symmetric across runs and matches what
/// step 8 will emit if we ship the unit change in #297.
fn compute_buckets(weights: &[u32], divisor: u32) -> BucketCounts {
    let mut b = BucketCounts::default();
    for &w in weights {
        if w == u32::MAX {
            b.eq_inf += 1;
            continue;
        }
        let q = if divisor == 1 {
            w as u64
        } else {
            round_half_even_div(w as u64, divisor as u64)
        };
        if q <= 65_534 {
            b.le_65534 += 1;
        }
        if q == 65_535 {
            b.eq_65535 += 1;
        }
        if q <= 16_777_214 {
            b.le_u24_max_minus_one += 1;
        } else {
            b.gt_u24_max_minus_one += 1;
        }
    }
    b
}

/// Round `n / d` to the nearest integer, breaking ties to even.
/// Matches IEEE 754's default rounding mode without going through
/// f64, so results are bit-deterministic on every architecture.
#[inline]
fn round_half_even_div(n: u64, d: u64) -> u64 {
    if d == 0 {
        return 0;
    }
    let q = n / d;
    let r = n - q * d;
    let twice = 2 * r;
    if twice < d {
        q
    } else if twice > d {
        q + 1
    } else {
        // Exact half — break to even.
        if q.is_multiple_of(2) { q } else { q + 1 }
    }
}

/// Map a weight value to its log-spaced histogram bucket label. The
/// buckets are `(0, 1]`, `(1, 10]`, ..., `(1e8, 1e9]`, `(1e9, INF)`,
/// `INF`. INF is `u32::MAX`. Labels are kept as strings so the JSON
/// preserves the human-readable form.
fn log_bucket_label(w: u32) -> String {
    if w == u32::MAX {
        return "inf".to_string();
    }
    let edges = [1u64, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000];
    let w64 = w as u64;
    for (i, &edge) in edges.iter().enumerate() {
        if w64 <= edge {
            let prev = if i == 0 { 0 } else { edges[i - 1] };
            return format!("({},{}]", prev, edge);
        }
    }
    format!("({},inf)", edges[edges.len() - 1])
}

/// Pre-populate the log histogram with every bucket label so the JSON
/// always has the same key set across runs (even if a bucket is
/// empty).
fn empty_log_histogram() -> BTreeMap<String, u64> {
    let mut m = BTreeMap::new();
    let edges = [1u64, 10, 100, 1_000, 10_000, 100_000, 1_000_000, 10_000_000, 100_000_000, 1_000_000_000];
    let mut prev = 0u64;
    for &edge in &edges {
        m.insert(format!("({},{}]", prev, edge), 0);
        prev = edge;
    }
    m.insert(format!("({},inf)", edges[edges.len() - 1]), 0);
    m.insert("inf".to_string(), 0);
    m
}

/// Pick the index for a percentile `p` (expressed as `p * 1000`, so
/// 99.9% is `99_900`). Uses the nearest-rank definition with
/// integer-only arithmetic for determinism: index = floor((n - 1) *
/// p_mille / 100_000).
#[inline]
fn percentile_index(n: usize, p_mille: u64) -> usize {
    if n == 0 {
        return 0;
    }
    let idx = ((n as u64 - 1).saturating_mul(p_mille)) / 100_000;
    idx as usize
}

// ---------- Section B helpers ----------------------------------------------

/// Region bbox in WGS84 degrees. Pulled from the snap index at boot;
/// the snap index records the bbox in i32-e7 fixed point so we
/// convert to f64 once here.
#[derive(Debug, Clone, Copy)]
struct Bbox {
    min_lon: f64,
    min_lat: f64,
    max_lon: f64,
    max_lat: f64,
}

fn compute_bbox(state: &ServerState) -> Bbox {
    let p = &state.snap_index.points;
    Bbox {
        min_lon: p.bbox_min_lon as f64 / 1e7,
        min_lat: p.bbox_min_lat as f64 / 1e7,
        max_lon: p.bbox_max_lon as f64 / 1e7,
        max_lat: p.bbox_max_lat as f64 / 1e7,
    }
}

/// A WGS84 OD pair.
#[derive(Debug, Clone, Copy)]
struct OdPair {
    origin_lon: f64,
    origin_lat: f64,
    dest_lon: f64,
    dest_lat: f64,
}

/// 100 deterministic OD pairs from the bbox using a fixed sub-seed,
/// plus 10 000 RNG pairs using `WEIGHT_PROFILE_SEED`. The corpus
/// sub-seed (0xC0_2F_15) is distinct from the main seed so the two
/// sets of OD pairs do not overlap by coincidence.
fn generate_od_pairs(bbox: &Bbox) -> Vec<OdPair> {
    let mut out = Vec::with_capacity(10_100);
    // Corpus (100 pairs).
    let mut corpus_rng = StdRng::seed_from_u64(0xC0_2F_15);
    for _ in 0..100 {
        out.push(rng_od_pair(&mut corpus_rng, bbox));
    }
    // RNG (10 000 pairs).
    let mut main_rng = StdRng::seed_from_u64(WEIGHT_PROFILE_SEED);
    for _ in 0..10_000 {
        out.push(rng_od_pair(&mut main_rng, bbox));
    }
    out
}

#[inline]
fn rng_od_pair(rng: &mut StdRng, bbox: &Bbox) -> OdPair {
    OdPair {
        origin_lon: rng.random_range(bbox.min_lon..bbox.max_lon),
        origin_lat: rng.random_range(bbox.min_lat..bbox.max_lat),
        dest_lon: rng.random_range(bbox.min_lon..bbox.max_lon),
        dest_lat: rng.random_range(bbox.min_lat..bbox.max_lat),
    }
}

/// Snap every OD pair to a (source_rank, target_rank) tuple via the
/// per-mode snap index + `rank_for_original` chain. Pairs that fail
/// to snap on either end (no road within `MAX_SNAP_DISTANCE_M` /
/// inaccessible to this mode) are silently dropped, mirroring how
/// `/route` handles unreachable requests. The drop is counted and
/// reported via the per-mode log line.
fn snap_od_pairs(state: &ServerState, pairs: &[OdPair], mode_idx: u8) -> Vec<(u32, u32)> {
    let mode_data = &state.modes[mode_idx as usize];
    pairs
        .iter()
        .filter_map(|p| {
            let src_orig = state.snap_index.snap(p.origin_lon, p.origin_lat, mode_idx)?;
            let dst_orig = state.snap_index.snap(p.dest_lon, p.dest_lat, mode_idx)?;
            let src_rank = mode_data.rank_for_original(src_orig)?;
            let dst_rank = mode_data.rank_for_original(dst_orig)?;
            Some((src_rank, dst_rank))
        })
        .collect()
}

/// Per-thread relaxation accumulator. One `EdgeBin` per thread keeps
/// the histogram local so the parallel sum has no atomic contention
/// on the hot loop. Per-thread results merge at the end of the
/// parallel pass.
#[derive(Default, Clone)]
struct EdgeBin {
    /// Count of relaxations bucketed by `w / 100_000`-and-stop log
    /// bucket index. Index 0 = "(0, 1]", up to index 10 = "(1e9, inf)".
    /// Used to populate the `log_histogram` field after merge.
    log: [u64; 12],
    /// Per-threshold relaxation counts (independent of unit).
    /// Indices: 0=u8 (cs), 1=u12 (cs), 2=u14 (cs), 3=u16 (cs), 4=u24 (cs),
    /// 5=u8 (s), 6=u12 (s), 7=u14 (s), 8=u16 (s), 9=u24 (s),
    /// 10=u8 (ds), 11=u12 (ds), 12=u14 (ds), 13=u16 (ds), 14=u24 (ds).
    over: [u64; 15],
    /// Total relaxations seen.
    n_relaxed: u64,
    /// Bitset of edge slots visited (sized to flat.weights.len()).
    /// Used to count distinct edges visited across all queries.
    visited: Vec<u64>,
    /// Per-query reached count.
    n_reached: u64,
    n_queries: u64,
}

impl EdgeBin {
    fn new(n_edges: usize) -> Self {
        Self {
            log: [0; 12],
            over: [0; 15],
            n_relaxed: 0,
            visited: vec![0u64; n_edges.div_ceil(64)],
            n_reached: 0,
            n_queries: 0,
        }
    }

    #[inline]
    fn record(&mut self, edge_idx: u32, weight: u32) {
        self.n_relaxed += 1;
        // Log bucket.
        let bucket = log_bucket_index(weight);
        self.log[bucket] += 1;
        // Threshold counts at cs.
        let w_cs = weight as u64;
        if w_cs > 255 {
            self.over[0] += 1;
        }
        if w_cs > 4095 {
            self.over[1] += 1;
        }
        if w_cs > 16_383 {
            self.over[2] += 1;
        }
        if w_cs > 65_534 {
            self.over[3] += 1;
        }
        if w_cs > 16_777_214 {
            self.over[4] += 1;
        }
        // Threshold counts at s (÷ 100, round-half-to-even).
        let w_s = round_half_even_div(w_cs, 100);
        if w_s > 255 {
            self.over[5] += 1;
        }
        if w_s > 4095 {
            self.over[6] += 1;
        }
        if w_s > 16_383 {
            self.over[7] += 1;
        }
        if w_s > 65_534 {
            self.over[8] += 1;
        }
        if w_s > 16_777_214 {
            self.over[9] += 1;
        }
        // Threshold counts at ds (÷ 10, round-half-to-even).
        let w_ds = round_half_even_div(w_cs, 10);
        if w_ds > 255 {
            self.over[10] += 1;
        }
        if w_ds > 4095 {
            self.over[11] += 1;
        }
        if w_ds > 16_383 {
            self.over[12] += 1;
        }
        if w_ds > 65_534 {
            self.over[13] += 1;
        }
        if w_ds > 16_777_214 {
            self.over[14] += 1;
        }
        // Visited bitset.
        let word = (edge_idx as usize) / 64;
        let bit = (edge_idx as usize) % 64;
        if word < self.visited.len() {
            self.visited[word] |= 1u64 << bit;
        }
    }

    fn merge_into(&mut self, other: &EdgeBin) {
        for i in 0..self.log.len() {
            self.log[i] += other.log[i];
        }
        for i in 0..self.over.len() {
            self.over[i] += other.over[i];
        }
        self.n_relaxed += other.n_relaxed;
        self.n_reached += other.n_reached;
        self.n_queries += other.n_queries;
        for (a, b) in self.visited.iter_mut().zip(other.visited.iter()) {
            *a |= *b;
        }
    }
}

#[inline]
fn log_bucket_index(w: u32) -> usize {
    let edges = [
        1u64,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
    ];
    if w == u32::MAX {
        return 11;
    }
    let w64 = w as u64;
    for (i, &edge) in edges.iter().enumerate() {
        if w64 <= edge {
            return i;
        }
    }
    10
}

#[inline]
fn log_bucket_label_by_index(i: usize) -> String {
    let edges = [
        1u64,
        10,
        100,
        1_000,
        10_000,
        100_000,
        1_000_000,
        10_000_000,
        100_000_000,
        1_000_000_000,
    ];
    if i == 11 {
        return "inf".to_string();
    }
    if i == 10 {
        return format!("({},inf)", edges[edges.len() - 1]);
    }
    let prev = if i == 0 { 0 } else { edges[i - 1] };
    format!("({},{}]", prev, edges[i])
}

/// Run bidirectional CCH P2P over every snapped OD pair, with each
/// relaxation recorded into a per-thread `EdgeBin`. Parallelised with
/// rayon over the OD list.
///
/// The bidirectional algorithm is a faithful port of the
/// `CchQuery::distance` shape in `server/query.rs` — same early-
/// termination condition, same stale-pop check, same priority
/// queue. The only difference is the relaxation step records the
/// (edge_idx, weight) before applying it. Recording happens
/// unconditionally (even for non-improving relaxations) because the
/// codec cares about every weight load — the relaxation cost is
/// paid regardless of whether the result improves the distance.
fn run_instrumented_queries(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    snapped: &[(u32, u32)],
) -> HotStats {
    let n_up_edges = up_adj_flat.weights.len();
    let n_down_edges = down_rev_flat.weights.len();

    // Each thread accumulates into one `EdgeBin` for UP edges and
    // one for DOWN — we union them at the end. The visited bitset
    // separates UP and DOWN because edge_idx is independent across
    // them (both index into their respective `weights` arrays).
    let (up_bin, dn_bin) = snapped
        .par_iter()
        .fold(
            || (EdgeBin::new(n_up_edges), EdgeBin::new(n_down_edges)),
            |(mut up, mut dn), &(src, dst)| {
                let reached = run_one_query(
                    n_nodes,
                    up_adj_flat,
                    down_rev_flat,
                    src,
                    dst,
                    &mut up,
                    &mut dn,
                );
                up.n_queries += 1;
                dn.n_queries += 1;
                if reached {
                    up.n_reached += 1;
                    dn.n_reached += 1;
                }
                (up, dn)
            },
        )
        .reduce(
            || (EdgeBin::new(n_up_edges), EdgeBin::new(n_down_edges)),
            |mut a, b| {
                a.0.merge_into(&b.0);
                a.1.merge_into(&b.1);
                a
            },
        );

    // Both bins observe the same per-query event sequence, so
    // `n_queries` / `n_reached` are duplicated. Take from `up_bin`
    // (single source of truth) and add UP + DOWN relaxation counts.
    let n_queries_total = up_bin.n_queries;
    let n_queries_reached = up_bin.n_reached;
    let n_queries_unreachable = n_queries_total - n_queries_reached;
    let n_relaxed_total = up_bin.n_relaxed + dn_bin.n_relaxed;
    let n_unique_edges_visited =
        count_set_bits(&up_bin.visited) + count_set_bits(&dn_bin.visited);

    let mut log_histogram = BTreeMap::new();
    for i in 0..12 {
        let label = log_bucket_label_by_index(i);
        let count = up_bin.log[i] + dn_bin.log[i];
        log_histogram.insert(label, count);
    }

    let denom = n_relaxed_total.max(1) as f64;
    let to_rate = |idx: usize| -> u64 { up_bin.over[idx] + dn_bin.over[idx] };
    let overflow_rates_cs = OverflowRates {
        u8: to_rate(0) as f64 / denom,
        u12: to_rate(1) as f64 / denom,
        u14: to_rate(2) as f64 / denom,
        u16: to_rate(3) as f64 / denom,
        u24: to_rate(4) as f64 / denom,
        n_over_u16: to_rate(3),
        n_over_u24: to_rate(4),
    };
    let overflow_rates_s = OverflowRates {
        u8: to_rate(5) as f64 / denom,
        u12: to_rate(6) as f64 / denom,
        u14: to_rate(7) as f64 / denom,
        u16: to_rate(8) as f64 / denom,
        u24: to_rate(9) as f64 / denom,
        n_over_u16: to_rate(8),
        n_over_u24: to_rate(9),
    };
    let overflow_rates_ds = OverflowRates {
        u8: to_rate(10) as f64 / denom,
        u12: to_rate(11) as f64 / denom,
        u14: to_rate(12) as f64 / denom,
        u16: to_rate(13) as f64 / denom,
        u24: to_rate(14) as f64 / denom,
        n_over_u16: to_rate(13),
        n_over_u24: to_rate(14),
    };

    HotStats {
        n_queries_total,
        n_queries_reached,
        n_queries_unreachable,
        n_relaxed_total,
        n_unique_edges_visited,
        overflow_rates_cs,
        overflow_rates_s,
        overflow_rates_ds,
        log_histogram,
    }
}

/// Count `1` bits in a packed `&[u64]` bitset. Each word's
/// `count_ones()` is a single CPU instruction on x86_64
/// (`popcntq`), so this is cheap even for the 5M-bit visited
/// bitset Belgium ends up producing.
fn count_set_bits(bits: &[u64]) -> u64 {
    bits.iter().map(|w| w.count_ones() as u64).sum()
}

/// One instrumented bidirectional CCH query. Mirrors
/// `CchQuery::distance` (see `server/query.rs`) but
///   (1) the priority queue + distance/parent arrays live on the
///       stack frame instead of in a thread-local, so the rayon
///       fold closure stays Send + Sync,
///   (2) each relaxation records the edge slot + weight into the
///       caller's `EdgeBin`, regardless of whether the relaxation
///       improves the best distance — every weight load is a real
///       memory access the codec must serve.
///
/// Returns true iff the search found a finite path.
fn run_one_query(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    source: u32,
    target: u32,
    up_bin: &mut EdgeBin,
    dn_bin: &mut EdgeBin,
) -> bool {
    if source == target {
        return true;
    }

    let mut dist_fwd = vec![u32::MAX; n_nodes];
    let mut dist_bwd = vec![u32::MAX; n_nodes];
    let mut pq_fwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
    let mut pq_bwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();

    dist_fwd[source as usize] = 0;
    dist_bwd[target as usize] = 0;
    pq_fwd.push(source, Reverse(0));
    pq_bwd.push(target, Reverse(0));

    let mut best_dist = u32::MAX;

    while !pq_fwd.is_empty() || !pq_bwd.is_empty() {
        let fwd_min = pq_fwd.peek().map(|(_, &Reverse(d))| d).unwrap_or(u32::MAX);
        let bwd_min = pq_bwd.peek().map(|(_, &Reverse(d))| d).unwrap_or(u32::MAX);
        if fwd_min >= best_dist && bwd_min >= best_dist {
            break;
        }

        // Forward UP step.
        if let Some((u, Reverse(d))) = pq_fwd.pop() {
            if d <= dist_fwd[u as usize] {
                let bwd_d = dist_bwd[u as usize];
                if bwd_d != u32::MAX {
                    let total = d.saturating_add(bwd_d);
                    if total < best_dist {
                        best_dist = total;
                    }
                }
                // Relax UP edges from `u`.
                let start = up_adj_flat.offsets[u as usize] as usize;
                let end = up_adj_flat.offsets[u as usize + 1] as usize;
                for slot in start..end {
                    let v = up_adj_flat.targets[slot];
                    let w = up_adj_flat.weights[slot];
                    // Record relaxation in the UP histogram.
                    up_bin.record(slot as u32, w);
                    let new_dist = d.saturating_add(w);
                    if new_dist < dist_fwd[v as usize] {
                        dist_fwd[v as usize] = new_dist;
                        pq_fwd.push(v, Reverse(new_dist));
                        let bwd_v = dist_bwd[v as usize];
                        if bwd_v != u32::MAX {
                            let total = new_dist.saturating_add(bwd_v);
                            if total < best_dist {
                                best_dist = total;
                            }
                        }
                    }
                }
            }
        }

        // Backward reversed-DOWN step.
        if let Some((u, Reverse(d))) = pq_bwd.pop() {
            if d <= dist_bwd[u as usize] {
                let fwd_d = dist_fwd[u as usize];
                if fwd_d != u32::MAX {
                    let total = d.saturating_add(fwd_d);
                    if total < best_dist {
                        best_dist = total;
                    }
                }
                let start = down_rev_flat.offsets[u as usize] as usize;
                let end = down_rev_flat.offsets[u as usize + 1] as usize;
                for slot in start..end {
                    let x = down_rev_flat.sources[slot];
                    let w = down_rev_flat.weights[slot];
                    // Record relaxation in the DOWN histogram.
                    dn_bin.record(slot as u32, w);
                    let new_dist = d.saturating_add(w);
                    if new_dist < dist_bwd[x as usize] {
                        dist_bwd[x as usize] = new_dist;
                        pq_bwd.push(x, Reverse(new_dist));
                        let fwd_x = dist_fwd[x as usize];
                        if fwd_x != u32::MAX {
                            let total = new_dist.saturating_add(fwd_x);
                            if total < best_dist {
                                best_dist = total;
                            }
                        }
                    }
                }
            }
        }
    }

    best_dist != u32::MAX
}

// ---------- Section C helpers ----------------------------------------------

/// Walk `weights` in contiguous blocks of each size in `block_sizes`
/// and produce one `BlockSizeStats` per size. The last partial block
/// is **kept** (no truncation) so the bits-needed histogram reflects
/// the real distribution that any codec would see on the actual
/// array. For determinism, the distinct-value count uses an in-place
/// sort + linear pass (no HashSet).
///
/// INF (`u32::MAX`) entries are excluded from the `min/max/distinct`
/// computation but counted into the per-block sentinel: a block that
/// is entirely INF contributes to `n_inf_blocks` and is omitted from
/// `bits_needed_histogram`. A block that is partially INF computes
/// `bits_needed` over the non-INF subset only — this models a codec
/// that stores INF separately via the existing `u32::MAX` sentinel
/// + an out-of-block escape (the proposal in #279).
fn compute_block_stats(weights: &[u32], block_sizes: &[usize]) -> BlockStats {
    let mut by_block_size = BTreeMap::new();
    for &block_size in block_sizes {
        if block_size == 0 {
            continue;
        }
        let stats = compute_block_size_stats(weights, block_size);
        by_block_size.insert(block_size.to_string(), stats);
    }
    BlockStats { by_block_size }
}

fn compute_block_size_stats(weights: &[u32], block_size: usize) -> BlockSizeStats {
    let mut bits_hist = vec![0u64; 33];
    let mut distinct_hist = vec![0u64; block_size + 1];
    let mut n_inf_blocks: u64 = 0;
    let mut n_blocks: u64 = 0;
    let mut total_bits: u64 = 0;
    let mut max_bits: u32 = 0;
    let mut scratch: Vec<u32> = Vec::with_capacity(block_size);

    let mut start = 0;
    while start < weights.len() {
        let end = (start + block_size).min(weights.len());
        let block = &weights[start..end];
        n_blocks += 1;
        start = end;

        // Collect non-INF values into the scratch buffer.
        scratch.clear();
        for &w in block {
            if w != u32::MAX {
                scratch.push(w);
            }
        }
        if scratch.is_empty() {
            n_inf_blocks += 1;
            distinct_hist[0] += 1;
            continue;
        }

        // Distinct-value count via in-place sort + linear scan.
        scratch.sort_unstable();
        let mut distinct: usize = 1;
        let mut last = scratch[0];
        for &v in scratch.iter().skip(1) {
            if v != last {
                distinct += 1;
                last = v;
            }
        }
        let distinct_capped = distinct.min(block_size);
        distinct_hist[distinct_capped] += 1;

        // bits_needed = ceil(log2(range + 1)) for the non-INF subset.
        let min_v = scratch[0];
        let max_v = *scratch.last().unwrap();
        let range = (max_v - min_v) as u64;
        let bits = bits_needed(range);
        bits_hist[bits as usize] += 1;
        total_bits += bits as u64;
        if bits > max_bits {
            max_bits = bits;
        }
    }

    let mean_bits = if n_blocks > n_inf_blocks {
        total_bits as f64 / (n_blocks - n_inf_blocks) as f64
    } else {
        0.0
    };

    BlockSizeStats {
        block_size: block_size as u32,
        n_blocks,
        bits_needed_histogram: bits_hist,
        n_inf_blocks,
        distinct_per_block_histogram: distinct_hist,
        mean_bits,
        max_bits,
    }
}

/// `bits_needed(range)` = ceil(log2(range + 1)).
///
/// Special cases:
///   * range == 0      → 0 bits (a constant block can be encoded as the
///                       min value alone, no per-edge bits needed).
///   * range == 1      → 1 bit.
///   * range == u32::MAX → 32 bits.
///
/// `range` is passed as u64 because `(max - min) + 1` overflows u32
/// when `min == 0` and `max == u32::MAX - 1`. We never actually hit
/// that boundary in practice (INF is filtered out before we get here)
/// but the upcast is free and keeps the implementation honest.
#[inline]
fn bits_needed(range: u64) -> u32 {
    if range == 0 {
        return 0;
    }
    // ceil(log2(range + 1)) == 64 - clz(range) for range > 0.
    64 - range.leading_zeros()
}

/// Return the current git SHA (short form) when invoked from a git
/// working tree, or `None` if `git` is unavailable. Used for
/// provenance in the JSON report. The lookup is best-effort —
/// failures are silently swallowed so the profiler still runs on a
/// detached checkout / CI snapshot.
fn git_sha() -> Option<String> {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "--short", "HEAD"])
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    let sha = String::from_utf8(output.stdout).ok()?.trim().to_string();
    if sha.is_empty() { None } else { Some(sha) }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_half_even_div_breaks_ties_to_even() {
        // Even quotient: keep.
        assert_eq!(round_half_even_div(5, 2), 2);
        // Odd quotient: round up.
        assert_eq!(round_half_even_div(7, 2), 4);
        // No tie: standard nearest.
        assert_eq!(round_half_even_div(6, 4), 2); // 1.5 -> 2 (even)
        assert_eq!(round_half_even_div(10, 4), 2); // 2.5 -> 2 (even)
        assert_eq!(round_half_even_div(14, 4), 4); // 3.5 -> 4 (even)
    }

    #[test]
    fn buckets_cs_count_each_threshold() {
        let weights = vec![0u32, 100, 65_534, 65_535, 65_536, 16_777_214, 16_777_215, u32::MAX];
        let b = compute_buckets(&weights, 1);
        // ≤ 65_534: 0, 100, 65_534 → 3
        assert_eq!(b.le_65534, 3);
        // == 65_535: 1
        assert_eq!(b.eq_65535, 1);
        // ≤ 16_777_214: 0, 100, 65_534, 65_535, 65_536, 16_777_214 → 6
        assert_eq!(b.le_u24_max_minus_one, 6);
        // > 16_777_214 (excluding INF): 16_777_215 → 1
        assert_eq!(b.gt_u24_max_minus_one, 1);
        // INF: 1
        assert_eq!(b.eq_inf, 1);
    }

    #[test]
    fn buckets_s_divide_by_100() {
        // 100 cs == 1 s, 50 cs == 0.5 s (-> 0 with round-half-to-even),
        // 150 cs == 1.5 s (-> 2 with round-half-to-even).
        let weights = vec![100u32, 50, 150];
        let b = compute_buckets(&weights, 100);
        // All quantise to 0, 0, 2 — all ≤ 65_534.
        assert_eq!(b.le_65534, 3);
        assert_eq!(b.eq_65535, 0);
    }

    #[test]
    fn percentile_index_is_deterministic() {
        // n = 10: p50 -> index 4 (sorted), p99 -> index 8.
        assert_eq!(percentile_index(10, 50_000), 4);
        assert_eq!(percentile_index(10, 99_000), 8);
        assert_eq!(percentile_index(10, 99_900), 8);
        // Edge: n = 1 → always index 0.
        assert_eq!(percentile_index(1, 50_000), 0);
        assert_eq!(percentile_index(1, 99_990), 0);
    }

    #[test]
    fn static_stats_picks_min_max_distinct() {
        let weights = vec![5u32, 1, 3, 1, 9, u32::MAX, 5];
        let s = compute_static_stats(&weights);
        assert_eq!(s.n_edges, 7);
        assert_eq!(s.n_inf, 1);
        assert_eq!(s.min, 1);
        assert_eq!(s.max, 9);
        // distinct non-INF set: {1, 3, 5, 9} → 4
        assert_eq!(s.distinct_count, 4);
    }

    #[test]
    fn bits_needed_edge_cases() {
        assert_eq!(bits_needed(0), 0);
        assert_eq!(bits_needed(1), 1);
        assert_eq!(bits_needed(2), 2);
        assert_eq!(bits_needed(3), 2);
        assert_eq!(bits_needed(4), 3);
        assert_eq!(bits_needed(255), 8);
        assert_eq!(bits_needed(256), 9);
        // u32::MAX as range: full 32 bits.
        assert_eq!(bits_needed(u32::MAX as u64), 32);
    }

    #[test]
    fn block_stats_constant_block_is_0_bits() {
        // Single block of 4 identical values: range = 0 → 0 bits, 1 distinct.
        let weights = vec![100u32, 100, 100, 100];
        let s = compute_block_size_stats(&weights, 4);
        assert_eq!(s.n_blocks, 1);
        assert_eq!(s.n_inf_blocks, 0);
        assert_eq!(s.bits_needed_histogram[0], 1);
        assert_eq!(s.distinct_per_block_histogram[1], 1);
        assert_eq!(s.max_bits, 0);
        assert_eq!(s.mean_bits, 0.0);
    }

    #[test]
    fn block_stats_inf_only_block_counted_separately() {
        // One INF-only block → counted in n_inf_blocks, not in
        // bits_needed_histogram.
        let weights = vec![u32::MAX; 4];
        let s = compute_block_size_stats(&weights, 4);
        assert_eq!(s.n_blocks, 1);
        assert_eq!(s.n_inf_blocks, 1);
        assert_eq!(s.bits_needed_histogram.iter().sum::<u64>(), 0);
        assert_eq!(s.distinct_per_block_histogram[0], 1);
    }

    #[test]
    fn block_stats_range_picks_correct_bits() {
        // Block 1: min=0, max=255 → range=255, bits=8.
        // Block 2: min=100, max=100 → range=0, bits=0.
        // Block 3 (partial, 2 values): min=0, max=1 → range=1, bits=1.
        let weights = vec![
            0u32, 100, 50, 255, // block 1 (block_size 4)
            100, 100, 100, 100, // block 2
            0, 1, // block 3 (partial)
        ];
        let s = compute_block_size_stats(&weights, 4);
        assert_eq!(s.n_blocks, 3);
        assert_eq!(s.n_inf_blocks, 0);
        assert_eq!(s.bits_needed_histogram[0], 1); // block 2
        assert_eq!(s.bits_needed_histogram[1], 1); // block 3
        assert_eq!(s.bits_needed_histogram[8], 1); // block 1
        assert_eq!(s.max_bits, 8);
    }

    #[test]
    fn block_stats_partial_inf_uses_non_inf_subset() {
        // Block with 2 INF and 2 real values: range computed over the
        // 2 real values only.
        let weights = vec![u32::MAX, 100, u32::MAX, 200];
        let s = compute_block_size_stats(&weights, 4);
        assert_eq!(s.n_blocks, 1);
        assert_eq!(s.n_inf_blocks, 0);
        // range = 200 - 100 = 100, ceil(log2(101)) = 7.
        assert_eq!(s.bits_needed_histogram[7], 1);
    }

    #[test]
    fn log_histogram_includes_every_bucket() {
        let weights = vec![0u32, 1, 5, 10, 50, 100, 1_000, 1_000_001, u32::MAX];
        let s = compute_static_stats(&weights);
        // All buckets are present (most are 0).
        let h = &s.log_histogram;
        assert!(h.contains_key("(0,1]"));
        assert!(h.contains_key("(1,10]"));
        assert!(h.contains_key("inf"));
        // INF count matches.
        assert_eq!(h["inf"], 1);
    }
}
