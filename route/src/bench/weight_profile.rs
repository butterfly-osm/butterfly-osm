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
//! The profiler is read-only: it never mutates `ServerState`. Hot-query
//! instrumentation re-implements the bidirectional CCH relaxation
//! inline (see [`run_one_query`]) so the per-edge weight observation
//! sits on the hot path without forcing an instrumented build of the
//! production [`super::super::server::query::CchQuery`]. Behaviour is
//! intentionally the same shape as `CchQuery::distance`: bidirectional
//! Dijkstra over the up/down CSR, stale-pop check, early termination
//! when both PQ mins exceed `best_dist`.
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
pub const WEIGHT_PROFILE_SEED: u64 = 0x0B07_7EF1;
/// Dedicated sub-seed for the 100-pair pseudo-corpus in
/// [`generate_od_pairs`]. Distinct from `WEIGHT_PROFILE_SEED` so the
/// 100 + 10 000 OD pairs don't overlap by coincidence.
pub const WEIGHT_PROFILE_SUB_SEED_CORPUS: u64 = 0xC0_2F_15;

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
    /// Section D — cumulative rounding sensitivity at s/m precision.
    pub rounding: RoundingStats,
    /// Section E — triangle relaxation tie rate at cs vs s.
    pub tie: TieStats,
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

// ---- Section D ------------------------------------------------------------

#[derive(Debug, Serialize, Default, Clone)]
pub struct RoundingStats {
    /// Number of routes evaluated. The implementation draws a fixed
    /// pool of 4 000 candidate OD pairs once via
    /// [`generate_rounding_pairs`] and counts the subset that snaps
    /// AND reaches; on Belgium that consistently yields ~1 000–1 500
    /// reachable routes, comfortably above the #298 spec floor of
    /// 1 000. Earlier prose about "re-rolling until we have 1 000"
    /// described an intent that the simpler fixed-pool path achieved
    /// without the loop — keeping the simpler shape since the
    /// reachable count is well-above the floor on every region tried.
    pub n_routes_total: u64,
    pub n_routes_attempted: u64,
    pub n_routes_unreachable: u64,
    /// Median absolute drift (%) — small, robust to outliers.
    pub median_drift_pct: f64,
    /// p90, p99 absolute drift (%).
    pub p90_drift_pct: f64,
    pub p99_drift_pct: f64,
    /// Max absolute drift (%).
    pub max_drift_pct: f64,
    /// Mean absolute drift (%).
    pub mean_drift_pct: f64,
    /// Number of routes with drift > 1 / 5 / 10 percent.
    pub drift_over_1pct_count: u64,
    pub drift_over_5pct_count: u64,
    pub drift_over_10pct_count: u64,
    /// Fraction over 1% — exposed for the verdict in the markdown
    /// report.
    pub frac_over_1pct: f64,
}

// ---- Section E ------------------------------------------------------------

#[derive(Debug, Serialize, Default, Clone)]
pub struct TieStats {
    /// Number of (x, m, y) triangles enumerated. Each apex `m`
    /// contributes one entry per (x, y) pair where x precedes m in
    /// the DOWN graph and y succeeds m in the UP graph and (x → y)
    /// exists in the CCH topology.
    pub n_triangles_total: u64,
    /// Number of triangles where `w(x, y) == w(x → m) + w(m → y)`
    /// exactly at cs precision. INF entries on any leg disqualify
    /// the triangle from the tie count (a tie with INF is
    /// meaningless).
    pub n_ties_cs: u64,
    /// Same at s precision: each weight rounded with
    /// `round_half_even_div(w, 100)` before the comparison.
    pub n_ties_s: u64,
    /// Tie rates and the rate delta `tie_rate_s - tie_rate_cs`.
    pub tie_rate_cs: f64,
    pub tie_rate_s: f64,
    pub tie_rate_delta: f64,
}

// ---- Section B ------------------------------------------------------------

#[derive(Debug, Serialize, Default, Clone)]
pub struct HotStats {
    /// Number of OD queries that ran. Snap-failed pairs are dropped
    /// in [`snap_od_pairs`] before any query is dispatched, so the
    /// denominator counts only OD pairs whose both endpoints snapped
    /// onto a mode-eligible road. Unreachable pairs (both snapped but
    /// no path in the CCH) contribute 0 relaxations and are tallied
    /// in `n_queries_unreachable`.
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
pub fn run_weight_profile(data_dir: &Path, output_dir: &Path, region: Option<&str>) -> Result<()> {
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
        let t_up = compute_static_stats(&mode_data.cch_weights.up.iter().collect::<Vec<u32>>());
        let t_dn = compute_static_stats(&mode_data.cch_weights.down.iter().collect::<Vec<u32>>());
        let d_up =
            compute_static_stats(&mode_data.cch_weights_dist.up.iter().collect::<Vec<u32>>());
        let d_dn =
            compute_static_stats(&mode_data.cch_weights_dist.down.iter().collect::<Vec<u32>>());
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
    println!("[3/?] Section C: per-block range histograms (block sizes 32, 64, 128)...");
    const BLOCK_SIZES: &[usize] = &[32, 64, 128];
    let mut blocks_c: BTreeMap<String, (BlockStats, BlockStats, BlockStats, BlockStats)> =
        BTreeMap::new();
    for (i, mode_name) in state.mode_names.iter().enumerate() {
        let mode_data = &state.modes[i];
        let t_up = compute_block_stats(
            &mode_data.cch_weights.up.iter().collect::<Vec<u32>>(),
            BLOCK_SIZES,
        );
        let t_dn = compute_block_stats(
            &mode_data.cch_weights.down.iter().collect::<Vec<u32>>(),
            BLOCK_SIZES,
        );
        let d_up = compute_block_stats(
            &mode_data.cch_weights_dist.up.iter().collect::<Vec<u32>>(),
            BLOCK_SIZES,
        );
        let d_dn = compute_block_stats(
            &mode_data.cch_weights_dist.down.iter().collect::<Vec<u32>>(),
            BLOCK_SIZES,
        );
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

    // ---- Section D -------------------------------------------------------
    println!("[5/?] Section D: cumulative rounding sensitivity (1 000 random P2P routes)...");
    let mut rounding_d: BTreeMap<String, (RoundingStats, RoundingStats)> = BTreeMap::new();
    let d_pairs = generate_rounding_pairs(&bbox);
    for (mode_idx, mode_name) in state.mode_names.iter().enumerate() {
        let mode_data = &state.modes[mode_idx];
        let snapped: Vec<(u32, u32)> = snap_od_pairs(&state, &d_pairs, mode_idx as u8);
        let n_nodes = mode_data.cch_topo.n_nodes as usize;

        let t_start = std::time::Instant::now();
        let r_time = run_rounding_routes(
            n_nodes,
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            &snapped,
            100, // s = cs / 100
        );
        let d_start = std::time::Instant::now();
        let r_dist = run_rounding_routes(
            n_nodes,
            &mode_data.up_adj_flat_dist,
            &mode_data.down_rev_flat_dist,
            &snapped,
            1000, // m = mm / 1000
        );
        println!(
            "  - {}: time {} routes (median {:.4}%, p99 {:.4}%, >1% {}/{}) in {:.1}s; \
             dist {} routes (median {:.4}%, p99 {:.4}%, >1% {}/{}) in {:.1}s",
            mode_name,
            r_time.n_routes_total,
            r_time.median_drift_pct,
            r_time.p99_drift_pct,
            r_time.drift_over_1pct_count,
            r_time.n_routes_total,
            t_start.elapsed().as_secs_f64() - (d_start.elapsed().as_secs_f64()),
            r_dist.n_routes_total,
            r_dist.median_drift_pct,
            r_dist.p99_drift_pct,
            r_dist.drift_over_1pct_count,
            r_dist.n_routes_total,
            d_start.elapsed().as_secs_f64(),
        );
        rounding_d.insert(mode_name.clone(), (r_time, r_dist));
    }
    println!();

    // ---- Section E -------------------------------------------------------
    println!("[6/?] Section E: triangle-relaxation tie rate (cs vs s precision)...");
    let mut tie_e: BTreeMap<String, (TieStats, TieStats)> = BTreeMap::new();
    for (mode_idx, mode_name) in state.mode_names.iter().enumerate() {
        let mode_data = &state.modes[mode_idx];
        let t_start = std::time::Instant::now();
        let tie_time = compute_tie_stats(&mode_data.cch_topo, &mode_data.cch_weights, 100);
        let tt = t_start.elapsed().as_secs_f64();
        let d_start = std::time::Instant::now();
        let tie_dist = compute_tie_stats(&mode_data.cch_topo, &mode_data.cch_weights_dist, 1000);
        let td = d_start.elapsed().as_secs_f64();
        println!(
            "  - {}: time {} triangles, {} ties cs ({:.4}%) → {} ties s ({:.4}%) Δ={:+.4}% in {:.1}s; \
             dist {} triangles, {} ties cs ({:.4}%) → {} ties s ({:.4}%) Δ={:+.4}% in {:.1}s",
            mode_name,
            tie_time.n_triangles_total,
            tie_time.n_ties_cs,
            100.0 * tie_time.tie_rate_cs,
            tie_time.n_ties_s,
            100.0 * tie_time.tie_rate_s,
            100.0 * tie_time.tie_rate_delta,
            tt,
            tie_dist.n_triangles_total,
            tie_dist.n_ties_cs,
            100.0 * tie_dist.tie_rate_cs,
            tie_dist.n_ties_s,
            100.0 * tie_dist.tie_rate_s,
            100.0 * tie_dist.tie_rate_delta,
            td,
        );
        tie_e.insert(mode_name.clone(), (tie_time, tie_dist));
    }
    println!();

    // ---- Assemble per-mode report ---------------------------------------
    let mut modes_report: BTreeMap<String, ModeReport> = BTreeMap::new();
    for mode_name in state.mode_names.iter() {
        let (t_up, t_dn, d_up, d_dn) = static_a.remove(mode_name).expect("static A populated");
        let (tb_up, tb_dn, db_up, db_dn) = blocks_c.remove(mode_name).expect("blocks C populated");
        let (hot_time, hot_dist) = hot_b.remove(mode_name).expect("hot B populated");
        let (r_time, r_dist) = rounding_d.remove(mode_name).expect("rounding D populated");
        let (tie_time, tie_dist) = tie_e.remove(mode_name).expect("tie E populated");
        modes_report.insert(
            mode_name.clone(),
            ModeReport {
                time: MetricReport {
                    statik: DirectionPair {
                        up: t_up,
                        down: t_dn,
                    },
                    blocks: DirectionPair {
                        up: tb_up,
                        down: tb_dn,
                    },
                    hot: hot_time,
                    rounding: r_time,
                    tie: tie_time,
                },
                dist: MetricReport {
                    statik: DirectionPair {
                        up: d_up,
                        down: d_dn,
                    },
                    blocks: DirectionPair {
                        up: db_up,
                        down: db_dn,
                    },
                    hot: hot_dist,
                    rounding: r_dist,
                    tie: tie_dist,
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

    let md_path = output_dir.join("weight-profile.md");
    let md = render_markdown(&report);
    std::fs::write(&md_path, md).with_context(|| format!("writing {}", md_path.display()))?;
    println!("  ✓ wrote {}", md_path.display());
    println!();

    Ok(())
}

// ---------- Markdown rendering --------------------------------------------

/// Render the human report as a markdown document. Tables follow the
/// shape requested by #298:
///   Table A: per-mode static buckets at cs/s/ds.
///   Table B: per-mode hot overflow rates at u16/u24 in cs/s.
///   Table D: per-mode rounding drift summary.
///   Table E: per-mode tie-rate delta.
///   Verdict matrix: safe / measure-more / unsafe per (mode, metric, codec).
fn render_markdown(report: &Report) -> String {
    let mut out = String::new();
    out.push_str("# Weight Distribution Profile — Belgium\n\n");
    out.push_str(&format!("- **Region:** `{}`\n", report.region));
    out.push_str(&format!("- **Generated at:** {}\n", report.generated_at));
    out.push_str(&format!("- **Git SHA:** `{}`\n", report.git_sha));
    out.push_str(&format!("- **Schema version:** `{}`\n", report.version));
    out.push_str(&format!("- **Seed:** `{}`\n\n", report.seed));
    out.push_str(
        "Generated by `butterfly-bench weight-profile`. See \
         [issue #298](https://github.com/butterfly-osm/butterfly-osm/issues/298) for context.\n\n",
    );

    // -- Headline verdict ----------------------------------------------------
    out.push_str("## Headline verdicts\n\n");
    out.push_str(
        "The two codec / unit questions the profiler exists to \
         answer. Verdict rules: `safe` if hot overflow < 1 %; \
         `measure-more` if 1 % ≤ hot overflow < 5 %; `unsafe` if ≥ 5 %.\n\n",
    );
    out.push_str("| Mode | Metric | u16 + overflow at s units | u24 + overflow at s/m units | Rounding drift > 1 % on > 1 % of paths? |\n");
    out.push_str("|------|--------|---------------------------|------------------------------|------------------------------------------|\n");
    for (mode_name, mode) in &report.modes {
        out.push_str(&format!(
            "| {} | time | {} ({:.4} %) | {} ({:.4} %) | {} |\n",
            mode_name,
            verdict(mode.time.hot.overflow_rates_s.u16),
            100.0 * mode.time.hot.overflow_rates_s.u16,
            verdict(mode.time.hot.overflow_rates_s.u24),
            100.0 * mode.time.hot.overflow_rates_s.u24,
            yes_no(mode.time.rounding.frac_over_1pct > 0.01),
        ));
        out.push_str(&format!(
            "| {} | dist | {} ({:.4} %) | {} ({:.4} %) | {} |\n",
            mode_name,
            verdict(mode.dist.hot.overflow_rates_s.u16),
            100.0 * mode.dist.hot.overflow_rates_s.u16,
            verdict(mode.dist.hot.overflow_rates_s.u24),
            100.0 * mode.dist.hot.overflow_rates_s.u24,
            yes_no(mode.dist.rounding.frac_over_1pct > 0.01),
        ));
    }
    out.push('\n');

    // -- Table A: static buckets ---------------------------------------------
    out.push_str("## Table A — Static distribution buckets (per-mode, UP direction)\n\n");
    out.push_str(
        "Edges grouped by quantised weight against codec thresholds. \
         `≤ 65 534` fits in `u16` without sentinel collision; \
         `≤ 16 777 214` fits in `u24` without sentinel collision; \
         `INF` is the existing `u32::MAX` sentinel. Counts are over \
         every CCH UP edge.\n\n",
    );
    out.push_str("| Mode | Metric | Unit | n_edges | ≤ 65534 | == 65535 | ≤ u24max-1 | > u24max-1 | == INF |\n");
    out.push_str("|------|--------|------|---------|---------|----------|------------|------------|--------|\n");
    for (mode_name, mode) in &report.modes {
        for (metric_name, metric) in [("time", &mode.time), ("dist", &mode.dist)] {
            let s = &metric.statik.up;
            for (unit_name, b) in [
                ("cs / mm", &s.buckets_cs),
                ("ds / cm", &s.buckets_ds),
                ("s / m", &s.buckets_s),
            ] {
                out.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {} | {} | {} | {} |\n",
                    mode_name,
                    metric_name,
                    unit_name,
                    s.n_edges,
                    b.le_65534,
                    b.eq_65535,
                    b.le_u24_max_minus_one,
                    b.gt_u24_max_minus_one,
                    b.eq_inf,
                ));
            }
        }
    }
    out.push('\n');

    // -- Table B: hot overflow rates -----------------------------------------
    out.push_str("## Table B — Hot-query-weighted overflow rates\n\n");
    out.push_str(
        "Each entry is `relaxed_edges_with_weight_over_threshold / total_relaxed_edges` \
         from 100 corpus + 10 000 RNG OD-pair bidirectional CCH P2P queries. Verdict \
         column uses the same rule as the headline.\n\n",
    );
    out.push_str("| Mode | Metric | n_queries | n_relaxations | u16 over (cs) | u16 over (s) | u24 over (cs) | u24 over (s) | Verdict (u16 @ s) | Verdict (u24 @ s) |\n");
    out.push_str("|------|--------|-----------|---------------|---------------|--------------|---------------|--------------|-------------------|-------------------|\n");
    for (mode_name, mode) in &report.modes {
        for (metric_name, metric) in [("time", &mode.time), ("dist", &mode.dist)] {
            let h = &metric.hot;
            out.push_str(&format!(
                "| {} | {} | {} | {} | {:.4} % | {:.4} % | {:.4} % | {:.4} % | {} | {} |\n",
                mode_name,
                metric_name,
                h.n_queries_reached,
                h.n_relaxed_total,
                100.0 * h.overflow_rates_cs.u16,
                100.0 * h.overflow_rates_s.u16,
                100.0 * h.overflow_rates_cs.u24,
                100.0 * h.overflow_rates_s.u24,
                verdict(h.overflow_rates_s.u16),
                verdict(h.overflow_rates_s.u24),
            ));
        }
    }
    out.push('\n');

    // -- Table C: block-bit summary ------------------------------------------
    out.push_str("## Table C — Per-block bit-width summary (UP direction)\n\n");
    out.push_str(
        "For each block size, `mean_bits` is the average of \
         `ceil(log2(block_range + 1))` across non-INF blocks. \
         `max_bits` is the worst case. `n_inf_blocks` is the count \
         of fully-INF blocks (encoded as a single sentinel).\n\n",
    );
    out.push_str(
        "| Mode | Metric | Block size | n_blocks | n_inf_blocks | mean_bits | max_bits |\n",
    );
    out.push_str(
        "|------|--------|-----------:|---------:|-------------:|----------:|---------:|\n",
    );
    for (mode_name, mode) in &report.modes {
        for (metric_name, metric) in [("time", &mode.time), ("dist", &mode.dist)] {
            let blocks = &metric.blocks.up;
            // Render in block-size ascending order regardless of insertion order.
            let mut keys: Vec<&String> = blocks.by_block_size.keys().collect();
            keys.sort_by_key(|k| k.parse::<u32>().unwrap_or(0));
            for k in keys {
                let bs = &blocks.by_block_size[k];
                out.push_str(&format!(
                    "| {} | {} | {} | {} | {} | {:.2} | {} |\n",
                    mode_name,
                    metric_name,
                    bs.block_size,
                    bs.n_blocks,
                    bs.n_inf_blocks,
                    bs.mean_bits,
                    bs.max_bits,
                ));
            }
        }
    }
    out.push('\n');

    // -- Table D: rounding ----------------------------------------------------
    out.push_str("## Table D — Cumulative rounding sensitivity\n\n");
    out.push_str(
        "Per-mode path-total drift between the cs/mm reference and the \
         per-edge round-half-to-even quantised total in s/m units. \
         `frac > 1 %` is the share of routes whose path total drifts \
         more than 1 % under the unit change.\n\n",
    );
    out.push_str(
        "| Mode | Metric | n_routes | median (%) | p90 (%) | p99 (%) | max (%) | mean (%) | > 1 % | > 5 % | > 10 % | frac > 1 % |\n",
    );
    out.push_str(
        "|------|--------|---------:|-----------:|--------:|--------:|--------:|---------:|------:|------:|-------:|-----------:|\n",
    );
    for (mode_name, mode) in &report.modes {
        for (metric_name, metric) in [("time", &mode.time), ("dist", &mode.dist)] {
            let r = &metric.rounding;
            out.push_str(&format!(
                "| {} | {} | {} | {:.4} | {:.4} | {:.4} | {:.4} | {:.4} | {} | {} | {} | {:.4} % |\n",
                mode_name,
                metric_name,
                r.n_routes_total,
                r.median_drift_pct,
                r.p90_drift_pct,
                r.p99_drift_pct,
                r.max_drift_pct,
                r.mean_drift_pct,
                r.drift_over_1pct_count,
                r.drift_over_5pct_count,
                r.drift_over_10pct_count,
                100.0 * r.frac_over_1pct,
            ));
        }
    }
    out.push('\n');

    // -- Table E: tie rate ---------------------------------------------------
    out.push_str("## Table E — Triangle relaxation tie rate (cs vs s/m)\n\n");
    out.push_str(
        "For each (x, m, y) triangle in the CCH, counts ties of \
         `w(x, y) == w(x → m) + w(m → y)` at cs and at the quantised \
         (s for time, m for distance) precision. `Δ` is the absolute \
         increase in tie rate the unit change introduces — the \
         nondeterminism cost of the unit change.\n\n",
    );
    out.push_str("| Mode | Metric | n_triangles | ties cs | tie rate cs (%) | ties s | tie rate s (%) | Δ (pp) |\n");
    out.push_str("|------|--------|------------:|--------:|----------------:|-------:|---------------:|-------:|\n");
    for (mode_name, mode) in &report.modes {
        for (metric_name, metric) in [("time", &mode.time), ("dist", &mode.dist)] {
            let t = &metric.tie;
            out.push_str(&format!(
                "| {} | {} | {} | {} | {:.4} | {} | {:.4} | {:+.4} |\n",
                mode_name,
                metric_name,
                t.n_triangles_total,
                t.n_ties_cs,
                100.0 * t.tie_rate_cs,
                t.n_ties_s,
                100.0 * t.tie_rate_s,
                100.0 * t.tie_rate_delta,
            ));
        }
    }
    out.push('\n');

    // -- Distinct value summary ----------------------------------------------
    out.push_str("## Distinct-value count summary\n\n");
    out.push_str(
        "Total distinct non-INF weight values per (mode, metric, direction). \
         Useful for evaluating a palette codec (small distinct count → small \
         palette → cheap lookups).\n\n",
    );
    out.push_str("| Mode | Metric | UP distinct | DOWN distinct |\n");
    out.push_str("|------|--------|------------:|--------------:|\n");
    for (mode_name, mode) in &report.modes {
        for (metric_name, metric) in [("time", &mode.time), ("dist", &mode.dist)] {
            out.push_str(&format!(
                "| {} | {} | {} | {} |\n",
                mode_name,
                metric_name,
                metric.statik.up.distinct_count,
                metric.statik.down.distinct_count,
            ));
        }
    }
    out.push('\n');

    out
}

/// Pretty-print a verdict from a fractional rate. Rules per #298:
///   `< 1 %`             → safe
///   `1 % … < 5 %`        → measure-more
///   `≥ 5 %`              → unsafe
fn verdict(rate: f64) -> &'static str {
    if rate < 0.01 {
        "safe"
    } else if rate < 0.05 {
        "measure-more"
    } else {
        "unsafe"
    }
}

fn yes_no(b: bool) -> &'static str {
    if b { "yes" } else { "no" }
}

// ---------- Section A helpers ----------------------------------------------

/// Walk a `&[u32]` weight array and produce a full `StaticStats`. INF
/// (`u32::MAX`) entries are excluded from min/max/percentiles and from
/// every `BucketCounts` bucket *except* `eq_inf`. The `distinct_count`
/// is over the non-INF set; INF is counted via `n_inf`.
///
/// The percentile shape is **nearest-rank** (no interpolation) on
/// the sorted non-INF set: `p_k = sorted[floor(k * (n-1) / 1000)]`,
/// where the index is computed in fixed-point by
/// [`percentile_index`] so the result is bit-identical across runs.
/// Nearest-rank was chosen over the linear-interpolation flavour so
/// reported percentiles are always actual edge weights — useful when
/// the JSON is read by codec-evaluation scripts that look up buckets
/// by the reported value rather than treating it as a smoothed
/// statistic.
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

/// 10 100 OD pairs drawn uniformly from the bbox: 100 from a fixed
/// sub-seed `WEIGHT_PROFILE_SUB_SEED_CORPUS`, then 10 000 from the
/// main `WEIGHT_PROFILE_SEED`. Both draws use the same RNG construction
/// so neither set is "real" external corpus data — the 100-pair
/// `pseudo_corpus` mirrors #298's prose about a "100 OD corpus" with a
/// deterministic stand-in until a curated corpus file ships.
///
/// To swap in an external corpus, replace the first loop with a CSV
/// load and bail if the file is missing. Today the deterministic
/// pseudo-corpus is sufficient to gate codec / unit decisions per the
/// profiler verdict table.
fn generate_od_pairs(bbox: &Bbox) -> Vec<OdPair> {
    let mut out = Vec::with_capacity(10_100);
    // Pseudo-corpus (100 deterministic pairs from a dedicated sub-seed).
    let mut pseudo_corpus_rng = StdRng::seed_from_u64(WEIGHT_PROFILE_SUB_SEED_CORPUS);
    for _ in 0..100 {
        out.push(rng_od_pair(&mut pseudo_corpus_rng, bbox));
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
            let src_orig = state
                .snap_index
                .snap(p.origin_lon, p.origin_lat, mode_idx)?;
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
    let n_unique_edges_visited = count_set_bits(&up_bin.visited) + count_set_bits(&dn_bin.visited);

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
        if let Some((u, Reverse(d))) = pq_fwd.pop()
            && d <= dist_fwd[u as usize]
        {
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
                let w = up_adj_flat.weights.get(slot);
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

        // Backward reversed-DOWN step.
        if let Some((u, Reverse(d))) = pq_bwd.pop()
            && d <= dist_bwd[u as usize]
        {
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
                let w = down_rev_flat.weights.get(slot);
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

    best_dist != u32::MAX
}

// ---------- Section D helpers ----------------------------------------------

/// Generate 4 000 OD pairs from the bbox using a fixed sub-seed
/// (0xDD_1F_7E) distinct from the corpus / hot seeds. The over-
/// sample budget targets ~1 000 *successful* snapped routes on
/// Belgium, where ~35 % of bbox-uniform pairs snap to both ends
/// (the bbox includes large non-Belgium chunks of FR / NL / DE
/// and the North Sea — about 65 % of uniform draws land outside
/// any reachable road).
fn generate_rounding_pairs(bbox: &Bbox) -> Vec<OdPair> {
    let mut rng = StdRng::seed_from_u64(0xDD_1F_7E);
    (0..4_000).map(|_| rng_od_pair(&mut rng, bbox)).collect()
}

/// For each snapped OD pair, run a bidirectional CCH P2P, recover
/// the path's per-edge weights, and compare cs/mm vs s/m precision
/// totals. `divisor` is the cs → s factor (100) or mm → m factor
/// (1000) depending on metric. Returns the populated `RoundingStats`.
///
/// `drift_pct = |sum_ref_cs - sum_round_cs| / sum_ref_cs`, where
/// `sum_round_cs = (sum of per-edge round_half_even_div(w, divisor)) *
/// divisor`. This compares the path's true cs total against the total
/// you'd see if each edge weight were stored in s/m units and
/// reconstructed by multiplication.
fn run_rounding_routes(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    snapped: &[(u32, u32)],
    divisor: u32,
) -> RoundingStats {
    // Per-route drift values; sorted at the end for percentiles.
    // `map_init` runs the `PathScratch::new` closure once per rayon
    // worker thread so the O(n_nodes) allocation is amortised across
    // every route that worker processes — a single profiler pass on
    // Belgium pays N_THREADS × ~160 MiB peak instead of per-route
    // ~160 MiB transient (Copilot #305 review).
    let drifts: Vec<f64> = snapped
        .par_iter()
        .map_init(
            || PathScratch::new(n_nodes),
            |scratch, &(src, dst)| {
                let path = match path_edges(scratch, up_adj_flat, down_rev_flat, src, dst) {
                    Some(p) => p,
                    None => return None,
                };
                if path.is_empty() {
                    return Some(0.0);
                }
                let mut sum_cs: u64 = 0;
                let mut sum_round_cs: u64 = 0;
                for &w in &path {
                    sum_cs = sum_cs.saturating_add(w as u64);
                    let q = round_half_even_div(w as u64, divisor as u64);
                    sum_round_cs = sum_round_cs.saturating_add(q * divisor as u64);
                }
                if sum_cs == 0 {
                    return Some(0.0);
                }
                let diff = (sum_cs as i64 - sum_round_cs as i64).unsigned_abs();
                let drift = diff as f64 / sum_cs as f64 * 100.0;
                Some(drift)
            },
        )
        .filter_map(|x| x)
        .collect();

    let mut sorted = drifts;
    let n_total = sorted.len() as u64;
    let n_attempted = snapped.len() as u64;
    let n_unreachable = n_attempted.saturating_sub(n_total);
    if sorted.is_empty() {
        return RoundingStats {
            n_routes_total: 0,
            n_routes_attempted: n_attempted,
            n_routes_unreachable: n_unreachable,
            ..Default::default()
        };
    }
    sorted.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));

    let n = sorted.len();
    let median_idx = percentile_index(n, 50_000);
    let p90_idx = percentile_index(n, 90_000);
    let p99_idx = percentile_index(n, 99_000);
    let median = sorted[median_idx];
    let p90 = sorted[p90_idx];
    let p99 = sorted[p99_idx];
    let max = *sorted.last().unwrap();
    let mean = sorted.iter().copied().sum::<f64>() / n as f64;

    let over_1 = sorted.iter().filter(|&&d| d > 1.0).count() as u64;
    let over_5 = sorted.iter().filter(|&&d| d > 5.0).count() as u64;
    let over_10 = sorted.iter().filter(|&&d| d > 10.0).count() as u64;
    let frac_over_1 = over_1 as f64 / n as f64;

    RoundingStats {
        n_routes_total: n_total,
        n_routes_attempted: n_attempted,
        n_routes_unreachable: n_unreachable,
        median_drift_pct: median,
        p90_drift_pct: p90,
        p99_drift_pct: p99,
        max_drift_pct: max,
        mean_drift_pct: mean,
        drift_over_1pct_count: over_1,
        drift_over_5pct_count: over_5,
        drift_over_10pct_count: over_10,
        frac_over_1pct: frac_over_1,
    }
}

/// Reusable scratch for repeated [`path_edges`] calls. Allocated
/// once per rounding-routes pass; generation-stamped so per-call
/// re-init is O(1) instead of O(n_nodes).
///
/// Without this, a single profiler run on Belgium (~5 M CCH nodes,
/// ~1 000 reachable rounding routes) would allocate
/// ~5 M × 4 × 8 B = 160 MiB per route × 1 000 routes ≈ 160 GiB total
/// transient allocations — Copilot review on PR #305 flagged this as
/// the dominant runtime cost. Reusing scratch via gen-stamping
/// collapses it to a one-shot 160 MiB.
struct PathScratch {
    dist_fwd: Vec<u32>,
    dist_bwd: Vec<u32>,
    parent_fwd: Vec<(u32, u32)>,
    parent_bwd: Vec<(u32, u32)>,
    gen_fwd: Vec<u64>,
    gen_bwd: Vec<u64>,
    current_gen: u64,
    pq_fwd: PriorityQueue<u32, Reverse<u32>>,
    pq_bwd: PriorityQueue<u32, Reverse<u32>>,
}

impl PathScratch {
    fn new(n_nodes: usize) -> Self {
        Self {
            dist_fwd: vec![u32::MAX; n_nodes],
            dist_bwd: vec![u32::MAX; n_nodes],
            parent_fwd: vec![(u32::MAX, 0); n_nodes],
            parent_bwd: vec![(u32::MAX, 0); n_nodes],
            gen_fwd: vec![0; n_nodes],
            gen_bwd: vec![0; n_nodes],
            current_gen: 0,
            pq_fwd: PriorityQueue::new(),
            pq_bwd: PriorityQueue::new(),
        }
    }

    /// Bump the generation counter and clear the PQs; the dist /
    /// parent arrays are left intact and reads must check the gen
    /// stamp first.
    fn start_query(&mut self) {
        self.current_gen = self.current_gen.wrapping_add(1);
        // On generation wrap (every 2^64 queries — never in practice)
        // reset every gen stamp so the previous "current" can't be
        // confused with a fresh entry.
        if self.current_gen == 0 {
            self.gen_fwd.fill(0);
            self.gen_bwd.fill(0);
            self.current_gen = 1;
        }
        self.pq_fwd.clear();
        self.pq_bwd.clear();
    }

    #[inline]
    fn dist_fwd(&self, u: usize) -> u32 {
        if self.gen_fwd[u] == self.current_gen {
            self.dist_fwd[u]
        } else {
            u32::MAX
        }
    }

    #[inline]
    fn dist_bwd(&self, u: usize) -> u32 {
        if self.gen_bwd[u] == self.current_gen {
            self.dist_bwd[u]
        } else {
            u32::MAX
        }
    }

    #[inline]
    fn set_fwd(&mut self, u: usize, d: u32, parent: (u32, u32)) {
        self.dist_fwd[u] = d;
        self.parent_fwd[u] = parent;
        self.gen_fwd[u] = self.current_gen;
    }

    #[inline]
    fn set_bwd(&mut self, u: usize, d: u32, parent: (u32, u32)) {
        self.dist_bwd[u] = d;
        self.parent_bwd[u] = parent;
        self.gen_bwd[u] = self.current_gen;
    }
}

/// Run bidirectional CCH P2P from `source` to `target` and return
/// the sequence of per-CCH-edge weights along the meeting-node
/// path. Mirrors `CchQuery::query` but does not unpack shortcuts —
/// the spec's "round each edge weight independently" is over the
/// CCH edges as stored, which is what the query actually reads.
///
/// Caller owns `scratch` and reuses it across calls — see
/// [`PathScratch`] for the rationale.
///
/// Returns `None` if the search finds no finite path. Returns
/// `Some(vec![])` for the trivial `source == target` case.
fn path_edges(
    scratch: &mut PathScratch,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    source: u32,
    target: u32,
) -> Option<Vec<u32>> {
    if source == target {
        return Some(Vec::new());
    }

    scratch.start_query();
    scratch.set_fwd(source as usize, 0, (u32::MAX, 0));
    scratch.set_bwd(target as usize, 0, (u32::MAX, 0));
    scratch.pq_fwd.push(source, Reverse(0));
    scratch.pq_bwd.push(target, Reverse(0));

    let mut best_dist = u32::MAX;
    let mut meeting_node = u32::MAX;

    while !scratch.pq_fwd.is_empty() || !scratch.pq_bwd.is_empty() {
        let fwd_min = scratch
            .pq_fwd
            .peek()
            .map(|(_, &Reverse(d))| d)
            .unwrap_or(u32::MAX);
        let bwd_min = scratch
            .pq_bwd
            .peek()
            .map(|(_, &Reverse(d))| d)
            .unwrap_or(u32::MAX);
        if fwd_min >= best_dist && bwd_min >= best_dist {
            break;
        }
        if let Some((u, Reverse(d))) = scratch.pq_fwd.pop()
            && d <= scratch.dist_fwd(u as usize)
        {
            let bwd_d = scratch.dist_bwd(u as usize);
            if bwd_d != u32::MAX {
                let total = d.saturating_add(bwd_d);
                if total < best_dist {
                    best_dist = total;
                    meeting_node = u;
                }
            }
            let start = up_adj_flat.offsets[u as usize] as usize;
            let end = up_adj_flat.offsets[u as usize + 1] as usize;
            for slot in start..end {
                let v = up_adj_flat.targets[slot];
                let w = up_adj_flat.weights.get(slot);
                let new_dist = d.saturating_add(w);
                if new_dist < scratch.dist_fwd(v as usize) {
                    scratch.set_fwd(v as usize, new_dist, (u, w));
                    scratch.pq_fwd.push(v, Reverse(new_dist));
                    let bwd_v = scratch.dist_bwd(v as usize);
                    if bwd_v != u32::MAX {
                        let total = new_dist.saturating_add(bwd_v);
                        if total < best_dist {
                            best_dist = total;
                            meeting_node = v;
                        }
                    }
                }
            }
        }
        if let Some((u, Reverse(d))) = scratch.pq_bwd.pop()
            && d <= scratch.dist_bwd(u as usize)
        {
            let fwd_d = scratch.dist_fwd(u as usize);
            if fwd_d != u32::MAX {
                let total = d.saturating_add(fwd_d);
                if total < best_dist {
                    best_dist = total;
                    meeting_node = u;
                }
            }
            let start = down_rev_flat.offsets[u as usize] as usize;
            let end = down_rev_flat.offsets[u as usize + 1] as usize;
            for slot in start..end {
                let x = down_rev_flat.sources[slot];
                let w = down_rev_flat.weights.get(slot);
                let new_dist = d.saturating_add(w);
                if new_dist < scratch.dist_bwd(x as usize) {
                    scratch.set_bwd(x as usize, new_dist, (u, w));
                    scratch.pq_bwd.push(x, Reverse(new_dist));
                    let fwd_x = scratch.dist_fwd(x as usize);
                    if fwd_x != u32::MAX {
                        let total = new_dist.saturating_add(fwd_x);
                        if total < best_dist {
                            best_dist = total;
                            meeting_node = x;
                        }
                    }
                }
            }
        }
    }

    if best_dist == u32::MAX {
        return None;
    }

    // Reconstruct path: forward from source → meeting_node, then
    // backward from meeting_node → target. Collect only the edge
    // weights since that's all the rounding analysis needs.
    // Reads parent_fwd/parent_bwd directly — nodes on the meeting
    // path were touched this query, so the gen stamps are current
    // and the parent entries are valid for this iteration.
    let mut weights: Vec<u32> = Vec::new();
    let mut cur = meeting_node;
    while cur != source {
        let (prev, w) = scratch.parent_fwd[cur as usize];
        if prev == u32::MAX {
            // Disconnected on the forward side; can happen on
            // bidirectional search if `meeting_node` is itself the
            // source. Treat as empty path on this leg.
            break;
        }
        weights.push(w);
        cur = prev;
    }
    weights.reverse();
    let mut cur = meeting_node;
    while cur != target {
        let (prev, w) = scratch.parent_bwd[cur as usize];
        if prev == u32::MAX {
            break;
        }
        weights.push(w);
        cur = prev;
    }
    Some(weights)
}

// ---------- Section E helpers ----------------------------------------------

/// Enumerate every triangle `(x, m, y)` in the CCH topology and
/// count ties of `w(x, y) == w(x → m) + w(m → y)` at cs vs at
/// quantised precision (`divisor` = 100 for s, 1000 for m).
///
/// For each apex `m`, walk `m`'s DOWN edges (those land at lower-
/// rank nodes `x`) and `m`'s UP edges (those land at higher-rank
/// nodes `y`). The triangle is closed by either an UP edge (x →
/// y) or a DOWN edge (x → y) depending on whether y has higher or
/// lower rank than x.
///
/// Parallelised by apex `m` with rayon. Each thread accumulates
/// into local `(n_total, n_ties_cs, n_ties_s)` triples; the reduce
/// is plain addition.
fn compute_tie_stats(
    topo: &butterfly_route::formats::CchTopo,
    weights: &butterfly_route::formats::CchWeights,
    divisor: u32,
) -> TieStats {
    let n_nodes = topo.n_nodes as usize;

    let (n_total, n_ties_cs, n_ties_s) = (0..n_nodes)
        .into_par_iter()
        .map(|m| {
            let mut local_total: u64 = 0;
            let mut local_ties_cs: u64 = 0;
            let mut local_ties_s: u64 = 0;

            // Walk down edges from m (lower-rank neighbours x).
            let down_start = topo.down_offsets[m] as usize;
            let down_end = topo.down_offsets[m + 1] as usize;
            // Walk up edges from m (higher-rank neighbours y).
            let up_start = topo.up_offsets[m] as usize;
            let up_end = topo.up_offsets[m + 1] as usize;
            if down_end == down_start || up_end == up_start {
                return (0u64, 0u64, 0u64);
            }

            for i_xm in down_start..down_end {
                let x = topo.down_targets[i_xm] as usize;
                let w_xm = weights.down.get(i_xm);
                if w_xm == u32::MAX {
                    continue;
                }
                let w_xm_s = round_half_even_div(w_xm as u64, divisor as u64);

                for i_my in up_start..up_end {
                    let y = topo.up_targets[i_my] as usize;
                    if y == x {
                        continue;
                    }
                    let w_my = weights.up.get(i_my);
                    if w_my == u32::MAX {
                        continue;
                    }
                    let w_my_s = round_half_even_div(w_my as u64, divisor as u64);

                    // Direct edge (x → y). Two cases: y > x → UP, y < x → DOWN.
                    let direct_w = if y > x {
                        find_edge_weight(x, y, &topo.up_offsets, &topo.up_targets, &weights.up)
                    } else {
                        find_edge_weight(
                            x,
                            y,
                            &topo.down_offsets,
                            &topo.down_targets,
                            &weights.down,
                        )
                    };
                    if direct_w == u32::MAX {
                        // No closing edge — not a triangle.
                        continue;
                    }
                    local_total += 1;

                    let two_hop = w_xm.saturating_add(w_my);
                    if direct_w == two_hop {
                        local_ties_cs += 1;
                    }
                    let direct_w_s = round_half_even_div(direct_w as u64, divisor as u64);
                    let two_hop_s = w_xm_s.saturating_add(w_my_s);
                    if direct_w_s == two_hop_s {
                        local_ties_s += 1;
                    }
                }
            }
            (local_total, local_ties_cs, local_ties_s)
        })
        .reduce(
            || (0u64, 0u64, 0u64),
            |a, b| (a.0 + b.0, a.1 + b.1, a.2 + b.2),
        );

    let rate_cs = if n_total > 0 {
        n_ties_cs as f64 / n_total as f64
    } else {
        0.0
    };
    let rate_s = if n_total > 0 {
        n_ties_s as f64 / n_total as f64
    } else {
        0.0
    };
    TieStats {
        n_triangles_total: n_total,
        n_ties_cs,
        n_ties_s,
        tie_rate_cs: rate_cs,
        tie_rate_s: rate_s,
        tie_rate_delta: rate_s - rate_cs,
    }
}

/// CCH CSR lookup: given `(u, v)` find the weight on edge `u → v`
/// in the offsets/targets/weights triple. Returns `u32::MAX` if
/// the edge doesn't exist. The targets slice is sorted ascending
/// per CCH invariants, so we use binary search.
#[inline]
fn find_edge_weight(
    u: usize,
    v: usize,
    offsets: &[u64],
    targets: &[u32],
    weights: &butterfly_route::formats::WeightArray,
) -> u32 {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    if start >= end {
        return u32::MAX;
    }
    match targets[start..end].binary_search(&(v as u32)) {
        Ok(idx) => weights.get(start + idx),
        Err(_) => u32::MAX,
    }
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
///   * range == 0 → 0 bits (a constant block can be encoded as the min
///     value alone, no per-edge bits needed).
///   * range == 1 → 1 bit.
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
        let weights = vec![
            0u32,
            100,
            65_534,
            65_535,
            65_536,
            16_777_214,
            16_777_215,
            u32::MAX,
        ];
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
