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

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

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

    // ---- Assemble per-mode report ---------------------------------------
    let mut modes_report: BTreeMap<String, ModeReport> = BTreeMap::new();
    for mode_name in state.mode_names.iter() {
        let (t_up, t_dn, d_up, d_dn) = static_a.remove(mode_name).expect("static A populated");
        let (tb_up, tb_dn, db_up, db_dn) =
            blocks_c.remove(mode_name).expect("blocks C populated");
        modes_report.insert(
            mode_name.clone(),
            ModeReport {
                time: MetricReport {
                    statik: DirectionPair { up: t_up, down: t_dn },
                    blocks: DirectionPair { up: tb_up, down: tb_dn },
                },
                dist: MetricReport {
                    statik: DirectionPair { up: d_up, down: d_dn },
                    blocks: DirectionPair { up: db_up, down: db_dn },
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
