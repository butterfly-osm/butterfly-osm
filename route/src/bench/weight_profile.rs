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

use std::path::Path;

use anyhow::{Context, Result};

/// Fixed seed for every RNG draw in this profiler (10 000 random OD
/// pairs, 1 000 rounding-sensitivity routes). Picked once; never
/// changed so two runs of the profiler on the same data emit
/// byte-identical JSON.
pub const WEIGHT_PROFILE_SEED: u64 = 0x0B07_7E_F1;

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

    // Subsequent commits land the real work here:
    //   - Section A: static distribution (per mode × metric × direction).
    //   - Section C: per-block range histograms (sizes 32/64/128).
    //   - Section B: hot-query-weighted overflow rates (10 000 RNG OD
    //                pairs + 100-OD test corpus).
    //   - Section D: cumulative rounding sensitivity at cs/mm → s/m.
    //   - Section E: triangle-relaxation tie rate at cs vs s precision.
    //   - Final pass: render `weight-profile.json` + `weight-profile.md`.

    println!("[scaffold] weight profiler not yet implemented — coming in follow-up commits");
    Ok(())
}
