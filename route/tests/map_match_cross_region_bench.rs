//! Bench harness for #194 — measures p50/p95 latency of
//! `map_match_multi_region` across three trace regimes:
//!
//! 1. **Single-region (Belgium-only)**: validates the fast-path has
//!    no cross-region overhead vs. plain single-region `map_match`.
//! 2. **Mixed (one cross-region transition)**: GPS trace mostly in
//!    one region with one transition straddling the border.
//! 3. **Full cross-region**: GPS trace alternating between regions.
//!
//! Emits a JSON summary at
//! `bench/route/results/2026-05-06-map-match-cross-region/summary.json`
//! when run with `--nocapture` so it can be tracked alongside other
//! benchmarks.
//!
//! Run:
//! ```bash
//! BUTTERFLY_TEST_DATA_DIR=/path/to/data \
//!   cargo test -p butterfly-route --release --test map_match_cross_region_bench \
//!   -- --ignored --nocapture
//! ```

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{Duration, Instant};

use butterfly_route::profile_abi::Mode;
use butterfly_route::server::map_match::{map_match, map_match_multi_region};
use butterfly_route::server::overlay::OverlayCluster;
use butterfly_route::server::regions::RegionsState;

const BE_CONTAINER: &str = "data/belgium/baseline.butterfly";
const LU_CONTAINER: &str = "data/luxembourg/luxembourg.butterfly";
const OVERLAY: &str = "data/be-lu-overlay.butterfly";

fn fixture_paths() -> Option<(PathBuf, PathBuf, PathBuf)> {
    let mut candidates: Vec<(PathBuf, PathBuf, PathBuf)> = Vec::new();
    candidates.push((
        PathBuf::from("../data/belgium/baseline.butterfly"),
        PathBuf::from("../data/luxembourg/luxembourg.butterfly"),
        PathBuf::from("../data/be-lu-overlay.butterfly"),
    ));
    candidates.push((
        PathBuf::from(BE_CONTAINER),
        PathBuf::from(LU_CONTAINER),
        PathBuf::from(OVERLAY),
    ));
    if let Ok(base) = std::env::var("BUTTERFLY_TEST_DATA_DIR") {
        let base = PathBuf::from(base);
        candidates.push((
            base.join("belgium").join("baseline.butterfly"),
            base.join("luxembourg").join("luxembourg.butterfly"),
            base.join("be-lu-overlay.butterfly"),
        ));
    }
    for (be, lu, ov) in &candidates {
        if be.exists() && lu.exists() && ov.exists() {
            return Some((
                be.canonicalize().unwrap_or_else(|_| be.clone()),
                lu.canonicalize().unwrap_or_else(|_| lu.clone()),
                ov.canonicalize().unwrap_or_else(|_| ov.clone()),
            ));
        }
    }
    None
}

fn load_regions_with_overlay(be: &Path, lu: &Path, overlay_path: &Path) -> Arc<RegionsState> {
    let mut regions = RegionsState::load_from_paths(&[be.to_path_buf(), lu.to_path_buf()])
        .expect("load BE+LU containers");
    let overlay = OverlayCluster::load(overlay_path).expect("load overlay");
    regions.overlay = Some(overlay);
    Arc::new(regions)
}

/// Pure Belgium trace (Brussels city, dense ~50 m spacing).
fn brussels_trace() -> Vec<(f64, f64)> {
    vec![
        (4.3517, 50.8503),
        (4.3537, 50.8513),
        (4.3557, 50.8523),
        (4.3577, 50.8533),
        (4.3597, 50.8543),
        (4.3617, 50.8553),
        (4.3637, 50.8563),
        (4.3657, 50.8573),
        (4.3677, 50.8583),
        (4.3697, 50.8593),
    ]
}

/// Mostly-Belgium trace with one BE→LU transition near the end
/// (approaches Pétange).
fn mixed_trace() -> Vec<(f64, f64)> {
    vec![
        (5.8108, 49.6841),
        (5.8275, 49.6712),
        (5.8430, 49.6580),
        (5.8540, 49.6450),
        (5.8550, 49.6300),
        (5.8410, 49.5610),
        (5.8730, 49.5520),
        (5.8865, 49.5575),
        (5.9210, 49.5365),
        (5.9530, 49.5025),
    ]
}

/// "Full cross-region" trace — every transition crosses the BE/LU
/// border. Three points only: cross-region routing scales as
/// `O(n_transitions × n_candidates_from × n_candidates_to ×
/// solve_cost)` so we keep it small for the bench.
fn zigzag_trace() -> Vec<(f64, f64)> {
    vec![
        (5.8410, 49.5610), // BE Athus
        (5.8730, 49.5520), // LU Pétange west
        (5.8410, 49.5610), // BE Athus
    ]
}

fn percentile(samples: &mut [Duration], p: f64) -> Duration {
    samples.sort();
    let idx = ((samples.len() as f64) * p).clamp(0.0, samples.len() as f64 - 1.0) as usize;
    samples[idx]
}

fn ms(d: Duration) -> f64 {
    d.as_secs_f64() * 1000.0
}

#[derive(serde::Serialize)]
struct RegimeStat {
    name: String,
    n_trials: usize,
    p50_ms: f64,
    p95_ms: f64,
    mean_ms: f64,
    n_matchings_first: usize,
    matched_tps_first: usize,
    total_tps_first: usize,
}

fn measure<F: FnMut() -> Option<usize>>(name: &str, n_trials: usize, mut f: F) -> RegimeStat {
    // Warm-up: thread-local state allocations etc.
    let _ = f();
    let mut samples: Vec<Duration> = Vec::with_capacity(n_trials);
    let mut n_matchings_first = 0usize;
    for trial in 0..n_trials {
        let t0 = Instant::now();
        let r = f();
        let dt = t0.elapsed();
        samples.push(dt);
        if trial == 0
            && let Some(matchings) = r
        {
            n_matchings_first = matchings;
        }
    }
    let p50 = percentile(&mut samples.clone(), 0.5);
    let p95 = percentile(&mut samples.clone(), 0.95);
    let mean: Duration = samples.iter().sum::<Duration>() / (samples.len() as u32);
    RegimeStat {
        name: name.to_string(),
        n_trials,
        p50_ms: ms(p50),
        p95_ms: ms(p95),
        mean_ms: ms(mean),
        n_matchings_first,
        matched_tps_first: 0,
        total_tps_first: 0,
    }
}

#[test]
#[ignore = "requires data/belgium + data/luxembourg + data/be-lu-overlay"]
fn bench_map_match_cross_region() {
    let Some((be, lu, ov)) = fixture_paths() else {
        eprintln!("skipping: BE + LU + overlay fixtures not on disk");
        return;
    };

    let regions = load_regions_with_overlay(&be, &lu, &ov);
    let be_state = &regions.regions[0].state;
    let be_mode_idx = *be_state.mode_lookup.get("car").expect("BE has car");

    // Keep N_TRIALS small for cross-region regimes (each trial is
    // ~10 s for mixed and ~30 s for zigzag). Single-region trials
    // run ~5 s each.
    const N_TRIALS: usize = 3;

    // Regime 1: pure Brussels (single-region) — both legacy
    // map_match() and new map_match_multi_region() should be ~the
    // same time. We measure both to verify the fast-path has no
    // measurable overhead.
    let trace_be = brussels_trace();
    let regime_legacy = {
        let trace = trace_be.clone();
        measure("brussels-legacy", N_TRIALS, || {
            map_match(be_state, Mode(be_mode_idx), &trace, Some(15.0), None, None)
                .map(|r| r.matchings.len())
        })
    };
    let regime_multi_be = {
        let trace = trace_be.clone();
        let regions_ref = regions.clone();
        measure("brussels-multi-region", N_TRIALS, || {
            map_match_multi_region(&regions_ref, "car", &trace, Some(15.0))
                .map(|r| r.matchings.len())
        })
    };

    // Regime 2: mixed trace — one BE→LU transition somewhere in the
    // middle.
    let trace_mixed = mixed_trace();
    let regime_mixed = {
        let trace = trace_mixed.clone();
        let regions_ref = regions.clone();
        measure("mixed-one-cross", N_TRIALS, || {
            map_match_multi_region(&regions_ref, "car", &trace, Some(15.0))
                .map(|r| r.matchings.len())
        })
    };

    // Regime 3: zigzag — every transition crosses.
    let trace_zigzag = zigzag_trace();
    let regime_zigzag = {
        let trace = trace_zigzag.clone();
        let regions_ref = regions.clone();
        measure("zigzag-full-cross", N_TRIALS, || {
            map_match_multi_region(&regions_ref, "car", &trace, Some(15.0))
                .map(|r| r.matchings.len())
        })
    };

    // ---- Emit JSON summary ---------------------------------------
    let regression_pct =
        ((regime_multi_be.p50_ms - regime_legacy.p50_ms) / regime_legacy.p50_ms.max(1e-6)) * 100.0;
    eprintln!("===== bench results =====");
    for r in [
        &regime_legacy,
        &regime_multi_be,
        &regime_mixed,
        &regime_zigzag,
    ] {
        eprintln!(
            "  {:<25} n={} p50={:.1} ms  p95={:.1} ms  mean={:.1} ms  matchings={}",
            r.name, r.n_trials, r.p50_ms, r.p95_ms, r.mean_ms, r.n_matchings_first
        );
    }
    eprintln!(
        "  single-region fast-path overhead: {:+.1}% (legacy → multi-region wrapper, p50)",
        regression_pct
    );

    let summary = serde_json::json!({
        "issue": 194,
        "date": "2026-05-06",
        "fixture": "belgium-luxembourg + be-lu-overlay",
        "trials_per_regime": N_TRIALS,
        "regimes": [
            {
                "name": regime_legacy.name,
                "trials": regime_legacy.n_trials,
                "p50_ms": regime_legacy.p50_ms,
                "p95_ms": regime_legacy.p95_ms,
                "mean_ms": regime_legacy.mean_ms,
                "n_matchings": regime_legacy.n_matchings_first,
            },
            {
                "name": regime_multi_be.name,
                "trials": regime_multi_be.n_trials,
                "p50_ms": regime_multi_be.p50_ms,
                "p95_ms": regime_multi_be.p95_ms,
                "mean_ms": regime_multi_be.mean_ms,
                "n_matchings": regime_multi_be.n_matchings_first,
            },
            {
                "name": regime_mixed.name,
                "trials": regime_mixed.n_trials,
                "p50_ms": regime_mixed.p50_ms,
                "p95_ms": regime_mixed.p95_ms,
                "mean_ms": regime_mixed.mean_ms,
                "n_matchings": regime_mixed.n_matchings_first,
            },
            {
                "name": regime_zigzag.name,
                "trials": regime_zigzag.n_trials,
                "p50_ms": regime_zigzag.p50_ms,
                "p95_ms": regime_zigzag.p95_ms,
                "mean_ms": regime_zigzag.mean_ms,
                "n_matchings": regime_zigzag.n_matchings_first,
            },
        ],
        "single_region_fastpath_p50_overhead_pct": regression_pct,
    });

    // Write next to other bench results.
    let out_dir = Path::new("../bench/route/results/2026-05-06-map-match-cross-region");
    let out_dir = if out_dir.exists() {
        out_dir.to_path_buf()
    } else {
        // Fall back to crate-relative path if running from workspace
        // root.
        PathBuf::from("bench/route/results/2026-05-06-map-match-cross-region")
    };
    if let Err(e) = std::fs::create_dir_all(&out_dir) {
        eprintln!("could not create bench dir {}: {}", out_dir.display(), e);
    }
    let path = out_dir.join("summary.json");
    if let Err(e) = std::fs::write(&path, serde_json::to_string_pretty(&summary).unwrap()) {
        eprintln!("could not write {}: {}", path.display(), e);
    } else {
        eprintln!("wrote {}", path.display());
    }

    // Soft assertion: single-region fast-path overhead should be
    // within 5% of legacy. Hard-fail at 25% (something's badly
    // wrong).
    assert!(
        regression_pct < 25.0,
        "single-region fast-path overhead {:.1}% exceeds 25%; refactor regressed perf",
        regression_pct
    );
}
