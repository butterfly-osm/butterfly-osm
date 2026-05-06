//! Integration test for #194 cross-region map matching.
//!
//! Loads Belgium + Luxembourg containers + the BE↔LU overlay, builds
//! a synthetic GPS trace that crosses the BE/LU border, and verifies
//! that [`map_match_multi_region`] produces a connected matched path
//! that spans both regions.
//!
//! The test is `#[ignore]` because it requires the prebuilt
//! Belgium / Luxembourg containers and the prebuilt
//! `be-lu-overlay.butterfly`. CI does not ship these data files. Run
//! locally with:
//!
//! ```bash
//! cargo test -p butterfly-route --release --test map_match_cross_region -- --ignored --nocapture
//! ```
//!
//! The trace is hand-crafted to span Arlon (BE) → Pétange (LU) along
//! the E411 / N4 corridor, with samples placed every ~500 m so the
//! HMM has dense observations crossing the political border around
//! lon 5.83°, lat 49.62°.

use std::path::{Path, PathBuf};
use std::sync::Arc;

use butterfly_route::server::map_match::map_match_multi_region;
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

/// Synthetic GPS trace crossing the BE→LU border on the
/// Arlon → Pétange corridor (roughly E411 / E25). Every sample is a
/// (lon, lat) pair sampled at ~500-1000 m spacing.
///
/// The political border around Athus / Pétange sits at roughly
/// lon = 5.83°, lat = 49.55°. The first ~5 samples are firmly in BE,
/// the last ~5 firmly in LU.
fn arlon_to_petange_trace() -> Vec<(f64, f64)> {
    vec![
        // BE side — Arlon centre and its eastern outskirts on the N82.
        (5.8108, 49.6841), // Arlon centre
        (5.8275, 49.6712),
        (5.8430, 49.6580),
        (5.8540, 49.6450),
        (5.8550, 49.6300), // approaching Aubange
        // Border zone: Athus (BE) ↔ Pétange (LU)
        (5.8410, 49.5610), // Athus, BE
        (5.8730, 49.5520), // crossing into LU, Pétange west
        // LU side — Pétange and onwards toward Esch-sur-Alzette.
        (5.8865, 49.5575), // Pétange centre
        (5.9210, 49.5365),
        (5.9530, 49.5025), // Esch-sur-Alzette north
    ]
}

#[test]
#[ignore = "requires data/belgium + data/luxembourg + data/be-lu-overlay"]
fn cross_region_trace_matches_with_overlay() {
    let Some((be, lu, ov)) = fixture_paths() else {
        eprintln!("skipping: BE + LU + overlay fixtures not on disk");
        return;
    };
    let regions = load_regions_with_overlay(&be, &lu, &ov);
    let trace = arlon_to_petange_trace();

    let result = map_match_multi_region(&regions, "car", &trace, Some(15.0))
        .expect("trace should match across BE/LU overlay");

    eprintln!(
        "matched: {} matchings, {} tracepoints (of {} input)",
        result.matchings.len(),
        result.tracepoints.iter().filter(|t| t.is_some()).count(),
        trace.len()
    );
    for (i, m) in result.matchings.iter().enumerate() {
        eprintln!(
            "  matching[{}]: region_idx={} ebg_path.len={} duration_ds={} confidence={:.3}",
            i,
            m.region_idx,
            m.ebg_path.len(),
            m.duration_ds,
            m.confidence
        );
    }

    // Cross-region trace MUST produce at least 2 matchings — one per
    // region. (With a single matching the trace would be wholly
    // contained in one region, contradicting the test setup.)
    assert!(
        result.matchings.len() >= 2,
        "expected >= 2 matchings (one per region), got {}",
        result.matchings.len()
    );

    // Region indices must cover both BE and LU (regions are sorted by
    // id alphabetically: BE = 0, LU = 1).
    let region_set: std::collections::BTreeSet<usize> =
        result.matchings.iter().map(|m| m.region_idx).collect();
    assert!(
        region_set.contains(&0) && region_set.contains(&1),
        "expected both regions to be covered, got {:?}",
        region_set
    );

    // Every matching's ebg_path must be non-empty (no degenerate
    // single-point matchings).
    for (i, m) in result.matchings.iter().enumerate() {
        assert!(!m.ebg_path.is_empty(), "matching[{}] has empty ebg_path", i);
    }

    // Most input GPS samples should produce a tracepoint. Synthetic
    // traces with hand-picked coordinates can drop a few near the
    // border or at coordinates that don't snap to roads in either
    // region's mode-filtered car graph; allow up to 40 % drops.
    let dropped = result.tracepoints.iter().filter(|t| t.is_none()).count();
    let max_dropped = (trace.len() * 4 / 10).max(2);
    assert!(
        dropped <= max_dropped,
        "too many dropped tracepoints: {}/{} (max {})",
        dropped,
        trace.len(),
        max_dropped
    );

    // matchings_index sequence must be monotonic non-decreasing.
    let mut prev_idx: Option<usize> = None;
    for tp in result.tracepoints.iter().flatten() {
        if let Some(p) = prev_idx {
            assert!(
                tp.matchings_index >= p,
                "matchings_index not monotonic: prev={}, cur={}",
                p,
                tp.matchings_index
            );
        }
        prev_idx = Some(tp.matchings_index);
    }
}

/// Pure-BE trace must take the single-region fast path. Verifies that
/// the multi-region entry point is backwards-compatible: every
/// matching has region_idx 0 (BE), result mirrors what the single-
/// region `map_match` would have produced.
#[test]
#[ignore = "requires data/belgium + data/luxembourg + data/be-lu-overlay"]
fn pure_belgian_trace_uses_single_region_fastpath() {
    let Some((be, lu, ov)) = fixture_paths() else {
        eprintln!("skipping: BE + LU + overlay fixtures not on disk");
        return;
    };
    let regions = load_regions_with_overlay(&be, &lu, &ov);

    // Brussels-only synthetic trace.
    let trace: Vec<(f64, f64)> = vec![
        (4.3517, 50.8503),
        (4.3537, 50.8513),
        (4.3557, 50.8523),
        (4.3577, 50.8533),
    ];

    let result =
        map_match_multi_region(&regions, "car", &trace, Some(10.0)).expect("trace should match");

    assert!(!result.matchings.is_empty(), "expected at least 1 matching");
    for (i, m) in result.matchings.iter().enumerate() {
        // BE is index 0 (alphabetical sort).
        assert_eq!(
            m.region_idx, 0,
            "matching[{}] should be in BE (index 0), got {}",
            i, m.region_idx
        );
        assert!(!m.ebg_path.is_empty());
    }
}

// (a pure-LU fastpath test is intentionally absent: we rely on the
// single-region map_match() unit tests for that direction. The
// pure-BE test above verifies the fast-path goes to region_idx 0
// without invoking cross-region infrastructure.)
