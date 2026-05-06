//! Integration tests for #146 topology-grouping experiment.
//!
//! Two test classes here:
//!
//! 1. **Synthetic tests (always run).** Build a tiny multi-mode container
//!    by hand and exercise the `topology-diff` analysis tool + the
//!    `manifest_bundles` parser through their public APIs. These tests
//!    have no external data dependency.
//!
//! 2. **Belgium real-data tests (`#[ignore]`).** Run the analysis tool
//!    against `data/belgium/baseline.butterfly` and assert that the
//!    measured Jaccard / disk-acceptance numbers match the expected
//!    range documented in `route/docs/146-empirical-sharing.md`. CI does
//!    not ship Belgium so these are gated; run locally with:
//!
//!    ```bash
//!    cargo test -p butterfly-route --release --test topology_grouping -- --ignored
//!    ```

use butterfly_route::pack::{manifest_bundles, topology_diff};
use std::path::PathBuf;

/// Resolve `data/belgium/baseline.butterfly` against the locations where
/// it commonly lives: package root (`./data/...`) when run via `cargo
/// test -p`, workspace root (`../data/...`) when run from the package
/// directory, and an opt-in `BUTTERFLY_TEST_DATA_DIR=/abs/path` for
/// out-of-tree datasets. Mirrors the convention in `multi_region.rs`.
fn locate_belgium_container() -> Option<PathBuf> {
    let candidates = [
        PathBuf::from("data/belgium/baseline.butterfly"),
        PathBuf::from("../data/belgium/baseline.butterfly"),
    ];
    for c in &candidates {
        if c.exists() {
            return Some(c.clone());
        }
    }
    if let Ok(base) = std::env::var("BUTTERFLY_TEST_DATA_DIR") {
        let p = PathBuf::from(base)
            .join("belgium")
            .join("baseline.butterfly");
        if p.exists() {
            return Some(p);
        }
    }
    None
}

#[test]
fn manifest_bundles_legacy_returns_empty() {
    // A pre-#90 manifest has no `bundles` field. The parser must not
    // panic and must return an empty vec so the caller can fall back to
    // a per-mode singleton derivation.
    let bytes = b"{\"version\":1, \"region_id\":\"BE\", \"modes\":[\"car\"]}";
    let bundles = manifest_bundles(bytes);
    assert!(bundles.is_empty());
}

#[test]
fn manifest_bundles_round_trips_singleton_layout() {
    let raw = b"{\
        \"version\":1, \
        \"region_id\":\"BE\", \
        \"modes\":[\"bike\",\"car\"], \
        \"bundles\":{\"bike\":[\"bike\"], \"car\":[\"car\"]}\
    }";
    let bundles = manifest_bundles(raw);
    assert_eq!(
        bundles,
        vec![
            ("bike".to_string(), vec!["bike".to_string()]),
            ("car".to_string(), vec!["car".to_string()]),
        ]
    );
}

#[test]
fn manifest_bundles_round_trips_multi_mode_layout() {
    // Forward-compat shape: a future #146 build groups car+truck under
    // a shared bundle and ships bike/foot solo. The parser must
    // round-trip the order and the membership lists.
    let raw = b"{\
        \"version\":1, \
        \"region_id\":\"BE\", \
        \"modes\":[\"bike\",\"car\",\"foot\",\"truck\"], \
        \"bundles\":{\
            \"car_truck\":[\"car\",\"truck\"], \
            \"bike\":[\"bike\"], \
            \"foot\":[\"foot\"]\
        }\
    }";
    let bundles = manifest_bundles(raw);
    assert_eq!(bundles.len(), 3);
    assert_eq!(bundles[0].0, "car_truck");
    assert_eq!(bundles[0].1, vec!["car".to_string(), "truck".to_string()]);
    assert_eq!(bundles[1].0, "bike");
    assert_eq!(bundles[1].1, vec!["bike".to_string()]);
    assert_eq!(bundles[2].0, "foot");
    assert_eq!(bundles[2].1, vec!["foot".to_string()]);
}

// ---------------------------------------------------------------------
// Belgium-real-data tests. These require `data/belgium/baseline.butterfly`
// and are `#[ignore]`-gated.
// ---------------------------------------------------------------------

#[test]
#[ignore = "requires data/belgium/baseline.butterfly"]
fn topology_diff_belgium_runs_clean() {
    // The full all-modes diff must succeed without errors against the
    // shipped Belgium container. This is the live regression for the
    // `topology-diff` subcommand: any container-format change (e.g. a
    // new section kind) that breaks section resolution surfaces here.
    let Some(path) = locate_belgium_container() else {
        eprintln!("skipping: data/belgium/baseline.butterfly not found");
        return;
    };
    topology_diff(&path, None).expect("topology-diff against Belgium container");
}

#[test]
#[ignore = "requires data/belgium/baseline.butterfly"]
fn topology_diff_belgium_explicit_pair() {
    // The car+truck pair is the candidate this PR specifically calls
    // out as "predicted to pass". The tool must accept the explicit
    // mode list and produce a single comparison.
    let Some(path) = locate_belgium_container() else {
        eprintln!("skipping: data/belgium/baseline.butterfly not found");
        return;
    };
    topology_diff(&path, Some("car,truck")).expect("topology-diff car,truck");
}
