//! Integration tests for #146 topology-grouping experiment.
//!
//! Two test classes here:
//!
//! 1. **Synthetic tests (always run).** Build a tiny multi-mode container
//!    by hand and exercise the `topology-diff` analysis tool + the
//!    `manifest_bundles` parser through their public APIs. These tests
//!    have no external data dependency.
//!
//! 2. **Belgium real-data tests (self-skipping, #587).** Run the analysis tool
//!    against `data/belgium/baseline.butterfly` and assert that the
//!    measured Jaccard / disk-acceptance numbers match the expected
//!    range documented in `route/docs/146-empirical-sharing.md`. CI does
//!    not ship Belgium, so they skip themselves there; run locally with:
//!
//!    ```bash
//!    BUTTERFLY_TEST_DATA_DIR=/path/to/data cargo test -p butterfly-route \
//!        --release --test topology_grouping
//!    ```

use butterfly_route::formats::butterfly_dat::Container;
use butterfly_route::pack::{manifest_bundles, topology_diff};
use butterfly_route::testutil;
use std::path::PathBuf;

/// Locate the Belgium container through the shared #587 data probe,
/// printing one skip line for this file when it is absent.
fn locate_belgium_container() -> Option<PathBuf> {
    testutil::require_container("topology_grouping")
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
// and self-skip without the container.
// ---------------------------------------------------------------------

#[test]
fn topology_diff_belgium_runs_clean() {
    // The full all-modes diff must succeed without errors against the
    // shipped Belgium container. This is the live regression for the
    // `topology-diff` subcommand: any container-format change (e.g. a
    // new section kind) that breaks section resolution surfaces here.
    let Some(path) = locate_belgium_container() else {
        return;
    };
    topology_diff(&path, None).expect("topology-diff against Belgium container");
}

/// The two modes the explicit-pair comparison runs on.
///
/// #595: this was `car,truck`. Truck is a DEFERRED profile (P5) and no shipped
/// container has ever carried a `mode/truck/topo` section, so the test could
/// not have passed on any real artifact — it only ever looked green because it
/// was `#[ignore]`d. `car` + `car_nodir` (car ignoring one-ways) inherits the
/// role truck was standing in for: the CLOSEST-topology pair the engine ships,
/// and therefore the strongest bundling candidate a `topology-diff` reader
/// would actually evaluate.
const EXPLICIT_PAIR: [&str; 2] = ["car", "car_nodir"];

#[test]
fn topology_diff_belgium_explicit_pair() {
    // The tool must accept an explicit mode list and produce a single
    // comparison for it.
    let Some(path) = locate_belgium_container() else {
        return;
    };
    if path.is_file() {
        let container = Container::open(&path).expect("open container");
        let modes = container.list_modes();
        for want in EXPLICIT_PAIR {
            if !modes.iter().any(|m| m == want) {
                let _: Option<()> = testutil::skip(
                    "topology_grouping::explicit_pair",
                    &format!("a container carrying the '{want}' mode (has {modes:?})"),
                );
                return;
            }
        }
    }
    let pair = EXPLICIT_PAIR.join(",");
    topology_diff(&path, Some(&pair)).unwrap_or_else(|e| panic!("topology-diff {pair}: {e}"));
}
