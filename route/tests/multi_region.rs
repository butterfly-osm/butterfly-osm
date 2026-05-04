//! Integration tests for #91 multi-region container loading + dispatch.
//!
//! These tests are `#[ignore]` because they require the prebuilt
//! Belgium and Luxembourg `.butterfly` containers; CI does not ship
//! either dataset. Run them locally with:
//!
//! ```bash
//! cargo test -p butterfly-route --release --test multi_region -- --ignored
//! ```
//!
//! Test inventory (per #91 spec):
//! - region discovery + loading from a directory of containers
//! - snap dispatch picks the correct region for points in BE vs LU
//! - BE → BE same-region routing works
//! - LU → LU same-region routing works
//! - BE → LU returns 501 with the helpful "spans regions" error
//! - malformed `--data-dir` (no `*.butterfly`) yields a clean error
//!
//! Test data: this repo does not ship Belgium or Luxembourg PBF/
//! container output; the tests look for them under `data/<region>/`
//! and silently skip if absent. The build script in #91 docs walks
//! through producing both containers.

use std::path::Path;

use butterfly_route::pack::{self, DEFAULT_REGION_ID, normalize_region_id};
use butterfly_route::server::regions::{DispatchError, RegionsState};

const BE_CONTAINER: &str = "data/belgium/baseline.butterfly";
const LU_CONTAINER: &str = "data/luxembourg/luxembourg.butterfly";

fn container_paths() -> Option<(std::path::PathBuf, std::path::PathBuf)> {
    // Tests run with CWD set to the package root (`route/`) by cargo,
    // but the data lives at the workspace root one level up. Probe a
    // few common locations: the repo root via `../`, the package root
    // (in case the data is symlinked in), and an absolute fallback.
    let candidates: &[(&str, &str)] = &[
        (
            "../data/belgium/baseline.butterfly",
            "../data/luxembourg/luxembourg.butterfly",
        ),
        (BE_CONTAINER, LU_CONTAINER),
        (
            "/home/snape/projects/butterfly-osm/data/belgium/baseline.butterfly",
            "/home/snape/projects/butterfly-osm/data/luxembourg/luxembourg.butterfly",
        ),
    ];
    for (be, lu) in candidates {
        let be_p = Path::new(be);
        let lu_p = Path::new(lu);
        if be_p.exists() && lu_p.exists() {
            return Some((
                be_p.canonicalize().unwrap_or_else(|_| be_p.to_path_buf()),
                lu_p.canonicalize().unwrap_or_else(|_| lu_p.to_path_buf()),
            ));
        }
    }
    None
}

/// Stage a temp directory with symlinks to the BE + LU containers,
/// preserving their disk-resident copies (the test does not copy
/// gigabytes around).
fn stage_dir(be: &Path, lu: &Path) -> tempfile::TempDir {
    let dir = tempfile::tempdir().expect("tempdir");
    let be_dst = dir.path().join("be.butterfly");
    let lu_dst = dir.path().join("lu.butterfly");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(be, &be_dst).expect("symlink BE");
        std::os::unix::fs::symlink(lu, &lu_dst).expect("symlink LU");
    }
    #[cfg(not(unix))]
    {
        std::fs::copy(be, &be_dst).expect("copy BE");
        std::fs::copy(lu, &lu_dst).expect("copy LU");
    }
    dir
}

// =============================================================================
// Pure unit tests (no container required)
// =============================================================================

#[test]
fn region_id_normalises_lowercase_to_uppercase() {
    assert_eq!(normalize_region_id("be").unwrap(), "BE");
    assert_eq!(normalize_region_id(" lu ").unwrap(), "LU");
    assert_eq!(normalize_region_id("FR").unwrap(), "FR");
    assert_eq!(normalize_region_id("Region-1").unwrap(), "REGION-1");
}

#[test]
fn region_id_rejects_invalid_chars() {
    assert!(normalize_region_id("BE/LU").is_err());
    assert!(normalize_region_id("BE LU").is_err());
    assert!(normalize_region_id("be.lu").is_err());
    assert!(normalize_region_id("").is_err());
    assert!(normalize_region_id("    ").is_err());
}

#[test]
fn region_id_caps_at_16_chars() {
    let ok = "A".repeat(16);
    let bad = "A".repeat(17);
    assert!(normalize_region_id(&ok).is_ok());
    assert!(normalize_region_id(&bad).is_err());
}

#[test]
fn manifest_region_id_returns_default_for_missing() {
    let s = pack::manifest_region_id(b"{\"version\": 1, \"modes\": []}");
    assert_eq!(s, DEFAULT_REGION_ID);
}

#[test]
fn manifest_region_id_parses_explicit_field() {
    let s = pack::manifest_region_id(b"{\"version\": 1, \"region_id\": \"LU\", \"modes\": []}");
    assert_eq!(s, "LU");
}

#[test]
fn manifest_region_id_normalises_case() {
    let s = pack::manifest_region_id(b"{\"version\": 1, \"region_id\": \"lu\", \"modes\": []}");
    assert_eq!(s, "LU");
}

#[test]
fn manifest_region_id_handles_garbage_fallsback() {
    // Truncated / malformed JSON: don't reject the container, just
    // fall back to the default region id.
    let s = pack::manifest_region_id(b"this is not json");
    assert_eq!(s, DEFAULT_REGION_ID);
}

// =============================================================================
// Discovery + dispatch tests (require BE + LU containers on disk)
// =============================================================================

/// Two regions discovered from a directory of `*.butterfly` files.
/// Verifies sorting (BE before LU) and that both are present.
#[test]
#[ignore = "requires data/belgium + data/luxembourg containers"]
fn loads_two_regions_from_directory() {
    let Some((be, lu)) = container_paths() else {
        eprintln!("skipping: BE + LU containers not on disk");
        return;
    };
    let dir = stage_dir(&be, &lu);
    let regions = RegionsState::load_from_dir(dir.path(), None, None).expect("load_from_dir");
    assert_eq!(
        regions.len(),
        2,
        "expected 2 regions, got {}",
        regions.len()
    );
    let ids = regions.region_ids();
    assert!(ids.contains(&"BE".to_string()), "missing BE in {:?}", ids);
    assert!(ids.contains(&"LU".to_string()), "missing LU in {:?}", ids);
}

/// `--regions BE` filter loads only the matching container.
#[test]
#[ignore = "requires data/belgium + data/luxembourg containers"]
fn region_filter_skips_unrequested_containers() {
    let Some((be, lu)) = container_paths() else {
        eprintln!("skipping: BE + LU containers not on disk");
        return;
    };
    let dir = stage_dir(&be, &lu);
    let regions = RegionsState::load_from_dir(dir.path(), Some(&["BE".to_string()]), None)
        .expect("load_from_dir BE-only");
    assert_eq!(regions.len(), 1);
    assert_eq!(regions.region_ids(), vec!["BE".to_string()]);
}

/// Snapping a Brussels coordinate must dispatch to BE; Luxembourg
/// city to LU. Verifies the multi-region snap-winner logic.
#[test]
#[ignore = "requires data/belgium + data/luxembourg containers"]
fn dispatcher_picks_right_region_for_known_points() {
    let Some((be, lu)) = container_paths() else {
        eprintln!("skipping: BE + LU containers not on disk");
        return;
    };
    let dir = stage_dir(&be, &lu);
    let regions = RegionsState::load_from_dir(dir.path(), None, None).expect("load_from_dir");

    // Brussels-Centraal → BE.
    let (state, region_id) = regions
        .dispatch_single_id(4.3567, 50.8453, "car")
        .expect("Brussels should snap into BE");
    assert_eq!(region_id, "BE", "Brussels should snap to BE");
    assert!(state.mode_lookup.contains_key("car"));

    // Luxembourg-Ville → LU.
    let (state, region_id) = regions
        .dispatch_single_id(6.1296, 49.6116, "car")
        .expect("Luxembourg-Ville should snap into LU");
    assert_eq!(region_id, "LU", "Luxembourg-Ville should snap to LU");
    assert!(state.mode_lookup.contains_key("car"));
}

/// BE → BE p2p dispatch returns the BE state without 501.
#[test]
#[ignore = "requires data/belgium + data/luxembourg containers"]
fn p2p_dispatch_same_region_be_to_be() {
    let Some((be, lu)) = container_paths() else {
        eprintln!("skipping: BE + LU containers not on disk");
        return;
    };
    let dir = stage_dir(&be, &lu);
    let regions = RegionsState::load_from_dir(dir.path(), None, None).expect("load_from_dir");

    // Brussels-Centraal → Bruges, both in BE.
    let (_state, region_id) = regions
        .dispatch_p2p_id(4.3567, 50.8453, 3.2247, 51.2093, "car")
        .expect("Brussels → Bruges should not 501");
    assert_eq!(region_id, "BE");
}

/// LU → LU p2p dispatch returns the LU state without 501.
#[test]
#[ignore = "requires data/belgium + data/luxembourg containers"]
fn p2p_dispatch_same_region_lu_to_lu() {
    let Some((be, lu)) = container_paths() else {
        eprintln!("skipping: BE + LU containers not on disk");
        return;
    };
    let dir = stage_dir(&be, &lu);
    let regions = RegionsState::load_from_dir(dir.path(), None, None).expect("load_from_dir");

    // Luxembourg-Ville → Esch-sur-Alzette, both in LU.
    let (_state, region_id) = regions
        .dispatch_p2p_id(6.1296, 49.6116, 5.9806, 49.4955, "car")
        .expect("LU-City → Esch should not 501");
    assert_eq!(region_id, "LU");
}

/// BE → LU is the **correctness invariant**: must return 501 with a
/// clear "spans regions" error. No silent wrong answer, no panic.
#[test]
#[ignore = "requires data/belgium + data/luxembourg containers"]
fn p2p_dispatch_cross_region_returns_501() {
    let Some((be, lu)) = container_paths() else {
        eprintln!("skipping: BE + LU containers not on disk");
        return;
    };
    let dir = stage_dir(&be, &lu);
    let regions = RegionsState::load_from_dir(dir.path(), None, None).expect("load_from_dir");

    // Brussels (BE) → Luxembourg-Ville (LU) — these clearly snap into
    // different regions.
    let err = match regions.dispatch_p2p_id(4.3567, 50.8453, 6.1296, 49.6116, "car") {
        Ok(_) => panic!("expected CrossRegion error, got Ok"),
        Err(e) => e,
    };
    match err {
        DispatchError::CrossRegion {
            ref src_region,
            ref dst_region,
        } => {
            assert_eq!(src_region, "BE");
            assert_eq!(dst_region, "LU");
        }
        other => panic!("expected CrossRegion, got {:?}", other),
    }
    // The HTTP rendering is 501.
    let (code, body) = err.into_response_parts();
    assert_eq!(code, axum::http::StatusCode::NOT_IMPLEMENTED);
    assert!(body.error.contains("BE"));
    assert!(body.error.contains("LU"));
    assert!(body.error.contains("#91"));
}

/// Empty `--data-dir` (no `*.butterfly`) is a hard error, not a
/// silent zero-region server.
#[test]
fn empty_data_dir_is_rejected() {
    let dir = tempfile::tempdir().expect("tempdir");
    let res = RegionsState::load_from_dir(dir.path(), None, None);
    assert!(res.is_err());
    let msg = res.err().expect("expected error").to_string();
    assert!(
        msg.contains("no *.butterfly"),
        "expected 'no *.butterfly' diagnostic, got: {}",
        msg
    );
}

/// `--data-dir` that points at a non-directory (e.g. a file) is
/// rejected with a path-aware error.
#[test]
fn non_directory_data_dir_is_rejected() {
    let f = tempfile::NamedTempFile::new().expect("tempfile");
    let res = RegionsState::load_from_dir(f.path(), None, None);
    assert!(res.is_err());
    let msg = res.err().expect("expected error").to_string();
    assert!(
        msg.contains("expected --data-dir to be a directory"),
        "expected 'directory' diagnostic, got: {}",
        msg
    );
}

/// `--regions` filter that excludes every container is a hard error.
/// Operator typo'd a region id; better to fail loudly than to start a
/// zero-region server.
#[test]
#[ignore = "requires data/belgium + data/luxembourg containers"]
fn region_filter_excluding_everything_is_rejected() {
    let Some((be, lu)) = container_paths() else {
        eprintln!("skipping: BE + LU containers not on disk");
        return;
    };
    let dir = stage_dir(&be, &lu);
    let res = RegionsState::load_from_dir(dir.path(), Some(&["FR".to_string()]), None);
    assert!(res.is_err());
    let msg = res.err().expect("expected error").to_string();
    assert!(msg.contains("no containers"), "got: {}", msg);
}

/// Synthetic two-region test that doesn't need real PBF data: build a
/// fake LU container by reusing the BE container under a different
/// region tag. Exercises the loader's dispatch table and the duplicate-
/// region-id rejection path. Skipped if BE container is absent.
#[test]
#[ignore = "requires data/belgium container"]
fn duplicate_region_id_is_rejected() {
    let Some((be, _)) = container_paths() else {
        eprintln!("skipping: BE container not on disk");
        return;
    };
    let dir = tempfile::tempdir().expect("tempdir");
    let dst1 = dir.path().join("be1.butterfly");
    let dst2 = dir.path().join("be2.butterfly");
    #[cfg(unix)]
    {
        std::os::unix::fs::symlink(&be, &dst1).expect("symlink 1");
        std::os::unix::fs::symlink(&be, &dst2).expect("symlink 2");
    }
    #[cfg(not(unix))]
    {
        std::fs::copy(&be, &dst1).expect("copy 1");
        std::fs::copy(&be, &dst2).expect("copy 2");
    }
    let res = RegionsState::load_from_dir(dir.path(), None, None);
    assert!(res.is_err());
    let msg = res.err().expect("expected error").to_string();
    assert!(
        msg.contains("duplicate region id"),
        "expected duplicate-region diagnostic, got: {}",
        msg
    );
}
