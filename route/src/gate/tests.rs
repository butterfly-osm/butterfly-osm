//! Offline unit checks — the Rust twins of `bench/test_postdeploy_gate.py`:
//! threshold derivation, refs resolution, matrix-plan parsing, geometry
//! helpers, class share, outlier share, the transit-feeds verdict and the
//! registry (no server, no reference set, no environment).

use std::path::Path;

use serde_json::json;

use super::ctx::{
    MATRIX_PLANS, RefsErr, SUBLINEAR_PLANS, Thresholds, outlier_frac, parse_matrix_plan, pct,
    refs_path, require_refs_dir,
};
use super::gates_rest::class_share;
use super::geom::{decode_polyline6, point_in_ring, ring_area2};

// ---- thresholds ----------------------------------------------------------
#[test]
fn bounds_derive_from_tol_and_slack() {
    let t = Thresholds::default();
    assert_eq!(t.band_level, (0.98, 1.09));
    assert_eq!(t.band_regional, (0.98, 1.12));
    assert_eq!(t.dur_p50, t.band_regional);
}

#[test]
fn changing_tol_moves_every_bound() {
    let mut t = Thresholds {
        tol: 0.10,
        ..Default::default()
    };
    t.derive_level_bounds();
    assert_eq!(t.band_level, (0.98, 1.13));
    assert_eq!(t.band_regional, (0.98, 1.16));
    assert_eq!(t.dur_p50, (0.98, 1.16));
}

#[test]
fn asymmetric_never_fast() {
    let t = Thresholds::default();
    // never more than 2 % fast, far more room on the slow side
    assert!(1.0 - t.band_level.0 < t.band_level.1 - 1.0);
    assert_eq!(t.never_fast, 0.98);
}

#[test]
fn windows_json_overrides_and_rederives() {
    let dir = tempfile::tempdir().unwrap();
    std::fs::write(
        dir.path().join("windows.json"),
        r#"{"never_fast": 0.97, "tol": 0.08, "slack_level": 0.02, "slack_regional": 0.05, "match_tol": 0.12}"#,
    )
    .unwrap();
    let mut t = Thresholds::default();
    t.apply_windows(Some(dir.path()));
    assert_eq!(t.never_fast, 0.97);
    assert_eq!(t.band_level, (0.97, 1.1));
    assert_eq!(t.band_regional, (0.97, 1.13));
    assert_eq!(t.like_for_like_km_tol, 0.12);
}

#[test]
fn missing_windows_json_is_not_an_error() {
    let dir = tempfile::tempdir().unwrap();
    let mut t = Thresholds::default();
    t.apply_windows(Some(dir.path()));
    assert_eq!(t.band_level, (0.98, 1.09));
}

// ---- matrix plan -----------------------------------------------------------
#[test]
fn closed_set_parses_verbatim() {
    for p in MATRIX_PLANS {
        assert_eq!(parse_matrix_plan(Some(p)), *p);
    }
}

#[test]
fn sublinear_is_a_strict_subset() {
    assert!(SUBLINEAR_PLANS.iter().all(|p| MATRIX_PLANS.contains(p)));
    assert!(SUBLINEAR_PLANS.len() < MATRIX_PLANS.len());
}

#[test]
fn missing_or_unknown_plan_can_never_pass() {
    assert_eq!(parse_matrix_plan(None), "<missing>");
    assert_eq!(parse_matrix_plan(Some("warp")), "<unknown:warp>");
    for v in ["<missing>", "<unknown:warp>"] {
        assert!(!MATRIX_PLANS.contains(&v));
    }
}

// ---- refs resolution -------------------------------------------------------
#[test]
fn unset_is_the_retired_skip_not_the_operator_error() {
    match require_refs_dir(None) {
        Err(RefsErr::Retired(msg)) => assert!(msg.contains("BUTTERFLY_REFS_DIR unset")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn missing_directory_raises_named_error() {
    match require_refs_dir(Some(Path::new("/nonexistent/refs-dir"))) {
        Err(RefsErr::Unavailable(msg)) => assert!(msg.contains("not a directory")),
        other => panic!("{other:?}"),
    }
}

#[test]
fn override_wins_without_touching_the_env() {
    assert_eq!(
        refs_path(None, "od.csv", Some("/tmp/x.csv")).unwrap(),
        "/tmp/x.csv"
    );
}

#[test]
fn join_under_refs_dir() {
    let dir = tempfile::tempdir().unwrap();
    let p = refs_path(Some(dir.path()), "od.csv", None).unwrap();
    assert_eq!(Path::new(&p), dir.path().join("od.csv"));
}

// ---- geometry --------------------------------------------------------------
#[test]
fn ring_area2_orientation() {
    let ccw = [(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)];
    let cw: Vec<_> = ccw.iter().rev().copied().collect();
    assert!(ring_area2(&ccw) > 0.0);
    assert!(ring_area2(&cw) < 0.0);
}

#[test]
fn point_in_ring_is_closure_tolerant() {
    let open = [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0)];
    let closed = [(0.0, 0.0), (2.0, 0.0), (2.0, 2.0), (0.0, 2.0), (0.0, 0.0)];
    for p in [(1.0, 1.0), (3.0, 1.0), (1.0, 2.5)] {
        assert_eq!(point_in_ring(p, &open), point_in_ring(p, &closed));
    }
}

/// Google's polyline algorithm at 1e6, the inverse of `decode_polyline6`.
fn encode_polyline6(pts: &[(f64, f64)]) -> String {
    let mut out = String::new();
    let (mut plat, mut plon) = (0i64, 0i64);
    for &(lon, lat) in pts {
        let (ilat, ilon) = ((lat * 1e6).round() as i64, (lon * 1e6).round() as i64);
        for d in [ilat - plat, ilon - plon] {
            let mut v = if d < 0 { !(d << 1) } else { d << 1 };
            while v >= 0x20 {
                out.push(((0x20 | (v & 0x1f)) + 63) as u8 as char);
                v >>= 5;
            }
            out.push((v + 63) as u8 as char);
        }
        plat = ilat;
        plon = ilon;
    }
    out
}

#[test]
fn polyline6_roundtrips_a_closed_ring() {
    let ring = [
        (4.35, 50.85),
        (4.36, 50.85),
        (4.36, 50.86),
        (4.35, 50.86),
        (4.35, 50.85),
    ];
    let decoded = decode_polyline6(&encode_polyline6(&ring));
    assert_eq!(decoded.len(), ring.len());
    for (a, b) in decoded.iter().zip(ring) {
        assert!((a.0 - b.0).abs() < 1e-9 && (a.1 - b.1).abs() < 1e-9);
    }
    assert_eq!(decoded.first(), decoded.last());
    assert!(ring_area2(&decoded[..4]) > 0.0);
}

#[test]
fn outlier_share_is_size_independent() {
    let (n, f) = outlier_frac(&[1.0, 1.0, 0.5, 1.5], 0.85, 1.2);
    assert_eq!((n, f), (2, 0.5));
    assert_eq!(outlier_frac(&[], 0.85, 1.2), (0, 0.0));
    assert_eq!(pct(&[3.0, 1.0, 2.0], 0.5), 2.0);
}

// ---- class share -----------------------------------------------------------
#[test]
fn share_by_length_and_token() {
    let d =
        json!({"annotations": {"distance": [100.0, 300.0], "classes": ["motorway", "toll,ferry"]}});
    assert_eq!(class_share(&d, "motorway"), Some(0.25));
    assert_eq!(class_share(&d, "toll"), Some(0.75));
}

#[test]
fn token_not_substring() {
    let d = json!({"annotations": {"distance": [100.0], "classes": ["motorway_link"]}});
    assert_eq!(class_share(&d, "motorway"), Some(0.0));
}

#[test]
fn missing_or_mismatched_is_none() {
    assert_eq!(class_share(&json!({}), "motorway"), None);
    let d = json!({"annotations": {"distance": [100.0, 200.0], "classes": ["motorway"]}});
    assert_eq!(class_share(&d, "motorway"), None);
}
