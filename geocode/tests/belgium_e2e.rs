//! End-to-end tests against a Belgium shard.
//!
//! These tests are `#[ignore]` by default because they require:
//!
//! 1. A built shard at `regions/belgium.bfgs` (see
//!    `cargo run --release -p butterfly-geocode -- build-shard ...`)
//! 2. A running PBF extract — the shard build needs `data/belgium.pbf`
//!
//! Run with:
//!
//! ```text
//! cargo test -p butterfly-geocode --release --test belgium_e2e -- --ignored
//! ```

use butterfly_geocode::{CountryId, Shard, execute, parse_heuristic};

const SHARD_PATH: &str = "regions/belgium.bfgs";

fn open_shard() -> Shard {
    Shard::open(SHARD_PATH).unwrap_or_else(|e| {
        panic!(
            "could not open {SHARD_PATH}: {e}\n\
             Build it first: cargo run --release -p butterfly-geocode -- build-shard \\\n\
                            --pbf data/belgium.pbf --out geocode/regions/belgium.bfgs"
        );
    })
}

#[test]
#[ignore]
fn rue_wayez_122_anderlecht_resolves_to_brussels() {
    let shard = open_shard();
    let q = parse_heuristic("Rue Wayez 122 Anderlecht", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(
        !results.is_empty(),
        "expected hits for Rue Wayez 122 Anderlecht"
    );
    let top = &results[0];
    // Anderlecht / Bruxelles area: lat ~50.6-50.8, lon ~4.2-4.4
    assert!(
        (50.5..51.0).contains(&top.lat) && (4.1..4.5).contains(&top.lon),
        "top hit out of Brussels bounds: lat={} lon={}",
        top.lat,
        top.lon
    );
    assert!(top.street.to_lowercase().contains("wayez"));
}

#[test]
#[ignore]
fn postcode_1000_centroid_in_brussels() {
    let shard = open_shard();
    let q = parse_heuristic("1000 Bruxelles", CountryId::BE);
    let results = execute(&q, &shard, 10);
    assert!(!results.is_empty());
    for r in &results {
        assert_eq!(
            r.postcode, "1000",
            "expected postcode=1000, got {}",
            r.postcode
        );
        assert!(
            (50.7..51.0).contains(&r.lat) && (4.2..4.5).contains(&r.lon),
            "postcode 1000 record out of Brussels: lat={} lon={}",
            r.lat,
            r.lon
        );
    }
}

#[test]
#[ignore]
fn grote_markt_antwerpen_at_correct_centroid() {
    let shard = open_shard();
    let q = parse_heuristic("Grote Markt Antwerpen", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(!results.is_empty());
    let top = &results[0];
    // Grote Markt Antwerpen ≈ 51.221, 4.401
    assert!(
        (51.10..51.30).contains(&top.lat) && (4.30..4.50).contains(&top.lon),
        "top hit out of Antwerp center: lat={} lon={}",
        top.lat,
        top.lon
    );
}

#[test]
#[ignore]
fn reverse_brussels_grand_place_returns_grand_place() {
    let shard = open_shard();
    // Brussels Grand-Place: ~50.8467, 4.3525
    let hits = shard.nearest_within(50.8467, 4.3525, 80.0, 5);
    assert!(
        !hits.is_empty(),
        "expected at least one record near Grand-Place"
    );
    let top_street = hits[0].0.street.to_lowercase();
    assert!(
        top_street.contains("grand-place")
            || top_street.contains("grote markt")
            || top_street.contains("place"),
        "expected Grand-Place/Grote Markt, got '{top_street}'"
    );
}

#[test]
#[ignore]
fn empty_query_returns_empty() {
    let shard = open_shard();
    let q = parse_heuristic("", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(results.is_empty());
}

#[test]
#[ignore]
fn fuzzy_misspelling_recovers() {
    let shard = open_shard();
    // Misspelled "Avenue Louise" → should fall back via fuzzy.
    let q = parse_heuristic("Avenue Louse 100", CountryId::BE);
    let results = execute(&q, &shard, 5);
    assert!(
        !results.is_empty(),
        "expected fuzzy fallback to recover Avenue Louise"
    );
    assert!(results.iter().any(|r| {
        r.reason_codes
            .iter()
            .any(|c| c == "STREET_FUZZY" || c == "STREET_EXACT")
    }));
}

#[test]
#[ignore]
fn axum_belgium_real_shard_smoke() {
    // Sanity check that the Axum router still works end-to-end against
    // the full Belgium shard. Same shape as `axum_e2e.rs` but with
    // the real shard.
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use butterfly_geocode::server::{ServerState, build_router};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    let shard = open_shard();
    let state = Arc::new(ServerState::new(shard));
    let app = build_router(state);

    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        let req = Request::builder()
            .uri("/geocode?q=Rue%20Wayez%20122%20Anderlecht&country=BE")
            .body(Body::empty())
            .unwrap();
        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body = std::str::from_utf8(&bytes).unwrap();
        let v: serde_json::Value = serde_json::from_str(body).unwrap();
        assert!(v["count"].as_u64().unwrap() > 0, "no hits: {body}");
    });
}
