//! End-to-end test for the post-libpostal recall+rerank pipeline (#205).
//!
//! Builds a tiny synthetic Belgium shard (5 OpenAddresses gold addresses
//! plus 5 OSM POI samples), emits the recall index sidecars, boots an
//! in-process Axum router, and exercises `/geocode` for top-1 correctness
//! on each gold query.
//!
//! No external data dependencies — runs in <1 s on every CI box.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use butterfly_geocode::server::{ServerState, build_router};
use butterfly_geocode::shard::AddressRecord;
use butterfly_geocode::shard::SourceTag;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use butterfly_geocode::{BuildOptions, CountryId, build_recall_index};
use http_body_util::BodyExt;
use tempfile::TempDir;
use tower::ServiceExt;

fn fixture_addresses() -> Vec<AddressRecord> {
    // Five OpenAddresses-tagged gold records (clean canonical
    // addresses) plus five OSM-tagged POI/place samples (less
    // structured; some house numbers missing). Coordinates are real
    // but the set is intentionally tiny so test outcomes don't churn
    // with upstream OSM updates.
    vec![
        // ---- OpenAddresses gold ----
        AddressRecord {
            street: "Rue Wayez".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.6883,
            lon: 4.3680,
            source: SourceTag::OpenAddresses,
            ..Default::default()
        },
        AddressRecord {
            street: "Rue de la Loi".into(),
            housenumber: "16".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8467,
            lon: 4.3673,
            source: SourceTag::OpenAddresses,
            ..Default::default()
        },
        AddressRecord {
            street: "Avenue Louise".into(),
            housenumber: "100".into(),
            postcode: "1050".into(),
            locality: "Ixelles".into(),
            lat: 50.8323,
            lon: 4.3690,
            source: SourceTag::OpenAddresses,
            ..Default::default()
        },
        AddressRecord {
            street: "Grote Markt".into(),
            housenumber: "1".into(),
            postcode: "2000".into(),
            locality: "Antwerpen".into(),
            lat: 51.2208,
            lon: 4.3997,
            source: SourceTag::OpenAddresses,
            ..Default::default()
        },
        AddressRecord {
            street: "Korenmarkt".into(),
            housenumber: "1".into(),
            postcode: "9000".into(),
            locality: "Gent".into(),
            lat: 51.0537,
            lon: 3.7239,
            source: SourceTag::OpenAddresses,
            ..Default::default()
        },
        // ---- OSM POI / place samples ----
        AddressRecord {
            street: "Grand-Place".into(),
            housenumber: String::new(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8467,
            lon: 4.3525,
            source: SourceTag::Osm,
            ..Default::default()
        },
        AddressRecord {
            street: "Manneken Pis".into(),
            housenumber: String::new(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8450,
            lon: 4.3499,
            source: SourceTag::Osm,
            ..Default::default()
        },
        AddressRecord {
            street: "Atomium".into(),
            housenumber: String::new(),
            postcode: "1020".into(),
            locality: "Laeken".into(),
            lat: 50.8949,
            lon: 4.3415,
            source: SourceTag::Osm,
            ..Default::default()
        },
        AddressRecord {
            street: "Onze-Lieve-Vrouwekathedraal".into(),
            housenumber: String::new(),
            postcode: "2000".into(),
            locality: "Antwerpen".into(),
            lat: 51.2200,
            lon: 4.4006,
            source: SourceTag::Osm,
            ..Default::default()
        },
        AddressRecord {
            street: "Belfort".into(),
            housenumber: String::new(),
            postcode: "9000".into(),
            locality: "Gent".into(),
            lat: 51.0537,
            lon: 3.7253,
            source: SourceTag::Osm,
            ..Default::default()
        },
    ]
}

fn make_app() -> (TempDir, axum::Router) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("be.bfgs");
    build_shard(&path, CountryId::BE, fixture_addresses()).expect("build_shard");
    let shard = Shard::open(&path).expect("open shard");
    build_recall_index(&path, &shard, &BuildOptions::default())
        .expect("build_recall_index emits sibling sidecars");
    let state =
        Arc::new(ServerState::new_with_recall_at(&path).expect("ServerState::new_with_recall_at"));
    let app = build_router(state);
    (dir, app)
}

async fn body_to_string(resp: axum::response::Response) -> String {
    let bytes = resp
        .into_body()
        .collect()
        .await
        .expect("collect body")
        .to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

async fn forward(app: &axum::Router, q: &str) -> serde_json::Value {
    let uri = format!("/geocode?q={}&country=BE", urlencoding(q));
    let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
    let resp = app.clone().oneshot(req).await.expect("router oneshot");
    assert_eq!(resp.status(), StatusCode::OK, "uri={uri}");
    let body = body_to_string(resp).await;
    serde_json::from_str(&body).expect("json")
}

fn urlencoding(s: &str) -> String {
    let mut out = String::with_capacity(s.len() * 2);
    for b in s.as_bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(*b as char);
            }
            b' ' => out.push_str("%20"),
            _ => out.push_str(&format!("%{:02X}", b)),
        }
    }
    out
}

#[tokio::test]
async fn gold_address_full_query_top_1_lands_on_canonical_record() {
    let (_dir, app) = make_app();
    let cases = [
        ("Rue Wayez 122 1070 Anderlecht", 50.6883, 4.3680),
        ("Rue de la Loi 16 1000 Bruxelles", 50.8467, 4.3673),
        ("Avenue Louise 100 1050 Ixelles", 50.8323, 4.3690),
        ("Grote Markt 1 2000 Antwerpen", 51.2208, 4.3997),
        ("Korenmarkt 1 9000 Gent", 51.0537, 3.7239),
    ];
    for (q, lat, lon) in cases {
        let v = forward(&app, q).await;
        let count = v["count"].as_u64().unwrap_or(0);
        assert!(count > 0, "no results for {q}: {v}");
        let top = &v["results"][0];
        let got_lat = top["lat"].as_f64().expect("lat");
        let got_lon = top["lon"].as_f64().expect("lon");
        assert!(
            (got_lat - lat).abs() < 0.01 && (got_lon - lon).abs() < 0.01,
            "top-1 for {} too far from gold: got=({got_lat},{got_lon}) expected=({lat},{lon})\n{v}",
            q,
        );
    }
}

#[tokio::test]
async fn osm_poi_query_finds_poi_record() {
    let (_dir, app) = make_app();
    // OSM POI queries with no house number — recall must still hit
    // the POI key (street alone or place alone).
    for q in ["Grand-Place Bruxelles", "Manneken Pis", "Atomium Laeken"] {
        let v = forward(&app, q).await;
        let count = v["count"].as_u64().unwrap_or(0);
        assert!(count > 0, "no results for OSM POI query {}: {v}", q);
    }
}

#[tokio::test]
async fn fuzzy_partial_query_still_recalls_candidates() {
    // Drop the postcode + house number — the recaller's prefix
    // expansion must still find Anderlecht's Rue Wayez.
    let (_dir, app) = make_app();
    let v = forward(&app, "Rue Wayez Anderlecht").await;
    assert!(
        v["count"].as_u64().unwrap_or(0) > 0,
        "no recall for partial: {v}"
    );
}

#[tokio::test]
async fn nonsense_query_returns_zero_or_low_score() {
    let (_dir, app) = make_app();
    let v = forward(&app, "ZZZZ XXXX 9999 Mordor").await;
    // Either zero results, or top-1 with a low score (the recaller
    // may pick up a fragment but rerank's lexical alignment will be
    // poor). Both are acceptable; an exact-match would be a bug.
    if let Some(count) = v["count"].as_u64()
        && count > 0
    {
        let top = &v["results"][0];
        let score = top["score"].as_f64().unwrap_or(0.0);
        assert!(
            score < 0.95,
            "nonsense query unexpectedly produced near-perfect score: {v}"
        );
    }
}
