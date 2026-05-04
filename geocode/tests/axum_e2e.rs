//! In-process Axum end-to-end tests.
//!
//! These tests boot the actual `build_router` against a small synthetic
//! Belgium shard (10 hand-curated records, written to a tempdir at test
//! start) and exercise the full HTTP surface:
//!
//! - JSON forward / reverse with valid + invalid params
//! - GeoJSON content-negotiation (Accept: application/geo+json) with
//!   Content-Type assertion
//! - Validation error envelopes (400 + JSON `{"error": ...}`)
//! - Country-restriction error
//! - `/metrics` endpoint
//!
//! The tests use [`tower::ServiceExt::oneshot`] so we don't need to
//! bind a real TCP socket. That keeps them fast (<1s for the suite)
//! and parallel-safe.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use butterfly_geocode::server::{ServerState, build_router};
use butterfly_geocode::shard::AddressRecord;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use http_body_util::BodyExt;
use tempfile::TempDir;
use tower::ServiceExt;

fn fixture_addresses() -> Vec<AddressRecord> {
    // Hand-curated Belgium addresses for deterministic e2e testing.
    // Coordinates are real (sourced from OSM as of writing) but the
    // set is intentionally tiny — we want the tests to pass regardless
    // of upstream OSM churn.
    vec![
        // Brussels-Anderlecht: Rue Wayez area
        AddressRecord {
            street: "Rue Wayez".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.6883,
            lon: 4.3680,
            ..Default::default()
        },
        AddressRecord {
            street: "Rue Wayez".into(),
            housenumber: "124".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.6884,
            lon: 4.3681,
            ..Default::default()
        },
        // Brussels-Bruxelles 1000
        AddressRecord {
            street: "Rue de la Loi".into(),
            housenumber: "16".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8467,
            lon: 4.3673,
            ..Default::default()
        },
        // Brussels Grand-Place
        AddressRecord {
            street: "Grand-Place".into(),
            housenumber: "1".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8467,
            lon: 4.3525,
            ..Default::default()
        },
        AddressRecord {
            street: "Grand-Place".into(),
            housenumber: "2".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8468,
            lon: 4.3526,
            ..Default::default()
        },
        // Antwerpen Grote Markt
        AddressRecord {
            street: "Grote Markt".into(),
            housenumber: "1".into(),
            postcode: "2000".into(),
            locality: "Antwerpen".into(),
            lat: 51.2208,
            lon: 4.3997,
            ..Default::default()
        },
        AddressRecord {
            street: "Grote Markt".into(),
            housenumber: "11".into(),
            postcode: "2000".into(),
            locality: "Antwerpen".into(),
            lat: 51.2210,
            lon: 4.3995,
            ..Default::default()
        },
        // Avenue Louise (for fuzzy test)
        AddressRecord {
            street: "Avenue Louise".into(),
            housenumber: "100".into(),
            postcode: "1050".into(),
            locality: "Ixelles".into(),
            lat: 50.8323,
            lon: 4.3690,
            ..Default::default()
        },
        AddressRecord {
            street: "Avenue Louise".into(),
            housenumber: "200".into(),
            postcode: "1050".into(),
            locality: "Ixelles".into(),
            lat: 50.8295,
            lon: 4.3712,
            ..Default::default()
        },
        // Gent Korenmarkt
        AddressRecord {
            street: "Korenmarkt".into(),
            housenumber: "1".into(),
            postcode: "9000".into(),
            locality: "Gent".into(),
            lat: 51.0537,
            lon: 3.7239,
            ..Default::default()
        },
    ]
}

/// Build a fixture shard in a tempdir and return the (TempDir, Shard).
/// The TempDir keeps the shard file alive for the test's lifetime.
fn make_fixture_shard() -> (TempDir, Shard) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fixture.bfgs");
    build_shard(&path, butterfly_geocode::CountryId::BE, fixture_addresses())
        .expect("build fixture shard");
    let s = Shard::open(&path).expect("open fixture shard");
    (dir, s)
}

fn make_app() -> (TempDir, axum::Router) {
    let (dir, shard) = make_fixture_shard();
    let state = Arc::new(ServerState::new(shard));
    let app = build_router(state);
    (dir, app)
}

async fn body_to_string(resp: axum::response::Response) -> String {
    let body = resp.into_body();
    let bytes = body.collect().await.expect("collect body").to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

#[tokio::test]
async fn forward_rue_wayez_returns_anderlecht_record() {
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode?q=Rue%20Wayez%20122%20Anderlecht&country=BE")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_string(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert!(v["count"].as_u64().unwrap() > 0, "no results: {body}");
    let top = &v["results"][0];
    let lat = top["lat"].as_f64().unwrap();
    let lon = top["lon"].as_f64().unwrap();
    // Rue Wayez 122: 50.6883, 4.3680. Allow 50m tolerance.
    let dlat = (lat - 50.6883).abs();
    let dlon = (lon - 4.3680).abs();
    assert!(
        dlat < 0.001 && dlon < 0.001,
        "top hit too far from Rue Wayez 122: lat={lat} lon={lon}"
    );
}

#[tokio::test]
async fn forward_grote_markt_antwerp() {
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode?q=Grote%20Markt%20Antwerpen&country=BE")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_to_string(resp).await).unwrap();
    assert!(v["count"].as_u64().unwrap() > 0);
    let top = &v["results"][0];
    let lat = top["lat"].as_f64().unwrap();
    let lon = top["lon"].as_f64().unwrap();
    // Grote Markt 1, Antwerpen: ~51.22, 4.40. Allow 100m tolerance.
    assert!((lat - 51.22).abs() < 0.005);
    assert!((lon - 4.40).abs() < 0.005);
}

#[tokio::test]
async fn reverse_grand_place_returns_record() {
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode/reverse?lat=50.8467&lon=4.3525&radius_m=200")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let v: serde_json::Value = serde_json::from_str(&body_to_string(resp).await).unwrap();
    assert!(
        v["count"].as_u64().unwrap() >= 1,
        "expected at least one Grand-Place record"
    );
    let top = &v["results"][0];
    assert_eq!(top["street"].as_str().unwrap(), "Grand-Place");
}

#[tokio::test]
async fn forward_empty_q_returns_400_with_envelope() {
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode?q=")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let body = body_to_string(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).expect("error JSON envelope");
    assert!(v["error"].is_string(), "expected `error` field: {body}");
}

#[tokio::test]
async fn forward_invalid_country_iso2_returns_400() {
    // Per #96 serve-the-world: any 2-uppercase-letter input is a
    // valid ISO 3166-1 alpha-2; the BAD_REQUEST path triggers when
    // the input is malformed (length != 2 or non-alphabetic).
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode?q=test&country=GBR")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_str(&body_to_string(resp).await).unwrap();
    assert!(v["error"].is_string());
}

#[tokio::test]
async fn forward_unloaded_country_returns_503() {
    // A valid ISO2 for a country with no loaded shard returns 503
    // (operator misconfiguration, not bad client input). With the
    // BE-only fixture, asking for ZW (Zimbabwe — valid ISO2 but no
    // shard) hits the SERVICE_UNAVAILABLE branch in the handler.
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode?q=test&country=ZW")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::SERVICE_UNAVAILABLE);
    let v: serde_json::Value = serde_json::from_str(&body_to_string(resp).await).unwrap();
    assert!(v["error"].is_string());
}

#[tokio::test]
async fn reverse_invalid_lat_returns_400() {
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode/reverse?lat=99&lon=4")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn forward_geojson_accept_returns_geojson_content_type() {
    // Per C2: when Accept: application/geo+json is sent, the response
    // MUST have Content-Type: application/geo+json (not
    // application/json which is what Axum's Json(...) defaults to).
    let (_dir, app) = make_app();
    let req = Request::builder()
        .uri("/geocode?q=Rue%20Wayez%20122&country=BE")
        .header(header::ACCEPT, "application/geo+json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert!(
        ct.starts_with("application/geo+json"),
        "expected Content-Type: application/geo+json, got: {ct:?}"
    );
    let body = body_to_string(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).unwrap();
    assert_eq!(v["type"].as_str().unwrap(), "FeatureCollection");
}

#[tokio::test]
async fn metrics_endpoint_returns_prometheus() {
    let (_dir, app) = make_app();
    // Hit some endpoint first so the prometheus counters have something.
    let req = Request::builder()
        .uri("/health")
        .body(Body::empty())
        .unwrap();
    let _ = app.clone().oneshot(req).await.unwrap();

    let req = Request::builder()
        .uri("/metrics")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_string(resp).await;
    // Prometheus exposition format starts with `#` (HELP/TYPE comments)
    // or with a metric name.
    assert!(
        body.contains("# HELP") || body.contains("# TYPE") || body.contains("axum_http"),
        "metrics body looks wrong: {body:?}"
    );
}

#[tokio::test]
async fn forward_q_too_long_returns_400() {
    // Per C3: the limit is in CHARACTERS, not bytes. 600 ASCII chars
    // is over 512 chars and should reject.
    let (_dir, app) = make_app();
    let q = "a".repeat(600);
    let uri = format!("/geocode?q={q}&country=BE");
    let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    let v: serde_json::Value = serde_json::from_str(&body_to_string(resp).await).unwrap();
    assert!(v["error"].as_str().unwrap().contains("too long"));
}
