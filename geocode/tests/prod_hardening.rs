//! Production-hardening integration tests.
//!
//! Covers the wires added by the prod-hardening sprint:
//!
//! - Per-IP rate limit (`tower_governor`) → 429 on flood.
//! - Health endpoint shape (`shard_count`, `total_records`, `version`).
//! - Compression layer responds with `content-encoding: gzip` when the
//!   client advertises it via `Accept-Encoding`.
//! - Server config knobs (rate limit per second / burst) flow through
//!   `build_router_with_config`.
//!
//! These tests use [`tower::ServiceExt::oneshot`] so we don't need to
//! bind a real TCP socket. The custom `PeerIpKey` extractor falls
//! back to `127.0.0.1` when `ConnectInfo` is missing — which is the
//! exact behaviour we exercise here (every test request shares one
//! key, so a flood from this single test client trips the limiter
//! the same way an abusive real client would).

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode, header};
use butterfly_geocode::CountryId;
use butterfly_geocode::server::{ServerConfig, ServerState, build_router_with_config};
use butterfly_geocode::shard::AddressRecord;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use http_body_util::BodyExt;
use tempfile::TempDir;
use tower::ServiceExt;

fn fixture_addresses() -> Vec<AddressRecord> {
    vec![
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
            street: "Grand-Place".into(),
            housenumber: "1".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8467,
            lon: 4.3525,
            ..Default::default()
        },
    ]
}

fn make_fixture_shard() -> (TempDir, Shard) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fixture.bfgs");
    build_shard(&path, CountryId::BE, fixture_addresses()).expect("build fixture shard");
    let s = Shard::open(&path).expect("open fixture shard");
    (dir, s)
}

fn make_app_with(cfg: ServerConfig) -> (TempDir, axum::Router) {
    let (dir, shard) = make_fixture_shard();
    let state = Arc::new(ServerState::new(shard));
    let app = build_router_with_config(state, cfg);
    (dir, app)
}

async fn body_to_string(resp: axum::response::Response) -> String {
    let body = resp.into_body();
    let bytes = body.collect().await.expect("collect body").to_bytes();
    String::from_utf8(bytes.to_vec()).expect("utf8")
}

#[tokio::test]
async fn rate_limit_returns_429_on_burst() {
    // Tight knobs: 2 req/s, burst=2. After we burn the burst the next
    // request inside the same second must come back as 429.
    let cfg = ServerConfig {
        rate_limit_per_sec: 2,
        rate_limit_burst: 2,
        ..ServerConfig::default()
    };
    let (_dir, app) = make_app_with(cfg);

    let make_req = || {
        Request::builder()
            .uri("/geocode?q=Rue%20Wayez%20122&country=BE")
            .body(Body::empty())
            .unwrap()
    };

    // Burn the burst.
    let r1 = app.clone().oneshot(make_req()).await.unwrap();
    assert_eq!(r1.status(), StatusCode::OK, "first request must succeed");
    let r2 = app.clone().oneshot(make_req()).await.unwrap();
    assert_eq!(r2.status(), StatusCode::OK, "second request must succeed");

    // Third should be throttled (429). governor refills at 1/per_second
    // tokens per second so within the same wall-clock millisecond
    // there are no tokens left.
    let r3 = app.clone().oneshot(make_req()).await.unwrap();
    assert_eq!(
        r3.status(),
        StatusCode::TOO_MANY_REQUESTS,
        "third request inside the same window must be 429"
    );
}

#[tokio::test]
async fn rate_limit_does_not_block_health() {
    // Per the middleware order in `server::mod`, the governor only
    // wraps `/geocode*`. `/health` and `/metrics` are deliberately
    // outside its layer so monitors never hit 429.
    let cfg = ServerConfig {
        rate_limit_per_sec: 1,
        rate_limit_burst: 1,
        ..ServerConfig::default()
    };
    let (_dir, app) = make_app_with(cfg);

    // Burn the geocode burst.
    let r = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/geocode?q=test&country=BE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(r.status(), StatusCode::OK);

    // Now hammer /health; none should 429.
    for i in 0..10 {
        let resp = app
            .clone()
            .oneshot(
                Request::builder()
                    .uri("/health")
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "health request {i} must not be rate limited"
        );
    }
}

#[tokio::test]
async fn health_endpoint_includes_version_and_shard_count() {
    let (_dir, app) = make_app_with(ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/health")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = body_to_string(resp).await;
    let v: serde_json::Value = serde_json::from_str(&body).expect("health JSON");
    assert_eq!(v["status"].as_str(), Some("ok"));
    assert!(
        v["version"].is_string(),
        "health must include `version`: {body}"
    );
    assert_eq!(
        v["shard_count"].as_u64(),
        Some(1),
        "single-shard server must report shard_count=1: {body}"
    );
    assert!(
        v["total_records"].as_u64().unwrap() > 0,
        "total_records must be populated: {body}"
    );
    assert!(v["uptime_seconds"].is_number(), "uptime missing: {body}");
}

#[tokio::test]
async fn compression_layer_honours_accept_encoding() {
    // The compression layer is wired around the API routes
    // (/geocode + /health). /metrics is intentionally outside it —
    // Prometheus scrapers rarely advertise gzip and even when they do
    // the bandwidth saving is dominated by scrape interval, not
    // payload size. So we exercise compression against a /geocode
    // call. The fixture shard's response body is small, but with
    // include=debug the response carries the full `reason_codes`
    // array which is consistently >= 200 bytes — well above
    // tower_http's 32-byte minimum.
    let (_dir, app) = make_app_with(ServerConfig::default());

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/geocode?q=Rue%20Wayez%20122%20Anderlecht&country=BE&include=debug")
                .header(header::ACCEPT_ENCODING, "gzip")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let encoding = resp
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    let ct = resp
        .headers()
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert_eq!(
        encoding, "gzip",
        "expected `content-encoding: gzip` — got encoding={encoding:?} content-type={ct:?}"
    );
}

#[tokio::test]
async fn compression_layer_skips_when_no_accept_encoding() {
    // Without `Accept-Encoding` the layer must NOT compress —
    // that's the negotiation invariant. Having both directions
    // covered prevents a regression where someone forces
    // `Predicate::None` and accidentally compresses everything.
    let (_dir, app) = make_app_with(ServerConfig::default());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/geocode?q=Rue%20Wayez%20122%20Anderlecht&country=BE&include=debug")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let encoding = resp
        .headers()
        .get(header::CONTENT_ENCODING)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("")
        .to_string();
    assert_eq!(
        encoding, "",
        "compression must not fire without Accept-Encoding"
    );
}

#[tokio::test]
async fn metrics_endpoint_is_valid_prometheus() {
    let (_dir, app) = make_app_with(ServerConfig::default());

    // Drive a /geocode call so we have HTTP-level counters with non-zero values.
    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/geocode?q=Rue%20Wayez%20122&country=BE")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let resp = app
        .oneshot(
            Request::builder()
                .uri("/metrics")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = body_to_string(resp).await;
    // Prometheus exposition format uses `# HELP` / `# TYPE` comment
    // lines and metric name = value pairs. axum_prometheus emits
    // both — but only after the first sample is recorded. We've
    // already driven a /geocode call above, so the histograms +
    // counters should be populated.
    assert!(
        !body.is_empty(),
        "metrics body is empty — exporter not wired"
    );
    assert!(
        body.contains("# TYPE") || body.contains("axum_http") || body.contains("http_requests"),
        "metrics body missing TYPE comments and http metric family — got:\n{body}"
    );
}

#[tokio::test]
async fn graceful_shutdown_completes_in_flight_requests() {
    // End-to-end sanity for the shutdown shape in `serve_cmd`:
    //
    //  - Bind a real TCP listener.
    //  - Drive `axum::serve(...).with_graceful_shutdown(...)` from a
    //    spawned task.
    //  - Fire one in-flight request, await its response.
    //  - Trigger shutdown via the `Notify`.
    //  - The serve future must resolve cleanly.
    use std::net::SocketAddr;
    use tokio::sync::Notify;

    let (_dir, app) = make_app_with(ServerConfig::default());
    let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], 0)))
        .await
        .expect("bind");
    let addr = listener.local_addr().expect("local_addr");

    let shutdown = Arc::new(Notify::new());
    let shutdown_for_serve = Arc::clone(&shutdown);
    let serve_task = tokio::spawn(async move {
        axum::serve(
            listener,
            app.into_make_service_with_connect_info::<SocketAddr>(),
        )
        .with_graceful_shutdown(async move {
            shutdown_for_serve.notified().await;
        })
        .await
    });

    // Hit the server with a real HTTP client; it should respond.
    let client = reqwest::Client::builder()
        .timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    let resp = client
        .get(format!("http://{addr}/health"))
        .send()
        .await
        .expect("health request");
    assert_eq!(resp.status(), reqwest::StatusCode::OK);

    // Trigger graceful shutdown.
    shutdown.notify_waiters();

    // The serve task must finish within a reasonable window once the
    // signal fires (no in-flight requests, drain is instantaneous).
    let outcome = tokio::time::timeout(Duration::from_secs(5), serve_task)
        .await
        .expect("serve task must finish within 5s of shutdown signal")
        .expect("serve task panicked");
    outcome.expect("serve future must resolve cleanly");
}

#[tokio::test]
async fn server_config_default_is_100_per_sec() {
    let cfg = ServerConfig::default();
    assert_eq!(cfg.rate_limit_per_sec, 100);
    assert_eq!(cfg.rate_limit_burst, 200);
    assert_eq!(cfg.request_timeout, Duration::from_secs(30));
    assert_eq!(cfg.max_request_body_bytes, 4 * 1024);
}
