//! HTTP server for forward + reverse geocoding.
//!
//! Endpoints (REST, JSON default, GeoJSON via `Accept` header):
//!
//! - `GET /geocode?q=...&country=BE&limit=N` — forward
//! - `GET /geocode/reverse?lat=...&lon=...&radius_m=...&limit=N` — reverse
//! - `GET /health` — uptime + record count
//! - `GET /metrics` — Prometheus
//!
//! Content negotiation per the project's standing API design preference
//! (CLAUDE.md memory: "User strongly prefers content negotiation
//! via Accept header over separate endpoints"). No `/format` variants.

pub mod handlers;
pub mod state;

use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use axum_prometheus::PrometheusMetricLayer;
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::control::admission::mk_admission_layer;

pub use state::ServerState;

/// Build the prometheus layer + handle exactly once per process.
/// `PrometheusMetricLayer::pair()` calls into the `metrics` crate's
/// global recorder, which can only be set once. The first call wins;
/// subsequent calls (e.g. from tests that build multiple routers in
/// the same process) reuse the cached pair.
fn prometheus_pair() -> &'static (PrometheusMetricLayer<'static>, PrometheusHandle) {
    static PAIR: OnceLock<(PrometheusMetricLayer<'static>, PrometheusHandle)> = OnceLock::new();
    PAIR.get_or_init(PrometheusMetricLayer::pair)
}

pub fn build_router(state: Arc<ServerState>) -> Router {
    let (prometheus_layer, metric_handle) = prometheus_pair().clone();

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Admission control wraps the geocode endpoints only — health
    // and metrics are intentionally excluded so monitors and probes
    // are not rate-limited (#97 §4 standard practice).
    let geocode_routes = Router::new()
        .route("/geocode", get(handlers::forward))
        .route("/geocode/reverse", get(handlers::reverse))
        .with_state(state.clone());
    let geocode_routes = mk_admission_layer(geocode_routes, state.admission.clone());

    let unauth = Router::new()
        .route("/health", get(handlers::health))
        .with_state(state.clone());

    let api = geocode_routes
        .merge(unauth)
        .layer(CompressionLayer::new())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(30),
        ));

    Router::new()
        .merge(api)
        .route(
            "/metrics",
            get(move || {
                let h = metric_handle.clone();
                async move { h.render() }
            }),
        )
        .layer(CatchPanicLayer::new())
        .layer(prometheus_layer)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
}
