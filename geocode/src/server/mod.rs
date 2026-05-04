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

use std::sync::Arc;
use std::time::Duration;

use axum::Router;
use axum::http::StatusCode;
use axum::routing::get;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub use state::ServerState;

pub fn build_router(state: Arc<ServerState>) -> Router {
    let (prometheus_layer, metric_handle) = axum_prometheus::PrometheusMetricLayer::pair();

    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    let api = Router::new()
        .route("/geocode", get(handlers::forward))
        .route("/geocode/reverse", get(handlers::reverse))
        .route("/health", get(handlers::health))
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
        .with_state(state)
}
