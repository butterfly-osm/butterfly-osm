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
use axum::routing::get;
use axum_prometheus::PrometheusMetricLayer;
use tower::ServiceBuilder;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::CorsLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

pub use state::ServerState;

#[must_use]
pub fn build_router(state: Arc<ServerState>) -> Router {
    let (prometheus_layer, metric_handle) = PrometheusMetricLayer::pair();

    Router::new()
        .route("/geocode", get(handlers::forward))
        .route("/geocode/reverse", get(handlers::reverse))
        .route("/health", get(handlers::health))
        .route(
            "/metrics",
            get(move || {
                let h = metric_handle.clone();
                async move { h.render() }
            }),
        )
        .layer(
            ServiceBuilder::new()
                .layer(CatchPanicLayer::new())
                .layer(TraceLayer::new_for_http())
                .layer(TimeoutLayer::new(Duration::from_secs(30)))
                .layer(CorsLayer::permissive())
                .layer(CompressionLayer::new())
                .layer(prometheus_layer),
        )
        .with_state(state)
}
