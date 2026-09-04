//! HTTP API router and OpenAPI documentation
//!
//! All handler logic lives in sibling modules (route, nearest, table, etc.).
//! This module assembles the Axum router and OpenAPI spec.

use axum::{
    Router,
    extract::DefaultBodyLimit,
    http::StatusCode,
    routing::{get, post},
};
use std::sync::Arc;
use std::time::Duration;
use tower::limit::ConcurrencyLimitLayer;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

use super::geometry::Point;
use super::regions::RegionsState;

// Re-export public items so that existing `super::api::` paths still work
pub use super::isochrone_handler::{run_phast_bounded_fast, run_phast_bounded_fast_reverse};
pub use super::types::{ErrorResponse, Waypoint, parse_mode, validate_coord};

/// OpenAPI documentation
#[derive(OpenApi)]
#[openapi(
    paths(
        super::route::route_handler,
        super::table::table_post_handler,
        super::isochrone_handler::isochrone_handler,
        super::isochrone_handler::isochrone_bulk_handler,
        super::nearest::nearest_handler,
        super::matching::match_trace_handler,
        super::trip::trip_handler,
        super::height_handler::height_handler,
        super::health_handler::health_handler,
        super::regions_handler::regions_handler,
        super::catchment::catchment_handler,
        super::transit_handler::transit_handler,
        super::transit_handler::transit_bulk_handler,
        version_handler,
    ),
    components(schemas(
        super::route::RouteRequest,
        super::route::RouteResponse,
        super::route::RouteAnnotations,
        super::route::RouteAlternative,
        super::route::SnapInfo,
        super::route::RouteDebugInfo,
        super::route::RouteStep,
        super::route::StepManeuver,
        super::table::TablePostRequest,
        super::table::TableResponse,
        super::isochrone_handler::BulkIsochroneRequest,
        super::isochrone_handler::IsochroneRequest,
        super::isochrone_handler::IsochroneResponse,
        super::isochrone_handler::ContourFeature,
        super::nearest::NearestRequest,
        super::nearest::NearestResponse,
        super::nearest::NearestWaypoint,
        Point,
        super::types::ErrorResponse,
        super::types::Waypoint,
        super::types::SnapRole,
        super::matching::MatchRequest,
        super::matching::MatchResponse,
        super::matching::MatchMatching,
        super::matching::MatchTracepoint,
        super::trip::TripRequest,
        super::trip::TripResponse,
        super::trip::Trip,
        super::trip::TripLeg,
        super::trip::TripWaypoint,
        super::elevation::HeightRequest,
        super::elevation::HeightResponse,
        super::elevation::HeightResult,
        super::regions_handler::LoadedRegion,
        super::regions_handler::RegionsResponse,
    )),
    tags(
        (name = "Routing", description = "Point-to-point routing with geometry and instructions"),
        (name = "Matrix", description = "Distance/duration matrix computation"),
        (name = "Isochrone", description = "Reachability polygons and bulk isochrones"),
        (name = "Search", description = "Nearest road snapping and map matching"),
        (name = "Elevation", description = "SRTM elevation lookup"),
        (name = "System", description = "Health, metrics, and diagnostics"),
    ),
    info(
        title = "Butterfly Route API",
        version = "2.0.0",
        description = "High-performance routing engine with exact turn-aware edge-based CCH queries.\n\nBelgium dataset: 5M edge-states, 14.6M arcs, 754K named roads.\n\n## Quick Start\n\nAll GET endpoints accept query parameters. All POST endpoints accept JSON bodies.\n\nCoordinates are always `[longitude, latitude]` (GeoJSON order).\n\nTransport modes: `car`, `bike`, `foot`."
    )
)]
struct ApiDoc;

/// Build the Axum router.
///
/// Accepts a multi-region [`RegionsState`]. Single-region deployments
/// wrap their loaded `ServerState` in a one-region `RegionsState` via
/// [`RegionsState::from_single`] before calling this; the router shape
/// is identical either way.
pub fn build_router(state: Arc<RegionsState>) -> Router {
    // CORS: fully permissive to allow browser-based clients (mapping apps, dashboards).
    // For production deployments requiring CORS restrictions, use a reverse proxy (nginx, caddy).
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // Prometheus metrics
    let (prometheus_layer, metric_handle) = axum_prometheus::PrometheusMetricLayer::pair();

    // Only expose /height when SRTM DEM tiles are actually loaded in at
    // least one region. Without them the handler can only 503, so
    // registering it would advertise a non-functional endpoint. When no
    // elevation data is staged the route is simply not registered (404).
    // It self-enables on the next boot once `<data>/srtm/` is populated —
    // no code change needed to re-enable. See butterfly-osm issue (SRTM
    // staging) for wiring the dataset into the deploy.
    let elevation_loaded = state
        .iter_regions()
        .any(|r| r.state_loaded().is_some_and(|s| s.elevation.is_some()));

    // API routes: normal endpoints with 120s timeout + response compression + concurrency limit
    let mut api_routes = Router::new()
        .route("/route", get(super::route::route_handler))
        .route("/nearest", get(super::nearest::nearest_handler))
        .route("/table", post(super::table::table_post_handler))
        .route(
            "/isochrone",
            get(super::isochrone_handler::isochrone_handler),
        )
        .route("/trip", post(super::trip::trip_handler))
        .route("/match", post(super::matching::match_trace_handler))
        .route("/catchment", post(super::catchment::catchment_handler))
        .route("/transit", get(super::transit_handler::transit_handler))
        .route(
            "/transit/bulk",
            post(super::transit_handler::transit_bulk_handler),
        )
        .route("/health", get(super::health_handler::health_handler))
        .route("/version", get(version_handler))
        .route("/regions", get(super::regions_handler::regions_handler));
    if elevation_loaded {
        api_routes = api_routes.route("/height", get(super::height_handler::height_handler));
        tracing::info!("/height endpoint enabled (SRTM elevation data loaded)");
    } else {
        tracing::info!("/height endpoint NOT registered — no SRTM elevation data loaded");
    }
    let api_routes = api_routes
        .layer(CompressionLayer::new())
        .layer(ConcurrencyLimitLayer::new(32))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(120),
        ));

    // Streaming routes: longer timeout, larger body limit, no compression, stricter concurrency
    // Streaming routes are memory-intensive (Arrow IPC, bulk isochrones), so limit to 4 concurrent
    // /table/stream has been replaced by Arrow Flight gRPC (see server/flight.rs)
    let stream_routes = Router::new()
        .route(
            "/isochrone/bulk",
            post(super::isochrone_handler::isochrone_bulk_handler),
        )
        .layer(DefaultBodyLimit::max(256 * 1024 * 1024)) // 256MB
        .layer(ConcurrencyLimitLayer::new(4))
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(600),
        ));

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .merge(api_routes)
        .merge(stream_routes)
        .route("/metrics", get(|| async move { metric_handle.render() }))
        .layer(CatchPanicLayer::new())
        .layer(prometheus_layer)
        .layer(TraceLayer::new_for_http())
        .layer(cors)
        .with_state(state)
}

/// `GET /version` — name and version (org standard, #516).
#[utoipa::path(get, path = "/version", tag = "System", summary = "Name and version",
    responses((status = 200, description = "{name, version}")))]
pub async fn version_handler() -> axum::Json<serde_json::Value> {
    axum::Json(serde_json::json!({
        "name": "butterfly-route",
        "version": env!("CARGO_PKG_VERSION"),
    }))
}

/// Every REST path the router mounts (#554). `/height` is mounted only when
/// `<data>/srtm/` exists and `/metrics`, `/swagger-ui` are not API paths, so
/// they are excluded from the OpenAPI parity check below.
pub const MOUNTED_PATHS: &[&str] = &[
    "/route",
    "/nearest",
    "/table",
    "/isochrone",
    "/isochrone/bulk",
    "/trip",
    "/match",
    "/catchment",
    "/transit",
    "/transit/bulk",
    "/health",
    "/version",
    "/regions",
    "/height",
];

#[cfg(test)]
mod openapi_parity {
    use super::*;
    use utoipa::OpenApi;

    /// #554: a mounted path without OpenAPI documentation is a defect — the
    /// Swagger UI is the only public reference of the REST surface.
    #[test]
    fn openapi_documents_every_mounted_path() {
        let doc = ApiDoc::openapi();
        let documented: std::collections::BTreeSet<String> =
            doc.paths.paths.keys().cloned().collect();
        let missing: Vec<&str> = MOUNTED_PATHS
            .iter()
            .copied()
            .filter(|p| !documented.contains(*p))
            .collect();
        assert!(missing.is_empty(), "mounted but undocumented: {missing:?}");
        let extra: Vec<&String> = documented
            .iter()
            .filter(|p| !MOUNTED_PATHS.contains(&p.as_str()))
            .collect();
        assert!(extra.is_empty(), "documented but not mounted: {extra:?}");
    }
}
