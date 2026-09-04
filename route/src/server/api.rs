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

/// REST paths that once existed and were REMOVED — `/table/stream`, the
/// pre-Flight Arrow-over-HTTP exception, went in #547. The docs may mention
/// them only as history (a line that says `removed` / `not mounted` /
/// `historical`); `docs_parity` below fails the build otherwise (#588).
pub const REMOVED_PATHS: &[&str] = &["/table/stream"];

#[cfg(test)]
mod docs_parity {
    use super::*;

    /// The public prose describing the REST surface, embedded at compile time
    /// so a stale mention fails `cargo test` instead of misleading a reader
    /// (#588 — `/table/stream` survived its removal in four places).
    const DOCS: &[(&str, &str)] = &[
        ("README.md", include_str!("../../../README.md")),
        (
            "docs/ENGINEERING.md",
            include_str!("../../../docs/ENGINEERING.md"),
        ),
        ("docs/api.md", include_str!("../../../docs/api.md")),
        (
            "docs/troubleshooting.md",
            include_str!("../../../docs/troubleshooting.md"),
        ),
        (
            "competitive_landscape.md",
            include_str!("../../../competitive_landscape.md"),
        ),
    ];

    /// A line may name a removed path only while saying that it is gone.
    fn marked_historical(line: &str) -> bool {
        let l = line.to_ascii_lowercase();
        l.contains("removed") || l.contains("not mounted") || l.contains("historical")
    }

    #[test]
    fn every_mounted_path_is_documented() {
        // docs/api.md is THE REST reference: every mounted path must be there.
        let (_, api_md) = DOCS
            .iter()
            .find(|(name, _)| *name == "docs/api.md")
            .expect("docs/api.md embedded");
        let missing_in_reference: Vec<&str> = MOUNTED_PATHS
            .iter()
            .copied()
            .filter(|p| !api_md.contains(p))
            .collect();
        assert!(
            missing_in_reference.is_empty(),
            "mounted but absent from docs/api.md: {missing_in_reference:?}"
        );
        let missing_everywhere: Vec<&str> = MOUNTED_PATHS
            .iter()
            .copied()
            .filter(|p| !DOCS.iter().any(|(_, doc)| doc.contains(p)))
            .collect();
        assert!(
            missing_everywhere.is_empty(),
            "mounted but documented nowhere: {missing_everywhere:?}"
        );
    }

    #[test]
    fn removed_paths_are_history_not_endpoints() {
        for p in REMOVED_PATHS {
            assert!(
                !MOUNTED_PATHS.contains(p),
                "{p} is listed both as mounted and as removed"
            );
        }
        let mut stale = Vec::new();
        for (name, doc) in DOCS {
            for (i, line) in doc.lines().enumerate() {
                for p in REMOVED_PATHS {
                    if line.contains(p) && !marked_historical(line) {
                        stale.push(format!("{name}:{}: {}", i + 1, line.trim()));
                    }
                }
            }
        }
        assert!(
            stale.is_empty(),
            "removed REST paths still documented as live — say `removed`, \
             `not mounted` or `historical` on the line, or drop the mention:\n{}",
            stale.join("\n")
        );
    }
}

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

    /// The three paths that take no request input at all — nothing to
    /// refuse, so nothing to document a refusal for. Every OTHER mounted
    /// path parses a coordinate, a mode or a body, and therefore MUST
    /// document what it answers when that parse fails.
    const INPUTLESS_PATHS: &[&str] = &["/health", "/version", "/regions"];

    /// #576: the router and the document must agree on the error SHAPE,
    /// not just on the path list. `/trip` and `/match` drifted for a
    /// year — they answered `{code, message}` while their OpenAPI entry
    /// said either nothing or `ErrorResponse` — because nothing checked.
    /// Every non-2xx response of every mounted path now has to name the
    /// one shared component; a new endpoint that invents its own body
    /// fails this test before it can ship.
    #[test]
    fn every_documented_error_response_is_the_one_error_shape() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serialises");
        let paths = doc["paths"].as_object().expect("paths object");
        let mut undocumented_errors: Vec<&str> = Vec::new();
        for path in MOUNTED_PATHS {
            let item = paths
                .get(*path)
                .unwrap_or_else(|| panic!("{path} missing from the OpenAPI document"));
            let mut documents_an_error = false;
            for (method, op) in item.as_object().expect("path item object") {
                let Some(responses) = op.get("responses").and_then(|r| r.as_object()) else {
                    continue;
                };
                for (status, response) in responses {
                    if status.starts_with('2') {
                        continue;
                    }
                    documents_an_error = true;
                    let schema = &response["content"]["application/json"]["schema"]["$ref"];
                    assert_eq!(
                        schema.as_str(),
                        Some("#/components/schemas/ErrorResponse"),
                        "{method} {path} answers {status} with something other than \
                         the shared ErrorResponse: {response}"
                    );
                }
            }
            if !documents_an_error && !INPUTLESS_PATHS.contains(path) {
                undocumented_errors.push(path);
            }
        }
        assert!(
            undocumented_errors.is_empty(),
            "these paths refuse bad input but document no error response: \
             {undocumented_errors:?}"
        );
    }

    /// Every REST handler module, embedded at compile time. `types.rs`
    /// is deliberately absent: it OWNS the deprecation window and is the
    /// one place allowed to name the legacy keys.
    const HANDLER_SOURCES: &[(&str, &str)] = &[
        ("route.rs", include_str!("route.rs")),
        ("table.rs", include_str!("table.rs")),
        ("isochrone_handler.rs", include_str!("isochrone_handler.rs")),
        ("nearest.rs", include_str!("nearest.rs")),
        ("matching.rs", include_str!("matching.rs")),
        ("trip.rs", include_str!("trip.rs")),
        ("catchment.rs", include_str!("catchment.rs")),
        ("transit_handler.rs", include_str!("transit_handler.rs")),
        ("height_handler.rs", include_str!("height_handler.rs")),
        ("health_handler.rs", include_str!("health_handler.rs")),
        ("regions_handler.rs", include_str!("regions_handler.rs")),
    ];

    /// The other half of the #576 guard: the document says
    /// `ErrorResponse` (the test above), and no handler may hand-build a
    /// body that says something else. The legacy shape was 35 inline
    /// `serde_json::json!({"code": ..., "message": ...})` literals in
    /// `/trip` and `/match`; a handler that types those keys again is
    /// re-opening the drift, so the keys themselves are the tripwire.
    /// The one place allowed to name them is `types.rs`, which serves
    /// them for the deprecation window.
    #[test]
    fn no_handler_hand_builds_an_error_body() {
        let mut offenders = Vec::new();
        for (name, src) in HANDLER_SOURCES {
            for (i, line) in src.lines().enumerate() {
                if line.contains("\"code\":") || line.contains("\"message\":") {
                    offenders.push(format!("{name}:{}: {}", i + 1, line.trim()));
                }
            }
        }
        assert!(
            offenders.is_empty(),
            "an error body is being hand-built instead of `types::ErrorResponse` \
             — the whole point of #576:\n{}",
            offenders.join("\n")
        );
    }

    /// The documented field is `error`, and the two legacy `/trip` and
    /// `/match` fields are documented AS deprecated for the length of
    /// the #576 window — a client reading the Swagger UI must be able to
    /// see which field survives it.
    #[test]
    fn the_error_component_documents_the_field_to_read_and_the_two_deprecated_ones() {
        let doc = serde_json::to_value(ApiDoc::openapi()).expect("openapi serialises");
        let schema = &doc["components"]["schemas"]["ErrorResponse"];
        let props = schema["properties"]
            .as_object()
            .expect("ErrorResponse properties");
        assert!(props.contains_key("error"), "{schema}");
        assert_eq!(
            schema["required"],
            serde_json::json!(["error"]),
            "only `error` is guaranteed: {schema}"
        );
        for legacy in ["code", "message"] {
            assert_eq!(
                props[legacy]["deprecated"],
                serde_json::json!(true),
                "the legacy `{legacy}` field must be documented as deprecated: {schema}"
            );
        }
    }
}
