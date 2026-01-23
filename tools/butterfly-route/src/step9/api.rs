//! HTTP API handlers with Axum and Utoipa

use axum::{
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
    routing::get,
    Json, Router,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tower_http::cors::{Any, CorsLayer};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::profile_abi::Mode;

use super::geometry::{build_geometry, build_isochrone_geometry, Point, RouteGeometry};
use super::query::{query_one_to_many, CchQuery};
use super::state::ServerState;
use super::unpack::unpack_path;

/// OpenAPI documentation
#[derive(OpenApi)]
#[openapi(
    paths(route, matrix, isochrone, health),
    components(schemas(
        RouteRequest,
        RouteResponse,
        MatrixRequest,
        MatrixResponse,
        IsochroneRequest,
        IsochroneResponse,
        Point,
        ErrorResponse
    )),
    info(
        title = "Butterfly Route API",
        version = "1.0.0",
        description = "High-performance routing engine with exact turn-aware queries"
    )
)]
struct ApiDoc;

/// Build the Axum router
pub fn build_router(state: Arc<ServerState>) -> Router {
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/route", get(route))
        .route("/matrix", get(matrix))
        .route("/isochrone", get(isochrone))
        .route("/health", get(health))
        .layer(cors)
        .with_state(state)
}

// ============ Route Endpoint ============

#[derive(Debug, Deserialize, ToSchema)]
pub struct RouteRequest {
    /// Source longitude
    #[schema(example = 4.3517)]
    src_lon: f64,
    /// Source latitude
    #[schema(example = 50.8503)]
    src_lat: f64,
    /// Destination longitude
    #[schema(example = 4.4017)]
    dst_lon: f64,
    /// Destination latitude
    #[schema(example = 50.8603)]
    dst_lat: f64,
    /// Transport mode: car, bike, or foot
    #[schema(example = "car")]
    mode: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RouteResponse {
    /// Duration in seconds
    pub duration_s: f64,
    /// Distance in meters
    pub distance_m: f64,
    /// Route geometry
    pub geometry: RouteGeometry,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

/// Calculate route between two points
#[utoipa::path(
    get,
    path = "/route",
    params(
        ("src_lon" = f64, Query, description = "Source longitude"),
        ("src_lat" = f64, Query, description = "Source latitude"),
        ("dst_lon" = f64, Query, description = "Destination longitude"),
        ("dst_lat" = f64, Query, description = "Destination latitude"),
        ("mode" = String, Query, description = "Transport mode: car, bike, foot"),
    ),
    responses(
        (status = 200, description = "Route found", body = RouteResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 404, description = "No route found", body = ErrorResponse),
    )
)]
async fn route(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<RouteRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    let mode_data = state.get_mode(mode);

    // Snap source and destination (returns original EBG node IDs)
    let src_orig = match state.spatial_index.snap(req.src_lon, req.src_lat, &mode_data.mask, 10) {
        Some(id) => {
            eprintln!("DEBUG: Snapped src ({}, {}) to original node {} for mode {:?}",
                req.src_lon, req.src_lat, id, mode);
            id
        }
        None => {
            eprintln!("DEBUG: Failed to snap src ({}, {}) for mode {:?}",
                req.src_lon, req.src_lat, mode);
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Could not snap source to road network".to_string(),
                }),
            )
                .into_response()
        }
    };

    let dst_orig = match state.spatial_index.snap(req.dst_lon, req.dst_lat, &mode_data.mask, 10) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Could not snap destination to road network".to_string(),
                }),
            )
                .into_response()
        }
    };

    // Convert to filtered node IDs for CCH query
    let src_filtered = mode_data.filtered_ebg.original_to_filtered[src_orig as usize];
    let dst_filtered = mode_data.filtered_ebg.original_to_filtered[dst_orig as usize];

    if src_filtered == u32::MAX || dst_filtered == u32::MAX {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Snapped node not accessible for this mode".to_string(),
            }),
        )
            .into_response();
    }

    // Run query (in filtered space)
    let query = CchQuery::new(&state, mode);
    let result = match query.query(src_filtered, dst_filtered) {
        Some(r) => r,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "No route found".to_string(),
                }),
            )
                .into_response()
        }
    };

    // Unpack path (in filtered space)
    let filtered_path = unpack_path(
        &mode_data.cch_topo,
        &result.forward_parent,
        &result.backward_parent,
        src_filtered,
        dst_filtered,
        result.meeting_node,
    );

    // Convert path from filtered to original EBG node IDs for geometry
    let ebg_path: Vec<u32> = filtered_path
        .iter()
        .map(|&filtered_id| mode_data.filtered_ebg.filtered_to_original[filtered_id as usize])
        .collect();

    // Build geometry (uses original EBG node IDs)
    let geometry = build_geometry(&ebg_path, &state.ebg_nodes, &state.nbg_geo, result.distance);

    // Debug info
    eprintln!("DEBUG: src_orig={}, dst_orig={}, src_filt={}, dst_filt={}, distance={}, meeting_filt={}, fwd_len={}, bwd_len={}, ebg_path_len={}",
        src_orig, dst_orig, src_filtered, dst_filtered, result.distance, result.meeting_node,
        result.forward_parent.len(), result.backward_parent.len(), ebg_path.len());

    Json(RouteResponse {
        duration_s: result.distance as f64 / 10.0, // deciseconds to seconds
        distance_m: geometry.distance_m,
        geometry,
    })
    .into_response()
}

// ============ Matrix Endpoint ============

#[derive(Debug, Deserialize, ToSchema)]
pub struct MatrixRequest {
    /// Source longitude
    src_lon: f64,
    /// Source latitude
    src_lat: f64,
    /// Destination longitudes (comma-separated)
    dst_lons: String,
    /// Destination latitudes (comma-separated)
    dst_lats: String,
    /// Transport mode
    mode: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MatrixResponse {
    /// Durations in seconds (null if unreachable)
    pub durations: Vec<Option<f64>>,
}

/// Calculate distance matrix from one source to many destinations
#[utoipa::path(
    get,
    path = "/matrix",
    params(
        ("src_lon" = f64, Query, description = "Source longitude"),
        ("src_lat" = f64, Query, description = "Source latitude"),
        ("dst_lons" = String, Query, description = "Destination longitudes (comma-separated)"),
        ("dst_lats" = String, Query, description = "Destination latitudes (comma-separated)"),
        ("mode" = String, Query, description = "Transport mode"),
    ),
    responses(
        (status = 200, description = "Matrix computed", body = MatrixResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn matrix(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<MatrixRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    let mode_data = state.get_mode(mode);

    // Parse destinations
    let dst_lons: Vec<f64> = req
        .dst_lons
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();
    let dst_lats: Vec<f64> = req
        .dst_lats
        .split(',')
        .filter_map(|s| s.trim().parse().ok())
        .collect();

    if dst_lons.len() != dst_lats.len() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Mismatched destination coordinates".to_string(),
            }),
        )
            .into_response();
    }

    // Snap source (original EBG node ID)
    let src_orig = match state.spatial_index.snap(req.src_lon, req.src_lat, &mode_data.mask, 10) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Could not snap source".to_string(),
                }),
            )
                .into_response()
        }
    };

    // Convert to filtered space
    let src_filtered = mode_data.filtered_ebg.original_to_filtered[src_orig as usize];
    if src_filtered == u32::MAX {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Source not accessible for this mode".to_string(),
            }),
        )
            .into_response();
    }

    // Snap destinations and convert to filtered space
    let targets_filtered: Vec<u32> = dst_lons
        .iter()
        .zip(dst_lats.iter())
        .filter_map(|(&lon, &lat)| {
            state.spatial_index.snap(lon, lat, &mode_data.mask, 10).and_then(|orig_id| {
                let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
                if filtered != u32::MAX { Some(filtered) } else { None }
            })
        })
        .collect();

    // Run queries (in filtered space)
    let distances = query_one_to_many(&state, mode, src_filtered, &targets_filtered);

    let durations: Vec<Option<f64>> = distances
        .into_iter()
        .map(|d| d.map(|ds| ds as f64 / 10.0))
        .collect();

    Json(MatrixResponse { durations }).into_response()
}

// ============ Isochrone Endpoint ============

#[derive(Debug, Deserialize, ToSchema)]
pub struct IsochroneRequest {
    /// Center longitude
    lon: f64,
    /// Center latitude
    lat: f64,
    /// Time limit in seconds
    time_s: u32,
    /// Transport mode
    mode: String,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct IsochroneResponse {
    /// Isochrone polygon coordinates
    pub polygon: Vec<Point>,
    /// Number of reachable nodes
    pub reachable_nodes: usize,
}

/// Calculate isochrone (reachable area within time limit)
#[utoipa::path(
    get,
    path = "/isochrone",
    params(
        ("lon" = f64, Query, description = "Center longitude"),
        ("lat" = f64, Query, description = "Center latitude"),
        ("time_s" = u32, Query, description = "Time limit in seconds"),
        ("mode" = String, Query, description = "Transport mode"),
    ),
    responses(
        (status = 200, description = "Isochrone computed", body = IsochroneResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn isochrone(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<IsochroneRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    let mode_data = state.get_mode(mode);
    let time_ds = req.time_s * 10; // Convert to deciseconds

    // Snap center (original EBG node ID)
    let center_orig = match state.spatial_index.snap(req.lon, req.lat, &mode_data.mask, 10) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Could not snap center to road network".to_string(),
                }),
            )
                .into_response()
        }
    };

    // Convert to filtered space
    let center_filtered = mode_data.filtered_ebg.original_to_filtered[center_orig as usize];
    if center_filtered == u32::MAX {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Center not accessible for this mode".to_string(),
            }),
        )
            .into_response();
    }

    // Run bounded Dijkstra (in filtered space)
    let settled_filtered = bounded_dijkstra(&state, mode, center_filtered, time_ds);

    // Convert settled nodes from filtered to original EBG node IDs for geometry
    let settled: Vec<(u32, u32)> = settled_filtered
        .iter()
        .map(|&(filtered_id, dist)| {
            (mode_data.filtered_ebg.filtered_to_original[filtered_id as usize], dist)
        })
        .collect();

    // Build isochrone geometry (uses original EBG node IDs)
    let polygon = build_isochrone_geometry(&settled, time_ds, &state.ebg_nodes, &state.nbg_geo);

    Json(IsochroneResponse {
        polygon,
        reachable_nodes: settled.len(),
    })
    .into_response()
}

/// Bounded Dijkstra for isochrone computation (operates in filtered node space)
fn bounded_dijkstra(
    state: &ServerState,
    mode: Mode,
    source: u32,
    max_time_ds: u32,
) -> Vec<(u32, u32)> {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let mode_data = state.get_mode(mode);
    let cch_topo = &mode_data.cch_topo;
    let n = cch_topo.n_nodes as usize;

    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
    let mut settled = Vec::new();

    dist[source as usize] = 0;
    pq.push(source, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        if d > max_time_ds {
            continue;
        }

        settled.push((u, d));

        // Relax UP edges
        let start = cch_topo.up_offsets[u as usize] as usize;
        let end = cch_topo.up_offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = cch_topo.up_targets[i];
            let w = mode_data.cch_weights.up[i];

            if w == u32::MAX {
                continue;
            }

            let new_dist = d.saturating_add(w);
            if new_dist <= max_time_ds && new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }

        // Also relax DOWN edges for isochrone (we want all reachable nodes)
        let start = cch_topo.down_offsets[u as usize] as usize;
        let end = cch_topo.down_offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = cch_topo.down_targets[i];
            let w = mode_data.cch_weights.down[i];

            if w == u32::MAX {
                continue;
            }

            let new_dist = d.saturating_add(w);
            if new_dist <= max_time_ds && new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    settled
}

// ============ Health Endpoint ============

/// Health check endpoint
#[utoipa::path(
    get,
    path = "/health",
    responses(
        (status = 200, description = "Server is healthy"),
    )
)]
async fn health() -> impl IntoResponse {
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION")
    }))
}

/// Parse mode string to Mode enum
fn parse_mode(s: &str) -> Result<Mode, String> {
    match s.to_lowercase().as_str() {
        "car" => Ok(Mode::Car),
        "bike" => Ok(Mode::Bike),
        "foot" => Ok(Mode::Foot),
        _ => Err(format!("Invalid mode: {}. Use car, bike, or foot.", s)),
    }
}
