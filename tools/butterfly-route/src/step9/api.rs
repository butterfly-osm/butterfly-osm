//! HTTP API handlers with Axum and Utoipa

use axum::{
    body::Body,
    extract::{Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use futures::stream::StreamExt;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;
use tower_http::cors::{Any, CorsLayer};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::profile_abi::Mode;
use crate::matrix::arrow_stream::{MatrixTile, ArrowMatrixWriter, ARROW_STREAM_CONTENT_TYPE, tiles_to_record_batch};
use crate::matrix::batched_phast::K_LANES;

use super::geometry::{build_geometry, build_isochrone_geometry, Point, RouteGeometry};
use super::query::{query_one_to_many, CchQuery};
use super::state::ServerState;
use super::unpack::unpack_path;

/// OpenAPI documentation
#[derive(OpenApi)]
#[openapi(
    paths(route, matrix, isochrone, health, matrix_bulk, matrix_stream),
    components(schemas(
        RouteRequest,
        RouteResponse,
        MatrixRequest,
        MatrixResponse,
        IsochroneRequest,
        IsochroneResponse,
        MatrixBulkRequest,
        MatrixBulkResponse,
        MatrixStreamRequest,
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
        .route("/matrix/bulk", post(matrix_bulk))
        .route("/matrix/stream", post(matrix_stream))
        .route("/isochrone", get(isochrone))
        .route("/health", get(health))
        .route("/debug/compare", get(debug_compare))
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

// ============ Bulk Matrix Endpoint ============

#[derive(Debug, Deserialize, ToSchema)]
pub struct MatrixBulkRequest {
    /// Source node IDs (filtered CCH space) OR source coordinates
    #[schema(example = json!([1000, 2000, 3000]))]
    pub sources: Vec<u32>,
    /// Target node IDs (filtered CCH space) OR target coordinates
    #[schema(example = json!([4000, 5000]))]
    pub targets: Vec<u32>,
    /// Transport mode: car, bike, or foot
    #[schema(example = "car")]
    pub mode: String,
    /// Output format: "json" or "arrow" (default: json)
    #[schema(example = "json")]
    pub format: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct MatrixBulkResponse {
    /// Row-major matrix of durations in deciseconds (u32::MAX = unreachable)
    pub durations_ds: Vec<u32>,
    /// Number of sources
    pub n_sources: usize,
    /// Number of targets
    pub n_targets: usize,
    /// Computation time in milliseconds
    pub compute_time_ms: u64,
}

/// Request for streaming matrix computation
#[derive(Debug, Deserialize, ToSchema)]
pub struct MatrixStreamRequest {
    /// Source node IDs (filtered CCH space)
    #[schema(example = json!([1000, 2000, 3000, 4000, 5000, 6000, 7000, 8000]))]
    pub sources: Vec<u32>,
    /// Target node IDs (filtered CCH space)
    #[schema(example = json!([4000, 5000, 6000]))]
    pub targets: Vec<u32>,
    /// Transport mode: car, bike, or foot
    #[schema(example = "car")]
    pub mode: String,
    /// Source tile size (default: 8, matches K_LANES)
    #[schema(example = 8)]
    pub src_tile_size: Option<u16>,
    /// Destination tile size for output chunking (default: 256)
    #[schema(example = 256)]
    pub dst_tile_size: Option<u16>,
}

/// Compute bulk distance matrix using K-lane batched PHAST
///
/// Returns a row-major matrix of durations from each source to each target.
/// For large matrices, use `format=arrow` to stream Arrow IPC output.
#[utoipa::path(
    post,
    path = "/matrix/bulk",
    request_body = MatrixBulkRequest,
    responses(
        (status = 200, description = "Matrix computed", body = MatrixBulkResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn matrix_bulk(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<MatrixBulkRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    let mode_data = state.get_mode(mode);
    let format = req.format.as_deref().unwrap_or("json");

    let start = std::time::Instant::now();

    // Use inline K-lane batched PHAST computation
    let (matrix, _n_batches) = compute_batched_matrix(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &mode_data.order,
        &req.sources,
        &req.targets,
    );

    let compute_time_ms = start.elapsed().as_millis() as u64;

    match format {
        "arrow" => {
            // Return Arrow IPC format
            let tile = MatrixTile::from_flat(
                0,
                0,
                req.sources.len() as u16,
                req.targets.len() as u16,
                &matrix,
            );

            let mut buf = Vec::new();
            match ArrowMatrixWriter::new(&mut buf) {
                Ok(mut writer) => {
                    if let Err(e) = writer.write_tile(&tile) {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse { error: format!("Arrow write error: {}", e) }),
                        ).into_response();
                    }
                    if let Err(e) = writer.finish() {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse { error: format!("Arrow finish error: {}", e) }),
                        ).into_response();
                    }
                }
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse { error: format!("Arrow init error: {}", e) }),
                    ).into_response();
                }
            }

            Response::builder()
                .status(StatusCode::OK)
                .header(header::CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
                .header("X-Compute-Time-Ms", compute_time_ms.to_string())
                .header("X-N-Sources", req.sources.len().to_string())
                .header("X-N-Targets", req.targets.len().to_string())
                .body(Body::from(buf))
                .unwrap()
                .into_response()
        }
        _ => {
            // Return JSON format
            Json(MatrixBulkResponse {
                durations_ds: matrix,
                n_sources: req.sources.len(),
                n_targets: req.targets.len(),
                compute_time_ms,
            }).into_response()
        }
    }
}

// ============ Streaming Matrix Endpoint ============

/// Stream distance matrix as Arrow IPC tiles
///
/// Computes the matrix in tiles (K sources × M targets at a time) and streams
/// each tile as an Arrow IPC RecordBatch as soon as it's ready.
///
/// ## Streaming Protocol
/// 1. Server sends Arrow IPC schema message
/// 2. Server streams RecordBatch messages as tiles complete
/// 3. Connection closes when all tiles are sent
/// 4. If client disconnects, computation stops (cancellation)
///
/// ## Backpressure
/// Uses a bounded channel (depth 4) - if client is slow to consume,
/// computation pauses until channel has space.
///
/// ## Tile Schema
/// Each RecordBatch contains one or more tiles:
/// - src_block_start: u32 (first source index)
/// - dst_block_start: u32 (first destination index)
/// - src_block_len: u16 (sources in tile)
/// - dst_block_len: u16 (destinations in tile)
/// - durations_ms: Binary (row-major packed u32 distances)
#[utoipa::path(
    post,
    path = "/matrix/stream",
    request_body = MatrixStreamRequest,
    responses(
        (status = 200, description = "Arrow IPC stream", content_type = "application/vnd.apache.arrow.stream"),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn matrix_stream(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<MatrixStreamRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    if req.sources.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "sources cannot be empty".into() })).into_response();
    }
    if req.targets.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "targets cannot be empty".into() })).into_response();
    }

    let src_tile_size = req.src_tile_size.unwrap_or(K_LANES as u16) as usize;
    let dst_tile_size = req.dst_tile_size.unwrap_or(256) as usize;

    // Clone data needed for async task
    let sources = req.sources.clone();
    let targets = req.targets.clone();
    let mode_data = state.get_mode(mode);
    let cch_topo = mode_data.cch_topo.clone();
    let cch_weights_up = mode_data.cch_weights.up.clone();
    let cch_weights_down = mode_data.cch_weights.down.clone();
    let order_perm = mode_data.order.perm.clone();

    // Bounded channel for backpressure (4 tiles in flight)
    let (tx, rx) = mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(4);

    // Cancellation token - dropped when receiver is dropped (client disconnect)
    let cancel = CancellationToken::new();
    let cancel_clone = cancel.clone();

    // Spawn computation task
    tokio::task::spawn_blocking(move || {
        compute_and_stream_tiles(
            &cch_topo,
            &cch_weights_up,
            &cch_weights_down,
            &order_perm,
            &sources,
            &targets,
            src_tile_size,
            dst_tile_size,
            tx,
            cancel_clone,
        );
    });

    // Convert receiver to stream
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    // Build streaming response
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
        .header("X-N-Sources", req.sources.len().to_string())
        .header("X-N-Targets", req.targets.len().to_string())
        .header("X-Src-Tile-Size", src_tile_size.to_string())
        .header("X-Dst-Tile-Size", dst_tile_size.to_string())
        .body(Body::from_stream(stream))
        .unwrap()
        .into_response()
}

/// Compute matrix tiles and stream them over channel
fn compute_and_stream_tiles(
    cch_topo: &crate::formats::CchTopo,
    weights_up: &[u32],
    weights_down: &[u32],
    perm: &[u32],
    sources: &[u32],
    targets: &[u32],
    src_tile_size: usize,
    dst_tile_size: usize,
    tx: mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
    cancel: CancellationToken,
) {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;
    use arrow::ipc::writer::StreamWriter;

    let n_nodes = cch_topo.n_nodes as usize;
    let n_src = sources.len();
    let n_tgt = targets.len();

    // Build inverse permutation
    let mut inv_perm = vec![0u32; n_nodes];
    for (node, &rank) in perm.iter().enumerate() {
        inv_perm[rank as usize] = node as u32;
    }

    // Send Arrow schema first
    let schema = Arc::new(crate::matrix::arrow_stream::matrix_tile_schema());
    let mut schema_buf = Vec::new();
    {
        let mut writer = match StreamWriter::try_new(&mut schema_buf, &schema) {
            Ok(w) => w,
            Err(e) => {
                let _ = tx.blocking_send(Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                return;
            }
        };
        if let Err(e) = writer.finish() {
            let _ = tx.blocking_send(Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
            return;
        }
    }
    if tx.blocking_send(Ok(bytes::Bytes::from(schema_buf))).is_err() {
        return; // Client disconnected
    }

    // Process sources in batches matching src_tile_size (aligned to K_LANES for efficiency)
    let effective_tile_size = ((src_tile_size + K_LANES - 1) / K_LANES) * K_LANES;

    for src_batch_start in (0..n_src).step_by(effective_tile_size) {
        // Check for cancellation
        if cancel.is_cancelled() {
            return;
        }

        let src_batch_end = (src_batch_start + effective_tile_size).min(n_src);
        let batch_sources = &sources[src_batch_start..src_batch_end];
        let actual_src_len = batch_sources.len();

        // Compute distances for this batch of sources to ALL nodes
        // We'll extract target distances after
        let dist_batch = compute_batch_distances(
            cch_topo,
            weights_up,
            weights_down,
            &inv_perm,
            batch_sources,
        );

        // Extract tiles for each destination chunk
        for dst_batch_start in (0..n_tgt).step_by(dst_tile_size) {
            if cancel.is_cancelled() {
                return;
            }

            let dst_batch_end = (dst_batch_start + dst_tile_size).min(n_tgt);
            let batch_targets = &targets[dst_batch_start..dst_batch_end];
            let actual_dst_len = batch_targets.len();

            // Extract distances for this tile
            let mut tile_data = Vec::with_capacity(actual_src_len * actual_dst_len * 4);
            for (lane, dist) in dist_batch.iter().enumerate() {
                if lane >= actual_src_len {
                    break;
                }
                for &tgt in batch_targets {
                    let d = if (tgt as usize) < n_nodes {
                        dist[tgt as usize]
                    } else {
                        u32::MAX
                    };
                    tile_data.extend_from_slice(&d.to_le_bytes());
                }
            }

            let tile = MatrixTile {
                src_block_start: src_batch_start as u32,
                dst_block_start: dst_batch_start as u32,
                src_block_len: actual_src_len as u16,
                dst_block_len: actual_dst_len as u16,
                durations_ms: tile_data,
            };

            // Serialize tile as Arrow RecordBatch
            let batch = match tiles_to_record_batch(&[tile]) {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx.blocking_send(Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                    return;
                }
            };

            let mut buf = Vec::new();
            {
                let mut writer = match StreamWriter::try_new(&mut buf, batch.schema_ref()) {
                    Ok(w) => w,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                        return;
                    }
                };
                if let Err(e) = writer.write(&batch) {
                    let _ = tx.blocking_send(Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                    return;
                }
                if let Err(e) = writer.finish() {
                    let _ = tx.blocking_send(Err(std::io::Error::new(std::io::ErrorKind::Other, e.to_string())));
                    return;
                }
            }

            // Send tile (backpressure: blocks if channel full)
            if tx.blocking_send(Ok(bytes::Bytes::from(buf))).is_err() {
                return; // Client disconnected
            }
        }
    }
}

/// Compute K-lane batched distances for a batch of sources
fn compute_batch_distances(
    cch_topo: &crate::formats::CchTopo,
    weights_up: &[u32],
    weights_down: &[u32],
    inv_perm: &[u32],
    sources: &[u32],
) -> Vec<Vec<u32>> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let n_nodes = cch_topo.n_nodes as usize;

    // Process in K_LANES chunks for efficiency
    let mut all_dist: Vec<Vec<u32>> = Vec::with_capacity(sources.len());

    for chunk in sources.chunks(K_LANES) {
        let k = chunk.len();

        // Initialize K distance arrays
        let mut dist: Vec<Vec<u32>> = (0..k)
            .map(|_| vec![u32::MAX; n_nodes])
            .collect();

        // Set origin distances
        for (lane, &src) in chunk.iter().enumerate() {
            if (src as usize) < n_nodes {
                dist[lane][src as usize] = 0;
            }
        }

        // Phase 1: K parallel upward searches
        for lane in 0..k {
            let origin = chunk[lane];
            if (origin as usize) >= n_nodes {
                continue;
            }

            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                if d > dist[lane][u as usize] {
                    continue;
                }

                let up_start = cch_topo.up_offsets[u as usize] as usize;
                let up_end = cch_topo.up_offsets[u as usize + 1] as usize;

                for i in up_start..up_end {
                    let v = cch_topo.up_targets[i];
                    let w = weights_up[i];

                    if w == u32::MAX {
                        continue;
                    }

                    let new_dist = d.saturating_add(w);
                    if new_dist < dist[lane][v as usize] {
                        dist[lane][v as usize] = new_dist;
                        pq.push(Reverse((new_dist, v)));
                    }
                }
            }
        }

        // Phase 2: Single K-lane downward scan
        for rank in (0..n_nodes).rev() {
            let u = inv_perm[rank];
            let u_idx = u as usize;

            let down_start = cch_topo.down_offsets[u_idx] as usize;
            let down_end = cch_topo.down_offsets[u_idx + 1] as usize;

            if down_start == down_end {
                continue;
            }

            // Check if ANY lane has finite distance
            let mut any_reachable = false;
            for lane in 0..k {
                if dist[lane][u_idx] != u32::MAX {
                    any_reachable = true;
                    break;
                }
            }
            if !any_reachable {
                continue;
            }

            // Relax DOWN edges for ALL K lanes
            for i in down_start..down_end {
                let v = cch_topo.down_targets[i];
                let w = weights_down[i];

                if w == u32::MAX {
                    continue;
                }

                let v_idx = v as usize;

                for lane in 0..k {
                    let d_u = dist[lane][u_idx];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        if new_dist < dist[lane][v_idx] {
                            dist[lane][v_idx] = new_dist;
                        }
                    }
                }
            }
        }

        // Collect distances
        all_dist.extend(dist);
    }

    all_dist
}

/// Compute batched matrix using K-lane PHAST (without owning data)
fn compute_batched_matrix(
    cch_topo: &crate::formats::CchTopo,
    cch_weights: &super::state::CchWeights,
    order: &crate::formats::OrderEbg,
    sources: &[u32],
    targets: &[u32],
) -> (Vec<u32>, usize) {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let n_nodes = cch_topo.n_nodes as usize;
    let n_src = sources.len();
    let n_tgt = targets.len();
    let mut matrix = vec![u32::MAX; n_src * n_tgt];

    // Build inverse permutation
    let mut inv_perm = vec![0u32; n_nodes];
    for (node, &rank) in order.perm.iter().enumerate() {
        inv_perm[rank as usize] = node as u32;
    }

    let mut n_batches = 0;

    // Process sources in batches of K
    for chunk in sources.chunks(K_LANES) {
        n_batches += 1;
        let k = chunk.len();

        // Initialize K distance arrays
        let mut dist: Vec<Vec<u32>> = (0..k)
            .map(|_| vec![u32::MAX; n_nodes])
            .collect();

        // Set origin distances
        for (lane, &src) in chunk.iter().enumerate() {
            if (src as usize) < n_nodes {
                dist[lane][src as usize] = 0;
            }
        }

        // Phase 1: K parallel upward searches
        for lane in 0..k {
            let origin = chunk[lane];
            if (origin as usize) >= n_nodes {
                continue;
            }

            let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
            pq.push(Reverse((0, origin)));

            while let Some(Reverse((d, u))) = pq.pop() {
                if d > dist[lane][u as usize] {
                    continue;
                }

                // Relax UP edges
                let up_start = cch_topo.up_offsets[u as usize] as usize;
                let up_end = cch_topo.up_offsets[u as usize + 1] as usize;

                for i in up_start..up_end {
                    let v = cch_topo.up_targets[i];
                    let w = cch_weights.up[i];

                    if w == u32::MAX {
                        continue;
                    }

                    let new_dist = d.saturating_add(w);
                    if new_dist < dist[lane][v as usize] {
                        dist[lane][v as usize] = new_dist;
                        pq.push(Reverse((new_dist, v)));
                    }
                }
            }
        }

        // Phase 2: Single K-lane downward scan
        for rank in (0..n_nodes).rev() {
            let u = inv_perm[rank];
            let u_idx = u as usize;

            let down_start = cch_topo.down_offsets[u_idx] as usize;
            let down_end = cch_topo.down_offsets[u_idx + 1] as usize;

            if down_start == down_end {
                continue;
            }

            // Check if ANY lane has finite distance
            let mut any_reachable = false;
            for lane in 0..k {
                if dist[lane][u_idx] != u32::MAX {
                    any_reachable = true;
                    break;
                }
            }
            if !any_reachable {
                continue;
            }

            // Relax DOWN edges for ALL K lanes
            for i in down_start..down_end {
                let v = cch_topo.down_targets[i];
                let w = cch_weights.down[i];

                if w == u32::MAX {
                    continue;
                }

                let v_idx = v as usize;

                // Update all K lanes
                for lane in 0..k {
                    let d_u = dist[lane][u_idx];
                    if d_u != u32::MAX {
                        let new_dist = d_u.saturating_add(w);
                        if new_dist < dist[lane][v_idx] {
                            dist[lane][v_idx] = new_dist;
                        }
                    }
                }
            }
        }

        // Copy distances to flat matrix
        let batch_start = (n_batches - 1) * K_LANES;
        for (lane, d) in dist.iter().enumerate() {
            let src_idx = batch_start + lane;
            if src_idx >= n_src {
                break;
            }
            for (tgt_idx, &tgt) in targets.iter().enumerate() {
                if (tgt as usize) < n_nodes {
                    matrix[src_idx * n_tgt + tgt_idx] = d[tgt as usize];
                }
            }
        }
    }

    (matrix, n_batches)
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

// ============ Debug Compare Endpoint ============

#[derive(Debug, Deserialize)]
pub struct DebugCompareRequest {
    src_lon: f64,
    src_lat: f64,
    dst_lon: f64,
    dst_lat: f64,
    mode: String,
}

#[derive(Debug, Serialize)]
pub struct DebugCompareResponse {
    cch_distance: Option<u32>,
    dijkstra_distance: Option<u32>,
    cch_meeting_rank: Option<u32>,
    src_rank: u32,
    dst_rank: u32,
    src_filtered: u32,
    dst_filtered: u32,
    cch_fwd_settled: usize,
    cch_bwd_settled: usize,
    dijkstra_settled: usize,
}

/// Debug endpoint comparing CCH query with plain Dijkstra on filtered EBG
async fn debug_compare(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<DebugCompareRequest>,
) -> impl IntoResponse {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    let mode_data = state.get_mode(mode);

    // Snap source and destination
    let src_orig = match state.spatial_index.snap(req.src_lon, req.src_lat, &mode_data.mask, 10) {
        Some(id) => id,
        None => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "Cannot snap source".to_string() })).into_response(),
    };
    let dst_orig = match state.spatial_index.snap(req.dst_lon, req.dst_lat, &mode_data.mask, 10) {
        Some(id) => id,
        None => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "Cannot snap dest".to_string() })).into_response(),
    };

    // Convert to filtered space
    let src_filtered = mode_data.filtered_ebg.original_to_filtered[src_orig as usize];
    let dst_filtered = mode_data.filtered_ebg.original_to_filtered[dst_orig as usize];

    if src_filtered == u32::MAX || dst_filtered == u32::MAX {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: "Node not accessible".to_string() })).into_response();
    }

    let n = mode_data.cch_topo.n_nodes as usize;
    let perm = &mode_data.order.perm;

    // Get ranks
    let src_rank = perm[src_filtered as usize];
    let dst_rank = perm[dst_filtered as usize];

    // ========== Validate down_rev structure ==========
    eprintln!("\nValidating down_rev structure...");
    match super::query::validate_down_rev(&mode_data.cch_topo, &mode_data.down_rev, perm) {
        Ok(()) => eprintln!("  ✓ down_rev validation passed"),
        Err(e) => eprintln!("  ✗ down_rev validation FAILED: {}", e),
    }

    // ========== Run CCH Query ==========
    eprintln!("\nCCH BIDIR QUERY DEBUG:");
    let query = CchQuery::new(&state, mode);
    let cch_result = query.query_with_debug(src_filtered, dst_filtered, true);
    let cch_distance = cch_result.as_ref().map(|r| r.distance);
    let cch_meeting_rank = cch_result.as_ref().map(|r| perm[r.meeting_node as usize]);
    let cch_fwd_settled = 0usize; // We'd need to modify query to track this
    let cch_bwd_settled = 0usize;

    // ========== Run Plain Dijkstra on Filtered EBG ==========
    // This uses ALL edges in the filtered EBG with proper weights
    let dijkstra_result = run_filtered_dijkstra(
        &mode_data.filtered_ebg,
        &mode_data.node_weights,
        src_filtered,
        dst_filtered,
    );
    let dijkstra_distance = dijkstra_result.0;
    let dijkstra_settled = dijkstra_result.1;

    // ========== Run Plain Dijkstra on CCH UP+DOWN graphs ==========
    // If this gives a better result than CCH query, the query algorithm is wrong
    let (cch_dijkstra_distance, cch_dijkstra_settled, cch_path) = run_cch_dijkstra_with_path(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        perm,
        src_filtered,
        dst_filtered,
    );

    // Check edge counts from source
    let src_up_start = mode_data.cch_topo.up_offsets[src_filtered as usize] as usize;
    let src_up_end = mode_data.cch_topo.up_offsets[src_filtered as usize + 1] as usize;
    let src_down_start = mode_data.cch_topo.down_offsets[src_filtered as usize] as usize;
    let src_down_end = mode_data.cch_topo.down_offsets[src_filtered as usize + 1] as usize;

    let tgt_up_incoming = count_incoming_up_edges(&mode_data.cch_topo, dst_filtered);
    let tgt_down_incoming = mode_data.down_rev.offsets[dst_filtered as usize + 1] - mode_data.down_rev.offsets[dst_filtered as usize];

    eprintln!("DEBUG COMPARE:");
    eprintln!("  Source {} (rank {}): {} UP edges, {} DOWN edges", src_filtered, src_rank, src_up_end - src_up_start, src_down_end - src_down_start);
    eprintln!("  Target {} (rank {}): {} incoming UP edges, {} incoming DOWN edges", dst_filtered, dst_rank, tgt_up_incoming, tgt_down_incoming);
    eprintln!("  CCH query: {:?}", cch_distance);
    eprintln!("  CCH Dijkstra (UP+DOWN): {:?}", cch_dijkstra_distance);
    eprintln!("  Plain Dijkstra (filtered EBG): {:?}", dijkstra_distance);

    // Verify down_rev structure: sample entries from target
    eprintln!("\n  Down_rev entries for target {}:", dst_filtered);
    let tgt_rev_start = mode_data.down_rev.offsets[dst_filtered as usize] as usize;
    let tgt_rev_end = mode_data.down_rev.offsets[dst_filtered as usize + 1] as usize;
    for i in tgt_rev_start..tgt_rev_end.min(tgt_rev_start + 5) {
        let src_node = mode_data.down_rev.sources[i];
        let edge_idx = mode_data.down_rev.edge_idx[i] as usize;
        let src_rank = perm[src_node as usize];
        let weight = mode_data.cch_weights.down[edge_idx];
        eprintln!("    {} (rank {}) → {} with weight {} (edge_idx {})", src_node, src_rank, dst_filtered, weight, edge_idx);
    }

    // Verify by looking at down edges TO target directly
    eprintln!("\n  Direct DOWN edges to target {}:", dst_filtered);
    let mut found = 0;
    for src_node in 0..mode_data.cch_topo.n_nodes {
        let start = mode_data.cch_topo.down_offsets[src_node as usize] as usize;
        let end = mode_data.cch_topo.down_offsets[src_node as usize + 1] as usize;
        for i in start..end {
            if mode_data.cch_topo.down_targets[i] == dst_filtered {
                let weight = mode_data.cch_weights.down[i];
                eprintln!("    {} (rank {}) → {} with weight {} (edge_idx {})", src_node, perm[src_node as usize], dst_filtered, weight, i);
                found += 1;
                if found >= 5 { break; }
            }
        }
        if found >= 5 { break; }
    }

    // Run separate UP-only and DOWN-only searches to verify
    eprintln!("\n  Running separate UP-only Dijkstra from source...");
    let up_only_dist = run_up_only_dijkstra(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        src_filtered,
    );
    let fwd_reachable = up_only_dist.iter().filter(|&&d| d != u32::MAX).count();
    eprintln!("    Reachable nodes via UP-only: {}", fwd_reachable);
    eprintln!("    dist_up[target={}] = {:?}", dst_filtered,
              if up_only_dist[dst_filtered as usize] == u32::MAX { "UNREACHABLE".to_string() }
              else { up_only_dist[dst_filtered as usize].to_string() });

    eprintln!("\n  Running separate DOWN-only Dijkstra to target...");
    let down_only_dist = run_down_only_to_target(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &mode_data.down_rev,
        dst_filtered,
    );
    let bwd_reachable = down_only_dist.iter().filter(|&&d| d != u32::MAX).count();
    eprintln!("    Reachable nodes via DOWN-only to target: {}", bwd_reachable);
    eprintln!("    dist_down[source={}] = {:?}", src_filtered,
              if down_only_dist[src_filtered as usize] == u32::MAX { "UNREACHABLE".to_string() }
              else { down_only_dist[src_filtered as usize].to_string() });

    // Find best meeting point manually
    let mut best_meet = u32::MAX;
    let mut best_meet_node = u32::MAX;
    for node in 0..mode_data.cch_topo.n_nodes {
        let d_up = up_only_dist[node as usize];
        let d_down = down_only_dist[node as usize];
        if d_up != u32::MAX && d_down != u32::MAX {
            let total = d_up.saturating_add(d_down);
            if total < best_meet {
                best_meet = total;
                best_meet_node = node;
            }
        }
    }
    eprintln!("    Best meeting: node {} with dist_up={} + dist_down={} = {}",
              best_meet_node,
              up_only_dist.get(best_meet_node as usize).unwrap_or(&u32::MAX),
              down_only_dist.get(best_meet_node as usize).unwrap_or(&u32::MAX),
              best_meet);

    // Analyze the CCH Dijkstra path and verify edge weights
    if !cch_path.is_empty() && cch_dijkstra_distance.is_some() {
        eprintln!("\n  CCH Dijkstra path analysis ({} nodes):", cch_path.len());

        // Verify path weights sum to distance
        let mut weight_sum = 0u32;
        let mut edge_details: Vec<String> = Vec::new();
        for i in 0..cch_path.len() - 1 {
            let u = cch_path[i] as usize;
            let v = cch_path[i + 1];
            let rank_u = perm[u];
            let rank_v = perm[v as usize];

            // Find the edge u→v in UP or DOWN graph
            let (edge_type, weight) = if rank_u < rank_v {
                // Should be in UP graph
                let start = mode_data.cch_topo.up_offsets[u] as usize;
                let end = mode_data.cch_topo.up_offsets[u + 1] as usize;
                let mut found_weight = None;
                for idx in start..end {
                    if mode_data.cch_topo.up_targets[idx] == v {
                        found_weight = Some(mode_data.cch_weights.up[idx]);
                        break;
                    }
                }
                ("UP", found_weight)
            } else {
                // Should be in DOWN graph
                let start = mode_data.cch_topo.down_offsets[u] as usize;
                let end = mode_data.cch_topo.down_offsets[u + 1] as usize;
                let mut found_weight = None;
                for idx in start..end {
                    if mode_data.cch_topo.down_targets[idx] == v {
                        found_weight = Some(mode_data.cch_weights.down[idx]);
                        break;
                    }
                }
                ("DOWN", found_weight)
            };

            match weight {
                Some(w) if w != u32::MAX => {
                    weight_sum = weight_sum.saturating_add(w);
                    if edge_details.len() < 5 {
                        edge_details.push(format!("  {}→{} ({}, w={})", u, v, edge_type, w));
                    }
                }
                Some(_) => {
                    edge_details.push(format!("  {}→{} ({}, w=MAX - BLOCKED!)", u, v, edge_type));
                }
                None => {
                    edge_details.push(format!("  {}→{} ({}, NOT FOUND!)", u, v, edge_type));
                }
            }
        }

        let reported = cch_dijkstra_distance.unwrap();
        eprintln!("    Reported distance: {}", reported);
        eprintln!("    Sum of edge weights: {}", weight_sum);
        if weight_sum != reported {
            eprintln!("    ⚠️ WEIGHT MISMATCH! Diff = {}", weight_sum as i64 - reported as i64);
        } else {
            eprintln!("    ✓ Weights match");
        }

        // Show first few edges
        if !edge_details.is_empty() {
            eprintln!("    First edges:");
            for detail in &edge_details {
                eprintln!("    {}", detail);
            }
        }

        // Count transitions
        let mut peaks = 0;
        let mut valleys = 0;
        let mut prev_rank = perm[cch_path[0] as usize];
        let mut going_up = true;
        for i in 1..cch_path.len() {
            let curr_rank = perm[cch_path[i] as usize];
            if going_up && curr_rank < prev_rank {
                // Was going up, now going down = peak
                peaks += 1;
                going_up = false;
            } else if !going_up && curr_rank > prev_rank {
                // Was going down, now going up = valley
                valleys += 1;
                going_up = true;
            } else if curr_rank > prev_rank {
                going_up = true;
            } else if curr_rank < prev_rank {
                going_up = false;
            }
            prev_rank = curr_rank;
        }
        eprintln!("    Peaks (up→down): {}, Valleys (down→up): {}", peaks, valleys);

        // For each valley, check if there's an UP shortcut that bypasses it
        if valleys > 0 {
            eprintln!("\n    Checking shortcuts at valleys:");
            prev_rank = perm[cch_path[0] as usize];
            going_up = true;
            let mut valley_count = 0;
            for i in 1..cch_path.len().saturating_sub(1) {
                let curr_rank = perm[cch_path[i] as usize];
                let next_rank = perm[cch_path[i+1] as usize];

                // Detect valley: was going down, now going up
                let was_going_down = prev_rank > curr_rank;
                let now_going_up = curr_rank < next_rank;

                if was_going_down && now_going_up && valley_count < 3 {
                    valley_count += 1;
                    let prev_node = cch_path[i-1] as usize;
                    let curr_node = cch_path[i] as usize;
                    let next_node = cch_path[i+1] as usize;

                    // Cost through valley
                    let down_edge_weight = find_edge_weight(&mode_data.cch_topo, &mode_data.cch_weights, prev_node, curr_node as u32, perm);
                    let up_edge_weight = find_edge_weight(&mode_data.cch_topo, &mode_data.cch_weights, curr_node, next_node as u32, perm);
                    let valley_cost = down_edge_weight.unwrap_or(u32::MAX).saturating_add(up_edge_weight.unwrap_or(u32::MAX));

                    // Check for direct UP shortcut from prev to next
                    let direct_up = if perm[prev_node] < perm[next_node] {
                        // Should be in UP graph
                        let start = mode_data.cch_topo.up_offsets[prev_node] as usize;
                        let end = mode_data.cch_topo.up_offsets[prev_node + 1] as usize;
                        let mut found = None;
                        for idx in start..end {
                            if mode_data.cch_topo.up_targets[idx] == next_node as u32 {
                                found = Some((mode_data.cch_weights.up[idx], mode_data.cch_topo.up_is_shortcut[idx]));
                                break;
                            }
                        }
                        found
                    } else {
                        None // Not an UP edge direction
                    };

                    eprintln!("      Valley {}: {} (rank {}) → {} (rank {}) → {} (rank {})",
                              valley_count, prev_node, perm[prev_node], curr_node, curr_rank, next_node, perm[next_node]);
                    eprintln!("        Through valley: {} + {} = {}", down_edge_weight.unwrap_or(0), up_edge_weight.unwrap_or(0), valley_cost);
                    match direct_up {
                        Some((w, is_shortcut)) => {
                            eprintln!("        Direct UP edge: w={}, shortcut={}", w, is_shortcut);
                            if w <= valley_cost {
                                eprintln!("        ✓ Shortcut is cheaper or equal - should be used!");
                            } else {
                                eprintln!("        Shortcut is more expensive (diff={})", w as i64 - valley_cost as i64);
                                // Show the middle node for this shortcut
                                if is_shortcut {
                                    let start = mode_data.cch_topo.up_offsets[prev_node] as usize;
                                    let end = mode_data.cch_topo.up_offsets[prev_node + 1] as usize;
                                    for idx in start..end {
                                        if mode_data.cch_topo.up_targets[idx] == next_node as u32 {
                                            let middle = mode_data.cch_topo.up_middle[idx];
                                            let middle_rank = perm[middle as usize];
                                            eprintln!("        Shortcut middle: {} (rank {})", middle, middle_rank);
                                            eprintln!("        Valley middle:   {} (rank {})", curr_node, curr_rank);
                                            // Compute expected shortcut weight
                                            let w_um = find_edge_weight(&mode_data.cch_topo, &mode_data.cch_weights, prev_node, middle, perm);
                                            let w_mv = find_edge_weight(&mode_data.cch_topo, &mode_data.cch_weights, middle as usize, next_node as u32, perm);
                                            eprintln!("        Shortcut path: w({}→{})={:?} + w({}→{})={:?}",
                                                      prev_node, middle, w_um, middle, next_node, w_mv);
                                            break;
                                        }
                                    }
                                }
                            }
                        }
                        None => eprintln!("        ✗ No direct UP edge exists"),
                    }
                }

                if curr_rank > prev_rank {
                    going_up = true;
                } else if curr_rank < prev_rank {
                    going_up = false;
                }
                prev_rank = curr_rank;
            }
        }
    }

    Json(serde_json::json!({
        "cch_distance": cch_distance,
        "cch_dijkstra_distance": cch_dijkstra_distance,
        "dijkstra_distance": dijkstra_distance,
        "cch_meeting_rank": cch_meeting_rank,
        "src_rank": src_rank,
        "dst_rank": dst_rank,
        "src_filtered": src_filtered,
        "dst_filtered": dst_filtered,
        "cch_fwd_settled": cch_fwd_settled,
        "cch_bwd_settled": cch_bwd_settled,
        "cch_dijkstra_settled": cch_dijkstra_settled,
        "dijkstra_settled": dijkstra_settled,
    })).into_response()
}

/// Count incoming UP edges to a node (edges v → u where rank(v) < rank(u))
fn count_incoming_up_edges(topo: &crate::formats::CchTopo, u: u32) -> usize {
    let n = topo.n_nodes as usize;
    let mut count = 0;
    // This is O(edges) but we only call it once for debugging
    for v in 0..n {
        let start = topo.up_offsets[v] as usize;
        let end = topo.up_offsets[v + 1] as usize;
        for i in start..end {
            if topo.up_targets[i] == u {
                count += 1;
            }
        }
    }
    count
}

/// Run Dijkstra on CCH UP+DOWN graphs combined
fn run_cch_dijkstra(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    src: u32,
    dst: u32,
) -> (Option<u32>, usize) {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
    let mut settled = 0usize;

    dist[src as usize] = 0;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        settled += 1;

        if u == dst {
            return (Some(d), settled);
        }

        // Relax UP edges
        let up_start = topo.up_offsets[u as usize] as usize;
        let up_end = topo.up_offsets[u as usize + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i];
            let w = weights.up[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }

        // Relax DOWN edges
        let down_start = topo.down_offsets[u as usize] as usize;
        let down_end = topo.down_offsets[u as usize + 1] as usize;
        for i in down_start..down_end {
            let v = topo.down_targets[i];
            let w = weights.down[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    (if dist[dst as usize] == u32::MAX { None } else { Some(dist[dst as usize]) }, settled)
}

/// Find edge weight in CCH graph
fn find_edge_weight(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    from: usize,
    to: u32,
    perm: &[u32],
) -> Option<u32> {
    let rank_from = perm[from];
    let rank_to = perm[to as usize];

    if rank_from < rank_to {
        // UP edge
        let start = topo.up_offsets[from] as usize;
        let end = topo.up_offsets[from + 1] as usize;
        for idx in start..end {
            if topo.up_targets[idx] == to {
                return Some(weights.up[idx]);
            }
        }
    } else {
        // DOWN edge
        let start = topo.down_offsets[from] as usize;
        let end = topo.down_offsets[from + 1] as usize;
        for idx in start..end {
            if topo.down_targets[idx] == to {
                return Some(weights.down[idx]);
            }
        }
    }
    None
}

/// Run Dijkstra on CCH UP+DOWN and return the path
fn run_cch_dijkstra_with_path(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    _perm: &[u32],
    src: u32,
    dst: u32,
) -> (Option<u32>, usize, Vec<u32>) {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut parent = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
    let mut settled = 0usize;

    dist[src as usize] = 0;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        settled += 1;

        if u == dst {
            // Reconstruct path
            let mut path = Vec::new();
            let mut curr = dst;
            while curr != u32::MAX {
                path.push(curr);
                curr = parent[curr as usize];
            }
            path.reverse();
            return (Some(d), settled, path);
        }

        // Relax UP edges
        let up_start = topo.up_offsets[u as usize] as usize;
        let up_end = topo.up_offsets[u as usize + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i];
            let w = weights.up[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                parent[v as usize] = u;
                pq.push(v, Reverse(new_dist));
            }
        }

        // Relax DOWN edges
        let down_start = topo.down_offsets[u as usize] as usize;
        let down_end = topo.down_offsets[u as usize + 1] as usize;
        for i in down_start..down_end {
            let v = topo.down_targets[i];
            let w = weights.down[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                parent[v as usize] = u;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    // Reconstruct path if destination was reached
    let path = if dist[dst as usize] != u32::MAX {
        let mut p = Vec::new();
        let mut curr = dst;
        while curr != u32::MAX {
            p.push(curr);
            curr = parent[curr as usize];
        }
        p.reverse();
        p
    } else {
        vec![]
    };

    (if dist[dst as usize] == u32::MAX { None } else { Some(dist[dst as usize]) }, settled, path)
}

/// Run Dijkstra using only UP edges from source
fn run_up_only_dijkstra(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    src: u32,
) -> Vec<u32> {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();

    dist[src as usize] = 0;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        // Only relax UP edges
        let up_start = topo.up_offsets[u as usize] as usize;
        let up_end = topo.up_offsets[u as usize + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i];
            let w = weights.up[i];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    dist
}

/// Run reverse Dijkstra using only DOWN edges to reach target
/// Returns dist[node] = shortest DOWN-only path from node to target
fn run_down_only_to_target(
    topo: &crate::formats::CchTopo,
    weights: &super::state::CchWeights,
    down_rev: &super::state::DownReverseAdj,
    dst: u32,
) -> Vec<u32> {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();

    dist[dst as usize] = 0;
    pq.push(dst, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        // Traverse reversed DOWN edges: for each DOWN edge x→u, update dist[x]
        let start = down_rev.offsets[u as usize] as usize;
        let end = down_rev.offsets[u as usize + 1] as usize;
        for i in start..end {
            let x = down_rev.sources[i];
            let edge_idx = down_rev.edge_idx[i] as usize;
            let w = weights.down[edge_idx];
            if w == u32::MAX {
                continue;
            }
            let new_dist = d.saturating_add(w);
            if new_dist < dist[x as usize] {
                dist[x as usize] = new_dist;
                pq.push(x, Reverse(new_dist));
            }
        }
    }

    dist
}

/// Run plain Dijkstra on filtered EBG (without CCH, using node weights only - no turn costs)
fn run_filtered_dijkstra(
    filtered_ebg: &crate::formats::FilteredEbg,
    node_weights: &[u32],
    src: u32,
    dst: u32,
) -> (Option<u32>, usize) {
    use priority_queue::PriorityQueue;
    use std::cmp::Reverse;

    let n = filtered_ebg.n_filtered_nodes as usize;
    let mut dist = vec![u32::MAX; n];
    let mut pq: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::new();
    let mut settled = 0usize;

    dist[src as usize] = 0;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        if d > dist[u as usize] {
            continue;
        }

        settled += 1;

        if u == dst {
            return (Some(d), settled);
        }

        // Relax all outgoing edges in filtered EBG
        let start = filtered_ebg.offsets[u as usize] as usize;
        let end = filtered_ebg.offsets[u as usize + 1] as usize;

        for i in start..end {
            let v = filtered_ebg.heads[i];
            // Get weight of target node (in original EBG space)
            let v_orig = filtered_ebg.filtered_to_original[v as usize];
            let w = node_weights[v_orig as usize];

            if w == 0 {
                continue; // Inaccessible
            }

            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(v, Reverse(new_dist));
            }
        }
    }

    (if dist[dst as usize] == u32::MAX { None } else { Some(dist[dst as usize]) }, settled)
}
