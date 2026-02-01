//! HTTP API handlers with Axum and Utoipa

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Path, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use tokio_stream::StreamExt;
use tower_http::cors::{Any, CorsLayer};
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::profile_abi::Mode;
use crate::matrix::bucket_ch::{table_bucket_parallel, table_bucket_full_flat, forward_build_buckets, backward_join_with_buckets};
use crate::matrix::arrow_stream::{MatrixTile, tiles_to_record_batch, record_batch_to_bytes, ARROW_STREAM_CONTENT_TYPE};

use super::geometry::{build_geometry, build_isochrone_geometry, Point, RouteGeometry};
use super::query::CchQuery;
use super::state::ServerState;
use super::unpack::unpack_path;

/// OpenAPI documentation
#[derive(OpenApi)]
#[openapi(
    paths(route, table_osrm, table_post, isochrone, health),
    components(schemas(
        RouteRequest,
        RouteResponse,
        TablePostRequest,
        TableResponse,
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

    // Streaming endpoint needs larger body limit for 50k+ coordinates
    let stream_route = Router::new()
        .route("/table/stream", post(table_stream))
        .layer(DefaultBodyLimit::max(256 * 1024 * 1024)); // 256MB limit

    Router::new()
        .merge(SwaggerUi::new("/swagger-ui").url("/api-docs/openapi.json", ApiDoc::openapi()))
        .route("/route", get(route))
        // OSRM-compatible table endpoint
        .route("/table/v1/:profile/*coords", get(table_osrm))
        // POST alternative for table (easier for large coordinate lists)
        .route("/table", post(table_post))
        // Arrow streaming for large matrices (50k×50k+)
        .merge(stream_route)
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

    // Convert to rank space (with rank-aligned CCH)
    let src_rank = mode_data.order.perm[src_filtered as usize];
    let dst_rank = mode_data.order.perm[dst_filtered as usize];

    // Run query (in rank space)
    let query = CchQuery::new(&state, mode);
    let result = match query.query(src_rank, dst_rank) {
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

    // Unpack path (in rank space)
    let rank_path = unpack_path(
        &mode_data.cch_topo,
        &result.forward_parent,
        &result.backward_parent,
        src_rank,
        dst_rank,
        result.meeting_node,
    );

    // Convert path from rank to filtered to original EBG node IDs for geometry
    let ebg_path: Vec<u32> = rank_path
        .iter()
        .map(|&rank| {
            let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            mode_data.filtered_ebg.filtered_to_original[filtered_id as usize]
        })
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

// ============ Table Endpoint (OSRM-compatible) ============

/// Query parameters for OSRM-style table endpoint
#[derive(Debug, Deserialize)]
pub struct TableQueryParams {
    /// Source indices (semicolon-separated, e.g., "0;1;2"). If omitted, all coordinates are sources.
    sources: Option<String>,
    /// Destination indices (semicolon-separated). If omitted, all coordinates are destinations.
    destinations: Option<String>,
}

/// POST request for table computation
#[derive(Debug, Deserialize, ToSchema)]
pub struct TablePostRequest {
    /// Source coordinates [[lon, lat], ...]
    #[schema(example = json!([[4.3517, 50.8503], [4.4017, 50.8603]]))]
    pub sources: Vec<[f64; 2]>,
    /// Destination coordinates [[lon, lat], ...]
    #[schema(example = json!([[4.3817, 50.8553], [4.4217, 50.8653]]))]
    pub destinations: Vec<[f64; 2]>,
    /// Transport mode: car, bike, or foot
    #[schema(example = "car")]
    pub mode: String,
}

/// Response for table computation (OSRM-compatible format)
#[derive(Debug, Serialize, ToSchema)]
pub struct TableResponse {
    /// Status code (always "Ok" on success)
    pub code: String,
    /// Row-major matrix of durations in seconds (null if unreachable)
    pub durations: Vec<Vec<Option<f64>>>,
    /// Source waypoints with snapped locations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<Waypoint>>,
    /// Destination waypoints with snapped locations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destinations: Option<Vec<Waypoint>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct Waypoint {
    /// Snapped location [lon, lat]
    pub location: [f64; 2],
    /// Name (empty for now)
    pub name: String,
}

/// OSRM-compatible table endpoint: GET /table/v1/{profile}/{coordinates}
///
/// Computes a distance matrix using bucket many-to-many algorithm.
/// Coordinates are semicolon-separated "lon,lat" pairs.
///
/// Example: GET /table/v1/car/4.35,50.85;4.40,50.86;4.38,50.84?sources=0&destinations=1;2
#[utoipa::path(
    get,
    path = "/table/v1/{profile}/{coords}",
    params(
        ("profile" = String, Path, description = "Transport mode: car, bike, foot"),
        ("coords" = String, Path, description = "Semicolon-separated lon,lat pairs"),
        ("sources" = Option<String>, Query, description = "Source indices (semicolon-separated)"),
        ("destinations" = Option<String>, Query, description = "Destination indices (semicolon-separated)"),
    ),
    responses(
        (status = 200, description = "Table computed", body = TableResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn table_osrm(
    State(state): State<Arc<ServerState>>,
    Path((profile, coords)): Path<(String, String)>,
    Query(params): Query<TableQueryParams>,
) -> impl IntoResponse {
    let mode = match parse_mode(&profile) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    // Parse coordinates: "lon,lat;lon,lat;..."
    let all_coords: Vec<(f64, f64)> = coords
        .split(';')
        .filter_map(|s| {
            let parts: Vec<&str> = s.split(',').collect();
            if parts.len() == 2 {
                let lon = parts[0].trim().parse().ok()?;
                let lat = parts[1].trim().parse().ok()?;
                Some((lon, lat))
            } else {
                None
            }
        })
        .collect();

    if all_coords.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: "No valid coordinates provided".into()
        })).into_response();
    }

    // Parse source/destination indices
    let source_indices: Vec<usize> = match &params.sources {
        Some(s) if !s.is_empty() => s.split(';').filter_map(|x| x.trim().parse().ok()).collect(),
        _ => (0..all_coords.len()).collect(), // all coordinates are sources
    };
    let dest_indices: Vec<usize> = match &params.destinations {
        Some(s) if !s.is_empty() => s.split(';').filter_map(|x| x.trim().parse().ok()).collect(),
        _ => (0..all_coords.len()).collect(), // all coordinates are destinations
    };

    // Extract source and destination coordinates
    let sources: Vec<[f64; 2]> = source_indices.iter()
        .filter_map(|&i| all_coords.get(i).map(|&(lon, lat)| [lon, lat]))
        .collect();
    let destinations: Vec<[f64; 2]> = dest_indices.iter()
        .filter_map(|&i| all_coords.get(i).map(|&(lon, lat)| [lon, lat]))
        .collect();

    if sources.is_empty() || destinations.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: "Invalid source or destination indices".into()
        })).into_response();
    }

    // Compute table using bucket M2M
    compute_table_bucket_m2m(&state, mode, &sources, &destinations).await
}

/// POST /table - Alternative table endpoint for easier large coordinate lists
#[utoipa::path(
    post,
    path = "/table",
    request_body = TablePostRequest,
    responses(
        (status = 200, description = "Table computed", body = TableResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn table_post(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<TablePostRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    if req.sources.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: "sources cannot be empty".into()
        })).into_response();
    }
    if req.destinations.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: "destinations cannot be empty".into()
        })).into_response();
    }

    compute_table_bucket_m2m(&state, mode, &req.sources, &req.destinations).await
}

/// Core table computation using bucket M2M algorithm
async fn compute_table_bucket_m2m(
    state: &Arc<ServerState>,
    mode: Mode,
    sources: &[[f64; 2]],
    destinations: &[[f64; 2]],
) -> Response {
    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // Snap sources to graph nodes and convert to RANK space
    // The bucket M2M algorithm operates on rank positions (CCH is rank-aligned)
    let mut sources_rank: Vec<u32> = Vec::with_capacity(sources.len());
    let mut source_waypoints: Vec<Waypoint> = Vec::with_capacity(sources.len());
    let mut source_valid: Vec<bool> = Vec::with_capacity(sources.len());

    for [lon, lat] in sources {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &mode_data.mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                // Convert filtered ID to rank position (same as /route endpoint)
                let rank = mode_data.order.perm[filtered as usize];
                sources_rank.push(rank);
                source_valid.push(true);
                // Get snapped location from EBG node
                let snapped = get_node_location(state, orig_id);
                source_waypoints.push(Waypoint { location: snapped, name: String::new() });
            } else {
                sources_rank.push(0);
                source_valid.push(false);
                source_waypoints.push(Waypoint { location: [*lon, *lat], name: String::new() });
            }
        } else {
            sources_rank.push(0);
            source_valid.push(false);
            source_waypoints.push(Waypoint { location: [*lon, *lat], name: String::new() });
        }
    }

    // Snap destinations to graph nodes and convert to RANK space
    let mut targets_rank: Vec<u32> = Vec::with_capacity(destinations.len());
    let mut dest_waypoints: Vec<Waypoint> = Vec::with_capacity(destinations.len());
    let mut target_valid: Vec<bool> = Vec::with_capacity(destinations.len());

    for [lon, lat] in destinations {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &mode_data.mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                // Convert filtered ID to rank position
                let rank = mode_data.order.perm[filtered as usize];
                targets_rank.push(rank);
                target_valid.push(true);
                let snapped = get_node_location(state, orig_id);
                dest_waypoints.push(Waypoint { location: snapped, name: String::new() });
            } else {
                targets_rank.push(0);
                target_valid.push(false);
                dest_waypoints.push(Waypoint { location: [*lon, *lat], name: String::new() });
            }
        } else {
            targets_rank.push(0);
            target_valid.push(false);
            dest_waypoints.push(Waypoint { location: [*lon, *lat], name: String::new() });
        }
    }

    // Run bucket M2M algorithm
    // Use sequential for small matrices (< 2500 cells) to avoid parallel overhead
    // Use parallel for large matrices where thread amortization helps
    let (matrix, _stats) = if sources_rank.len() * targets_rank.len() < 2500 {
        table_bucket_full_flat(
            n_nodes,
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            &sources_rank,
            &targets_rank,
        )
    } else {
        table_bucket_parallel(
            n_nodes,
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            &sources_rank,
            &targets_rank,
        )
    };

    // Convert flat matrix to 2D array with nulls for invalid/unreachable
    let n_sources = sources.len();
    let n_targets = destinations.len();
    let mut durations: Vec<Vec<Option<f64>>> = Vec::with_capacity(n_sources);

    for src_idx in 0..n_sources {
        let mut row: Vec<Option<f64>> = Vec::with_capacity(n_targets);
        for tgt_idx in 0..n_targets {
            if !source_valid[src_idx] || !target_valid[tgt_idx] {
                row.push(None);
            } else {
                let dist = matrix[src_idx * n_targets + tgt_idx];
                if dist == u32::MAX {
                    row.push(None);
                } else {
                    // Convert deciseconds to seconds
                    row.push(Some(dist as f64 / 10.0));
                }
            }
        }
        durations.push(row);
    }

    Json(TableResponse {
        code: "Ok".into(),
        durations,
        sources: Some(source_waypoints),
        destinations: Some(dest_waypoints),
    }).into_response()
}

// ============ Arrow Streaming Table Endpoint ============

/// Request for streaming table computation
#[derive(Debug, Deserialize, ToSchema)]
pub struct TableStreamRequest {
    /// Source coordinates [[lon, lat], ...]
    pub sources: Vec<[f64; 2]>,
    /// Destination coordinates [[lon, lat], ...]
    pub destinations: Vec<[f64; 2]>,
    /// Transport mode: car, bike, or foot
    pub mode: String,
    /// Tile size for sources (default 1000)
    #[serde(default = "default_tile_size")]
    pub src_tile_size: usize,
    /// Tile size for destinations (default 1000)
    #[serde(default = "default_tile_size")]
    pub dst_tile_size: usize,
}

fn default_tile_size() -> usize { 1000 }

/// Arrow streaming endpoint for large matrices
///
/// Computes distance matrix in tiles and streams results as Apache Arrow IPC.
/// Use this for matrices larger than 10k×10k where JSON would be too large.
///
/// Response: Arrow IPC stream with tiles containing:
/// - src_block_start, dst_block_start: tile offsets
/// - src_block_len, dst_block_len: tile dimensions
/// - durations_ms: packed u32 distances in milliseconds
async fn table_stream(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<TableStreamRequest>,
) -> impl IntoResponse {
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    };

    if req.sources.is_empty() || req.destinations.is_empty() {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse {
            error: "sources and destinations cannot be empty".into()
        })).into_response();
    }

    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // Convert all sources to rank space, keeping track of valid indices
    let mut sources_rank: Vec<u32> = Vec::with_capacity(req.sources.len());
    let mut valid_src_indices: Vec<usize> = Vec::with_capacity(req.sources.len());
    for (i, [lon, lat]) in req.sources.iter().enumerate() {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &mode_data.mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                let rank = mode_data.order.perm[filtered as usize];
                sources_rank.push(rank);
                valid_src_indices.push(i);
            }
        }
    }

    // Convert all destinations to rank space
    let mut targets_rank: Vec<u32> = Vec::with_capacity(req.destinations.len());
    let mut valid_dst_indices: Vec<usize> = Vec::with_capacity(req.destinations.len());
    for (i, [lon, lat]) in req.destinations.iter().enumerate() {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &mode_data.mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                let rank = mode_data.order.perm[filtered as usize];
                targets_rank.push(rank);
                valid_dst_indices.push(i);
            }
        }
    }

    let n_valid_sources = sources_rank.len();
    let n_valid_targets = targets_rank.len();
    let n_total_sources = req.sources.len();
    let n_total_targets = req.destinations.len();
    let src_tile_size = req.src_tile_size.min(n_total_sources).max(1);
    let dst_tile_size = req.dst_tile_size.min(n_total_targets).max(1);

    // Create channel for streaming tiles
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(8);

    // Clone what we need for the spawned task
    let up_adj_flat = mode_data.up_adj_flat.clone();
    let down_rev_flat = mode_data.down_rev_flat.clone();

    // Spawn compute task - SOURCE-BLOCK OUTER LOOP to avoid repeated forward computation
    // For 10k×10k with 1000×1000 tiles: forward computed 10x (once per src block) instead of 100x
    tokio::task::spawn_blocking(move || {
        // Generate source and destination blocks
        let src_blocks: Vec<(usize, usize)> = (0..n_total_sources)
            .step_by(src_tile_size)
            .map(|start| (start, (start + src_tile_size).min(n_total_sources)))
            .collect();

        let dst_blocks: Vec<(usize, usize)> = (0..n_total_targets)
            .step_by(dst_tile_size)
            .map(|start| (start, (start + dst_tile_size).min(n_total_targets)))
            .collect();

        // Process source blocks in parallel (forward computed ONCE per source block)
        src_blocks.par_iter().for_each(|&(src_start, src_end)| {
            let tile_rows = src_end - src_start;

            // Extract sources for this block
            let mut block_src_ranks: Vec<u32> = Vec::new();
            let mut block_src_orig_indices: Vec<usize> = Vec::new();

            for (valid_idx, &orig_idx) in valid_src_indices.iter().enumerate() {
                if orig_idx >= src_start && orig_idx < src_end {
                    block_src_ranks.push(sources_rank[valid_idx]);
                    block_src_orig_indices.push(orig_idx);
                }
            }

            if block_src_ranks.is_empty() {
                // No valid sources in this block - emit empty tiles for all dst blocks
                for &(dst_start, dst_end) in &dst_blocks {
                    let tile_cols = dst_end - dst_start;
                    let durations_ms = vec![u32::MAX; tile_rows * tile_cols];
                    let tile = MatrixTile::from_flat(
                        src_start as u32,
                        dst_start as u32,
                        tile_rows as u16,
                        tile_cols as u16,
                        &durations_ms,
                    );
                    if let Ok(batch) = tiles_to_record_batch(&[tile]) {
                        if let Ok(bytes) = record_batch_to_bytes(&batch) {
                            let _ = tx.blocking_send(Ok(bytes));
                        }
                    }
                }
                return;
            }

            // FORWARD PHASE: Compute forward searches ONCE for this source block
            let source_buckets = std::sync::Arc::new(forward_build_buckets(n_nodes, &up_adj_flat, &block_src_ranks));

            // BACKWARD PHASE: Process destination blocks in parallel
            // This maintains high parallelism while avoiding repeated forward work
            dst_blocks.par_iter().for_each(|&(dst_start, dst_end)| {
                let source_buckets = source_buckets.clone();
                let tile_cols = dst_end - dst_start;

                // Extract destinations for this block
                let mut block_dst_ranks: Vec<u32> = Vec::new();
                let mut block_dst_orig_indices: Vec<usize> = Vec::new();

                for (valid_idx, &orig_idx) in valid_dst_indices.iter().enumerate() {
                    if orig_idx >= dst_start && orig_idx < dst_end {
                        block_dst_ranks.push(targets_rank[valid_idx]);
                        block_dst_orig_indices.push(orig_idx);
                    }
                }

                // Build output tile
                let mut durations_ms = vec![u32::MAX; tile_rows * tile_cols];

                if !block_dst_ranks.is_empty() {
                    // BACKWARD + JOIN using prebuilt source buckets
                    let tile_matrix = backward_join_with_buckets(
                        n_nodes,
                        &down_rev_flat,
                        &source_buckets,
                        &block_dst_ranks,
                    );

                    // Map computed distances to output positions
                    for (tile_src_idx, &orig_src_idx) in block_src_orig_indices.iter().enumerate() {
                        let out_row = orig_src_idx - src_start;

                        for (tile_dst_idx, &orig_dst_idx) in block_dst_orig_indices.iter().enumerate() {
                            let out_col = orig_dst_idx - dst_start;
                            let d = tile_matrix[tile_src_idx * block_dst_ranks.len() + tile_dst_idx];
                            durations_ms[out_row * tile_cols + out_col] =
                                if d == u32::MAX { u32::MAX } else { d * 100 };
                        }
                    }
                }

                let tile = MatrixTile::from_flat(
                    src_start as u32,
                    dst_start as u32,
                    tile_rows as u16,
                    tile_cols as u16,
                    &durations_ms,
                );

                // Stream this tile immediately
                if let Ok(batch) = tiles_to_record_batch(&[tile]) {
                    if let Ok(bytes) = record_batch_to_bytes(&batch) {
                        let _ = tx.blocking_send(Ok(bytes));
                    }
                }
            });  // end dst_blocks.par_iter()
        });  // end src_blocks.par_iter()
    });

    // Convert receiver to stream
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
        .body(Body::from_stream(stream))
        .unwrap()
        .into_response()
}

/// Get the location (lon, lat) of an EBG node
fn get_node_location(state: &ServerState, node_id: u32) -> [f64; 2] {
    let node = &state.ebg_nodes.nodes[node_id as usize];
    // EBG node has geom_idx pointing to NBG edge index
    let edge_idx = node.geom_idx as usize;
    // Polylines are indexed by edge index (same order as edges)
    if edge_idx < state.nbg_geo.polylines.len() {
        let polyline = &state.nbg_geo.polylines[edge_idx];
        if !polyline.lon_fxp.is_empty() {
            return [
                polyline.lon_fxp[0] as f64 / 1e7,
                polyline.lat_fxp[0] as f64 / 1e7,
            ];
        }
    }
    [0.0, 0.0]
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

    // Convert to filtered space, then to rank space
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
    let center_rank = mode_data.order.perm[center_filtered as usize];

    // Run PHAST bounded query (in rank space) - uses thread-local state
    // Returns Vec<(rank, dist)> of settled nodes only
    let phast_settled = run_phast_bounded_fast(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        center_rank,
        time_ds,
        mode,
    );

    // Convert settled nodes from rank to original EBG node IDs for geometry
    let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
    for (rank, dist) in phast_settled {
        // rank -> filtered -> original
        let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        let original_id = mode_data.filtered_ebg.filtered_to_original[filtered_id as usize];
        settled.push((original_id, dist));
    }

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

// =============================================================================
// THREAD-LOCAL PHAST STATE (eliminates 9.6MB memset per query)
// =============================================================================

use std::cell::RefCell;

/// Block size for block-gated downward scan
/// Each block contains BLOCK_SIZE consecutive ranks
const PHAST_BLOCK_SIZE: usize = 4096;

/// Thread-local PHAST state with generation stamping and block gating
/// Eliminates O(n) initialization per query by using version stamps
/// Block gating skips large portions of the graph in downward phase
struct PhastState {
    /// Distance array (persistent across queries)
    dist: Vec<u32>,
    /// Version stamp per node (marks which generation set the distance)
    version: Vec<u32>,
    /// Version stamp per block (marks which blocks have active nodes)
    block_active: Vec<u32>,
    /// Number of blocks
    n_blocks: usize,
    /// Current generation (incremented per query)
    current_gen: u32,
    /// Priority queue (reused across queries)
    pq: std::collections::BinaryHeap<std::cmp::Reverse<(u32, u32)>>,
}

impl PhastState {
    fn new(n_nodes: usize) -> Self {
        let n_blocks = (n_nodes + PHAST_BLOCK_SIZE - 1) / PHAST_BLOCK_SIZE;
        Self {
            dist: vec![u32::MAX; n_nodes],
            version: vec![0; n_nodes],
            block_active: vec![0; n_blocks],
            n_blocks,
            current_gen: 0,
            pq: std::collections::BinaryHeap::with_capacity(n_nodes / 100),
        }
    }

    /// Start a new query (O(1) instead of O(n))
    #[inline]
    fn start_query(&mut self) {
        self.current_gen = self.current_gen.wrapping_add(1);
        if self.current_gen == 0 {
            // Overflow - reset all versions (rare, every ~4B queries)
            self.version.iter_mut().for_each(|v| *v = 0);
            self.block_active.iter_mut().for_each(|v| *v = 0);
            self.current_gen = 1;
        }
        self.pq.clear();
    }

    /// Get distance (returns MAX if not set this query)
    #[inline]
    fn get_dist(&self, node: usize) -> u32 {
        if self.version[node] == self.current_gen {
            self.dist[node]
        } else {
            u32::MAX
        }
    }

    /// Set distance (also marks version and block as active)
    #[inline]
    fn set_dist(&mut self, node: usize, dist: u32) {
        self.dist[node] = dist;
        self.version[node] = self.current_gen;
        // Mark block as active
        let block_idx = node / PHAST_BLOCK_SIZE;
        self.block_active[block_idx] = self.current_gen;
    }

    /// Check if a block is active this query
    #[inline]
    fn is_block_active(&self, block_idx: usize) -> bool {
        self.block_active[block_idx] == self.current_gen
    }
}

thread_local! {
    /// Thread-local PHAST state for car mode (most common)
    static PHAST_STATE_CAR: RefCell<Option<PhastState>> = const { RefCell::new(None) };
    /// Thread-local PHAST state for bike mode
    static PHAST_STATE_BIKE: RefCell<Option<PhastState>> = const { RefCell::new(None) };
    /// Thread-local PHAST state for foot mode
    static PHAST_STATE_FOOT: RefCell<Option<PhastState>> = const { RefCell::new(None) };
}

/// Run PHAST bounded query using thread-local state
/// Returns Vec<(rank, dist)> of settled nodes only - avoids 9.6MB output allocation
fn run_phast_bounded_fast(
    cch_topo: &crate::formats::CchTopo,
    cch_weights: &super::state::CchWeights,
    origin_rank: u32,
    threshold: u32,
    mode: crate::profile_abi::Mode,
) -> Vec<(u32, u32)> {
    use std::cmp::Reverse;
    use crate::profile_abi::Mode;

    let n_nodes = cch_topo.n_nodes as usize;

    // Get thread-local state for this mode
    let state_cell = match mode {
        Mode::Car => &PHAST_STATE_CAR,
        Mode::Bike => &PHAST_STATE_BIKE,
        Mode::Foot => &PHAST_STATE_FOOT,
    };

    state_cell.with(|cell| {
        let mut state_opt = cell.borrow_mut();

        // Initialize if needed (only first query per thread)
        if state_opt.is_none() {
            *state_opt = Some(PhastState::new(n_nodes));
        }
        let state = state_opt.as_mut().unwrap();

        // Verify size matches (in case different datasets)
        if state.dist.len() != n_nodes {
            *state = PhastState::new(n_nodes);
        }

        // Start new query (O(1) instead of O(n) memset)
        state.start_query();
        state.set_dist(origin_rank as usize, 0);

        // Track settled nodes during upward phase
        let mut upward_settled: Vec<u32> = Vec::with_capacity(n_nodes / 100);

        // Phase 1: Upward search (PQ-based, UP edges only)
        state.pq.push(Reverse((0, origin_rank)));

        while let Some(Reverse((d, u))) = state.pq.pop() {
            if d > threshold {
                break;
            }

            if d > state.get_dist(u as usize) {
                continue; // Stale entry
            }

            upward_settled.push(u);

            let up_start = cch_topo.up_offsets[u as usize] as usize;
            let up_end = cch_topo.up_offsets[u as usize + 1] as usize;

            for i in up_start..up_end {
                let v = cch_topo.up_targets[i] as usize;
                let w = cch_weights.up[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d.saturating_add(w);
                if new_dist < state.get_dist(v) {
                    state.set_dist(v, new_dist);
                    state.pq.push(Reverse((new_dist, v as u32)));
                }
            }
        }

        // Phase 2: Block-gated downward scan (linear, DOWN edges only)
        // Only scan blocks that have active nodes - huge savings for bounded queries
        // Process blocks in reverse order (highest rank first)
        for block_idx in (0..state.n_blocks).rev() {
            // Skip blocks with no active nodes
            if !state.is_block_active(block_idx) {
                continue;
            }

            // Process nodes in this block in reverse rank order
            let block_start = block_idx * PHAST_BLOCK_SIZE;
            let block_end = ((block_idx + 1) * PHAST_BLOCK_SIZE).min(n_nodes);

            for rank in (block_start..block_end).rev() {
                let d_u = state.get_dist(rank);

                if d_u == u32::MAX || d_u > threshold {
                    continue;
                }

                let down_start = cch_topo.down_offsets[rank] as usize;
                let down_end = cch_topo.down_offsets[rank + 1] as usize;

                for i in down_start..down_end {
                    let v = cch_topo.down_targets[i] as usize;
                    let w = cch_weights.down[i];

                    if w == u32::MAX {
                        continue;
                    }

                    let new_dist = d_u.saturating_add(w);
                    if new_dist < state.get_dist(v) {
                        // set_dist marks the target block as active too
                        state.set_dist(v, new_dist);
                    }
                }
            }
        }

        // Collect settled nodes (only those within threshold)
        // Only scan active blocks - much faster than full n_nodes scan
        let mut result: Vec<(u32, u32)> = Vec::with_capacity(n_nodes / 10);
        for block_idx in 0..state.n_blocks {
            if !state.is_block_active(block_idx) {
                continue;
            }
            let block_start = block_idx * PHAST_BLOCK_SIZE;
            let block_end = ((block_idx + 1) * PHAST_BLOCK_SIZE).min(n_nodes);
            for rank in block_start..block_end {
                if state.version[rank] == state.current_gen {
                    let d = state.dist[rank];
                    if d <= threshold {
                        result.push((rank as u32, d));
                    }
                }
            }
        }
        result
    })
}

/// Run PHAST bounded query (in rank space) - LEGACY version with per-query allocation
///
/// PHAST is a two-phase algorithm:
/// 1. Upward phase: PQ-based Dijkstra using only UP edges from origin
/// 2. Downward phase: Linear scan in reverse rank order, relaxing DOWN edges
///
/// With rank-aligned CCH, node_id == rank, so no inv_perm lookup is needed
/// in the downward phase, giving excellent cache efficiency.
#[allow(dead_code)]
fn run_phast_bounded(
    cch_topo: &crate::formats::CchTopo,
    cch_weights: &super::state::CchWeights,
    origin_rank: u32,
    threshold: u32,
) -> Vec<u32> {
    use std::cmp::Reverse;
    use std::collections::BinaryHeap;

    let n_nodes = cch_topo.n_nodes as usize;
    let mut dist = vec![u32::MAX; n_nodes];
    dist[origin_rank as usize] = 0;

    // Phase 1: Upward search (PQ-based, UP edges only)
    let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();
    pq.push(Reverse((0, origin_rank)));

    while let Some(Reverse((d, u))) = pq.pop() {
        // Early stop: if current min distance exceeds threshold, stop
        if d > threshold {
            break;
        }

        if d > dist[u as usize] {
            continue; // Stale entry
        }

        // Relax UP edges only
        let up_start = cch_topo.up_offsets[u as usize] as usize;
        let up_end = cch_topo.up_offsets[u as usize + 1] as usize;

        for i in up_start..up_end {
            let v = cch_topo.up_targets[i];
            let w = cch_weights.up[i];

            if w == u32::MAX {
                continue;
            }

            let new_dist = d.saturating_add(w);
            if new_dist < dist[v as usize] {
                dist[v as usize] = new_dist;
                pq.push(Reverse((new_dist, v)));
            }
        }
    }

    // Phase 2: Downward scan (linear, DOWN edges only)
    // Process nodes in DECREASING rank order (highest rank first)
    // With rank-aligned CCH: u = rank (no inv_perm lookup needed)
    for rank in (0..n_nodes).rev() {
        let u = rank;
        let d_u = dist[u];

        // Skip unreachable nodes or nodes beyond threshold
        if d_u == u32::MAX || d_u > threshold {
            continue;
        }

        // Relax DOWN edges
        let down_start = cch_topo.down_offsets[u] as usize;
        let down_end = cch_topo.down_offsets[u + 1] as usize;

        for i in down_start..down_end {
            let v = cch_topo.down_targets[i] as usize;
            let w = cch_weights.down[i];

            if w == u32::MAX {
                continue;
            }

            let new_dist = d_u.saturating_add(w);
            if new_dist < dist[v] {
                dist[v] = new_dist;
            }
        }
    }

    dist
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
