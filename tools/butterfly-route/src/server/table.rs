//! /table and /table/stream handlers — distance/duration matrix computation

use axum::{
    body::Body,
    extract::State,
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    Json,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use utoipa::ToSchema;

use crate::matrix::arrow_stream::{
    record_batch_to_bytes, tiles_to_record_batch, MatrixTile, ARROW_STREAM_CONTENT_TYPE,
};
use crate::matrix::bucket_ch::{
    backward_join_with_buckets, forward_build_buckets, table_bucket_full_flat,
    table_bucket_parallel, DownReverseAdjFlat, UpAdjFlat,
};
use crate::profile_abi::Mode;

use super::state::ServerState;
use super::types::{get_node_location, parse_mode, validate_coord, ErrorResponse, Waypoint};

// ============ Types ============

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
    /// Annotations to return: "duration" (default), "distance", or "duration,distance"
    #[serde(default = "default_annotations")]
    #[schema(example = "duration,distance")]
    pub annotations: String,
    /// Exclude road types: comma-separated list of "toll", "ferry", "motorway"
    #[serde(default)]
    pub exclude: Option<String>,
    /// Avoid polygon(s) as JSON array of coordinate rings
    #[serde(default)]
    pub avoid_polygons: Option<String>,
}

pub fn default_annotations() -> String {
    "duration".to_string()
}

/// Response for table computation (OSRM-compatible format)
#[derive(Debug, Serialize, ToSchema)]
pub struct TableResponse {
    /// Status code (always "Ok" on success)
    pub code: String,
    /// Row-major matrix of durations in seconds (null if unreachable)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durations: Option<Vec<Vec<Option<f64>>>>,
    /// Row-major matrix of distances in meters (null if unreachable)
    /// Distances represent shortest-distance routes (independent of time optimization)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distances: Option<Vec<Vec<Option<f64>>>>,
    /// Source waypoints with snapped locations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sources: Option<Vec<Waypoint>>,
    /// Destination waypoints with snapped locations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destinations: Option<Vec<Waypoint>>,
}

/// Request for streaming table computation
#[derive(Debug, Deserialize, ToSchema)]
pub struct TableStreamRequest {
    /// Source coordinates [[lon, lat], ...]
    #[schema(example = json!([[4.3517, 50.8503], [4.3617, 50.8553]]))]
    pub sources: Vec<[f64; 2]>,
    /// Destination coordinates [[lon, lat], ...]
    #[schema(example = json!([[4.4017, 50.8603], [4.4117, 50.8653]]))]
    pub destinations: Vec<[f64; 2]>,
    /// Transport mode: car, bike, or foot
    #[schema(example = "car")]
    pub mode: String,
    /// Tile size for sources (default 1000)
    #[serde(default = "default_tile_size")]
    #[schema(example = 1000)]
    pub src_tile_size: usize,
    /// Tile size for destinations (default 1000)
    #[serde(default = "default_tile_size")]
    #[schema(example = 1000)]
    pub dst_tile_size: usize,
    /// Exclude road types: comma-separated list of "toll", "ferry", "motorway"
    #[serde(default)]
    pub exclude: Option<String>,
    /// Avoid polygon(s) as JSON array of coordinate rings
    #[serde(default)]
    pub avoid_polygons: Option<String>,
}

pub fn default_tile_size() -> usize {
    1000
}

// ============ Handlers ============

/// POST /table - Distance/duration matrix computation
///
/// Returns a matrix of travel times and/or distances between sources and destinations.
/// Use `annotations` to control which metrics are returned:
/// - `"duration"` (default): travel times in seconds
/// - `"distance"`: shortest distances in meters
/// - `"duration,distance"`: both metrics
#[utoipa::path(
    post,
    path = "/table",
    tag = "Matrix",
    summary = "Compute distance/duration matrix",
    description = "Computes a many-to-many distance and/or duration matrix using Bucket CH.\nBest for matrices up to ~10K cells. For larger matrices, use POST /table/stream.",
    request_body(content = TablePostRequest, description = "Source and destination coordinates with mode",
        example = json!({
            "sources": [[4.3517, 50.8503], [4.3617, 50.8553]],
            "destinations": [[4.4017, 50.8603], [4.4117, 50.8653]],
            "mode": "car",
            "annotations": "duration,distance"
        })
    ),
    responses(
        (status = 200, description = "Matrix computed", body = TableResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
pub async fn table_post_handler(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<TablePostRequest>,
) -> impl IntoResponse {
    for (i, [lon, lat]) in req.sources.iter().enumerate() {
        if let Err(e) = validate_coord(*lon, *lat, &format!("source[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    }
    for (i, [lon, lat]) in req.destinations.iter().enumerate() {
        if let Err(e) = validate_coord(*lon, *lat, &format!("destination[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    }

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    if req.sources.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "sources cannot be empty".into(),
            }),
        )
            .into_response();
    }
    // Guard against memory explosion: max 10,000 sources × destinations for /table
    // (use /table/stream for larger matrices)
    const MAX_TABLE_CELLS: usize = 10_000_000;
    if req.sources.len() * req.destinations.len() > MAX_TABLE_CELLS {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!(
                    "matrix too large: {}×{} = {} cells exceeds limit of {}. Use POST /table/stream for large matrices.",
                    req.sources.len(), req.destinations.len(),
                    req.sources.len() * req.destinations.len(),
                    MAX_TABLE_CELLS
                ),
            }),
        )
            .into_response();
    }
    if req.destinations.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "destinations cannot be empty".into(),
            }),
        )
            .into_response();
    }

    // Parse annotations
    let annotations: Vec<&str> = req.annotations.split(',').map(|s| s.trim()).collect();
    for &a in &annotations {
        if !a.is_empty() && a != "duration" && a != "distance" {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!(
                        "Invalid annotation: '{}'. Use 'duration', 'distance', or 'duration,distance'.",
                        a
                    ),
                }),
            )
                .into_response();
        }
    }
    let want_duration = annotations.contains(&"duration") || !annotations.contains(&"distance");
    let want_distance = annotations.contains(&"distance");

    // Parse exclude parameter
    let exclude_mask = match super::exclude::parse_exclude_option(&req.exclude) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let mode_data = state.get_mode(mode);

    // Compute avoid weights (includes exclude if both present)
    let avoid_result = if let Some(ref avoid_str) = avoid_json {
        match super::avoid::compute_avoid_weights(&state, mode_data, avoid_str, exclude_mask) {
            Ok((weights, flags)) => Some((weights, flags)),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
            }
        }
    } else {
        None
    };

    // Get exclude weights if only exclude (no avoid)
    let exclude_weights = if avoid_result.is_none() {
        exclude_mask.map(|exc| state.get_exclude_weights(mode, exc))
    } else {
        None
    };

    // Build snap mask
    let snap_mask: Vec<u64> = if let Some((_, ref avoid_flags)) = avoid_result {
        super::avoid::build_avoid_mask(
            &mode_data.mask,
            avoid_flags,
            exclude_mask.map(|exc| (state.edge_exclude_flags.as_slice(), exc)),
        )
    } else if let Some(exc) = exclude_mask {
        super::exclude::build_exclude_mask(&mode_data.mask, &state.edge_exclude_flags, exc)
    } else {
        mode_data.mask.clone()
    };

    // Determine custom weights: avoid takes priority, then exclude
    let custom_weights_ref: Option<&super::exclude::ExcludeWeights> =
        if let Some((ref aw, _)) = avoid_result {
            Some(aw)
        } else {
            exclude_weights.as_deref()
        };

    compute_table_bucket_m2m(
        &state,
        mode,
        &req.sources,
        &req.destinations,
        want_duration,
        want_distance,
        custom_weights_ref,
        &snap_mask,
    )
    .await
}

/// Core table computation using bucket M2M algorithm
#[allow(clippy::too_many_arguments)]
pub async fn compute_table_bucket_m2m(
    state: &Arc<ServerState>,
    mode: Mode,
    sources: &[[f64; 2]],
    destinations: &[[f64; 2]],
    want_duration: bool,
    want_distance: bool,
    custom_weights: Option<&super::exclude::ExcludeWeights>,
    snap_mask: &[u64],
) -> Response {
    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // Snap sources to graph nodes and convert to RANK space
    // The bucket M2M algorithm operates on rank positions (CCH is rank-aligned)
    let mut sources_rank: Vec<u32> = Vec::with_capacity(sources.len());
    let mut source_waypoints: Vec<Waypoint> = Vec::with_capacity(sources.len());
    let mut source_valid: Vec<bool> = Vec::with_capacity(sources.len());

    for [lon, lat] in sources {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, snap_mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                let rank = mode_data.order.perm[filtered as usize];
                sources_rank.push(rank);
                source_valid.push(true);
                let snapped = get_node_location(state, orig_id);
                source_waypoints.push(Waypoint {
                    location: snapped,
                    name: String::new(),
                });
            } else {
                sources_rank.push(0);
                source_valid.push(false);
                source_waypoints.push(Waypoint {
                    location: [*lon, *lat],
                    name: String::new(),
                });
            }
        } else {
            sources_rank.push(0);
            source_valid.push(false);
            source_waypoints.push(Waypoint {
                location: [*lon, *lat],
                name: String::new(),
            });
        }
    }

    // Snap destinations to graph nodes and convert to RANK space
    let mut targets_rank: Vec<u32> = Vec::with_capacity(destinations.len());
    let mut dest_waypoints: Vec<Waypoint> = Vec::with_capacity(destinations.len());
    let mut target_valid: Vec<bool> = Vec::with_capacity(destinations.len());

    for [lon, lat] in destinations {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, snap_mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                let rank = mode_data.order.perm[filtered as usize];
                targets_rank.push(rank);
                target_valid.push(true);
                let snapped = get_node_location(state, orig_id);
                dest_waypoints.push(Waypoint {
                    location: snapped,
                    name: String::new(),
                });
            } else {
                targets_rank.push(0);
                target_valid.push(false);
                dest_waypoints.push(Waypoint {
                    location: [*lon, *lat],
                    name: String::new(),
                });
            }
        } else {
            targets_rank.push(0);
            target_valid.push(false);
            dest_waypoints.push(Waypoint {
                location: [*lon, *lat],
                name: String::new(),
            });
        }
    }

    let n_sources = sources.len();
    let n_targets = destinations.len();
    let use_parallel = sources_rank.len() * targets_rank.len() >= 2500;

    // Select flat adjacencies based on custom weights (exclude or avoid)
    let (time_up, time_down) = if let Some(cw) = custom_weights {
        (&cw.time_up_flat, &cw.time_down_flat)
    } else {
        (&mode_data.up_adj_flat, &mode_data.down_rev_flat)
    };
    let (dist_up, dist_down) = if let Some(cw) = custom_weights {
        (&cw.dist_up_flat, &cw.dist_down_flat)
    } else {
        (&mode_data.up_adj_flat_dist, &mode_data.down_rev_flat_dist)
    };

    // Compute duration matrix if requested
    let durations = if want_duration {
        let (matrix, _stats) = if use_parallel {
            table_bucket_parallel(n_nodes, time_up, time_down, &sources_rank, &targets_rank)
        } else {
            table_bucket_full_flat(n_nodes, time_up, time_down, &sources_rank, &targets_rank)
        };

        Some(flat_matrix_to_2d(
            &matrix,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            |v| v as f64 / 10.0, // deciseconds -> seconds
        ))
    } else {
        None
    };

    // Compute distance matrix if requested (independent shortest-distance metric)
    let distances = if want_distance {
        let (matrix, _stats) = if use_parallel {
            table_bucket_parallel(n_nodes, dist_up, dist_down, &sources_rank, &targets_rank)
        } else {
            table_bucket_full_flat(n_nodes, dist_up, dist_down, &sources_rank, &targets_rank)
        };

        Some(flat_matrix_to_2d(
            &matrix,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            |v| v as f64 / 1000.0, // millimeters -> meters
        ))
    } else {
        None
    };

    Json(TableResponse {
        code: "Ok".into(),
        durations,
        distances,
        sources: Some(source_waypoints),
        destinations: Some(dest_waypoints),
    })
    .into_response()
}

/// Convert flat u32 matrix to 2D Option<f64> matrix with null for invalid/unreachable
pub fn flat_matrix_to_2d(
    matrix: &[u32],
    n_sources: usize,
    n_targets: usize,
    source_valid: &[bool],
    target_valid: &[bool],
    convert: impl Fn(u32) -> f64,
) -> Vec<Vec<Option<f64>>> {
    let mut result: Vec<Vec<Option<f64>>> = Vec::with_capacity(n_sources);
    for src_idx in 0..n_sources {
        let mut row: Vec<Option<f64>> = Vec::with_capacity(n_targets);
        for tgt_idx in 0..n_targets {
            if !source_valid[src_idx] || !target_valid[tgt_idx] {
                row.push(None);
            } else {
                let val = matrix[src_idx * n_targets + tgt_idx];
                if val == u32::MAX {
                    row.push(None);
                } else {
                    row.push(Some(convert(val)));
                }
            }
        }
        result.push(row);
    }
    result
}

// ============ Arrow Streaming Handler ============

/// Arrow streaming endpoint for large matrices
///
/// Computes distance matrix in tiles and streams results as Apache Arrow IPC.
/// Use this for matrices larger than 10k x 10k where JSON would be too large.
///
/// Response: Arrow IPC stream with tiles containing:
/// - src_block_start, dst_block_start: tile offsets
/// - src_block_len, dst_block_len: tile dimensions
/// - durations_ms: packed u32 distances in milliseconds
#[utoipa::path(
    post,
    path = "/table/stream",
    tag = "Matrix",
    summary = "Stream large distance matrix as Arrow IPC",
    description = "Computes a distance matrix in tiles and streams results as Apache Arrow IPC format.\nDesigned for large matrices (10K+ sources/destinations) where JSON would be too large.\nBenchmarked at 50K\u{00d7}50K (2.5 billion distances) in 9.5 minutes with 2.4GB RAM overhead.\n\nNo hard point-count limit \u{2014} memory is bounded by tile-by-tile streaming regardless of input size.\n\nThe response is a binary Arrow IPC stream. Each record batch contains one tile with:\n- `src_block_start`, `dst_block_start`: tile offsets\n- `src_block_len`, `dst_block_len`: tile dimensions\n- `durations_ms`: packed u32 array of durations in milliseconds\n\nSupports cooperative cancellation on client disconnect.",
    request_body(content = TableStreamRequest, description = "Sources, destinations, mode, and optional tile sizes",
        example = json!({
            "sources": [[4.3517, 50.8503], [4.3617, 50.8553], [4.3717, 50.8603]],
            "destinations": [[4.4017, 50.8603], [4.4117, 50.8653], [4.4217, 50.8703]],
            "mode": "car",
            "src_tile_size": 1000,
            "dst_tile_size": 1000
        })
    ),
    responses(
        (status = 200, description = "Arrow IPC stream", content_type = "application/vnd.apache.arrow.stream"),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
pub async fn table_stream_handler(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<TableStreamRequest>,
) -> impl IntoResponse {
    for (i, [lon, lat]) in req.sources.iter().enumerate() {
        if let Err(e) = validate_coord(*lon, *lat, &format!("source[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    }
    for (i, [lon, lat]) in req.destinations.iter().enumerate() {
        if let Err(e) = validate_coord(*lon, *lat, &format!("destination[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    }

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    if req.sources.is_empty() || req.destinations.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "sources and destinations cannot be empty".into(),
            }),
        )
            .into_response();
    }

    // No hard point-count limit: /table/stream is designed for arbitrarily large matrices.
    // Memory is bounded by tile-by-tile streaming (src_tile_size x dst_tile_size per tile).
    // The only per-request allocation is the rank vectors (4 bytes per coordinate).

    // Parse exclude parameter
    let exclude_mask = match super::exclude::parse_exclude_option(&req.exclude) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // Compute avoid weights (includes exclude if both present)
    let avoid_result = if let Some(ref avoid_str) = avoid_json {
        match super::avoid::compute_avoid_weights(&state, mode_data, avoid_str, exclude_mask) {
            Ok((weights, flags)) => Some((weights, flags)),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
            }
        }
    } else {
        None
    };

    // Get exclude weights if only exclude (no avoid)
    let exclude_weights = if avoid_result.is_none() {
        exclude_mask.map(|exc| state.get_exclude_weights(mode, exc))
    } else {
        None
    };

    // Build snap mask
    let snap_mask: std::borrow::Cow<'_, [u64]> = if let Some((_, ref avoid_flags)) = avoid_result {
        std::borrow::Cow::Owned(super::avoid::build_avoid_mask(
            &mode_data.mask,
            avoid_flags,
            exclude_mask.map(|exc| (state.edge_exclude_flags.as_slice(), exc)),
        ))
    } else if let Some(exc) = exclude_mask {
        std::borrow::Cow::Owned(super::exclude::build_exclude_mask(
            &mode_data.mask,
            &state.edge_exclude_flags,
            exc,
        ))
    } else {
        std::borrow::Cow::Borrowed(&mode_data.mask)
    };

    // Convert all sources to rank space, keeping track of valid indices
    let mut sources_rank: Vec<u32> = Vec::with_capacity(req.sources.len());
    let mut valid_src_indices: Vec<usize> = Vec::with_capacity(req.sources.len());
    for (i, [lon, lat]) in req.sources.iter().enumerate() {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &snap_mask, 10) {
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
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &snap_mask, 10) {
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

    let Some(n_total_cells) = n_total_sources.checked_mul(n_total_targets) else {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "matrix dimensions overflow".into(),
            }),
        )
            .into_response();
    };

    // Select flat adjacencies based on custom weights (exclude or avoid)
    let up_adj_flat = if let Some((ref aw, _)) = avoid_result {
        aw.time_up_flat.clone()
    } else if let Some(ref ew) = exclude_weights {
        ew.time_up_flat.clone()
    } else {
        mode_data.up_adj_flat.clone()
    };
    let down_rev_flat = if let Some((ref aw, _)) = avoid_result {
        aw.time_down_flat.clone()
    } else if let Some(ref ew) = exclude_weights {
        ew.time_down_flat.clone()
    } else {
        mode_data.down_rev_flat.clone()
    };

    // ----------------------------------------------------------------
    // Smart algorithm selection:
    //   - Small matrices (N*M <= 50,000): Bucket M2M (fast, low overhead)
    //   - Large matrices: PHAST tiling/streaming (amortizes cost)
    // Both paths return Arrow IPC, so the client sees no difference.
    // ----------------------------------------------------------------
    const BUCKET_M2M_THRESHOLD: usize = 50_000;

    if n_total_sources * n_total_targets <= BUCKET_M2M_THRESHOLD {
        // --- SMALL MATRIX PATH: Bucket M2M → single Arrow IPC tile ---
        return table_stream_bucket_path(
            n_nodes,
            &up_adj_flat,
            &down_rev_flat,
            n_total_sources,
            n_total_targets,
            n_total_cells,
            n_valid_sources,
            n_valid_targets,
            &sources_rank,
            &targets_rank,
            &valid_src_indices,
            &valid_dst_indices,
        );
    }

    // --- LARGE MATRIX PATH: PHAST tiling/streaming ---
    let src_tile_size = req
        .src_tile_size
        .min(n_total_sources)
        .min(u16::MAX as usize)
        .max(1);
    let dst_tile_size = req
        .dst_tile_size
        .min(n_total_targets)
        .min(u16::MAX as usize)
        .max(1);

    // Calculate total tiles for progress tracking
    let n_src_blocks = n_total_sources.div_ceil(src_tile_size);
    let n_dst_blocks = n_total_targets.div_ceil(dst_tile_size);
    let n_total_tiles = n_src_blocks * n_dst_blocks;

    // Create channel for streaming tiles
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(8);

    // Cancellation flag: set when client disconnects (channel closed)
    let cancelled = Arc::new(AtomicBool::new(false));

    let cancelled_outer = cancelled.clone();

    // Spawn compute task - SOURCE-BLOCK OUTER LOOP to avoid repeated forward computation
    // For 10k x 10k with 1000 x 1000 tiles: forward computed 10x (once per src block) instead of 100x
    tokio::task::spawn_blocking(move || {
        let cancelled = cancelled_outer;
        // Generate source and destination blocks
        let src_blocks: Vec<(usize, usize)> = (0..n_total_sources)
            .step_by(src_tile_size)
            .map(|start| (start, (start + src_tile_size).min(n_total_sources)))
            .collect();

        let dst_blocks: Vec<(usize, usize)> = (0..n_total_targets)
            .step_by(dst_tile_size)
            .map(|start| (start, (start + dst_tile_size).min(n_total_targets)))
            .collect();

        // Helper: send a tile through the channel, returning false if cancelled
        let send_tile = |tx: &tokio::sync::mpsc::Sender<Result<bytes::Bytes, std::io::Error>>,
                         cancelled: &AtomicBool,
                         tile: MatrixTile|
         -> bool {
            match tiles_to_record_batch(&[tile]) {
                Ok(batch) => match record_batch_to_bytes(&batch) {
                    Ok(bytes) => {
                        if tx.blocking_send(Ok(bytes)).is_err() {
                            cancelled.store(true, Ordering::Relaxed);
                            return false;
                        }
                    }
                    Err(e) => {
                        let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
                        cancelled.store(true, Ordering::Relaxed);
                        return false;
                    }
                },
                Err(e) => {
                    let _ = tx.blocking_send(Err(std::io::Error::other(e.to_string())));
                    cancelled.store(true, Ordering::Relaxed);
                    return false;
                }
            }
            true
        };

        // Process source blocks in parallel (forward computed ONCE per source block)
        src_blocks.par_iter().for_each(|&(src_start, src_end)| {
            if cancelled.load(Ordering::Relaxed) {
                return;
            }

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
                    if cancelled.load(Ordering::Relaxed) {
                        return;
                    }
                    let tile_cols = dst_end - dst_start;
                    let durations_ms = vec![u32::MAX; tile_rows * tile_cols];
                    let tile = MatrixTile::from_flat(
                        src_start as u32,
                        dst_start as u32,
                        tile_rows as u16,
                        tile_cols as u16,
                        &durations_ms,
                    );
                    if !send_tile(&tx, &cancelled, tile) {
                        return;
                    }
                }
                return;
            }

            // FORWARD PHASE: Compute forward searches ONCE for this source block
            let source_buckets = std::sync::Arc::new(forward_build_buckets(
                n_nodes,
                &up_adj_flat,
                &block_src_ranks,
            ));

            // BACKWARD PHASE: Process destination blocks in parallel
            // This maintains high parallelism while avoiding repeated forward work
            dst_blocks.par_iter().for_each(|&(dst_start, dst_end)| {
                if cancelled.load(Ordering::Relaxed) {
                    return;
                }

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

                        for (tile_dst_idx, &orig_dst_idx) in
                            block_dst_orig_indices.iter().enumerate()
                        {
                            let out_col = orig_dst_idx - dst_start;
                            let d =
                                tile_matrix[tile_src_idx * block_dst_ranks.len() + tile_dst_idx];
                            durations_ms[out_row * tile_cols + out_col] = if d == u32::MAX {
                                u32::MAX
                            } else {
                                d.saturating_mul(100)
                            };
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

                // Stream this tile -- stop computation if client disconnected
                send_tile(&tx, &cancelled, tile);
            }); // end dst_blocks.par_iter()
        }); // end src_blocks.par_iter()
    });

    // Convert receiver to stream
    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
        // Progress tracking headers
        .header("X-Total-Tiles", n_total_tiles.to_string())
        .header("X-Total-Sources", n_total_sources.to_string())
        .header("X-Total-Destinations", n_total_targets.to_string())
        .header("X-Total-Cells", n_total_cells.to_string())
        .header("X-Valid-Sources", n_valid_sources.to_string())
        .header("X-Valid-Destinations", n_valid_targets.to_string())
        .body(Body::from_stream(stream))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build streaming response",
            )
                .into_response()
        })
}

// ============ Bucket M2M path for small streaming matrices ============

/// Compute a small matrix using Bucket M2M and return as a single Arrow IPC response.
///
/// This avoids the overhead of PHAST tiling/streaming for matrices where Bucket M2M
/// is significantly faster (N*M <= 50,000). The result is identical Arrow IPC format —
/// a single tile covering the entire matrix.
#[allow(clippy::too_many_arguments)]
fn table_stream_bucket_path(
    n_nodes: usize,
    up_adj_flat: &UpAdjFlat,
    down_rev_flat: &DownReverseAdjFlat,
    n_total_sources: usize,
    n_total_targets: usize,
    n_total_cells: usize,
    n_valid_sources: usize,
    n_valid_targets: usize,
    sources_rank: &[u32],
    targets_rank: &[u32],
    valid_src_indices: &[usize],
    valid_dst_indices: &[usize],
) -> Response {
    // Use parallel variant for matrices >= 2500 cells, sequential for smaller
    let use_parallel = sources_rank.len() * targets_rank.len() >= 2500;

    let (bucket_matrix, _stats) = if use_parallel {
        table_bucket_parallel(
            n_nodes,
            up_adj_flat,
            down_rev_flat,
            sources_rank,
            targets_rank,
        )
    } else {
        table_bucket_full_flat(
            n_nodes,
            up_adj_flat,
            down_rev_flat,
            sources_rank,
            targets_rank,
        )
    };

    // bucket_matrix is a flat [n_valid_sources x n_valid_targets] array of deciseconds (u32).
    // We need to map it into the full [n_total_sources x n_total_targets] tile with:
    //   - u32::MAX for rows/cols where snap failed (invalid sources/destinations)
    //   - values converted from deciseconds to milliseconds (multiply by 100)
    let mut durations_ms = vec![u32::MAX; n_total_sources * n_total_targets];

    for (valid_src_idx, &orig_src_idx) in valid_src_indices.iter().enumerate() {
        for (valid_dst_idx, &orig_dst_idx) in valid_dst_indices.iter().enumerate() {
            let d = bucket_matrix[valid_src_idx * targets_rank.len() + valid_dst_idx];
            durations_ms[orig_src_idx * n_total_targets + orig_dst_idx] = if d == u32::MAX {
                u32::MAX
            } else {
                d.saturating_mul(100) // deciseconds -> milliseconds
            };
        }
    }

    // Encode as a single Arrow IPC tile covering the entire matrix.
    // For small matrices (up to 50K cells), the entire result fits in one tile.
    // Source/destination dimensions are clamped to u16::MAX (65535) which is always
    // sufficient since n_total_sources * n_total_targets <= 50,000.
    let tile = MatrixTile::from_flat(
        0,
        0,
        n_total_sources as u16,
        n_total_targets as u16,
        &durations_ms,
    );

    let batch = match tiles_to_record_batch(&[tile]) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Arrow encoding error: {}", e),
                }),
            )
                .into_response();
        }
    };

    let bytes = match record_batch_to_bytes(&batch) {
        Ok(b) => b,
        Err(e) => {
            return (
                StatusCode::INTERNAL_SERVER_ERROR,
                Json(ErrorResponse {
                    error: format!("Arrow serialization error: {}", e),
                }),
            )
                .into_response();
        }
    };

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, ARROW_STREAM_CONTENT_TYPE)
        .header("X-Total-Tiles", "1")
        .header("X-Total-Sources", n_total_sources.to_string())
        .header("X-Total-Destinations", n_total_targets.to_string())
        .header("X-Total-Cells", n_total_cells.to_string())
        .header("X-Valid-Sources", n_valid_sources.to_string())
        .header("X-Valid-Destinations", n_valid_targets.to_string())
        .header("X-Algorithm", "bucket-m2m")
        .body(Body::from(bytes))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build response",
            )
                .into_response()
        })
}
