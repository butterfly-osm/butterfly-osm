//! /table and /table/stream handlers — distance/duration matrix computation

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use utoipa::ToSchema;

use crate::matrix::arrow_stream::{
    ARROW_STREAM_CONTENT_TYPE, MatrixTile, record_batch_to_bytes, tiles_to_record_batch,
};
use crate::matrix::bucket_ch::{
    DownReverseAdjFlat, UpAdjFlat, backward_join_with_buckets, forward_build_buckets,
    table_bucket_full_flat, table_bucket_parallel,
};
use crate::matrix::neighbors::{RadiusParam, auto_radius_km, build_neighbors, parse_radius};
use crate::profile_abi::Mode;

use super::regions::RegionsState;
use super::state::ServerState;
use super::types::{
    ErrorResponse, SnapRole, Waypoint, get_node_location, parse_mode, validate_coord,
};

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
    /// Optional Euclidean pre-filter radius in kilometres.
    /// Accepts a positive number, the string "auto" (server-computed p95 × 1.1),
    /// or null/0 to disable. Pairs beyond the radius are returned as null.
    #[serde(default)]
    pub radius_km: Option<serde_json::Value>,
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
    /// Optional Euclidean pre-filter radius in kilometres.
    /// Accepts a positive number, the string "auto" (server-computed p95 × 1.1),
    /// or null/0 to disable. Pairs beyond the radius are emitted as u32::MAX.
    #[serde(default)]
    pub radius_km: Option<serde_json::Value>,
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
    State(regions): State<Arc<RegionsState>>,
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

    // Region dispatch (#91): every source + every destination must
    // snap to the same region. Mixed-region matrices are rejected
    // with 501 (cross-region matrix is part of the overlay design,
    // PR C / Phase 2).
    let started_dispatch = std::time::Instant::now();
    let coords_iter = req
        .sources
        .iter()
        .chain(req.destinations.iter())
        .map(|&[lon, lat]| (lon, lat));
    let (state, region_id): (Arc<ServerState>, String) =
        match regions.dispatch_many(coords_iter, &req.mode) {
            Ok(pair) => pair,
            Err(e) => {
                let (code, body) = e.into_response_parts();
                return (code, Json(body)).into_response();
            }
        };

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
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
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let mode_data = state.get_mode(mode);

    // Compute avoid weights (includes exclude if both present)
    let avoid_entry = if let Some(ref avoid_str) = avoid_json {
        match super::avoid::compute_avoid_weights(&state, mode_data, avoid_str, exclude_mask) {
            Ok(entry) => Some(entry),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
            }
        }
    } else {
        None
    };

    // Get exclude weights if only exclude (no avoid)
    let exclude_weights = if avoid_entry.is_none() {
        exclude_mask.map(|exc| state.get_exclude_weights(mode, exc))
    } else {
        None
    };

    // Build snap mask
    let snap_mask: Vec<u64> = if let Some(ref entry) = avoid_entry {
        super::avoid::build_avoid_mask(
            &mode_data.mask,
            &entry.flags,
            exclude_mask.map(|exc| (state.edge_exclude_flags.as_slice(), exc)),
        )
    } else if let Some(exc) = exclude_mask {
        super::exclude::build_exclude_mask(&mode_data.mask, &state.edge_exclude_flags, exc)
    } else {
        mode_data.mask.clone()
    };

    // Determine custom weights: avoid takes priority, then exclude.
    // /table holds an Arc<AvoidEntry>; the borrow points into the cache
    // so /table cache hits avoid the prior ~200 MB deep clone.
    let custom_weights_ref: Option<&super::exclude::ExcludeWeights> =
        if let Some(ref entry) = avoid_entry {
            Some(&entry.weights)
        } else {
            exclude_weights.as_deref()
        };

    let radius_param = parse_radius(req.radius_km.as_ref());

    let resp = compute_table_bucket_m2m(
        &state,
        mode,
        &req.sources,
        &req.destinations,
        want_duration,
        want_distance,
        custom_weights_ref,
        &snap_mask,
        radius_param,
    )
    .await;
    super::region_metrics::record_query(
        &region_id,
        "table",
        started_dispatch.elapsed().as_secs_f64(),
    );
    resp
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
    radius_param: RadiusParam,
) -> Response {
    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // K-best snap with the directional #197 role filter. Use the same
    // primary that /route uses, so the matrix and routes agree on every
    // pair where the primary pair connects.
    //
    // Phase 1 (this block): snap K=1 per source/destination. Cheap —
    // each iterate_rings call exits as soon as one candidate is found
    // and no closer ring can beat it.
    //
    // Phase 2 (apply_k_best_fallback): only for the small fraction of
    // pairs whose K=1 primary doesn't connect do we escalate to a K=64
    // snap for the affected source/destination indices. That cost
    // remains O(failed_rows + failed_cols), not O(n_sources + n_targets).
    //
    // Pre-#368 the matrix paid the K=64 snap upfront for every
    // src/dst — ≈ 2.1 ms × N on serial / N/20 parallel. Belgium 100×100
    // matrix snap dropped from ~20 ms total → ~1 ms; 1000×1000 from
    // ~200 ms → ~13 ms. Healthy matrices never see the escalation cost.
    const SNAP_K: usize = 64;
    let _ = SNAP_K; // referenced from apply_k_best_fallback's docs

    let src_role_filter = SnapRole::Src.role_filter(mode_data);
    let dst_role_filter = SnapRole::Dst.role_filter(mode_data);

    let t_pre = std::time::Instant::now();

    // (rank, snapped, valid). Per-row candidate list is built lazily on
    // first miss (see apply_k_best_fallback's lazy K=64 escalator).
    type SnapResult = (u32, (f64, f64), bool);

    let source_results: Vec<SnapResult> = sources
        .par_iter()
        .map(|&[lon, lat]| {
            if let Some((orig_id, plon, plat, _)) = state.snap_index.snap_with_info_filtered_role(
                lon,
                lat,
                mode.0,
                Some(snap_mask),
                src_role_filter,
            ) {
                let rank = mode_data.orig_to_rank[orig_id as usize];
                if rank != u32::MAX {
                    return (rank, (plon, plat), true);
                }
            }
            (0, (lon, lat), false)
        })
        .collect();

    let target_results: Vec<SnapResult> = destinations
        .par_iter()
        .map(|&[lon, lat]| {
            if let Some((orig_id, plon, plat, _)) = state.snap_index.snap_with_info_filtered_role(
                lon,
                lat,
                mode.0,
                Some(snap_mask),
                dst_role_filter,
            ) {
                let rank = mode_data.orig_to_rank[orig_id as usize];
                if rank != u32::MAX {
                    return (rank, (plon, plat), true);
                }
            }
            (0, (lon, lat), false)
        })
        .collect();

    let mut sources_rank: Vec<u32> = Vec::with_capacity(sources.len());
    let mut source_waypoints: Vec<Waypoint> = Vec::with_capacity(sources.len());
    let mut source_valid: Vec<bool> = Vec::with_capacity(sources.len());
    let mut sources_snapped: Vec<(f64, f64)> = Vec::with_capacity(sources.len());
    for (rank, (plon, plat), valid) in source_results {
        sources_rank.push(rank);
        source_valid.push(valid);
        sources_snapped.push((plon, plat));
        source_waypoints.push(Waypoint {
            location: [plon, plat],
            name: String::new(),
        });
    }

    let mut targets_rank: Vec<u32> = Vec::with_capacity(destinations.len());
    let mut dest_waypoints: Vec<Waypoint> = Vec::with_capacity(destinations.len());
    let mut target_valid: Vec<bool> = Vec::with_capacity(destinations.len());
    let mut targets_snapped: Vec<(f64, f64)> = Vec::with_capacity(destinations.len());
    for (rank, (plon, plat), valid) in target_results {
        targets_rank.push(rank);
        target_valid.push(valid);
        targets_snapped.push((plon, plat));
        dest_waypoints.push(Waypoint {
            location: [plon, plat],
            name: String::new(),
        });
    }

    // Build the per-source neighbour mask if a radius was requested.
    // NOTE: this is a correctness-preserving "mask-at-emit" integration — the
    // full N×M bucket M2M still runs, and pruned pairs are nulled out below.
    // Pruning the inner solver per source would require refactoring bucket_ch
    // to accept per-source target slices without losing its amortised forward
    // phase; that's a follow-up optimisation and is unnecessary for
    // correctness.
    let neighbor_mask: Option<Vec<Vec<u32>>> = match radius_param {
        RadiusParam::None => None,
        RadiusParam::Km(r) => Some(build_neighbors(&sources_snapped, &targets_snapped, r)),
        RadiusParam::Auto => {
            let r = auto_radius_km(&sources_snapped, &targets_snapped);
            if r > 0.0 {
                Some(build_neighbors(&sources_snapped, &targets_snapped, r))
            } else {
                None
            }
        }
    };

    let n_sources = sources.len();
    let n_targets = destinations.len();
    let use_parallel = sources_rank.len() * targets_rank.len() >= 2500;
    tracing::debug!(
        "compute_table_bucket_m2m: snap+rebuild took {:?} n_src={} n_tgt={}",
        t_pre.elapsed(),
        sources.len(),
        destinations.len()
    );

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

    // #372: when both duration and distance are requested AND the
    // length-along-time flats are available (container shipped with
    // cch.lat.<mode>.u32 from PR #379), use the 2-channel bucket-M2M.
    // It produces both matrices in a single forward+backward pass with
    // the time-shortest path's geometry — distance numbers correspond
    // to the same path as the duration (matching /route's per-cell
    // unpack semantics).
    //
    // Custom-weight paths (exclude/avoid) don't have length-along-time
    // recustomisation yet; they fall back to the two-pass distance-
    // shortest legacy below.
    let use_2channel = want_duration
        && want_distance
        && custom_weights.is_none()
        && mode_data.up_adj_flat_lat.is_some()
        && mode_data.down_rev_flat_lat.is_some();

    let (durations, distances) = if use_2channel {
        let t_2ch = std::time::Instant::now();
        let up_lat = mode_data
            .up_adj_flat_lat
            .as_ref()
            .expect("guarded by use_2channel");
        let dn_lat = mode_data
            .down_rev_flat_lat
            .as_ref()
            .expect("guarded by use_2channel");
        let (time_mat, lat_mat, _stats) = crate::matrix::bucket_ch::table_bucket_full_flat_lat(
            n_nodes,
            time_up,
            time_down,
            up_lat,
            dn_lat,
            &sources_rank,
            &targets_rank,
        );
        tracing::debug!(
            "compute_table_bucket_m2m: 2-channel M2M took {:?}",
            t_2ch.elapsed(),
        );
        let dur = flat_matrix_to_2d(
            &time_mat,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            neighbor_mask.as_deref(),
            |v| v as f64,
        );
        let dist = flat_matrix_to_2d(
            &lat_mat,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            neighbor_mask.as_deref(),
            |v| v as f64,
        );
        (Some(dur), Some(dist))
    } else {
        // Legacy two-pass: separate distance-shortest CCH for `distance`.
        let durations = if want_duration {
            let t_dur = std::time::Instant::now();
            let (matrix, _stats) = if use_parallel {
                table_bucket_parallel(n_nodes, time_up, time_down, &sources_rank, &targets_rank)
            } else {
                table_bucket_full_flat(n_nodes, time_up, time_down, &sources_rank, &targets_rank)
            };
            tracing::debug!(
                "compute_table_bucket_m2m: duration M2M took {:?} parallel={}",
                t_dur.elapsed(),
                use_parallel
            );

            Some(flat_matrix_to_2d(
                &matrix,
                n_sources,
                n_targets,
                &source_valid,
                &target_valid,
                neighbor_mask.as_deref(),
                |v| v as f64,
            ))
        } else {
            None
        };

        let distances = if want_distance {
            let t_dist = std::time::Instant::now();
            let (matrix, _stats) = if use_parallel {
                table_bucket_parallel(n_nodes, dist_up, dist_down, &sources_rank, &targets_rank)
            } else {
                table_bucket_full_flat(n_nodes, dist_up, dist_down, &sources_rank, &targets_rank)
            };
            tracing::debug!(
                "compute_table_bucket_m2m: distance M2M took {:?} parallel={}",
                t_dist.elapsed(),
                use_parallel
            );

            Some(flat_matrix_to_2d(
                &matrix,
                n_sources,
                n_targets,
                &source_valid,
                &target_valid,
                neighbor_mask.as_deref(),
                |v| v as f64,
            ))
        } else {
            None
        };
        (durations, distances)
    };

    let t_post_m2m = std::time::Instant::now();
    let _ = t_post_m2m;

    // Per-cell K-best fallback (#197 matrix gap).
    //
    // Bucket M2M uses only the primary candidate per src/dst. For the
    // small fraction of pairs the primary snap is still unsuitable
    // for this particular OD pair (usually same-geometry directional
    // ambiguity or dynamic exclude/avoid effects), even though K-best
    // would connect. /route already does this fallback inline; we
    // mirror it here so /table agrees with /route.
    // The K-best snap (expensive — iterates all samples within 5 km)
    // is done LAZILY for only the affected src/dst rows/cols, so a
    // healthy matrix pays zero K-best snap cost.
    let (durations, distances) = apply_k_best_fallback(
        state,
        mode_data,
        mode,
        durations,
        distances,
        sources,
        destinations,
        &source_valid,
        &target_valid,
        snap_mask,
        src_role_filter,
        dst_role_filter,
        custom_weights,
        want_duration,
        want_distance,
    );

    tracing::debug!(
        "compute_table_bucket_m2m: post-m2m to response took {:?}",
        t_post_m2m.elapsed()
    );

    let t_resp = std::time::Instant::now();
    let resp = Json(TableResponse {
        code: "Ok".into(),
        durations,
        distances,
        sources: Some(source_waypoints),
        destinations: Some(dest_waypoints),
    })
    .into_response();
    tracing::debug!(
        "compute_table_bucket_m2m: json+into_response took {:?}",
        t_resp.elapsed()
    );
    resp
}

/// 2D matrix of Option<f64> — None for unreachable/invalid cells.
type MatrixGrid = Option<Vec<Vec<Option<f64>>>>;

/// For each cell where bucket-M2M returned None (unreachable under the
/// primary src/dst snap pair), retry with the K-best candidate combo
/// enumeration — the same fallback /route uses for #197.
///
/// Lazy K-best: the expensive `snap_k_with_info_filtered_role`
/// (iterates all samples within 5 km) is only invoked for src/dst rows
/// and columns that contain at least one None cell. Healthy matrices
/// pay zero overhead beyond the cheap primary snap done upfront.
#[allow(clippy::too_many_arguments)]
fn apply_k_best_fallback(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    mut durations: MatrixGrid,
    mut distances: MatrixGrid,
    sources: &[[f64; 2]],
    destinations: &[[f64; 2]],
    source_valid: &[bool],
    target_valid: &[bool],
    snap_mask: &[u64],
    src_role_filter: Option<&[u64]>,
    dst_role_filter: Option<&[u64]>,
    custom_weights: Option<&super::exclude::ExcludeWeights>,
    want_duration: bool,
    want_distance: bool,
) -> (MatrixGrid, MatrixGrid) {
    use super::query::CchQuery;
    const SNAP_K: usize = 64;

    // Cap per-cell fallback combos. /route uses 400 because a single
    // hopeless query at 20s wall is acceptable; /table can have
    // hundreds of failed cells so the per-cell budget must be smaller
    // or total latency explodes (the unbounded version ran 88 s on
    // Belgium 50×50 scattered).
    //
    // Connectivity-aware role masks should keep this path cold on the
    // base graph. Keep the cap broad enough to preserve /route parity
    // for the remaining dynamic or geometrically ambiguous cases.
    const MAX_FALLBACK_COMBOS: usize = 200;

    let _t_fb_start = std::time::Instant::now();
    let n_sources = sources.len();
    let n_targets = destinations.len();

    // Decide whether any cell needs the fallback. Skip the (cheap)
    // CchQuery construction entirely on the common path.
    let needs_fallback = |grid: &MatrixGrid| -> bool {
        if let Some(g) = grid {
            for (i, row) in g.iter().enumerate() {
                if !source_valid[i] {
                    continue;
                }
                for (j, cell) in row.iter().enumerate() {
                    if target_valid[j] && cell.is_none() {
                        return true;
                    }
                }
            }
        }
        false
    };
    let need_time = want_duration && needs_fallback(&durations);
    let need_dist = want_distance && needs_fallback(&distances);
    tracing::debug!(
        "apply_k_best_fallback: needs_fallback decision took {:?}, need_time={}, need_dist={}",
        _t_fb_start.elapsed(),
        need_time,
        need_dist
    );
    if !need_time && !need_dist {
        return (durations, distances);
    }

    // Time CchQuery: use the same backend as /route (flats from
    // mode_data, or recustomised flats if exclude/avoid are in play).
    // with_custom_weights expects the *reverse* down-adjacency
    // (DownReverseAdjFlat) for the bidirectional backward search — same
    // layout as `mode_data.down_rev_flat`.
    let time_query = if need_time {
        Some(match custom_weights {
            Some(cw) => CchQuery::with_custom_weights(
                &mode_data.cch_topo,
                &cw.time_up_flat,
                &cw.time_down_flat,
                &cw.time_weights,
            ),
            None => CchQuery::new(state, mode),
        })
    } else {
        None
    };

    // Distance CchQuery: the CCH topology is shared between time and
    // distance, and the metric-dependent INF sets agree (both are gated
    // on mode access + exclude flags). So we reuse the TIME flats for
    // topology + topo_edge_idx and override with the distance-metric
    // weights. The standalone `*_dist` flats and the `dist_*` flats on
    // `ExcludeWeights` intentionally omit `topo_edge_idx` because PHAST
    // doesn't need it — they cannot back a `CchQuery` directly.
    let dist_query = if need_dist {
        let (up_flat, down_flat, weights) = match custom_weights {
            Some(cw) => (&cw.time_up_flat, &cw.time_down_flat, &cw.dist_weights),
            None => (
                &mode_data.up_adj_flat,
                &mode_data.down_rev_flat,
                &mode_data.cch_weights_dist,
            ),
        };
        Some(CchQuery::with_custom_weights(
            &mode_data.cch_topo,
            up_flat,
            down_flat,
            weights,
        ))
    } else {
        None
    };

    // Build (i+j)-ordered combo enumeration; same shape as /route.
    let combo_enum = |k_src: usize, k_dst: usize| -> Vec<(usize, usize)> {
        let mut order = Vec::new();
        for sum in 0..(k_src + k_dst) {
            for i in 0..k_src {
                if let Some(j) = sum.checked_sub(i)
                    && j < k_dst
                {
                    order.push((i, j));
                }
            }
        }
        if order.len() > MAX_FALLBACK_COMBOS {
            order.truncate(MAX_FALLBACK_COMBOS);
        }
        order
    };

    let t_fb_work = std::time::Instant::now();
    // Build the list of cells needing fallback AND the set of unique
    // src/tgt indices touched by them. We snap K=64 only for those
    // indices — healthy matrices snap zero rows/cols here.
    let mut work: Vec<(usize, usize, bool, bool)> = Vec::new();
    let mut src_idx_set: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut tgt_idx_set: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for src_idx in 0..n_sources {
        if !source_valid[src_idx] {
            continue;
        }
        for tgt_idx in 0..n_targets {
            if !target_valid[tgt_idx] {
                continue;
            }
            let dur_missing = durations
                .as_ref()
                .map(|d| d[src_idx][tgt_idx].is_none())
                .unwrap_or(false);
            let dist_missing = distances
                .as_ref()
                .map(|d| d[src_idx][tgt_idx].is_none())
                .unwrap_or(false);
            if dur_missing || dist_missing {
                work.push((src_idx, tgt_idx, dur_missing, dist_missing));
                src_idx_set.insert(src_idx);
                tgt_idx_set.insert(tgt_idx);
            }
        }
    }

    tracing::debug!(
        "apply_k_best_fallback: built work list of {} cells (unique src={}, tgt={}) in {:?}",
        work.len(),
        src_idx_set.len(),
        tgt_idx_set.len(),
        t_fb_work.elapsed()
    );

    if work.is_empty() {
        return (durations, distances);
    }

    // Lazy K=64 escalation: snap each affected src/tgt index ONCE, in
    // parallel. `sources_candidates[i]` is None for indices not in the
    // failed set — those rows never see the K=64 cost.
    let t_fb_snap = std::time::Instant::now();
    let mut sources_candidates: Vec<Option<Vec<u32>>> = vec![None; n_sources];
    let mut targets_candidates: Vec<Option<Vec<u32>>> = vec![None; n_targets];
    let needed_src: Vec<usize> = src_idx_set.into_iter().collect();
    let needed_tgt: Vec<usize> = tgt_idx_set.into_iter().collect();
    let src_snapped: Vec<(usize, Vec<u32>)> = needed_src
        .par_iter()
        .map(|&i| {
            let [lon, lat] = sources[i];
            let cands = state.snap_index.snap_k_with_info_filtered_role(
                lon,
                lat,
                mode.0,
                SNAP_K,
                Some(snap_mask),
                src_role_filter,
            );
            let ranks: Vec<u32> = cands
                .iter()
                .filter_map(|(orig_id, _, _, _)| {
                    let r = mode_data.orig_to_rank[*orig_id as usize];
                    if r == u32::MAX { None } else { Some(r) }
                })
                .collect();
            (i, ranks)
        })
        .collect();
    let tgt_snapped: Vec<(usize, Vec<u32>)> = needed_tgt
        .par_iter()
        .map(|&i| {
            let [lon, lat] = destinations[i];
            let cands = state.snap_index.snap_k_with_info_filtered_role(
                lon,
                lat,
                mode.0,
                SNAP_K,
                Some(snap_mask),
                dst_role_filter,
            );
            let ranks: Vec<u32> = cands
                .iter()
                .filter_map(|(orig_id, _, _, _)| {
                    let r = mode_data.orig_to_rank[*orig_id as usize];
                    if r == u32::MAX { None } else { Some(r) }
                })
                .collect();
            (i, ranks)
        })
        .collect();
    for (i, ranks) in src_snapped {
        sources_candidates[i] = Some(ranks);
    }
    for (i, ranks) in tgt_snapped {
        targets_candidates[i] = Some(ranks);
    }
    tracing::debug!(
        "apply_k_best_fallback: lazy K={} snap for {} src + {} tgt took {:?}",
        SNAP_K,
        needed_src.len(),
        needed_tgt.len(),
        t_fb_snap.elapsed()
    );

    let t_fb_run = std::time::Instant::now();
    // Solve per cell in parallel — CchQuery is Sync (immutable
    // references to topology + weights; thread-local search state
    // lives in CchQueryState). Each cell is independent, so rayon
    // gives close to linear speed-up on n_cores.
    let time_query_ref = time_query.as_ref();
    let dist_query_ref = dist_query.as_ref();
    let patches: Vec<(usize, usize, Option<f64>, Option<f64>)> = work
        .par_iter()
        .map(|&(src_idx, tgt_idx, dur_missing, dist_missing)| {
            let empty: Vec<u32> = Vec::new();
            let src_cands = sources_candidates[src_idx].as_ref().unwrap_or(&empty);
            let tgt_cands = targets_candidates[tgt_idx].as_ref().unwrap_or(&empty);
            let order = combo_enum(src_cands.len(), tgt_cands.len());
            let mut dur_done = !dur_missing;
            let mut dist_done = !dist_missing;
            let mut dur_val: Option<f64> = None;
            let mut dist_val: Option<f64> = None;
            for &(i, j) in &order {
                let s_rank = src_cands[i];
                let d_rank = tgt_cands[j];
                if s_rank == d_rank {
                    continue;
                }
                if !dur_done
                    && let Some(tq) = time_query_ref
                    && let Some(r) = tq.query(s_rank, d_rank)
                {
                    // r.distance is already in seconds (post-#297).
                    dur_val = Some(r.distance as f64);
                    dur_done = true;
                }
                if !dist_done
                    && let Some(dq) = dist_query_ref
                    && let Some(r) = dq.query(s_rank, d_rank)
                {
                    // r.distance is already in meters (post-#297).
                    dist_val = Some(r.distance as f64);
                    dist_done = true;
                }
                if dur_done && dist_done {
                    break;
                }
            }
            (src_idx, tgt_idx, dur_val, dist_val)
        })
        .collect();

    tracing::debug!(
        "apply_k_best_fallback: ran {} cells in {:?}",
        patches.len(),
        t_fb_run.elapsed()
    );

    // Apply patches sequentially (cheap O(failed_cells) writes).
    for (src_idx, tgt_idx, dur_val, dist_val) in patches {
        if let Some(grid) = durations.as_mut()
            && let Some(v) = dur_val
        {
            grid[src_idx][tgt_idx] = Some(v);
        }
        if let Some(grid) = distances.as_mut()
            && let Some(v) = dist_val
        {
            grid[src_idx][tgt_idx] = Some(v);
        }
    }

    (durations, distances)
}

/// Convert flat u32 matrix to 2D Option<f64> matrix with null for invalid/unreachable.
///
/// If `neighbor_mask` is supplied, any (src, tgt) pair not present in
/// `neighbor_mask[src]` is emitted as `None` regardless of the computed
/// distance. The mask is indexed by the original source/target positions
/// (i.e. the full `n_sources`/`n_targets`) so callers pre-filter using
/// haversine distances on the original inputs.
#[allow(clippy::too_many_arguments)]
pub fn flat_matrix_to_2d(
    matrix: &[u32],
    n_sources: usize,
    n_targets: usize,
    source_valid: &[bool],
    target_valid: &[bool],
    neighbor_mask: Option<&[Vec<u32>]>,
    convert: impl Fn(u32) -> f64,
) -> Vec<Vec<Option<f64>>> {
    let mut result: Vec<Vec<Option<f64>>> = Vec::with_capacity(n_sources);
    for src_idx in 0..n_sources {
        let mut row: Vec<Option<f64>> = Vec::with_capacity(n_targets);
        // Neighbour mask for this source is a sorted Vec<u32>; use binary
        // search so the inner loop is O(n_targets × log k).
        let src_neighbors: Option<&[u32]> = neighbor_mask.map(|nm| nm[src_idx].as_slice());
        for tgt_idx in 0..n_targets {
            if !source_valid[src_idx] || !target_valid[tgt_idx] {
                row.push(None);
                continue;
            }
            if let Some(ns) = src_neighbors
                && ns.binary_search(&(tgt_idx as u32)).is_err()
            {
                row.push(None);
                continue;
            }
            let val = matrix[src_idx * n_targets + tgt_idx];
            if val == u32::MAX {
                row.push(None);
            } else {
                row.push(Some(convert(val)));
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
    State(regions): State<Arc<RegionsState>>,
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

    // Region dispatch (#91): every source + every destination must
    // snap to the same region for the streaming matrix.
    let started_dispatch = std::time::Instant::now();
    let coords_iter = req
        .sources
        .iter()
        .chain(req.destinations.iter())
        .map(|&[lon, lat]| (lon, lat));
    let (state, region_id): (Arc<ServerState>, String) =
        match regions.dispatch_many(coords_iter, &req.mode) {
            Ok(pair) => pair,
            Err(e) => {
                let (code, body) = e.into_response_parts();
                return (code, Json(body)).into_response();
            }
        };

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
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
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // Compute avoid weights (includes exclude if both present)
    let avoid_entry = if let Some(ref avoid_str) = avoid_json {
        match super::avoid::compute_avoid_weights(&state, mode_data, avoid_str, exclude_mask) {
            Ok(entry) => Some(entry),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
            }
        }
    } else {
        None
    };

    // Get exclude weights if only exclude (no avoid)
    let exclude_weights = if avoid_entry.is_none() {
        exclude_mask.map(|exc| state.get_exclude_weights(mode, exc))
    } else {
        None
    };

    // Build snap mask
    let snap_mask: std::borrow::Cow<'_, [u64]> = if let Some(ref entry) = avoid_entry {
        std::borrow::Cow::Owned(super::avoid::build_avoid_mask(
            &mode_data.mask,
            &entry.flags,
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

    // Convert all sources to rank space, keeping track of valid indices.
    // We also keep a full-length snapped coordinate vector (indexed by the
    // original request order) for downstream haversine pre-filtering.
    let mut sources_rank: Vec<u32> = Vec::with_capacity(req.sources.len());
    let mut valid_src_indices: Vec<usize> = Vec::with_capacity(req.sources.len());
    let mut sources_snapped: Vec<(f64, f64)> = Vec::with_capacity(req.sources.len());
    let src_role_filter = SnapRole::Src.role_filter(mode_data);
    let dst_role_filter = SnapRole::Dst.role_filter(mode_data);

    for (i, [lon, lat]) in req.sources.iter().enumerate() {
        let mut matched = false;
        if let Some(orig_id) = state.snap_index.snap_filtered_role(
            *lon,
            *lat,
            mode.0,
            Some(&snap_mask[..]),
            src_role_filter,
        ) {
            let rank = mode_data.orig_to_rank[orig_id as usize];
            if rank != u32::MAX {
                sources_rank.push(rank);
                valid_src_indices.push(i);
                let snapped = get_node_location(&state, orig_id);
                sources_snapped.push((snapped[0], snapped[1]));
                matched = true;
            }
        }
        if !matched {
            sources_snapped.push((*lon, *lat));
        }
    }

    // Convert all destinations to rank space
    let mut targets_rank: Vec<u32> = Vec::with_capacity(req.destinations.len());
    let mut valid_dst_indices: Vec<usize> = Vec::with_capacity(req.destinations.len());
    let mut targets_snapped: Vec<(f64, f64)> = Vec::with_capacity(req.destinations.len());
    for (i, [lon, lat]) in req.destinations.iter().enumerate() {
        let mut matched = false;
        if let Some(orig_id) = state.snap_index.snap_filtered_role(
            *lon,
            *lat,
            mode.0,
            Some(&snap_mask[..]),
            dst_role_filter,
        ) {
            let rank = mode_data.orig_to_rank[orig_id as usize];
            if rank != u32::MAX {
                targets_rank.push(rank);
                valid_dst_indices.push(i);
                let snapped = get_node_location(&state, orig_id);
                targets_snapped.push((snapped[0], snapped[1]));
                matched = true;
            }
        }
        if !matched {
            targets_snapped.push((*lon, *lat));
        }
    }

    // Build the Euclidean neighbour mask once for the entire stream request.
    // Pairs outside the radius will be emitted as u32::MAX by the tile
    // assembler below (and by the small-matrix bucket path).
    let radius_param = parse_radius(req.radius_km.as_ref());
    let neighbor_mask: Option<Arc<Vec<Vec<u32>>>> = match radius_param {
        RadiusParam::None => None,
        RadiusParam::Km(r) => Some(Arc::new(build_neighbors(
            &sources_snapped,
            &targets_snapped,
            r,
        ))),
        RadiusParam::Auto => {
            let r = auto_radius_km(&sources_snapped, &targets_snapped);
            if r > 0.0 {
                Some(Arc::new(build_neighbors(
                    &sources_snapped,
                    &targets_snapped,
                    r,
                )))
            } else {
                None
            }
        }
    };

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

    // ----------------------------------------------------------------
    // Smart algorithm selection:
    //   - Small matrices (N*M <= 50,000): Bucket M2M (fast, low overhead)
    //   - Large matrices: PHAST tiling/streaming (amortizes cost)
    // Both paths return Arrow IPC, so the client sees no difference.
    // ----------------------------------------------------------------
    const BUCKET_M2M_THRESHOLD: usize = 50_000;

    if n_total_sources * n_total_targets <= BUCKET_M2M_THRESHOLD {
        // --- SMALL MATRIX PATH: Bucket M2M → single Arrow IPC tile ---
        // Borrow the flats directly from the cached avoid entry /
        // exclude weights / mode data — no deep clone on the hot path.
        let up_adj_flat: &UpAdjFlat = if let Some(ref entry) = avoid_entry {
            &entry.weights.time_up_flat
        } else if let Some(ref ew) = exclude_weights {
            &ew.time_up_flat
        } else {
            &mode_data.up_adj_flat
        };
        let down_rev_flat: &DownReverseAdjFlat = if let Some(ref entry) = avoid_entry {
            &entry.weights.time_down_flat
        } else if let Some(ref ew) = exclude_weights {
            &ew.time_down_flat
        } else {
            &mode_data.down_rev_flat
        };
        let resp = table_stream_bucket_path(
            n_nodes,
            up_adj_flat,
            down_rev_flat,
            n_total_sources,
            n_total_targets,
            n_total_cells,
            n_valid_sources,
            n_valid_targets,
            &sources_rank,
            &targets_rank,
            &valid_src_indices,
            &valid_dst_indices,
            neighbor_mask.as_ref().map(|v| v.as_slice()),
        );
        super::region_metrics::record_query(
            &region_id,
            "table_stream",
            started_dispatch.elapsed().as_secs_f64(),
        );
        return resp;
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
    let neighbor_mask_for_phast = neighbor_mask.clone();

    // Move the Arc-wrapped state into the spawn_blocking closure so we
    // can borrow flat adjacencies straight from the cached avoid entry
    // / exclude weights / mode_data — no deep clone of the 100+ MB
    // UpAdjFlat / DownReverseAdjFlat on the hot path.
    let state_for_phast = Arc::clone(&state);
    let avoid_entry_for_phast = avoid_entry.clone();
    let exclude_weights_for_phast = exclude_weights.clone();

    // Spawn compute task - SOURCE-BLOCK OUTER LOOP to avoid repeated forward computation
    // For 10k x 10k with 1000 x 1000 tiles: forward computed 10x (once per src block) instead of 100x
    tokio::task::spawn_blocking(move || {
        let cancelled = cancelled_outer;
        let neighbor_mask = neighbor_mask_for_phast;
        let state = state_for_phast;
        let avoid_entry = avoid_entry_for_phast;
        let exclude_weights = exclude_weights_for_phast;
        let mode_data = state.get_mode(mode);
        let up_adj_flat: &UpAdjFlat = if let Some(ref entry) = avoid_entry {
            &entry.weights.time_up_flat
        } else if let Some(ref ew) = exclude_weights {
            &ew.time_up_flat
        } else {
            &mode_data.up_adj_flat
        };
        let down_rev_flat: &DownReverseAdjFlat = if let Some(ref entry) = avoid_entry {
            &entry.weights.time_down_flat
        } else if let Some(ref ew) = exclude_weights {
            &ew.time_down_flat
        } else {
            &mode_data.down_rev_flat
        };
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
                up_adj_flat,
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
                        down_rev_flat,
                        &source_buckets,
                        &block_dst_ranks,
                    );

                    // Map computed distances to output positions. When a
                    // `neighbor_mask` is supplied, pairs not in the source's
                    // neighbour list are skipped — they remain at the
                    // initialised `u32::MAX` (== unreachable) value. The
                    // mask-at-emit fallback is correct because bucket_ch has
                    // already done the full work; avoiding the copy simply
                    // enforces the contract that pruned pairs are null.
                    for (tile_src_idx, &orig_src_idx) in block_src_orig_indices.iter().enumerate() {
                        let out_row = orig_src_idx - src_start;
                        let neighbors: Option<&[u32]> =
                            neighbor_mask.as_ref().map(|nm| nm[orig_src_idx].as_slice());

                        for (tile_dst_idx, &orig_dst_idx) in
                            block_dst_orig_indices.iter().enumerate()
                        {
                            if let Some(ns) = neighbors
                                && ns.binary_search(&(orig_dst_idx as u32)).is_err()
                            {
                                continue;
                            }
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

    super::region_metrics::record_query(
        &region_id,
        "table_stream",
        started_dispatch.elapsed().as_secs_f64(),
    );
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
    neighbor_mask: Option<&[Vec<u32>]>,
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
        let neighbors: Option<&[u32]> = neighbor_mask.map(|nm| nm[orig_src_idx].as_slice());
        for (valid_dst_idx, &orig_dst_idx) in valid_dst_indices.iter().enumerate() {
            if let Some(ns) = neighbors
                && ns.binary_search(&(orig_dst_idx as u32)).is_err()
            {
                continue;
            }
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
