//! /match handler — GPS trace map matching (HMM + Viterbi)

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

use super::geometry::{GeometryFormat, RouteGeometry, build_geometry};
use super::regions::RegionsState;
use super::route::{RouteStep, build_steps, lookup_road_name};
use super::state::ServerState;
use super::types::{parse_mode, validate_coord};

// ============ Types ============

/// Request for GPS trace map matching
#[derive(Debug, Deserialize, ToSchema)]
pub struct MatchRequest {
    /// GPS coordinates [[lon, lat], ...] -- at least 2 points
    #[schema(example = json!([[4.3517, 50.8503], [4.3537, 50.8513], [4.3557, 50.8523], [4.3577, 50.8533]]))]
    coordinates: Vec<[f64; 2]>,
    /// Transport mode (e.g. "car", "bike", "foot" -- depends on available models)
    #[serde(default = "default_match_mode")]
    #[schema(example = "car")]
    mode: String,
    /// GPS accuracy in meters (default: 10)
    #[serde(default)]
    #[schema(example = 10.0)]
    gps_accuracy: Option<f64>,
    /// Geometry format: "polyline6" (default), "geojson", or "points"
    #[serde(default = "default_match_geometry")]
    #[schema(example = "polyline6")]
    geometry: String,
    /// Whether to include turn-by-turn steps
    #[serde(default)]
    #[schema(example = true)]
    steps: bool,
    /// Exclude road types: comma-separated list of "toll", "ferry", "motorway"
    #[serde(default)]
    exclude: Option<String>,
    /// Avoid polygon(s) as JSON array of coordinate rings
    #[serde(default)]
    avoid_polygons: Option<String>,
}

pub fn default_match_mode() -> String {
    "car".to_string()
}

pub fn default_match_geometry() -> String {
    "polyline6".to_string()
}

/// Response for map matching
#[derive(Debug, Serialize, ToSchema)]
pub struct MatchResponse {
    /// Status code
    code: String,
    /// Matched routes (trace may be split at gaps)
    matchings: Vec<MatchMatching>,
    /// Per-observation tracepoint info (null if observation couldn't be matched)
    #[schema(value_type = Vec<Option<MatchTracepoint>>)]
    tracepoints: Vec<Option<MatchTracepoint>>,
}

/// A matched sub-route
#[derive(Debug, Serialize, ToSchema)]
pub struct MatchMatching {
    /// Route geometry
    geometry: RouteGeometry,
    /// Duration in seconds
    duration: f64,
    /// Distance in meters
    distance: f64,
    /// Confidence score (0.0 to 1.0)
    confidence: f64,
    /// Turn-by-turn steps (if requested)
    #[serde(skip_serializing_if = "Option::is_none")]
    steps: Option<Vec<RouteStep>>,
}

/// Tracepoint in matched response
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct MatchTracepoint {
    /// Snapped location [lon, lat]
    location: [f64; 2],
    /// Road name at this location (empty if unknown)
    name: String,
    /// Index into the matchings array
    matchings_index: usize,
    /// Index within the matching's waypoint sequence
    waypoint_index: usize,
}

// ============ Handler ============

/// Map match a GPS trace to the road network
#[utoipa::path(
    post,
    path = "/match",
    tag = "Search",
    summary = "Map match a GPS trace to the road network",
    description = "Snaps a sequence of GPS coordinates to the most likely route on the road network\nusing HMM + Viterbi decoding (Newson & Krumm 2009).\n\nThe trace may be split into multiple sub-matchings if gaps are detected.\nMaximum 500 coordinates per request.",
    request_body(content = MatchRequest, description = "GPS trace coordinates with optional accuracy",
        example = json!({
            "coordinates": [[4.3517, 50.8503], [4.3537, 50.8513], [4.3557, 50.8523], [4.3577, 50.8533]],
            "mode": "car",
            "gps_accuracy": 10.0,
            "steps": true
        })
    ),
    responses(
        (status = 200, description = "Trace matched", body = MatchResponse),
        (status = 400, description = "Bad request", body = super::types::ErrorResponse),
        (status = 404, description = "No match found", body = super::types::ErrorResponse),
    )
)]
pub async fn match_trace_handler(
    State(regions): State<Arc<RegionsState>>,
    Json(req): Json<MatchRequest>,
) -> impl IntoResponse {
    if req.coordinates.len() < 2 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "At least 2 coordinates required"
            })),
        )
            .into_response();
    }

    // Region dispatch (#91 + #194):
    //   - Single-region traces: take the existing intra-region fast
    //     path (same code as before #194; zero overhead).
    //   - Cross-region traces (#194): when an overlay is loaded and
    //     the trace spans regions, route through
    //     `map_match_multi_region`. When no overlay is loaded, fall
    //     back to the historical 501 response.
    let started_dispatch = std::time::Instant::now();
    let coords_iter = req.coordinates.iter().map(|&[lon, lat]| (lon, lat));
    let (state, region_id): (Arc<ServerState>, String) =
        match regions.dispatch_many(coords_iter, &req.mode) {
            Ok(pair) => pair,
            Err(super::regions::DispatchError::CrossRegion { .. })
                if regions.overlay.is_some() =>
            {
                return cross_region_match_inner(regions, req, started_dispatch).await;
            }
            Err(e) => {
                let (code, body) = e.into_response_parts();
                return (
                    code,
                    Json(serde_json::json!({
                        "code": "InvalidValue",
                        "message": body.error
                    })),
                )
                    .into_response();
            }
        };

    // Validate mode
    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response();
        }
    };

    // Validate coordinates
    if req.coordinates.len() < 2 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "At least 2 coordinates required"
            })),
        )
            .into_response();
    }

    if req.coordinates.len() > 500 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "Maximum 500 coordinates allowed"
            })),
        )
            .into_response();
    }

    for (i, &[lon, lat]) in req.coordinates.iter().enumerate() {
        if let Err(e) = validate_coord(lon, lat, &format!("coordinate[{}]", i)) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response();
        }
    }

    // Validate GPS accuracy
    if let Some(acc) = req.gps_accuracy
        && (acc <= 0.0 || acc > 100.0 || acc.is_nan())
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "gps_accuracy must be between 0 and 100 meters"
            })),
        )
            .into_response();
    }

    // Parse geometry format
    let geom_format = match GeometryFormat::parse(&req.geometry) {
        Ok(f) => f,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response();
        }
    };

    // Parse exclude parameter
    let exclude_mask = match super::exclude::parse_exclude_option(&req.exclude) {
        Ok(m) => m,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response();
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response();
        }
    };

    // Convert coordinates
    let coords: Vec<(f64, f64)> = req
        .coordinates
        .iter()
        .map(|&[lon, lat]| (lon, lat))
        .collect();

    // Extract owned values before the spawn_blocking closure
    let gps_accuracy = req.gps_accuracy;
    let want_steps = req.steps;

    // Map matching is CPU-heavy: HMM Viterbi decoding with many sequential P2P queries
    // for long GPS traces can take seconds. Offload to a blocking thread to avoid
    // starving the Tokio async runtime under high concurrency.
    let state_clone = state.clone();
    let blocking_result = tokio::task::spawn_blocking(move || {
        // Build snap mask and weights: avoid takes priority, then exclude
        let mode_data = state_clone.get_mode(mode);

        let avoid_result = if let Some(ref avoid_str) = avoid_json {
            super::avoid::compute_avoid_weights(&state_clone, mode_data, avoid_str, exclude_mask)
                .ok()
        } else {
            None
        };

        let exclude_weights = if avoid_result.is_none() {
            exclude_mask.map(|exc| state_clone.get_exclude_weights(mode, exc))
        } else {
            None
        };

        let snap_mask: Option<Vec<u64>> = if let Some((_, ref avoid_flags)) = avoid_result {
            Some(super::avoid::build_avoid_mask(
                &mode_data.mask,
                avoid_flags,
                exclude_mask.map(|exc| (state_clone.edge_exclude_flags.as_slice(), exc)),
            ))
        } else {
            exclude_mask.map(|exc| {
                super::exclude::build_exclude_mask(
                    &mode_data.mask,
                    &state_clone.edge_exclude_flags,
                    exc,
                )
            })
        };

        let cch_weights = if let Some((ref aw, _)) = avoid_result {
            Some(&aw.time_weights)
        } else {
            exclude_weights.as_ref().map(|ew| &ew.time_weights)
        };

        // Run map matching -- returns None if no observations could be matched
        let result = super::map_match::map_match(
            &state_clone,
            mode,
            &coords,
            gps_accuracy,
            snap_mask.as_deref(),
            cch_weights,
        )?;

        // Build response
        let matchings: Vec<MatchMatching> = result
            .matchings
            .iter()
            .map(|m| {
                let (geometry, distance_m) = build_geometry(
                    &m.ebg_path,
                    &state_clone.ebg_nodes,
                    &state_clone.edge_geom,
                    geom_format,
                );
                let duration_s = m.duration_ds as f64 / 10.0;

                let steps = if want_steps {
                    Some(build_steps(
                        &m.ebg_path,
                        &state_clone.ebg_nodes,
                        &state_clone.nbg_geo,
                        &state_clone.edge_geom,
                        &mode_data.node_weights,
                        &state_clone.way_names,
                        geom_format,
                    ))
                } else {
                    None
                };

                MatchMatching {
                    geometry,
                    duration: duration_s,
                    distance: distance_m,
                    confidence: m.confidence,
                    steps,
                }
            })
            .collect();

        let tracepoints: Vec<Option<MatchTracepoint>> = result
            .tracepoints
            .iter()
            .map(|tp| {
                tp.as_ref().map(|t| {
                    let name = lookup_road_name(
                        t.ebg_id,
                        &state_clone.ebg_nodes,
                        &state_clone.nbg_geo,
                        &state_clone.way_names,
                    )
                    .unwrap_or_default();
                    MatchTracepoint {
                        location: [t.lon, t.lat],
                        name,
                        matchings_index: t.matchings_index,
                        waypoint_index: t.waypoint_index,
                    }
                })
            })
            .collect();

        Some(MatchResponse {
            code: "Ok".to_string(),
            matchings,
            tracepoints,
        })
    })
    .await;

    let resp = match blocking_result {
        Ok(Some(response)) => Json(response).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NoMatch",
                "message": "Could not match trace to road network"
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "InternalError",
                "message": format!("map match computation failed: {}", e)
            })),
        )
            .into_response(),
    };
    super::region_metrics::record_query(
        &region_id,
        "match",
        started_dispatch.elapsed().as_secs_f64(),
    );
    resp
}

// =============================================================================
// Cross-region map matching (#194)
// =============================================================================

/// Handle a cross-region GPS trace by dispatching to
/// [`super::map_match::map_match_multi_region`]. The trace is built
/// against the union of every loaded region; when transitions cross a
/// boundary, the new function consults the overlay matrix instead of
/// returning 501.
///
/// The output `MatchResponse.matchings` is split into one entry per
/// contiguous same-region run, mirroring the intra-region split-at-
/// gaps semantics. Each `Matching` carries its own geometry, duration,
/// distance, and confidence; the caller can stitch them client-side
/// (typical mobile-app behaviour) or treat them as independent runs.
async fn cross_region_match_inner(
    regions: Arc<RegionsState>,
    req: MatchRequest,
    started_dispatch: std::time::Instant,
) -> axum::response::Response {
    // Validate inputs (same checks as the single-region path).
    if req.coordinates.len() < 2 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "At least 2 coordinates required"
            })),
        )
            .into_response();
    }
    if req.coordinates.len() > 500 {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "Maximum 500 coordinates allowed"
            })),
        )
            .into_response();
    }
    for (i, &[lon, lat]) in req.coordinates.iter().enumerate() {
        if let Err(e) = validate_coord(lon, lat, &format!("coordinate[{}]", i)) {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response();
        }
    }
    if let Some(acc) = req.gps_accuracy
        && (acc <= 0.0 || acc > 100.0 || acc.is_nan())
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "gps_accuracy must be between 0 and 100 meters"
            })),
        )
            .into_response();
    }

    let geom_format = match GeometryFormat::parse(&req.geometry) {
        Ok(f) => f,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({ "code": "InvalidValue", "message": e })),
            )
                .into_response();
        }
    };

    // Cross-region path does not support exclude/avoid in the MVP —
    // those would require per-region recustomization wired into the
    // overlay solver, which is out of scope for #194. Reject with a
    // clear error.
    if req.exclude.as_deref().is_some_and(|s| !s.is_empty()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "exclude is not supported on cross-region map matching (yet)"
            })),
        )
            .into_response();
    }
    if req.avoid_polygons.as_deref().is_some_and(|s| !s.is_empty()) {
        return (
            StatusCode::BAD_REQUEST,
            Json(serde_json::json!({
                "code": "InvalidValue",
                "message": "avoid_polygons is not supported on cross-region map matching (yet)"
            })),
        )
            .into_response();
    }

    let coords: Vec<(f64, f64)> = req
        .coordinates
        .iter()
        .map(|&[lon, lat]| (lon, lat))
        .collect();
    let mode_name = req.mode.clone();
    let gps_accuracy = req.gps_accuracy;
    let want_steps = req.steps;
    let regions_clone = regions.clone();

    let blocking_result = tokio::task::spawn_blocking(move || {
        let result = super::map_match::map_match_multi_region(
            &regions_clone,
            &mode_name,
            &coords,
            gps_accuracy,
        )?;

        let matchings: Vec<MatchMatching> = result
            .matchings
            .iter()
            .map(|m| {
                let region_idx = m.region_idx;
                let entry = &regions_clone.regions[region_idx];
                let mode_idx = entry
                    .state
                    .mode_lookup
                    .get(&mode_name)
                    .copied()
                    .unwrap_or(0);
                let mode_data = entry.state.get_mode(crate::profile_abi::Mode(mode_idx));
                let (geometry, distance_m) = build_geometry(
                    &m.ebg_path,
                    &entry.state.ebg_nodes,
                    &entry.state.edge_geom,
                    geom_format,
                );
                let duration_s = m.duration_ds as f64 / 10.0;
                let steps = if want_steps {
                    Some(build_steps(
                        &m.ebg_path,
                        &entry.state.ebg_nodes,
                        &entry.state.nbg_geo,
                        &entry.state.edge_geom,
                        &mode_data.node_weights,
                        &entry.state.way_names,
                        geom_format,
                    ))
                } else {
                    None
                };
                MatchMatching {
                    geometry,
                    duration: duration_s,
                    distance: distance_m,
                    confidence: m.confidence,
                    steps,
                }
            })
            .collect();

        let tracepoints: Vec<Option<MatchTracepoint>> = result
            .tracepoints
            .iter()
            .map(|tp| {
                tp.as_ref().map(|t| {
                    // Use the matching's region_idx — tracepoint and
                    // matching share the same region by construction.
                    let region_idx = result
                        .matchings
                        .get(t.matchings_index)
                        .map(|m| m.region_idx)
                        .unwrap_or(0);
                    let entry = &regions_clone.regions[region_idx];
                    let name = lookup_road_name(
                        t.ebg_id,
                        &entry.state.ebg_nodes,
                        &entry.state.nbg_geo,
                        &entry.state.way_names,
                    )
                    .unwrap_or_default();
                    MatchTracepoint {
                        location: [t.lon, t.lat],
                        name,
                        matchings_index: t.matchings_index,
                        waypoint_index: t.waypoint_index,
                    }
                })
            })
            .collect();

        Some(MatchResponse {
            code: "Ok".to_string(),
            matchings,
            tracepoints,
        })
    })
    .await;

    let resp = match blocking_result {
        Ok(Some(response)) => Json(response).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(serde_json::json!({
                "code": "NoMatch",
                "message": "Could not match cross-region trace"
            })),
        )
            .into_response(),
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "code": "InternalError",
                "message": format!("map match computation failed: {}", e)
            })),
        )
            .into_response(),
    };
    super::region_metrics::record_query(
        "cross_region",
        "match",
        started_dispatch.elapsed().as_secs_f64(),
    );
    resp
}
