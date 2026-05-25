//! /route handler — point-to-point routing with geometry, steps, alternatives

use axum::{
    Json,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

use super::geometry::{GeometryFormat, Point, RouteGeometry, build_geometry, build_raw_points};
use super::query::CchQuery;
use super::regions::RegionsState;
use super::state::ServerState;
use super::types::{ErrorResponse, SnapRole, parse_mode, validate_coord};
use super::unpack::unpack_path;

// ============ Types ============

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
    /// Optional traffic profile name. If set, the server routes against
    /// the synthetic mode `<mode>_<traffic>` (e.g. `car` + `rush_hour` →
    /// `car_rush_hour`). The variant must have been built by
    /// `step8-customize --traffic ...` at pipeline time.
    #[serde(default)]
    traffic: Option<String>,
    /// Geometry encoding: polyline6 (default), geojson, points
    #[serde(default = "default_geometries")]
    geometries: String,
    /// Number of alternative routes (0 or 1 = single route, max 5)
    #[serde(default = "default_alternatives")]
    alternatives: u32,
    /// Include turn-by-turn step instructions
    #[serde(default)]
    steps: bool,
    /// Per-edge annotations: comma-separated list of "duration", "distance", "speed", "nodes"
    #[serde(default)]
    annotations: Option<String>,
    /// Bearing hints per waypoint: "angle,range;angle,range" (0-360 degrees).
    /// First pair for source, second for destination. Filters snap candidates by edge direction.
    #[serde(default)]
    bearings: Option<String>,
    /// Exclude road types: comma-separated list of "toll", "ferry", "motorway"
    #[serde(default)]
    exclude: Option<String>,
    /// Avoid polygon(s) as JSON: `[[lon,lat],...]` or `[[[lon,lat],...],...]`
    #[serde(default)]
    avoid_polygons: Option<String>,
    /// Include debug information in response
    #[serde(default)]
    debug: bool,
}

pub fn default_alternatives() -> u32 {
    0
}

pub fn default_geometries() -> String {
    "polyline6".to_string()
}

pub fn default_direction() -> String {
    "depart".to_string()
}

/// Debug information about snapping
#[derive(Debug, Serialize, ToSchema)]
pub struct SnapInfo {
    /// Snapped longitude
    pub lon: f64,
    /// Snapped latitude
    pub lat: f64,
    /// Distance from original coordinate to snapped point (meters)
    pub snap_distance_m: f64,
    /// Internal EBG node ID (for debugging)
    pub ebg_node_id: u32,
}

/// Debug information for route response
#[derive(Debug, Serialize, ToSchema)]
pub struct RouteDebugInfo {
    /// Where the source was snapped to
    pub src_snapped: SnapInfo,
    /// Where the destination was snapped to
    pub dst_snapped: SnapInfo,
}

/// Per-edge annotation arrays for a route
#[derive(Debug, Serialize, ToSchema)]
pub struct RouteAnnotations {
    /// Per-edge duration in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub duration: Option<Vec<f64>>,
    /// Per-edge distance in meters
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance: Option<Vec<f64>>,
    /// Per-edge speed in km/h
    #[serde(skip_serializing_if = "Option::is_none")]
    pub speed: Option<Vec<f64>>,
    /// Per-edge EBG node IDs
    #[serde(skip_serializing_if = "Option::is_none")]
    pub nodes: Option<Vec<u32>>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct RouteResponse {
    /// Primary route duration in seconds
    pub duration_s: f64,
    /// Primary route distance in meters
    pub distance_m: f64,
    /// Primary route geometry
    pub geometry: RouteGeometry,
    /// Turn-by-turn steps (only if steps=true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<Vec<RouteStep>>,
    /// Per-edge annotations (only if annotations param is set)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub annotations: Option<RouteAnnotations>,
    /// Alternative routes (only if alternatives > 0)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub alternatives: Option<Vec<RouteAlternative>>,
    /// Debug information (only present if debug=true in request)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub debug: Option<RouteDebugInfo>,
}

/// An alternative route
#[derive(Debug, Serialize, ToSchema)]
pub struct RouteAlternative {
    /// Duration in seconds
    pub duration_s: f64,
    /// Distance in meters
    pub distance_m: f64,
    /// Route geometry
    pub geometry: RouteGeometry,
    /// Turn-by-turn steps (only if steps=true)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub steps: Option<Vec<RouteStep>>,
}

/// A step in turn-by-turn instructions
#[derive(Debug, Serialize, ToSchema)]
pub struct RouteStep {
    /// Distance of this step in meters
    pub distance_m: f64,
    /// Duration of this step in seconds
    pub duration_s: f64,
    /// Geometry of this step
    pub geometry: RouteGeometry,
    /// Maneuver at the start of this step
    pub maneuver: StepManeuver,
}

/// Maneuver instruction
#[derive(Debug, Serialize, ToSchema)]
pub struct StepManeuver {
    /// Location [lon, lat] of the maneuver
    pub location: [f64; 2],
    /// Bearing before the maneuver (0-360 degrees)
    pub bearing_before: u16,
    /// Bearing after the maneuver (0-360 degrees)
    pub bearing_after: u16,
    /// Turn type: depart, arrive, turn, continue, roundabout, fork, merge
    #[serde(rename = "type")]
    pub maneuver_type: String,
    /// Turn modifier: left, right, slight left, slight right, sharp left, sharp right, uturn, straight
    #[serde(skip_serializing_if = "Option::is_none")]
    pub modifier: Option<String>,
    /// Road name at this maneuver (e.g. "Rue de la Loi")
    #[serde(skip_serializing_if = "Option::is_none")]
    pub name: Option<String>,
}

// ============ Handler ============

/// Calculate route between two points
///
/// Supports alternative routes via `alternatives` parameter
/// and turn-by-turn instructions via `steps=true`.
#[utoipa::path(
    get,
    path = "/route",
    tag = "Routing",
    summary = "Calculate route between two points",
    description = "Computes the shortest path between source and destination using edge-based CCH.\nSupports turn-by-turn instructions with road names and alternative routes.\n\nContent negotiation:\n- `Accept: application/json` (default) -> JSON response\n- `Accept: application/gpx+xml` -> GPX 1.1 XML track",
    params(
        ("src_lon" = f64, Query, description = "Source longitude", example = 4.3517),
        ("src_lat" = f64, Query, description = "Source latitude", example = 50.8503),
        ("dst_lon" = f64, Query, description = "Destination longitude", example = 4.4017),
        ("dst_lat" = f64, Query, description = "Destination latitude", example = 50.8603),
        ("mode" = String, Query, description = "Transport mode (e.g. car, bike, foot — depends on available models)", example = "car"),
        ("geometries" = Option<String>, Query, description = "Geometry encoding: polyline6 (default), geojson, points", example = "polyline6"),
        ("alternatives" = Option<u32>, Query, description = "Number of alternative routes (0-5)", example = 0),
        ("steps" = Option<bool>, Query, description = "Include turn-by-turn instructions with road names", example = true),
        ("annotations" = Option<String>, Query, description = "Per-edge annotations: comma-separated list of 'duration', 'distance', 'speed', 'nodes'", example = json!(null)),
        ("bearings" = Option<String>, Query, description = "Bearing hints: 'angle,range;angle,range' (source;destination). Filters snap by edge bearing.", example = json!(null)),
        ("exclude" = Option<String>, Query, description = "Exclude road types: comma-separated list of 'toll', 'ferry', 'motorway'", example = json!(null)),
    ),
    responses(
        (status = 200, description = "Route found", body = RouteResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 404, description = "No route found", body = ErrorResponse),
    )
)]
// Note: route computation is fast (<10ms typical) and bounded by ConcurrencyLimitLayer(32),
// so spawn_blocking is not needed here. /match and /trip use spawn_blocking for long computations.
pub async fn route_handler(
    State(regions): State<Arc<RegionsState>>,
    Query(req): Query<RouteRequest>,
    headers: HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = validate_coord(req.src_lon, req.src_lat, "source") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
    }
    if let Err(e) = validate_coord(req.dst_lon, req.dst_lat, "destination") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
    }

    // Region dispatch (#91 Phase 2): when an overlay is loaded, hand
    // cross-region queries off to the cross-region coordinator instead
    // of returning 501. Same-region queries always fall through to the
    // existing intra-region implementation below.
    let started_dispatch = std::time::Instant::now();
    let (state, region_id): (Arc<ServerState>, String) = match regions.dispatch_p2p_with_overlay(
        req.src_lon,
        req.src_lat,
        req.dst_lon,
        req.dst_lat,
        &req.mode,
    ) {
        Ok(super::regions::P2pPlan::SameRegion { state, region }) => (state, region),
        Ok(super::regions::P2pPlan::CrossRegion {
            src_state,
            src_region,
            dst_state,
            dst_region,
            overlay,
        }) => {
            return cross_region_route_inner(
                src_state, src_region, dst_state, dst_region, overlay, req,
            )
            .into_response();
        }
        Err(e) => {
            let (code, body) = e.into_response_parts();
            return (code, Json(body)).into_response();
        }
    };

    // Resolve the effective mode name. If `traffic=<v>` is set, synthesize
    // `<mode>_<v>` and look that up — produced at pipeline time by
    // `step8-customize --traffic ...`. Falling back to the base mode is
    // intentionally disabled: a 400 is preferable to silently routing on
    // freeflow weights when the caller asked for traffic.
    let effective_mode_name = match &req.traffic {
        Some(v) if !v.trim().is_empty() => format!("{}_{}", req.mode, v.trim()),
        _ => req.mode.clone(),
    };
    let mode = match parse_mode(&effective_mode_name, &state.mode_lookup) {
        Ok(m) => m,
        Err(_) if req.traffic.is_some() => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!(
                        "Unknown traffic variant '{}' for mode '{}'. Build it with `step8-customize --traffic`.",
                        req.traffic.as_deref().unwrap_or(""),
                        req.mode
                    ),
                }),
            )
                .into_response();
        }
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let geom_format = match GeometryFormat::parse(&req.geometries) {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Parse and validate annotations parameter
    let annotation_flags = if let Some(ref ann_str) = req.annotations {
        let mut want_duration = false;
        let mut want_distance = false;
        let mut want_speed = false;
        let mut want_nodes = false;
        if !ann_str.is_empty() {
            for token in ann_str.split(',') {
                let token = token.trim();
                match token {
                    "duration" => want_duration = true,
                    "distance" => want_distance = true,
                    "speed" => want_speed = true,
                    "nodes" => want_nodes = true,
                    other => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(ErrorResponse {
                                error: format!(
                                    "Unknown annotation '{}'. Valid: duration, distance, speed, nodes",
                                    other
                                ),
                            }),
                        )
                            .into_response();
                    }
                }
            }
        }
        Some((want_duration, want_distance, want_speed, want_nodes))
    } else {
        None
    };

    // Parse bearing hints: "angle,range;angle,range" (source;destination)
    let bearing_hints: Option<Vec<(u16, u16)>> = if let Some(ref b_str) = req.bearings {
        let mut hints = Vec::new();
        for part in b_str.split(';') {
            let part = part.trim();
            if part.is_empty() {
                hints.push((0, 360)); // no constraint
                continue;
            }
            let tokens: Vec<&str> = part.split(',').collect();
            if tokens.len() != 2 {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: format!(
                            "Invalid bearing format '{}'. Expected 'angle,range'.",
                            part
                        ),
                    }),
                )
                    .into_response();
            }
            let angle: u16 = match tokens[0].trim().parse() {
                Ok(v) if v <= 360 => v,
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("Invalid bearing angle: '{}'", tokens[0]),
                        }),
                    )
                        .into_response();
                }
            };
            let range: u16 = match tokens[1].trim().parse() {
                Ok(v) if v <= 180 => v,
                _ => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("Invalid bearing range: '{}'", tokens[1]),
                        }),
                    )
                        .into_response();
                }
            };
            hints.push((angle, range));
        }
        if hints.len() > 2 {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!(
                        "bearings has {} pairs, expected at most 2 (source;destination)",
                        hints.len()
                    ),
                }),
            )
                .into_response();
        }
        Some(hints)
    } else {
        None
    };

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
    let num_alternatives = (req.alternatives.min(5)) as usize;

    // Compute avoid weights — time-only for P2P route (skip distance + flat adj).
    // Uses sparse triangle relaxation; #238 fix made pass 1 of sparse mark every
    // node that is either incident to a changed edge OR a potential middle of a
    // triangle relaxing one, so it now matches the full path's output. /route
    // stays on the fast time-only path.
    let avoid_entry = if let Some(ref avoid_str) = avoid_json {
        match super::avoid::compute_avoid_weights_time_only(
            &state,
            mode_data,
            avoid_str,
            exclude_mask,
        ) {
            Ok(entry) => Some(entry),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
            }
        }
    } else {
        None
    };

    // Build snap mask (with optional avoid/exclude filtering)
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

    // #197: role-aware snap with multi-candidate fallback. Source
    // point must snap to an EBG node with at least one mode-valid
    // OUTBOUND arc; destination must snap to one with at least one
    // INBOUND arc. The `Src`/`Dst` enum values resolve to the per-
    // mode `has_outbound` / `has_inbound` bitsets built at boot.
    //
    // Even with role filtering, the geometrically-closest candidate
    // can still be the wrong one for two-way roads where both EBG
    // nodes (one per direction of travel) are present at the same
    // polyline vertex. We collect the top-K candidates per role, try
    // (src, dst) combinations in expected-best order, and pick the
    // first pair that produces a valid route. K is SNAP_K (defined
    // below — 64 at the time of this fix) and the fallback only runs
    // when the primary candidate fails. Typical healthy queries pay
    // one P2P query; pathological pairs pay up to MAX_FALLBACK_COMBOS
    // (400) — see the SNAP_K block below for the empirical sweep.
    let src_role_filter = SnapRole::Src.role_filter(mode_data);
    let dst_role_filter = SnapRole::Dst.role_filter(mode_data);

    // SNAP_K = number of EBG-id candidates per role. The same
    // physical polyline vertex contributes 2 candidates (one per
    // traversal direction of the underlying NBG edge), so K=64 ≈
    // 32 unique physical points per side. Empirically (#197
    // verification on 1563 Belgium pairs that OSRM routes but
    // pre-fix Butterfly 404s):
    //   K=4,  cap=16  →  89 % fixed
    //   K=8,  cap=64  →  94 % fixed
    //   K=20, cap=96  →  96.7 % fixed
    //   K=32, cap=192 →  98 % fixed
    //   K=64, cap=400 →  98.7 % fixed
    // The role masks are connectivity-aware: source candidates must
    // be able to reach the main routing core and destination candidates
    // must be reachable from it. The K-best fallback remains for
    // same-geometry directional ambiguity and dynamic exclude/avoid
    // cases, not for ordinary disconnected-component cleanup.
    //
    // Best-case (top-1 src × top-1 dst routes) is one P2P query.
    // Worst case is bounded by MAX_FALLBACK_COMBOS below; typical
    // Belgium P2P is 5-50 ms so tail latency is ~2-20 s for the
    // ~1.3 % pathological pairs. Best-case latency on healthy
    // pairs is unchanged from pre-fix.
    const SNAP_K: usize = 64;
    let src_bearing = bearing_hints.as_ref().and_then(|h| h.first().copied());
    let dst_bearing = bearing_hints.as_ref().and_then(|h| h.get(1).copied());

    // Snap top-K source candidates.
    let src_candidates: Vec<(u32, f64, f64, f64)> = if let Some((angle, range)) = src_bearing {
        // Bearing-filtered top-K: collect candidates by repeated
        // bearing-filtered single snaps over an exclusion bitset.
        // For now we accept that bearing hints reduce to a single
        // best candidate (the historical behaviour) — bearing implies
        // direction, so the selected candidate is already the
        // intended one. K=1 path.
        match state.snap_index.snap_with_bearing_filtered_role(
            req.src_lon,
            req.src_lat,
            mode.0,
            angle,
            range,
            Some(&snap_mask),
            src_role_filter,
        ) {
            Some(t) => vec![t],
            None => Vec::new(),
        }
    } else {
        state.snap_index.snap_k_with_info_filtered_role(
            req.src_lon,
            req.src_lat,
            mode.0,
            SNAP_K,
            Some(&snap_mask),
            src_role_filter,
        )
    };
    if src_candidates.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Could not snap source to road network".to_string(),
            }),
        )
            .into_response();
    }

    // Snap top-K destination candidates.
    let dst_candidates: Vec<(u32, f64, f64, f64)> = if let Some((angle, range)) = dst_bearing {
        match state.snap_index.snap_with_bearing_filtered_role(
            req.dst_lon,
            req.dst_lat,
            mode.0,
            angle,
            range,
            Some(&snap_mask),
            dst_role_filter,
        ) {
            Some(t) => vec![t],
            None => Vec::new(),
        }
    } else {
        state.snap_index.snap_k_with_info_filtered_role(
            req.dst_lon,
            req.dst_lat,
            mode.0,
            SNAP_K,
            Some(&snap_mask),
            dst_role_filter,
        )
    };
    if dst_candidates.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Could not snap destination to road network".to_string(),
            }),
        )
            .into_response();
    }

    // Pick the primary (best) candidates. The fallback search runs
    // later, after the CCH query is built, so we can run multiple
    // P2P queries against the same query state with cheap retries.
    // The snap-info reported back to the client is updated to match
    // whichever candidate produced the winning route.
    let make_snap_info = |t: &(u32, f64, f64, f64)| -> (u32, SnapInfo) {
        (
            t.0,
            SnapInfo {
                lon: t.1,
                lat: t.2,
                snap_distance_m: t.3,
                ebg_node_id: t.0,
            },
        )
    };
    let (src_orig, mut src_snap_info) = make_snap_info(&src_candidates[0]);
    let (_dst_orig, mut dst_snap_info) = make_snap_info(&dst_candidates[0]);

    // Convert to rank space directly (#153: collapses
    // original_to_filtered → perm into a single mapping read).
    // Map candidate lists from EBG ids to ranks, dropping any that
    // resolve to u32::MAX (not in the mode's filtered subgraph). If
    // role filtering is correct the lookup should always succeed —
    // we filter defensively to avoid a hard error mid-fallback.
    let src_rank_candidates: Vec<u32> = src_candidates
        .iter()
        .map(|c| mode_data.orig_to_rank[c.0 as usize])
        .filter(|&r| r != u32::MAX)
        .collect();
    let dst_rank_candidates: Vec<u32> = dst_candidates
        .iter()
        .map(|c| mode_data.orig_to_rank[c.0 as usize])
        .filter(|&r| r != u32::MAX)
        .collect();
    if src_rank_candidates.is_empty() || dst_rank_candidates.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Snapped node not accessible for this mode".to_string(),
            }),
        )
            .into_response();
    }
    let mut src_rank = src_rank_candidates[0];
    let mut dst_rank = dst_rank_candidates[0];
    // Track which candidate index won so we can update the snap-info
    // block returned in the response.
    let mut chosen_src_idx: usize = 0;
    let mut chosen_dst_idx: usize = 0;

    // Same-edge: return consistent zero-distance, zero-duration result
    if src_rank == dst_rank {
        let snap_point = Point {
            lon: src_snap_info.lon,
            lat: src_snap_info.lat,
        };

        if wants_gpx(&headers) {
            super::region_metrics::record_query(
                &region_id,
                "route",
                started_dispatch.elapsed().as_secs_f64(),
            );
            return gpx_response(format_gpx(&[snap_point], "Route"));
        }

        let point_geom = RouteGeometry::from_points(vec![snap_point], geom_format);
        let debug_info = if req.debug {
            Some(RouteDebugInfo {
                src_snapped: src_snap_info,
                dst_snapped: dst_snap_info,
            })
        } else {
            None
        };
        super::region_metrics::record_query(
            &region_id,
            "route",
            started_dispatch.elapsed().as_secs_f64(),
        );
        return Json(RouteResponse {
            duration_s: 0.0,
            distance_m: 0.0,
            geometry: point_geom,
            steps: if req.steps { Some(vec![]) } else { None },
            annotations: None,
            alternatives: None,
            debug: debug_info,
        })
        .into_response();
    }

    // Helper: build route from query result — returns (geometry, duration_s, distance_m, steps, ebg_path)
    // src_rank / dst_rank are passed in so the closure doesn't bind
    // them by reference (which would conflict with the #197 fallback
    // logic that reassigns src_rank / dst_rank on a successful retry).
    let build_route = |result: &super::query::QueryResult,
                       weights: &super::state::CchWeights,
                       format: GeometryFormat,
                       want_steps: bool,
                       src_rank: u32,
                       dst_rank: u32|
     -> (RouteGeometry, f64, f64, Option<Vec<RouteStep>>, Vec<u32>) {
        let rank_path = unpack_path(
            &mode_data.cch_topo,
            weights,
            &result.forward_parent,
            &result.backward_parent,
            src_rank,
            dst_rank,
            result.meeting_node,
        );
        let ebg_path: Vec<u32> = rank_path
            .iter()
            .map(|&rank| {
                let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
                mode_data.filtered_to_original[filtered_id as usize]
            })
            .collect();
        let (geometry, distance_m) =
            build_geometry(&ebg_path, &state.ebg_nodes, &state.edge_geom, format);
        let duration_s = result.distance as f64;
        let steps = if want_steps {
            Some(build_steps(
                &ebg_path,
                &state.ebg_nodes,
                &state.nbg_geo,
                &state.edge_geom,
                &mode_data.node_weights,
                &state.way_names,
                format,
            ))
        } else {
            None
        };
        (geometry, duration_s, distance_m, steps, ebg_path)
    };

    // Run primary query (with optional avoid/exclude weights)
    let exclude_weights = if avoid_entry.is_none() {
        exclude_mask.map(|exc| state.get_exclude_weights(mode, exc))
    } else {
        None // avoid_entry already incorporates exclude
    };
    let query = if let Some(ref entry) = avoid_entry {
        CchQuery::with_custom_weights(
            &mode_data.cch_topo,
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            &entry.weights.time_weights,
        )
    } else if let Some(ref ew) = exclude_weights {
        CchQuery::with_custom_weights(
            &mode_data.cch_topo,
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            &ew.time_weights,
        )
    } else {
        CchQuery::new(&state, mode)
    };
    // #197: multi-candidate fallback. Try the best (src, dst)
    // combination first; if it fails, retry with the next candidates
    // in (src_idx, dst_idx) order biased toward the closer-to-input
    // ones (since `snap_k_with_info_filtered_role` returns sorted by
    // distance). This catches the residual two-way-road case where
    // the role filter alone can't disambiguate which direction's
    // EBG node is the right starting state for THIS particular
    // src→dst pair.
    //
    // Ordering: enumerate by (i+j) ascending, then by i ascending
    // (prefer closer src over closer dst when sums tie). This
    // produces (0,0), (0,1), (1,0), (0,2), (1,1), (2,0), … so the
    // first additional query swaps to the second-best dst, the
    // second swaps to the second-best src, etc.
    let mut combo_order: Vec<(usize, usize)> = Vec::new();
    for sum in 0..(src_rank_candidates.len() + dst_rank_candidates.len()) {
        for i in 0..src_rank_candidates.len() {
            let j = sum.checked_sub(i);
            if let Some(j) = j
                && j < dst_rank_candidates.len()
            {
                combo_order.push((i, j));
            }
        }
    }
    // Hard cap on total fallback combinations attempted per query
    // to bound tail latency for genuinely-unreachable pairs. K=64
    // produces up to 4096 combos worst case. Cap at 400 so we
    // cover roughly (i+j) ≤ 28 in the enumeration above — enough
    // to reach ~28 candidates deep on either side, which exhausts
    // deep same-geometry directional ambiguity and dynamic
    // exclude/avoid cases that still benefit from wider search.
    // Worst case ≈ 400 × 5-50 ms = 2-20 s tail, only on the
    // ~1.3 % pathological pairs that genuinely have no nearby
    // dynamic or geometrically ambiguous cases.
    const MAX_FALLBACK_COMBOS: usize = 400;
    if combo_order.len() > MAX_FALLBACK_COMBOS {
        combo_order.truncate(MAX_FALLBACK_COMBOS);
    }
    let mut result_opt: Option<super::query::QueryResult> = None;
    for &(i, j) in &combo_order {
        let s = src_rank_candidates[i];
        let d = dst_rank_candidates[j];
        if s == d {
            // Same-rank pair already short-circuited above when the
            // primary candidates collide. Subsequent collisions in
            // fallback are degenerate; skip.
            continue;
        }
        if let Some(r) = query.query(s, d) {
            src_rank = s;
            dst_rank = d;
            chosen_src_idx = i;
            chosen_dst_idx = j;
            result_opt = Some(r);
            break;
        }
    }
    // If the fallback selected a non-primary candidate, update the
    // snap-info reported in the response so callers see WHICH
    // physical snap was used.
    if chosen_src_idx != 0 {
        let (_, info) = make_snap_info(&src_candidates[chosen_src_idx]);
        src_snap_info = info;
    }
    if chosen_dst_idx != 0 {
        let (_, info) = make_snap_info(&dst_candidates[chosen_dst_idx]);
        dst_snap_info = info;
    }
    let _ = src_orig;
    let result = match result_opt {
        Some(r) => r,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: "No route found".to_string(),
                }),
            )
                .into_response();
        }
    };

    let active_weights = if let Some(ref entry) = avoid_entry {
        &entry.weights.time_weights
    } else if let Some(ref ew) = exclude_weights {
        &ew.time_weights
    } else {
        &mode_data.cch_weights
    };

    let (geometry, duration_s, distance_m, steps, ebg_path) = build_route(
        &result,
        active_weights,
        geom_format,
        req.steps,
        src_rank,
        dst_rank,
    );

    // GPX output: skip annotations, alternatives, debug — just emit track points
    if wants_gpx(&headers) {
        let (raw_points, _) = build_raw_points(&ebg_path, &state.ebg_nodes, &state.edge_geom);
        super::region_metrics::record_query(
            &region_id,
            "route",
            started_dispatch.elapsed().as_secs_f64(),
        );
        return gpx_response(format_gpx(&raw_points, "Route"));
    }

    // Build per-edge annotations if requested
    let route_annotations =
        if let Some((want_dur, want_dist, want_spd, want_nds)) = annotation_flags {
            let mut ann = RouteAnnotations {
                duration: None,
                distance: None,
                speed: None,
                nodes: None,
            };
            if want_dur || want_spd {
                let durations: Vec<f64> = ebg_path
                    .iter()
                    .map(|&eid| {
                        let w = mode_data
                            .node_weights
                            .get(eid as usize)
                            .copied()
                            .unwrap_or(0);
                        w as f64
                    })
                    .collect();
                if want_dur {
                    ann.duration = Some(durations.clone());
                }
                if want_spd {
                    let distances: Vec<f64> = ebg_path
                        .iter()
                        .map(|&eid| state.ebg_nodes.nodes[eid as usize].length_m as f64)
                        .collect();
                    ann.speed = Some(
                        durations
                            .iter()
                            .zip(distances.iter())
                            .map(|(&dur, &dist)| {
                                if dur > 0.0 {
                                    dist * 3.6 / dur // km/h = (m/s) * 3.6
                                } else {
                                    0.0
                                }
                            })
                            .collect(),
                    );
                }
            }
            if want_dist {
                ann.distance = Some(
                    ebg_path
                        .iter()
                        .map(|&eid| state.ebg_nodes.nodes[eid as usize].length_m as f64)
                        .collect(),
                );
            }
            if want_nds {
                ann.nodes = Some(ebg_path.clone());
            }
            Some(ann)
        } else {
            None
        };

    // Compute alternative routes if requested
    let alternatives = if num_alternatives > 0 {
        let mut alt_routes = Vec::new();
        // Clone weights to apply route penalties for alternative computation.
        // This clones ~200MB (up + down weight arrays). Acceptable for alternatives
        // since they're requested rarely (only when alternatives > 0).
        // A proper fix (penalty views) would require changing the CchQuery API.
        let mut penalized_weights = if let Some(ref entry) = avoid_entry {
            entry.weights.time_weights.clone()
        } else if let Some(ref ew) = exclude_weights {
            ew.time_weights.clone()
        } else {
            mode_data.cch_weights.clone()
        };

        // Penalize edges of the primary route
        for &(_node, edge_idx) in &result.forward_parent {
            let idx = edge_idx as usize;
            if idx < penalized_weights.up.len() {
                let new_val = penalized_weights.up[idx].saturating_mul(3);
                penalized_weights.up.to_mut()[idx] = new_val;
            }
        }
        for &(_node, edge_idx) in &result.backward_parent {
            let idx = edge_idx as usize;
            if idx < penalized_weights.down.len() {
                let new_val = penalized_weights.down[idx].saturating_mul(3);
                penalized_weights.down.to_mut()[idx] = new_val;
            }
        }

        for _alt_idx in 0..num_alternatives {
            let alt_query = CchQuery::with_custom_weights(
                &mode_data.cch_topo,
                &mode_data.up_adj_flat,
                &mode_data.down_rev_flat,
                &penalized_weights,
            );

            if let Some(alt_result) = alt_query.query(src_rank, dst_rank) {
                // Skip if same distance as primary (exact duplicate)
                if alt_result.distance == result.distance {
                    continue;
                }
                // Skip if distance is more than 2x primary (too indirect)
                if alt_result.distance > result.distance.saturating_mul(2) {
                    break;
                }

                let (alt_geom, alt_dur, alt_dist, alt_steps, _alt_path) = build_route(
                    &alt_result,
                    &penalized_weights,
                    geom_format,
                    req.steps,
                    src_rank,
                    dst_rank,
                );

                // Penalize this alternative's edges for next iteration
                for &(_node, edge_idx) in &alt_result.forward_parent {
                    let idx = edge_idx as usize;
                    if idx < penalized_weights.up.len() {
                        let new_val = penalized_weights.up[idx].saturating_mul(3);
                        penalized_weights.up.to_mut()[idx] = new_val;
                    }
                }
                for &(_node, edge_idx) in &alt_result.backward_parent {
                    let idx = edge_idx as usize;
                    if idx < penalized_weights.down.len() {
                        let new_val = penalized_weights.down[idx].saturating_mul(3);
                        penalized_weights.down.to_mut()[idx] = new_val;
                    }
                }

                alt_routes.push(RouteAlternative {
                    duration_s: alt_dur,
                    distance_m: alt_dist,
                    geometry: alt_geom,
                    steps: alt_steps,
                });
            } else {
                break; // No more routes possible
            }
        }

        if alt_routes.is_empty() {
            None
        } else {
            Some(alt_routes)
        }
    } else {
        None
    };

    // Build debug info if requested
    let debug_info = if req.debug {
        Some(RouteDebugInfo {
            src_snapped: src_snap_info,
            dst_snapped: dst_snap_info,
        })
    } else {
        None
    };

    super::region_metrics::record_query(
        &region_id,
        "route",
        started_dispatch.elapsed().as_secs_f64(),
    );
    Json(RouteResponse {
        duration_s,
        distance_m,
        geometry,
        steps,
        annotations: route_annotations,
        alternatives,
        debug: debug_info,
    })
    .into_response()
}

// ============ Cross-region handler (#91 Phase 2) ============

/// Backwards-compatible alias for [`route_handler`]. Cross-region
/// dispatch is now handled inline by `route_handler`; callers that
/// previously imported this name continue to work.
pub async fn cross_region_route_handler(
    state: State<Arc<RegionsState>>,
    query: Query<RouteRequest>,
    headers: HeaderMap,
) -> impl IntoResponse {
    route_handler(state, query, headers).await.into_response()
}

fn cross_region_route_inner(
    src_state: Arc<ServerState>,
    src_region: String,
    dst_state: Arc<ServerState>,
    dst_region: String,
    overlay: Arc<super::overlay::OverlayCluster>,
    req: RouteRequest,
) -> axum::response::Response {
    use super::cross_region::solve_cross_region;

    let effective_mode_name = match &req.traffic {
        Some(v) if !v.trim().is_empty() => format!("{}_{}", req.mode, v.trim()),
        _ => req.mode.clone(),
    };
    let src_mode = match parse_mode(&effective_mode_name, &src_state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };
    let dst_mode = match parse_mode(&effective_mode_name, &dst_state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let src_mode_data = src_state.get_mode(src_mode);
    let dst_mode_data = dst_state.get_mode(dst_mode);

    // #197: role-aware snap (cross-region path).
    let src_role_filter = SnapRole::Src.role_filter(src_mode_data);
    let dst_role_filter = SnapRole::Dst.role_filter(dst_mode_data);

    let (src_orig, src_snap) = match src_state.snap_index.snap_with_info_filtered_role(
        req.src_lon,
        req.src_lat,
        src_mode.0,
        None,
        src_role_filter,
    ) {
        Some(t) => (t.0, t),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Could not snap source in region {}", src_region),
                }),
            )
                .into_response();
        }
    };
    let (dst_orig, dst_snap) = match dst_state.snap_index.snap_with_info_filtered_role(
        req.dst_lon,
        req.dst_lat,
        dst_mode.0,
        None,
        dst_role_filter,
    ) {
        Some(t) => (t.0, t),
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Could not snap destination in region {}", dst_region),
                }),
            )
                .into_response();
        }
    };

    let src_rank = src_mode_data.orig_to_rank[src_orig as usize];
    let dst_rank = dst_mode_data.orig_to_rank[dst_orig as usize];
    if src_rank == u32::MAX || dst_rank == u32::MAX {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Snapped node not accessible for this mode".to_string(),
            }),
        )
            .into_response();
    }

    let solution = match solve_cross_region(
        &src_state,
        &src_region,
        src_rank,
        &dst_state,
        &dst_region,
        dst_rank,
        &req.mode,
        &overlay,
    ) {
        Some(s) => s,
        None => {
            return (
                StatusCode::NOT_FOUND,
                Json(ErrorResponse {
                    error: format!(
                        "No cross-region route found from {} to {}",
                        src_region, dst_region
                    ),
                }),
            )
                .into_response();
        }
    };

    // Cross-region solver costs are now in seconds (post-#297).
    let duration_s = solution.total_cost as f64;

    let geom_format = match GeometryFormat::parse(&req.geometries) {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Translate the chosen border EBG nodes back to per-region CCH ranks
    // so we can run a path-recovery query within each region.
    let src_border_rank = *src_mode_data
        .orig_to_rank
        .get(solution.src_border_ebg as usize)
        .unwrap_or(&u32::MAX);
    let dst_border_rank = *dst_mode_data
        .orig_to_rank
        .get(solution.dst_border_ebg as usize)
        .unwrap_or(&u32::MAX);

    if src_border_rank == u32::MAX || dst_border_rank == u32::MAX {
        // Border picked by the picker doesn't translate into either
        // region's mode-filtered CCH. Treat as no-route rather than
        // returning a degenerate straight-line polyline.
        return (
            StatusCode::NOT_FOUND,
            Json(ErrorResponse {
                error: format!(
                    "Cross-region border {}↔{} not accessible for mode '{}'",
                    src_region, dst_region, req.mode
                ),
            }),
        )
            .into_response();
    }

    // Look up the chosen border representative lat/lon for the
    // border-crossing segment that connects the two regions.
    let src_border = overlay
        .region_representatives(&src_region)
        .get(solution.src_border_idx as usize)
        .copied();
    let dst_border = overlay
        .region_representatives(&dst_region)
        .get(solution.dst_border_idx as usize)
        .copied();

    // Recover the per-leg EBG paths and stitch geometry. If the access
    // or egress leg is zero-length (src == its border, or dst's border
    // == dst), the corresponding leg yields an empty path and we fall
    // back to the snap point. Collapse-into-border legs are valid; we
    // keep going.
    let (src_points, src_dist_m) =
        leg_points_and_distance(&src_state, src_mode, src_rank, src_border_rank);
    let (dst_points, dst_dist_m) =
        leg_points_and_distance(&dst_state, dst_mode, dst_border_rank, dst_rank);

    let src_border_pt = src_border.map(|b| Point {
        lon: b.lon,
        lat: b.lat,
    });
    let dst_border_pt = dst_border.map(|b| Point {
        lon: b.lon,
        lat: b.lat,
    });
    let src_snap_pt = Point {
        lon: src_snap.1,
        lat: src_snap.2,
    };
    let dst_snap_pt = Point {
        lon: dst_snap.1,
        lat: dst_snap.2,
    };

    let all_points = stitch_cross_region_polyline(
        &src_points,
        src_snap_pt,
        src_border_pt,
        dst_border_pt,
        &dst_points,
        dst_snap_pt,
    );

    let border_crossing_m = match (src_border, dst_border) {
        (Some(sb), Some(db)) => crate::nbg::haversine_distance(sb.lat, sb.lon, db.lat, db.lon),
        _ => 0.0,
    };

    let geom = RouteGeometry::from_points(all_points, geom_format);

    let distance_m = src_dist_m + border_crossing_m + dst_dist_m;

    Json(RouteResponse {
        duration_s,
        distance_m,
        geometry: geom,
        steps: None,
        annotations: None,
        alternatives: None,
        debug: None,
    })
    .into_response()
}

/// Run a CCH P2P query inside a single region with path recovery,
/// unpack the shortcut path to original EBG edges, and walk the
/// geometry to produce a deduped point list and a precise length-mm
/// based distance in metres.
///
/// Returns `(empty Vec, 0.0)` when:
/// - `src == dst` (legs degenerate when the source already equals its
///   border representative)
/// - the bidirectional search finds no path
pub fn leg_points_and_distance(
    state: &ServerState,
    mode: crate::profile_abi::Mode,
    src_rank: u32,
    dst_rank: u32,
) -> (Vec<Point>, f64) {
    if src_rank == dst_rank {
        return (Vec::new(), 0.0);
    }
    let mode_data = state.get_mode(mode);
    let query = CchQuery::new(state, mode);
    let result = match query.query(src_rank, dst_rank) {
        Some(r) => r,
        None => return (Vec::new(), 0.0),
    };

    let rank_path = unpack_path(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &result.forward_parent,
        &result.backward_parent,
        src_rank,
        dst_rank,
        result.meeting_node,
    );
    let ebg_path: Vec<u32> = rank_path
        .iter()
        .map(|&rank| {
            let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            mode_data.filtered_to_original[filtered_id as usize]
        })
        .collect();

    build_raw_points(&ebg_path, &state.ebg_nodes, &state.edge_geom)
}

/// Pure stitching of a cross-region polyline. Concatenates the access
/// leg, the chosen source-side border representative, the
/// destination-side border representative, and the egress leg into a
/// single deduplicated point sequence.
///
/// Empty legs (degenerate `src == border` cases) fall back to the
/// snap point of the corresponding side. Adjacent duplicate vertices
/// — produced by edge-boundary overlap or by a leg whose terminal
/// vertex coincides with the border representative — are collapsed.
///
/// This is the pure geometry kernel of cross-region routing; the
/// CCH-driven part lives in [`leg_points_and_distance`]. Exposed as
/// `pub` (module-private to `server`) so the synthetic 2-region
/// integration test can verify polyline assembly without needing two
/// real `ServerState` instances.
pub fn stitch_cross_region_polyline(
    src_leg: &[Point],
    src_snap: Point,
    src_border: Option<Point>,
    dst_border: Option<Point>,
    dst_leg: &[Point],
    dst_snap: Point,
) -> Vec<Point> {
    let mut all_points: Vec<Point> = Vec::with_capacity(src_leg.len() + dst_leg.len() + 2);

    if src_leg.is_empty() {
        all_points.push(src_snap);
    } else {
        all_points.extend_from_slice(src_leg);
    }

    fn push_unique(out: &mut Vec<Point>, p: Point) {
        let dup = match out.last().copied() {
            Some(prev) => (prev.lon - p.lon).abs() < 1e-9 && (prev.lat - p.lat).abs() < 1e-9,
            None => false,
        };
        if !dup {
            out.push(p);
        }
    }

    if let Some(b) = src_border {
        push_unique(&mut all_points, b);
    }
    if let Some(b) = dst_border {
        push_unique(&mut all_points, b);
    }

    if dst_leg.is_empty() {
        push_unique(&mut all_points, dst_snap);
    } else {
        // Skip the first egress point if it duplicates the LU-side
        // border we already emitted.
        let mut iter = dst_leg.iter().copied();
        if let Some(first) = iter.next() {
            push_unique(&mut all_points, first);
            for p in iter {
                all_points.push(p);
            }
        }
    }

    // Collapse any back-to-back duplicates produced by edge-boundary
    // overlap (each EBG edge polyline starts at the previous edge's
    // end vertex).
    all_points.dedup_by(|a, b| (a.lon - b.lon).abs() < 1e-9 && (a.lat - b.lat).abs() < 1e-9);
    all_points
}

// ============ GPX formatting ============

/// Check whether the Accept header requests GPX output.
fn wants_gpx(headers: &HeaderMap) -> bool {
    headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/gpx+xml"))
        .unwrap_or(false)
}

/// Format route points as a GPX 1.1 XML document.
///
/// GPX uses `lat` then `lon` attributes (opposite of GeoJSON).
fn format_gpx(points: &[Point], name: &str) -> String {
    use std::fmt::Write;

    let now = chrono::Utc::now().format("%Y-%m-%dT%H:%M:%SZ");

    let mut xml = String::with_capacity(128 + points.len() * 64);
    let _ = writeln!(
        xml,
        "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n\
         <gpx version=\"1.1\" creator=\"Butterfly Route\" \
         xmlns=\"http://www.topografix.com/GPX/1/1\">\n  \
         <metadata>\n    \
         <name>{name}</name>\n    \
         <time>{now}</time>\n  \
         </metadata>\n  \
         <trk>\n    \
         <name>{name}</name>\n    \
         <trkseg>",
    );

    for pt in points {
        // 7 decimal places ~ 1cm precision
        let _ = writeln!(
            xml,
            "      <trkpt lat=\"{:.7}\" lon=\"{:.7}\"/>",
            pt.lat, pt.lon,
        );
    }

    xml.push_str("    </trkseg>\n  </trk>\n</gpx>\n");
    xml
}

/// Build an axum response with GPX content type.
fn gpx_response(body: String) -> axum::response::Response {
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            "application/gpx+xml; charset=utf-8",
        )],
        body,
    )
        .into_response()
}

// ============ Helper functions ============

/// Look up the road name for an EBG edge via geom_idx → NbgEdge.first_osm_way_id.
///
/// #282: takes the abstract `WayNames` (either mmap-backed
/// `WayNamesIdx` or legacy `HashMap`) and returns an owned `String` so
/// callers don't have to thread a lifetime through the step-building
/// pipeline. The lookup itself is O(log n) (binary search on the
/// mmap-backed array) or O(1) (HashMap) — the allocation only happens
/// when there's actually a name to return.
pub fn lookup_road_name(
    edge_id: u32,
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
    way_names: &crate::server::state::WayNames,
) -> Option<String> {
    let node = &ebg_nodes.nodes[edge_id as usize];
    let geom_idx = node.geom_idx as usize;
    if geom_idx < nbg_geo.edges.len() {
        let way_id = nbg_geo.edges[geom_idx].first_osm_way_id;
        way_names.get(way_id).map(|s| s.to_string())
    } else {
        None
    }
}

/// Build turn-by-turn step instructions from EBG path
pub(crate) fn build_steps(
    ebg_path: &[u32],
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
    node_weights: &[u32],
    way_names: &crate::server::state::WayNames,
    format: GeometryFormat,
) -> Vec<RouteStep> {
    if ebg_path.len() < 2 {
        return vec![];
    }

    let mut steps = Vec::new();

    // Get start location for depart maneuver
    let first_node = &ebg_nodes.nodes[ebg_path[0] as usize];
    let start_loc = get_edge_start_location(first_node, edge_geom);
    let start_bearing = get_edge_bearing(first_node, edge_geom, true);

    // Depart step (first edge)
    let first_distance = first_node.length_m as f64;
    let first_duration =
        if (ebg_path[0] as usize) < node_weights.len() && node_weights[ebg_path[0] as usize] > 0 {
            node_weights[ebg_path[0] as usize] as f64
        } else {
            0.0
        };
    let first_geom = build_edge_geometry(ebg_path[0], ebg_nodes, edge_geom, format);

    steps.push(RouteStep {
        distance_m: first_distance,
        duration_s: first_duration,
        geometry: first_geom,
        maneuver: StepManeuver {
            location: start_loc,
            bearing_before: 0,
            bearing_after: start_bearing,
            maneuver_type: "depart".to_string(),
            modifier: None,
            name: lookup_road_name(ebg_path[0], ebg_nodes, nbg_geo, way_names),
        },
    });

    // Intermediate steps — group consecutive edges with same bearing direction
    // For now, create one step per significant turn
    let mut accumulated_distance = 0.0;
    let mut accumulated_duration = 0.0;
    let mut segment_edges: Vec<u32> = Vec::new();
    let mut prev_end_bearing = get_edge_bearing(first_node, edge_geom, false);

    for i in 1..ebg_path.len() {
        let edge_id = ebg_path[i];
        let node = &ebg_nodes.nodes[edge_id as usize];
        let edge_distance = node.length_m as f64;
        let edge_duration =
            if (edge_id as usize) < node_weights.len() && node_weights[edge_id as usize] > 0 {
                node_weights[edge_id as usize] as f64
            } else {
                0.0
            };

        let cur_start_bearing = get_edge_bearing(node, edge_geom, true);
        let turn_angle = bearing_diff(prev_end_bearing, cur_start_bearing);
        let turn_type = classify_turn(turn_angle);

        // If significant turn or last edge, emit a step
        if turn_type != "straight" || i == ebg_path.len() - 1 {
            if !segment_edges.is_empty() {
                // Emit accumulated straight segment
                let seg_geom =
                    build_multi_edge_geometry(&segment_edges, ebg_nodes, edge_geom, format);
                let seg_start =
                    get_edge_start_location(&ebg_nodes.nodes[segment_edges[0] as usize], edge_geom);
                let seg_start_bearing =
                    get_edge_bearing(&ebg_nodes.nodes[segment_edges[0] as usize], edge_geom, true);

                steps.push(RouteStep {
                    distance_m: accumulated_distance,
                    duration_s: accumulated_duration,
                    geometry: seg_geom,
                    maneuver: StepManeuver {
                        location: seg_start,
                        bearing_before: prev_end_bearing,
                        bearing_after: seg_start_bearing,
                        maneuver_type: "continue".to_string(),
                        modifier: Some("straight".to_string()),
                        name: lookup_road_name(segment_edges[0], ebg_nodes, nbg_geo, way_names),
                    },
                });
                accumulated_distance = 0.0;
                accumulated_duration = 0.0;
                segment_edges.clear();
            }

            if i == ebg_path.len() - 1 {
                // Arrive step
                let arrive_loc = get_edge_end_location(node, edge_geom);
                let arrive_geom = build_edge_geometry(edge_id, ebg_nodes, edge_geom, format);
                steps.push(RouteStep {
                    distance_m: edge_distance,
                    duration_s: edge_duration,
                    geometry: arrive_geom,
                    maneuver: StepManeuver {
                        location: arrive_loc,
                        bearing_before: get_edge_bearing(node, edge_geom, false),
                        bearing_after: 0,
                        maneuver_type: "arrive".to_string(),
                        modifier: None,
                        name: lookup_road_name(edge_id, ebg_nodes, nbg_geo, way_names),
                    },
                });
            } else {
                // Turn step
                let turn_loc = get_edge_start_location(node, edge_geom);
                let is_roundabout = (node.class_bits & 0x08) != 0; // bit3 = roundabout
                let m_type = if is_roundabout { "roundabout" } else { "turn" };

                let turn_geom = build_edge_geometry(edge_id, ebg_nodes, edge_geom, format);
                steps.push(RouteStep {
                    distance_m: edge_distance,
                    duration_s: edge_duration,
                    geometry: turn_geom,
                    maneuver: StepManeuver {
                        location: turn_loc,
                        bearing_before: prev_end_bearing,
                        bearing_after: cur_start_bearing,
                        maneuver_type: m_type.to_string(),
                        modifier: Some(turn_type.to_string()),
                        name: lookup_road_name(edge_id, ebg_nodes, nbg_geo, way_names),
                    },
                });
            }
        } else {
            // Accumulate straight segment
            segment_edges.push(edge_id);
            accumulated_distance += edge_distance;
            accumulated_duration += edge_duration;
        }

        prev_end_bearing = get_edge_bearing(node, edge_geom, false);
    }

    steps
}

/// Get start location of an EBG edge
fn get_edge_start_location(
    node: &crate::formats::ebg_nodes::EbgNode,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
) -> [f64; 2] {
    let poly = edge_geom.polyline(node.geom_idx);
    if !poly.is_empty() {
        let (lon, lat) = poly.at(0);
        return [lon, lat];
    }
    [0.0, 0.0]
}

/// Get end location of an EBG edge
fn get_edge_end_location(
    node: &crate::formats::ebg_nodes::EbgNode,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
) -> [f64; 2] {
    let poly = edge_geom.polyline(node.geom_idx);
    if !poly.is_empty() {
        let last = poly.len() - 1;
        let (lon, lat) = poly.at(last);
        return [lon, lat];
    }
    [0.0, 0.0]
}

/// Get bearing of an EBG edge (at start or end)
fn get_edge_bearing(
    node: &crate::formats::ebg_nodes::EbgNode,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
    at_start: bool,
) -> u16 {
    let poly = edge_geom.polyline(node.geom_idx);
    if poly.len() >= 2 {
        let (i0, i1) = if at_start {
            (0, 1)
        } else {
            (poly.len() - 2, poly.len() - 1)
        };
        let (lon1, lat1) = poly.at(i0);
        let (lon2, lat2) = poly.at(i1);
        return compute_bearing(lat1, lon1, lat2, lon2);
    }
    0
}

/// Compute bearing between two points (degrees 0-359)
pub fn compute_bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> u16 {
    let lat1_r = lat1.to_radians();
    let lat2_r = lat2.to_radians();
    let dlon = (lon2 - lon1).to_radians();

    let y = dlon.sin() * lat2_r.cos();
    let x = lat1_r.cos() * lat2_r.sin() - lat1_r.sin() * lat2_r.cos() * dlon.cos();
    let bearing = y.atan2(x).to_degrees();
    ((bearing + 360.0) % 360.0) as u16
}

/// Compute signed bearing difference (how much to turn from b1 to b2)
/// Returns 0-360: 0=straight, 90=right, 180=uturn, 270=left
pub fn bearing_diff(b1: u16, b2: u16) -> u16 {
    ((b2 as i32 - b1 as i32 + 360) % 360) as u16
}

/// Classify turn by angle
pub fn classify_turn(angle: u16) -> &'static str {
    match angle {
        0..=15 | 345..=360 => "straight",
        16..=60 => "slight right",
        61..=120 => "right",
        121..=160 => "sharp right",
        161..=200 => "uturn",
        201..=240 => "sharp left",
        241..=300 => "left",
        301..=344 => "slight left",
        _ => "straight",
    }
}

/// Build geometry for a single edge
fn build_edge_geometry(
    edge_id: u32,
    ebg_nodes: &crate::formats::EbgNodes,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
    format: GeometryFormat,
) -> RouteGeometry {
    let node = &ebg_nodes.nodes[edge_id as usize];
    let poly = edge_geom.polyline(node.geom_idx);
    let points: Vec<Point> = poly.iter().map(|(lon, lat)| Point { lon, lat }).collect();
    RouteGeometry::from_points(points, format)
}

/// Build geometry for multiple consecutive edges
fn build_multi_edge_geometry(
    edge_ids: &[u32],
    ebg_nodes: &crate::formats::EbgNodes,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
    format: GeometryFormat,
) -> RouteGeometry {
    let mut points = Vec::new();

    for &edge_id in edge_ids {
        let node = &ebg_nodes.nodes[edge_id as usize];
        let poly = edge_geom.polyline(node.geom_idx);
        let start = if points.is_empty() { 0 } else { 1 }; // skip duplicate at join
        for j in start..poly.len() {
            let (lon, lat) = poly.at(j);
            points.push(Point { lon, lat });
        }
    }

    RouteGeometry::from_points(points, format)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wants_gpx_true() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/gpx+xml".parse().unwrap());
        assert!(wants_gpx(&headers));
    }

    #[test]
    fn test_wants_gpx_with_quality() {
        let mut headers = HeaderMap::new();
        headers.insert(
            "accept",
            "application/gpx+xml;q=0.9, application/json"
                .parse()
                .unwrap(),
        );
        assert!(wants_gpx(&headers));
    }

    #[test]
    fn test_wants_gpx_false_for_json() {
        let mut headers = HeaderMap::new();
        headers.insert("accept", "application/json".parse().unwrap());
        assert!(!wants_gpx(&headers));
    }

    #[test]
    fn test_wants_gpx_false_for_missing_header() {
        let headers = HeaderMap::new();
        assert!(!wants_gpx(&headers));
    }

    #[test]
    fn test_format_gpx_structure() {
        let points = vec![
            Point {
                lon: 4.3517,
                lat: 50.8503,
            },
            Point {
                lon: 4.3525,
                lat: 50.8510,
            },
        ];
        let gpx = format_gpx(&points, "Test Route");

        // Verify XML declaration
        assert!(gpx.starts_with("<?xml version=\"1.0\" encoding=\"UTF-8\"?>"));

        // Verify GPX root element
        assert!(gpx.contains("<gpx version=\"1.1\" creator=\"Butterfly Route\""));
        assert!(gpx.contains("xmlns=\"http://www.topografix.com/GPX/1/1\""));

        // Verify metadata
        assert!(gpx.contains("<name>Test Route</name>"));
        assert!(gpx.contains("<time>"));

        // Verify track structure
        assert!(gpx.contains("<trk>"));
        assert!(gpx.contains("<trkseg>"));
        assert!(gpx.contains("</trkseg>"));
        assert!(gpx.contains("</trk>"));
        assert!(gpx.contains("</gpx>"));

        // Verify points: GPX uses lat then lon (opposite of GeoJSON)
        assert!(gpx.contains("lat=\"50.8503000\" lon=\"4.3517000\""));
        assert!(gpx.contains("lat=\"50.8510000\" lon=\"4.3525000\""));
    }

    #[test]
    fn test_format_gpx_empty_points() {
        let gpx = format_gpx(&[], "Empty");
        assert!(gpx.contains("<trkseg>"));
        assert!(gpx.contains("</trkseg>"));
        // No trkpt elements
        assert!(!gpx.contains("<trkpt"));
    }

    #[test]
    fn test_format_gpx_single_point() {
        let points = vec![Point {
            lon: 4.3517,
            lat: 50.8503,
        }];
        let gpx = format_gpx(&points, "Single");
        let trkpt_count = gpx.matches("<trkpt").count();
        assert_eq!(trkpt_count, 1);
        assert!(gpx.contains("lat=\"50.8503000\" lon=\"4.3517000\""));
    }

    #[test]
    fn test_format_gpx_self_closing_trkpt() {
        // trkpt elements without elevation should be self-closing
        let points = vec![Point {
            lon: 4.3517,
            lat: 50.8503,
        }];
        let gpx = format_gpx(&points, "Route");
        assert!(gpx.contains("/>"));
        assert!(!gpx.contains("</trkpt>"));
    }
}
