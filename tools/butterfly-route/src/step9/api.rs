//! HTTP API handlers with Axum and Utoipa

use axum::{
    body::Body,
    extract::{DefaultBodyLimit, Query, State},
    http::{header, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;
use utoipa::{OpenApi, ToSchema};
use utoipa_swagger_ui::SwaggerUi;

use crate::matrix::arrow_stream::{
    record_batch_to_bytes, tiles_to_record_batch, MatrixTile, ARROW_STREAM_CONTENT_TYPE,
};
use crate::matrix::bucket_ch::{
    backward_join_with_buckets, forward_build_buckets, table_bucket_full_flat,
    table_bucket_parallel,
};
use crate::profile_abi::Mode;

use super::geometry::{
    build_geometry, build_isochrone_geometry_concave, encode_polyline6, GeometryFormat, Point,
    RouteGeometry,
};
use super::query::CchQuery;
use super::state::ServerState;
use super::unpack::unpack_path;

/// OpenAPI documentation
#[derive(OpenApi)]
#[openapi(
    paths(route, table_post, isochrone, nearest, health),
    components(schemas(
        RouteRequest,
        RouteResponse,
        RouteAlternative,
        SnapInfo,
        RouteDebugInfo,
        RouteStep,
        StepManeuver,
        TablePostRequest,
        TableResponse,
        IsochroneRequest,
        IsochroneResponse,
        NearestRequest,
        NearestResponse,
        NearestWaypoint,
        Point,
        ErrorResponse,
        Waypoint
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

    // Prometheus metrics
    let (prometheus_layer, metric_handle) = axum_prometheus::PrometheusMetricLayer::pair();

    // API routes: normal endpoints with 120s timeout + response compression
    let api_routes = Router::new()
        .route("/route", get(route))
        .route("/nearest", get(nearest))
        .route("/table", post(table_post))
        .route("/isochrone", get(isochrone))
        .route("/trip", post(super::trip::trip_handler))
        .route("/height", get(height))
        .route("/health", get(health))
        .route("/debug/compare", get(debug_compare))
        .layer(CompressionLayer::new())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            Duration::from_secs(120),
        ));

    // Streaming routes: longer timeout, larger body limit, no compression
    let stream_routes = Router::new()
        .route("/table/stream", post(table_stream))
        .route("/isochrone/bulk", post(isochrone_bulk))
        .layer(DefaultBodyLimit::max(256 * 1024 * 1024)) // 256MB
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
    /// Geometry encoding: polyline6 (default), geojson, points
    #[serde(default = "default_geometries")]
    geometries: String,
    /// Number of alternative routes (0 or 1 = single route, max 5)
    #[serde(default = "default_alternatives")]
    alternatives: u32,
    /// Include turn-by-turn step instructions
    #[serde(default)]
    steps: bool,
    /// Include debug information in response
    #[serde(default)]
    debug: bool,
}
fn default_alternatives() -> u32 {
    0
}

fn default_geometries() -> String {
    "polyline6".to_string()
}
fn default_direction() -> String {
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

#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

/// Calculate route between two points
///
/// Supports alternative routes via `alternatives` parameter
/// and turn-by-turn instructions via `steps=true`.
#[utoipa::path(
    get,
    path = "/route",
    params(
        ("src_lon" = f64, Query, description = "Source longitude"),
        ("src_lat" = f64, Query, description = "Source latitude"),
        ("dst_lon" = f64, Query, description = "Destination longitude"),
        ("dst_lat" = f64, Query, description = "Destination latitude"),
        ("mode" = String, Query, description = "Transport mode: car, bike, foot"),
        ("geometries" = Option<String>, Query, description = "Geometry encoding: polyline6 (default), geojson, points"),
        ("alternatives" = Option<u32>, Query, description = "Number of alternative routes (0-5)"),
        ("steps" = Option<bool>, Query, description = "Include turn-by-turn instructions"),
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
    if let Err(e) = validate_coord(req.src_lon, req.src_lat, "source") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
    }
    if let Err(e) = validate_coord(req.dst_lon, req.dst_lat, "destination") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
    }

    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let geom_format = match GeometryFormat::parse(&req.geometries) {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let mode_data = state.get_mode(mode);
    let num_alternatives = (req.alternatives.min(5)) as usize;

    // Snap source and destination with debug info
    let (src_orig, src_snap_info) =
        match state
            .spatial_index
            .snap_with_info(req.src_lon, req.src_lat, &mode_data.mask, 10)
        {
            Some((id, lon, lat, dist)) => (
                id,
                SnapInfo {
                    lon,
                    lat,
                    snap_distance_m: dist,
                    ebg_node_id: id,
                },
            ),
            None => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "Could not snap source to road network".to_string(),
                    }),
                )
                    .into_response()
            }
        };

    let (_dst_orig, dst_snap_info) =
        match state
            .spatial_index
            .snap_with_info(req.dst_lon, req.dst_lat, &mode_data.mask, 10)
        {
            Some((id, lon, lat, dist)) => (
                id,
                SnapInfo {
                    lon,
                    lat,
                    snap_distance_m: dist,
                    ebg_node_id: id,
                },
            ),
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
    let dst_filtered = mode_data.filtered_ebg.original_to_filtered[_dst_orig as usize];

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

    // Helper: build route from query result
    let build_route = |result: &super::query::QueryResult,
                       format: GeometryFormat,
                       want_steps: bool|
     -> (RouteGeometry, f64, f64, Option<Vec<RouteStep>>) {
        let rank_path = unpack_path(
            &mode_data.cch_topo,
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
                mode_data.filtered_ebg.filtered_to_original[filtered_id as usize]
            })
            .collect();
        let geometry = build_geometry(
            &ebg_path,
            &state.ebg_nodes,
            &state.nbg_geo,
            result.distance,
            format,
        );
        let duration_s = result.distance as f64 / 10.0;
        let distance_m = geometry.distance_m;
        let steps = if want_steps {
            Some(build_steps(
                &ebg_path,
                &state.ebg_nodes,
                &state.nbg_geo,
                &mode_data.node_weights,
                &state.way_names,
                format,
            ))
        } else {
            None
        };
        (geometry, duration_s, distance_m, steps)
    };

    // Run primary query
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

    let (geometry, duration_s, distance_m, steps) = build_route(&result, geom_format, req.steps);

    // Compute alternative routes if requested
    let alternatives = if num_alternatives > 0 {
        let mut alt_routes = Vec::new();
        let mut penalized_weights = mode_data.cch_weights.clone();

        // Penalize edges of the primary route
        for &(_node, edge_idx) in &result.forward_parent {
            let idx = edge_idx as usize;
            if idx < penalized_weights.up.len() {
                penalized_weights.up[idx] = penalized_weights.up[idx].saturating_mul(3);
            }
        }
        for &(_node, edge_idx) in &result.backward_parent {
            let idx = edge_idx as usize;
            if idx < penalized_weights.down.len() {
                penalized_weights.down[idx] = penalized_weights.down[idx].saturating_mul(3);
            }
        }

        for _alt_idx in 0..num_alternatives {
            let alt_query = CchQuery::with_custom_weights(
                &mode_data.cch_topo,
                &mode_data.down_rev,
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

                let (alt_geom, alt_dur, alt_dist, alt_steps) =
                    build_route(&alt_result, geom_format, req.steps);

                // Penalize this alternative's edges for next iteration
                for &(_node, edge_idx) in &alt_result.forward_parent {
                    let idx = edge_idx as usize;
                    if idx < penalized_weights.up.len() {
                        penalized_weights.up[idx] = penalized_weights.up[idx].saturating_mul(3);
                    }
                }
                for &(_node, edge_idx) in &alt_result.backward_parent {
                    let idx = edge_idx as usize;
                    if idx < penalized_weights.down.len() {
                        penalized_weights.down[idx] = penalized_weights.down[idx].saturating_mul(3);
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

    Json(RouteResponse {
        duration_s,
        distance_m,
        geometry,
        steps,
        alternatives,
        debug: debug_info,
    })
    .into_response()
}

/// Look up the road name for an EBG edge via geom_idx → NbgEdge.first_osm_way_id
fn lookup_road_name(
    edge_id: u32,
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
    way_names: &std::collections::HashMap<i64, String>,
) -> Option<String> {
    let node = &ebg_nodes.nodes[edge_id as usize];
    let geom_idx = node.geom_idx as usize;
    if geom_idx < nbg_geo.edges.len() {
        let way_id = nbg_geo.edges[geom_idx].first_osm_way_id;
        way_names.get(&way_id).cloned()
    } else {
        None
    }
}

/// Build turn-by-turn step instructions from EBG path
pub(crate) fn build_steps(
    ebg_path: &[u32],
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
    node_weights: &[u32],
    way_names: &std::collections::HashMap<i64, String>,
    format: GeometryFormat,
) -> Vec<RouteStep> {
    if ebg_path.len() < 2 {
        return vec![];
    }

    let mut steps = Vec::new();

    // Get start location for depart maneuver
    let first_node = &ebg_nodes.nodes[ebg_path[0] as usize];
    let start_loc = get_edge_start_location(first_node, nbg_geo);
    let start_bearing = get_edge_bearing(first_node, nbg_geo, true);

    // Depart step (first edge)
    let first_distance = first_node.length_mm as f64 / 1000.0;
    let first_duration =
        if (ebg_path[0] as usize) < node_weights.len() && node_weights[ebg_path[0] as usize] > 0 {
            node_weights[ebg_path[0] as usize] as f64 / 10.0
        } else {
            0.0
        };
    let first_geom = build_edge_geometry(ebg_path[0], ebg_nodes, nbg_geo, format);

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
    let mut prev_end_bearing = get_edge_bearing(first_node, nbg_geo, false);

    for i in 1..ebg_path.len() {
        let edge_id = ebg_path[i];
        let node = &ebg_nodes.nodes[edge_id as usize];
        let edge_distance = node.length_mm as f64 / 1000.0;
        let edge_duration =
            if (edge_id as usize) < node_weights.len() && node_weights[edge_id as usize] > 0 {
                node_weights[edge_id as usize] as f64 / 10.0
            } else {
                0.0
            };

        let cur_start_bearing = get_edge_bearing(node, nbg_geo, true);
        let turn_angle = bearing_diff(prev_end_bearing, cur_start_bearing);
        let turn_type = classify_turn(turn_angle);

        // If significant turn or last edge, emit a step
        if turn_type != "straight" || i == ebg_path.len() - 1 {
            if !segment_edges.is_empty() {
                // Emit accumulated straight segment
                let seg_geom =
                    build_multi_edge_geometry(&segment_edges, ebg_nodes, nbg_geo, format);
                let seg_start =
                    get_edge_start_location(&ebg_nodes.nodes[segment_edges[0] as usize], nbg_geo);
                let seg_start_bearing =
                    get_edge_bearing(&ebg_nodes.nodes[segment_edges[0] as usize], nbg_geo, true);

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
                let arrive_loc = get_edge_end_location(node, nbg_geo);
                let arrive_geom = build_edge_geometry(edge_id, ebg_nodes, nbg_geo, format);
                steps.push(RouteStep {
                    distance_m: edge_distance,
                    duration_s: edge_duration,
                    geometry: arrive_geom,
                    maneuver: StepManeuver {
                        location: arrive_loc,
                        bearing_before: get_edge_bearing(node, nbg_geo, false),
                        bearing_after: 0,
                        maneuver_type: "arrive".to_string(),
                        modifier: None,
                        name: lookup_road_name(edge_id, ebg_nodes, nbg_geo, way_names),
                    },
                });
            } else {
                // Turn step
                let turn_loc = get_edge_start_location(node, nbg_geo);
                let is_roundabout = (node.class_bits & 0x08) != 0; // bit3 = roundabout
                let m_type = if is_roundabout { "roundabout" } else { "turn" };

                let turn_geom = build_edge_geometry(edge_id, ebg_nodes, nbg_geo, format);
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

        prev_end_bearing = get_edge_bearing(node, nbg_geo, false);
    }

    steps
}

/// Get start location of an EBG edge
fn get_edge_start_location(
    node: &crate::formats::ebg_nodes::EbgNode,
    nbg_geo: &crate::formats::NbgGeo,
) -> [f64; 2] {
    let geom_idx = node.geom_idx as usize;
    if geom_idx < nbg_geo.polylines.len() {
        let poly = &nbg_geo.polylines[geom_idx];
        if !poly.lat_fxp.is_empty() {
            return [poly.lon_fxp[0] as f64 / 1e7, poly.lat_fxp[0] as f64 / 1e7];
        }
    }
    [0.0, 0.0]
}

/// Get end location of an EBG edge
fn get_edge_end_location(
    node: &crate::formats::ebg_nodes::EbgNode,
    nbg_geo: &crate::formats::NbgGeo,
) -> [f64; 2] {
    let geom_idx = node.geom_idx as usize;
    if geom_idx < nbg_geo.polylines.len() {
        let poly = &nbg_geo.polylines[geom_idx];
        if !poly.lat_fxp.is_empty() {
            let last = poly.lat_fxp.len() - 1;
            return [
                poly.lon_fxp[last] as f64 / 1e7,
                poly.lat_fxp[last] as f64 / 1e7,
            ];
        }
    }
    [0.0, 0.0]
}

/// Get bearing of an EBG edge (at start or end)
fn get_edge_bearing(
    node: &crate::formats::ebg_nodes::EbgNode,
    nbg_geo: &crate::formats::NbgGeo,
    at_start: bool,
) -> u16 {
    let geom_idx = node.geom_idx as usize;
    if geom_idx < nbg_geo.polylines.len() {
        let poly = &nbg_geo.polylines[geom_idx];
        if poly.lat_fxp.len() >= 2 {
            let (i0, i1) = if at_start {
                (0, 1)
            } else {
                (poly.lat_fxp.len() - 2, poly.lat_fxp.len() - 1)
            };
            let lat1 = poly.lat_fxp[i0] as f64 / 1e7;
            let lon1 = poly.lon_fxp[i0] as f64 / 1e7;
            let lat2 = poly.lat_fxp[i1] as f64 / 1e7;
            let lon2 = poly.lon_fxp[i1] as f64 / 1e7;
            return compute_bearing(lat1, lon1, lat2, lon2);
        }
    }
    0
}

/// Compute bearing between two points (degrees 0-359)
fn compute_bearing(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> u16 {
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
fn bearing_diff(b1: u16, b2: u16) -> u16 {
    ((b2 as i32 - b1 as i32 + 360) % 360) as u16
}

/// Classify turn by angle
fn classify_turn(angle: u16) -> &'static str {
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
    nbg_geo: &crate::formats::NbgGeo,
    format: GeometryFormat,
) -> RouteGeometry {
    let node = &ebg_nodes.nodes[edge_id as usize];
    let geom_idx = node.geom_idx as usize;
    let mut points = Vec::new();

    if geom_idx < nbg_geo.polylines.len() {
        let poly = &nbg_geo.polylines[geom_idx];
        for j in 0..poly.lat_fxp.len() {
            points.push(Point {
                lon: poly.lon_fxp[j] as f64 / 1e7,
                lat: poly.lat_fxp[j] as f64 / 1e7,
            });
        }
    }

    let distance_m = node.length_mm as f64 / 1000.0;
    RouteGeometry::from_points(points, distance_m, 0, format)
}

/// Build geometry for multiple consecutive edges
fn build_multi_edge_geometry(
    edge_ids: &[u32],
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
    format: GeometryFormat,
) -> RouteGeometry {
    let mut points = Vec::new();
    let mut total_distance_mm: u64 = 0;

    for &edge_id in edge_ids {
        let node = &ebg_nodes.nodes[edge_id as usize];
        let geom_idx = node.geom_idx as usize;
        total_distance_mm += node.length_mm as u64;

        if geom_idx < nbg_geo.polylines.len() {
            let poly = &nbg_geo.polylines[geom_idx];
            let start = if points.is_empty() { 0 } else { 1 }; // skip duplicate at join
            for j in start..poly.lat_fxp.len() {
                points.push(Point {
                    lon: poly.lon_fxp[j] as f64 / 1e7,
                    lat: poly.lat_fxp[j] as f64 / 1e7,
                });
            }
        }
    }

    RouteGeometry::from_points(points, total_distance_mm as f64 / 1000.0, 0, format)
}

// ============ Nearest Endpoint ============

#[derive(Debug, Deserialize, ToSchema)]
pub struct NearestRequest {
    /// Longitude to snap
    #[schema(example = 4.3517)]
    lon: f64,
    /// Latitude to snap
    #[schema(example = 50.8503)]
    lat: f64,
    /// Transport mode: car, bike, or foot
    #[schema(example = "car")]
    mode: String,
    /// Number of nearest results (default 1, max 10)
    #[serde(default = "default_number")]
    number: u32,
}

fn default_number() -> u32 {
    1
}

/// A nearest waypoint result
#[derive(Debug, Serialize, ToSchema)]
pub struct NearestWaypoint {
    /// Snapped location [lon, lat]
    pub location: [f64; 2],
    /// Distance from query point to snapped point in meters
    pub distance: f64,
    /// Edge length in meters
    pub edge_length_m: f64,
}

/// Response for nearest endpoint
#[derive(Debug, Serialize, ToSchema)]
pub struct NearestResponse {
    /// Status code
    pub code: String,
    /// Nearest waypoints
    pub waypoints: Vec<NearestWaypoint>,
}

/// Find nearest road segments to a coordinate
#[utoipa::path(
    get,
    path = "/nearest",
    params(
        ("lon" = f64, Query, description = "Longitude"),
        ("lat" = f64, Query, description = "Latitude"),
        ("mode" = String, Query, description = "Transport mode: car, bike, foot"),
        ("number" = Option<u32>, Query, description = "Number of results (default 1, max 10)"),
    ),
    responses(
        (status = 200, description = "Nearest roads found", body = NearestResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn nearest(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<NearestRequest>,
) -> impl IntoResponse {
    if let Err(e) = validate_coord(req.lon, req.lat, "query point") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
    }
    if req.number > 100 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("number {} exceeds maximum of 100", req.number),
            }),
        )
            .into_response();
    }

    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let mode_data = state.get_mode(mode);
    let k = (req.number.clamp(1, 100)) as usize;

    let results = state
        .spatial_index
        .snap_k_with_info(req.lon, req.lat, &mode_data.mask, k);

    if results.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "No road found within snap distance".to_string(),
            }),
        )
            .into_response();
    }

    let waypoints: Vec<NearestWaypoint> = results
        .iter()
        .map(|&(ebg_id, snap_lon, snap_lat, dist_m)| {
            let edge_length = state.ebg_nodes.nodes[ebg_id as usize].length_mm as f64 / 1000.0;
            NearestWaypoint {
                location: [snap_lon, snap_lat],
                distance: dist_m,
                edge_length_m: edge_length,
            }
        })
        .collect();

    Json(NearestResponse {
        code: "Ok".to_string(),
        waypoints,
    })
    .into_response()
}

// ============ Table Endpoint (OSRM-compatible) ============

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
}

fn default_annotations() -> String {
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

#[derive(Debug, Serialize, ToSchema)]
pub struct Waypoint {
    /// Snapped location [lon, lat]
    pub location: [f64; 2],
    /// Name (empty for now)
    pub name: String,
}

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

    let mode = match parse_mode(&req.mode) {
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
    let want_duration = annotations.contains(&"duration");
    let want_distance = annotations.contains(&"distance");

    // Default: if neither specified, return duration
    let (want_duration, want_distance) = if !want_duration && !want_distance {
        (true, false)
    } else {
        (want_duration, want_distance)
    };

    compute_table_bucket_m2m(
        &state,
        mode,
        &req.sources,
        &req.destinations,
        want_duration,
        want_distance,
    )
    .await
}

/// Core table computation using bucket M2M algorithm
async fn compute_table_bucket_m2m(
    state: &Arc<ServerState>,
    mode: Mode,
    sources: &[[f64; 2]],
    destinations: &[[f64; 2]],
    want_duration: bool,
    want_distance: bool,
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
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &mode_data.mask, 10) {
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

    // Compute duration matrix if requested
    let durations = if want_duration {
        let (matrix, _stats) = if use_parallel {
            table_bucket_parallel(
                n_nodes,
                &mode_data.up_adj_flat,
                &mode_data.down_rev_flat,
                &sources_rank,
                &targets_rank,
            )
        } else {
            table_bucket_full_flat(
                n_nodes,
                &mode_data.up_adj_flat,
                &mode_data.down_rev_flat,
                &sources_rank,
                &targets_rank,
            )
        };

        Some(flat_matrix_to_2d(
            &matrix,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            |v| v as f64 / 10.0, // deciseconds → seconds
        ))
    } else {
        None
    };

    // Compute distance matrix if requested (independent shortest-distance metric)
    let distances = if want_distance {
        let (matrix, _stats) = if use_parallel {
            table_bucket_parallel(
                n_nodes,
                &mode_data.up_adj_flat_dist,
                &mode_data.down_rev_flat_dist,
                &sources_rank,
                &targets_rank,
            )
        } else {
            table_bucket_full_flat(
                n_nodes,
                &mode_data.up_adj_flat_dist,
                &mode_data.down_rev_flat_dist,
                &sources_rank,
                &targets_rank,
            )
        };

        Some(flat_matrix_to_2d(
            &matrix,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            |v| v as f64 / 1000.0, // millimeters → meters
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
fn flat_matrix_to_2d(
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

fn default_tile_size() -> usize {
    1000
}

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

    let mode = match parse_mode(&req.mode) {
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

    // Calculate total tiles for progress tracking
    let n_src_blocks = n_total_sources.div_ceil(src_tile_size);
    let n_dst_blocks = n_total_targets.div_ceil(dst_tile_size);
    let n_total_tiles = n_src_blocks * n_dst_blocks;
    let n_total_cells = n_total_sources * n_total_targets;

    // Create channel for streaming tiles
    let (tx, rx) = tokio::sync::mpsc::channel::<Result<bytes::Bytes, std::io::Error>>(8);

    // Cancellation flag: set when client disconnects (channel closed)
    let cancelled = Arc::new(AtomicBool::new(false));

    // Clone what we need for the spawned task
    let up_adj_flat = mode_data.up_adj_flat.clone();
    let down_rev_flat = mode_data.down_rev_flat.clone();
    let cancelled_outer = cancelled.clone();

    // Spawn compute task - SOURCE-BLOCK OUTER LOOP to avoid repeated forward computation
    // For 10k×10k with 1000×1000 tiles: forward computed 10x (once per src block) instead of 100x
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

                // Stream this tile — stop computation if client disconnected
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
    /// Transport mode (car, bike, foot)
    mode: String,
    /// Direction: "depart" (default) or "arrive"
    #[serde(default = "default_direction")]
    direction: String,
    /// Geometry encoding: polyline6 (default), geojson, points
    #[serde(default = "default_geometries")]
    geometries: String,
    /// Optional fields to include: "network" adds reachable road geometries
    #[serde(default)]
    include: Option<String>,
}

#[derive(Debug, Serialize, ToSchema)]
pub struct IsochroneResponse {
    /// Polygon as encoded polyline6 string (default)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub polygon: Option<String>,
    /// Polygon as GeoJSON coordinates [[lon, lat], ...]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Vec<Vec<f64>>>)]
    pub polygon_geojson: Option<Vec<[f64; 2]>>,
    /// Polygon as point array [{lon, lat}, ...]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub polygon_points: Option<Vec<Point>>,
    /// Number of reachable edges
    pub reachable_edges: usize,
    /// Network isochrone - reachable road segments (only if include=network)
    /// Each segment is [[lon, lat], [lon, lat], ...]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<Vec<Vec<[f64; 2]>>>,
}

/// Calculate isochrone (reachable area within time limit)
///
/// Content negotiation:
/// - Accept: application/json (default) → JSON response
/// - Accept: application/octet-stream → WKB binary polygon
///
/// Optional fields via `include` parameter:
/// - include=network → adds reachable road segments as polylines
#[utoipa::path(
    get,
    path = "/isochrone",
    params(
        ("lon" = f64, Query, description = "Center longitude"),
        ("lat" = f64, Query, description = "Center latitude"),
        ("time_s" = u32, Query, description = "Time limit in seconds"),
        ("mode" = String, Query, description = "Transport mode (car, bike, foot)"),
        ("direction" = Option<String>, Query, description = "Direction: 'depart' (default) or 'arrive'"),
        ("geometries" = Option<String>, Query, description = "Geometry encoding: polyline6 (default), geojson, points"),
        ("include" = Option<String>, Query, description = "Optional fields: 'network' for road geometries"),
    ),
    responses(
        (status = 200, description = "Isochrone computed", body = IsochroneResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
async fn isochrone(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<IsochroneRequest>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = validate_coord(req.lon, req.lat, "center") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
    }
    if req.time_s == 0 || req.time_s > 7200 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("time_s must be between 1 and 7200, got {}", req.time_s),
            }),
        )
            .into_response();
    }

    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let geom_format = match GeometryFormat::parse(&req.geometries) {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let reverse = match req.direction.as_str() {
        "depart" => false,
        "arrive" => true,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid direction: '{}'. Use 'depart' or 'arrive'.", other),
                }),
            )
                .into_response()
        }
    };

    let mode_data = state.get_mode(mode);
    let time_ds = req.time_s * 10;

    // Parse include parameter
    let include_network = req
        .include
        .as_ref()
        .map(|s| s.split(',').any(|p| p.trim() == "network"))
        .unwrap_or(false);

    // Check Accept header for content negotiation
    let wants_wkb = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/octet-stream") || s.contains("application/wkb"))
        .unwrap_or(false);

    // Snap center
    let center_orig = match state
        .spatial_index
        .snap(req.lon, req.lat, &mode_data.mask, 10)
    {
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

    // Run PHAST (forward for depart, reverse for arrive)
    let phast_settled = if reverse {
        run_phast_bounded_fast_reverse(
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            center_rank,
            time_ds,
            mode,
        )
    } else {
        run_phast_bounded_fast(
            &mode_data.cch_topo,
            &mode_data.cch_weights,
            center_rank,
            time_ds,
            mode,
        )
    };

    // Convert to original IDs
    let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
    for (rank, dist) in phast_settled {
        let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        let original_id = mode_data.filtered_ebg.filtered_to_original[filtered_id as usize];
        settled.push((original_id, dist));
    }

    // Build polygon
    let polygon = build_isochrone_geometry_concave(
        &settled,
        time_ds,
        &mode_data.node_weights,
        &state.ebg_nodes,
        &state.nbg_geo,
    );

    // WKB binary response (content negotiation via Accept header)
    if wants_wkb {
        use crate::range::contour::ContourResult;
        use crate::range::wkb_stream::encode_polygon_wkb;
        let coords: Vec<(f64, f64)> = polygon.iter().map(|p| (p.lon, p.lat)).collect();
        let contour = ContourResult {
            outer_ring: coords,
            holes: vec![],
            stats: Default::default(),
        };
        match encode_polygon_wkb(&contour) {
            Some(wkb) => {
                return (
                    [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                    wkb,
                )
                    .into_response()
            }
            None => return (StatusCode::NO_CONTENT, Vec::<u8>::new()).into_response(),
        }
    }

    // Build network if requested
    let network = if include_network {
        Some(build_network_geometry(
            &settled,
            time_ds,
            &mode_data.node_weights,
            &state.ebg_nodes,
            &state.nbg_geo,
        ))
    } else {
        None
    };

    let (poly_encoded, poly_geojson, poly_points) = match geom_format {
        GeometryFormat::Polyline6 => (Some(encode_polyline6(&polygon)), None, None),
        GeometryFormat::GeoJson => {
            // Ensure CCW orientation, truncate to 5 decimal places (~1m), close ring
            // Isochrone polygons come from a 30m raster grid — 5 decimals is honest precision
            use crate::range::wkb_stream::ensure_ccw;
            let trunc = |v: f64| (v * 1e5).round() / 1e5;
            let mut coords: Vec<(f64, f64)> = polygon
                .iter()
                .map(|p| (trunc(p.lon), trunc(p.lat)))
                .collect();
            ensure_ccw(&mut coords);
            let mut ring: Vec<[f64; 2]> = coords.into_iter().map(|(x, y)| [x, y]).collect();
            // Close ring if needed
            if let (Some(first), Some(last)) = (ring.first().copied(), ring.last().copied()) {
                if first != last {
                    ring.push(first);
                }
            }
            (None, Some(ring), None)
        }
        GeometryFormat::Points => (None, None, Some(polygon)),
    };

    Json(IsochroneResponse {
        polygon: poly_encoded,
        polygon_geojson: poly_geojson,
        polygon_points: poly_points,
        reachable_edges: settled.len(),
        network,
    })
    .into_response()
}

/// Build network geometry - all reachable road segments as polylines
fn build_network_geometry(
    settled: &[(u32, u32)],
    time_ds: u32,
    node_weights: &[u32],
    ebg_nodes: &crate::formats::EbgNodes,
    nbg_geo: &crate::formats::NbgGeo,
) -> Vec<Vec<[f64; 2]>> {
    let mut network: Vec<Vec<[f64; 2]>> = Vec::with_capacity(settled.len());

    for &(ebg_id, dist_ds) in settled {
        if dist_ds > time_ds {
            continue;
        }

        let weight_ds = if (ebg_id as usize) < node_weights.len() {
            node_weights[ebg_id as usize]
        } else {
            continue;
        };

        if weight_ds == 0 || weight_ds == u32::MAX {
            continue;
        }

        let node = &ebg_nodes.nodes[ebg_id as usize];
        let geom_idx = node.geom_idx as usize;
        if geom_idx >= nbg_geo.polylines.len() {
            continue;
        }
        let polyline = &nbg_geo.polylines[geom_idx];
        if polyline.lat_fxp.is_empty() {
            continue;
        }

        let dist_end_ds = dist_ds.saturating_add(weight_ds);

        if dist_end_ds <= time_ds {
            // Fully reachable
            let coords: Vec<[f64; 2]> = polyline
                .lon_fxp
                .iter()
                .zip(polyline.lat_fxp.iter())
                .map(|(&lon, &lat)| [lon as f64 / 1e7, lat as f64 / 1e7])
                .collect();
            if coords.len() >= 2 {
                network.push(coords);
            }
        } else {
            // Partially reachable - clip
            let cut_fraction = (time_ds - dist_ds) as f32 / weight_ds as f32;
            let n_pts = polyline.lat_fxp.len();
            let cut_idx = ((n_pts - 1) as f32 * cut_fraction).ceil() as usize;
            let cut_idx = cut_idx.min(n_pts - 1).max(1);

            let coords: Vec<[f64; 2]> = polyline.lon_fxp[..=cut_idx]
                .iter()
                .zip(polyline.lat_fxp[..=cut_idx].iter())
                .map(|(&lon, &lat)| [lon as f64 / 1e7, lat as f64 / 1e7])
                .collect();
            if coords.len() >= 2 {
                network.push(coords);
            }
        }
    }

    network
}

/// Bulk isochrone request
#[derive(Debug, Deserialize)]
pub struct BulkIsochroneRequest {
    /// List of origins as [lon, lat] pairs
    origins: Vec<[f64; 2]>,
    /// Time limit in seconds
    time_s: u32,
    /// Transport mode
    mode: String,
}

/// POST /isochrone/bulk - Compute multiple isochrones in parallel, return WKB stream
///
/// Returns a binary stream of WKB polygons with length-prefixed format:
/// For each isochrone: [4 bytes: origin_idx as u32][4 bytes: wkb_len as u32][wkb_len bytes: WKB]
async fn isochrone_bulk(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<BulkIsochroneRequest>,
) -> impl IntoResponse {
    use crate::range::contour::ContourResult;
    use crate::range::wkb_stream::encode_polygon_wkb;
    use rayon::prelude::*;

    if req.origins.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            "origins cannot be empty".as_bytes().to_vec(),
        )
            .into_response();
    }
    for (i, &[lon, lat]) in req.origins.iter().enumerate() {
        if let Err(e) = validate_coord(lon, lat, &format!("origin[{}]", i)) {
            return (StatusCode::BAD_REQUEST, e.into_bytes()).into_response();
        }
    }
    if req.time_s == 0 || req.time_s > 7200 {
        return (
            StatusCode::BAD_REQUEST,
            format!("time_s must be between 1 and 7200, got {}", req.time_s).into_bytes(),
        )
            .into_response();
    }

    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => return (StatusCode::BAD_REQUEST, e.into_bytes()).into_response(),
    };

    let mode_data = state.get_mode(mode);
    let time_ds = req.time_s * 10;

    // Process all origins in parallel
    let results: Vec<(u32, Vec<u8>)> = req
        .origins
        .par_iter()
        .enumerate()
        .filter_map(|(idx, &[lon, lat])| {
            // Snap origin
            let center_orig = state.spatial_index.snap(lon, lat, &mode_data.mask, 10)?;
            let center_filtered = mode_data.filtered_ebg.original_to_filtered[center_orig as usize];
            if center_filtered == u32::MAX {
                return None;
            }
            let center_rank = mode_data.order.perm[center_filtered as usize];

            // Run PHAST - Note: thread-local state handles per-thread allocation
            let phast_settled = run_phast_bounded_fast(
                &mode_data.cch_topo,
                &mode_data.cch_weights,
                center_rank,
                time_ds,
                mode,
            );

            // Convert to original IDs
            let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
            for (rank, dist) in phast_settled {
                let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
                let original_id = mode_data.filtered_ebg.filtered_to_original[filtered_id as usize];
                settled.push((original_id, dist));
            }

            // Build polygon using frontier-based concave hull
            let points = build_isochrone_geometry_concave(
                &settled,
                time_ds,
                &mode_data.node_weights,
                &state.ebg_nodes,
                &state.nbg_geo,
            );
            let outer_ring: Vec<(f64, f64)> = points.iter().map(|p| (p.lon, p.lat)).collect();
            let contour = ContourResult {
                outer_ring,
                holes: vec![],
                stats: Default::default(),
            };

            // Encode WKB
            encode_polygon_wkb(&contour).map(|wkb| (idx as u32, wkb))
        })
        .collect();

    // Build response: concatenated length-prefixed WKB
    let n_total_origins = req.origins.len();
    let n_successful = results.len();
    let mut response = Vec::with_capacity(results.len() * 500);
    for (origin_idx, wkb) in results {
        response.extend_from_slice(&origin_idx.to_le_bytes());
        response.extend_from_slice(&(wkb.len() as u32).to_le_bytes());
        response.extend_from_slice(&wkb);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        // Progress tracking headers
        .header("X-Total-Origins", n_total_origins.to_string())
        .header("X-Successful-Isochrones", n_successful.to_string())
        .header(
            "X-Failed-Isochrones",
            (n_total_origins - n_successful).to_string(),
        )
        .body(Body::from(response))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build bulk isochrone response",
            )
                .into_response()
        })
}

/// Bounded Dijkstra for isochrone computation (operates in filtered node space)
#[allow(dead_code)]
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
        let n_blocks = n_nodes.div_ceil(PHAST_BLOCK_SIZE);
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
pub fn run_phast_bounded_fast(
    cch_topo: &crate::formats::CchTopo,
    cch_weights: &super::state::CchWeights,
    origin_rank: u32,
    threshold: u32,
    mode: crate::profile_abi::Mode,
) -> Vec<(u32, u32)> {
    use crate::profile_abi::Mode;
    use std::cmp::Reverse;

    let n_nodes = cch_topo.n_nodes as usize;

    // Get thread-local state for this mode
    let state_cell = match mode {
        Mode::Car => &PHAST_STATE_CAR,
        Mode::Bike => &PHAST_STATE_BIKE,
        Mode::Foot => &PHAST_STATE_FOOT,
    };

    state_cell.with(|cell| {
        let mut state_opt = cell.borrow_mut();

        // Initialize or reinitialize if needed
        let state = state_opt.get_or_insert_with(|| PhastState::new(n_nodes));

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

/// Run REVERSE PHAST bounded query — computes d(all → target) for reverse isochrones.
///
/// Swaps up/down adjacencies: upward uses DOWN-reverse edges, downward uses UP edges.
/// Uses a PULL approach in the downward phase (for each node v, pull from higher-rank
/// neighbors via up_adj_flat[v]) since we need reversed UP edges.
pub fn run_phast_bounded_fast_reverse(
    up_adj_flat: &crate::matrix::bucket_ch::UpAdjFlat,
    down_rev_flat: &crate::matrix::bucket_ch::DownReverseAdjFlat,
    target_rank: u32,
    threshold: u32,
    mode: crate::profile_abi::Mode,
) -> Vec<(u32, u32)> {
    use crate::profile_abi::Mode;
    use std::cmp::Reverse;

    let n_nodes = up_adj_flat.offsets.len() - 1;

    let state_cell = match mode {
        Mode::Car => &PHAST_STATE_CAR,
        Mode::Bike => &PHAST_STATE_BIKE,
        Mode::Foot => &PHAST_STATE_FOOT,
    };

    state_cell.with(|cell| {
        let mut state_opt = cell.borrow_mut();

        // Initialize or reinitialize if needed
        let state = state_opt.get_or_insert_with(|| PhastState::new(n_nodes));
        if state.dist.len() != n_nodes {
            *state = PhastState::new(n_nodes);
        }

        state.start_query();
        state.set_dist(target_rank as usize, 0);

        // Phase 1: Upward search using DOWN-reverse edges (goes to higher rank nodes)
        state.pq.push(Reverse((0, target_rank)));

        while let Some(Reverse((d, u))) = state.pq.pop() {
            if d > threshold {
                break;
            }
            if d > state.get_dist(u as usize) {
                continue;
            }

            // down_rev_flat[u] gives higher-rank neighbors with DOWN weights
            let start = down_rev_flat.offsets[u as usize] as usize;
            let end = down_rev_flat.offsets[u as usize + 1] as usize;

            for i in start..end {
                let v = down_rev_flat.sources[i] as usize; // v has higher rank
                let w = down_rev_flat.weights[i];

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

        // Phase 2: Plain downward PULL scan using UP edges
        // For each node v (decreasing rank), pull from higher-rank neighbors u
        // via up_adj_flat[v].targets (which have higher rank).
        //
        // NOTE: Block-gating is NOT used here because PULL cannot propagate
        // block activation downward (unlike PUSH in forward PHAST). A PUSH
        // approach would need a reverse-UP adjacency we don't have.
        for v in (0..n_nodes).rev() {
            let up_start = up_adj_flat.offsets[v] as usize;
            let up_end = up_adj_flat.offsets[v + 1] as usize;

            for i in up_start..up_end {
                let u = up_adj_flat.targets[i] as usize; // u has higher rank
                let w = up_adj_flat.weights[i];

                let d_u = state.get_dist(u);
                if d_u == u32::MAX || d_u > threshold {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                if new_dist < state.get_dist(v) {
                    state.set_dist(v, new_dist);
                }
            }
        }

        // Collect settled nodes (full scan — no block-gating)
        let mut result: Vec<(u32, u32)> = Vec::with_capacity(n_nodes / 10);
        for rank in 0..n_nodes {
            if state.version[rank] == state.current_gen {
                let d = state.dist[rank];
                if d <= threshold {
                    result.push((rank as u32, d));
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

// ============ Height Endpoint ============

/// Query elevation for coordinates using SRTM data
async fn height(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<super::elevation::HeightRequest>,
) -> impl IntoResponse {
    let elevation = match &state.elevation {
        Some(e) => e,
        None => {
            return (
                StatusCode::SERVICE_UNAVAILABLE,
                Json(ErrorResponse {
                    error: "Elevation data not loaded. Place SRTM .hgt files in data/srtm/"
                        .to_string(),
                }),
            )
                .into_response()
        }
    };

    match super::elevation::handle_height_request(elevation, &req) {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    }
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
async fn health(State(state): State<Arc<ServerState>>) -> impl IntoResponse {
    let uptime = state.started_at.elapsed();
    Json(serde_json::json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "uptime_s": uptime.as_secs(),
        "modes": ["car", "bike", "foot"],
        "data_dir": state.data_dir,
        "nodes_count": state.ebg_nodes.n_nodes,
        "edges_count": state.ebg_csr.n_arcs,
        "named_roads_count": state.way_names.len(),
    }))
}

// ============ Input Validation ============

/// Validate that a coordinate is within valid bounds.
fn validate_coord(lon: f64, lat: f64, label: &str) -> Result<(), String> {
    if !(-180.0..=180.0).contains(&lon) {
        return Err(format!(
            "{} longitude {} is outside valid range [-180, 180]",
            label, lon
        ));
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err(format!(
            "{} latitude {} is outside valid range [-90, 90]",
            label, lat
        ));
    }
    if lon.is_nan() || lat.is_nan() {
        return Err(format!("{} coordinates contain NaN", label));
    }
    Ok(())
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
    let mode = match parse_mode(&req.mode) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response()
        }
    };

    let mode_data = state.get_mode(mode);

    // Snap source and destination
    let src_orig = match state
        .spatial_index
        .snap(req.src_lon, req.src_lat, &mode_data.mask, 10)
    {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Cannot snap source".to_string(),
                }),
            )
                .into_response()
        }
    };
    let dst_orig = match state
        .spatial_index
        .snap(req.dst_lon, req.dst_lat, &mode_data.mask, 10)
    {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Cannot snap dest".to_string(),
                }),
            )
                .into_response()
        }
    };

    // Convert to filtered space
    let src_filtered = mode_data.filtered_ebg.original_to_filtered[src_orig as usize];
    let dst_filtered = mode_data.filtered_ebg.original_to_filtered[dst_orig as usize];

    if src_filtered == u32::MAX || dst_filtered == u32::MAX {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Node not accessible".to_string(),
            }),
        )
            .into_response();
    }

    let _n = mode_data.cch_topo.n_nodes as usize;
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
    let tgt_down_incoming = mode_data.down_rev.offsets[dst_filtered as usize + 1]
        - mode_data.down_rev.offsets[dst_filtered as usize];

    eprintln!("DEBUG COMPARE:");
    eprintln!(
        "  Source {} (rank {}): {} UP edges, {} DOWN edges",
        src_filtered,
        src_rank,
        src_up_end - src_up_start,
        src_down_end - src_down_start
    );
    eprintln!(
        "  Target {} (rank {}): {} incoming UP edges, {} incoming DOWN edges",
        dst_filtered, dst_rank, tgt_up_incoming, tgt_down_incoming
    );
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
        eprintln!(
            "    {} (rank {}) → {} with weight {} (edge_idx {})",
            src_node, src_rank, dst_filtered, weight, edge_idx
        );
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
                eprintln!(
                    "    {} (rank {}) → {} with weight {} (edge_idx {})",
                    src_node, perm[src_node as usize], dst_filtered, weight, i
                );
                found += 1;
                if found >= 5 {
                    break;
                }
            }
        }
        if found >= 5 {
            break;
        }
    }

    // Run separate UP-only and DOWN-only searches to verify
    eprintln!("\n  Running separate UP-only Dijkstra from source...");
    let up_only_dist =
        run_up_only_dijkstra(&mode_data.cch_topo, &mode_data.cch_weights, src_filtered);
    let fwd_reachable = up_only_dist.iter().filter(|&&d| d != u32::MAX).count();
    eprintln!("    Reachable nodes via UP-only: {}", fwd_reachable);
    eprintln!(
        "    dist_up[target={}] = {:?}",
        dst_filtered,
        if up_only_dist[dst_filtered as usize] == u32::MAX {
            "UNREACHABLE".to_string()
        } else {
            up_only_dist[dst_filtered as usize].to_string()
        }
    );

    eprintln!("\n  Running separate DOWN-only Dijkstra to target...");
    let down_only_dist = run_down_only_to_target(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &mode_data.down_rev,
        dst_filtered,
    );
    let bwd_reachable = down_only_dist.iter().filter(|&&d| d != u32::MAX).count();
    eprintln!(
        "    Reachable nodes via DOWN-only to target: {}",
        bwd_reachable
    );
    eprintln!(
        "    dist_down[source={}] = {:?}",
        src_filtered,
        if down_only_dist[src_filtered as usize] == u32::MAX {
            "UNREACHABLE".to_string()
        } else {
            down_only_dist[src_filtered as usize].to_string()
        }
    );

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
    eprintln!(
        "    Best meeting: node {} with dist_up={} + dist_down={} = {}",
        best_meet_node,
        up_only_dist
            .get(best_meet_node as usize)
            .unwrap_or(&u32::MAX),
        down_only_dist
            .get(best_meet_node as usize)
            .unwrap_or(&u32::MAX),
        best_meet
    );

    // Analyze the CCH Dijkstra path and verify edge weights
    if let Some(reported) = cch_dijkstra_distance.filter(|_| !cch_path.is_empty()) {
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
        eprintln!("    Reported distance: {}", reported);
        eprintln!("    Sum of edge weights: {}", weight_sum);
        if weight_sum != reported {
            eprintln!(
                "    ⚠️ WEIGHT MISMATCH! Diff = {}",
                weight_sum as i64 - reported as i64
            );
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
        eprintln!(
            "    Peaks (up→down): {}, Valleys (down→up): {}",
            peaks, valleys
        );

        // For each valley, check if there's an UP shortcut that bypasses it
        if valleys > 0 {
            eprintln!("\n    Checking shortcuts at valleys:");
            prev_rank = perm[cch_path[0] as usize];
            let mut _going_up2 = true;
            let mut valley_count = 0;
            for i in 1..cch_path.len().saturating_sub(1) {
                let curr_rank = perm[cch_path[i] as usize];
                let next_rank = perm[cch_path[i + 1] as usize];

                // Detect valley: was going down, now going up
                let was_going_down = prev_rank > curr_rank;
                let now_going_up = curr_rank < next_rank;

                if was_going_down && now_going_up && valley_count < 3 {
                    valley_count += 1;
                    let prev_node = cch_path[i - 1] as usize;
                    let curr_node = cch_path[i] as usize;
                    let next_node = cch_path[i + 1] as usize;

                    // Cost through valley
                    let down_edge_weight = find_edge_weight(
                        &mode_data.cch_topo,
                        &mode_data.cch_weights,
                        prev_node,
                        curr_node as u32,
                        perm,
                    );
                    let up_edge_weight = find_edge_weight(
                        &mode_data.cch_topo,
                        &mode_data.cch_weights,
                        curr_node,
                        next_node as u32,
                        perm,
                    );
                    let valley_cost = down_edge_weight
                        .unwrap_or(u32::MAX)
                        .saturating_add(up_edge_weight.unwrap_or(u32::MAX));

                    // Check for direct UP shortcut from prev to next
                    let direct_up = if perm[prev_node] < perm[next_node] {
                        // Should be in UP graph
                        let start = mode_data.cch_topo.up_offsets[prev_node] as usize;
                        let end = mode_data.cch_topo.up_offsets[prev_node + 1] as usize;
                        let mut found = None;
                        for idx in start..end {
                            if mode_data.cch_topo.up_targets[idx] == next_node as u32 {
                                found = Some((
                                    mode_data.cch_weights.up[idx],
                                    mode_data.cch_topo.up_is_shortcut[idx],
                                ));
                                break;
                            }
                        }
                        found
                    } else {
                        None // Not an UP edge direction
                    };

                    eprintln!(
                        "      Valley {}: {} (rank {}) → {} (rank {}) → {} (rank {})",
                        valley_count,
                        prev_node,
                        perm[prev_node],
                        curr_node,
                        curr_rank,
                        next_node,
                        perm[next_node]
                    );
                    eprintln!(
                        "        Through valley: {} + {} = {}",
                        down_edge_weight.unwrap_or(0),
                        up_edge_weight.unwrap_or(0),
                        valley_cost
                    );
                    match direct_up {
                        Some((w, is_shortcut)) => {
                            eprintln!("        Direct UP edge: w={}, shortcut={}", w, is_shortcut);
                            if w <= valley_cost {
                                eprintln!(
                                    "        ✓ Shortcut is cheaper or equal - should be used!"
                                );
                            } else {
                                eprintln!(
                                    "        Shortcut is more expensive (diff={})",
                                    w as i64 - valley_cost as i64
                                );
                                // Show the middle node for this shortcut
                                if is_shortcut {
                                    let start = mode_data.cch_topo.up_offsets[prev_node] as usize;
                                    let end = mode_data.cch_topo.up_offsets[prev_node + 1] as usize;
                                    for idx in start..end {
                                        if mode_data.cch_topo.up_targets[idx] == next_node as u32 {
                                            let middle = mode_data.cch_topo.up_middle[idx];
                                            let middle_rank = perm[middle as usize];
                                            eprintln!(
                                                "        Shortcut middle: {} (rank {})",
                                                middle, middle_rank
                                            );
                                            eprintln!(
                                                "        Valley middle:   {} (rank {})",
                                                curr_node, curr_rank
                                            );
                                            // Compute expected shortcut weight
                                            let w_um = find_edge_weight(
                                                &mode_data.cch_topo,
                                                &mode_data.cch_weights,
                                                prev_node,
                                                middle,
                                                perm,
                                            );
                                            let w_mv = find_edge_weight(
                                                &mode_data.cch_topo,
                                                &mode_data.cch_weights,
                                                middle as usize,
                                                next_node as u32,
                                                perm,
                                            );
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
                    _going_up2 = true;
                } else if curr_rank < prev_rank {
                    _going_up2 = false;
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
    }))
    .into_response()
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
#[allow(dead_code)]
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

    (
        if dist[dst as usize] == u32::MAX {
            None
        } else {
            Some(dist[dst as usize])
        },
        settled,
    )
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

    (
        if dist[dst as usize] == u32::MAX {
            None
        } else {
            Some(dist[dst as usize])
        },
        settled,
        path,
    )
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

    (
        if dist[dst as usize] == u32::MAX {
            None
        } else {
            Some(dist[dst as usize])
        },
        settled,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    // === E4: Bearing computation tests ===

    #[test]
    fn test_compute_bearing_north() {
        let b = compute_bearing(50.0, 4.0, 51.0, 4.0);
        assert!(
            !(5..=355).contains(&b),
            "North bearing should be ~0, got {}",
            b
        );
    }

    #[test]
    fn test_compute_bearing_east() {
        let b = compute_bearing(50.0, 4.0, 50.0, 5.0);
        assert!(
            (b as i16 - 90).unsigned_abs() < 5,
            "East bearing should be ~90, got {}",
            b
        );
    }

    #[test]
    fn test_compute_bearing_south() {
        let b = compute_bearing(51.0, 4.0, 50.0, 4.0);
        assert!(
            (b as i16 - 180).unsigned_abs() < 5,
            "South bearing should be ~180, got {}",
            b
        );
    }

    #[test]
    fn test_compute_bearing_west() {
        let b = compute_bearing(50.0, 5.0, 50.0, 4.0);
        assert!(
            (b as i16 - 270).unsigned_abs() < 5,
            "West bearing should be ~270, got {}",
            b
        );
    }

    #[test]
    fn test_compute_bearing_northeast() {
        let b = compute_bearing(50.0, 4.0, 50.5, 4.5);
        assert!(b > 20 && b < 70, "NE bearing should be ~30-60, got {}", b);
    }

    // === E4: Bearing difference tests ===

    #[test]
    fn test_bearing_diff_straight() {
        assert_eq!(bearing_diff(90, 90), 0);
        assert_eq!(bearing_diff(0, 0), 0);
        assert_eq!(bearing_diff(359, 359), 0);
    }

    #[test]
    fn test_bearing_diff_right_turn() {
        assert_eq!(bearing_diff(0, 90), 90);
        assert_eq!(bearing_diff(270, 0), 90);
    }

    #[test]
    fn test_bearing_diff_left_turn() {
        assert_eq!(bearing_diff(90, 0), 270);
        assert_eq!(bearing_diff(0, 270), 270);
    }

    #[test]
    fn test_bearing_diff_uturn() {
        assert_eq!(bearing_diff(0, 180), 180);
        assert_eq!(bearing_diff(90, 270), 180);
    }

    #[test]
    fn test_bearing_diff_wrap_around() {
        assert_eq!(bearing_diff(350, 10), 20);
        assert_eq!(bearing_diff(10, 350), 340);
    }

    // === E4: Turn classification tests ===

    #[test]
    fn test_classify_turn_straight() {
        assert_eq!(classify_turn(0), "straight");
        assert_eq!(classify_turn(10), "straight");
        assert_eq!(classify_turn(350), "straight");
        assert_eq!(classify_turn(360), "straight");
    }

    #[test]
    fn test_classify_turn_slight_right() {
        assert_eq!(classify_turn(20), "slight right");
        assert_eq!(classify_turn(45), "slight right");
        assert_eq!(classify_turn(60), "slight right");
    }

    #[test]
    fn test_classify_turn_right() {
        assert_eq!(classify_turn(90), "right");
        assert_eq!(classify_turn(100), "right");
        assert_eq!(classify_turn(120), "right");
    }

    #[test]
    fn test_classify_turn_sharp_right() {
        assert_eq!(classify_turn(130), "sharp right");
        assert_eq!(classify_turn(150), "sharp right");
    }

    #[test]
    fn test_classify_turn_uturn() {
        assert_eq!(classify_turn(180), "uturn");
        assert_eq!(classify_turn(170), "uturn");
        assert_eq!(classify_turn(195), "uturn");
    }

    #[test]
    fn test_classify_turn_sharp_left() {
        assert_eq!(classify_turn(210), "sharp left");
        assert_eq!(classify_turn(230), "sharp left");
    }

    #[test]
    fn test_classify_turn_left() {
        assert_eq!(classify_turn(270), "left");
        assert_eq!(classify_turn(250), "left");
        assert_eq!(classify_turn(300), "left");
    }

    #[test]
    fn test_classify_turn_slight_left() {
        assert_eq!(classify_turn(310), "slight left");
        assert_eq!(classify_turn(330), "slight left");
    }

    #[test]
    fn test_classify_turn_all_angles_classified() {
        for angle in 0..=360u16 {
            let result = classify_turn(angle);
            assert!(
                [
                    "straight",
                    "slight right",
                    "right",
                    "sharp right",
                    "uturn",
                    "sharp left",
                    "left",
                    "slight left"
                ]
                .contains(&result),
                "Angle {} classified as unexpected '{}'",
                angle,
                result
            );
        }
    }

    #[test]
    fn test_bearing_reverse_is_180_off() {
        let fwd = compute_bearing(50.0, 4.0, 51.0, 5.0);
        let rev = compute_bearing(51.0, 5.0, 50.0, 4.0);
        let diff = bearing_diff(fwd, rev);
        assert!(
            (diff as i16 - 180).unsigned_abs() < 5,
            "Reverse bearing should differ by ~180, got diff={}",
            diff
        );
    }
}
