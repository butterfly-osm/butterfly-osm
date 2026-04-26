//! /nearest handler — snap to nearest road segments

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

use super::state::ServerState;
use super::types::{ErrorResponse, parse_mode, validate_coord};

// ============ Types ============

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
    /// Number of nearest results (default 1, max 100)
    #[serde(default = "default_number")]
    number: u32,
}

pub fn default_number() -> u32 {
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

// ============ Handler ============

/// Find nearest road segments to a coordinate
#[utoipa::path(
    get,
    path = "/nearest",
    tag = "Search",
    summary = "Find nearest road segments",
    description = "Snaps a coordinate to the nearest road segments accessible by the given transport mode.\nReturns up to `number` results sorted by distance.",
    params(
        ("lon" = f64, Query, description = "Longitude", example = 4.3517),
        ("lat" = f64, Query, description = "Latitude", example = 50.8503),
        ("mode" = String, Query, description = "Transport mode (e.g. car, bike, foot — depends on available models)", example = "car"),
        ("number" = Option<u32>, Query, description = "Number of results (default 1, max 100)", example = 5),
    ),
    responses(
        (status = 200, description = "Nearest roads found", body = NearestResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
pub async fn nearest_handler(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<NearestRequest>,
) -> impl IntoResponse {
    if let Err(e) = validate_coord(req.lon, req.lat, "query point") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
    }
    if req.number == 0 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "number must be at least 1".into(),
            }),
        )
            .into_response();
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

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let k = req.number as usize;

    let results = state
        .snap_index
        .snap_k_with_info(req.lon, req.lat, mode.0, k);

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
