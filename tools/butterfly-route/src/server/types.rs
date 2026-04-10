//! Shared types used by multiple API handler modules

use axum::Json;
use serde::Serialize;
use utoipa::ToSchema;

use crate::profile_abi::Mode;

/// Standard error response body
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

/// A waypoint with snapped location (used by table and trip responses)
#[derive(Debug, Serialize, ToSchema)]
pub struct Waypoint {
    /// Snapped location [lon, lat]
    pub location: [f64; 2],
    /// Name (empty for now)
    pub name: String,
}

/// Validate that a coordinate is within valid bounds.
pub fn validate_coord(lon: f64, lat: f64, label: &str) -> Result<(), String> {
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

/// Parse mode string to Mode using dynamic lookup in state's mode_lookup table
pub fn parse_mode(
    s: &str,
    mode_lookup: &std::collections::HashMap<String, u8>,
) -> Result<Mode, String> {
    let s_lower = s.to_lowercase();
    match mode_lookup.get(&s_lower) {
        Some(&idx) => Ok(Mode(idx)),
        None => {
            let mut available: Vec<&str> = mode_lookup.keys().map(|s| s.as_str()).collect();
            available.sort(); // deterministic error message
            Err(format!(
                "Invalid mode: {}. Available: {}.",
                s,
                available.join(", ")
            ))
        }
    }
}

/// Helper: return a 400 Bad Request JSON error response
pub fn bad_request(error: String) -> (axum::http::StatusCode, Json<ErrorResponse>) {
    (
        axum::http::StatusCode::BAD_REQUEST,
        Json(ErrorResponse { error }),
    )
}

/// Get the location (lon, lat) of an EBG node
pub fn get_node_location(state: &super::state::ServerState, node_id: u32) -> [f64; 2] {
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
