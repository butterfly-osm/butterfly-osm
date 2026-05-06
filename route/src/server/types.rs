//! Shared types used by multiple API handler modules

use axum::Json;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::profile_abi::Mode;

/// Standard error response body
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
}

/// Directional role of a snap query (#197). The packed snap index
/// stores one EBG node per directed edge in the underlying NBG, so
/// the geometrically-closest sample to a coordinate may have valid
/// outgoing transitions but no valid incoming transitions in the
/// requested mode (and vice versa). Returning that node for the
/// "wrong" role would cause /route to 404 even though a route
/// exists. The server picks the per-mode role bitset to apply based
/// on this enum.
///
/// `Src` is the current Rust default (via `#[default]`) and the
/// `/nearest` HTTP default (via `#[serde(default)]` on the request
/// struct), matching what most callers want. `Either` is the legacy
/// *behaviour* — the unfiltered snap that was the only option before
/// #197 — kept available for callers that explicitly want it (e.g.
/// `/isochrone` from a single point, where that point is *always* a
/// source but historically went through the unfiltered snap).
/// Practical usage:
///   - `/route` source point → `Src`
///   - `/route` destination point → `Dst`
///   - `/nearest` defaults to `Src` (current default), with
///     `role=src|dst|either` as a query parameter.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize, ToSchema, Default)]
#[serde(rename_all = "lowercase")]
pub enum SnapRole {
    /// Source role: snap candidates must have at least one mode-valid
    /// outbound arc.
    #[default]
    Src,
    /// Destination role: snap candidates must have at least one
    /// mode-valid inbound arc.
    Dst,
    /// No role filter; behaves like the legacy snap.
    Either,
}

impl SnapRole {
    /// Resolve to the EBG-id-indexed bitset to use as the snap
    /// `role_filter`, or `None` for the unfiltered legacy behaviour.
    pub fn role_filter<'a>(
        &self,
        mode_data: &'a crate::server::state::ModeData,
    ) -> Option<&'a [u64]> {
        match self {
            SnapRole::Src => Some(&mode_data.has_outbound),
            SnapRole::Dst => Some(&mode_data.has_inbound),
            SnapRole::Either => None,
        }
    }
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
    // EBG node has geom_idx pointing to NBG edge index. Read the first
    // polyline vertex via the flat edge geometry (#155); falls back to
    // [0.0, 0.0] for empty polylines, matching the legacy behaviour.
    let polyline = state.edge_geom.polyline(node.geom_idx);
    if !polyline.is_empty() {
        let (lon, lat) = polyline.at(0);
        return [lon, lat];
    }
    [0.0, 0.0]
}
