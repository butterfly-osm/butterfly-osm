//! Route handlers

use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::Json as ResponseJson,
};
use butterfly_extract::{Extractor, TileTelemetry, GlobalPercentiles, CanonicalNodeProbe};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::sync::{Arc, Mutex};
use utoipa::ToSchema;

/// Server state containing telemetry data
#[derive(Clone)]
pub struct AppState {
    pub extractor: Arc<Mutex<Extractor>>,
}

/// Telemetry query parameters with bbox filtering
#[derive(Debug, Deserialize, ToSchema)]
pub struct TelemetryQuery {
    /// Minimum latitude for bounding box filter
    #[serde(rename = "min_lat")]
    pub min_lat: Option<f64>,
    /// Maximum latitude for bounding box filter  
    #[serde(rename = "max_lat")]
    pub max_lat: Option<f64>,
    /// Minimum longitude for bounding box filter
    #[serde(rename = "min_lon")]
    pub min_lon: Option<f64>,
    /// Maximum longitude for bounding box filter
    #[serde(rename = "max_lon")]
    pub max_lon: Option<f64>,
    /// Include global percentiles in response
    #[serde(default)]
    pub include_global: bool,
}

/// Telemetry API response
#[derive(Debug, Serialize, ToSchema)]
pub struct TelemetryResponse {
    /// Total number of tiles returned
    pub total_tiles: usize,
    /// Query bounding box (if provided)
    pub bbox: Option<BboxInfo>,
    /// Global percentiles (if requested)
    pub global_percentiles: Option<GlobalPercentiles>,
    /// Tile telemetry data
    pub tiles: Vec<TileTelemetry>,
}

/// Bounding box information
#[derive(Debug, Serialize, ToSchema)]
pub struct BboxInfo {
    pub min_lat: f64,
    pub max_lat: f64,
    pub min_lon: f64,
    pub max_lon: f64,
}

/// Error response structure
#[derive(Debug, Serialize, ToSchema)]
pub struct ErrorResponse {
    pub error: String,
    pub code: u16,
}

/// GET /telemetry - Spatial density telemetry with bbox filtering
#[utoipa::path(
    get,
    path = "/telemetry",
    params(
        ("min_lat" = Option<f64>, Query, description = "Minimum latitude for bbox filter"),
        ("max_lat" = Option<f64>, Query, description = "Maximum latitude for bbox filter"),
        ("min_lon" = Option<f64>, Query, description = "Minimum longitude for bbox filter"),
        ("max_lon" = Option<f64>, Query, description = "Maximum longitude for bbox filter"),
        ("include_global" = Option<bool>, Query, description = "Include global percentiles")
    ),
    responses(
        (status = 200, description = "Telemetry data retrieved successfully", body = TelemetryResponse),
        (status = 400, description = "Invalid query parameters", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    tag = "telemetry"
)]
pub async fn get_telemetry(
    State(state): State<AppState>,
    Query(params): Query<TelemetryQuery>,
) -> Result<ResponseJson<TelemetryResponse>, (StatusCode, ResponseJson<ErrorResponse>)> {
    // Validate bbox parameters
    if let Err(validation_error) = validate_bbox_params(&params) {
        return Err((
            StatusCode::BAD_REQUEST,
            ResponseJson(ErrorResponse {
                error: validation_error,
                code: 400,
            }),
        ));
    }
    
    // Get telemetry data based on bbox filtering
    let extractor = state.extractor.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ResponseJson(ErrorResponse {
                error: "Failed to acquire extractor lock".to_string(),
                code: 500,
            }),
        )
    })?;
    
    let tiles = match (params.min_lat, params.max_lat, params.min_lon, params.max_lon) {
        (Some(min_lat), Some(max_lat), Some(min_lon), Some(max_lon)) => {
            extractor.get_telemetry_for_bbox(min_lat, max_lat, min_lon, max_lon)
        }
        _ => extractor.get_telemetry(),
    };
    
    // Get global percentiles if requested
    let global_percentiles = if params.include_global {
        Some(extractor.get_global_percentiles())
    } else {
        None
    };
    
    // Create bbox info if all parameters provided
    let bbox = match (params.min_lat, params.max_lat, params.min_lon, params.max_lon) {
        (Some(min_lat), Some(max_lat), Some(min_lon), Some(max_lon)) => {
            Some(BboxInfo {
                min_lat,
                max_lat,
                min_lon,
                max_lon,
            })
        }
        _ => None,
    };
    
    let response = TelemetryResponse {
        total_tiles: tiles.len(),
        bbox,
        global_percentiles,
        tiles,
    };
    
    Ok(ResponseJson(response))
}

/// Validate bbox query parameters
fn validate_bbox_params(params: &TelemetryQuery) -> Result<(), String> {
    // Check if bbox parameters are consistent
    let has_any_bbox = params.min_lat.is_some() || 
                       params.max_lat.is_some() || 
                       params.min_lon.is_some() || 
                       params.max_lon.is_some();
    
    if has_any_bbox {
        // If any bbox parameter is provided, all must be provided
        match (params.min_lat, params.max_lat, params.min_lon, params.max_lon) {
            (Some(min_lat), Some(max_lat), Some(min_lon), Some(max_lon)) => {
                // Validate lat/lon ranges
                if !(-90.0..=90.0).contains(&min_lat) {
                    return Err("min_lat must be between -90 and 90".to_string());
                }
                if !(-90.0..=90.0).contains(&max_lat) {
                    return Err("max_lat must be between -90 and 90".to_string());
                }
                if !(-180.0..=180.0).contains(&min_lon) {
                    return Err("min_lon must be between -180 and 180".to_string());
                }
                if !(-180.0..=180.0).contains(&max_lon) {
                    return Err("max_lon must be between -180 and 180".to_string());
                }
                
                // Validate bbox consistency
                if min_lat >= max_lat {
                    return Err("min_lat must be less than max_lat".to_string());
                }
                if min_lon >= max_lon {
                    return Err("min_lon must be less than max_lon".to_string());
                }
            }
            _ => {
                return Err("When using bbox filtering, all parameters (min_lat, max_lat, min_lon, max_lon) must be provided".to_string());
            }
        }
    }
    
    Ok(())
}

/// Route endpoint placeholder (legacy)
pub async fn route_handler() -> ResponseJson<Value> {
    ResponseJson(serde_json::json!({"message": "Route endpoint not implemented"}))
}

/// Probe/snap query parameters for canonical mapping validation
#[derive(Debug, Deserialize, ToSchema)]
pub struct ProbeSnapQuery {
    /// Latitude for snap probe
    #[serde(rename = "lat")]
    pub lat: f64,
    /// Longitude for snap probe
    #[serde(rename = "lon")]
    pub lon: f64,
    /// Original node ID to validate (optional)
    #[serde(rename = "node_id")]
    pub node_id: Option<i64>,
    /// Maximum search radius in meters
    #[serde(rename = "radius", default = "default_probe_radius")]
    pub radius: f64,
}

fn default_probe_radius() -> f64 {
    100.0 // 100 meter default search radius
}

/// Probe/snap response for canonical mapping validation
#[derive(Debug, Serialize, ToSchema)]
pub struct ProbeSnapResponse {
    /// Query coordinates
    pub query_lat: f64,
    pub query_lon: f64,
    /// Search radius used
    pub search_radius: f64,
    /// Canonical mapping results
    pub canonical_nodes: Vec<CanonicalNodeProbe>,
    /// Validation status
    pub validation_status: ValidationStatus,
    /// Distance to nearest canonical node
    pub nearest_distance: Option<f64>,
}


/// Validation status for probe results
#[derive(Debug, Serialize, ToSchema)]
pub enum ValidationStatus {
    /// Canonical mapping found and valid
    Valid,
    /// No canonical nodes found within radius
    NotFound,
    /// Mapping found but suspicious (large distance)
    Suspicious,
    /// Error in validation process
    Error(String),
}

/// GET /probe/snap - Snap probe endpoint for QA/debugging canonical mapping
#[utoipa::path(
    get,
    path = "/probe/snap",
    params(
        ("lat" = f64, Query, description = "Latitude for snap probe"),
        ("lon" = f64, Query, description = "Longitude for snap probe"),
        ("node_id" = Option<i64>, Query, description = "Original node ID to validate"),
        ("radius" = Option<f64>, Query, description = "Maximum search radius in meters (default: 100)")
    ),
    responses(
        (status = 200, description = "Probe results retrieved successfully", body = ProbeSnapResponse),
        (status = 400, description = "Invalid query parameters", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    tag = "probe"
)]
pub async fn probe_snap(
    State(state): State<AppState>,
    Query(params): Query<ProbeSnapQuery>,
) -> Result<ResponseJson<ProbeSnapResponse>, (StatusCode, ResponseJson<ErrorResponse>)> {
    // Validate coordinates
    if !(-90.0..=90.0).contains(&params.lat) {
        return Err((
            StatusCode::BAD_REQUEST,
            ResponseJson(ErrorResponse {
                error: "lat must be between -90 and 90".to_string(),
                code: 400,
            }),
        ));
    }

    if !(-180.0..=180.0).contains(&params.lon) {
        return Err((
            StatusCode::BAD_REQUEST,
            ResponseJson(ErrorResponse {
                error: "lon must be between -180 and 180".to_string(),
                code: 400,
            }),
        ));
    }

    if params.radius <= 0.0 || params.radius > 10000.0 {
        return Err((
            StatusCode::BAD_REQUEST,
            ResponseJson(ErrorResponse {
                error: "radius must be between 0 and 10000 meters".to_string(),
                code: 400,
            }),
        ));
    }

    // Perform canonical mapping probe
    let extractor = state.extractor.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ResponseJson(ErrorResponse {
                error: "Failed to acquire extractor lock".to_string(),
                code: 500,
            }),
        )
    })?;
    
    let canonical_nodes = extractor.probe_canonical_mapping(
        params.lat,
        params.lon,
        params.radius,
        params.node_id,
    );

    // Determine validation status
    let validation_status = if canonical_nodes.is_empty() {
        ValidationStatus::NotFound
    } else {
        let nearest_distance = canonical_nodes.iter()
            .map(|node| node.distance)
            .min_by(|a, b| a.partial_cmp(b).unwrap())
            .unwrap_or(f64::INFINITY);

        if nearest_distance > params.radius * 0.8 {
            ValidationStatus::Suspicious
        } else {
            ValidationStatus::Valid
        }
    };

    let nearest_distance = canonical_nodes.iter()
        .map(|node| node.distance)
        .min_by(|a, b| a.partial_cmp(b).unwrap());

    let response = ProbeSnapResponse {
        query_lat: params.lat,
        query_lon: params.lon,
        search_radius: params.radius,
        canonical_nodes,
        validation_status,
        nearest_distance,
    };

    Ok(ResponseJson(response))
}

// ==== M3.4 - Graph Debug APIs ====

/// GET /graph/stats - Graph statistics endpoint
#[utoipa::path(
    get,
    path = "/graph/stats",
    responses(
        (status = 200, description = "Graph statistics retrieved successfully", body = Value),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    tag = "graph"
)]
pub async fn graph_stats(
    State(state): State<AppState>,
) -> Result<ResponseJson<Value>, (StatusCode, ResponseJson<ErrorResponse>)> {
    let mut extractor = state.extractor.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ResponseJson(ErrorResponse {
                error: "Failed to acquire extractor lock".to_string(),
                code: 500,
            }),
        )
    })?;

    let stats = extractor.get_graph_stats();
    Ok(ResponseJson(stats))
}

/// GET /graph/edge/{id} - Edge details endpoint
#[utoipa::path(
    get,
    path = "/graph/edge/{id}",
    params(
        ("id" = String, Path, description = "Edge ID in format 'start_end'")
    ),
    responses(
        (status = 200, description = "Edge details retrieved successfully", body = Value),
        (status = 404, description = "Edge not found", body = ErrorResponse),
        (status = 500, description = "Internal server error", body = ErrorResponse)
    ),
    tag = "graph"
)]
pub async fn graph_edge(
    State(state): State<AppState>,
    Path(edge_id): Path<String>,
) -> Result<ResponseJson<Value>, (StatusCode, ResponseJson<ErrorResponse>)> {
    let extractor = state.extractor.lock().map_err(|_| {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            ResponseJson(ErrorResponse {
                error: "Failed to acquire extractor lock".to_string(),
                code: 500,
            }),
        )
    })?;

    match extractor.get_edge_details(&edge_id) {
        Some(edge_details) => Ok(ResponseJson(edge_details)),
        None => Err((
            StatusCode::NOT_FOUND,
            ResponseJson(ErrorResponse {
                error: format!("Edge '{}' not found", edge_id),
                code: 404,
            }),
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_bbox_validation() {
        // Valid bbox
        let valid_params = TelemetryQuery {
            min_lat: Some(52.0),
            max_lat: Some(53.0),
            min_lon: Some(13.0),
            max_lon: Some(14.0),
            include_global: false,
        };
        assert!(validate_bbox_params(&valid_params).is_ok());
        
        // Invalid: min >= max
        let invalid_params = TelemetryQuery {
            min_lat: Some(53.0),
            max_lat: Some(52.0),
            min_lon: Some(13.0),
            max_lon: Some(14.0),
            include_global: false,
        };
        assert!(validate_bbox_params(&invalid_params).is_err());
        
        // Invalid: out of range
        let invalid_range = TelemetryQuery {
            min_lat: Some(-95.0),
            max_lat: Some(53.0),
            min_lon: Some(13.0),
            max_lon: Some(14.0),
            include_global: false,
        };
        assert!(validate_bbox_params(&invalid_range).is_err());
        
        // Invalid: incomplete bbox
        let incomplete_params = TelemetryQuery {
            min_lat: Some(52.0),
            max_lat: None,
            min_lon: Some(13.0),
            max_lon: Some(14.0),
            include_global: false,
        };
        assert!(validate_bbox_params(&incomplete_params).is_err());
        
        // Valid: no bbox
        let no_bbox = TelemetryQuery {
            min_lat: None,
            max_lat: None,
            min_lon: None,
            max_lon: None,
            include_global: true,
        };
        assert!(validate_bbox_params(&no_bbox).is_ok());
    }
}
