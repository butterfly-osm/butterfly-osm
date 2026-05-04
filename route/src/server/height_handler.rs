//! /height handler — elevation lookup from SRTM DEM tiles

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
    response::IntoResponse,
};
use std::sync::Arc;

use super::regions::RegionsState;
use super::types::ErrorResponse;

/// Query elevation for coordinates using SRTM data
#[utoipa::path(
    get,
    path = "/height",
    tag = "Elevation",
    summary = "Look up elevation for coordinates",
    description = "Returns elevation in meters above sea level for each coordinate using SRTM DEM data.\nCoordinates are passed as pipe-separated `lon,lat` pairs (Valhalla convention).\n\nReturns `null` elevation for coordinates outside SRTM coverage.",
    params(
        ("coordinates" = String, Query, description = "Pipe-separated lon,lat pairs", example = "4.3517,50.8503|4.4017,50.8603"),
    ),
    responses(
        (status = 200, description = "Elevations returned", body = super::elevation::HeightResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
        (status = 503, description = "Elevation data not loaded", body = ErrorResponse),
    )
)]
pub async fn height_handler(
    State(regions): State<Arc<RegionsState>>,
    Query(req): Query<super::elevation::HeightRequest>,
) -> impl IntoResponse {
    // Elevation data (SRTM tiles) is geographically global and lives
    // on the primary region; height queries don't need per-region
    // dispatch.
    let state = regions.primary();
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
                .into_response();
        }
    };

    match super::elevation::handle_height_request(elevation, &req) {
        Ok(resp) => Json(resp).into_response(),
        Err(e) => (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response(),
    }
}
