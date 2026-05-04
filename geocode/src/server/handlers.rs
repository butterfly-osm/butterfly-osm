//! HTTP request handlers.

use std::sync::Arc;

use axum::Json;
use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Response};
use serde::{Deserialize, Serialize};

use super::state::ServerState;
use crate::geocoder::executor::{GeocodedResult, execute};
use crate::parser::heuristic::parse_heuristic;
use crate::routing::CountryId;
use crate::shard::reader::haversine_m;

#[derive(Debug, Deserialize)]
pub struct ForwardParams {
    pub q: String,
    #[serde(default)]
    pub country: Option<String>,
    #[serde(default)]
    pub limit: Option<usize>,
    #[serde(default)]
    pub include: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct ReverseParams {
    pub lat: f64,
    pub lon: f64,
    #[serde(default)]
    pub radius_m: Option<f64>,
    #[serde(default)]
    pub limit: Option<usize>,
}

pub async fn forward(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<ForwardParams>,
) -> Response {
    if params.q.trim().is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "q must be non-empty");
    }
    if params.q.len() > 512 {
        return error_response(StatusCode::BAD_REQUEST, "q too long (max 512 bytes)");
    }
    let limit = params.limit.unwrap_or(5).clamp(1, 50);

    let country = match params.country.as_deref() {
        Some(s) => match CountryId::from_iso2(s) {
            Some(c) => c,
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "country must be an ISO 3166-1 alpha-2 code (MVP supports BE only)",
                );
            }
        },
        None => CountryId::BE,
    };
    if country != CountryId::BE {
        return error_response(
            StatusCode::BAD_REQUEST,
            "MVP supports BE only — multi-country routing tracked in #96",
        );
    }

    let parsed = parse_heuristic(&params.q, country);
    let results = execute(&parsed, &state.shard, limit);

    let include_debug = params
        .include
        .as_deref()
        .map(|s| s.split(',').any(|t| t.trim().eq_ignore_ascii_case("debug")))
        .unwrap_or(false);

    if accept_geojson(&headers) {
        Json(to_geojson(&results, include_debug)).into_response()
    } else {
        Json(ForwardResponse {
            query: params.q,
            country: country.iso2(),
            count: results.len(),
            results: results
                .iter()
                .map(|r| ForwardItem::from(r, include_debug))
                .collect(),
        })
        .into_response()
    }
}

pub async fn reverse(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<ReverseParams>,
) -> Response {
    if !(-90.0..=90.0).contains(&params.lat) {
        return error_response(StatusCode::BAD_REQUEST, "lat out of range [-90, 90]");
    }
    if !(-180.0..=180.0).contains(&params.lon) {
        return error_response(StatusCode::BAD_REQUEST, "lon out of range [-180, 180]");
    }
    let radius = params.radius_m.unwrap_or(200.0).clamp(1.0, 50_000.0);
    let limit = params.limit.unwrap_or(1).clamp(1, 50);

    let hits = state.shard.nearest_within(params.lat, params.lon, radius, limit);
    let mut results: Vec<GeocodedResult> = Vec::with_capacity(hits.len());
    for (rec, dist) in &hits {
        let r = GeocodedResult {
            lat: rec.lat,
            lon: rec.lon,
            street: rec.street.to_string(),
            housenumber: rec.housenumber.to_string(),
            postcode: rec.postcode.to_string(),
            locality: rec.locality.to_string(),
            score: 1.0 - (dist / radius).clamp(0.0, 1.0) as f32,
            reason_codes: vec!["NEAREST".to_string()],
        };
        results.push(r);
    }

    if results.is_empty()
        && let Some(rec) = state.shard.nearest(params.lat, params.lon)
    {
        let _dist = haversine_m(params.lat, params.lon, rec.lat, rec.lon);
        results.push(GeocodedResult {
            lat: rec.lat,
            lon: rec.lon,
            street: rec.street.to_string(),
            housenumber: rec.housenumber.to_string(),
            postcode: rec.postcode.to_string(),
            locality: rec.locality.to_string(),
            score: 0.0,
            reason_codes: vec!["NEAREST_OUT_OF_RADIUS".to_string()],
        });
    }

    if accept_geojson(&headers) {
        Json(to_geojson(&results, false)).into_response()
    } else {
        Json(ReverseResponse {
            count: results.len(),
            results: results
                .iter()
                .map(|r| ForwardItem::from(r, false))
                .collect(),
        })
        .into_response()
    }
}

pub async fn health(State(state): State<Arc<ServerState>>) -> Json<HealthResponse> {
    Json(HealthResponse {
        status: "ok",
        version: state.version,
        uptime_seconds: state.started_at.elapsed().as_secs(),
        record_count: state.shard.record_count() as u64,
    })
}

fn accept_geojson(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase().contains("geo+json"))
        .unwrap_or(false)
}

fn error_response(status: StatusCode, message: &str) -> Response {
    (
        status,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response()
}

#[derive(Debug, Serialize)]
struct ForwardResponse {
    query: String,
    country: &'static str,
    count: usize,
    results: Vec<ForwardItem>,
}

#[derive(Debug, Serialize)]
struct ReverseResponse {
    count: usize,
    results: Vec<ForwardItem>,
}

#[derive(Debug, Serialize)]
struct ForwardItem {
    lat: f64,
    lon: f64,
    street: String,
    housenumber: String,
    postcode: String,
    locality: String,
    score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_codes: Option<Vec<String>>,
}

impl ForwardItem {
    fn from(r: &GeocodedResult, include_debug: bool) -> Self {
        Self {
            lat: r.lat,
            lon: r.lon,
            street: r.street.clone(),
            housenumber: r.housenumber.clone(),
            postcode: r.postcode.clone(),
            locality: r.locality.clone(),
            score: r.score,
            reason_codes: if include_debug {
                Some(r.reason_codes.clone())
            } else {
                None
            },
        }
    }
}

#[derive(Debug, Serialize)]
struct HealthResponse {
    status: &'static str,
    version: &'static str,
    uptime_seconds: u64,
    record_count: u64,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

fn to_geojson(results: &[GeocodedResult], include_debug: bool) -> serde_json::Value {
    let features: Vec<serde_json::Value> = results
        .iter()
        .map(|r| {
            let mut props = serde_json::json!({
                "street": r.street,
                "housenumber": r.housenumber,
                "postcode": r.postcode,
                "locality": r.locality,
                "score": r.score,
            });
            if include_debug
                && let serde_json::Value::Object(ref mut m) = props
            {
                m.insert(
                    "reason_codes".to_string(),
                    serde_json::Value::Array(
                        r.reason_codes
                            .iter()
                            .map(|s| serde_json::Value::String(s.clone()))
                            .collect(),
                    ),
                );
            }
            serde_json::json!({
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [r.lon, r.lat],
                },
                "properties": props,
            })
        })
        .collect();
    serde_json::json!({
        "type": "FeatureCollection",
        "features": features,
    })
}
