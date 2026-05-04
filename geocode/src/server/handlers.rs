//! HTTP request handlers.
//!
//! ## Concurrency model (C6)
//!
//! Geocode work is CPU-bound (binary searches over the inverted index,
//! string normalization, fuzzy similarity scoring). All such work runs
//! on the blocking thread pool via [`tokio::task::spawn_blocking`] so
//! it doesn't starve the async request thread pool. Tokio's default
//! blocking pool is 512 threads — large enough to absorb concurrent
//! geocode requests at the throughput target without backpressure.
//!
//! ## Content negotiation (C2)
//!
//! `application/json` is the default. Clients that send
//! `Accept: application/geo+json` get a GeoJSON `FeatureCollection`
//! body with the **correct** `Content-Type: application/geo+json`
//! response header. Axum's `Json(...)` responder always serves
//! `application/json`, so the GeoJSON path bypasses it and builds a
//! [`Response`] manually with the proper header.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};

use super::state::ServerState;
use crate::confidence::Confidence;
use crate::control::budget::compute_budget;
use crate::geocoder::executor::{
    GeocodedResult, apply_rerank, build_nearest_result, execute_with_control, reason,
};
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
    // Per C3: enforce a CHARACTER-count limit (max 512 chars), not a
    // byte-count limit. UTF-8 chars are 1-4 bytes; the previous
    // `String::len()` byte check let through long ASCII inputs and
    // rejected short multi-byte ones.
    if params.q.chars().count() > 512 {
        return error_response(StatusCode::BAD_REQUEST, "q too long (max 512 characters)");
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

    let include_debug = params
        .include
        .as_deref()
        .map(|s| s.split(',').any(|t| t.trim().eq_ignore_ascii_case("debug")))
        .unwrap_or(false);

    // Per C6: geocode work is CPU-bound. Run on the blocking pool so
    // we don't starve the async runtime thread pool. Inside the blocking
    // task we (1) parse, (2) recompute the budget against live shard
    // statistics per #97 §1, (3) execute under the control-plane hooks
    // so admission/fanout/recombination metrics fire, and finally
    // (4) layer the GBDT rerank + action-threshold pass on top of the
    // control-plane results when a model is configured (#96 §Confidence
    // Model). The rerank step is a no-op when `rerank_model` is None.
    let q_text = params.q.clone();
    let state_clone = Arc::clone(&state);
    let exec_result: Result<
        Result<(Vec<GeocodedResult>, Confidence), crate::control::AdmissionError>,
        _,
    > = tokio::task::spawn_blocking(move || {
        let mut parsed = parse_heuristic(&q_text, country);
        let stats = state_clone.shard.stats();
        parsed.execution_budget = compute_budget(&parsed, stats, state_clone.control.budget_policy);
        let raw = execute_with_control(&parsed, &state_clone.shard, limit, &state_clone.control)?;
        let (ranked, action) = apply_rerank(
            raw,
            &parsed,
            &state_clone.shard,
            state_clone.rerank_model.as_ref(),
            &state_clone.confidence_config,
        );
        Ok((ranked, action))
    })
    .await;
    let (results, action) = match exec_result {
        Ok(Ok(pair)) => pair,
        Ok(Err(e)) => {
            return error_response(StatusCode::PAYLOAD_TOO_LARGE, &e.to_string());
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panicked in forward");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };
    state
        .control
        .general
        .record_per_country_fanout(country.iso2(), results.len() as u32);

    if accept_geojson(&headers) {
        geojson_response(&results, include_debug, action)
    } else {
        Json(ForwardResponse {
            query: params.q,
            country: country.iso2(),
            confidence: action.as_str(),
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
    let lat = params.lat;
    let lon = params.lon;

    // Per C6: spatial query (R-tree traversal + haversine) is also
    // CPU-bound — `spawn_blocking` keeps the async pool free.
    let state_clone = Arc::clone(&state);
    let results = match tokio::task::spawn_blocking(move || -> Vec<GeocodedResult> {
        let hits = state_clone.shard.nearest_within(lat, lon, radius, limit);
        let mut results: Vec<GeocodedResult> = Vec::with_capacity(hits.len());
        for (rec, dist) in &hits {
            let score = 1.0 - (dist / radius).clamp(0.0, 1.0) as f32;
            results.push(build_nearest_result(rec, score, reason::NEAREST));
        }

        if results.is_empty()
            && let Some(rec) = state_clone.shard.nearest(lat, lon)
        {
            let _dist = haversine_m(lat, lon, rec.lat, rec.lon);
            results.push(build_nearest_result(
                &rec,
                0.0,
                reason::NEAREST_OUT_OF_RADIUS,
            ));
        }

        results
    })
    .await
    {
        Ok(r) => r,
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panicked in reverse");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    if accept_geojson(&headers) {
        geojson_response(&results, false, Confidence::Accept)
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

pub async fn health(State(state): State<Arc<ServerState>>) -> Response {
    Json(_health(state)).into_response()
}

fn _health(state: Arc<ServerState>) -> HealthResponse {
    HealthResponse {
        status: "ok",
        version: state.version,
        uptime_seconds: state.started_at.elapsed().as_secs(),
        record_count: state.shard.record_count() as u64,
    }
}

fn accept_geojson(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase().contains("geo+json"))
        .unwrap_or(false)
}

/// Build a GeoJSON response with the correct `application/geo+json`
/// Content-Type (per C2). Axum's `Json(...)` always serves
/// `application/json`, which violates RFC 7946 §12 for GeoJSON
/// responses.
fn geojson_response(
    results: &[GeocodedResult],
    include_debug: bool,
    confidence: Confidence,
) -> Response {
    let body = match serde_json::to_vec(&to_geojson(results, include_debug, confidence)) {
        Ok(b) => b,
        Err(e) => {
            tracing::error!(error = %e, "failed to serialize geojson");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };
    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/geo+json")
        .body(body.into())
        .unwrap_or_else(|_| {
            error_response(StatusCode::INTERNAL_SERVER_ERROR, "response build failed")
        })
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
    /// Action tier of the top-1 result per #96 §Confidence Model:
    /// `accept` / `caution` / `review` / `reject`. Always `accept`
    /// in the no-model fallback path.
    confidence: &'static str,
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
                Some(r.reason_codes.iter().map(|c| c.to_string()).collect())
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

fn to_geojson(
    results: &[GeocodedResult],
    include_debug: bool,
    confidence: Confidence,
) -> serde_json::Value {
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
            if include_debug && let serde_json::Value::Object(ref mut m) = props {
                m.insert(
                    "reason_codes".to_string(),
                    serde_json::Value::Array(
                        r.reason_codes
                            .iter()
                            .map(|s| serde_json::Value::String(s.to_string()))
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
        "confidence": confidence.as_str(),
        "features": features,
    })
}
