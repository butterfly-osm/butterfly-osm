//! HTTP request handlers (#205).
//!
//! ## Pipeline
//!
//! `/geocode` (forward) runs `recall_then_rerank`. The handler:
//!
//! 1. Validates input.
//! 2. Picks a dispatch country (pinned via `country=` or top-1 of
//!    `state.classifier.classify(q)` overlapping with loaded shards).
//! 3. Runs `state.parser.signals(q, &shard)` for a [`TaggerSignals`]
//!    bundle (heuristic backend yields neutral signals; neural
//!    backend yields per-byte BIO logits + posterior).
//! 4. Calls
//!    [`recall_then_rerank`](crate::geocoder::executor::recall_then_rerank)
//!    with the dispatch country's recall index + the cross-shard
//!    [`Reranker`].
//!
//! ## Concurrency model
//!
//! Geocode work is CPU-bound (FST traversal, GBDT inference). All
//! such work runs on the blocking thread pool via
//! [`tokio::task::spawn_blocking`] so it doesn't starve the async
//! request thread pool.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::{IntoResponse, Json, Response};
use serde::{Deserialize, Serialize};

use super::state::ServerState;
use crate::confidence::Confidence;
use crate::geocoder::executor::recall_then_rerank;
use crate::geocoder::recall::{RecallBudget, TaggerSignals};
use crate::geocoder::rerank::RankedResult;
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
    #[serde(default)]
    pub country: Option<String>,
}

pub async fn forward(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    Query(params): Query<ForwardParams>,
) -> Response {
    if params.q.trim().is_empty() {
        return error_response(StatusCode::BAD_REQUEST, "q must be non-empty");
    }
    if params.q.chars().count() > 512 {
        return error_response(StatusCode::BAD_REQUEST, "q too long (max 512 characters)");
    }
    let limit = params.limit.unwrap_or(5).clamp(1, 50);

    let pinned_country = match params.country.as_deref() {
        Some(s) => match CountryId::from_iso2(s) {
            Some(c) => Some(c),
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "country must be a 2-letter ISO 3166-1 alpha-2 code (e.g. BE, FR, JP, US)",
                );
            }
        },
        None => None,
    };

    let dispatch_country = match pinned_country {
        Some(c) => {
            if !state.shards.contains_key(&c) {
                return error_response(
                    StatusCode::SERVICE_UNAVAILABLE,
                    &format!(
                        "no shard loaded for country={} (loaded: {})",
                        c.iso2(),
                        state.loaded_countries().join(", ")
                    ),
                );
            }
            c
        }
        None => {
            let posterior = state.classifier.classify(&params.q);
            match state.pick_shard(&posterior) {
                Some((c, _)) => c,
                None => {
                    return error_response(StatusCode::SERVICE_UNAVAILABLE, "no shards loaded");
                }
            }
        }
    };

    let include_debug = params
        .include
        .as_deref()
        .map(|s| s.split(',').any(|t| t.trim().eq_ignore_ascii_case("debug")))
        .unwrap_or(false);

    let q_text = params.q.clone();
    let state_clone = Arc::clone(&state);
    let join: Result<(Vec<RankedResult>, &'static str), _> =
        tokio::task::spawn_blocking(move || -> (Vec<RankedResult>, &'static str) {
            let shard = state_clone
                .shards
                .get(&dispatch_country)
                .expect("dispatch_country verified to be loaded above")
                .clone();

            // Tagger signals — neutral on the heuristic backend, real
            // on the neural backend. Failure here is upgraded to a
            // logged event but does not fail the whole request; we
            // fall back to neutral signals so the user still gets a
            // best-effort recall + rerank pass.
            let signals = match state_clone.parser.signals(&q_text, &shard) {
                Ok(s) => s,
                Err(e) => {
                    tracing::warn!(
                        error = %e,
                        parser = state_clone.parser.name(),
                        "parser failed; falling back to neutral signals"
                    );
                    TaggerSignals::default()
                }
            };

            // Per-shard adaptive budget — sized off the shard's
            // p95 posting-list size.
            let budget = match state_clone.recaller.stats_for(dispatch_country) {
                Some(s) => RecallBudget::default().adapt_to_stats(s.p95_postings),
                None => RecallBudget::default(),
            };

            let shard_for = {
                let shards = state_clone.shards.clone();
                move |c: CountryId| shards.get(&c).cloned()
            };
            let results = recall_then_rerank(
                &q_text,
                &signals,
                &[dispatch_country],
                &state_clone.recaller,
                &state_clone.reranker,
                &budget,
                shard_for,
                limit,
            );
            (results, dispatch_country.iso2())
        })
        .await;

    let (results, country_code) = match join {
        Ok(t) => t,
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panicked in forward");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };

    let action = results
        .first()
        .map(|r| r.action)
        .unwrap_or(Confidence::Reject);

    state
        .general_metrics
        .record_per_country_fanout(country_code, results.len() as u32);

    if accept_geojson(&headers) {
        geojson_response(&results, country_code, include_debug, action)
    } else {
        let reason_codes = if matches!(action, Confidence::Reject) && !results.is_empty() {
            Some(vec![crate::confidence::RC_BELOW_THRESHOLD.to_string()])
        } else {
            None
        };
        Json(ForwardResponse {
            query: params.q,
            country: country_code,
            confidence: action.as_str(),
            count: results.len(),
            results: results
                .iter()
                .map(|r| ForwardItem::from(r, country_code, include_debug))
                .collect(),
            reason_codes,
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

    let pinned: Option<CountryId> = match params.country.as_deref() {
        Some(s) => match CountryId::from_iso2(s) {
            Some(c) => Some(c),
            None => {
                return error_response(
                    StatusCode::BAD_REQUEST,
                    "country must be a 2-letter ISO 3166-1 alpha-2 code (e.g. BE, FR, JP, US)",
                );
            }
        },
        None => None,
    };
    if let Some(c) = pinned
        && !state.shards.contains_key(&c)
    {
        return error_response(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!(
                "no shard loaded for country={} (loaded: {})",
                c.iso2(),
                state.loaded_countries().join(", ")
            ),
        );
    }

    let state_clone = Arc::clone(&state);
    let results = match tokio::task::spawn_blocking(move || -> Vec<(RankedResult, &'static str)> {
        let candidate_countries: Vec<CountryId> = if let Some(c) = pinned {
            vec![c]
        } else {
            let mut candidates = state_clone.classifier.supported_for_point(lat, lon);
            let registry = state_clone.classifier.registry();
            candidates.sort_by(|a, b| {
                let area = |c: &CountryId| {
                    registry
                        .get(*c)
                        .map(|p| p.bbox.area_deg2())
                        .unwrap_or(f64::INFINITY)
                };
                area(a)
                    .partial_cmp(&area(b))
                    .unwrap_or(std::cmp::Ordering::Equal)
            });
            candidates
        };

        let mk_result = |rec: &crate::shard::reader::ShardRecord, c: CountryId, dist: f64| {
            let score = 1.0 - (dist / radius).clamp(0.0, 1.0) as f32;
            RankedResult {
                country: c,
                address_id: rec.id as u64,
                record_id: rec.id,
                source: rec.source,
                lat: rec.lat,
                lon: rec.lon,
                street: rec.street.to_string(),
                housenumber: rec.housenumber.to_string(),
                postcode: rec.postcode.to_string(),
                locality: rec.locality.to_string(),
                score,
                features: crate::geocoder::rerank::RerankFeatures::default(),
                action: Confidence::Accept,
                reason_codes: vec!["NEAREST"],
            }
        };

        for &c in &candidate_countries {
            if let Some(shard) = state_clone.shards.get(&c) {
                let hits = shard.nearest_within(lat, lon, radius, limit);
                if !hits.is_empty() {
                    return hits
                        .iter()
                        .map(|(rec, dist)| (mk_result(rec, c, *dist), c.iso2()))
                        .collect();
                }
            }
        }
        // Out-of-radius fallback.
        let mut tried: std::collections::HashSet<CountryId> =
            std::collections::HashSet::with_capacity(candidate_countries.len());
        let mut best: Option<(f64, RankedResult, &'static str)> = None;
        for &c in &candidate_countries {
            if let Some(shard) = state_clone.shards.get(&c) {
                tried.insert(c);
                if let Some(rec) = shard.nearest(lat, lon) {
                    let dist = haversine_m(lat, lon, rec.lat, rec.lon);
                    let mut r = mk_result(&rec, c, dist);
                    r.score = 0.0;
                    r.reason_codes = vec!["NEAREST_OUT_OF_RADIUS"];
                    if best.as_ref().map(|(d, _, _)| dist < *d).unwrap_or(true) {
                        best = Some((dist, r, c.iso2()));
                    }
                }
            }
        }
        for (&c, shard) in &state_clone.shards {
            if tried.contains(&c) {
                continue;
            }
            if let Some(rec) = shard.nearest(lat, lon) {
                let dist = haversine_m(lat, lon, rec.lat, rec.lon);
                let mut r = mk_result(&rec, c, dist);
                r.score = 0.0;
                r.reason_codes = vec!["NEAREST_OUT_OF_RADIUS"];
                if best.as_ref().map(|(d, _, _)| dist < *d).unwrap_or(true) {
                    best = Some((dist, r, c.iso2()));
                }
            }
        }
        if let Some((_, r, code)) = best {
            return vec![(r, code)];
        }
        Vec::new()
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
        let pairs: Vec<RankedResult> = results.iter().map(|(r, _)| r.clone()).collect();
        let country = results.first().map(|(_, c)| *c).unwrap_or("");
        geojson_response(&pairs, country, false, Confidence::Accept)
    } else {
        let items: Vec<ForwardItem> = results
            .iter()
            .map(|(r, c)| ForwardItem::from(r, c, false))
            .collect();
        Json(ReverseResponse {
            count: items.len(),
            results: items,
        })
        .into_response()
    }
}

pub async fn health(State(state): State<Arc<ServerState>>) -> Response {
    Json(_health(state)).into_response()
}

fn _health(state: Arc<ServerState>) -> HealthResponse {
    let countries: Vec<String> = state
        .loaded_countries()
        .into_iter()
        .map(|s| s.to_string())
        .collect();
    let total_records = state.total_record_count() as u64;
    HealthResponse {
        status: "ok",
        version: state.version,
        uptime_seconds: state.started_at.elapsed().as_secs(),
        record_count: total_records,
        shard_count: countries.len() as u32,
        total_records,
        countries,
    }
}

fn accept_geojson(headers: &HeaderMap) -> bool {
    headers
        .get(header::ACCEPT)
        .and_then(|v| v.to_str().ok())
        .map(|s| s.to_ascii_lowercase().contains("geo+json"))
        .unwrap_or(false)
}

fn geojson_response(
    results: &[RankedResult],
    country_code: &str,
    include_debug: bool,
    confidence: Confidence,
) -> Response {
    let body = match serde_json::to_vec(&to_geojson(
        results,
        country_code,
        include_debug,
        confidence,
    )) {
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
    confidence: &'static str,
    count: usize,
    results: Vec<ForwardItem>,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_codes: Option<Vec<String>>,
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
    #[serde(skip_serializing_if = "Option::is_none")]
    country: Option<String>,
    score: f32,
    #[serde(skip_serializing_if = "Option::is_none")]
    reason_codes: Option<Vec<String>>,
}

impl ForwardItem {
    fn from(r: &RankedResult, country: &str, include_debug: bool) -> Self {
        Self {
            lat: r.lat,
            lon: r.lon,
            street: r.street.clone(),
            housenumber: r.housenumber.clone(),
            postcode: r.postcode.clone(),
            locality: r.locality.clone(),
            country: Some(country.to_string()),
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
    shard_count: u32,
    total_records: u64,
    countries: Vec<String>,
}

#[derive(Debug, Serialize)]
struct ErrorResponse {
    error: String,
}

fn to_geojson(
    results: &[RankedResult],
    country_code: &str,
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
                "country": country_code,
            });
            if let serde_json::Value::Object(ref mut m) = props
                && include_debug
            {
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
