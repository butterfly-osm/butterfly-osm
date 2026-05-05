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
//!
//! ## Multi-country dispatch (#96)
//!
//! - **Forward**: caller may pin `country=<ISO2>` to bypass the
//!   routing classifier; otherwise the classifier returns a posterior
//!   and the handler picks the top country with a loaded shard. The
//!   control-plane single-shard pipeline runs against that shard.
//! - **Reverse**: lat/lon → [`crate::routing::country_for_point`]
//!   picks the most-specific bbox-containing country; falls back to
//!   the union of all loaded shards' nearest results if the point
//!   sits outside every known bbox.

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
use crate::routing::{CountryId, classify_country, supported_countries_for_point};
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
    // Per C3: enforce a CHARACTER-count limit (max 512 chars), not a
    // byte-count limit.
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

    // Pick the dispatch country: either pinned (must have a loaded
    // shard) or classifier top-1 with a loaded shard.
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
            let posterior = classify_country(&params.q);
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

    // Outcome enum for the blocking task: fold parser failures, admission
    // rejections, and successful pipelines into a single result so the
    // async caller can dispatch on each independently without nested
    // `Result<Result<...>>` gymnastics.
    enum Outcome {
        Ok(Vec<GeocodedResult>, Confidence),
        ParserFailed(String, &'static str),
        Admission(crate::control::AdmissionError),
    }

    // Per C6: geocode work is CPU-bound. Run on the blocking pool.
    // Inside the blocking task we (1) parse via the configured backend
    // (heuristic / neural — #98 Phase 1), (2) recompute the budget
    // against live shard statistics per #97 §1, (3) execute under the
    // control-plane hooks so admission/fanout/recombination metrics
    // fire, and finally (4) layer the GBDT rerank + action-threshold
    // pass on top of the control-plane results when a model is
    // configured. The rerank step is a no-op when `rerank_model` is
    // None.
    let q_text = params.q.clone();
    let state_clone = Arc::clone(&state);
    let join_result: Result<Outcome, _> = tokio::task::spawn_blocking(move || {
        let shard = state_clone
            .shards
            .get(&dispatch_country)
            .expect("dispatch_country verified to be loaded above");
        let mut parsed = match state_clone.parser.parse(&q_text, dispatch_country, shard) {
            Ok(p) => p,
            Err(e) => {
                return Outcome::ParserFailed(e.to_string(), state_clone.parser.name());
            }
        };
        // Collapse country_candidates to the dispatch country so the
        // executor's clean-query fast path can fire (`is_clean()`
        // requires len == 1).
        parsed.country_candidates = vec![(dispatch_country, 1.0)];
        let stats = shard.stats();
        parsed.execution_budget = compute_budget(&parsed, stats, state_clone.control.budget_policy);
        let raw = match execute_with_control(&parsed, shard, limit, &state_clone.control) {
            Ok(r) => r,
            Err(e) => return Outcome::Admission(e),
        };
        let (mut ranked, action) = apply_rerank(
            raw,
            &parsed,
            shard,
            state_clone.rerank_model.as_ref(),
            &state_clone.confidence_config,
        );
        // Tag every result with the country it came from.
        for r in &mut ranked {
            r.country = Some(dispatch_country.iso2());
        }
        Outcome::Ok(ranked, action)
    })
    .await;
    let (results, action) = match join_result {
        Ok(Outcome::Ok(r, a)) => (r, a),
        Ok(Outcome::ParserFailed(msg, name)) => {
            tracing::error!(error = %msg, parser = %name, "parser failed");
            return error_response(
                StatusCode::INTERNAL_SERVER_ERROR,
                "parser failure (see server logs)",
            );
        }
        Ok(Outcome::Admission(e)) => {
            return admission_rejection_response(&e.to_string());
        }
        Err(e) => {
            tracing::error!(error = %e, "spawn_blocking panicked in forward");
            return error_response(StatusCode::INTERNAL_SERVER_ERROR, "internal error");
        }
    };
    state
        .control
        .general
        .record_per_country_fanout(dispatch_country.iso2(), results.len() as u32);

    if accept_geojson(&headers) {
        geojson_response(&results, include_debug, action)
    } else {
        // On reject the rerank layer drops every candidate. Surface
        // `BELOW_THRESHOLD` on the response envelope so clients can
        // distinguish "no results" from "results below the action
        // threshold" without re-deriving from the score.
        let reason_codes = if matches!(action, Confidence::Reject) {
            Some(vec![crate::confidence::RC_BELOW_THRESHOLD.to_string()])
        } else {
            None
        };
        Json(ForwardResponse {
            query: params.q,
            country: dispatch_country.iso2(),
            confidence: action.as_str(),
            count: results.len(),
            results: results
                .iter()
                .map(|r| ForwardItem::from(r, include_debug))
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
    let results = match tokio::task::spawn_blocking(move || -> Vec<GeocodedResult> {
        // Reverse dispatch: instead of picking the smallest-bbox
        // country (which loses Aachen → DE because BE has the smaller
        // bbox), gather every country whose bbox contains the point
        // and try each loaded shard in order. Order: smallest-bbox
        // first as a heuristic to try the most-specific country
        // before fanning out. With a pinned country we still honour
        // the pin.
        let candidate_countries: Vec<CountryId> = if let Some(c) = pinned {
            vec![c]
        } else {
            let mut candidates = supported_countries_for_point(lat, lon);
            // Sort by bbox area ascending = smallest-bbox-first
            // heuristic. The smallest containing bbox is most
            // commonly the right country (typical case: the point
            // lives squarely inside the smaller of two overlapping
            // bboxes).
            let registry = crate::routing::Classifier::shipped().registry();
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

        let query_shard =
            |shard: &crate::shard::reader::Shard, c: CountryId| -> Vec<GeocodedResult> {
                let hits = shard.nearest_within(lat, lon, radius, limit);
                let mut results: Vec<GeocodedResult> = Vec::with_capacity(hits.len());
                for (rec, dist) in &hits {
                    let score = 1.0 - (dist / radius).clamp(0.0, 1.0) as f32;
                    let mut r = build_nearest_result(rec, score, reason::NEAREST);
                    r.country = Some(c.iso2());
                    results.push(r);
                }
                results
            };

        // Path A: walk every loaded shard for the candidate countries
        // (in order). Return the first non-empty within-radius set so
        // the targeted shard (smallest bbox) wins when it has results.
        for &c in &candidate_countries {
            if let Some(shard) = state_clone.shards.get(&c) {
                let results = query_shard(shard, c);
                if !results.is_empty() {
                    return results;
                }
            }
        }

        // Path B: no in-radius hit anywhere. Out-of-radius fallback —
        // try the candidate shards in order, then any other loaded
        // shard the bbox didn't list. Surface the closest one. This
        // handles the case where the bbox heuristic was wrong (e.g.
        // a point right on the border) AND the case where the point
        // sits outside every loaded bbox entirely.
        let mut tried: std::collections::HashSet<CountryId> =
            std::collections::HashSet::with_capacity(candidate_countries.len());
        let mut best: Option<(f64, GeocodedResult)> = None;
        // Helper closure body inlined twice to avoid sharing a mutable
        // capture: `tried` is read in the second loop for membership
        // and would conflict with the closure's mutable borrow.
        for &c in &candidate_countries {
            if let Some(shard) = state_clone.shards.get(&c) {
                tried.insert(c);
                if let Some(rec) = shard.nearest(lat, lon) {
                    let dist = haversine_m(lat, lon, rec.lat, rec.lon);
                    let mut r = build_nearest_result(&rec, 0.0, reason::NEAREST_OUT_OF_RADIUS);
                    r.country = Some(c.iso2());
                    if best.as_ref().map(|(d, _)| dist < *d).unwrap_or(true) {
                        best = Some((dist, r));
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
                let mut r = build_nearest_result(&rec, 0.0, reason::NEAREST_OUT_OF_RADIUS);
                r.country = Some(c.iso2());
                if best.as_ref().map(|(d, _)| dist < *d).unwrap_or(true) {
                    best = Some((dist, r));
                }
            }
        }
        if let Some((_, r)) = best {
            return vec![r];
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
    // The MVP server holds exactly one shard per `ServerState`. The
    // `shard_count` field is plumbed through anyway so that when
    // multi-shard support lands (#96) the wire format does not have to
    // change — operators that scrape /health for monitoring won't see
    // a breaking schema flip.
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

/// Build a 429 Too Many Requests response for a per-query admission
/// rejection (#97 over-budget admission). The pre-execution check is
/// not "Payload Too Large" (413); it's a rate/cost-based throttle, so
/// the right status is 429 with `Retry-After`.
fn admission_rejection_response(message: &str) -> Response {
    let mut resp = (
        StatusCode::TOO_MANY_REQUESTS,
        Json(ErrorResponse {
            error: message.to_string(),
        }),
    )
        .into_response();
    if let Ok(v) = header::HeaderValue::from_str("1") {
        resp.headers_mut().insert(header::RETRY_AFTER, v);
    }
    resp
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
    /// Envelope-level reason codes. Populated with `BELOW_THRESHOLD`
    /// on the reject path so clients can distinguish a true "no
    /// matches" empty result from "matches existed but the reranker
    /// suppressed them".
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

// NOTE on per-record source field: PR #173 Copilot finding §5 asks
// for a `source` field on this envelope mirroring the BFGS v4
// per-record source byte. The byte IS persisted at shard build time
// (handlers/executors honour it), but exposing it on the JSON
// envelope requires plumbing `SourceTag` through `GeocodedResult`,
// which lives in `geocoder/executor.rs` — outside this PR's
// territory. Tracked as a follow-up against the executor module.
// The README has been adjusted to describe the byte's audit role at
// shard level rather than promising an API field.

impl ForwardItem {
    fn from(r: &GeocodedResult, include_debug: bool) -> Self {
        Self {
            lat: r.lat,
            lon: r.lon,
            street: r.street.clone(),
            housenumber: r.housenumber.clone(),
            postcode: r.postcode.clone(),
            locality: r.locality.clone(),
            country: r.country.map(|s| s.to_string()),
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
    /// Sum of records across every loaded shard. Retained as
    /// `record_count` for backwards compatibility with single-shard
    /// scrapers; `total_records` is the canonical multi-shard name.
    record_count: u64,
    /// Number of country shards loaded.
    shard_count: u32,
    /// Sum of records across every loaded shard.
    total_records: u64,
    /// ISO2 codes for every shard the server has open.
    countries: Vec<String>,
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
            if let serde_json::Value::Object(ref mut m) = props {
                if let Some(c) = r.country {
                    m.insert(
                        "country".to_string(),
                        serde_json::Value::String(c.to_string()),
                    );
                }
                if include_debug {
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
