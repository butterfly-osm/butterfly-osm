//! /table handler — distance/duration matrix computation (bulk matrices live on the Flight `matrix` action)

use axum::{
    Json,
    extract::State,
    http::StatusCode,
    response::{IntoResponse, Response},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

use crate::matrix::MatrixPlan;
use crate::matrix::neighbors::{
    RadiusParam, auto_radius_km, build_neighbors, build_neighbors_per_origin, parse_radius,
};
use crate::model::types::Mode;

use super::query_context::QueryContext;
use super::regions::RegionsState;
use super::state::ServerState;
use super::types::{ErrorResponse, SnapRole, Waypoint, parse_mode, validate_coord};

/// #594: response header naming the matrix plan `/table` actually ran —
/// `bucket`, `phast_fwd` or `phast_rev` (see [`MatrixPlan`]). The value comes
/// from the engine that produced the served duration grid (a distance-only
/// request reports its distance run), so a client can assert the SELECTION per
/// request instead of inferring it from wall clock. Free: one static header on
/// a response that already exists.
pub const MATRIX_PLAN_HEADER: &str = "x-butterfly-matrix-plan";

// ============ Types ============

/// POST request for table computation
///
/// `deny_unknown_fields` (#415): reject unrecognised parameters with 400
/// rather than silently ignoring them. Without this, a client sending
/// `max_minutes` (or a typo) to a server build that predates the field would
/// get a full unbounded matrix back with no error — silently wrong results.
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)]
pub struct TablePostRequest {
    /// Source coordinates [[lon, lat], ...]
    #[schema(example = json!([[4.3517, 50.8503], [4.4017, 50.8603]]))]
    pub origins: Vec<[f64; 2]>,
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
    /// Exclude road types: comma-separated list of "toll", "ferry", "motorway"
    #[serde(default)]
    pub exclude: Option<String>,
    /// Avoid polygon(s) as JSON array of coordinate rings
    #[serde(default)]
    pub avoid_polygons: Option<String>,
    /// Optional Euclidean pre-filter radius in kilometres.
    /// Accepts a positive number, the string "auto" (server-computed p95 × 1.1),
    /// or null/0 to disable. Pairs beyond the radius are returned as null.
    #[serde(default)]
    pub radius_km: Option<serde_json::Value>,
    /// Optional drive-time bound in minutes (#415). When set, only cells whose
    /// travel time ≤ `max_minutes` are returned (others are null), and the
    /// search itself early-stops at the bound so compute is proportional to
    /// the time-reachable region rather than the full source×target product.
    /// Exact: returned durations/distances are identical to the unbounded
    /// matrix filtered to ≤ `max_minutes`. Orthogonal to `radius_km`.
    #[serde(default)]
    pub max_minutes: Option<f64>,
    /// Uncertainty bands (#521): "bands" adds durations_best/durations_worst
    /// (best = nights/free-flow, worst = weekday peaks; hidden weight sets).
    /// Explicit opt-in: runs the matrix three times. car only.
    #[serde(default)]
    pub uncertainty: Option<String>,
}

pub fn default_annotations() -> String {
    "duration".to_string()
}

/// Convert an optional `max_minutes` request field into a CCH time-weight
/// threshold in seconds (the unit of the time metric, post-#297). Returns
/// `Ok(None)` when unset, an error on out-of-range values. `u32::MAX` stays
/// reserved as the "unbounded" sentinel inside the matrix engine, so the
/// 24 h cap keeps any real threshold far below it.
pub fn parse_max_minutes(max_minutes: Option<f64>) -> Result<Option<u32>, String> {
    match max_minutes {
        None => Ok(None),
        Some(m) => {
            if !m.is_finite() || m <= 0.0 {
                return Err(format!("max_minutes must be a positive number, got {m}"));
            }
            if m > 1440.0 {
                return Err(format!("max_minutes must be ≤ 1440 (24 h), got {m}"));
            }
            // ceil so a cell exactly at the bound is included.
            Ok(Some((m * 60.0).ceil() as u32))
        }
    }
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
    pub origins: Option<Vec<Waypoint>>,
    /// Destination waypoints with snapped locations
    #[serde(skip_serializing_if = "Option::is_none")]
    pub destinations: Option<Vec<Waypoint>>,
    /// Optimistic (best band: nights, free-flow) durations — only with uncertainty=bands
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durations_best: Option<Vec<Vec<Option<f64>>>>,
    /// Pessimistic (worst band: weekday peaks) durations — only with uncertainty=bands
    #[serde(skip_serializing_if = "Option::is_none")]
    pub durations_worst: Option<Vec<Vec<Option<f64>>>>,
}

// ============ Handlers ============

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
    tag = "Matrix",
    summary = "Compute distance/duration matrix",
    description = "Computes a many-to-many distance and/or duration matrix using Bucket CH.\nBest for matrices up to ~10K cells. For larger matrices, use the Flight `matrix` action (port 3002).",
    request_body(content = TablePostRequest, description = "Source and destination coordinates with mode",
        example = json!({
            "origins": [[4.3517, 50.8503], [4.3617, 50.8553]],
            "destinations": [[4.4017, 50.8603], [4.4117, 50.8653]],
            "mode": "car",
            "annotations": "duration,distance"
        })
    ),
    responses(
        (status = 200, description = "Matrix computed", body = TableResponse,
            headers(("x-butterfly-matrix-plan" = String,
                description = "#594: the matrix plan actually run — bucket | phast_fwd | phast_rev"))),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
pub async fn table_post_handler(
    State(regions): State<Arc<RegionsState>>,
    Json(req): Json<TablePostRequest>,
) -> impl IntoResponse {
    for (i, [lon, lat]) in req.origins.iter().enumerate() {
        if let Err(e) = validate_coord(*lon, *lat, &format!("source[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    }
    for (i, [lon, lat]) in req.destinations.iter().enumerate() {
        if let Err(e) = validate_coord(*lon, *lat, &format!("destination[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    }

    // Region dispatch (#91): every source + every destination must
    // snap to the same region. Mixed-region matrices are rejected
    // with 501 (cross-region matrix is part of the overlay design,
    // PR C / Phase 2).
    let coords_iter = req
        .origins
        .iter()
        .chain(req.destinations.iter())
        .map(|&[lon, lat]| (lon, lat));
    let ctx = match QueryContext::from_points(&regions, coords_iter, &req.mode) {
        Ok(ctx) => ctx,
        Err(e) => {
            let (code, body) = e.into_response_parts();
            return (code, Json(body)).into_response();
        }
    };
    let state: Arc<ServerState> = Arc::clone(&ctx.state);

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

    if req.origins.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("sources cannot be empty")),
        )
            .into_response();
    }
    // Refuse an over-large request loudly and early, before any snapping.
    // Endpoints first and only then cells: the endpoint ceiling is what
    // bounds per-endpoint state, and it is also what makes the cell
    // multiplication safe to evaluate at all.
    let size_check =
        crate::server::types::validate_matrix_endpoints(req.origins.len(), req.destinations.len())
            .and_then(|()| {
                crate::server::types::validate_table_cells(
                    req.origins.len(),
                    req.destinations.len(),
                )
            });
    if let Err(error) = size_check {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(error))).into_response();
    }
    if req.destinations.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("destinations cannot be empty")),
        )
            .into_response();
    }

    // Parse annotations
    let annotations: Vec<&str> = req.annotations.split(',').map(|s| s.trim()).collect();
    for &a in &annotations {
        if !a.is_empty() && a != "duration" && a != "distance" {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!(
                    "Invalid annotation: '{}'. Use 'duration', 'distance', or 'duration,distance'.",
                    a
                ))),
            )
                .into_response();
        }
    }
    let want_duration = annotations.contains(&"duration") || !annotations.contains(&"distance");
    let want_distance = annotations.contains(&"distance");

    // Parse exclude parameter
    let exclude_mask = match super::exclude::parse_exclude_option(&req.exclude) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

    let mode_data = state.get_mode(mode);

    // #566: one resolution of exclude + avoid_polygons — avoid weights
    // (borrowed from the cache, no deep clone), the snap mask and the
    // avoid-over-exclude priority. #561: the mask is BORROWED when
    // neither option is present, where /table used to clone the whole
    // bitset on every request.
    let weight_plan = match super::avoid::resolve_weights(
        &state,
        &mode_data,
        mode,
        exclude_mask,
        avoid_json.as_deref(),
    ) {
        Ok(p) => p,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };
    let snap_mask: &[u64] = &weight_plan.snap_mask;
    let custom_weights_ref: Option<&super::exclude::ExcludeWeights> = weight_plan.weights();

    let radius_param = parse_radius(req.radius_km.as_ref());
    // #531: a per-origin radii array must be exactly one entry per origin —
    // reject a mismatch with 400 rather than silently no-filtering the tail.
    if let RadiusParam::PerOrigin(radii) = &radius_param
        && radii.len() != req.origins.len()
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(format!(
                "radius_km array length {} must equal origins length {}",
                radii.len(),
                req.origins.len()
            ))),
        )
            .into_response();
    }

    let threshold_s = match parse_max_minutes(req.max_minutes) {
        Ok(t) => t,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

    // #539: the whole matrix compute is sync CPU work — demote this worker
    // out of the async scheduler so concurrent /table storms cannot pin
    // every tokio worker and starve /health into liveness kills.
    let resp = tokio::task::block_in_place(|| {
        compute_table_bucket_m2m(
            &state,
            mode,
            &req.origins,
            &req.destinations,
            want_duration,
            want_distance,
            custom_weights_ref,
            snap_mask,
            radius_param,
            threshold_s,
        )
    });

    // #521 uncertainty bands: two more full matrix passes on the hidden band
    // weight sets, merged into the typical response as durations_best /
    // durations_worst. Opt-in only — 3x cost.
    let resp = match req.uncertainty.as_deref() {
        None => resp,
        Some("bands") => {
            if req.mode != "car" || req.exclude.is_some() || req.avoid_polygons.is_some() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::new("uncertainty=bands is car-only and incompatible with exclude/avoid_polygons")),
                )
                    .into_response();
            }
            let Some((pess, opt)) = state.band_modes() else {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::new("uncertainty bands not available: the loaded edge_speeds table has no best/worst columns")),
                )
                    .into_response();
            };
            let mut band_grids: Vec<serde_json::Value> = Vec::with_capacity(2);
            for band in [opt, pess] {
                let md = state.get_mode(band);
                let r = tokio::task::block_in_place(|| {
                    compute_table_bucket_m2m(
                        &state,
                        band,
                        &req.origins,
                        &req.destinations,
                        true,
                        false,
                        None,
                        &md.mask,
                        parse_radius(req.radius_km.as_ref()),
                        threshold_s,
                    )
                });
                let bytes = match axum::body::to_bytes(r.into_body(), 256 * 1024 * 1024).await {
                    Ok(b) => b,
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse::new(format!("band matrix pass failed: {e}"))),
                        )
                            .into_response();
                    }
                };
                let v: serde_json::Value = match serde_json::from_slice(&bytes) {
                    Ok(v) => v,
                    Err(e) => {
                        return (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            Json(ErrorResponse::new(format!(
                                "band matrix pass returned non-JSON: {e}"
                            ))),
                        )
                            .into_response();
                    }
                };
                band_grids.push(
                    v.get("durations")
                        .cloned()
                        .unwrap_or(serde_json::Value::Null),
                );
            }
            // #594: the merged response is a NEW response — carry the typical
            // pass's plan header across so `uncertainty=bands` reports the
            // plan too instead of silently dropping it.
            let plan_header = resp.headers().get(MATRIX_PLAN_HEADER).cloned();
            let bytes = match axum::body::to_bytes(resp.into_body(), 256 * 1024 * 1024).await {
                Ok(b) => b,
                Err(e) => {
                    return (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        Json(ErrorResponse::new(format!(
                            "median matrix pass failed: {e}"
                        ))),
                    )
                        .into_response();
                }
            };
            match serde_json::from_slice::<serde_json::Value>(&bytes) {
                Ok(mut v) => {
                    let q75 = band_grids.pop().unwrap_or(serde_json::Value::Null);
                    let q25 = band_grids.pop().unwrap_or(serde_json::Value::Null);
                    if let Some(obj) = v.as_object_mut() {
                        obj.insert("durations_best".into(), q25);
                        obj.insert("durations_worst".into(), q75);
                    }
                    let mut merged = Json(v).into_response();
                    if let Some(h) = plan_header {
                        merged
                            .headers_mut()
                            .insert(axum::http::HeaderName::from_static(MATRIX_PLAN_HEADER), h);
                    }
                    merged
                }
                // Median pass errored (4xx body): pass it through untouched.
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse::new(
                            "table computation failed before band merge",
                        )),
                    )
                        .into_response();
                }
            }
        }
        Some(other) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!(
                    "unknown uncertainty value '{other}' (expected 'bands')"
                ))),
            )
                .into_response();
        }
    };
    ctx.record("table");
    resp
}

/// Core table computation using bucket M2M algorithm
#[allow(clippy::too_many_arguments)]
pub fn compute_table_bucket_m2m(
    state: &Arc<ServerState>,
    mode: Mode,
    sources: &[[f64; 2]],
    destinations: &[[f64; 2]],
    want_duration: bool,
    want_distance: bool,
    custom_weights: Option<&super::exclude::ExcludeWeights>,
    snap_mask: &[u64],
    radius_param: RadiusParam,
    threshold_s: Option<u32>,
) -> Response {
    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // K-best snap with the directional #197 role filter. Use the same
    // primary that /route uses, so the matrix and routes agree on every
    // pair where the primary pair connects.
    //
    // Phase 1 (this block): snap K=1 per source/destination. Cheap —
    // each iterate_rings call exits as soon as one candidate is found
    // and no closer ring can beat it.
    //
    // Phase 2 (apply_k_best_fallback): only for the small fraction of
    // pairs whose K=1 primary doesn't connect do we escalate to a K=64
    // snap for the affected source/destination indices. That cost
    // remains O(failed_rows + failed_cols), not O(n_sources + n_targets).
    //
    // Pre-#368 the matrix paid the K=64 snap upfront for every
    // src/dst — ≈ 2.1 ms × N on serial / N/20 parallel. Belgium 100×100
    // matrix snap dropped from ~20 ms total → ~1 ms; 1000×1000 from
    // ~200 ms → ~13 ms. Healthy matrices never see the escalation cost.
    use super::snap_kbest::SNAP_K;
    let _ = SNAP_K; // referenced from apply_k_best_fallback's docs

    let src_role_filter = SnapRole::Src.role_filter(&mode_data);
    let dst_role_filter = SnapRole::Dst.role_filter(&mode_data);

    let t_pre = std::time::Instant::now();

    // (rank, snapped, valid). Per-row candidate list is built lazily on
    // first miss (see apply_k_best_fallback's lazy K=64 escalator).
    type SnapResult = (u32, (f64, f64), bool, Vec<(u32, u32, u32, bool)>);

    // #502: phantom seed sets — (rank, time_part, len_part) per endpoint.
    // Base-weights matrices seed BOTH directed twins of up to 3 near-
    // equidistant physical edges (K=4 snap) so the search picks the
    // departure/arrival direction; exclude/avoid keep the single-seed
    // legacy (their custom weight vectors aren't reflected in seed costs).
    let phantom_ok = custom_weights.is_none();
    let snap_endpoint = |lon: f64, lat: f64, role: super::types::SnapRole| -> SnapResult {
        if phantom_ok
            && let Some(pe) = super::phantom::phantom_for(
                state,
                &mode_data,
                mode,
                lon,
                lat,
                role,
                Some(snap_mask),
            )
        {
            let seeds: Vec<(u32, u32, u32, bool)> = pe
                .seeds
                .iter()
                .map(|x| (x.rank, x.part_time, x.part_len, x.direct_ok))
                .collect();
            let primary_rank = mode_data.orig_to_rank[pe.primary_ebg as usize];
            let rank = if primary_rank != u32::MAX {
                primary_rank
            } else {
                seeds[0].0
            };
            return (rank, (pe.snapped_lon, pe.snapped_lat), true, seeds);
        }
        if let Some((orig_id, plon, plat, _)) = state.snap_index.snap_with_info_filtered_role(
            lon,
            lat,
            mode.0,
            Some(snap_mask),
            role.role_filter(&mode_data),
            None,
        ) {
            let rank = mode_data.orig_to_rank[orig_id as usize];
            if rank != u32::MAX {
                return (rank, (plon, plat), true, vec![(rank, 0, 0, true)]);
            }
        }
        (0, (lon, lat), false, vec![])
    };

    let source_results: Vec<SnapResult> = sources
        .par_iter()
        .map(|&[lon, lat]| snap_endpoint(lon, lat, super::types::SnapRole::Src))
        .collect();

    let target_results: Vec<SnapResult> = destinations
        .par_iter()
        .map(|&[lon, lat]| snap_endpoint(lon, lat, super::types::SnapRole::Dst))
        .collect();

    let mut sources_rank: Vec<u32> = Vec::with_capacity(sources.len());
    let mut source_waypoints: Vec<Waypoint> = Vec::with_capacity(sources.len());
    let mut source_valid: Vec<bool> = Vec::with_capacity(sources.len());
    let mut sources_snapped: Vec<(f64, f64)> = Vec::with_capacity(sources.len());
    let mut src_seedsets: Vec<Vec<(u32, u32, u32, bool)>> = Vec::with_capacity(sources.len());
    for (rank, (plon, plat), valid, seeds) in source_results {
        src_seedsets.push(seeds);
        sources_rank.push(rank);
        source_valid.push(valid);
        sources_snapped.push((plon, plat));
        source_waypoints.push(Waypoint {
            location: [plon, plat],
            name: String::new(),
        });
    }

    let mut targets_rank: Vec<u32> = Vec::with_capacity(destinations.len());
    let mut dest_waypoints: Vec<Waypoint> = Vec::with_capacity(destinations.len());
    let mut target_valid: Vec<bool> = Vec::with_capacity(destinations.len());
    let mut targets_snapped: Vec<(f64, f64)> = Vec::with_capacity(destinations.len());
    let mut tgt_seedsets: Vec<Vec<(u32, u32, u32, bool)>> = Vec::with_capacity(destinations.len());
    for (rank, (plon, plat), valid, seeds) in target_results {
        tgt_seedsets.push(seeds);
        targets_rank.push(rank);
        target_valid.push(valid);
        targets_snapped.push((plon, plat));
        dest_waypoints.push(Waypoint {
            location: [plon, plat],
            name: String::new(),
        });
    }

    // Build the per-source neighbour mask if a radius was requested.
    // NOTE: this is a correctness-preserving "mask-at-emit" integration — the
    // full N×M bucket M2M still runs, and pruned pairs are nulled out below.
    // Pruning the inner solver per source would require refactoring bucket_ch
    // to accept per-source target slices without losing its amortised forward
    // phase; that's a follow-up optimisation and is unnecessary for
    // correctness.
    let neighbor_mask: Option<Vec<Vec<u32>>> = match match radius_param {
        RadiusParam::None => Ok(None),
        RadiusParam::Km(r) => build_neighbors(&sources_snapped, &targets_snapped, r).map(Some),
        RadiusParam::Auto => {
            let r = auto_radius_km(&sources_snapped, &targets_snapped);
            if r > 0.0 {
                build_neighbors(&sources_snapped, &targets_snapped, r).map(Some)
            } else {
                Ok(None)
            }
        }
        // #531 per-origin radii: validated len==origins upstream; a wrong
        // length degrades to no-filter for the missing tail (get→inf).
        RadiusParam::PerOrigin(radii) => {
            build_neighbors_per_origin(&sources_snapped, &targets_snapped, &radii).map(Some)
        }
    } {
        Ok(mask) => mask,
        // Over the neighbour budget: refuse, rather than allocate a mask
        // that would take the process down.
        Err(error) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(error))).into_response();
        }
    };

    let n_sources = sources.len();
    let n_targets = destinations.len();
    tracing::debug!(
        "compute_table_bucket_m2m: snap+rebuild took {:?} n_src={} n_tgt={}",
        t_pre.elapsed(),
        sources.len(),
        destinations.len()
    );

    // Select flat adjacencies based on custom weights (exclude or avoid)
    let (time_up, time_down) = if let Some(cw) = custom_weights {
        (&cw.time_up_flat, &cw.time_down_flat)
    } else {
        (&mode_data.up_adj_flat, &mode_data.down_rev_flat)
    };
    let (dist_up, dist_down) = if let Some(cw) = custom_weights {
        (&cw.dist_up_flat, &cw.dist_down_flat)
    } else {
        (&mode_data.up_adj_flat_dist, &mode_data.down_rev_flat_dist)
    };

    // #372: when both duration and distance are requested AND the
    // length-along-time flats are available (container shipped with
    // cch.lat.<mode>.u32 from PR #379), use the 2-channel bucket-M2M.
    // It produces both matrices in a single forward+backward pass with
    // the time-shortest path's geometry — distance numbers correspond
    // to the same path as the duration (matching /route's per-cell
    // unpack semantics).
    //
    // Custom-weight paths (exclude/avoid) don't have length-along-time
    // recustomisation yet; they fall back to the two-pass distance-
    // shortest legacy below.
    let use_2channel = want_duration
        && want_distance
        && custom_weights.is_none()
        && mode_data.up_adj_flat_len_along_time.is_some()
        && mode_data.down_rev_flat_len_along_time.is_some();

    // #415 max_minutes: when set, bound the TIME search at `threshold` so
    // compute is proportional to the reachable region, and null every cell
    // whose time exceeds it. `u32::MAX` = unbounded (byte-identical to the
    // pre-#415 path). Reachability is ALWAYS defined by time, even when only
    // `distance` is requested — so the bounded branch computes the time
    // matrix regardless and masks the distance cells against it.
    let threshold = threshold_s.unwrap_or(u32::MAX);
    let bounded = threshold_s.is_some();
    // When bounded we always need the TIME grid internally — it defines which
    // cells are within the bound (and therefore which distance cells are
    // valid), even for a distance-only request.
    //
    // CRITICAL: we do NOT null > threshold cells here. The threshold mask is
    // applied to the FINAL grids AFTER the K-best fallback (below). If we
    // masked a bucket-M2M-reached cell to null now, the bounded fallback
    // would treat it as a snap gap and "improve" it via an alternative K=64
    // snap — surfacing a value the unbounded matrix never shows (its primary
    // snap kept the original, fallback-skipped value). Masking last keeps the
    // served matrix exactly equal to the unbounded matrix filtered to
    // ≤ max_minutes. The bound still pays off: the SEARCH already early-stopped
    // at `threshold`, so out-of-bound cells are mostly unreached (MAX) and the
    // bounded fallback's distance_bounded gate keeps them null cheaply.
    let need_dur_internal = want_duration || bounded;

    // #509: phantom seeds go INTO the bucket engine (super-source forward,
    // shift-trick backward, pure-meet guard) — one sweep per endpoint, S×T
    // cells. Replaces the #502 API-layer SeedExpansion, which multiplied
    // engine cells by ~(avg seeds)^2 (measured 12-15x slower at every size).

    // #594: the plan the engine ACTUALLY ran, reported verbatim on the
    // response. Written by the call whose result is served; never re-derived
    // from the shape here.
    let mut plan = MatrixPlan::default();
    let (durations, distances) = if use_2channel {
        let t_2ch = std::time::Instant::now();
        let up_lat = mode_data
            .up_adj_flat_len_along_time
            .as_ref()
            .expect("guarded by use_2channel");
        let dn_lat = mode_data
            .down_rev_flat_len_along_time
            .as_ref()
            .expect("guarded by use_2channel");
        // #395: always go through the `_parallel_` wrapper — it now
        // dispatches internally to the pooled sequential 2-channel
        // path for small N (≤ SEQUENTIAL_FAST_PATH_CELL_THRESHOLD)
        // and the rayon-parallel path above it.
        // #415: the `_bounded` variant early-stops the time sweeps at
        // `threshold`; `u32::MAX` reproduces the unbounded result exactly.
        // #527: shape-aware router — plain-path 2-channel lopsided matrices
        // ride the seeded-PHAST field (distance included); custom weights
        // keep the 2-channel bucket. down_len_flat() builds the forward-down
        // len flat lazily on first use.
        let phast_ctx2 = if custom_weights.is_none() {
            mode_data
                .down_len_flat()
                .map(|dl| (&mode_data.down_adj_flat, dl, mode))
        } else {
            None
        };
        let (time_mat, lat_mat, stats) = crate::matrix::bucket_ch::table_seeded_bounded_routed_2ch(
            n_nodes,
            time_up,
            time_down,
            up_lat,
            dn_lat,
            phast_ctx2,
            &src_seedsets,
            &tgt_seedsets,
            threshold,
        );
        plan = stats.plan;
        tracing::debug!(
            "compute_table_bucket_m2m: 2-channel M2M took {:?}",
            t_2ch.elapsed(),
        );
        let dur = flat_matrix_to_2d(
            &time_mat,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            neighbor_mask.as_deref(),
            |v| v as f64,
        );
        let dist = flat_matrix_to_2d(
            &lat_mat,
            n_sources,
            n_targets,
            &source_valid,
            &target_valid,
            neighbor_mask.as_deref(),
            |v| v as f64,
        );
        (Some(dur), Some(dist))
    } else {
        // Legacy two-pass: separate distance-shortest CCH for `distance`.
        // The time matrix is computed whenever duration is requested OR a
        // bound is in effect (reachability is defined by time).
        let time_mat: Option<Vec<u32>> = if need_dur_internal {
            let t_dur = std::time::Instant::now();
            // #526 shape-aware router: plain-path duration matrices may take
            // the seeded-PHAST lopsided plan; custom weights keep bucket.
            let phast_ctx = if custom_weights.is_none() {
                Some((&mode_data.down_adj_flat, mode))
            } else {
                None
            };
            let (matrix, stats) = crate::matrix::bucket_ch::table_seeded_bounded_routed(
                n_nodes,
                time_up,
                time_down,
                phast_ctx,
                &src_seedsets,
                &tgt_seedsets,
                threshold,
            );
            plan = stats.plan;
            tracing::debug!(
                "compute_table_bucket_m2m: duration M2M took {:?}",
                t_dur.elapsed()
            );
            Some(matrix)
        } else {
            None
        };

        let distances = if want_distance {
            let t_dist = std::time::Instant::now();
            // Distance weights are metres — the minutes bound does not apply to
            // the distance search. Distance cells are bounded by the final
            // time-grid mask after the fallback.
            // Distance-metric run: seed costs must be the LENGTH partials —
            // swap (part_time, part_len) so the engine's cost channel is metres.
            let swap = |sets: &[Vec<(u32, u32, u32, bool)>]| -> Vec<Vec<(u32, u32, u32, bool)>> {
                sets.iter()
                    .map(|v| v.iter().map(|&(r, t, l, ok)| (r, l, t, ok)).collect())
                    .collect()
            };
            let (matrix, stats) = crate::matrix::bucket_ch::table_bucket_parallel_seeded_bounded(
                n_nodes,
                dist_up,
                dist_down,
                &swap(&src_seedsets),
                &swap(&tgt_seedsets),
                u32::MAX,
            );
            // Only when this is the ONLY engine run (distance-only request) is
            // it the plan of the served grid; otherwise the duration run above
            // owns the report.
            if !need_dur_internal {
                plan = stats.plan;
            }
            tracing::debug!(
                "compute_table_bucket_m2m: distance M2M took {:?}",
                t_dist.elapsed()
            );
            Some(flat_matrix_to_2d(
                &matrix,
                n_sources,
                n_targets,
                &source_valid,
                &target_valid,
                neighbor_mask.as_deref(),
                |v| v as f64,
            ))
        } else {
            None
        };

        let durations = if need_dur_internal {
            let tm = time_mat.expect("time matrix computed when need_dur_internal");
            Some(flat_matrix_to_2d(
                &tm,
                n_sources,
                n_targets,
                &source_valid,
                &target_valid,
                neighbor_mask.as_deref(),
                |v| v as f64,
            ))
        } else {
            None
        };
        (durations, distances)
    };

    let t_post_m2m = std::time::Instant::now();
    let _ = t_post_m2m;

    // Per-cell K-best fallback (#197 matrix gap).
    //
    // Bucket M2M uses only the primary candidate per src/dst. For the
    // small fraction of pairs the primary snap is still unsuitable
    // for this particular OD pair (usually same-geometry directional
    // ambiguity or dynamic exclude/avoid effects), even though K-best
    // would connect. /route already does this fallback inline; we
    // mirror it here so /table agrees with /route.
    // The K-best snap (expensive — iterates all samples within 5 km)
    // is done LAZILY for only the affected src/dst rows/cols, so a
    // healthy matrix pays zero K-best snap cost.
    //
    // #415: SKIP the fallback when a minutes bound is in effect. Under a bound,
    // most MAX cells are unreached because they are genuinely beyond the bound
    // (the search early-stopped) — NOT snap gaps. Running the K-best rescue on
    // them would fire a ~max_minutes-isochrone-sized `distance_bounded` search
    // per cell × K combos, re-doing exactly the work the bound saved (measured:
    // an 8×250 matrix went 45 ms unbounded → 60 s+ with the bounded fallback).
    // So under a bound we trade the rare in-bound primary-snap-disconnected
    // cell (returned null instead of rescued — value never wrong) for the
    // bound's whole point: compute proportional to the reachable region. This
    // matches the Flight `matrix` behaviour; the unbounded /table path keeps
    // full snap-gap fidelity.
    let (durations, distances) = if bounded {
        (durations, distances)
    } else {
        apply_k_best_fallback(
            state,
            &mode_data,
            mode,
            durations,
            distances,
            sources,
            destinations,
            &source_valid,
            &target_valid,
            neighbor_mask.as_deref(),
            snap_mask,
            src_role_filter,
            dst_role_filter,
            custom_weights,
            need_dur_internal,
            want_distance,
            threshold_s,
        )
    };

    // #415: apply the time bound to the FINAL grids, after the fallback. A
    // bucket-M2M-reached cell with time > threshold was kept non-null through
    // the fallback (so it wasn't snap-"improved"); null it now — along with its
    // distance cell — so the served matrix is exactly the unbounded matrix
    // filtered to ≤ max_minutes. The duration grid is the time reference.
    let (mut durations, mut distances) = (durations, distances);
    if let Some(thr) = threshold_s {
        let thr_f = thr as f64;
        let n_s = durations.as_ref().map_or(0, |g| g.len());
        for i in 0..n_s {
            let n_t = durations.as_ref().map_or(0, |g| g[i].len());
            for j in 0..n_t {
                let over = durations
                    .as_ref()
                    .and_then(|g| g[i][j])
                    .is_some_and(|v| v > thr_f);
                if over {
                    if let Some(g) = durations.as_mut() {
                        g[i][j] = None;
                    }
                    if let Some(g) = distances.as_mut() {
                        g[i][j] = None;
                    }
                }
            }
        }
    }
    // Drop the internally-computed duration grid if the caller didn't ask.
    if !want_duration {
        durations = None;
    }

    tracing::debug!(
        "compute_table_bucket_m2m: post-m2m to response took {:?}",
        t_post_m2m.elapsed()
    );

    let t_resp = std::time::Instant::now();
    let mut resp = Json(TableResponse {
        code: "Ok".into(),
        durations,
        distances,
        origins: Some(source_waypoints),
        destinations: Some(dest_waypoints),
        durations_best: None,
        durations_worst: None,
    })
    .into_response();
    // #594: report the plan that ran. `as_str` is a &'static str from the
    // closed MatrixPlan set, so the header value can never fail to parse.
    resp.headers_mut().insert(
        axum::http::HeaderName::from_static(MATRIX_PLAN_HEADER),
        axum::http::HeaderValue::from_static(plan.as_str()),
    );
    tracing::debug!(
        "compute_table_bucket_m2m: json+into_response took {:?}",
        t_resp.elapsed()
    );
    resp
}

/// 2D matrix of Option<f64> — None for unreachable/invalid cells.
type MatrixGrid = Option<Vec<Vec<Option<f64>>>>;

/// For each cell where bucket-M2M returned None (unreachable under the
/// primary src/dst snap pair), retry with the K-best candidate combo
/// enumeration — the same fallback /route uses for #197.
///
/// Lazy K-best: the expensive `snap_k_with_info_filtered_role`
/// (iterates all samples within 5 km) is only invoked for src/dst rows
/// and columns that contain at least one None cell. Healthy matrices
/// pay zero overhead beyond the cheap primary snap done upfront.
#[allow(clippy::too_many_arguments)]
fn apply_k_best_fallback(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    mut durations: MatrixGrid,
    mut distances: MatrixGrid,
    sources: &[[f64; 2]],
    destinations: &[[f64; 2]],
    source_valid: &[bool],
    target_valid: &[bool],
    neighbor_mask: Option<&[Vec<u32>]>,
    snap_mask: &[u64],
    src_role_filter: Option<&[u64]>,
    dst_role_filter: Option<&[u64]>,
    custom_weights: Option<&super::exclude::ExcludeWeights>,
    want_duration: bool,
    want_distance: bool,
    threshold_s: Option<u32>,
) -> (MatrixGrid, MatrixGrid) {
    use super::query::CchQuery;
    use super::snap_kbest::SNAP_K;

    // #415: when a minutes bound is in effect, a None cell may be None
    // because it is genuinely beyond the bound (correctly excluded) rather
    // than a snap/connectivity gap. The fallback must NOT resurrect those.
    // We gate every fill on a bounded time query so out-of-bound pairs stay
    // None, while in-bound snap-gap cells are still recovered. The gate
    // itself lives in `snap_kbest::cell_with_kbest_fallback` (#567).
    let bounded = threshold_s.is_some();

    // Cap per-cell fallback combos. /route uses 400 because a single
    // hopeless query at 20s wall is acceptable; /table can have
    // hundreds of failed cells so the per-cell budget must be smaller
    // or total latency explodes (the unbounded version ran 88 s on
    // Belgium 50×50 scattered).
    //
    // Connectivity-aware role masks should keep this path cold on the
    // base graph. Keep the cap broad enough to preserve /route parity
    // for the remaining dynamic or geometrically ambiguous cases.
    const MAX_FALLBACK_COMBOS: usize = super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS;

    let _t_fb_start = std::time::Instant::now();
    let n_sources = sources.len();
    let n_targets = destinations.len();

    // Decide whether any cell needs the fallback. Skip the (cheap)
    // CchQuery construction entirely on the common path.
    let needs_fallback = |grid: &MatrixGrid| -> bool {
        if let Some(g) = grid {
            for (i, row) in g.iter().enumerate() {
                if !source_valid[i] {
                    continue;
                }
                for (j, cell) in row.iter().enumerate() {
                    if target_valid[j] && cell.is_none() {
                        return true;
                    }
                }
            }
        }
        false
    };
    let need_dist = want_distance && needs_fallback(&distances);
    // When bounded we always need the time query (even for a distance-only
    // request) — it is the gate that decides whether a recovered cell is
    // within the minutes bound.
    let need_time = (want_duration && needs_fallback(&durations)) || (bounded && need_dist);
    tracing::debug!(
        "apply_k_best_fallback: needs_fallback decision took {:?}, need_time={}, need_dist={}",
        _t_fb_start.elapsed(),
        need_time,
        need_dist
    );
    if !need_time && !need_dist {
        return (durations, distances);
    }

    // Time CchQuery: use the same backend as /route (flats from
    // mode_data, or recustomised flats if exclude/avoid are in play).
    // with_custom_weights expects the *reverse* down-adjacency
    // (DownReverseAdjFlat) for the bidirectional backward search — same
    // layout as `mode_data.down_rev_flat`.
    let time_query = if need_time {
        Some(match custom_weights {
            Some(cw) => CchQuery::with_custom_weights(
                &mode_data.cch_topo,
                &cw.time_up_flat,
                &cw.time_down_flat,
                &cw.time_weights,
            ),
            None => CchQuery::new(mode_data),
        })
    } else {
        None
    };

    // Distance CchQuery: the CCH topology is shared between time and
    // distance, and the metric-dependent INF sets agree (both are gated
    // on mode access + exclude flags). So we reuse the TIME flats for
    // topology + topo_edge_idx and override with the distance-metric
    // weights. The standalone `*_dist` flats and the `dist_*` flats on
    // `ExcludeWeights` intentionally omit `topo_edge_idx` because PHAST
    // doesn't need it — they cannot back a `CchQuery` directly.
    let dist_query = if need_dist {
        let (up_flat, down_flat, weights) = match custom_weights {
            Some(cw) => (&cw.time_up_flat, &cw.time_down_flat, &cw.dist_weights),
            None => (
                &mode_data.up_adj_flat,
                &mode_data.down_rev_flat,
                &mode_data.cch_weights_dist,
            ),
        };
        Some(CchQuery::with_custom_weights(
            &mode_data.cch_topo,
            up_flat,
            down_flat,
            weights,
        ))
    } else {
        None
    };

    let t_fb_work = std::time::Instant::now();
    // Build the list of cells needing fallback AND the set of unique
    // src/tgt indices touched by them. We snap K=64 only for those
    // indices — healthy matrices snap zero rows/cols here.
    let mut work: Vec<(usize, usize, bool, bool)> = Vec::new();
    let mut src_idx_set: std::collections::HashSet<usize> = std::collections::HashSet::new();
    let mut tgt_idx_set: std::collections::HashSet<usize> = std::collections::HashSet::new();
    for src_idx in 0..n_sources {
        if !source_valid[src_idx] {
            continue;
        }
        for tgt_idx in 0..n_targets {
            if !target_valid[tgt_idx] {
                continue;
            }
            // #531: a cell the radius neighbor-mask deliberately pruned is
            // NOT a snap gap — skip it so the fallback can't re-populate a
            // pair the caller asked to exclude (rows are sorted → bsearch).
            if let Some(mask) = neighbor_mask
                && mask[src_idx].binary_search(&(tgt_idx as u32)).is_err()
            {
                continue;
            }
            let dur_missing = durations
                .as_ref()
                .map(|d| d[src_idx][tgt_idx].is_none())
                .unwrap_or(false);
            let dist_missing = distances
                .as_ref()
                .map(|d| d[src_idx][tgt_idx].is_none())
                .unwrap_or(false);
            if dur_missing || dist_missing {
                work.push((src_idx, tgt_idx, dur_missing, dist_missing));
                src_idx_set.insert(src_idx);
                tgt_idx_set.insert(tgt_idx);
            }
        }
    }

    tracing::debug!(
        "apply_k_best_fallback: built work list of {} cells (unique src={}, tgt={}) in {:?}",
        work.len(),
        src_idx_set.len(),
        tgt_idx_set.len(),
        t_fb_work.elapsed()
    );

    if work.is_empty() {
        return (durations, distances);
    }

    // Lazy K=64 escalation: snap each affected src/tgt index ONCE, in
    // parallel. `sources_candidates[i]` is None for indices not in the
    // failed set — those rows never see the K=64 cost.
    let t_fb_snap = std::time::Instant::now();
    let mut sources_candidates: Vec<Option<Vec<u32>>> = vec![None; n_sources];
    let mut targets_candidates: Vec<Option<Vec<u32>>> = vec![None; n_targets];
    let needed_src: Vec<usize> = src_idx_set.into_iter().collect();
    let needed_tgt: Vec<usize> = tgt_idx_set.into_iter().collect();
    let src_snapped: Vec<(usize, Vec<u32>)> = needed_src
        .par_iter()
        .map(|&i| {
            let [lon, lat] = sources[i];
            let cands = state.snap_index.snap_k_with_info_filtered_role(
                lon,
                lat,
                mode.0,
                SNAP_K,
                Some(snap_mask),
                src_role_filter,
            );
            let ranks: Vec<u32> = cands
                .iter()
                .filter_map(|(orig_id, _, _, _)| {
                    let r = mode_data.orig_to_rank[*orig_id as usize];
                    if r == u32::MAX { None } else { Some(r) }
                })
                .collect();
            (i, ranks)
        })
        .collect();
    let tgt_snapped: Vec<(usize, Vec<u32>)> = needed_tgt
        .par_iter()
        .map(|&i| {
            let [lon, lat] = destinations[i];
            let cands = state.snap_index.snap_k_with_info_filtered_role(
                lon,
                lat,
                mode.0,
                SNAP_K,
                Some(snap_mask),
                dst_role_filter,
            );
            let ranks: Vec<u32> = cands
                .iter()
                .filter_map(|(orig_id, _, _, _)| {
                    let r = mode_data.orig_to_rank[*orig_id as usize];
                    if r == u32::MAX { None } else { Some(r) }
                })
                .collect();
            (i, ranks)
        })
        .collect();
    for (i, ranks) in src_snapped {
        sources_candidates[i] = Some(ranks);
    }
    for (i, ranks) in tgt_snapped {
        targets_candidates[i] = Some(ranks);
    }
    tracing::debug!(
        "apply_k_best_fallback: lazy K={} snap for {} src + {} tgt took {:?}",
        SNAP_K,
        needed_src.len(),
        needed_tgt.len(),
        t_fb_snap.elapsed()
    );

    let t_fb_run = std::time::Instant::now();
    // Solve per cell in parallel — CchQuery is Sync (immutable
    // references to topology + weights; thread-local search state
    // lives in CchQueryState). Each cell is independent, so rayon
    // gives close to linear speed-up on n_cores.
    let time_query_ref = time_query.as_ref();
    let dist_query_ref = dist_query.as_ref();
    let patches: Vec<(usize, usize, Option<f64>, Option<f64>)> = work
        .par_iter()
        .map(|&(src_idx, tgt_idx, dur_missing, dist_missing)| {
            let empty: Vec<u32> = Vec::new();
            let src_cands = sources_candidates[src_idx].as_ref().unwrap_or(&empty);
            let tgt_cands = targets_candidates[tgt_idx].as_ref().unwrap_or(&empty);
            // Shared #197 escalation (#567). `threshold_s` carries the
            // bounded rule: under a minutes bound a cell is recovered only
            // when the pair's travel time is ≤ the bound, and that time gate
            // covers the distance channel too. Values are already seconds /
            // metres (post-#297).
            let cell = super::snap_kbest::cell_with_kbest_fallback(
                time_query_ref,
                dist_query_ref,
                src_cands,
                tgt_cands,
                dur_missing,
                dist_missing,
                threshold_s,
                MAX_FALLBACK_COMBOS,
            );
            (
                src_idx,
                tgt_idx,
                cell.time.map(|t| t as f64),
                cell.distance.map(|d| d as f64),
            )
        })
        .collect();

    tracing::debug!(
        "apply_k_best_fallback: ran {} cells in {:?}",
        patches.len(),
        t_fb_run.elapsed()
    );

    // Apply patches sequentially (cheap O(failed_cells) writes).
    for (src_idx, tgt_idx, dur_val, dist_val) in patches {
        if let Some(grid) = durations.as_mut()
            && let Some(v) = dur_val
        {
            grid[src_idx][tgt_idx] = Some(v);
        }
        if let Some(grid) = distances.as_mut()
            && let Some(v) = dist_val
        {
            grid[src_idx][tgt_idx] = Some(v);
        }
    }

    (durations, distances)
}

/// Convert flat u32 matrix to 2D Option<f64> matrix with null for invalid/unreachable.
///
/// If `neighbor_mask` is supplied, any (src, tgt) pair not present in
/// `neighbor_mask[src]` is emitted as `None` regardless of the computed
/// distance. The mask is indexed by the original source/target positions
/// (i.e. the full `n_sources`/`n_targets`) so callers pre-filter using
/// haversine distances on the original inputs.
#[allow(clippy::too_many_arguments)]
pub fn flat_matrix_to_2d(
    matrix: &[u32],
    n_sources: usize,
    n_targets: usize,
    source_valid: &[bool],
    target_valid: &[bool],
    neighbor_mask: Option<&[Vec<u32>]>,
    convert: impl Fn(u32) -> f64,
) -> Vec<Vec<Option<f64>>> {
    let mut result: Vec<Vec<Option<f64>>> = Vec::with_capacity(n_sources);
    for src_idx in 0..n_sources {
        let mut row: Vec<Option<f64>> = Vec::with_capacity(n_targets);
        // Neighbour mask for this source is a sorted Vec<u32>; use binary
        // search so the inner loop is O(n_targets × log k).
        let src_neighbors: Option<&[u32]> = neighbor_mask.map(|nm| nm[src_idx].as_slice());
        for tgt_idx in 0..n_targets {
            if !source_valid[src_idx] || !target_valid[tgt_idx] {
                row.push(None);
                continue;
            }
            if let Some(ns) = src_neighbors
                && ns.binary_search(&(tgt_idx as u32)).is_err()
            {
                row.push(None);
                continue;
            }
            let val = matrix[src_idx * n_targets + tgt_idx];
            if val == u32::MAX {
                row.push(None);
            } else {
                row.push(Some(convert(val)));
            }
        }
        result.push(row);
    }
    result
}

// ============ Arrow Streaming Handler ============

// ============ Bucket M2M path for small streaming matrices ============

#[cfg(test)]
mod max_minutes_tests {
    use super::{MATRIX_PLAN_HEADER, MatrixPlan, TablePostRequest, parse_max_minutes};

    #[test]
    fn none_passes_through() {
        assert_eq!(parse_max_minutes(None).unwrap(), None);
    }

    #[test]
    fn unknown_field_is_rejected_not_silently_ignored() {
        // #415: a server that supports max_minutes must accept it...
        let ok = r#"{"origins":[[4.35,50.85]],"destinations":[[4.4,51.2]],"mode":"car","max_minutes":15}"#;
        assert!(serde_json::from_str::<TablePostRequest>(ok).is_ok());
        // ...and `deny_unknown_fields` must REJECT a typo / unsupported param
        // (the silent-ignore bug: pre-#415 builds dropped these and returned a
        // full unbounded matrix with no error).
        let typo = r#"{"origins":[[4.35,50.85]],"destinations":[[4.4,51.2]],"mode":"car","max_minute":15}"#;
        assert!(serde_json::from_str::<TablePostRequest>(typo).is_err());
        let bogus =
            r#"{"origins":[[4.35,50.85]],"destinations":[[4.4,51.2]],"mode":"car","wat":1}"#;
        assert!(serde_json::from_str::<TablePostRequest>(bogus).is_err());
    }

    #[test]
    fn converts_minutes_to_seconds_ceil() {
        assert_eq!(parse_max_minutes(Some(10.0)).unwrap(), Some(600));
        assert_eq!(parse_max_minutes(Some(0.5)).unwrap(), Some(30));
        // ceil so a cell exactly at the bound is included.
        assert_eq!(parse_max_minutes(Some(1.001)).unwrap(), Some(61));
    }

    #[test]
    fn rejects_non_positive_and_nonfinite() {
        assert!(parse_max_minutes(Some(0.0)).is_err());
        assert!(parse_max_minutes(Some(-5.0)).is_err());
        assert!(parse_max_minutes(Some(f64::NAN)).is_err());
        assert!(parse_max_minutes(Some(f64::INFINITY)).is_err());
    }

    #[test]
    fn rejects_above_24h_cap() {
        assert!(parse_max_minutes(Some(1441.0)).is_err());
        assert_eq!(parse_max_minutes(Some(1440.0)).unwrap(), Some(86400));
    }

    /// #594: the handler builds the plan header from `&'static str`s with
    /// `from_static`, which PANICS on an invalid name/value. Build every one
    /// of them here so an added plan variant (or a renamed header) fails in
    /// CI rather than in a live `/table` response.
    #[test]
    fn plan_header_name_and_every_value_are_valid() {
        let name = axum::http::HeaderName::from_static(MATRIX_PLAN_HEADER);
        assert_eq!(name.as_str(), "x-butterfly-matrix-plan");
        for plan in [
            MatrixPlan::Bucket,
            MatrixPlan::PhastFwd,
            MatrixPlan::PhastRev,
            MatrixPlan::Mixed,
        ] {
            let v = axum::http::HeaderValue::from_static(plan.as_str());
            assert_eq!(v.to_str().unwrap(), plan.as_str());
        }
        // The default a request reports when no PHAST branch ran.
        assert_eq!(MatrixPlan::default(), MatrixPlan::Bucket);
    }
}
