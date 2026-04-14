//! `GET /transit` — multimodal access + transit + egress routing.
//!
//! A query combines up to three legs on each side of the transit portion:
//!
//!   1. **Access leg** (origin → first boarding stop) using any loaded
//!      road mode — typically `foot`, but `car` for park-and-ride,
//!      `bike` for bike-and-ride, etc.
//!   2. **Transit portion** — one or more rides on any merged GTFS feed
//!      (SNCB, De Lijn, TEC, STIB for Belgium), with walking stop-to-stop
//!      transfers between legs where needed.
//!   3. **Egress leg** (last alighting stop → destination) again using
//!      any loaded road mode.
//!
//! The access and egress modes are independent. This is what the
//! standard "walk + car + train + bus + walk" multimodal pattern looks
//! like to the server: `access_mode=car` for the drive-to-station leg,
//! RAPTOR handles the train-to-bus transfer internally, `egress_mode=foot`
//! for the walk-to-destination.
//!
//! Flow:
//!   1. Resolve `access_mode` / `egress_mode` to loaded `ModeData`.
//!   2. Select up to `max_access_stops` nearest transit stops within the
//!      per-mode access radius of the origin, and similarly for egress.
//!   3. Compute per-stop access/egress times from the origin/destination
//!      using the selected mode's CCH via distance-only `CchQuery`.
//!   4. Run RAPTOR with these times as access/egress offsets.
//!   5. Reconstruct the journey and return it as JSON.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    Json,
    extract::{Query, State},
    http::StatusCode,
};
use serde::{Deserialize, Serialize};

use crate::server::geometry::build_raw_points;
use crate::server::query::CchQuery;
use crate::server::state::ModeData;
use crate::server::unpack::unpack_path;
use crate::transit::gtfs::haversine_m;
use crate::transit::raptor::{RaptorLeg, RaptorQuery, run_raptor};
use crate::transit::timetable::{StopIdx, Timetable};

use super::state::ServerState;
use super::types::ErrorResponse;

/// Per-mode defaults for access/egress fan-out. `(radius_m, max_stops, speed_mps)`.
///
/// Notes on `max_stops`:
///
/// - **foot** — 20 is fine because walking access is bounded by a tight
///   2 km radius and most European cities have dozens of stops in a
///   2 km disc. Increasing K brings diminishing returns.
/// - **bike** — 60 covers the longer 8 km radius; cyclists shouldn't
///   care about stops farther than that.
/// - **car** — 500 (not 200) because the 30 km car radius sweeps past
///   hundreds of dense-urban local bus stops before reaching the
///   sparse rail stations that are the real point of a park-and-ride.
///   SNCB stops spacing is ~10 km in the commuter belt; bus stops are
///   ~200 m apart. Without a big K, the car candidate set is all bus
///   and no rail, which produces a worse journey. This is a band-aid
///   for the O(N) linear-scan in `candidate_stops` — the proper fix
///   is a spatial index plus mode-aware stop weighting (issue #102).
/// - **other** — a reasonable middle (100).
fn default_access_params(mode: &str) -> (u32, usize, f64) {
    match mode {
        "foot" => (2_000, 20, 1.3),
        "bike" => (8_000, 60, 4.2),
        "car" => (30_000, 500, 13.9),
        // Any other mode (truck, bus, scooter…) treat as fast road mode.
        _ => (20_000, 100, 11.1),
    }
}

/// Query parameters for `GET /transit`.
#[derive(Debug, Clone, Deserialize)]
pub struct TransitRequest {
    pub origin_lon: f64,
    pub origin_lat: f64,
    pub dest_lon: f64,
    pub dest_lat: f64,
    /// HH:MM or HH:MM:SS in service-local time. Default: "08:00:00".
    #[serde(default)]
    pub depart: Option<String>,
    /// Road mode for the access leg (origin → first stop). Any loaded
    /// mode is accepted (`foot`, `car`, `bike`, …). Default: `"foot"`.
    #[serde(default)]
    pub access_mode: Option<String>,
    /// Road mode for the egress leg (last stop → destination). Default:
    /// same as `access_mode` or `"foot"` if neither is given.
    #[serde(default)]
    pub egress_mode: Option<String>,
    /// Max access radius (meters). Default: per-mode (foot=2000,
    /// bike=8000, car=30000).
    #[serde(default)]
    pub max_access_m: Option<u32>,
    /// Max egress radius (meters). Default: per-mode.
    #[serde(default)]
    pub max_egress_m: Option<u32>,
    /// DEPRECATED alias for `max_access_m` + `max_egress_m` when both
    /// modes are `foot`. Retained for backward compatibility with the
    /// previous release; new callers should use the explicit fields.
    #[serde(default)]
    pub max_walk_m: Option<u32>,
    /// Max number of access / egress stops. Default: per-mode.
    #[serde(default)]
    pub max_access_stops: Option<usize>,
    /// Walking speed (m/s) — only used when `access_mode` is `foot`
    /// and its CCH returns raw distances in mm. Default: 1.3.
    #[serde(default)]
    pub walk_speed_mps: Option<f64>,
    /// Access / egress leg geometry mode (#114). Accepted values:
    ///
    /// - `"straight"` (default): legs carry only `from`/`to` endpoints
    ///   with a haversine `distance_m`. Cheap.
    /// - `"full"`: legs carry a routed polyline `geometry` produced
    ///   by unpacking the CCH shortest path, and `distance_m` is the
    ///   real summed road distance. Costs a few milliseconds per leg.
    ///
    /// Anything else is rejected with HTTP 400.
    #[serde(default)]
    pub geometry: Option<String>,
}

/// One leg of the returned transit plan.
#[derive(Debug, Serialize)]
#[serde(tag = "type", rename_all = "snake_case")]
// Boxing Transit would hurt readability and this enum is only used for
// JSON output on a single response — allocations here are not hot.
#[allow(clippy::large_enum_variant)]
pub enum TransitLegOut {
    /// Walking leg — either the foot-transfer between two transit stops
    /// inside a RAPTOR round, or an access/egress leg when the selected
    /// road mode is `foot`.
    Walk {
        from: [f64; 2],
        to: [f64; 2],
        duration_s: u32,
        distance_m: u32,
        /// Optional routed polyline for access/egress walks. Populated
        /// when the request sets `geometry=full` (#114). Omitted from
        /// the JSON response when `None`.
        #[serde(skip_serializing_if = "Option::is_none")]
        geometry: Option<Vec<[f64; 2]>>,
    },
    /// Driving leg — an access or egress leg whose road mode is `car`.
    Drive {
        from: [f64; 2],
        to: [f64; 2],
        duration_s: u32,
        distance_m: u32,
        /// Optional routed polyline (#114).
        #[serde(skip_serializing_if = "Option::is_none")]
        geometry: Option<Vec<[f64; 2]>>,
    },
    /// Generic road leg for any non-foot, non-car mode (`bike`, `truck`…).
    /// The `mode` field carries the loaded mode's name.
    Road {
        mode: String,
        from: [f64; 2],
        to: [f64; 2],
        duration_s: u32,
        distance_m: u32,
        /// Optional routed polyline (#114).
        #[serde(skip_serializing_if = "Option::is_none")]
        geometry: Option<Vec<[f64; 2]>>,
    },
    Transit {
        /// `Arc<str>` cloned from the timetable — zero-copy on the hot
        /// path and serialised as a plain JSON string (#118).
        from_stop_id: Arc<str>,
        from_stop_name: Arc<str>,
        from: [f64; 2],
        to_stop_id: Arc<str>,
        to_stop_name: Arc<str>,
        to: [f64; 2],
        board_time: String,
        alight_time: String,
        duration_s: u32,
        route_short_name: Arc<str>,
        route_long_name: Arc<str>,
        headsign: Arc<str>,
    },
}

/// Full response.
#[derive(Debug, Serialize)]
pub struct TransitResponse {
    pub origin: [f64; 2],
    pub destination: [f64; 2],
    pub depart_time: String,
    pub arrival_time: String,
    pub total_duration_s: u32,
    /// Road mode used for the access (origin→first stop) leg.
    pub access_mode: String,
    /// Road mode used for the egress (last stop→destination) leg.
    pub egress_mode: String,
    pub legs: Vec<TransitLegOut>,
}

/// Wrap a mode-name + coords into a road-leg JSON variant.
fn road_leg(
    mode: &str,
    from: [f64; 2],
    to: [f64; 2],
    duration_s: u32,
    distance_m: u32,
    geometry: Option<Vec<[f64; 2]>>,
) -> TransitLegOut {
    match mode {
        "foot" => TransitLegOut::Walk {
            from,
            to,
            duration_s,
            distance_m,
            geometry,
        },
        "car" => TransitLegOut::Drive {
            from,
            to,
            duration_s,
            distance_m,
            geometry,
        },
        other => TransitLegOut::Road {
            mode: other.to_string(),
            from,
            to,
            duration_s,
            distance_m,
            geometry,
        },
    }
}

pub async fn transit_handler(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<TransitRequest>,
) -> Result<Json<TransitResponse>, (StatusCode, Json<ErrorResponse>)> {
    compute_transit_journey(state.as_ref(), &req).map(Json)
}

/// Reusable access-side computation result for a transit query (#120).
///
/// The access fan-out (origin snap → candidate stops → CCH 1-to-N) is
/// independent of the destination and depart time, so two queries from
/// the same origin can share this. See [`OriginGroupKey`] and the bulk
/// handler for how this is exploited.
///
/// The cached state stores per-stop *raw* walk seconds; the per-query
/// `max_access_s` cap (which depends on the egress mode) is reapplied
/// in [`compute_transit_journey_with_access`].
pub struct AccessContext {
    /// Resolved access mode name (lowercased).
    pub access_mode: String,
    /// Index into `state.modes` for the access mode.
    pub access_idx: u8,
    /// Resolved access radius (meters).
    pub max_access_m: u32,
    /// Resolved max number of access candidate stops.
    pub max_access_stops: usize,
    /// Resolved walking speed (m/s).
    pub walk_speed_mps: f64,
    /// Origin coordinates as supplied (used by the response builder).
    pub origin_lon: f64,
    pub origin_lat: f64,
    /// Per-stop raw walk seconds from the origin to each candidate
    /// access stop that snapped on the access mode's network. Already
    /// deduped to fastest path per stop, NOT yet filtered by
    /// `max_access_s` (that filter is per-query).
    pub origin_walks: Vec<(StopIdx, u32)>,
}

/// Single-query transit computation. Thin wrapper over
/// [`compute_access_context`] + [`compute_transit_journey_with_access`]
/// — kept stable for the `/transit` handler and the integration tests.
/// The bulk handler (#120) calls those two directly so it can share an
/// access context across queries from the same origin.
pub fn compute_transit_journey(
    state: &ServerState,
    req: &TransitRequest,
) -> Result<TransitResponse, (StatusCode, Json<ErrorResponse>)> {
    let access = compute_access_context(state, req)?;
    compute_transit_journey_with_access(state, req, &access)
}

/// Compute the access-side context (#120): origin validation, mode
/// resolution, candidate stop selection, per-stop access walk times.
///
/// This phase is the ~30–40 % of single-query cost that is identical
/// across queries sharing the same origin / access_mode / radius.
/// Two queries with different `depart` times or different destinations
/// from the same origin produce the same `AccessContext`.
pub fn compute_access_context(
    state: &ServerState,
    req: &TransitRequest,
) -> Result<AccessContext, (StatusCode, Json<ErrorResponse>)> {
    let Some(transit) = state.transit.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "transit subsystem is not loaded (no transit/ directory)".to_string(),
            }),
        ));
    };

    // Origin-only coordinate validation. Destination bounds are checked
    // in compute_transit_journey_with_access so a bad dest in a bulk
    // group doesn't poison the shared access context.
    if !(-180.0..=180.0).contains(&req.origin_lon) || !(-90.0..=90.0).contains(&req.origin_lat) {
        return Err(bad_request("invalid coordinates"));
    }

    let access_mode = req.access_mode.as_deref().unwrap_or("foot").to_lowercase();
    let Some(&access_idx) = state.mode_lookup.get(access_mode.as_str()) else {
        return Err(bad_request(&format!(
            "access_mode='{}' is not a loaded mode (add it with --modes, or drop the field to use foot)",
            access_mode
        )));
    };
    let access_mode_data: &ModeData = &state.modes[access_idx as usize];

    let (access_default_radius, access_default_stops, _access_speed) =
        default_access_params(&access_mode);

    // Same legacy_walk semantics as the pre-#120 path: applies when the
    // per-side override is absent and the mode is foot. We don't yet
    // know egress_mode here, so we honour it whenever access_mode is
    // foot. The egress side reapplies the same logic.
    let legacy_walk = req.max_walk_m.filter(|_| access_mode == "foot");

    let max_access_m = req
        .max_access_m
        .or(legacy_walk)
        .unwrap_or_else(|| {
            if access_mode == "foot" {
                transit.config.max_walk_m.max(access_default_radius)
            } else {
                access_default_radius
            }
        })
        .min(60_000); // Absolute safety cap.

    let max_access_stops = req
        .max_access_stops
        .or_else(|| {
            let cfg = transit.config.max_access_stops;
            if cfg == 0 { None } else { Some(cfg) }
        })
        .unwrap_or(access_default_stops)
        .clamp(1, 500);

    let walk_speed_mps = req.walk_speed_mps.unwrap_or(1.3);
    if !(0.3..=3.0).contains(&walk_speed_mps) {
        return Err(bad_request("walk_speed_mps must be in 0.3..3.0"));
    }

    let snapshot = transit.snapshot();
    let timetable = snapshot.timetable.as_ref();
    if timetable.n_stops() == 0 {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "timetable has zero stops".to_string(),
            }),
        ));
    }

    // R-tree candidate-stop lookup — shared across same-origin queries.
    let stop_index = snapshot.stop_index.as_ref();
    let access_candidates = stop_index.k_nearest(
        req.origin_lon,
        req.origin_lat,
        max_access_m,
        max_access_stops,
    );
    if access_candidates.is_empty() {
        return Err(not_found(&format!(
            "no transit stops within max_access_m ({max_access_m} m, mode={access_mode}) of origin"
        )));
    }

    let origin_ranks = snap_stop_ranks_on_mode(&access_candidates, timetable, state, access_idx);
    let origin_source_rank = snap_to_rank(req.origin_lon, req.origin_lat, state, access_idx)
        .ok_or_else(|| {
            not_found(&format!(
                "origin could not snap to the {access_mode} network"
            ))
        })?;

    // Access 1-to-N via the distance-only CchQuery (#103). Reuses the
    // thread-local `CCH_QUERY_STATE` with O(1) per-query reset.
    let access_query = CchQuery::with_custom_weights(
        &access_mode_data.cch_topo,
        &access_mode_data.down_rev,
        &access_mode_data.cch_weights,
    );
    let origin_to_stop_ranks: Vec<u32> = origin_ranks.iter().filter_map(|r| *r).collect();
    let origin_to_stop_map: Vec<usize> = origin_ranks
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.map(|_| i))
        .collect();
    let origin_ds: Vec<u32> = access_query
        .distances_one_to_many(origin_source_rank, &origin_to_stop_ranks)
        .into_iter()
        .map(|d| d.unwrap_or(u32::MAX))
        .collect();

    // Per-stop walk seconds (raw, not yet filtered by max_access_s).
    // Dedup: keep the fastest path when multiple candidate snaps land
    // on the same stop.
    let mut walks: HashMap<StopIdx, u32> = HashMap::new();
    for (k, idx) in origin_to_stop_map.iter().enumerate() {
        let raw = origin_ds[k];
        if raw == u32::MAX {
            continue;
        }
        let walk_s = raw.div_ceil(10);
        let stop = access_candidates[*idx].0;
        let keep = walks
            .get(&stop)
            .map(|&existing| walk_s < existing)
            .unwrap_or(true);
        if keep {
            walks.insert(stop, walk_s);
        }
    }
    // Stable order: sort by stop index so two queries from the same
    // origin produce byte-identical RAPTOR sources downstream.
    let mut origin_walks: Vec<(StopIdx, u32)> = walks.into_iter().collect();
    origin_walks.sort_by_key(|(s, _)| *s);

    Ok(AccessContext {
        access_mode,
        access_idx,
        max_access_m,
        max_access_stops,
        walk_speed_mps,
        origin_lon: req.origin_lon,
        origin_lat: req.origin_lat,
        origin_walks,
    })
}

/// Run RAPTOR + egress + response build for a single query, reusing
/// the supplied [`AccessContext`] (#120). The caller is responsible
/// for ensuring the context belongs to the same origin / access_mode /
/// radius / max_stops / walk_speed as `req` — the bulk handler
/// guarantees this via `OriginGroupKey`.
pub fn compute_transit_journey_with_access(
    state: &ServerState,
    req: &TransitRequest,
    access: &AccessContext,
) -> Result<TransitResponse, (StatusCode, Json<ErrorResponse>)> {
    let Some(transit) = state.transit.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "transit subsystem is not loaded (no transit/ directory)".to_string(),
            }),
        ));
    };

    // Per-query destination validation (mirrors the pre-#120 single
    // function: a bad dest is rejected without affecting any other
    // query in the same bulk group).
    if !(-180.0..=180.0).contains(&req.dest_lon) || !(-90.0..=90.0).contains(&req.dest_lat) {
        return Err(bad_request("invalid coordinates"));
    }

    let access_mode = access.access_mode.as_str();
    let access_idx = access.access_idx;

    let egress_mode = req.egress_mode.as_deref().unwrap_or("foot").to_lowercase();
    let geometry_full = match req.geometry.as_deref() {
        None | Some("") | Some("straight") => false,
        Some("full") => true,
        Some(other) => {
            return Err(bad_request(&format!(
                "geometry='{}' is invalid; accepted values are 'straight' (default) or 'full'",
                other
            )));
        }
    };

    // foot is required to be a loaded mode — it's the inter-stop
    // walking transfer base. Also used as the mode for routed
    // middle-walk polylines under `geometry=full` (#121).
    let Some(&foot_idx) = state.mode_lookup.get("foot") else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "foot mode is required for /transit (inter-stop transfers are always foot). Load it with --modes foot"
                    .to_string(),
            }),
        ));
    };

    let Some(&egress_idx) = state.mode_lookup.get(egress_mode.as_str()) else {
        return Err(bad_request(&format!(
            "egress_mode='{}' is not a loaded mode",
            egress_mode
        )));
    };
    let egress_mode_data: &ModeData = &state.modes[egress_idx as usize];

    let (egress_default_radius, egress_default_stops, _egress_speed) =
        default_access_params(&egress_mode);

    // legacy_walk takes effect when *either* mode is foot — same as
    // the pre-#120 single function.
    let legacy_walk = req
        .max_walk_m
        .filter(|_| access_mode == "foot" || egress_mode == "foot");

    let max_access_m = access.max_access_m;
    let max_egress_m = req
        .max_egress_m
        .or_else(|| {
            if egress_mode == "foot" {
                legacy_walk
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            if egress_mode == "foot" {
                transit.config.max_walk_m.max(egress_default_radius)
            } else {
                egress_default_radius
            }
        })
        .min(60_000);

    // Mirror the pre-#120 max_access_stops resolution: query-level
    // override > config > max(access_default, egress_default).
    let max_access_stops = req
        .max_access_stops
        .or_else(|| {
            let cfg = transit.config.max_access_stops;
            if cfg == 0 { None } else { Some(cfg) }
        })
        .unwrap_or_else(|| access.max_access_stops.max(egress_default_stops))
        .clamp(1, 500);

    let walk_speed_mps = access.walk_speed_mps;

    let depart_s = parse_depart(req.depart.as_deref().unwrap_or("08:00:00"))
        .map_err(|e| bad_request(&format!("invalid depart: {e}")))?;

    let snapshot = transit.snapshot();
    let timetable = snapshot.timetable.as_ref();
    let transfers = snapshot.transfers.as_ref();
    let stop_index = snapshot.stop_index.as_ref();

    let egress_candidates =
        stop_index.k_nearest(req.dest_lon, req.dest_lat, max_egress_m, max_access_stops);
    if egress_candidates.is_empty() {
        return Err(not_found(&format!(
            "no transit stops within max_egress_m ({max_egress_m} m, mode={egress_mode}) of destination"
        )));
    }

    let dest_ranks = snap_stop_ranks_on_mode(&egress_candidates, timetable, state, egress_idx);
    let dest_source_rank =
        snap_to_rank(req.dest_lon, req.dest_lat, state, egress_idx).ok_or_else(|| {
            not_found(&format!(
                "destination could not snap to the {egress_mode} network"
            ))
        })?;

    // Egress 1-to-N via distance-only CchQuery. K sources × 1 target.
    let egress_query = CchQuery::with_custom_weights(
        &egress_mode_data.cch_topo,
        &egress_mode_data.down_rev,
        &egress_mode_data.cch_weights,
    );
    let egress_sources: Vec<u32> = egress_candidates
        .iter()
        .zip(dest_ranks.iter())
        .filter_map(|(_, r)| *r)
        .collect();
    let egress_idx_map: Vec<usize> = dest_ranks
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.map(|_| i))
        .collect();
    let egress_ds: Vec<u32> = egress_sources
        .iter()
        .map(|&src| {
            egress_query
                .distance(src, dest_source_rank)
                .unwrap_or(u32::MAX)
        })
        .collect();

    // Per-mode upper bound on access/egress walking time. Identical
    // formula to the pre-#120 path; depends on both modes so it lives
    // in the per-query path.
    let max_access_s = {
        let (_, _, access_speed) = default_access_params(access_mode);
        let (_, _, egress_speed) = default_access_params(&egress_mode);
        let a = ((max_access_m as f64) / access_speed) as u32 + 60;
        let e = ((max_egress_m as f64) / egress_speed) as u32 + 60;
        a.max(e)
    };
    let max_access_s = if access_mode == "foot" && egress_mode == "foot" {
        ((max_access_m.max(max_egress_m) as f64) / walk_speed_mps) as u32 + 60
    } else {
        max_access_s
    };

    // Materialise per-query RAPTOR sources from the cached origin walks
    // by applying the per-query `max_access_s` cap and folding in the
    // per-query `depart_s`. Dedup is already done in the access context
    // build, so the only filter here is the cap.
    let mut sources_for_raptor: Vec<(StopIdx, u32)> = Vec::with_capacity(access.origin_walks.len());
    let mut origin_walk_s: HashMap<StopIdx, u32> =
        HashMap::with_capacity(access.origin_walks.len());
    for &(stop, walk_s) in &access.origin_walks {
        if walk_s > max_access_s {
            continue;
        }
        origin_walk_s.insert(stop, walk_s);
        sources_for_raptor.push((stop, depart_s.saturating_add(walk_s)));
    }

    let mut target_weights: HashMap<StopIdx, u32> = HashMap::new();
    for (k, idx) in egress_idx_map.iter().enumerate() {
        let raw = egress_ds[k];
        if raw == u32::MAX {
            continue;
        }
        let walk_s = raw.div_ceil(10);
        if walk_s > max_access_s {
            continue;
        }
        let stop = egress_candidates[*idx].0;
        let keep = target_weights
            .get(&stop)
            .map(|&existing| walk_s < existing)
            .unwrap_or(true);
        if keep {
            target_weights.insert(stop, walk_s);
        }
    }

    if sources_for_raptor.is_empty() {
        return Err(not_found("no access stops reachable within walking time"));
    }
    if target_weights.is_empty() {
        return Err(not_found("no egress stops reachable within walking time"));
    }

    let query = RaptorQuery {
        sources: &sources_for_raptor,
        target_weights: &target_weights,
    };
    let Some(journey) = run_raptor(timetable, transfers, &query) else {
        return Err(not_found("no transit journey found"));
    };

    // Build response.
    let egress_walk = *target_weights.get(&journey.final_stop).unwrap_or(&0);
    let origin_walk = *origin_walk_s.get(&journey.origin_stop).unwrap_or(&0);

    let total_arrival_s = journey.arrival_time.saturating_add(egress_walk);
    let total_duration_s = total_arrival_s.saturating_sub(depart_s);

    // +2 covers the access and egress legs wrapping the RAPTOR-found
    // inner legs. Pre-sized so the hot-path response build never
    // reallocates (#118).
    let mut legs: Vec<TransitLegOut> = Vec::with_capacity(journey.legs.len() + 2);

    // Leg 0: access leg from origin to the first transit boarding stop,
    // labelled with the access mode.
    let first_stop = &timetable.stops[journey.origin_stop as usize];
    let access_from = [access.origin_lon, access.origin_lat];
    let access_to = [first_stop.lon, first_stop.lat];
    let (access_distance_m, access_geometry) = if geometry_full {
        match build_routed_road_leg(
            state,
            access_idx,
            access_from[0],
            access_from[1],
            access_to[0],
            access_to[1],
        ) {
            Some((poly, dist)) => (dist, Some(poly)),
            None => (
                haversine_m(access_from[0], access_from[1], access_to[0], access_to[1]) as u32,
                None,
            ),
        }
    } else {
        (
            haversine_m(access_from[0], access_from[1], access_to[0], access_to[1]) as u32,
            None,
        )
    };
    legs.push(road_leg(
        access_mode,
        access_from,
        access_to,
        origin_walk,
        access_distance_m,
        access_geometry,
    ));

    // Middle legs: decode the RaptorJourney.
    for leg in &journey.legs {
        match leg {
            RaptorLeg::Walk {
                from_stop,
                to_stop,
                duration_s,
            } => {
                let f = &timetable.stops[*from_stop as usize];
                let t = &timetable.stops[*to_stop as usize];
                // Routed polyline for middle walking transfers (#121).
                // Skip when geometry=straight, when duration_s == 0
                // (same-station / synthetic injected edge with no
                // road-network counterpart), or when the foot CCH
                // can't bridge the two stops — fall back to straight
                // line + haversine in those cases.
                let (distance_m, geometry) = if geometry_full && *duration_s > 0 {
                    match build_routed_road_leg(state, foot_idx, f.lon, f.lat, t.lon, t.lat) {
                        Some((poly, dist)) => (dist, Some(poly)),
                        None => (haversine_m(f.lon, f.lat, t.lon, t.lat) as u32, None),
                    }
                } else {
                    (haversine_m(f.lon, f.lat, t.lon, t.lat) as u32, None)
                };
                legs.push(TransitLegOut::Walk {
                    from: [f.lon, f.lat],
                    to: [t.lon, t.lat],
                    duration_s: *duration_s,
                    distance_m,
                    geometry,
                });
            }
            RaptorLeg::Ride {
                route,
                trip_in_route: _,
                from_stop,
                to_stop,
                board_time,
                alight_time,
            } => {
                let meta = &timetable.route_meta[*route as usize];
                let f = &timetable.stops[*from_stop as usize];
                let t = &timetable.stops[*to_stop as usize];
                legs.push(TransitLegOut::Transit {
                    from_stop_id: f.id.clone(),
                    from_stop_name: f.name.clone(),
                    from: [f.lon, f.lat],
                    to_stop_id: t.id.clone(),
                    to_stop_name: t.name.clone(),
                    to: [t.lon, t.lat],
                    board_time: format_hms(*board_time),
                    alight_time: format_hms(*alight_time),
                    duration_s: alight_time.saturating_sub(*board_time),
                    route_short_name: meta.short_name.clone(),
                    route_long_name: meta.long_name.clone(),
                    headsign: meta.headsign.clone(),
                });
            }
        }
    }

    // Final leg: egress from the last stop to the destination, labelled
    // with the egress mode.
    let last_stop = &timetable.stops[journey.final_stop as usize];
    let egress_from = [last_stop.lon, last_stop.lat];
    let egress_to = [req.dest_lon, req.dest_lat];
    let (egress_distance_m, egress_geometry) = if geometry_full {
        match build_routed_road_leg(
            state,
            egress_idx,
            egress_from[0],
            egress_from[1],
            egress_to[0],
            egress_to[1],
        ) {
            Some((poly, dist)) => (dist, Some(poly)),
            None => (
                haversine_m(egress_from[0], egress_from[1], egress_to[0], egress_to[1]) as u32,
                None,
            ),
        }
    } else {
        (
            haversine_m(egress_from[0], egress_from[1], egress_to[0], egress_to[1]) as u32,
            None,
        )
    };
    legs.push(road_leg(
        &egress_mode,
        egress_from,
        egress_to,
        egress_walk,
        egress_distance_m,
        egress_geometry,
    ));

    Ok(TransitResponse {
        origin: [access.origin_lon, access.origin_lat],
        destination: [req.dest_lon, req.dest_lat],
        depart_time: format_hms(depart_s),
        arrival_time: format_hms(total_arrival_s),
        total_duration_s,
        access_mode: access.access_mode.clone(),
        egress_mode,
        legs,
    })
}

// =====================================================================
// Bulk endpoint: POST /transit/bulk (issue #105, #120)
// =====================================================================

/// Request body for `POST /transit/bulk`. Carries a batch of
/// independent transit queries and an optional per-batch override
/// of the per-query parameters (applied as defaults to each query
/// that doesn't set them explicitly).
#[derive(Debug, Deserialize)]
pub struct TransitBulkRequest {
    pub queries: Vec<TransitRequest>,
    /// Optional per-batch default: passed down to any query that
    /// omits its own `max_walk_m`.
    #[serde(default)]
    pub max_walk_m: Option<u32>,
    /// Optional per-batch default: passed down to any query that
    /// omits its own `access_mode`.
    #[serde(default)]
    pub access_mode: Option<String>,
    /// Optional per-batch default: passed down to any query that
    /// omits its own `egress_mode`.
    #[serde(default)]
    pub egress_mode: Option<String>,
}

/// One result slot in a bulk response. Either a successful
/// [`TransitResponse`] or a machine-readable error with HTTP status.
#[derive(Debug, Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum TransitBulkResult {
    Ok { journey: Box<TransitResponse> },
    Err { status: u16, error: String },
}

#[derive(Debug, Serialize)]
pub struct TransitBulkResponse {
    pub count: usize,
    pub results: Vec<TransitBulkResult>,
}

/// Origin grouping key for `/transit/bulk` (#120).
///
/// Two queries that hash to the same key share the same access fan-out
/// (origin snap + R-tree candidate stops + access CCH 1-to-N). The
/// coordinate is quantised to 6 decimals (~11 cm) so two queries from
/// literally the same origin point group, while two queries 2 m apart
/// — which would produce a *different* origin snap and hence a
/// different access tree — get their own group.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
struct OriginGroupKey {
    /// Longitude × 1e6, rounded.
    lon_q6: i64,
    /// Latitude × 1e6, rounded.
    lat_q6: i64,
    /// Lowercased access mode name.
    access_mode: String,
    /// Resolved access radius (or `u32::MAX` sentinel for "use default").
    /// We use the request value directly so that two queries with
    /// different overrides go to different groups even if the resolved
    /// value would coincide.
    max_access_m: Option<u32>,
    /// Same idea for `max_access_stops`.
    max_access_stops: Option<usize>,
    /// Walking speed quantised to centi-mm/s (1e-5 m/s) so two queries
    /// with the same `walk_speed_mps` literal hash to the same key.
    walk_speed_centimps: Option<u32>,
    /// `max_walk_m` participates because it is folded into the access
    /// radius via the legacy alias path.
    max_walk_m: Option<u32>,
}

impl OriginGroupKey {
    fn from_request(req: &TransitRequest) -> Self {
        let access_mode = req.access_mode.as_deref().unwrap_or("foot").to_lowercase();
        let walk_speed_centimps = req.walk_speed_mps.map(|v| (v * 100_000.0).round() as u32);
        Self {
            lon_q6: (req.origin_lon * 1_000_000.0).round() as i64,
            lat_q6: (req.origin_lat * 1_000_000.0).round() as i64,
            access_mode,
            max_access_m: req.max_access_m,
            max_access_stops: req.max_access_stops,
            walk_speed_centimps,
            max_walk_m: req.max_walk_m,
        }
    }
}

/// `POST /transit/bulk` — batch multimodal routing.
///
/// Runs every query in the batch in parallel via Rayon. Two performance
/// tricks compound here:
///
/// 1. **Origin grouping (#120)** — queries are grouped by
///    [`OriginGroupKey`] and the access fan-out (snap + R-tree + CCH
///    1-to-N, ~30–40 % of single-query cost) is computed once per
///    unique group, then shared across every query in the group via
///    [`compute_transit_journey_with_access`].
/// 2. **Thread-local scratch reuse** — each Rayon worker reuses its
///    `RAPTOR_STATE` and `CCH_QUERY_STATE` thread-locals across calls.
///
/// For a workload of N queries with M ≪ N unique origins, the access
/// phase amortises by a factor of N / M. For matrix-shaped workloads
/// (every origin distinct), grouping is a no-op and the overhead is
/// the cost of a single HashMap insert per query.
///
/// Validation runs **per query**, not per group — a malformed query in
/// a group still yields a typed `TransitBulkResult::Err` instead of
/// poisoning the rest.
///
/// Cancellation: if the client disconnects, Axum drops the handler
/// future. This is not yet plumbed through to a cooperative
/// per-query cancellation flag — a follow-up.
pub async fn transit_bulk_handler(
    State(state): State<Arc<ServerState>>,
    Json(req): Json<TransitBulkRequest>,
) -> Result<Json<TransitBulkResponse>, (StatusCode, Json<ErrorResponse>)> {
    // Soft cap on batch size. 100k is generous for interactive use;
    // operators doing matrix-style work should look at `/table/stream`
    // for the road side and batch transit in chunks of ~10k.
    const MAX_BATCH: usize = 100_000;
    if req.queries.len() > MAX_BATCH {
        return Err((
            StatusCode::PAYLOAD_TOO_LARGE,
            Json(ErrorResponse {
                error: format!(
                    "bulk batch size {} exceeds MAX_BATCH {MAX_BATCH}",
                    req.queries.len()
                ),
            }),
        ));
    }
    if state.transit.is_none() {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "transit subsystem is not loaded".to_string(),
            }),
        ));
    }

    // Apply per-batch defaults to every query that omits the field.
    let batch_max_walk_m = req.max_walk_m;
    let batch_access_mode = req.access_mode.clone();
    let batch_egress_mode = req.egress_mode.clone();
    let mut queries = req.queries;
    for q in &mut queries {
        if q.max_walk_m.is_none()
            && let Some(m) = batch_max_walk_m
        {
            q.max_walk_m = Some(m);
        }
        if q.access_mode.is_none()
            && let Some(ref s) = batch_access_mode
        {
            q.access_mode = Some(s.clone());
        }
        if q.egress_mode.is_none()
            && let Some(ref s) = batch_egress_mode
        {
            q.egress_mode = Some(s.clone());
        }
    }
    let count = queries.len();

    // Move the actual work off the async executor onto the Rayon
    // thread pool via `spawn_blocking` — single-query transit work
    // is pure CPU and non-trivially long, so holding a Tokio worker
    // for the whole batch is wrong.
    let state_clone = Arc::clone(&state);
    let results: Vec<TransitBulkResult> =
        tokio::task::spawn_blocking(move || run_bulk(state_clone.as_ref(), &queries))
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    Json(ErrorResponse {
                        error: format!("bulk task panicked: {e}"),
                    }),
                )
            })?;

    Ok(Json(TransitBulkResponse { count, results }))
}

/// Synchronous core of `transit_bulk_handler`. Exposed so tests can
/// drive the grouped path without an axum runtime.
///
/// Groups queries by [`OriginGroupKey`], computes one
/// [`AccessContext`] per unique group in parallel, then runs every
/// query through [`compute_transit_journey_with_access`] reusing the
/// cached context for its group.
pub fn run_bulk(state: &ServerState, queries: &[TransitRequest]) -> Vec<TransitBulkResult> {
    use rayon::prelude::*;

    // 1. Group queries by origin key. Each entry holds the list of
    //    indices that share the same access fan-out.
    let mut groups: HashMap<OriginGroupKey, Vec<usize>> = HashMap::new();
    for (i, q) in queries.iter().enumerate() {
        groups
            .entry(OriginGroupKey::from_request(q))
            .or_default()
            .push(i);
    }

    // 2. Compute one AccessContext per unique group, in parallel.
    //    On error (bad origin, unsnappable mode, …) the group's queries
    //    will all surface that error in step 3 — we record it once.
    //
    //    We pick an arbitrary representative query per group (the first
    //    one) to drive the access build. All queries in the group share
    //    the same key, so any of them works.
    type GroupResult = Result<AccessContext, (StatusCode, ErrorResponse)>;
    let group_results: HashMap<OriginGroupKey, GroupResult> = groups
        .par_iter()
        .map(|(key, idxs)| {
            let rep = &queries[idxs[0]];
            let res = compute_access_context(state, rep).map_err(|(sc, json)| (sc, json.0));
            (key.clone(), res)
        })
        .collect();

    // 3. Run every query through compute_transit_journey_with_access
    //    using its group's cached AccessContext. If the access build
    //    failed for the group, every query in it returns that same
    //    error.
    queries
        .par_iter()
        .map(|q| {
            let key = OriginGroupKey::from_request(q);
            match group_results.get(&key) {
                Some(Ok(ctx)) => match compute_transit_journey_with_access(state, q, ctx) {
                    Ok(resp) => TransitBulkResult::Ok {
                        journey: Box::new(resp),
                    },
                    Err((status, err)) => TransitBulkResult::Err {
                        status: status.as_u16(),
                        error: err.0.error,
                    },
                },
                Some(Err((status, err))) => TransitBulkResult::Err {
                    status: status.as_u16(),
                    error: err.error.clone(),
                },
                None => TransitBulkResult::Err {
                    // Should be unreachable — every query inserted itself
                    // into `groups`. Defensive fallback.
                    status: StatusCode::INTERNAL_SERVER_ERROR.as_u16(),
                    error: "bulk grouping bug: no access context for query".to_string(),
                },
            }
        })
        .collect()
}

fn bad_request(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::BAD_REQUEST,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn not_found(msg: &str) -> (StatusCode, Json<ErrorResponse>) {
    (
        StatusCode::NOT_FOUND,
        Json(ErrorResponse {
            error: msg.to_string(),
        }),
    )
}

fn parse_depart(s: &str) -> Result<u32, String> {
    // Accept HH:MM or HH:MM:SS, or full ISO "YYYY-MM-DDTHH:MM:SS".
    let trimmed = s.trim();
    let time_part = if let Some((_, t)) = trimmed.split_once('T') {
        t
    } else {
        trimmed
    };
    let mut it = time_part.split(':');
    let hh: u32 = it
        .next()
        .ok_or_else(|| "missing hour".to_string())?
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    let mm: u32 = it
        .next()
        .ok_or_else(|| "missing minute".to_string())?
        .parse()
        .map_err(|e: std::num::ParseIntError| e.to_string())?;
    let ss: u32 = match it.next() {
        Some(s) => s
            .parse()
            .map_err(|e: std::num::ParseIntError| e.to_string())?,
        None => 0,
    };
    if hh >= 48 || mm >= 60 || ss >= 60 {
        return Err("out-of-range time component".to_string());
    }
    Ok(hh * 3600 + mm * 60 + ss)
}

fn format_hms(secs: u32) -> String {
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{:02}:{:02}:{:02}", h, m, s)
}

/// Snap each candidate stop to the given mode's CCH rank. Returns one
/// entry per input candidate — `None` if the stop can't snap on that
/// mode's network (e.g. a bus stop on a restricted lane that foot can't
/// walk to, or a pedestrian-only plaza that car can't drive on).
fn snap_stop_ranks_on_mode(
    candidates: &[(StopIdx, f64)],
    tt: &Timetable,
    state: &ServerState,
    mode_idx: u8,
) -> Vec<Option<u32>> {
    candidates
        .iter()
        .map(|(s, _)| {
            let stop = &tt.stops[*s as usize];
            let r = snap_to_rank(stop.lon, stop.lat, state, mode_idx);
            if r.is_none() {
                tracing::debug!(
                    stop_id = &*stop.id,
                    mode = mode_idx,
                    "stop failed snap for this mode"
                );
            }
            r
        })
        .collect()
}

/// Snap a (lon,lat) to the given mode's CCH and return the rank.
///
/// Uses the per-mode spatial index (#116) when available — a single
/// R-tree walk with no rejection loop. Falls back to the global index
/// with mask filtering if the mode wasn't pre-indexed (shouldn't
/// happen in production but keeps the code safe against misconfiguration).
/// Compute the routed (polyline, distance_m) for a single access or
/// egress leg in the given mode (#114). Returns `None` if either
/// endpoint fails to snap or the CCH query reports no route. On any
/// kind of failure the caller falls back to the straight-line leg
/// shape — the journey duration from RAPTOR is already final, so the
/// routed geometry is pure cosmetic metadata.
fn build_routed_road_leg(
    state: &ServerState,
    mode_idx: u8,
    from_lon: f64,
    from_lat: f64,
    to_lon: f64,
    to_lat: f64,
) -> Option<(Vec<[f64; 2]>, u32)> {
    let src_rank = snap_to_rank(from_lon, from_lat, state, mode_idx)?;
    let dst_rank = snap_to_rank(to_lon, to_lat, state, mode_idx)?;
    if src_rank == dst_rank {
        return Some((vec![[from_lon, from_lat], [to_lon, to_lat]], 0));
    }
    let mode_data = &state.modes[mode_idx as usize];
    let query = CchQuery::with_custom_weights(
        &mode_data.cch_topo,
        &mode_data.down_rev,
        &mode_data.cch_weights,
    );
    let result = query.query(src_rank, dst_rank)?;
    let rank_path = unpack_path(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &result.forward_parent,
        &result.backward_parent,
        src_rank,
        dst_rank,
        result.meeting_node,
    );
    let ebg_path: Vec<u32> = rank_path
        .iter()
        .map(|&rank| {
            let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            mode_data.filtered_ebg.filtered_to_original[filtered_id as usize]
        })
        .collect();
    let (points, distance_m) = build_raw_points(&ebg_path, &state.ebg_nodes, &state.nbg_geo);
    if points.is_empty() {
        return None;
    }
    let polyline: Vec<[f64; 2]> = points.into_iter().map(|p| [p.lon, p.lat]).collect();
    Some((polyline, distance_m.round() as u32))
}

fn snap_to_rank(lon: f64, lat: f64, state: &ServerState, mode_idx: u8) -> Option<u32> {
    let mode_data = &state.modes[mode_idx as usize];
    let orig = match state.mode_spatial_indexes.get(&mode_idx) {
        Some(mode_index) => mode_index.snap_unfiltered(lon, lat)?,
        None => state.spatial_index.snap(lon, lat, &mode_data.mask, 10)?,
    };
    let filtered = mode_data.filtered_ebg.original_to_filtered[orig as usize];
    if filtered == u32::MAX {
        return None;
    }
    Some(mode_data.order.perm[filtered as usize])
}
