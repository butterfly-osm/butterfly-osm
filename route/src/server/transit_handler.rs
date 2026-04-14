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
//!      using the selected mode's CCH (1-to-N via `table_bucket_parallel`).
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

use crate::matrix::bucket_ch::table_bucket_parallel;
use crate::server::state::ModeData;
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
#[derive(Debug, Deserialize)]
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
    },
    /// Driving leg — an access or egress leg whose road mode is `car`.
    Drive {
        from: [f64; 2],
        to: [f64; 2],
        duration_s: u32,
        distance_m: u32,
    },
    /// Generic road leg for any non-foot, non-car mode (`bike`, `truck`…).
    /// The `mode` field carries the loaded mode's name.
    Road {
        mode: String,
        from: [f64; 2],
        to: [f64; 2],
        duration_s: u32,
        distance_m: u32,
    },
    Transit {
        from_stop_id: String,
        from_stop_name: String,
        from: [f64; 2],
        to_stop_id: String,
        to_stop_name: String,
        to: [f64; 2],
        board_time: String,
        alight_time: String,
        duration_s: u32,
        route_short_name: String,
        route_long_name: String,
        headsign: String,
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
) -> TransitLegOut {
    match mode {
        "foot" => TransitLegOut::Walk {
            from,
            to,
            duration_s,
            distance_m,
        },
        "car" => TransitLegOut::Drive {
            from,
            to,
            duration_s,
            distance_m,
        },
        other => TransitLegOut::Road {
            mode: other.to_string(),
            from,
            to,
            duration_s,
            distance_m,
        },
    }
}

pub async fn transit_handler(
    State(state): State<Arc<ServerState>>,
    Query(req): Query<TransitRequest>,
) -> Result<Json<TransitResponse>, (StatusCode, Json<ErrorResponse>)> {
    let Some(transit) = state.transit.as_ref() else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "transit subsystem is not loaded (no transit/ directory)".to_string(),
            }),
        ));
    };

    // Input validation.
    if !(-180.0..=180.0).contains(&req.origin_lon)
        || !(-90.0..=90.0).contains(&req.origin_lat)
        || !(-180.0..=180.0).contains(&req.dest_lon)
        || !(-90.0..=90.0).contains(&req.dest_lat)
    {
        return Err(bad_request("invalid coordinates"));
    }

    // Resolve access / egress road modes. Default BOTH sides to "foot"
    // when unspecified. Notably, when `access_mode=car` is passed but
    // `egress_mode` is omitted, we still default egress to foot — the
    // real park-and-ride pattern is "drive to station, walk to office",
    // not "drive from the destination side too". Symmetric-drive users
    // can explicitly set `egress_mode=car`.
    //
    // The transit subsystem always requires `foot` as a loaded mode
    // because the inter-stop walking transfer graph is foot-only — but
    // access and egress are independent and can use any loaded road mode.
    let access_mode = req.access_mode.as_deref().unwrap_or("foot").to_lowercase();
    let egress_mode = req.egress_mode.as_deref().unwrap_or("foot").to_lowercase();

    let Some(&foot_idx) = state.mode_lookup.get("foot") else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "foot mode is required for /transit (inter-stop transfers are always foot). Load it with --modes foot"
                    .to_string(),
            }),
        ));
    };
    let foot = &state.modes[foot_idx as usize];

    let Some(&access_idx) = state.mode_lookup.get(access_mode.as_str()) else {
        return Err(bad_request(&format!(
            "access_mode='{}' is not a loaded mode (add it with --modes, or drop the field to use foot)",
            access_mode
        )));
    };
    let access_mode_data: &ModeData = &state.modes[access_idx as usize];
    let Some(&egress_idx) = state.mode_lookup.get(egress_mode.as_str()) else {
        return Err(bad_request(&format!(
            "egress_mode='{}' is not a loaded mode",
            egress_mode
        )));
    };
    let egress_mode_data: &ModeData = &state.modes[egress_idx as usize];

    let (access_default_radius, access_default_stops, _access_speed) =
        default_access_params(&access_mode);
    let (egress_default_radius, egress_default_stops, _egress_speed) =
        default_access_params(&egress_mode);

    // Backward-compat: `max_walk_m` is an alias that applies only when
    // the corresponding per-side override is absent AND the mode is foot.
    let legacy_walk = req
        .max_walk_m
        .filter(|_| access_mode == "foot" || egress_mode == "foot");

    let max_access_m = req
        .max_access_m
        .or_else(|| {
            if access_mode == "foot" {
                legacy_walk
            } else {
                None
            }
        })
        .unwrap_or_else(|| {
            if access_mode == "foot" {
                transit.config.max_walk_m.max(access_default_radius)
            } else {
                access_default_radius
            }
        })
        .min(60_000); // Absolute safety cap.

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

    // Precedence: query param > transit.toml value > per-mode default.
    // A config value of 0 is treated as "use the per-mode default"
    // (see issue #110 — the config knob was previously dead).
    let max_access_stops = req
        .max_access_stops
        .or_else(|| {
            let cfg = transit.config.max_access_stops;
            if cfg == 0 { None } else { Some(cfg) }
        })
        .unwrap_or_else(|| access_default_stops.max(egress_default_stops))
        .clamp(1, 500);

    let walk_speed_mps = req.walk_speed_mps.unwrap_or(1.3);
    if !(0.3..=3.0).contains(&walk_speed_mps) {
        return Err(bad_request("walk_speed_mps must be in 0.3..3.0"));
    }

    let depart_s = parse_depart(req.depart.as_deref().unwrap_or("08:00:00"))
        .map_err(|e| bad_request(&format!("invalid depart: {e}")))?;

    let snapshot = transit.snapshot();
    let timetable = snapshot.timetable.as_ref();
    let transfers = snapshot.transfers.as_ref();

    if timetable.n_stops() == 0 {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "timetable has zero stops".to_string(),
            }),
        ));
    }

    // Pick candidate stops by great-circle distance (fast prefilter).
    let access_candidates = candidate_stops(
        timetable,
        req.origin_lon,
        req.origin_lat,
        max_access_m,
        max_access_stops,
    );
    let egress_candidates = candidate_stops(
        timetable,
        req.dest_lon,
        req.dest_lat,
        max_egress_m,
        max_access_stops,
    );

    if access_candidates.is_empty() {
        return Err(not_found(&format!(
            "no transit stops within max_access_m ({max_access_m} m, mode={access_mode}) of origin"
        )));
    }
    if egress_candidates.is_empty() {
        return Err(not_found(&format!(
            "no transit stops within max_egress_m ({max_egress_m} m, mode={egress_mode}) of destination"
        )));
    }

    // Snap origin and destination + every candidate stop on the
    // *access/egress* mode graphs (not foot, unless the selected mode is
    // foot). A stop may be reachable by car but not foot (e.g. a highway
    // service area with a bus stop), or vice versa — we snap to whatever
    // mode the user requested.
    let origin_ranks =
        snap_stop_ranks_on_mode(&access_candidates, timetable, state.as_ref(), access_idx);
    let dest_ranks =
        snap_stop_ranks_on_mode(&egress_candidates, timetable, state.as_ref(), egress_idx);

    let origin_source_rank =
        snap_to_rank(req.origin_lon, req.origin_lat, state.as_ref(), access_idx).ok_or_else(
            || {
                not_found(&format!(
                    "origin could not snap to the {access_mode} network"
                ))
            },
        )?;
    let dest_source_rank = snap_to_rank(req.dest_lon, req.dest_lat, state.as_ref(), egress_idx)
        .ok_or_else(|| {
            not_found(&format!(
                "destination could not snap to the {egress_mode} network"
            ))
        })?;

    // Access 1-to-N CCH on the access mode.
    let access_n_nodes = access_mode_data.cch_topo.n_nodes as usize;
    let orig_sources = [origin_source_rank];
    let origin_to_stop_ranks: Vec<u32> = origin_ranks.iter().filter_map(|r| *r).collect();
    let origin_to_stop_map: Vec<usize> = origin_ranks
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.map(|_| i))
        .collect();
    let (origin_ds, _) = table_bucket_parallel(
        access_n_nodes,
        &access_mode_data.up_adj_flat,
        &access_mode_data.down_rev_flat,
        &orig_sources,
        &origin_to_stop_ranks,
    );

    // Egress 1-to-N CCH on the egress mode. Each egress stop is a
    // source, destination is the sole target.
    let egress_n_nodes = egress_mode_data.cch_topo.n_nodes as usize;
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
    let dest_targets = [dest_source_rank];
    let (egress_ds, _) = table_bucket_parallel(
        egress_n_nodes,
        &egress_mode_data.up_adj_flat,
        &egress_mode_data.down_rev_flat,
        &egress_sources,
        &dest_targets,
    );

    // A cheap-and-correct per-mode upper bound on the access time
    // derived from the radius and the mode's canonical speed. Add a
    // generous buffer for first-mile / last-mile variation.
    let max_access_s = {
        let (_, _, access_speed) = default_access_params(&access_mode);
        let (_, _, egress_speed) = default_access_params(&egress_mode);
        let a = ((max_access_m as f64) / access_speed) as u32 + 60;
        let e = ((max_egress_m as f64) / egress_speed) as u32 + 60;
        a.max(e)
    };
    // foot-only case: honour walk_speed_mps override if provided.
    let max_access_s = if access_mode == "foot" && egress_mode == "foot" {
        ((max_access_m.max(max_egress_m) as f64) / walk_speed_mps) as u32 + 60
    } else {
        max_access_s
    };
    // Suppress unused warning when foot is the default.
    let _ = foot;

    let mut sources_for_raptor: Vec<(StopIdx, u32)> = Vec::new();
    // Map stop_idx → walking seconds (for the response).
    let mut origin_walk_s: HashMap<StopIdx, u32> = HashMap::new();
    for (k, idx) in origin_to_stop_map.iter().enumerate() {
        let raw = origin_ds[k];
        if raw == u32::MAX {
            continue;
        }
        let walk_s = raw.div_ceil(10);
        if walk_s > max_access_s {
            continue;
        }
        let stop = access_candidates[*idx].0;
        let dep_at_stop = depart_s.saturating_add(walk_s);
        // If multiple access paths hit the same stop, keep the fastest.
        let keep = origin_walk_s
            .get(&stop)
            .map(|&existing| walk_s < existing)
            .unwrap_or(true);
        if keep {
            origin_walk_s.insert(stop, walk_s);
            sources_for_raptor.retain(|(s, _)| *s != stop);
            sources_for_raptor.push((stop, dep_at_stop));
        }
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

    let mut legs: Vec<TransitLegOut> = Vec::new();

    // Leg 0: access leg from origin to the first transit boarding stop,
    // labelled with the access mode.
    let first_stop = &timetable.stops[journey.origin_stop as usize];
    legs.push(road_leg(
        &access_mode,
        [req.origin_lon, req.origin_lat],
        [first_stop.lon, first_stop.lat],
        origin_walk,
        haversine_m(
            req.origin_lon,
            req.origin_lat,
            first_stop.lon,
            first_stop.lat,
        ) as u32,
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
                legs.push(TransitLegOut::Walk {
                    from: [f.lon, f.lat],
                    to: [t.lon, t.lat],
                    duration_s: *duration_s,
                    distance_m: haversine_m(f.lon, f.lat, t.lon, t.lat) as u32,
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
    legs.push(road_leg(
        &egress_mode,
        [last_stop.lon, last_stop.lat],
        [req.dest_lon, req.dest_lat],
        egress_walk,
        haversine_m(last_stop.lon, last_stop.lat, req.dest_lon, req.dest_lat) as u32,
    ));

    Ok(Json(TransitResponse {
        origin: [req.origin_lon, req.origin_lat],
        destination: [req.dest_lon, req.dest_lat],
        depart_time: format_hms(depart_s),
        arrival_time: format_hms(total_arrival_s),
        total_duration_s,
        access_mode,
        egress_mode,
        legs,
    }))
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

/// Candidate stops within `max_walk_m` of a point, up to `k` of them,
/// sorted by straight-line distance.
fn candidate_stops(
    tt: &Timetable,
    lon: f64,
    lat: f64,
    max_walk_m: u32,
    k: usize,
) -> Vec<(StopIdx, f64)> {
    let max_m = max_walk_m as f64;
    let mut v: Vec<(StopIdx, f64)> = Vec::new();
    for (i, stop) in tt.stops.iter().enumerate() {
        // Skip pure station parents (no trips touching them) — they're
        // covered via their children when those children are closer.
        if tt.routes_for_stop(i as StopIdx).is_empty() && stop.parent_station.is_none() {
            continue;
        }
        let d = haversine_m(lon, lat, stop.lon, stop.lat);
        if d <= max_m {
            v.push((i as StopIdx, d));
        }
    }
    v.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
    v.truncate(k);
    v
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
                    stop_id = stop.id.as_str(),
                    mode = mode_idx,
                    "stop failed snap for this mode"
                );
            }
            r
        })
        .collect()
}

/// Snap a (lon,lat) to the given mode's CCH and return the rank.
fn snap_to_rank(lon: f64, lat: f64, state: &ServerState, mode_idx: u8) -> Option<u32> {
    let mode_data = &state.modes[mode_idx as usize];
    let orig = state.spatial_index.snap(lon, lat, &mode_data.mask, 10)?;
    let filtered = mode_data.filtered_ebg.original_to_filtered[orig as usize];
    if filtered == u32::MAX {
        return None;
    }
    Some(mode_data.order.perm[filtered as usize])
}
