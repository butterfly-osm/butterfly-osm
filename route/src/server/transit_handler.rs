//! `GET /transit` — multimodal walk + transit routing.
//!
//! Flow:
//!   1. Snap the origin and destination to the foot CCH.
//!   2. Select up to `max_access_stops` nearest transit stops within
//!      `max_walk_m` of the origin, and similarly for the destination.
//!   3. Compute walking times from origin → each access stop, and from
//!      each egress stop → destination, using the foot CCH 1-to-N.
//!   4. Run RAPTOR with these walks as access/egress offsets.
//!   5. Reconstruct the journey and return it as JSON.

use std::collections::HashMap;
use std::sync::Arc;

use axum::{
    extract::{Query, State},
    http::StatusCode,
    Json,
};
use serde::{Deserialize, Serialize};

use crate::matrix::bucket_ch::table_bucket_parallel;
use crate::transit::gtfs::haversine_m;
use crate::transit::raptor::{run_raptor, RaptorLeg, RaptorQuery};
use crate::transit::timetable::{StopIdx, Timetable};

use super::state::ServerState;
use super::types::ErrorResponse;

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
    /// Max walking radius (meters) from origin / to destination.
    /// Default: from config or 1000 m.
    #[serde(default)]
    pub max_walk_m: Option<u32>,
    /// Max number of access / egress stops. Default: 20.
    #[serde(default)]
    pub max_access_stops: Option<usize>,
    /// Walking speed (m/s). Default: 1.3 (≈4.7 km/h).
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
    Walk {
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
    pub legs: Vec<TransitLegOut>,
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
    let max_walk_m = req
        .max_walk_m
        .unwrap_or(transit.config.max_walk_m)
        .min(5_000);
    let max_access_stops = req
        .max_access_stops
        .unwrap_or(transit.config.max_access_stops)
        .clamp(1, 200);
    let walk_speed_mps = req.walk_speed_mps.unwrap_or(1.3);
    if !(0.3..=3.0).contains(&walk_speed_mps) {
        return Err(bad_request("walk_speed_mps must be in 0.3..3.0"));
    }

    let depart_s = parse_depart(req.depart.as_deref().unwrap_or("08:00:00"))
        .map_err(|e| bad_request(&format!("invalid depart: {e}")))?;

    // Foot mode is required.
    let Some(&foot_idx) = state.mode_lookup.get("foot") else {
        return Err((
            StatusCode::SERVICE_UNAVAILABLE,
            Json(ErrorResponse {
                error: "foot mode is required for /transit (load it with --modes foot)".to_string(),
            }),
        ));
    };
    let foot = &state.modes[foot_idx as usize];

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
        max_walk_m,
        max_access_stops,
    );
    let egress_candidates = candidate_stops(
        timetable,
        req.dest_lon,
        req.dest_lat,
        max_walk_m,
        max_access_stops,
    );

    if access_candidates.is_empty() {
        return Err(not_found("no transit stops within max_walk_m of origin"));
    }
    if egress_candidates.is_empty() {
        return Err(not_found(
            "no transit stops within max_walk_m of destination",
        ));
    }

    // Snap origin and destination on the foot graph and compute walking
    // times to/from each candidate stop via the foot CCH.
    let origin_ranks = snap_stop_ranks(&access_candidates, timetable, state.as_ref(), foot_idx);
    let dest_ranks = snap_stop_ranks(&egress_candidates, timetable, state.as_ref(), foot_idx);

    let origin_source_rank =
        match snap_to_rank(req.origin_lon, req.origin_lat, state.as_ref(), foot_idx) {
            Some(r) => r,
            None => return Err(not_found("origin could not snap to the foot network")),
        };
    let dest_source_rank = match snap_to_rank(req.dest_lon, req.dest_lat, state.as_ref(), foot_idx)
    {
        Some(r) => r,
        None => return Err(not_found("destination could not snap to the foot network")),
    };

    let n_nodes = foot.cch_topo.n_nodes as usize;

    // Origin → access stops (forward walk).
    let orig_sources = [origin_source_rank];
    let origin_to_stop_ranks: Vec<u32> = origin_ranks.iter().filter_map(|r| *r).collect();
    let origin_to_stop_map: Vec<usize> = origin_ranks
        .iter()
        .enumerate()
        .filter_map(|(i, r)| r.map(|_| i))
        .collect();
    let (origin_ds, _) = table_bucket_parallel(
        n_nodes,
        &foot.up_adj_flat,
        &foot.down_rev_flat,
        &orig_sources,
        &origin_to_stop_ranks,
    );

    // Egress stops → destination: we query forward from each egress stop
    // to the destination (one row each). A single batched call uses
    // dest_source as a target.
    // To avoid building a separate reverse algorithm, we use the fact
    // that walking is symmetric on the foot network in practice; a
    // forward call from each egress stop to destination gives us the
    // correct time.
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
        n_nodes,
        &foot.up_adj_flat,
        &foot.down_rev_flat,
        &egress_sources,
        &dest_targets,
    );

    // Convert deciseconds → seconds (rounding up), apply max_walk_m cap
    // indirectly via the walking-time cap derived from max_walk_m /
    // walk_speed.
    let max_access_s = ((max_walk_m as f64) / walk_speed_mps) as u32 + 30;

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

    // Leg 0: walk from origin to the first transit boarding stop.
    let first_stop = &timetable.stops[journey.origin_stop as usize];
    legs.push(TransitLegOut::Walk {
        from: [req.origin_lon, req.origin_lat],
        to: [first_stop.lon, first_stop.lat],
        duration_s: origin_walk,
        distance_m: haversine_m(
            req.origin_lon,
            req.origin_lat,
            first_stop.lon,
            first_stop.lat,
        ) as u32,
    });

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

    // Final leg: walk from the last stop to the destination.
    let last_stop = &timetable.stops[journey.final_stop as usize];
    legs.push(TransitLegOut::Walk {
        from: [last_stop.lon, last_stop.lat],
        to: [req.dest_lon, req.dest_lat],
        duration_s: egress_walk,
        distance_m: haversine_m(last_stop.lon, last_stop.lat, req.dest_lon, req.dest_lat) as u32,
    });

    Ok(Json(TransitResponse {
        origin: [req.origin_lon, req.origin_lat],
        destination: [req.dest_lon, req.dest_lat],
        depart_time: format_hms(depart_s),
        arrival_time: format_hms(total_arrival_s),
        total_duration_s,
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

/// Snap each candidate stop to the foot CCH rank. Returns one entry
/// per input candidate — `None` if the stop can't snap.
fn snap_stop_ranks(
    candidates: &[(StopIdx, f64)],
    tt: &Timetable,
    state: &ServerState,
    foot_idx: u8,
) -> Vec<Option<u32>> {
    candidates
        .iter()
        .map(|(s, _)| {
            let stop = &tt.stops[*s as usize];
            let r = snap_to_rank(stop.lon, stop.lat, state, foot_idx);
            if r.is_none() {
                tracing::debug!(stop_id = stop.id.as_str(), "stop failed foot snap");
            }
            r
        })
        .collect()
}

/// Snap a (lon,lat) to the foot CCH and return the rank.
fn snap_to_rank(lon: f64, lat: f64, state: &ServerState, foot_idx: u8) -> Option<u32> {
    let foot = &state.modes[foot_idx as usize];
    let orig = state.spatial_index.snap(lon, lat, &foot.mask, 10)?;
    let filtered = foot.filtered_ebg.original_to_filtered[orig as usize];
    if filtered == u32::MAX {
        return None;
    }
    Some(foot.order.perm[filtered as usize])
}
