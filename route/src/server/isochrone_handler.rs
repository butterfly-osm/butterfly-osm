//! /isochrone and /isochrone/bulk handlers — reachability polygons

use axum::{
    Json,
    body::Body,
    extract::State,
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

use super::geometry::{
    GeometryFormat, IsochroneFlats, IsochroneQuery, IsochroneSnapError, Point, ReachModel,
    ThresholdMetric, encode_contour, isochrone_polygons, primary_outer_ring, reachable_polylines,
};
use super::query_context::QueryContext;
use super::regions::RegionsState;
use super::route::{default_direction, default_geometries};
use super::state::ServerState;
use super::types::{ErrorResponse, ValidatedJson, ValidatedQuery, parse_mode, validate_coord};
use crate::range::ContourPolygon;

// ============ Types ============

#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)] // #612: a parameter we cannot honour is refused, not ignored
pub struct IsochroneRequest {
    /// Center longitude
    #[schema(example = 4.3517)]
    pub lon: f64,
    /// Center latitude
    #[schema(example = 50.8503)]
    pub lat: f64,
    /// Time limit in seconds (1-7200). Mutually exclusive with contours.
    #[serde(default)]
    #[schema(example = 600)]
    pub time_s: Option<u32>,
    /// Multiple time contours as comma-separated seconds (e.g. "300,600,1200", max 10).
    /// Mutually exclusive with time_s.
    #[serde(default)]
    pub contours: Option<String>,
    /// Isodistance (#612): distance limit in METRES (1-100000) of road length
    /// accumulated along the time-shortest path — the same path `/route` and
    /// `/table` report, and the same metres. The one-contour form of
    /// `contours_m`. Mutually exclusive with `time_s` / `contours`.
    #[serde(default)]
    #[schema(example = json!(null))]
    pub distance_m: Option<u32>,
    /// Multiple isodistance contours as comma-separated metres (e.g.
    /// "5000,10000,20000", max 10). Mutually exclusive with `time_s` /
    /// `contours`.
    #[serde(default)]
    pub contours_m: Option<String>,
    /// Transport mode (car, bike, foot)
    #[schema(example = "car")]
    pub mode: String,
    /// Direction: "depart" (default) or "arrive"
    #[serde(default = "default_direction")]
    #[schema(example = "depart")]
    pub direction: String,
    /// Geometry encoding: polyline6 (default), geojson, points
    #[serde(default = "default_geometries")]
    #[schema(example = "geojson")]
    pub geometries: String,
    /// Optional fields to include: "network" adds reachable road geometries
    #[serde(default)]
    pub include: Option<String>,
    /// Exclude road types: comma-separated list of "toll", "ferry", "motorway"
    #[serde(default)]
    pub exclude: Option<String>,
    /// Avoid polygon(s) as JSON: `[[lon,lat],...]` or `[[[lon,lat],...],...]`
    #[serde(default)]
    pub avoid_polygons: Option<String>,
    /// Bands (#521): "bands" adds best/worst contour features per threshold
    /// (hidden best-/worst-speed weight sets: nights / weekday peaks). Explicit
    /// opt-in (2 extra PHAST passes). car only, JSON only.
    #[serde(default)]
    pub uncertainty: Option<String>,
}

/// A single contour polygon in an isochrone response
#[derive(Debug, Serialize, ToSchema)]
pub struct ContourFeature {
    /// Contour threshold in seconds — set on a time isochrone.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_s: Option<u32>,
    /// Contour threshold in metres — set on an isodistance (#612), where
    /// `time_s` is absent. Exactly one of the two is present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub distance_m: Option<u32>,
    /// Polygon as encoded polyline6 string
    #[serde(skip_serializing_if = "Option::is_none")]
    pub polygon: Option<String>,
    /// Polygon as GeoJSON coordinates [[lon, lat], ...]
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Vec<Vec<f64>>>)]
    pub polygon_geojson: Option<Vec<[f64; 2]>>,
    /// Polygon as point array [{lon, lat}, ...]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub polygon_points: Option<Vec<Point>>,
    /// Full GeoJSON geometry (`geometries=geojson` only): a `Polygon` with
    /// exactly one ring — an isochrone is one simple polygon by definition,
    /// never holed, never a `MultiPolygon` (#535/#542, enforced by type
    /// since #570). `polygon`/`polygon_geojson`/`polygon_points` carry the
    /// same ring for backward compatibility.
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Object>)]
    pub geometry: Option<serde_json::Value>,
    /// Number of reachable edges within this contour
    pub reachable_edges: usize,
    /// Band tag (only with uncertainty=bands): "best" | "worst";
    /// absent on the median contour.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub band: Option<&'static str>,
}

/// GeoJSON geometry of a traced topology: a `Polygon`, ring CCW, closed, 5
/// decimals. The builder emits at most one polygon and never a hole (#570),
/// so the multi-component / hole shapes below are unreachable by
/// construction — they are the encoder's total definition, not a promise.
pub(crate) fn topology_geojson(polys: &[ContourPolygon]) -> serde_json::Value {
    use crate::range::wkb_stream::{ensure_ccw, ensure_cw};
    let trunc = |v: f64| (v * 1e5).round() / 1e5;
    let ring_json = |ring: &[(f64, f64)], cw: bool| -> serde_json::Value {
        let mut coords: Vec<(f64, f64)> = ring.iter().map(|&(x, y)| (trunc(x), trunc(y))).collect();
        if cw {
            ensure_cw(&mut coords);
        } else {
            ensure_ccw(&mut coords);
        }
        if let (Some(&first), Some(&last)) = (coords.first(), coords.last())
            && first != last
        {
            coords.push(first);
        }
        serde_json::Value::Array(
            coords
                .into_iter()
                .map(|(x, y)| serde_json::json!([x, y]))
                .collect(),
        )
    };
    let poly_json = |p: &ContourPolygon| -> serde_json::Value {
        let mut rings = vec![ring_json(&p.outer, false)];
        rings.extend(p.holes.iter().map(|h| ring_json(h, true)));
        serde_json::Value::Array(rings)
    };
    let polys: Vec<&ContourPolygon> = polys.iter().filter(|p| p.outer.len() >= 3).collect();
    match polys.len() {
        0 => serde_json::json!({"type": "Polygon", "coordinates": []}),
        1 => serde_json::json!({"type": "Polygon", "coordinates": poly_json(polys[0])}),
        _ => serde_json::json!({
            "type": "MultiPolygon",
            "coordinates": polys.iter().map(|p| poly_json(p)).collect::<Vec<_>>()
        }),
    }
}

/// Isochrone response -- always returns a `contours` array (even for a single contour)
#[derive(Debug, Serialize, ToSchema)]
pub struct IsochroneResponse {
    /// Contour polygons (one per threshold value)
    pub contours: Vec<ContourFeature>,
    /// Network isochrone - reachable road segments (only if include=network)
    /// Each segment is [[lon, lat], [lon, lat], ...]
    #[serde(skip_serializing_if = "Option::is_none")]
    pub network: Option<Vec<Vec<[f64; 2]>>>,
}

/// Bulk isochrone request
#[derive(Debug, Deserialize, ToSchema)]
#[serde(deny_unknown_fields)] // #612: a parameter we cannot honour is refused, not ignored
pub struct BulkIsochroneRequest {
    /// List of origins as [lon, lat] pairs (max 10,000)
    #[schema(example = json!([[4.3517, 50.8503], [4.3617, 50.8553], [4.3717, 50.8603]]))]
    origins: Vec<[f64; 2]>,
    /// Time limit in seconds (1-7200). The one-contour time threshold;
    /// exactly one of `time_s` / `distance_m` must be set.
    #[serde(default)]
    #[schema(example = 600)]
    time_s: Option<u32>,
    /// Isodistance threshold in METRES (1-100000) of road length along the
    /// time-shortest path (#612). Exactly one of `time_s` / `distance_m`
    /// must be set.
    #[serde(default)]
    #[schema(example = json!(null))]
    distance_m: Option<u32>,
    /// Transport mode: car, bike, or foot
    #[schema(example = "car")]
    mode: String,
    /// Exclude road types: comma-separated list of "toll", "ferry", "motorway"
    #[serde(default)]
    exclude: Option<String>,
    /// Avoid polygon(s) as JSON array of coordinate rings
    #[serde(default)]
    avoid_polygons: Option<String>,
}

/// ONE rendering of every reason [`isochrone_polygons`] can refuse to start,
/// so the REST single, bulk and band surfaces cannot describe the same
/// refusal differently. All five are 400s: they are properties of the
/// request, not of the server.
pub fn isochrone_error_message(e: IsochroneSnapError) -> String {
    match e {
        IsochroneSnapError::NoSnap => "Could not snap center to road network".to_string(),
        IsochroneSnapError::NotAccessible => "Center not accessible for this mode".to_string(),
        IsochroneSnapError::NoLengthChannel => {
            "a distance threshold needs length-along-time weights, which this \
             dataset does not carry for the requested mode. Use a time \
             threshold (time_s / contours)"
                .to_string()
        }
        IsochroneSnapError::LengthWithCustomWeights => {
            "a distance threshold is incompatible with exclude / avoid_polygons: \
             those change which path is time-shortest, and the length-along-time \
             weights still describe the unmodified one"
                .to_string()
        }
    }
}

// ============ Thresholds ============

/// Largest time threshold an isochrone accepts, in seconds (2 h).
pub const MAX_TIME_S: u32 = 7200;
/// Largest isodistance threshold, in metres (100 km) — #612, the same
/// ceiling `distance_m` carried before #373 removed it.
pub const MAX_DISTANCE_M: u32 = 100_000;
/// Largest number of contours one request may ask for.
pub const MAX_CONTOURS: usize = 10;

/// What one isochrone request asks for: a metric, and the thresholds in that
/// metric's unit (sorted, de-duplicated, 1..=[`MAX_CONTOURS`] values).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RequestedContours {
    pub metric: ThresholdMetric,
    pub values: Vec<u32>,
}

impl RequestedContours {
    /// The threshold labels a [`ContourFeature`] carries: seconds on a time
    /// isochrone, metres on an isodistance, never both.
    fn label(&self, threshold: u32) -> (Option<u32>, Option<u32>) {
        match self.metric {
            ThresholdMetric::Time => (Some(threshold), None),
            ThresholdMetric::LengthAlongTime => (None, Some(threshold)),
        }
    }
}

/// How ONE transport spells the two threshold families, so a refusal from
/// the shared parser names the parameters the CALLER can actually set — the
/// Flight action has no `time_s` to offer and must not say so.
pub struct ThresholdSpelling {
    /// The time family, e.g. `"time_s / contours (seconds)"`.
    pub time: &'static str,
    /// The distance family, e.g. `"distance_m / contours_m (metres)"`.
    pub distance: &'static str,
}

/// The REST `/isochrone` and `/isochrone/bulk` spelling.
pub const REST_SPELLING: ThresholdSpelling = ThresholdSpelling {
    time: "time_s / contours (seconds)",
    distance: "distance_m / contours_m (metres)",
};

/// The Flight `isochrone` action's spelling.
pub const FLIGHT_SPELLING: ThresholdSpelling = ThresholdSpelling {
    time: "intervals (seconds)",
    distance: "intervals_m (metres)",
};

/// Parse the four threshold parameters into ONE metric and its values
/// (#612).
///
/// Two families, one per metric: seconds and metres. Within a family the
/// singular form is the one-contour spelling of the plural, and the plural
/// wins when both are given — the #554 rule, unchanged. ACROSS families
/// nothing wins: a threshold measures one quantity, and a request that names
/// both is a caller who does not know which answer they want, so it is
/// refused rather than silently resolved.
///
/// Every isochrone surface parses through here, so they cannot drift on what
/// a valid threshold is; only the [`ThresholdSpelling`] differs, so each
/// surface's refusal still names its own parameters.
pub fn parse_requested_contours(
    time_s: Option<u32>,
    contours: Option<&str>,
    distance_m: Option<u32>,
    contours_m: Option<&str>,
    spelling: &ThresholdSpelling,
) -> Result<RequestedContours, String> {
    let wants_time = time_s.is_some() || contours.is_some();
    let wants_distance = distance_m.is_some() || contours_m.is_some();
    match (wants_time, wants_distance) {
        (false, false) => {
            return Err(format!(
                "Provide a threshold: {} for an isochrone, or {} for an isodistance",
                spelling.time, spelling.distance
            ));
        }
        (true, true) => {
            return Err(format!(
                "Provide EITHER a time threshold, {}, OR a distance threshold, {} \
                 — not both",
                spelling.time, spelling.distance
            ));
        }
        _ => {}
    }

    let (metric, single, multi, unit, max) = if wants_time {
        (
            ThresholdMetric::Time,
            time_s,
            contours,
            "seconds",
            MAX_TIME_S,
        )
    } else {
        (
            ThresholdMetric::LengthAlongTime,
            distance_m,
            contours_m,
            "metres",
            MAX_DISTANCE_M,
        )
    };

    let mut values: Vec<u32> = Vec::new();
    if let Some(list) = multi {
        for part in list.split(',') {
            let part = part.trim();
            match part.parse::<u32>() {
                Ok(v) if (1..=max).contains(&v) => values.push(v),
                Ok(v) => {
                    return Err(format!(
                        "contour value must be between 1 and {max} {unit}, got {v}"
                    ));
                }
                Err(_) => return Err(format!("invalid contour value: '{part}'")),
            }
        }
    } else if let Some(v) = single {
        if v == 0 || v > max {
            return Err(format!(
                "threshold must be between 1 and {max} {unit}, got {v}"
            ));
        }
        values.push(v);
    }

    values.sort_unstable();
    values.dedup();
    if values.is_empty() || values.len() > MAX_CONTOURS {
        return Err(format!(
            "contours must have 1-{MAX_CONTOURS} values, got {}",
            values.len()
        ));
    }
    Ok(RequestedContours { metric, values })
}

// ============ Handlers ============

/// Calculate isochrone (reachable area within time limit)
///
/// Content negotiation:
/// - Accept: application/json (default) -> JSON response
/// - Accept: application/octet-stream -> WKB binary polygon
///
/// Optional fields via `include` parameter:
/// - include=network -> adds reachable road segments as polylines
#[utoipa::path(
    get,
    path = "/isochrone",
    tag = "Isochrone",
    summary = "Compute reachability polygon",
    description = "Computes the area reachable within a threshold using PHAST.\nSupports forward (depart) and reverse (arrive) isochrones.\n\nThe threshold is EITHER a time (`time_s` / `contours`, seconds) OR a distance (`distance_m` / `contours_m`, metres). A distance threshold is an **isodistance**: road length accumulated along the time-shortest path \u{2014} the same path and the same metres `/route` and `/table` report for the same pair, so the matrix is its exact truth. Never both in one request.\n\n`time_s` is the one-contour form of `contours`, `distance_m` of `contours_m` (the multi form wins when both are given).\n\nContent negotiation:\n- `Accept: application/json` \u{2192} JSON polygon\n- `Accept: application/octet-stream` \u{2192} WKB binary polygon (single contour only)",
    params(
        ("lon" = f64, Query, description = "Center longitude", example = 4.3517),
        ("lat" = f64, Query, description = "Center latitude", example = 50.8503),
        ("time_s" = Option<u32>, Query, description = "Time limit in seconds (1-7200) — the one-contour form of contours.", example = 600),
        ("contours" = Option<String>, Query, description = "Comma-separated time contours in seconds (e.g. '300,600,1200', max 10). The multi-contour form of time_s.", example = json!(null)),
        ("distance_m" = Option<u32>, Query, description = "Isodistance limit in metres (1-100000) along the time-shortest path \u{2014} the one-contour form of contours_m. Mutually exclusive with time_s / contours.", example = json!(null)),
        ("contours_m" = Option<String>, Query, description = "Comma-separated isodistance contours in metres (e.g. '5000,10000,20000', max 10). Mutually exclusive with time_s / contours.", example = json!(null)),
        ("mode" = String, Query, description = "Transport mode (e.g. car, bike, foot \u{2014} depends on available models)", example = "car"),
        ("direction" = Option<String>, Query, description = "Direction: 'depart' (default) or 'arrive'", example = "depart"),
        ("geometries" = Option<String>, Query, description = "Geometry encoding: polyline6 (default), geojson, points", example = "geojson"),
        ("include" = Option<String>, Query, description = "Optional: 'network' adds reachable road geometries", example = json!(null)),
        ("exclude" = Option<String>, Query, description = "Exclude road types: comma-separated list of 'toll', 'ferry', 'motorway'", example = json!(null)),
    ),
    responses(
        (status = 200, description = "Isochrone computed", body = IsochroneResponse),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
pub async fn isochrone_handler(
    State(regions): State<Arc<RegionsState>>,
    ValidatedQuery(req): ValidatedQuery<IsochroneRequest>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = validate_coord(req.lon, req.lat, "center") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
    }

    // Region dispatch (#91): the isochrone origin determines the
    // region. Reachable polygon stays inside that region — cross-
    // region reachability is part of the cross-region overlay (PR C).
    let ctx = match QueryContext::from_point(&regions, req.lon, req.lat, &req.mode) {
        Ok(ctx) => ctx,
        Err(e) => {
            let (code, body) = e.into_response_parts();
            return (code, Json(body)).into_response();
        }
    };
    let state = Arc::clone(&ctx.state);
    let _: &Arc<ServerState> = &state;

    // What the thresholds measure, and what they are (#612).
    let requested = match parse_requested_contours(
        req.time_s,
        req.contours.as_deref(),
        req.distance_m,
        req.contours_m.as_deref(),
        &REST_SPELLING,
    ) {
        Ok(r) => r,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

    // #521 bands: explicit opt-in, plain car path only, JSON only.
    let bands_requested = match req.uncertainty.as_deref() {
        None => false,
        Some("bands") => {
            if req.mode != "car" || req.avoid_polygons.is_some() || req.exclude.is_some() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse::new("uncertainty=bands is car-only and incompatible with avoid_polygons/exclude".to_string())),
                )
                    .into_response();
            }
            true
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

    let geom_format = match GeometryFormat::parse(&req.geometries) {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

    let reverse = match req.direction.to_lowercase().as_str() {
        "depart" => false,
        "arrive" => true,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(format!(
                    "Invalid direction: '{}'. Use 'depart' or 'arrive'.",
                    other
                ))),
            )
                .into_response();
        }
    };

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

    // #566: one resolution of exclude + avoid_polygons — the cached avoid
    // weights, the snap mask (BORROWED when neither option is present)
    // and the avoid-over-exclude priority.
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

    // Parse include parameter
    let include_network = req
        .include
        .as_ref()
        .map(|s| s.split(',').any(|p| p.trim() == "network"))
        .unwrap_or(false);

    // Check Accept header for content negotiation
    let wants_wkb = headers
        .get("accept")
        .and_then(|v| v.to_str().ok())
        .map(|s| s.contains("application/octet-stream") || s.contains("application/wkb"))
        .unwrap_or(false);

    let snap_mask: &[u64] = &weight_plan.snap_mask;

    // Recustomized flats (avoid takes priority, then exclude). `Some`
    // also selects the LEGACY single seed inside the core: phantom partial
    // costs assume base weights.
    let flats = weight_plan.weights().map(|w| IsochroneFlats {
        up: &w.time_up_flat,
        down_fwd: &w.time_down_fwd_flat,
        down_rev: &w.time_down_flat,
    });

    // #559: the WKB guards depend only on parsed input — reject BEFORE the
    // seeded PHAST + topology pipeline. An unauthenticated
    // `Accept: application/octet-stream` + `contours=a,b` (or
    // `uncertainty=bands`) used to pay for a full isochrone it was never
    // going to receive.
    if let Some(err) = wkb_request_rejection(wants_wkb, requested.values.len(), bands_requested) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(err.to_string())),
        )
            .into_response();
    }
    // WKB serves ONE contour (guarded above) — the others would be computed
    // only to be discarded.
    let thresholds: Vec<u32> = if wants_wkb {
        requested.values.iter().copied().take(1).collect()
    } else {
        requested.values.clone()
    };

    // THE pipeline (#549): snap -> phantom seeds -> seeded PHAST ->
    // rank->original -> per-threshold frontier -> topology, shared with
    // bands, /isochrone/bulk, Flight `isochrone` and the catchment hull.
    let field = match isochrone_polygons(
        &state,
        &mode_data,
        mode,
        &IsochroneQuery {
            metric: requested.metric,
            lon: req.lon,
            lat: req.lat,
            thresholds: &thresholds,
            reverse,
            mode_name: &req.mode,
            snap_mask: Some(snap_mask),
            flats,
            include_network: include_network && !wants_wkb,
        },
    ) {
        Ok(f) => f,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new(isochrone_error_message(e))),
            )
                .into_response();
        }
    };

    // WKB path (content negotiation). One contour, no bands: guaranteed by
    // `wkb_request_rejection` above, before any PHAST work (#559).
    if wants_wkb {
        use crate::range::contour::ContourResult;
        use crate::range::wkb_stream::encode_polygon_wkb;

        let contour =
            ContourResult::from_topology(field.topologies.into_iter().next().unwrap_or_default());
        ctx.record("isochrone");
        return match encode_polygon_wkb(&contour) {
            Some(wkb) => (
                [(axum::http::header::CONTENT_TYPE, "application/octet-stream")],
                wkb,
            )
                .into_response(),
            None => (StatusCode::NO_CONTENT, Vec::<u8>::new()).into_response(),
        };
    }

    // JSON path -- always returns contours array
    let mut contour_features: Vec<ContourFeature> = thresholds
        .iter()
        .zip(field.topologies.iter())
        .map(|(&threshold, topology)| {
            let (time_s, distance_m) = requested.label(threshold);
            let polygon = primary_outer_ring(topology);
            let reachable = field
                .settled
                .iter()
                .filter(|&&(_, d)| d <= threshold)
                .count();
            let (poly_enc, poly_geo, poly_pts) = encode_contour(&polygon, geom_format);
            ContourFeature {
                time_s,
                distance_m,
                polygon: poly_enc,
                polygon_geojson: poly_geo,
                polygon_points: poly_pts,
                geometry: matches!(geom_format, GeometryFormat::GeoJson)
                    .then(|| topology_geojson(topology)),
                reachable_edges: reachable,
                band: None,
            }
        })
        .collect();

    // #521 uncertainty bands: two extra seeded PHAST passes on the hidden
    // band weight sets — best (night speeds) reaches farther, worst (weekday
    // peak speeds) less far. Same thresholds.
    if bands_requested {
        let Some((pess, opt)) = state.band_modes() else {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse::new("uncertainty bands not available: the loaded edge_speeds table has no best/worst columns".to_string())),
            )
                .into_response();
        };
        for (band_mode, tag) in [(opt, "best"), (pess, "worst")] {
            match band_isochrone_features(
                &state,
                band_mode,
                &req,
                reverse,
                &requested,
                geom_format,
                tag,
            ) {
                Some(mut feats) => contour_features.append(&mut feats),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse::new(format!(
                            "band '{tag}': could not snap/compute isochrone"
                        ))),
                    )
                        .into_response();
                }
            }
        }
    }

    ctx.record("isochrone");
    Json(IsochroneResponse {
        contours: contour_features,
        // `include=network` shares the max-threshold frontier with the
        // contour at that threshold (#549: it used to be recomputed).
        network: field.network,
    })
    .into_response()
}

/// #521: contour features for ONE hidden band weight set — the SAME core
/// (`isochrone_polygons`) against the band's `ModeData`, then the SAME
/// contour encoder as the median branch. Plain path only by construction
/// (bands reject avoid/exclude upstream), so no weight override and no snap
/// mask beyond the band mode's own.
fn band_isochrone_features(
    state: &ServerState,
    band: crate::model::types::Mode,
    req: &IsochroneRequest,
    reverse: bool,
    requested: &RequestedContours,
    geom_format: GeometryFormat,
    tag: &'static str,
) -> Option<Vec<ContourFeature>> {
    let md = state.get_mode(band);
    let field = isochrone_polygons(
        state,
        &md,
        band,
        &IsochroneQuery {
            metric: requested.metric,
            lon: req.lon,
            lat: req.lat,
            thresholds: &requested.values,
            reverse,
            // The contour config keys off the mode name the CLIENT asked
            // for, not the hidden band mode's internal name.
            mode_name: &req.mode,
            snap_mask: Some(&md.mask),
            flats: None,
            include_network: false,
        },
    )
    .ok()?;

    Some(
        requested
            .values
            .iter()
            .zip(field.topologies.iter())
            .map(|(&threshold, topology)| {
                let (time_s, distance_m) = requested.label(threshold);
                let polygon = primary_outer_ring(topology);
                let reachable = field
                    .settled
                    .iter()
                    .filter(|&&(_, d)| d <= threshold)
                    .count();
                let (poly_enc, poly_geo, poly_pts) = encode_contour(&polygon, geom_format);
                ContourFeature {
                    time_s,
                    distance_m,
                    polygon: poly_enc,
                    polygon_geojson: poly_geo,
                    polygon_points: poly_pts,
                    geometry: matches!(geom_format, GeometryFormat::GeoJson)
                        .then(|| topology_geojson(topology)),
                    reachable_edges: reachable,
                    band: Some(tag),
                }
            })
            .collect(),
    )
}

/// `include=network`: the reached road polylines as (lon, lat) f64 — the SAME
/// set the polygon is stamped from (`reachable_polylines`), by construction.
pub fn build_network_geometry(
    settled: &[(u32, u32)],
    time_s: u32,
    node_weights: &[u32],
    ebg_nodes: &crate::formats::EbgNodes,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
    model: &ReachModel<'_>,
) -> Vec<Vec<[f64; 2]>> {
    // The caller only wants the polylines; never scan for the legacy
    // min-label anchor here (#549).
    reachable_polylines(
        settled,
        time_s,
        node_weights,
        ebg_nodes,
        edge_geom,
        model,
        false,
    )
    .0
    .into_iter()
    .filter(|p| p.len() >= 2)
    .map(|p| {
        p.into_iter()
            .map(|(lat_e7, lon_e7)| [lon_e7 as f64 / 1e7, lat_e7 as f64 / 1e7])
            .collect()
    })
    .collect()
}

/// #559: why a WKB (`Accept: application/octet-stream` / `application/wkb`)
/// isochrone request cannot be served, decided from PARSED INPUT ALONE so
/// the handler rejects it before the seeded PHAST + topology pipeline runs.
/// `None` = serveable. The WKB branch relies on this having run: it serves
/// exactly one contour and never a band.
pub(crate) fn wkb_request_rejection(
    wants_wkb: bool,
    n_thresholds: usize,
    bands_requested: bool,
) -> Option<&'static str> {
    if !wants_wkb {
        return None;
    }
    if bands_requested {
        return Some("uncertainty=bands requires the JSON response (Accept: application/json)");
    }
    if n_thresholds > 1 {
        return Some("WKB only supports single contour. Use JSON for multiple.");
    }
    None
}

/// Depart-field frontier (2026-09-03): PHAST labels are HEAD arrivals, so the
/// partially reachable edges are the UNREACHED successors of reached edges.
/// Scans every CCH arc (original + shortcut) out of each reached node: an arc
/// `e→f` of weight `w_arc` arrives at f's head at `label(e) + w_arc`, having
/// entered f at `label(e) + w_arc − w(f)` (an original arc weighs
/// `w(f) + turn(e,f)`; a shortcut is a real path ending with f, so the same
/// subtraction is a valid, possibly later, entry). Every original arc is in
/// the hierarchy, so the minimum over reached predecessors IS f's true entry.
/// Returns `(original EBG id, fraction of f driven before T)`, sorted.
pub fn depart_frontier(
    settled_ranks: &[(u32, u32)],
    threshold: u32,
    up: &crate::matrix::bucket_ch::UpAdjFlat,
    down: &crate::matrix::bucket_ch::DownAdjFlat,
    md: &super::state::ModeData,
    node_weights: &[u32],
) -> Vec<(u32, f32)> {
    use rustc_hash::FxHashMap;
    let n_nodes = up.offsets.len() - 1;
    let mut reached = vec![0u64; n_nodes.div_ceil(64)];
    for &(r, d) in settled_ranks {
        if d <= threshold {
            reached[(r >> 6) as usize] |= 1u64 << (r & 63);
        }
    }
    let is_reached = |v: usize| (reached[v >> 6] >> (v & 63)) & 1 == 1;
    let rank_to_filtered = &md.cch_topo.rank_to_filtered;
    let filtered_to_original = &md.filtered_to_original;
    // original id -> (earliest entry, w(f))
    let mut best: FxHashMap<u32, (u32, u32)> = FxHashMap::default();
    let mut scan = |offsets: &[u64],
                    targets: &[u32],
                    weights: &crate::formats::WeightArray,
                    r: usize,
                    d: u32| {
        let (a, b) = (offsets[r] as usize, offsets[r + 1] as usize);
        for (i, &target) in (a..b).zip(&targets[a..b]) {
            let cand = d.saturating_add(weights.get(i));
            if cand <= threshold {
                continue; // f itself is reached: whole edge, not frontier
            }
            let v = target as usize;
            if is_reached(v) {
                continue;
            }
            let orig = filtered_to_original[rank_to_filtered[v] as usize];
            let wf = node_weights[orig as usize];
            if wf == 0 || wf == u32::MAX {
                continue;
            }
            let entry = cand.saturating_sub(wf);
            if entry >= threshold {
                continue;
            }
            best.entry(orig)
                .and_modify(|e| e.0 = e.0.min(entry))
                .or_insert((entry, wf));
        }
    };
    for &(r, d) in settled_ranks {
        if d <= threshold {
            scan(&up.offsets[..], &up.targets[..], &up.weights, r as usize, d);
            scan(
                &down.offsets[..],
                &down.targets[..],
                &down.weights,
                r as usize,
                d,
            );
        }
    }
    let mut out: Vec<(u32, f32)> = best
        .into_iter()
        .map(|(orig, (entry, wf))| (orig, (threshold - entry) as f32 / wf as f32))
        .collect();
    out.sort_unstable_by_key(|&(orig, _)| orig);
    out
}

/// #620: the ENTRY cost of every directed edge an isodistance may draw, in
/// both channels — `(original EBG id) -> (entry length m, entry time)`.
///
/// A settled state's entry is its exact label minus its own weights: the
/// cheapest way to reach its tail AND continue into it. An unsettled
/// successor `f` of a settled state `e` (a frontier edge) gets the
/// time-cheapest entry over its settled predecessors, read on the SAME arc
/// slot in both flats. States whose entry length is already at or past the
/// budget are dropped: nothing on them is admissible. What the caller does
/// with two entries on one physical segment is `length_reach_fragments`.
#[allow(clippy::too_many_arguments)]
pub fn depart_entries_2ch(
    settled: &[(u32, u32, u32)], // (rank, time label, length label)
    threshold_len: u32,
    up_len: &crate::matrix::bucket_ch::UpAdjFlat,
    down_len: &crate::matrix::bucket_ch::DownAdjFlat,
    up_time: &crate::matrix::bucket_ch::UpAdjFlat,
    down_time: &crate::matrix::bucket_ch::DownAdjFlat,
    md: &super::state::ModeData,
    w_len: &[u32],
    w_time: &[u32],
) -> rustc_hash::FxHashMap<u32, (u32, u32)> {
    use rustc_hash::FxHashMap;
    let n_nodes = up_len.offsets.len() - 1;
    let mut is_settled = vec![0u64; n_nodes.div_ceil(64)];
    for &(r, _, _) in settled {
        is_settled[(r >> 6) as usize] |= 1u64 << (r & 63);
    }
    let settled_bit = |v: usize| (is_settled[v >> 6] >> (v & 63)) & 1 == 1;
    let rank_to_filtered = &md.cch_topo.rank_to_filtered;
    let filtered_to_original = &md.filtered_to_original;
    let orig_of = |rank: usize| filtered_to_original[rank_to_filtered[rank] as usize];
    let mut entries: FxHashMap<u32, (u32, u32)> = FxHashMap::default();
    // 1. settled states: exact entries from their own labels.
    for &(r, t, l) in settled {
        let orig = orig_of(r as usize);
        let (wl, wt) = (w_len[orig as usize], w_time[orig as usize]);
        if wl == 0 || wl == u32::MAX {
            continue;
        }
        let el = l.saturating_sub(wl);
        if el >= threshold_len {
            continue;
        }
        entries.insert(orig, (el, t.saturating_sub(wt)));
    }
    // 2. unsettled successors of in-budget states: time-cheapest entry.
    let mut scan = |offsets: &[u64],
                    targets: &[u32],
                    wl_arc: &crate::formats::WeightArray,
                    wt_arc: &crate::formats::WeightArray,
                    r: usize,
                    t: u32,
                    l: u32| {
        let (a, b) = (offsets[r] as usize, offsets[r + 1] as usize);
        for (i, &target) in (a..b).zip(&targets[a..b]) {
            let v = target as usize;
            if settled_bit(v) {
                continue; // its own label decides
            }
            let orig = orig_of(v);
            let wl = w_len[orig as usize];
            if wl == 0 || wl == u32::MAX {
                continue;
            }
            let el = l.saturating_add(wl_arc.get(i)).saturating_sub(wl);
            if el >= threshold_len {
                continue;
            }
            let et = t
                .saturating_add(wt_arc.get(i))
                .saturating_sub(w_time[orig as usize]);
            entries
                .entry(orig)
                .and_modify(|e| {
                    if (et, el) < (e.1, e.0) {
                        *e = (el, et);
                    }
                })
                .or_insert((el, et));
        }
    };
    for &(r, t, l) in settled {
        if l > threshold_len {
            continue;
        }
        scan(
            &up_len.offsets[..],
            &up_len.targets[..],
            &up_len.weights,
            &up_time.weights,
            r as usize,
            t,
            l,
        );
        scan(
            &down_len.offsets[..],
            &down_len.targets[..],
            &down_len.weights,
            &down_time.weights,
            r as usize,
            t,
            l,
        );
    }
    entries
}

// ============ Bulk Isochrone Handler ============

/// POST /isochrone/bulk - Compute multiple isochrones in parallel, return WKB stream
///
/// Returns a binary stream of WKB polygons with length-prefixed format:
/// For each isochrone: [4 bytes: origin_idx as u32][4 bytes: wkb_len as u32][wkb_len bytes: WKB]
#[utoipa::path(
    post,
    path = "/isochrone/bulk",
    tag = "Isochrone",
    summary = "Compute multiple isochrones in parallel",
    description = "Computes isochrones for multiple origins in parallel using rayon + PHAST.\nReturns a binary stream of WKB polygons with length-prefixed framing.\n\nBinary format per isochrone:\n- 4 bytes: origin index (u32 LE)\n- 4 bytes: WKB length (u32 LE)\n- N bytes: WKB polygon\n\nMaximum 10,000 origins. Supports cooperative cancellation on client disconnect.",
    request_body(content = BulkIsochroneRequest, description = "Origins, time limit, and mode"),
    responses(
        (status = 200, description = "Binary WKB stream", content_type = "application/octet-stream"),
        (status = 400, description = "Bad request", body = ErrorResponse),
    )
)]
pub async fn isochrone_bulk_handler(
    State(regions): State<Arc<RegionsState>>,
    ValidatedJson(req): ValidatedJson<BulkIsochroneRequest>,
) -> impl IntoResponse {
    // #539: seconds of sync rayon work — demote this worker out of the async
    // scheduler so bulk storms can't starve /health (liveness kills).
    tokio::task::block_in_place(move || isochrone_bulk_sync(regions, req))
}

fn isochrone_bulk_sync(
    regions: Arc<RegionsState>,
    req: BulkIsochroneRequest,
) -> axum::response::Response {
    use crate::range::contour::ContourResult;
    use crate::range::wkb_stream::encode_polygon_wkb;

    if req.origins.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new("origins cannot be empty")),
        )
            .into_response();
    }
    const MAX_BULK_ORIGINS: usize = 10_000;
    if req.origins.len() > MAX_BULK_ORIGINS {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse::new(format!(
                "too many origins: {} exceeds maximum of {}",
                req.origins.len(),
                MAX_BULK_ORIGINS
            ))),
        )
            .into_response();
    }
    for (i, &[lon, lat]) in req.origins.iter().enumerate() {
        if let Err(e) = validate_coord(lon, lat, &format!("origin[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    }
    // One threshold, in seconds or in metres (#612) — the same parser the
    // single endpoint uses, so the two cannot disagree about what is valid.
    let requested =
        match parse_requested_contours(req.time_s, None, req.distance_m, None, &REST_SPELLING) {
            Ok(r) => r,
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
            }
        };

    // Region dispatch (#91): every origin must snap to the same
    // region. Mixed-region bulk is rejected with 501 — same rule as
    // single /isochrone.
    let coords_iter = req.origins.iter().map(|&[lon, lat]| (lon, lat));
    let ctx = match QueryContext::from_points(&regions, coords_iter, &req.mode) {
        Ok(ctx) => ctx,
        Err(e) => {
            let (code, body) = e.into_response_parts();
            return (code, Json(body)).into_response();
        }
    };
    let state = Arc::clone(&ctx.state);

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse::new(e))).into_response();
        }
    };

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

    // #566: one resolution of exclude + avoid_polygons. #561: the snap
    // mask is BORROWED when neither option is present — /isochrone/bulk
    // used to clone the whole edge bitset on every request.
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

    // Recustomized flats (avoid > exclude). `Some` also selects the legacy
    // single seed inside the core — phantom partials assume base weights.
    // Bulk is depart-only, so `down_rev` is never read; it is carried so the
    // one query shape serves every surface.
    let flats = weight_plan.weights().map(|w| IsochroneFlats {
        up: &w.time_up_flat,
        down_fwd: &w.time_down_fwd_flat,
        down_rev: &w.time_down_flat,
    });

    // Bulk isochrones are depart-only (no `direction` field), so origins
    // act as sources.
    let thresholds = &requested.values[..];

    // Process all origins in parallel
    let results: Vec<(u32, Vec<u8>)> = req
        .origins
        .par_iter()
        .enumerate()
        .filter_map(|(idx, &[lon, lat])| {
            // THE pipeline (#549) — same seeds, same frontier, same anchor
            // and pin as REST /isochrone.
            let field = isochrone_polygons(
                &state,
                &mode_data,
                mode,
                &IsochroneQuery {
                    metric: requested.metric,
                    lon,
                    lat,
                    thresholds,
                    reverse: false,
                    mode_name: &req.mode,
                    snap_mask: Some(snap_mask),
                    flats,
                    include_network: false,
                },
            )
            .ok()?;
            let contour = ContourResult::from_topology(
                field.topologies.into_iter().next().unwrap_or_default(),
            );

            // Encode WKB
            encode_polygon_wkb(&contour).map(|wkb| (idx as u32, wkb))
        })
        .collect();

    // Build response: concatenated length-prefixed WKB
    let n_total_origins = req.origins.len();
    let n_successful = results.len();
    let mut response = Vec::with_capacity(results.len() * 500);
    for (origin_idx, wkb) in results {
        response.extend_from_slice(&origin_idx.to_le_bytes());
        response.extend_from_slice(&(wkb.len() as u32).to_le_bytes());
        response.extend_from_slice(&wkb);
    }

    ctx.record("isochrone_bulk");

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "application/octet-stream")
        // Progress tracking headers
        .header("X-Total-Origins", n_total_origins.to_string())
        .header("X-Successful-Isochrones", n_successful.to_string())
        .header(
            "X-Failed-Isochrones",
            (n_total_origins - n_successful).to_string(),
        )
        .body(Body::from(response))
        .unwrap_or_else(|_| {
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                "Failed to build bulk isochrone response",
            )
                .into_response()
        })
}

#[cfg(test)]
mod threshold_tests {
    //! #612: the one parser every isochrone surface — REST single, REST
    //! bulk, Flight `isochrone` — runs its thresholds through. What it
    //! decides is which metric the whole query then uses, so its refusals
    //! are part of the API contract, not an implementation detail.
    use super::*;

    fn ok(t: Option<u32>, c: Option<&str>, d: Option<u32>, cm: Option<&str>) -> RequestedContours {
        parse_requested_contours(t, c, d, cm, &REST_SPELLING).expect("valid thresholds")
    }

    fn err(t: Option<u32>, c: Option<&str>, d: Option<u32>, cm: Option<&str>) -> String {
        parse_requested_contours(t, c, d, cm, &REST_SPELLING).expect_err("refused")
    }

    #[test]
    fn a_time_threshold_is_seconds_and_a_distance_threshold_is_metres() {
        let time = ok(Some(600), None, None, None);
        assert_eq!(time.metric, ThresholdMetric::Time);
        assert_eq!(time.values, vec![600]);
        let dist = ok(None, None, Some(5000), None);
        assert_eq!(dist.metric, ThresholdMetric::LengthAlongTime);
        assert_eq!(dist.values, vec![5000]);
    }

    #[test]
    fn both_metrics_offer_the_same_multi_contour_shape() {
        assert_eq!(
            ok(None, Some("600,300,300"), None, None).values,
            vec![300, 600]
        );
        let m = ok(None, None, None, Some("20000, 5000 ,10000"));
        assert_eq!(m.metric, ThresholdMetric::LengthAlongTime);
        assert_eq!(m.values, vec![5000, 10000, 20000]);
    }

    /// #554's rule, unchanged and now mirrored on the distance family: the
    /// singular form is the one-contour spelling of the plural, and the
    /// plural wins when both are given.
    #[test]
    fn the_multi_form_wins_over_the_single_one_within_a_family() {
        assert_eq!(ok(Some(600), Some("120"), None, None).values, vec![120]);
        assert_eq!(ok(None, None, Some(5000), Some("900")).values, vec![900]);
    }

    /// ACROSS families nothing wins. A threshold measures one quantity, and
    /// resolving the ambiguity silently is exactly the class of defect #612
    /// is about.
    #[test]
    fn a_request_that_names_both_metrics_is_refused() {
        for (t, c, d, cm) in [
            (Some(600), None, Some(5000), None),
            (Some(600), None, None, Some("5000")),
            (None, Some("600"), Some(5000), None),
        ] {
            let e = err(t, c, d, cm);
            assert!(e.contains("not both"), "{e}");
        }
    }

    #[test]
    fn no_threshold_at_all_names_every_parameter_that_would_have_worked() {
        let e = err(None, None, None, None);
        for name in ["time_s", "contours", "distance_m", "contours_m"] {
            assert!(e.contains(name), "the refusal must name {name}: {e}");
        }
    }

    #[test]
    fn each_family_carries_its_own_bounds_and_unit() {
        assert!(err(Some(0), None, None, None).contains("seconds"));
        assert!(err(Some(MAX_TIME_S + 1), None, None, None).contains("7200"));
        assert_eq!(
            ok(Some(MAX_TIME_S), None, None, None).values,
            vec![MAX_TIME_S]
        );
        assert!(err(None, None, Some(0), None).contains("metres"));
        assert!(err(None, None, Some(MAX_DISTANCE_M + 1), None).contains("100000"));
        assert_eq!(
            ok(None, None, Some(MAX_DISTANCE_M), None).values,
            vec![MAX_DISTANCE_M]
        );
        // A distance-legal value that is NOT a time-legal one: the bound has
        // to follow the family, not the caller's habit.
        assert!(parse_requested_contours(None, None, Some(50_000), None, &REST_SPELLING).is_ok());
        assert!(parse_requested_contours(Some(50_000), None, None, None, &REST_SPELLING).is_err());
    }

    #[test]
    fn more_than_ten_contours_is_refused_in_either_metric() {
        let many: Vec<String> = (1..=11).map(|i| (i * 100).to_string()).collect();
        let list = many.join(",");
        assert!(err(None, Some(&list), None, None).contains("1-10"));
        assert!(err(None, None, None, Some(&list)).contains("1-10"));
    }

    #[test]
    fn a_contour_that_is_not_a_number_names_itself() {
        assert!(err(None, Some("300,soon"), None, None).contains("soon"));
        assert!(err(None, None, None, Some("5000,far")).contains("far"));
    }

    /// The two threshold labels are mutually exclusive on the wire: a
    /// contour carries seconds OR metres, never both, never neither.
    #[test]
    fn a_contour_is_labelled_in_exactly_one_unit() {
        let (t, d) = ok(Some(600), None, None, None).label(600);
        assert_eq!((t, d), (Some(600), None));
        let (t, d) = ok(None, None, Some(5000), None).label(5000);
        assert_eq!((t, d), (None, Some(5000)));
    }
}
