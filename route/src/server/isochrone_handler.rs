//! /isochrone and /isochrone/bulk handlers — reachability polygons

use axum::{
    Json,
    body::Body,
    extract::{Query, State},
    http::{StatusCode, header},
    response::{IntoResponse, Response},
};
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use utoipa::ToSchema;

use super::geometry::{
    GeometryFormat, IsochroneFlats, IsochroneQuery, IsochroneSnapError, Point, ReachModel,
    encode_contour, isochrone_polygons, primary_outer_ring, reachable_polylines,
};
use super::query_context::QueryContext;
use super::regions::RegionsState;
use super::route::{default_direction, default_geometries};
use super::state::ServerState;
use super::types::{ErrorResponse, parse_mode, validate_coord};
use crate::range::ContourPolygon;

// ============ Types ============

#[derive(Debug, Deserialize, ToSchema)]
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
    /// Contour threshold in seconds
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_s: Option<u32>,
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
pub struct BulkIsochroneRequest {
    /// List of origins as [lon, lat] pairs (max 10,000)
    #[schema(example = json!([[4.3517, 50.8503], [4.3617, 50.8553], [4.3717, 50.8603]]))]
    origins: Vec<[f64; 2]>,
    /// Time limit in seconds (1-7200)
    #[schema(example = 600)]
    time_s: u32,
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
    description = "Computes the area reachable within a time limit using PHAST.\nSupports forward (depart) and reverse (arrive) isochrones.\n\n`time_s` is the one-contour form of `contours` (`contours` wins when both are given).\n\nContent negotiation:\n- `Accept: application/json` \u{2192} JSON polygon\n- `Accept: application/octet-stream` \u{2192} WKB binary polygon (single contour only)",
    params(
        ("lon" = f64, Query, description = "Center longitude", example = 4.3517),
        ("lat" = f64, Query, description = "Center latitude", example = 50.8503),
        ("time_s" = Option<u32>, Query, description = "Time limit in seconds (1-7200) — the one-contour form of contours.", example = 600),
        ("contours" = Option<String>, Query, description = "Comma-separated time contours in seconds (e.g. '300,600,1200', max 10). Mutually exclusive with time_s.", example = json!(null)),
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
    Query(req): Query<IsochroneRequest>,
    headers: axum::http::HeaderMap,
) -> impl IntoResponse {
    if let Err(e) = validate_coord(req.lon, req.lat, "center") {
        return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
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

    // Determine isochrone metric: exactly one of {time_s, contours}.
    // The pre-#371 `distance_m` (isodistance) variant was removed — that
    // mode ran PHAST on a separate distance-shortest CCH metric, which
    // produced reachability sets along a different geometric path from
    // every other drivetime endpoint. Reachable-by-time is the only
    // semantically consistent isochrone for a drivetime engine.
    enum IsoMetric {
        Time(u32),           // threshold in seconds (post-#297; was ds)
        MultiTime(Vec<u32>), // sorted thresholds in seconds
    }

    // #554: `time_s` is the one-contour spelling of `contours`; `contours` wins
    // when both are given (they used to be mutually exclusive, a 400 for no
    // benefit).
    if req.time_s.is_none() && req.contours.is_none() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Provide time_s (one contour) or contours (comma-separated seconds)"
                    .to_string(),
            }),
        )
            .into_response();
    }

    let metric = if let (Some(t), None) = (req.time_s, req.contours.as_ref()) {
        if t == 0 || t > 7200 {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("time_s must be between 1 and 7200, got {}", t),
                }),
            )
                .into_response();
        }
        IsoMetric::Time(t) // seconds (post-#297; weights are also in s)
    } else if let Some(ref contours_str) = req.contours {
        let mut values = Vec::new();
        for part in contours_str.split(',') {
            let part = part.trim();
            match part.parse::<u32>() {
                Ok(v) if (1..=7200).contains(&v) => values.push(v), // seconds (post-#297)
                Ok(v) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("contour value must be between 1 and 7200, got {}", v),
                        }),
                    )
                        .into_response();
                }
                Err(_) => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("invalid contour value: '{}'", part),
                        }),
                    )
                        .into_response();
                }
            }
        }
        if values.is_empty() || values.len() > 10 {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("contours must have 1-10 values, got {}", values.len()),
                }),
            )
                .into_response();
        }
        values.sort_unstable();
        values.dedup();
        IsoMetric::MultiTime(values)
    } else {
        // The `provided != 1` guard above already returns 400 when no
        // metric is set, so this branch is unreachable today. We keep
        // it as a structured 500 instead of `unreachable!()` so that a
        // future edit which adds a fourth metric option to the
        // `provided` count but forgets the matching arm degrades into
        // a logged 500 instead of a process panic caught only by
        // `CatchPanicLayer`. (#141)
        return (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(ErrorResponse {
                error: "isochrone metric dispatch fell through; this is a server bug \
                        — the request validator and metric parser disagree about which \
                        fields are accepted"
                    .to_string(),
            }),
        )
            .into_response();
    };

    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // #521 bands: explicit opt-in, plain car path only, JSON only.
    let bands_requested = match req.uncertainty.as_deref() {
        None => false,
        Some("bands") => {
            if req.mode != "car" || req.avoid_polygons.is_some() || req.exclude.is_some() {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(ErrorResponse {
                        error: "uncertainty=bands is car-only and incompatible with avoid_polygons/exclude".to_string(),
                    }),
                )
                    .into_response();
            }
            true
        }
        Some(other) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("unknown uncertainty value '{other}' (expected 'bands')"),
                }),
            )
                .into_response();
        }
    };

    let geom_format = match GeometryFormat::parse(&req.geometries) {
        Ok(f) => f,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let reverse = match req.direction.to_lowercase().as_str() {
        "depart" => false,
        "arrive" => true,
        other => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Invalid direction: '{}'. Use 'depart' or 'arrive'.", other),
                }),
            )
                .into_response();
        }
    };

    // Parse exclude parameter
    let exclude_mask = match super::exclude::parse_exclude_option(&req.exclude) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
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
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
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

    // Build list of thresholds with their labels. All time-based after #371.
    let thresholds: Vec<(u32, Option<u32>)> = match &metric {
        IsoMetric::Time(s) => vec![(*s, Some(*s))],
        IsoMetric::MultiTime(vals) => vals.iter().map(|&s| (s, Some(s))).collect(),
    };
    // #559: the WKB guards depend only on parsed input — reject BEFORE the
    // seeded PHAST + topology pipeline. An unauthenticated
    // `Accept: application/octet-stream` + `contours=a,b` (or
    // `uncertainty=bands`) used to pay for a full isochrone it was never
    // going to receive.
    if let Some(err) = wkb_request_rejection(wants_wkb, thresholds.len(), bands_requested) {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: err.to_string(),
            }),
        )
            .into_response();
    }
    // WKB serves ONE contour (guarded above) — the others would be computed
    // only to be discarded.
    let requested: Vec<u32> = if wants_wkb {
        thresholds.iter().take(1).map(|&(t, _)| t).collect()
    } else {
        thresholds.iter().map(|&(t, _)| t).collect()
    };

    // THE pipeline (#549): snap -> phantom seeds -> seeded PHAST ->
    // rank->original -> per-threshold frontier -> topology, shared with
    // bands, /isochrone/bulk, Flight `isochrone` and the catchment hull.
    let field = match isochrone_polygons(
        &state,
        &mode_data,
        mode,
        &IsochroneQuery {
            lon: req.lon,
            lat: req.lat,
            thresholds: &requested,
            reverse,
            mode_name: &req.mode,
            snap_mask: Some(snap_mask),
            flats,
            include_network: include_network && !wants_wkb,
        },
    ) {
        Ok(f) => f,
        Err(IsochroneSnapError::NoSnap) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Could not snap center to road network".to_string(),
                }),
            )
                .into_response();
        }
        Err(IsochroneSnapError::NotAccessible) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Center not accessible for this mode".to_string(),
                }),
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
        .map(|(&(threshold, time_s), topology)| {
            let polygon = primary_outer_ring(topology);
            let reachable = field
                .settled
                .iter()
                .filter(|&&(_, d)| d <= threshold)
                .count();
            let (poly_enc, poly_geo, poly_pts) = encode_contour(&polygon, geom_format);
            ContourFeature {
                time_s,
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
                Json(ErrorResponse {
                    error: "uncertainty bands not available: the loaded edge_speeds table has no best/worst columns".to_string(),
                }),
            )
                .into_response();
        };
        for (band_mode, tag) in [(opt, "best"), (pess, "worst")] {
            match band_isochrone_features(
                &state,
                band_mode,
                &req,
                reverse,
                &thresholds,
                geom_format,
                tag,
            ) {
                Some(mut feats) => contour_features.append(&mut feats),
                None => {
                    return (
                        StatusCode::BAD_REQUEST,
                        Json(ErrorResponse {
                            error: format!("band '{tag}': could not snap/compute isochrone"),
                        }),
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
    thresholds: &[(u32, Option<u32>)],
    geom_format: GeometryFormat,
    tag: &'static str,
) -> Option<Vec<ContourFeature>> {
    let md = state.get_mode(band);
    let requested: Vec<u32> = thresholds.iter().map(|&(t, _)| t).collect();
    let field = isochrone_polygons(
        state,
        &md,
        band,
        &IsochroneQuery {
            lon: req.lon,
            lat: req.lat,
            thresholds: &requested,
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
        thresholds
            .iter()
            .zip(field.topologies.iter())
            .map(|(&(threshold, time_s), topology)| {
                let polygon = primary_outer_ring(topology);
                let reachable = field
                    .settled
                    .iter()
                    .filter(|&&(_, d)| d <= threshold)
                    .count();
                let (poly_enc, poly_geo, poly_pts) = encode_contour(&polygon, geom_format);
                ContourFeature {
                    time_s,
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
        (status = 400, description = "Bad request"),
    )
)]
pub async fn isochrone_bulk_handler(
    State(regions): State<Arc<RegionsState>>,
    Json(req): Json<BulkIsochroneRequest>,
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
            Json(ErrorResponse {
                error: "origins cannot be empty".into(),
            }),
        )
            .into_response();
    }
    const MAX_BULK_ORIGINS: usize = 10_000;
    if req.origins.len() > MAX_BULK_ORIGINS {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!(
                    "too many origins: {} exceeds maximum of {}",
                    req.origins.len(),
                    MAX_BULK_ORIGINS
                ),
            }),
        )
            .into_response();
    }
    for (i, &[lon, lat]) in req.origins.iter().enumerate() {
        if let Err(e) = validate_coord(lon, lat, &format!("origin[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    }
    if req.time_s == 0 || req.time_s > 7200 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: format!("time_s must be between 1 and 7200, got {}", req.time_s),
            }),
        )
            .into_response();
    }

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
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Parse exclude parameter
    let exclude_mask = match super::exclude::parse_exclude_option(&req.exclude) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Parse avoid_polygons
    let avoid_json = match super::avoid::parse_avoid_option(&req.avoid_polygons) {
        Ok(v) => v,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    let mode_data = state.get_mode(mode);
    // Weights and thresholds are both seconds (post-#297).
    let time_s = req.time_s;

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
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
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
    let thresholds = [time_s];

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
                    lon,
                    lat,
                    thresholds: &thresholds,
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
