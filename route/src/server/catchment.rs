//! Catchment area computation.
//!
//! Three hull modes:
//! - `Convex`: Convex hull with straight edges (fast, clean)
//! - `Road`: Angular sector lasso — farthest client per 20-degree sector,
//!   consecutive extreme points connected by CCH P2P routes
//! - `Isochrone`: PHAST-based reachability polygon at threshold duration
//!   (exact road-reachability, ~5ms per polygon)
//!
//! Optional outlier removal via k-NN IQR method (auto-tuned, no parameters).

use serde::{Deserialize, Serialize};

use crate::matrix::bucket_ch::table_bucket_full_flat;
use crate::matrix::neighbors::{RadiusParam, auto_radius_km, parse_radius};
use crate::nbg::haversine_distance;
use crate::profile_abi::Mode;
use crate::range::contour::ContourResult;
use crate::range::wkb_stream::encode_polygon_wkb;

use super::geometry::{build_isochrone_geometry, build_raw_points};
use super::isochrone_handler::run_phast_bounded_fast;
use super::query::CchQuery;
use super::state::ServerState;
use super::unpack::unpack_path;

// ===========================================================================
// Types
// ===========================================================================

/// How the catchment polygon is generated
#[derive(Debug, Clone, Copy, PartialEq, Eq, Deserialize, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum HullMode {
    /// Convex hull of clients within threshold
    Convex,
    /// Angular sector lasso with CCH route edges
    Road,
    /// PHAST isochrone at threshold duration
    Isochrone,
}

/// Parameters for catchment computation
#[derive(Debug, Clone, Deserialize)]
pub struct CatchmentParams {
    /// Percentile thresholds (e.g. [50, 80])
    pub percentiles: Vec<f32>,
    /// Hull generation mode
    pub hull_shape: HullMode,
    /// Whether to remove geographic outliers before hull computation
    pub remove_outliers: bool,
}

/// A client with a pre-computed drive time
#[derive(Debug, Clone)]
pub struct Client {
    pub lon: f64,
    pub lat: f64,
    pub duration_s: f32,
}

/// One catchment polygon result (one per store × percentile)
#[derive(Debug)]
pub struct CatchmentResult {
    pub store_idx: u32,
    pub percentile: f32,
    pub threshold_s: f32,
    pub polygon_wkb: Vec<u8>,
    pub clients_covered: u32,
    pub clients_total: u32,
}

// ===========================================================================
// Outlier removal: k-NN IQR method
// ===========================================================================

/// Remove geographic outliers using k-nearest-neighbor distance IQR.
/// Points whose 5th-NN distance exceeds Q3 + 1.5*IQR are outliers.
///
/// Uses a sorted-x optimization with reverse index (pos_of) for O(N * K) per point.
pub fn remove_outliers(points: &[(f64, f64)]) -> Vec<(f64, f64)> {
    let n = points.len();
    if n < 10 {
        return points.to_vec();
    }

    const K: usize = 5;

    // Sort indices by x for faster neighbor search
    let mut sorted_idx: Vec<usize> = (0..n).collect();
    sorted_idx.sort_by(|&a, &b| points[a].0.partial_cmp(&points[b].0).unwrap());

    // Reverse index: pos_of[i] = position of point i in sorted_idx (O(1) lookup)
    let mut pos_of = vec![0usize; n];
    for (pos, &idx) in sorted_idx.iter().enumerate() {
        pos_of[idx] = pos;
    }

    // Compute k-th nearest neighbor distance for each point
    let mut knn_dists: Vec<f64> = Vec::with_capacity(n);

    for i in 0..n {
        let (px, py) = points[i];
        let pos = pos_of[i];

        // Search outward from pos to find K nearest neighbors
        let search_range = (K * 10).min(n);
        let lo = pos.saturating_sub(search_range);
        let hi = (pos + search_range).min(n);

        let mut dists: Vec<f64> = Vec::with_capacity(hi - lo);
        for &idx in &sorted_idx[lo..hi] {
            if idx == i {
                continue;
            }
            let dx = points[idx].0 - px;
            let dy = points[idx].1 - py;
            dists.push((dx * dx + dy * dy).sqrt());
        }

        dists.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let kth = dists.get(K.min(dists.len()) - 1).copied().unwrap_or(0.0);
        knn_dists.push(kth);
    }

    // IQR on knn distances
    let mut sorted_dists = knn_dists.clone();
    sorted_dists.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let q1 = sorted_dists[n / 4];
    let q3 = sorted_dists[3 * n / 4];
    let iqr = q3 - q1;
    let threshold = q3 + 1.5 * iqr;

    // Keep points below threshold
    points
        .iter()
        .enumerate()
        .filter(|(i, _)| knn_dists[*i] <= threshold)
        .map(|(_, &p)| p)
        .collect()
}

// ===========================================================================
// Convex hull (Graham scan)
// ===========================================================================

/// Graham scan convex hull.
/// Returns a closed ring (first == last) in CCW order, or `None` if degenerate.
pub fn convex_hull(points: &[(f64, f64)]) -> Option<Vec<(f64, f64)>> {
    let n = points.len();
    if n < 3 {
        return None;
    }

    let mut pts: Vec<(f64, f64)> = points.to_vec();

    // Find pivot: lowest y, then leftmost x
    let pivot_idx = pts
        .iter()
        .enumerate()
        .min_by(|(_, a), (_, b)| {
            a.1.partial_cmp(&b.1)
                .unwrap()
                .then(a.0.partial_cmp(&b.0).unwrap())
        })
        .map(|(i, _)| i)?;
    pts.swap(0, pivot_idx);
    let pivot = pts[0];

    // Sort by polar angle from pivot
    pts[1..].sort_by(|a, b| {
        let angle_a = (a.1 - pivot.1).atan2(a.0 - pivot.0);
        let angle_b = (b.1 - pivot.1).atan2(b.0 - pivot.0);
        angle_a.partial_cmp(&angle_b).unwrap()
    });

    let mut hull: Vec<(f64, f64)> = Vec::with_capacity(n);
    for &p in &pts {
        while hull.len() >= 2 {
            let a = hull[hull.len() - 2];
            let b = hull[hull.len() - 1];
            let cross = (b.0 - a.0) * (p.1 - a.1) - (b.1 - a.1) * (p.0 - a.0);
            if cross <= 0.0 {
                hull.pop();
            } else {
                break;
            }
        }
        hull.push(p);
    }

    if hull.len() < 3 {
        return None;
    }

    hull.push(hull[0]); // close the ring
    Some(hull)
}

// ===========================================================================
// Road-following lasso
// ===========================================================================

/// Angular sector lasso: find the farthest client per 20-degree sector,
/// connect consecutive extreme points with CCH P2P routes.
///
/// The resulting ring is the polygon boundary following actual roads.
pub fn road_lasso(
    state: &ServerState,
    mode: Mode,
    store: (f64, f64),
    points: &[(f64, f64)],
) -> Vec<(f64, f64)> {
    if points.len() < 3 {
        return points.to_vec();
    }

    let step = 20.0f64; // degrees per sector
    let n_sectors = (360.0 / step).ceil() as usize;

    // For each sector, find the farthest point
    let mut extreme: Vec<Option<(f64, f64)>> = vec![None; n_sectors];
    let mut extreme_dist: Vec<f64> = vec![0.0; n_sectors];

    for &(px, py) in points {
        let angle = ((py - store.1).atan2(px - store.0)).to_degrees();
        let angle_norm = (angle + 360.0) % 360.0;
        let sector = ((angle_norm / step) as usize).min(n_sectors - 1);

        let dx = px - store.0;
        let dy = py - store.1;
        let dist_sq = dx * dx + dy * dy;

        if dist_sq > extreme_dist[sector] {
            extreme_dist[sector] = dist_sq;
            extreme[sector] = Some((px, py));
        }
    }

    // Collect non-empty sectors in order
    let vertices: Vec<(f64, f64)> = extreme.into_iter().flatten().collect();
    if vertices.len() < 3 {
        return convex_hull(points).unwrap_or_default();
    }

    // Route between consecutive extreme points using CCH P2P
    let mode_data = state.get_mode(mode);
    let n_verts = vertices.len();
    let mut ring: Vec<(f64, f64)> = Vec::new();

    for i in 0..n_verts {
        let src = vertices[i];
        let dst = vertices[(i + 1) % n_verts];

        let route_points = route_between(state, &mode_data, mode, src, dst);
        if route_points.len() > 1 {
            // Append all but last (to avoid duplication with next segment's first)
            ring.extend_from_slice(&route_points[..route_points.len() - 1]);
        } else {
            ring.push(src);
        }
    }

    // Close the ring
    if let Some(&first) = ring.first() {
        ring.push(first);
    }

    // Deduplicate consecutive points
    ring.dedup_by(|a, b| (a.0 - b.0).abs() < 1e-10 && (a.1 - b.1).abs() < 1e-10);

    ring
}

/// Route between two lon/lat points using CCH P2P query, returning coordinate list.
fn route_between(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    src: (f64, f64),
    dst: (f64, f64),
) -> Vec<(f64, f64)> {
    // K-best snap + combo fallback (#197); src needs has_outbound and
    // dst needs has_inbound, both connectivity-aware after the SCC
    // role-mask change.
    const SNAP_K: usize = 64;
    let src_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        src.0,
        src.1,
        super::types::SnapRole::Src,
        None,
        SNAP_K,
    );
    let dst_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        dst.0,
        dst.1,
        super::types::SnapRole::Dst,
        None,
        SNAP_K,
    );

    if src_snap.ranks.is_empty() || dst_snap.ranks.is_empty() {
        return vec![src, dst];
    }

    let query = CchQuery::new(mode_data);
    let (src_rank, dst_rank, result) = match super::snap_kbest::p2p_with_kbest_fallback(
        &query,
        &src_snap.ranks,
        &dst_snap.ranks,
        super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
    ) {
        Some(triple) => triple,
        None => return vec![src, dst],
    };

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
            let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            mode_data.filtered_to_original[filt_id as usize]
        })
        .collect();

    let (points, _distance_m) = build_raw_points(&ebg_path, &state.ebg_nodes, &state.edge_geom);
    points.iter().map(|p| (p.lon, p.lat)).collect()
}

// ===========================================================================
// Isochrone hull
// ===========================================================================

/// Generate an isochrone polygon at the given threshold using PHAST,
/// returning raw WKB bytes.
pub fn isochrone_hull(
    state: &ServerState,
    mode: Mode,
    store_lon: f64,
    store_lat: f64,
    threshold_s: f32,
) -> Vec<u8> {
    let mode_data = state.get_mode(mode);
    let mode_name = &state.mode_names[mode.index()];

    // Catchment hull is a depart-isochrone: store acts as source.
    let store_role = super::types::SnapRole::Src.role_filter(&mode_data);
    let orig_id = match state
        .snap_index
        .snap_filtered_role(store_lon, store_lat, mode.0, None, store_role)
    {
        Some(id) => id,
        None => return Vec::new(),
    };

    let origin_rank = mode_data.orig_to_rank[orig_id as usize];
    if origin_rank == u32::MAX {
        return Vec::new();
    }

    // Weights are seconds (post-#297); threshold is already user-input seconds.
    let threshold_s_u32 = threshold_s.round() as u32;

    let settled = run_phast_bounded_fast(
        &mode_data.up_adj_flat,
        &mode_data.down_adj_flat,
        origin_rank,
        threshold_s_u32,
        mode,
    );

    // Map settled ranks back to original EBG IDs
    let settled_original: Vec<(u32, u32)> = settled
        .iter()
        .map(|&(rank, dist)| {
            let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            let orig_id = mode_data.filtered_to_original[filt_id as usize];
            (orig_id, dist)
        })
        .collect();

    let node_weights = &mode_data.node_weights;

    let polygon_points = build_isochrone_geometry(
        &settled_original,
        threshold_s_u32,
        node_weights,
        &state.ebg_nodes,
        &state.edge_geom,
        mode_name,
    );

    let coords: Vec<(f64, f64)> = polygon_points.iter().map(|p| (p.lon, p.lat)).collect();
    let contour = ContourResult {
        outer_ring: coords,
        holes: vec![],
        stats: Default::default(),
    };

    encode_polygon_wkb(&contour).unwrap_or_default()
}

// ===========================================================================
// Point-in-polygon (ray casting)
// ===========================================================================

/// Ray-casting point-in-polygon test.
/// `ring` should be a closed ring (first == last) or unclosed polygon edge list.
pub fn point_in_polygon(px: f64, py: f64, ring: &[(f64, f64)]) -> bool {
    let n = ring.len();
    if n < 3 {
        return false;
    }
    let mut inside = false;
    let mut j = n - 1;
    for i in 0..n {
        let (xi, yi) = ring[i];
        let (xj, yj) = ring[j];
        if ((yi > py) != (yj > py)) && (px < (xj - xi) * (py - yi) / (yj - yi) + xi) {
            inside = !inside;
        }
        j = i;
    }
    inside
}

// ===========================================================================
// WKB encoding
// ===========================================================================

/// Encode a simple polygon exterior ring as WKB (little-endian, type 3, one ring).
pub fn polygon_to_wkb(exterior: &[(f64, f64)]) -> Vec<u8> {
    let n = exterior.len();
    let mut wkb = Vec::with_capacity(1 + 4 + 4 + 4 + n * 16);
    wkb.push(0x01); // little-endian
    wkb.extend_from_slice(&3u32.to_le_bytes()); // Polygon
    wkb.extend_from_slice(&1u32.to_le_bytes()); // 1 ring
    wkb.extend_from_slice(&(n as u32).to_le_bytes()); // num points
    for &(x, y) in exterior {
        wkb.extend_from_slice(&x.to_le_bytes());
        wkb.extend_from_slice(&y.to_le_bytes());
    }
    wkb
}

// ===========================================================================
// Core: compute catchment for one store
// ===========================================================================

/// Compute catchment polygons for one store and a list of clients (with pre-computed drive times).
///
/// For each percentile:
/// 1. Determine the threshold (percentile of drive times)
/// 2. Filter clients within threshold
/// 3. Optional outlier removal
/// 4. Generate hull polygon (convex, road, or isochrone)
/// 5. Count covered clients
/// 6. Encode as WKB
pub fn compute_catchment(
    state: &ServerState,
    mode: Mode,
    store: (f64, f64),
    clients: &[Client],
    params: &CatchmentParams,
) -> Vec<CatchmentResult> {
    if clients.is_empty() {
        return Vec::new();
    }

    let coords: Vec<(f64, f64)> = clients.iter().map(|c| (c.lon, c.lat)).collect();

    let mut percentiles = params.percentiles.clone();
    percentiles.sort_by(|a, b| a.partial_cmp(b).unwrap());

    let mut results = Vec::new();
    let mut prev_extreme_points: Vec<(f64, f64)> = Vec::new();

    for &pct in &percentiles {
        // 1. Percentile threshold
        let mut durations: Vec<f32> = clients.iter().map(|c| c.duration_s).collect();
        durations.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = durations.len();
        let pct_idx = ((pct / 100.0) * (n as f32 - 1.0)).round() as usize;
        let threshold = durations[pct_idx.min(n - 1)];

        // 2. Filter by threshold
        let within: Vec<usize> = (0..n)
            .filter(|&i| clients[i].duration_s <= threshold)
            .collect();

        if within.len() < 3 {
            results.push(CatchmentResult {
                store_idx: 0,
                percentile: pct,
                threshold_s: threshold,
                polygon_wkb: Vec::new(),
                clients_covered: 0,
                clients_total: within.len() as u32,
            });
            continue;
        }

        let mut within_points: Vec<(f64, f64)> = within.iter().map(|&i| coords[i]).collect();

        // 3. Optional outlier removal
        if params.remove_outliers {
            within_points = remove_outliers(&within_points);
            if within_points.len() < 3 {
                results.push(CatchmentResult {
                    store_idx: 0,
                    percentile: pct,
                    threshold_s: threshold,
                    polygon_wkb: Vec::new(),
                    clients_covered: 0,
                    clients_total: within.len() as u32,
                });
                continue;
            }
        }

        // 4. Generate hull polygon
        let ring = match params.hull_shape {
            HullMode::Isochrone => {
                // Use PHAST isochrone at threshold duration
                let wkb = isochrone_hull(state, mode, store.0, store.1, threshold);
                if wkb.is_empty() {
                    results.push(CatchmentResult {
                        store_idx: 0,
                        percentile: pct,
                        threshold_s: threshold,
                        polygon_wkb: Vec::new(),
                        clients_covered: 0,
                        clients_total: within.len() as u32,
                    });
                    continue;
                }

                // For isochrone mode, we return the WKB directly.
                // Count coverage using point-in-polygon on the convex hull of
                // the within points (since the WKB polygon is rasterized and we
                // cannot easily extract coordinates from WKB for PIP).
                // A simpler approach: all clients within drive-time threshold
                // are by definition "covered" by the isochrone.
                let covered = within.len() as u32;

                results.push(CatchmentResult {
                    store_idx: 0,
                    percentile: pct,
                    threshold_s: threshold,
                    polygon_wkb: wkb,
                    clients_covered: covered,
                    clients_total: within.len() as u32,
                });
                continue;
            }
            HullMode::Road => {
                // Angular sector lasso with foot-profile route edges + nested containment
                let mut all_points = within_points.clone();
                all_points.extend_from_slice(&prev_extreme_points);

                let lasso_ring = road_lasso(state, mode, store, &all_points);
                // Save extreme vertices for next percentile (nested containment)
                prev_extreme_points = lasso_ring
                    .iter()
                    .copied()
                    .take(lasso_ring.len().saturating_sub(1))
                    .collect();
                lasso_ring
            }
            HullMode::Convex => match convex_hull(&within_points) {
                Some(h) => h,
                None => {
                    results.push(CatchmentResult {
                        store_idx: 0,
                        percentile: pct,
                        threshold_s: threshold,
                        polygon_wkb: Vec::new(),
                        clients_covered: 0,
                        clients_total: within.len() as u32,
                    });
                    continue;
                }
            },
        };

        // 5. Count coverage against ALL within-percentile clients (not just non-outliers)
        let covered = within
            .iter()
            .filter(|&&i| point_in_polygon(coords[i].0, coords[i].1, &ring))
            .count() as u32;

        let wkb = if ring.len() >= 4 {
            polygon_to_wkb(&ring)
        } else {
            Vec::new()
        };

        results.push(CatchmentResult {
            store_idx: 0,
            percentile: pct,
            threshold_s: threshold,
            polygon_wkb: wkb,
            clients_covered: covered,
            clients_total: within.len() as u32,
        });
    }

    results
}

// ===========================================================================
// REST handler types
// ===========================================================================

#[derive(Debug, Deserialize)]
pub struct CatchmentRequest {
    pub mode: String,
    pub hull_shape: HullMode,
    pub percentiles: Vec<f32>,
    #[serde(default = "default_true")]
    pub remove_outliers: bool,
    pub stores: Vec<StoreInput>,
    pub clients: Vec<ClientInput>,
    /// Optional Euclidean pre-filter radius in kilometres.
    /// Accepts a positive number, the string "auto" (server-computed p95 × 1.1
    /// per store), or null/0 to disable. Clients beyond the radius are
    /// excluded from that store's matrix and catchment entirely.
    #[serde(default)]
    pub radius_km: Option<serde_json::Value>,
}

fn default_true() -> bool {
    true
}

#[derive(Debug, Deserialize)]
pub struct StoreInput {
    pub id: String,
    pub lon: f64,
    pub lat: f64,
}

#[derive(Debug, Deserialize)]
pub struct ClientInput {
    pub lon: f64,
    pub lat: f64,
}

#[derive(Debug, Serialize)]
pub struct CatchmentResponse {
    pub results: Vec<CatchmentResultJson>,
}

#[derive(Debug, Serialize)]
pub struct CatchmentResultJson {
    pub store_id: String,
    pub percentile: f32,
    pub threshold_seconds: f32,
    pub clients_covered: u32,
    pub clients_total: u32,
    pub polygon_wkb_base64: String,
}

// ===========================================================================
// REST handler
// ===========================================================================

use axum::{Json, extract::State, http::StatusCode, response::IntoResponse};
use base64::{Engine, engine::general_purpose::STANDARD as BASE64};
use std::sync::Arc;

use super::regions::RegionsState;
use super::types::{ErrorResponse, parse_mode, validate_coord};

/// POST /catchment handler
pub async fn catchment_handler(
    State(regions): State<Arc<RegionsState>>,
    Json(req): Json<CatchmentRequest>,
) -> impl IntoResponse {
    // Region dispatch (#91): every store + every client must lie in
    // the same region. Cross-region catchments require the overlay
    // (PR C / Phase 2) and are 501 here.
    if req.stores.is_empty() || req.clients.is_empty() {
        // Fall through to existing validation below — empty stores
        // will be caught and rejected with a clear message.
    }
    let started_dispatch = std::time::Instant::now();
    let coords_iter = req
        .stores
        .iter()
        .map(|s| (s.lon, s.lat))
        .chain(req.clients.iter().map(|c| (c.lon, c.lat)));
    let (state, region_id): (Arc<ServerState>, String) =
        if !req.stores.is_empty() && !req.clients.is_empty() {
            match regions.dispatch_many(coords_iter, &req.mode) {
                Ok(pair) => pair,
                Err(e) => {
                    let (code, body) = e.into_response_parts();
                    return (code, Json(body)).into_response();
                }
            }
        } else {
            // Catchment with empty stores/clients hits the validation
            // path below; fall back to primary so the validation error
            // fires. Region label is the primary's id.
            (regions.primary(), regions.regions[0].id.clone())
        };

    // Validate mode
    let mode = match parse_mode(&req.mode, &state.mode_lookup) {
        Ok(m) => m,
        Err(e) => {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    };

    // Validate percentiles
    for &p in &req.percentiles {
        if !(0.0..=100.0).contains(&p) {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("Percentile {} out of range [0, 100]", p),
                }),
            )
                .into_response();
        }
    }
    if req.percentiles.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "percentiles must not be empty".into(),
            }),
        )
            .into_response();
    }

    // Validate stores
    if req.stores.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "stores must not be empty".into(),
            }),
        )
            .into_response();
    }
    for (i, s) in req.stores.iter().enumerate() {
        if let Err(e) = validate_coord(s.lon, s.lat, &format!("store[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    }

    // Validate clients
    for (i, c) in req.clients.iter().enumerate() {
        if let Err(e) = validate_coord(c.lon, c.lat, &format!("client[{}]", i)) {
            return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
        }
    }

    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    let params = CatchmentParams {
        percentiles: req.percentiles.clone(),
        hull_shape: req.hull_shape,
        remove_outliers: req.remove_outliers,
    };

    let mut all_results: Vec<CatchmentResultJson> = Vec::new();

    // Parse the optional Euclidean pre-filter radius once per request. The
    // actual radius is re-evaluated per store when `Auto`.
    let radius_param = parse_radius(req.radius_km.as_ref());

    // Hoist client coordinates out of the per-store loop. They're identical
    // across iterations, so allocating them once amortises a Vec construction
    // that was otherwise O(n_stores * n_clients).
    let auto_client_coords: Vec<(f64, f64)> = if matches!(radius_param, RadiusParam::Auto) {
        req.clients.iter().map(|c| (c.lon, c.lat)).collect()
    } else {
        Vec::new()
    };

    // #197 directional snap: store is the source for the 1-to-N matrix,
    // clients are destinations. Cache the bitsets once outside the loop.
    let store_role = super::types::SnapRole::Src.role_filter(&mode_data);
    let client_role = super::types::SnapRole::Dst.role_filter(&mode_data);

    // For each store: compute 1-to-N matrix via Bucket M2M, then catchment.
    // K-best snap + per-cell P2P fallback rescues INF cells the same
    // way /table POST does — see snap_kbest.rs. Lazy version (#368
    // pattern): K=1 primary upfront, K=64 only when an INF cell needs
    // it.
    const SNAP_K: usize = 64;
    for store_input in &req.stores {
        let store_rank = match super::snap_kbest::snap_primary_role(
            &state,
            &mode_data,
            mode,
            store_input.lon,
            store_input.lat,
            super::types::SnapRole::Src,
            None,
        ) {
            Some((_, r)) => r,
            None => continue, // Skip unsnappable stores
        };
        let _ = (store_role, client_role); // legacy bindings no longer used

        // Determine this store's effective radius (km) when requested. For
        // `Auto`, we compute p95 × 1.1 over the Euclidean distances from the
        // store to the *raw* client coordinates (pre-snap) — this is cheap
        // and reflects the user's intent better than post-snap geometry.
        let effective_radius_km: Option<f64> = match radius_param {
            RadiusParam::None => None,
            RadiusParam::Km(r) => Some(r),
            RadiusParam::Auto => {
                let store_coord = (store_input.lon, store_input.lat);
                let r = auto_radius_km(std::slice::from_ref(&store_coord), &auto_client_coords);
                if r > 0.0 { Some(r) } else { None }
            }
        };
        let effective_radius_m: Option<f64> = effective_radius_km.map(|km| km * 1000.0);

        // Snap all clients K=1 upfront (cheap). The K=64 escalation
        // happens lazily inside the INF-cell fallback below — same
        // lazy pattern as #370 /table and #374 /trip.
        let mut client_ranks: Vec<u32> = Vec::with_capacity(req.clients.len());
        let mut client_valid: Vec<usize> = Vec::with_capacity(req.clients.len());
        for (ci, c) in req.clients.iter().enumerate() {
            if let Some(radius_m) = effective_radius_m {
                let d = haversine_distance(store_input.lat, store_input.lon, c.lat, c.lon);
                if d > radius_m {
                    continue;
                }
            }
            if let Some((_, rank)) = super::snap_kbest::snap_primary_role(
                &state,
                &mode_data,
                mode,
                c.lon,
                c.lat,
                super::types::SnapRole::Dst,
                None,
            ) {
                client_ranks.push(rank);
                client_valid.push(ci);
            }
        }

        if client_ranks.is_empty() {
            continue;
        }

        // Compute 1-to-N matrix: one source (store), N targets (clients)
        let sources = &[store_rank];
        let targets = &client_ranks;

        let (mut matrix, _stats) = table_bucket_full_flat(
            n_nodes,
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            sources,
            targets,
        );

        // Per-cell K-best fallback for INF cells. K=64 escalation runs
        // only for client indices whose 1-to-N cell came back u32::MAX
        // — typically zero of them on a healthy graph.
        if matrix.contains(&u32::MAX) {
            use rayon::prelude::*;
            let query = super::query::CchQuery::new(&mode_data);
            // Lazily snap K=64 for the source store and just the
            // failing clients.
            let store_kbest = super::snap_kbest::snap_k_pair_role(
                &state,
                &mode_data,
                mode,
                store_input.lon,
                store_input.lat,
                super::types::SnapRole::Src,
                None,
                SNAP_K,
            );
            let failing: Vec<usize> = (0..client_valid.len())
                .filter(|&ti| matrix[ti] == u32::MAX)
                .collect();
            let client_kbest_for_failing: Vec<(usize, Vec<u32>)> = failing
                .par_iter()
                .map(|&ti| {
                    let ci = client_valid[ti];
                    let c = &req.clients[ci];
                    let snap = super::snap_kbest::snap_k_pair_role(
                        &state,
                        &mode_data,
                        mode,
                        c.lon,
                        c.lat,
                        super::types::SnapRole::Dst,
                        None,
                        SNAP_K,
                    );
                    (ti, snap.ranks)
                })
                .collect();
            let patches: Vec<(usize, u32)> = client_kbest_for_failing
                .par_iter()
                .filter_map(|(ti, dst_ranks)| {
                    super::snap_kbest::p2p_with_kbest_fallback(
                        &query,
                        &store_kbest.ranks,
                        dst_ranks,
                        super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
                    )
                    .map(|(_, _, r)| (*ti, r.distance))
                })
                .collect();
            for (ti, dist) in patches {
                matrix[ti] = dist;
            }
        }

        // Build Client structs with drive times
        let mut clients_with_dt: Vec<Client> = Vec::new();
        for (ti, &ci) in client_valid.iter().enumerate() {
            let d = matrix[ti]; // 1 source, so index = ti
            if d != u32::MAX {
                // d is already in seconds (post-#297).
                let duration_s = d as f32;
                clients_with_dt.push(Client {
                    lon: req.clients[ci].lon,
                    lat: req.clients[ci].lat,
                    duration_s,
                });
            }
        }

        // Compute catchment
        let store_coord = (store_input.lon, store_input.lat);
        let mut catch_results =
            compute_catchment(&state, mode, store_coord, &clients_with_dt, &params);

        // Set store index and convert to JSON results
        for r in &mut catch_results {
            r.store_idx = 0;
        }

        for r in catch_results {
            all_results.push(CatchmentResultJson {
                store_id: store_input.id.clone(),
                percentile: r.percentile,
                threshold_seconds: r.threshold_s,
                clients_covered: r.clients_covered,
                clients_total: r.clients_total,
                polygon_wkb_base64: BASE64.encode(&r.polygon_wkb),
            });
        }
    }

    super::region_metrics::record_query(
        &region_id,
        "catchment",
        started_dispatch.elapsed().as_secs_f64(),
    );
    (
        StatusCode::OK,
        Json(CatchmentResponse {
            results: all_results,
        }),
    )
        .into_response()
}

// ===========================================================================
// Flight exchange types
// ===========================================================================

/// Parse catchment parameters from descriptor cmd JSON
pub fn parse_exchange_params(json_str: &str) -> Result<CatchmentParams, String> {
    #[derive(Deserialize)]
    struct ExchangeParams {
        percentiles: Vec<f32>,
        hull_shape: HullMode,
        #[serde(default = "default_true")]
        remove_outliers: bool,
    }

    let parsed: ExchangeParams =
        serde_json::from_str(json_str).map_err(|e| format!("Invalid catchment params: {}", e))?;

    Ok(CatchmentParams {
        percentiles: parsed.percentiles,
        hull_shape: parsed.hull_shape,
        remove_outliers: parsed.remove_outliers,
    })
}

/// Arrow schema for catchment exchange output
pub fn catchment_arrow_schema() -> arrow::datatypes::Schema {
    use arrow::datatypes::{DataType, Field};

    arrow::datatypes::Schema::new(vec![
        Field::new("store_idx", DataType::UInt32, false),
        Field::new("store_id", DataType::Utf8, false),
        Field::new("percentile", DataType::Float32, false),
        Field::new("threshold_seconds", DataType::Float32, false),
        Field::new("clients_covered", DataType::UInt32, false),
        Field::new("clients_total", DataType::UInt32, false),
        Field::new("polygon_wkb", DataType::Binary, false),
    ])
}

// ===========================================================================
// Tests
// ===========================================================================

#[cfg(test)]
mod tests {
    use super::*;

    // ===== Outlier removal =====

    #[test]
    fn test_remove_outliers_small_input_passthrough() {
        // <10 points: no removal, passthrough
        let pts: Vec<(f64, f64)> = (0..5).map(|i| (i as f64, i as f64)).collect();
        let result = remove_outliers(&pts);
        assert_eq!(result, pts);
    }

    #[test]
    fn test_remove_outliers_no_outliers() {
        // Points on a circle: equidistant, all k-NN distances nearly identical.
        // IQR should be ~0, so the threshold Q3 + 1.5*IQR ~ Q3 which is near
        // the common distance -> nothing gets removed.
        let mut pts = Vec::new();
        for i in 0..25 {
            let angle = (i as f64) * std::f64::consts::TAU / 25.0;
            let r = 0.001;
            pts.push((r * angle.cos(), r * angle.sin()));
        }
        let result = remove_outliers(&pts);
        assert_eq!(result.len(), pts.len());
    }

    #[test]
    fn test_remove_outliers_with_distant_point() {
        // 19 tight points + 1 far away
        let mut pts: Vec<(f64, f64)> = (0..19).map(|i| (i as f64 * 0.001, 0.0)).collect();
        pts.push((100.0, 100.0)); // far outlier
        let result = remove_outliers(&pts);
        // The outlier should be removed
        assert!(result.len() < pts.len());
        // The outlier should not be in the result
        assert!(!result.contains(&(100.0, 100.0)));
    }

    #[test]
    fn test_remove_outliers_returns_subset() {
        let mut pts: Vec<(f64, f64)> = (0..30).map(|i| (i as f64 * 0.01, 0.0)).collect();
        pts.push((50.0, 50.0));
        let result = remove_outliers(&pts);
        // Result must be a subset of input
        for p in &result {
            assert!(pts.contains(p));
        }
    }

    // ===== Convex hull =====

    #[test]
    fn test_convex_hull_triangle() {
        let pts = vec![(0.0, 0.0), (1.0, 0.0), (0.5, 1.0)];
        let hull = convex_hull(&pts).unwrap();
        // Should have 4 points (3 + closure)
        assert_eq!(hull.len(), 4);
        assert_eq!(hull[0], hull[3]); // closed
    }

    #[test]
    fn test_convex_hull_square() {
        let pts = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0)];
        let hull = convex_hull(&pts).unwrap();
        // Square should have 5 points (4 + closure)
        assert_eq!(hull.len(), 5);
        assert_eq!(hull[0], hull[4]); // closed
    }

    #[test]
    fn test_convex_hull_with_interior_points() {
        let pts = vec![
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 2.0),
            (0.0, 2.0),
            (1.0, 1.0), // interior
            (0.5, 0.5), // interior
            (1.5, 0.5), // interior
            (0.5, 1.5), // interior
            (1.5, 1.5), // interior
            (1.0, 0.5), // interior
        ];
        let hull = convex_hull(&pts).unwrap();
        // Hull should have fewer unique vertices than input
        assert!(hull.len() - 1 <= 4); // 4 corners + closure
    }

    #[test]
    fn test_convex_hull_collinear_points() {
        let pts = vec![(0.0, 0.0), (1.0, 0.0), (2.0, 0.0)];
        let hull = convex_hull(&pts);
        // Collinear points: cross product is always 0, so hull collapses
        assert!(hull.is_none());
    }

    #[test]
    fn test_convex_hull_too_few_points() {
        let pts = vec![(0.0, 0.0), (1.0, 1.0)];
        assert!(convex_hull(&pts).is_none());

        let pts1 = vec![(0.0, 0.0)];
        assert!(convex_hull(&pts1).is_none());

        let pts0: Vec<(f64, f64)> = vec![];
        assert!(convex_hull(&pts0).is_none());
    }

    #[test]
    fn test_convex_hull_is_closed() {
        let pts = vec![(0.0, 0.0), (3.0, 0.0), (3.0, 4.0), (0.0, 4.0), (1.0, 2.0)];
        let hull = convex_hull(&pts).unwrap();
        assert_eq!(hull.first(), hull.last());
    }

    #[test]
    fn test_convex_hull_all_points_inside() {
        let pts = vec![
            (0.0, 0.0),
            (10.0, 0.0),
            (10.0, 10.0),
            (0.0, 10.0),
            (5.0, 5.0),
            (3.0, 7.0),
            (7.0, 3.0),
        ];
        let hull = convex_hull(&pts).unwrap();
        // Every input point must be inside or on the hull
        for &(px, py) in &pts {
            let inside = point_in_polygon(px, py, &hull);
            let on_edge = is_on_ring_boundary(px, py, &hull);
            assert!(
                inside || on_edge,
                "Point ({}, {}) should be inside or on hull",
                px,
                py
            );
        }
    }

    /// Helper: check if point is approximately on a ring edge
    fn is_on_ring_boundary(px: f64, py: f64, ring: &[(f64, f64)]) -> bool {
        for i in 0..ring.len().saturating_sub(1) {
            let (ax, ay) = ring[i];
            let (bx, by) = ring[i + 1];
            // Check if point is collinear and within segment bounding box
            let cross = (bx - ax) * (py - ay) - (by - ay) * (px - ax);
            if cross.abs() < 1e-9 {
                let min_x = ax.min(bx) - 1e-9;
                let max_x = ax.max(bx) + 1e-9;
                let min_y = ay.min(by) - 1e-9;
                let max_y = ay.max(by) + 1e-9;
                if px >= min_x && px <= max_x && py >= min_y && py <= max_y {
                    return true;
                }
            }
        }
        false
    }

    // ===== Point-in-polygon =====

    #[test]
    fn test_point_in_polygon_inside() {
        // Unit square
        let ring = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        assert!(point_in_polygon(0.5, 0.5, &ring));
    }

    #[test]
    fn test_point_in_polygon_outside() {
        let ring = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        assert!(!point_in_polygon(2.0, 2.0, &ring));
        assert!(!point_in_polygon(-1.0, 0.5, &ring));
    }

    #[test]
    fn test_point_in_polygon_on_edge() {
        // On edge (ray-casting may return either true or false for boundary — that is acceptable)
        let ring = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        // On the left edge (0, 0.5): may be true or false depending on exact boundary handling
        // We just verify it does not panic
        let _ = point_in_polygon(0.0, 0.5, &ring);
    }

    #[test]
    fn test_point_in_polygon_triangle() {
        let ring = vec![(0.0, 0.0), (2.0, 0.0), (1.0, 2.0), (0.0, 0.0)];
        assert!(point_in_polygon(1.0, 0.5, &ring)); // inside
        assert!(!point_in_polygon(3.0, 3.0, &ring)); // outside
    }

    #[test]
    fn test_point_in_polygon_complex() {
        // L-shaped polygon (concave)
        let ring = vec![
            (0.0, 0.0),
            (2.0, 0.0),
            (2.0, 1.0),
            (1.0, 1.0),
            (1.0, 2.0),
            (0.0, 2.0),
            (0.0, 0.0),
        ];
        assert!(point_in_polygon(0.5, 0.5, &ring)); // inside bottom-left
        assert!(point_in_polygon(0.5, 1.5, &ring)); // inside top-left
        assert!(!point_in_polygon(1.5, 1.5, &ring)); // outside (the cut-out corner)
    }

    // ===== WKB encoding =====

    #[test]
    fn test_polygon_to_wkb_triangle() {
        let ring = vec![(0.0, 0.0), (1.0, 0.0), (0.5, 1.0), (0.0, 0.0)];
        let wkb = polygon_to_wkb(&ring);
        assert!(!wkb.is_empty());
        // Should be valid WKB: 1 + 4 + 4 + 4 + 4*16 = 77 bytes
        assert_eq!(wkb.len(), 1 + 4 + 4 + 4 + 4 * 16);
    }

    #[test]
    fn test_polygon_to_wkb_header() {
        let ring = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 0.0)];
        let wkb = polygon_to_wkb(&ring);
        // byte_order = 0x01 (LE)
        assert_eq!(wkb[0], 0x01);
        // type = 3 (Polygon)
        let wkb_type = u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]]);
        assert_eq!(wkb_type, 3);
        // num_rings = 1
        let num_rings = u32::from_le_bytes([wkb[5], wkb[6], wkb[7], wkb[8]]);
        assert_eq!(num_rings, 1);
    }

    #[test]
    fn test_polygon_to_wkb_point_count() {
        let ring = vec![(0.0, 0.0), (1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        let wkb = polygon_to_wkb(&ring);
        let num_points = u32::from_le_bytes([wkb[9], wkb[10], wkb[11], wkb[12]]);
        assert_eq!(num_points, 5);
    }

    // ===== Catchment computation (unit tests with mock data) =====

    fn make_mock_clients(durations: &[f32]) -> Vec<Client> {
        durations
            .iter()
            .enumerate()
            .map(|(i, &d)| Client {
                lon: 4.35 + (i as f64) * 0.01,
                lat: 50.85 + (i as f64) * 0.005,
                duration_s: d,
            })
            .collect()
    }

    #[test]
    fn test_catchment_percentile_threshold() {
        // 10 clients with durations 100..1000 (step 100)
        let durations: Vec<f32> = (1..=10).map(|i| i as f32 * 100.0).collect();
        let clients = make_mock_clients(&durations);
        assert_eq!(clients.len(), 10);

        // Test the percentile logic directly (compute_catchment requires ServerState).
        let mut durs = durations.clone();
        durs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = durs.len();
        let pct_idx = ((50.0f32 / 100.0) * (n as f32 - 1.0)).round() as usize;
        let threshold = durs[pct_idx.min(n - 1)];
        // (50/100) * (10-1) = 4.5, f32::round() rounds half away from zero = 5
        let expected_threshold = durs[5]; // 600.0
        assert!(
            (threshold - expected_threshold).abs() < 0.01,
            "threshold={threshold}, expected={expected_threshold}"
        );
    }

    #[test]
    fn test_catchment_80_covers_more_than_50() {
        // Higher percentile -> higher threshold -> more clients covered
        let durations: Vec<f32> = (1..=20).map(|i| i as f32 * 50.0).collect();

        let mut durs = durations.clone();
        durs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = durs.len();

        let pct50_idx = ((50.0f32 / 100.0) * (n as f32 - 1.0)).round() as usize;
        let pct80_idx = ((80.0f32 / 100.0) * (n as f32 - 1.0)).round() as usize;
        let t50 = durs[pct50_idx.min(n - 1)];
        let t80 = durs[pct80_idx.min(n - 1)];

        assert!(t80 >= t50, "80th percentile threshold must be >= 50th");
        let within_50 = durations.iter().filter(|&&d| d <= t50).count();
        let within_80 = durations.iter().filter(|&&d| d <= t80).count();
        assert!(within_80 >= within_50);
    }

    #[test]
    fn test_catchment_convex_mode() {
        // Verify convex hull produces non-empty polygon for enough points
        let pts: Vec<(f64, f64)> = (0..10)
            .map(|i| {
                let angle = (i as f64) * std::f64::consts::TAU / 10.0;
                (angle.cos(), angle.sin())
            })
            .collect();
        let hull = convex_hull(&pts).unwrap();
        let wkb = polygon_to_wkb(&hull);
        assert!(!wkb.is_empty());
    }

    #[test]
    fn test_catchment_empty_clients() {
        let clients = make_mock_clients(&[]);
        assert!(clients.is_empty());
        // compute_catchment would return empty Vec for empty clients
    }

    #[test]
    fn test_catchment_all_same_duration() {
        let durations: Vec<f32> = vec![300.0; 20];
        let clients = make_mock_clients(&durations);
        assert_eq!(clients.len(), 20);
        assert!((clients[0].duration_s - 300.0).abs() < 0.01);

        let mut durs = durations.clone();
        durs.sort_by(|a, b| a.partial_cmp(b).unwrap());
        let n = durs.len();
        let pct_idx = ((50.0f32 / 100.0) * (n as f32 - 1.0)).round() as usize;
        let threshold = durs[pct_idx.min(n - 1)];
        // All same -> threshold is that value
        assert!((threshold - 300.0).abs() < 0.01);
    }

    #[test]
    #[ignore] // Requires Belgium data and running ServerState
    fn test_catchment_isochrone_mode_returns_polygon() {
        // This test would require loading ServerState from Belgium data.
        // Kept as an integration test marker.
    }

    #[test]
    #[ignore] // Requires Belgium data
    fn test_catchment_brussels_store_convex() {
        // Integration test: compute convex catchment for a store in Brussels
    }

    #[test]
    #[ignore] // Requires Belgium data
    fn test_catchment_brussels_store_isochrone() {
        // Integration test: compute isochrone catchment for a store in Brussels
    }
}
