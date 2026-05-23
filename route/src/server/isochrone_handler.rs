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
use std::cell::RefCell;
use std::sync::Arc;
use utoipa::ToSchema;

use super::geometry::{GeometryFormat, Point, build_isochrone_geometry, encode_polyline6};
use super::regions::RegionsState;
use super::route::{default_direction, default_geometries};
use super::state::ServerState;
use super::types::{ErrorResponse, SnapRole, parse_mode, validate_coord};

// ============ Types ============

#[derive(Debug, Deserialize, ToSchema)]
pub struct IsochroneRequest {
    /// Center longitude
    #[schema(example = 4.3517)]
    pub lon: f64,
    /// Center latitude
    #[schema(example = 50.8503)]
    pub lat: f64,
    /// Time limit in seconds (1-7200). Mutually exclusive with distance_m and contours.
    #[serde(default)]
    #[schema(example = 600)]
    pub time_s: Option<u32>,
    /// Distance limit in meters (1-100000). Mutually exclusive with time_s and contours.
    #[serde(default)]
    pub distance_m: Option<u32>,
    /// Multiple time contours as comma-separated seconds (e.g. "300,600,1200", max 10).
    /// Mutually exclusive with time_s and distance_m.
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
}

/// A single contour polygon in an isochrone response
#[derive(Debug, Serialize, ToSchema)]
pub struct ContourFeature {
    /// Contour threshold in seconds (present for time-based isochrones)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub time_s: Option<u32>,
    /// Contour threshold in meters (present for distance-based isochrones)
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
    /// Number of reachable edges within this contour
    pub reachable_edges: usize,
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

// =============================================================================
// THREAD-LOCAL PHAST STATE (eliminates 9.6MB memset per query)
// =============================================================================

/// Block size for block-gated downward scan
/// Each block contains BLOCK_SIZE consecutive ranks
const PHAST_BLOCK_SIZE: usize = 4096;

/// Thread-local PHAST state with generation stamping and block gating
/// Eliminates O(n) initialization per query by using version stamps
/// Block gating skips large portions of the graph in downward phase
pub struct PhastState {
    /// Distance array (persistent across queries)
    dist: Vec<u32>,
    /// Version stamp per node (marks which generation set the distance)
    version: Vec<u32>,
    /// Version stamp per block (marks which blocks have active nodes)
    block_active: Vec<u32>,
    /// Number of blocks
    n_blocks: usize,
    /// Current generation (incremented per query)
    current_gen: u32,
    /// Priority queue (reused across queries)
    pq: std::collections::BinaryHeap<std::cmp::Reverse<(u32, u32)>>,
}

impl PhastState {
    fn new(n_nodes: usize) -> Self {
        let n_blocks = n_nodes.div_ceil(PHAST_BLOCK_SIZE);
        Self {
            dist: vec![u32::MAX; n_nodes],
            version: vec![0; n_nodes],
            block_active: vec![0; n_blocks],
            n_blocks,
            current_gen: 0,
            pq: std::collections::BinaryHeap::with_capacity(n_nodes / 100),
        }
    }

    /// Start a new query (O(1) instead of O(n))
    #[inline]
    fn start_query(&mut self) {
        self.current_gen = self.current_gen.wrapping_add(1);
        if self.current_gen == 0 {
            // Overflow - reset all versions (rare, every ~4B queries)
            self.version.iter_mut().for_each(|v| *v = 0);
            self.block_active.iter_mut().for_each(|v| *v = 0);
            self.current_gen = 1;
        }
        self.pq.clear();
    }

    /// Get distance (returns MAX if not set this query)
    #[inline]
    fn get_dist(&self, node: usize) -> u32 {
        if self.version[node] == self.current_gen {
            self.dist[node]
        } else {
            u32::MAX
        }
    }

    /// Set distance (also marks version and block as active)
    #[inline]
    fn set_dist(&mut self, node: usize, dist: u32) {
        self.dist[node] = dist;
        self.version[node] = self.current_gen;
        // Mark block as active
        let block_idx = node / PHAST_BLOCK_SIZE;
        self.block_active[block_idx] = self.current_gen;
    }

    /// Check if a block is active this query
    #[inline]
    fn is_block_active(&self, block_idx: usize) -> bool {
        self.block_active[block_idx] == self.current_gen
    }
}

thread_local! {
    /// Thread-local PHAST state array, one slot per mode (indexed by mode.index())
    static PHAST_STATES: RefCell<[Option<PhastState>; crate::profile_abi::MAX_MODES]> = const { RefCell::new([
        None, None, None, None,
        None, None, None, None,
    ]) };
}

/// Run PHAST bounded query using thread-local state.
///
/// Reads weights, targets, and offsets directly from the pre-built
/// `UpAdjFlat` / `DownAdjFlat` flats — never touches `cch_weights.up/.down`
/// on the inner loop. After #149, this is what makes
/// `madvise(MADV_DONTNEED)` over the cch_weights byte ranges actually
/// reclaim RSS.
///
/// Returns `Vec<(rank, dist)>` of settled nodes only — avoids the 9.6 MB
/// output allocation a full distance vector would require.
pub fn run_phast_bounded_fast(
    up_adj_flat: &crate::matrix::bucket_ch::UpAdjFlat,
    down_adj_flat: &crate::matrix::bucket_ch::DownAdjFlat,
    origin_rank: u32,
    threshold: u32,
    mode: crate::profile_abi::Mode,
) -> Vec<(u32, u32)> {
    use std::cmp::Reverse;

    let total_start = std::time::Instant::now();
    let n_nodes = up_adj_flat.offsets.len() - 1;
    let mode_idx = mode.index();

    // Get thread-local state for this mode
    PHAST_STATES.with(|cell| {
        let mut states = cell.borrow_mut();
        let state_slot = &mut states[mode_idx];

        // Initialize or reinitialize if needed
        let state = state_slot.get_or_insert_with(|| PhastState::new(n_nodes));

        // Verify size matches (in case different datasets)
        if state.dist.len() != n_nodes {
            *state = PhastState::new(n_nodes);
        }

        // Start new query (O(1) instead of O(n) memset)
        state.start_query();
        state.set_dist(origin_rank as usize, 0);

        // Track settled nodes during upward phase
        let mut upward_settled: Vec<u32> = Vec::with_capacity(n_nodes / 100);

        // Phase 1: Upward search (PQ-based, UP edges only). Reads weights
        // from `up_adj_flat` (pre-filtered for INF), so the hot loop is
        // branch-free w.r.t. weight validity.
        let upward_start = std::time::Instant::now();
        state.pq.push(Reverse((0, origin_rank)));

        while let Some(Reverse((d, u))) = state.pq.pop() {
            if d > threshold {
                break;
            }

            if d > state.get_dist(u as usize) {
                continue; // Stale entry
            }

            upward_settled.push(u);

            let up_start = up_adj_flat.offsets[u as usize] as usize;
            let up_end = up_adj_flat.offsets[u as usize + 1] as usize;

            for i in up_start..up_end {
                let v = up_adj_flat.targets[i] as usize;
                let w = up_adj_flat.weights[i];
                let new_dist = d.saturating_add(w);
                if new_dist < state.get_dist(v) {
                    state.set_dist(v, new_dist);
                    state.pq.push(Reverse((new_dist, v as u32)));
                }
            }
        }
        let upward_us = upward_start.elapsed().as_micros();

        // Phase 2: Block-gated downward scan (linear, DOWN edges only).
        // Reads from `down_adj_flat` — same shape as the legacy
        // `cch_topo.down_*` + `cch_weights.down` pair, but pre-filtered.
        let downward_start = std::time::Instant::now();
        let mut blocks_active = 0usize;
        for block_idx in (0..state.n_blocks).rev() {
            // Skip blocks with no active nodes
            if !state.is_block_active(block_idx) {
                continue;
            }
            blocks_active += 1;

            // Process nodes in this block in reverse rank order
            let block_start = block_idx * PHAST_BLOCK_SIZE;
            let block_end = ((block_idx + 1) * PHAST_BLOCK_SIZE).min(n_nodes);

            for rank in (block_start..block_end).rev() {
                let d_u = state.get_dist(rank);

                if d_u == u32::MAX || d_u > threshold {
                    continue;
                }

                let down_start = down_adj_flat.offsets[rank] as usize;
                let down_end = down_adj_flat.offsets[rank + 1] as usize;

                for i in down_start..down_end {
                    let v = down_adj_flat.targets[i] as usize;
                    let w = down_adj_flat.weights[i];
                    let new_dist = d_u.saturating_add(w);
                    if new_dist < state.get_dist(v) {
                        // set_dist marks the target block as active too
                        state.set_dist(v, new_dist);
                    }
                }
            }
        }
        let downward_us = downward_start.elapsed().as_micros();

        // Collect settled nodes (only those within threshold)
        // Only scan active blocks - much faster than full n_nodes scan
        let collect_start = std::time::Instant::now();
        let mut result: Vec<(u32, u32)> = Vec::with_capacity(n_nodes / 10);
        for block_idx in 0..state.n_blocks {
            if !state.is_block_active(block_idx) {
                continue;
            }
            let block_start = block_idx * PHAST_BLOCK_SIZE;
            let block_end = ((block_idx + 1) * PHAST_BLOCK_SIZE).min(n_nodes);
            for rank in block_start..block_end {
                if state.version[rank] == state.current_gen {
                    let d = state.dist[rank];
                    if d <= threshold {
                        result.push((rank as u32, d));
                    }
                }
            }
        }
        let collect_us = collect_start.elapsed().as_micros();
        let total_us = total_start.elapsed().as_micros();

        tracing::debug!(
            threshold_ds = threshold,
            upward_us = upward_us,
            downward_us = downward_us,
            collect_us = collect_us,
            total_us = total_us,
            upward_settled = upward_settled.len(),
            settled_nodes = result.len(),
            blocks_active = blocks_active,
            blocks_total = state.n_blocks,
            "PHAST forward timing"
        );

        result
    })
}

thread_local! {
    /// Thread-local PHAST state array for REVERSE queries, one slot per mode (indexed by mode.index()).
    /// Separate from PHAST_STATES to avoid conflicts when forward and reverse queries run on the same thread.
    static PHAST_STATES_REV: RefCell<[Option<PhastState>; crate::profile_abi::MAX_MODES]> = const { RefCell::new([
        None, None, None, None,
        None, None, None, None,
    ]) };
}

/// Run REVERSE PHAST bounded query -- computes d(all -> target) for reverse isochrones.
///
/// Swaps up/down adjacencies: upward uses DOWN-reverse edges, downward uses UP edges.
/// Uses a PULL approach in the downward phase (for each node v, pull from higher-rank
/// neighbors via up_adj_flat[v]) since we need reversed UP edges.
pub fn run_phast_bounded_fast_reverse(
    up_adj_flat: &crate::matrix::bucket_ch::UpAdjFlat,
    down_rev_flat: &crate::matrix::bucket_ch::DownReverseAdjFlat,
    target_rank: u32,
    threshold: u32,
    mode: crate::profile_abi::Mode,
) -> Vec<(u32, u32)> {
    use std::cmp::Reverse;

    let total_start = std::time::Instant::now();
    let n_nodes = up_adj_flat.offsets.len() - 1;
    let mode_idx = mode.index();

    PHAST_STATES_REV.with(|cell| {
        let mut states = cell.borrow_mut();
        let state_slot = &mut states[mode_idx];

        // Initialize or reinitialize if needed
        let state = state_slot.get_or_insert_with(|| PhastState::new(n_nodes));
        if state.dist.len() != n_nodes {
            *state = PhastState::new(n_nodes);
        }

        state.start_query();
        state.set_dist(target_rank as usize, 0);

        // Phase 1: Upward search using DOWN-reverse edges (goes to higher rank nodes)
        let upward_start = std::time::Instant::now();
        state.pq.push(Reverse((0, target_rank)));

        while let Some(Reverse((d, u))) = state.pq.pop() {
            if d > threshold {
                break;
            }
            if d > state.get_dist(u as usize) {
                continue;
            }

            // down_rev_flat[u] gives higher-rank neighbors with DOWN weights
            let start = down_rev_flat.offsets[u as usize] as usize;
            let end = down_rev_flat.offsets[u as usize + 1] as usize;

            for i in start..end {
                let v = down_rev_flat.sources[i] as usize; // v has higher rank
                let w = down_rev_flat.weights[i];

                if w == u32::MAX {
                    continue;
                }

                let new_dist = d.saturating_add(w);
                if new_dist < state.get_dist(v) {
                    state.set_dist(v, new_dist);
                    state.pq.push(Reverse((new_dist, v as u32)));
                }
            }
        }
        let upward_us = upward_start.elapsed().as_micros();

        // Phase 2: Plain downward PULL scan using UP edges
        // For each node v (decreasing rank), pull from higher-rank neighbors u
        // via up_adj_flat[v].targets (which have higher rank).
        //
        // NOTE: Block-gating is NOT used here because PULL cannot propagate
        // block activation downward (unlike PUSH in forward PHAST). A PUSH
        // approach would need a reverse-UP adjacency we don't have.
        let downward_start = std::time::Instant::now();
        for v in (0..n_nodes).rev() {
            let up_start = up_adj_flat.offsets[v] as usize;
            let up_end = up_adj_flat.offsets[v + 1] as usize;

            for i in up_start..up_end {
                let u = up_adj_flat.targets[i] as usize; // u has higher rank
                let w = up_adj_flat.weights[i];

                let d_u = state.get_dist(u);
                if d_u == u32::MAX || d_u > threshold {
                    continue;
                }

                let new_dist = d_u.saturating_add(w);
                if new_dist < state.get_dist(v) {
                    state.set_dist(v, new_dist);
                }
            }
        }
        let downward_us = downward_start.elapsed().as_micros();

        // Collect settled nodes (full scan -- no block-gating)
        let collect_start = std::time::Instant::now();
        let mut result: Vec<(u32, u32)> = Vec::with_capacity(n_nodes / 10);
        for rank in 0..n_nodes {
            if state.version[rank] == state.current_gen {
                let d = state.dist[rank];
                if d <= threshold {
                    result.push((rank as u32, d));
                }
            }
        }
        let collect_us = collect_start.elapsed().as_micros();
        let total_us = total_start.elapsed().as_micros();

        tracing::debug!(
            threshold_ds = threshold,
            upward_us = upward_us,
            downward_us = downward_us,
            collect_us = collect_us,
            total_us = total_us,
            settled_nodes = result.len(),
            "PHAST reverse timing"
        );

        result
    })
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
    description = "Computes the area reachable within a time or distance limit using PHAST.\nSupports forward (depart) and reverse (arrive) isochrones.\n\nProvide exactly one of: `time_s`, `distance_m`, or `contours`.\n\nContent negotiation:\n- `Accept: application/json` \u{2192} JSON polygon\n- `Accept: application/octet-stream` \u{2192} WKB binary polygon (single contour only)",
    params(
        ("lon" = f64, Query, description = "Center longitude", example = 4.3517),
        ("lat" = f64, Query, description = "Center latitude", example = 50.8503),
        ("time_s" = Option<u32>, Query, description = "Time limit in seconds (1-7200). Mutually exclusive with distance_m, contours.", example = 600),
        ("distance_m" = Option<u32>, Query, description = "Distance limit in meters (1-100000). Mutually exclusive with time_s, contours.", example = json!(null)),
        ("contours" = Option<String>, Query, description = "Comma-separated time contours in seconds (e.g. '300,600,1200', max 10). Mutually exclusive with time_s, distance_m.", example = json!(null)),
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
    let started_dispatch = std::time::Instant::now();
    let (state, region_id) = match regions.dispatch_single_id(req.lon, req.lat, &req.mode) {
        Ok(pair) => pair,
        Err(e) => {
            let (code, body) = e.into_response_parts();
            return (code, Json(body)).into_response();
        }
    };
    let _: &Arc<ServerState> = &state;

    // Determine isochrone metric: exactly one of {time_s, distance_m, contours}
    enum IsoMetric {
        Time(u32),           // threshold in deciseconds
        Distance(u32),       // threshold in millimeters
        MultiTime(Vec<u32>), // sorted thresholds in deciseconds
    }

    let provided = [
        req.time_s.is_some(),
        req.distance_m.is_some(),
        req.contours.is_some(),
    ]
    .iter()
    .filter(|&&b| b)
    .count();

    if provided != 1 {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Provide exactly one of: time_s, distance_m, or contours".to_string(),
            }),
        )
            .into_response();
    }

    let metric = if let Some(t) = req.time_s {
        if t == 0 || t > 7200 {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("time_s must be between 1 and 7200, got {}", t),
                }),
            )
                .into_response();
        }
        IsoMetric::Time(t * 10) // convert to deciseconds
    } else if let Some(d) = req.distance_m {
        if d == 0 || d > 100_000 {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: format!("distance_m must be between 1 and 100000, got {}", d),
                }),
            )
                .into_response();
        }
        IsoMetric::Distance(d * 1000) // convert to millimeters
    } else if let Some(ref contours_str) = req.contours {
        let mut values = Vec::new();
        for part in contours_str.split(',') {
            let part = part.trim();
            match part.parse::<u32>() {
                Ok(v) if (1..=7200).contains(&v) => values.push(v * 10), // deciseconds
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

    // Compute avoid weights (includes exclude if both present)
    let avoid_entry = if let Some(ref avoid_str) = avoid_json {
        match super::avoid::compute_avoid_weights(&state, mode_data, avoid_str, exclude_mask) {
            Ok(entry) => Some(entry),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
            }
        }
    } else {
        None
    };

    // Determine PHAST threshold (max of all contour values) and whether to use distance weights
    let (phast_threshold, use_distance_weights) = match &metric {
        IsoMetric::Time(ds) => (*ds, false),
        IsoMetric::Distance(mm) => (*mm, true),
        IsoMetric::MultiTime(vals) => (*vals.last().unwrap(), false),
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

    // Build snap mask (with optional avoid/exclude filtering)
    let snap_mask: std::borrow::Cow<'_, [u64]> = if let Some(ref entry) = avoid_entry {
        std::borrow::Cow::Owned(super::avoid::build_avoid_mask(
            &mode_data.mask,
            &entry.flags,
            exclude_mask.map(|exc| (state.edge_exclude_flags.as_slice(), exc)),
        ))
    } else if let Some(exc) = exclude_mask {
        std::borrow::Cow::Owned(super::exclude::build_exclude_mask(
            &mode_data.mask,
            &state.edge_exclude_flags,
            exc,
        ))
    } else {
        std::borrow::Cow::Borrowed(&mode_data.mask)
    };

    // Snap center — directional role tracks isochrone direction:
    //   depart  → center acts as a source     → SnapRole::Src (needs outbound arcs)
    //   arrive  → center acts as a destination → SnapRole::Dst (needs inbound arcs)
    let center_role = if reverse {
        SnapRole::Dst
    } else {
        SnapRole::Src
    };
    let center_role_filter = center_role.role_filter(mode_data);

    let center_orig = match state.snap_index.snap_filtered_role(
        req.lon,
        req.lat,
        mode.0,
        Some(&snap_mask),
        center_role_filter,
    ) {
        Some(id) => id,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "Could not snap center to road network".to_string(),
                }),
            )
                .into_response();
        }
    };

    let center_rank = mode_data.orig_to_rank[center_orig as usize];
    if center_rank == u32::MAX {
        return (
            StatusCode::BAD_REQUEST,
            Json(ErrorResponse {
                error: "Center not accessible for this mode".to_string(),
            }),
        )
            .into_response();
    }

    // Get custom weights (avoid takes priority, then exclude)
    let exclude_weights = if avoid_entry.is_none() {
        exclude_mask.map(|exc| state.get_exclude_weights(mode, exc))
    } else {
        None
    };

    // Select weights based on metric type and custom weights.
    // - `up_flat` / `down_flat` (target-keyed reverse): used by the
    //   bounded-search reverse PHAST and as ambient state for snap path.
    // - `down_fwd_flat`: used by the *forward* isochrone downward scan.
    let (up_flat, down_flat, down_fwd_flat, node_weights) = if use_distance_weights {
        if let Some(ref entry) = avoid_entry {
            (
                &entry.weights.dist_up_flat,
                &entry.weights.dist_down_flat,
                &entry.weights.dist_down_fwd_flat,
                state.node_weights_dist.as_slice(),
            )
        } else if let Some(ref ew) = exclude_weights {
            (
                &ew.dist_up_flat,
                &ew.dist_down_flat,
                &ew.dist_down_fwd_flat,
                state.node_weights_dist.as_slice(),
            )
        } else {
            (
                &mode_data.up_adj_flat_dist,
                &mode_data.down_rev_flat_dist,
                &mode_data.down_adj_flat_dist,
                state.node_weights_dist.as_slice(),
            )
        }
    } else if let Some(ref entry) = avoid_entry {
        (
            &entry.weights.time_up_flat,
            &entry.weights.time_down_flat,
            &entry.weights.time_down_fwd_flat,
            mode_data.node_weights.as_slice(),
        )
    } else if let Some(ref ew) = exclude_weights {
        (
            &ew.time_up_flat,
            &ew.time_down_flat,
            &ew.time_down_fwd_flat,
            mode_data.node_weights.as_slice(),
        )
    } else {
        (
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            &mode_data.down_adj_flat,
            mode_data.node_weights.as_slice(),
        )
    };

    // Run PHAST once with max threshold
    let phast_settled = if reverse {
        run_phast_bounded_fast_reverse(up_flat, down_flat, center_rank, phast_threshold, mode)
    } else {
        run_phast_bounded_fast(up_flat, down_fwd_flat, center_rank, phast_threshold, mode)
    };

    // Convert to original IDs
    let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
    for (rank, dist) in phast_settled {
        let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        let original_id = mode_data.filtered_to_original[filtered_id as usize];
        settled.push((original_id, dist));
    }

    // Helper: build polygon for a single contour threshold from the settled set
    let build_contour_polygon = |threshold: u32| -> Vec<Point> {
        build_isochrone_geometry(
            &settled,
            threshold,
            node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
            &req.mode,
        )
    };

    // Helper: encode polygon in requested format
    #[allow(clippy::type_complexity)]
    let encode_polygon = |polygon: &[Point],
                          format: GeometryFormat|
     -> (Option<String>, Option<Vec<[f64; 2]>>, Option<Vec<Point>>) {
        match format {
            GeometryFormat::Polyline6 => (Some(encode_polyline6(polygon)), None, None),
            GeometryFormat::GeoJson => {
                use crate::range::wkb_stream::ensure_ccw;
                let trunc = |v: f64| (v * 1e5).round() / 1e5;
                let mut coords: Vec<(f64, f64)> = polygon
                    .iter()
                    .map(|p| (trunc(p.lon), trunc(p.lat)))
                    .collect();
                ensure_ccw(&mut coords);
                let mut ring: Vec<[f64; 2]> = coords.into_iter().map(|(x, y)| [x, y]).collect();
                if let (Some(first), Some(last)) = (ring.first().copied(), ring.last().copied())
                    && first != last
                {
                    ring.push(first);
                }
                (None, Some(ring), None)
            }
            GeometryFormat::Points => (None, None, Some(polygon.to_vec())),
        }
    };

    // Build list of thresholds with their labels
    let thresholds: Vec<(u32, Option<u32>, Option<u32>)> = match &metric {
        IsoMetric::Time(ds) => vec![(*ds, Some(*ds / 10), None)],
        IsoMetric::Distance(mm) => vec![(*mm, None, Some(*mm / 1000))],
        IsoMetric::MultiTime(vals) => vals.iter().map(|&ds| (ds, Some(ds / 10), None)).collect(),
    };

    // WKB path (content negotiation)
    if wants_wkb {
        use crate::range::contour::ContourResult;
        use crate::range::wkb_stream::encode_polygon_wkb;

        if thresholds.len() > 1 {
            return (
                StatusCode::BAD_REQUEST,
                Json(ErrorResponse {
                    error: "WKB only supports single contour. Use JSON for multiple.".to_string(),
                }),
            )
                .into_response();
        }
        let polygon = build_contour_polygon(thresholds[0].0);
        let coords: Vec<(f64, f64)> = polygon.iter().map(|p| (p.lon, p.lat)).collect();
        let contour = ContourResult {
            outer_ring: coords,
            holes: vec![],
            stats: Default::default(),
        };
        super::region_metrics::record_query(
            &region_id,
            "isochrone",
            started_dispatch.elapsed().as_secs_f64(),
        );
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
    let contour_features: Vec<ContourFeature> = thresholds
        .iter()
        .map(|&(threshold, time_s, distance_m)| {
            let polygon = build_contour_polygon(threshold);
            let reachable = settled.iter().filter(|&&(_, d)| d <= threshold).count();
            let (poly_enc, poly_geo, poly_pts) = encode_polygon(&polygon, geom_format);
            ContourFeature {
                time_s,
                distance_m,
                polygon: poly_enc,
                polygon_geojson: poly_geo,
                polygon_points: poly_pts,
                reachable_edges: reachable,
            }
        })
        .collect();

    // Build network at max threshold if requested
    let network = if include_network {
        Some(build_network_geometry(
            &settled,
            phast_threshold,
            node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
        ))
    } else {
        None
    };

    super::region_metrics::record_query(
        &region_id,
        "isochrone",
        started_dispatch.elapsed().as_secs_f64(),
    );
    Json(IsochroneResponse {
        contours: contour_features,
        network,
    })
    .into_response()
}

/// Build network geometry - all reachable road segments as polylines
pub fn build_network_geometry(
    settled: &[(u32, u32)],
    time_ds: u32,
    node_weights: &[u32],
    ebg_nodes: &crate::formats::EbgNodes,
    edge_geom: &crate::server::edge_geom::EdgeGeometry,
) -> Vec<Vec<[f64; 2]>> {
    let mut network: Vec<Vec<[f64; 2]>> = Vec::with_capacity(settled.len());

    for &(ebg_id, dist_ds) in settled {
        if dist_ds > time_ds {
            continue;
        }

        let weight_ds = if (ebg_id as usize) < node_weights.len() {
            node_weights[ebg_id as usize]
        } else {
            continue;
        };

        if weight_ds == 0 || weight_ds == u32::MAX {
            continue;
        }

        let node = &ebg_nodes.nodes[ebg_id as usize];
        let polyline = edge_geom.polyline(node.geom_idx);
        if polyline.is_empty() {
            continue;
        }

        let dist_end_ds = dist_ds.saturating_add(weight_ds);

        if dist_end_ds <= time_ds {
            // Fully reachable — emit every (lon, lat) f64 pair.
            let coords: Vec<[f64; 2]> = polyline.iter().map(|(lon, lat)| [lon, lat]).collect();
            if coords.len() >= 2 {
                network.push(coords);
            }
        } else {
            // Partially reachable - clip to cut_idx (inclusive).
            let cut_fraction = (time_ds - dist_ds) as f32 / weight_ds as f32;
            let n_pts = polyline.len();
            let cut_idx = ((n_pts - 1) as f32 * cut_fraction).ceil() as usize;
            let cut_idx = cut_idx.min(n_pts - 1).max(1);

            let coords: Vec<[f64; 2]> = (0..=cut_idx)
                .map(|i| {
                    let (lon, lat) = polyline.at(i);
                    [lon, lat]
                })
                .collect();
            if coords.len() >= 2 {
                network.push(coords);
            }
        }
    }

    network
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
    let started_dispatch = std::time::Instant::now();
    let coords_iter = req.origins.iter().map(|&[lon, lat]| (lon, lat));
    let (state, region_id) = match regions.dispatch_many(coords_iter, &req.mode) {
        Ok(pair) => pair,
        Err(e) => {
            let (code, body) = e.into_response_parts();
            return (code, Json(body)).into_response();
        }
    };

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
    let time_ds = req.time_s * 10;

    // Compute avoid weights (includes exclude if both present)
    let avoid_entry = if let Some(ref avoid_str) = avoid_json {
        match super::avoid::compute_avoid_weights(&state, mode_data, avoid_str, exclude_mask) {
            Ok(entry) => Some(entry),
            Err(e) => {
                return (StatusCode::BAD_REQUEST, Json(ErrorResponse { error: e })).into_response();
            }
        }
    } else {
        None
    };

    // Get exclude weights if only exclude (no avoid)
    let exclude_weights = if avoid_entry.is_none() {
        exclude_mask.map(|exc| state.get_exclude_weights(mode, exc))
    } else {
        None
    };

    // Build snap mask
    let snap_mask: Vec<u64> = if let Some(ref entry) = avoid_entry {
        super::avoid::build_avoid_mask(
            &mode_data.mask,
            &entry.flags,
            exclude_mask.map(|exc| (state.edge_exclude_flags.as_slice(), exc)),
        )
    } else if let Some(exc) = exclude_mask {
        super::exclude::build_exclude_mask(&mode_data.mask, &state.edge_exclude_flags, exc)
    } else {
        mode_data.mask.clone()
    };

    // Select forward flat adjacencies for PHAST
    let (up_flat, down_fwd_flat) = if let Some(ref entry) = avoid_entry {
        (
            &entry.weights.time_up_flat,
            &entry.weights.time_down_fwd_flat,
        )
    } else if let Some(ref ew) = exclude_weights {
        (&ew.time_up_flat, &ew.time_down_fwd_flat)
    } else {
        (&mode_data.up_adj_flat, &mode_data.down_adj_flat)
    };

    // Bulk isochrones are depart-only (no `direction` field), so origins
    // act as sources. Apply the #197 directional role filter.
    let origin_role_filter = SnapRole::Src.role_filter(mode_data);

    // Process all origins in parallel
    let results: Vec<(u32, Vec<u8>)> = req
        .origins
        .par_iter()
        .enumerate()
        .filter_map(|(idx, &[lon, lat])| {
            // Snap origin
            let center_orig = state.snap_index.snap_filtered_role(
                lon,
                lat,
                mode.0,
                Some(&snap_mask),
                origin_role_filter,
            )?;
            let center_rank = mode_data.orig_to_rank[center_orig as usize];
            if center_rank == u32::MAX {
                return None;
            }

            // Run PHAST - Note: thread-local state handles per-thread allocation
            let phast_settled =
                run_phast_bounded_fast(up_flat, down_fwd_flat, center_rank, time_ds, mode);

            // Convert to original IDs
            let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
            for (rank, dist) in phast_settled {
                let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
                let original_id = mode_data.filtered_to_original[filtered_id as usize];
                settled.push((original_id, dist));
            }

            // Build polygon using frontier-based concave hull
            let points = build_isochrone_geometry(
                &settled,
                time_ds,
                &mode_data.node_weights,
                &state.ebg_nodes,
                &state.edge_geom,
                &req.mode,
            );
            let outer_ring: Vec<(f64, f64)> = points.iter().map(|p| (p.lon, p.lat)).collect();
            let contour = ContourResult {
                outer_ring,
                holes: vec![],
                stats: Default::default(),
            };

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

    super::region_metrics::record_query(
        &region_id,
        "isochrone_bulk",
        started_dispatch.elapsed().as_secs_f64(),
    );

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
