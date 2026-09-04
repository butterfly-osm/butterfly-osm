//! Geometry reconstruction from EBG path

use rustc_hash::FxHashSet;
use serde::Serialize;
use utoipa::ToSchema;

use crate::formats::{CchTopo, CchWeights, EbgNodes};
use crate::matrix::bucket_ch::{DownAdjFlat, DownReverseAdjFlat, UpAdjFlat};
use crate::model::types::Mode;
use crate::range::{ContourPolygon, ReachableSegment, SparseContourConfig};
use crate::server::edge_geom::EdgeGeometry;
use crate::server::state::{ModeData, ServerState};
use crate::server::types::SnapRole;

/// A point in WGS84 coordinates
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
pub struct Point {
    pub lon: f64,
    pub lat: f64,
}

/// Geometry encoding format
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeometryFormat {
    /// Array of {lon, lat} objects (legacy)
    Points,
    /// Google Encoded Polyline with 6-digit precision
    Polyline6,
    /// GeoJSON LineString
    GeoJson,
}

impl GeometryFormat {
    pub fn parse(s: &str) -> Result<Self, String> {
        match s.to_lowercase().as_str() {
            "polyline6" => Ok(GeometryFormat::Polyline6),
            "geojson" => Ok(GeometryFormat::GeoJson),
            "points" => Ok(GeometryFormat::Points),
            other => Err(format!(
                "Unknown geometry format '{}'. Use: polyline6, geojson, points",
                other
            )),
        }
    }
}

/// Route geometry — serialized differently based on format
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RouteGeometry {
    /// Encoded polyline string (only for polyline6 format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub polyline: Option<String>,
    /// GeoJSON coordinates [[lon, lat], ...] (only for geojson format)
    #[serde(skip_serializing_if = "Option::is_none")]
    #[schema(value_type = Option<Vec<Vec<f64>>>)]
    pub coordinates_geojson: Option<Vec<[f64; 2]>>,
    /// Point array [{lon, lat}, ...] (only for points format)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub coordinates: Option<Vec<Point>>,
}

impl RouteGeometry {
    /// Create geometry in the requested format from raw coordinate list
    pub fn from_points(points: Vec<Point>, format: GeometryFormat) -> Self {
        match format {
            GeometryFormat::Polyline6 => RouteGeometry {
                polyline: Some(encode_polyline6(&points)),
                coordinates_geojson: None,
                coordinates: None,
            },
            GeometryFormat::GeoJson => RouteGeometry {
                polyline: None,
                coordinates_geojson: Some(points.iter().map(|p| [p.lon, p.lat]).collect()),
                coordinates: None,
            },
            GeometryFormat::Points => RouteGeometry {
                polyline: None,
                coordinates_geojson: None,
                coordinates: Some(points),
            },
        }
    }
}

/// Encode coordinates as Google Encoded Polyline with 6-digit precision
///
/// Reference: https://developers.google.com/maps/documentation/utilities/polylinealgorithm
/// Polyline6 uses 1e6 multiplier (6 decimal places) instead of the standard 1e5
pub fn encode_polyline6(points: &[Point]) -> String {
    let mut result = String::with_capacity(points.len() * 6);
    let mut prev_lat: i64 = 0;
    let mut prev_lon: i64 = 0;

    for p in points {
        let lat = (p.lat * 1e6).round() as i64;
        let lon = (p.lon * 1e6).round() as i64;

        encode_value(lat - prev_lat, &mut result);
        encode_value(lon - prev_lon, &mut result);

        prev_lat = lat;
        prev_lon = lon;
    }

    result
}

/// Encode a single signed integer as variable-length encoded characters
fn encode_value(value: i64, out: &mut String) {
    // Left-shift and invert if negative
    let mut v = if value < 0 {
        (!value) << 1 | 1
    } else {
        value << 1
    } as u64;

    // Break into 5-bit chunks, set continuation bit on all but last
    loop {
        let mut chunk = (v & 0x1F) as u8;
        v >>= 5;
        if v > 0 {
            chunk |= 0x20; // continuation bit
        }
        out.push((chunk + 63) as char);
        if v == 0 {
            break;
        }
    }
}

/// Extract raw deduped coordinate list and total distance from EBG path.
///
/// This is the shared core for both `build_geometry` and GPX output.
/// Returns a freshly allocated `Vec<Point>`; prefer
/// [`build_raw_points_into`] in hot paths where the caller can supply a
/// reusable buffer.
pub fn build_raw_points(
    ebg_path: &[u32],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
) -> (Vec<Point>, f64) {
    let mut coordinates = Vec::new();
    let total_distance_m = build_raw_points_into(ebg_path, ebg_nodes, edge_geom, &mut coordinates);
    (coordinates, total_distance_m)
}

/// #273: in-place variant — appends points into `coordinates`.
/// Clears `coordinates` first; returns total distance in metres.
pub fn build_raw_points_into(
    ebg_path: &[u32],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    coordinates: &mut Vec<Point>,
) -> f64 {
    coordinates.clear();
    let mut total_distance_m = 0.0;

    for &ebg_id in ebg_path {
        let node = &ebg_nodes.nodes[ebg_id as usize];
        let polyline = edge_geom.polyline(node.geom_idx);
        let n = polyline.len();
        if n == 0 {
            total_distance_m += node.length_m as f64;
            continue;
        }
        // #493: a per-edge polyline is stored in ONE orientation, but the path may
        // traverse the edge either way (customized/recustomized shortcut unpacks
        // pick edges whose stored orientation is reversed). Appending forward
        // unconditionally makes the polyline zigzag (~2× length). Orient each edge
        // so the endpoint nearest the running path connects first; length_m is
        // orientation-independent so the returned distance is unchanged.
        // #522: the FIRST edge has no running path yet — orient it against the
        // NEXT edge's nearer endpoint instead (a reversed first edge drew an
        // out-and-back: 684 m of polyline on a 491 m path, foot pair in Forest).
        let reversed = match coordinates.last() {
            Some(prev) => dist_sq(prev, polyline.at(n - 1)) < dist_sq(prev, polyline.at(0)),
            None => ebg_path.get(1).is_some_and(|&next_id| {
                let next = &ebg_nodes.nodes[next_id as usize];
                let np = edge_geom.polyline(next.geom_idx);
                if np.is_empty() {
                    return false;
                }
                let d = |a: (f64, f64), b: (f64, f64)| {
                    let (dx, dy) = (a.0 - b.0, a.1 - b.1);
                    dx * dx + dy * dy
                };
                let near = |from: (f64, f64)| d(from, np.at(0)).min(d(from, np.at(np.len() - 1)));
                // if the first edge's STORED START is closer to the next edge
                // than its stored end, the traversal runs tail-ward: reverse.
                near(polyline.at(0)) < near(polyline.at(n - 1))
            }),
        };
        if reversed {
            for j in (0..n).rev() {
                let (lon, lat) = polyline.at(j);
                coordinates.push(Point { lon, lat });
            }
        } else {
            for (lon, lat) in polyline.iter() {
                coordinates.push(Point { lon, lat });
            }
        }

        // #297: EBG `length_m` is in metres (was `length_mm` in v1).
        total_distance_m += node.length_m as f64;
    }

    coordinates.dedup_by(|a, b| (a.lon - b.lon).abs() < 1e-9 && (a.lat - b.lat).abs() < 1e-9);

    total_distance_m
}

/// Unpack a CCH query result into the original-EBG path it represents,
/// then build that path's polyline and return its length in metres.
///
/// This is THE route-geometry builder. `/route` and the Flight
/// `route_batch` batch surface both reach their polyline through it, so
/// neither can grow its own idea of what a route looks like: #493 was
/// exactly that failure — a surface that appended every edge's stored
/// polyline forward, drawing a route whose polyline was ~2x the
/// `distance_m` the same surface reported, because half the traversals
/// run against the stored orientation.
///
/// `rank_path` and `ebg_path` are caller-owned scratch so a batch of
/// thousands of pairs pays no per-pair allocation; both are cleared on
/// entry and left holding this pair's path (callers need `ebg_path`
/// afterwards for steps and annotations). The returned length is the sum
/// of the traversed edges' `length_m` — the SAME number every surface
/// reports as `distance_m`, which is why polyline length and
/// `distance_m` must agree.
#[allow(clippy::too_many_arguments)]
pub fn build_route_points_into(
    topo: &CchTopo,
    weights: &CchWeights,
    filtered_to_original: &[u32],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    forward_parent: &[(u32, u32)],
    backward_parent: &[(u32, u32)],
    src_rank: u32,
    rank_path: &mut Vec<u32>,
    ebg_path: &mut Vec<u32>,
    points: &mut Vec<Point>,
) -> f64 {
    crate::server::unpack::unpack_path_into(
        topo,
        weights,
        forward_parent,
        backward_parent,
        src_rank,
        rank_path,
    );
    ebg_path.clear();
    ebg_path.reserve(rank_path.len());
    for &rank in rank_path.iter() {
        let filtered_id = topo.rank_to_filtered[rank as usize];
        ebg_path.push(filtered_to_original[filtered_id as usize]);
    }
    build_raw_points_into(ebg_path, ebg_nodes, edge_geom, points)
}

/// Squared planar distance between a Point and a (lon, lat) tuple — cheap
/// endpoint-proximity test for edge orientation (no need for true metric distance).
#[inline]
fn dist_sq(a: &Point, b: (f64, f64)) -> f64 {
    let (dx, dy) = (a.lon - b.0, a.lat - b.1);
    dx * dx + dy * dy
}

/// Build route geometry from EBG node sequence
pub fn build_geometry(
    ebg_path: &[u32],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    format: GeometryFormat,
) -> (RouteGeometry, f64) {
    let (coordinates, total_distance_m) = build_raw_points(ebg_path, ebg_nodes, edge_geom);
    (
        RouteGeometry::from_points(coordinates, format),
        total_distance_m,
    )
}

/// Build isochrone geometry using sparse tile rasterization + boundary tracing
///
/// This is the validated algorithm that:
/// 1. Stamps reachable road segments into a sparse tile grid
/// 2. For frontier edges: clips polyline at cut_fraction, stamps only reachable prefix
/// 3. Applies local morphology (dilation/erosion) to create fillable regions
/// 4. Extracts boundary via Moore-neighbor tracing (O(perimeter))
///
/// This respects road network topology and produces geometrically correct isochrones.
#[allow(clippy::too_many_arguments)]
pub fn build_isochrone_geometry(
    settled_nodes: &[(u32, u32)], // (original_ebg_id, distance) — seconds for time, meters for isodistance (post-#297)
    max_threshold: u32,
    node_weights: &[u32], // Edge costs indexed by original EBG node ID
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    mode_name: &str,
    origin_anchor: Option<(f64, f64)>, // exact snapped (lon, lat) of the query origin (#497/#506)
    pin: Option<(f64, f64)>,
    model: &ReachModel<'_>,
) -> Vec<Point> {
    build_isochrone_topology(
        settled_nodes,
        max_threshold,
        node_weights,
        ebg_nodes,
        edge_geom,
        mode_name,
        origin_anchor,
        pin,
        model,
    )
    .into_iter()
    .next()
    .map(|p| {
        p.outer
            .into_iter()
            .map(|(lon, lat)| Point { lon, lat })
            .collect()
    })
    .unwrap_or_default()
}

/// The isochrone topology served to the API: the ONE polygon of the
/// origin's component, no holes, WGS84 `(lon, lat)` — an isochrone is one
/// simple polygon by definition (#535/#542), enforced by the contour type
/// since #570. `build_isochrone_geometry` is the ring-only view of this.
#[allow(clippy::too_many_arguments)]
pub fn build_isochrone_topology(
    settled_nodes: &[(u32, u32)],
    max_threshold: u32,
    node_weights: &[u32],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    mode_name: &str,
    origin_anchor: Option<(f64, f64)>,
    pin: Option<(f64, f64)>,
    model: &ReachModel<'_>,
) -> Vec<ContourPolygon> {
    let geo_start = std::time::Instant::now();
    let result = build_isochrone_geometry_sparse(
        settled_nodes,
        max_threshold,
        node_weights,
        ebg_nodes,
        edge_geom,
        mode_name,
        origin_anchor,
        pin,
        model,
    );
    let geo_us = geo_start.elapsed().as_micros();
    tracing::debug!(
        threshold = max_threshold,
        settled_input = settled_nodes.len(),
        polygon_vertices = result.first().map_or(0, |p| p.outer.len()),
        components = result.len(),
        geometry_us = geo_us,
        "isochrone geometry pipeline timing"
    );
    result
}

/// How PHAST labels map onto road reach (2026-09-03, found via #543's gate).
///
/// Every PHAST label is the cost at the **head** of the directed edge: a
/// depart seed is the remainder of the origin edge past the snap
/// (`phantom.rs`) and an original CCH arc `e→f` weighs `w(f) + turn(e,f)`
/// (`customization.rs`). Consequences:
/// * **Depart**: `label(e) ≤ T` ⇒ the WHOLE edge is driven within T. The
///   partially reachable edges are the unreached successors `f`, entered at
///   `label(e) + turn(e,f) < T`; they are NOT in the settled set and are
///   enumerated from the reached edges' arcs (`depart_frontier`). The former
///   rule (`label + w(e) ≤ T` else cut the edge itself) counted the edge's
///   own weight twice: every fast edge at the boundary was cut one full
///   weight early (a 3.9 km motorway edge lost its last ~200 m), the edges
///   beyond it (reached, shorter) became detached islands, and the true
///   frontier was never drawn. Measured on dev: 4-6 % of road points >150 m
///   outside the polygon were reachable within T (up to 178 s early).
/// * **Arrive**: the seed is the prefix up to the snap and the label of `x`
///   is the cost from x's tail to the target; the settled edge itself is the
///   partial: whole iff `label + w ≤ T`, else its head-side fraction
///   `(T − label)/w`. Verified against `/table` (no outside road point
///   reached before 0.98 T).
pub enum ReachModel<'a> {
    /// `(original EBG id, fraction of the edge driven from its tail before T)`
    Depart {
        frontier: &'a [(u32, f32)],
    },
    Arrive,
}

impl<'a> ReachModel<'a> {
    /// `Arrive` for a reverse (arrive) field, else `Depart` with `frontier`.
    pub fn for_direction(reverse: bool, frontier: &'a [(u32, f32)]) -> Self {
        if reverse {
            ReachModel::Arrive
        } else {
            ReachModel::Depart { frontier }
        }
    }
}

/// Lat-first e7 polylines plus the legacy anchor fallback.
pub type ReachPolylines = (Vec<Vec<(i32, i32)>>, Option<(i32, i32)>);

/// ONE definition of "which part of which road is reached", shared by the
/// polygon stamp and `include=network` so they can never disagree. Returns
/// lat-first e7 polylines (whole edges, then oriented frontier fragments)
/// and the legacy anchor fallback (start of the minimum-label edge).
///
/// `want_anchor` = false skips the min-label scan entirely: the caller
/// already knows the exact snapped origin (`origin_anchor`), so the derived
/// fallback would be thrown away (#549).
pub fn reachable_polylines(
    settled_nodes: &[(u32, u32)], // (original_ebg_id, label)
    max_threshold: u32,
    node_weights: &[u32],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    model: &ReachModel<'_>,
    want_anchor: bool,
) -> ReachPolylines {
    let mut out: Vec<Vec<(i32, i32)>> = Vec::with_capacity(settled_nodes.len());
    let mut anchor: Option<(i32, i32)> = None;
    let mut anchor_dist = u32::MAX;
    let mut partial: Vec<(u32, f32)> = Vec::new();

    for &(ebg_id, dist) in settled_nodes {
        if dist > max_threshold {
            continue;
        }
        let Some(&weight) = node_weights.get(ebg_id as usize) else {
            continue;
        };
        if weight == 0 || weight == u32::MAX {
            continue;
        }
        let node = &ebg_nodes.nodes[ebg_id as usize];
        let polyline = edge_geom.polyline(node.geom_idx);
        if polyline.is_empty() {
            continue;
        }
        if want_anchor && dist < anchor_dist {
            anchor_dist = dist;
            anchor = Some(polyline.at_lat_lon_e7(0));
        }
        let whole = match model {
            ReachModel::Depart { .. } => true,
            ReachModel::Arrive => dist.saturating_add(weight) <= max_threshold,
        };
        if whole {
            out.push(polyline.iter_lat_lon_e7().collect());
        } else {
            partial.push((ebg_id, (max_threshold - dist) as f32 / weight as f32));
        }
    }
    if let ReachModel::Depart { frontier } = model {
        partial.extend_from_slice(frontier);
    }
    if partial.is_empty() {
        // Nothing hangs off the whole edges: never build the endpoint set
        // (it hashes 2 entries per reached edge — millions on a wide car
        // isochrone — for a lookup nobody performs).
        return (out, anchor);
    }

    // Endpoints of whole edges: a frontier fragment hangs off one of them.
    let mut reached_ends: FxHashSet<(i32, i32)> =
        FxHashSet::with_capacity_and_hasher(out.len() * 2, Default::default());
    for points in &out {
        reached_ends.insert(points[0]);
        reached_ends.insert(points[points.len() - 1]);
    }

    // Frontier fragments. A stored polyline is shared by both directed twins
    // (#493), so "from index 0 to the cut" is wrong for the twin that runs it
    // backwards: the fragment would sit at the FAR end, detached (#542
    // confetti). The true start of a fragment is an endpoint of a whole edge.
    for (ebg_id, fraction) in partial {
        let node = &ebg_nodes.nodes[ebg_id as usize];
        let polyline = edge_geom.polyline(node.geom_idx);
        if polyline.is_empty() {
            continue;
        }
        let mut points: Vec<(i32, i32)> = polyline.iter_lat_lon_e7().collect();
        match frontier_orientation(&points, &reached_ends) {
            Some(true) => points.reverse(),
            Some(false) => {}
            None => continue, // hangs off nothing we drew: confetti only
        }
        let cut = partial_polyline(&points, fraction.clamp(0.0, 1.0));
        if !cut.is_empty() {
            out.push(cut);
        }
    }
    (out, anchor)
}

/// Sparse-raster isochrone topology from a PHAST field (see `ReachModel`).
///
/// Pipeline: reachable polylines → stamp (sparse 64×64 bit tiles) → balanced
/// closing → +1-cell halo → boundary tracing → ONE simple polygon (the
/// origin's component, #497).
#[allow(clippy::too_many_arguments)]
pub fn build_isochrone_geometry_sparse(
    settled_nodes: &[(u32, u32)], // (original_ebg_id, label)
    max_time: u32,
    node_weights: &[u32], // Edge costs indexed by original EBG node ID
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    mode_name: &str,
    origin_anchor: Option<(f64, f64)>, // exact snapped (lon, lat); fallback = min-label edge start
    // The raw query point (lon, lat). #535: a pin in a car-free zone snaps
    // tens of metres away and used to sit OUTSIDE its own isochrone; the
    // access leg pin→snap (≤ 500 m) is stamped so the pin is always inside.
    pin: Option<(f64, f64)>,
    model: &ReachModel<'_>,
) -> Vec<ContourPolygon> {
    let config = SparseContourConfig::for_mode_name_with_threshold(mode_name, max_time);
    let (polylines, anchor) = reachable_polylines(
        settled_nodes,
        max_time,
        node_weights,
        ebg_nodes,
        edge_geom,
        model,
        // The min-label fallback is only consulted when the caller has no
        // exact snap; don't scan for it otherwise (#549).
        origin_anchor.is_none(),
    );
    if polylines.is_empty() {
        return vec![];
    }
    let mut segments: Vec<ReachableSegment> = polylines
        .into_iter()
        .map(|points| ReachableSegment { points })
        .collect();
    if let (Some((slon, slat)), Some((plon, plat))) = (origin_anchor, pin) {
        let kx = 111_320.0 * slat.to_radians().cos();
        let access_m = ((plon - slon) * kx).hypot((plat - slat) * 110_540.0);
        if access_m > 1.0 && access_m <= 500.0 {
            segments.push(ReachableSegment {
                points: vec![
                    ((plat * 1e7) as i32, (plon * 1e7) as i32),
                    ((slat * 1e7) as i32, (slon * 1e7) as i32),
                ],
            });
        }
    }

    // Prefer the EXACT snapped origin when the handler supplies it (#506 —
    // the derived min-label edge START can sit a whole edge away from the
    // snap on long rural chains); the derived anchor remains the fallback.
    let anchor = origin_anchor
        .map(|(lon, lat)| ((lat * 1e7) as i32, (lon * 1e7) as i32))
        .or(anchor);
    match crate::range::generate_sparse_contour_anchored(&segments, &config, anchor) {
        // ONE simple polygon, no holes — the tracer cannot return more (#570).
        Ok(result) if result.ring.len() >= 3 => vec![ContourPolygon {
            outer: result.ring,
            holes: vec![],
        }],
        _ => vec![],
    }
}

// ===========================================================================
// ONE isochrone pipeline (#549)
// ===========================================================================

/// The recustomized flats an isochrone runs on when the request carries
/// `avoid_polygons` / `exclude` — otherwise the mode's own flats are used.
#[derive(Clone, Copy)]
pub struct IsochroneFlats<'a> {
    /// UP adjacency (upward sweep, both directions).
    pub up: &'a UpAdjFlat,
    /// DOWN adjacency (forward downward scan).
    pub down_fwd: &'a DownAdjFlat,
    /// Target-keyed reverse DOWN adjacency (arrive field).
    pub down_rev: &'a DownReverseAdjFlat,
}

/// One isochrone query against one weight set — the input every surface
/// (REST single/contours, REST bands, `/isochrone/bulk`, Flight `isochrone`,
/// catchment road hull) used to spell out for itself before #549.
pub struct IsochroneQuery<'a> {
    /// Raw query point (also the stamped access-leg pin, #535).
    pub lon: f64,
    pub lat: f64,
    /// One topology per entry, returned in THIS order.
    pub thresholds: &'a [u32],
    /// `true` = arrive field (reverse PHAST); `false` = depart.
    pub reverse: bool,
    /// Contour-config key — the mode name AS THE CALLER NAMES IT
    /// (`SparseContourConfig::for_mode_name_with_threshold`).
    pub mode_name: &'a str,
    /// Snap bitset; `None` = unfiltered (Flight / catchment).
    pub snap_mask: Option<&'a [u64]>,
    /// Recustomized flats (avoid / exclude). `Some` ALSO forces the legacy
    /// single seed: phantom partial costs assume base weights.
    pub flats: Option<IsochroneFlats<'a>>,
    /// `include=network` at the max threshold (shares that frontier).
    pub include_network: bool,
}

/// Everything the surfaces need back: the settled field (for
/// `reachable_edges` counts), one topology per requested threshold, the
/// exact snapped anchor, and the reached network when asked for.
pub struct IsochroneField {
    /// `(original EBG id, label)` within the MAX threshold.
    pub settled: Vec<(u32, u32)>,
    /// One entry per `IsochroneQuery::thresholds` entry, same order.
    pub topologies: Vec<Vec<ContourPolygon>>,
    /// `include=network`: reached road polylines as `[lon, lat]`.
    pub network: Option<Vec<Vec<[f64; 2]>>>,
    /// Exact snapped center (`origin_anchor`), when the phantom snap ran.
    pub anchor: Option<(f64, f64)>,
}

/// Why an isochrone could not even start. Each surface renders its own
/// message (400 / 404 / empty WKB) — the core stays transport-agnostic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum IsochroneSnapError {
    /// The center does not snap to the road network.
    NoSnap,
    /// It snaps, but the snapped edge is not accessible for this mode.
    NotAccessible,
}

/// Depart frontiers memoised per DISTINCT threshold: `include=network` at
/// the max threshold reuses that threshold's contour frontier instead of
/// recomputing it (#549 — `depart_frontier` used to run twice per request).
struct FrontierCache<'a> {
    /// `(threshold, frontier)`, at most one entry per distinct threshold.
    slots: Vec<(u32, Vec<(u32, f32)>)>,
    /// An arrive field has no frontier: the settled edge IS the partial.
    reverse: bool,
    phast_settled: &'a [(u32, u32)],
    up: &'a UpAdjFlat,
    down_fwd: &'a DownAdjFlat,
    mode_data: &'a ModeData,
    node_weights: &'a [u32],
}

impl FrontierCache<'_> {
    fn slot(&mut self, threshold: u32) -> usize {
        if let Some(i) = self.slots.iter().position(|(t, _)| *t == threshold) {
            return i;
        }
        let frontier = if self.reverse {
            Vec::new()
        } else {
            crate::server::isochrone_handler::depart_frontier(
                self.phast_settled,
                threshold,
                self.up,
                self.down_fwd,
                self.mode_data,
                self.node_weights,
            )
        };
        self.slots.push((threshold, frontier));
        self.slots.len() - 1
    }
}

/// THE isochrone pipeline: snap → phantom seeds → seeded PHAST →
/// rank→original → per-threshold depart frontier → `ReachModel` →
/// topology (one simple polygon, the origin's component).
///
/// Every isochrone surface goes through here so they cannot drift apart:
/// same seeds, same thresholds, same anchor, same access-leg pin.
pub fn isochrone_polygons(
    state: &ServerState,
    mode_data: &ModeData,
    mode: Mode,
    q: &IsochroneQuery<'_>,
) -> Result<IsochroneField, IsochroneSnapError> {
    // Directional snap role (#197): depart → the center is a source (needs
    // outbound arcs), arrive → a destination (needs inbound).
    let role = if q.reverse {
        SnapRole::Dst
    } else {
        SnapRole::Src
    };
    let center_orig = state
        .snap_index
        .snap_filtered_role(
            q.lon,
            q.lat,
            mode.0,
            q.snap_mask,
            role.role_filter(mode_data),
        )
        .ok_or(IsochroneSnapError::NoSnap)?;
    let center_rank = mode_data.orig_to_rank[center_orig as usize];
    if center_rank == u32::MAX {
        return Err(IsochroneSnapError::NotAccessible);
    }

    // #506: phantom center — seed both directed twins (and near-equidistant
    // parallel edges) so the polygon isn't committed to one departure /
    // arrival direction of the snapped edge. Custom-weight paths
    // (avoid/exclude) keep the legacy single seed.
    let (seeds, anchor) = if q.flats.is_none() {
        crate::server::phantom::isochrone_center_seeds(
            state,
            mode_data,
            mode,
            q.lon,
            q.lat,
            role,
            q.snap_mask,
            q.reverse,
            center_rank,
        )
    } else {
        (vec![(center_rank, 0)], None)
    };

    let up = q.flats.map_or(&mode_data.up_adj_flat, |f| f.up);
    let down_fwd = q.flats.map_or(&mode_data.down_adj_flat, |f| f.down_fwd);
    let down_rev = q.flats.map_or(&mode_data.down_rev_flat, |f| f.down_rev);
    let node_weights = &mode_data.node_weights[..];

    // One PHAST run at the MAX threshold; every contour is a slice of it.
    let max_threshold = q.thresholds.iter().copied().max().unwrap_or(0);
    let phast_settled = if q.reverse {
        crate::range::phast_seeded::run_phast_bounded_fast_reverse_seeded(
            up,
            down_rev,
            &seeds,
            max_threshold,
            mode,
        )
    } else {
        crate::range::phast_seeded::run_phast_bounded_fast_seeded(
            up,
            down_fwd,
            &seeds,
            max_threshold,
            mode,
        )
    };

    // Rank → original EBG id (ranks are kept: the depart frontier scans arcs).
    let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
    for &(rank, dist) in &phast_settled {
        let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        settled.push((mode_data.filtered_to_original[filtered_id as usize], dist));
    }

    let mut frontiers = FrontierCache {
        slots: Vec::new(),
        reverse: q.reverse,
        phast_settled: &phast_settled,
        up,
        down_fwd,
        mode_data,
        node_weights,
    };
    let mut topologies = Vec::with_capacity(q.thresholds.len());
    for &threshold in q.thresholds {
        let slot = frontiers.slot(threshold);
        topologies.push(build_isochrone_topology(
            &settled,
            threshold,
            node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
            q.mode_name,
            anchor,
            Some((q.lon, q.lat)),
            &ReachModel::for_direction(q.reverse, &frontiers.slots[slot].1),
        ));
    }

    let network = q.include_network.then(|| {
        let slot = frontiers.slot(max_threshold);
        crate::server::isochrone_handler::build_network_geometry(
            &settled,
            max_threshold,
            node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
            &ReachModel::for_direction(q.reverse, &frontiers.slots[slot].1),
        )
    });

    Ok(IsochroneField {
        settled,
        topologies,
        network,
        anchor,
    })
}

/// The three mutually exclusive contour encodings of one ring:
/// `(polygon, polygon_geojson, polygon_points)`.
pub type EncodedContour = (Option<String>, Option<Vec<[f64; 2]>>, Option<Vec<Point>>);

/// ONE contour encoder for every surface (#548.3): the ring is normalised
/// exactly once — CCW like the WKB encoder, then CLOSED — and only then
/// rendered in the requested format. The band branch used to re-encode
/// inline and shipped an unclosed GeoJSON ring; by construction it cannot
/// any more.
pub fn encode_contour(ring: &[Point], format: GeometryFormat) -> EncodedContour {
    fn normalise(mut coords: Vec<(f64, f64)>) -> Vec<(f64, f64)> {
        crate::range::wkb_stream::ensure_ccw(&mut coords);
        if let (Some(&first), Some(&last)) = (coords.first(), coords.last())
            && first != last
        {
            coords.push(first);
        }
        coords
    }
    match format {
        GeometryFormat::Polyline6 => {
            let pts: Vec<Point> = normalise(ring.iter().map(|p| (p.lon, p.lat)).collect())
                .into_iter()
                .map(|(lon, lat)| Point { lon, lat })
                .collect();
            (Some(encode_polyline6(&pts)), None, None)
        }
        GeometryFormat::GeoJson => {
            // 5 decimals (~1 m) — the JSON surfaces have always truncated
            // before orienting, so the ring's winding is judged on the same
            // coordinates the client receives.
            let trunc = |v: f64| (v * 1e5).round() / 1e5;
            let coords = normalise(ring.iter().map(|p| (trunc(p.lon), trunc(p.lat))).collect());
            (
                None,
                Some(coords.into_iter().map(|(x, y)| [x, y]).collect()),
                None,
            )
        }
        GeometryFormat::Points => {
            let pts: Vec<Point> = normalise(ring.iter().map(|p| (p.lon, p.lat)).collect())
                .into_iter()
                .map(|(lon, lat)| Point { lon, lat })
                .collect();
            (None, None, Some(pts))
        }
    }
}

/// The primary polygon's outer ring as `Point`s — the legacy
/// `polygon` / `polygon_geojson` / `polygon_points` view of a topology.
pub fn primary_outer_ring(topology: &[ContourPolygon]) -> Vec<Point> {
    topology
        .first()
        .map(|p| {
            p.outer
                .iter()
                .map(|&(lon, lat)| Point { lon, lat })
                .collect()
        })
        .unwrap_or_default()
}

/// Orientation of a frontier edge's stored polyline for this traversal:
/// `Some(false)` = stored order, `Some(true)` = reversed (only its LAST
/// point touches the reached set), `None` = neither endpoint touches it.
/// A frontier edge's true start is an endpoint of an already-reached edge,
/// so the `None` case cannot be placed and is skipped rather than stamped
/// at a guessed end (that guess produced floating slivers, #542). Both
/// ends reached keeps the stored order: either fragment then lies on
/// reachable roads.
pub(crate) fn frontier_orientation(
    points: &[(i32, i32)],
    reached_ends: &FxHashSet<(i32, i32)>,
) -> Option<bool> {
    match points {
        [first, .., last] => match (reached_ends.contains(first), reached_ends.contains(last)) {
            (false, false) => None,
            (false, true) => Some(true),
            _ => Some(false),
        },
        [only] => reached_ends.contains(only).then_some(false),
        [] => None,
    }
}

/// Partial polyline from its first point to `fraction` of its VERTEX span
/// (lat-first `(lat_e7, lon_e7)`, matching the sparse contour stamper).
fn partial_polyline(points: &[(i32, i32)], fraction: f32) -> Vec<(i32, i32)> {
    let n_pts = points.len();

    if n_pts == 0 || fraction <= 0.0 {
        return vec![];
    }

    if n_pts == 1 || fraction >= 1.0 {
        return points.to_vec();
    }

    // Find the segment where the cut occurs
    let n_segments = n_pts - 1;
    let segment_frac = fraction * n_segments as f32;
    let segment_idx = (segment_frac.floor() as usize).min(n_segments - 1);
    let local_frac = segment_frac - segment_idx as f32;

    // Include all points up to and including the start of the cut segment.
    let mut out: Vec<(i32, i32)> = points[..=segment_idx].to_vec();

    // Add the interpolated cut point
    if local_frac > 0.0 && segment_idx + 1 < n_pts {
        let (lat1, lon1) = points[segment_idx];
        let (lat2, lon2) = points[segment_idx + 1];
        let lat = lat1 + ((lat2 - lat1) as f32 * local_frac) as i32;
        let lon = lon1 + ((lon2 - lon1) as f32 * local_frac) as i32;
        out.push((lat, lon));
    }

    out
}

#[cfg(test)]
mod frontier_orientation_tests {
    use super::*;

    #[test]
    fn frontier_orientation_from_reached_endpoints() {
        let mut reached = FxHashSet::default();
        reached.insert((10, 10));
        let fwd = [(10, 10), (20, 20), (30, 30)];
        let rev = [(30, 30), (20, 20), (10, 10)];
        let both = [(10, 10), (20, 20), (10, 10)];
        let none = [(50, 50), (60, 60)];
        assert_eq!(frontier_orientation(&fwd, &reached), Some(false));
        assert_eq!(frontier_orientation(&rev, &reached), Some(true));
        assert_eq!(
            frontier_orientation(&both, &reached),
            Some(false),
            "keep stored order"
        );
        assert_eq!(
            frontier_orientation(&none, &reached),
            None,
            "unplaceable: skipped"
        );
    }

    #[test]
    fn partial_polyline_cuts_from_the_first_point() {
        let pts = [(0, 0), (0, 1000), (0, 2000)];
        assert_eq!(partial_polyline(&pts, 0.25), vec![(0, 0), (0, 500)]);
        assert_eq!(partial_polyline(&pts, 0.5), vec![(0, 0), (0, 1000)]);
        assert_eq!(partial_polyline(&pts, 1.0), pts.to_vec());
        assert!(partial_polyline(&pts, 0.0).is_empty());
        // A reversed frontier edge is cut from its TRUE start (the far end of
        // the stored order) once the caller reverses it.
        let mut rev = pts.to_vec();
        rev.reverse();
        assert_eq!(partial_polyline(&rev, 0.25), vec![(0, 2000), (0, 1500)]);
    }
}

/// Decode polyline6 back to coordinates (for testing round-trip)
#[cfg(test)]
pub fn decode_polyline6(encoded: &str) -> Vec<(f64, f64)> {
    let mut result = Vec::new();
    let mut lat: i64 = 0;
    let mut lon: i64 = 0;
    let chars: Vec<u8> = encoded.bytes().collect();
    let mut i = 0;

    while i < chars.len() {
        // Decode latitude
        let mut shift = 0u32;
        let mut value: i64 = 0;
        loop {
            let b = (chars[i] as i64) - 63;
            i += 1;
            value |= (b & 0x1F) << shift;
            shift += 5;
            if b < 0x20 {
                break;
            }
        }
        lat += if (value & 1) != 0 {
            !(value >> 1)
        } else {
            value >> 1
        };

        // Decode longitude
        shift = 0;
        value = 0;
        loop {
            let b = (chars[i] as i64) - 63;
            i += 1;
            value |= (b & 0x1F) << shift;
            shift += 5;
            if b < 0x20 {
                break;
            }
        }
        lon += if (value & 1) != 0 {
            !(value >> 1)
        } else {
            value >> 1
        };

        result.push((lat as f64 / 1e6, lon as f64 / 1e6));
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    // #493: an edge whose stored polyline is reversed relative to traversal must
    // be oriented to connect, not appended forward (which zigzags → ~2× length).
    #[test]
    fn build_raw_points_orients_reversed_edges() {
        use crate::formats::ArcCow;
        use crate::formats::ebg_nodes::{EbgNode, EbgNodes};
        use crate::formats::edge_geom::{EdgeGeomOffsets, EdgeGeomPoints};
        use crate::server::edge_geom::EdgeGeometry;
        // edge0 forward: lon 0→1→2 ; edge1 STORED reversed: lon 4→3 (traversed 2→3→4).
        let off = EdgeGeomOffsets {
            n_edges: 2,
            n_points: 5,
            offsets: ArcCow::from_vec(vec![0u32, 3, 5]),
        };
        let pts = EdgeGeomPoints {
            n_points: 5,
            bbox_min_lon: 0,
            bbox_min_lat: 0,
            bbox_max_lon: 40_000_000,
            bbox_max_lat: 0,
            points: ArcCow::from_vec(vec![
                0, 0, 10_000_000, 0, 20_000_000, 0, 40_000_000, 0, 30_000_000, 0,
            ]),
        };
        let geom = EdgeGeometry::from_sections(off, pts).unwrap();
        let mk = |gi: u32| EbgNode {
            tail_nbg: 0,
            head_nbg: 0,
            geom_idx: gi,
            length_m: 10,
            class_bits: 0,
            primary_way: 0,
        };
        let ebg = EbgNodes {
            n_nodes: 2,
            created_unix: 0,
            inputs_sha: [0u8; 32],
            nodes: ArcCow::from_vec(vec![mk(0), mk(1)]),
        };
        let mut coords = Vec::new();
        build_raw_points_into(&[0, 1], &ebg, &geom, &mut coords);
        let lons: Vec<i64> = coords.iter().map(|p| p.lon.round() as i64).collect();
        assert_eq!(
            lons,
            vec![0, 1, 2, 3, 4],
            "reversed edge must be oriented to connect (monotonic), not zigzag 0,1,2,4,3"
        );
    }

    #[test]
    fn test_encode_polyline6_empty() {
        let points: Vec<Point> = vec![];
        let encoded = encode_polyline6(&points);
        assert_eq!(encoded, "");
    }

    #[test]
    fn test_encode_polyline6_single_point() {
        let points = vec![Point {
            lon: 4.351700,
            lat: 50.850300,
        }];
        let encoded = encode_polyline6(&points);
        assert!(!encoded.is_empty());
        let decoded = decode_polyline6(&encoded);
        assert_eq!(decoded.len(), 1);
        assert!((decoded[0].0 - 50.850300).abs() < 1e-6);
        assert!((decoded[0].1 - 4.351700).abs() < 1e-6);
    }

    #[test]
    fn test_encode_polyline6_round_trip() {
        let points = vec![
            Point {
                lon: 4.351700,
                lat: 50.850300,
            },
            Point {
                lon: 4.401700,
                lat: 50.860300,
            },
            Point {
                lon: 4.867100,
                lat: 50.467400,
            },
        ];
        let encoded = encode_polyline6(&points);
        let decoded = decode_polyline6(&encoded);
        assert_eq!(decoded.len(), 3);
        for (i, pt) in points.iter().enumerate() {
            assert!(
                (decoded[i].0 - pt.lat).abs() < 1e-6,
                "lat mismatch at {}: {} vs {}",
                i,
                decoded[i].0,
                pt.lat
            );
            assert!(
                (decoded[i].1 - pt.lon).abs() < 1e-6,
                "lon mismatch at {}: {} vs {}",
                i,
                decoded[i].1,
                pt.lon
            );
        }
    }

    #[test]
    fn test_encode_polyline6_negative_coords() {
        let points = vec![
            Point {
                lon: -73.985428,
                lat: 40.748817,
            }, // NYC
            Point {
                lon: -118.243685,
                lat: 34.052234,
            }, // LA
        ];
        let encoded = encode_polyline6(&points);
        let decoded = decode_polyline6(&encoded);
        assert_eq!(decoded.len(), 2);
        for (i, pt) in points.iter().enumerate() {
            assert!(
                (decoded[i].0 - pt.lat).abs() < 1e-6,
                "lat mismatch at {}",
                i
            );
            assert!(
                (decoded[i].1 - pt.lon).abs() < 1e-6,
                "lon mismatch at {}",
                i
            );
        }
    }

    #[test]
    fn test_encode_polyline6_close_points() {
        // Points separated by ~1 meter
        let points = vec![
            Point {
                lon: 4.351700,
                lat: 50.850300,
            },
            Point {
                lon: 4.351714,
                lat: 50.850309,
            },
        ];
        let encoded = encode_polyline6(&points);
        let decoded = decode_polyline6(&encoded);
        assert_eq!(decoded.len(), 2);
        for (i, pt) in points.iter().enumerate() {
            assert!((decoded[i].0 - pt.lat).abs() < 1e-6);
            assert!((decoded[i].1 - pt.lon).abs() < 1e-6);
        }
    }

    #[test]
    fn test_geometry_format_parse() {
        assert_eq!(
            GeometryFormat::parse("polyline6").unwrap(),
            GeometryFormat::Polyline6
        );
        assert_eq!(
            GeometryFormat::parse("POLYLINE6").unwrap(),
            GeometryFormat::Polyline6
        );
        assert_eq!(
            GeometryFormat::parse("geojson").unwrap(),
            GeometryFormat::GeoJson
        );
        assert_eq!(
            GeometryFormat::parse("GeoJson").unwrap(),
            GeometryFormat::GeoJson
        );
        assert_eq!(
            GeometryFormat::parse("points").unwrap(),
            GeometryFormat::Points
        );
        assert!(GeometryFormat::parse("invalid").is_err());
        assert!(GeometryFormat::parse("").is_err());
    }

    #[test]
    fn test_route_geometry_polyline6_format() {
        let points = vec![
            Point {
                lon: 4.3517,
                lat: 50.8503,
            },
            Point {
                lon: 4.4017,
                lat: 50.8603,
            },
        ];
        let geom = RouteGeometry::from_points(points, GeometryFormat::Polyline6);
        assert!(geom.polyline.is_some());
        assert!(geom.coordinates_geojson.is_none());
        assert!(geom.coordinates.is_none());
    }

    #[test]
    fn test_route_geometry_geojson_format() {
        let points = vec![
            Point {
                lon: 4.3517,
                lat: 50.8503,
            },
            Point {
                lon: 4.4017,
                lat: 50.8603,
            },
        ];
        let geom = RouteGeometry::from_points(points, GeometryFormat::GeoJson);
        assert!(geom.polyline.is_none());
        assert!(geom.coordinates_geojson.is_some());
        assert!(geom.coordinates.is_none());
        let coords = geom.coordinates_geojson.unwrap();
        assert_eq!(coords.len(), 2);
        assert!((coords[0][0] - 4.3517).abs() < 1e-10);
        assert!((coords[0][1] - 50.8503).abs() < 1e-10);
        assert!((coords[1][0] - 4.4017).abs() < 1e-10);
        assert!((coords[1][1] - 50.8603).abs() < 1e-10);
    }

    #[test]
    fn test_route_geometry_points_format() {
        let points = vec![
            Point {
                lon: 4.3517,
                lat: 50.8503,
            },
            Point {
                lon: 4.4017,
                lat: 50.8603,
            },
        ];
        let geom = RouteGeometry::from_points(points, GeometryFormat::Points);
        assert!(geom.polyline.is_none());
        assert!(geom.coordinates_geojson.is_none());
        assert!(geom.coordinates.is_some());
        let coords = geom.coordinates.unwrap();
        assert_eq!(coords.len(), 2);
        assert!((coords[0].lon - 4.3517).abs() < 1e-10);
        assert!((coords[0].lat - 50.8503).abs() < 1e-10);
    }

    #[test]
    fn test_polyline6_geojson_same_coordinates() {
        let points = vec![
            Point {
                lon: 4.3517,
                lat: 50.8503,
            },
            Point {
                lon: 4.4017,
                lat: 50.8603,
            },
            Point {
                lon: 4.8671,
                lat: 50.4674,
            },
        ];
        let poly_geom = RouteGeometry::from_points(points.clone(), GeometryFormat::Polyline6);
        let json_geom = RouteGeometry::from_points(points.clone(), GeometryFormat::GeoJson);

        // Decode polyline and compare to geojson coordinates
        let decoded = decode_polyline6(poly_geom.polyline.as_ref().unwrap());
        let geojson_coords = json_geom.coordinates_geojson.unwrap();

        assert_eq!(decoded.len(), geojson_coords.len());
        for i in 0..decoded.len() {
            assert!(
                (decoded[i].0 - geojson_coords[i][1]).abs() < 1e-6,
                "lat mismatch at {}",
                i
            );
            assert!(
                (decoded[i].1 - geojson_coords[i][0]).abs() < 1e-6,
                "lon mismatch at {}",
                i
            );
        }
    }

    #[test]
    fn test_route_geometry_has_no_distance_or_duration() {
        // RouteGeometry is pure geometry — distance and duration belong at the
        // route/step/alternative level, not embedded in the geometry object.
        let points = vec![
            Point {
                lon: 4.3517,
                lat: 50.8503,
            },
            Point {
                lon: 4.4017,
                lat: 50.8603,
            },
        ];
        let geom = RouteGeometry::from_points(points, GeometryFormat::GeoJson);
        let json = serde_json::to_value(&geom).unwrap();
        let obj = json.as_object().unwrap();
        assert!(
            !obj.contains_key("distance_m"),
            "geometry should not contain distance_m"
        );
        assert!(
            !obj.contains_key("duration_ds"),
            "geometry should not contain duration_ds"
        );
        assert!(
            !obj.contains_key("duration_s"),
            "geometry should not contain duration_s"
        );
        // Should only have the geometry-related keys
        assert!(obj.contains_key("coordinates_geojson"));
    }
}
