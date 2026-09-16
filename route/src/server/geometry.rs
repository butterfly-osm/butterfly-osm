//! Geometry reconstruction from EBG path

use rustc_hash::{FxHashMap, FxHashSet};
use serde::Serialize;
use utoipa::ToSchema;

use crate::formats::nbg_geo::NbgEdge;
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
/// Equirectangular segment length in meters (fine at street scale).
pub fn seg_len_m(a: &Point, b: &Point) -> f64 {
    let ky = 111_320.0;
    let kx = 111_320.0 * (a.lat.to_radians().cos());
    let (dx, dy) = ((a.lon - b.lon) * kx, (a.lat - b.lat) * ky);
    (dx * dx + dy * dy).sqrt()
}

/// Remove `cut_m` meters of polyline from the start, inserting an
/// interpolated boundary point (#522 phantom end clipping).
fn cut_polyline_start(pts: &mut Vec<Point>, cut_m: f64) {
    if cut_m <= 0.0 || pts.len() < 2 {
        return;
    }
    let mut acc = 0.0;
    for i in 0..pts.len() - 1 {
        let l = seg_len_m(&pts[i], &pts[i + 1]);
        if acc + l >= cut_m {
            let t = if l > 0.0 { (cut_m - acc) / l } else { 0.0 };
            let p = Point {
                lon: pts[i].lon + (pts[i + 1].lon - pts[i].lon) * t,
                lat: pts[i].lat + (pts[i + 1].lat - pts[i].lat) * t,
            };
            pts.drain(0..=i);
            pts[0] = p;
            return;
        }
        acc += l;
    }
    // cut longer than the polyline: keep the final point only
    let last = *pts.last().unwrap();
    pts.clear();
    pts.push(last);
}

/// Remove `cut_m` meters of polyline from the end (mirror of the above).
fn cut_polyline_end(pts: &mut Vec<Point>, cut_m: f64) {
    if cut_m <= 0.0 || pts.len() < 2 {
        return;
    }
    let mut acc = 0.0;
    for i in (1..pts.len()).rev() {
        let l = seg_len_m(&pts[i - 1], &pts[i]);
        if acc + l >= cut_m {
            let t = if l > 0.0 { (cut_m - acc) / l } else { 0.0 };
            let p = Point {
                lon: pts[i].lon + (pts[i - 1].lon - pts[i].lon) * t,
                lat: pts[i].lat + (pts[i - 1].lat - pts[i].lat) * t,
            };
            pts.truncate(i + 1);
            let n = pts.len();
            pts[n - 1] = p;
            return;
        }
        acc += l;
    }
    let first = pts[0];
    pts.clear();
    pts.push(first);
}

/// Bill only the PARTIAL first and last edges of a phantom-seeded route
/// (#522, #604).
///
/// `end_clip` is `(src_frac, dst_frac)`: the fraction of the first edge
/// already behind the origin snap, and the fraction of the last edge already
/// travelled at the destination snap — exactly the partials the seeded query
/// charged into `duration_s`. Cuts `pts` in place and returns the corrected
/// distance, so geometry, distance and duration bill the same road.
///
/// ONE body for `/route` and Flight `route_batch` (#604). They used to
/// disagree: duration was clipped on both, distance and geometry only on
/// `/route`, so the same pair returned a slightly longer distance depending
/// on which transport the caller used. A shared helper is the only shape in
/// which they cannot drift apart again.
pub fn clip_route_ends(
    ebg_nodes: &EbgNodes,
    ebg_path: &[u32],
    pts: &mut Vec<Point>,
    distance_m: f64,
    end_clip: Option<(f64, f64)>,
) -> f64 {
    let Some((fs, fd)) = end_clip else {
        return distance_m;
    };
    let (Some(&e0), Some(&en)) = (ebg_path.first(), ebg_path.last()) else {
        return distance_m;
    };
    let head_cut = fs * ebg_nodes.nodes[e0 as usize].length_m as f64;
    let tail_cut = (1.0 - fd) * ebg_nodes.nodes[en as usize].length_m as f64;
    cut_polyline_start(pts, head_cut);
    cut_polyline_end(pts, tail_cut);
    (distance_m - head_cut - tail_cut).max(0.0)
}

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

/// The isochrone topology served to the API: the ONE polygon of the
/// origin's component, no holes, WGS84 `(lon, lat)` — an isochrone is one
/// simple polygon by definition (#535/#542), enforced by the contour type
/// since #570.
#[allow(clippy::too_many_arguments)]
pub fn build_isochrone_topology(
    settled_nodes: &[(u32, u32)],
    max_threshold: u32,
    node_weights: &[u32],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    config: SparseContourConfig,
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
        config,
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
/// * **Arrive**: the exact mirror. A reverse label is the cost from the
///   HEAD of `x` to the snap (`isochrone_polygons` removes the seed shift,
///   #544), so `x` is entered from its tail at `label + w(x)` and the
///   partially reachable edges are the SETTLED ones — no successor scan and
///   no reverse-UP adjacency: `arrive_reach` is the whole rule. Whole iff
///   `label + w ≤ T`, else the head-side fraction `(T − label)/w`, nothing
///   at all once `label ≥ T`. Truth is `/table`'s many-to-one column, which
///   seeds the same reverse field with the same shift.
pub enum ReachModel<'a> {
    /// `(original EBG id, fraction of the edge driven from its tail before T)`
    Depart {
        frontier: &'a [(u32, f32)],
    },
    Arrive,
    /// #620: a depart ISODISTANCE. Reach is decided per physical segment from
    /// the exact `(time, length)` entries of BOTH directed twins
    /// (`length_reach_fragments`): `fragments` = `(original EBG id, fraction
    /// of that edge driven from ITS tail)`, a whole edge being `1.0`; every
    /// edge is oriented exactly on its shared polyline through `nbg_edges`
    /// (forward iff its tail is the NBG edge's `u_node`).
    DepartLength {
        fragments: &'a [(u32, f32)],
        nbg_edges: &'a [NbgEdge],
    },
    /// #620, arrive: the mirror — `fragments` = `(original EBG id, fraction
    /// of that edge driven INTO its head)`, decided per physical segment by
    /// `length_reach_fragments_arrive`.
    ArriveLength {
        fragments: &'a [(u32, f32)],
        nbg_edges: &'a [NbgEdge],
    },
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

/// ONE definition of the ARRIVE field's reach, the mirror of
/// `depart_frontier` (#544).
///
/// A normalised arrive label is the cost from the HEAD of the edge to the
/// snap, so driving the edge from a point at fraction `φ` of its length
/// costs `(1 − φ)·w + label`: the reachable part of the edge is its
/// head-side `(T − label)/w`, capped at the whole edge. Everything with a
/// reachable part is therefore ALREADY in the settled set — an edge the
/// field never labelled cannot have one — which is why the arrive direction
/// needs no predecessor scan and no reverse-UP adjacency.
///
/// Returns the reachable head-side fraction in `(0, 1]`, or `None` when no
/// point of the edge is reachable before `threshold`.
pub fn arrive_reach(label: u32, weight: u32, threshold: u32) -> Option<f32> {
    if label >= threshold || weight == 0 {
        return None;
    }
    let budget = threshold - label;
    if budget >= weight {
        return Some(1.0);
    }
    Some(budget as f32 / weight as f32)
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
    match model {
        ReachModel::DepartLength {
            fragments,
            nbg_edges,
        } => {
            return length_polylines(
                settled_nodes,
                max_threshold,
                fragments,
                nbg_edges,
                ebg_nodes,
                edge_geom,
                want_anchor,
                false,
            );
        }
        ReachModel::ArriveLength {
            fragments,
            nbg_edges,
        } => {
            return length_polylines(
                settled_nodes,
                max_threshold,
                fragments,
                nbg_edges,
                ebg_nodes,
                edge_geom,
                want_anchor,
                true,
            );
        }
        _ => {}
    }
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
        let reach = match model {
            ReachModel::Depart { .. }
            | ReachModel::DepartLength { .. }
            | ReachModel::ArriveLength { .. } => Some(1.0),
            ReachModel::Arrive => arrive_reach(dist, weight, max_threshold),
        };
        match reach {
            None => continue,
            Some(f) if f >= 1.0 => out.push(polyline.iter_lat_lon_e7().collect()),
            Some(f) => partial.push((ebg_id, f)),
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

/// #620: which part of each directed edge an ISODISTANCE serves.
///
/// An isodistance is the set of road points whose length along the
/// TIME-shortest path is within `threshold_len`. A time field could draw a
/// reached edge whole and a frontier edge from its tail because "unreached"
/// meant the head's time exceeds T, so the tail-side arrival was
/// time-optimal for every point it drew. On the carried channel that
/// implication is gone: a segment's head can be reached FAST by a LONG road,
/// and then the head side is the time-optimal arrival for most of the
/// segment while the tail side is within the length budget — measured
/// 2026-09-16, one served endpoint sat 19.5 km along its time-shortest path
/// for a 5 km isodistance.
///
/// So the rule is per physical segment, from the exact `(entry length,
/// entry time)` of both directed twins (`depart_entries_2ch`): a point at
/// `x` metres from edge `s`'s tail is reached via `s` at
/// `entry_t(s) + x·w_t(s)/len`, via its twin at
/// `entry_t(s') + (len − x)·w_t(s')/len`; the faster one is the
/// time-shortest path to that point, and the point is admissible iff THAT
/// side's length is within budget. Along `s` this is the prefix
/// `[0, min(len, budget − entry_len(s), x_m)]`, `x_m` the meeting point of
/// the two arrivals — nothing at all once the twin is faster from the tail.
/// The twin's own prefix covers the other end. A one-way segment has no
/// twin: only the budget cuts it.
pub fn length_reach_fragments(
    entries: &FxHashMap<u32, (u32, u32)>, // orig -> (entry length m, entry time)
    threshold_len: u32,
    twin_of: &[u32],
    w_len: &[u32],
    w_time: &[u32],
) -> Vec<(u32, f32)> {
    let mut out: Vec<(u32, f32)> = Vec::with_capacity(entries.len());
    for (&orig, &(el, et)) in entries {
        let len = w_len[orig as usize];
        if len == 0 || len == u32::MAX || el >= threshold_len {
            continue;
        }
        let mut x = (len.min(threshold_len - el)) as f64;
        let twin = twin_of.get(orig as usize).copied().unwrap_or(u32::MAX);
        if twin != u32::MAX
            && let Some(&(_, et2)) = entries.get(&twin)
        {
            let (wt, wt2) = (w_time[orig as usize] as f64, w_time[twin as usize] as f64);
            let denom = wt + wt2;
            if denom > 0.0 {
                // entry_t(s) + x·wt/len == entry_t(s') + (len − x)·wt2/len
                let x_m = (et2 as f64 + wt2 - et as f64) * len as f64 / denom;
                if x_m <= 0.0 {
                    continue; // the twin is the faster arrival from this tail on
                }
                x = x.min(x_m);
            }
        }
        out.push((orig, (x / len as f64).min(1.0) as f32));
    }
    out.sort_unstable_by_key(|&(orig, _)| orig);
    out
}

/// #620, ARRIVE: the mirror of [`length_reach_fragments`]. An arrive label
/// is the cost from the edge's HEAD to the snap, so a point at `x` from
/// edge `s`'s tail reaches the snap via `s` at
/// `label_t(s) + (len − x)·w_t(s)/len` and via its twin at
/// `label_t(s') + x·w_t(s')/len`; the faster is the time-shortest path from
/// that point, and it is admissible iff THAT side's length is within budget.
/// Along `s` this is the SUFFIX `[max(x_m, len − (budget − label_len(s))),
/// len]`, `x_m` the meeting point — returned as the fraction of `s` driven
/// into its head, like [`arrive_reach`].
pub fn length_reach_fragments_arrive(
    labels: &FxHashMap<u32, (u32, u32)>, // orig -> (label length m, label time)
    threshold_len: u32,
    twin_of: &[u32],
    w_len: &[u32],
    w_time: &[u32],
) -> Vec<(u32, f32)> {
    let mut out: Vec<(u32, f32)> = Vec::with_capacity(labels.len());
    for (&orig, &(ll, lt)) in labels {
        let len = w_len[orig as usize];
        if len == 0 || len == u32::MAX || ll >= threshold_len {
            continue;
        }
        let lenf = len as f64;
        // the budget-admissible suffix starts here (measured from the tail)
        let mut x_start = (lenf - (threshold_len - ll) as f64).max(0.0);
        let twin = twin_of.get(orig as usize).copied().unwrap_or(u32::MAX);
        if twin != u32::MAX
            && let Some(&(_, lt2)) = labels.get(&twin)
        {
            let (wt, wt2) = (w_time[orig as usize] as f64, w_time[twin as usize] as f64);
            let denom = wt + wt2;
            if denom > 0.0 {
                // label_t(s) + (len − x)·wt/len == label_t(s') + x·wt2/len
                let x_m = (lt as f64 + wt - lt2 as f64) * lenf / denom;
                if x_m >= lenf {
                    continue; // the twin is the faster departure all the way to the head
                }
                x_start = x_start.max(x_m);
            }
        }
        let frac = ((lenf - x_start) / lenf).clamp(0.0, 1.0) as f32;
        if frac > 0.0 {
            out.push((orig, frac));
        }
    }
    out.sort_unstable_by_key(|&(orig, _)| orig);
    out
}

fn twin_of_entry(ctx: &LengthReachCtx<'_>, twin: u32) -> Option<(u32, u32, u32, u32)> {
    let &(l, t) = ctx.entries.get(&twin)?;
    Some((l, t, ctx.w_len[twin as usize], ctx.w_time[twin as usize]))
}

/// The `DepartLength` / `ArriveLength` polylines: every fragment from ITS
/// edge's tail (depart) or into its head (arrive), the edge oriented exactly
/// on the shared polyline. The anchor keeps the legacy definition (start of
/// the minimum-label edge).
#[allow(clippy::too_many_arguments)]
fn length_polylines(
    settled_nodes: &[(u32, u32)],
    max_threshold: u32,
    fragments: &[(u32, f32)],
    nbg_edges: &[NbgEdge],
    ebg_nodes: &EbgNodes,
    edge_geom: &EdgeGeometry,
    want_anchor: bool,
    from_head: bool,
) -> ReachPolylines {
    let mut anchor: Option<(i32, i32)> = None;
    if want_anchor {
        let mut best = u32::MAX;
        for &(ebg_id, dist) in settled_nodes {
            if dist <= max_threshold && dist < best {
                let node = &ebg_nodes.nodes[ebg_id as usize];
                let polyline = edge_geom.polyline(node.geom_idx);
                if !polyline.is_empty() {
                    best = dist;
                    anchor = Some(polyline.at_lat_lon_e7(0));
                }
            }
        }
    }
    let mut out: Vec<Vec<(i32, i32)>> = Vec::with_capacity(fragments.len());
    for &(ebg_id, fraction) in fragments {
        let node = &ebg_nodes.nodes[ebg_id as usize];
        let polyline = edge_geom.polyline(node.geom_idx);
        if polyline.is_empty() {
            continue;
        }
        let mut points: Vec<(i32, i32)> = polyline.iter_lat_lon_e7().collect();
        let forward = nbg_edges
            .get(node.geom_idx as usize)
            .is_none_or(|e| e.u_node == node.tail_nbg);
        if !forward {
            points.reverse();
        }
        if fraction >= 1.0 {
            out.push(points);
        } else {
            if from_head {
                points.reverse(); // cut from the head end
            }
            let cut = partial_polyline(&points, fraction.clamp(0.0, 1.0));
            if !cut.is_empty() {
                out.push(cut);
            }
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
    // Raster + simplification tuning, picked by the caller from the mode AND
    // the metric (#612): a threshold in seconds and one in metres imply
    // different extents, so they select different tiers.
    config: SparseContourConfig,
    origin_anchor: Option<(f64, f64)>, // exact snapped (lon, lat); fallback = min-label edge start
    // The raw query point (lon, lat). #535: a pin in a car-free zone snaps
    // tens of metres away and used to sit OUTSIDE its own isochrone; the
    // access leg pin→snap (≤ 500 m) is stamped so the pin is always inside.
    pin: Option<(f64, f64)>,
    model: &ReachModel<'_>,
) -> Vec<ContourPolygon> {
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

/// What an isochrone's thresholds MEASURE (#612).
///
/// Both variants run the SAME search on the SAME hierarchy and settle on the
/// SAME paths — the time-shortest ones. Only the quantity accumulated along
/// that path, and therefore the quantity the threshold is compared against,
/// differs. That is the whole point: the isodistance removed in #373 ran
/// PHAST on the separate distance-shortest CCH, so it reported reachability
/// along a different geometric path from every other endpoint in the engine.
/// This one cannot: its metres are the metres `/route`, `/table` and the
/// Flight matrix already report for the same origin and destination
/// (`cch_weights_len_along_time`, #371/#372), which makes the matrix an
/// exact independent truth for the polygon.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThresholdMetric {
    /// Seconds of travel time — the classic isochrone.
    Time,
    /// Metres of road length accumulated along the time-shortest path —
    /// the isodistance.
    LengthAlongTime,
}

/// One isochrone query against one weight set — the input every surface
/// (REST single/contours, REST bands, `/isochrone/bulk`, Flight `isochrone`,
/// catchment road hull) used to spell out for itself before #549.
pub struct IsochroneQuery<'a> {
    /// What `thresholds` are measured in (#612). `Time` = seconds,
    /// `LengthAlongTime` = metres.
    pub metric: ThresholdMetric,
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
    /// #612: a distance threshold was asked for on a mode that carries no
    /// length-along-time weights — a container built before PR #377. There
    /// is no honest answer to give: the only other length in the container
    /// is the distance-shortest metric, which measures a DIFFERENT path and
    /// is exactly what #373 removed.
    NoLengthChannel,
    /// #612: a distance threshold was asked for together with
    /// `avoid_polygons` / `exclude`. Those recustomize the time weights for
    /// this one request; nothing recustomizes the length-along-time channel
    /// to match, so the metres would be the metres of a path the modified
    /// weights no longer choose.
    LengthWithCustomWeights,
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
    /// #620: set on a depart ISODISTANCE — the slots then hold
    /// `length_reach_fragments` and the model is `DepartLength`.
    length: Option<LengthReachCtx<'a>>,
}

/// Everything a depart isodistance needs to decide reach per physical
/// segment (#620): both twins' entries, the twin map, both weight channels.
struct LengthReachCtx<'a> {
    /// Depart: entries `(entry length, entry time)`; arrive: labels
    /// `(label length, label time)` — the mirror needs no predecessor scan.
    reverse: bool,
    entries: FxHashMap<u32, (u32, u32)>,
    twin_of: &'a [u32],
    w_len: &'a [u32],
    w_time: &'a [u32],
    nbg_edges: &'a [NbgEdge],
}

impl<'a> FrontierCache<'a> {
    fn slot(&mut self, threshold: u32) -> usize {
        if let Some(i) = self.slots.iter().position(|(t, _)| *t == threshold) {
            return i;
        }
        let frontier = if let Some(ctx) = &self.length {
            if ctx.reverse {
                length_reach_fragments_arrive(
                    &ctx.entries,
                    threshold,
                    ctx.twin_of,
                    ctx.w_len,
                    ctx.w_time,
                )
            } else {
                length_reach_fragments(&ctx.entries, threshold, ctx.twin_of, ctx.w_len, ctx.w_time)
            }
        } else if self.reverse {
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
    /// The reach model for a computed slot.
    fn model(&self, slot: usize) -> ReachModel<'_> {
        match &self.length {
            Some(ctx) if ctx.reverse => ReachModel::ArriveLength {
                fragments: &self.slots[slot].1,
                nbg_edges: ctx.nbg_edges,
            },
            Some(ctx) => ReachModel::DepartLength {
                fragments: &self.slots[slot].1,
                nbg_edges: ctx.nbg_edges,
            },
            None => ReachModel::for_direction(self.reverse, &self.slots[slot].1),
        }
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

    // #612: the isodistance runs on the length-along-time channel, which
    // only exists as a weight set of its own — nothing derives it from a
    // recustomized one, and old containers do not carry it at all. Both
    // gaps are refused here rather than silently answered on the wrong
    // metric, which is the mistake #373 removed.
    let len_flats = match q.metric {
        ThresholdMetric::Time => None,
        ThresholdMetric::LengthAlongTime => {
            if q.flats.is_some() {
                return Err(IsochroneSnapError::LengthWithCustomWeights);
            }
            let up_len = mode_data
                .up_adj_flat_len_along_time
                .as_ref()
                .ok_or(IsochroneSnapError::NoLengthChannel)?;
            let down_fwd_len = mode_data
                .down_len_flat()
                .ok_or(IsochroneSnapError::NoLengthChannel)?;
            let down_rev_len = mode_data
                .down_rev_flat_len_along_time
                .as_ref()
                .ok_or(IsochroneSnapError::NoLengthChannel)?;
            Some((up_len, down_fwd_len, down_rev_len))
        }
    };

    // #506: phantom center — seed both directed twins (and near-equidistant
    // parallel edges) so the polygon isn't committed to one departure /
    // arrival direction of the snapped edge. Custom-weight paths
    // (avoid/exclude) keep the legacy single seed.
    // `shift` is the arrive field's seed offset (#544): every label comes
    // back as `true cost + shift`, and is normalised below. Depart: 0.
    // #612: the isodistance seeds BOTH channels and carries a second shift,
    // for the metres of the arrival edge past the snap.
    let (seeds, shift, seeds_2ch, shift_len, anchor) = match (len_flats, q.flats.is_none()) {
        (None, true) => {
            let (s, sh, a) = crate::server::phantom::isochrone_center_seeds(
                state,
                mode_data,
                mode,
                q.lon,
                q.lat,
                role,
                q.snap_mask,
                q.reverse,
                center_rank,
            );
            (s, sh, Vec::new(), 0, a)
        }
        (None, false) => (vec![(center_rank, 0)], 0, Vec::new(), 0, None),
        // A length metric never takes the custom-weight branch: it was
        // refused above.
        (Some(_), _) => {
            let (s, sh_t, sh_l, a) = crate::server::phantom::isochrone_center_seeds_2ch(
                state,
                mode_data,
                mode,
                q.lon,
                q.lat,
                role,
                q.snap_mask,
                q.reverse,
                center_rank,
            );
            (Vec::new(), sh_t, s, sh_l, a)
        }
    };

    let up = q.flats.map_or(&mode_data.up_adj_flat, |f| f.up);
    let down_fwd = q.flats.map_or(&mode_data.down_adj_flat, |f| f.down_fwd);
    let down_rev = q.flats.map_or(&mode_data.down_rev_flat, |f| f.down_rev);
    // The per-edge weight the reach model bills an edge at, in the unit of
    // the metric: seconds from the mode's time weights, metres from the
    // EBG's own `length_m`.
    let node_weights: &[u32] = match q.metric {
        ThresholdMetric::Time => &mode_data.node_weights[..],
        ThresholdMetric::LengthAlongTime => &state.node_weights_dist[..],
    };

    // One PHAST run at the MAX threshold; every contour is a slice of it.
    let max_threshold = q.thresholds.iter().copied().max().unwrap_or(0);
    // The label and the shift live in the metric's own unit: seconds for a
    // time field, metres for an isodistance.
    let mut raw_2ch: Option<Vec<(u32, u32, u32)>> = None;
    // The time shift outlives the rebinding below: an isodistance field is
    // read on the length channel (its `shift` is `shift_len`), but its time
    // labels are normalised by the TIME shift (#620: the arrive mirror used
    // to subtract metres from seconds, clamping the near-snap twins to 0 s).
    let shift_time = shift;
    // #620: the latest time at which a labelled state still draws something
    // — the bound both isodistance fields run to, so every twin that can
    // decide a cut is labelled exact. Depart: a state entered at
    // `(l − w_len, t − w_time)` draws `min(w_len, budget − entry_len)` metres
    // of itself; arrive: a label is the cost from the HEAD, it draws
    // `min(w_len, budget − label_len)` metres into that head.
    let budget_len = max_threshold.saturating_add(shift_len);
    let orig_of_rank = |rank: u32| {
        mode_data.filtered_to_original[mode_data.cch_topo.rank_to_filtered[rank as usize] as usize]
            as usize
    };
    let edge_w = |rank: u32| -> Option<(u64, u64)> {
        let o = orig_of_rank(rank);
        let (wl, wt) = (state.node_weights_dist[o], mode_data.node_weights[o]);
        (wl != 0 && wl != u32::MAX && wt != u32::MAX).then_some((wl as u64, wt as u64))
    };
    let cut_time_depart = |rank: u32, t: u32, l: u32| -> Option<u32> {
        let (wl, wt) = edge_w(rank)?;
        let el = (l as u64).saturating_sub(wl);
        if el >= budget_len as u64 {
            return None;
        }
        let x = wl.min(budget_len as u64 - el);
        Some(
            (t as u64)
                .saturating_sub(wt)
                .saturating_add((x * wt).div_ceil(wl)) as u32,
        )
    };
    let cut_time_arrive = |rank: u32, t: u32, l: u32| -> Option<u32> {
        let (wl, wt) = edge_w(rank)?;
        if l >= budget_len {
            return None;
        }
        let x = wl.min((budget_len - l) as u64);
        Some((t as u64).saturating_add((x * wt).div_ceil(wl)) as u32)
    };
    let (phast_settled, shift) = match len_flats {
        None => {
            let f = if q.reverse {
                // The field carries the seed shift, so the bound must too: a
                // state whose TRUE cost to the snap is `max_threshold` is
                // labelled `max_threshold + shift` (#544).
                crate::range::phast_seeded::run_phast_bounded_fast_reverse_seeded(
                    up,
                    down_rev,
                    &seeds,
                    max_threshold.saturating_add(shift),
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
            (f, shift)
        }
        Some((up_len, down_fwd_len, down_rev_len)) => {
            let raw = if q.reverse {
                crate::range::phast_seeded::run_phast_reverse_seeded_2ch_by_len(
                    up,
                    down_rev,
                    up_len,
                    down_rev_len,
                    &seeds_2ch,
                    mode,
                    cut_time_arrive,
                )
            } else {
                crate::range::phast_seeded::run_phast_seeded_2ch_by_len(
                    up,
                    down_fwd,
                    up_len,
                    down_fwd_len,
                    &seeds_2ch,
                    budget_len,
                    mode,
                    cut_time_depart,
                )
            };
            // The contour threshold reads the ONE label the metric is about,
            // over the states within the budget; the reach model keeps both
            // channels of EVERY label, past the budget included (#620).
            let settled: Vec<(u32, u32)> = raw
                .iter()
                .filter(|&&(_, _, l)| l <= budget_len)
                .map(|&(r, _t, l)| (r, l))
                .collect();
            raw_2ch = Some(raw);
            (settled, shift_len)
        }
    };

    // Rank → original EBG id, and (arrive) label → true cost by removing the
    // seed shift, so `settled` means the same thing in both directions: the
    // cost of the road part of the journey between the snap and the edge's
    // far end. Only the seed edges themselves can sit before the snap
    // (their true cost is negative, `-part_time`): they clamp to 0, i.e.
    // they are drawn as if the snap were at their head. The deviation is
    // bounded by that one edge, the same approximation the depart seed edge
    // carries (it is stamped whole though only its post-snap part is
    // driven), and the pin stamp covers the centre either way.
    let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
    for &(rank, dist) in &phast_settled {
        let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        settled.push((
            mode_data.filtered_to_original[filtered_id as usize],
            dist.saturating_sub(shift),
        ));
    }

    let mut frontiers = FrontierCache {
        slots: Vec::new(),
        reverse: q.reverse,
        phast_settled: &phast_settled,
        // The frontier walks the SAME arcs in the SAME unit as the field:
        // time weights for a time field, length-along-time weights for an
        // isodistance. The two flats share their topology, so slot `i`
        // addresses the same CCH arc in both.
        up: len_flats.map_or(up, |(u, _, _)| u),
        down_fwd: len_flats.map_or(down_fwd, |(_, d, _)| d),
        mode_data,
        node_weights,
        // #620: a depart isodistance decides reach per physical segment from
        // both twins' exact (time, length) entries, not per directed edge.
        length: match (&raw_2ch, len_flats) {
            (Some(raw), Some(_)) => Some(LengthReachCtx {
                reverse: q.reverse,
                entries: if q.reverse {
                    // Arrive labels carry the seed shift on both channels
                    // (#544/#612); the mirror reads them normalised.
                    raw.iter()
                        .map(|&(r, t, l)| {
                            let f = mode_data.cch_topo.rank_to_filtered[r as usize];
                            (
                                mode_data.filtered_to_original[f as usize],
                                (l.saturating_sub(shift_len), t.saturating_sub(shift_time)),
                            )
                        })
                        .collect()
                } else {
                    crate::server::isochrone_handler::depart_entries_2ch(
                        raw,
                        mode_data,
                        &state.node_weights_dist,
                        &mode_data.node_weights,
                    )
                },
                twin_of: state.twin_of(),
                w_len: &state.node_weights_dist,
                w_time: &mode_data.node_weights,
                nbg_edges: &state.nbg_geo.edges,
            }),
            _ => None,
        },
    };
    // Diagnostic (#620): `BUTTERFLY_ISO_TRACE=lon,lat,radius_m` logs, for an
    // isodistance, every entry whose polyline passes within `radius_m` of the
    // point — twin, weights, entry, fragment — so a served point can be
    // explained from the engine's own numbers rather than inferred from
    // `/table`. Read per query; costs nothing when unset.
    let iso_trace: Option<(f64, f64, f64)> =
        std::env::var("BUTTERFLY_ISO_TRACE").ok().and_then(|v| {
            let mut it = v.split(',').map(|x| x.trim().parse::<f64>().ok());
            Some((it.next()??, it.next()??, it.next()??))
        });
    let mut topologies = Vec::with_capacity(q.thresholds.len());
    for &threshold in q.thresholds {
        let slot = frontiers.slot(threshold);
        if let (Some(ctx), Some((tlon, tlat, radius))) = (&frontiers.length, iso_trace) {
            let frac_of: FxHashMap<u32, f32> = frontiers.slots[slot].1.iter().copied().collect();
            let kx = tlat.to_radians().cos() * 111_320.0;
            for (&orig, &(el, et)) in &ctx.entries {
                let node = &state.ebg_nodes.nodes[orig as usize];
                let poly = state.edge_geom.polyline(node.geom_idx);
                let near = poly.iter_lat_lon_e7().any(|(lat_e7, lon_e7)| {
                    let dx = (lon_e7 as f64 / 1e7 - tlon) * kx;
                    let dy = (lat_e7 as f64 / 1e7 - tlat) * 110_540.0;
                    (dx * dx + dy * dy).sqrt() <= radius
                });
                if !near {
                    continue;
                }
                let twin = ctx.twin_of.get(orig as usize).copied().unwrap_or(u32::MAX);
                let nbg = state.nbg_geo.edges.get(node.geom_idx as usize);
                tracing::info!(
                    threshold,
                    orig,
                    twin,
                    geom_idx = node.geom_idx,
                    forward = nbg.is_none_or(|e| e.u_node == node.tail_nbg),
                    w_len = ctx.w_len[orig as usize],
                    w_time = ctx.w_time[orig as usize],
                    entry_len = el,
                    entry_time = et,
                    twin_entry = ?twin_of_entry(ctx, twin),
                    frac = ?frac_of.get(&orig),
                    verts = poly.len(),
                    "isodistance trace (#620)"
                );
            }
        }
        topologies.push(build_isochrone_topology(
            &settled,
            threshold,
            node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
            match q.metric {
                ThresholdMetric::Time => {
                    SparseContourConfig::for_mode_name_with_threshold(q.mode_name, threshold)
                }
                ThresholdMetric::LengthAlongTime => {
                    SparseContourConfig::for_mode_name_with_distance(q.mode_name, threshold)
                }
            },
            anchor,
            Some((q.lon, q.lat)),
            &frontiers.model(slot),
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
            &frontiers.model(slot),
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

/// Partial polyline from its first point to `fraction` of its LENGTH
/// (lat-first `(lat_e7, lon_e7)`, matching the sparse contour stamper).
///
/// #620: the fraction every reach model hands over is a share of the edge's
/// cost or length, and the vertices of a stored polyline are anything but
/// evenly spaced — this used to cut at `fraction` of the VERTEX span, so a
/// 17-vertex 1 007 m edge cut at 0.596 (600 m, the meeting point of its two
/// arrivals) was served to its 11th vertex, 664 m in, and a 21-vertex edge
/// cut at the 701 m budget was served 895 m in. The cut is now at
/// `fraction` of the polyline's own length (equirectangular metres).
fn partial_polyline(points: &[(i32, i32)], fraction: f32) -> Vec<(i32, i32)> {
    let n_pts = points.len();

    if n_pts == 0 || fraction <= 0.0 {
        return vec![];
    }

    if n_pts == 1 || fraction >= 1.0 {
        return points.to_vec();
    }

    let kx = (points[0].0 as f64 / 1e7).to_radians().cos();
    let seg_len = |i: usize| {
        let (lat1, lon1) = points[i];
        let (lat2, lon2) = points[i + 1];
        let dy = (lat2 - lat1) as f64;
        let dx = (lon2 - lon1) as f64 * kx;
        (dx * dx + dy * dy).sqrt()
    };
    let total: f64 = (0..n_pts - 1).map(seg_len).sum();
    if total <= 0.0 {
        return points.to_vec();
    }
    let target = fraction as f64 * total;

    // Walk to the segment holding the cut, then interpolate inside it.
    let mut walked = 0.0;
    let mut out: Vec<(i32, i32)> = Vec::with_capacity(n_pts);
    out.push(points[0]);
    for i in 0..n_pts - 1 {
        let l = seg_len(i);
        if walked + l >= target || i == n_pts - 2 {
            let local = if l > 0.0 {
                ((target - walked) / l).clamp(0.0, 1.0)
            } else {
                1.0
            };
            let (lat1, lon1) = points[i];
            let (lat2, lon2) = points[i + 1];
            let lat = lat1 + ((lat2 - lat1) as f64 * local).round() as i32;
            let lon = lon1 + ((lon2 - lon1) as f64 * local).round() as i32;
            if (lat, lon) != points[i] {
                out.push((lat, lon));
            }
            break;
        }
        walked += l;
        out.push(points[i + 1]);
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

    /// #620: the fraction is a share of the LENGTH, never of the vertex
    /// count — vertices are dense on bends and sparse on straights.
    #[test]
    fn partial_polyline_cuts_by_length_not_by_vertex_count() {
        // 4 vertices, 3 segments of 100, 100 and 1800 units: half the length
        // (1000) lies 800 into the LAST segment, not at the 2nd vertex.
        let pts = [(0, 0), (0, 100), (0, 200), (0, 2000)];
        assert_eq!(
            partial_polyline(&pts, 0.5),
            vec![(0, 0), (0, 100), (0, 200), (0, 1000)]
        );
        // 10 % of the length (200) is exactly the third vertex: no
        // duplicate point is appended.
        assert_eq!(
            partial_polyline(&pts, 0.1),
            vec![(0, 0), (0, 100), (0, 200)]
        );
        // Reversed: 10 % of the length from the far end lies inside the
        // long segment.
        let mut rev = pts.to_vec();
        rev.reverse();
        assert_eq!(partial_polyline(&rev, 0.1), vec![(0, 2000), (0, 1800)]);
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

#[cfg(test)]
mod length_reach_tests {
    //! #620: reach on the carried channel is decided per physical segment
    //! from both twins' entries. Numbers from the 2026-09-16 'rural WB'
    //! offender: a 448 m two-way segment entered from its tail at
    //! (3 425 m, 622 s) and from its head at (5 884 m, 635 s) for a 5 000 m
    //! isodistance. The engine used to draw it whole from the tail and serve
    //! its head — whose time-shortest path is the 5 884 m one.
    use super::length_reach_fragments;
    use rustc_hash::FxHashMap;

    const LEN: u32 = 448;
    const WT: u32 = 40; // ~40 km/h, both directions

    fn fragments(entries: &[(u32, u32, u32)], twin_of: &[u32], budget: u32) -> Vec<(u32, f32)> {
        let m: FxHashMap<u32, (u32, u32)> =
            entries.iter().map(|&(id, l, t)| (id, (l, t))).collect();
        let w_len = [LEN, LEN, 1000, 1000];
        let w_time = [WT, WT, 60, 60];
        length_reach_fragments(&m, budget, twin_of, &w_len, &w_time)
    }

    #[test]
    fn a_fast_long_arrival_at_the_head_cuts_the_tail_side_at_the_meeting_point() {
        // edge 0 = tail side (A->B), edge 1 = its twin (B->A).
        let f = fragments(
            &[(0, 3425, 622), (1, 5884, 635)],
            &[1, 0, u32::MAX, u32::MAX],
            5000,
        );
        // meeting point: 622 + x·40/448 = 635 + (448−x)·40/448 → x = (635+40−622)·448/80 = 296.8 m
        let (id, frac) = f
            .iter()
            .copied()
            .find(|&(id, _)| id == 0)
            .expect("tail side is drawn");
        assert_eq!(id, 0);
        let x = frac as f64 * LEN as f64;
        assert!(
            (x - 296.8).abs() < 1.0,
            "tail-side prefix must stop at the meeting point, got {x:.1} m"
        );
        // The head side's entry length is already past the budget: nothing.
        assert!(
            !f.iter().any(|&(id, _)| id == 1),
            "the 5 884 m side draws nothing: {f:?}"
        );
    }

    #[test]
    fn without_a_twin_only_the_budget_cuts() {
        let f = fragments(
            &[(0, 3425, 622)],
            &[u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            5000,
        );
        assert_eq!(f, vec![(0, 1.0)], "3425 + 448 ≤ 5000: whole edge");
        let f = fragments(
            &[(0, 4800, 622)],
            &[u32::MAX, u32::MAX, u32::MAX, u32::MAX],
            5000,
        );
        let x = f[0].1 as f64 * LEN as f64;
        assert!((x - 200.0).abs() < 0.5, "budget leaves 200 m, got {x:.1}");
    }

    #[test]
    fn a_twin_faster_from_the_tail_on_draws_nothing_on_this_side() {
        // Head side enters at 600 s: faster than the tail side (622 s) even at x = 0.
        let f = fragments(
            &[(0, 3425, 622), (1, 4000, 560)],
            &[1, 0, u32::MAX, u32::MAX],
            5000,
        );
        assert!(
            !f.iter().any(|&(id, _)| id == 0),
            "tail side is never the time-shortest arrival: {f:?}"
        );
        // and the head side draws its own admissible prefix (4000 + 448 ≤ 5000: whole).
        assert!(f.iter().any(|&(id, frac)| id == 1 && frac >= 1.0), "{f:?}");
    }

    #[test]
    fn an_entry_at_or_past_the_budget_is_dropped() {
        let f = fragments(&[(0, 5000, 100), (2, 6000, 100)], &[u32::MAX; 4], 5000);
        assert!(f.is_empty(), "{f:?}");
    }

    #[test]
    fn output_is_sorted_by_edge_id() {
        let f = fragments(&[(2, 100, 10), (0, 100, 10)], &[u32::MAX; 4], 5000);
        assert_eq!(f.iter().map(|&(id, _)| id).collect::<Vec<_>>(), vec![0, 2]);
    }
}

#[cfg(test)]
mod length_reach_arrive_tests {
    //! #620, arrive: the mirror. Labels are costs from the edge's HEAD to the
    //! snap. Same 448 m two-way segment: from its head the snap is
    //! (3 425 m, 622 s) away; from its tail (the twin's head) it is
    //! (5 884 m, 635 s) away, via a long fast road.
    use super::length_reach_fragments_arrive;
    use rustc_hash::FxHashMap;

    const LEN: u32 = 448;
    const WT: u32 = 40;

    fn fragments(labels: &[(u32, u32, u32)], twin_of: &[u32], budget: u32) -> Vec<(u32, f32)> {
        let m: FxHashMap<u32, (u32, u32)> = labels.iter().map(|&(id, l, t)| (id, (l, t))).collect();
        length_reach_fragments_arrive(
            &m,
            budget,
            twin_of,
            &[LEN, LEN, 1000, 1000],
            &[WT, WT, 60, 60],
        )
    }

    #[test]
    fn the_suffix_starts_at_the_meeting_point_when_the_twin_leaves_faster_from_the_tail() {
        let f = fragments(
            &[(0, 3425, 622), (1, 5884, 635)],
            &[1, 0, u32::MAX, u32::MAX],
            5000,
        );
        // 622 + (448−x)·40/448 == 635 + x·40/448 → x_m = (622+40−635)·448/80 = 151.2 m
        let (_, frac) = f
            .iter()
            .copied()
            .find(|&(id, _)| id == 0)
            .expect("head side is drawn");
        let served_from_tail = LEN as f64 - frac as f64 * LEN as f64;
        assert!(
            (served_from_tail - 151.2).abs() < 1.0,
            "suffix must start at the meeting point, got {served_from_tail:.1} m"
        );
        assert!(
            !f.iter().any(|&(id, _)| id == 1),
            "the 5 884 m side draws nothing: {f:?}"
        );
    }

    #[test]
    fn without_a_twin_only_the_budget_cuts_from_the_head() {
        let f = fragments(&[(0, 3425, 622)], &[u32::MAX; 4], 5000);
        assert_eq!(f, vec![(0, 1.0)]);
        let f = fragments(&[(0, 4800, 622)], &[u32::MAX; 4], 5000);
        let x = f[0].1 as f64 * LEN as f64;
        assert!(
            (x - 200.0).abs() < 0.5,
            "200 m of budget left from the head, got {x:.1}"
        );
    }

    #[test]
    fn a_twin_faster_all_the_way_to_the_head_draws_nothing_on_this_side() {
        let f = fragments(
            &[(0, 3425, 700), (1, 4000, 560)],
            &[1, 0, u32::MAX, u32::MAX],
            5000,
        );
        assert!(!f.iter().any(|&(id, _)| id == 0), "{f:?}");
        assert!(f.iter().any(|&(id, frac)| id == 1 && frac >= 1.0), "{f:?}");
    }

    #[test]
    fn a_label_at_or_past_the_budget_is_dropped() {
        assert!(fragments(&[(0, 5000, 1), (2, 7000, 1)], &[u32::MAX; 4], 5000).is_empty());
    }
}
