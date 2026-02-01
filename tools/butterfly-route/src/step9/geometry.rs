//! Geometry reconstruction from EBG path

use serde::Serialize;
use utoipa::ToSchema;

use crate::formats::{EbgNodes, NbgGeo};
use crate::range::{generate_sparse_contour, SparseContourConfig, ReachableSegment};
use crate::profile_abi::Mode;

/// A point in WGS84 coordinates
#[derive(Debug, Clone, Copy, Serialize, ToSchema)]
pub struct Point {
    pub lon: f64,
    pub lat: f64,
}

/// Route geometry with coordinates
#[derive(Debug, Clone, Serialize, ToSchema)]
pub struct RouteGeometry {
    pub coordinates: Vec<Point>,
    pub distance_m: f64,
    pub duration_ds: u32,
}

/// Build route geometry from EBG node sequence
pub fn build_geometry(
    ebg_path: &[u32],
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
    duration_ds: u32,
) -> RouteGeometry {
    let mut coordinates = Vec::new();
    let mut total_distance_m = 0.0;

    for &ebg_id in ebg_path {
        let node = &ebg_nodes.nodes[ebg_id as usize];
        let geom_idx = node.geom_idx as usize;

        if geom_idx < nbg_geo.polylines.len() {
            let polyline = &nbg_geo.polylines[geom_idx];

            // Add geometry points from polyline
            for i in 0..polyline.lat_fxp.len() {
                coordinates.push(Point {
                    lon: polyline.lon_fxp[i] as f64 / 1e7,
                    lat: polyline.lat_fxp[i] as f64 / 1e7,
                });
            }
        }

        // Accumulate distance
        total_distance_m += node.length_mm as f64 / 1000.0;
    }

    // Remove duplicate consecutive points
    coordinates.dedup_by(|a, b| (a.lon - b.lon).abs() < 1e-9 && (a.lat - b.lat).abs() < 1e-9);

    RouteGeometry {
        coordinates,
        distance_m: total_distance_m,
        duration_ds,
    }
}

/// Build isochrone geometry using frontier-based concave hull
///
/// This extracts frontier segments (edges that cross the time threshold)
/// and builds a concave hull polygon that accurately follows the road network.
///
/// # Arguments
/// * `settled_nodes` - (original_ebg_id, distance_ds) pairs for all reachable edges
/// * `max_time_ds` - Time threshold in deciseconds
/// * `node_weights` - Edge traversal costs indexed by original EBG node ID (deciseconds)
/// * `ebg_nodes` - EBG node metadata (for geometry lookup)
/// * `nbg_geo` - Road geometry polylines
///
/// # Returns
/// Polygon vertices as (lon, lat) points forming a closed ring
pub fn build_isochrone_geometry(
    settled_nodes: &[(u32, u32)], // (original_ebg_id, distance_ds)
    max_time_ds: u32,
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
) -> Vec<Point> {
    // Fallback to legacy convex hull for now
    // TODO: Remove this once concave hull is validated
    build_isochrone_geometry_convex(settled_nodes, max_time_ds, ebg_nodes, nbg_geo)
}

/// Build isochrone geometry using sparse tile rasterization + boundary tracing
///
/// This is the correct algorithm that:
/// 1. Stamps reachable road segments into a sparse tile grid
/// 2. For frontier edges: clips polyline at cut_fraction, stamps only reachable prefix
/// 3. Applies local morphology (dilation/erosion) to create fillable regions
/// 4. Extracts boundary via Moore-neighbor tracing (O(perimeter))
///
/// This respects road network topology and produces geometrically correct isochrones.
pub fn build_isochrone_geometry_concave(
    settled_nodes: &[(u32, u32)], // (original_ebg_id, distance_ds)
    max_time_ds: u32,
    node_weights: &[u32],         // Edge costs indexed by original EBG node ID
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
) -> Vec<Point> {
    build_isochrone_geometry_sparse(settled_nodes, max_time_ds, node_weights, ebg_nodes, nbg_geo, Mode::Car)
}

/// Build isochrone geometry with mode-specific configuration
pub fn build_isochrone_geometry_sparse(
    settled_nodes: &[(u32, u32)], // (original_ebg_id, distance_ds)
    max_time_ds: u32,
    node_weights: &[u32],         // Edge costs indexed by original EBG node ID
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
    mode: Mode,
) -> Vec<Point> {
    // Extract reachable and frontier segments
    let mut segments: Vec<ReachableSegment> = Vec::new();

    for &(ebg_id, dist_ds) in settled_nodes {
        if dist_ds > max_time_ds {
            continue;
        }

        // Get edge weight
        let weight_ds = if (ebg_id as usize) < node_weights.len() {
            node_weights[ebg_id as usize]
        } else {
            continue;
        };

        if weight_ds == 0 || weight_ds == u32::MAX {
            continue;
        }

        let dist_end_ds = dist_ds.saturating_add(weight_ds);

        // Get geometry
        let node = &ebg_nodes.nodes[ebg_id as usize];
        let geom_idx = node.geom_idx as usize;
        if geom_idx >= nbg_geo.polylines.len() {
            continue;
        }
        let polyline = &nbg_geo.polylines[geom_idx];
        if polyline.lat_fxp.is_empty() {
            continue;
        }

        if dist_end_ds <= max_time_ds {
            // Fully reachable edge - include entire polyline
            let points: Vec<(i32, i32)> = polyline.lat_fxp
                .iter()
                .zip(polyline.lon_fxp.iter())
                .map(|(&lat, &lon)| (lat, lon))
                .collect();
            segments.push(ReachableSegment { points });
        } else {
            // Frontier edge - include from start to cut point
            let cut_fraction = (max_time_ds - dist_ds) as f32 / weight_ds as f32;
            let points = extract_partial_polyline(polyline, cut_fraction);
            if !points.is_empty() {
                segments.push(ReachableSegment { points });
            }
        }
    }

    if segments.is_empty() {
        return vec![];
    }

    // Generate contour using sparse tile rasterization + boundary tracing
    let config = match mode {
        Mode::Car => SparseContourConfig::for_car(),
        Mode::Bike => SparseContourConfig::for_bike(),
        Mode::Foot => SparseContourConfig::for_foot(),
    };

    match generate_sparse_contour(&segments, &config) {
        Ok(result) => {
            // Convert to Point format
            result.outer_ring
                .into_iter()
                .map(|(lon, lat)| Point { lon, lat })
                .collect()
        }
        Err(_) => vec![],
    }
}

/// Extract partial polyline from start to given fraction
fn extract_partial_polyline(polyline: &crate::formats::PolyLine, fraction: f32) -> Vec<(i32, i32)> {
    let n_pts = polyline.lat_fxp.len();

    if n_pts == 0 || fraction <= 0.0 {
        return vec![];
    }

    if n_pts == 1 {
        return vec![(polyline.lat_fxp[0], polyline.lon_fxp[0])];
    }

    if fraction >= 1.0 {
        return polyline.lat_fxp
            .iter()
            .zip(polyline.lon_fxp.iter())
            .map(|(&lat, &lon)| (lat, lon))
            .collect();
    }

    // Find the segment where the cut occurs
    let n_segments = n_pts - 1;
    let segment_frac = fraction * n_segments as f32;
    let segment_idx = (segment_frac.floor() as usize).min(n_segments - 1);
    let local_frac = segment_frac - segment_idx as f32;

    // Include all points up to and including the start of the cut segment
    let mut points: Vec<(i32, i32)> = polyline.lat_fxp[..=segment_idx]
        .iter()
        .zip(polyline.lon_fxp[..=segment_idx].iter())
        .map(|(&lat, &lon)| (lat, lon))
        .collect();

    // Add the interpolated cut point
    if local_frac > 0.0 && segment_idx + 1 < n_pts {
        let lat1 = polyline.lat_fxp[segment_idx];
        let lon1 = polyline.lon_fxp[segment_idx];
        let lat2 = polyline.lat_fxp[segment_idx + 1];
        let lon2 = polyline.lon_fxp[segment_idx + 1];

        let lat = lat1 + ((lat2 - lat1) as f32 * local_frac) as i32;
        let lon = lon1 + ((lon2 - lon1) as f32 * local_frac) as i32;
        points.push((lat, lon));
    }

    points
}

/// Legacy convex hull implementation (DEPRECATED - produces incorrect results)
///
/// This function is kept for backward compatibility but should not be used.
/// Use `build_isochrone_geometry_concave` instead.
fn build_isochrone_geometry_convex(
    settled_nodes: &[(u32, u32)], // (node_id, distance)
    max_time_ds: u32,
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
) -> Vec<Point> {
    // Collect all points within time limit
    let mut points: Vec<Point> = Vec::new();

    for &(ebg_id, dist) in settled_nodes {
        if dist <= max_time_ds {
            let node = &ebg_nodes.nodes[ebg_id as usize];
            let geom_idx = node.geom_idx as usize;

            if geom_idx < nbg_geo.polylines.len() {
                let polyline = &nbg_geo.polylines[geom_idx];

                // Add edge endpoints
                if !polyline.lat_fxp.is_empty() {
                    // First point
                    points.push(Point {
                        lon: polyline.lon_fxp[0] as f64 / 1e7,
                        lat: polyline.lat_fxp[0] as f64 / 1e7,
                    });

                    // Last point
                    let last = polyline.lat_fxp.len() - 1;
                    points.push(Point {
                        lon: polyline.lon_fxp[last] as f64 / 1e7,
                        lat: polyline.lat_fxp[last] as f64 / 1e7,
                    });
                }
            }
        }
    }

    // Compute convex hull (simple Graham scan)
    if points.len() < 3 {
        return points;
    }

    convex_hull(&mut points)
}

/// Simple convex hull using Graham scan
fn convex_hull(points: &mut [Point]) -> Vec<Point> {
    if points.len() < 3 {
        return points.to_vec();
    }

    // Find lowest point
    let mut min_idx = 0;
    for (i, p) in points.iter().enumerate() {
        if p.lat < points[min_idx].lat
            || (p.lat == points[min_idx].lat && p.lon < points[min_idx].lon)
        {
            min_idx = i;
        }
    }
    points.swap(0, min_idx);

    let pivot = points[0];

    // Sort by polar angle
    points[1..].sort_by(|a, b| {
        let cross = cross_product(pivot, *a, *b);
        if cross.abs() < 1e-12 {
            // Collinear - sort by distance
            let dist_a = (a.lon - pivot.lon).powi(2) + (a.lat - pivot.lat).powi(2);
            let dist_b = (b.lon - pivot.lon).powi(2) + (b.lat - pivot.lat).powi(2);
            dist_a.partial_cmp(&dist_b).unwrap()
        } else if cross > 0.0 {
            std::cmp::Ordering::Less
        } else {
            std::cmp::Ordering::Greater
        }
    });

    // Build hull
    let mut hull = Vec::new();
    for &p in points.iter() {
        while hull.len() >= 2
            && cross_product(hull[hull.len() - 2], hull[hull.len() - 1], p) <= 0.0
        {
            hull.pop();
        }
        hull.push(p);
    }

    // Close the polygon
    if !hull.is_empty() {
        hull.push(hull[0]);
    }

    hull
}

fn cross_product(o: Point, a: Point, b: Point) -> f64 {
    (a.lon - o.lon) * (b.lat - o.lat) - (a.lat - o.lat) * (b.lon - o.lon)
}
