//! Geometry reconstruction from EBG path

use serde::Serialize;
use utoipa::ToSchema;

use crate::formats::{EbgNodes, NbgGeo};

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

/// Build isochrone geometry (simplified convex hull for now)
pub fn build_isochrone_geometry(
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
