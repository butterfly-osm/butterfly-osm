//! Geometry reconstruction from EBG path

use serde::Serialize;
use utoipa::ToSchema;

use crate::formats::{EbgNodes, NbgGeo};
use crate::profile_abi::Mode;
use crate::range::{generate_sparse_contour, ReachableSegment, SparseContourConfig};

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
    /// Distance in meters
    pub distance_m: f64,
    /// Duration in deciseconds
    pub duration_ds: u32,
}

impl RouteGeometry {
    /// Create geometry in the requested format from raw coordinate list
    pub fn from_points(
        points: Vec<Point>,
        distance_m: f64,
        duration_ds: u32,
        format: GeometryFormat,
    ) -> Self {
        match format {
            GeometryFormat::Polyline6 => RouteGeometry {
                polyline: Some(encode_polyline6(&points)),
                coordinates_geojson: None,
                coordinates: None,
                distance_m,
                duration_ds,
            },
            GeometryFormat::GeoJson => RouteGeometry {
                polyline: None,
                coordinates_geojson: Some(points.iter().map(|p| [p.lon, p.lat]).collect()),
                coordinates: None,
                distance_m,
                duration_ds,
            },
            GeometryFormat::Points => RouteGeometry {
                polyline: None,
                coordinates_geojson: None,
                coordinates: Some(points),
                distance_m,
                duration_ds,
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

/// Build route geometry from EBG node sequence
pub fn build_geometry(
    ebg_path: &[u32],
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
    duration_ds: u32,
    format: GeometryFormat,
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

    RouteGeometry::from_points(coordinates, total_distance_m, duration_ds, format)
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
    node_weights: &[u32], // Edge costs indexed by original EBG node ID
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
) -> Vec<Point> {
    build_isochrone_geometry_sparse(
        settled_nodes,
        max_time_ds,
        node_weights,
        ebg_nodes,
        nbg_geo,
        Mode::Car,
    )
}

/// Build isochrone geometry with mode-specific configuration
///
/// Stamps all reachable edges into a sparse tile grid, then traces the boundary.
/// Previous optimization (near-frontier stamping) was removed because it caused
/// incorrect polygons for larger isochrones where the frontier became too sparse.
pub fn build_isochrone_geometry_sparse(
    settled_nodes: &[(u32, u32)], // (original_ebg_id, distance_ds)
    max_time_ds: u32,
    node_weights: &[u32], // Edge costs indexed by original EBG node ID
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
    mode: Mode,
) -> Vec<Point> {
    let config = match mode {
        Mode::Car => SparseContourConfig::for_car(),
        Mode::Bike => SparseContourConfig::for_bike(),
        Mode::Foot => SparseContourConfig::for_foot(),
    };

    let mut segments: Vec<ReachableSegment> = Vec::new();

    for &(ebg_id, dist_ds) in settled_nodes {
        if dist_ds > max_time_ds {
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
            // Fully reachable edge (near frontier)
            let points: Vec<(i32, i32)> = polyline
                .lat_fxp
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
    match generate_sparse_contour(&segments, &config) {
        Ok(result) => result
            .outer_ring
            .into_iter()
            .map(|(lon, lat)| Point { lon, lat })
            .collect(),
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
        return polyline
            .lat_fxp
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
        while hull.len() >= 2 && cross_product(hull[hull.len() - 2], hull[hull.len() - 1], p) <= 0.0
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
        let geom = RouteGeometry::from_points(points, 1234.5, 100, GeometryFormat::Polyline6);
        assert!(geom.polyline.is_some());
        assert!(geom.coordinates_geojson.is_none());
        assert!(geom.coordinates.is_none());
        assert!((geom.distance_m - 1234.5).abs() < 0.01);
        assert_eq!(geom.duration_ds, 100);
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
        let geom = RouteGeometry::from_points(points, 1234.5, 100, GeometryFormat::GeoJson);
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
        let geom = RouteGeometry::from_points(points, 1234.5, 100, GeometryFormat::Points);
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
        let poly_geom =
            RouteGeometry::from_points(points.clone(), 100.0, 50, GeometryFormat::Polyline6);
        let json_geom =
            RouteGeometry::from_points(points.clone(), 100.0, 50, GeometryFormat::GeoJson);

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
}
