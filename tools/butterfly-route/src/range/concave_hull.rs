//! Concave hull-based isochrone polygon generation
//!
//! Uses frontier cut points (where threshold crosses roads) to generate
//! detailed polygons with thousands of vertices, matching Valhalla quality.
//!
//! Key insight: Only use frontier points, not all reachable points.
//! - All reachable: 100K+ points (slow)
//! - Frontier only: 2K-5K points (fast)

use geo::{
    algorithm::concave_hull::ConcaveHull, algorithm::simplify::Simplify, Coord, MultiPoint, Polygon,
};

use super::frontier::ReachableSegment;

/// Configuration for concave hull polygon generation
#[derive(Debug, Clone)]
pub struct ConcaveHullConfig {
    /// Concavity parameter (larger = more convex, smaller = more detailed)
    /// Typical range: 1.0 - 5.0
    /// - 1.0: Very detailed, follows road network closely
    /// - 2.0: Good balance (default)
    /// - 5.0: Smoother, more convex
    pub concavity: f64,

    /// Simplification tolerance in degrees (0 = no simplification)
    /// 0.0001 degrees ≈ 11m at equator
    pub simplify_tolerance: f64,

    /// Whether to include intermediate polyline points from frontier segments
    /// true: More detail, more points to process
    /// false: Only segment endpoints and cut points (faster)
    pub include_intermediate_points: bool,
}

impl Default for ConcaveHullConfig {
    fn default() -> Self {
        Self {
            concavity: 2.0,
            simplify_tolerance: 0.0, // No simplification by default
            include_intermediate_points: true,
        }
    }
}

impl ConcaveHullConfig {
    /// Configuration matching Valhalla's default (generalize=50m)
    pub fn valhalla_default() -> Self {
        Self {
            concavity: 2.0,
            simplify_tolerance: 0.00045, // ~50m
            include_intermediate_points: true,
        }
    }

    /// High detail configuration (minimal simplification)
    pub fn high_detail() -> Self {
        Self {
            concavity: 1.5,
            simplify_tolerance: 0.0,
            include_intermediate_points: true,
        }
    }

    /// Fast configuration (fewer points, more simplification)
    pub fn fast() -> Self {
        Self {
            concavity: 3.0,
            simplify_tolerance: 0.001, // ~110m
            include_intermediate_points: false,
        }
    }
}

/// Statistics from concave hull generation
#[derive(Debug, Default, Clone)]
pub struct ConcaveHullStats {
    pub input_segments: usize,
    pub input_points: usize,
    pub hull_vertices: usize,
    pub final_vertices: usize,
    pub collect_time_us: u64,
    pub hull_time_us: u64,
    pub simplify_time_us: u64,
    pub total_time_us: u64,
}

/// Result of concave hull generation
pub struct ConcaveHullResult {
    /// Outer ring as (lon, lat) pairs in WGS84
    pub outer_ring: Vec<(f64, f64)>,
    /// Holes (currently not supported, always empty)
    pub holes: Vec<Vec<(f64, f64)>>,
    /// Generation statistics
    pub stats: ConcaveHullStats,
}

/// Generate isochrone polygon using concave hull on frontier points
///
/// This approach:
/// 1. Collects all points from frontier segments (roads at the boundary)
/// 2. Computes concave hull to form a polygon
/// 3. Optionally simplifies the result
///
/// Much faster than using all reachable points because frontier is small.
pub fn generate_concave_hull(
    segments: &[ReachableSegment],
    config: &ConcaveHullConfig,
) -> ConcaveHullResult {
    let mut stats = ConcaveHullStats {
        input_segments: segments.len(),
        ..Default::default()
    };

    let total_start = std::time::Instant::now();

    if segments.is_empty() {
        stats.total_time_us = total_start.elapsed().as_micros() as u64;
        return ConcaveHullResult {
            outer_ring: vec![],
            holes: vec![],
            stats,
        };
    }

    // Step 1: Collect points from segments
    let collect_start = std::time::Instant::now();

    let mut coords: Vec<Coord<f64>> = Vec::new();

    for seg in segments {
        if config.include_intermediate_points {
            // Include all polyline points
            for &(lat_fxp, lon_fxp) in &seg.points {
                let lon = lon_fxp as f64 / 1e7;
                let lat = lat_fxp as f64 / 1e7;
                coords.push(Coord { x: lon, y: lat });
            }
        } else {
            // Only include endpoints
            if let Some(&(lat_fxp, lon_fxp)) = seg.points.first() {
                let lon = lon_fxp as f64 / 1e7;
                let lat = lat_fxp as f64 / 1e7;
                coords.push(Coord { x: lon, y: lat });
            }
            if seg.points.len() > 1 {
                if let Some(&(lat_fxp, lon_fxp)) = seg.points.last() {
                    let lon = lon_fxp as f64 / 1e7;
                    let lat = lat_fxp as f64 / 1e7;
                    coords.push(Coord { x: lon, y: lat });
                }
            }
        }
    }

    stats.input_points = coords.len();
    stats.collect_time_us = collect_start.elapsed().as_micros() as u64;

    if coords.len() < 3 {
        stats.total_time_us = total_start.elapsed().as_micros() as u64;
        return ConcaveHullResult {
            outer_ring: coords.into_iter().map(|c| (c.x, c.y)).collect(),
            holes: vec![],
            stats,
        };
    }

    // Step 2: Compute concave hull
    let hull_start = std::time::Instant::now();

    let multi_point: MultiPoint<f64> = coords.into_iter().collect();
    let hull: Polygon<f64> = multi_point.concave_hull(config.concavity);

    stats.hull_vertices = hull.exterior().0.len();
    stats.hull_time_us = hull_start.elapsed().as_micros() as u64;

    // Step 3: Simplify if requested
    let simplify_start = std::time::Instant::now();

    let final_hull = if config.simplify_tolerance > 0.0 {
        hull.simplify(&config.simplify_tolerance)
    } else {
        hull
    };

    stats.final_vertices = final_hull.exterior().0.len();
    stats.simplify_time_us = simplify_start.elapsed().as_micros() as u64;

    // Convert to output format
    let outer_ring: Vec<(f64, f64)> = final_hull.exterior().0.iter().map(|c| (c.x, c.y)).collect();

    stats.total_time_us = total_start.elapsed().as_micros() as u64;

    ConcaveHullResult {
        outer_ring,
        holes: vec![],
        stats,
    }
}

/// Generate polygon using ONLY frontier segments (edges that cross the threshold)
///
/// This is even faster than using all reachable segments because:
/// - Fully reachable segments are interior points (don't affect boundary)
/// - Only frontier segments define the actual isochrone boundary
///
/// Use extract_frontier_segments() to get frontier-only segments first.
pub fn generate_frontier_polygon(
    frontier_segments: &[ReachableSegment],
    config: &ConcaveHullConfig,
) -> ConcaveHullResult {
    generate_concave_hull(frontier_segments, config)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_empty_segments() {
        let config = ConcaveHullConfig::default();
        let result = generate_concave_hull(&[], &config);
        assert!(result.outer_ring.is_empty());
    }
}
