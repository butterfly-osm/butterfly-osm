//! Isochrone consistency tests
//!
//! Verifies that isochrone polygons are geometrically correct:
//! - Points INSIDE the polygon have drive time <= threshold
//! - Points OUTSIDE the polygon have drive time > threshold

use geo::{Contains, Coord, Point, Polygon};
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;

use crate::profile_abi::Mode;

use super::geometry::{build_isochrone_geometry_sparse, Point as IsoPoint};

/// Test result for a single isochrone
#[derive(Debug)]
#[allow(dead_code)]
pub struct IsochroneTestResult {
    pub origin: (f64, f64),
    pub threshold_s: u32,
    pub n_samples: usize,
    pub n_snapped: usize,  // How many samples successfully snapped to roads
    pub inside_correct: usize,
    pub inside_violations: Vec<ViolationInfo>,
    pub outside_correct: usize,
    pub outside_violations: Vec<ViolationInfo>,
    pub unreachable_inside: usize,
}

#[derive(Debug, Clone)]
pub struct ViolationInfo {
    #[allow(dead_code)]
    pub sampled_point: (f64, f64),    // Original random sample
    pub snapped_point: (f64, f64),    // Snapped road point (used for containment check)
    #[allow(dead_code)]
    pub snap_distance_m: f64,          // Distance from sampled to snapped
    pub drive_time_s: f32,
    pub threshold_s: u32,
}

#[allow(dead_code)]
impl IsochroneTestResult {
    pub fn passed(&self) -> bool {
        self.inside_violations.is_empty() && self.outside_violations.is_empty()
    }

    pub fn total_violations(&self) -> usize {
        self.inside_violations.len() + self.outside_violations.len()
    }
}

/// Convert IsoPoint vec to geo::Polygon
pub fn points_to_polygon(points: &[IsoPoint]) -> Option<Polygon<f64>> {
    if points.len() < 3 {
        return None;
    }

    let coords: Vec<Coord<f64>> = points
        .iter()
        .map(|p| Coord { x: p.lon, y: p.lat })
        .collect();

    let poly = Polygon::new(coords.into(), vec![]);

    // Validate and fix if needed
    if !poly.exterior().0.is_empty() {
        Some(poly)
    } else {
        None
    }
}

/// Sample random points within a bounding box
pub fn sample_points_in_bbox(
    min_lon: f64,
    max_lon: f64,
    min_lat: f64,
    max_lat: f64,
    n_points: usize,
    seed: u64,
) -> Vec<(f64, f64)> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut points = Vec::with_capacity(n_points);

    for _ in 0..n_points {
        let lon = rng.random_range(min_lon..max_lon);
        let lat = rng.random_range(min_lat..max_lat);
        points.push((lon, lat));
    }

    points
}

/// Get bounding box of polygon with buffer
pub fn polygon_bbox_with_buffer(points: &[IsoPoint], buffer_factor: f64) -> (f64, f64, f64, f64) {
    if points.is_empty() {
        return (0.0, 0.0, 0.0, 0.0);
    }

    let min_lon = points.iter().map(|p| p.lon).fold(f64::INFINITY, f64::min);
    let max_lon = points.iter().map(|p| p.lon).fold(f64::NEG_INFINITY, f64::max);
    let min_lat = points.iter().map(|p| p.lat).fold(f64::INFINITY, f64::min);
    let max_lat = points.iter().map(|p| p.lat).fold(f64::NEG_INFINITY, f64::max);

    let width = max_lon - min_lon;
    let height = max_lat - min_lat;
    let center_lon = (min_lon + max_lon) / 2.0;
    let center_lat = (min_lat + max_lat) / 2.0;

    let buffered_width = width * buffer_factor;
    let buffered_height = height * buffer_factor;

    (
        center_lon - buffered_width / 2.0,
        center_lon + buffered_width / 2.0,
        center_lat - buffered_height / 2.0,
        center_lat + buffered_height / 2.0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::Path;

    #[test]
    fn test_sample_points_deterministic() {
        let points1 = sample_points_in_bbox(4.0, 5.0, 50.0, 51.0, 10, 42);
        let points2 = sample_points_in_bbox(4.0, 5.0, 50.0, 51.0, 10, 42);
        assert_eq!(points1, points2, "Same seed should produce same points");
    }

    #[test]
    fn test_polygon_bbox_with_buffer() {
        let points = vec![
            IsoPoint { lon: 4.0, lat: 50.0 },
            IsoPoint { lon: 5.0, lat: 50.0 },
            IsoPoint { lon: 5.0, lat: 51.0 },
            IsoPoint { lon: 4.0, lat: 51.0 },
        ];

        let (min_lon, max_lon, min_lat, max_lat) = polygon_bbox_with_buffer(&points, 1.0);
        assert!((min_lon - 4.0).abs() < 0.001);
        assert!((max_lon - 5.0).abs() < 0.001);
        assert!((min_lat - 50.0).abs() < 0.001);
        assert!((max_lat - 51.0).abs() < 0.001);

        // With buffer
        let (min_lon, max_lon, _min_lat, _max_lat) = polygon_bbox_with_buffer(&points, 1.5);
        assert!(min_lon < 4.0);
        assert!(max_lon > 5.0);
    }

    #[test]
    fn test_points_to_polygon() {
        let points = vec![
            IsoPoint { lon: 4.0, lat: 50.0 },
            IsoPoint { lon: 5.0, lat: 50.0 },
            IsoPoint { lon: 5.0, lat: 51.0 },
            IsoPoint { lon: 4.0, lat: 51.0 },
            IsoPoint { lon: 4.0, lat: 50.0 }, // Close the ring
        ];

        let poly = points_to_polygon(&points);
        assert!(poly.is_some());

        let poly = poly.unwrap();

        // Test point containment
        let inside = Point::new(4.5, 50.5);
        let outside = Point::new(3.0, 50.5);

        assert!(poly.contains(&inside), "Point (4.5, 50.5) should be inside");
        assert!(!poly.contains(&outside), "Point (3.0, 50.5) should be outside");
    }

    #[test]
    fn test_empty_polygon() {
        let points: Vec<IsoPoint> = vec![];
        assert!(points_to_polygon(&points).is_none());

        let points = vec![
            IsoPoint { lon: 4.0, lat: 50.0 },
            IsoPoint { lon: 5.0, lat: 50.0 },
        ];
        assert!(points_to_polygon(&points).is_none());
    }

    /// Integration test: Isochrone consistency with drive times
    ///
    /// Requires Belgium data
    /// Run with: cargo test -p butterfly-route test_isochrone_consistency -- --ignored --nocapture
    #[test]
    #[ignore] // Run manually with --ignored flag
    fn test_isochrone_consistency_brussels() {
        // Try multiple possible data locations
        let possible_paths = [
            "./data/belgium",
            "../data/belgium",
            "../../data/belgium",
            "data/belgium",
        ];

        let data_dir = possible_paths
            .iter()
            .map(Path::new)
            .find(|p| p.exists());

        let data_dir = match data_dir {
            Some(p) => p,
            None => {
                eprintln!("Skipping: Belgium data not found in any of {:?}", possible_paths);
                return;
            }
        };

        use crate::step9::state::ServerState;
        use crate::step9::query::CchQuery;

        // Load server state
        let state = ServerState::load(data_dir).expect("Failed to load server state");
        let mode = Mode::Car;
        let mode_data = state.get_mode(mode);

        // Test case: Brussels center, 10 min isochrone
        let origin_lon = 4.3517;
        let origin_lat = 50.8503;
        let threshold_s = 600; // 10 minutes
        let threshold_ds = threshold_s * 10;

        // Snap origin
        let origin_ebg = state.spatial_index.snap(origin_lon, origin_lat, &mode_data.mask, 10)
            .expect("Failed to snap origin");
        let origin_filtered = mode_data.filtered_ebg.original_to_filtered[origin_ebg as usize];
        assert_ne!(origin_filtered, u32::MAX, "Origin not in filtered graph");
        let origin_rank = mode_data.order.perm[origin_filtered as usize];

        // Compute PHAST distances
        let phast_settled = crate::step9::api::run_phast_bounded_fast(
            &mode_data.cch_topo,
            &mode_data.cch_weights,
            origin_rank,
            threshold_ds,
            mode,
        );

        // Convert to original IDs
        let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
        for (rank, dist) in phast_settled {
            let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            let original_id = mode_data.filtered_ebg.filtered_to_original[filtered_id as usize];
            settled.push((original_id, dist));
        }

        // Build isochrone polygon
        let polygon_points = build_isochrone_geometry_sparse(
            &settled,
            threshold_ds,
            &mode_data.node_weights,
            &state.ebg_nodes,
            &state.nbg_geo,
            mode,
        );

        assert!(!polygon_points.is_empty(), "Isochrone polygon should not be empty");
        assert!(polygon_points.len() >= 3, "Isochrone polygon should have at least 3 points");

        let polygon = points_to_polygon(&polygon_points).expect("Failed to create polygon");

        // Sample test points - we sample random points then snap to roads
        // The test semantics: "Is this ROAD POINT inside the polygon?"
        // This matches the isochrone definition: polygon should contain road surface
        let (min_lon, max_lon, min_lat, max_lat) = polygon_bbox_with_buffer(&polygon_points, 1.3);
        let sample_points = sample_points_in_bbox(min_lon, max_lon, min_lat, max_lat, 100, 12345);

        let mut inside_correct = 0;
        let mut inside_violations: Vec<ViolationInfo> = Vec::new();
        let mut outside_correct = 0;
        let mut outside_violations: Vec<ViolationInfo> = Vec::new();
        let mut unreachable = 0;
        let mut n_snapped = 0;

        // Create query engine
        let query = CchQuery::new(&state, mode);

        // Maximum snap distance for test samples (500m - larger than routing to get more coverage)
        const MAX_SNAP_DISTANCE_M: f64 = 500.0;

        for (lon, lat) in &sample_points {
            // Snap the random point to the nearest road
            let snap_result = state.spatial_index.snap_with_info(*lon, *lat, &mode_data.mask, 10);

            let (dst_ebg, snapped_lon, snapped_lat, snap_dist_m) = match snap_result {
                Some(result) => result,
                None => continue, // No road nearby, skip this sample
            };

            // Reject samples that snapped too far (likely water/parks)
            if snap_dist_m > MAX_SNAP_DISTANCE_M {
                continue;
            }

            n_snapped += 1;

            // Use SNAPPED coordinates for polygon containment check
            // This is the correct semantics: "is this road point inside the polygon?"
            let snapped_point = Point::new(snapped_lon, snapped_lat);
            let is_inside = polygon.contains(&snapped_point);

            // Compute drive time from origin to snapped EBG node
            let drive_time = compute_drive_time_ebg(mode_data, &query, origin_ebg, dst_ebg);

            match drive_time {
                Some(time_ds) => {
                    let time_s = time_ds as f32 / 10.0;
                    if is_inside {
                        if time_s <= threshold_s as f32 {
                            inside_correct += 1;
                        } else {
                            // Inside polygon but drive time exceeds threshold
                            // Allow 10% tolerance for boundary effects
                            let excess_ratio = time_s / threshold_s as f32;
                            if excess_ratio > 1.10 {
                                inside_violations.push(ViolationInfo {
                                    sampled_point: (*lon, *lat),
                                    snapped_point: (snapped_lon, snapped_lat),
                                    snap_distance_m: snap_dist_m,
                                    drive_time_s: time_s,
                                    threshold_s,
                                });
                                eprintln!("INSIDE VIOLATION: snapped ({:.4}, {:.4}) drive time {:.1}s > {}s ({}% over)",
                                    snapped_lon, snapped_lat, time_s, threshold_s,
                                    ((excess_ratio - 1.0) * 100.0) as u32);
                            } else {
                                // Within 10% tolerance - count as correct for boundary
                                inside_correct += 1;
                            }
                        }
                    } else if time_s > threshold_s as f32 {
                        outside_correct += 1;
                    } else {
                        // Outside polygon but drive time within threshold
                        // Allow 10% tolerance for boundary effects
                        let margin_ratio = time_s / threshold_s as f32;
                        if margin_ratio < 0.90 {
                            outside_violations.push(ViolationInfo {
                                sampled_point: (*lon, *lat),
                                snapped_point: (snapped_lon, snapped_lat),
                                snap_distance_m: snap_dist_m,
                                drive_time_s: time_s,
                                threshold_s,
                            });
                            eprintln!("OUTSIDE VIOLATION: snapped ({:.4}, {:.4}) drive time {:.1}s <= {}s ({}% under)",
                                snapped_lon, snapped_lat, time_s, threshold_s,
                                ((1.0 - margin_ratio) * 100.0) as u32);
                        } else {
                            // Within 10% of threshold - boundary case, count as correct
                            outside_correct += 1;
                        }
                    }
                }
                None => {
                    unreachable += 1;
                }
            }
        }

        println!("\nIsochrone consistency test results (Brussels 10min):");
        println!("  Samples attempted: {}", sample_points.len());
        println!("  Samples snapped to roads: {}", n_snapped);
        println!("  Inside correct: {}", inside_correct);
        println!("  Inside violations (>10% over threshold): {}", inside_violations.len());
        println!("  Outside correct: {}", outside_correct);
        println!("  Outside violations (<90% of threshold): {}", outside_violations.len());
        println!("  Unreachable (no route): {}", unreachable);

        // Print worst violations for debugging
        if !inside_violations.is_empty() {
            println!("\n  Worst inside violations:");
            let mut sorted = inside_violations.clone();
            sorted.sort_by(|a, b| b.drive_time_s.partial_cmp(&a.drive_time_s).unwrap());
            for v in sorted.iter().take(3) {
                println!("    snapped ({:.4}, {:.4}): {:.1}s > {}s ({:.0}% over)",
                    v.snapped_point.0, v.snapped_point.1,
                    v.drive_time_s, v.threshold_s,
                    (v.drive_time_s / v.threshold_s as f32 - 1.0) * 100.0);
            }
        }

        if !outside_violations.is_empty() {
            println!("\n  Worst outside violations:");
            let mut sorted = outside_violations.clone();
            sorted.sort_by(|a, b| a.drive_time_s.partial_cmp(&b.drive_time_s).unwrap());
            for v in sorted.iter().take(3) {
                println!("    snapped ({:.4}, {:.4}): {:.1}s <= {}s ({:.0}% under)",
                    v.snapped_point.0, v.snapped_point.1,
                    v.drive_time_s, v.threshold_s,
                    (1.0 - v.drive_time_s / v.threshold_s as f32) * 100.0);
            }
        }

        // Allow some tolerance - polygon is geographic approximation
        // Only count hard violations (>10% deviation from threshold)
        let total_violations = inside_violations.len() + outside_violations.len();
        let total_tested = inside_correct + inside_violations.len() + outside_correct + outside_violations.len();

        if total_tested == 0 {
            panic!("No samples could be tested - check data paths and snapping");
        }

        let violation_rate = total_violations as f32 / total_tested as f32;
        println!("\n  Total violations: {}/{} ({:.1}%)", total_violations, total_tested, violation_rate * 100.0);

        assert!(violation_rate < 0.10,
            "Violation rate {:.1}% exceeds 10% threshold", violation_rate * 100.0);
    }

    /// Compute drive time from origin EBG node to destination EBG node
    /// Returns drive time in deciseconds (ds), or None if no route
    fn compute_drive_time_ebg(
        mode_data: &crate::step9::state::ModeData,
        query: &crate::step9::query::CchQuery,
        origin_ebg: u32,
        dst_ebg: u32,
    ) -> Option<u32> {
        // Convert to filtered
        let src_filtered = mode_data.filtered_ebg.original_to_filtered[origin_ebg as usize];
        let dst_filtered = mode_data.filtered_ebg.original_to_filtered[dst_ebg as usize];

        if src_filtered == u32::MAX || dst_filtered == u32::MAX {
            return None;
        }

        // Convert filtered to rank (CchQuery expects rank IDs)
        let src_rank = mode_data.order.perm[src_filtered as usize];
        let dst_rank = mode_data.order.perm[dst_filtered as usize];

        // Run bidirectional query in rank space
        let result = query.query(src_rank, dst_rank)?;
        Some(result.distance)
    }
}
