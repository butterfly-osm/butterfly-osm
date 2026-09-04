//! Isochrone consistency tests
//!
//! Verifies that isochrone polygons are geometrically correct:
//! - Points INSIDE the polygon have drive time <= threshold
//! - Points OUTSIDE the polygon have drive time > threshold

use geo::{Contains, Coord, Point, Polygon};
use rand::rngs::StdRng;
#[allow(unused_imports)]
use rand::{Rng, RngExt, SeedableRng};

use crate::profile_abi::Mode;

use super::geometry::{Point as IsoPoint, build_isochrone_geometry_sparse};

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
    let max_lon = points
        .iter()
        .map(|p| p.lon)
        .fold(f64::NEG_INFINITY, f64::max);
    let min_lat = points.iter().map(|p| p.lat).fold(f64::INFINITY, f64::min);
    let max_lat = points
        .iter()
        .map(|p| p.lat)
        .fold(f64::NEG_INFINITY, f64::max);

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

    /// Per-violation diagnostic captured by the consistency suite.
    /// Stores only the fields actually consulted by the eprintln /
    /// assert paths below; richer per-violation context is logged
    /// inline via `eprintln!` and doesn't need to be retained on the
    /// struct.
    #[derive(Debug, Clone)]
    struct ViolationInfo {
        snapped_point: (f64, f64),
        drive_time_s: f32,
        threshold_s: u32,
    }

    // The aggregate `IsochroneTestResult` struct that previously held
    // (origin, n_samples, violations, …) was deleted in the
    // ship-readiness sweep — the consistency test computes those
    // numbers as locals and prints them inline; nothing constructed
    // or read the struct.

    /// #548.3 / #549: ONE contour encoder for the median branch, the
    /// uncertainty=bands branch, the bulk endpoint and Flight. The band
    /// branch used to re-encode inline and shipped an OPEN GeoJSON ring;
    /// closure is now structural — no branch can forget it.
    #[test]
    fn encode_contour_closes_the_ring_in_every_format() {
        use crate::server::geometry::{GeometryFormat, decode_polyline6, encode_contour};

        // Deliberately CW and deliberately OPEN: exactly what the sparse
        // tracer hands over.
        let ring: Vec<IsoPoint> = [(4.35, 50.85), (4.35, 50.86), (4.36, 50.86), (4.36, 50.85)]
            .iter()
            .map(|&(lon, lat)| IsoPoint { lon, lat })
            .collect();

        let (poly, geo, pts) = encode_contour(&ring, GeometryFormat::Polyline6);
        let decoded = decode_polyline6(&poly.expect("polyline6 ring"));
        assert_eq!(decoded.len(), ring.len() + 1, "polyline6 ring must close");
        assert_eq!(
            decoded.first(),
            decoded.last(),
            "polyline6 first != last: ring left open"
        );
        assert!(geo.is_none() && pts.is_none());

        let (poly, geo, pts) = encode_contour(&ring, GeometryFormat::GeoJson);
        let geo = geo.expect("geojson ring");
        assert_eq!(geo.len(), ring.len() + 1, "geojson ring must close");
        assert_eq!(
            geo.first(),
            geo.last(),
            "geojson first != last: this is the #548.3 band bug"
        );
        assert!(poly.is_none() && pts.is_none());

        let (poly, geo, pts) = encode_contour(&ring, GeometryFormat::Points);
        let pts = pts.expect("points ring");
        assert_eq!(pts.len(), ring.len() + 1, "points ring must close");
        assert_eq!(
            (pts[0].lon, pts[0].lat),
            (pts[pts.len() - 1].lon, pts[pts.len() - 1].lat),
            "points first != last: ring left open"
        );
        assert!(poly.is_none() && geo.is_none());
    }

    /// The median and band branches encode the SAME ring through the SAME
    /// encoder, and the `geometry` object a feature carries must be the
    /// very same ring as its `polygon_geojson` — winding, truncation and
    /// closure included.
    #[test]
    fn band_and_median_contours_encode_identically() {
        use crate::range::ContourPolygon;
        use crate::server::geometry::{GeometryFormat, encode_contour, primary_outer_ring};
        use crate::server::isochrone_handler::topology_geojson;

        let topology = vec![ContourPolygon {
            outer: vec![
                (4.351712, 50.850311),
                (4.351712, 50.860317),
                (4.361718, 50.860317),
                (4.361718, 50.850311),
            ],
            holes: vec![],
        }];
        let ring = primary_outer_ring(&topology);

        for format in [
            GeometryFormat::Polyline6,
            GeometryFormat::GeoJson,
            GeometryFormat::Points,
        ] {
            // The median branch and the band branch build their feature
            // from the same two calls (#549) — same ring in, same three
            // fields out.
            let median = encode_contour(&ring, format);
            let band = encode_contour(&primary_outer_ring(&topology), format);
            assert_eq!(median.0, band.0, "polyline6 differs between branches");
            assert_eq!(median.1, band.1, "polygon_geojson differs between branches");
            let same_points = match (&median.2, &band.2) {
                (Some(a), Some(b)) => {
                    a.len() == b.len()
                        && a.iter()
                            .zip(b)
                            .all(|(p, q)| p.lon == q.lon && p.lat == q.lat)
                }
                (None, None) => true,
                _ => false,
            };
            assert!(same_points, "polygon_points differs between branches");
        }

        // `geometry` (full topology) ≡ `polygon_geojson` (outer ring).
        let (_, geo, _) = encode_contour(&ring, GeometryFormat::GeoJson);
        let geo = geo.expect("geojson ring");
        let outer = topology_geojson(&topology)["coordinates"][0].clone();
        let outer: Vec<[f64; 2]> = serde_json::from_value(outer).expect("outer ring array");
        assert_eq!(
            geo, outer,
            "the feature's geometry and polygon_geojson must be the same ring"
        );
    }

    #[test]
    fn test_sample_points_deterministic() {
        let points1 = sample_points_in_bbox(4.0, 5.0, 50.0, 51.0, 10, 42);
        let points2 = sample_points_in_bbox(4.0, 5.0, 50.0, 51.0, 10, 42);
        assert_eq!(points1, points2, "Same seed should produce same points");
    }

    #[test]
    fn test_polygon_bbox_with_buffer() {
        let points = vec![
            IsoPoint {
                lon: 4.0,
                lat: 50.0,
            },
            IsoPoint {
                lon: 5.0,
                lat: 50.0,
            },
            IsoPoint {
                lon: 5.0,
                lat: 51.0,
            },
            IsoPoint {
                lon: 4.0,
                lat: 51.0,
            },
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
            IsoPoint {
                lon: 4.0,
                lat: 50.0,
            },
            IsoPoint {
                lon: 5.0,
                lat: 50.0,
            },
            IsoPoint {
                lon: 5.0,
                lat: 51.0,
            },
            IsoPoint {
                lon: 4.0,
                lat: 51.0,
            },
            IsoPoint {
                lon: 4.0,
                lat: 50.0,
            }, // Close the ring
        ];

        let poly = points_to_polygon(&points);
        assert!(poly.is_some());

        let poly = poly.unwrap();

        // Test point containment
        let inside = Point::new(4.5, 50.5);
        let outside = Point::new(3.0, 50.5);

        assert!(poly.contains(&inside), "Point (4.5, 50.5) should be inside");
        assert!(
            !poly.contains(&outside),
            "Point (3.0, 50.5) should be outside"
        );
    }

    #[test]
    fn test_empty_polygon() {
        let points: Vec<IsoPoint> = vec![];
        assert!(points_to_polygon(&points).is_none());

        let points = vec![
            IsoPoint {
                lon: 4.0,
                lat: 50.0,
            },
            IsoPoint {
                lon: 5.0,
                lat: 50.0,
            },
        ];
        assert!(points_to_polygon(&points).is_none());
    }

    /// Integration test: Isochrone consistency with drive times.
    ///
    /// Self-skips (#587) without the Belgium artifact; runs under a plain
    /// `cargo test` on a data-full runner:
    /// `BUTTERFLY_TEST_DATA_DIR=/path/to/data cargo test -p butterfly-route \
    ///     test_isochrone_consistency -- --nocapture`
    #[test]
    fn test_isochrone_consistency_brussels() {
        use crate::server::query::CchQuery;

        let Some(state) = crate::testutil::belgium_state("isochrone_test") else {
            return;
        };
        let mode_name = "car";
        let mode_idx = *state
            .mode_lookup
            .get(mode_name)
            .expect("car mode not found in data dir");
        let mode = Mode(mode_idx);
        let mode_data = state.get_mode(mode);

        // Test case: Brussels center, 10 min isochrone
        let origin_lon = 4.3517;
        let origin_lat = 50.8503;
        let threshold_s = 600u32; // 10 minutes (weights already in s post-#297)

        // Snap origin
        let origin_ebg = state
            .snap_index
            .snap(origin_lon, origin_lat, mode.0)
            .expect("Failed to snap origin");
        let origin_rank = mode_data.orig_to_rank[origin_ebg as usize];
        assert_ne!(origin_rank, u32::MAX, "Origin not in filtered graph");

        // Compute PHAST distances
        let phast_settled = crate::range::phast_seeded::run_phast_bounded_fast(
            &mode_data.up_adj_flat,
            &mode_data.down_adj_flat,
            origin_rank,
            threshold_s,
            mode,
        );

        // Convert to original IDs
        let mut settled: Vec<(u32, u32)> = Vec::with_capacity(phast_settled.len());
        for (rank, dist) in phast_settled {
            let filtered_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            let original_id = mode_data.filtered_to_original[filtered_id as usize];
            settled.push((original_id, dist));
        }

        // Build isochrone polygon
        // Whole reached edges only (no frontier fragments): this test checks
        // ring validity, not reach fidelity — see `gate_isochrone_reach_truth`.
        let polygon_points = build_isochrone_geometry_sparse(
            &settled,
            threshold_s,
            &mode_data.node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
            mode_name,
            None,
            None,
            &crate::server::geometry::ReachModel::Depart { frontier: &[] },
        )
        .into_iter()
        .next()
        .map(|p| {
            p.outer
                .into_iter()
                .map(|(lon, lat)| IsoPoint { lon, lat })
                .collect::<Vec<IsoPoint>>()
        })
        .unwrap_or_default();

        assert!(
            !polygon_points.is_empty(),
            "Isochrone polygon should not be empty"
        );
        assert!(
            polygon_points.len() >= 3,
            "Isochrone polygon should have at least 3 points"
        );

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
        let mode_data = state.get_mode(mode);
        let query = CchQuery::new(&mode_data);

        // Maximum snap distance for test samples (500m - larger than routing to get more coverage)
        const MAX_SNAP_DISTANCE_M: f64 = 500.0;

        for (lon, lat) in &sample_points {
            // Snap the random point to the nearest road
            let snap_result = state.snap_index.snap_with_info(*lon, *lat, mode.0);

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
            let drive_time = compute_drive_time_ebg(&mode_data, &query, origin_ebg, dst_ebg);

            match drive_time {
                Some(time_s_u) => {
                    let time_s = time_s_u as f32;
                    if is_inside {
                        if time_s <= threshold_s as f32 {
                            inside_correct += 1;
                        } else {
                            // Inside polygon but drive time exceeds threshold
                            // Allow 10% tolerance for boundary effects
                            let excess_ratio = time_s / threshold_s as f32;
                            if excess_ratio > 1.10 {
                                inside_violations.push(ViolationInfo {
                                    snapped_point: (snapped_lon, snapped_lat),
                                    drive_time_s: time_s,
                                    threshold_s,
                                });
                                let _ = (*lon, *lat, snap_dist_m); // values logged via eprintln below
                                eprintln!(
                                    "INSIDE VIOLATION: snapped ({:.4}, {:.4}) drive time {:.1}s > {}s ({}% over)",
                                    snapped_lon,
                                    snapped_lat,
                                    time_s,
                                    threshold_s,
                                    ((excess_ratio - 1.0) * 100.0) as u32
                                );
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
                                snapped_point: (snapped_lon, snapped_lat),
                                drive_time_s: time_s,
                                threshold_s,
                            });
                            let _ = (*lon, *lat, snap_dist_m); // values logged via eprintln below
                            eprintln!(
                                "OUTSIDE VIOLATION: snapped ({:.4}, {:.4}) drive time {:.1}s <= {}s ({}% under)",
                                snapped_lon,
                                snapped_lat,
                                time_s,
                                threshold_s,
                                ((1.0 - margin_ratio) * 100.0) as u32
                            );
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
        println!(
            "  Inside violations (>10% over threshold): {}",
            inside_violations.len()
        );
        println!("  Outside correct: {}", outside_correct);
        println!(
            "  Outside violations (<90% of threshold): {}",
            outside_violations.len()
        );
        println!("  Unreachable (no route): {}", unreachable);

        // Print worst violations for debugging
        if !inside_violations.is_empty() {
            println!("\n  Worst inside violations:");
            let mut sorted = inside_violations.clone();
            sorted.sort_by(|a, b| b.drive_time_s.partial_cmp(&a.drive_time_s).unwrap());
            for v in sorted.iter().take(3) {
                println!(
                    "    snapped ({:.4}, {:.4}): {:.1}s > {}s ({:.0}% over)",
                    v.snapped_point.0,
                    v.snapped_point.1,
                    v.drive_time_s,
                    v.threshold_s,
                    (v.drive_time_s / v.threshold_s as f32 - 1.0) * 100.0
                );
            }
        }

        if !outside_violations.is_empty() {
            println!("\n  Worst outside violations:");
            let mut sorted = outside_violations.clone();
            sorted.sort_by(|a, b| a.drive_time_s.partial_cmp(&b.drive_time_s).unwrap());
            for v in sorted.iter().take(3) {
                println!(
                    "    snapped ({:.4}, {:.4}): {:.1}s <= {}s ({:.0}% under)",
                    v.snapped_point.0,
                    v.snapped_point.1,
                    v.drive_time_s,
                    v.threshold_s,
                    (1.0 - v.drive_time_s / v.threshold_s as f32) * 100.0
                );
            }
        }

        // Allow some tolerance - polygon is geographic approximation
        // Only count hard violations (>10% deviation from threshold)
        let total_violations = inside_violations.len() + outside_violations.len();
        let total_tested =
            inside_correct + inside_violations.len() + outside_correct + outside_violations.len();

        if total_tested == 0 {
            panic!("No samples could be tested - check data paths and snapping");
        }

        let violation_rate = total_violations as f32 / total_tested as f32;
        println!(
            "\n  Total violations: {}/{} ({:.1}%)",
            total_violations,
            total_tested,
            violation_rate * 100.0
        );

        assert!(
            violation_rate < 0.10,
            "Violation rate {:.1}% exceeds 10% threshold",
            violation_rate * 100.0
        );
    }

    /// Compute drive time from origin EBG node to destination EBG node.
    /// Returns drive time in seconds (post-#297), or None if no route.
    fn compute_drive_time_ebg(
        mode_data: &crate::server::state::ModeData,
        query: &crate::server::query::CchQuery,
        origin_ebg: u32,
        dst_ebg: u32,
    ) -> Option<u32> {
        // Snap to rank space directly (#153)
        let src_rank = mode_data.orig_to_rank[origin_ebg as usize];
        let dst_rank = mode_data.orig_to_rank[dst_ebg as usize];

        if src_rank == u32::MAX || dst_rank == u32::MAX {
            return None;
        }

        // Run bidirectional query in rank space
        let result = query.query(src_rank, dst_rank)?;
        Some(result.distance)
    }
}

/// #559: the WKB guards must be decidable from parsed input alone so the
/// handler can reject before any PHAST work. This pins the pure guard the
/// handler calls ABOVE `isochrone_polygons`.
mod wkb_guard_tests {
    use crate::server::isochrone_handler::wkb_request_rejection;

    #[test]
    fn wkb_request_rejection_is_decided_from_parsed_input_alone() {
        // JSON never rejects here, whatever the contours / bands.
        assert_eq!(wkb_request_rejection(false, 1, false), None);
        assert_eq!(wkb_request_rejection(false, 3, true), None);
        // WKB: one contour, no bands → serveable.
        assert_eq!(wkb_request_rejection(true, 1, false), None);
        // WKB + several contours → 400, message unchanged.
        assert_eq!(
            wkb_request_rejection(true, 2, false),
            Some("WKB only supports single contour. Use JSON for multiple.")
        );
        // WKB + bands → 400 (bands win over the contour count, as before).
        assert_eq!(
            wkb_request_rejection(true, 1, true),
            Some("uncertainty=bands requires the JSON response (Accept: application/json)")
        );
        assert_eq!(
            wkb_request_rejection(true, 2, true),
            Some("uncertainty=bands requires the JSON response (Accept: application/json)")
        );
    }

    /// #559 is an ORDER defect, not a logic one: before the hoist the two
    /// guards sat inside the `if wants_wkb` block AFTER `isochrone_polygons`,
    /// so an unauthenticated 400 still paid a full seeded PHAST + contour
    /// topology. Only the handler's own source can witness that order without
    /// a loaded `ServerState` (which needs a built Belgium container, i.e. not
    /// a unit test): assert the guard call precedes the pipeline call. Move
    /// the guard back down and this fails.
    #[test]
    fn wkb_guard_runs_before_the_phast_pipeline() {
        const HANDLER: &str = include_str!("isochrone_handler.rs");
        let guard = HANDLER
            .find("if let Some(err) = wkb_request_rejection(")
            .expect("isochrone_handler calls wkb_request_rejection");
        let pipeline = HANDLER
            .find("let field = match isochrone_polygons(")
            .expect("isochrone_handler runs the isochrone_polygons pipeline");
        assert!(
            guard < pipeline,
            "#559: the WKB guard (byte {guard}) must run BEFORE the seeded \
             PHAST + topology pipeline (byte {pipeline})"
        );
        assert_eq!(
            HANDLER.matches("wkb_request_rejection(").count(),
            2,
            "exactly one definition and one call site — no second copy"
        );
    }
}

/// #558: `depart_frontier` is the ONE definition of "which unreached edge is
/// entered before T, and how far" for the polygon stamp, `include=network`,
/// `/isochrone/bulk`, Flight `isochrone` and the catchment hull. A synthetic
/// 6-edge CCH with hand-computed labels pins its arithmetic and invariants.
///
/// Reach model (`geometry.rs::ReachModel`): a PHAST label is the arrival at
/// the HEAD of the directed edge; an original CCH arc `e→f` weighs
/// `w(f) + turn(e,f)`; so `f` is entered at `label(e) + w_arc − w(f)`.
mod depart_frontier_tests {
    use std::borrow::Cow;

    use crate::formats::{
        ArcCow, BitsetField, CchTopo, CchWeights, EbgNode, EbgNodes, EdgeGeomOffsets,
        EdgeGeomPoints, WeightArray,
    };
    use crate::matrix::bucket_ch::{DownAdjFlat, DownReverseAdjFlat, UpAdjFlat};
    use crate::profile_abi::Mode;
    use crate::server::edge_geom::EdgeGeometry;
    use crate::server::geometry::{ReachModel, reachable_polylines};
    use crate::server::isochrone_handler::depart_frontier;
    use crate::server::state::ModeData;

    /// Edge weights `w(e)` by ORIGINAL EBG id. `e0` is the origin edge.
    const W: [u32; 6] = [10, 40, 100, 30, 50, 200];

    /// Original CCH arcs `(from, to, w_arc = w(to) + turn)` by original id:
    ///   e0→e1 turn 5, e0→e2 turn 0, e1→e3 turn 10, e1→e4 turn 0,
    ///   e1→e5 turn 60, e2→e4 turn 20, e2→e5 turn 0, e3→e5 turn 0.
    const ARCS: [(u32, u32, u32); 8] = [
        (0, 1, 45),
        (0, 2, 100),
        (1, 3, 40),
        (1, 4, 50),
        (1, 5, 260),
        (2, 4, 70),
        (2, 5, 200),
        (3, 5, 200),
    ];

    /// Depart field seeded with `label(e0) = 6` (remainder of the origin
    /// edge past the snap), head arrivals:
    ///   e1 = 6+45 = 51, e2 = 6+100 = 106, e3 = 51+40 = 91,
    ///   e4 = min(51+50, 106+70) = 101, e5 = min(91+200, 51+260, 106+200) = 291.
    const LABEL: [u32; 6] = [6, 51, 106, 91, 101, 291];

    /// rank → filtered → original. Deliberately NON-identity and
    /// non-involutive so a swapped lookup cannot pass by coincidence. The
    /// resulting ranks: e0=0, e3=1, e4=2, e5=3, e1=4, e2=5 — so e0→e1 and
    /// e0→e2 are UP arcs, e1→e3 / e1→e4 / e2→e4 are DOWN arcs.
    const RANK_TO_FILTERED: [u32; 6] = [1, 5, 4, 2, 3, 0];
    const FILTERED_TO_ORIGINAL: [u32; 6] = [2, 0, 5, 1, 4, 3];

    fn rank_of(orig: u32) -> u32 {
        (0..6u32)
            .find(|&r| FILTERED_TO_ORIGINAL[RANK_TO_FILTERED[r as usize] as usize] == orig)
            .expect("every original id has a rank")
    }

    /// CSR over `edges = (source rank, target rank, weight)`.
    fn csr(edges: &[(u32, u32, u32)]) -> (Vec<u64>, Vec<u32>, Vec<u32>) {
        let mut sorted = edges.to_vec();
        sorted.sort_unstable();
        let mut offsets = vec![0u64; 7];
        for &(s, _, _) in &sorted {
            offsets[s as usize + 1] += 1;
        }
        for i in 0..6 {
            offsets[i + 1] += offsets[i];
        }
        let targets = sorted.iter().map(|&(_, t, _)| t).collect();
        let weights = sorted.iter().map(|&(_, _, w)| w).collect();
        (offsets, targets, weights)
    }

    /// The fixture as a real `ModeData`: flats built by the production
    /// builders from a `CchTopo` + `CchWeights`, so the test exercises the
    /// same rank-indexed UP / forward-DOWN adjacency `depart_frontier` scans
    /// in serve.
    fn fixture() -> ModeData {
        let mut up_edges = Vec::new();
        let mut down_edges = Vec::new();
        for &(a, b, w) in &ARCS {
            let (ra, rb) = (rank_of(a), rank_of(b));
            if rb > ra {
                up_edges.push((ra, rb, w));
            } else {
                down_edges.push((ra, rb, w));
            }
        }
        assert_eq!(up_edges.len(), 3, "fixture has 3 UP arcs");
        assert_eq!(down_edges.len(), 5, "fixture has 5 DOWN arcs");
        let (up_off, up_tg, up_w) = csr(&up_edges);
        let (dn_off, dn_tg, dn_w) = csr(&down_edges);
        let topo = CchTopo {
            n_nodes: 6,
            n_shortcuts: 0,
            n_original_arcs: ARCS.len() as u64,
            inputs_sha: [0u8; 32],
            up_offsets: up_off.into(),
            up_targets: up_tg.clone().into(),
            up_is_shortcut: BitsetField::from_bools(&vec![false; up_tg.len()]),
            up_middle: vec![u32::MAX; up_tg.len()].into(),
            down_offsets: dn_off.into(),
            down_targets: dn_tg.clone().into(),
            down_is_shortcut: BitsetField::from_bools(&vec![false; dn_tg.len()]),
            down_middle: vec![u32::MAX; dn_tg.len()].into(),
            rank_to_filtered: RANK_TO_FILTERED.to_vec().into(),
        };
        let weights = CchWeights {
            up: WeightArray::from_vec_u32(up_w),
            down: WeightArray::from_vec_u32(dn_w),
            up_middle: ArcCow::from_vec(Vec::new()),
            down_middle: ArcCow::from_vec(Vec::new()),
        };
        let up_adj_flat = UpAdjFlat::build(&topo, &weights);
        let down_rev_flat = DownReverseAdjFlat::build(&topo, &weights);
        let down_adj_flat = DownAdjFlat::build(&topo, &weights);
        let up_adj_flat_dist = UpAdjFlat::build(&topo, &weights);
        let down_rev_flat_dist = DownReverseAdjFlat::build(&topo, &weights);
        let orig_to_rank: Vec<u32> = (0..6u32).map(rank_of).collect();
        ModeData {
            mode: Mode::from_u8(0),
            cch_topo: topo,
            cch_weights: weights.clone(),
            cch_weights_dist: weights,
            cch_weights_len_along_time: None,
            orig_to_rank: ArcCow::from_vec(orig_to_rank),
            filtered_to_original: ArcCow::from_vec(FILTERED_TO_ORIGINAL.to_vec()),
            n_filtered_nodes: 6,
            n_original_nodes: 6,
            node_weights: Cow::Owned(W.to_vec()),
            mask: vec![0b11_1111],
            has_outbound: vec![0b11_1111],
            has_inbound: vec![0b11_1111],
            up_adj_flat,
            down_rev_flat,
            down_adj_flat,
            up_adj_flat_dist,
            down_rev_flat_dist,
            up_adj_flat_len_along_time: None,
            down_rev_flat_len_along_time: None,
            down_adj_flat_len_along_time_lazy: std::sync::OnceLock::new(),
            exclude_cache: crate::server::exclude::ExcludeWeightCache::default(),
        }
    }

    /// The PHAST output shape: `(rank, label)` for every settled node, in a
    /// deliberately scrambled order (the frontier must not depend on it).
    fn settled_ranks() -> Vec<(u32, u32)> {
        [4u32, 0, 5, 2, 1, 3]
            .iter()
            .map(|&o| (rank_of(o), LABEL[o as usize]))
            .collect()
    }

    fn frontier(md: &ModeData, threshold: u32) -> Vec<(u32, f32)> {
        depart_frontier(
            &settled_ranks(),
            threshold,
            &md.up_adj_flat,
            &md.down_adj_flat,
            md,
            &md.node_weights,
        )
    }

    /// The served fraction, computed exactly as the engine does.
    fn frac(threshold: u32, entry: u32, wf: u32) -> f32 {
        (threshold - entry) as f32 / wf as f32
    }

    #[test]
    fn depart_frontier_entry_is_label_plus_arc_minus_edge_weight_for_up_and_down_arcs() {
        let md = fixture();
        // T = 95: reached e0 (6), e1 (51), e3 (91). Unreached successors:
        //   e2 via the UP arc e0→e2:   entry = 6 + 100 − 100 = 6
        //   e4 via the DOWN arc e1→e4: entry = 51 + 50 − 50 = 51
        //   e5 via the UP arc e3→e5:   entry = 91 + 200 − 200 = 91
        //   (e1→e5 would enter at 51 + 60 = 111 > T: not a candidate)
        assert_eq!(
            frontier(&md, 95),
            vec![
                (2, frac(95, 6, 100)),
                (4, frac(95, 51, 50)),
                (5, frac(95, 91, 200)),
            ]
        );
        // Turn penalties are part of the entry: T = 90 leaves e3 (91)
        // unreached, entered from e1 at 51 + 10 = 61 (NOT 51).
        assert_eq!(
            frontier(&md, 90),
            vec![
                (2, frac(90, 6, 100)),
                (3, frac(90, 61, 30)),
                (4, frac(90, 51, 50)),
            ]
        );
    }

    #[test]
    fn depart_frontier_excludes_arcs_entered_at_or_after_the_budget() {
        let md = fixture();
        // T = 91: e5 is entered at 91 from e3 (== T) and at 111 from e1
        // (> T): zero metres driven before T — not a frontier edge.
        let f = frontier(&md, 91);
        assert!(
            f.iter().all(|&(o, _)| o != 5),
            "e5 entered at/after T must be absent: {f:?}"
        );
        assert_eq!(f, vec![(2, frac(91, 6, 100)), (4, frac(91, 51, 50))]);
        // T = 56: e3's only reached predecessor enters it at 61 > 56.
        let f = frontier(&md, 56);
        assert_eq!(f, vec![(2, frac(56, 6, 100)), (4, frac(56, 51, 50))]);
    }

    #[test]
    fn depart_frontier_never_reports_a_reached_successor() {
        let md = fixture();
        // T = 300: everything is reached (e5 at 291). The arc e2→e5 alone
        // would say "arrives 306 > T, entered at 106 < T" — but e5 IS
        // reached through e3, so it is a whole edge, not a frontier edge.
        assert_eq!(frontier(&md, 300), Vec::<(u32, f32)>::new());
        // T = 95: e1 and e3 are reached successors of e0 / e1.
        let f = frontier(&md, 95);
        assert!(
            f.iter().all(|&(o, _)| o != 0 && o != 1 && o != 3),
            "reached edges must never be frontier: {f:?}"
        );
    }

    #[test]
    fn depart_frontier_takes_the_minimum_entry_over_reached_predecessors() {
        let md = fixture();
        // T = 150: e5 (291) is unreached with THREE reached predecessors —
        // e1 (entry 111, DOWN arc), e2 (entry 106, DOWN arc), e3 (entry 91,
        // UP arc). The earliest entry wins, whatever the scan order.
        assert_eq!(frontier(&md, 150), vec![(5, frac(150, 91, 200))]);
        let mut reversed = settled_ranks();
        reversed.reverse();
        let f = depart_frontier(
            &reversed,
            150,
            &md.up_adj_flat,
            &md.down_adj_flat,
            &md,
            &md.node_weights,
        );
        assert_eq!(f, vec![(5, frac(150, 91, 200))]);
    }

    /// Issue #558 invariant, brute-forced from the arc list for every
    /// threshold: for each reported `(f, fraction)`, `label(f) > T`,
    /// `0 < fraction < 1`, and the entry is the minimum over reached
    /// predecessors `e` of `label(e) + w_arc(e→f) − w(f)`, kept only if `< T`.
    #[test]
    fn depart_frontier_matches_a_brute_force_reference_for_every_threshold() {
        let md = fixture();
        for t in 0..=320u32 {
            let reached = |o: u32| LABEL[o as usize] <= t;
            let mut best: std::collections::BTreeMap<u32, u32> = Default::default();
            for &(a, b, w) in &ARCS {
                if !reached(a) || reached(b) {
                    continue;
                }
                let entry = LABEL[a as usize] + w - W[b as usize];
                if entry >= t {
                    continue;
                }
                best.entry(b)
                    .and_modify(|e| *e = (*e).min(entry))
                    .or_insert(entry);
            }
            let expected: Vec<(u32, f32)> = best
                .iter()
                .map(|(&b, &entry)| (b, frac(t, entry, W[b as usize])))
                .collect();
            let got = frontier(&md, t);
            assert_eq!(got, expected, "T = {t}");
            for &(f, fraction) in &got {
                assert!(LABEL[f as usize] > t, "T = {t}: e{f} is reached");
                assert!(
                    fraction > 0.0 && fraction < 1.0,
                    "T = {t}: e{f} fraction {fraction}"
                );
            }
        }
    }

    /// Same fixture through `reachable_polylines` (the sole caller's
    /// consumer): whole reached edges, then the frontier edges cut at the
    /// served fraction — from their TRUE start (an endpoint of a reached
    /// edge), even when the stored polyline runs the other way.
    #[test]
    fn reachable_polylines_cuts_the_frontier_edge_at_the_served_fraction() {
        const LON0: i32 = 40_000_000;
        const LAT0: i32 = 500_000_000;
        // Stored polylines `(lon_e7, lat_e7)` by original id. e2 is stored
        // REVERSED (its last point is e0's head); everything else forward.
        let polylines: [[(i32, i32); 2]; 6] = [
            [(LON0, LAT0), (LON0 + 100, LAT0)],
            [(LON0 + 100, LAT0), (LON0 + 200, LAT0)],
            [(LON0 + 100, LAT0 + 1000), (LON0 + 100, LAT0)],
            [(LON0 + 200, LAT0), (LON0 + 300, LAT0)],
            [(LON0 + 200, LAT0), (LON0 + 200, LAT0 + 1000)],
            [(LON0 + 300, LAT0), (LON0 + 300, LAT0 + 1000)],
        ];
        let mut flat = Vec::new();
        for pl in &polylines {
            for &(lon, lat) in pl {
                flat.push(lon);
                flat.push(lat);
            }
        }
        let geom = EdgeGeometry::from_sections(
            EdgeGeomOffsets {
                n_edges: 6,
                n_points: 12,
                offsets: ArcCow::from_vec((0..=6u32).map(|i| i * 2).collect()),
            },
            EdgeGeomPoints {
                n_points: 12,
                bbox_min_lon: LON0,
                bbox_min_lat: LAT0,
                bbox_max_lon: LON0 + 300,
                bbox_max_lat: LAT0 + 1000,
                points: ArcCow::from_vec(flat),
            },
        )
        .unwrap();
        let ebg = EbgNodes {
            n_nodes: 6,
            created_unix: 0,
            inputs_sha: [0u8; 32],
            nodes: ArcCow::from_vec(
                (0..6u32)
                    .map(|i| EbgNode {
                        tail_nbg: i,
                        head_nbg: i + 1,
                        geom_idx: i,
                        length_m: 1,
                        class_bits: 0,
                        primary_way: 0,
                    })
                    .collect(),
            ),
        };

        let md = fixture();
        // T = 56: whole e0 (6), e1 (51); frontier e2 at (56−6)/100 = 0.5
        // and e4 at (56−51)/50 = 0.1; e3 (entry 61) and e5 are out.
        let front = frontier(&md, 56);
        assert_eq!(front, vec![(2, 0.5), (4, frac(56, 51, 50))]);

        let settled_orig: Vec<(u32, u32)> = (0..6u32).map(|o| (o, LABEL[o as usize])).collect();
        let (out, anchor) = reachable_polylines(
            &settled_orig,
            56,
            &W,
            &ebg,
            &geom,
            &ReachModel::Depart { frontier: &front },
            true,
        );
        // Lat-first e7, whole edges first (settled order), then the frontier
        // fragments (frontier order).
        assert_eq!(
            out,
            vec![
                vec![(LAT0, LON0), (LAT0, LON0 + 100)],
                vec![(LAT0, LON0 + 100), (LAT0, LON0 + 200)],
                // e2: reversed to start at e0's head, then cut at 0.5
                vec![(LAT0, LON0 + 100), (LAT0 + 500, LON0 + 100)],
                // e4: stored forward from e1's head, cut at 0.1
                vec![(LAT0, LON0 + 200), (LAT0 + 100, LON0 + 200)],
            ]
        );
        assert_eq!(anchor, Some((LAT0, LON0)), "start of the min-label edge");
    }
}
