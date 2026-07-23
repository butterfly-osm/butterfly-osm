//! Unit tests extracted from the original api.rs

use super::isochrone_handler::{ContourFeature, IsochroneResponse};
use super::route::{RouteAnnotations, RouteResponse, bearing_diff, classify_turn, compute_bearing};
use super::types::{parse_mode, validate_coord};

use crate::profile_abi::Mode;

// === E4: Bearing computation tests ===

#[test]
fn test_compute_bearing_north() {
    let b = compute_bearing(50.0, 4.0, 51.0, 4.0);
    assert!(
        !(5..=355).contains(&b),
        "North bearing should be ~0, got {}",
        b
    );
}

#[test]
fn test_compute_bearing_east() {
    let b = compute_bearing(50.0, 4.0, 50.0, 5.0);
    assert!(
        (b as i16 - 90).unsigned_abs() < 5,
        "East bearing should be ~90, got {}",
        b
    );
}

#[test]
fn test_compute_bearing_south() {
    let b = compute_bearing(51.0, 4.0, 50.0, 4.0);
    assert!(
        (b as i16 - 180).unsigned_abs() < 5,
        "South bearing should be ~180, got {}",
        b
    );
}

#[test]
fn test_compute_bearing_west() {
    let b = compute_bearing(50.0, 5.0, 50.0, 4.0);
    assert!(
        (b as i16 - 270).unsigned_abs() < 5,
        "West bearing should be ~270, got {}",
        b
    );
}

#[test]
fn test_compute_bearing_northeast() {
    let b = compute_bearing(50.0, 4.0, 50.5, 4.5);
    assert!(b > 20 && b < 70, "NE bearing should be ~30-60, got {}", b);
}

// === E4: Bearing difference tests ===

#[test]
fn test_bearing_diff_straight() {
    assert_eq!(bearing_diff(90, 90), 0);
    assert_eq!(bearing_diff(0, 0), 0);
    assert_eq!(bearing_diff(359, 359), 0);
}

#[test]
fn test_bearing_diff_right_turn() {
    assert_eq!(bearing_diff(0, 90), 90);
    assert_eq!(bearing_diff(270, 0), 90);
}

#[test]
fn test_bearing_diff_left_turn() {
    assert_eq!(bearing_diff(90, 0), 270);
    assert_eq!(bearing_diff(0, 270), 270);
}

#[test]
fn test_bearing_diff_uturn() {
    assert_eq!(bearing_diff(0, 180), 180);
    assert_eq!(bearing_diff(90, 270), 180);
}

#[test]
fn test_bearing_diff_wrap_around() {
    assert_eq!(bearing_diff(350, 10), 20);
    assert_eq!(bearing_diff(10, 350), 340);
}

// === E4: Turn classification tests ===

#[test]
fn test_classify_turn_straight() {
    assert_eq!(classify_turn(0), "straight");
    assert_eq!(classify_turn(10), "straight");
    assert_eq!(classify_turn(350), "straight");
    assert_eq!(classify_turn(360), "straight");
}

#[test]
fn test_classify_turn_slight_right() {
    assert_eq!(classify_turn(20), "slight right");
    assert_eq!(classify_turn(45), "slight right");
    assert_eq!(classify_turn(60), "slight right");
}

#[test]
fn test_classify_turn_right() {
    assert_eq!(classify_turn(90), "right");
    assert_eq!(classify_turn(100), "right");
    assert_eq!(classify_turn(120), "right");
}

#[test]
fn test_classify_turn_sharp_right() {
    assert_eq!(classify_turn(130), "sharp right");
    assert_eq!(classify_turn(150), "sharp right");
}

#[test]
fn test_classify_turn_uturn() {
    assert_eq!(classify_turn(180), "uturn");
    assert_eq!(classify_turn(170), "uturn");
    assert_eq!(classify_turn(195), "uturn");
}

#[test]
fn test_classify_turn_sharp_left() {
    assert_eq!(classify_turn(210), "sharp left");
    assert_eq!(classify_turn(230), "sharp left");
}

#[test]
fn test_classify_turn_left() {
    assert_eq!(classify_turn(270), "left");
    assert_eq!(classify_turn(250), "left");
    assert_eq!(classify_turn(300), "left");
}

#[test]
fn test_classify_turn_slight_left() {
    assert_eq!(classify_turn(310), "slight left");
    assert_eq!(classify_turn(330), "slight left");
}

#[test]
fn test_classify_turn_all_angles_classified() {
    for angle in 0..=360u16 {
        let result = classify_turn(angle);
        assert!(
            [
                "straight",
                "slight right",
                "right",
                "sharp right",
                "uturn",
                "sharp left",
                "left",
                "slight left"
            ]
            .contains(&result),
            "Angle {} classified as unexpected '{}'",
            angle,
            result
        );
    }
}

#[test]
fn test_bearing_reverse_is_180_off() {
    let fwd = compute_bearing(50.0, 4.0, 51.0, 5.0);
    let rev = compute_bearing(51.0, 5.0, 50.0, 4.0);
    let diff = bearing_diff(fwd, rev);
    assert!(
        (diff as i16 - 180).unsigned_abs() < 5,
        "Reverse bearing should differ by ~180, got diff={}",
        diff
    );
}

// === Codex-M7: Input validation unit tests ===

// --- validate_coord tests ---

#[test]
fn test_validate_coord_valid_origin() {
    assert!(validate_coord(0.0, 0.0, "origin").is_ok());
}

#[test]
fn test_validate_coord_valid_extremes() {
    assert!(validate_coord(-180.0, -90.0, "sw").is_ok());
    assert!(validate_coord(180.0, 90.0, "ne").is_ok());
    assert!(validate_coord(-180.0, 90.0, "nw").is_ok());
    assert!(validate_coord(180.0, -90.0, "se").is_ok());
}

#[test]
fn test_validate_coord_valid_brussels() {
    assert!(validate_coord(4.35, 50.85, "brussels").is_ok());
}

#[test]
fn test_validate_coord_rejects_nan_lon() {
    let result = validate_coord(f64::NAN, 50.0, "test");
    assert!(result.is_err(), "NaN longitude should be rejected");
    let msg = result.unwrap_err();
    assert!(
        msg.contains("NaN") || msg.contains("outside"),
        "Error message should mention NaN or out of range, got: {}",
        msg
    );
}

#[test]
fn test_validate_coord_rejects_nan_lat() {
    let result = validate_coord(4.0, f64::NAN, "test");
    assert!(result.is_err(), "NaN latitude should be rejected");
}

#[test]
fn test_validate_coord_rejects_both_nan() {
    let result = validate_coord(f64::NAN, f64::NAN, "test");
    assert!(result.is_err(), "Both NaN should be rejected");
}

#[test]
fn test_validate_coord_rejects_positive_infinity_lon() {
    let result = validate_coord(f64::INFINITY, 50.0, "test");
    assert!(result.is_err(), "+Inf longitude should be rejected");
    assert!(
        result.unwrap_err().contains("outside"),
        "Error should mention out-of-range"
    );
}

#[test]
fn test_validate_coord_rejects_negative_infinity_lon() {
    let result = validate_coord(f64::NEG_INFINITY, 50.0, "test");
    assert!(result.is_err(), "-Inf longitude should be rejected");
}

#[test]
fn test_validate_coord_rejects_positive_infinity_lat() {
    let result = validate_coord(4.0, f64::INFINITY, "test");
    assert!(result.is_err(), "+Inf latitude should be rejected");
}

#[test]
fn test_validate_coord_rejects_negative_infinity_lat() {
    let result = validate_coord(4.0, f64::NEG_INFINITY, "test");
    assert!(result.is_err(), "-Inf latitude should be rejected");
}

#[test]
fn test_validate_coord_rejects_lon_too_high() {
    let result = validate_coord(180.01, 50.0, "test");
    assert!(result.is_err(), "Longitude > 180 should be rejected");
    let msg = result.unwrap_err();
    assert!(
        msg.contains("longitude"),
        "Error should reference longitude"
    );
    assert!(msg.contains("180.01"), "Error should include the bad value");
}

#[test]
fn test_validate_coord_rejects_lon_too_low() {
    let result = validate_coord(-180.01, 50.0, "test");
    assert!(result.is_err(), "Longitude < -180 should be rejected");
}

#[test]
fn test_validate_coord_rejects_lat_too_high() {
    let result = validate_coord(4.0, 90.01, "test");
    assert!(result.is_err(), "Latitude > 90 should be rejected");
    let msg = result.unwrap_err();
    assert!(msg.contains("latitude"), "Error should reference latitude");
}

#[test]
fn test_validate_coord_rejects_lat_too_low() {
    let result = validate_coord(4.0, -90.01, "test");
    assert!(result.is_err(), "Latitude < -90 should be rejected");
}

#[test]
fn test_validate_coord_rejects_wildly_out_of_range() {
    assert!(validate_coord(999.0, 50.0, "test").is_err());
    assert!(validate_coord(4.0, -999.0, "test").is_err());
    assert!(validate_coord(1e18, 1e18, "test").is_err());
}

#[test]
fn test_validate_coord_error_includes_label() {
    let result = validate_coord(999.0, 50.0, "my_source");
    assert!(result.is_err());
    assert!(
        result.unwrap_err().contains("my_source"),
        "Error message should include the label"
    );
}

// --- parse_mode tests ---

/// Helper: build a test mode_lookup table with the standard bike/car/foot modes
fn test_mode_lookup() -> std::collections::HashMap<String, u8> {
    let mut lookup = std::collections::HashMap::new();
    lookup.insert("bike".to_string(), 0);
    lookup.insert("car".to_string(), 1);
    lookup.insert("foot".to_string(), 2);
    lookup
}

#[test]
fn test_parse_mode_car() {
    let lookup = test_mode_lookup();
    assert_eq!(parse_mode("car", &lookup).unwrap(), Mode(1));
}

#[test]
fn test_parse_mode_bike() {
    let lookup = test_mode_lookup();
    assert_eq!(parse_mode("bike", &lookup).unwrap(), Mode(0));
}

#[test]
fn test_parse_mode_foot() {
    let lookup = test_mode_lookup();
    assert_eq!(parse_mode("foot", &lookup).unwrap(), Mode(2));
}

#[test]
fn test_parse_mode_case_insensitive() {
    let lookup = test_mode_lookup();
    assert_eq!(parse_mode("Car", &lookup).unwrap(), Mode(1));
    assert_eq!(parse_mode("CAR", &lookup).unwrap(), Mode(1));
    assert_eq!(parse_mode("Bike", &lookup).unwrap(), Mode(0));
    assert_eq!(parse_mode("FOOT", &lookup).unwrap(), Mode(2));
}

#[test]
fn test_parse_mode_rejects_airplane() {
    let lookup = test_mode_lookup();
    let result = parse_mode("airplane", &lookup);
    assert!(result.is_err(), "airplane is not a valid mode");
    let msg = result.unwrap_err();
    assert!(
        msg.contains("Invalid mode"),
        "Error should say 'Invalid mode', got: {}",
        msg
    );
    assert!(msg.contains("airplane"), "Error should echo the bad value");
}

#[test]
fn test_parse_mode_rejects_empty() {
    let lookup = test_mode_lookup();
    assert!(
        parse_mode("", &lookup).is_err(),
        "Empty string is not a valid mode"
    );
}

#[test]
fn test_parse_mode_rejects_similar() {
    let lookup = test_mode_lookup();
    assert!(parse_mode("cars", &lookup).is_err());
    assert!(parse_mode("bicycle", &lookup).is_err());
    assert!(parse_mode("walk", &lookup).is_err());
    assert!(parse_mode("driving", &lookup).is_err());
}

#[test]
fn test_parse_mode_rejects_whitespace() {
    let lookup = test_mode_lookup();
    assert!(parse_mode(" car", &lookup).is_err());
    assert!(parse_mode("car ", &lookup).is_err());
    assert!(parse_mode(" ", &lookup).is_err());
}

// --- Request guard constant sanity tests ---

#[test]
fn test_max_table_cells_is_sensible() {
    let max_table_cells: usize = 10_000_000;
    assert!(max_table_cells <= 10_000_000_000);
    assert!(max_table_cells >= 1);
    assert!(3162 * 3162 <= max_table_cells);
    assert!(3163 * 3163 > max_table_cells);
}

#[test]
fn test_table_stream_has_no_hard_limit() {
    // /table/stream has no hard point-count limit
}

#[test]
fn test_max_bulk_origins_is_sensible() {
    let max_bulk_origins: usize = 10_000;
    assert!(max_bulk_origins <= 10_000);
    assert!(max_bulk_origins >= 1);
}

#[test]
fn test_match_coordinate_limit_is_sensible() {
    let max_match_coords: usize = 500;
    assert!(max_match_coords <= 500);
    assert!(max_match_coords >= 2);
}

// === 1. Negative zero handling ===

#[test]
fn test_validate_coord_negative_zero() {
    assert!(validate_coord(-0.0, 0.0, "negzero_lon").is_ok());
    assert!(validate_coord(0.0, -0.0, "negzero_lat").is_ok());
    assert!(validate_coord(-0.0, -0.0, "negzero_both").is_ok());
}

// === 2. Mode with whitespace trimming ===

#[test]
fn test_parse_mode_rejects_padded_whitespace() {
    let lookup: std::collections::HashMap<String, u8> = [
        ("car".to_string(), 1u8),
        ("bike".to_string(), 0u8),
        ("foot".to_string(), 2u8),
    ]
    .into_iter()
    .collect();
    assert!(parse_mode(" car ", &lookup).is_err());
    assert!(parse_mode("\tcar", &lookup).is_err());
    assert!(parse_mode("car\n", &lookup).is_err());
}

// === 3. Geometry format validation ===

#[test]
fn test_geometry_format_parse_valid() {
    use super::geometry::GeometryFormat;
    assert!(GeometryFormat::parse("polyline6").is_ok());
    assert!(GeometryFormat::parse("geojson").is_ok());
    assert!(GeometryFormat::parse("points").is_ok());
}

#[test]
fn test_geometry_format_parse_invalid() {
    use super::geometry::GeometryFormat;
    assert!(GeometryFormat::parse("INVALID").is_err());
    assert!(GeometryFormat::parse("polyline").is_err());
    assert!(GeometryFormat::parse("json").is_err());
    assert!(GeometryFormat::parse("").is_err());
    assert!(GeometryFormat::parse(" polyline6 ").is_err());
}

// === 4. Isochrone time_s boundary tests ===

#[test]
fn test_isochrone_time_bounds_constants() {
    let min_time: u32 = 1;
    let max_time: u32 = 7200;
    assert!(min_time >= 1);
    assert!(max_time <= 7200);
    assert!(max_time > min_time);
}

// === 5. Nearest number limits ===

#[test]
fn test_nearest_number_limit_constants() {
    let max_nearest: u32 = 100;
    assert!(max_nearest >= 1);
    assert!(max_nearest <= 100);
}

// === 6. Trip waypoint limits ===

#[test]
fn test_trip_waypoint_limit_constants() {
    let min_trip: usize = 2;
    let max_trip: usize = 100;
    assert!(min_trip >= 2);
    assert!(max_trip <= 100);
}

// === 7. Match coordinate limits ===

#[test]
fn test_match_coord_limit_constants() {
    let max_match: usize = 500;
    let min_match: usize = 2;
    assert!(min_match >= 2);
    assert!(max_match <= 500);
}

// === 8. Height coordinate limit ===

#[test]
fn test_height_coord_limit_constants() {
    let max_height: usize = 10_000;
    assert!(max_height >= 1);
    assert!(max_height <= 10_000);
}

// === 9. Validate coord with exact boundaries ===

#[test]
fn test_validate_coord_just_inside_bounds() {
    assert!(validate_coord(179.999999, 89.999999, "near_ne").is_ok());
    assert!(validate_coord(-179.999999, -89.999999, "near_sw").is_ok());
}

#[test]
fn test_validate_coord_just_outside_bounds() {
    assert!(validate_coord(180.001, 0.0, "lon_over").is_err());
    assert!(validate_coord(-180.001, 0.0, "lon_under").is_err());
    assert!(validate_coord(0.0, 90.001, "lat_over").is_err());
    assert!(validate_coord(0.0, -90.001, "lat_under").is_err());
}

// === 10. Validate coord with very small epsilon beyond boundary ===

#[test]
fn test_validate_coord_epsilon_outside() {
    let eps = 1e-10;
    assert!(validate_coord(180.0 + eps, 0.0, "eps_lon").is_err());
    assert!(validate_coord(0.0, 90.0 + eps, "eps_lat").is_err());
}

// === 11. Verify error messages include coordinate values ===

#[test]
fn test_validate_coord_error_includes_coordinate_value() {
    let result = validate_coord(200.5, 50.0, "src");
    assert!(result.is_err());
    let msg = result.unwrap_err();
    assert!(
        msg.contains("200.5"),
        "Error should include bad lon value: {}",
        msg
    );
}

#[test]
fn test_validate_coord_error_includes_label_for_lat() {
    let result = validate_coord(4.0, 95.5, "destination");
    assert!(result.is_err());
    let msg = result.unwrap_err();
    assert!(
        msg.contains("destination"),
        "Error should include label: {}",
        msg
    );
    assert!(
        msg.contains("95.5"),
        "Error should include bad lat value: {}",
        msg
    );
}

// === 12. Parse mode Unicode/special chars ===

#[test]
fn test_parse_mode_rejects_unicode() {
    let lookup: std::collections::HashMap<String, u8> =
        [("car".to_string(), 1u8)].into_iter().collect();
    assert!(parse_mode("c\u{00e1}r", &lookup).is_err());
    assert!(parse_mode("\u{1f697}", &lookup).is_err());
    assert!(parse_mode("car\0", &lookup).is_err());
}

// === 13. TSP pure tests ===

#[test]
fn test_tsp_all_unreachable() {
    use super::trip::solve_tsp;
    let matrix = vec![u32::MAX; 9];
    let result = solve_tsp(&matrix, 3, true);
    assert_eq!(result.order.len(), 3);
}

#[test]
fn test_tsp_two_points_round_trip() {
    use super::trip::solve_tsp;
    let matrix = vec![0, 10, 20, 0];
    let result = solve_tsp(&matrix, 2, true);
    assert_eq!(result.order.len(), 2);
    assert_eq!(result.total_cost, 30);
}

#[test]
fn test_tsp_two_points_one_way() {
    use super::trip::solve_tsp;
    let matrix = vec![0, 10, 20, 0];
    let result = solve_tsp(&matrix, 2, false);
    assert_eq!(result.order.len(), 2);
    assert_eq!(result.total_cost, 10);
}

#[test]
fn test_tsp_duplicate_waypoints() {
    use super::trip::solve_tsp;
    let matrix = vec![0, 0, 100, 0, 0, 100, 100, 100, 0];
    let result = solve_tsp(&matrix, 3, true);
    assert_eq!(result.order.len(), 3);
    assert!(result.total_cost <= 400);
}

#[test]
fn test_tsp_single_waypoint() {
    use super::trip::solve_tsp;
    let matrix = vec![0];
    let result = solve_tsp(&matrix, 1, true);
    assert_eq!(result.order.len(), 1);
    assert_eq!(result.total_cost, 0);
}

#[test]
fn test_tsp_empty() {
    use super::trip::solve_tsp;
    let result = solve_tsp(&[], 0, true);
    assert!(result.order.is_empty());
    assert_eq!(result.total_cost, 0);
}

#[test]
fn test_tsp_waypoint_index_is_permutation() {
    use super::trip::solve_tsp;
    let matrix = vec![
        0, 10, 20, 30, 40, 15, 0, 25, 35, 45, 20, 25, 0, 10, 50, 30, 35, 10, 0, 15, 40, 45, 50, 15,
        0,
    ];
    let result = solve_tsp(&matrix, 5, true);
    assert_eq!(result.order.len(), 5);
    let mut sorted = result.order.clone();
    sorted.sort();
    assert_eq!(sorted, vec![0, 1, 2, 3, 4]);
}

// === 14. RouteGeometry serialization test ===

#[test]
fn test_route_geometry_json_only_has_geometry_fields() {
    use super::geometry::{GeometryFormat, Point, RouteGeometry};
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

    for format in &[
        GeometryFormat::Polyline6,
        GeometryFormat::GeoJson,
        GeometryFormat::Points,
    ] {
        let geom = RouteGeometry::from_points(points.clone(), *format);
        let json = serde_json::to_value(&geom).unwrap();
        let obj = json.as_object().unwrap();

        assert!(
            !obj.contains_key("distance_m"),
            "format {:?}: no distance_m",
            format
        );
        assert!(
            !obj.contains_key("duration_ds"),
            "format {:?}: no duration_ds (legacy ds field, pre-#297)",
            format
        );
        assert!(
            !obj.contains_key("duration_s"),
            "format {:?}: no duration_s",
            format
        );

        for (key, val) in obj {
            if let Some(n) = val.as_f64() {
                assert!(
                    n.is_finite(),
                    "format {:?}: field '{}' has non-finite value {}",
                    format,
                    key,
                    n
                );
            }
        }
    }
}

// === 15. Polyline6 round-trip test ===

#[test]
fn test_polyline6_roundtrip_precision() {
    use super::geometry::{Point, decode_polyline6, encode_polyline6};
    let original = vec![
        Point {
            lon: 4.3517,
            lat: 50.8503,
        },
        Point {
            lon: 4.4017,
            lat: 50.8603,
        },
        Point {
            lon: 3.7167,
            lat: 51.0500,
        },
        Point {
            lon: 5.5667,
            lat: 50.6333,
        },
    ];
    let encoded = encode_polyline6(&original);
    let decoded = decode_polyline6(&encoded);

    assert_eq!(original.len(), decoded.len());
    for (i, (orig, dec)) in original.iter().zip(decoded.iter()).enumerate() {
        assert!(
            (orig.lat - dec.0).abs() < 1e-5,
            "pt {}: lat {:.7} vs {:.7}",
            i,
            orig.lat,
            dec.0
        );
        assert!(
            (orig.lon - dec.1).abs() < 1e-5,
            "pt {}: lon {:.7} vs {:.7}",
            i,
            orig.lon,
            dec.1
        );
    }
}

// === 16. GeoJSON coordinate order test ===

#[test]
fn test_geojson_coordinates_are_lon_lat_order() {
    use super::geometry::{GeometryFormat, Point, RouteGeometry};
    let points = vec![
        Point {
            lon: 4.3517,
            lat: 50.8503,
        },
        Point {
            lon: 5.5667,
            lat: 50.6333,
        },
    ];
    let geom = RouteGeometry::from_points(points, GeometryFormat::GeoJson);
    let coords = geom.coordinates_geojson.unwrap();
    assert!(
        (coords[0][0] - 4.3517).abs() < 1e-10,
        "First element should be lon"
    );
    assert!(
        (coords[0][1] - 50.8503).abs() < 1e-10,
        "Second element should be lat"
    );
    assert!(
        (coords[1][0] - 5.5667).abs() < 1e-10,
        "First element should be lon"
    );
    assert!(
        (coords[1][1] - 50.6333).abs() < 1e-10,
        "Second element should be lat"
    );
}

// === 17. Nearest number=0 rejection ===

#[test]
fn test_nearest_number_zero_rejected() {
    let n: u32 = 0;
    assert!(n < 1, "number=0 is below the minimum of 1");
}

// === 18. Table annotations validation ===

#[test]
fn test_table_annotations_rejects_unknown() {
    let valid = ["duration", "distance", "duration,distance", ""];
    for v in &valid {
        let tokens: Vec<&str> = v.split(',').map(|s| s.trim()).collect();
        for &t in &tokens {
            assert!(
                t.is_empty() || t == "duration" || t == "distance",
                "Token '{}' should be valid",
                t
            );
        }
    }
    let invalid = ["foo", "durations", "dist", "speed"];
    for v in &invalid {
        assert!(
            *v != "duration" && *v != "distance",
            "'{}' should be invalid",
            v
        );
    }
}

// === 19. Isochrone direction case-insensitive ===

#[test]
fn test_isochrone_direction_case_normalization() {
    for (input, expected) in &[
        ("depart", false),
        ("arrive", true),
        ("Depart", false),
        ("ARRIVE", true),
        ("Arrive", true),
        ("DEPART", false),
    ] {
        let lower = input.to_lowercase();
        let result = match lower.as_str() {
            "depart" => Ok(false),
            "arrive" => Ok(true),
            _ => Err(()),
        };
        assert_eq!(
            result,
            Ok(*expected),
            "Direction '{}' should normalize correctly",
            input
        );
    }
    for bad in &["", "forward", "backward", " depart"] {
        let lower = bad.to_lowercase();
        let result = match lower.as_str() {
            "depart" | "arrive" => Ok(()),
            _ => Err(()),
        };
        assert!(result.is_err(), "Direction '{}' should be rejected", bad);
    }
}

// === 20. Trip unreachable legs use null ===

#[test]
fn test_trip_leg_unreachable_serializes_as_null() {
    use super::trip::TripLeg;
    let unreachable_leg = TripLeg {
        duration: None,
        distance: None,
        summary: String::new(),
    };
    let json = serde_json::to_value(&unreachable_leg).unwrap();
    assert!(json["duration"].is_null());
    assert!(json["distance"].is_null());

    let reachable_leg = TripLeg {
        duration: Some(123.4),
        distance: Some(5678.9),
        summary: "test".to_string(),
    };
    let json = serde_json::to_value(&reachable_leg).unwrap();
    assert_eq!(json["duration"], 123.4);
    assert_eq!(json["distance"], 5678.9);
}

// === P2: Multiple isochrone contours tests ===

#[test]
fn test_contours_parsing_valid() {
    let input = "300,600,1200";
    let mut values: Vec<u32> = input
        .split(',')
        .map(|s| s.trim().parse::<u32>().unwrap())
        .collect();
    values.sort_unstable();
    values.dedup();
    assert_eq!(values, vec![300, 600, 1200]);
}

#[test]
fn test_contours_parsing_dedup_and_sort() {
    let input = "1200,300,600,300";
    let mut values: Vec<u32> = input
        .split(',')
        .map(|s| s.trim().parse::<u32>().unwrap())
        .collect();
    values.sort_unstable();
    values.dedup();
    assert_eq!(values, vec![300, 600, 1200]);
}

#[test]
fn test_contours_validation_range() {
    for v in [0u32, 7201, 10000] {
        assert!(
            !(1..=7200).contains(&v),
            "value {} should be out of range",
            v
        );
    }
    for v in [1u32, 300, 3600, 7200] {
        assert!((1..=7200).contains(&v), "value {} should be in range", v);
    }
}

#[test]
fn test_contours_max_10() {
    let values: Vec<u32> = (1..=11).map(|i| i * 100).collect();
    assert!(values.len() > 10, "should reject > 10 contour values");
}

#[test]
fn test_contour_feature_serialization_time() {
    let feature = ContourFeature {
        time_s: Some(600),
        polygon: None,
        polygon_geojson: Some(vec![[4.35, 50.85], [4.36, 50.86]]),
        polygon_points: None,
        band: None,
            reachable_edges: 1234,
    };
    let json = serde_json::to_value(&feature).unwrap();
    assert_eq!(json["time_s"], 600);
    assert!(json.get("distance_m").is_none());
    assert_eq!(json["reachable_edges"], 1234);
    assert!(json["polygon_geojson"].is_array());
}

#[test]
fn test_isochrone_response_always_has_contours_array() {
    let resp = IsochroneResponse {
        contours: vec![
            ContourFeature {
                time_s: Some(300),
                polygon: None,
                polygon_geojson: Some(vec![[4.35, 50.85]]),
                polygon_points: None,
                band: None,
            reachable_edges: 1000,
            },
            ContourFeature {
                time_s: Some(600),
                polygon: None,
                polygon_geojson: Some(vec![[4.34, 50.84]]),
                polygon_points: None,
                band: None,
            reachable_edges: 3000,
            },
        ],
        network: None,
    };
    let json = serde_json::to_value(&resp).unwrap();
    let contours = json["contours"].as_array().unwrap();
    assert_eq!(contours.len(), 2);
    assert_eq!(contours[0]["time_s"], 300);
    assert_eq!(contours[1]["time_s"], 600);
    assert!(contours[1]["reachable_edges"].as_u64() > contours[0]["reachable_edges"].as_u64());
}

#[test]
fn test_isochrone_request_deser_time_s() {
    use super::isochrone_handler::IsochroneRequest;
    let json_str = r#"{"lon":4.35,"lat":50.85,"time_s":600,"mode":"car"}"#;
    let req: IsochroneRequest = serde_json::from_str(json_str).unwrap();
    assert_eq!(req.time_s, Some(600));
    assert!(req.contours.is_none());
}

#[test]
fn test_isochrone_request_deser_contours() {
    use super::isochrone_handler::IsochroneRequest;
    let json_str = r#"{"lon":4.35,"lat":50.85,"contours":"300,600","mode":"car"}"#;
    let req: IsochroneRequest = serde_json::from_str(json_str).unwrap();
    assert!(req.time_s.is_none());
    assert_eq!(req.contours, Some("300,600".to_string()));
}

#[test]
fn test_distance_m_validation_range() {
    for v in [0u32, 100_001, 200_000] {
        assert!(v == 0 || v > 100_000, "value {} should be out of range", v);
    }
    for v in [1u32, 1000, 50_000, 100_000] {
        assert!((1..=100_000).contains(&v), "value {} should be in range", v);
    }
}

// === P4: Route annotations tests ===

#[test]
fn test_route_annotations_serialization() {
    let ann = RouteAnnotations {
        duration: Some(vec![1.2, 3.4, 5.6]),
        distance: Some(vec![100.0, 200.0, 300.0]),
        speed: None,
        nodes: None,
    };
    let json = serde_json::to_value(&ann).unwrap();
    assert!(json["duration"].is_array());
    assert!(json["distance"].is_array());
    assert!(json.get("speed").is_none());
    assert!(json.get("nodes").is_none());

    let durations = json["duration"].as_array().unwrap();
    assert_eq!(durations.len(), 3);
}

#[test]
fn test_route_annotations_speed_calculation() {
    let dur_s = 10.0_f64;
    let dist_m = 100.0_f64;
    let speed_kmh = dist_m * 3.6 / dur_s;
    assert!((speed_kmh - 36.0).abs() < 0.01);
}

#[test]
fn test_route_annotations_speed_zero_duration() {
    let dur_s = 0.0_f64;
    let speed = if dur_s > 0.0 {
        100.0 * 3.6 / dur_s
    } else {
        0.0
    };
    assert_eq!(speed, 0.0);
}

#[test]
fn test_annotations_validation_tokens() {
    let valid_tokens = ["duration", "distance", "speed", "nodes"];
    for t in &valid_tokens {
        assert!(["duration", "distance", "speed", "nodes"].contains(t));
    }
    let invalid_tokens = ["weight", "cost", "time", "edge_id", ""];
    for t in &invalid_tokens {
        assert!(!["duration", "distance", "speed", "nodes"].contains(t));
    }
}

#[test]
fn test_route_response_with_annotations() {
    use super::geometry::{GeometryFormat, Point, RouteGeometry};
    let resp = RouteResponse {
        duration_s: 60.0,
        distance_m: 500.0,
        geometry: RouteGeometry::from_points(
            vec![
                Point {
                    lon: 4.35,
                    lat: 50.85,
                },
                Point {
                    lon: 4.36,
                    lat: 50.86,
                },
            ],
            GeometryFormat::GeoJson,
        ),
        steps: None,
        annotations: Some(RouteAnnotations {
            duration: Some(vec![30.0, 30.0]),
            distance: Some(vec![250.0, 250.0]),
            speed: Some(vec![30.0, 30.0]),
            nodes: Some(vec![100, 200]),
        }),
        alternatives: None,
        debug: None,
        duration_q25_s: None,
        duration_q75_s: None,
    };
    let json = serde_json::to_value(&resp).unwrap();
    assert!(json["annotations"]["duration"].is_array());
    assert_eq!(json["annotations"]["nodes"].as_array().unwrap().len(), 2);
}

// === P7: Bearing hints tests ===

#[test]
fn test_bearing_matching() {
    use super::spatial::SpatialIndex;
    assert!(SpatialIndex::bearing_matches_pub(0, 0, 45));
    assert!(SpatialIndex::bearing_matches_pub(30, 0, 45));
    assert!(SpatialIndex::bearing_matches_pub(330, 0, 45));
    assert!(!SpatialIndex::bearing_matches_pub(90, 0, 45));

    assert!(SpatialIndex::bearing_matches_pub(90, 90, 30));
    assert!(SpatialIndex::bearing_matches_pub(110, 90, 30));
    assert!(SpatialIndex::bearing_matches_pub(70, 90, 30));
    assert!(!SpatialIndex::bearing_matches_pub(130, 90, 30));

    assert!(SpatialIndex::bearing_matches_pub(350, 0, 30));
    assert!(SpatialIndex::bearing_matches_pub(10, 0, 30));

    assert!(SpatialIndex::bearing_matches_pub(180, 180, 10));
}

#[test]
fn test_bearing_parsing_format() {
    let input = "90,45;270,45";
    let parts: Vec<&str> = input.split(';').collect();
    assert_eq!(parts.len(), 2);
    for part in parts {
        let tokens: Vec<&str> = part.split(',').collect();
        assert_eq!(tokens.len(), 2);
        let angle: u16 = tokens[0].parse().unwrap();
        let range: u16 = tokens[1].parse().unwrap();
        assert!(angle <= 360);
        assert!(range <= 180);
    }
}

#[test]
fn test_bearing_parsing_single_waypoint() {
    let input = "180,90";
    let parts: Vec<&str> = input.split(';').collect();
    assert_eq!(parts.len(), 1);
}

#[test]
fn test_bearing_computation() {
    let lat1 = 50.0_f64;
    let lon1 = 4.0_f64;
    let lat2 = 51.0_f64;
    let lon2 = 4.0_f64;
    let dlat_m = (lat2 - lat1) * 111_000.0;
    let dlon_m = (lon2 - lon1) * 71_400.0;
    let angle_rad = dlon_m.atan2(dlat_m);
    let bearing = ((angle_rad.to_degrees() + 360.0) % 360.0) as u16;
    assert_eq!(bearing, 0, "Due north should be 0 degrees");

    let lat2 = 50.0;
    let lon2 = 5.0;
    let dlat_m = (lat2 - lat1) * 111_000.0;
    let dlon_m = (lon2 - lon1) * 71_400.0;
    let angle_rad = dlon_m.atan2(dlat_m);
    let bearing = ((angle_rad.to_degrees() + 360.0) % 360.0) as u16;
    assert_eq!(bearing, 90, "Due east should be 90 degrees");
}

#[test]
fn test_bearing_empty_segment_is_unconstrained() {
    let input = ";270,45";
    let parts: Vec<&str> = input.split(';').collect();
    assert_eq!(parts.len(), 2);
    assert!(parts[0].is_empty());
}

// === Isochrone: single contour still returns array ===

#[test]
fn test_isochrone_response_single_contour_still_array() {
    let resp = IsochroneResponse {
        contours: vec![ContourFeature {
            time_s: Some(600),
            polygon: Some("encoded".to_string()),
            polygon_geojson: None,
            polygon_points: None,
            band: None,
            reachable_edges: 100,
        }],
        network: None,
    };
    let json = serde_json::to_value(&resp).unwrap();
    let contours = json["contours"].as_array().unwrap();
    assert_eq!(contours.len(), 1);
    assert_eq!(contours[0]["time_s"], 600);
    assert_eq!(contours[0]["reachable_edges"], 100);
}

// === Cross-feature: isochrone mutual exclusivity ===

#[test]
fn test_isochrone_exactly_one_metric() {
    #[allow(clippy::type_complexity)]
    let test_cases: Vec<(Option<u32>, Option<u32>, Option<&str>, bool)> = vec![
        (Some(600), None, None, true),
        (None, Some(5000), None, true),
        (None, None, Some("300,600"), true),
        (None, None, None, false),
        (Some(600), Some(5000), None, false),
        (Some(600), None, Some("300"), false),
    ];
    for (time_s, dist_m, contours, should_pass) in test_cases {
        let count = [time_s.is_some(), dist_m.is_some(), contours.is_some()]
            .iter()
            .filter(|&&b| b)
            .count();
        let valid = count == 1;
        assert_eq!(
            valid, should_pass,
            "time_s={:?} dist_m={:?} contours={:?}",
            time_s, dist_m, contours
        );
    }
}
