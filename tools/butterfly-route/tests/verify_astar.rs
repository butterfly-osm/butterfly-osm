use butterfly_route::graph::RouteGraph;
use butterfly_route::parse::parse_pbf;
use butterfly_route::route::find_route;

#[test]
fn test_monaco_routes_consistent() {
    // Build graph from Monaco
    let data = parse_pbf("../../data/monaco-latest.osm.pbf").expect("Failed to parse PBF");
    let graph = RouteGraph::from_osm_data(data);

    // Test multiple routes
    let test_cases = vec![
        // (from_lat, from_lon, to_lat, to_lon, expected_approx_time_min)
        (43.7384, 7.4246, 43.7403, 7.4268, 0.4), // Short route
        (43.73, 7.42, 43.74, 7.43, 2.2),         // Medium route
    ];

    for (from_lat, from_lon, to_lat, to_lon, expected_time) in test_cases {
        let result = find_route(&graph, (from_lat, from_lon), (to_lat, to_lon))
            .expect("Route should be found");

        let time_minutes = result.time_seconds / 60.0;

        // Verify time is within 10% of expected
        let tolerance = expected_time * 0.1;
        assert!(
            (time_minutes - expected_time).abs() < tolerance,
            "Route time {:.1}min doesn't match expected {:.1}min",
            time_minutes,
            expected_time
        );

        // Verify distance is reasonable (not zero, not absurdly large)
        assert!(result.distance_meters > 0.0, "Distance should be positive");
        assert!(result.distance_meters < 100_000.0, "Distance too large for Monaco");

        println!(
            "✓ Route ({:.4},{:.4}) → ({:.4},{:.4}): {:.1}min, {:.0}m",
            from_lat, from_lon, to_lat, to_lon, time_minutes, result.distance_meters
        );
    }
}
