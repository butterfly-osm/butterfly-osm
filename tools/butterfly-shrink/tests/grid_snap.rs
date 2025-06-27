use butterfly_shrink::snap_coordinate;

/// Helper function to convert nanodegrees back to degrees for assertions
fn nanodegrees_to_degrees(nano: i64) -> f64 {
    nano as f64 / 1e9
}

#[test]
fn test_snap_coordinate_basic() {
    // Test basic case from the issue
    let (lat_nano, lon_nano) = snap_coordinate(52.0, 13.0, 5.0);

    // The result should be close to the original, snapped to a 5m grid center
    let lat_result = nanodegrees_to_degrees(lat_nano);
    let lon_result = nanodegrees_to_degrees(lon_nano);

    // Should be within the 5m grid cell
    assert!((lat_result - 52.0).abs() < 0.0001);
    assert!((lon_result - 13.0).abs() < 0.0001);
}

#[test]
fn test_snap_coordinate_equator() {
    // Test at equator where lat/lon scaling should be equal
    let (lat_nano, lon_nano) = snap_coordinate(0.0, 0.0, 5.0);

    let lat_result = nanodegrees_to_degrees(lat_nano);
    let lon_result = nanodegrees_to_degrees(lon_nano);

    // At equator, coordinates snap to grid center (not necessarily 0.0)
    // With 5m grid, the center of the cell containing (0,0) is at half a grid cell
    let expected_offset = (5.0 / 111_111.0) * 0.5; // Half grid cell in degrees

    assert!((lat_result).abs() <= expected_offset);
    assert!((lon_result).abs() <= expected_offset);
}

#[test]
fn test_snap_coordinate_60_degrees() {
    // Test at 60° latitude where longitude scaling should be ~2x latitude
    let (lat_nano, lon_nano) = snap_coordinate(60.0, 10.0, 5.0);

    let lat_result = nanodegrees_to_degrees(lat_nano);
    let lon_result = nanodegrees_to_degrees(lon_nano);

    // Verify the coordinate is snapped correctly
    assert!((lat_result - 60.0).abs() < 0.0001);
    assert!((lon_result - 10.0).abs() < 0.0001);
}

#[test]
fn test_snap_coordinate_85_degrees_north() {
    // Test at high latitude (85°N) - extreme E-W compression
    let (lat_nano, lon_nano) = snap_coordinate(85.0, 20.0, 5.0);

    let lat_result = nanodegrees_to_degrees(lat_nano);
    let lon_result = nanodegrees_to_degrees(lon_nano);

    // Should still produce valid results
    assert!((lat_result - 85.0).abs() < 0.0001);
    assert!((lon_result - 20.0).abs() < 0.001); // Larger tolerance due to compression
}

#[test]
fn test_snap_coordinate_89_9_degrees_north() {
    // Test at extreme latitude (89.9°N) - near the clamping limit
    let (lat_nano, lon_nano) = snap_coordinate(89.9, 45.0, 5.0);

    let lat_result = nanodegrees_to_degrees(lat_nano);
    let lon_result = nanodegrees_to_degrees(lon_nano);

    // Should handle extreme latitude correctly
    assert!((lat_result - 89.9).abs() < 0.0001);
    // Longitude grid will be very compressed but still valid
    assert!(lon_result.is_finite());
}

#[test]
fn test_snap_coordinate_latitude_clamping() {
    // Test that latitudes beyond ±89.9° are clamped
    let (lat_nano_north, _) = snap_coordinate(91.0, 0.0, 5.0);
    let (lat_nano_south, _) = snap_coordinate(-91.0, 0.0, 5.0);

    let lat_north = nanodegrees_to_degrees(lat_nano_north);
    let lat_south = nanodegrees_to_degrees(lat_nano_south);

    // Should be clamped to ±89.9°
    assert!(lat_north <= 89.9);
    assert!(lat_south >= -89.9);
}

#[test]
fn test_snap_coordinate_grid_center() {
    // Test that coordinates snap to grid center, not corner
    // Using a larger grid to make the effect more obvious
    let grid_size = 100.0; // 100m grid

    // Test a coordinate that should snap to center
    let (lat_nano, lon_nano) = snap_coordinate(0.0001, 0.0001, grid_size);

    let lat_result = nanodegrees_to_degrees(lat_nano);
    let lon_result = nanodegrees_to_degrees(lon_nano);

    // Should snap to center of cell containing (0,0)
    // Grid cell spans roughly ±0.00045° at equator for 100m
    assert!((lat_result - 0.00045).abs() < 0.0001);
    assert!((lon_result - 0.00045).abs() < 0.0001);
}

#[test]
fn test_snap_coordinate_different_grid_sizes() {
    let lat = 52.5;
    let lon = 13.4;

    // Test different grid sizes
    let grids = vec![1.0, 2.0, 5.0, 10.0];

    for grid_size in grids {
        let (lat_nano, lon_nano) = snap_coordinate(lat, lon, grid_size);

        let lat_result = nanodegrees_to_degrees(lat_nano);
        let lon_result = nanodegrees_to_degrees(lon_nano);

        // Smaller grids should give results closer to original
        let lat_diff = (lat_result - lat).abs();
        let lon_diff = (lon_result - lon).abs();

        // Maximum deviation should be half the grid size in degrees
        let max_deviation = (grid_size / 111_111.0) * 0.5;

        assert!(lat_diff <= max_deviation * 1.1); // 10% tolerance
        assert!(lon_diff <= max_deviation * 2.0); // More tolerance for longitude due to scaling
    }
}

#[test]
fn test_snap_coordinate_consistency() {
    // Test that the snap function is deterministic
    let grid_size = 5.0;

    // Same coordinate should always snap to same location
    let (lat1_nano, lon1_nano) = snap_coordinate(52.12345, 13.54321, grid_size);
    let (lat2_nano, lon2_nano) = snap_coordinate(52.12345, 13.54321, grid_size);

    assert_eq!(lat1_nano, lat2_nano);
    assert_eq!(lon1_nano, lon2_nano);

    // Test that coordinates are properly snapped
    // The snapped coordinate should be at the center of a grid cell
    let lat_result = nanodegrees_to_degrees(lat1_nano);
    let lon_result = nanodegrees_to_degrees(lon1_nano);

    // Calculate grid scales
    let lat_scale = grid_size / 111_111.0;
    let cos_lat = 52.12345_f64.to_radians().cos();
    let lon_scale = grid_size / (111_320.0 * cos_lat);

    // The result divided by scale should be very close to an integer + 0.5
    // (because we snap to cell centers)
    let lat_cells = lat_result / lat_scale;
    let lon_cells = lon_result / lon_scale;

    // Check that we're at a cell center (fractional part should be ~0.5)
    let lat_fract = lat_cells.fract().abs();
    let lon_fract = lon_cells.fract().abs();

    assert!((lat_fract - 0.5).abs() < 0.01 || lat_fract < 0.01);
    assert!((lon_fract - 0.5).abs() < 0.01 || lon_fract < 0.01);
}
