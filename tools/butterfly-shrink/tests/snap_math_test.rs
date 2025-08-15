//! Test grid snapping math

use butterfly_shrink::snap_coordinate;

#[test]
fn test_grid_math() {
    let lat: f64 = 48.73750;
    let lon: f64 = 2.34900;
    let grid_size = 5.0;
    
    // Calculate expected grid cell
    let lat_scale = grid_size / 111_111.0;
    let cos_lat = lat.to_radians().cos();
    let lon_scale = grid_size / (111_320.0 * cos_lat);
    
    println!("Grid size: {}m", grid_size);
    println!("Lat scale: {:.8} degrees", lat_scale);
    println!("Lon scale: {:.8} degrees", lon_scale);
    println!("Cos(lat): {:.6}", cos_lat);
    
    // Test multiple points in same cell
    let test_points = vec![
        (lat, lon),
        (lat + lat_scale * 0.1, lon + lon_scale * 0.1),  // Should be in same cell
        (lat + lat_scale * 0.4, lon + lon_scale * 0.4),  // Should be in same cell
        (lat + lat_scale * 0.6, lon + lon_scale * 0.6),  // Should be in next cell
        (lat + lat_scale * 1.0, lon + lon_scale * 1.0),  // Should be in next cell
    ];
    
    println!("\nSnapping test points:");
    let mut last_snap = (0i64, 0i64);
    for (i, (test_lat, test_lon)) in test_points.iter().enumerate() {
        let (lat_nano, lon_nano) = snap_coordinate(*test_lat, *test_lon, grid_size);
        let same_as_last = if i > 0 && (lat_nano, lon_nano) == last_snap {
            "SAME"
        } else {
            "DIFFERENT"
        };
        println!("  Point {}: ({:.8}, {:.8}) -> ({}, {}) {}",
                 i, test_lat, test_lon, lat_nano, lon_nano, same_as_last);
        last_snap = (lat_nano, lon_nano);
    }
    
    // Check that points within half a grid cell snap to same location
    let (snap1_lat, snap1_lon) = snap_coordinate(lat, lon, grid_size);
    let (snap2_lat, snap2_lon) = snap_coordinate(lat + lat_scale * 0.3, lon + lon_scale * 0.3, grid_size);
    
    assert_eq!((snap1_lat, snap1_lon), (snap2_lat, snap2_lon),
               "Points within same grid cell should snap to same location");
}