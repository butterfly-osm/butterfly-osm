//! Detailed snap debugging

#[test]
fn test_snap_detailed() {
    let lat: f64 = 48.73750;
    let lon: f64 = 2.34900;
    let grid_meters = 5.0;
    
    // Manual calculation
    let lat_scale = grid_meters / 111_111.0;
    let cos_lat = lat.to_radians().cos().abs();
    let lon_scale = grid_meters / (111_320.0 * cos_lat);
    
    println!("Input: lat={}, lon={}", lat, lon);
    println!("Grid size: {}m", grid_meters);
    println!("Lat scale: {:.10} degrees/cell", lat_scale);
    println!("Cos(lat): {:.10}", cos_lat);
    println!("Lon scale: {:.10} degrees/cell", lon_scale);
    
    // Test two points that should be in the same cell
    let offset = lon_scale * 0.3; // 30% of a cell
    let lon2 = lon + offset;
    
    println!("\nPoint 1: lon={:.10}", lon);
    println!("Point 2: lon={:.10} (offset={:.10})", lon2, offset);
    
    // Manual snapping calculation
    let lon1_cell = (lon / lon_scale).floor();
    let lon2_cell = (lon2 / lon_scale).floor();
    
    println!("\nCell calculation:");
    println!("lon1 / scale = {:.10} / {:.10} = {:.10}", lon, lon_scale, lon / lon_scale);
    println!("lon2 / scale = {:.10} / {:.10} = {:.10}", lon2, lon_scale, lon2 / lon_scale);
    println!("floor(lon1/scale) = {}", lon1_cell);
    println!("floor(lon2/scale) = {}", lon2_cell);
    
    let lon1_snapped = (lon1_cell + 0.5) * lon_scale;
    let lon2_snapped = (lon2_cell + 0.5) * lon_scale;
    
    println!("\nSnapped values:");
    println!("lon1_snapped = ({} + 0.5) * {} = {:.10}", lon1_cell, lon_scale, lon1_snapped);
    println!("lon2_snapped = ({} + 0.5) * {} = {:.10}", lon2_cell, lon_scale, lon2_snapped);
    
    let lon1_nano = (lon1_snapped * 1e9).round() as i64;
    let lon2_nano = (lon2_snapped * 1e9).round() as i64;
    
    println!("\nNanodegrees:");
    println!("lon1_nano = {}", lon1_nano);
    println!("lon2_nano = {}", lon2_nano);
    
    if lon1_nano == lon2_nano {
        println!("✓ Same cell!");
    } else {
        println!("✗ Different cells!");
    }
    
    assert_eq!(lon1_cell, lon2_cell, "Points should be in same cell");
}