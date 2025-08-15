//! Debug grid snapping

use butterfly_shrink::snap_coordinate;

#[test]
fn debug_snapping() {
    let coords = vec![
        (48.73750, 2.34900),    
        (48.737502, 2.349002),  
        (48.73755, 2.34905),    
        (48.74000, 2.35000),    
    ];
    
    println!("\nSnapping with 5m grid:");
    for (lat, lon) in &coords {
        let (lat_nano, lon_nano) = snap_coordinate(*lat, *lon, 5.0);
        println!("({:.6}, {:.6}) -> ({}, {})", lat, lon, lat_nano, lon_nano);
    }
    
    // Try with an even closer pair
    println!("\nTesting very close nodes (within 1m):");
    let lat1 = 48.73750;
    let lon1 = 2.34900;
    let lat2 = lat1 + 0.000001;  // ~0.1m difference
    let lon2 = lon1 + 0.000001;  // ~0.1m difference
    
    let (lat1_nano, lon1_nano) = snap_coordinate(lat1, lon1, 5.0);
    let (lat2_nano, lon2_nano) = snap_coordinate(lat2, lon2, 5.0);
    
    println!("Node 1: ({:.8}, {:.8}) -> ({}, {})", lat1, lon1, lat1_nano, lon1_nano);
    println!("Node 2: ({:.8}, {:.8}) -> ({}, {})", lat2, lon2, lat2_nano, lon2_nano);
    
    if lat1_nano == lat2_nano && lon1_nano == lon2_nano {
        println!("✓ Nodes deduplicated!");
    } else {
        println!("✗ Nodes NOT deduplicated");
    }
}