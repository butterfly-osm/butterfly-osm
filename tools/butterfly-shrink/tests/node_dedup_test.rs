//! Test node deduplication

use butterfly_shrink::snap_coordinate;
use std::collections::HashMap;

#[test]
fn test_node_deduplication() {
    // Test that nearby nodes are deduplicated
    // At 48.7 degrees latitude, 5m is approximately:
    // Latitude: 5m / 111111m = 0.000045 degrees
    // Longitude: 5m / (111320m * cos(48.7)) = 0.000068 degrees
    let nodes = vec![
        (1, 48.73750, 2.34900),    // Base node
        (2, 48.737502, 2.349002),  // Very close (< 5m), should be deduplicated  
        (3, 48.73755, 2.34905),    // ~50m away, should NOT be deduplicated
        (4, 48.74000, 2.35000),    // Far away, should NOT be deduplicated
    ];
    
    let mut grid_cells = HashMap::new();
    let mut node_mappings = HashMap::new();
    let grid_size = 5.0; // 5 meter grid
    
    for (id, lat, lon) in nodes {
        let (lat_nano, lon_nano) = snap_coordinate(lat, lon, grid_size);
        let grid_key = (lat_nano, lon_nano);
        
        let representative_id = if let Some(&rep_id) = grid_cells.get(&grid_key) {
            rep_id
        } else {
            grid_cells.insert(grid_key, id);
            id
        };
        
        node_mappings.insert(id, representative_id);
    }
    
    println!("Grid cells used: {}", grid_cells.len());
    println!("Node mappings: {:?}", node_mappings);
    
    // Check that some deduplication occurred
    assert!(grid_cells.len() < 4, "Should have deduplicated some nodes. Got {} cells", grid_cells.len());
    
    // Nodes 1 and 2 should map to the same representative
    let rep1 = node_mappings[&1];
    let rep2 = node_mappings[&2];
    assert_eq!(rep1, rep2, "Nodes 1 and 2 should have same representative");
    
    // Node 4 should be different
    let rep4 = node_mappings[&4];
    assert_ne!(rep1, rep4, "Node 4 should have different representative");
}