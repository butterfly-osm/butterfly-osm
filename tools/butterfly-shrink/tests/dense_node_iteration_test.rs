//! Test DenseNode iteration methods

use osmpbf::{Element, ElementReader};
use std::path::PathBuf;

#[test]
fn test_dense_node_iteration() {
    let monaco_path = PathBuf::from("/tmp/monaco.pbf");
    if !monaco_path.exists() {
        println!("Monaco PBF not found, skipping test");
        return;
    }
    
    let reader = ElementReader::from_path(&monaco_path).unwrap();
    
    let mut first_dense_processed = false;
    
    reader.for_each(|element| {
        match element {
            Element::DenseNode(dense) => {
                if !first_dense_processed {
                    println!("Found DenseNode, testing iteration methods...");
                    
                    // Try different ways to access dense node data
                    println!("DenseNode methods available:");
                    
                    // Check if DenseNode has any accessible fields or methods
                    // This will help us understand the API
                    
                    first_dense_processed = true;
                }
            },
            _ => {}
        }
    }).unwrap();
}