//! Analyze PBF structure

use osmpbf::{Element, ElementReader};
use std::path::PathBuf;

#[test]
fn analyze_monaco_pbf() {
    let monaco_path = PathBuf::from("/tmp/monaco.pbf");
    if !monaco_path.exists() {
        println!("Monaco PBF not found, skipping test");
        return;
    }
    
    let reader = ElementReader::from_path(&monaco_path).unwrap();
    
    let mut node_count = 0;
    let mut dense_node_count = 0;
    let mut way_count = 0;
    let mut relation_count = 0;
    let mut dense_node_items = 0;
    
    reader.for_each(|element| {
        match element {
            Element::Node(_) => {
                node_count += 1;
            },
            Element::DenseNode(dense) => {
                dense_node_count += 1;
                // Dense nodes in osmpbf are not iterable directly
                // We'll just count the block for now
                dense_node_items += 1; // Placeholder
                if dense_node_count <= 3 {
                    println!("DenseNode block {} found", dense_node_count);
                }
            },
            Element::Way(_) => {
                way_count += 1;
            },
            Element::Relation(_) => {
                relation_count += 1;
            }
        }
    }).unwrap();
    
    println!("\nMonaco PBF analysis:");
    println!("  Regular nodes: {}", node_count);
    println!("  Dense node blocks: {}", dense_node_count);
    println!("  Dense node items: {}", dense_node_items);
    println!("  Ways: {}", way_count);
    println!("  Relations: {}", relation_count);
    println!("  Total nodes: {}", node_count + dense_node_items);
}