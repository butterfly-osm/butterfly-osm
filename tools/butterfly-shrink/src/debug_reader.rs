//! Debug reader to test what elements we get

use osmpbf::{Element, ElementReader};
use std::path::Path;

pub fn debug_elements(input_path: &Path) -> Result<(), Box<dyn std::error::Error>> {
    let reader = ElementReader::from_path(input_path)?;
    
    let mut node_count = 0;
    let mut dense_node_count = 0;
    let mut way_count = 0;
    let mut relation_count = 0;
    
    reader.for_each(|element| {
        match element {
            Element::Node(_) => {
                node_count += 1;
                if node_count <= 5 {
                    println!("Found regular node #{}", node_count);
                }
            }
            Element::DenseNode(_) => {
                dense_node_count += 1;
                if dense_node_count <= 5 {
                    println!("Found dense node #{}", dense_node_count);
                }
            }
            Element::Way(_) => {
                way_count += 1;
                if way_count <= 5 {
                    println!("Found way #{}", way_count);
                }
            }
            Element::Relation(_) => {
                relation_count += 1;
                if relation_count <= 5 {
                    println!("Found relation #{}", relation_count);
                }
            }
        }
    })?;
    
    println!("Element counts:");
    println!("  Nodes: {}", node_count);
    println!("  DenseNodes: {}", dense_node_count);
    println!("  Ways: {}", way_count);
    println!("  Relations: {}", relation_count);
    
    Ok(())
}