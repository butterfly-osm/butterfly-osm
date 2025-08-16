use std::collections::HashMap;

pub fn debug_ways(input_path: &std::path::Path) -> butterfly_common::Result<()> {
    use osmpbf::{Element, ElementReader};
    
    let mut highway_counts: HashMap<String, usize> = HashMap::new();
    let mut total_ways = 0;
    let mut ways_with_highway = 0;
    
    let reader = ElementReader::from_path(input_path)
        .map_err(|e| butterfly_common::Error::InvalidInput(format!("Failed to open: {}", e)))?;
    
    reader.for_each(|element| {
        if let Element::Way(way) = element {
            total_ways += 1;
            
            for (k, v) in way.tags() {
                if k == "highway" {
                    ways_with_highway += 1;
                    *highway_counts.entry(v.to_string()).or_insert(0) += 1;
                    break;
                }
            }
        }
    }).map_err(|e| butterfly_common::Error::InvalidInput(format!("Failed to read: {}", e)))?;
    
    println!("\n=== Way Analysis ===");
    println!("Total ways: {}", total_ways);
    println!("Ways with highway tag: {}", ways_with_highway);
    println!("\nHighway tag distribution:");
    
    let mut sorted: Vec<_> = highway_counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    
    for (tag, count) in sorted.iter().take(30) {
        println!("  {:20} {:8}", tag, count);
    }
    
    // Check which would be kept by default car preset
    let car_tags = vec![
        "motorway", "motorway_link", "trunk", "trunk_link",
        "primary", "primary_link", "secondary", "secondary_link",
        "tertiary", "tertiary_link", "unclassified", "residential",
        "living_street", "service", "road", "escape"
    ];
    
    let kept: usize = sorted.iter()
        .filter(|(tag, _)| car_tags.contains(&tag.as_str()))
        .map(|(_, count)| count)
        .sum();
    
    println!("\nCar preset would keep: {} ways", kept);
    
    Ok(())
}