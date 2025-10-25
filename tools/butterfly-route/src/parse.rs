use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Way {
    pub nodes: Vec<i64>,
    pub highway: String,
    pub maxspeed: Option<u32>,
    pub oneway: bool,
}

pub struct OsmData {
    pub nodes: HashMap<i64, (f64, f64)>,
    pub ways: Vec<Way>,
}

pub fn parse_pbf<P: AsRef<Path>>(path: P) -> Result<OsmData> {
    let reader = ElementReader::from_path(path)
        .context("Failed to open PBF file")?;

    let mut nodes = HashMap::new();
    let mut ways = Vec::new();

    reader.for_each(|element| {
        match element {
            Element::Node(node) => {
                nodes.insert(node.id(), (node.lat(), node.lon()));
            }
            Element::DenseNode(node) => {
                nodes.insert(node.id(), (node.lat(), node.lon()));
            }
            Element::Way(way) => {
                if let Some(highway_tag) = way.tags().find(|t| t.0 == "highway") {
                    let maxspeed = way.tags()
                        .find(|t| t.0 == "maxspeed")
                        .and_then(|t| t.1.parse::<u32>().ok());

                    let oneway = way.tags()
                        .find(|t| t.0 == "oneway")
                        .map(|t| t.1 == "yes")
                        .unwrap_or(false);

                    ways.push(Way {
                        nodes: way.refs().collect(),
                        highway: highway_tag.1.to_string(),
                        maxspeed,
                        oneway,
                    });
                }
            }
            _ => {}
        }
    }).context("Failed to parse PBF file")?;

    println!("Parsed {} nodes", nodes.len());
    println!("Parsed {} highway ways", ways.len());

    Ok(OsmData { nodes, ways })
}
