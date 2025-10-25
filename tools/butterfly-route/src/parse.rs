use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader, RelMemberType};
use std::collections::HashMap;
use std::path::Path;

#[derive(Debug, Clone)]
pub struct Way {
    pub id: i64,
    pub nodes: Vec<i64>,
    pub highway: String,
    pub maxspeed: Option<u32>,
    pub oneway: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct TurnRestriction {
    pub restriction_type: String,
    pub from_way: i64,
    pub via_node: i64,
    pub to_way: i64,
}

pub struct OsmData {
    pub nodes: HashMap<i64, (f64, f64)>,
    pub ways: Vec<Way>,
    pub restrictions: Vec<TurnRestriction>,
}

pub fn parse_pbf<P: AsRef<Path>>(path: P) -> Result<OsmData> {
    let reader = ElementReader::from_path(path)
        .context("Failed to open PBF file")?;

    let mut nodes = HashMap::new();
    let mut ways = Vec::new();
    let mut restrictions = Vec::new();

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
                        id: way.id(),
                        nodes: way.refs().collect(),
                        highway: highway_tag.1.to_string(),
                        maxspeed,
                        oneway,
                    });
                }
            }
            Element::Relation(relation) => {
                // Parse turn restrictions
                let is_restriction = relation.tags()
                    .any(|(k, v)| k == "type" && v == "restriction");

                if !is_restriction {
                    return;
                }

                // Get restriction type (e.g., "no_left_turn", "only_straight_on")
                let restriction_type = relation.tags()
                    .find(|(k, _)| *k == "restriction")
                    .map(|(_, v)| v.to_string());

                if restriction_type.is_none() {
                    return;
                }

                // Extract from/via/to members
                let mut from_way: Option<i64> = None;
                let mut via_node: Option<i64> = None;
                let mut to_way: Option<i64> = None;

                for member in relation.members() {
                    if let Ok(role) = member.role() {
                        match role {
                            "from" if member.member_type == RelMemberType::Way => {
                                from_way = Some(member.member_id);
                            }
                            "via" if member.member_type == RelMemberType::Node => {
                                via_node = Some(member.member_id);
                            }
                            "to" if member.member_type == RelMemberType::Way => {
                                to_way = Some(member.member_id);
                            }
                            _ => {}
                        }
                    }
                }

                // Only store if we have all required members (from way, via node, to way)
                if let (Some(from), Some(via), Some(to)) = (from_way, via_node, to_way) {
                    restrictions.push(TurnRestriction {
                        restriction_type: restriction_type.unwrap(),
                        from_way: from,
                        via_node: via,
                        to_way: to,
                    });
                }
            }
            _ => {}
        }
    }).context("Failed to parse PBF file")?;

    println!("Parsed {} nodes", nodes.len());
    println!("Parsed {} highway ways", ways.len());
    println!("Parsed {} turn restrictions", restrictions.len());

    Ok(OsmData { nodes, ways, restrictions })
}
