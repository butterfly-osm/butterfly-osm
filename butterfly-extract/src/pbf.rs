//! PBF reading and parsing for OSM data
//!
//! Provides streaming PBF parser with routing-relevant filtering

use osmpbf::BlobReader;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufReader;
use std::path::Path;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum PbfError {
    #[error("IO error: {0}")]
    Io(#[from] std::io::Error),
    #[error("PBF decode error: {0}")]
    Decode(String),
}

/// OSM primitive types for routing
#[derive(Debug, Clone, PartialEq)]
pub enum OsmPrimitive {
    Node {
        id: i64,
        lat: f64,
        lon: f64,
        tags: HashMap<String, String>,
    },
    Way {
        id: i64,
        nodes: Vec<i64>,
        tags: HashMap<String, String>,
    },
    Relation {
        id: i64,
        members: Vec<RelationMember>,
        tags: HashMap<String, String>,
    },
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct RelationMember {
    pub id: i64,
    pub role: String,
    pub member_type: MemberType,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MemberType {
    Node,
    Way,
    Relation,
}

/// Streaming PBF reader with routing-specific filtering
pub struct PbfReader {
    reader: BlobReader<BufReader<File>>,
}

impl PbfReader {
    /// Create a new PBF reader from file path
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self, PbfError> {
        let file = File::open(path)?;
        let buf_reader = BufReader::new(file);
        let reader = BlobReader::new(buf_reader);

        Ok(Self { reader })
    }

    /// Stream OSM primitives with routing-relevant filtering
    pub fn stream_routing_data<F>(&mut self, mut callback: F) -> Result<(), PbfError>
    where
        F: FnMut(OsmPrimitive) -> bool, // return false to stop streaming
    {
        for blob in &mut self.reader {
            let blob =
                blob.map_err(|e| PbfError::Decode(format!("Failed to read blob: {:?}", e)))?;

            if let Ok(primitive_block) = blob.to_primitiveblock() {
                for group in primitive_block.groups() {
                    // Process nodes
                    for node in group.nodes() {
                        let tags: HashMap<String, String> = node
                            .tags()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();

                        // Only include nodes with routing-relevant tags or referenced by ways
                        if has_routing_tags(&tags) {
                            let primitive = OsmPrimitive::Node {
                                id: node.id(),
                                lat: node.lat(),
                                lon: node.lon(),
                                tags,
                            };

                            if !callback(primitive) {
                                return Ok(());
                            }
                        }
                    }

                    // Process dense nodes
                    for node in group.dense_nodes() {
                        let tags: HashMap<String, String> = node
                            .tags()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();

                        if has_routing_tags(&tags) {
                            let primitive = OsmPrimitive::Node {
                                id: node.id(),
                                lat: node.lat(),
                                lon: node.lon(),
                                tags,
                            };

                            if !callback(primitive) {
                                return Ok(());
                            }
                        }
                    }

                    // Process ways
                    for way in group.ways() {
                        let tags: HashMap<String, String> = way
                            .tags()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();

                        // Include ways with highway tags or routing-relevant tags
                        if has_routing_tags(&tags) || tags.contains_key("highway") {
                            let primitive = OsmPrimitive::Way {
                                id: way.id(),
                                nodes: way.refs().collect(),
                                tags,
                            };

                            if !callback(primitive) {
                                return Ok(());
                            }
                        }
                    }

                    // Process relations
                    for relation in group.relations() {
                        let tags: HashMap<String, String> = relation
                            .tags()
                            .map(|(k, v)| (k.to_string(), v.to_string()))
                            .collect();

                        // Include relations with routing-relevant tags
                        if has_routing_tags(&tags) || is_routing_relation(&tags) {
                            let members: Vec<RelationMember> = relation
                                .members()
                                .map(|m| RelationMember {
                                    id: m.member_id,
                                    role: m.role().unwrap_or("").to_string(),
                                    member_type: match m.member_type {
                                        osmpbf::RelMemberType::Node => MemberType::Node,
                                        osmpbf::RelMemberType::Way => MemberType::Way,
                                        osmpbf::RelMemberType::Relation => MemberType::Relation,
                                    },
                                })
                                .collect();

                            let primitive = OsmPrimitive::Relation {
                                id: relation.id(),
                                members,
                                tags,
                            };

                            if !callback(primitive) {
                                return Ok(());
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }

    /// Count routing-relevant primitives in the file
    pub fn count_routing_primitives(&mut self) -> Result<(usize, usize, usize), PbfError> {
        let mut nodes = 0;
        let mut ways = 0;
        let mut relations = 0;

        self.stream_routing_data(|primitive| {
            match primitive {
                OsmPrimitive::Node { .. } => nodes += 1,
                OsmPrimitive::Way { .. } => ways += 1,
                OsmPrimitive::Relation { .. } => relations += 1,
            }
            true // continue counting
        })?;

        Ok((nodes, ways, relations))
    }
}

/// Check if tags contain routing-relevant information
fn has_routing_tags(tags: &HashMap<String, String>) -> bool {
    // Highway tags
    if tags.contains_key("highway") {
        return true;
    }

    // Routing-specific tags
    for key in &[
        "access",
        "vehicle",
        "motor_vehicle",
        "bicycle",
        "foot",
        "pedestrian",
        "barrier",
        "maxspeed",
        "oneway",
        "junction",
        "cycleway",
        "footway",
        "sidewalk",
        "lanes",
        "turn:lanes",
        "surface",
        "tracktype",
        "smoothness",
        "restriction",
        "except",
        "toll",
        "ferry",
        "bridge",
        "tunnel",
        "layer",
        "car",
        "motorcycle",
        "hgv",
        "bus",
        "taxi",
        "emergency",
        "delivery",
        "service",
        "psv",
        "goods",
        "agricultural",
        "forestry",
        "destination",
        "weight",
        "maxweight",
        "width",
        "maxwidth",
        "height",
        "maxheight",
        "length",
        "maxlength",
        "axleload",
        "maxaxleload",
    ] {
        if tags.contains_key(*key) {
            return true;
        }
    }

    false
}

/// Check if relation is routing-relevant
fn is_routing_relation(tags: &HashMap<String, String>) -> bool {
    if let Some(relation_type) = tags.get("type") {
        match relation_type.as_str() {
            "route" => true,
            "restriction" => true,
            "multipolygon" => {
                // Only include multipolygons with routing-relevant tags
                has_routing_tags(tags)
            }
            _ => false,
        }
    } else {
        false
    }
}
