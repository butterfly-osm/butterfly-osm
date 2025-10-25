use crate::geo::haversine_distance;
use crate::parse::{OsmData, TurnRestriction};
use anyhow::{Context, Result};
use petgraph::graph::{EdgeIndex, Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use rstar::{primitives::GeomWithData, RTree};
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
struct SerializableGraph {
    nodes: Vec<i64>,
    edges: Vec<(usize, usize, f64)>,
    coords: HashMap<i64, (f64, f64)>,
    spatial_points: Vec<([f64; 2], i64)>,
    restrictions: Vec<TurnRestriction>,
    edge_to_way: Vec<(usize, i64)>, // (edge_index, way_id)
}

#[derive(Debug)]
pub struct RouteGraph {
    pub graph: Graph<i64, f64>,
    pub node_map: HashMap<i64, NodeIndex>,
    pub coords: HashMap<i64, (f64, f64)>,
    pub spatial_index: RTree<GeomWithData<[f64; 2], i64>>,
    pub edge_to_way: HashMap<EdgeIndex, i64>,
    // Key: (from_way_id, via_node_osm_id), Value: Set of restricted to_way_ids
    pub restrictions: HashMap<(i64, i64), HashSet<i64>>,
}

fn get_speed(highway_type: &str, maxspeed: Option<u32>) -> f64 {
    if let Some(speed) = maxspeed {
        return speed as f64;
    }

    match highway_type {
        "motorway" => 120.0,
        "trunk" => 100.0,
        "primary" => 80.0,
        "secondary" => 60.0,
        "tertiary" => 50.0,
        "residential" => 30.0,
        "service" => 20.0,
        _ => 50.0,
    }
}

impl RouteGraph {
    pub fn from_osm_data(data: OsmData) -> Self {
        let mut graph = Graph::new();
        let mut node_map = HashMap::new();
        let mut used_nodes = HashMap::new();

        // First pass: collect all nodes used in highway ways
        for way in &data.ways {
            for node_id in &way.nodes {
                if let Some(&coord) = data.nodes.get(node_id) {
                    used_nodes.insert(*node_id, coord);
                }
            }
        }

        // Add only used nodes to graph
        for (osm_id, _) in &used_nodes {
            let idx = graph.add_node(*osm_id);
            node_map.insert(*osm_id, idx);
        }

        let mut edge_count = 0;
        let mut edge_to_way = HashMap::new();

        // Add edges from ways and build edge-to-way mapping
        for way in &data.ways {
            let speed_kmh = get_speed(&way.highway, way.maxspeed);
            let speed_ms = speed_kmh * 1000.0 / 3600.0; // km/h to m/s

            for window in way.nodes.windows(2) {
                let (node_a, node_b) = (window[0], window[1]);

                if let (Some(&idx_a), Some(&idx_b), Some(&coord_a), Some(&coord_b)) = (
                    node_map.get(&node_a),
                    node_map.get(&node_b),
                    used_nodes.get(&node_a),
                    used_nodes.get(&node_b),
                ) {
                    let distance = haversine_distance(coord_a.0, coord_a.1, coord_b.0, coord_b.1);
                    let time_seconds = distance / speed_ms;

                    let edge_idx = graph.add_edge(idx_a, idx_b, time_seconds);
                    edge_to_way.insert(edge_idx, way.id);
                    edge_count += 1;

                    if !way.oneway {
                        let edge_idx_rev = graph.add_edge(idx_b, idx_a, time_seconds);
                        edge_to_way.insert(edge_idx_rev, way.id);
                        edge_count += 1;
                    }
                }
            }
        }

        // Build R-tree spatial index for fast nearest neighbor queries
        let points: Vec<GeomWithData<[f64; 2], i64>> = used_nodes
            .iter()
            .map(|(id, coord)| GeomWithData::new([coord.1, coord.0], *id)) // [lon, lat], osm_id
            .collect();

        let spatial_index = RTree::bulk_load(points);

        // Build turn restrictions index
        let mut restrictions: HashMap<(i64, i64), HashSet<i64>> = HashMap::new();
        for restriction in &data.restrictions {
            restrictions
                .entry((restriction.from_way, restriction.via_node))
                .or_insert_with(HashSet::new)
                .insert(restriction.to_way);
        }

        println!("Built graph: {} nodes, {} edges", graph.node_count(), edge_count);
        println!("Built R-tree spatial index with {} points", spatial_index.size());
        println!("Loaded {} turn restrictions", data.restrictions.len());

        RouteGraph {
            graph,
            node_map,
            coords: used_nodes,
            spatial_index,
            edge_to_way,
            restrictions,
        }
    }

    pub fn save<P: AsRef<Path>>(&self, path: P) -> Result<()> {
        let nodes: Vec<i64> = self.graph.node_weights().copied().collect();
        let edges: Vec<(usize, usize, f64)> = self.graph
            .edge_references()
            .map(|e| (e.source().index(), e.target().index(), *e.weight()))
            .collect();

        // Extract spatial points from coords for rebuilding R-tree on load
        let spatial_points: Vec<([f64; 2], i64)> = self
            .coords
            .iter()
            .map(|(id, coord)| ([coord.1, coord.0], *id)) // [lon, lat], osm_id
            .collect();

        // Extract edge-to-way mapping
        let edge_to_way: Vec<(usize, i64)> = self
            .edge_to_way
            .iter()
            .map(|(edge_idx, way_id)| (edge_idx.index(), *way_id))
            .collect();

        // Convert restrictions HashMap to Vec
        let restrictions: Vec<TurnRestriction> = self
            .restrictions
            .iter()
            .flat_map(|((from_way, via_node), to_ways)| {
                to_ways.iter().map(move |to_way| TurnRestriction {
                    restriction_type: "no_turn".to_string(), // We lose the specific type, but that's okay
                    from_way: *from_way,
                    via_node: *via_node,
                    to_way: *to_way,
                })
            })
            .collect();

        let serializable = SerializableGraph {
            nodes,
            edges,
            coords: self.coords.clone(),
            spatial_points,
            restrictions,
            edge_to_way,
        };

        let file = File::create(path).context("Failed to create graph file")?;
        let writer = BufWriter::new(file);
        bincode::serialize_into(writer, &serializable).context("Failed to serialize graph")?;
        Ok(())
    }

    pub fn load<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = File::open(path).context("Failed to open graph file")?;
        let reader = BufReader::new(file);
        let serializable: SerializableGraph = bincode::deserialize_from(reader)
            .context("Failed to deserialize graph")?;

        let mut graph = Graph::new();
        let mut node_map = HashMap::new();

        // Recreate nodes
        for (idx, osm_id) in serializable.nodes.iter().enumerate() {
            let node_idx = graph.add_node(*osm_id);
            assert_eq!(node_idx.index(), idx, "Node index mismatch during deserialization");
            node_map.insert(*osm_id, node_idx);
        }

        // Recreate edges
        for (from_idx, to_idx, weight) in serializable.edges {
            graph.add_edge(NodeIndex::new(from_idx), NodeIndex::new(to_idx), weight);
        }

        // Rebuild R-tree from spatial points
        let points: Vec<GeomWithData<[f64; 2], i64>> = serializable
            .spatial_points
            .iter()
            .map(|(coords, id)| GeomWithData::new(*coords, *id))
            .collect();

        let spatial_index = RTree::bulk_load(points);

        // Rebuild edge_to_way mapping
        let edge_to_way: HashMap<EdgeIndex, i64> = serializable
            .edge_to_way
            .iter()
            .map(|(idx, way_id)| (EdgeIndex::new(*idx), *way_id))
            .collect();

        // Rebuild restrictions index
        let mut restrictions: HashMap<(i64, i64), HashSet<i64>> = HashMap::new();
        for restriction in &serializable.restrictions {
            restrictions
                .entry((restriction.from_way, restriction.via_node))
                .or_insert_with(HashSet::new)
                .insert(restriction.to_way);
        }

        Ok(RouteGraph {
            graph,
            node_map,
            coords: serializable.coords,
            spatial_index,
            edge_to_way,
            restrictions,
        })
    }
}
