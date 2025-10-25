use crate::geo::haversine_distance;
use crate::parse::OsmData;
use anyhow::{Context, Result};
use petgraph::graph::{Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use rstar::{primitives::GeomWithData, RTree};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fs::File;
use std::io::{BufReader, BufWriter};
use std::path::Path;

#[derive(Debug, Serialize, Deserialize)]
struct SerializableGraph {
    nodes: Vec<i64>,
    edges: Vec<(usize, usize, f64)>,
    coords: HashMap<i64, (f64, f64)>,
    spatial_points: Vec<([f64; 2], i64)>,
}

#[derive(Debug)]
pub struct RouteGraph {
    pub graph: Graph<i64, f64>,
    pub node_map: HashMap<i64, NodeIndex>,
    pub coords: HashMap<i64, (f64, f64)>,
    pub spatial_index: RTree<GeomWithData<[f64; 2], i64>>,
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

        // Add edges from ways
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

                    graph.add_edge(idx_a, idx_b, time_seconds);
                    edge_count += 1;

                    if !way.oneway {
                        graph.add_edge(idx_b, idx_a, time_seconds);
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

        println!("Built graph: {} nodes, {} edges", graph.node_count(), edge_count);
        println!("Built R-tree spatial index with {} points", spatial_index.size());

        RouteGraph {
            graph,
            node_map,
            coords: used_nodes,
            spatial_index,
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

        let serializable = SerializableGraph {
            nodes,
            edges,
            coords: self.coords.clone(),
            spatial_points,
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

        Ok(RouteGraph {
            graph,
            node_map,
            coords: serializable.coords,
            spatial_index,
        })
    }
}
