use crate::ch::CHGraph;
use crate::graph::RouteGraph;
use petgraph::graph::{Graph, NodeIndex};
use rstar::RTree;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};

/// Highway road types (OSM tags) to include in L1 layer
pub const HIGHWAY_TYPES: &[&str] = &[
    "motorway",
    "motorway_link",
    "trunk",
    "trunk_link",
    "primary",
    "primary_link",
];

/// Point type for highway entry points spatial index
#[derive(Debug, Clone)]
pub struct HighwayEntryPoint {
    pub osm_id: i64,
    pub position: [f64; 2], // [lon, lat]
}

impl rstar::RTreeObject for HighwayEntryPoint {
    type Envelope = rstar::AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        rstar::AABB::from_point(self.position)
    }
}

impl rstar::PointDistance for HighwayEntryPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.position[0] - point[0];
        let dy = self.position[1] - point[1];
        dx * dx + dy * dy
    }
}

/// L1 Highway Network with Contraction Hierarchies
#[derive(Debug, Serialize, Deserialize)]
pub struct HighwayNetwork {
    /// CH-preprocessed highway graph
    pub ch_graph: CHGraph,

    /// Boundary nodes from L0 tiles (OSM ID -> TileId)
    pub boundary_nodes: HashMap<i64, (u16, u16)>,

    /// Spatial index of highway entry points (not serialized)
    #[serde(skip)]
    pub entry_index: Option<RTree<HighwayEntryPoint>>,
}

impl HighwayNetwork {
    /// Extract highway subgraph from full road graph
    pub fn from_road_graph(
        graph: &RouteGraph,
        boundary_nodes: HashMap<i64, (u16, u16)>,
    ) -> Result<Self, String> {
        println!("Extracting highway network from graph with {} nodes", graph.graph.node_count());
        println!("Including {} boundary nodes from L0 tiles", boundary_nodes.len());

        // Create filtered graph with only highway nodes
        let mut highway_graph = Graph::new();
        let mut highway_node_map: HashMap<i64, NodeIndex> = HashMap::new();
        let mut highway_coords: HashMap<i64, (f64, f64)> = HashMap::new();

        // Collect highway nodes and boundary nodes
        let mut highway_nodes = HashSet::new();

        // Add all boundary nodes (needed for L0 → L1 transitions)
        for &osm_id in boundary_nodes.keys() {
            highway_nodes.insert(osm_id);
        }

        // Add nodes that have highway edges
        // We'll identify these by checking edge types in the way_types map
        // For now, we include nodes based on their connectivity to other nodes
        // TODO: Properly filter based on OSM way tags (need to store way_types in RoadGraph)

        // For initial implementation, we'll use a simple heuristic:
        // Include nodes that are boundary nodes OR have high betweenness
        // (as a proxy for highway nodes)
        // This is a TEMPORARY solution - we need proper OSM tag filtering

        // Add all nodes first (we'll filter more intelligently later with OSM tags)
        for (&osm_id, &coords) in &graph.coords {
            let node_idx = highway_graph.add_node(osm_id);
            highway_node_map.insert(osm_id, node_idx);
            highway_coords.insert(osm_id, coords);
        }

        // Add all edges (we'll filter based on tags later)
        for edge in graph.graph.edge_references() {
            let source = edge.source();
            let target = edge.target();
            let weight = *edge.weight();

            if let (Some(&source_osm), Some(&target_osm)) = (
                graph.graph.node_weight(source),
                graph.graph.node_weight(target),
            ) {
                if let (Some(&source_idx), Some(&target_idx)) = (
                    highway_node_map.get(&source_osm),
                    highway_node_map.get(&target_osm),
                ) {
                    highway_graph.add_edge(source_idx, target_idx, weight);
                }
            }
        }

        println!(
            "Highway subgraph: {} nodes, {} edges",
            highway_graph.node_count(),
            highway_graph.edge_count()
        );

        // TODO: Implement proper filtering based on OSM way tags
        // For now, we're using the full graph as a placeholder

        // Convert to RouteGraph format for CH preprocessing
        use rstar::{primitives::GeomWithData, RTree as RSRTree};

        let spatial_points: Vec<GeomWithData<[f64; 2], i64>> = highway_coords
            .iter()
            .map(|(id, coord)| GeomWithData::new([coord.1, coord.0], *id))
            .collect();
        let spatial_index = RSRTree::bulk_load(spatial_points);

        let highway_route_graph = RouteGraph {
            graph: highway_graph,
            node_map: highway_node_map.clone(),
            coords: highway_coords.clone(),
            spatial_index,
            edge_to_way: HashMap::new(), // Not needed for highway network
            restrictions: HashMap::new(), // Turn restrictions handled in L0
            raw_restrictions: Vec::new(),
        };

        // Run CH preprocessing
        println!("Running CH preprocessing on highway network...");
        let ch_graph = CHGraph::from_route_graph(&highway_route_graph)?;

        Ok(Self {
            ch_graph,
            boundary_nodes,
            entry_index: None,
        })
    }

    /// Build spatial index for highway entry points
    pub fn build_entry_index(&mut self) {
        let points: Vec<HighwayEntryPoint> = self
            .ch_graph
            .coords
            .iter()
            .map(|(&osm_id, &(lat, lon))| HighwayEntryPoint {
                osm_id,
                position: [lon, lat],
            })
            .collect();

        self.entry_index = Some(RTree::bulk_load(points));
        println!("Built spatial index for {} highway entry points", points.len());
    }

    /// Find nearest highway entry point to given coordinates
    pub fn nearest_entry_point(&self, lat: f64, lon: f64) -> Option<i64> {
        if let Some(ref index) = self.entry_index {
            index
                .nearest_neighbor(&[lon, lat])
                .map(|point| point.osm_id)
        } else {
            // Fallback: linear search
            self.ch_graph
                .coords
                .iter()
                .min_by(|(_, (lat1, lon1)), (_, (lat2, lon2))| {
                    let dist1 = (lat - lat1).powi(2) + (lon - lon1).powi(2);
                    let dist2 = (lat - lat2).powi(2) + (lon - lon2).powi(2);
                    dist1.partial_cmp(&dist2).unwrap()
                })
                .map(|(&osm_id, _)| osm_id)
        }
    }

    /// Save highway network to file
    pub fn save(&self, path: &str) -> Result<(), String> {
        use std::fs::File;
        use std::io::BufWriter;

        let file = File::create(path)
            .map_err(|e| format!("Failed to create file: {}", e))?;
        let writer = BufWriter::new(file);

        bincode::serialize_into(writer, self)
            .map_err(|e| format!("Failed to serialize highway network: {}", e))?;

        println!("Highway network saved to {}", path);
        Ok(())
    }

    /// Load highway network from file
    pub fn load(path: &str) -> Result<Self, String> {
        use std::fs::File;
        use std::io::BufReader;

        let file = File::open(path)
            .map_err(|e| format!("Failed to open file: {}", e))?;
        let reader = BufReader::new(file);

        let mut network: Self = bincode::deserialize_from(reader)
            .map_err(|e| format!("Failed to deserialize highway network: {}", e))?;

        // Rebuild spatial index
        network.build_entry_index();

        println!("Highway network loaded from {}", path);
        Ok(network)
    }
}

/// Extract highway nodes based on OSM tags (to be implemented with proper tag storage)
pub fn filter_highway_nodes(
    graph: &RouteGraph,
    way_types: &HashMap<i64, String>,
) -> HashSet<i64> {
    let mut highway_nodes = HashSet::new();

    // For each way, check if it's a highway type
    for (way_id, way_type) in way_types {
        if HIGHWAY_TYPES.contains(&way_type.as_str()) {
            // Add all nodes in this way to highway_nodes
            // TODO: Need to store way -> nodes mapping in RouteGraph
        }
    }

    highway_nodes
}
