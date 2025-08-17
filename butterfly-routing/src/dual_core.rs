//! Dual core graph construction (Time Graph + Nav Graph) with XXH3 consistency

use crate::profiles::{EdgeId, TransportProfile};
use butterfly_geometry::Point2D;
use butterfly_geometry::{FullFidelityGeometry, NavigationGeometry, SnapSkeleton};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use xxhash_rust::xxh3::xxh3_64;

/// Node identifier in the dual core system
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct NodeId(pub u64);

impl NodeId {
    pub fn new(id: u64) -> Self {
        Self(id)
    }
}

/// Edge weight for time-only routing
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TimeWeight {
    pub time_seconds: u16,    // Quantized time weight
    pub distance_meters: u16, // Distance for fallback calculations
}

impl TimeWeight {
    pub fn new(time_seconds: f64, distance_meters: f64) -> Self {
        Self {
            time_seconds: (time_seconds.round() as u16).min(u16::MAX),
            distance_meters: (distance_meters.round() as u16).min(u16::MAX),
        }
    }

    pub fn time_seconds_f64(&self) -> f64 {
        self.time_seconds as f64
    }

    pub fn distance_meters_f64(&self) -> f64 {
        self.distance_meters as f64
    }
}

/// Time-only graph edge (no geometry)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeEdge {
    pub edge_id: EdgeId,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub weights: HashMap<TransportProfile, TimeWeight>,
    pub turn_restrictions: Vec<TurnRestriction>,
}

impl TimeEdge {
    pub fn new(edge_id: EdgeId, from_node: NodeId, to_node: NodeId) -> Self {
        Self {
            edge_id,
            from_node,
            to_node,
            weights: HashMap::new(),
            turn_restrictions: Vec::new(),
        }
    }

    pub fn add_weight(&mut self, profile: TransportProfile, weight: TimeWeight) {
        self.weights.insert(profile, weight);
    }

    pub fn get_weight(&self, profile: &TransportProfile) -> Option<&TimeWeight> {
        self.weights.get(profile)
    }
}

/// Navigation graph edge (with geometry)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NavEdge {
    pub edge_id: EdgeId,
    pub from_node: NodeId,
    pub to_node: NodeId,
    pub weights: HashMap<TransportProfile, TimeWeight>,
    pub turn_restrictions: Vec<TurnRestriction>,
    pub snap_skeleton: SnapSkeleton,
    pub nav_geometry: NavigationGeometry,
    pub full_geometry: Option<FullFidelityGeometry>,
}

impl NavEdge {
    pub fn new(
        edge_id: EdgeId,
        from_node: NodeId,
        to_node: NodeId,
        snap_skeleton: SnapSkeleton,
        nav_geometry: NavigationGeometry,
        full_geometry: Option<FullFidelityGeometry>,
    ) -> Self {
        Self {
            edge_id,
            from_node,
            to_node,
            weights: HashMap::new(),
            turn_restrictions: Vec::new(),
            snap_skeleton,
            nav_geometry,
            full_geometry,
        }
    }

    pub fn add_weight(&mut self, profile: TransportProfile, weight: TimeWeight) {
        self.weights.insert(profile, weight);
    }

    pub fn get_weight(&self, profile: &TransportProfile) -> Option<&TimeWeight> {
        self.weights.get(profile)
    }

    /// Get geometry for a specific pass
    pub fn get_geometry(&self, pass: GeometryPass) -> Vec<Point2D> {
        match pass {
            GeometryPass::Snap => self.snap_skeleton.points.clone(),
            GeometryPass::Navigation => self.nav_geometry.simplified_points.clone(),
            GeometryPass::FullFidelity => self
                .full_geometry
                .as_ref()
                .map(|fg| fg.to_points())
                .unwrap_or_else(|| self.nav_geometry.simplified_points.clone()),
        }
    }
}

/// Turn restriction for routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnRestriction {
    pub from_edge: EdgeId,
    pub via_node: NodeId,
    pub to_edge: EdgeId,
    pub restriction_type: RestrictionType,
    pub profiles: Vec<TransportProfile>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum RestrictionType {
    NoTurn,   // no_left_turn, no_right_turn, etc.
    OnlyTurn, // only_left_turn, only_right_turn, etc.
    NoUturn,  // no_u_turn
}

/// Geometry pass selector
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum GeometryPass {
    Snap,         // Pass A - snap skeleton
    Navigation,   // Pass B - navigation geometry
    FullFidelity, // Pass C - full fidelity
}

/// Graph node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GraphNode {
    pub node_id: NodeId,
    pub coordinate: Point2D,
    pub elevation: Option<f32>,
    pub connected_edges: Vec<EdgeId>,
}

impl GraphNode {
    pub fn new(node_id: NodeId, coordinate: Point2D) -> Self {
        Self {
            node_id,
            coordinate,
            elevation: None,
            connected_edges: Vec::new(),
        }
    }

    pub fn add_edge(&mut self, edge_id: EdgeId) {
        if !self.connected_edges.contains(&edge_id) {
            self.connected_edges.push(edge_id);
        }
    }
}

/// Time-only graph for fast routing
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TimeGraph {
    pub nodes: HashMap<NodeId, GraphNode>,
    pub edges: HashMap<EdgeId, TimeEdge>,
    pub profiles: Vec<TransportProfile>,
    pub consistency_hash: u64,
}

impl TimeGraph {
    pub fn new(profiles: Vec<TransportProfile>) -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            profiles,
            consistency_hash: 0,
        }
    }

    pub fn add_node(&mut self, node: GraphNode) {
        self.nodes.insert(node.node_id, node);
    }

    pub fn add_edge(&mut self, edge: TimeEdge) {
        // Update node connectivity
        if let Some(from_node) = self.nodes.get_mut(&edge.from_node) {
            from_node.add_edge(edge.edge_id);
        }
        if let Some(to_node) = self.nodes.get_mut(&edge.to_node) {
            to_node.add_edge(edge.edge_id);
        }

        self.edges.insert(edge.edge_id, edge);
    }

    pub fn calculate_consistency_hash(&mut self) {
        let mut hasher_input = Vec::new();

        // Hash all node positions and IDs
        let mut sorted_nodes: Vec<_> = self.nodes.iter().collect();
        sorted_nodes.sort_by_key(|(id, _)| id.0);

        for (node_id, node) in sorted_nodes {
            hasher_input.extend_from_slice(&node_id.0.to_le_bytes());
            hasher_input.extend_from_slice(&node.coordinate.x.to_le_bytes());
            hasher_input.extend_from_slice(&node.coordinate.y.to_le_bytes());
        }

        // Hash all edge weights and connectivity
        let mut sorted_edges: Vec<_> = self.edges.iter().collect();
        sorted_edges.sort_by_key(|(id, _)| id.0);

        for (edge_id, edge) in sorted_edges {
            hasher_input.extend_from_slice(&edge_id.0.to_le_bytes());
            hasher_input.extend_from_slice(&edge.from_node.0.to_le_bytes());
            hasher_input.extend_from_slice(&edge.to_node.0.to_le_bytes());

            // Hash weights for each profile in deterministic order
            for profile in &self.profiles {
                if let Some(weight) = edge.get_weight(profile) {
                    hasher_input.extend_from_slice(&weight.time_seconds.to_le_bytes());
                    hasher_input.extend_from_slice(&weight.distance_meters.to_le_bytes());
                }
            }
        }

        self.consistency_hash = xxh3_64(&hasher_input);
    }
}

/// Navigation graph with geometry
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct NavGraph {
    pub nodes: HashMap<NodeId, GraphNode>,
    pub edges: HashMap<EdgeId, NavEdge>,
    pub profiles: Vec<TransportProfile>,
    pub consistency_hash: u64,
}

impl NavGraph {
    pub fn new(profiles: Vec<TransportProfile>) -> Self {
        Self {
            nodes: HashMap::new(),
            edges: HashMap::new(),
            profiles,
            consistency_hash: 0,
        }
    }

    pub fn add_node(&mut self, node: GraphNode) {
        self.nodes.insert(node.node_id, node);
    }

    pub fn add_edge(&mut self, edge: NavEdge) {
        // Update node connectivity
        if let Some(from_node) = self.nodes.get_mut(&edge.from_node) {
            from_node.add_edge(edge.edge_id);
        }
        if let Some(to_node) = self.nodes.get_mut(&edge.to_node) {
            to_node.add_edge(edge.edge_id);
        }

        self.edges.insert(edge.edge_id, edge);
    }

    pub fn calculate_consistency_hash(&mut self) {
        let mut hasher_input = Vec::new();

        // Hash all node positions and IDs
        let mut sorted_nodes: Vec<_> = self.nodes.iter().collect();
        sorted_nodes.sort_by_key(|(id, _)| id.0);

        for (node_id, node) in sorted_nodes {
            hasher_input.extend_from_slice(&node_id.0.to_le_bytes());
            hasher_input.extend_from_slice(&node.coordinate.x.to_le_bytes());
            hasher_input.extend_from_slice(&node.coordinate.y.to_le_bytes());
        }

        // Hash all edge weights and geometry
        let mut sorted_edges: Vec<_> = self.edges.iter().collect();
        sorted_edges.sort_by_key(|(id, _)| id.0);

        for (edge_id, edge) in sorted_edges {
            hasher_input.extend_from_slice(&edge_id.0.to_le_bytes());
            hasher_input.extend_from_slice(&edge.from_node.0.to_le_bytes());
            hasher_input.extend_from_slice(&edge.to_node.0.to_le_bytes());

            // Hash weights for each profile
            for profile in &self.profiles {
                if let Some(weight) = edge.get_weight(profile) {
                    hasher_input.extend_from_slice(&weight.time_seconds.to_le_bytes());
                    hasher_input.extend_from_slice(&weight.distance_meters.to_le_bytes());
                }
            }

            // Hash geometry from snap skeleton (most stable)
            for point in &edge.snap_skeleton.points {
                hasher_input.extend_from_slice(&point.x.to_le_bytes());
                hasher_input.extend_from_slice(&point.y.to_le_bytes());
            }
        }

        self.consistency_hash = xxh3_64(&hasher_input);
    }
}

/// Dual core graph system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualCoreGraph {
    pub time_graph: TimeGraph,
    pub nav_graph: NavGraph,
    pub consistency_verified: bool,
    pub build_timestamp: u64,
}

impl DualCoreGraph {
    pub fn new(profiles: Vec<TransportProfile>) -> Self {
        Self {
            time_graph: TimeGraph::new(profiles.clone()),
            nav_graph: NavGraph::new(profiles),
            consistency_verified: false,
            build_timestamp: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        }
    }

    /// Verify consistency between time and nav graphs
    pub fn verify_consistency(&mut self) -> Result<(), String> {
        // Calculate consistency hashes
        self.time_graph.calculate_consistency_hash();
        self.nav_graph.calculate_consistency_hash();

        // Check that both graphs have the same structure
        if self.time_graph.nodes.len() != self.nav_graph.nodes.len() {
            return Err(format!(
                "Node count mismatch: time={}, nav={}",
                self.time_graph.nodes.len(),
                self.nav_graph.nodes.len()
            ));
        }

        if self.time_graph.edges.len() != self.nav_graph.edges.len() {
            return Err(format!(
                "Edge count mismatch: time={}, nav={}",
                self.time_graph.edges.len(),
                self.nav_graph.edges.len()
            ));
        }

        // Check that all nodes exist in both graphs
        for node_id in self.time_graph.nodes.keys() {
            if !self.nav_graph.nodes.contains_key(node_id) {
                return Err(format!("Node {:?} missing from nav graph", node_id));
            }
        }

        // Check that all edges exist in both graphs with same weights
        for (edge_id, time_edge) in &self.time_graph.edges {
            if let Some(nav_edge) = self.nav_graph.edges.get(edge_id) {
                // Verify same connectivity
                if time_edge.from_node != nav_edge.from_node
                    || time_edge.to_node != nav_edge.to_node
                {
                    return Err(format!(
                        "Edge {:?} connectivity mismatch: time=({:?},{:?}), nav=({:?},{:?})",
                        edge_id,
                        time_edge.from_node,
                        time_edge.to_node,
                        nav_edge.from_node,
                        nav_edge.to_node
                    ));
                }

                // Verify same weights for all profiles
                for profile in &self.time_graph.profiles {
                    let time_weight = time_edge.get_weight(profile);
                    let nav_weight = nav_edge.get_weight(profile);

                    match (time_weight, nav_weight) {
                        (Some(tw), Some(nw)) => {
                            if tw.time_seconds != nw.time_seconds
                                || tw.distance_meters != nw.distance_meters
                            {
                                return Err(format!(
                                    "Edge {:?} weight mismatch for {:?}: time=({},{}), nav=({},{})",
                                    edge_id,
                                    profile,
                                    tw.time_seconds,
                                    tw.distance_meters,
                                    nw.time_seconds,
                                    nw.distance_meters
                                ));
                            }
                        }
                        (None, None) => {} // Both missing is OK
                        _ => {
                            return Err(format!(
                                "Edge {:?} weight presence mismatch for {:?}",
                                edge_id, profile
                            ));
                        }
                    }
                }
            } else {
                return Err(format!("Edge {:?} missing from nav graph", edge_id));
            }
        }

        self.consistency_verified = true;
        Ok(())
    }

    /// Get consistency digests for verification
    pub fn get_consistency_digests(&mut self) -> (u64, u64) {
        if !self.consistency_verified {
            self.time_graph.calculate_consistency_hash();
            self.nav_graph.calculate_consistency_hash();
        }
        (
            self.time_graph.consistency_hash,
            self.nav_graph.consistency_hash,
        )
    }

    /// Check if consistency verification is required
    pub fn needs_verification(&self) -> bool {
        !self.consistency_verified
    }

    /// Get graph statistics
    pub fn get_stats(&self) -> DualCoreStats {
        DualCoreStats {
            time_graph_nodes: self.time_graph.nodes.len(),
            time_graph_edges: self.time_graph.edges.len(),
            nav_graph_nodes: self.nav_graph.nodes.len(),
            nav_graph_edges: self.nav_graph.edges.len(),
            profiles: self.time_graph.profiles.len(),
            consistency_verified: self.consistency_verified,
            build_timestamp: self.build_timestamp,
            time_graph_hash: self.time_graph.consistency_hash,
            nav_graph_hash: self.nav_graph.consistency_hash,
        }
    }
}

/// Statistics about the dual core graph system
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DualCoreStats {
    pub time_graph_nodes: usize,
    pub time_graph_edges: usize,
    pub nav_graph_nodes: usize,
    pub nav_graph_edges: usize,
    pub profiles: usize,
    pub consistency_verified: bool,
    pub build_timestamp: u64,
    pub time_graph_hash: u64,
    pub nav_graph_hash: u64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_node_creation() {
        let node_id = NodeId::new(1);
        let coordinate = Point2D::new(10.0, 20.0);
        let node = GraphNode::new(node_id, coordinate);

        assert_eq!(node.node_id, node_id);
        assert_eq!(node.coordinate.x, 10.0);
        assert_eq!(node.coordinate.y, 20.0);
        assert!(node.connected_edges.is_empty());
    }

    #[test]
    fn test_time_weight_quantization() {
        let weight = TimeWeight::new(123.7, 456.2);
        assert_eq!(weight.time_seconds, 124);
        assert_eq!(weight.distance_meters, 456);

        // Test overflow protection
        let large_weight = TimeWeight::new(100000.0, 100000.0);
        assert_eq!(large_weight.time_seconds, u16::MAX);
        assert_eq!(large_weight.distance_meters, u16::MAX);
    }

    #[test]
    fn test_time_edge_creation() {
        let edge_id = EdgeId(1);
        let from_node = NodeId::new(1);
        let to_node = NodeId::new(2);

        let mut edge = TimeEdge::new(edge_id, from_node, to_node);
        let weight = TimeWeight::new(30.0, 100.0);
        edge.add_weight(TransportProfile::Car, weight);

        assert_eq!(edge.edge_id, edge_id);
        assert_eq!(edge.from_node, from_node);
        assert_eq!(edge.to_node, to_node);

        let retrieved_weight = edge.get_weight(&TransportProfile::Car).unwrap();
        assert_eq!(retrieved_weight.time_seconds, 30);
        assert_eq!(retrieved_weight.distance_meters, 100);
    }

    #[test]
    fn test_time_graph_consistency_hash() {
        let profiles = vec![TransportProfile::Car, TransportProfile::Bicycle];
        let mut graph = TimeGraph::new(profiles);

        // Add some nodes and edges
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        let node2 = GraphNode::new(NodeId::new(2), Point2D::new(1.0, 1.0));
        graph.add_node(node1);
        graph.add_node(node2);

        let mut edge = TimeEdge::new(EdgeId(1), NodeId::new(1), NodeId::new(2));
        edge.add_weight(TransportProfile::Car, TimeWeight::new(30.0, 100.0));
        graph.add_edge(edge);

        graph.calculate_consistency_hash();
        let hash1 = graph.consistency_hash;

        // Hash should be deterministic
        graph.calculate_consistency_hash();
        let hash2 = graph.consistency_hash;
        assert_eq!(hash1, hash2);

        // Hash should change when we add data
        let node3 = GraphNode::new(NodeId::new(3), Point2D::new(2.0, 2.0));
        graph.add_node(node3);
        graph.calculate_consistency_hash();
        let hash3 = graph.consistency_hash;
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_dual_core_consistency_verification() {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add matching nodes to both graphs
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        let node2 = GraphNode::new(NodeId::new(2), Point2D::new(1.0, 1.0));
        dual_core.time_graph.add_node(node1.clone());
        dual_core.time_graph.add_node(node2.clone());
        dual_core.nav_graph.add_node(node1);
        dual_core.nav_graph.add_node(node2);

        // Add matching edges to both graphs
        let mut time_edge = TimeEdge::new(EdgeId(1), NodeId::new(1), NodeId::new(2));
        let weight = TimeWeight::new(30.0, 100.0);
        time_edge.add_weight(TransportProfile::Car, weight);
        dual_core.time_graph.add_edge(time_edge);

        let snap_skeleton = SnapSkeleton::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 1.0)],
            vec![],
            141.42,
            5.0,
        );
        let nav_geometry = NavigationGeometry::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 1.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge = NavEdge::new(
            EdgeId(1),
            NodeId::new(1),
            NodeId::new(2),
            snap_skeleton,
            nav_geometry,
            None,
        );
        nav_edge.add_weight(TransportProfile::Car, weight);
        dual_core.nav_graph.add_edge(nav_edge);

        // Verification should pass
        assert!(dual_core.verify_consistency().is_ok());
        assert!(dual_core.consistency_verified);
    }

    #[test]
    fn test_dual_core_consistency_mismatch() {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add node only to time graph
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        dual_core.time_graph.add_node(node1);

        // Verification should fail due to node count mismatch
        assert!(dual_core.verify_consistency().is_err());
    }

    #[test]
    fn test_nav_edge_geometry_access() {
        let snap_skeleton = SnapSkeleton::new(
            vec![
                Point2D::new(0.0, 0.0),
                Point2D::new(0.5, 0.5),
                Point2D::new(1.0, 1.0),
            ],
            vec![],
            141.42,
            5.0,
        );
        let nav_geometry = NavigationGeometry::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 1.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );

        let nav_edge = NavEdge::new(
            EdgeId(1),
            NodeId::new(1),
            NodeId::new(2),
            snap_skeleton,
            nav_geometry,
            None,
        );

        let snap_geom = nav_edge.get_geometry(GeometryPass::Snap);
        let nav_geom = nav_edge.get_geometry(GeometryPass::Navigation);
        let full_geom = nav_edge.get_geometry(GeometryPass::FullFidelity);

        assert_eq!(snap_geom.len(), 3); // Snap has more points
        assert_eq!(nav_geom.len(), 2); // Nav is simplified
        assert_eq!(full_geom.len(), 2); // Falls back to nav since no Pass C
    }

    #[test]
    fn test_dual_core_stats() {
        let profiles = vec![TransportProfile::Car, TransportProfile::Bicycle];
        let mut dual_core = DualCoreGraph::new(profiles);

        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        dual_core.time_graph.add_node(node1.clone());
        dual_core.nav_graph.add_node(node1);

        let stats = dual_core.get_stats();
        assert_eq!(stats.time_graph_nodes, 1);
        assert_eq!(stats.nav_graph_nodes, 1);
        assert_eq!(stats.profiles, 2);
        assert!(!stats.consistency_verified);
    }
}
