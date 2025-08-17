//! Dijkstra's algorithm implementation for distance-based routing

use crate::dual_core::{DualCoreGraph, NodeId, GeometryPass};
use crate::profiles::{TransportProfile, EdgeId};
use butterfly_geometry::Point2D;
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Ordering;

/// Distance-based routing result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RouteResult {
    pub profile: TransportProfile,
    pub start_node: NodeId,
    pub end_node: NodeId,
    pub total_distance: f64,
    pub total_time: f64,
    pub node_path: Vec<NodeId>,
    pub edge_path: Vec<EdgeId>,
    pub geometry: Option<Vec<Point2D>>,
    pub computation_stats: ComputationStats,
}

impl RouteResult {
    pub fn new(
        profile: TransportProfile,
        start_node: NodeId,
        end_node: NodeId,
        total_distance: f64,
        total_time: f64,
        node_path: Vec<NodeId>,
        edge_path: Vec<EdgeId>,
        geometry: Option<Vec<Point2D>>,
        computation_stats: ComputationStats,
    ) -> Self {
        Self {
            profile,
            start_node,
            end_node,
            total_distance,
            total_time,
            node_path,
            edge_path,
            geometry,
            computation_stats,
        }
    }
}

/// Statistics about route computation
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ComputationStats {
    pub nodes_explored: usize,
    pub edges_relaxed: usize,
    pub computation_time_ms: u64,
    pub graph_type: GraphType,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum GraphType {
    TimeOnly,
    Navigation,
}

/// Priority queue entry for Dijkstra's algorithm
#[derive(Debug, Clone, PartialEq)]
struct DijkstraNode {
    node_id: NodeId,
    distance: f64,
    time: f64,
}

impl Eq for DijkstraNode {}

impl Ord for DijkstraNode {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse ordering for distance (primary) and time (secondary)
        other.distance.partial_cmp(&self.distance)
            .unwrap_or(Ordering::Equal)
            .then_with(|| other.time.partial_cmp(&self.time).unwrap_or(Ordering::Equal))
    }
}

impl PartialOrd for DijkstraNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Distance-based Dijkstra router
pub struct DistanceRouter {
    dual_core: DualCoreGraph,
    enable_turn_restrictions: bool,
}

impl DistanceRouter {
    pub fn new(dual_core: DualCoreGraph) -> Result<Self, String> {
        let mut router = Self { 
            dual_core,
            enable_turn_restrictions: true, // Enable by default
        };
        
        // Verify consistency before using
        router.dual_core.verify_consistency()?;
        
        Ok(router)
    }

    pub fn with_turn_restrictions(dual_core: DualCoreGraph, enable_turn_restrictions: bool) -> Result<Self, String> {
        let mut router = Self { 
            dual_core,
            enable_turn_restrictions,
        };
        
        // Verify consistency before using
        router.dual_core.verify_consistency()?;
        
        Ok(router)
    }

    /// Route using time graph (distance-based weights)
    pub fn route_time_graph(
        &self,
        start_node: NodeId,
        end_node: NodeId,
        profile: TransportProfile,
    ) -> Result<RouteResult, String> {
        let start_time = std::time::Instant::now();
        
        let (node_path, edge_path, total_distance, total_time, stats) = 
            self.dijkstra_time_graph(start_node, end_node, &profile)?;
        
        let computation_time = start_time.elapsed().as_millis() as u64;
        let computation_stats = ComputationStats {
            nodes_explored: stats.0,
            edges_relaxed: stats.1,
            computation_time_ms: computation_time,
            graph_type: GraphType::TimeOnly,
        };

        Ok(RouteResult::new(
            profile,
            start_node,
            end_node,
            total_distance,
            total_time,
            node_path,
            edge_path,
            None, // No geometry in time graph
            computation_stats,
        ))
    }

    /// Route using navigation graph (with geometry)
    pub fn route_nav_graph(
        &self,
        start_node: NodeId,
        end_node: NodeId,
        profile: TransportProfile,
        geometry_pass: GeometryPass,
    ) -> Result<RouteResult, String> {
        let start_time = std::time::Instant::now();
        
        let (node_path, edge_path, total_distance, total_time, stats) = 
            self.dijkstra_nav_graph(start_node, end_node, &profile)?;
        
        // Extract geometry
        let geometry = self.extract_route_geometry(&edge_path, geometry_pass)?;
        
        let computation_time = start_time.elapsed().as_millis() as u64;
        let computation_stats = ComputationStats {
            nodes_explored: stats.0,
            edges_relaxed: stats.1,
            computation_time_ms: computation_time,
            graph_type: GraphType::Navigation,
        };

        Ok(RouteResult::new(
            profile,
            start_node,
            end_node,
            total_distance,
            total_time,
            node_path,
            edge_path,
            Some(geometry),
            computation_stats,
        ))
    }

    /// Dijkstra implementation for time graph
    fn dijkstra_time_graph(
        &self,
        start_node: NodeId,
        end_node: NodeId,
        profile: &TransportProfile,
    ) -> Result<(Vec<NodeId>, Vec<EdgeId>, f64, f64, (usize, usize)), String> {
        let graph = &self.dual_core.time_graph;
        
        if !graph.nodes.contains_key(&start_node) {
            return Err(format!("Start node {:?} not found", start_node));
        }
        if !graph.nodes.contains_key(&end_node) {
            return Err(format!("End node {:?} not found", end_node));
        }

        let mut distances: HashMap<NodeId, f64> = HashMap::new();
        let mut times: HashMap<NodeId, f64> = HashMap::new();
        let mut predecessors: HashMap<NodeId, (NodeId, EdgeId)> = HashMap::new();
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut heap = BinaryHeap::new();

        distances.insert(start_node, 0.0);
        times.insert(start_node, 0.0);
        heap.push(DijkstraNode {
            node_id: start_node,
            distance: 0.0,
            time: 0.0,
        });

        let mut nodes_explored = 0;
        let mut edges_relaxed = 0;

        while let Some(current) = heap.pop() {
            if visited.contains(&current.node_id) {
                continue;
            }

            visited.insert(current.node_id);
            nodes_explored += 1;

            if current.node_id == end_node {
                break;
            }

            // Explore neighbors
            if let Some(node) = graph.nodes.get(&current.node_id) {
                for &edge_id in &node.connected_edges {
                    if let Some(edge) = graph.edges.get(&edge_id) {
                        let neighbor_id = if edge.from_node == current.node_id {
                            edge.to_node
                        } else if edge.to_node == current.node_id {
                            edge.from_node
                        } else {
                            continue;
                        };

                        if visited.contains(&neighbor_id) {
                            continue;
                        }

                        // Check turn restrictions if enabled
                        if self.enable_turn_restrictions {
                            if let Some(prev_edge_id) = self.get_previous_edge(&predecessors, current.node_id) {
                                if self.is_turn_restricted(prev_edge_id, current.node_id, edge_id, profile) {
                                    continue; // Skip this edge due to turn restriction
                                }
                            }
                        }

                        if let Some(weight) = edge.get_weight(profile) {
                            let edge_distance = weight.distance_meters_f64();
                            let edge_time = weight.time_seconds_f64();
                            let new_distance = current.distance + edge_distance;
                            let new_time = current.time + edge_time;

                            let should_relax = match distances.get(&neighbor_id) {
                                Some(&existing_distance) => new_distance < existing_distance,
                                None => true,
                            };

                            if should_relax {
                                distances.insert(neighbor_id, new_distance);
                                times.insert(neighbor_id, new_time);
                                predecessors.insert(neighbor_id, (current.node_id, edge_id));

                                heap.push(DijkstraNode {
                                    node_id: neighbor_id,
                                    distance: new_distance,
                                    time: new_time,
                                });

                                edges_relaxed += 1;
                            }
                        }
                    }
                }
            }
        }

        // Reconstruct path
        if !distances.contains_key(&end_node) {
            return Err("No path found".to_string());
        }

        let mut node_path = Vec::new();
        let mut edge_path = Vec::new();
        let mut current_node = end_node;

        while current_node != start_node {
            node_path.push(current_node);
            if let Some((prev_node, edge_id)) = predecessors.get(&current_node) {
                edge_path.push(*edge_id);
                current_node = *prev_node;
            } else {
                return Err("Path reconstruction failed".to_string());
            }
        }
        node_path.push(start_node);

        node_path.reverse();
        edge_path.reverse();

        let total_distance = distances[&end_node];
        let total_time = times[&end_node];

        Ok((node_path, edge_path, total_distance, total_time, (nodes_explored, edges_relaxed)))
    }

    /// Dijkstra implementation for navigation graph
    fn dijkstra_nav_graph(
        &self,
        start_node: NodeId,
        end_node: NodeId,
        profile: &TransportProfile,
    ) -> Result<(Vec<NodeId>, Vec<EdgeId>, f64, f64, (usize, usize)), String> {
        let graph = &self.dual_core.nav_graph;
        
        if !graph.nodes.contains_key(&start_node) {
            return Err(format!("Start node {:?} not found", start_node));
        }
        if !graph.nodes.contains_key(&end_node) {
            return Err(format!("End node {:?} not found", end_node));
        }

        let mut distances: HashMap<NodeId, f64> = HashMap::new();
        let mut times: HashMap<NodeId, f64> = HashMap::new();
        let mut predecessors: HashMap<NodeId, (NodeId, EdgeId)> = HashMap::new();
        let mut visited: HashSet<NodeId> = HashSet::new();
        let mut heap = BinaryHeap::new();

        distances.insert(start_node, 0.0);
        times.insert(start_node, 0.0);
        heap.push(DijkstraNode {
            node_id: start_node,
            distance: 0.0,
            time: 0.0,
        });

        let mut nodes_explored = 0;
        let mut edges_relaxed = 0;

        while let Some(current) = heap.pop() {
            if visited.contains(&current.node_id) {
                continue;
            }

            visited.insert(current.node_id);
            nodes_explored += 1;

            if current.node_id == end_node {
                break;
            }

            // Explore neighbors (same algorithm as time graph)
            if let Some(node) = graph.nodes.get(&current.node_id) {
                for &edge_id in &node.connected_edges {
                    if let Some(edge) = graph.edges.get(&edge_id) {
                        let neighbor_id = if edge.from_node == current.node_id {
                            edge.to_node
                        } else if edge.to_node == current.node_id {
                            edge.from_node
                        } else {
                            continue;
                        };

                        if visited.contains(&neighbor_id) {
                            continue;
                        }

                        // Check turn restrictions if enabled
                        if self.enable_turn_restrictions {
                            if let Some(prev_edge_id) = self.get_previous_edge(&predecessors, current.node_id) {
                                if self.is_turn_restricted(prev_edge_id, current.node_id, edge_id, profile) {
                                    continue; // Skip this edge due to turn restriction
                                }
                            }
                        }

                        if let Some(weight) = edge.get_weight(profile) {
                            let edge_distance = weight.distance_meters_f64();
                            let edge_time = weight.time_seconds_f64();
                            let new_distance = current.distance + edge_distance;
                            let new_time = current.time + edge_time;

                            let should_relax = match distances.get(&neighbor_id) {
                                Some(&existing_distance) => new_distance < existing_distance,
                                None => true,
                            };

                            if should_relax {
                                distances.insert(neighbor_id, new_distance);
                                times.insert(neighbor_id, new_time);
                                predecessors.insert(neighbor_id, (current.node_id, edge_id));

                                heap.push(DijkstraNode {
                                    node_id: neighbor_id,
                                    distance: new_distance,
                                    time: new_time,
                                });

                                edges_relaxed += 1;
                            }
                        }
                    }
                }
            }
        }

        // Reconstruct path (same as time graph)
        if !distances.contains_key(&end_node) {
            return Err("No path found".to_string());
        }

        let mut node_path = Vec::new();
        let mut edge_path = Vec::new();
        let mut current_node = end_node;

        while current_node != start_node {
            node_path.push(current_node);
            if let Some((prev_node, edge_id)) = predecessors.get(&current_node) {
                edge_path.push(*edge_id);
                current_node = *prev_node;
            } else {
                return Err("Path reconstruction failed".to_string());
            }
        }
        node_path.push(start_node);

        node_path.reverse();
        edge_path.reverse();

        let total_distance = distances[&end_node];
        let total_time = times[&end_node];

        Ok((node_path, edge_path, total_distance, total_time, (nodes_explored, edges_relaxed)))
    }

    /// Extract geometry for a route from navigation graph
    fn extract_route_geometry(
        &self,
        edge_path: &[EdgeId],
        geometry_pass: GeometryPass,
    ) -> Result<Vec<Point2D>, String> {
        let mut geometry = Vec::new();
        
        for edge_id in edge_path {
            if let Some(nav_edge) = self.dual_core.nav_graph.edges.get(edge_id) {
                let edge_geometry = nav_edge.get_geometry(geometry_pass);
                
                // Avoid duplicate points at edge boundaries
                if geometry.is_empty() {
                    geometry.extend(edge_geometry);
                } else {
                    // Skip first point of edge to avoid duplication with last point of previous edge
                    geometry.extend(edge_geometry.iter().skip(1));
                }
            } else {
                return Err(format!("Edge {:?} not found in navigation graph", edge_id));
            }
        }
        
        Ok(geometry)
    }

    /// Get previous edge from predecessors map
    fn get_previous_edge(&self, predecessors: &HashMap<NodeId, (NodeId, EdgeId)>, node_id: NodeId) -> Option<EdgeId> {
        predecessors.get(&node_id).map(|(_, edge_id)| *edge_id)
    }

    /// Check if a turn is restricted for the given profile
    fn is_turn_restricted(&self, from_edge: EdgeId, via_node: NodeId, to_edge: EdgeId, profile: &TransportProfile) -> bool {
        // Check turn restrictions in time graph
        if let Some(time_edge) = self.dual_core.time_graph.edges.get(&from_edge) {
            for restriction in &time_edge.turn_restrictions {
                if restriction.from_edge == from_edge && 
                   restriction.via_node == via_node && 
                   restriction.to_edge == to_edge &&
                   restriction.profiles.contains(profile) {
                    match restriction.restriction_type {
                        crate::dual_core::RestrictionType::NoTurn => return true,
                        crate::dual_core::RestrictionType::NoUturn => {
                            // Check if this is a U-turn (going back on same way)
                            if from_edge == to_edge {
                                return true;
                            }
                        }
                        crate::dual_core::RestrictionType::OnlyTurn => {
                            // For only_turn restrictions, we need to check all other possible turns
                            // This is more complex and would require additional context
                            // For now, we'll implement basic no_turn and no_u_turn
                        }
                    }
                }
            }
        }

        // Also check in nav graph if available
        if let Some(nav_edge) = self.dual_core.nav_graph.edges.get(&from_edge) {
            for restriction in &nav_edge.turn_restrictions {
                if restriction.from_edge == from_edge && 
                   restriction.via_node == via_node && 
                   restriction.to_edge == to_edge &&
                   restriction.profiles.contains(profile) {
                    match restriction.restriction_type {
                        crate::dual_core::RestrictionType::NoTurn => return true,
                        crate::dual_core::RestrictionType::NoUturn => {
                            if from_edge == to_edge {
                                return true;
                            }
                        }
                        crate::dual_core::RestrictionType::OnlyTurn => {
                            // Basic implementation for only_turn
                        }
                    }
                }
            }
        }

        false
    }

    /// Get dual core graph reference
    pub fn dual_core(&self) -> &DualCoreGraph {
        &self.dual_core
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dual_core::{TimeEdge, NavEdge, GraphNode, TimeWeight};
    use butterfly_geometry::{SnapSkeleton, NavigationGeometry};

    fn create_test_dual_core() -> DualCoreGraph {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add nodes
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        let node2 = GraphNode::new(NodeId::new(2), Point2D::new(1.0, 0.0));
        let node3 = GraphNode::new(NodeId::new(3), Point2D::new(2.0, 0.0));

        dual_core.time_graph.add_node(node1.clone());
        dual_core.time_graph.add_node(node2.clone());
        dual_core.time_graph.add_node(node3.clone());

        dual_core.nav_graph.add_node(node1);
        dual_core.nav_graph.add_node(node2);
        dual_core.nav_graph.add_node(node3);

        // Add edges
        let mut time_edge1 = TimeEdge::new(EdgeId(1), NodeId::new(1), NodeId::new(2));
        time_edge1.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.time_graph.add_edge(time_edge1);

        let mut time_edge2 = TimeEdge::new(EdgeId(2), NodeId::new(2), NodeId::new(3));
        time_edge2.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.time_graph.add_edge(time_edge2);

        // Nav edges with geometry
        let snap_skeleton1 = SnapSkeleton::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            1000.0,
            5.0,
        );
        let nav_geometry1 = NavigationGeometry::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge1 = NavEdge::new(
            EdgeId(1),
            NodeId::new(1),
            NodeId::new(2),
            snap_skeleton1,
            nav_geometry1,
            None,
        );
        nav_edge1.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.nav_graph.add_edge(nav_edge1);

        let snap_skeleton2 = SnapSkeleton::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(2.0, 0.0)],
            vec![],
            1000.0,
            5.0,
        );
        let nav_geometry2 = NavigationGeometry::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(2.0, 0.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge2 = NavEdge::new(
            EdgeId(2),
            NodeId::new(2),
            NodeId::new(3),
            snap_skeleton2,
            nav_geometry2,
            None,
        );
        nav_edge2.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.nav_graph.add_edge(nav_edge2);

        dual_core
    }

    #[test]
    fn test_distance_router_creation() {
        let dual_core = create_test_dual_core();
        let router = DistanceRouter::new(dual_core);
        assert!(router.is_ok());
    }

    #[test]
    fn test_time_graph_routing() {
        let dual_core = create_test_dual_core();
        let router = DistanceRouter::new(dual_core).unwrap();

        let result = router.route_time_graph(
            NodeId::new(1),
            NodeId::new(3),
            TransportProfile::Car,
        ).unwrap();

        assert_eq!(result.start_node, NodeId::new(1));
        assert_eq!(result.end_node, NodeId::new(3));
        assert_eq!(result.profile, TransportProfile::Car);
        assert_eq!(result.total_distance, 2000.0); // Two 1000m edges
        assert_eq!(result.total_time, 120.0);      // Two 60s edges
        assert_eq!(result.node_path.len(), 3);     // 3 nodes in path
        assert_eq!(result.edge_path.len(), 2);     // 2 edges in path
        assert!(result.geometry.is_none());       // No geometry in time graph
    }

    #[test]
    fn test_nav_graph_routing() {
        let dual_core = create_test_dual_core();
        let router = DistanceRouter::new(dual_core).unwrap();

        let result = router.route_nav_graph(
            NodeId::new(1),
            NodeId::new(3),
            TransportProfile::Car,
            GeometryPass::Navigation,
        ).unwrap();

        assert_eq!(result.start_node, NodeId::new(1));
        assert_eq!(result.end_node, NodeId::new(3));
        assert_eq!(result.total_distance, 2000.0);
        assert_eq!(result.total_time, 120.0);
        assert!(result.geometry.is_some());
        
        let geometry = result.geometry.unwrap();
        assert_eq!(geometry.len(), 3); // Start + intermediate + end points
        assert_eq!(geometry[0], Point2D::new(0.0, 0.0));
        assert_eq!(geometry[1], Point2D::new(1.0, 0.0));
        assert_eq!(geometry[2], Point2D::new(2.0, 0.0));
    }

    #[test]
    fn test_no_path_found() {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add disconnected nodes
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        let node2 = GraphNode::new(NodeId::new(2), Point2D::new(1.0, 0.0));

        dual_core.time_graph.add_node(node1.clone());
        dual_core.time_graph.add_node(node2.clone());
        dual_core.nav_graph.add_node(node1);
        dual_core.nav_graph.add_node(node2);

        let router = DistanceRouter::new(dual_core).unwrap();

        let result = router.route_time_graph(
            NodeId::new(1),
            NodeId::new(2),
            TransportProfile::Car,
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("No path found"));
    }

    #[test]
    fn test_dijkstra_node_ordering() {
        let node1 = DijkstraNode {
            node_id: NodeId::new(1),
            distance: 100.0,
            time: 50.0,
        };

        let node2 = DijkstraNode {
            node_id: NodeId::new(2),
            distance: 200.0,
            time: 75.0,
        };

        // Smaller distance should have higher priority (min-heap)
        assert!(node1 > node2);
    }

    #[test]
    fn test_computation_stats() {
        let dual_core = create_test_dual_core();
        let router = DistanceRouter::new(dual_core).unwrap();

        let result = router.route_time_graph(
            NodeId::new(1),
            NodeId::new(3),
            TransportProfile::Car,
        ).unwrap();

        assert!(result.computation_stats.nodes_explored > 0);
        assert!(result.computation_stats.edges_relaxed > 0);
        // computation_time_ms is u64, so always >= 0 - test that it's reasonable instead
        assert!(result.computation_stats.computation_time_ms < 1000); // Should complete quickly in tests
        assert_eq!(result.computation_stats.graph_type, GraphType::TimeOnly);
    }

    #[test]
    fn test_turn_restrictions_enabled() {
        let dual_core = create_test_dual_core();
        let router = DistanceRouter::with_turn_restrictions(dual_core, true).unwrap();
        
        // Basic routing should still work with turn restrictions enabled
        let result = router.route_time_graph(
            NodeId::new(1),
            NodeId::new(3),
            TransportProfile::Car,
        ).unwrap();

        assert_eq!(result.start_node, NodeId::new(1));
        assert_eq!(result.end_node, NodeId::new(3));
        assert!(result.total_distance > 0.0);
    }

    #[test]
    fn test_turn_restrictions_disabled() {
        let dual_core = create_test_dual_core();
        let router = DistanceRouter::with_turn_restrictions(dual_core, false).unwrap();
        
        // Routing should work the same with turn restrictions disabled
        let result = router.route_time_graph(
            NodeId::new(1),
            NodeId::new(3),
            TransportProfile::Car,
        ).unwrap();

        assert_eq!(result.start_node, NodeId::new(1));
        assert_eq!(result.end_node, NodeId::new(3));
        assert!(result.total_distance > 0.0);
    }

    fn create_test_dual_core_with_restrictions() -> DualCoreGraph {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add nodes
        let node1 = GraphNode::new(NodeId::new(1), Point2D::new(0.0, 0.0));
        let node2 = GraphNode::new(NodeId::new(2), Point2D::new(1.0, 0.0));
        let node3 = GraphNode::new(NodeId::new(3), Point2D::new(2.0, 0.0));
        let node4 = GraphNode::new(NodeId::new(4), Point2D::new(1.0, 1.0));

        dual_core.time_graph.add_node(node1.clone());
        dual_core.time_graph.add_node(node2.clone());
        dual_core.time_graph.add_node(node3.clone());
        dual_core.time_graph.add_node(node4.clone());

        dual_core.nav_graph.add_node(node1);
        dual_core.nav_graph.add_node(node2);
        dual_core.nav_graph.add_node(node3);
        dual_core.nav_graph.add_node(node4);

        // Add edges with turn restrictions
        let mut time_edge1 = TimeEdge::new(EdgeId(1), NodeId::new(1), NodeId::new(2));
        time_edge1.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        
        let mut time_edge2 = TimeEdge::new(EdgeId(2), NodeId::new(2), NodeId::new(3));
        time_edge2.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        
        let mut time_edge3 = TimeEdge::new(EdgeId(3), NodeId::new(2), NodeId::new(4));
        time_edge3.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));

        // Add turn restriction: no right turn from edge 1 to edge 3 at node 2
        use crate::dual_core::{TurnRestriction, RestrictionType};
        let turn_restriction = TurnRestriction {
            from_edge: EdgeId(1),
            via_node: NodeId::new(2),
            to_edge: EdgeId(3),
            restriction_type: RestrictionType::NoTurn,
            profiles: vec![TransportProfile::Car],
        };
        time_edge1.turn_restrictions.push(turn_restriction);

        dual_core.time_graph.add_edge(time_edge1);
        dual_core.time_graph.add_edge(time_edge2);
        dual_core.time_graph.add_edge(time_edge3);

        // Add corresponding nav edges (must match time edges for consistency)
        let snap_skeleton1 = SnapSkeleton::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            1000.0,
            5.0,
        );
        let nav_geometry1 = NavigationGeometry::new(
            vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge1 = NavEdge::new(
            EdgeId(1),
            NodeId::new(1),
            NodeId::new(2),
            snap_skeleton1,
            nav_geometry1,
            None,
        );
        nav_edge1.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.nav_graph.add_edge(nav_edge1);

        // Add nav edge 2
        let snap_skeleton2 = SnapSkeleton::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(2.0, 0.0)],
            vec![],
            1000.0,
            5.0,
        );
        let nav_geometry2 = NavigationGeometry::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(2.0, 0.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge2 = NavEdge::new(
            EdgeId(2),
            NodeId::new(2),
            NodeId::new(3),
            snap_skeleton2,
            nav_geometry2,
            None,
        );
        nav_edge2.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.nav_graph.add_edge(nav_edge2);

        // Add nav edge 3
        let snap_skeleton3 = SnapSkeleton::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(1.0, 1.0)],
            vec![],
            1000.0,
            5.0,
        );
        let nav_geometry3 = NavigationGeometry::new(
            vec![Point2D::new(1.0, 0.0), Point2D::new(1.0, 1.0)],
            vec![],
            500.0,
            0.5,
            1.0,
            0.8,
        );
        let mut nav_edge3 = NavEdge::new(
            EdgeId(3),
            NodeId::new(2),
            NodeId::new(4),
            snap_skeleton3,
            nav_geometry3,
            None,
        );
        nav_edge3.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
        dual_core.nav_graph.add_edge(nav_edge3);

        dual_core
    }

    #[test]
    fn test_turn_restriction_logic() {
        let dual_core = create_test_dual_core_with_restrictions();
        let router = DistanceRouter::with_turn_restrictions(dual_core, true).unwrap();
        
        // Test turn restriction checking
        let is_restricted = router.is_turn_restricted(
            EdgeId(1),        // from edge
            NodeId::new(2),   // via node  
            EdgeId(3),        // to edge
            &TransportProfile::Car
        );
        
        assert!(is_restricted, "Turn from edge 1 to edge 3 via node 2 should be restricted");
        
        // Test non-restricted turn
        let is_not_restricted = router.is_turn_restricted(
            EdgeId(1),        // from edge
            NodeId::new(2),   // via node
            EdgeId(2),        // to edge (different edge)
            &TransportProfile::Car
        );
        
        assert!(!is_not_restricted, "Turn from edge 1 to edge 2 via node 2 should not be restricted");
    }
}
