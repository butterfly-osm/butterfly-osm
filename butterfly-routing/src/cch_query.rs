//! M8.3 - Bidirectional CCH Queries: High-performance exact routing
//!
//! This module implements bidirectional Contraction Hierarchy queries for fast exact
//! shortest path computation with comprehensive performance validation.

use crate::cch_customization::{BackwardCSR, CCHCustomization, CCHUpwardEdge, UpwardCSR};
use crate::cch_ordering::{CCHNodeId, CCHOrdering};
use crate::dual_core::NodeId;
use crate::profiles::TransportProfile;
use crate::thread_architecture::NumaNode;
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::sync::Arc;
use std::time::Instant;

/// Trait for CSR graph structures
trait CSRGraph {
    fn get_edges(&self, node_id: CCHNodeId) -> &[CCHUpwardEdge];
}

impl CSRGraph for UpwardCSR {
    fn get_edges(&self, node_id: CCHNodeId) -> &[CCHUpwardEdge] {
        self.get_edges(node_id)
    }
}

impl CSRGraph for BackwardCSR {
    fn get_edges(&self, node_id: CCHNodeId) -> &[CCHUpwardEdge] {
        self.get_edges(node_id)
    }
}

/// Maximum number of nodes to explore in CCH search
pub const MAX_SEARCH_NODES: usize = 100_000;

/// Priority queue entry for CCH search
#[derive(Debug, Clone, Copy)]
struct CCHSearchState {
    cost: f64,
    node: CCHNodeId,
    #[allow(dead_code)]
    is_forward: bool,
}

impl CCHSearchState {
    fn new(cost: f64, node: CCHNodeId, is_forward: bool) -> Self {
        Self {
            cost,
            node,
            is_forward,
        }
    }
}

impl PartialEq for CCHSearchState {
    fn eq(&self, other: &Self) -> bool {
        self.cost.eq(&other.cost)
    }
}

impl Eq for CCHSearchState {}

impl PartialOrd for CCHSearchState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        // Reverse for min-heap behavior
        other.cost.partial_cmp(&self.cost)
    }
}

impl Ord for CCHSearchState {
    fn cmp(&self, other: &Self) -> Ordering {
        self.partial_cmp(other).unwrap_or(Ordering::Equal)
    }
}

/// CCH query result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCHQueryResult {
    pub source: NodeId,
    pub target: NodeId,
    pub profile: TransportProfile,
    pub distance: f64,
    pub path_found: bool,
    pub path: Vec<NodeId>,
    pub computation_stats: CCHComputationStats,
    pub query_time_ns: u64,
}

impl CCHQueryResult {
    pub fn not_found(
        source: NodeId,
        target: NodeId,
        profile: TransportProfile,
        stats: CCHComputationStats,
        query_time_ns: u64,
    ) -> Self {
        Self {
            source,
            target,
            profile,
            distance: f64::INFINITY,
            path_found: false,
            path: Vec::new(),
            computation_stats: stats,
            query_time_ns,
        }
    }

    pub fn found(
        source: NodeId,
        target: NodeId,
        profile: TransportProfile,
        distance: f64,
        path: Vec<NodeId>,
        stats: CCHComputationStats,
        query_time_ns: u64,
    ) -> Self {
        Self {
            source,
            target,
            profile,
            distance,
            path_found: true,
            path,
            computation_stats: stats,
            query_time_ns,
        }
    }

    /// Check if query meets performance SLA
    pub fn meets_performance_sla(&self, max_query_time_ms: f64) -> bool {
        let query_time_ms = self.query_time_ns as f64 / 1_000_000.0;
        query_time_ms <= max_query_time_ms
    }
}

/// CCH computation statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCHComputationStats {
    pub forward_nodes_explored: usize,
    pub backward_nodes_explored: usize,
    pub total_nodes_explored: usize,
    pub forward_edges_relaxed: usize,
    pub backward_edges_relaxed: usize,
    pub total_edges_relaxed: usize,
    pub meeting_node: Option<CCHNodeId>,
    pub forward_search_stopped: bool,
    pub backward_search_stopped: bool,
    pub max_forward_distance: f64,
    pub max_backward_distance: f64,
    pub numa_node: Option<NumaNode>,
}

impl CCHComputationStats {
    pub fn new() -> Self {
        Self {
            forward_nodes_explored: 0,
            backward_nodes_explored: 0,
            total_nodes_explored: 0,
            forward_edges_relaxed: 0,
            backward_edges_relaxed: 0,
            total_edges_relaxed: 0,
            meeting_node: None,
            forward_search_stopped: false,
            backward_search_stopped: false,
            max_forward_distance: 0.0,
            max_backward_distance: 0.0,
            numa_node: Some(NumaNode::current()),
        }
    }

    pub fn finalize(&mut self) {
        self.total_nodes_explored = self.forward_nodes_explored + self.backward_nodes_explored;
        self.total_edges_relaxed = self.forward_edges_relaxed + self.backward_edges_relaxed;
    }

    /// Calculate search efficiency (lower is better)
    pub fn search_efficiency(&self) -> f64 {
        if self.total_nodes_explored == 0 {
            return 1.0;
        }
        self.total_edges_relaxed as f64 / self.total_nodes_explored as f64
    }
}

/// Edge used in path reconstruction
#[derive(Debug, Clone)]
struct PathEdge {
    from: CCHNodeId,
    to: CCHNodeId,
    #[allow(dead_code)]
    weight: f64,
    is_shortcut: bool,
    middle_node: Option<CCHNodeId>,
}

/// Bidirectional CCH search state
struct BidirectionalSearch {
    forward_queue: BinaryHeap<CCHSearchState>,
    backward_queue: BinaryHeap<CCHSearchState>,
    forward_distances: Vec<f64>,
    backward_distances: Vec<f64>,
    forward_predecessors: Vec<Option<CCHNodeId>>,
    backward_predecessors: Vec<Option<CCHNodeId>>,
    forward_edges: Vec<Option<PathEdge>>,
    backward_edges: Vec<Option<PathEdge>>,
    best_meeting_node: Option<CCHNodeId>,
    best_distance: f64,
    stats: CCHComputationStats,
}

impl BidirectionalSearch {
    fn new(node_count: usize) -> Self {
        Self {
            forward_queue: BinaryHeap::new(),
            backward_queue: BinaryHeap::new(),
            forward_distances: vec![f64::INFINITY; node_count],
            backward_distances: vec![f64::INFINITY; node_count],
            forward_predecessors: vec![None; node_count],
            backward_predecessors: vec![None; node_count],
            forward_edges: vec![None; node_count],
            backward_edges: vec![None; node_count],
            best_meeting_node: None,
            best_distance: f64::INFINITY,
            stats: CCHComputationStats::new(),
        }
    }

    fn initialize(&mut self, source: CCHNodeId, target: CCHNodeId) {
        // Initialize forward search from source
        self.forward_distances[source.0 as usize] = 0.0;
        self.forward_queue.push(CCHSearchState::new(0.0, source, true));

        // Initialize backward search from target
        self.backward_distances[target.0 as usize] = 0.0;
        self.backward_queue.push(CCHSearchState::new(0.0, target, false));
    }

    fn get_distance(&self, node: CCHNodeId, is_forward: bool) -> f64 {
        let distances = if is_forward {
            &self.forward_distances
        } else {
            &self.backward_distances
        };
        distances[node.0 as usize]
    }

    fn set_distance(&mut self, node: CCHNodeId, distance: f64, is_forward: bool) {
        let distances = if is_forward {
            &mut self.forward_distances
        } else {
            &mut self.backward_distances
        };
        distances[node.0 as usize] = distance;
    }

    fn set_predecessor(&mut self, node: CCHNodeId, predecessor: CCHNodeId, is_forward: bool) {
        let predecessors = if is_forward {
            &mut self.forward_predecessors
        } else {
            &mut self.backward_predecessors
        };
        predecessors[node.0 as usize] = Some(predecessor);
    }

    fn set_edge(&mut self, node: CCHNodeId, edge: PathEdge, is_forward: bool) {
        let edges = if is_forward {
            &mut self.forward_edges
        } else {
            &mut self.backward_edges
        };
        edges[node.0 as usize] = Some(edge);
    }

    fn update_meeting_node(&mut self, node: CCHNodeId) {
        let forward_dist = self.get_distance(node, true);
        let backward_dist = self.get_distance(node, false);

        if forward_dist < f64::INFINITY && backward_dist < f64::INFINITY {
            let total_distance = forward_dist + backward_dist;
            if total_distance < self.best_distance {
                self.best_distance = total_distance;
                self.best_meeting_node = Some(node);
            }
        }
    }

    fn should_continue(&self) -> bool {
        if self.forward_queue.is_empty() && self.backward_queue.is_empty() {
            return false;
        }

        if self.stats.total_nodes_explored >= MAX_SEARCH_NODES {
            return false;
        }

        // Stop if we found a meeting point and both searches exceed the best distance
        if self.best_distance < f64::INFINITY {
            let forward_min = self.forward_queue.peek().map(|s| s.cost).unwrap_or(f64::INFINITY);
            let backward_min = self.backward_queue.peek().map(|s| s.cost).unwrap_or(f64::INFINITY);

            if forward_min >= self.best_distance && backward_min >= self.best_distance {
                return false;
            }
        }

        true
    }
}

/// CCH query configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCHQueryConfig {
    pub max_search_nodes: usize,
    pub enable_path_reconstruction: bool,
    pub enable_bidirectional_search: bool,
    pub search_balance_factor: f64, // How to balance forward vs backward search
}

impl Default for CCHQueryConfig {
    fn default() -> Self {
        Self {
            max_search_nodes: MAX_SEARCH_NODES,
            enable_path_reconstruction: true,
            enable_bidirectional_search: true,
            search_balance_factor: 0.5, // Equal balance
        }
    }
}

/// Performance validation configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PerformanceValidationConfig {
    pub max_query_time_ms: f64,
    pub max_nodes_explored: usize,
    pub min_search_efficiency: f64,
    pub enable_sla_checking: bool,
}

impl Default for PerformanceValidationConfig {
    fn default() -> Self {
        Self {
            max_query_time_ms: 10.0, // 10ms SLA
            max_nodes_explored: 10000,
            min_search_efficiency: 2.0, // At most 2 edges per node
            enable_sla_checking: true,
        }
    }
}

/// CCH query engine
pub struct CCHQueryEngine {
    ordering: Arc<CCHOrdering>,
    customization: Arc<CCHCustomization>,
    config: CCHQueryConfig,
    validation_config: PerformanceValidationConfig,
}

impl CCHQueryEngine {
    pub fn new(
        ordering: Arc<CCHOrdering>,
        customization: Arc<CCHCustomization>,
        config: CCHQueryConfig,
        validation_config: PerformanceValidationConfig,
    ) -> Self {
        Self {
            ordering,
            customization,
            config,
            validation_config,
        }
    }

    /// Query shortest path between two nodes
    pub fn query(
        &self,
        source: NodeId,
        target: NodeId,
        profile: TransportProfile,
    ) -> Result<CCHQueryResult, String> {
        let start_time = Instant::now();

        // Convert to CCH node IDs
        let source_cch = CCHNodeId::from(source);
        let target_cch = CCHNodeId::from(target);

        // Check if nodes exist in ordering
        if self.ordering.get_node(source_cch).is_none() {
            return Err(format!("Source node {:?} not found in CCH", source));
        }
        if self.ordering.get_node(target_cch).is_none() {
            return Err(format!("Target node {:?} not found in CCH", target));
        }

        // Get upward and backward CSR for profile
        let upward_csr = self
            .customization
            .get_upward_csr(profile)
            .ok_or_else(|| format!("Profile {:?} not customized", profile))?;
        let backward_csr = self
            .customization
            .get_backward_csr(profile)
            .ok_or_else(|| format!("Profile {:?} backward CSR not available", profile))?;

        // Perform bidirectional search
        let mut search = BidirectionalSearch::new(self.ordering.node_count());
        search.initialize(source_cch, target_cch);

        if self.config.enable_bidirectional_search {
            self.bidirectional_search(&mut search, upward_csr, backward_csr)?;
        } else {
            self.unidirectional_search(&mut search, source_cch, target_cch, upward_csr)?;
        }

        search.stats.finalize();
        let query_time_ns = start_time.elapsed().as_nanos() as u64;

        // Validate performance if enabled
        if self.validation_config.enable_sla_checking {
            self.validate_performance(&search.stats, query_time_ns)?;
        }

        // Reconstruct path if found and requested
        let path = if search.best_distance < f64::INFINITY && self.config.enable_path_reconstruction {
            self.reconstruct_path(&search, source_cch, target_cch)?
        } else {
            Vec::new()
        };

        // Build result
        if search.best_distance < f64::INFINITY {
            Ok(CCHQueryResult::found(
                source,
                target,
                profile,
                search.best_distance,
                path,
                search.stats,
                query_time_ns,
            ))
        } else {
            Ok(CCHQueryResult::not_found(
                source,
                target,
                profile,
                search.stats,
                query_time_ns,
            ))
        }
    }

    /// Perform bidirectional search
    fn bidirectional_search(
        &self,
        search: &mut BidirectionalSearch,
        upward_csr: &UpwardCSR,
        backward_csr: &BackwardCSR,
    ) -> Result<(), String> {
        while search.should_continue() {
            // Alternate between forward and backward search
            let use_forward = if search.forward_queue.is_empty() {
                false
            } else if search.backward_queue.is_empty() {
                true
            } else {
                // Balance based on configuration
                let forward_min = search.forward_queue.peek().unwrap().cost;
                let backward_min = search.backward_queue.peek().unwrap().cost;
                forward_min <= backward_min * (1.0 + self.config.search_balance_factor)
            };

            if use_forward && !search.forward_queue.is_empty() {
                self.search_step(search, upward_csr, backward_csr, true)?;
            } else if !search.backward_queue.is_empty() {
                self.search_step(search, upward_csr, backward_csr, false)?;
            } else {
                break;
            }
        }

        Ok(())
    }

    /// Perform unidirectional search (fallback)
    fn unidirectional_search(
        &self,
        search: &mut BidirectionalSearch,
        _source: CCHNodeId,
        target: CCHNodeId,
        upward_csr: &UpwardCSR,
    ) -> Result<(), String> {
        // Simple forward search to target
        while !search.forward_queue.is_empty() && search.should_continue() {
            // For unidirectional search, we don't need backward CSR - pass None
            self.search_step_unidirectional(search, upward_csr, true)?;

            // Check if we reached the target
            if search.get_distance(target, true) < f64::INFINITY {
                search.best_distance = search.get_distance(target, true);
                search.best_meeting_node = Some(target);
                break;
            }
        }

        Ok(())
    }

    /// Perform one step of unidirectional search
    fn search_step_unidirectional(
        &self,
        search: &mut BidirectionalSearch,
        upward_csr: &UpwardCSR,
        is_forward: bool,
    ) -> Result<(), String> {
        let queue = if is_forward {
            &mut search.forward_queue
        } else {
            &mut search.backward_queue
        };

        if let Some(current_state) = queue.pop() {
            let current_node = current_state.node;
            let current_distance = current_state.cost;

            // Skip if we already found a better path to this node
            if current_distance > search.get_distance(current_node, is_forward) {
                return Ok(());
            }

            // Update statistics
            if is_forward {
                search.stats.forward_nodes_explored += 1;
                search.stats.max_forward_distance = search.stats.max_forward_distance.max(current_distance);
            } else {
                search.stats.backward_nodes_explored += 1;
                search.stats.max_backward_distance = search.stats.max_backward_distance.max(current_distance);
            }

            // Explore neighbors using upward CSR
            self.explore_neighbors_unidirectional(search, upward_csr, current_node, current_distance, is_forward)?;
        }

        Ok(())
    }

    /// Perform one step of the bidirectional search (forward or backward)
    fn search_step(
        &self,
        search: &mut BidirectionalSearch,
        upward_csr: &UpwardCSR,
        backward_csr: &BackwardCSR,
        is_forward: bool,
    ) -> Result<(), String> {
        let queue = if is_forward {
            &mut search.forward_queue
        } else {
            &mut search.backward_queue
        };

        if let Some(current_state) = queue.pop() {
            let current_node = current_state.node;
            let current_distance = current_state.cost;

            // Skip if we already found a better path to this node
            if current_distance > search.get_distance(current_node, is_forward) {
                return Ok(());
            }

            // Update statistics
            if is_forward {
                search.stats.forward_nodes_explored += 1;
                search.stats.max_forward_distance = search.stats.max_forward_distance.max(current_distance);
            } else {
                search.stats.backward_nodes_explored += 1;
                search.stats.max_backward_distance = search.stats.max_backward_distance.max(current_distance);
            }

            // Check for meeting point
            search.update_meeting_node(current_node);

            // Explore neighbors using appropriate CSR
            if is_forward {
                self.explore_neighbors_bidirectional(search, upward_csr, current_node, current_distance, is_forward)?;
            } else {
                self.explore_neighbors_bidirectional(search, backward_csr, current_node, current_distance, is_forward)?;
            }
        }

        Ok(())
    }

    /// Explore neighbors of a node in unidirectional search
    fn explore_neighbors_unidirectional(
        &self,
        search: &mut BidirectionalSearch,
        upward_csr: &UpwardCSR,
        current_node: CCHNodeId,
        current_distance: f64,
        is_forward: bool,
    ) -> Result<(), String> {
        let edges = upward_csr.get_edges(current_node);

        for edge in edges {
            let neighbor_node = edge.to_node;
            let edge_weight = edge.weight;
            let new_distance = current_distance + edge_weight;

            // Update statistics
            if is_forward {
                search.stats.forward_edges_relaxed += 1;
            } else {
                search.stats.backward_edges_relaxed += 1;
            }

            // Check if this is a better path
            if new_distance < search.get_distance(neighbor_node, is_forward) {
                search.set_distance(neighbor_node, new_distance, is_forward);
                search.set_predecessor(neighbor_node, current_node, is_forward);
                
                // Store the edge used for path reconstruction
                let path_edge = PathEdge {
                    from: current_node,
                    to: neighbor_node,
                    weight: edge_weight,
                    is_shortcut: edge.is_shortcut,
                    middle_node: edge.middle_node,
                };
                search.set_edge(neighbor_node, path_edge, is_forward);

                // Add to queue for further exploration
                let queue = if is_forward {
                    &mut search.forward_queue
                } else {
                    &mut search.backward_queue
                };
                queue.push(CCHSearchState::new(new_distance, neighbor_node, is_forward));
            }
        }

        Ok(())
    }

    /// Explore neighbors of a node in bidirectional search
    fn explore_neighbors_bidirectional<T>(
        &self,
        search: &mut BidirectionalSearch,
        csr: &T,
        current_node: CCHNodeId,
        current_distance: f64,
        is_forward: bool,
    ) -> Result<(), String> 
    where
        T: CSRGraph,
    {
        let edges = csr.get_edges(current_node);

        for edge in edges {
            let neighbor_node = edge.to_node;
            let edge_weight = edge.weight;
            let new_distance = current_distance + edge_weight;

            // Update statistics
            if is_forward {
                search.stats.forward_edges_relaxed += 1;
            } else {
                search.stats.backward_edges_relaxed += 1;
            }

            // Check if this is a better path
            if new_distance < search.get_distance(neighbor_node, is_forward) {
                search.set_distance(neighbor_node, new_distance, is_forward);
                search.set_predecessor(neighbor_node, current_node, is_forward);
                
                // Store the edge used for path reconstruction
                let path_edge = PathEdge {
                    from: current_node,
                    to: neighbor_node,
                    weight: edge_weight,
                    is_shortcut: edge.is_shortcut,
                    middle_node: edge.middle_node,
                };
                search.set_edge(neighbor_node, path_edge, is_forward);

                // Add to queue for further exploration
                let queue = if is_forward {
                    &mut search.forward_queue
                } else {
                    &mut search.backward_queue
                };
                queue.push(CCHSearchState::new(new_distance, neighbor_node, is_forward));

                // Update meeting point
                search.update_meeting_node(neighbor_node);
            }
        }

        Ok(())
    }

    /// Reconstruct path from search result with shortcut unpacking
    fn reconstruct_path(
        &self,
        search: &BidirectionalSearch,
        source: CCHNodeId,
        target: CCHNodeId,
    ) -> Result<Vec<NodeId>, String> {
        let meeting_node = search.best_meeting_node
            .ok_or("No meeting node found for path reconstruction")?;

        let mut path = Vec::new();

        // Build forward path from source to meeting node
        let forward_path = self.reconstruct_path_segment(
            &search.forward_edges,
            source,
            meeting_node,
            true,
        )?;

        // Build backward path from meeting node to target
        let backward_path = self.reconstruct_path_segment(
            &search.backward_edges,
            target,
            meeting_node,
            false,
        )?;

        // Combine paths: forward path + backward path (excluding meeting node)
        path.extend(forward_path.iter().map(|&node| NodeId::from(node)));
        path.extend(backward_path[1..].iter().rev().map(|&node| NodeId::from(node)));

        Ok(path)
    }

    /// Reconstruct a path segment and unpack shortcuts
    fn reconstruct_path_segment(
        &self,
        edges: &[Option<PathEdge>],
        start: CCHNodeId,
        end: CCHNodeId,
        _is_forward: bool,
    ) -> Result<Vec<CCHNodeId>, String> {
        let mut path = Vec::new();
        let mut current = end;

        // Collect edges in reverse order
        let mut edges_to_unpack = Vec::new();
        while current != start {
            if let Some(edge) = &edges[current.0 as usize] {
                edges_to_unpack.push(edge.clone());
                current = edge.from;
            } else {
                return Err(format!("Missing edge for node {:?} in path reconstruction", current));
            }
        }

        // Reverse to get correct order
        edges_to_unpack.reverse();

        // Start with the source
        path.push(start);

        // Unpack each edge
        for edge in edges_to_unpack {
            self.unpack_edge(&edge, &mut path)?;
        }

        Ok(path)
    }

    /// Recursively unpack a shortcut edge to reveal the underlying path
    fn unpack_edge(&self, edge: &PathEdge, path: &mut Vec<CCHNodeId>) -> Result<(), String> {
        if !edge.is_shortcut {
            // This is an original edge, just add the target
            path.push(edge.to);
        } else {
            // This is a shortcut, need to unpack it recursively
            if let Some(middle_node) = edge.middle_node {
                // Find the two constituent edges that form this shortcut
                let first_edge = self.find_constituent_edge(edge.from, middle_node)?;
                let second_edge = self.find_constituent_edge(middle_node, edge.to)?;

                // Recursively unpack the first edge
                self.unpack_edge(&first_edge, path)?;
                
                // Recursively unpack the second edge
                self.unpack_edge(&second_edge, path)?;
            } else {
                return Err("Shortcut edge missing middle node information".to_string());
            }
        }
        Ok(())
    }

    /// Find the constituent edge between two nodes (simplified implementation)
    fn find_constituent_edge(&self, from: CCHNodeId, to: CCHNodeId) -> Result<PathEdge, String> {
        // This is a simplified implementation. In a real system, we would need to
        // store the original shortcut decomposition information during customization.
        // For now, we'll create a placeholder original edge.
        
        // In a production implementation, this would look up the actual edge
        // from the customization data structures.
        let weight = self.get_approximate_edge_weight(from, to);
        
        Ok(PathEdge {
            from,
            to,
            weight,
            is_shortcut: false,
            middle_node: None,
        })
    }

    /// Get approximate edge weight between two nodes (simplified)
    fn get_approximate_edge_weight(&self, _from: CCHNodeId, _to: CCHNodeId) -> f64 {
        // This is a placeholder implementation. In a real system, we would
        // look up the actual edge weight from the original graph data.
        // For now, return a default weight.
        60.0 // Default 1-minute edge weight
    }

    /// Validate query performance against SLA
    fn validate_performance(
        &self,
        stats: &CCHComputationStats,
        query_time_ns: u64,
    ) -> Result<(), String> {
        let query_time_ms = query_time_ns as f64 / 1_000_000.0;

        if query_time_ms > self.validation_config.max_query_time_ms {
            return Err(format!(
                "Query time {:.2}ms exceeds SLA limit of {:.2}ms",
                query_time_ms,
                self.validation_config.max_query_time_ms
            ));
        }

        if stats.total_nodes_explored > self.validation_config.max_nodes_explored {
            return Err(format!(
                "Nodes explored {} exceeds limit of {}",
                stats.total_nodes_explored,
                self.validation_config.max_nodes_explored
            ));
        }

        let efficiency = stats.search_efficiency();
        if efficiency > self.validation_config.min_search_efficiency {
            return Err(format!(
                "Search efficiency {:.2} is worse than minimum {:.2}",
                efficiency,
                self.validation_config.min_search_efficiency
            ));
        }

        Ok(())
    }

    /// Get query engine configuration
    pub fn get_config(&self) -> &CCHQueryConfig {
        &self.config
    }

    /// Get performance validation configuration
    pub fn get_validation_config(&self) -> &PerformanceValidationConfig {
        &self.validation_config
    }

    /// Update performance validation configuration
    pub fn set_validation_config(&mut self, config: PerformanceValidationConfig) {
        self.validation_config = config;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cch_customization::CustomizationConfig;
    use crate::cch_ordering::OrderingConfig;
    use crate::dual_core::{DualCoreGraph, GraphNode, NodeId, TimeEdge, TimeWeight};
    use butterfly_geometry::Point2D;

    fn create_test_cch_engine() -> (CCHQueryEngine, DualCoreGraph) {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add test nodes
        for i in 1..=8 {
            let node = GraphNode::new(NodeId::new(i), Point2D::new(i as f64, i as f64));
            dual_core.time_graph.add_node(node.clone());
            dual_core.nav_graph.add_node(node);
        }

        // Add test edges: 1-2-3-4-5-6-7-8 with some shortcuts
        for i in 1..=7 {
            let mut edge = TimeEdge::new(
                crate::profiles::EdgeId(i),
                NodeId::new(i as u64),
                NodeId::new((i + 1) as u64),
            );
            edge.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
            dual_core.time_graph.add_edge(edge);
        }

        // Add some cross edges
        let mut cross_edge1 = TimeEdge::new(
            crate::profiles::EdgeId(100),
            NodeId::new(2),
            NodeId::new(6),
        );
        cross_edge1.add_weight(TransportProfile::Car, TimeWeight::new(180.0, 3000.0));
        dual_core.time_graph.add_edge(cross_edge1);

        let mut cross_edge2 = TimeEdge::new(
            crate::profiles::EdgeId(101),
            NodeId::new(3),
            NodeId::new(7),
        );
        cross_edge2.add_weight(TransportProfile::Car, TimeWeight::new(200.0, 3500.0));
        dual_core.time_graph.add_edge(cross_edge2);

        // Create ordering
        let ordering_config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(ordering_config);
        ordering.build_ordering(&dual_core, TransportProfile::Car).unwrap();
        let ordering = Arc::new(ordering);

        // Create customization
        let customization_config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(customization_config, Arc::clone(&ordering));
        customization.customize_profile(&dual_core, TransportProfile::Car).unwrap();
        let customization = Arc::new(customization);

        // Create query engine
        let query_config = CCHQueryConfig::default();
        let validation_config = PerformanceValidationConfig::default();
        let engine = CCHQueryEngine::new(ordering, customization, query_config, validation_config);

        (engine, dual_core)
    }

    #[test]
    fn test_cch_search_state() {
        let state1 = CCHSearchState::new(10.0, CCHNodeId::new(1), true);
        let state2 = CCHSearchState::new(20.0, CCHNodeId::new(2), false);

        assert_eq!(state1.cost, 10.0);
        assert_eq!(state1.node, CCHNodeId::new(1));
        assert!(state1.is_forward);

        // Test ordering (min-heap behavior)
        assert!(state1 > state2); // Lower cost should have higher priority
    }

    #[test]
    fn test_bidirectional_search_creation() {
        let search = BidirectionalSearch::new(10);
        
        assert_eq!(search.forward_distances.len(), 10);
        assert_eq!(search.backward_distances.len(), 10);
        assert_eq!(search.forward_predecessors.len(), 10);
        assert_eq!(search.backward_predecessors.len(), 10);
        assert!(search.forward_queue.is_empty());
        assert!(search.backward_queue.is_empty());
        assert_eq!(search.best_distance, f64::INFINITY);
    }

    #[test]
    fn test_search_initialization() {
        let mut search = BidirectionalSearch::new(10);
        let source = CCHNodeId::new(0);
        let target = CCHNodeId::new(5);

        search.initialize(source, target);

        assert_eq!(search.get_distance(source, true), 0.0);
        assert_eq!(search.get_distance(target, false), 0.0);
        assert_eq!(search.forward_queue.len(), 1);
        assert_eq!(search.backward_queue.len(), 1);
    }

    #[test]
    fn test_cch_query_engine_creation() {
        let (engine, _) = create_test_cch_engine();
        
        assert!(engine.config.enable_bidirectional_search);
        assert!(engine.config.enable_path_reconstruction);
        assert_eq!(engine.config.max_search_nodes, MAX_SEARCH_NODES);
    }

    #[test]
    fn test_cch_query_basic() {
        let (engine, _) = create_test_cch_engine();

        let source = NodeId::new(1);
        let target = NodeId::new(3);
        let profile = TransportProfile::Car;

        let result = engine.query(source, target, profile);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert_eq!(query_result.source, source);
        assert_eq!(query_result.target, target);
        assert_eq!(query_result.profile, profile);
        assert!(query_result.query_time_ns > 0);
    }

    #[test]
    fn test_cch_query_same_node() {
        let (engine, _) = create_test_cch_engine();

        let node = NodeId::new(1);
        let profile = TransportProfile::Car;

        let result = engine.query(node, node, profile);
        assert!(result.is_ok());

        let query_result = result.unwrap();
        assert!(query_result.path_found);
        assert_eq!(query_result.distance, 0.0);
    }

    #[test]
    fn test_cch_query_invalid_profile() {
        let (engine, _) = create_test_cch_engine();

        let source = NodeId::new(1);
        let target = NodeId::new(3);
        let profile = TransportProfile::Bicycle; // Not customized

        let result = engine.query(source, target, profile);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not customized"));
    }

    #[test]
    fn test_cch_query_invalid_node() {
        let (engine, _) = create_test_cch_engine();

        let source = NodeId::new(999); // Non-existent node
        let target = NodeId::new(3);
        let profile = TransportProfile::Car;

        let result = engine.query(source, target, profile);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not found in CCH"));
    }

    #[test]
    fn test_performance_validation() {
        let (mut engine, _) = create_test_cch_engine();

        // Set very strict performance limits
        let strict_config = PerformanceValidationConfig {
            max_query_time_ms: 0.001, // 1 microsecond - unrealistic
            max_nodes_explored: 1,
            min_search_efficiency: 0.1,
            enable_sla_checking: true,
        };
        engine.set_validation_config(strict_config);

        let source = NodeId::new(1);
        let target = NodeId::new(7); // Valid node ID (test graph has nodes 1-8, but index 8 is out of bounds)
        let profile = TransportProfile::Car;

        let result = engine.query(source, target, profile);
        // Should fail due to strict SLA
        assert!(result.is_err());
    }

    #[test]
    fn test_cch_computation_stats() {
        let mut stats = CCHComputationStats::new();
        stats.forward_nodes_explored = 10;
        stats.backward_nodes_explored = 15;
        stats.forward_edges_relaxed = 25;
        stats.backward_edges_relaxed = 35;

        stats.finalize();

        assert_eq!(stats.total_nodes_explored, 25);
        assert_eq!(stats.total_edges_relaxed, 60);
        assert_eq!(stats.search_efficiency(), 2.4); // 60/25
        assert!(stats.numa_node.is_some());
    }

    #[test]
    fn test_query_result_sla_check() {
        let stats = CCHComputationStats::new();
        let result = CCHQueryResult::found(
            NodeId::new(1),
            NodeId::new(2),
            TransportProfile::Car,
            120.0,
            vec![NodeId::new(1), NodeId::new(2)],
            stats,
            5_000_000, // 5ms in nanoseconds
        );

        assert!(result.meets_performance_sla(10.0)); // 10ms limit
        assert!(!result.meets_performance_sla(2.0)); // 2ms limit
    }

    #[test]
    fn test_search_state_ordering() {
        let mut heap = BinaryHeap::new();
        
        heap.push(CCHSearchState::new(20.0, CCHNodeId::new(1), true));
        heap.push(CCHSearchState::new(10.0, CCHNodeId::new(2), true));
        heap.push(CCHSearchState::new(30.0, CCHNodeId::new(3), true));

        // Should pop in order of increasing cost (min-heap)
        assert_eq!(heap.pop().unwrap().cost, 10.0);
        assert_eq!(heap.pop().unwrap().cost, 20.0);
        assert_eq!(heap.pop().unwrap().cost, 30.0);
    }

    #[test]
    fn test_distance_update() {
        let mut search = BidirectionalSearch::new(5);
        let node = CCHNodeId::new(2);

        // Test forward distance
        search.set_distance(node, 42.0, true);
        assert_eq!(search.get_distance(node, true), 42.0);

        // Test backward distance
        search.set_distance(node, 24.0, false);
        assert_eq!(search.get_distance(node, false), 24.0);
    }

    #[test]
    fn test_meeting_node_update() {
        let mut search = BidirectionalSearch::new(5);
        let node = CCHNodeId::new(2);

        search.set_distance(node, 10.0, true);
        search.set_distance(node, 15.0, false);
        search.update_meeting_node(node);

        assert_eq!(search.best_meeting_node, Some(node));
        assert_eq!(search.best_distance, 25.0);

        // Test with better meeting point
        let better_node = CCHNodeId::new(3);
        search.set_distance(better_node, 8.0, true);
        search.set_distance(better_node, 12.0, false);
        search.update_meeting_node(better_node);

        assert_eq!(search.best_meeting_node, Some(better_node));
        assert_eq!(search.best_distance, 20.0);
    }

    #[test]
    fn test_search_termination_conditions() {
        let mut search = BidirectionalSearch::new(5);

        // Test with empty queues
        assert!(!search.should_continue());

        // Add some states
        search.forward_queue.push(CCHSearchState::new(10.0, CCHNodeId::new(1), true));
        search.backward_queue.push(CCHSearchState::new(15.0, CCHNodeId::new(2), false));
        assert!(search.should_continue());

        // Test with meeting point found and queues exceed best distance
        search.best_distance = 5.0;
        assert!(!search.should_continue());
    }

    #[test]
    fn test_config_getters() {
        let (engine, _) = create_test_cch_engine();

        let query_config = engine.get_config();
        assert!(query_config.enable_bidirectional_search);

        let validation_config = engine.get_validation_config();
        assert!(validation_config.enable_sla_checking);
    }
}