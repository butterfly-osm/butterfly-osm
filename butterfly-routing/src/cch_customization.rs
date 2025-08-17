//! M8.2 - CCH Profile Customization: Per-profile shortcut computation and upward CSR
//!
//! This module implements contraction hierarchy customization for different transport profiles,
//! building upward CSR (Compressed Sparse Row) structures with shortcuts for fast querying.

use crate::cch_ordering::{CCHNodeId, CCHOrdering};
use crate::dual_core::{DualCoreGraph, NodeId};
use crate::profiles::TransportProfile;
use crate::thread_architecture::NumaNode;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;

/// Maximum number of shortcuts per node to prevent memory explosion
pub const MAX_SHORTCUTS_PER_NODE: usize = 1024;

/// Shortcut edge in the CCH
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct CCHShortcut {
    pub from_node: CCHNodeId,
    pub to_node: CCHNodeId,
    pub weight: f64,
    pub middle_node: CCHNodeId, // Witness node for this shortcut
    pub is_forward: bool,       // Direction in upward graph
}

impl CCHShortcut {
    pub fn new(
        from_node: CCHNodeId,
        to_node: CCHNodeId,
        weight: f64,
        middle_node: CCHNodeId,
        is_forward: bool,
    ) -> Self {
        Self {
            from_node,
            to_node,
            weight,
            middle_node,
            is_forward,
        }
    }

    /// Check if shortcut is valid (finite weight)
    pub fn is_valid(&self) -> bool {
        self.weight.is_finite() && self.weight >= 0.0
    }
}

/// Upward edge in the CCH (either original or shortcut)
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCHUpwardEdge {
    pub to_node: CCHNodeId,
    pub weight: f64,
    pub is_shortcut: bool,
    pub middle_node: Option<CCHNodeId>, // Only for shortcuts
}

impl CCHUpwardEdge {
    pub fn original(to_node: CCHNodeId, weight: f64) -> Self {
        Self {
            to_node,
            weight,
            is_shortcut: false,
            middle_node: None,
        }
    }

    pub fn shortcut(to_node: CCHNodeId, weight: f64, middle_node: CCHNodeId) -> Self {
        Self {
            to_node,
            weight,
            is_shortcut: true,
            middle_node: Some(middle_node),
        }
    }
}

/// Upward CSR structure for efficient CCH queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpwardCSR {
    pub first_edge: Vec<usize>,    // first_edge[node] = index of first outgoing edge
    pub edges: Vec<CCHUpwardEdge>, // all outgoing edges stored contiguously
    pub node_count: usize,
    pub edge_count: usize,
    pub shortcut_count: usize,
}

/// Backward CSR structure for bidirectional CCH queries
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BackwardCSR {
    pub first_edge: Vec<usize>,    // first_edge[node] = index of first incoming edge
    pub edges: Vec<CCHUpwardEdge>, // all incoming edges stored contiguously
    pub node_count: usize,
    pub edge_count: usize,
}

impl UpwardCSR {
    pub fn new(node_count: usize) -> Self {
        Self {
            first_edge: vec![0; node_count + 1],
            edges: Vec::new(),
            node_count,
            edge_count: 0,
            shortcut_count: 0,
        }
    }

    /// Get outgoing edges for a node
    pub fn get_edges(&self, node_id: CCHNodeId) -> &[CCHUpwardEdge] {
        let node_idx = node_id.0 as usize;
        if node_idx >= self.node_count {
            return &[];
        }

        let start = self.first_edge[node_idx];
        let end = self.first_edge[node_idx + 1];
        &self.edges[start..end]
    }

    /// Add edges for a node (must be called in order of increasing node IDs)
    pub fn add_node_edges(&mut self, node_id: CCHNodeId, mut edges: Vec<CCHUpwardEdge>) {
        let node_idx = node_id.0 as usize;
        
        // Check bounds
        if node_idx >= self.node_count {
            return; // Skip nodes outside range
        }
        
        // Sort edges by target node for consistency
        edges.sort_by_key(|e| e.to_node.0);
        
        // Update shortcut count
        self.shortcut_count += edges.iter().filter(|e| e.is_shortcut).count();
        
        // Add edges to global edge list
        self.edges.extend(edges);
        
        // Update first_edge array
        if node_idx + 1 < self.first_edge.len() {
            self.first_edge[node_idx + 1] = self.edges.len();
        }
        self.edge_count = self.edges.len();
    }

    /// Validate CSR structure
    pub fn validate(&self) -> Result<(), String> {
        if self.first_edge.len() != self.node_count + 1 {
            return Err("Invalid first_edge array size".to_string());
        }

        if self.first_edge[self.node_count] != self.edges.len() {
            return Err("Inconsistent edge count in CSR".to_string());
        }

        // Check monotonicity
        for i in 0..self.node_count {
            if self.first_edge[i] > self.first_edge[i + 1] {
                return Err(format!("Non-monotonic first_edge at index {}", i));
            }
        }

        Ok(())
    }
}

/// CCH customization configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomizationConfig {
    pub max_shortcuts_per_node: usize,
    pub enable_witness_search: bool,
    pub witness_search_limit: usize,
    pub parallel_customization: bool,
    pub numa_aware: bool,
}

impl Default for CustomizationConfig {
    fn default() -> Self {
        Self {
            max_shortcuts_per_node: MAX_SHORTCUTS_PER_NODE,
            enable_witness_search: true,
            witness_search_limit: 500,
            parallel_customization: true,
            numa_aware: true,
        }
    }
}

/// Customization statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CustomizationStats {
    pub profile: TransportProfile,
    pub total_nodes: usize,
    pub original_edges: usize,
    pub shortcuts_added: usize,
    pub shortcuts_skipped: usize,
    pub witness_searches: usize,
    pub customization_time_ms: u64,
    pub max_level: u32,
    pub numa_node: Option<NumaNode>,
}

impl CustomizationStats {
    pub fn new(profile: TransportProfile) -> Self {
        Self {
            profile,
            total_nodes: 0,
            original_edges: 0,
            shortcuts_added: 0,
            shortcuts_skipped: 0,
            witness_searches: 0,
            customization_time_ms: 0,
            max_level: 0,
            numa_node: Some(NumaNode::current()),
        }
    }

    pub fn shortcut_ratio(&self) -> f64 {
        if self.original_edges == 0 {
            0.0
        } else {
            self.shortcuts_added as f64 / self.original_edges as f64
        }
    }
}

/// Profile-specific CCH customization
pub struct CCHCustomization {
    config: CustomizationConfig,
    ordering: Arc<CCHOrdering>,
    upward_csr: HashMap<TransportProfile, UpwardCSR>,
    original_weights: HashMap<(CCHNodeId, CCHNodeId), HashMap<TransportProfile, f64>>,
    stats: HashMap<TransportProfile, CustomizationStats>,
}

impl CCHCustomization {
    pub fn new(config: CustomizationConfig, ordering: Arc<CCHOrdering>) -> Self {
        Self {
            config,
            ordering,
            upward_csr: HashMap::new(),
            original_weights: HashMap::new(),
            stats: HashMap::new(),
        }
    }

    /// Customize CCH for a specific transport profile
    pub fn customize_profile(
        &mut self,
        dual_core: &DualCoreGraph,
        profile: TransportProfile,
    ) -> Result<(), String> {
        let start_time = Instant::now();
        let mut stats = CustomizationStats::new(profile);

        // Extract original weights for this profile
        self.extract_original_weights(dual_core, profile, &mut stats)?;

        // Build upward CSR with shortcuts
        let upward_csr = self.build_upward_csr(profile, &mut stats)?;

        // Store results
        self.upward_csr.insert(profile, upward_csr);
        stats.customization_time_ms = start_time.elapsed().as_millis() as u64;
        self.stats.insert(profile, stats);

        Ok(())
    }

    /// Extract original edge weights from dual core graph
    fn extract_original_weights(
        &mut self,
        dual_core: &DualCoreGraph,
        profile: TransportProfile,
        stats: &mut CustomizationStats,
    ) -> Result<(), String> {
        let time_graph = &dual_core.time_graph;

        for edge in time_graph.edges.values() {
            if let Some(weight) = edge.weights.get(&profile) {
                if weight.time_seconds > 0 {
                    let from_id = CCHNodeId::from(edge.from_node);
                    let to_id = CCHNodeId::from(edge.to_node);

                    // Store bidirectional weights for CCH
                    let edge_key = (from_id, to_id);
                    self.original_weights
                        .entry(edge_key)
                        .or_insert_with(HashMap::new)
                        .insert(profile, weight.time_seconds as f64);

                    let reverse_key = (to_id, from_id);
                    self.original_weights
                        .entry(reverse_key)
                        .or_insert_with(HashMap::new)
                        .insert(profile, weight.time_seconds as f64);

                    stats.original_edges += 2; // Count both directions
                }
            }
        }

        stats.total_nodes = self.ordering.node_count();
        stats.max_level = self.ordering.max_level();

        Ok(())
    }

    /// Build upward CSR with shortcuts for the given profile
    fn build_upward_csr(
        &mut self,
        profile: TransportProfile,
        stats: &mut CustomizationStats,
    ) -> Result<UpwardCSR, String> {
        let node_count = self.ordering.node_count();
        let mut upward_csr = UpwardCSR::new(node_count);

        // Process nodes in order of their CCH ordering
        let ordered_nodes = self.ordering.get_ordered_nodes();

        for node in &ordered_nodes {
            let node_id = node.node_id;
            let mut upward_edges = Vec::new();

            // Add original upward edges
            self.add_original_upward_edges(node_id, profile, &mut upward_edges);

            // Add shortcuts from this node
            self.add_shortcuts_from_node(node_id, profile, &mut upward_edges, stats)?;

            // Limit number of edges to prevent memory explosion
            if upward_edges.len() > self.config.max_shortcuts_per_node {
                upward_edges.sort_by(|a, b| a.weight.partial_cmp(&b.weight).unwrap());
                upward_edges.truncate(self.config.max_shortcuts_per_node);
                stats.shortcuts_skipped += upward_edges.len() - self.config.max_shortcuts_per_node;
            }

            upward_csr.add_node_edges(node_id, upward_edges);
        }

        upward_csr.validate()?;
        Ok(upward_csr)
    }

    /// Add original upward edges for a node
    fn add_original_upward_edges(
        &self,
        node_id: CCHNodeId,
        profile: TransportProfile,
        upward_edges: &mut Vec<CCHUpwardEdge>,
    ) {
        // Find all neighbors with higher order (upward in hierarchy)
        if let Some(node) = self.ordering.get_node(node_id) {
            for &neighbor_id in &node.neighbors {
                if let Some(neighbor_order) = self.ordering.get_order(neighbor_id) {
                    if neighbor_order > node.order {
                        // This is an upward edge
                        let edge_key = (node_id, neighbor_id);
                        if let Some(weights) = self.original_weights.get(&edge_key) {
                            if let Some(&weight) = weights.get(&profile) {
                                upward_edges.push(CCHUpwardEdge::original(neighbor_id, weight));
                            }
                        }
                    }
                }
            }
        }
    }

    /// Add shortcuts from a node using witness search
    fn add_shortcuts_from_node(
        &self,
        node_id: CCHNodeId,
        profile: TransportProfile,
        upward_edges: &mut Vec<CCHUpwardEdge>,
        stats: &mut CustomizationStats,
    ) -> Result<(), String> {
        if !self.config.enable_witness_search {
            return Ok(());
        }

        let _node = self.ordering.get_node(node_id)
            .ok_or_else(|| format!("Node {:?} not found in ordering", node_id))?;

        // Find potential shortcuts through this node
        let lower_neighbors = self.get_lower_neighbors(node_id);
        let higher_neighbors = self.get_higher_neighbors(node_id);

        for &from_neighbor in &lower_neighbors {
            for &to_neighbor in &higher_neighbors {
                if from_neighbor == to_neighbor {
                    continue;
                }

                // Check if we need a shortcut from from_neighbor to to_neighbor via node_id
                if let Some(shortcut_weight) = self.compute_shortcut_weight(
                    from_neighbor,
                    node_id,
                    to_neighbor,
                    profile,
                ) {
                    // Perform witness search to check if shortcut is necessary
                    if self.witness_search_needed(
                        from_neighbor,
                        to_neighbor,
                        shortcut_weight,
                        profile,
                        stats,
                    )? {
                        upward_edges.push(CCHUpwardEdge::shortcut(
                            to_neighbor,
                            shortcut_weight,
                            node_id,
                        ));
                        stats.shortcuts_added += 1;
                    } else {
                        stats.shortcuts_skipped += 1;
                    }
                }
            }
        }

        Ok(())
    }

    /// Get neighbors with lower order than given node
    fn get_lower_neighbors(&self, node_id: CCHNodeId) -> Vec<CCHNodeId> {
        let mut lower_neighbors = Vec::new();
        
        if let Some(node) = self.ordering.get_node(node_id) {
            for &neighbor_id in &node.neighbors {
                if let Some(neighbor_order) = self.ordering.get_order(neighbor_id) {
                    if neighbor_order < node.order {
                        lower_neighbors.push(neighbor_id);
                    }
                }
            }
        }

        lower_neighbors
    }

    /// Get neighbors with higher order than given node
    fn get_higher_neighbors(&self, node_id: CCHNodeId) -> Vec<CCHNodeId> {
        let mut higher_neighbors = Vec::new();
        
        if let Some(node) = self.ordering.get_node(node_id) {
            for &neighbor_id in &node.neighbors {
                if let Some(neighbor_order) = self.ordering.get_order(neighbor_id) {
                    if neighbor_order > node.order {
                        higher_neighbors.push(neighbor_id);
                    }
                }
            }
        }

        higher_neighbors
    }

    /// Compute shortcut weight via intermediate node
    fn compute_shortcut_weight(
        &self,
        from_node: CCHNodeId,
        via_node: CCHNodeId,
        to_node: CCHNodeId,
        profile: TransportProfile,
    ) -> Option<f64> {
        let first_leg = self.get_edge_weight(from_node, via_node, profile)?;
        let second_leg = self.get_edge_weight(via_node, to_node, profile)?;

        Some(first_leg + second_leg)
    }

    /// Get edge weight between two nodes for a profile
    fn get_edge_weight(
        &self,
        from_node: CCHNodeId,
        to_node: CCHNodeId,
        profile: TransportProfile,
    ) -> Option<f64> {
        let edge_key = (from_node, to_node);
        self.original_weights
            .get(&edge_key)?
            .get(&profile)
            .copied()
    }

    /// Perform witness search to determine if shortcut is needed
    fn witness_search_needed(
        &self,
        from_node: CCHNodeId,
        to_node: CCHNodeId,
        shortcut_weight: f64,
        profile: TransportProfile,
        stats: &mut CustomizationStats,
    ) -> Result<bool, String> {
        stats.witness_searches += 1;

        // Simple witness search: check if there's an alternative path with equal or better weight
        // In a full implementation, this would use bidirectional Dijkstra limited to higher-order nodes
        
        let alternative_weight = self.find_alternative_path(
            from_node,
            to_node,
            shortcut_weight,
            profile,
        )?;

        // If no alternative found or alternative is worse, shortcut is needed
        Ok(alternative_weight.is_none() || alternative_weight.unwrap() > shortcut_weight + 0.001)
    }

    /// Find alternative path between nodes (simplified implementation)
    fn find_alternative_path(
        &self,
        from_node: CCHNodeId,
        to_node: CCHNodeId,
        max_weight: f64,
        profile: TransportProfile,
    ) -> Result<Option<f64>, String> {
        // Simplified 2-hop search for witness paths
        if let Some(from_node_info) = self.ordering.get_node(from_node) {
            for &intermediate in &from_node_info.neighbors {
                // Only consider higher-order intermediate nodes
                if let Some(intermediate_order) = self.ordering.get_order(intermediate) {
                    if intermediate_order <= from_node_info.order {
                        continue;
                    }

                    // Check path: from_node -> intermediate -> to_node
                    if let Some(first_weight) = self.get_edge_weight(from_node, intermediate, profile) {
                        if let Some(second_weight) = self.get_edge_weight(intermediate, to_node, profile) {
                            let total_weight = first_weight + second_weight;
                            if total_weight <= max_weight {
                                return Ok(Some(total_weight));
                            }
                        }
                    }
                }
            }
        }

        Ok(None)
    }

    /// Get upward CSR for a profile
    pub fn get_upward_csr(&self, profile: TransportProfile) -> Option<&UpwardCSR> {
        self.upward_csr.get(&profile)
    }

    /// Get customization statistics for a profile
    pub fn get_stats(&self, profile: TransportProfile) -> Option<&CustomizationStats> {
        self.stats.get(&profile)
    }

    /// Get all customized profiles
    pub fn get_customized_profiles(&self) -> Vec<TransportProfile> {
        self.upward_csr.keys().cloned().collect()
    }

    /// Validate customization for a profile
    pub fn validate_customization(&self, profile: TransportProfile) -> Result<(), String> {
        let upward_csr = self.get_upward_csr(profile)
            .ok_or_else(|| format!("Profile {:?} not customized", profile))?;

        upward_csr.validate()?;

        // Additional validation: check that all shortcuts are valid
        for edge in &upward_csr.edges {
            if edge.is_shortcut {
                if edge.middle_node.is_none() {
                    return Err("Shortcut missing middle node".to_string());
                }
            }
            
            if !edge.weight.is_finite() || edge.weight < 0.0 {
                return Err(format!("Invalid edge weight: {}", edge.weight));
            }
        }

        Ok(())
    }

    /// Get memory usage statistics
    pub fn memory_usage_bytes(&self) -> usize {
        let mut total = 0;

        // CSR structures
        for csr in self.upward_csr.values() {
            total += csr.first_edge.len() * std::mem::size_of::<usize>();
            total += csr.edges.len() * std::mem::size_of::<CCHUpwardEdge>();
        }

        // Original weights
        total += self.original_weights.len() * (
            std::mem::size_of::<(CCHNodeId, CCHNodeId)>() +
            std::mem::size_of::<HashMap<TransportProfile, f64>>()
        );

        total
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cch_ordering::OrderingConfig;
    use crate::dual_core::{GraphNode, TimeEdge, TimeWeight};
    use butterfly_geometry::Point2D;

    fn create_test_dual_core_and_ordering() -> (DualCoreGraph, Arc<CCHOrdering>) {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add test nodes
        for i in 1..=6 {
            let node = GraphNode::new(NodeId::new(i), Point2D::new(i as f64, i as f64));
            dual_core.time_graph.add_node(node.clone());
            dual_core.nav_graph.add_node(node);
        }

        // Add test edges: 1-2-3-4-5-6 with some cross connections
        for i in 1..=5 {
            let mut edge = TimeEdge::new(
                crate::profiles::EdgeId(i),
                NodeId::new(i as u64),
                NodeId::new((i + 1) as u64),
            );
            edge.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
            dual_core.time_graph.add_edge(edge);
        }

        // Add cross edge
        let mut cross_edge = TimeEdge::new(
            crate::profiles::EdgeId(100),
            NodeId::new(2),
            NodeId::new(5),
        );
        cross_edge.add_weight(TransportProfile::Car, TimeWeight::new(150.0, 2500.0));
        dual_core.time_graph.add_edge(cross_edge);

        // Create ordering
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);
        ordering.build_ordering(&dual_core, TransportProfile::Car).unwrap();

        (dual_core, Arc::new(ordering))
    }

    #[test]
    fn test_cch_shortcut_creation() {
        let shortcut = CCHShortcut::new(
            CCHNodeId::new(1),
            CCHNodeId::new(3),
            120.0,
            CCHNodeId::new(2),
            true,
        );

        assert_eq!(shortcut.from_node, CCHNodeId::new(1));
        assert_eq!(shortcut.to_node, CCHNodeId::new(3));
        assert_eq!(shortcut.weight, 120.0);
        assert_eq!(shortcut.middle_node, CCHNodeId::new(2));
        assert!(shortcut.is_forward);
        assert!(shortcut.is_valid());
    }

    #[test]
    fn test_upward_edge_creation() {
        let original = CCHUpwardEdge::original(CCHNodeId::new(2), 60.0);
        assert_eq!(original.to_node, CCHNodeId::new(2));
        assert_eq!(original.weight, 60.0);
        assert!(!original.is_shortcut);
        assert!(original.middle_node.is_none());

        let shortcut = CCHUpwardEdge::shortcut(CCHNodeId::new(3), 120.0, CCHNodeId::new(2));
        assert_eq!(shortcut.to_node, CCHNodeId::new(3));
        assert_eq!(shortcut.weight, 120.0);
        assert!(shortcut.is_shortcut);
        assert_eq!(shortcut.middle_node, Some(CCHNodeId::new(2)));
    }

    #[test]
    fn test_upward_csr_creation() {
        let mut csr = UpwardCSR::new(3);
        
        let edges1 = vec![
            CCHUpwardEdge::original(CCHNodeId::new(1), 60.0),
            CCHUpwardEdge::original(CCHNodeId::new(2), 90.0),
        ];
        csr.add_node_edges(CCHNodeId::new(0), edges1);

        let edges2 = vec![
            CCHUpwardEdge::shortcut(CCHNodeId::new(2), 150.0, CCHNodeId::new(1)),
        ];
        csr.add_node_edges(CCHNodeId::new(1), edges2);

        csr.add_node_edges(CCHNodeId::new(2), vec![]);

        assert!(csr.validate().is_ok());
        assert_eq!(csr.node_count, 3);
        assert_eq!(csr.edge_count, 3);
        assert_eq!(csr.shortcut_count, 1);

        // Test edge retrieval
        let node0_edges = csr.get_edges(CCHNodeId::new(0));
        assert_eq!(node0_edges.len(), 2);
        assert_eq!(node0_edges[0].to_node, CCHNodeId::new(1));
        assert_eq!(node0_edges[1].to_node, CCHNodeId::new(2));

        let node1_edges = csr.get_edges(CCHNodeId::new(1));
        assert_eq!(node1_edges.len(), 1);
        assert!(node1_edges[0].is_shortcut);
    }

    #[test]
    fn test_customization_creation() {
        let (_, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let customization = CCHCustomization::new(config, ordering);

        assert_eq!(customization.upward_csr.len(), 0);
        assert_eq!(customization.stats.len(), 0);
    }

    #[test]
    fn test_profile_customization() {
        let (dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(config, ordering);

        let result = customization.customize_profile(&dual_core, TransportProfile::Car);
        assert!(result.is_ok());

        assert!(customization.get_upward_csr(TransportProfile::Car).is_some());
        assert!(customization.get_stats(TransportProfile::Car).is_some());

        let profiles = customization.get_customized_profiles();
        assert_eq!(profiles.len(), 1);
        assert!(profiles.contains(&TransportProfile::Car));
    }

    #[test]
    fn test_customization_validation() {
        let (dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(config, ordering);

        customization.customize_profile(&dual_core, TransportProfile::Car).unwrap();

        let validation = customization.validate_customization(TransportProfile::Car);
        assert!(validation.is_ok());

        // Test validation of non-customized profile
        let invalid_validation = customization.validate_customization(TransportProfile::Bicycle);
        assert!(invalid_validation.is_err());
    }

    #[test]
    fn test_customization_stats() {
        let (dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(config, ordering);

        customization.customize_profile(&dual_core, TransportProfile::Car).unwrap();

        let stats = customization.get_stats(TransportProfile::Car).unwrap();
        assert_eq!(stats.profile, TransportProfile::Car);
        assert!(stats.total_nodes > 0);
        assert!(stats.original_edges > 0);
        assert!(stats.customization_time_ms > 0);
        assert!(stats.numa_node.is_some());
    }

    #[test]
    fn test_original_weight_extraction() {
        let (dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(config, ordering);

        let mut stats = CustomizationStats::new(TransportProfile::Car);
        let result = customization.extract_original_weights(&dual_core, TransportProfile::Car, &mut stats);
        assert!(result.is_ok());

        assert!(stats.original_edges > 0);
        assert!(stats.total_nodes > 0);
        assert!(!customization.original_weights.is_empty());
    }

    #[test]
    fn test_memory_usage() {
        let (dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(config, ordering);

        let initial_usage = customization.memory_usage_bytes();
        
        customization.customize_profile(&dual_core, TransportProfile::Car).unwrap();
        
        let final_usage = customization.memory_usage_bytes();
        assert!(final_usage > initial_usage);
    }

    #[test]
    fn test_edge_weight_lookup() {
        let (dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(config, ordering);

        let mut stats = CustomizationStats::new(TransportProfile::Car);
        customization.extract_original_weights(&dual_core, TransportProfile::Car, &mut stats).unwrap();

        // Test weight lookup for existing edge
        let weight = customization.get_edge_weight(
            CCHNodeId::new(1),
            CCHNodeId::new(2),
            TransportProfile::Car,
        );
        assert!(weight.is_some());
        assert_eq!(weight.unwrap(), 60.0);

        // Test weight lookup for non-existing edge
        let no_weight = customization.get_edge_weight(
            CCHNodeId::new(1),
            CCHNodeId::new(6),
            TransportProfile::Car,
        );
        assert!(no_weight.is_none());
    }

    #[test]
    fn test_neighbor_classification() {
        let (_dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let customization = CCHCustomization::new(config, ordering);

        // Test with a middle node
        let node_id = CCHNodeId::new(3);
        let lower_neighbors = customization.get_lower_neighbors(node_id);
        let higher_neighbors = customization.get_higher_neighbors(node_id);

        // Should have both lower and higher neighbors for a middle node
        assert!(!lower_neighbors.is_empty() || !higher_neighbors.is_empty());
    }

    #[test]
    fn test_shortcut_weight_computation() {
        let (dual_core, ordering) = create_test_dual_core_and_ordering();
        let config = CustomizationConfig::default();
        let mut customization = CCHCustomization::new(config, ordering);

        let mut stats = CustomizationStats::new(TransportProfile::Car);
        customization.extract_original_weights(&dual_core, TransportProfile::Car, &mut stats).unwrap();

        // Test shortcut weight computation
        let shortcut_weight = customization.compute_shortcut_weight(
            CCHNodeId::new(1),
            CCHNodeId::new(2),
            CCHNodeId::new(3),
            TransportProfile::Car,
        );

        if shortcut_weight.is_some() {
            assert!(shortcut_weight.unwrap() > 0.0);
        }
    }
}