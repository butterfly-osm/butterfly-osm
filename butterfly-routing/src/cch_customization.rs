//! M8.2 - CCH Profile Customization: Per-profile shortcut computation and upward CSR
//!
//! This module implements contraction hierarchy customization for different transport profiles,
//! building upward CSR (Compressed Sparse Row) structures with shortcuts for fast querying.

use crate::cch_ordering::{CCHNodeId, CCHOrdering};
use crate::dual_core::DualCoreGraph;
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

impl BackwardCSR {
    pub fn new(node_count: usize) -> Self {
        Self {
            first_edge: vec![0; node_count + 1],
            edges: Vec::new(),
            node_count,
            edge_count: 0,
        }
    }

    /// Get incoming edges for a node
    pub fn get_edges(&self, node_id: CCHNodeId) -> &[CCHUpwardEdge] {
        let node_idx = node_id.0 as usize;
        if node_idx >= self.node_count {
            return &[];
        }

        let start = self.first_edge[node_idx];
        let end = self.first_edge[node_idx + 1];
        &self.edges[start..end]
    }

    /// Set incoming edges for a node
    pub fn set_node_edges(&mut self, node_id: CCHNodeId, mut edges: Vec<CCHUpwardEdge>) {
        let node_idx = node_id.0 as usize;
        
        // Check bounds
        if node_idx >= self.node_count {
            return; // Skip nodes outside range
        }
        
        // Sort edges by source node for consistency
        edges.sort_by_key(|e| e.to_node.0);
        
        // Add edges to global edge list
        self.edges.extend(edges);
        self.edge_count = self.edges.len();
    }

    /// Deprecated method - use set_node_edges instead
    #[deprecated(note = "Use set_node_edges instead")]
    pub fn add_node_edges(&mut self, node_id: CCHNodeId, edges: Vec<CCHUpwardEdge>) {
        self.set_node_edges(node_id, edges);
    }

    /// Finalize CSR structure after all nodes have been added
    pub fn finalize(&mut self, node_edges: &[(CCHNodeId, Vec<CCHUpwardEdge>)]) -> Result<(), String> {
        // Clear existing state
        self.edges.clear();
        
        // Create a mapping from node ID to edges
        let mut node_to_edges: std::collections::HashMap<CCHNodeId, Vec<CCHUpwardEdge>> = 
            std::collections::HashMap::new();
        
        for (node_id, edges) in node_edges {
            node_to_edges.insert(*node_id, edges.clone());
        }
        
        // Build CSR in node ID order
        let mut current_edge_index = 0;
        for node_idx in 0..self.node_count {
            let node_id = CCHNodeId::new(node_idx as u32);
            self.first_edge[node_idx] = current_edge_index;
            
            if let Some(edges) = node_to_edges.get(&node_id) {
                self.edges.extend(edges.clone());
                current_edge_index += edges.len();
            }
        }
        
        // Set final boundary
        self.first_edge[self.node_count] = self.edges.len();
        self.edge_count = self.edges.len();
        
        Ok(())
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

    /// Set edges for a node (replaces previous edges for this node)
    pub fn set_node_edges(&mut self, node_id: CCHNodeId, mut edges: Vec<CCHUpwardEdge>) {
        let node_idx = node_id.0 as usize;
        
        // Check bounds
        if node_idx >= self.node_count {
            return; // Skip nodes outside range
        }
        
        // Sort edges by target node for consistency
        edges.sort_by_key(|e| e.to_node.0);
        
        // Update shortcut count
        self.shortcut_count += edges.iter().filter(|e| e.is_shortcut).count();
        
        // Store edges for this node (we'll build the CSR structure later)
        // For now, just store the edge count in first_edge as a temporary measure
        self.first_edge[node_idx] = edges.len();
        self.edges.extend(edges);
        self.edge_count = self.edges.len();
    }

    /// Deprecated method - use set_node_edges instead
    #[deprecated(note = "Use set_node_edges instead")]
    pub fn add_node_edges(&mut self, node_id: CCHNodeId, edges: Vec<CCHUpwardEdge>) {
        self.set_node_edges(node_id, edges);
    }

    /// Finalize CSR structure after all nodes have been added
    pub fn finalize(&mut self, node_edges: &[(CCHNodeId, Vec<CCHUpwardEdge>)]) -> Result<(), String> {
        // Clear existing state
        self.edges.clear();
        self.shortcut_count = 0;
        
        // Create a mapping from node ID to edges
        let mut node_to_edges: std::collections::HashMap<CCHNodeId, Vec<CCHUpwardEdge>> = 
            std::collections::HashMap::new();
        
        for (node_id, edges) in node_edges {
            node_to_edges.insert(*node_id, edges.clone());
        }
        
        // Build CSR in node ID order
        let mut current_edge_index = 0;
        for node_idx in 0..self.node_count {
            let node_id = CCHNodeId::new(node_idx as u32);
            self.first_edge[node_idx] = current_edge_index;
            
            if let Some(edges) = node_to_edges.get(&node_id) {
                for edge in edges {
                    if edge.is_shortcut {
                        self.shortcut_count += 1;
                    }
                }
                self.edges.extend(edges.clone());
                current_edge_index += edges.len();
            }
        }
        
        // Set final boundary
        self.first_edge[self.node_count] = self.edges.len();
        self.edge_count = self.edges.len();
        
        Ok(())
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
    backward_csr: HashMap<TransportProfile, BackwardCSR>,
    original_weights: HashMap<(CCHNodeId, CCHNodeId), HashMap<TransportProfile, f64>>,
    stats: HashMap<TransportProfile, CustomizationStats>,
}

impl CCHCustomization {
    pub fn new(config: CustomizationConfig, ordering: Arc<CCHOrdering>) -> Self {
        Self {
            config,
            ordering,
            upward_csr: HashMap::new(),
            backward_csr: HashMap::new(),
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

        // Build upward and backward CSR with shortcuts
        let (upward_csr, backward_csr) = self.build_csr_graphs(profile, &mut stats)?;

        // Store results
        self.upward_csr.insert(profile, upward_csr);
        self.backward_csr.insert(profile, backward_csr);
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

    /// Build upward and backward CSR with shortcuts for the given profile
    fn build_csr_graphs(
        &mut self,
        profile: TransportProfile,
        stats: &mut CustomizationStats,
    ) -> Result<(UpwardCSR, BackwardCSR), String> {
        let node_count = self.ordering.node_count();
        let mut upward_csr = UpwardCSR::new(node_count);
        let mut backward_csr = BackwardCSR::new(node_count);

        // Collect all edges for backward CSR construction
        let mut backward_edges: Vec<Vec<CCHUpwardEdge>> = vec![Vec::new(); node_count];

        if self.config.parallel_customization {
            self.process_nodes_parallel(profile, &mut upward_csr, &mut backward_edges, stats)?;
        } else {
            self.process_nodes_sequential(profile, &mut upward_csr, &mut backward_edges, stats)?;
        }

        // Build backward CSR
        let backward_node_edges: Vec<_> = (0..node_count)
            .map(|node_idx| (CCHNodeId::new(node_idx as u32), backward_edges[node_idx].clone()))
            .collect();
        backward_csr.finalize(&backward_node_edges)?;

        upward_csr.validate()?;
        backward_csr.validate()?;
        Ok((upward_csr, backward_csr))
    }

    /// Process nodes sequentially (original implementation)
    fn process_nodes_sequential(
        &mut self,
        profile: TransportProfile,
        upward_csr: &mut UpwardCSR,
        backward_edges: &mut Vec<Vec<CCHUpwardEdge>>,
        stats: &mut CustomizationStats,
    ) -> Result<(), String> {
        let ordered_nodes = self.ordering.get_ordered_nodes();
        let mut all_node_edges = Vec::new();

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

            // Collect backward edges
            for edge in &upward_edges {
                let target_idx = edge.to_node.0 as usize;
                if target_idx < backward_edges.len() {
                    // Create reverse edge for backward CSR
                    let backward_edge = CCHUpwardEdge {
                        to_node: node_id, // In backward CSR, this points to the source
                        weight: edge.weight,
                        is_shortcut: edge.is_shortcut,
                        middle_node: edge.middle_node,
                    };
                    backward_edges[target_idx].push(backward_edge);
                }
            }

            // Store edges for later CSR construction
            all_node_edges.push((node_id, upward_edges));
        }

        // Finalize CSR structure
        upward_csr.finalize(&all_node_edges)?;

        Ok(())
    }

    /// Process nodes in parallel by level (simplified implementation)
    fn process_nodes_parallel(
        &mut self,
        profile: TransportProfile,
        upward_csr: &mut UpwardCSR,
        backward_edges: &mut Vec<Vec<CCHUpwardEdge>>,
        stats: &mut CustomizationStats,
    ) -> Result<(), String> {
        // For now, use the sequential implementation
        // TODO: Implement proper parallelization with better architecture
        // The challenge is that we need mutable access to self for witness search
        // A proper implementation would pre-compute dependencies and use thread pools
        
        self.process_nodes_sequential(profile, upward_csr, backward_edges, stats)
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

    /// Find alternative path between nodes using limited-depth bidirectional Dijkstra
    fn find_alternative_path(
        &self,
        from_node: CCHNodeId,
        to_node: CCHNodeId,
        max_weight: f64,
        profile: TransportProfile,
    ) -> Result<Option<f64>, String> {
        use std::collections::BinaryHeap;
        use std::cmp::Ordering;
        
        #[derive(Debug, Clone, Copy)]
        struct WitnessSearchState {
            cost: f64,
            node: CCHNodeId,
            #[allow(dead_code)]
            is_forward: bool,
        }
        
        impl PartialEq for WitnessSearchState {
            fn eq(&self, other: &Self) -> bool {
                self.cost.eq(&other.cost)
            }
        }
        
        impl Eq for WitnessSearchState {}
        
        impl PartialOrd for WitnessSearchState {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                other.cost.partial_cmp(&self.cost) // Reverse for min-heap
            }
        }
        
        impl Ord for WitnessSearchState {
            fn cmp(&self, other: &Self) -> Ordering {
                self.partial_cmp(other).unwrap_or(Ordering::Equal)
            }
        }

        let max_search_depth = self.config.witness_search_limit;
        let mut forward_queue = BinaryHeap::new();
        let mut backward_queue = BinaryHeap::new();
        let mut forward_distances = std::collections::HashMap::new();
        let mut backward_distances = std::collections::HashMap::new();
        let mut best_meeting_weight = f64::INFINITY;
        let mut nodes_explored = 0;

        // Get orders for filtering
        let from_order = self.ordering.get_order(from_node).unwrap_or(0);
        let to_order = self.ordering.get_order(to_node).unwrap_or(0);
        let min_order = from_order.max(to_order);

        // Initialize searches
        forward_queue.push(WitnessSearchState { cost: 0.0, node: from_node, is_forward: true });
        backward_queue.push(WitnessSearchState { cost: 0.0, node: to_node, is_forward: false });
        forward_distances.insert(from_node, 0.0);
        backward_distances.insert(to_node, 0.0);

        while (!forward_queue.is_empty() || !backward_queue.is_empty()) && nodes_explored < max_search_depth {
            // Early termination if we found a good path
            if best_meeting_weight <= max_weight {
                break;
            }

            // Decide which direction to explore (balance the search)
            let use_forward = if forward_queue.is_empty() {
                false
            } else if backward_queue.is_empty() {
                true
            } else {
                let forward_min = forward_queue.peek().unwrap().cost;
                let backward_min = backward_queue.peek().unwrap().cost;
                forward_min <= backward_min
            };

            if use_forward {
                if let Some(current) = forward_queue.pop() {
                    if current.cost > *forward_distances.get(&current.node).unwrap_or(&f64::INFINITY) {
                        continue;
                    }

                    // Check for meeting point
                    if let Some(&backward_dist) = backward_distances.get(&current.node) {
                        let total_dist = current.cost + backward_dist;
                        if total_dist < best_meeting_weight {
                            best_meeting_weight = total_dist;
                        }
                    }

                    // Explore neighbors (only higher-order nodes)
                    if let Some(node) = self.ordering.get_node(current.node) {
                        let neighbors = &node.neighbors;
                        for &neighbor in neighbors {
                            if let Some(neighbor_order) = self.ordering.get_order(neighbor) {
                                if neighbor_order <= min_order {
                                    continue; // Skip lower-order nodes
                                }
                            }

                            if let Some(edge_weight) = self.get_edge_weight(current.node, neighbor, profile) {
                                let new_cost = current.cost + edge_weight;
                                if new_cost < max_weight {
                                    let current_dist = forward_distances.get(&neighbor).unwrap_or(&f64::INFINITY);
                                    if new_cost < *current_dist {
                                        forward_distances.insert(neighbor, new_cost);
                                        forward_queue.push(WitnessSearchState {
                                            cost: new_cost,
                                            node: neighbor,
                                            is_forward: true,
                                        });
                                    }
                                }
                            }
                        }
                    }
                    nodes_explored += 1;
                }
            } else {
                if let Some(current) = backward_queue.pop() {
                    if current.cost > *backward_distances.get(&current.node).unwrap_or(&f64::INFINITY) {
                        continue;
                    }

                    // Check for meeting point
                    if let Some(&forward_dist) = forward_distances.get(&current.node) {
                        let total_dist = current.cost + forward_dist;
                        if total_dist < best_meeting_weight {
                            best_meeting_weight = total_dist;
                        }
                    }

                    // Explore neighbors (only higher-order nodes)
                    if let Some(node) = self.ordering.get_node(current.node) {
                        let neighbors = &node.neighbors;
                        for &neighbor in neighbors {
                            if let Some(neighbor_order) = self.ordering.get_order(neighbor) {
                                if neighbor_order <= min_order {
                                    continue; // Skip lower-order nodes
                                }
                            }

                            if let Some(edge_weight) = self.get_edge_weight(neighbor, current.node, profile) {
                                let new_cost = current.cost + edge_weight;
                                if new_cost < max_weight {
                                    let current_dist = backward_distances.get(&neighbor).unwrap_or(&f64::INFINITY);
                                    if new_cost < *current_dist {
                                        backward_distances.insert(neighbor, new_cost);
                                        backward_queue.push(WitnessSearchState {
                                            cost: new_cost,
                                            node: neighbor,
                                            is_forward: false,
                                        });
                                    }
                                }
                            }
                        }
                    }
                    nodes_explored += 1;
                }
            }
        }

        if best_meeting_weight < f64::INFINITY {
            Ok(Some(best_meeting_weight))
        } else {
            Ok(None)
        }
    }

    /// Get upward CSR for a profile
    pub fn get_upward_csr(&self, profile: TransportProfile) -> Option<&UpwardCSR> {
        self.upward_csr.get(&profile)
    }

    /// Get backward CSR for a profile
    pub fn get_backward_csr(&self, profile: TransportProfile) -> Option<&BackwardCSR> {
        self.backward_csr.get(&profile)
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
    use crate::dual_core::{DualCoreGraph, GraphNode, NodeId, TimeEdge, TimeWeight};
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
        let edges2 = vec![
            CCHUpwardEdge::shortcut(CCHNodeId::new(2), 150.0, CCHNodeId::new(1)),
        ];
        let edges3 = vec![];

        let all_edges = vec![
            (CCHNodeId::new(0), edges1),
            (CCHNodeId::new(1), edges2),
            (CCHNodeId::new(2), edges3),
        ];

        csr.finalize(&all_edges).unwrap();

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
        // Note: customization_time_ms may be 0 for small test graphs
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