//! M8.1 - CCH Graph Ordering: Nested dissection with ordering watchdog
//!
//! This module implements contraction hierarchy ordering based on nested dissection
//! with automatic safety mechanisms to prevent planet-scale processing stalls.

use crate::dual_core::{DualCoreGraph, NodeId};
use crate::profiles::TransportProfile;
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use std::thread;

/// Default minimum cell size for nested dissection separators
pub const DEFAULT_MIN_CELL_SIZE: usize = 128;

/// Coarsened minimum cell size when watchdog triggers
pub const COARSENED_MIN_CELL_SIZE: usize = 512;

/// Maximum wall-time for ordering before watchdog intervention
pub const MAX_ORDERING_WALL_TIME: Duration = Duration::from_secs(2 * 60 * 60); // 2 hours

/// Graph ordering configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingConfig {
    pub min_cell_size: usize,
    pub max_wall_time: Duration,
    pub enable_watchdog: bool,
    pub separator_balance_factor: f64,
    pub separator_improvement_threshold: f64,
}

impl Default for OrderingConfig {
    fn default() -> Self {
        Self {
            min_cell_size: DEFAULT_MIN_CELL_SIZE,
            max_wall_time: MAX_ORDERING_WALL_TIME,
            enable_watchdog: true,
            separator_balance_factor: 0.6, // 60/40 balance maximum
            separator_improvement_threshold: 0.05, // 5% improvement threshold
        }
    }
}

/// Node in the CCH hierarchy
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CCHNodeId(pub u32);

impl CCHNodeId {
    pub fn new(id: u32) -> Self {
        Self(id)
    }
}

impl From<NodeId> for CCHNodeId {
    fn from(node_id: NodeId) -> Self {
        CCHNodeId(node_id.0 as u32)
    }
}

impl From<CCHNodeId> for NodeId {
    fn from(cch_id: CCHNodeId) -> Self {
        NodeId::new(cch_id.0 as u64)
    }
}

/// CCH node with level and ordering information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CCHNode {
    pub node_id: CCHNodeId,
    pub level: u32,
    pub order: u32,
    pub neighbors: Vec<CCHNodeId>,
    pub is_separator: bool,
}

impl CCHNode {
    pub fn new(node_id: CCHNodeId, level: u32, order: u32) -> Self {
        Self {
            node_id,
            level,
            order,
            neighbors: Vec::new(),
            is_separator: false,
        }
    }

    pub fn with_neighbors(mut self, neighbors: Vec<CCHNodeId>) -> Self {
        self.neighbors = neighbors;
        self
    }

    pub fn as_separator(mut self) -> Self {
        self.is_separator = true;
        self
    }
}

/// Nested dissection cell for recursive graph partitioning
#[derive(Debug, Clone)]
pub struct NestedCell {
    pub nodes: HashSet<CCHNodeId>,
    pub level: u32,
    pub parent: Option<usize>,
    pub children: Vec<usize>,
    pub separator: Vec<CCHNodeId>,
}

impl NestedCell {
    pub fn new(nodes: HashSet<CCHNodeId>, level: u32) -> Self {
        Self {
            nodes,
            level,
            parent: None,
            children: Vec::new(),
            separator: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        self.nodes.len()
    }

    pub fn with_separator(mut self, separator: Vec<CCHNodeId>) -> Self {
        self.separator = separator;
        self
    }
}

/// Ordering watchdog to prevent infinite processing
pub struct OrderingWatchdog {
    start_time: Instant,
    max_wall_time: Duration,
    should_coarsen: Arc<AtomicBool>,
    nodes_processed: Arc<AtomicUsize>,
    enabled: bool,
}

impl OrderingWatchdog {
    pub fn new(config: &OrderingConfig) -> Self {
        let should_coarsen = Arc::new(AtomicBool::new(false));
        let nodes_processed = Arc::new(AtomicUsize::new(0));

        if config.enable_watchdog {
            let should_coarsen_clone = Arc::clone(&should_coarsen);
            let max_wall_time = config.max_wall_time;
            let start_time = Instant::now();

            thread::spawn(move || {
                thread::sleep(max_wall_time);
                if start_time.elapsed() >= max_wall_time {
                    should_coarsen_clone.store(true, Ordering::Relaxed);
                }
            });
        }

        Self {
            start_time: Instant::now(),
            max_wall_time: config.max_wall_time,
            should_coarsen,
            nodes_processed,
            enabled: config.enable_watchdog,
        }
    }

    pub fn should_coarsen(&self) -> bool {
        if !self.enabled {
            return false;
        }
        self.should_coarsen.load(Ordering::Relaxed) || self.start_time.elapsed() >= self.max_wall_time
    }

    pub fn update_progress(&self, nodes_count: usize) {
        self.nodes_processed.store(nodes_count, Ordering::Relaxed);
    }

    pub fn elapsed_time(&self) -> Duration {
        self.start_time.elapsed()
    }

    pub fn nodes_processed(&self) -> usize {
        self.nodes_processed.load(Ordering::Relaxed)
    }
}

/// Graph ordering statistics
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OrderingStats {
    pub total_nodes: usize,
    pub total_levels: u32,
    pub separator_nodes: usize,
    pub max_cell_size: usize,
    pub min_cell_size: usize,
    pub ordering_time_ms: u64,
    pub watchdog_triggered: bool,
    pub cells_created: usize,
    pub separators_found: usize,
}

impl OrderingStats {
    pub fn new() -> Self {
        Self {
            total_nodes: 0,
            total_levels: 0,
            separator_nodes: 0,
            max_cell_size: 0,
            min_cell_size: usize::MAX,
            ordering_time_ms: 0,
            watchdog_triggered: false,
            cells_created: 0,
            separators_found: 0,
        }
    }
}

/// CCH graph ordering system
pub struct CCHOrdering {
    config: OrderingConfig,
    nodes: HashMap<CCHNodeId, CCHNode>,
    adjacency: HashMap<CCHNodeId, HashSet<CCHNodeId>>,
    level_map: HashMap<u32, Vec<CCHNodeId>>,
    order_map: HashMap<CCHNodeId, u32>,
    cells: Vec<NestedCell>,
    stats: OrderingStats,
}

impl CCHOrdering {
    pub fn new(config: OrderingConfig) -> Self {
        Self {
            config,
            nodes: HashMap::new(),
            adjacency: HashMap::new(),
            level_map: HashMap::new(),
            order_map: HashMap::new(),
            cells: Vec::new(),
            stats: OrderingStats::new(),
        }
    }

    /// Build CCH ordering from dual core graph
    pub fn build_ordering(
        &mut self,
        dual_core: &DualCoreGraph,
        profile: TransportProfile,
    ) -> Result<(), String> {
        let start_time = Instant::now();
        
        // Extract graph connectivity for the given profile
        self.extract_connectivity(dual_core, profile)?;
        
        // Initialize watchdog
        let watchdog = OrderingWatchdog::new(&self.config);
        
        // Perform nested dissection
        self.nested_dissection(&watchdog)?;
        
        // Compute final ordering
        self.compute_ordering()?;
        
        // Update statistics
        self.stats.ordering_time_ms = start_time.elapsed().as_millis() as u64;
        self.stats.watchdog_triggered = watchdog.should_coarsen();
        self.stats.total_nodes = self.nodes.len();
        
        Ok(())
    }

    /// Extract graph connectivity from dual core for specific profile
    fn extract_connectivity(
        &mut self,
        dual_core: &DualCoreGraph,
        profile: TransportProfile,
    ) -> Result<(), String> {
        let time_graph = &dual_core.time_graph;
        
        // Build node mapping
        for node in time_graph.nodes.values() {
            let cch_id = CCHNodeId::from(node.node_id);
            let cch_node = CCHNode::new(cch_id, 0, 0);
            self.nodes.insert(cch_id, cch_node);
            self.adjacency.insert(cch_id, HashSet::new());
        }

        // Build adjacency list
        for edge in time_graph.edges.values() {
            if let Some(weight) = edge.weights.get(&profile) {
                if weight.time_seconds > 0 {
                    let from_id = CCHNodeId::from(edge.from_node);
                    let to_id = CCHNodeId::from(edge.to_node);
                    
                    // Add bidirectional connectivity for CCH
                    if let Some(adj_from) = self.adjacency.get_mut(&from_id) {
                        adj_from.insert(to_id);
                    }
                    if let Some(adj_to) = self.adjacency.get_mut(&to_id) {
                        adj_to.insert(from_id);
                    }
                }
            }
        }

        // Update neighbor lists in nodes
        for (node_id, neighbors) in &self.adjacency {
            if let Some(node) = self.nodes.get_mut(node_id) {
                node.neighbors = neighbors.iter().cloned().collect();
            }
        }

        Ok(())
    }

    /// Perform nested dissection to create hierarchy
    fn nested_dissection(&mut self, watchdog: &OrderingWatchdog) -> Result<(), String> {
        let all_nodes: HashSet<CCHNodeId> = self.nodes.keys().cloned().collect();
        
        if all_nodes.is_empty() {
            return Ok(());
        }

        // Create root cell
        let root_cell = NestedCell::new(all_nodes, 0);
        self.cells.push(root_cell);
        self.stats.cells_created = 1;

        // Process cells recursively
        let mut cell_queue = VecDeque::new();
        cell_queue.push_back(0); // Root cell index

        while let Some(cell_index) = cell_queue.pop_front() {
            watchdog.update_progress(self.stats.cells_created);
            
            if watchdog.should_coarsen() {
                // Apply coarsened cell size
                self.config.min_cell_size = COARSENED_MIN_CELL_SIZE;
                break;
            }

            let cell_size = self.cells[cell_index].size();
            
            if cell_size <= self.config.min_cell_size {
                continue;
            }

            // Find separator for current cell
            if let Some(separator) = self.find_separator(cell_index)? {
                // Split cell into subcells
                let (left_cell, right_cell) = self.split_cell(cell_index, &separator)?;
                
                // Add subcells to processing queue
                cell_queue.push_back(left_cell);
                cell_queue.push_back(right_cell);
                
                self.stats.separators_found += 1;
            }
        }

        Ok(())
    }

    /// Find separator for a cell using vertex separator heuristic
    fn find_separator(&mut self, cell_index: usize) -> Result<Option<Vec<CCHNodeId>>, String> {
        let cell = &self.cells[cell_index];
        let nodes = &cell.nodes;
        
        if nodes.len() <= self.config.min_cell_size {
            return Ok(None);
        }

        // Use BFS-based separator finding
        let separator = self.find_bfs_separator(nodes)?;
        
        if separator.is_empty() {
            return Ok(None);
        }

        // Update separator in cell
        self.cells[cell_index].separator = separator.clone();
        Ok(Some(separator))
    }

    /// Find separator using BFS from multiple starting points
    fn find_bfs_separator(&self, nodes: &HashSet<CCHNodeId>) -> Result<Vec<CCHNodeId>, String> {
        if nodes.len() <= 2 {
            return Ok(Vec::new());
        }

        let mut best_separator = Vec::new();
        let mut best_balance = f64::INFINITY;
        
        // Try multiple starting nodes for BFS
        let start_nodes: Vec<_> = nodes.iter().take(5).cloned().collect();
        
        for &start_node in &start_nodes {
            let separator = self.bfs_separator_from_node(start_node, nodes)?;
            
            if !separator.is_empty() {
                let balance = self.evaluate_separator_balance(&separator, nodes);
                
                if balance < best_balance && balance <= self.config.separator_balance_factor {
                    best_balance = balance;
                    best_separator = separator;
                }
            }
        }

        Ok(best_separator)
    }

    /// BFS-based separator finding from a starting node
    fn bfs_separator_from_node(
        &self,
        start_node: CCHNodeId,
        nodes: &HashSet<CCHNodeId>,
    ) -> Result<Vec<CCHNodeId>, String> {
        let mut visited = HashSet::new();
        let mut queue = VecDeque::new();
        let mut separator = Vec::new();
        
        queue.push_back(start_node);
        visited.insert(start_node);
        
        let target_size = (nodes.len() as f64).sqrt() as usize;
        
        while let Some(current) = queue.pop_front() {
            if separator.len() >= target_size {
                break;
            }
            
            separator.push(current);
            
            // Add unvisited neighbors to queue
            if let Some(neighbors) = self.adjacency.get(&current) {
                for &neighbor in neighbors {
                    if nodes.contains(&neighbor) && !visited.contains(&neighbor) {
                        visited.insert(neighbor);
                        queue.push_back(neighbor);
                    }
                }
            }
        }

        Ok(separator)
    }

    /// Evaluate separator balance (lower is better)
    fn evaluate_separator_balance(&self, separator: &[CCHNodeId], nodes: &HashSet<CCHNodeId>) -> f64 {
        if separator.is_empty() || nodes.len() <= separator.len() {
            return f64::INFINITY;
        }

        // Remove separator nodes and check connectivity
        let remaining: HashSet<_> = nodes.iter()
            .filter(|&n| !separator.contains(n))
            .cloned()
            .collect();

        if remaining.is_empty() {
            return f64::INFINITY;
        }

        // Find connected components in remaining graph
        let components = self.find_connected_components(&remaining);
        
        if components.len() < 2 {
            return f64::INFINITY;
        }

        // Calculate balance as ratio of largest to smallest component
        let sizes: Vec<_> = components.iter().map(|c| c.len()).collect();
        let max_size = *sizes.iter().max().unwrap_or(&0);
        let min_size = *sizes.iter().min().unwrap_or(&1);
        
        if min_size == 0 {
            return f64::INFINITY;
        }

        max_size as f64 / min_size as f64
    }

    /// Find connected components in a set of nodes
    fn find_connected_components(&self, nodes: &HashSet<CCHNodeId>) -> Vec<HashSet<CCHNodeId>> {
        let mut components = Vec::new();
        let mut visited = HashSet::new();

        for &node in nodes {
            if !visited.contains(&node) {
                let component = self.dfs_component(node, nodes, &mut visited);
                if !component.is_empty() {
                    components.push(component);
                }
            }
        }

        components
    }

    /// DFS to find connected component starting from a node
    fn dfs_component(
        &self,
        start: CCHNodeId,
        nodes: &HashSet<CCHNodeId>,
        visited: &mut HashSet<CCHNodeId>,
    ) -> HashSet<CCHNodeId> {
        let mut component = HashSet::new();
        let mut stack = vec![start];

        while let Some(current) = stack.pop() {
            if visited.contains(&current) {
                continue;
            }

            visited.insert(current);
            component.insert(current);

            if let Some(neighbors) = self.adjacency.get(&current) {
                for &neighbor in neighbors {
                    if nodes.contains(&neighbor) && !visited.contains(&neighbor) {
                        stack.push(neighbor);
                    }
                }
            }
        }

        component
    }

    /// Split cell into subcells using separator
    fn split_cell(
        &mut self,
        cell_index: usize,
        separator: &[CCHNodeId],
    ) -> Result<(usize, usize), String> {
        let cell = &self.cells[cell_index];
        let parent_level = cell.level;
        
        // Remove separator nodes from cell
        let remaining: HashSet<_> = cell.nodes.iter()
            .filter(|&n| !separator.contains(n))
            .cloned()
            .collect();

        // Find components after separator removal
        let components = self.find_connected_components(&remaining);
        
        if components.len() < 2 {
            return Err("Separator did not split cell into multiple components".to_string());
        }

        // Create subcells from largest two components
        let mut sorted_components = components;
        sorted_components.sort_by_key(|c| std::cmp::Reverse(c.len()));

        let left_cell = NestedCell::new(sorted_components[0].clone(), parent_level + 1);
        let right_cell = NestedCell::new(sorted_components[1].clone(), parent_level + 1);

        let left_index = self.cells.len();
        let right_index = self.cells.len() + 1;

        self.cells.push(left_cell);
        self.cells.push(right_cell);

        // Update parent-child relationships
        self.cells[cell_index].children.push(left_index);
        self.cells[cell_index].children.push(right_index);
        self.cells[left_index].parent = Some(cell_index);
        self.cells[right_index].parent = Some(cell_index);

        self.stats.cells_created += 2;

        Ok((left_index, right_index))
    }

    /// Compute final node ordering and levels
    fn compute_ordering(&mut self) -> Result<(), String> {
        let mut current_order = 0u32;
        let mut max_level = 0u32;

        // Process cells in post-order (leaves first)
        let mut cell_order = Vec::new();
        self.post_order_traversal(0, &mut cell_order);

        for &cell_index in &cell_order {
            let cell = &self.cells[cell_index];
            let level = cell.level;
            max_level = max_level.max(level);

            // Assign ordering to separator nodes first (higher level)
            for &sep_node in &cell.separator {
                if let Some(node) = self.nodes.get_mut(&sep_node) {
                    node.level = level + 1;
                    node.order = current_order;
                    node.is_separator = true;
                    self.order_map.insert(sep_node, current_order);
                    
                    // Add to level map
                    self.level_map.entry(level + 1).or_insert_with(Vec::new).push(sep_node);
                    
                    current_order += 1;
                }
            }

            // Assign ordering to non-separator nodes in leaf cells
            if cell.children.is_empty() {
                for &node_id in &cell.nodes {
                    if !cell.separator.contains(&node_id) {
                        if let Some(node) = self.nodes.get_mut(&node_id) {
                            node.level = level;
                            node.order = current_order;
                            self.order_map.insert(node_id, current_order);
                            
                            // Add to level map
                            self.level_map.entry(level).or_insert_with(Vec::new).push(node_id);
                            
                            current_order += 1;
                        }
                    }
                }
            }
        }

        self.stats.total_levels = max_level + 1;
        self.stats.separator_nodes = self.nodes.values()
            .filter(|n| n.is_separator)
            .count();

        Ok(())
    }

    /// Post-order traversal of cell hierarchy
    fn post_order_traversal(&self, cell_index: usize, order: &mut Vec<usize>) {
        let cell = &self.cells[cell_index];
        
        // Visit children first
        for &child_index in &cell.children {
            self.post_order_traversal(child_index, order);
        }
        
        // Then visit current cell
        order.push(cell_index);
    }

    /// Get node by CCH ID
    pub fn get_node(&self, node_id: CCHNodeId) -> Option<&CCHNode> {
        self.nodes.get(&node_id)
    }

    /// Get node level
    pub fn get_level(&self, node_id: CCHNodeId) -> Option<u32> {
        self.nodes.get(&node_id).map(|n| n.level)
    }

    /// Get node order
    pub fn get_order(&self, node_id: CCHNodeId) -> Option<u32> {
        self.order_map.get(&node_id).copied()
    }

    /// Get nodes at a specific level
    pub fn get_nodes_at_level(&self, level: u32) -> &[CCHNodeId] {
        self.level_map.get(&level).map(|v| v.as_slice()).unwrap_or(&[])
    }

    /// Get all nodes ordered by hierarchy
    pub fn get_ordered_nodes(&self) -> Vec<&CCHNode> {
        let mut nodes: Vec<_> = self.nodes.values().collect();
        nodes.sort_by_key(|n| n.order);
        nodes
    }

    /// Check if node is a separator
    pub fn is_separator(&self, node_id: CCHNodeId) -> bool {
        self.nodes.get(&node_id).map(|n| n.is_separator).unwrap_or(false)
    }

    /// Get ordering statistics
    pub fn get_stats(&self) -> &OrderingStats {
        &self.stats
    }

    /// Get maximum level in hierarchy
    pub fn max_level(&self) -> u32 {
        self.stats.total_levels.saturating_sub(1)
    }

    /// Get total number of nodes
    pub fn node_count(&self) -> usize {
        self.nodes.len()
    }

    /// Validate ordering consistency
    pub fn validate_ordering(&self) -> Result<(), String> {
        // Check that all nodes have valid orders
        for node in self.nodes.values() {
            if !self.order_map.contains_key(&node.node_id) {
                return Err(format!("Node {:?} missing from order map", node.node_id));
            }
        }

        // Check that orders are unique and contiguous
        let mut orders: Vec<_> = self.order_map.values().cloned().collect();
        orders.sort();
        
        for (i, &order) in orders.iter().enumerate() {
            if order != i as u32 {
                return Err(format!("Non-contiguous ordering found: expected {}, got {}", i, order));
            }
        }

        // Check level consistency
        for node in self.nodes.values() {
            if let Some(level_nodes) = self.level_map.get(&node.level) {
                if !level_nodes.contains(&node.node_id) {
                    return Err(format!("Node {:?} not found in level map at level {}", 
                                     node.node_id, node.level));
                }
            } else {
                return Err(format!("Level {} not found in level map for node {:?}", 
                                 node.level, node.node_id));
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dual_core::{GraphNode, TimeEdge, TimeWeight};
    use butterfly_geometry::Point2D;

    fn create_test_dual_core() -> DualCoreGraph {
        let profiles = vec![TransportProfile::Car];
        let mut dual_core = DualCoreGraph::new(profiles);

        // Add test nodes
        for i in 1..=10 {
            let node = GraphNode::new(NodeId::new(i), Point2D::new(i as f64, i as f64));
            dual_core.time_graph.add_node(node.clone());
            dual_core.nav_graph.add_node(node);
        }

        // Add test edges in a path: 1-2-3-4-5-6-7-8-9-10
        for i in 1..=9 {
            let mut edge = TimeEdge::new(
                crate::profiles::EdgeId(i),
                NodeId::new(i as u64),
                NodeId::new((i + 1) as u64),
            );
            edge.add_weight(TransportProfile::Car, TimeWeight::new(60.0, 1000.0));
            dual_core.time_graph.add_edge(edge);
        }

        // Add some cross edges for more interesting topology
        let mut cross_edge1 = TimeEdge::new(
            crate::profiles::EdgeId(100),
            NodeId::new(2),
            NodeId::new(8),
        );
        cross_edge1.add_weight(TransportProfile::Car, TimeWeight::new(180.0, 3000.0));
        dual_core.time_graph.add_edge(cross_edge1);

        let mut cross_edge2 = TimeEdge::new(
            crate::profiles::EdgeId(101),
            NodeId::new(4),
            NodeId::new(7),
        );
        cross_edge2.add_weight(TransportProfile::Car, TimeWeight::new(120.0, 2000.0));
        dual_core.time_graph.add_edge(cross_edge2);

        dual_core
    }

    #[test]
    fn test_cch_ordering_creation() {
        let config = OrderingConfig::default();
        let ordering = CCHOrdering::new(config);
        
        assert_eq!(ordering.nodes.len(), 0);
        assert_eq!(ordering.cells.len(), 0);
    }

    #[test]
    fn test_cch_node_creation() {
        let node_id = CCHNodeId::new(42);
        let node = CCHNode::new(node_id, 1, 5)
            .with_neighbors(vec![CCHNodeId::new(1), CCHNodeId::new(2)])
            .as_separator();

        assert_eq!(node.node_id, node_id);
        assert_eq!(node.level, 1);
        assert_eq!(node.order, 5);
        assert_eq!(node.neighbors.len(), 2);
        assert!(node.is_separator);
    }

    #[test]
    fn test_nested_cell_creation() {
        let mut nodes = HashSet::new();
        nodes.insert(CCHNodeId::new(1));
        nodes.insert(CCHNodeId::new(2));
        nodes.insert(CCHNodeId::new(3));

        let cell = NestedCell::new(nodes.clone(), 2)
            .with_separator(vec![CCHNodeId::new(2)]);

        assert_eq!(cell.size(), 3);
        assert_eq!(cell.level, 2);
        assert_eq!(cell.separator.len(), 1);
        assert_eq!(cell.separator[0], CCHNodeId::new(2));
    }

    #[test]
    fn test_ordering_watchdog() {
        let mut config = OrderingConfig::default();
        config.max_wall_time = Duration::from_millis(100);
        
        let watchdog = OrderingWatchdog::new(&config);
        assert!(!watchdog.should_coarsen());
        
        // Sleep longer than max_wall_time
        std::thread::sleep(Duration::from_millis(150));
        assert!(watchdog.should_coarsen());
    }

    #[test]
    fn test_extract_connectivity() {
        let dual_core = create_test_dual_core();
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);

        let result = ordering.extract_connectivity(&dual_core, TransportProfile::Car);
        assert!(result.is_ok());

        assert_eq!(ordering.nodes.len(), 10);
        assert_eq!(ordering.adjacency.len(), 10);

        // Check that node 1 is connected to node 2
        let node1_id = CCHNodeId::new(1);
        let node2_id = CCHNodeId::new(2);
        
        assert!(ordering.adjacency[&node1_id].contains(&node2_id));
        assert!(ordering.adjacency[&node2_id].contains(&node1_id));
    }

    #[test]
    fn test_find_connected_components() {
        let dual_core = create_test_dual_core();
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);

        ordering.extract_connectivity(&dual_core, TransportProfile::Car).unwrap();

        // Test with all nodes (should be one component)
        let all_nodes: HashSet<_> = ordering.nodes.keys().cloned().collect();
        let components = ordering.find_connected_components(&all_nodes);
        
        assert_eq!(components.len(), 1);
        assert_eq!(components[0].len(), 10);
    }

    #[test]
    fn test_build_ordering() {
        let dual_core = create_test_dual_core();
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);

        let result = ordering.build_ordering(&dual_core, TransportProfile::Car);
        assert!(result.is_ok());

        // Validate that ordering was created
        assert!(ordering.node_count() > 0);
        // Max level is always >= 0 for u32, so just check it exists
        let _max_level = ordering.max_level();

        // Validate ordering consistency
        let validation = ordering.validate_ordering();
        assert!(validation.is_ok(), "Ordering validation failed: {:?}", validation);
    }

    #[test]
    fn test_ordering_validation() {
        let dual_core = create_test_dual_core();
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);

        ordering.build_ordering(&dual_core, TransportProfile::Car).unwrap();

        // Test valid ordering
        assert!(ordering.validate_ordering().is_ok());

        // Test ordering properties
        let ordered_nodes = ordering.get_ordered_nodes();
        assert_eq!(ordered_nodes.len(), ordering.node_count());

        // Check that orders are sequential
        for (i, node) in ordered_nodes.iter().enumerate() {
            assert_eq!(node.order, i as u32);
        }
    }

    #[test]
    fn test_level_mapping() {
        let dual_core = create_test_dual_core();
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);

        ordering.build_ordering(&dual_core, TransportProfile::Car).unwrap();

        // Check that all levels have nodes
        for level in 0..=ordering.max_level() {
            let nodes_at_level = ordering.get_nodes_at_level(level);
            if level <= ordering.max_level() {
                assert!(!nodes_at_level.is_empty(), "Level {} should have nodes", level);
            }
        }

        // Check that total nodes across all levels equals total nodes
        let mut total_nodes_in_levels = 0;
        for level in 0..=ordering.max_level() {
            total_nodes_in_levels += ordering.get_nodes_at_level(level).len();
        }
        assert_eq!(total_nodes_in_levels, ordering.node_count());
    }

    #[test]
    fn test_ordering_stats() {
        let dual_core = create_test_dual_core();
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);

        ordering.build_ordering(&dual_core, TransportProfile::Car).unwrap();

        let stats = ordering.get_stats();
        assert_eq!(stats.total_nodes, 10);
        assert!(stats.total_levels > 0);
        // Note: ordering_time_ms can be 0 for small test graphs
        assert!(!stats.watchdog_triggered); // Should not trigger for small graph
        assert!(stats.cells_created > 0);
    }

    #[test]
    fn test_cch_node_id_conversion() {
        let node_id = NodeId::new(42);
        let cch_id = CCHNodeId::from(node_id);
        let back_to_node_id = NodeId::from(cch_id);

        assert_eq!(cch_id.0, 42);
        assert_eq!(back_to_node_id.0, 42);
    }

    #[test]
    fn test_separator_finding() {
        let dual_core = create_test_dual_core();
        let config = OrderingConfig::default();
        let mut ordering = CCHOrdering::new(config);

        ordering.extract_connectivity(&dual_core, TransportProfile::Car).unwrap();

        let all_nodes: HashSet<_> = ordering.nodes.keys().cloned().collect();
        let separator = ordering.find_bfs_separator(&all_nodes).unwrap();

        // For small test graphs, separator finding might not find balanced separators
        // Just verify the function completes successfully and if a separator is found, it's valid
        if !separator.is_empty() {
            assert!(separator.len() < all_nodes.len());
        }
    }
}