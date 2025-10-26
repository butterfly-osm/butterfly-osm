// Contraction Hierarchies implementation
//
// This module implements CH preprocessing and query algorithms for fast routing.
//
// Key concepts:
// - Node ordering: Rank nodes by importance (residential → highways)
// - Contraction: Remove nodes one-by-one, create shortcuts
// - Shortcuts: Edges that bypass contracted nodes
// - Bidirectional search: Search "up" the hierarchy from both ends

use crate::geo::haversine_distance;
use crate::graph::RouteGraph;
use anyhow::Result;
use petgraph::graph::{EdgeIndex, Graph, NodeIndex};
use petgraph::visit::EdgeRef;
use petgraph::Direction;
use serde::{Deserialize, Serialize};
use std::collections::{BinaryHeap, HashMap, HashSet};
use std::cmp::Ordering;
use std::time::Instant;

/// A shortcut edge that bypasses a contracted node
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Shortcut {
    /// The middle node that was contracted (for unpacking)
    pub via_node: NodeIndex,
    /// The original edges this shortcut replaces
    pub edge1: EdgeIndex,
    pub edge2: EdgeIndex,
}

/// Contracted graph with hierarchy information
#[derive(Debug, Serialize, Deserialize)]
pub struct CHGraph {
    /// The underlying graph (includes original + shortcut edges)
    pub graph: Graph<i64, f64>,

    /// Maps OSM ID to NodeIndex
    pub node_map: HashMap<i64, NodeIndex>,

    /// Node coordinates for heuristic calculations
    pub coords: HashMap<i64, (f64, f64)>,

    /// Node levels (importance) - higher = more important
    pub node_levels: HashMap<NodeIndex, usize>,

    /// Shortcut metadata - which edges are shortcuts and what they expand to
    pub shortcuts: HashMap<EdgeIndex, Shortcut>,
}

/// Ordering information for a node during contraction
#[derive(Debug)]
struct NodeOrdering {
    node: NodeIndex,
    priority: i32,
}

impl PartialEq for NodeOrdering {
    fn eq(&self, other: &Self) -> bool {
        self.priority == other.priority
    }
}

impl Eq for NodeOrdering {}

impl PartialOrd for NodeOrdering {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for NodeOrdering {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: lower priority values come first
        other.priority.cmp(&self.priority)
    }
}

impl CHGraph {
    /// Create a CH graph from a regular route graph
    pub fn from_route_graph(route_graph: &RouteGraph) -> Result<Self> {
        let start = Instant::now();
        println!("Starting Contraction Hierarchies preprocessing...");

        // Clone the graph and related structures
        let mut graph = route_graph.graph.clone();
        let node_map = route_graph.node_map.clone();
        let coords = route_graph.coords.clone();

        println!("Cloned graph: {} nodes, {} edges",
            graph.node_count(), graph.edge_count());

        // Initialize node levels
        let mut node_levels = HashMap::new();
        for node in graph.node_indices() {
            node_levels.insert(node, 0);
        }

        let mut shortcuts = HashMap::new();

        // Step 1: Node ordering
        println!("\n[1/2] Computing node ordering...");
        let ordering = Self::compute_node_ordering(&graph);
        println!("Node ordering computed in {:.2}s", start.elapsed().as_secs_f64());

        // Step 2: Contract nodes in order
        println!("\n[2/2] Contracting nodes and creating shortcuts...");
        let contract_start = Instant::now();
        let mut contracted = HashSet::new();

        for (level, node_order) in ordering.iter().enumerate() {
            if level % 100000 == 0 && level > 0 {
                println!("Contracted {} / {} nodes ({:.1}%), shortcuts so far: {}",
                    level, ordering.len(),
                    100.0 * level as f64 / ordering.len() as f64,
                    shortcuts.len());
            }

            let node = node_order.node;
            node_levels.insert(node, level);
            contracted.insert(node);

            // Create shortcuts for this node
            let new_shortcuts = Self::contract_node(&graph, node, &contracted);

            for (source, target, weight, via_node, edge1, edge2) in new_shortcuts {
                let edge_idx = graph.add_edge(source, target, weight);
                shortcuts.insert(edge_idx, Shortcut { via_node, edge1, edge2 });
            }
        }

        println!("Contraction completed in {:.2}s", contract_start.elapsed().as_secs_f64());
        println!("Created {} shortcuts", shortcuts.len());
        println!("Final graph: {} nodes, {} edges ({}% increase)",
            graph.node_count(),
            graph.edge_count(),
            100.0 * (graph.edge_count() as f64 / route_graph.graph.edge_count() as f64 - 1.0));

        println!("\n✓ CH preprocessing completed in {:.2}s", start.elapsed().as_secs_f64());

        Ok(CHGraph {
            graph,
            node_map,
            coords,
            node_levels,
            shortcuts,
        })
    }

    /// Compute node ordering using edge difference heuristic
    fn compute_node_ordering(graph: &Graph<i64, f64>) -> Vec<NodeOrdering> {
        let mut priorities = BinaryHeap::new();
        let mut contracted = HashSet::new();

        // Compute initial priorities for all nodes
        for node in graph.node_indices() {
            let priority = Self::compute_priority(graph, node, &contracted);
            priorities.push(NodeOrdering { node, priority });
        }

        let mut ordering = Vec::with_capacity(graph.node_count());

        // Extract nodes in priority order
        while let Some(node_order) = priorities.pop() {
            contracted.insert(node_order.node);
            ordering.push(node_order);
        }

        ordering
    }

    /// Compute priority for a node using edge difference heuristic
    /// Priority = shortcuts_added - (edges_removed / 2)
    /// Lower priority = contract earlier
    fn compute_priority(graph: &Graph<i64, f64>, node: NodeIndex, contracted: &HashSet<NodeIndex>) -> i32 {
        // Count neighbors not yet contracted
        let in_neighbors: Vec<NodeIndex> = graph
            .edges_directed(node, Direction::Incoming)
            .map(|e| e.source())
            .filter(|n| !contracted.contains(n))
            .collect();

        let out_neighbors: Vec<NodeIndex> = graph
            .edges_directed(node, Direction::Outgoing)
            .map(|e| e.target())
            .filter(|n| !contracted.contains(n))
            .collect();

        let edges_removed = in_neighbors.len() + out_neighbors.len();

        // Count how many shortcuts would be needed
        let mut shortcuts_needed = 0;
        for &in_neighbor in &in_neighbors {
            for &out_neighbor in &out_neighbors {
                if in_neighbor != out_neighbor {
                    // Would need a shortcut from in_neighbor to out_neighbor
                    // TODO: Use witness search to check if shortcut is actually needed
                    shortcuts_needed += 1;
                }
            }
        }

        // Edge difference heuristic
        // Prefer nodes that don't add many shortcuts
        (shortcuts_needed as i32) - (edges_removed as i32 / 2)
    }

    /// Contract a node and return shortcuts to create
    /// Returns: Vec<(source, target, weight, via_node, edge1, edge2)>
    fn contract_node(
        graph: &Graph<i64, f64>,
        node: NodeIndex,
        contracted: &HashSet<NodeIndex>,
    ) -> Vec<(NodeIndex, NodeIndex, f64, NodeIndex, EdgeIndex, EdgeIndex)> {
        let mut shortcuts = Vec::new();

        // Get uncontracted neighbors
        let in_edges: Vec<(NodeIndex, EdgeIndex, f64)> = graph
            .edges_directed(node, Direction::Incoming)
            .filter(|e| !contracted.contains(&e.source()))
            .map(|e| (e.source(), e.id(), *e.weight()))
            .collect();

        let out_edges: Vec<(NodeIndex, EdgeIndex, f64)> = graph
            .edges_directed(node, Direction::Outgoing)
            .filter(|e| !contracted.contains(&e.target()))
            .map(|e| (e.target(), e.id(), *e.weight()))
            .collect();

        // For each pair of (incoming, outgoing) edges, check if we need a shortcut
        for (in_neighbor, edge1, weight1) in &in_edges {
            for (out_neighbor, edge2, weight2) in &out_edges {
                if in_neighbor != out_neighbor {
                    let shortcut_weight = weight1 + weight2;

                    // TODO: Implement witness search
                    // For now, always create shortcuts
                    // A proper witness search would check if there's an alternative path
                    // that doesn't go through this node

                    shortcuts.push((
                        *in_neighbor,
                        *out_neighbor,
                        shortcut_weight,
                        node,
                        *edge1,
                        *edge2,
                    ));
                }
            }
        }

        shortcuts
    }

    /// Query CH graph using bidirectional Dijkstra
    /// Searches "upward" in the hierarchy from both start and goal
    pub fn query(&self, start_osm: i64, goal_osm: i64) -> Option<(f64, Vec<NodeIndex>)> {
        let start_idx = *self.node_map.get(&start_osm)?;
        let goal_idx = *self.node_map.get(&goal_osm)?;

        println!("CH query from {} to {}", start_osm, goal_osm);
        let query_start = Instant::now();

        // Bidirectional Dijkstra on CH graph
        // Forward search: only follow edges to nodes of higher level
        // Backward search: only follow edges from nodes of higher level

        let mut forward_dist: HashMap<NodeIndex, f64> = HashMap::new();
        let mut backward_dist: HashMap<NodeIndex, f64> = HashMap::new();
        let mut forward_prev: HashMap<NodeIndex, (NodeIndex, EdgeIndex)> = HashMap::new();
        let mut backward_prev: HashMap<NodeIndex, (NodeIndex, EdgeIndex)> = HashMap::new();

        let mut forward_heap = BinaryHeap::new();
        let mut backward_heap = BinaryHeap::new();

        forward_dist.insert(start_idx, 0.0);
        backward_dist.insert(goal_idx, 0.0);
        forward_heap.push(DijkstraState { node: start_idx, cost: 0.0 });
        backward_heap.push(DijkstraState { node: goal_idx, cost: 0.0 });

        let mut best_dist = f64::INFINITY;
        let mut meeting_node = None;

        let mut iterations = 0;

        // Alternate between forward and backward search
        loop {
            iterations += 1;

            // Forward step
            if let Some(current) = forward_heap.pop() {
                if current.cost > best_dist {
                    // Can't improve, stop forward search
                } else {
                    let current_level = self.node_levels[&current.node];

                    // Check if we've met the backward search
                    if let Some(&back_dist) = backward_dist.get(&current.node) {
                        let total_dist = current.cost + back_dist;
                        if total_dist < best_dist {
                            best_dist = total_dist;
                            meeting_node = Some(current.node);
                        }
                    }

                    // Explore neighbors (only upward)
                    for edge in self.graph.edges(current.node) {
                        let neighbor = edge.target();
                        let neighbor_level = self.node_levels[&neighbor];

                        // Only go "up" the hierarchy
                        if neighbor_level >= current_level {
                            let new_cost = current.cost + edge.weight();

                            if new_cost < *forward_dist.get(&neighbor).unwrap_or(&f64::INFINITY) {
                                forward_dist.insert(neighbor, new_cost);
                                forward_prev.insert(neighbor, (current.node, edge.id()));
                                forward_heap.push(DijkstraState { node: neighbor, cost: new_cost });
                            }
                        }
                    }
                }
            }

            // Backward step
            if let Some(current) = backward_heap.pop() {
                if current.cost > best_dist {
                    // Can't improve, stop backward search
                } else {
                    let current_level = self.node_levels[&current.node];

                    // Check if we've met the forward search
                    if let Some(&fwd_dist) = forward_dist.get(&current.node) {
                        let total_dist = fwd_dist + current.cost;
                        if total_dist < best_dist {
                            best_dist = total_dist;
                            meeting_node = Some(current.node);
                        }
                    }

                    // Explore neighbors (only upward, searching backwards)
                    for edge in self.graph.edges_directed(current.node, Direction::Incoming) {
                        let neighbor = edge.source();
                        let neighbor_level = self.node_levels[&neighbor];

                        // Only go "up" the hierarchy
                        if neighbor_level >= current_level {
                            let new_cost = current.cost + edge.weight();

                            if new_cost < *backward_dist.get(&neighbor).unwrap_or(&f64::INFINITY) {
                                backward_dist.insert(neighbor, new_cost);
                                backward_prev.insert(neighbor, (current.node, edge.id()));
                                backward_heap.push(DijkstraState { node: neighbor, cost: new_cost });
                            }
                        }
                    }
                }
            }

            // Stop when both heaps are empty or can't improve
            if (forward_heap.is_empty() || forward_heap.peek().map_or(true, |s| s.cost > best_dist))
                && (backward_heap.is_empty() || backward_heap.peek().map_or(true, |s| s.cost > best_dist)) {
                break;
            }
        }

        println!("CH query completed in {:.3}s", query_start.elapsed().as_secs_f64());
        println!("Iterations: {}, Best distance: {:.1}m", iterations, best_dist);

        if let Some(meet) = meeting_node {
            // Reconstruct path
            let path = self.reconstruct_path(start_idx, goal_idx, meet, &forward_prev, &backward_prev);
            Some((best_dist, path))
        } else {
            None
        }
    }

    /// Reconstruct path from meeting node
    fn reconstruct_path(
        &self,
        start: NodeIndex,
        goal: NodeIndex,
        meeting_node: NodeIndex,
        forward_prev: &HashMap<NodeIndex, (NodeIndex, EdgeIndex)>,
        backward_prev: &HashMap<NodeIndex, (NodeIndex, EdgeIndex)>,
    ) -> Vec<NodeIndex> {
        let mut path = Vec::new();

        // Forward path (start -> meeting_node)
        let mut current = meeting_node;
        let mut forward_path = vec![current];
        while current != start {
            if let Some(&(prev, _)) = forward_prev.get(&current) {
                forward_path.push(prev);
                current = prev;
            } else {
                break;
            }
        }
        forward_path.reverse();
        path.extend(forward_path);

        // Backward path (meeting_node -> goal)
        current = meeting_node;
        while current != goal {
            if let Some(&(next, _)) = backward_prev.get(&current) {
                path.push(next);
                current = next;
            } else {
                break;
            }
        }

        path
    }
}

/// State for Dijkstra's algorithm in CH query
#[derive(Clone, Debug)]
struct DijkstraState {
    node: NodeIndex,
    cost: f64,
}

impl PartialEq for DijkstraState {
    fn eq(&self, other: &Self) -> bool {
        self.cost == other.cost
    }
}

impl Eq for DijkstraState {}

impl PartialOrd for DijkstraState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for DijkstraState {
    fn cmp(&self, other: &Self) -> Ordering {
        // Min-heap: reverse ordering
        other.cost.partial_cmp(&self.cost).unwrap_or(Ordering::Equal)
    }
}
