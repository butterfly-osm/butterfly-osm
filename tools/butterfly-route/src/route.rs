use crate::geo::{haversine_distance, nearest_node_spatial};
use crate::graph::RouteGraph;
use anyhow::{anyhow, Result};
use petgraph::algo::astar;
use petgraph::graph::{EdgeIndex, NodeIndex};
use petgraph::visit::EdgeRef;
use std::cell::Cell;
use std::cmp::Ordering;
use std::collections::{BinaryHeap, HashMap};
use std::time::Instant;

pub struct RouteResult {
    pub distance_meters: f64,
    pub time_seconds: f64,
    pub node_count: usize,
}

// State for A* with turn restriction support
#[derive(Clone, Debug)]
struct AStarState {
    node: NodeIndex,
    prev_edge: Option<EdgeIndex>,
    cost: f64,
    estimated_total: f64,
}

impl PartialEq for AStarState {
    fn eq(&self, other: &Self) -> bool {
        self.estimated_total == other.estimated_total
    }
}

impl Eq for AStarState {}

impl PartialOrd for AStarState {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for AStarState {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse ordering for min-heap
        other.estimated_total.partial_cmp(&self.estimated_total)
            .unwrap_or(Ordering::Equal)
    }
}

// Profiling counters
#[derive(Debug, Default)]
struct ProfilingStats {
    total_iterations: usize,
    edges_explored: usize,
    restrictions_checked: usize,
    restrictions_blocked: usize,
    heuristic_calls: usize,
    heap_operations: usize,
}

// A* with turn restriction support
fn astar_with_restrictions(
    graph: &RouteGraph,
    start: NodeIndex,
    goal: NodeIndex,
    goal_coord: (f64, f64),
) -> Option<(f64, Vec<NodeIndex>)> {
    let profile_start = Instant::now();
    let mut stats = ProfilingStats::default();

    let mut open_set = BinaryHeap::new();
    let mut came_from: HashMap<NodeIndex, (NodeIndex, EdgeIndex)> = HashMap::new();
    let mut g_score: HashMap<NodeIndex, f64> = HashMap::new();

    g_score.insert(start, 0.0);

    let time_in_heuristic = Cell::new(0u128);
    let time_in_restrictions = Cell::new(0u128);
    let time_in_heap = Cell::new(0u128);

    // Heuristic function
    let heuristic = |idx: NodeIndex| -> f64 {
        let t = Instant::now();
        let result = if let Some(&osm_id) = graph.graph.node_weight(idx) {
            if let Some(&coord) = graph.coords.get(&osm_id) {
                let distance = haversine_distance(coord.0, coord.1, goal_coord.0, goal_coord.1);
                distance / 33.33 // Optimistic time estimate (120 km/h)
            } else {
                0.0
            }
        } else {
            0.0
        };
        time_in_heuristic.set(time_in_heuristic.get() + t.elapsed().as_nanos());
        result
    };

    let heap_t = Instant::now();
    stats.heuristic_calls += 1;
    open_set.push(AStarState {
        node: start,
        prev_edge: None,
        cost: 0.0,
        estimated_total: heuristic(start),
    });
    stats.heap_operations += 1;
    time_in_heap.set(time_in_heap.get() + heap_t.elapsed().as_nanos());

    while let Some(current) = open_set.pop() {
        stats.total_iterations += 1;
        stats.heap_operations += 1;

        if current.node == goal {
            let path_t = Instant::now();
            // Reconstruct path
            let mut path = vec![goal];
            let mut current_node = goal;
            while let Some(&(prev_node, _)) = came_from.get(&current_node) {
                path.push(prev_node);
                current_node = prev_node;
            }
            path.reverse();

            let total_time = profile_start.elapsed();
            eprintln!("\n=== A* Profiling Statistics ===");
            eprintln!("Total time: {:.3}s", total_time.as_secs_f64());
            eprintln!("Iterations: {}", stats.total_iterations);
            eprintln!("Edges explored: {}", stats.edges_explored);
            eprintln!("Restrictions checked: {}", stats.restrictions_checked);
            eprintln!("Restrictions blocked: {}", stats.restrictions_blocked);
            eprintln!("Heuristic calls: {}", stats.heuristic_calls);
            eprintln!("Heap operations: {}", stats.heap_operations);
            eprintln!("\nTime breakdown:");
            eprintln!("  Heuristic: {:.3}s ({:.1}%)",
                time_in_heuristic.get() as f64 / 1e9,
                100.0 * time_in_heuristic.get() as f64 / total_time.as_nanos() as f64);
            eprintln!("  Restrictions: {:.3}s ({:.1}%)",
                time_in_restrictions.get() as f64 / 1e9,
                100.0 * time_in_restrictions.get() as f64 / total_time.as_nanos() as f64);
            eprintln!("  Heap ops: {:.3}s ({:.1}%)",
                time_in_heap.get() as f64 / 1e9,
                100.0 * time_in_heap.get() as f64 / total_time.as_nanos() as f64);
            eprintln!("  Path reconstruction: {:.3}s", path_t.elapsed().as_secs_f64());
            eprintln!("================================\n");

            return Some((current.cost, path));
        }

        // Explore neighbors
        for edge in graph.graph.edges(current.node) {
            stats.edges_explored += 1;
            let neighbor = edge.target();
            let edge_cost = *edge.weight();
            let tentative_g_score = current.cost + edge_cost;

            // Check turn restrictions
            if let Some(prev_edge_idx) = current.prev_edge {
                let rest_t = Instant::now();
                stats.restrictions_checked += 1;

                // Get the way IDs for the previous edge and current edge
                if let (Some(&from_way), Some(&to_way)) = (
                    graph.edge_to_way.get(&prev_edge_idx),
                    graph.edge_to_way.get(&edge.id())
                ) {
                    // Get the OSM node ID for the current intersection
                    if let Some(&via_node_osm) = graph.graph.node_weight(current.node) {
                        // Check if this turn is restricted
                        if let Some(restricted_ways) = graph.restrictions.get(&(from_way, via_node_osm)) {
                            if restricted_ways.contains(&to_way) {
                                // This turn is restricted, skip this edge
                                stats.restrictions_blocked += 1;
                                time_in_restrictions.set(time_in_restrictions.get() + rest_t.elapsed().as_nanos());
                                continue;
                            }
                        }
                    }
                }
                time_in_restrictions.set(time_in_restrictions.get() + rest_t.elapsed().as_nanos());
            }

            if tentative_g_score < *g_score.get(&neighbor).unwrap_or(&f64::INFINITY) {
                came_from.insert(neighbor, (current.node, edge.id()));
                g_score.insert(neighbor, tentative_g_score);

                let heap_t = Instant::now();
                stats.heuristic_calls += 1;
                open_set.push(AStarState {
                    node: neighbor,
                    prev_edge: Some(edge.id()),
                    cost: tentative_g_score,
                    estimated_total: tentative_g_score + heuristic(neighbor),
                });
                stats.heap_operations += 1;
                time_in_heap.set(time_in_heap.get() + heap_t.elapsed().as_nanos());
            }
        }
    }

    None
}

pub fn find_route(
    graph: &RouteGraph,
    from: (f64, f64),
    to: (f64, f64),
) -> Result<RouteResult> {
    let total_start = Instant::now();

    // Use R-tree for fast nearest neighbor search - O(log n) instead of O(n)
    let rtree_start = Instant::now();
    let start_osm_id = nearest_node_spatial(from, &graph.spatial_index)
        .ok_or_else(|| anyhow!("Could not find start node"))?;

    let end_osm_id = nearest_node_spatial(to, &graph.spatial_index)
        .ok_or_else(|| anyhow!("Could not find end node"))?;
    let rtree_time = rtree_start.elapsed();

    let start_idx = graph.node_map.get(&start_osm_id)
        .ok_or_else(|| anyhow!("Start node not in graph"))?;

    let end_idx = graph.node_map.get(&end_osm_id)
        .ok_or_else(|| anyhow!("End node not in graph"))?;

    let goal_coord = graph.coords.get(&end_osm_id)
        .ok_or_else(|| anyhow!("Goal coordinates not found"))?;

    println!("Routing from node {} to node {}", start_osm_id, end_osm_id);

    eprintln!("\n=== Overall Profiling ===");
    eprintln!("R-tree lookups (×2): {:.6}s", rtree_time.as_secs_f64());

    // Use custom A* with turn restriction support if restrictions exist
    let routing_start = Instant::now();
    let result = if !graph.restrictions.is_empty() {
        eprintln!("Using A* with turn restrictions ({} restrictions loaded)",
            graph.restrictions.iter().map(|(_, set)| set.len()).sum::<usize>());
        astar_with_restrictions(graph, *start_idx, *end_idx, *goal_coord)
    } else {
        eprintln!("Using standard A* (no restrictions)");
        // Fall back to standard A* for better performance when no restrictions
        let heuristic = |idx: NodeIndex| -> f64 {
            if let Some(&osm_id) = graph.graph.node_weight(idx) {
                if let Some(&coord) = graph.coords.get(&osm_id) {
                    let distance = haversine_distance(coord.0, coord.1, goal_coord.0, goal_coord.1);
                    return distance / 33.33;
                }
            }
            0.0
        };

        astar(
            &graph.graph,
            *start_idx,
            |idx| idx == *end_idx,
            |e| *e.weight(),
            heuristic,
        )
    };
    let routing_time = routing_start.elapsed();

    let (time_seconds, path) = result
        .ok_or_else(|| anyhow!("No route found between points"))?;

    // Rough distance calculation (assume average speed)
    let distance_meters = time_seconds * 15.0; // ~50 km/h average

    let total_time = total_start.elapsed();
    eprintln!("\nTotal query time: {:.3}s", total_time.as_secs_f64());
    eprintln!("  R-tree: {:.3}s ({:.1}%)",
        rtree_time.as_secs_f64(),
        100.0 * rtree_time.as_secs_f64() / total_time.as_secs_f64());
    eprintln!("  Routing: {:.3}s ({:.1}%)",
        routing_time.as_secs_f64(),
        100.0 * routing_time.as_secs_f64() / total_time.as_secs_f64());
    eprintln!("========================\n");

    Ok(RouteResult {
        distance_meters,
        time_seconds,
        node_count: path.len(),
    })
}
