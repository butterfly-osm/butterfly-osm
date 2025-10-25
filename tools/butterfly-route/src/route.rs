use crate::geo::{haversine_distance, nearest_node_spatial};
use crate::graph::RouteGraph;
use anyhow::{anyhow, Result};
use petgraph::algo::astar;
use petgraph::graph::NodeIndex;

pub struct RouteResult {
    pub distance_meters: f64,
    pub time_seconds: f64,
    pub node_count: usize,
}

pub fn find_route(
    graph: &RouteGraph,
    from: (f64, f64),
    to: (f64, f64),
) -> Result<RouteResult> {
    // Use R-tree for fast nearest neighbor search - O(log n) instead of O(n)
    let start_osm_id = nearest_node_spatial(from, &graph.spatial_index)
        .ok_or_else(|| anyhow!("Could not find start node"))?;

    let end_osm_id = nearest_node_spatial(to, &graph.spatial_index)
        .ok_or_else(|| anyhow!("Could not find end node"))?;

    let start_idx = graph.node_map.get(&start_osm_id)
        .ok_or_else(|| anyhow!("Start node not in graph"))?;

    let end_idx = graph.node_map.get(&end_osm_id)
        .ok_or_else(|| anyhow!("End node not in graph"))?;

    let goal_coord = graph.coords.get(&end_osm_id)
        .ok_or_else(|| anyhow!("Goal coordinates not found"))?;

    println!("Routing from node {} to node {}", start_osm_id, end_osm_id);

    // A* heuristic: straight-line distance / max speed (120 km/h = 33.33 m/s)
    let heuristic = |idx: NodeIndex| -> f64 {
        if let Some(&osm_id) = graph.graph.node_weight(idx) {
            if let Some(&coord) = graph.coords.get(&osm_id) {
                let distance = haversine_distance(coord.0, coord.1, goal_coord.0, goal_coord.1);
                return distance / 33.33; // Optimistic time estimate (120 km/h)
            }
        }
        0.0
    };

    let result = astar(
        &graph.graph,
        *start_idx,
        |idx| idx == *end_idx,
        |e| *e.weight(),
        heuristic,
    );

    let (time_seconds, path) = result
        .ok_or_else(|| anyhow!("No route found between points"))?;

    // Rough distance calculation (assume average speed)
    let distance_meters = time_seconds * 15.0; // ~50 km/h average

    Ok(RouteResult {
        distance_meters,
        time_seconds,
        node_count: path.len(),
    })
}
