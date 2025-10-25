use crate::geo::nearest_node;
use crate::graph::RouteGraph;
use anyhow::{anyhow, Result};
use petgraph::algo::dijkstra;

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
    let start_osm_id = nearest_node(from, &graph.coords)
        .ok_or_else(|| anyhow!("Could not find start node"))?;

    let end_osm_id = nearest_node(to, &graph.coords)
        .ok_or_else(|| anyhow!("Could not find end node"))?;

    let start_idx = graph.node_map.get(&start_osm_id)
        .ok_or_else(|| anyhow!("Start node not in graph"))?;

    let end_idx = graph.node_map.get(&end_osm_id)
        .ok_or_else(|| anyhow!("End node not in graph"))?;

    println!("Routing from node {} to node {}", start_osm_id, end_osm_id);

    let result = dijkstra(&graph.graph, *start_idx, Some(*end_idx), |e| *e.weight());

    let time_seconds = result.get(end_idx)
        .ok_or_else(|| anyhow!("No route found between points"))?;

    // Rough distance calculation (assume average speed)
    let distance_meters = time_seconds * 15.0; // ~50 km/h average

    Ok(RouteResult {
        distance_meters,
        time_seconds: *time_seconds,
        node_count: result.len(),
    })
}
