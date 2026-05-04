//! Shortcut unpacking - expand CCH shortcuts to original EBG edges

use crate::formats::{CchTopo, CchWeights};

/// Unpack a path of CCH edges to original EBG edges.
///
/// Uses the RELAXED middles from CchWeights (post-triangle-relaxation) instead
/// of the topology middles (from contraction). This ensures the unpacked path
/// follows the actual shortest path, not the initial contraction decomposition.
pub fn unpack_path(
    topo: &CchTopo,
    weights: &CchWeights,
    forward_path: &[(u32, u32)], // (node, up_edge_idx) from source → meeting
    backward_path: &[(u32, u32)], // (node, down_edge_idx) from target → meeting
    source: u32,
    _target: u32,
    _meeting_node: u32,
) -> Vec<u32> {
    let mut result = vec![source];

    // === Forward part: source → meeting (UP edges) ===
    let mut current = source;
    for &(_node, edge_idx) in forward_path {
        let actual_idx = edge_idx as usize;
        let edges = unpack_up_edge(topo, weights, current, actual_idx);
        result.extend(edges);
        current = topo.up_targets[actual_idx];
    }

    // === Backward part reversed: meeting → target (DOWN edges) ===
    for &(node, edge_idx) in backward_path.iter().rev() {
        let actual_idx = edge_idx as usize;
        let edges = unpack_down_edge(topo, weights, node, actual_idx);
        result.extend(edges);
    }

    result
}

/// Unpack a single UP edge to original edges
fn unpack_up_edge(topo: &CchTopo, weights: &CchWeights, source: u32, edge_idx: usize) -> Vec<u32> {
    if !topo.up_is_shortcut.bit(edge_idx) {
        return vec![topo.up_targets[edge_idx]];
    }

    // Use relaxed middle from weights (optimal), falling back to topo middle
    let middle = if edge_idx < weights.up_middle.len() {
        weights.up_middle[edge_idx]
    } else {
        topo.up_middle[edge_idx]
    };
    let target = topo.up_targets[edge_idx];

    let mut result = Vec::new();

    // source -> middle (DOWN edge)
    if let Some(down_idx) = find_down_edge(topo, source as usize, middle) {
        result.extend(unpack_down_edge(topo, weights, source, down_idx));
    }

    // middle -> target (UP edge)
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        result.extend(unpack_up_edge(topo, weights, middle, up_idx));
    }

    result
}

/// Unpack a single DOWN edge to original edges
fn unpack_down_edge(
    topo: &CchTopo,
    weights: &CchWeights,
    source: u32,
    edge_idx: usize,
) -> Vec<u32> {
    if !topo.down_is_shortcut.bit(edge_idx) {
        return vec![topo.down_targets[edge_idx]];
    }

    // Use relaxed middle from weights (optimal), falling back to topo middle
    let middle = if edge_idx < weights.down_middle.len() {
        weights.down_middle[edge_idx]
    } else {
        topo.down_middle[edge_idx]
    };
    let target = topo.down_targets[edge_idx];

    let mut result = Vec::new();

    // source -> middle (DOWN edge)
    if let Some(down_idx) = find_down_edge(topo, source as usize, middle) {
        result.extend(unpack_down_edge(topo, weights, source, down_idx));
    }

    // middle -> target (UP edge)
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        result.extend(unpack_up_edge(topo, weights, middle, up_idx));
    }

    result
}

/// Find UP edge index from source to target
fn find_up_edge(topo: &CchTopo, source: usize, target: u32) -> Option<usize> {
    let start = topo.up_offsets[source] as usize;
    let end = topo.up_offsets[source + 1] as usize;
    let slice = &topo.up_targets[start..end];
    slice.binary_search(&target).ok().map(|i| start + i)
}

/// Find DOWN edge index from source to target
fn find_down_edge(topo: &CchTopo, source: usize, target: u32) -> Option<usize> {
    let start = topo.down_offsets[source] as usize;
    let end = topo.down_offsets[source + 1] as usize;
    let slice = &topo.down_targets[start..end];
    slice.binary_search(&target).ok().map(|i| start + i)
}
