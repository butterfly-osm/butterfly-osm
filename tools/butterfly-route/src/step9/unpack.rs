//! Shortcut unpacking - expand CCH shortcuts to original EBG edges

use crate::formats::CchTopo;

/// Unpack a path of CCH edges to original EBG edges
///
/// Forward path: sequence of (node, edge_idx) where edge_idx encodes both the
/// index and whether it's an UP or DOWN edge:
///   - edge_idx < n_up_edges: UP edge at index edge_idx
///   - edge_idx >= n_up_edges: DOWN edge at index (edge_idx - n_up_edges)
pub fn unpack_path(
    topo: &CchTopo,
    forward_path: &[(u32, u32)],  // (node, encoded_edge_idx)
    _backward_path: &[(u32, u32)], // Empty for plain Dijkstra
    source: u32,
    _target: u32,
    _meeting_node: u32,
) -> Vec<u32> {
    // Start with the source node (EBG edge)
    let mut result = vec![source];

    let n_up = topo.up_targets.len() as u32;

    // Unpack forward path
    let mut current = source;
    for &(_node, edge_idx) in forward_path {
        let (is_up, actual_idx) = if edge_idx < n_up {
            (true, edge_idx as usize)
        } else {
            (false, (edge_idx - n_up) as usize)
        };

        if is_up {
            let edges = unpack_up_edge(topo, current, actual_idx);
            result.extend(edges);
            current = topo.up_targets[actual_idx];
        } else {
            let edges = unpack_down_edge(topo, current, actual_idx);
            result.extend(edges);
            current = topo.down_targets[actual_idx];
        }
    }

    result
}

/// Unpack a single UP edge to original edges
fn unpack_up_edge(topo: &CchTopo, _source: u32, edge_idx: usize) -> Vec<u32> {
    if !topo.up_is_shortcut[edge_idx] {
        // Original edge - return the target as the edge ID
        return vec![topo.up_targets[edge_idx]];
    }

    // Shortcut - recursively unpack
    let middle = topo.up_middle[edge_idx];
    let target = topo.up_targets[edge_idx];

    // Find the two edges: source->middle and middle->target
    // source->middle is in DOWN graph (since rank(middle) < rank(source))
    // middle->target is in UP graph (since rank(target) > rank(middle))

    let mut result = Vec::new();

    // Unpack source -> middle (DOWN edge from source)
    if let Some(down_idx) = find_down_edge(topo, _source as usize, middle) {
        result.extend(unpack_down_edge(topo, _source, down_idx));
    }

    // Unpack middle -> target (UP edge from middle)
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        result.extend(unpack_up_edge(topo, middle, up_idx));
    }

    result
}

/// Unpack a single DOWN edge to original edges
fn unpack_down_edge(topo: &CchTopo, source: u32, edge_idx: usize) -> Vec<u32> {
    if !topo.down_is_shortcut[edge_idx] {
        return vec![topo.down_targets[edge_idx]];
    }

    let middle = topo.down_middle[edge_idx];
    let target = topo.down_targets[edge_idx];

    let mut result = Vec::new();

    // source -> middle (DOWN edge)
    if let Some(down_idx) = find_down_edge(topo, source as usize, middle) {
        result.extend(unpack_down_edge(topo, source, down_idx));
    }

    // middle -> target (UP edge from middle)
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        result.extend(unpack_up_edge(topo, middle, up_idx));
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
