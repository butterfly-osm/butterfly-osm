//! Shortcut unpacking - expand CCH shortcuts to original EBG edges

use crate::formats::CchTopo;

/// Unpack a path of CCH edges to original EBG edges
///
/// The CCH bidirectional search produces:
/// - forward_path: source → meeting via UP edges (each edge_idx is an UP edge index)
/// - backward_path: target → meeting via reversed DOWN edges (each edge_idx is a DOWN edge index)
///
/// The actual route is: source →(UP)→ meeting →(DOWN)→ target
/// So we process forward_path normally, then backward_path in REVERSE order.
pub fn unpack_path(
    topo: &CchTopo,
    forward_path: &[(u32, u32)], // (node, up_edge_idx) from source → meeting
    backward_path: &[(u32, u32)], // (node, down_edge_idx) from target → meeting
    source: u32,
    _target: u32,
    _meeting_node: u32,
) -> Vec<u32> {
    // Start with the source node (EBG edge)
    let mut result = vec![source];

    // === Forward part: source → meeting (UP edges) ===
    let mut current = source;
    for &(_node, edge_idx) in forward_path {
        let actual_idx = edge_idx as usize;
        let edges = unpack_up_edge(topo, current, actual_idx);
        result.extend(edges);
        current = topo.up_targets[actual_idx];
    }

    // === Backward part reversed: meeting → target (DOWN edges) ===
    // backward_path goes target→...→meeting, we reverse for meeting→target
    // Each entry (node, edge_idx) has node = source of the DOWN edge
    for &(node, edge_idx) in backward_path.iter().rev() {
        let actual_idx = edge_idx as usize;
        let edges = unpack_down_edge(topo, node, actual_idx);
        result.extend(edges);
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
    } else {
        tracing::trace!(
            source = _source,
            middle,
            "unpack_up_edge: missing DOWN sub-edge"
        );
    }

    // Unpack middle -> target (UP edge from middle)
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        result.extend(unpack_up_edge(topo, middle, up_idx));
    } else {
        tracing::trace!(middle, target, "unpack_up_edge: missing UP sub-edge");
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
    } else {
        tracing::trace!(source, middle, "unpack_down_edge: missing DOWN sub-edge");
    }

    // middle -> target (UP edge from middle)
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        result.extend(unpack_up_edge(topo, middle, up_idx));
    } else {
        tracing::trace!(middle, target, "unpack_down_edge: missing UP sub-edge");
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
