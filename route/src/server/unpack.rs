//! Shortcut unpacking - expand CCH shortcuts to original EBG edges

use crate::formats::{CchTopo, CchWeights};

/// Unpack a path of CCH edges to original EBG edges. Returns a freshly
/// allocated `Vec<u32>`; prefer [`unpack_path_into`] in hot paths where
/// the caller can supply a reusable buffer.
///
/// Uses the RELAXED middles from CchWeights (post-triangle-relaxation) instead
/// of the topology middles (from contraction). This ensures the unpacked path
/// follows the actual shortest path, not the initial contraction decomposition.
pub fn unpack_path(
    topo: &CchTopo,
    weights: &CchWeights,
    forward_path: &[(u32, u32)],
    backward_path: &[(u32, u32)],
    source: u32,
    _target: u32,
    _meeting_node: u32,
) -> Vec<u32> {
    let mut out = Vec::new();
    unpack_path_into(topo, weights, forward_path, backward_path, source, &mut out);
    out
}

/// #273: in-place path unpack — appends original EBG edges to `out`.
///
/// Clears `out` first, then writes the source node followed by every
/// expanded edge. Reuses the caller's `Vec<u32>` across pairs so a
/// 10k-pair `route_batch` no longer pays N allocations for the final
/// path and ~N allocations per recursive shortcut hop.
pub fn unpack_path_into(
    topo: &CchTopo,
    weights: &CchWeights,
    forward_path: &[(u32, u32)],
    backward_path: &[(u32, u32)],
    source: u32,
    out: &mut Vec<u32>,
) {
    out.clear();
    out.push(source);

    // Forward part: source → meeting (UP edges).
    let mut current = source;
    for &(_node, edge_idx) in forward_path {
        let actual_idx = edge_idx as usize;
        unpack_up_edge_into(topo, weights, current, actual_idx, out);
        current = topo.up_targets[actual_idx];
    }

    // Backward part reversed: meeting → target (DOWN edges).
    for &(node, edge_idx) in backward_path.iter().rev() {
        let actual_idx = edge_idx as usize;
        unpack_down_edge_into(topo, weights, node, actual_idx, out);
    }
}

/// Unpack one UP edge in place — appends expanded edges to `out`.
fn unpack_up_edge_into(
    topo: &CchTopo,
    weights: &CchWeights,
    source: u32,
    edge_idx: usize,
    out: &mut Vec<u32>,
) {
    if !topo.up_is_shortcut.bit(edge_idx) {
        out.push(topo.up_targets[edge_idx]);
        return;
    }
    let middle = if edge_idx < weights.up_middle.len() {
        weights.up_middle[edge_idx]
    } else {
        topo.up_middle.get(edge_idx)
    };
    let target = topo.up_targets[edge_idx];
    if let Some(down_idx) = find_down_edge(topo, source as usize, middle) {
        unpack_down_edge_into(topo, weights, source, down_idx, out);
    }
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        unpack_up_edge_into(topo, weights, middle, up_idx, out);
    }
}

/// Unpack one DOWN edge in place — appends expanded edges to `out`.
fn unpack_down_edge_into(
    topo: &CchTopo,
    weights: &CchWeights,
    source: u32,
    edge_idx: usize,
    out: &mut Vec<u32>,
) {
    if !topo.down_is_shortcut.bit(edge_idx) {
        out.push(topo.down_targets[edge_idx]);
        return;
    }
    let middle = if edge_idx < weights.down_middle.len() {
        weights.down_middle[edge_idx]
    } else {
        topo.down_middle.get(edge_idx)
    };
    let target = topo.down_targets[edge_idx];
    if let Some(down_idx) = find_down_edge(topo, source as usize, middle) {
        unpack_down_edge_into(topo, weights, source, down_idx, out);
    }
    if let Some(up_idx) = find_up_edge(topo, middle as usize, target) {
        unpack_up_edge_into(topo, weights, middle, up_idx, out);
    }
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
