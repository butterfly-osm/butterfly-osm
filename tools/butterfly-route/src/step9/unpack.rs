//! Shortcut unpacking - expand CCH shortcuts to original EBG edges

use crate::formats::CchTopo;

/// Unpack a path of CCH edges to original EBG edges
///
/// Forward path: sequence of (node, up_edge_idx) - UP edges from source to meeting
/// Backward path: sequence of (node, down_edge_idx) - DOWN edges, stored in reverse (target to meeting)
pub fn unpack_path(
    topo: &CchTopo,
    forward_path: &[(u32, u32)],  // (node, up_edge_idx)
    backward_path: &[(u32, u32)], // (node, down_edge_idx)
    source: u32,
    _target: u32,
    _meeting_node: u32,
) -> Vec<u32> {
    // Start with the source node (EBG edge)
    let mut result = vec![source];

    // Unpack forward path (source -> meeting) via UP edges
    // Each unpack returns nodes from source (exclusive) to target (inclusive)
    let mut current = source;
    for &(_node, edge_idx) in forward_path {
        let edges = unpack_up_edge(topo, current, edge_idx as usize);
        result.extend(edges);
        current = topo.up_targets[edge_idx as usize];
    }

    // Unpack backward path (meeting -> target) via DOWN edges
    // backward_path is stored as (target...meeting) after reconstruct_path's reverse
    // So iter().rev() gives us meeting->target order
    //
    // Each entry (node, down_edge_idx) represents a DOWN edge FROM some higher-rank node TO node
    // In the backward search: we found node X by relaxing edge (prev -> X) in the DOWN graph
    // parent_bwd[X] = (prev, down_edge_idx of prev->X)
    //
    // After reconstruct_path(target, meeting), backward_path is [(near_target, edge), ..., (meeting, edge)]
    // reversed: [(meeting, edge), ..., (near_target, edge)]
    // Wait, that's still confusing. Let me re-derive:
    //
    // reconstruct_path walks from meeting back to target using parent_bwd:
    //   current = meeting
    //   parent_bwd[meeting] = (prev1, edge1) -- DOWN edge prev1->meeting
    //   path.push((meeting, edge1))
    //   current = prev1
    //   ... until current == target
    // Then reverse: path goes from (target-adjacent) to (meeting)
    //
    // So backward_path[i] = (node_i, down_edge_idx) where down_edge_idx is the DOWN edge
    // from parent(node_i) to node_i. The edge source is NOT node_i, it's the PARENT.
    //
    // To iterate meeting->target, we iterate backward_path in reverse:
    // - Start at meeting
    // - backward_path[-1] = (meeting, edge_to_meeting) - edge FROM prev(meeting) TO meeting
    // - We want to go FROM meeting TO prev(meeting)... but that's the opposite direction!
    //
    // The DOWN edge goes prev->meeting, but we want meeting->target direction.
    // In terms of the original EBG: DOWN edge x->y (rank(x) > rank(y)) means
    // "x can reach y with this cost". For path reconstruction meeting->target,
    // we need to follow the DOWN edges in their natural direction.
    //
    // Actually the backward search computes distances from target backward.
    // parent_bwd[x] = (u, edge) means: edge x->u in DOWN graph led us to x from u.
    // Wait no, let me re-read the query code:
    //
    //   for i in start..end {
    //       let x = self.down_rev.sources[i];       // source node of edge x→u
    //       let orig_idx = self.down_rev.edge_idx[i] as usize; // index in down_weights
    //       ...
    //       parent_bwd[x as usize] = Some((u, orig_idx as u32));
    //
    // So DOWN edge is x->u (x has higher rank than u), and we record parent_bwd[x] = (u, orig_idx).
    // The path from meeting to target follows DOWN edges: meeting->...->target.
    //
    // backward_path after reconstruct_path(target, meeting):
    //   start = target, end = meeting
    //   current = meeting
    //   parent_bwd[meeting] = (prev1, edge1) where edge1 is DOWN prev1->meeting? No wait...
    //
    // I'm getting confused. Let me just look at what DOWN edges mean:
    // - DOWN edge x->y means rank(x) > rank(y)
    // - In the backward search, we're finding paths from target to all nodes
    // - We traverse DOWN edges in REVERSE: if there's edge x->y, we update dist_bwd[x] from dist_bwd[y]
    // - This is because DOWN edges naturally flow away from high-rank nodes
    //
    // For path reconstruction from meeting to target:
    // - meeting has high rank, target has lower rank (in contraction order)
    // - We follow DOWN edges meeting->...->target
    // - parent_bwd[x] = (u, edge_idx) means "we reached x by reversing DOWN edge x->u"
    //   i.e., the DOWN edge goes x->u, and we're recording that x was reached from u
    //
    // So the actual DOWN path is x->u, meaning to go meeting->target we follow:
    //   meeting --(DOWN)--> child1 --(DOWN)--> ... --(DOWN)--> target
    //
    // reconstruct_path(target, meeting) builds:
    //   current = meeting
    //   parent_bwd[meeting] = (child1, edge_meeting_to_child1)
    //   path.push((meeting, edge_meeting_to_child1))
    //   current = child1
    //   parent_bwd[child1] = (child2, edge_child1_to_child2)
    //   path.push((child1, edge_child1_to_child2))
    //   ...
    //   until current == target
    //
    // After reverse, path is from (near target) to (meeting).
    // To iterate meeting->target, we iterate in reverse order.
    //
    // For each (node, edge_idx):
    // - edge_idx is a DOWN edge index
    // - The DOWN edge goes from the previous node in the path to this node
    // - We need to unpack this DOWN edge

    // Iterate backward_path in reverse to go meeting->target
    // Each (node, edge_idx) means: edge_idx is a DOWN edge FROM node TO some child
    // (because parent_bwd[x] = (u, edge_idx) where edge x->u is a DOWN edge)
    for &(node, edge_idx) in backward_path.iter().rev() {
        let edges = unpack_down_edge(topo, node, edge_idx as usize);
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
