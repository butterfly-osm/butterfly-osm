//! Lift NBG ordering to EBG (edge-state graph)
//!
//! The key insight: compute ND ordering on the physical node graph (NBG),
//! then lift to edge-states by giving all states of a physical node
//! consecutive ranks in a block.
//!
//! rank(edge_state) = base_rank(physical_node) * block_size + local_index

use anyhow::Result;
use crate::formats::{EbgCsr, EbgNodes};
use super::ordering::NbgNdOrdering;

/// Lifted ordering for EBG
pub struct LiftedEbgOrdering {
    /// Number of edge-states
    pub n_states: u32,
    /// perm[state_id] = rank
    pub perm: Vec<u32>,
    /// inv_perm[rank] = state_id
    pub inv_perm: Vec<u32>,
}

/// Lift NBG ordering to EBG ordering
///
/// Each edge-state is identified by its head node (the node it points to).
/// States with the same head node get consecutive ranks based on the
/// NBG ordering of that head node.
pub fn lift_ordering_to_ebg(
    nbg_ordering: &NbgNdOrdering,
    ebg_nodes: &EbgNodes,
    ebg_csr: &EbgCsr,
) -> Result<LiftedEbgOrdering> {
    let n_states = ebg_csr.n_nodes as usize;
    let n_nbg_nodes = nbg_ordering.n_nodes as usize;

    println!("Lifting NBG ordering to {} edge-states...", n_states);

    // Count how many edge-states each NBG node has
    // Edge-state's "physical node" is its head (the node it points to)
    let mut counts: Vec<u32> = vec![0; n_nbg_nodes];
    for state_id in 0..n_states {
        let head_node = ebg_nodes.nodes[state_id].head_nbg as usize;
        if head_node < n_nbg_nodes {
            counts[head_node] += 1;
        }
    }

    // Compute block size (max states per node)
    let block_size = *counts.iter().max().unwrap_or(&1) as usize;
    println!("  Block size: {} (max edge-states per node)", block_size);

    // Compute base offset for each NBG node in rank order
    // base_offset[nbg_node] = starting rank for states of this node
    let mut base_offset: Vec<u64> = vec![0; n_nbg_nodes];
    let mut current_offset: u64 = 0;

    for rank in 0..n_nbg_nodes {
        let nbg_node = nbg_ordering.inv_perm[rank] as usize;
        base_offset[nbg_node] = current_offset;
        current_offset += counts[nbg_node] as u64;
    }

    // Assign ranks to edge-states
    // Reset counts to use as local index
    let mut local_idx: Vec<u32> = vec![0; n_nbg_nodes];
    let mut perm: Vec<u32> = vec![0; n_states];
    let mut inv_perm: Vec<u32> = vec![0; n_states];

    for (state_id, perm_entry) in perm.iter_mut().enumerate() {
        let head_node = ebg_nodes.nodes[state_id].head_nbg as usize;
        if head_node < n_nbg_nodes {
            let rank = base_offset[head_node] + local_idx[head_node] as u64;
            *perm_entry = rank as u32;
            inv_perm[rank as usize] = state_id as u32;
            local_idx[head_node] += 1;
        }
    }

    // Verify
    let mut rank_check = vec![false; n_states];
    for &rank_val in &perm {
        let rank = rank_val as usize;
        assert!(!rank_check[rank], "Duplicate rank {}", rank);
        rank_check[rank] = true;
    }

    println!("  Lifted ordering: {} states, ranks 0..{}", n_states, n_states);

    Ok(LiftedEbgOrdering {
        n_states: n_states as u32,
        perm,
        inv_perm,
    })
}
