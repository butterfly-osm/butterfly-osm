//! NBG CH weight customization
//!
//! Applies edge weights to the contracted NBG CH topology.

use anyhow::Result;

use crate::formats::NbgCsr;
use super::contraction::NbgChTopo;
use super::ordering::NbgNdOrdering;

/// Customized NBG CH weights
#[derive(Debug, Clone)]
pub struct NbgChWeights {
    pub n_up_edges: u64,
    pub n_down_edges: u64,

    /// UP edge weights (same order as up_heads)
    pub up_weights: Vec<u32>,

    /// DOWN edge weights (same order as down_heads)
    pub down_weights: Vec<u32>,
}

/// Customize NBG CH with edge weights
pub fn customize_nbg_ch(
    nbg_csr: &NbgCsr,
    topo: &NbgChTopo,
    ordering: &NbgNdOrdering,
    base_weights: &[u32],  // Indexed by edge index in NBG
) -> Result<NbgChWeights> {
    println!("Customizing NBG CH ({} UP, {} DOWN edges)...",
             topo.n_up_edges, topo.n_down_edges);

    let n_nodes = topo.n_nodes as usize;

    // Build edge weight lookup: for each (u, v) pair, store the weight
    // Note: NBG edges are undirected but stored as directed pairs
    let mut edge_weights: Vec<Vec<(u32, u32)>> = vec![Vec::new(); n_nodes]; // (neighbor, weight)

    for u in 0..n_nodes {
        let start = nbg_csr.offsets[u] as usize;
        let end = nbg_csr.offsets[u + 1] as usize;
        for i in start..end {
            let v = nbg_csr.heads[i];
            let edge_idx = nbg_csr.edge_idx[i] as usize;
            let weight = base_weights[edge_idx];
            edge_weights[u].push((v, weight));
        }
    }

    // Compute shortcut weights bottom-up (low rank to high rank)
    // Store weights for all edges including shortcuts
    let mut all_weights: Vec<Vec<(u32, u32)>> = vec![Vec::new(); n_nodes]; // (neighbor, weight)

    // First pass: copy original edge weights
    for u in 0..n_nodes {
        for &(v, w) in &edge_weights[u] {
            all_weights[u].push((v, w));
        }
    }

    // Second pass: compute shortcut weights in rank order
    println!("  Computing shortcut weights...");
    for rank in 0..n_nodes {
        let node = ordering.inv_perm[rank];

        // For each shortcut that uses this node as middle, compute its weight
        // Shortcuts were created as (u, mid=node, v) so weight = w(u,node) + w(node,v)

        // Find all edges incident to this node
        let edges: Vec<(u32, u32)> = all_weights[node as usize].clone();

        // For each pair of higher-ranked neighbors connected through this node
        for (u, w_to_u) in &edges {
            let u_rank = ordering.perm[*u as usize];
            if u_rank <= rank as u32 {
                continue;
            }

            for (v, w_to_v) in &edges {
                if *v <= *u {
                    continue;  // Avoid duplicates
                }

                let v_rank = ordering.perm[*v as usize];
                if v_rank <= rank as u32 {
                    continue;
                }

                // Check if shortcut (u, v) through node exists
                let shortcut_weight = w_to_u.saturating_add(*w_to_v);

                // Update or add the shortcut weight
                if let Some(entry) = all_weights[*u as usize].iter_mut().find(|(n, _)| *n == *v) {
                    entry.1 = entry.1.min(shortcut_weight);
                } else {
                    all_weights[*u as usize].push((*v, shortcut_weight));
                }

                if let Some(entry) = all_weights[*v as usize].iter_mut().find(|(n, _)| *n == *u) {
                    entry.1 = entry.1.min(shortcut_weight);
                } else {
                    all_weights[*v as usize].push((*u, shortcut_weight));
                }
            }
        }
    }

    // Build UP and DOWN weight arrays matching the topology
    let mut up_weights: Vec<u32> = Vec::with_capacity(topo.n_up_edges as usize);
    let mut down_weights: Vec<u32> = Vec::with_capacity(topo.n_down_edges as usize);

    for u in 0..n_nodes {
        let start = topo.up_offsets[u] as usize;
        let end = topo.up_offsets[u + 1] as usize;

        for i in start..end {
            let v = topo.up_heads[i];

            // Find weight for (u, v)
            let weight = all_weights[u].iter()
                .find(|(n, _)| *n == v)
                .map(|(_, w)| *w)
                .unwrap_or(u32::MAX);

            up_weights.push(weight);
        }
    }

    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;

        for i in start..end {
            let v = topo.down_heads[i];

            // Find weight for (u, v)
            let weight = all_weights[u].iter()
                .find(|(n, _)| *n == v)
                .map(|(_, w)| *w)
                .unwrap_or(u32::MAX);

            down_weights.push(weight);
        }
    }

    // Verify
    let n_inf_up = up_weights.iter().filter(|&&w| w == u32::MAX).count();
    let n_inf_down = down_weights.iter().filter(|&&w| w == u32::MAX).count();

    if n_inf_up > 0 || n_inf_down > 0 {
        println!("  WARNING: {} UP edges and {} DOWN edges have infinite weight",
                 n_inf_up, n_inf_down);
    }

    println!("  Customization complete");

    Ok(NbgChWeights {
        n_up_edges: topo.n_up_edges,
        n_down_edges: topo.n_down_edges,
        up_weights,
        down_weights,
    })
}
