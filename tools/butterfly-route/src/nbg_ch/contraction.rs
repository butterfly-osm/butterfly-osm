//! Node-based CH contraction
//!
//! Contracts the NBG according to a given ordering, creating shortcuts
//! to preserve shortest-path distances.

use anyhow::Result;
use std::collections::BinaryHeap;
use std::cmp::Reverse;

use crate::formats::NbgCsr;
use super::ordering::NbgNdOrdering;

/// NBG CH topology (contracted graph)
#[derive(Debug, Clone)]
pub struct NbgChTopo {
    pub n_nodes: u32,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub n_shortcuts: u64,

    // UP adjacency: u → higher-ranked neighbors
    pub up_offsets: Vec<u64>,
    pub up_heads: Vec<u32>,
    pub up_is_shortcut: Vec<bool>,
    pub up_shortcut_mid: Vec<u32>,  // Only valid if up_is_shortcut[i]

    // DOWN adjacency: u → lower-ranked neighbors
    pub down_offsets: Vec<u64>,
    pub down_heads: Vec<u32>,
    pub down_is_shortcut: Vec<bool>,
    pub down_shortcut_mid: Vec<u32>,
}

/// Contract NBG according to ordering
pub fn contract_nbg(
    nbg_csr: &NbgCsr,
    ordering: &NbgNdOrdering,
) -> Result<NbgChTopo> {
    let n_nodes = nbg_csr.n_nodes as usize;
    println!("Contracting NBG ({} nodes)...", n_nodes);

    // Initialize edge lists (will be sorted by rank later)
    let mut adj: Vec<Vec<(u32, bool, u32)>> = vec![Vec::new(); n_nodes]; // (neighbor, is_shortcut, mid)

    // Copy original edges (not shortcuts)
    println!("  Initializing with {} original edges...", nbg_csr.heads.len());
    for u in 0..n_nodes {
        let start = nbg_csr.offsets[u] as usize;
        let end = nbg_csr.offsets[u + 1] as usize;
        for i in start..end {
            let v = nbg_csr.heads[i];
            adj[u].push((v, false, 0));
        }
    }

    // Contract nodes in rank order (low rank first)
    println!("  Contracting nodes...");
    let mut n_shortcuts = 0u64;
    let mut last_progress = 0;

    for rank in 0..n_nodes {
        let node = ordering.inv_perm[rank];

        if rank * 100 / n_nodes > last_progress {
            last_progress = rank * 100 / n_nodes;
            if last_progress % 10 == 0 {
                println!("    {}% ({} shortcuts)", last_progress, n_shortcuts);
            }
        }

        // Find all neighbors with higher rank
        let neighbors: Vec<(u32, bool, u32)> = adj[node as usize]
            .iter()
            .filter(|(v, _, _)| ordering.perm[*v as usize] > rank as u32)
            .cloned()
            .collect();

        // For each pair of higher-ranked neighbors, potentially add shortcut
        // This is the simplest contraction strategy (no witness search)
        // A full implementation would do witness search to avoid unnecessary shortcuts
        for i in 0..neighbors.len() {
            for j in (i + 1)..neighbors.len() {
                let (u, _, _) = neighbors[i];
                let (v, _, _) = neighbors[j];

                // Add shortcut u → v and v → u through node
                adj[u as usize].push((v, true, node));
                adj[v as usize].push((u, true, node));
                n_shortcuts += 2;
            }
        }
    }

    println!("  {} shortcuts created", n_shortcuts);

    // Build UP and DOWN adjacency lists
    println!("  Building UP/DOWN adjacency...");

    let mut up_edges: Vec<Vec<(u32, bool, u32)>> = vec![Vec::new(); n_nodes];
    let mut down_edges: Vec<Vec<(u32, bool, u32)>> = vec![Vec::new(); n_nodes];

    for u in 0..n_nodes {
        let u_rank = ordering.perm[u];
        for (v, is_sc, mid) in &adj[u] {
            let v_rank = ordering.perm[*v as usize];
            if v_rank > u_rank {
                up_edges[u].push((*v, *is_sc, *mid));
            } else {
                down_edges[u].push((*v, *is_sc, *mid));
            }
        }
    }

    // Build CSR format
    let mut up_offsets = Vec::with_capacity(n_nodes + 1);
    let mut up_heads = Vec::new();
    let mut up_is_shortcut = Vec::new();
    let mut up_shortcut_mid = Vec::new();

    let mut up_offset = 0u64;
    for u in 0..n_nodes {
        up_offsets.push(up_offset);
        for (v, is_sc, mid) in &up_edges[u] {
            up_heads.push(*v);
            up_is_shortcut.push(*is_sc);
            up_shortcut_mid.push(*mid);
            up_offset += 1;
        }
    }
    up_offsets.push(up_offset);

    let mut down_offsets = Vec::with_capacity(n_nodes + 1);
    let mut down_heads = Vec::new();
    let mut down_is_shortcut = Vec::new();
    let mut down_shortcut_mid = Vec::new();

    let mut down_offset = 0u64;
    for u in 0..n_nodes {
        down_offsets.push(down_offset);
        for (v, is_sc, mid) in &down_edges[u] {
            down_heads.push(*v);
            down_is_shortcut.push(*is_sc);
            down_shortcut_mid.push(*mid);
            down_offset += 1;
        }
    }
    down_offsets.push(down_offset);

    let n_up_edges = up_heads.len() as u64;
    let n_down_edges = down_heads.len() as u64;

    println!("  {} UP edges, {} DOWN edges", n_up_edges, n_down_edges);

    Ok(NbgChTopo {
        n_nodes: n_nodes as u32,
        n_up_edges,
        n_down_edges,
        n_shortcuts,

        up_offsets,
        up_heads,
        up_is_shortcut,
        up_shortcut_mid,

        down_offsets,
        down_heads,
        down_is_shortcut,
        down_shortcut_mid,
    })
}

/// Contract NBG with witness search (production quality)
pub fn contract_nbg_with_witness(
    nbg_csr: &NbgCsr,
    ordering: &NbgNdOrdering,
    base_weights: &[u32],  // Edge weights indexed by edge_idx
) -> Result<NbgChTopo> {
    let n_nodes = nbg_csr.n_nodes as usize;
    println!("Contracting NBG with witness search ({} nodes)...", n_nodes);

    // Build adjacency with weights
    let mut adj: Vec<Vec<(u32, u32, bool, u32)>> = vec![Vec::new(); n_nodes]; // (neighbor, weight, is_shortcut, mid)

    // Initialize with original edges
    for u in 0..n_nodes {
        let start = nbg_csr.offsets[u] as usize;
        let end = nbg_csr.offsets[u + 1] as usize;
        for i in start..end {
            let v = nbg_csr.heads[i];
            let edge_idx = nbg_csr.edge_idx[i] as usize;
            let weight = base_weights[edge_idx];
            adj[u].push((v, weight, false, 0));
        }
    }

    let mut n_shortcuts = 0u64;
    let mut last_progress = 0;

    // Contract in rank order
    for rank in 0..n_nodes {
        let node = ordering.inv_perm[rank];

        if rank * 100 / n_nodes > last_progress {
            last_progress = rank * 100 / n_nodes;
            if last_progress % 10 == 0 {
                println!("    {}% ({} shortcuts)", last_progress, n_shortcuts);
            }
        }

        // Find neighbors with higher rank and their edge weights
        let neighbors: Vec<(u32, u32)> = adj[node as usize]
            .iter()
            .filter(|(v, _, _, _)| ordering.perm[*v as usize] > rank as u32)
            .map(|(v, w, _, _)| (*v, *w))
            .collect();

        if neighbors.len() < 2 {
            continue;
        }

        // For each pair, check if shortcut is needed via witness search
        for i in 0..neighbors.len() {
            for j in (i + 1)..neighbors.len() {
                let (u, w_to_u) = neighbors[i];
                let (v, w_to_v) = neighbors[j];

                let shortcut_weight = w_to_u.saturating_add(w_to_v);

                // Witness search: can we reach v from u without using node?
                let witness_dist = witness_search(
                    &adj,
                    ordering,
                    u,
                    v,
                    node,
                    shortcut_weight,
                );

                if witness_dist > shortcut_weight {
                    // Need shortcut: no witness path exists
                    adj[u as usize].push((v, shortcut_weight, true, node));
                    adj[v as usize].push((u, shortcut_weight, true, node));
                    n_shortcuts += 2;
                }
            }
        }
    }

    println!("  {} shortcuts created (with witness search)", n_shortcuts);

    // Build UP/DOWN adjacency (same as before)
    let mut up_edges: Vec<Vec<(u32, bool, u32)>> = vec![Vec::new(); n_nodes];
    let mut down_edges: Vec<Vec<(u32, bool, u32)>> = vec![Vec::new(); n_nodes];

    for u in 0..n_nodes {
        let u_rank = ordering.perm[u];
        for (v, _w, is_sc, mid) in &adj[u] {
            let v_rank = ordering.perm[*v as usize];
            if v_rank > u_rank {
                up_edges[u].push((*v, *is_sc, *mid));
            } else {
                down_edges[u].push((*v, *is_sc, *mid));
            }
        }
    }

    // Build CSR
    let mut up_offsets = Vec::with_capacity(n_nodes + 1);
    let mut up_heads = Vec::new();
    let mut up_is_shortcut = Vec::new();
    let mut up_shortcut_mid = Vec::new();

    let mut up_offset = 0u64;
    for u in 0..n_nodes {
        up_offsets.push(up_offset);
        for (v, is_sc, mid) in &up_edges[u] {
            up_heads.push(*v);
            up_is_shortcut.push(*is_sc);
            up_shortcut_mid.push(*mid);
            up_offset += 1;
        }
    }
    up_offsets.push(up_offset);

    let mut down_offsets = Vec::with_capacity(n_nodes + 1);
    let mut down_heads = Vec::new();
    let mut down_is_shortcut = Vec::new();
    let mut down_shortcut_mid = Vec::new();

    let mut down_offset = 0u64;
    for u in 0..n_nodes {
        down_offsets.push(down_offset);
        for (v, is_sc, mid) in &down_edges[u] {
            down_heads.push(*v);
            down_is_shortcut.push(*is_sc);
            down_shortcut_mid.push(*mid);
            down_offset += 1;
        }
    }
    down_offsets.push(down_offset);

    Ok(NbgChTopo {
        n_nodes: n_nodes as u32,
        n_up_edges: up_heads.len() as u64,
        n_down_edges: down_heads.len() as u64,
        n_shortcuts,

        up_offsets,
        up_heads,
        up_is_shortcut,
        up_shortcut_mid,

        down_offsets,
        down_heads,
        down_is_shortcut,
        down_shortcut_mid,
    })
}

/// Bounded Dijkstra for witness search
fn witness_search(
    adj: &[Vec<(u32, u32, bool, u32)>],
    ordering: &NbgNdOrdering,
    source: u32,
    target: u32,
    forbidden: u32,
    max_dist: u32,
) -> u32 {
    // Simple Dijkstra bounded by max_dist
    let mut dist: Vec<u32> = vec![u32::MAX; adj.len()];
    let mut heap: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();

    dist[source as usize] = 0;
    heap.push(Reverse((0, source)));

    while let Some(Reverse((d, u))) = heap.pop() {
        if u == target {
            return d;
        }

        if d > dist[u as usize] || d > max_dist {
            continue;
        }

        for (v, w, _, _) in &adj[u as usize] {
            if *v == forbidden {
                continue;
            }

            // Only use edges to higher-ranked nodes (contracted ones are gone)
            if ordering.perm[*v as usize] <= ordering.perm[u as usize] {
                continue;
            }

            let new_dist = d.saturating_add(*w);
            if new_dist < dist[*v as usize] && new_dist <= max_dist {
                dist[*v as usize] = new_dist;
                heap.push(Reverse((new_dist, *v)));
            }
        }
    }

    u32::MAX
}
