//! Parallel Node-based CH contraction
//!
//! Uses a fully parallel approach: no witness search, just create all shortcuts.
//! Good ordering quality (from nested dissection) ensures reasonable CH quality.

use anyhow::Result;
use rayon::prelude::*;
use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};
use parking_lot::Mutex;

use crate::formats::{NbgCsr, NbgGeo};
use super::ordering::NbgNdOrdering;

/// NBG CH topology (contracted graph)
#[derive(Debug, Clone)]
pub struct NbgChTopo {
    pub n_nodes: u32,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub n_shortcuts: u64,
    pub n_original_edges: u64,

    // UP adjacency: u → higher-ranked neighbors
    pub up_offsets: Vec<u64>,
    pub up_heads: Vec<u32>,
    pub up_weights: Vec<u32>,

    // DOWN adjacency: u → lower-ranked neighbors
    pub down_offsets: Vec<u64>,
    pub down_heads: Vec<u32>,
    pub down_weights: Vec<u32>,
}

/// Parallel CH contraction without witness search
pub fn contract_nbg(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    ordering: &NbgNdOrdering,
) -> Result<NbgChTopo> {
    let n_nodes = nbg_csr.n_nodes as usize;
    println!("Contracting NBG in parallel ({} nodes)...", n_nodes);

    // Build edge weights from geometry
    println!("  Building edge weights...");
    let edge_weights: Vec<u32> = nbg_geo.edges.iter().map(|e| e.length_mm).collect();

    // Build initial adjacency: adj[u] = [(neighbor, weight), ...]
    println!("  Building initial adjacency...");
    let mut adj: Vec<Mutex<Vec<(u32, u32)>>> = (0..n_nodes)
        .map(|_| Mutex::new(Vec::new()))
        .collect();

    // Initialize with original edges
    for u in 0..n_nodes {
        let start = nbg_csr.offsets[u] as usize;
        let end = nbg_csr.offsets[u + 1] as usize;
        let mut edges = Vec::with_capacity(end - start);
        for i in start..end {
            let v = nbg_csr.heads[i];
            let edge_idx = nbg_csr.edge_idx[i] as usize;
            let w = edge_weights[edge_idx];
            edges.push((v, w));
        }
        *adj[u].lock() = edges;
    }

    let n_original: u64 = adj.iter().map(|a| a.lock().len() as u64).sum();
    println!("    {} original directed edges", n_original);

    // Contract nodes in parallel batches
    // Key insight: nodes with disjoint neighbor sets can be contracted in parallel
    // We process in rank order but parallelize across nodes at each level
    println!("  Contracting nodes (parallel)...");

    let n_shortcuts = AtomicU64::new(0);

    // Process in larger batches for better parallelism
    // Nodes in same batch are contracted "simultaneously" - shortcuts added atomically
    let batch_size = 50000; // ~50K nodes per batch

    for batch_start in (0..n_nodes).step_by(batch_size) {
        let batch_end = (batch_start + batch_size).min(n_nodes);

        // Collect all shortcuts to add for this batch
        let batch_shortcuts: Vec<(u32, u32, u32)> = (batch_start..batch_end)
            .into_par_iter()
            .flat_map(|rank| {
                let node = ordering.inv_perm[rank] as usize;
                let adj_lock = adj[node].lock();

                // Find neighbors with higher rank
                let neighbors: Vec<(u32, u32)> = adj_lock
                    .iter()
                    .filter(|(v, _)| ordering.perm[*v as usize] > rank as u32)
                    .cloned()
                    .collect();
                drop(adj_lock);

                if neighbors.len() < 2 {
                    return Vec::new();
                }

                // Generate all shortcuts for this node
                let mut shortcuts = Vec::new();
                for i in 0..neighbors.len() {
                    let (u, w_to_u) = neighbors[i];
                    for j in (i + 1)..neighbors.len() {
                        let (v, w_to_v) = neighbors[j];
                        let shortcut_weight = w_to_u.saturating_add(w_to_v);
                        shortcuts.push((u, v, shortcut_weight));
                    }
                }
                shortcuts
            })
            .collect();

        // Add shortcuts to adjacency lists
        let batch_count = batch_shortcuts.len() as u64 * 2;
        n_shortcuts.fetch_add(batch_count, AtomicOrdering::Relaxed);

        for (u, v, w) in batch_shortcuts {
            adj[u as usize].lock().push((v, w));
            adj[v as usize].lock().push((u, w));
        }

        let pct = (batch_end * 100) / n_nodes;
        let total_shortcuts = n_shortcuts.load(AtomicOrdering::Relaxed);
        if pct % 10 == 0 || batch_end == n_nodes {
            println!("    {}% complete, {} shortcuts", pct, total_shortcuts);
        }
    }

    let total_shortcuts = n_shortcuts.load(AtomicOrdering::Relaxed);
    println!("    {} total shortcuts created", total_shortcuts);

    // Build UP and DOWN adjacency in parallel
    println!("  Building UP/DOWN adjacency...");

    let (up_adj, down_adj): (Vec<Vec<(u32, u32)>>, Vec<Vec<(u32, u32)>>) = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let u_rank = ordering.perm[u];
            let edges = adj[u].lock();

            // Deduplicate: keep minimum weight per neighbor
            let mut best: std::collections::HashMap<u32, u32> = std::collections::HashMap::new();
            for (v, w) in edges.iter() {
                let entry = best.entry(*v).or_insert(*w);
                if *w < *entry {
                    *entry = *w;
                }
            }

            let mut up = Vec::new();
            let mut down = Vec::new();

            for (v, w) in best {
                let v_rank = ordering.perm[v as usize];
                if v_rank > u_rank {
                    up.push((v, w));
                } else {
                    down.push((v, w));
                }
            }

            (up, down)
        })
        .unzip();

    // Convert to CSR format
    println!("  Converting to CSR...");

    let mut up_offsets = Vec::with_capacity(n_nodes + 1);
    let mut up_heads = Vec::new();
    let mut up_weights = Vec::new();

    let mut offset = 0u64;
    for u in 0..n_nodes {
        up_offsets.push(offset);
        for (v, w) in &up_adj[u] {
            up_heads.push(*v);
            up_weights.push(*w);
            offset += 1;
        }
    }
    up_offsets.push(offset);

    let mut down_offsets = Vec::with_capacity(n_nodes + 1);
    let mut down_heads = Vec::new();
    let mut down_weights = Vec::new();

    let mut offset = 0u64;
    for u in 0..n_nodes {
        down_offsets.push(offset);
        for (v, w) in &down_adj[u] {
            down_heads.push(*v);
            down_weights.push(*w);
            offset += 1;
        }
    }
    down_offsets.push(offset);

    println!("    {} UP edges, {} DOWN edges", up_heads.len(), down_heads.len());

    Ok(NbgChTopo {
        n_nodes: n_nodes as u32,
        n_up_edges: up_heads.len() as u64,
        n_down_edges: down_heads.len() as u64,
        n_shortcuts: total_shortcuts,
        n_original_edges: n_original,
        up_offsets,
        up_heads,
        up_weights,
        down_offsets,
        down_heads,
        down_weights,
    })
}
