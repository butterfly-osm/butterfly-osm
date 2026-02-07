//! Node-based CH contraction with witness search
//!
//! Contracts the NBG according to a given ordering, creating shortcuts
//! only when necessary (verified via witness search).

use anyhow::Result;
use rayon::prelude::*;
use std::collections::BinaryHeap;
use std::cmp::Reverse;

use crate::formats::{NbgCsr, NbgGeo};
use super::ordering::NbgNdOrdering;

/// Maximum nodes to settle in witness search
const WITNESS_LIMIT: usize = 500;

/// If a node has more than this many higher-ranked neighbors,
/// skip witness search (too expensive)
#[allow(dead_code)]
const MAX_NEIGHBORS_FOR_WITNESS: usize = 10;

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

/// Contract NBG with witness search
pub fn contract_nbg(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    ordering: &NbgNdOrdering,
) -> Result<NbgChTopo> {
    let n_nodes = nbg_csr.n_nodes as usize;
    println!("Contracting NBG with witness search ({} nodes)...", n_nodes);

    // Build edge weights
    println!("  Building edge weights...");
    let edge_weights: Vec<u32> = nbg_geo.edges.iter().map(|e| e.length_mm).collect();

    // Build adjacency: adj[u] = [(neighbor, weight), ...]
    println!("  Building initial adjacency...");
    let mut adj: Vec<Vec<(u32, u32)>> = vec![Vec::new(); n_nodes];

    for (u, adj_u) in adj.iter_mut().enumerate() {
        let start = nbg_csr.offsets[u] as usize;
        let end = nbg_csr.offsets[u + 1] as usize;
        for i in start..end {
            let v = nbg_csr.heads[i];
            let w = edge_weights[nbg_csr.edge_idx[i] as usize];
            adj_u.push((v, w));
        }
        // Sort and dedup (keep min weight)
        adj_u.sort_by_key(|(v, _)| *v);
        adj_u.dedup_by(|a, b| {
            if a.0 == b.0 { b.1 = b.1.min(a.1); true } else { false }
        });
    }

    let n_original: u64 = adj.iter().map(|a| a.len() as u64).sum();
    println!("    {} original directed edges", n_original);

    // Contract in rank order with witness search
    println!("  Contracting nodes with witness search...");
    let mut n_shortcuts: u64 = 0;
    let mut n_witnessed: u64 = 0;
    let report_interval = (n_nodes / 20).max(1);

    // Reusable witness search state
    let mut witness = WitnessState::new(n_nodes);

    for rank in 0..n_nodes {
        let node = ordering.inv_perm[rank] as usize;

        // Find neighbors with higher rank
        let neighbors: Vec<(u32, u32)> = adj[node]
            .iter()
            .filter(|(v, _)| ordering.perm[*v as usize] as usize > rank)
            .cloned()
            .collect();

        if neighbors.len() >= 2 {
            for i in 0..neighbors.len() {
                let (u, w_u) = neighbors[i];
                for &(v, w_v) in &neighbors[(i + 1)..] {
                    let shortcut_weight = w_u.saturating_add(w_v);

                    // Witness search: is there a path u→v not through node?
                    let witness_dist = witness.search(
                        &adj, ordering, u, v, node as u32, shortcut_weight, rank
                    );

                    if witness_dist > shortcut_weight {
                        // No witness - need shortcut
                        insert_edge(&mut adj[u as usize], v, shortcut_weight);
                        insert_edge(&mut adj[v as usize], u, shortcut_weight);
                        n_shortcuts += 2;
                    } else {
                        n_witnessed += 1;
                    }
                }
            }
        }

        if rank > 0 && rank % report_interval == 0 {
            let pct = (rank * 100) / n_nodes;
            println!("    {}% complete, {} shortcuts, {} witnessed",
                     pct, n_shortcuts, n_witnessed);
        }
    }

    println!("    100% complete, {} shortcuts, {} witnessed", n_shortcuts, n_witnessed);

    // Build UP and DOWN adjacency
    println!("  Building UP/DOWN adjacency...");

    #[allow(clippy::type_complexity)]
    let (up_adj, down_adj): (Vec<Vec<(u32, u32)>>, Vec<Vec<(u32, u32)>>) = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let u_rank = ordering.perm[u];
            let mut up = Vec::new();
            let mut down = Vec::new();
            for &(v, w) in &adj[u] {
                if ordering.perm[v as usize] > u_rank {
                    up.push((v, w));
                } else {
                    down.push((v, w));
                }
            }
            (up, down)
        })
        .unzip();

    // Convert to CSR
    println!("  Converting to CSR...");

    let mut up_offsets = Vec::with_capacity(n_nodes + 1);
    let mut up_heads = Vec::new();
    let mut up_weights = Vec::new();
    let mut offset = 0u64;
    for up_adj_u in &up_adj {
        up_offsets.push(offset);
        for &(v, w) in up_adj_u {
            up_heads.push(v);
            up_weights.push(w);
            offset += 1;
        }
    }
    up_offsets.push(offset);

    let mut down_offsets = Vec::with_capacity(n_nodes + 1);
    let mut down_heads = Vec::new();
    let mut down_weights = Vec::new();
    let mut offset = 0u64;
    for down_adj_u in &down_adj {
        down_offsets.push(offset);
        for &(v, w) in down_adj_u {
            down_heads.push(v);
            down_weights.push(w);
            offset += 1;
        }
    }
    down_offsets.push(offset);

    println!("    {} UP edges, {} DOWN edges", up_heads.len(), down_heads.len());

    Ok(NbgChTopo {
        n_nodes: n_nodes as u32,
        n_up_edges: up_heads.len() as u64,
        n_down_edges: down_heads.len() as u64,
        n_shortcuts,
        n_original_edges: n_original,
        up_offsets,
        up_heads,
        up_weights,
        down_offsets,
        down_heads,
        down_weights,
    })
}

/// Reusable witness search state
struct WitnessState {
    dist: Vec<u32>,
    heap: BinaryHeap<Reverse<(u32, u32)>>,
    touched: Vec<u32>,  // Track which nodes we touched for fast reset
}

impl WitnessState {
    fn new(n_nodes: usize) -> Self {
        Self {
            dist: vec![u32::MAX; n_nodes],
            heap: BinaryHeap::with_capacity(1024),
            touched: Vec::with_capacity(1024),
        }
    }

    fn reset(&mut self) {
        for &node in &self.touched {
            self.dist[node as usize] = u32::MAX;
        }
        self.touched.clear();
        self.heap.clear();
    }

    /// Bounded Dijkstra witness search
    #[allow(clippy::too_many_arguments)]
    fn search(
        &mut self,
        adj: &[Vec<(u32, u32)>],
        ordering: &NbgNdOrdering,
        source: u32,
        target: u32,
        forbidden: u32,
        max_dist: u32,
        current_rank: usize,
    ) -> u32 {
        self.reset();

        self.dist[source as usize] = 0;
        self.touched.push(source);
        self.heap.push(Reverse((0, source)));

        let mut settled = 0;

        while let Some(Reverse((d, u))) = self.heap.pop() {
            if u == target {
                return d;
            }

            if d > self.dist[u as usize] || d > max_dist {
                continue;
            }

            settled += 1;
            if settled > WITNESS_LIMIT {
                return u32::MAX;
            }

            for &(v, w) in &adj[u as usize] {
                if v == forbidden {
                    continue;
                }

                // Only use uncontracted nodes
                if (ordering.perm[v as usize] as usize) <= current_rank {
                    continue;
                }

                let new_dist = d.saturating_add(w);
                if new_dist < self.dist[v as usize] && new_dist <= max_dist {
                    if self.dist[v as usize] == u32::MAX {
                        self.touched.push(v);
                    }
                    self.dist[v as usize] = new_dist;
                    self.heap.push(Reverse((new_dist, v)));
                }
            }
        }

        u32::MAX
    }
}

/// Insert edge into sorted adjacency, keeping min weight
#[inline]
fn insert_edge(adj: &mut Vec<(u32, u32)>, target: u32, weight: u32) {
    match adj.binary_search_by_key(&target, |(v, _)| *v) {
        Ok(idx) => {
            if weight < adj[idx].1 {
                adj[idx].1 = weight;
            }
        }
        Err(idx) => {
            adj.insert(idx, (target, weight));
        }
    }
}
