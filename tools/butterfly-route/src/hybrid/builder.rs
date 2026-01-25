//! Hybrid State Graph Builder
//!
//! Constructs the hybrid state graph from EBG and turn rules.

use std::collections::{HashMap, HashSet};
use crate::formats::{EbgCsr, EbgNodes, TurnRule};
use super::state_graph::{HybridStateGraph, HybridGraphStats};

/// Builder for the hybrid state graph
pub struct HybridGraphBuilder {
    /// Set of complex NBG nodes (have turn restrictions)
    complex_nodes: HashSet<u32>,
}

impl HybridGraphBuilder {
    /// Create a new builder
    pub fn new() -> Self {
        Self {
            complex_nodes: HashSet::new(),
        }
    }

    /// Classify NBG nodes as simple or complex based on turn rules
    ///
    /// A node is "complex" if ANY turn rule references it as via_node.
    /// All other nodes are "simple".
    pub fn classify_nodes(&mut self, turn_rules: &[TurnRule], nbg_node_map: &HashMap<i64, u32>) {
        self.complex_nodes.clear();

        for rule in turn_rules {
            // Map OSM node ID to compact NBG node ID
            if let Some(&nbg_id) = nbg_node_map.get(&rule.via_node_id) {
                self.complex_nodes.insert(nbg_id);
            }
        }

        println!("  Classified {} nodes as complex (have turn restrictions)",
            self.complex_nodes.len());
    }

    /// Build the hybrid state graph from EBG
    ///
    /// # Arguments
    /// * `ebg_nodes` - EBG node data (tail_nbg, head_nbg per node)
    /// * `ebg_csr` - EBG adjacency (CSR format)
    /// * `turn_table` - Turn table for looking up turn costs
    /// * `weights` - Per-arc edge weights
    /// * `turns` - Per-arc turn costs
    /// * `n_nbg_nodes` - Total number of NBG nodes
    pub fn build(
        &self,
        ebg_nodes: &EbgNodes,
        ebg_csr: &EbgCsr,
        weights: &[u32],
        turns: &[u32],
        n_nbg_nodes: usize,
    ) -> HybridStateGraph {
        let n_ebg_nodes = ebg_nodes.nodes.len();
        let n_ebg_arcs = ebg_csr.heads.len();

        // Extract (tail, head) from EBG nodes
        let ebg_tail_head: Vec<(u32, u32)> = ebg_nodes.nodes
            .iter()
            .map(|n| (n.tail_nbg, n.head_nbg))
            .collect();

        // Build is_complex array for all NBG nodes
        let mut is_complex = vec![false; n_nbg_nodes];
        for &node in &self.complex_nodes {
            if (node as usize) < n_nbg_nodes {
                is_complex[node as usize] = true;
            }
        }

        // Count simple and complex nodes
        let n_complex = self.complex_nodes.len();
        let n_simple = n_nbg_nodes - n_complex;

        println!("  Simple nodes: {} ({:.2}%)", n_simple,
            100.0 * n_simple as f64 / n_nbg_nodes as f64);
        println!("  Complex nodes: {} ({:.2}%)", n_complex,
            100.0 * n_complex as f64 / n_nbg_nodes as f64);

        // === Phase 1: Enumerate hybrid states ===

        // Node-states: one per simple NBG node
        // We'll number them 0..n_node_states
        let mut node_state_to_nbg: Vec<u32> = Vec::with_capacity(n_simple);
        let mut nbg_to_node_state: Vec<u32> = vec![u32::MAX; n_nbg_nodes];

        for nbg_node in 0..n_nbg_nodes {
            if !is_complex[nbg_node] {
                let state_id = node_state_to_nbg.len() as u32;
                nbg_to_node_state[nbg_node] = state_id;
                node_state_to_nbg.push(nbg_node as u32);
            }
        }
        let n_node_states = node_state_to_nbg.len() as u32;

        // Edge-states: one per EBG node whose head is a complex NBG node
        // We'll number them n_node_states..n_states
        let mut edge_state_to_ebg: Vec<u32> = Vec::new();
        let mut ebg_to_edge_state: Vec<u32> = vec![u32::MAX; n_ebg_nodes];

        for ebg_id in 0..n_ebg_nodes {
            let head_nbg = ebg_tail_head[ebg_id].1 as usize;
            if is_complex[head_nbg] {
                let state_id = n_node_states + edge_state_to_ebg.len() as u32;
                ebg_to_edge_state[ebg_id] = state_id;
                edge_state_to_ebg.push(ebg_id as u32);
            }
        }
        let n_edge_states = edge_state_to_ebg.len() as u32;
        let n_states = n_node_states + n_edge_states;

        println!("  Node-states: {}", n_node_states);
        println!("  Edge-states: {}", n_edge_states);
        println!("  Total hybrid states: {} (was {} EBG nodes)", n_states, n_ebg_nodes);
        println!("  State reduction: {:.2}x", n_ebg_nodes as f64 / n_states as f64);

        // === Phase 2: Build hybrid adjacency ===

        // For each hybrid state, collect outgoing arcs
        let mut adjacency: Vec<Vec<(u32, u32, u32)>> = vec![Vec::new(); n_states as usize];

        // Process all EBG arcs
        for src_ebg in 0..n_ebg_nodes {
            let (tail_nbg, head_nbg) = ebg_tail_head[src_ebg];

            // Determine the hybrid state for this EBG node
            let src_state = if is_complex[head_nbg as usize] {
                // Arriving at complex node: use edge-state
                ebg_to_edge_state[src_ebg]
            } else {
                // Arriving at simple node: use node-state
                nbg_to_node_state[head_nbg as usize]
            };

            // Skip if this EBG node doesn't contribute to hybrid graph
            // (shouldn't happen if logic is correct)
            if src_state == u32::MAX {
                continue;
            }

            // Process outgoing EBG arcs
            let arc_start = ebg_csr.offsets[src_ebg] as usize;
            let arc_end = ebg_csr.offsets[src_ebg + 1] as usize;

            for arc_idx in arc_start..arc_end {
                let tgt_ebg = ebg_csr.heads[arc_idx] as usize;
                let (_, tgt_head_nbg) = ebg_tail_head[tgt_ebg];

                // Determine target hybrid state
                let tgt_state = if is_complex[tgt_head_nbg as usize] {
                    ebg_to_edge_state[tgt_ebg]
                } else {
                    nbg_to_node_state[tgt_head_nbg as usize]
                };

                if tgt_state == u32::MAX {
                    continue;
                }

                // Compute weight: edge traversal + turn cost
                let edge_weight = weights.get(arc_idx).copied().unwrap_or(u32::MAX);
                let turn_cost = turns.get(arc_idx).copied().unwrap_or(0);
                let total_weight = edge_weight.saturating_add(turn_cost);

                if total_weight == u32::MAX {
                    continue; // Skip unreachable arcs
                }

                adjacency[src_state as usize].push((tgt_state, total_weight, arc_idx as u32));
            }
        }

        // Also need to handle transitions FROM node-states
        // A node-state at NBG node v can transition to all nodes reachable from v
        // But wait - we already handled this above because we process all EBG nodes
        // and map them to node-states when destination is simple.
        //
        // Actually, there's a subtlety: multiple EBG nodes (different incoming edges)
        // may map to the same node-state. We need to merge their outgoing arcs.
        //
        // The current logic handles this: all EBG nodes arriving at the same simple node
        // get mapped to the same src_state, so their arcs all go into the same adjacency list.

        // Deduplicate arcs (multiple EBG arcs may produce same hybrid arc)
        // Keep the one with minimum weight
        for arcs in &mut adjacency {
            if arcs.is_empty() {
                continue;
            }
            // Sort by (target, weight) and deduplicate keeping min weight
            arcs.sort_by_key(|&(t, w, _)| (t, w));
            let mut i = 0;
            for j in 1..arcs.len() {
                if arcs[j].0 != arcs[i].0 {
                    i += 1;
                    arcs[i] = arcs[j];
                }
                // If same target and lower weight (shouldn't happen after sort), update
            }
            arcs.truncate(i + 1);
        }

        // Count total arcs
        let n_hybrid_arcs: usize = adjacency.iter().map(|a| a.len()).sum();

        println!("  Hybrid arcs: {} (was {} EBG arcs)", n_hybrid_arcs, n_ebg_arcs);
        println!("  Arc reduction: {:.2}x", n_ebg_arcs as f64 / n_hybrid_arcs as f64);

        // === Phase 3: Materialize CSR ===

        let mut offsets = Vec::with_capacity(n_states as usize + 1);
        let mut targets = Vec::with_capacity(n_hybrid_arcs);
        let mut arc_weights = Vec::with_capacity(n_hybrid_arcs);
        let mut ebg_arc_idx = Vec::with_capacity(n_hybrid_arcs);

        let mut offset = 0u64;
        for state in 0..n_states as usize {
            offsets.push(offset);
            for &(tgt, w, arc_idx) in &adjacency[state] {
                targets.push(tgt);
                arc_weights.push(w);
                ebg_arc_idx.push(arc_idx);
                offset += 1;
            }
        }
        offsets.push(offset);

        // Build stats
        let stats = HybridGraphStats {
            n_nbg_nodes,
            n_ebg_nodes,
            n_hybrid_states: n_states as usize,
            n_node_states: n_node_states as usize,
            n_edge_states: n_edge_states as usize,
            n_simple_nodes: n_simple,
            n_complex_nodes: n_complex,
            n_hybrid_arcs,
            n_ebg_arcs,
            state_reduction_ratio: n_ebg_nodes as f64 / n_states as f64,
            arc_reduction_ratio: n_ebg_arcs as f64 / n_hybrid_arcs as f64,
        };

        HybridStateGraph {
            n_node_states,
            n_edge_states,
            n_states,
            offsets,
            targets,
            weights: arc_weights,
            ebg_arc_idx,
            node_state_to_nbg,
            edge_state_to_ebg,
            nbg_to_node_state,
            ebg_to_edge_state,
            is_complex,
            stats,
        }
    }
}

impl Default for HybridGraphBuilder {
    fn default() -> Self {
        Self::new()
    }
}
