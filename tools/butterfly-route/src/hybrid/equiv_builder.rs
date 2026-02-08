//! Equivalence-Class Hybrid State Graph Builder
//!
//! Builds hybrid state graph using behavior equivalence classes.
//! Only collapses edges with IDENTICAL behavior signatures, preserving the degree invariant.
//!
//! Key insight from K(node) analysis (Belgium, car mode):
//! - 98% of nodes have K=1 (all incoming edges equivalent)
//! - Median K=1, p99=2, max=10
//! - We can collapse to nearly NBG size while preserving exact turn semantics

use super::equivalence::BehaviorSignature;
use super::state_graph::{HybridGraphStats, HybridStateGraph};
use crate::formats::{EbgCsr, EbgNodes};
use std::collections::HashMap;

/// Equivalence-class hybrid graph builder
pub struct EquivHybridBuilder;

impl EquivHybridBuilder {
    /// Build hybrid state graph using equivalence classes
    ///
    /// For each NBG node, incoming EBG edges are grouped by their behavior signature.
    /// Each equivalence class becomes one hybrid state, using ONE representative's outgoing edges.
    ///
    /// # Arguments
    /// * `ebg_nodes` - EBG node data (tail_nbg, head_nbg per node)
    /// * `ebg_csr` - EBG adjacency (CSR format)
    /// * `weights` - Per-EBG-node edge weights
    /// * `turns` - Per-arc turn costs
    /// * `n_nbg_nodes` - Total number of NBG nodes
    pub fn build(
        ebg_nodes: &EbgNodes,
        ebg_csr: &EbgCsr,
        weights: &[u32],
        turns: &[u32],
        n_nbg_nodes: usize,
    ) -> HybridStateGraph {
        let n_ebg_nodes = ebg_nodes.nodes.len();
        let n_ebg_arcs = ebg_csr.heads.len();

        // Extract (tail, head) from EBG nodes
        let ebg_tail_head: Vec<(u32, u32)> = ebg_nodes
            .nodes
            .iter()
            .map(|n| (n.tail_nbg, n.head_nbg))
            .collect();

        println!("Building equivalence-class hybrid state graph...");
        println!(
            "  Input: {} NBG nodes, {} EBG nodes, {} arcs",
            n_nbg_nodes, n_ebg_nodes, n_ebg_arcs
        );

        // === Phase 1: Build reverse mapping NBG → incoming EBG edges ===
        let mut nbg_incoming: Vec<Vec<u32>> = vec![Vec::new(); n_nbg_nodes];
        for (ebg_id, &(_tail, head)) in ebg_tail_head.iter().enumerate() {
            if (head as usize) < n_nbg_nodes {
                nbg_incoming[head as usize].push(ebg_id as u32);
            }
        }

        // === Phase 2: Group incoming edges by behavior signature ===
        // For each NBG node, compute equivalence classes

        // equivalence_classes[nbg_node] = vec of (representative_ebg, class_members)
        // We pick the first edge in each class as representative
        let mut equivalence_classes: Vec<Vec<(u32, Vec<u32>)>> = Vec::with_capacity(n_nbg_nodes);
        let mut total_classes = 0usize;
        let mut nodes_with_reduction = 0usize;
        let mut nodes_fully_collapsed = 0usize;

        for incoming in &nbg_incoming {
            if incoming.is_empty() {
                equivalence_classes.push(Vec::new());
                continue;
            }

            // Compute signature for each incoming edge
            let mut sig_to_class: HashMap<BehaviorSignature, Vec<u32>> = HashMap::new();

            for &in_ebg in incoming {
                let sig = compute_signature(in_ebg, &ebg_csr.offsets, &ebg_csr.heads, turns);
                sig_to_class.entry(sig).or_default().push(in_ebg);
            }

            // Convert to class list: (representative, members)
            let classes: Vec<(u32, Vec<u32>)> = sig_to_class
                .into_values()
                .map(|members| {
                    let representative = members[0]; // Pick first as representative
                    (representative, members)
                })
                .collect();

            let k = classes.len();
            let indeg = incoming.len();

            if k < indeg {
                nodes_with_reduction += 1;
            }
            if k == 1 {
                nodes_fully_collapsed += 1;
            }

            total_classes += k;
            equivalence_classes.push(classes);
        }

        let n_reachable_nbg = equivalence_classes.iter().filter(|c| !c.is_empty()).count();

        println!("  Equivalence class analysis:");
        println!("    Reachable NBG nodes: {}", n_reachable_nbg);
        println!(
            "    Total classes: {} (was {} EBG nodes)",
            total_classes, n_ebg_nodes
        );
        println!(
            "    Nodes fully collapsed (K=1): {} ({:.1}%)",
            nodes_fully_collapsed,
            100.0 * nodes_fully_collapsed as f64 / n_reachable_nbg as f64
        );
        println!(
            "    Nodes with reduction (K<indeg): {} ({:.1}%)",
            nodes_with_reduction,
            100.0 * nodes_with_reduction as f64 / n_reachable_nbg as f64
        );

        // === Phase 3: Assign state IDs ===
        // Each equivalence class becomes one state
        // State numbering: 0..n_states where each state is an equivalence class

        // For compatibility with existing code, we still distinguish "node-states" and "edge-states"
        // Node-state: any class at a node with K=1 (can be represented by NBG node)
        // Edge-state: any class at a node with K>1 (needs to track which class)
        //
        // Actually, for simplicity, we'll treat ALL states uniformly as equivalence class states.
        // The distinction between "node-state" and "edge-state" becomes less meaningful.
        // We'll use n_node_states for K=1 nodes (compatible with existing format).

        let mut node_state_to_nbg: Vec<u32> = Vec::new(); // For K=1 nodes: state → NBG
        let mut edge_state_to_ebg: Vec<u32> = Vec::new(); // For K>1 classes: state → representative EBG

        let mut nbg_to_node_state: Vec<u32> = vec![u32::MAX; n_nbg_nodes];
        let mut ebg_to_edge_state: Vec<u32> = vec![u32::MAX; n_ebg_nodes];

        // class_to_state[nbg][class_idx] = state_id
        let mut class_to_state: Vec<Vec<u32>> = vec![Vec::new(); n_nbg_nodes];
        // state_representative[state_id] = representative EBG node
        let mut state_representative: Vec<u32> = Vec::new();

        // First pass: assign node-states for K=1 nodes
        for nbg_node in 0..n_nbg_nodes {
            let classes = &equivalence_classes[nbg_node];
            if classes.len() == 1 {
                // K=1: all incoming edges equivalent, create one node-state
                let state_id = node_state_to_nbg.len() as u32;
                node_state_to_nbg.push(nbg_node as u32);
                nbg_to_node_state[nbg_node] = state_id;
                class_to_state[nbg_node].push(state_id);

                let representative = classes[0].0;
                state_representative.push(representative);

                // Mark all members as mapping to this state
                for &member in &classes[0].1 {
                    ebg_to_edge_state[member as usize] = state_id;
                }
            }
        }
        let n_node_states = node_state_to_nbg.len() as u32;

        // Second pass: assign edge-states for K>1 nodes
        for nbg_node in 0..n_nbg_nodes {
            let classes = &equivalence_classes[nbg_node];
            if classes.len() > 1 {
                // K>1: multiple equivalence classes, each gets an edge-state
                for (representative, members) in classes {
                    let state_id = n_node_states + edge_state_to_ebg.len() as u32;
                    edge_state_to_ebg.push(*representative);
                    class_to_state[nbg_node].push(state_id);
                    state_representative.push(*representative);

                    // Mark all members as mapping to this state
                    for &member in members {
                        ebg_to_edge_state[member as usize] = state_id;
                    }
                }
            }
        }
        let n_edge_states = edge_state_to_ebg.len() as u32;
        let n_states = n_node_states + n_edge_states;

        println!("  State assignment:");
        println!("    Node-states (K=1 nodes): {}", n_node_states);
        println!("    Edge-states (K>1 classes): {}", n_edge_states);
        println!(
            "    Total states: {} (was {} EBG nodes)",
            n_states, n_ebg_nodes
        );
        println!(
            "    State reduction: {:.2}x",
            n_ebg_nodes as f64 / n_states as f64
        );

        // === Phase 4: Build adjacency ===
        // For each state, use the representative's outgoing edges
        // This is the KEY DIFFERENCE from naive hybrid:
        // - We use ONE representative's edges, not union of all members' edges
        // - Since members are equivalent, they have identical outgoing patterns
        // - This preserves the degree invariant!

        let mut adjacency: Vec<Vec<(u32, u32, u32)>> = vec![Vec::new(); n_states as usize];
        let mut in_degree: Vec<u32> = vec![0; n_states as usize];

        for state_id in 0..n_states {
            let representative = state_representative[state_id as usize];

            // Get representative's outgoing arcs in EBG
            let arc_start = ebg_csr.offsets[representative as usize] as usize;
            let arc_end = ebg_csr.offsets[representative as usize + 1] as usize;

            for arc_idx in arc_start..arc_end {
                let tgt_ebg = ebg_csr.heads[arc_idx] as usize;

                // Find target state
                // The target EBG edge maps to a specific equivalence class at tgt_head_nbg
                let tgt_state = ebg_to_edge_state[tgt_ebg];

                if tgt_state == u32::MAX {
                    // Target not in any equivalence class (shouldn't happen for valid graph)
                    continue;
                }

                // Compute weight: edge traversal + turn cost
                let edge_weight = weights.get(tgt_ebg).copied().unwrap_or(u32::MAX);
                let turn_cost = turns.get(arc_idx).copied().unwrap_or(0);
                let total_weight = edge_weight.saturating_add(turn_cost);

                if total_weight == u32::MAX {
                    continue; // Skip unreachable arcs
                }

                adjacency[state_id as usize].push((tgt_state, total_weight, arc_idx as u32));
                in_degree[tgt_state as usize] += 1;
            }
        }

        // Count total arcs and compute degree statistics
        let n_hybrid_arcs: usize = adjacency.iter().map(|a| a.len()).sum();
        let avg_out_degree = n_hybrid_arcs as f64 / n_states as f64;
        let max_out_degree = adjacency.iter().map(|a| a.len()).max().unwrap_or(0);
        let avg_in_degree = in_degree.iter().map(|&d| d as f64).sum::<f64>() / n_states as f64;
        let max_in_degree = *in_degree.iter().max().unwrap_or(&0);

        // Compare to EBG edges-per-node
        let ebg_avg_degree = n_ebg_arcs as f64 / n_ebg_nodes as f64;

        // Compute in-degree distribution in EBG for comparison
        let mut ebg_in_degree: Vec<u32> = vec![0; n_ebg_nodes];
        for &tgt in &ebg_csr.heads {
            ebg_in_degree[tgt as usize] += 1;
        }
        let ebg_avg_in_degree =
            ebg_in_degree.iter().map(|&d| d as f64).sum::<f64>() / n_ebg_nodes as f64;
        let ebg_max_in_degree = *ebg_in_degree.iter().max().unwrap_or(&0);

        println!("  Adjacency statistics:");
        println!(
            "    Hybrid arcs: {} (was {} EBG arcs)",
            n_hybrid_arcs, n_ebg_arcs
        );
        println!(
            "    Arc reduction: {:.2}x",
            n_ebg_arcs as f64 / n_hybrid_arcs as f64
        );
        println!(
            "    Out-degree: avg {:.2} (EBG was {:.2}), max {}",
            avg_out_degree, ebg_avg_degree, max_out_degree
        );
        println!(
            "    In-degree:  avg {:.2} (EBG was {:.2}), max {} (EBG was {})",
            avg_in_degree, ebg_avg_in_degree, max_in_degree, ebg_max_in_degree
        );
        println!(
            "    Out-degree ratio: {:.2}x",
            avg_out_degree / ebg_avg_degree
        );
        println!(
            "    In-degree ratio:  {:.2}x",
            avg_in_degree / ebg_avg_in_degree
        );

        // Check the degree invariant - BOTH in and out degree matter for CCH!
        let out_deg_ratio = avg_out_degree / ebg_avg_degree;
        let in_deg_ratio = avg_in_degree / ebg_avg_in_degree;

        if out_deg_ratio <= 1.05 && in_deg_ratio <= 1.05 {
            println!("  VERIFIED: Both in/out degree invariants preserved");
        } else if out_deg_ratio <= 1.05 {
            println!(
                "  WARNING: In-degree increased by {:.2}x! This will hurt CCH contraction.",
                in_deg_ratio
            );
            println!("           Equivalence classes accumulate incoming edges from all members.");
        } else {
            println!(
                "  WARNING: Degree increased! Out: {:.2}x, In: {:.2}x",
                out_deg_ratio, in_deg_ratio
            );
        }

        // === Phase 5: Materialize CSR ===
        let mut offsets = Vec::with_capacity(n_states as usize + 1);
        let mut targets = Vec::with_capacity(n_hybrid_arcs);
        let mut arc_weights = Vec::with_capacity(n_hybrid_arcs);
        let mut ebg_arc_idx = Vec::with_capacity(n_hybrid_arcs);

        let mut offset = 0u64;
        for adj_list in &adjacency {
            offsets.push(offset);
            for &(tgt, w, arc_idx) in adj_list {
                targets.push(tgt);
                arc_weights.push(w);
                ebg_arc_idx.push(arc_idx);
                offset += 1;
            }
        }
        offsets.push(offset);

        // Build is_complex array (for K>1 nodes)
        let mut is_complex = vec![false; n_nbg_nodes];
        for nbg_node in 0..n_nbg_nodes {
            if equivalence_classes[nbg_node].len() > 1 {
                is_complex[nbg_node] = true;
            }
        }
        let n_complex = is_complex.iter().filter(|&&x| x).count();
        let n_simple = n_nbg_nodes - n_complex;

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

        println!("\n  SUMMARY:");
        println!(
            "    States: {} → {} ({:.2}x reduction)",
            n_ebg_nodes, n_states, stats.state_reduction_ratio
        );
        println!(
            "    Arcs: {} → {} ({:.2}x reduction)",
            n_ebg_arcs, n_hybrid_arcs, stats.arc_reduction_ratio
        );
        println!(
            "    Out-degree: {:.2} → {:.2} ({:.2}x)",
            ebg_avg_degree, avg_out_degree, out_deg_ratio
        );
        println!(
            "    In-degree:  {:.2} → {:.2} ({:.2}x)",
            ebg_avg_in_degree, avg_in_degree, in_deg_ratio
        );
        println!(
            "    Max in-degree: {} → {} ({:.2}x)",
            ebg_max_in_degree,
            max_in_degree,
            max_in_degree as f64 / ebg_max_in_degree as f64
        );

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

/// Compute behavior signature for an incoming edge
/// The signature is the vector of (outgoing_edge, turn_cost) pairs
fn compute_signature(
    in_ebg: u32,
    ebg_offsets: &[u64],
    ebg_targets: &[u32],
    turn_costs: &[u32],
) -> BehaviorSignature {
    let arc_start = ebg_offsets[in_ebg as usize] as usize;
    let arc_end = ebg_offsets[in_ebg as usize + 1] as usize;

    let mut turn_cost_vec: Vec<(u32, u32)> = Vec::with_capacity(arc_end - arc_start);

    for (arc_idx, &out_ebg) in ebg_targets.iter().enumerate().take(arc_end).skip(arc_start) {
        let cost = turn_costs.get(arc_idx).copied().unwrap_or(0);
        turn_cost_vec.push((out_ebg, cost));
    }

    // Sort by outgoing edge for consistent comparison
    turn_cost_vec.sort_unstable_by_key(|(out, _)| *out);

    BehaviorSignature {
        turn_costs: turn_cost_vec,
    }
}
