//! Hybrid State Graph Data Structures
//!
//! Defines the state graph with mixed node-states and edge-states.

use crate::formats::hybrid_state::HybridState as FormatHybridState;
use crate::profile_abi::Mode;

/// A state in the hybrid graph
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum HybridState {
    /// Node-state: "at NBG node v" (for simple nodes)
    /// Fields: (nbg_node_id)
    Node(u32),

    /// Edge-state: "arrived at NBG node v via directed edge" (for complex nodes)
    /// Fields: (ebg_node_id) - references the original EBG node
    Edge(u32),
}

impl HybridState {
    /// Get the NBG node this state represents arrival at
    #[inline]
    pub fn nbg_node(&self, ebg_nodes: &[(u32, u32)]) -> u32 {
        match self {
            HybridState::Node(n) => *n,
            HybridState::Edge(ebg_id) => ebg_nodes[*ebg_id as usize].1, // head_nbg
        }
    }

    /// Check if this is a node-state
    #[inline]
    pub fn is_node_state(&self) -> bool {
        matches!(self, HybridState::Node(_))
    }

    /// Check if this is an edge-state
    #[inline]
    pub fn is_edge_state(&self) -> bool {
        matches!(self, HybridState::Edge(_))
    }
}

/// An arc in the hybrid state graph
#[derive(Debug, Clone, Copy)]
pub struct HybridArc {
    /// Target state ID in the hybrid graph
    pub target: u32,
    /// Weight of the transition (edge traversal + turn cost)
    pub weight: u32,
    /// Original EBG arc index (for geometry reconstruction), or u32::MAX if synthetic
    pub ebg_arc_idx: u32,
}

/// Statistics about the hybrid graph
#[derive(Debug, Clone, Default)]
pub struct HybridGraphStats {
    pub n_nbg_nodes: usize,
    pub n_ebg_nodes: usize,
    pub n_hybrid_states: usize,
    pub n_node_states: usize,
    pub n_edge_states: usize,
    pub n_simple_nodes: usize,
    pub n_complex_nodes: usize,
    pub n_hybrid_arcs: usize,
    pub n_ebg_arcs: usize,
    pub state_reduction_ratio: f64,
    pub arc_reduction_ratio: f64,
}

/// The hybrid state graph
///
/// States are numbered 0..n_states where:
/// - States 0..n_node_states are node-states
/// - States n_node_states..n_states are edge-states
pub struct HybridStateGraph {
    /// Number of node-states (simple nodes)
    pub n_node_states: u32,
    /// Number of edge-states (complex node arrivals)
    pub n_edge_states: u32,
    /// Total states = n_node_states + n_edge_states
    pub n_states: u32,

    /// CSR offsets for adjacency (n_states + 1)
    pub offsets: Vec<u64>,
    /// CSR targets (hybrid state IDs)
    pub targets: Vec<u32>,
    /// CSR weights (edge traversal + turn cost in milliseconds)
    pub weights: Vec<u32>,
    /// CSR EBG arc indices (for geometry reconstruction)
    pub ebg_arc_idx: Vec<u32>,

    /// Mapping: node-state index → NBG node ID
    /// node_state_to_nbg[i] = NBG node for state i (where i < n_node_states)
    pub node_state_to_nbg: Vec<u32>,

    /// Mapping: edge-state index → EBG node ID
    /// edge_state_to_ebg[i] = EBG node for state (n_node_states + i)
    pub edge_state_to_ebg: Vec<u32>,

    /// Reverse mapping: NBG node → node-state ID (or u32::MAX if complex)
    pub nbg_to_node_state: Vec<u32>,

    /// Reverse mapping: EBG node → edge-state ID (or u32::MAX if simple destination)
    pub ebg_to_edge_state: Vec<u32>,

    /// Which NBG nodes are complex (have turn restrictions)
    pub is_complex: Vec<bool>,

    /// Statistics
    pub stats: HybridGraphStats,
}

impl HybridStateGraph {
    /// Get the hybrid state ID for arriving at an NBG node
    /// If coming from a specific EBG edge (for complex nodes), provide it
    #[inline]
    pub fn get_arrival_state(&self, nbg_node: u32, via_ebg_node: Option<u32>) -> u32 {
        if self.is_complex[nbg_node as usize] {
            // Complex node: need edge-state
            let ebg_id = via_ebg_node.expect("Complex node requires incoming edge");
            self.ebg_to_edge_state[ebg_id as usize]
        } else {
            // Simple node: use node-state
            self.nbg_to_node_state[nbg_node as usize]
        }
    }

    /// Get the NBG node that a state represents arrival at
    #[inline]
    pub fn state_to_nbg(&self, state: u32, ebg_heads: &[u32]) -> u32 {
        if state < self.n_node_states {
            // Node-state
            self.node_state_to_nbg[state as usize]
        } else {
            // Edge-state
            let edge_idx = state - self.n_node_states;
            let ebg_id = self.edge_state_to_ebg[edge_idx as usize];
            ebg_heads[ebg_id as usize]
        }
    }

    /// Get outgoing arcs from a state (CSR access)
    #[inline]
    pub fn outgoing(&self, state: u32) -> impl Iterator<Item = HybridArc> + '_ {
        let start = self.offsets[state as usize] as usize;
        let end = self.offsets[state as usize + 1] as usize;
        (start..end).map(move |i| HybridArc {
            target: self.targets[i],
            weight: self.weights[i],
            ebg_arc_idx: self.ebg_arc_idx[i],
        })
    }

    /// Check if a state is a node-state
    #[inline]
    pub fn is_node_state(&self, state: u32) -> bool {
        state < self.n_node_states
    }

    /// Check if a state is an edge-state
    #[inline]
    pub fn is_edge_state(&self, state: u32) -> bool {
        state >= self.n_node_states
    }

    /// Get statistics about the graph
    pub fn stats(&self) -> &HybridGraphStats {
        &self.stats
    }

    /// Print summary statistics
    pub fn print_stats(&self) {
        println!("Hybrid State Graph Statistics:");
        println!("  NBG nodes:        {:>12}", self.stats.n_nbg_nodes);
        println!("  EBG nodes:        {:>12}", self.stats.n_ebg_nodes);
        println!("  Hybrid states:    {:>12}", self.stats.n_hybrid_states);
        println!(
            "    Node-states:    {:>12} ({:.1}%)",
            self.stats.n_node_states,
            100.0 * self.stats.n_node_states as f64 / self.stats.n_hybrid_states as f64
        );
        println!(
            "    Edge-states:    {:>12} ({:.1}%)",
            self.stats.n_edge_states,
            100.0 * self.stats.n_edge_states as f64 / self.stats.n_hybrid_states as f64
        );
        println!(
            "  Simple nodes:     {:>12} ({:.2}%)",
            self.stats.n_simple_nodes,
            100.0 * self.stats.n_simple_nodes as f64 / self.stats.n_nbg_nodes as f64
        );
        println!(
            "  Complex nodes:    {:>12} ({:.2}%)",
            self.stats.n_complex_nodes,
            100.0 * self.stats.n_complex_nodes as f64 / self.stats.n_nbg_nodes as f64
        );
        println!("  Hybrid arcs:      {:>12}", self.stats.n_hybrid_arcs);
        println!("  EBG arcs:         {:>12}", self.stats.n_ebg_arcs);
        println!(
            "  State reduction:  {:>12.2}x",
            self.stats.state_reduction_ratio
        );
        println!(
            "  Arc reduction:    {:>12.2}x",
            self.stats.arc_reduction_ratio
        );
    }

    /// Convert to format struct for serialization
    ///
    /// # Arguments
    /// * `mode` - The routing mode (car/bike/foot)
    /// * `ebg_head_nbg` - For each EBG node, the head NBG node (for coordinate lookup)
    /// * `inputs_sha` - SHA-256 of input files (truncated to 32 bytes)
    pub fn to_format(
        &self,
        mode: Mode,
        ebg_head_nbg: Vec<u32>,
        inputs_sha: [u8; 32],
    ) -> FormatHybridState {
        FormatHybridState {
            mode,
            n_states: self.n_states,
            n_node_states: self.n_node_states,
            n_edge_states: self.n_edge_states,
            n_arcs: self.stats.n_hybrid_arcs as u64,
            n_nbg_nodes: self.stats.n_nbg_nodes as u32,
            n_ebg_nodes: self.stats.n_ebg_nodes as u32,
            inputs_sha,
            offsets: self.offsets.clone(),
            targets: self.targets.clone(),
            weights: self.weights.clone(),
            node_state_to_nbg: self.node_state_to_nbg.clone(),
            edge_state_to_ebg: self.edge_state_to_ebg.clone(),
            nbg_to_node_state: self.nbg_to_node_state.clone(),
            ebg_to_edge_state: self.ebg_to_edge_state.clone(),
            ebg_head_nbg,
        }
    }
}
