//! Equivalence Class Analysis for Hybrid State Graph
//!
//! Computes K(node) - the number of unique behavior signatures per node.
//! This determines whether equivalence-class hybrid will help.
//!
//! Behavior signature for incoming edge e at node v:
//! - Turn cost vector to each outgoing edge (the main differentiator)
//!
//! If median K ≤ 4-8: equivalence-class hybrid will help
//! If median K ≈ indeg: no hybrid can help, edge-based is optimal

use std::collections::HashMap;
use std::hash::{Hash, Hasher};

/// Behavior signature for an incoming edge at a node
/// Two incoming edges with identical signatures can share a state
#[derive(Clone, Debug)]
pub struct BehaviorSignature {
    /// Turn costs to each outgoing edge, sorted by outgoing edge ID
    /// This captures the complete behavior of this incoming edge
    pub turn_costs: Vec<(u32, u32)>, // (out_edge_id, cost)
}

impl PartialEq for BehaviorSignature {
    fn eq(&self, other: &Self) -> bool {
        self.turn_costs == other.turn_costs
    }
}

impl Eq for BehaviorSignature {}

impl Hash for BehaviorSignature {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.turn_costs.hash(state);
    }
}

/// Result of equivalence class analysis
#[derive(Debug, Clone)]
pub struct EquivalenceAnalysis {
    /// Number of NBG nodes analyzed
    pub n_nodes: usize,
    /// Total incoming edges across all nodes
    pub total_indeg: usize,
    /// Total unique signatures (sum of K(node) for all nodes)
    pub total_k: usize,
    /// Percentiles of K(node)
    pub k_p50: usize,
    pub k_p90: usize,
    pub k_p95: usize,
    pub k_p99: usize,
    pub k_max: usize,
    /// Indeg distribution for comparison
    pub indeg_p50: usize,
    pub indeg_p90: usize,
    pub indeg_p99: usize,
    pub indeg_max: usize,
    /// Reduction ratio: total_indeg / total_k
    pub reduction_ratio: f64,
    /// Nodes where K < indeg (these benefit from equivalence classes)
    pub nodes_with_reduction: usize,
    /// Nodes where K = 1 (all incoming edges are equivalent)
    pub nodes_fully_collapsed: usize,
    /// Nodes where K = indeg (no equivalence, edge-based optimal)
    pub nodes_no_benefit: usize,
}

/// Analyze equivalence classes for the EBG
///
/// For each node, computes K = number of unique behavior signatures among incoming edges.
pub fn analyze_equivalence_classes(
    ebg_nodes: &[(u32, u32)],   // (tail_nbg, head_nbg) for each EBG node
    ebg_offsets: &[u64],        // CSR offsets for EBG adjacency
    ebg_targets: &[u32],        // CSR targets (EBG node IDs)
    turn_costs: &[u32],         // Turn cost for each EBG arc
) -> EquivalenceAnalysis {
    use std::collections::HashSet;

    // Build reverse mapping: NBG node -> list of incoming EBG edges
    let mut nbg_incoming: HashMap<u32, Vec<u32>> = HashMap::new();
    for (ebg_id, &(_tail, head)) in ebg_nodes.iter().enumerate() {
        nbg_incoming.entry(head).or_default().push(ebg_id as u32);
    }

    // For each NBG node, compute K(node)
    let mut k_values: Vec<usize> = Vec::new();
    let mut indeg_values: Vec<usize> = Vec::new();
    let mut total_indeg = 0usize;
    let mut total_k = 0usize;
    let mut nodes_with_reduction = 0usize;
    let mut nodes_fully_collapsed = 0usize;
    let mut nodes_no_benefit = 0usize;

    for (_nbg_node, incoming_edges) in &nbg_incoming {
        let indeg = incoming_edges.len();
        if indeg == 0 {
            continue;
        }

        // Compute behavior signature for each incoming edge
        let mut signatures: HashSet<BehaviorSignature> = HashSet::new();

        for &in_ebg in incoming_edges {
            let sig = compute_signature(
                in_ebg,
                ebg_offsets,
                ebg_targets,
                turn_costs,
            );
            signatures.insert(sig);
        }

        let k = signatures.len();

        k_values.push(k);
        indeg_values.push(indeg);
        total_indeg += indeg;
        total_k += k;

        if k < indeg {
            nodes_with_reduction += 1;
        }
        if k == 1 {
            nodes_fully_collapsed += 1;
        }
        if k == indeg {
            nodes_no_benefit += 1;
        }
    }

    // Sort for percentile computation
    k_values.sort_unstable();
    indeg_values.sort_unstable();

    let n_nodes = k_values.len();

    // Percentiles
    let percentile = |values: &[usize], p: f64| -> usize {
        if values.is_empty() {
            return 0;
        }
        let idx = ((values.len() as f64 * p) as usize).min(values.len() - 1);
        values[idx]
    };

    EquivalenceAnalysis {
        n_nodes,
        total_indeg,
        total_k,
        k_p50: percentile(&k_values, 0.50),
        k_p90: percentile(&k_values, 0.90),
        k_p95: percentile(&k_values, 0.95),
        k_p99: percentile(&k_values, 0.99),
        k_max: *k_values.last().unwrap_or(&0),
        indeg_p50: percentile(&indeg_values, 0.50),
        indeg_p90: percentile(&indeg_values, 0.90),
        indeg_p99: percentile(&indeg_values, 0.99),
        indeg_max: *indeg_values.last().unwrap_or(&0),
        reduction_ratio: if total_k > 0 {
            total_indeg as f64 / total_k as f64
        } else {
            1.0
        },
        nodes_with_reduction,
        nodes_fully_collapsed,
        nodes_no_benefit,
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

    for arc_idx in arc_start..arc_end {
        let out_ebg = ebg_targets[arc_idx];
        let cost = turn_costs.get(arc_idx).copied().unwrap_or(0);
        turn_cost_vec.push((out_ebg, cost));
    }

    // Sort by outgoing edge for consistent comparison
    turn_cost_vec.sort_unstable_by_key(|(out, _)| *out);

    BehaviorSignature {
        turn_costs: turn_cost_vec,
    }
}

impl EquivalenceAnalysis {
    /// Print analysis results
    pub fn print(&self) {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  EQUIVALENCE CLASS ANALYSIS");
        println!("═══════════════════════════════════════════════════════════════");
        println!("  Nodes analyzed: {:>12}", self.n_nodes);
        println!("  Total indeg:    {:>12}", self.total_indeg);
        println!("  Total K:        {:>12}", self.total_k);
        println!("  Reduction:      {:>12.2}x", self.reduction_ratio);
        println!("───────────────────────────────────────────────────────────────");
        println!("  K(node) distribution (unique signatures per node):");
        println!("    p50: {:>8}", self.k_p50);
        println!("    p90: {:>8}", self.k_p90);
        println!("    p95: {:>8}", self.k_p95);
        println!("    p99: {:>8}", self.k_p99);
        println!("    max: {:>8}", self.k_max);
        println!("───────────────────────────────────────────────────────────────");
        println!("  Indeg distribution (for comparison):");
        println!("    p50: {:>8}", self.indeg_p50);
        println!("    p90: {:>8}", self.indeg_p90);
        println!("    p99: {:>8}", self.indeg_p99);
        println!("    max: {:>8}", self.indeg_max);
        println!("───────────────────────────────────────────────────────────────");
        println!("  Node breakdown:");
        println!("    Fully collapsed (K=1):     {:>8} ({:.1}%)",
            self.nodes_fully_collapsed,
            100.0 * self.nodes_fully_collapsed as f64 / self.n_nodes as f64);
        println!("    Partial reduction (K<indeg): {:>6} ({:.1}%)",
            self.nodes_with_reduction,
            100.0 * self.nodes_with_reduction as f64 / self.n_nodes as f64);
        println!("    No benefit (K=indeg):      {:>8} ({:.1}%)",
            self.nodes_no_benefit,
            100.0 * self.nodes_no_benefit as f64 / self.n_nodes as f64);
        println!("───────────────────────────────────────────────────────────────");

        // Verdict
        if self.k_p50 <= 4 {
            println!("  ✅ VERDICT: Equivalence-class hybrid WILL HELP");
            println!("     Median K={} is small → significant state reduction possible", self.k_p50);
            println!("     Reduction ratio: {:.2}x fewer states than edge-based", self.reduction_ratio);
        } else if self.k_p50 <= 8 {
            println!("  ⚠️ VERDICT: Equivalence-class hybrid MAY HELP");
            println!("     Median K={} is moderate → some reduction possible", self.k_p50);
        } else {
            println!("  ❌ VERDICT: Equivalence-class hybrid UNLIKELY to help");
            println!("     Median K={} is high → edge-based is likely optimal", self.k_p50);
        }

        println!("═══════════════════════════════════════════════════════════════\n");
    }
}

// =============================================================================
// Densifier Analysis for Hybrid CCH Ordering
// =============================================================================

/// Result of densifier analysis
#[derive(Debug, Clone)]
pub struct DensifierAnalysis {
    /// Number of states analyzed
    pub n_states: usize,
    /// in×out distribution percentiles
    pub inout_p50: usize,
    pub inout_p90: usize,
    pub inout_p95: usize,
    pub inout_p99: usize,
    pub inout_p999: usize,
    pub inout_max: usize,
    /// Out-degree distribution
    pub outdeg_p50: usize,
    pub outdeg_p90: usize,
    pub outdeg_p99: usize,
    pub outdeg_max: usize,
    /// In-degree distribution
    pub indeg_p50: usize,
    pub indeg_p90: usize,
    pub indeg_p99: usize,
    pub indeg_max: usize,
    /// Top densifiers (state_id, in×out score)
    pub top_densifiers: Vec<(u32, usize)>,
    /// Thresholds for constraint-based ordering
    pub threshold_1pct: usize,
    pub threshold_5pct: usize,
    pub count_above_100: usize,
    pub count_above_50: usize,
    pub count_above_25: usize,
}

/// Analyze densifier distribution for a hybrid state graph
///
/// Densifier score = in-degree × out-degree
/// High-score nodes cause fill-in explosion if contracted early
pub fn analyze_densifiers(
    offsets: &[u64],     // CSR offsets for hybrid graph
    targets: &[u32],     // CSR targets
) -> DensifierAnalysis {
    let n_states = offsets.len() - 1;

    // Compute out-degree for each state
    let mut out_degrees: Vec<usize> = Vec::with_capacity(n_states);
    for state in 0..n_states {
        let start = offsets[state] as usize;
        let end = offsets[state + 1] as usize;
        out_degrees.push(end - start);
    }

    // Build reverse adjacency to compute in-degree
    let mut in_degrees: Vec<usize> = vec![0; n_states];
    for state in 0..n_states {
        let start = offsets[state] as usize;
        let end = offsets[state + 1] as usize;
        for i in start..end {
            let tgt = targets[i] as usize;
            if tgt < n_states {
                in_degrees[tgt] += 1;
            }
        }
    }

    // Compute in×out scores
    let mut inout_scores: Vec<(u32, usize)> = Vec::with_capacity(n_states);
    for state in 0..n_states {
        let score = in_degrees[state] * out_degrees[state];
        inout_scores.push((state as u32, score));
    }

    // Sort for percentiles
    let mut sorted_inout: Vec<usize> = inout_scores.iter().map(|(_, s)| *s).collect();
    sorted_inout.sort_unstable();

    let mut sorted_indeg = in_degrees.clone();
    sorted_indeg.sort_unstable();

    let mut sorted_outdeg = out_degrees.clone();
    sorted_outdeg.sort_unstable();

    // Percentile helper
    let percentile = |values: &[usize], p: f64| -> usize {
        if values.is_empty() {
            return 0;
        }
        let idx = ((values.len() as f64 * p) as usize).min(values.len() - 1);
        values[idx]
    };

    // Find top densifiers
    inout_scores.sort_by_key(|(_, s)| std::cmp::Reverse(*s));
    let top_densifiers: Vec<(u32, usize)> = inout_scores.iter().take(20).cloned().collect();

    // Count above thresholds
    let count_above_100 = sorted_inout.iter().filter(|&&s| s > 100).count();
    let count_above_50 = sorted_inout.iter().filter(|&&s| s > 50).count();
    let count_above_25 = sorted_inout.iter().filter(|&&s| s > 25).count();

    DensifierAnalysis {
        n_states,
        inout_p50: percentile(&sorted_inout, 0.50),
        inout_p90: percentile(&sorted_inout, 0.90),
        inout_p95: percentile(&sorted_inout, 0.95),
        inout_p99: percentile(&sorted_inout, 0.99),
        inout_p999: percentile(&sorted_inout, 0.999),
        inout_max: *sorted_inout.last().unwrap_or(&0),
        outdeg_p50: percentile(&sorted_outdeg, 0.50),
        outdeg_p90: percentile(&sorted_outdeg, 0.90),
        outdeg_p99: percentile(&sorted_outdeg, 0.99),
        outdeg_max: *sorted_outdeg.last().unwrap_or(&0),
        indeg_p50: percentile(&sorted_indeg, 0.50),
        indeg_p90: percentile(&sorted_indeg, 0.90),
        indeg_p99: percentile(&sorted_indeg, 0.99),
        indeg_max: *sorted_indeg.last().unwrap_or(&0),
        top_densifiers,
        threshold_1pct: percentile(&sorted_inout, 0.99),
        threshold_5pct: percentile(&sorted_inout, 0.95),
        count_above_100,
        count_above_50,
        count_above_25,
    }
}

impl DensifierAnalysis {
    /// Print analysis results
    pub fn print(&self) {
        println!("\n═══════════════════════════════════════════════════════════════");
        println!("  DENSIFIER ANALYSIS (in × out)");
        println!("═══════════════════════════════════════════════════════════════");
        println!("  States analyzed: {:>12}", self.n_states);
        println!("───────────────────────────────────────────────────────────────");
        println!("  in×out distribution (densifier score):");
        println!("    p50:  {:>8}", self.inout_p50);
        println!("    p90:  {:>8}", self.inout_p90);
        println!("    p95:  {:>8}", self.inout_p95);
        println!("    p99:  {:>8}", self.inout_p99);
        println!("    p999: {:>8}", self.inout_p999);
        println!("    max:  {:>8}", self.inout_max);
        println!("───────────────────────────────────────────────────────────────");
        println!("  Out-degree distribution:");
        println!("    p50: {:>8}  p90: {:>8}  p99: {:>8}  max: {:>8}",
            self.outdeg_p50, self.outdeg_p90, self.outdeg_p99, self.outdeg_max);
        println!("  In-degree distribution:");
        println!("    p50: {:>8}  p90: {:>8}  p99: {:>8}  max: {:>8}",
            self.indeg_p50, self.indeg_p90, self.indeg_p99, self.indeg_max);
        println!("───────────────────────────────────────────────────────────────");
        println!("  Thresholds for constrained ordering:");
        println!("    Top 1%  (force late): in×out > {:>6}", self.threshold_1pct);
        println!("    Top 5%  (force late): in×out > {:>6}", self.threshold_5pct);
        println!("───────────────────────────────────────────────────────────────");
        println!("  States above thresholds:");
        println!("    in×out > 100: {:>8} ({:.3}%)",
            self.count_above_100, 100.0 * self.count_above_100 as f64 / self.n_states as f64);
        println!("    in×out > 50:  {:>8} ({:.3}%)",
            self.count_above_50, 100.0 * self.count_above_50 as f64 / self.n_states as f64);
        println!("    in×out > 25:  {:>8} ({:.3}%)",
            self.count_above_25, 100.0 * self.count_above_25 as f64 / self.n_states as f64);
        println!("───────────────────────────────────────────────────────────────");
        println!("  Top 20 densifiers:");
        for (i, (state, score)) in self.top_densifiers.iter().take(10).enumerate() {
            println!("    #{:>2}: state {:>8} → in×out = {:>6}", i + 1, state, score);
        }
        if self.top_densifiers.len() > 10 {
            println!("    ... (showing top 10 of 20)");
        }
        println!("───────────────────────────────────────────────────────────────");

        // Recommendations
        if self.inout_max > 500 {
            println!("  ⚠️  WARNING: Very high max densifier ({})", self.inout_max);
            println!("     These nodes MUST be contracted late to avoid fill-in explosion");
        }
        if self.count_above_100 > 0 {
            println!("  → Recommendation: Force {} states with in×out > 100 to late ranks",
                self.count_above_100);
        }
        println!("═══════════════════════════════════════════════════════════════\n");
    }
}
