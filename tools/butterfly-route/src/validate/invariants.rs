//! Graph and Weight Invariant Validation
//!
//! Fast-fail checks for CCH correctness:
//! - Non-negative weights (required for Dijkstra)
//! - No overflow potential
//! - Deterministic tie-breaking
//! - Weight domain sanity

use anyhow::Result;
use std::path::Path;

use crate::formats::{CchTopoFile, CchWeightsFile, OrderEbgFile};
use crate::profile_abi::Mode;

/// Invariant check results
#[derive(Debug, Default)]
pub struct InvariantResult {
    pub passed: bool,
    pub checks_run: usize,
    pub checks_passed: usize,
    pub errors: Vec<String>,
    pub warnings: Vec<String>,
}

impl InvariantResult {
    fn new() -> Self {
        Self {
            passed: true,
            ..Default::default()
        }
    }

    fn fail(&mut self, msg: String) {
        self.passed = false;
        self.errors.push(msg);
    }

    fn warn(&mut self, msg: String) {
        self.warnings.push(msg);
    }

    fn check_passed(&mut self) {
        self.checks_run += 1;
        self.checks_passed += 1;
    }

    fn check_failed(&mut self, msg: String) {
        self.checks_run += 1;
        self.fail(msg);
    }
}

/// Maximum reasonable weight (24 hours in milliseconds)
const MAX_REASONABLE_WEIGHT: u32 = 24 * 60 * 60 * 1000;

/// Run all invariant checks on CCH data
pub fn validate_invariants(
    topo_path: &Path,
    weights_path: &Path,
    order_path: &Path,
    mode: Mode,
) -> Result<InvariantResult> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n🔍 Invariant Validation ({} mode)", mode_name);

    // Load data
    println!("\nLoading CCH data...");
    let topo = CchTopoFile::read(topo_path)?;
    let weights = CchWeightsFile::read(weights_path)?;
    let order = OrderEbgFile::read(order_path)?;

    println!("  ✓ {} nodes, {} up edges, {} down edges",
             topo.n_nodes, topo.up_targets.len(), topo.down_targets.len());

    let mut result = InvariantResult::new();

    // 1. Non-negative weights
    println!("\n1. Checking non-negative weights...");
    check_non_negative_weights(&weights, &mut result);

    // 2. Weight domain sanity
    println!("\n2. Checking weight domain...");
    check_weight_domain(&weights, &mut result);

    // 3. No INF on supposedly finite paths
    println!("\n3. Checking for unexpected INF weights...");
    check_inf_consistency(&topo, &weights, &mut result);

    // 4. CSR structure validity
    println!("\n4. Checking CSR structure...");
    check_csr_structure(&topo, &mut result);

    // 5. Hierarchy property
    println!("\n5. Checking hierarchy property...");
    check_hierarchy_property(&topo, &order, &mut result);

    // 6. Overflow potential
    println!("\n6. Checking overflow potential...");
    check_overflow_potential(&topo, &weights, &mut result);

    // 7. Deterministic tie-breaking readiness
    println!("\n7. Checking deterministic tie-breaking...");
    check_tie_breaking_readiness(&topo, &weights, &mut result);

    // Summary
    println!("\n=== INVARIANT CHECK SUMMARY ===");
    println!("  Checks run:    {}", result.checks_run);
    println!("  Checks passed: {}", result.checks_passed);
    println!("  Errors:        {}", result.errors.len());
    println!("  Warnings:      {}", result.warnings.len());

    if !result.errors.is_empty() {
        println!("\n  Errors:");
        for (i, e) in result.errors.iter().enumerate() {
            println!("    {}. {}", i + 1, e);
        }
    }

    if !result.warnings.is_empty() {
        println!("\n  Warnings:");
        for (i, w) in result.warnings.iter().enumerate() {
            println!("    {}. {}", i + 1, w);
        }
    }

    if result.passed {
        println!("\n✅ All invariant checks passed!");
    } else {
        println!("\n❌ Invariant checks failed!");
    }

    Ok(result)
}

/// Check 1: All weights must be non-negative (u32 is inherently non-negative)
/// But we check for "suspicious" patterns like very large values near MAX
fn check_non_negative_weights(weights: &crate::formats::CchWeights, result: &mut InvariantResult) {
    // u32 is always non-negative, so this always passes
    // But we check for suspicious patterns
    let near_max_threshold = u32::MAX - 1000;

    let mut near_max_up = 0usize;
    let mut near_max_down = 0usize;

    for &w in &weights.up {
        if w >= near_max_threshold && w != u32::MAX {
            near_max_up += 1;
        }
    }

    for &w in &weights.down {
        if w >= near_max_threshold && w != u32::MAX {
            near_max_down += 1;
        }
    }

    if near_max_up > 0 || near_max_down > 0 {
        result.warn(format!(
            "Found {} UP and {} DOWN weights near u32::MAX (potential overflow risk)",
            near_max_up, near_max_down
        ));
    }

    result.check_passed();
    println!("  ✓ All weights non-negative (u32)");
}

/// Check 2: Weight values are in reasonable domain
fn check_weight_domain(weights: &crate::formats::CchWeights, result: &mut InvariantResult) {
    let mut max_up = 0u32;
    let mut max_down = 0u32;
    let mut inf_up = 0usize;
    let mut inf_down = 0usize;
    let mut zero_up = 0usize;
    let mut zero_down = 0usize;
    let mut excessive_up = 0usize;
    let mut excessive_down = 0usize;

    for &w in &weights.up {
        if w == u32::MAX {
            inf_up += 1;
        } else {
            max_up = max_up.max(w);
            if w == 0 {
                zero_up += 1;
            }
            if w > MAX_REASONABLE_WEIGHT {
                excessive_up += 1;
            }
        }
    }

    for &w in &weights.down {
        if w == u32::MAX {
            inf_down += 1;
        } else {
            max_down = max_down.max(w);
            if w == 0 {
                zero_down += 1;
            }
            if w > MAX_REASONABLE_WEIGHT {
                excessive_down += 1;
            }
        }
    }

    let total_up = weights.up.len();
    let total_down = weights.down.len();
    let inf_pct_up = (inf_up as f64 / total_up as f64) * 100.0;
    let inf_pct_down = (inf_down as f64 / total_down as f64) * 100.0;

    println!("  UP edges:   max={} ms, INF={} ({:.2}%), zero={}",
             max_up, inf_up, inf_pct_up, zero_up);
    println!("  DOWN edges: max={} ms, INF={} ({:.2}%), zero={}",
             max_down, inf_down, inf_pct_down, zero_down);

    // Check for excessive values
    if excessive_up > 0 || excessive_down > 0 {
        result.warn(format!(
            "{} UP and {} DOWN edges exceed 24h ({}ms) - may indicate issues",
            excessive_up, excessive_down, MAX_REASONABLE_WEIGHT
        ));
    }

    // Check for too many INF edges (could indicate problems)
    if inf_pct_up > 50.0 || inf_pct_down > 50.0 {
        result.warn(format!(
            "High INF percentage: {:.1}% UP, {:.1}% DOWN",
            inf_pct_up, inf_pct_down
        ));
    }

    result.check_passed();
    println!("  ✓ Weight domain checked");
}

/// Check 3: INF weights should only appear where expected
fn check_inf_consistency(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    result: &mut InvariantResult,
) {
    // Count nodes that have ALL INF outgoing edges (both UP and DOWN)
    let n = topo.n_nodes as usize;
    let mut isolated_nodes = 0usize;

    for u in 0..n {
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        let down_start = topo.down_offsets[u] as usize;
        let down_end = topo.down_offsets[u + 1] as usize;

        let has_finite_up = (up_start..up_end).any(|i| weights.up[i] != u32::MAX);
        let has_finite_down = (down_start..down_end).any(|i| weights.down[i] != u32::MAX);

        if !has_finite_up && !has_finite_down && (up_end > up_start || down_end > down_start) {
            isolated_nodes += 1;
        }
    }

    if isolated_nodes > 0 {
        result.warn(format!(
            "{} nodes have edges but all with INF weight (mode-specific filtering expected)",
            isolated_nodes
        ));
    }

    result.check_passed();
    println!("  ✓ INF consistency checked ({} isolated nodes)", isolated_nodes);
}

/// Check 4: CSR offsets are valid
fn check_csr_structure(topo: &crate::formats::CchTopo, result: &mut InvariantResult) {
    let n = topo.n_nodes as usize;

    // Check UP offsets are monotonically increasing
    let mut up_valid = true;
    for i in 0..n {
        if topo.up_offsets[i] > topo.up_offsets[i + 1] {
            up_valid = false;
            result.check_failed(format!(
                "UP offsets not monotonic at node {}: {} > {}",
                i, topo.up_offsets[i], topo.up_offsets[i + 1]
            ));
            break;
        }
    }
    if up_valid {
        result.check_passed();
        println!("  ✓ UP CSR offsets valid");
    }

    // Check DOWN offsets are monotonically increasing
    let mut down_valid = true;
    for i in 0..n {
        if topo.down_offsets[i] > topo.down_offsets[i + 1] {
            down_valid = false;
            result.check_failed(format!(
                "DOWN offsets not monotonic at node {}: {} > {}",
                i, topo.down_offsets[i], topo.down_offsets[i + 1]
            ));
            break;
        }
    }
    if down_valid {
        result.check_passed();
        println!("  ✓ DOWN CSR offsets valid");
    }

    // Check target nodes are in range
    let mut targets_valid = true;
    for &t in &topo.up_targets {
        if t as usize >= n {
            targets_valid = false;
            result.check_failed(format!("UP target {} out of range (n={})", t, n));
            break;
        }
    }
    for &t in &topo.down_targets {
        if t as usize >= n {
            targets_valid = false;
            result.check_failed(format!("DOWN target {} out of range (n={})", t, n));
            break;
        }
    }
    if targets_valid {
        result.check_passed();
        println!("  ✓ All targets in range");
    }
}

/// Check 5: Hierarchy property (UP goes to higher rank, DOWN goes to lower)
fn check_hierarchy_property(
    topo: &crate::formats::CchTopo,
    order: &crate::formats::OrderEbg,
    result: &mut InvariantResult,
) {
    let n = topo.n_nodes as usize;
    let perm = &order.perm;

    // Check UP edges go to higher rank
    let mut up_violations = 0usize;
    for u in 0..n {
        let rank_u = perm[u];
        let start = topo.up_offsets[u] as usize;
        let end = topo.up_offsets[u + 1] as usize;
        for i in start..end {
            let v = topo.up_targets[i] as usize;
            let rank_v = perm[v];
            if rank_v <= rank_u {
                up_violations += 1;
                if up_violations <= 3 {
                    result.fail(format!(
                        "UP edge {}→{} violates hierarchy: rank[{}]={} >= rank[{}]={}",
                        u, v, u, rank_u, v, rank_v
                    ));
                }
            }
        }
    }

    if up_violations == 0 {
        result.check_passed();
        println!("  ✓ All UP edges go to higher rank");
    } else {
        result.check_failed(format!("{} UP edges violate hierarchy", up_violations));
    }

    // Check DOWN edges go to lower rank
    let mut down_violations = 0usize;
    for u in 0..n {
        let rank_u = perm[u];
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let v = topo.down_targets[i] as usize;
            let rank_v = perm[v];
            if rank_v >= rank_u {
                down_violations += 1;
                if down_violations <= 3 {
                    result.fail(format!(
                        "DOWN edge {}→{} violates hierarchy: rank[{}]={} <= rank[{}]={}",
                        u, v, u, rank_u, v, rank_v
                    ));
                }
            }
        }
    }

    if down_violations == 0 {
        result.check_passed();
        println!("  ✓ All DOWN edges go to lower rank");
    } else {
        result.check_failed(format!("{} DOWN edges violate hierarchy", down_violations));
    }
}

/// Check 6: Potential for overflow in path cost accumulation
fn check_overflow_potential(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    result: &mut InvariantResult,
) {
    // Find the maximum degree (worst case for accumulation in a single chain)
    let n = topo.n_nodes as usize;
    let mut max_degree = 0usize;

    for u in 0..n {
        let up_deg = (topo.up_offsets[u + 1] - topo.up_offsets[u]) as usize;
        let down_deg = (topo.down_offsets[u + 1] - topo.down_offsets[u]) as usize;
        max_degree = max_degree.max(up_deg + down_deg);
    }

    // Find max finite weight
    let max_finite_up = weights.up.iter()
        .filter(|&&w| w != u32::MAX)
        .max()
        .copied()
        .unwrap_or(0);
    let max_finite_down = weights.down.iter()
        .filter(|&&w| w != u32::MAX)
        .max()
        .copied()
        .unwrap_or(0);
    let max_weight = max_finite_up.max(max_finite_down);

    // Theoretical worst case: n edges with max weight
    // But in practice, paths are much shorter
    // Check if max_weight * reasonable_path_length could overflow
    let reasonable_path_length = 10000u64; // 10k edges is extremely long
    let theoretical_max = (max_weight as u64).saturating_mul(reasonable_path_length);

    if theoretical_max > u32::MAX as u64 {
        result.warn(format!(
            "Potential overflow: max_weight={} × {}edges = {} > u32::MAX",
            max_weight, reasonable_path_length, theoretical_max
        ));
        println!("  ⚠ Potential overflow with very long paths");
        println!("    max_weight={}, max_degree={}", max_weight, max_degree);
        println!("    Use saturating_add in queries to prevent issues");
    } else {
        result.check_passed();
        println!("  ✓ No overflow risk for reasonable paths");
        println!("    max_weight={}, max_degree={}", max_weight, max_degree);
    }
}

/// Check 7: Deterministic tie-breaking readiness
fn check_tie_breaking_readiness(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    result: &mut InvariantResult,
) {
    // Count edges with identical weights from same source
    let n = topo.n_nodes as usize;
    let mut nodes_with_ties = 0usize;
    let mut total_tie_groups = 0usize;

    for u in 0..n {
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;

        if up_end - up_start < 2 {
            continue;
        }

        // Collect finite weights and targets
        let mut edges: Vec<(u32, u32)> = Vec::new();
        for i in up_start..up_end {
            let w = weights.up[i];
            if w != u32::MAX {
                edges.push((w, topo.up_targets[i]));
            }
        }

        // Check for duplicate weights
        edges.sort_by_key(|e| e.0);
        let mut has_tie = false;
        for i in 1..edges.len() {
            if edges[i].0 == edges[i - 1].0 {
                has_tie = true;
                total_tie_groups += 1;
            }
        }
        if has_tie {
            nodes_with_ties += 1;
        }
    }

    if nodes_with_ties > 0 {
        println!("  ⚠ {} nodes have edges with identical weights ({} tie groups)",
                 nodes_with_ties, total_tie_groups);
        println!("    Ensure deterministic tie-breaking by node_id in queries");
        result.warn(format!(
            "{} nodes have weight ties - use node_id for deterministic tie-breaking",
            nodes_with_ties
        ));
    } else {
        println!("  ✓ No weight ties detected");
    }

    result.check_passed();
}
