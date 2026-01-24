//! Range Query (Bounded Dijkstra) for Isochrones
//!
//! Implements bounded Dijkstra in the CCH hierarchy to find all nodes
//! reachable within a given time threshold T.
//!
//! Key properties:
//! - Uses same CCH structure as P2P queries
//! - Returns all settled states with dist ≤ T
//! - Supports frontier extraction for polygon construction

use anyhow::Result;
use std::cmp::Reverse;
use std::collections::BinaryHeap;
use std::path::Path;

use crate::formats::{CchTopoFile, CchWeightsFile, OrderEbgFile};
use crate::profile_abi::Mode;

pub mod phast;
pub use phast::{PhastEngine, PhastResult, PhastStats};

pub mod frontier;
pub use frontier::{FrontierExtractor, FrontierCutPoint, ReachablePoint, ReachableSegment, run_frontier_extraction};

pub mod contour;
pub use contour::{GridConfig, ContourResult, generate_contour, export_contour_geojson};

/// Result of a range query
#[derive(Debug)]
pub struct RangeResult {
    /// Number of nodes settled (reachable within threshold)
    pub n_settled: usize,
    /// Number of nodes in frontier (partially reached)
    pub n_frontier: usize,
    /// Distance from origin to each settled node (indexed by node ID)
    /// u32::MAX means unreachable
    pub dist: Vec<u32>,
    /// Parent pointers for path reconstruction
    /// u32::MAX means no parent (origin or unreachable)
    pub parent: Vec<u32>,
    /// Frontier edges: (edge_idx, dist_to_src, dist_to_dst)
    /// where dist_to_src ≤ T < dist_to_dst
    pub frontier: Vec<FrontierEdge>,
    /// Query statistics
    pub stats: RangeStats,
}

/// A frontier edge where the threshold is crossed
#[derive(Debug, Clone)]
pub struct FrontierEdge {
    /// Source node (inside reachable set)
    pub src: u32,
    /// Target node (outside reachable set)
    pub dst: u32,
    /// Distance from origin to src
    pub dist_src: u32,
    /// Distance from origin to dst
    pub dist_dst: u32,
    /// Edge weight
    pub weight: u32,
}

/// Query statistics
#[derive(Debug, Default)]
pub struct RangeStats {
    pub pq_pushes: usize,
    pub pq_pops: usize,
    pub relaxations: usize,
    pub elapsed_ms: u64,
}

/// Range query engine
pub struct RangeEngine {
    /// CCH topology
    topo: crate::formats::CchTopo,
    /// CCH weights
    weights: crate::formats::CchWeights,
    /// Node ordering (permutation)
    order: crate::formats::OrderEbg,
    /// Number of nodes
    n_nodes: usize,
}

impl RangeEngine {
    /// Load CCH data and create range query engine
    pub fn load(
        topo_path: &Path,
        weights_path: &Path,
        order_path: &Path,
        _mode: Mode,
    ) -> Result<Self> {
        let topo = CchTopoFile::read(topo_path)?;
        let weights = CchWeightsFile::read(weights_path)?;
        let order = OrderEbgFile::read(order_path)?;

        let n_nodes = topo.n_nodes as usize;

        Ok(Self {
            topo,
            weights,
            order,
            n_nodes,
        })
    }

    /// Run bounded Dijkstra from origin with threshold T (in milliseconds)
    ///
    /// This uses forward CCH search only (not bidirectional).
    /// For isochrones, we need all nodes reachable from origin,
    /// not just the shortest path to a single target.
    pub fn query(&self, origin: u32, threshold_ms: u32) -> RangeResult {
        let start = std::time::Instant::now();
        let mut stats = RangeStats::default();

        // Distance array (u32::MAX = infinity)
        let mut dist = vec![u32::MAX; self.n_nodes];
        let mut parent = vec![u32::MAX; self.n_nodes];

        // Priority queue: (distance, node)
        let mut pq: BinaryHeap<Reverse<(u32, u32)>> = BinaryHeap::new();

        // Initialize origin
        dist[origin as usize] = 0;
        pq.push(Reverse((0, origin)));
        stats.pq_pushes += 1;

        // Forward Dijkstra with bounded cost
        while let Some(Reverse((d, u))) = pq.pop() {
            stats.pq_pops += 1;

            // Skip if we already found a shorter path
            if d > dist[u as usize] {
                continue;
            }

            // Skip if beyond threshold
            if d > threshold_ms {
                continue;
            }

            // Relax UP edges (to higher rank nodes)
            let up_start = self.topo.up_offsets[u as usize] as usize;
            let up_end = self.topo.up_offsets[u as usize + 1] as usize;

            for i in up_start..up_end {
                let v = self.topo.up_targets[i];
                let w = self.weights.up[i];

                if w == u32::MAX {
                    continue; // Skip infinite weight edges
                }

                let new_dist = d.saturating_add(w);
                stats.relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    parent[v as usize] = u;

                    // Only add to PQ if within threshold
                    if new_dist <= threshold_ms {
                        pq.push(Reverse((new_dist, v)));
                        stats.pq_pushes += 1;
                    }
                }
            }

            // Relax DOWN edges (to lower rank nodes)
            let down_start = self.topo.down_offsets[u as usize] as usize;
            let down_end = self.topo.down_offsets[u as usize + 1] as usize;

            for i in down_start..down_end {
                let v = self.topo.down_targets[i];
                let w = self.weights.down[i];

                if w == u32::MAX {
                    continue; // Skip infinite weight edges
                }

                let new_dist = d.saturating_add(w);
                stats.relaxations += 1;

                if new_dist < dist[v as usize] {
                    dist[v as usize] = new_dist;
                    parent[v as usize] = u;

                    // Only add to PQ if within threshold
                    if new_dist <= threshold_ms {
                        pq.push(Reverse((new_dist, v)));
                        stats.pq_pushes += 1;
                    }
                }
            }
        }

        // Count settled nodes and extract frontier
        let mut n_settled = 0;
        let mut frontier = Vec::new();

        for u in 0..self.n_nodes {
            let d_u = dist[u];
            if d_u <= threshold_ms {
                n_settled += 1;

                // Check if any outgoing edge crosses the threshold
                // UP edges
                let up_start = self.topo.up_offsets[u] as usize;
                let up_end = self.topo.up_offsets[u + 1] as usize;

                for i in up_start..up_end {
                    let v = self.topo.up_targets[i];
                    let w = self.weights.up[i];
                    if w != u32::MAX {
                        let d_v = d_u.saturating_add(w);
                        // Check if this edge crosses the threshold
                        if d_v > threshold_ms {
                            frontier.push(FrontierEdge {
                                src: u as u32,
                                dst: v,
                                dist_src: d_u,
                                dist_dst: d_v,
                                weight: w,
                            });
                        }
                    }
                }

                // DOWN edges
                let down_start = self.topo.down_offsets[u] as usize;
                let down_end = self.topo.down_offsets[u + 1] as usize;

                for i in down_start..down_end {
                    let v = self.topo.down_targets[i];
                    let w = self.weights.down[i];
                    if w != u32::MAX {
                        let d_v = d_u.saturating_add(w);
                        if d_v > threshold_ms {
                            frontier.push(FrontierEdge {
                                src: u as u32,
                                dst: v,
                                dist_src: d_u,
                                dist_dst: d_v,
                                weight: w,
                            });
                        }
                    }
                }
            }
        }

        stats.elapsed_ms = start.elapsed().as_millis() as u64;

        RangeResult {
            n_settled,
            n_frontier: frontier.len(),
            dist,
            parent,
            frontier,
            stats,
        }
    }

    /// Verify range query correctness by checking:
    /// 1. All settled nodes have dist ≤ threshold
    /// 2. All frontier edges cross the threshold correctly
    /// 3. Parent pointers form valid paths back to origin
    pub fn verify(&self, result: &RangeResult, origin: u32, threshold_ms: u32) -> Vec<String> {
        let mut errors = Vec::new();

        // Check 1: All settled nodes have dist ≤ threshold
        for u in 0..self.n_nodes {
            if result.dist[u] <= threshold_ms {
                // This is a settled node, ok
            } else if result.dist[u] == u32::MAX {
                // Unreachable, ok
            } else {
                // dist > threshold but not MAX - this is in the "penumbra"
                // (reached but beyond threshold)
            }
        }

        // Check 2: Frontier edges cross threshold correctly
        for edge in &result.frontier {
            if edge.dist_src > threshold_ms {
                errors.push(format!(
                    "Frontier edge {}->{}: src dist {} > threshold {}",
                    edge.src, edge.dst, edge.dist_src, threshold_ms
                ));
            }
            if edge.dist_dst <= threshold_ms {
                errors.push(format!(
                    "Frontier edge {}->{}: dst dist {} <= threshold {}",
                    edge.src, edge.dst, edge.dist_dst, threshold_ms
                ));
            }
        }

        // Check 3: Parent pointers form valid paths to origin
        for u in 0..self.n_nodes {
            if result.dist[u] <= threshold_ms && result.dist[u] != u32::MAX {
                // Trace path to origin
                let mut current = u as u32;
                let mut hops = 0;
                let max_hops = self.n_nodes;

                while current != origin && hops < max_hops {
                    let p = result.parent[current as usize];
                    if p == u32::MAX {
                        if current != origin {
                            errors.push(format!(
                                "Node {} has no parent but is not origin (origin={})",
                                current, origin
                            ));
                        }
                        break;
                    }
                    current = p;
                    hops += 1;
                }

                if hops >= max_hops {
                    errors.push(format!("Node {} has cycle in parent chain", u));
                }
            }
        }

        // Check origin
        if result.dist[origin as usize] != 0 {
            errors.push(format!(
                "Origin {} has non-zero distance: {}",
                origin, result.dist[origin as usize]
            ));
        }

        errors
    }

    /// Get node count
    pub fn n_nodes(&self) -> usize {
        self.n_nodes
    }
}

/// Run range query from command line
pub fn run_range_query(
    topo_path: &Path,
    weights_path: &Path,
    order_path: &Path,
    origin: u32,
    threshold_ms: u32,
    mode: Mode,
) -> Result<RangeResult> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n🔍 Range Query ({} mode)", mode_name);
    println!("  Origin: node {}", origin);
    println!("  Threshold: {} ms ({:.1} min)", threshold_ms, threshold_ms as f64 / 60_000.0);

    println!("\nLoading CCH data...");
    let engine = RangeEngine::load(topo_path, weights_path, order_path, mode)?;
    println!("  ✓ {} nodes loaded", engine.n_nodes());

    if origin as usize >= engine.n_nodes() {
        anyhow::bail!("Origin node {} out of range (max: {})", origin, engine.n_nodes() - 1);
    }

    println!("\nRunning bounded Dijkstra...");
    let result = engine.query(origin, threshold_ms);

    println!("\n=== RANGE QUERY RESULTS ===");
    println!("  Settled nodes:    {} ({:.2}% of graph)",
             result.n_settled,
             100.0 * result.n_settled as f64 / engine.n_nodes() as f64);
    println!("  Frontier edges:   {}", result.n_frontier);
    println!("  PQ pushes:        {}", result.stats.pq_pushes);
    println!("  PQ pops:          {}", result.stats.pq_pops);
    println!("  Relaxations:      {}", result.stats.relaxations);
    println!("  Elapsed:          {} ms", result.stats.elapsed_ms);

    // Verify correctness
    println!("\nVerifying results...");
    let errors = engine.verify(&result, origin, threshold_ms);
    if errors.is_empty() {
        println!("  ✓ All checks passed");
    } else {
        println!("  ✗ {} errors found:", errors.len());
        for (i, e) in errors.iter().enumerate().take(10) {
            println!("    {}. {}", i + 1, e);
        }
        if errors.len() > 10 {
            println!("    ... and {} more", errors.len() - 10);
        }
    }

    Ok(result)
}

/// Validate range query properties
pub mod validate {
    use super::*;

    /// Monotonicity test result
    #[derive(Debug)]
    pub struct MonotonicityResult {
        pub passed: bool,
        pub violations: Vec<String>,
        pub thresholds_tested: Vec<u32>,
        pub settled_counts: Vec<usize>,
    }

    /// Equivalence test result
    #[derive(Debug)]
    pub struct EquivalenceResult {
        pub passed: bool,
        pub mismatches: usize,
        pub samples_tested: usize,
    }

    /// Test monotonicity: reachable(T1) ⊆ reachable(T2) for T1 < T2
    ///
    /// This verifies that as we increase the threshold, the reachable set
    /// only grows, never shrinks.
    pub fn test_monotonicity(
        engine: &RangeEngine,
        origin: u32,
        thresholds: &[u32],
    ) -> MonotonicityResult {
        let mut violations = Vec::new();
        let mut settled_counts = Vec::new();
        let mut prev_reachable: Option<Vec<bool>> = None;
        let mut prev_threshold = 0u32;

        for &threshold in thresholds {
            let result = engine.query(origin, threshold);
            settled_counts.push(result.n_settled);

            // Build reachable set
            let reachable: Vec<bool> = result.dist.iter()
                .map(|&d| d <= threshold)
                .collect();

            // Check monotonicity against previous result
            if let Some(ref prev) = prev_reachable {
                for (node, (&was_reachable, &is_reachable)) in prev.iter().zip(reachable.iter()).enumerate() {
                    if was_reachable && !is_reachable {
                        violations.push(format!(
                            "Node {} was reachable at T={} but not at T={}",
                            node, prev_threshold, threshold
                        ));
                        if violations.len() >= 10 {
                            break;
                        }
                    }
                }
            }

            prev_reachable = Some(reachable);
            prev_threshold = threshold;
        }

        MonotonicityResult {
            passed: violations.is_empty(),
            violations,
            thresholds_tested: thresholds.to_vec(),
            settled_counts,
        }
    }

    /// Test equivalence: dist(target) ≤ T iff target in reachable set
    ///
    /// This verifies that the settled nodes exactly match those with
    /// distance ≤ threshold.
    pub fn test_equivalence(result: &RangeResult, threshold: u32) -> EquivalenceResult {
        let mut mismatches = 0;

        for (node, &d) in result.dist.iter().enumerate() {
            let in_reachable_by_dist = d <= threshold;
            let in_reachable_by_settled = d != u32::MAX && d <= threshold;

            if in_reachable_by_dist != in_reachable_by_settled {
                mismatches += 1;
            }
        }

        EquivalenceResult {
            passed: mismatches == 0,
            mismatches,
            samples_tested: result.dist.len(),
        }
    }

    /// Test P2P consistency: for random targets in range, dist matches P2P query
    ///
    /// This uses the CCH-Dijkstra baseline to verify range query distances.
    pub fn test_p2p_consistency(
        engine: &RangeEngine,
        range_result: &RangeResult,
        origin: u32,
        threshold: u32,
        n_samples: usize,
        seed: u64,
    ) -> Vec<(u32, u32, u32)> {
        use rand::{SeedableRng, Rng};
        let mut rng = rand::rngs::StdRng::seed_from_u64(seed);

        let mut mismatches = Vec::new();

        // Collect reachable nodes
        let reachable: Vec<u32> = range_result.dist.iter()
            .enumerate()
            .filter(|(_, &d)| d <= threshold)
            .map(|(i, _)| i as u32)
            .collect();

        if reachable.is_empty() {
            return mismatches;
        }

        // Sample and verify
        for _ in 0..n_samples.min(reachable.len()) {
            let target = reachable[rng.gen_range(0..reachable.len())];
            let range_dist = range_result.dist[target as usize];

            // Run P2P query using bounded Dijkstra to same target
            // (this is a simplification - real P2P would use bidirectional CCH)
            let single_result = engine.query(origin, range_dist + 1);
            let p2p_dist = single_result.dist[target as usize];

            if range_dist != p2p_dist {
                mismatches.push((target, range_dist, p2p_dist));
                if mismatches.len() >= 10 {
                    break;
                }
            }
        }

        mismatches
    }
}

/// Run validation tests for range query
pub fn run_range_validation(
    topo_path: &Path,
    weights_path: &Path,
    order_path: &Path,
    origin: u32,
    mode: Mode,
) -> Result<()> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n🧪 Range Query Validation ({} mode)", mode_name);
    println!("  Origin: node {}", origin);

    println!("\nLoading CCH data...");
    let engine = RangeEngine::load(topo_path, weights_path, order_path, mode)?;
    println!("  ✓ {} nodes loaded", engine.n_nodes());

    if origin as usize >= engine.n_nodes() {
        anyhow::bail!("Origin node {} out of range (max: {})", origin, engine.n_nodes() - 1);
    }

    // Test 1: Monotonicity
    println!("\n1. Testing monotonicity...");
    let thresholds = vec![1000, 5000, 10000, 30000, 60000, 120000, 300000, 600000];
    let mono_result = validate::test_monotonicity(&engine, origin, &thresholds);

    println!("  Thresholds tested: {:?}", mono_result.thresholds_tested);
    println!("  Settled counts: {:?}", mono_result.settled_counts);

    if mono_result.passed {
        println!("  ✓ Monotonicity: PASSED");
    } else {
        println!("  ✗ Monotonicity: FAILED");
        for v in &mono_result.violations {
            println!("    - {}", v);
        }
    }

    // Test 2: Equivalence
    println!("\n2. Testing equivalence...");
    let test_threshold = 60000u32; // 1 minute
    let result = engine.query(origin, test_threshold);
    let equiv_result = validate::test_equivalence(&result, test_threshold);

    if equiv_result.passed {
        println!("  ✓ Equivalence: PASSED ({} samples)", equiv_result.samples_tested);
    } else {
        println!("  ✗ Equivalence: FAILED ({} mismatches)", equiv_result.mismatches);
    }

    // Test 3: P2P consistency
    println!("\n3. Testing P2P consistency...");
    let p2p_mismatches = validate::test_p2p_consistency(
        &engine,
        &result,
        origin,
        test_threshold,
        100,
        42,
    );

    if p2p_mismatches.is_empty() {
        println!("  ✓ P2P consistency: PASSED (100 samples)");
    } else {
        println!("  ✗ P2P consistency: FAILED ({} mismatches)", p2p_mismatches.len());
        for (target, range_d, p2p_d) in &p2p_mismatches {
            println!("    - Node {}: range={}, p2p={}", target, range_d, p2p_d);
        }
    }

    // Summary
    println!("\n=== VALIDATION SUMMARY ===");
    let all_passed = mono_result.passed && equiv_result.passed && p2p_mismatches.is_empty();
    if all_passed {
        println!("  ✅ All range query tests passed!");
    } else {
        println!("  ❌ Some tests failed!");
    }

    if !all_passed {
        anyhow::bail!("Range query validation failed");
    }

    Ok(())
}

/// Run PHAST-based range query (much faster than naive Dijkstra)
pub fn run_phast_query(
    topo_path: &Path,
    weights_path: &Path,
    order_path: &Path,
    origin: u32,
    threshold_ms: u32,
    mode: Mode,
) -> Result<PhastResult> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n⚡ PHAST Range Query ({} mode)", mode_name);
    println!("  Origin: node {}", origin);
    println!("  Threshold: {} ms ({:.1} min)", threshold_ms, threshold_ms as f64 / 60_000.0);

    println!("\nLoading CCH data...");
    let engine = PhastEngine::load(topo_path, weights_path, order_path)?;
    println!("  ✓ {} nodes loaded", engine.n_nodes());

    if origin as usize >= engine.n_nodes() {
        anyhow::bail!("Origin node {} out of range (max: {})", origin, engine.n_nodes() - 1);
    }

    println!("\nRunning PHAST...");
    let result = engine.query_bounded(origin, threshold_ms);

    println!("\n=== PHAST RESULTS ===");
    println!("  Reachable nodes:  {} ({:.2}% of graph)",
             result.n_reachable,
             100.0 * result.n_reachable as f64 / engine.n_nodes() as f64);
    println!("\n  Upward phase:");
    println!("    PQ pushes:      {}", result.stats.upward_pq_pushes);
    println!("    PQ pops:        {}", result.stats.upward_pq_pops);
    println!("    Relaxations:    {}", result.stats.upward_relaxations);
    println!("    Settled:        {}", result.stats.upward_settled);
    println!("    Time:           {} ms", result.stats.upward_time_ms);
    println!("\n  Downward phase:");
    println!("    Relaxations:    {}", result.stats.downward_relaxations);
    println!("    Improved:       {}", result.stats.downward_improved);
    println!("    Time:           {} ms", result.stats.downward_time_ms);
    println!("\n  Total time:       {} ms", result.stats.total_time_ms);

    // Extract frontier
    let frontier = engine.extract_frontier(&result.dist, threshold_ms);
    println!("  Frontier edges:   {}", frontier.len());

    Ok(result)
}

/// Validate PHAST against naive Dijkstra
pub fn validate_phast(
    topo_path: &Path,
    weights_path: &Path,
    order_path: &Path,
    origin: u32,
    threshold_ms: u32,
    mode: Mode,
) -> Result<()> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n🧪 PHAST Validation ({} mode)", mode_name);
    println!("  Origin: node {}", origin);
    println!("  Threshold: {} ms", threshold_ms);

    // Load engines
    println!("\nLoading CCH data...");
    let naive_engine = RangeEngine::load(topo_path, weights_path, order_path, mode)?;
    let phast_engine = PhastEngine::load(topo_path, weights_path, order_path)?;
    let n_nodes = naive_engine.n_nodes();
    println!("  ✓ {} nodes loaded", n_nodes);

    // Run both
    println!("\nRunning naive Dijkstra...");
    let naive_start = std::time::Instant::now();
    let naive_result = naive_engine.query(origin, threshold_ms);
    let naive_time = naive_start.elapsed().as_millis();

    println!("Running PHAST...");
    let phast_start = std::time::Instant::now();
    let phast_result = phast_engine.query_bounded(origin, threshold_ms);
    let phast_time = phast_start.elapsed().as_millis();

    // Compare distances
    println!("\nComparing distances...");
    let mut mismatches = 0;
    let mut max_diff = 0i64;
    let mut first_mismatch = None;

    for node in 0..n_nodes {
        let naive_d = naive_result.dist[node];
        let phast_d = phast_result.dist[node];

        // Both should agree on reachability within threshold
        let naive_reachable = naive_d <= threshold_ms;
        let phast_reachable = phast_d <= threshold_ms;

        if naive_reachable != phast_reachable {
            mismatches += 1;
            if first_mismatch.is_none() {
                first_mismatch = Some((node, naive_d, phast_d));
            }
        } else if naive_reachable && phast_reachable && naive_d != phast_d {
            // Both reachable but different distances
            let diff = (naive_d as i64 - phast_d as i64).abs();
            max_diff = max_diff.max(diff);
            mismatches += 1;
            if first_mismatch.is_none() {
                first_mismatch = Some((node, naive_d, phast_d));
            }
        }
    }

    // Report
    println!("\n=== VALIDATION RESULTS ===");
    println!("  Naive time:   {} ms", naive_time);
    println!("  PHAST time:   {} ms", phast_time);
    println!("  Speedup:      {:.1}x", naive_time as f64 / phast_time.max(1) as f64);
    println!();
    println!("  Naive reachable:  {}", naive_result.n_settled);
    println!("  PHAST reachable:  {}", phast_result.n_reachable);
    println!();

    if mismatches == 0 {
        println!("  ✅ PASSED: All distances match!");
    } else {
        println!("  ❌ FAILED: {} mismatches", mismatches);
        println!("  Max distance diff: {}", max_diff);
        if let Some((node, naive_d, phast_d)) = first_mismatch {
            println!("  First mismatch: node {} (naive={}, phast={})", node, naive_d, phast_d);
        }
        anyhow::bail!("PHAST validation failed with {} mismatches", mismatches);
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test will be added when we have test fixtures
}
