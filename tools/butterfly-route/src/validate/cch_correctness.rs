//! CCH Correctness Validation (Parallel)
//!
//! Compares bidirectional CCH query vs CCH-Dijkstra (exact baseline)
//! on random node pairs. Zero tolerance for cost mismatches.
//! Uses Rayon for massive parallelization.

use anyhow::Result;
use priority_queue::PriorityQueue;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use rayon::prelude::*;
use std::cmp::Reverse;
use std::path::Path;
use std::sync::atomic::{AtomicUsize, Ordering};

use crate::formats::{CchTopoFile, CchWeightsFile, OrderEbgFile};
use crate::profile_abi::Mode;

/// Validation result
#[derive(Debug)]
pub struct ValidationResult {
    pub total_pairs: usize,
    pub routable_pairs: usize,
    pub unreachable_pairs: usize,
    pub mismatches: usize,
    pub max_diff: i64,
    pub avg_bidi_us: f64,
    pub avg_baseline_us: f64,
}

/// Single failure record
#[derive(Debug, Clone)]
pub struct Failure {
    pub src: u32,
    pub dst: u32,
    pub bidi_cost: u32,
    pub baseline_cost: u32,
}

/// Query pair generated upfront
#[derive(Clone, Copy)]
struct QueryPair {
    src: u32,
    dst: u32,
}

/// Per-query result
struct QueryResult {
    routable: bool,
    unreachable: bool,
    mismatch: bool,
    diff: i64,
    failure: Option<Failure>,
    bidi_ns: u64,
    baseline_ns: u64,
}

/// Reverse DOWN adjacency: for each node, list of (source, weight) pairs
struct DownReverse {
    offsets: Vec<u64>,
    sources: Vec<u32>,
    weights: Vec<u32>,
}

// Make DownReverse safe to share across threads (it's read-only after construction)
unsafe impl Sync for DownReverse {}

/// Run CCH correctness validation with parallel processing
pub fn validate_cch_correctness(
    topo_path: &Path,
    weights_path: &Path,
    order_path: &Path,
    n_pairs: usize,
    seed: u64,
    mode: Mode,
) -> Result<(ValidationResult, Vec<Failure>)> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    let n_threads = rayon::current_num_threads();
    println!("\n🔬 CCH Correctness Validation ({} mode) - {} threads", mode_name, n_threads);
    println!("   Pairs: {}", n_pairs);
    println!("   Seed: {}", seed);

    // Load data
    println!("\nLoading CCH topology...");
    let topo = CchTopoFile::read(topo_path)?;
    println!("  ✓ {} nodes, {} up edges, {} down edges",
             topo.n_nodes, topo.up_targets.len(), topo.down_targets.len());

    println!("Loading CCH weights...");
    let weights = CchWeightsFile::read(weights_path)?;
    println!("  ✓ {} up weights, {} down weights", weights.up.len(), weights.down.len());

    println!("Loading ordering...");
    let order = OrderEbgFile::read(order_path)?;
    println!("  ✓ {} nodes", order.n_nodes);

    let n = topo.n_nodes as usize;

    // Build reverse DOWN adjacency once (for backward search)
    println!("Building reverse DOWN adjacency...");
    let down_rev = build_down_reverse(&topo, &weights);
    println!("  ✓ Built");

    // Find routable nodes (have at least one finite outgoing edge)
    println!("Finding routable nodes...");
    let routable_nodes = find_routable_nodes(&topo, &weights);
    println!("  ✓ {} routable nodes out of {}", routable_nodes.len(), n);

    if routable_nodes.is_empty() {
        anyhow::bail!("No routable nodes found!");
    }

    // Pre-generate all query pairs
    println!("Generating {} query pairs...", n_pairs);
    let pairs = generate_query_pairs(&topo, &weights, &routable_nodes, n_pairs, seed);
    println!("  ✓ Generated");

    // Progress tracking
    let completed = AtomicUsize::new(0);
    let start_time = std::time::Instant::now();

    println!("\nRunning {} queries in parallel...", n_pairs);

    // Process queries in parallel using chunks
    let chunk_size = 1000.max(n_pairs / 100);
    let results: Vec<QueryResult> = pairs
        .par_chunks(chunk_size)
        .flat_map(|chunk| {
            // Each thread gets its own buffers
            let mut dist_fwd = vec![u32::MAX; n];
            let mut dist_bwd = vec![u32::MAX; n];
            let mut dist_dij = vec![u32::MAX; n];
            let mut gen_fwd = vec![0u32; n];
            let mut gen_bwd = vec![0u32; n];
            let mut gen_dij = vec![0u32; n];
            let mut current_gen = 1u32;

            let mut pq_fwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::with_capacity(10000);
            let mut pq_bwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::with_capacity(10000);
            let mut pq_dij: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::with_capacity(10000);

            let chunk_results: Vec<QueryResult> = chunk.iter().map(|pair| {
                current_gen += 1;

                // Run bidirectional CCH
                let t0 = std::time::Instant::now();
                let bidi_cost = bidi_cch_query(
                    &topo, &weights, &down_rev,
                    pair.src, pair.dst,
                    &mut dist_fwd, &mut dist_bwd,
                    &mut gen_fwd, &mut gen_bwd,
                    current_gen,
                    &mut pq_fwd, &mut pq_bwd,
                );
                let bidi_ns = t0.elapsed().as_nanos() as u64;

                current_gen += 1;

                // Run CCH-Dijkstra (baseline)
                let t1 = std::time::Instant::now();
                let baseline_cost = cch_dijkstra_query(
                    &topo, &weights,
                    pair.src, pair.dst,
                    &mut dist_dij,
                    &mut gen_dij,
                    current_gen,
                    &mut pq_dij,
                );
                let baseline_ns = t1.elapsed().as_nanos() as u64;

                // Compare
                match (bidi_cost, baseline_cost) {
                    (Some(b), Some(d)) if b == d => {
                        QueryResult {
                            routable: true,
                            unreachable: false,
                            mismatch: false,
                            diff: 0,
                            failure: None,
                            bidi_ns,
                            baseline_ns,
                        }
                    }
                    (Some(b), Some(d)) => {
                        let diff = (b as i64) - (d as i64);
                        QueryResult {
                            routable: true,
                            unreachable: false,
                            mismatch: true,
                            diff,
                            failure: Some(Failure {
                                src: pair.src,
                                dst: pair.dst,
                                bidi_cost: b,
                                baseline_cost: d,
                            }),
                            bidi_ns,
                            baseline_ns,
                        }
                    }
                    (None, None) => {
                        QueryResult {
                            routable: false,
                            unreachable: true,
                            mismatch: false,
                            diff: 0,
                            failure: None,
                            bidi_ns,
                            baseline_ns,
                        }
                    }
                    (Some(b), None) => {
                        QueryResult {
                            routable: false,
                            unreachable: false,
                            mismatch: true,
                            diff: 0,
                            failure: Some(Failure {
                                src: pair.src,
                                dst: pair.dst,
                                bidi_cost: b,
                                baseline_cost: u32::MAX,
                            }),
                            bidi_ns,
                            baseline_ns,
                        }
                    }
                    (None, Some(d)) => {
                        QueryResult {
                            routable: false,
                            unreachable: false,
                            mismatch: true,
                            diff: 0,
                            failure: Some(Failure {
                                src: pair.src,
                                dst: pair.dst,
                                bidi_cost: u32::MAX,
                                baseline_cost: d,
                            }),
                            bidi_ns,
                            baseline_ns,
                        }
                    }
                }
            }).collect();

            // Update progress
            let done = completed.fetch_add(chunk.len(), Ordering::Relaxed) + chunk.len();
            if done % (n_pairs / 10).max(1) < chunk_size {
                let pct = (done as f64 / n_pairs as f64) * 100.0;
                let elapsed = start_time.elapsed().as_secs_f64();
                let qps = done as f64 / elapsed;
                eprintln!("  {:5.1}% ({}/{}) - {:.0} q/s", pct, done, n_pairs, qps);
            }

            chunk_results
        })
        .collect();

    // Aggregate results
    let mut routable_pairs = 0usize;
    let mut unreachable_pairs = 0usize;
    let mut mismatches = 0usize;
    let mut max_diff: i64 = 0;
    let mut failures: Vec<Failure> = Vec::new();
    let mut total_bidi_ns = 0u64;
    let mut total_baseline_ns = 0u64;

    for r in &results {
        if r.routable {
            routable_pairs += 1;
        }
        if r.unreachable {
            unreachable_pairs += 1;
        }
        if r.mismatch {
            mismatches += 1;
            max_diff = max_diff.max(r.diff.abs());
            if let Some(f) = &r.failure {
                if failures.len() < 1000 {
                    failures.push(f.clone());
                }
            }
        }
        total_bidi_ns += r.bidi_ns;
        total_baseline_ns += r.baseline_ns;
    }

    let elapsed = start_time.elapsed();
    let avg_bidi_us = (total_bidi_ns as f64 / n_pairs as f64) / 1000.0;
    let avg_baseline_us = (total_baseline_ns as f64 / n_pairs as f64) / 1000.0;

    println!("\n=== VALIDATION COMPLETE ===");
    println!("  Total time: {:.2}s ({:.0} q/s)", elapsed.as_secs_f64(), n_pairs as f64 / elapsed.as_secs_f64());
    println!("  Total pairs: {}", n_pairs);
    println!("  Routable:    {}", routable_pairs);
    println!("  Unreachable: {}", unreachable_pairs);
    println!("  MISMATCHES:  {}", mismatches);
    if mismatches > 0 {
        println!("  Max diff:    {}", max_diff);
        println!("\n  First 5 failures:");
        for f in failures.iter().take(5) {
            let diff = (f.bidi_cost as i64) - (f.baseline_cost as i64);
            println!("    src={} dst={} bidi={} baseline={} diff={}",
                     f.src, f.dst, f.bidi_cost, f.baseline_cost, diff);
        }
    }
    println!("  Avg bidi:    {:.1} µs", avg_bidi_us);
    println!("  Avg baseline:{:.1} µs", avg_baseline_us);

    if mismatches == 0 {
        println!("\n✅ VALIDATION PASSED - 0 mismatches");
    } else {
        println!("\n❌ VALIDATION FAILED - {} mismatches", mismatches);
    }

    Ok((ValidationResult {
        total_pairs: n_pairs,
        routable_pairs,
        unreachable_pairs,
        mismatches,
        max_diff,
        avg_bidi_us,
        avg_baseline_us,
    }, failures))
}

fn generate_query_pairs(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    routable_nodes: &[u32],
    n_pairs: usize,
    seed: u64,
) -> Vec<QueryPair> {
    let mut rng = StdRng::seed_from_u64(seed);
    let mut pairs = Vec::with_capacity(n_pairs);

    for _ in 0..n_pairs {
        let (src, dst) = if rng.gen_bool(0.5) {
            // Close pair: random walk from src
            let src = routable_nodes[rng.gen_range(0..routable_nodes.len())];
            let dst = random_walk_neighbor(topo, weights, src, rng.gen_range(1..10), &mut rng);
            (src, dst)
        } else {
            // Long range: uniform random
            let src = routable_nodes[rng.gen_range(0..routable_nodes.len())];
            let dst = routable_nodes[rng.gen_range(0..routable_nodes.len())];
            (src, dst)
        };
        pairs.push(QueryPair { src, dst });
    }

    pairs
}

fn build_down_reverse(topo: &crate::formats::CchTopo, weights: &crate::formats::CchWeights) -> DownReverse {
    let n = topo.n_nodes as usize;

    // Count incoming DOWN edges per node
    let mut counts = vec![0u32; n];
    for u in 0..n {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let v = topo.down_targets[i] as usize;
            counts[v] += 1;
        }
    }

    // Build offsets
    let mut offsets = Vec::with_capacity(n + 1);
    let mut offset = 0u64;
    for &count in &counts {
        offsets.push(offset);
        offset += count as u64;
    }
    offsets.push(offset);

    // Allocate and fill
    let total = offset as usize;
    let mut sources = vec![0u32; total];
    let mut rev_weights = vec![0u32; total];
    let mut pos = vec![0u32; n];

    for u in 0..n {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let v = topo.down_targets[i] as usize;
            let w = weights.down[i];
            let idx = offsets[v] as usize + pos[v] as usize;
            sources[idx] = u as u32;
            rev_weights[idx] = w;
            pos[v] += 1;
        }
    }

    DownReverse { offsets, sources, weights: rev_weights }
}

fn find_routable_nodes(topo: &crate::formats::CchTopo, weights: &crate::formats::CchWeights) -> Vec<u32> {
    let n = topo.n_nodes as usize;
    let mut routable = Vec::with_capacity(n / 2);

    for u in 0..n {
        // Check if has at least one finite outgoing edge (UP or DOWN)
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        let has_up = (up_start..up_end).any(|i| weights.up[i] != u32::MAX);

        let down_start = topo.down_offsets[u] as usize;
        let down_end = topo.down_offsets[u + 1] as usize;
        let has_down = (down_start..down_end).any(|i| weights.down[i] != u32::MAX);

        if has_up || has_down {
            routable.push(u as u32);
        }
    }

    routable
}

fn random_walk_neighbor(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    start: u32,
    steps: usize,
    rng: &mut StdRng,
) -> u32 {
    let mut current = start;

    for _ in 0..steps {
        // Collect all finite neighbors
        let mut neighbors = Vec::new();

        let up_start = topo.up_offsets[current as usize] as usize;
        let up_end = topo.up_offsets[current as usize + 1] as usize;
        for i in up_start..up_end {
            if weights.up[i] != u32::MAX {
                neighbors.push(topo.up_targets[i]);
            }
        }

        let down_start = topo.down_offsets[current as usize] as usize;
        let down_end = topo.down_offsets[current as usize + 1] as usize;
        for i in down_start..down_end {
            if weights.down[i] != u32::MAX {
                neighbors.push(topo.down_targets[i]);
            }
        }

        if neighbors.is_empty() {
            break;
        }

        current = neighbors[rng.gen_range(0..neighbors.len())];
    }

    current
}

/// Bidirectional CCH query with generation-based clearing
fn bidi_cch_query(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    down_rev: &DownReverse,
    src: u32,
    dst: u32,
    dist_fwd: &mut [u32],
    dist_bwd: &mut [u32],
    gen_fwd: &mut [u32],
    gen_bwd: &mut [u32],
    gen: u32,
    pq_fwd: &mut PriorityQueue<u32, Reverse<u32>>,
    pq_bwd: &mut PriorityQueue<u32, Reverse<u32>>,
) -> Option<u32> {
    pq_fwd.clear();
    pq_bwd.clear();

    // Initialize
    dist_fwd[src as usize] = 0;
    gen_fwd[src as usize] = gen;
    dist_bwd[dst as usize] = 0;
    gen_bwd[dst as usize] = gen;

    pq_fwd.push(src, Reverse(0));
    pq_bwd.push(dst, Reverse(0));

    let mut best = u32::MAX;

    while !pq_fwd.is_empty() || !pq_bwd.is_empty() {
        // Forward step - traverse UP edges
        if let Some((u, Reverse(d))) = pq_fwd.pop() {
            let u_idx = u as usize;
            if gen_fwd[u_idx] != gen || d > dist_fwd[u_idx] {
                // Skip stale entries
            } else {
                // Check meeting
                if gen_bwd[u_idx] == gen {
                    best = best.min(d.saturating_add(dist_bwd[u_idx]));
                }

                // Relax UP edges
                let start = topo.up_offsets[u_idx] as usize;
                let end = topo.up_offsets[u_idx + 1] as usize;
                for i in start..end {
                    let v = topo.up_targets[i];
                    let w = weights.up[i];
                    if w == u32::MAX { continue; }

                    let v_idx = v as usize;
                    let nd = d.saturating_add(w);
                    let old = if gen_fwd[v_idx] == gen { dist_fwd[v_idx] } else { u32::MAX };

                    if nd < old {
                        dist_fwd[v_idx] = nd;
                        gen_fwd[v_idx] = gen;
                        pq_fwd.push(v, Reverse(nd));

                        // Check meeting
                        if gen_bwd[v_idx] == gen {
                            best = best.min(nd.saturating_add(dist_bwd[v_idx]));
                        }
                    }
                }
            }
        }

        // Backward step - traverse reversed DOWN edges
        if let Some((u, Reverse(d))) = pq_bwd.pop() {
            let u_idx = u as usize;
            if gen_bwd[u_idx] != gen || d > dist_bwd[u_idx] {
                // Skip stale entries
            } else {
                // Check meeting
                if gen_fwd[u_idx] == gen {
                    best = best.min(dist_fwd[u_idx].saturating_add(d));
                }

                // Relax reversed DOWN edges
                let start = down_rev.offsets[u_idx] as usize;
                let end = down_rev.offsets[u_idx + 1] as usize;
                for i in start..end {
                    let x = down_rev.sources[i];
                    let w = down_rev.weights[i];
                    if w == u32::MAX { continue; }

                    let x_idx = x as usize;
                    let nd = d.saturating_add(w);
                    let old = if gen_bwd[x_idx] == gen { dist_bwd[x_idx] } else { u32::MAX };

                    if nd < old {
                        dist_bwd[x_idx] = nd;
                        gen_bwd[x_idx] = gen;
                        pq_bwd.push(x, Reverse(nd));

                        // Check meeting
                        if gen_fwd[x_idx] == gen {
                            best = best.min(dist_fwd[x_idx].saturating_add(nd));
                        }
                    }
                }
            }
        }
    }

    if best == u32::MAX { None } else { Some(best) }
}

/// CCH-Dijkstra (baseline) - explores both UP and DOWN in any order
fn cch_dijkstra_query(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    src: u32,
    dst: u32,
    dist: &mut [u32],
    gen: &mut [u32],
    current_gen: u32,
    pq: &mut PriorityQueue<u32, Reverse<u32>>,
) -> Option<u32> {
    pq.clear();

    dist[src as usize] = 0;
    gen[src as usize] = current_gen;
    pq.push(src, Reverse(0));

    while let Some((u, Reverse(d))) = pq.pop() {
        let u_idx = u as usize;
        if gen[u_idx] != current_gen || d > dist[u_idx] {
            continue;
        }

        if u == dst {
            return Some(d);
        }

        // Relax UP edges
        let up_start = topo.up_offsets[u_idx] as usize;
        let up_end = topo.up_offsets[u_idx + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i];
            let w = weights.up[i];
            if w == u32::MAX { continue; }

            let v_idx = v as usize;
            let nd = d.saturating_add(w);
            let old = if gen[v_idx] == current_gen { dist[v_idx] } else { u32::MAX };

            if nd < old {
                dist[v_idx] = nd;
                gen[v_idx] = current_gen;
                pq.push(v, Reverse(nd));
            }
        }

        // Relax DOWN edges
        let down_start = topo.down_offsets[u_idx] as usize;
        let down_end = topo.down_offsets[u_idx + 1] as usize;
        for i in down_start..down_end {
            let v = topo.down_targets[i];
            let w = weights.down[i];
            if w == u32::MAX { continue; }

            let v_idx = v as usize;
            let nd = d.saturating_add(w);
            let old = if gen[v_idx] == current_gen { dist[v_idx] } else { u32::MAX };

            if nd < old {
                dist[v_idx] = nd;
                gen[v_idx] = current_gen;
                pq.push(v, Reverse(nd));
            }
        }
    }

    let dst_idx = dst as usize;
    if gen[dst_idx] == current_gen && dist[dst_idx] != u32::MAX {
        Some(dist[dst_idx])
    } else {
        None
    }
}

// ============================================================================
// REGRESSION TEST SUITE
// ============================================================================

/// Regression test case with expected behavior
#[derive(Debug, Clone)]
pub struct RegressionCase {
    pub name: &'static str,
    pub src: u32,
    pub dst: u32,
    pub expect_reachable: bool,
}

/// Result of a single regression test
#[derive(Debug)]
pub struct RegressionResult {
    pub name: &'static str,
    pub passed: bool,
    pub bidi_cost: Option<u32>,
    pub baseline_cost: Option<u32>,
    pub error: Option<String>,
}

/// Run targeted regression tests for edge cases
///
/// Categories tested:
/// 1. Same node (trivial: cost = 0)
/// 2. Adjacent edges (single hop)
/// 3. High-degree nodes (roundabouts, intersections)
/// 4. Long chains (motorway segments)
/// 5. Disconnected components (expect no route)
/// 6. Self-loops and U-turns
pub fn run_regression_tests(
    topo_path: &Path,
    weights_path: &Path,
    order_path: &Path,
    mode: Mode,
) -> Result<Vec<RegressionResult>> {
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n🧪 CCH Regression Tests ({} mode)", mode_name);

    // Load data
    let topo = CchTopoFile::read(topo_path)?;
    let weights = CchWeightsFile::read(weights_path)?;
    let _order = OrderEbgFile::read(order_path)?;

    let n = topo.n_nodes as usize;
    println!("  Loaded: {} nodes, {} up + {} down edges",
             n, topo.up_targets.len(), topo.down_targets.len());

    // Build reverse DOWN adjacency
    let down_rev = build_down_reverse(&topo, &weights);

    // Generate test cases dynamically from the graph structure
    let cases = generate_regression_cases(&topo, &weights, n);
    println!("  Generated {} regression cases", cases.len());

    // Allocate buffers
    let mut dist_fwd = vec![u32::MAX; n];
    let mut dist_bwd = vec![u32::MAX; n];
    let mut dist_dij = vec![u32::MAX; n];
    let mut gen_fwd = vec![0u32; n];
    let mut gen_bwd = vec![0u32; n];
    let mut gen_dij = vec![0u32; n];
    let mut current_gen = 1u32;

    let mut pq_fwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::with_capacity(10000);
    let mut pq_bwd: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::with_capacity(10000);
    let mut pq_dij: PriorityQueue<u32, Reverse<u32>> = PriorityQueue::with_capacity(10000);

    let mut results = Vec::with_capacity(cases.len());
    let mut passed = 0;
    let mut failed = 0;

    for case in &cases {
        current_gen += 1;

        // Run bidirectional CCH
        let bidi_cost = bidi_cch_query(
            &topo, &weights, &down_rev,
            case.src, case.dst,
            &mut dist_fwd, &mut dist_bwd,
            &mut gen_fwd, &mut gen_bwd,
            current_gen,
            &mut pq_fwd, &mut pq_bwd,
        );

        current_gen += 1;

        // Run baseline
        let baseline_cost = cch_dijkstra_query(
            &topo, &weights,
            case.src, case.dst,
            &mut dist_dij,
            &mut gen_dij,
            current_gen,
            &mut pq_dij,
        );

        // Check correctness
        let (test_passed, error) = match (bidi_cost, baseline_cost, case.expect_reachable) {
            // Both agree on reachability and cost
            (Some(b), Some(d), true) if b == d => (true, None),
            (None, None, false) => (true, None),

            // Cost mismatch
            (Some(b), Some(d), _) if b != d => {
                (false, Some(format!("Cost mismatch: bidi={} baseline={} diff={}", b, d, b as i64 - d as i64)))
            }

            // Reachability mismatch
            (Some(b), None, _) => {
                (false, Some(format!("Bidi found route (cost={}) but baseline says unreachable", b)))
            }
            (None, Some(d), _) => {
                (false, Some(format!("Bidi says unreachable but baseline found route (cost={})", d)))
            }

            // Expected reachable but both say unreachable
            (None, None, true) => {
                (false, Some("Expected reachable but both say unreachable".to_string()))
            }

            // Expected unreachable but both found route
            (Some(b), Some(d), false) if b == d => {
                (false, Some(format!("Expected unreachable but found route (cost={})", b)))
            }

            _ => (true, None), // Catch-all for edge cases
        };

        if test_passed {
            passed += 1;
        } else {
            failed += 1;
            println!("    ❌ {}: {:?}", case.name, error);
        }

        results.push(RegressionResult {
            name: case.name,
            passed: test_passed,
            bidi_cost,
            baseline_cost,
            error,
        });
    }

    println!("\n  Results: {} passed, {} failed", passed, failed);

    if failed == 0 {
        println!("  ✅ All regression tests passed!");
    } else {
        println!("  ❌ {} regression tests failed", failed);
    }

    Ok(results)
}

/// Generate regression test cases from graph structure
fn generate_regression_cases(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    n: usize,
) -> Vec<RegressionCase> {
    let mut cases = Vec::new();

    // 1. Same node (trivial case)
    for &node in &[0u32, (n / 2) as u32, (n - 1) as u32] {
        if (node as usize) < n {
            cases.push(RegressionCase {
                name: "same_node",
                src: node,
                dst: node,
                expect_reachable: true,
            });
        }
    }

    // 2. Adjacent edges (single hop via UP edges)
    let mut adjacent_count = 0;
    for u in 0..n.min(10000) {
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        for i in up_start..up_end {
            if weights.up[i] != u32::MAX && adjacent_count < 10 {
                cases.push(RegressionCase {
                    name: "adjacent_up",
                    src: u as u32,
                    dst: topo.up_targets[i],
                    expect_reachable: true,
                });
                adjacent_count += 1;
            }
        }
        if adjacent_count >= 10 { break; }
    }

    // 3. Adjacent edges (single hop via DOWN edges)
    let mut down_count = 0;
    for u in 0..n.min(10000) {
        let down_start = topo.down_offsets[u] as usize;
        let down_end = topo.down_offsets[u + 1] as usize;
        for i in down_start..down_end {
            if weights.down[i] != u32::MAX && down_count < 10 {
                cases.push(RegressionCase {
                    name: "adjacent_down",
                    src: u as u32,
                    dst: topo.down_targets[i],
                    expect_reachable: true,
                });
                down_count += 1;
            }
        }
        if down_count >= 10 { break; }
    }

    // 4. High-degree nodes (potential roundabouts/intersections)
    // Find nodes with many edges
    let mut high_degree_nodes = Vec::new();
    for u in 0..n {
        let up_deg = (topo.up_offsets[u + 1] - topo.up_offsets[u]) as usize;
        let down_deg = (topo.down_offsets[u + 1] - topo.down_offsets[u]) as usize;
        let total_deg = up_deg + down_deg;
        if total_deg >= 10 {
            high_degree_nodes.push((u as u32, total_deg));
        }
    }
    high_degree_nodes.sort_by(|a, b| b.1.cmp(&a.1));

    // Test routes between high-degree nodes
    for i in 0..high_degree_nodes.len().min(5) {
        for j in (i+1)..high_degree_nodes.len().min(10) {
            cases.push(RegressionCase {
                name: "high_degree_pair",
                src: high_degree_nodes[i].0,
                dst: high_degree_nodes[j].0,
                expect_reachable: true, // Might not always be true
            });
        }
    }

    // 5. Long chain test (follow UP edges multiple hops)
    if let Some(start) = find_chain_start(topo, weights, n) {
        let chain_end = follow_chain(topo, weights, start, 20);
        if chain_end != start {
            cases.push(RegressionCase {
                name: "long_chain",
                src: start,
                dst: chain_end,
                expect_reachable: true,
            });
        }
    }

    // 6. Isolated nodes (disconnected - expect no route)
    for u in 0..n.min(100000) {
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        let down_start = topo.down_offsets[u] as usize;
        let down_end = topo.down_offsets[u + 1] as usize;

        // Check if node has no finite edges
        let has_finite_up = (up_start..up_end).any(|i| weights.up[i] != u32::MAX);
        let has_finite_down = (down_start..down_end).any(|i| weights.down[i] != u32::MAX);

        if !has_finite_up && !has_finite_down {
            // This node is isolated - route to another node should fail
            let other = if u == 0 { 1 } else { 0 };
            cases.push(RegressionCase {
                name: "isolated_node",
                src: u as u32,
                dst: other as u32,
                expect_reachable: false,
            });
            break; // Just need one
        }
    }

    // 7. Reverse direction test
    // For each adjacent pair, also test the reverse
    let mut reverse_cases = Vec::new();
    for case in cases.iter().take(20) {
        if case.src != case.dst {
            reverse_cases.push(RegressionCase {
                name: "reverse_direction",
                src: case.dst,
                dst: case.src,
                expect_reachable: true, // May not always be true for directed graphs
            });
        }
    }
    cases.extend(reverse_cases);

    // 8. Two-hop paths (test triangle inequality)
    let mut two_hop_count = 0;
    'outer: for u in 0..n.min(1000) {
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;

        for i in up_start..up_end {
            if weights.up[i] == u32::MAX { continue; }
            let v = topo.up_targets[i] as usize;

            let v_up_start = topo.up_offsets[v] as usize;
            let v_up_end = topo.up_offsets[v + 1] as usize;

            for j in v_up_start..v_up_end {
                if weights.up[j] != u32::MAX && two_hop_count < 10 {
                    cases.push(RegressionCase {
                        name: "two_hop",
                        src: u as u32,
                        dst: topo.up_targets[j],
                        expect_reachable: true,
                    });
                    two_hop_count += 1;
                    if two_hop_count >= 10 { break 'outer; }
                }
            }
        }
    }

    cases
}

/// Find a node that starts a chain (has exactly one outgoing finite edge)
fn find_chain_start(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    n: usize,
) -> Option<u32> {
    for u in 0..n.min(100000) {
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;

        let finite_count = (up_start..up_end)
            .filter(|&i| weights.up[i] != u32::MAX)
            .count();

        if finite_count == 1 {
            return Some(u as u32);
        }
    }
    None
}

/// Follow UP edges for a number of hops
fn follow_chain(
    topo: &crate::formats::CchTopo,
    weights: &crate::formats::CchWeights,
    start: u32,
    max_hops: usize,
) -> u32 {
    let mut current = start;

    for _ in 0..max_hops {
        let u = current as usize;
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;

        // Find first finite neighbor
        let mut next = None;
        for i in up_start..up_end {
            if weights.up[i] != u32::MAX {
                next = Some(topo.up_targets[i]);
                break;
            }
        }

        match next {
            Some(n) => current = n,
            None => break,
        }
    }

    current
}
