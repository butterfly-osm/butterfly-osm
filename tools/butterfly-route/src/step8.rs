//! Step 8: CCH Customization
//!
//! Applies per-mode weights to the CCH shortcuts using bottom-up customization
//! + parallel triangle relaxation.
//!
//! # Algorithm Overview
//!
//! CCH customization processes nodes in contraction order (lowest rank first).
//! For each edge in the up/down graphs:
//!
//! - **Original edges**: weight = edge_weight[target] + turn_penalty[arc]
//! - **Shortcuts u→w via m**: weight = weight(u→m) + weight(m→w)
//!
//! # Dependency Order (CRITICAL for bottom-up)
//!
//! For each node u processed at rank r:
//! 1. **DOWN edges must be processed FIRST**, in order of INCREASING target rank
//!    - Down shortcut u→v via m requires down_weights[u→m]
//!    - Since rank(m) < rank(v), processing by increasing rank ensures u→m before u→v
//! 2. **UP edges processed SECOND** (order doesn't matter within UP)
//!    - Up shortcut u→v via m requires down_weights[u→m] and up_weights[m→v]
//!    - down_weights[u→m] computed in phase 1
//!    - up_weights[m→v] computed when node m was processed (rank(m) < rank(u))
//!
//! # Triangle Relaxation (parallel)
//!
//! After bottom-up, triangle relaxation discovers cheaper paths through alternative
//! contracted nodes. Uses `AtomicU32::fetch_min` for lock-free parallel processing:
//! - Relaxation only *decreases* weights (monotone)
//! - Stale reads (Relaxed ordering) are safe: missed updates caught by next pass
//! - Convergence check (0 updates) guarantees correctness

use anyhow::Result;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::formats::{mod_turns, mod_weights, CchTopo, CchTopoFile, EbgNodesFile, FilteredEbgFile, HybridStateFile, OrderEbgFile};
use crate::profile_abi::Mode;

/// Configuration for Step 8
pub struct Step8Config {
    pub cch_topo_path: PathBuf,
    pub filtered_ebg_path: PathBuf,
    pub weights_path: PathBuf, // w.*.u32
    pub turns_path: PathBuf,   // t.*.u32
    pub order_path: PathBuf,
    pub ebg_nodes_path: PathBuf, // ebg.nodes from step4
    pub mode: Mode,
    pub outdir: PathBuf,
}

/// Result of Step 8 customization
#[derive(Debug)]
pub struct Step8Result {
    pub output_path: PathBuf,
    pub distance_output_path: PathBuf,
    pub mode: Mode,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub customize_time_ms: u64,
}

/// Sorted filtered EBG adjacency for fast arc index lookup
/// Uses filtered node IDs but stores original arc indices for turn penalty lookup
struct SortedFilteredEbgAdj {
    offsets: Vec<u64>,
    sorted_heads: Vec<u32>,       // Filtered node IDs (targets)
    sorted_orig_arc_idx: Vec<u32>, // Original arc indices for turn penalty lookup
}

impl SortedFilteredEbgAdj {
    /// Build sorted adjacency from FilteredEbg
    fn build(filtered_ebg: &crate::formats::FilteredEbg) -> Self {
        let n_nodes = filtered_ebg.n_filtered_nodes as usize;
        let n_arcs = filtered_ebg.n_filtered_arcs as usize;

        let sorted_per_node: Vec<Vec<(u32, u32)>> = (0..n_nodes)
            .into_par_iter()
            .map(|u| {
                let start = filtered_ebg.offsets[u] as usize;
                let end = filtered_ebg.offsets[u + 1] as usize;
                let mut edges: Vec<(u32, u32)> = (start..end)
                    .map(|i| (filtered_ebg.heads[i], filtered_ebg.original_arc_idx[i]))
                    .collect();
                edges.sort_unstable_by_key(|(head, _)| *head);
                edges
            })
            .collect();

        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut sorted_heads = Vec::with_capacity(n_arcs);
        let mut sorted_orig_arc_idx = Vec::with_capacity(n_arcs);

        let mut offset = 0u64;
        for edges in sorted_per_node {
            offsets.push(offset);
            for (head, orig_arc_idx) in edges {
                sorted_heads.push(head);
                sorted_orig_arc_idx.push(orig_arc_idx);
            }
            offset = sorted_heads.len() as u64;
        }
        offsets.push(offset);

        Self { offsets, sorted_heads, sorted_orig_arc_idx }
    }

    #[inline]
    fn find_original_arc_index(&self, u: usize, v: u32) -> Option<u32> {
        let start = self.offsets[u] as usize;
        let end = self.offsets[u + 1] as usize;
        if start >= end { return None; }
        match self.sorted_heads[start..end].binary_search(&v) {
            Ok(idx) => Some(self.sorted_orig_arc_idx[start + idx]),
            Err(_) => None,
        }
    }
}

// ===================================================================
// Main customization entry point
// ===================================================================

/// Customize CCH for a specific mode (time + distance weights, parallelized)
pub fn customize_cch(config: Step8Config) -> Result<Step8Result> {
    let start_time = std::time::Instant::now();
    let mode_name = match config.mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };
    println!("\n🎨 Step 8: Customizing CCH for {}...\n", mode_name);

    // Load all data
    println!("Loading CCH topology...");
    let topo = CchTopoFile::read(&config.cch_topo_path)?;
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();
    println!("  ✓ {} nodes, {} up edges, {} down edges", n_nodes, n_up, n_down);

    println!("Loading filtered EBG...");
    let filtered_ebg = FilteredEbgFile::read(&config.filtered_ebg_path)?;
    println!("  ✓ {} filtered nodes, {} arcs", filtered_ebg.n_filtered_nodes, filtered_ebg.n_filtered_arcs);

    println!("Loading weights ({})...", mode_name);
    let weights = mod_weights::read_all(&config.weights_path)?;
    println!("  ✓ {} node weights", weights.weights.len());

    println!("Loading turn penalties ({})...", mode_name);
    let turns = mod_turns::read_all(&config.turns_path)?;
    println!("  ✓ {} arc penalties", turns.penalties.len());

    println!("Loading EBG nodes...");
    let ebg_nodes = EbgNodesFile::read(&config.ebg_nodes_path)?;
    println!("  ✓ {} EBG nodes", ebg_nodes.n_nodes);

    // Build shared structures
    println!("\nBuilding sorted filtered EBG adjacency (parallel)...");
    let sorted_ebg = SortedFilteredEbgAdj::build(&filtered_ebg);
    println!("  ✓ Built sorted adjacency");

    let rank_to_filtered = &topo.rank_to_filtered;

    println!("Pre-sorting down edges by target rank (parallel)...");
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end { return Vec::new(); }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();
    println!("  ✓ Pre-sorted down edges");

    println!("Building reverse DOWN adjacency...");
    let rev_down = build_reverse_down_adj_for_relax(&topo);
    println!("  ✓ {} entries", rev_down.sources.len());

    // ===================================================================
    // Bottom-up customization: TIME and DISTANCE in parallel
    //
    // INVARIANT: Each bottom-up pass is internally sequential (rank order).
    // But the two metrics are independent → run concurrently via rayon::join.
    // ===================================================================
    println!("\n⚡ Bottom-up customization (time + distance in parallel)...");
    let bu_start = std::time::Instant::now();

    let ((time_up, time_down), (dist_up, dist_down)) = rayon::join(
        || {
            bottom_up_customize(&topo, &sorted_down_indices, |u_rank, v_rank| {
                compute_original_weight_rank_aligned(
                    u_rank, v_rank,
                    &weights.weights, &turns.penalties,
                    &sorted_ebg,
                    &filtered_ebg.filtered_to_original,
                    rank_to_filtered,
                )
            })
        },
        || {
            bottom_up_customize(&topo, &sorted_down_indices, |_u_rank, v_rank| {
                compute_distance_weight_rank_aligned(
                    v_rank,
                    &weights.weights,
                    &ebg_nodes.nodes,
                    &filtered_ebg.filtered_to_original,
                    rank_to_filtered,
                )
            })
        },
    );

    println!("  ✓ Both bottom-up passes in {:.2}s", bu_start.elapsed().as_secs_f64());

    // ===================================================================
    // Triangle relaxation (parallel internally via atomics)
    //
    // INVARIANT: relaxation only DECREASES weights (fetch_min).
    // Run sequentially since each already saturates all cores.
    // ===================================================================
    println!("\n🔺 Triangle relaxation for TIME (parallel)...");
    let tr_start = std::time::Instant::now();
    let (time_up, time_down, time_relax_count, time_relax_passes) =
        triangle_relax_parallel(&topo, time_up, time_down, &rev_down);
    println!("  ✓ {:.2}s, {} updates in {} passes",
        tr_start.elapsed().as_secs_f64(), time_relax_count, time_relax_passes);

    println!("\n🔺 Triangle relaxation for DISTANCE (parallel)...");
    let tr_start = std::time::Instant::now();
    let (dist_up, dist_down, dist_relax_count, dist_relax_passes) =
        triangle_relax_parallel(&topo, dist_up, dist_down, &rev_down);
    println!("  ✓ {:.2}s, {} updates in {} passes",
        tr_start.elapsed().as_secs_f64(), dist_relax_count, dist_relax_passes);

    // Sanity checks
    sanity_check_weights(&topo, &time_up, &time_down, "Time", 95.0)?;
    sanity_check_weights_simple(&dist_up, &dist_down, "Distance", 95.0)?;

    // Write outputs
    std::fs::create_dir_all(&config.outdir)?;

    let output_path = config.outdir.join(format!("cch.w.{}.u32", mode_name));
    println!("\nWriting time weights...");
    write_cch_weights(&output_path, &time_up, &time_down, config.mode)?;
    println!("  ✓ Written {}", output_path.display());

    let distance_output_path = config.outdir.join(format!("cch.d.{}.u32", mode_name));
    println!("Writing distance weights...");
    write_cch_weights(&distance_output_path, &dist_up, &dist_down, config.mode)?;
    println!("  ✓ Written {}", distance_output_path.display());

    let customize_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step8Result {
        output_path,
        distance_output_path,
        mode: config.mode,
        n_up_edges: n_up as u64,
        n_down_edges: n_down as u64,
        customize_time_ms,
    })
}

// ===================================================================
// Reusable customization building blocks
// ===================================================================

/// Reverse DOWN adjacency for triangle relaxation.
/// For each node m, stores all incoming DOWN edges x→m.
struct ReverseDownAdj {
    offsets: Vec<u64>,
    sources: Vec<u32>,
    edge_idx: Vec<usize>,
}

fn build_reverse_down_adj_for_relax(topo: &CchTopo) -> ReverseDownAdj {
    let n_nodes = topo.n_nodes as usize;

    let mut counts = vec![0u64; n_nodes];
    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            counts[topo.down_targets[i] as usize] += 1;
        }
    }

    let mut offsets = vec![0u64; n_nodes + 1];
    for m in 0..n_nodes {
        offsets[m + 1] = offsets[m] + counts[m];
    }

    let total = offsets[n_nodes] as usize;
    let mut sources = vec![0u32; total];
    let mut edge_idx = vec![0usize; total];
    let mut insert = vec![0u64; n_nodes];

    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let m = topo.down_targets[i] as usize;
            let pos = (offsets[m] + insert[m]) as usize;
            sources[pos] = u as u32;
            edge_idx[pos] = i;
            insert[m] += 1;
        }
    }

    ReverseDownAdj { offsets, sources, edge_idx }
}

/// Generic bottom-up CCH customization.
///
/// INVARIANT: Processes ranks in ascending order (sequential, NOT parallel).
/// For each rank u:
///   1. DOWN edges sorted by target rank (ensures u→m done before u→v when rank(m) < rank(v))
///   2. UP edges after DOWN (UP shortcuts need down_weights[u→m])
///
/// `orig_weight_fn(u_rank, v_rank) -> u32` provides original edge weight.
/// Shortcuts always use: weight(u→m) + weight(m→v) via stored middle node.
fn bottom_up_customize(
    topo: &CchTopo,
    sorted_down_indices: &[Vec<usize>],
    orig_weight_fn: impl Fn(usize, usize) -> u32,
) -> (Vec<u32>, Vec<u32>) {
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();

    let mut up_weights = vec![u32::MAX; n_up];
    let mut down_weights = vec![u32::MAX; n_down];

    for rank in 0..n_nodes {
        let u = rank;

        // PHASE 1: DOWN edges (sorted by target rank for correct dependency order)
        for &i in &sorted_down_indices[u] {
            let v = topo.down_targets[i] as usize;
            if !topo.down_is_shortcut[i] {
                down_weights[i] = orig_weight_fn(u, v);
            } else {
                let m = topo.down_middle[i] as usize;
                let w_um = find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                down_weights[i] = w_um.saturating_add(w_mv);
            }
        }

        // PHASE 2: UP edges (all down_weights[u→*] are now computed)
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i] as usize;
            if !topo.up_is_shortcut[i] {
                up_weights[i] = orig_weight_fn(u, v);
            } else {
                let m = topo.up_middle[i] as usize;
                let w_um = find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                up_weights[i] = w_um.saturating_add(w_mv);
            }
        }
    }

    (up_weights, down_weights)
}

/// Parallel triangle relaxation using atomic fetch_min.
///
/// For each apex m (processed in parallel), relaxes edges x→y where:
///   - x→m is a DOWN edge from x (rank[x] > rank[m])
///   - m→y is an UP edge from m (rank[y] > rank[m])
///   - w(x,y) = min(w(x,y), w(x,m) + w(m,y))
///
/// INVARIANT: Only decreases weights (monotone via fetch_min).
/// Relaxed ordering is safe: stale reads may miss an update in this pass,
/// but the convergence check (0 updates) ensures all triangles are optimal.
///
/// Returns (up_weights, down_weights, total_relaxations, passes).
fn triangle_relax_parallel(
    topo: &CchTopo,
    up_weights: Vec<u32>,
    down_weights: Vec<u32>,
    rev_down: &ReverseDownAdj,
) -> (Vec<u32>, Vec<u32>, u64, u32) {
    let n_nodes = topo.n_nodes as usize;

    // Convert to atomic arrays for lock-free parallel relaxation
    let atomic_up: Vec<AtomicU32> = up_weights.into_iter().map(AtomicU32::new).collect();
    let atomic_down: Vec<AtomicU32> = down_weights.into_iter().map(AtomicU32::new).collect();

    let mut total_relaxations = 0u64;
    let mut pass = 0u32;

    loop {
        pass += 1;
        let pass_updates = AtomicU64::new(0);

        // Process all apexes in parallel
        (0..n_nodes).into_par_iter().for_each(|m| {
            let rev_start = rev_down.offsets[m] as usize;
            let rev_end = rev_down.offsets[m + 1] as usize;

            for i_rev in rev_start..rev_end {
                let x = rev_down.sources[i_rev] as usize;
                let edge_idx_xm = rev_down.edge_idx[i_rev];
                let w_xm = atomic_down[edge_idx_xm].load(Ordering::Relaxed);

                if w_xm == u32::MAX { continue; }

                let up_start = topo.up_offsets[m] as usize;
                let up_end = topo.up_offsets[m + 1] as usize;

                for i_my in up_start..up_end {
                    let y = topo.up_targets[i_my] as usize;
                    if y == x { continue; }

                    let w_my = atomic_up[i_my].load(Ordering::Relaxed);
                    if w_my == u32::MAX { continue; }

                    let new_weight = w_xm.saturating_add(w_my);

                    if y > x {
                        // UP edge from x
                        if let Some(idx) = find_edge_index(x, y, &topo.up_offsets, &topo.up_targets) {
                            let old = atomic_up[idx].fetch_min(new_weight, Ordering::Relaxed);
                            if new_weight < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        // DOWN edge from x
                        if let Some(idx) = find_edge_index(x, y, &topo.down_offsets, &topo.down_targets) {
                            let old = atomic_down[idx].fetch_min(new_weight, Ordering::Relaxed);
                            if new_weight < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        });

        let pu = pass_updates.into_inner();
        println!("  Pass {}: {} updates", pass, pu);
        total_relaxations += pu;

        if pu == 0 { break; }
        if pass >= 100 {
            println!("  WARNING: Did not converge after 100 passes!");
            break;
        }
    }

    let up = atomic_up.into_iter().map(AtomicU32::into_inner).collect();
    let down = atomic_down.into_iter().map(AtomicU32::into_inner).collect();

    (up, down, total_relaxations, pass)
}

// ===================================================================
// Original edge weight functions
// ===================================================================

/// Compute weight for an original edge (deprecated - use rank-aligned version)
#[inline]
#[allow(dead_code)]
fn compute_original_weight(
    u: usize,
    v: usize,
    node_weights: &[u32],
    turn_penalties: &[u32],
    sorted_ebg: &SortedFilteredEbgAdj,
    filtered_to_original: &[u32],
) -> u32 {
    let original_v = filtered_to_original[v] as usize;
    let w_v = node_weights[original_v];
    if w_v == 0 { return u32::MAX; }

    match sorted_ebg.find_original_arc_index(u, v as u32) {
        Some(orig_arc_idx) => w_v.saturating_add(turn_penalties[orig_arc_idx as usize]),
        None => u32::MAX,
    }
}

/// Compute time weight for an original edge in rank-aligned CCH.
/// Converts rank → filtered_id → original_id for weight + turn penalty lookup.
#[inline]
fn compute_original_weight_rank_aligned(
    u_rank: usize,
    v_rank: usize,
    node_weights: &[u32],
    turn_penalties: &[u32],
    sorted_ebg: &SortedFilteredEbgAdj,
    filtered_to_original: &[u32],
    rank_to_filtered: &[u32],
) -> u32 {
    let u_filtered = rank_to_filtered[u_rank] as usize;
    let v_filtered = rank_to_filtered[v_rank] as usize;
    let original_v = filtered_to_original[v_filtered] as usize;
    let w_v = node_weights[original_v];

    if w_v == 0 { return u32::MAX; }

    match sorted_ebg.find_original_arc_index(u_filtered, v_filtered as u32) {
        Some(orig_arc_idx) => w_v.saturating_add(turn_penalties[orig_arc_idx as usize]),
        None => u32::MAX,
    }
}

/// Compute distance weight for an original edge in rank-aligned CCH.
/// Distance = length_mm (physical distance, mode-independent).
/// Accessibility uses same check as time: node_weights[v] == 0 → inaccessible.
/// No turn penalties for distance.
#[inline]
fn compute_distance_weight_rank_aligned(
    v_rank: usize,
    node_weights: &[u32], // Time weights, for accessibility check only
    ebg_nodes: &[crate::formats::ebg_nodes::EbgNode],
    filtered_to_original: &[u32],
    rank_to_filtered: &[u32],
) -> u32 {
    let v_filtered = rank_to_filtered[v_rank] as usize;
    let original_v = filtered_to_original[v_filtered] as usize;

    if node_weights[original_v] == 0 { return u32::MAX; }

    ebg_nodes[original_v].length_mm
}

// ===================================================================
// CCH CSR lookup helpers
// ===================================================================

#[inline]
fn find_edge_weight(
    u: usize,
    v: usize,
    offsets: &[u64],
    targets: &[u32],
    weights: &[u32],
) -> u32 {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    if start >= end { return u32::MAX; }
    match targets[start..end].binary_search(&(v as u32)) {
        Ok(idx) => weights[start + idx],
        Err(_) => u32::MAX,
    }
}

#[inline]
fn find_edge_index(
    u: usize,
    v: usize,
    offsets: &[u64],
    targets: &[u32],
) -> Option<usize> {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    if start >= end { return None; }
    match targets[start..end].binary_search(&(v as u32)) {
        Ok(idx) => Some(start + idx),
        Err(_) => None,
    }
}

// ===================================================================
// Sanity checks
// ===================================================================

fn sanity_check_weights(
    topo: &CchTopo,
    up_weights: &[u32],
    down_weights: &[u32],
    label: &str,
    fail_threshold: f64,
) -> Result<()> {
    let n_up = up_weights.len();
    let n_down = down_weights.len();

    let mut up_orig_max = 0usize;
    let mut up_short_max = 0usize;
    let mut up_orig_total = 0usize;
    let mut up_short_total = 0usize;
    let mut down_orig_max = 0usize;
    let mut down_short_max = 0usize;
    let mut down_orig_total = 0usize;
    let mut down_short_total = 0usize;

    for i in 0..n_up {
        if topo.up_is_shortcut[i] {
            up_short_total += 1;
            if up_weights[i] == u32::MAX { up_short_max += 1; }
        } else {
            up_orig_total += 1;
            if up_weights[i] == u32::MAX { up_orig_max += 1; }
        }
    }
    for i in 0..n_down {
        if topo.down_is_shortcut[i] {
            down_short_total += 1;
            if down_weights[i] == u32::MAX { down_short_max += 1; }
        } else {
            down_orig_total += 1;
            if down_weights[i] == u32::MAX { down_orig_max += 1; }
        }
    }

    let total_max = up_orig_max + up_short_max + down_orig_max + down_short_max;
    let total_edges = n_up + n_down;
    let max_pct = (total_max as f64 / total_edges as f64) * 100.0;

    println!("\n📊 {} sanity check:", label);
    println!("  Unreachable: {} / {} ({:.2}%)", total_max, total_edges, max_pct);
    println!("    Up original:  {} / {} ({:.2}%)", up_orig_max, up_orig_total,
             if up_orig_total > 0 { up_orig_max as f64 / up_orig_total as f64 * 100.0 } else { 0.0 });
    println!("    Up shortcuts: {} / {} ({:.2}%)", up_short_max, up_short_total,
             if up_short_total > 0 { up_short_max as f64 / up_short_total as f64 * 100.0 } else { 0.0 });
    println!("    Down original:  {} / {} ({:.2}%)", down_orig_max, down_orig_total,
             if down_orig_total > 0 { down_orig_max as f64 / down_orig_total as f64 * 100.0 } else { 0.0 });
    println!("    Down shortcuts: {} / {} ({:.2}%)", down_short_max, down_short_total,
             if down_short_total > 0 { down_short_max as f64 / down_short_total as f64 * 100.0 } else { 0.0 });

    if max_pct > fail_threshold {
        anyhow::bail!("CRITICAL: {}% of {} edges are unreachable!", max_pct, label);
    }
    Ok(())
}

fn sanity_check_weights_simple(
    up_weights: &[u32],
    down_weights: &[u32],
    label: &str,
    fail_threshold: f64,
) -> Result<()> {
    let max_count = up_weights.iter().filter(|&&w| w == u32::MAX).count()
        + down_weights.iter().filter(|&&w| w == u32::MAX).count();
    let total = up_weights.len() + down_weights.len();
    let pct = (max_count as f64 / total as f64) * 100.0;
    println!("\n📊 {} sanity check:", label);
    println!("  Unreachable: {} / {} ({:.2}%)", max_count, total, pct);
    if pct > fail_threshold {
        anyhow::bail!("CRITICAL: {}% of {} edges are unreachable!", pct, label);
    }
    Ok(())
}

// ===================================================================
// File I/O
// ===================================================================

fn write_cch_weights(
    path: &std::path::Path,
    up_weights: &[u32],
    down_weights: &[u32],
    mode: Mode,
) -> Result<()> {
    use crate::formats::crc::Digest;

    const MAGIC: u32 = 0x43434857; // "CCHW"
    const VERSION: u16 = 1;

    let mut writer = BufWriter::new(File::create(path)?);
    let mut crc_digest = Digest::new();

    // Header (32 bytes)
    let magic_bytes = MAGIC.to_le_bytes();
    let version_bytes = VERSION.to_le_bytes();
    let mode_byte = mode as u8;
    let reserved = 0u8;
    let n_up = (up_weights.len() as u64).to_le_bytes();
    let n_down = (down_weights.len() as u64).to_le_bytes();
    let padding = [0u8; 8];

    writer.write_all(&magic_bytes)?;
    writer.write_all(&version_bytes)?;
    writer.write_all(&[mode_byte, reserved])?;
    writer.write_all(&n_up)?;
    writer.write_all(&n_down)?;
    writer.write_all(&padding)?;

    crc_digest.update(&magic_bytes);
    crc_digest.update(&version_bytes);
    crc_digest.update(&[mode_byte, reserved]);
    crc_digest.update(&n_up);
    crc_digest.update(&n_down);
    crc_digest.update(&padding);

    for &w in up_weights {
        let bytes = w.to_le_bytes();
        writer.write_all(&bytes)?;
        crc_digest.update(&bytes);
    }

    for &w in down_weights {
        let bytes = w.to_le_bytes();
        writer.write_all(&bytes)?;
        crc_digest.update(&bytes);
    }

    let crc = crc_digest.finalize();
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(&crc.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

// ==========================================================================
// Hybrid State Graph CCH Customization
// ==========================================================================

/// Configuration for Step 8 with hybrid state graph
pub struct Step8HybridConfig {
    pub cch_topo_path: PathBuf,
    pub hybrid_state_path: PathBuf,
    pub mode: Mode,
    pub outdir: PathBuf,
}

/// Sorted hybrid state graph adjacency for fast arc index lookup
struct SortedHybridAdj {
    offsets: Vec<u64>,
    sorted_targets: Vec<u32>,
    sorted_weights: Vec<u32>,
}

impl SortedHybridAdj {
    fn build(hybrid: &crate::formats::HybridState) -> Self {
        let n_states = hybrid.n_states as usize;
        let n_arcs = hybrid.n_arcs as usize;

        let sorted_per_state: Vec<Vec<(u32, u32)>> = (0..n_states)
            .into_par_iter()
            .map(|u| {
                let start = hybrid.offsets[u] as usize;
                let end = hybrid.offsets[u + 1] as usize;
                let mut edges: Vec<(u32, u32)> = (start..end)
                    .map(|i| (hybrid.targets[i], hybrid.weights[i]))
                    .collect();
                edges.sort_unstable_by_key(|(target, _)| *target);
                edges
            })
            .collect();

        let mut offsets = Vec::with_capacity(n_states + 1);
        let mut sorted_targets = Vec::with_capacity(n_arcs);
        let mut sorted_weights = Vec::with_capacity(n_arcs);

        let mut offset = 0u64;
        for edges in sorted_per_state {
            offsets.push(offset);
            for (target, weight) in edges {
                sorted_targets.push(target);
                sorted_weights.push(weight);
            }
            offset = sorted_targets.len() as u64;
        }
        offsets.push(offset);

        Self { offsets, sorted_targets, sorted_weights }
    }

    #[inline]
    fn find_weight(&self, u: usize, v: u32) -> Option<u32> {
        let start = self.offsets[u] as usize;
        let end = self.offsets[u + 1] as usize;
        if start >= end { return None; }
        match self.sorted_targets[start..end].binary_search(&v) {
            Ok(idx) => Some(self.sorted_weights[start + idx]),
            Err(_) => None,
        }
    }
}

/// Customize CCH for hybrid state graph (uses parallel triangle relaxation)
pub fn customize_cch_hybrid(config: Step8HybridConfig) -> Result<Step8Result> {
    let start_time = std::time::Instant::now();
    let mode_name = match config.mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };
    println!("\n🎨 Step 8: Customizing CCH for {} (HYBRID)...\n", mode_name);

    println!("Loading CCH topology (hybrid)...");
    let topo = CchTopoFile::read(&config.cch_topo_path)?;
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();
    println!("  ✓ {} nodes, {} up edges, {} down edges", n_nodes, n_up, n_down);

    println!("Loading hybrid state graph...");
    let hybrid = HybridStateFile::read(&config.hybrid_state_path)?;
    println!("  ✓ {} states, {} arcs", hybrid.n_states, hybrid.n_arcs);

    if hybrid.n_states != topo.n_nodes {
        anyhow::bail!(
            "State count mismatch: hybrid has {} states, CCH topo has {} nodes",
            hybrid.n_states, topo.n_nodes
        );
    }

    println!("\nBuilding sorted hybrid adjacency (parallel)...");
    let sorted_hybrid = SortedHybridAdj::build(&hybrid);
    println!("  ✓ Built sorted adjacency");

    let rank_to_state = &topo.rank_to_filtered;

    println!("Pre-sorting down edges by target rank (parallel)...");
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end { return Vec::new(); }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();
    println!("  ✓ Pre-sorted down edges");

    // Bottom-up customization (sequential, single metric for hybrid)
    println!("\nCustomizing weights (bottom-up)...");
    let (up_weights, down_weights) = bottom_up_customize(
        &topo,
        &sorted_down_indices,
        |u_rank, v_rank| compute_hybrid_original_weight(u_rank, v_rank, &sorted_hybrid, rank_to_state),
    );
    println!("  ✓ Initial customization complete");

    // Parallel triangle relaxation
    println!("\nBuilding reverse DOWN adjacency...");
    let rev_down = build_reverse_down_adj_for_relax(&topo);
    println!("  ✓ {} entries", rev_down.sources.len());

    println!("\n🔺 Triangle relaxation (parallel)...");
    let tr_start = std::time::Instant::now();
    let (up_weights, down_weights, relax_count, relax_passes) =
        triangle_relax_parallel(&topo, up_weights, down_weights, &rev_down);
    println!("  ✓ {:.2}s, {} updates in {} passes",
        tr_start.elapsed().as_secs_f64(), relax_count, relax_passes);

    sanity_check_weights(&topo, &up_weights, &down_weights, "Hybrid", 95.0)?;

    std::fs::create_dir_all(&config.outdir)?;
    let output_path = config.outdir.join(format!("cch.w.hybrid.{}.u32", mode_name));

    println!("\nWriting output...");
    write_cch_weights(&output_path, &up_weights, &down_weights, config.mode)?;
    println!("  ✓ Written {}", output_path.display());

    let customize_time_ms = start_time.elapsed().as_millis() as u64;

    // Hybrid mode doesn't produce distance weights (no EBG nodes available)
    let distance_output_path = config.outdir.join(format!("cch.d.hybrid.{}.u32", mode_name));

    Ok(Step8Result {
        output_path,
        distance_output_path,
        mode: config.mode,
        n_up_edges: n_up as u64,
        n_down_edges: n_down as u64,
        customize_time_ms,
    })
}

#[inline]
fn compute_hybrid_original_weight(
    u_rank: usize,
    v_rank: usize,
    sorted_hybrid: &SortedHybridAdj,
    rank_to_state: &[u32],
) -> u32 {
    let u_state = rank_to_state[u_rank] as usize;
    let v_state = rank_to_state[v_rank];
    match sorted_hybrid.find_weight(u_state, v_state) {
        Some(w) => w,
        None => u32::MAX,
    }
}
