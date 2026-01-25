//! Step 8: CCH Customization
//!
//! Applies per-mode weights to the CCH shortcuts using bottom-up customization.
//!
//! # Algorithm Overview
//!
//! CCH customization processes nodes in contraction order (lowest rank first).
//! For each edge in the up/down graphs:
//!
//! - **Original edges**: weight = edge_weight[target] + turn_penalty[arc]
//! - **Shortcuts u→w via m**: weight = weight(u→m) + weight(m→w)
//!
//! # Dependency Order (CRITICAL)
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
//! # Performance
//!
//! - Edge lookup uses binary search (CCH edges sorted by target in Step 7)
//! - Original edge arc lookup uses binary search on sorted EBG adjacency
//! - Parallel processing via Rayon for independent node batches

use anyhow::Result;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::formats::{mod_turns, mod_weights, CchTopoFile, FilteredEbgFile, OrderEbgFile};
use crate::profile_abi::Mode;

/// Configuration for Step 8
pub struct Step8Config {
    pub cch_topo_path: PathBuf,
    pub filtered_ebg_path: PathBuf,
    pub weights_path: PathBuf, // w.*.u32
    pub turns_path: PathBuf,   // t.*.u32
    pub order_path: PathBuf,
    pub mode: Mode,
    pub outdir: PathBuf,
}

/// Result of Step 8 customization
#[derive(Debug)]
pub struct Step8Result {
    pub output_path: PathBuf,
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

        // Collect and sort edges per node in parallel
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

        // Flatten into CSR structure
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

        Self {
            offsets,
            sorted_heads,
            sorted_orig_arc_idx,
        }
    }

    /// Find original arc index for filtered edge u→v using binary search
    #[inline]
    fn find_original_arc_index(&self, u: usize, v: u32) -> Option<u32> {
        let start = self.offsets[u] as usize;
        let end = self.offsets[u + 1] as usize;
        if start >= end {
            return None;
        }
        let slice = &self.sorted_heads[start..end];
        match slice.binary_search(&v) {
            Ok(idx) => Some(self.sorted_orig_arc_idx[start + idx]),
            Err(_) => None,
        }
    }
}

/// Customize CCH for a specific mode
pub fn customize_cch(config: Step8Config) -> Result<Step8Result> {
    let start_time = std::time::Instant::now();
    let mode_name = match config.mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };
    println!("\n🎨 Step 8: Customizing CCH for {}...\n", mode_name);

    // Load CCH topology
    println!("Loading CCH topology...");
    let topo = CchTopoFile::read(&config.cch_topo_path)?;
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();
    println!(
        "  ✓ {} nodes, {} up edges, {} down edges",
        n_nodes, n_up, n_down
    );

    // Load filtered EBG (for arc→turn_idx mapping and node ID mapping)
    println!("Loading filtered EBG...");
    let filtered_ebg = FilteredEbgFile::read(&config.filtered_ebg_path)?;
    println!("  ✓ {} filtered nodes, {} arcs", filtered_ebg.n_filtered_nodes, filtered_ebg.n_filtered_arcs);

    // Note: With rank-aligned CCH (Version 2), we no longer need the order file.
    // The rank_to_filtered mapping is stored directly in the CCH topology.
    // Keeping the order_path in config for backward compatibility but not loading it.

    // Load weights and turn penalties
    println!("Loading weights ({})...", mode_name);
    let weights = mod_weights::read_all(&config.weights_path)?;
    println!("  ✓ {} node weights", weights.weights.len());

    println!("Loading turn penalties ({})...", mode_name);
    let turns = mod_turns::read_all(&config.turns_path)?;
    println!("  ✓ {} arc penalties", turns.penalties.len());

    // Allocate weight arrays
    let mut up_weights = vec![u32::MAX; n_up];
    let mut down_weights = vec![u32::MAX; n_down];

    // Build sorted filtered EBG adjacency for fast arc lookup
    println!("\nBuilding sorted filtered EBG adjacency (parallel)...");
    let sorted_ebg = SortedFilteredEbgAdj::build(&filtered_ebg);
    println!("  ✓ Built sorted adjacency");

    // Note: We don't need ebg_csr.turn_idx - turn penalties are indexed by arc_idx directly

    // Process in contraction order (bottom-up by rank)
    // With rank-aligned CCH: node_id == rank, no inv_perm lookup needed!
    println!("\nCustomizing weights (bottom-up)...");

    // rank_to_filtered: convert rank position back to filtered_id for weight lookups
    let rank_to_filtered = &topo.rank_to_filtered;

    let report_interval = (n_nodes / 20).max(1);

    // Pre-compute sorted down edge indices for each node (sorted by target rank)
    // With rank-aligned CCH, targets ARE already ranks - just sort directly
    println!("  Pre-sorting down edges by target rank (parallel)...");
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            // u is the rank (node_id == rank in rank-aligned CCH)
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end {
                return Vec::new();
            }
            let mut indices: Vec<usize> = (start..end).collect();
            // down_targets[i] is already the target's rank - no perm lookup needed!
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();
    println!("  ✓ Pre-sorted down edges");

    // Main customization loop
    // With rank-aligned CCH: u IS the rank (node_id == rank)
    for rank in 0..n_nodes {
        if rank % report_interval == 0 {
            let pct = (rank as f64 / n_nodes as f64) * 100.0;
            println!("  {:5.1}% customized", pct);
        }

        // In rank-aligned CCH, node_id == rank (no inv_perm lookup!)
        let u = rank;

        // ===== PHASE 1: Process DOWN edges (sorted by target rank) =====
        // This MUST come before UP edges because UP shortcuts depend on down_weights[u→m]
        for &i in &sorted_down_indices[u] {
            // v is the target's rank (rank-aligned CCH)
            let v = topo.down_targets[i] as usize;

            if !topo.down_is_shortcut[i] {
                // Original edge: weight = w[original_v] + turn_penalty
                // Need to convert rank -> filtered_id -> original_id for weight lookup
                let weight = compute_original_weight_rank_aligned(
                    u,
                    v,
                    &weights.weights,
                    &turns.penalties,
                    &sorted_ebg,
                    &filtered_ebg.filtered_to_original,
                    rank_to_filtered,
                );
                down_weights[i] = weight;
            } else {
                // Shortcut via m: weight = weight(u→m) + weight(m→v)
                // rank(m) < rank(v) < rank(u)
                // m is the middle node's rank (rank-aligned CCH)
                let m = topo.down_middle[i] as usize;

                // u→m: DOWN edge from u (rank(m) < rank(u))
                // Already computed because we process by increasing target rank
                let w_um = find_edge_weight(
                    u,
                    m,
                    &topo.down_offsets,
                    &topo.down_targets,
                    &down_weights,
                );

                // m→v: UP edge from m (rank(v) > rank(m))
                // Already computed because node m was processed earlier
                let w_mv = find_edge_weight(
                    m,
                    v,
                    &topo.up_offsets,
                    &topo.up_targets,
                    &up_weights,
                );

                down_weights[i] = w_um.saturating_add(w_mv);
            }
        }

        // ===== PHASE 2: Process UP edges =====
        // All down_weights[u→*] are now computed, so UP shortcuts can safely read them
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;

        for i in up_start..up_end {
            // v is the target's rank (rank-aligned CCH)
            let v = topo.up_targets[i] as usize;

            if !topo.up_is_shortcut[i] {
                // Original edge: weight = w[original_v] + turn_penalty
                let weight = compute_original_weight_rank_aligned(
                    u,
                    v,
                    &weights.weights,
                    &turns.penalties,
                    &sorted_ebg,
                    &filtered_ebg.filtered_to_original,
                    rank_to_filtered,
                );
                up_weights[i] = weight;
            } else {
                // Shortcut via m: weight = weight(u→m) + weight(m→v)
                // rank(m) < rank(u) < rank(v)
                let m = topo.up_middle[i] as usize;

                // u→m: DOWN edge from u (rank(m) < rank(u))
                // Just computed in phase 1
                let w_um = find_edge_weight(
                    u,
                    m,
                    &topo.down_offsets,
                    &topo.down_targets,
                    &down_weights,
                );

                // m→v: UP edge from m (rank(v) > rank(m))
                // Already computed because node m was processed earlier
                let w_mv = find_edge_weight(
                    m,
                    v,
                    &topo.up_offsets,
                    &topo.up_targets,
                    &up_weights,
                );

                up_weights[i] = w_um.saturating_add(w_mv);
            }
        }
    }

    println!("  ✓ Initial customization complete");

    // ===== PASS 2: Triangle Relaxation =====
    // Process nodes in apex-rank order (lowest to highest).
    // For each apex m, relax edges x→y where:
    //   - x→m is a DOWN edge from x (rank[x] > rank[m])
    //   - m→y is an UP edge from m (rank[y] > rank[m])
    //   - w(x,y) = min(w(x,y), w(x,m) + w(m,y))
    //
    // This finds cheaper paths through alternative contracted nodes.

    // Build reverse DOWN adjacency: for each node m, list incoming DOWN edges x→m
    println!("\nBuilding reverse DOWN adjacency...");
    let mut down_rev_counts = vec![0u64; n_nodes];
    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let m = topo.down_targets[i] as usize;
            down_rev_counts[m] += 1;
        }
    }

    let mut down_rev_offsets = vec![0u64; n_nodes + 1];
    for m in 0..n_nodes {
        down_rev_offsets[m + 1] = down_rev_offsets[m] + down_rev_counts[m];
    }

    let total_down_rev = down_rev_offsets[n_nodes] as usize;
    let mut down_rev_sources = vec![0u32; total_down_rev];
    let mut down_rev_edge_idx = vec![0usize; total_down_rev];
    let mut down_rev_insert = vec![0u64; n_nodes];

    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let m = topo.down_targets[i] as usize;
            let pos = (down_rev_offsets[m] + down_rev_insert[m]) as usize;
            down_rev_sources[pos] = u as u32;
            down_rev_edge_idx[pos] = i;
            down_rev_insert[m] += 1;
        }
    }
    println!("  ✓ Built reverse DOWN adjacency ({} entries)", total_down_rev);

    // Triangle relaxation by apex rank (lowest to highest)
    // Iterate until convergence (no more updates)
    println!("\nTriangle relaxation (iterating until convergence)...");
    let mut total_relaxations = 0u64;
    let mut pass = 0;

    loop {
        pass += 1;
        let mut pass_relaxations = 0u64;

        for rank in 0..n_nodes {
            if pass == 1 && rank % report_interval == 0 {
                let pct = (rank as f64 / n_nodes as f64) * 100.0;
                println!("  Pass 1: {:5.1}% relaxed ({} updates so far)", pct, pass_relaxations);
            }

            // In rank-aligned CCH, node_id == rank (no inv_perm lookup!)
            let m = rank; // Apex node (lowest rank in triangle)

            // For each incoming DOWN edge x→m (i.e., edges from higher-rank nodes to m)
            let rev_start = down_rev_offsets[m] as usize;
            let rev_end = down_rev_offsets[m + 1] as usize;

            for i_rev in rev_start..rev_end {
                // x is a rank (rank-aligned CCH)
                let x = down_rev_sources[i_rev] as usize;
                let edge_idx_xm = down_rev_edge_idx[i_rev];
                let w_xm = down_weights[edge_idx_xm];

                if w_xm == u32::MAX {
                    continue;
                }

                // For each UP edge m→y
                let up_start = topo.up_offsets[m] as usize;
                let up_end = topo.up_offsets[m + 1] as usize;

                for i_my in up_start..up_end {
                    // y is a rank (rank-aligned CCH)
                    let y = topo.up_targets[i_my] as usize;

                    if y == x {
                        continue; // Skip self-loop
                    }

                    let w_my = up_weights[i_my];

                    if w_my == u32::MAX {
                        continue;
                    }

                    let new_weight = w_xm.saturating_add(w_my);

                    // Check if x→y edge exists and relax
                    // In rank-aligned CCH, x and y ARE already ranks
                    let rank_x = x;
                    let rank_y = y;

                    if rank_y > rank_x {
                        // UP edge from x
                        if let Some(idx) = find_edge_index(x, y, &topo.up_offsets, &topo.up_targets) {
                            if new_weight < up_weights[idx] {
                                up_weights[idx] = new_weight;
                                pass_relaxations += 1;
                            }
                        }
                    } else {
                        // DOWN edge from x
                        if let Some(idx) = find_edge_index(x, y, &topo.down_offsets, &topo.down_targets) {
                            if new_weight < down_weights[idx] {
                                down_weights[idx] = new_weight;
                                pass_relaxations += 1;
                            }
                        }
                    }
                }
            }
        }

        println!("  Pass {}: {} updates", pass, pass_relaxations);
        total_relaxations += pass_relaxations;

        if pass_relaxations == 0 {
            break; // Converged
        }

        if pass >= 100 {
            println!("  WARNING: Did not converge after 100 passes!");
            break;
        }
    }
    println!("  ✓ Triangle relaxation complete: {} total updates in {} passes", total_relaxations, pass);

    // Detailed sanity check
    let mut up_orig_max = 0usize;
    let mut up_short_max = 0usize;
    let mut down_orig_max = 0usize;
    let mut down_short_max = 0usize;
    let mut up_orig_total = 0usize;
    let mut up_short_total = 0usize;
    let mut down_orig_total = 0usize;
    let mut down_short_total = 0usize;

    for i in 0..n_up {
        if topo.up_is_shortcut[i] {
            up_short_total += 1;
            if up_weights[i] == u32::MAX {
                up_short_max += 1;
            }
        } else {
            up_orig_total += 1;
            if up_weights[i] == u32::MAX {
                up_orig_max += 1;
            }
        }
    }

    for i in 0..n_down {
        if topo.down_is_shortcut[i] {
            down_short_total += 1;
            if down_weights[i] == u32::MAX {
                down_short_max += 1;
            }
        } else {
            down_orig_total += 1;
            if down_weights[i] == u32::MAX {
                down_orig_max += 1;
            }
        }
    }

    let up_max_count = up_orig_max + up_short_max;
    let down_max_count = down_orig_max + down_short_max;
    let total_max = up_max_count + down_max_count;
    let total_edges = n_up + n_down;
    let max_pct = (total_max as f64 / total_edges as f64) * 100.0;

    println!("\n📊 Sanity check:");
    println!(
        "  Unreachable edges: {} / {} ({:.2}%)",
        total_max, total_edges, max_pct
    );
    println!("    Up original:  {} / {} ({:.2}%)", up_orig_max, up_orig_total,
             up_orig_max as f64 / up_orig_total as f64 * 100.0);
    println!("    Up shortcuts: {} / {} ({:.2}%)", up_short_max, up_short_total,
             up_short_max as f64 / up_short_total as f64 * 100.0);
    println!("    Down original:  {} / {} ({:.2}%)", down_orig_max, down_orig_total,
             down_orig_max as f64 / down_orig_total as f64 * 100.0);
    println!("    Down shortcuts: {} / {} ({:.2}%)", down_short_max, down_short_total,
             down_short_max as f64 / down_short_total as f64 * 100.0);

    // Note: High unreachable percentage is expected for modes with many restricted roads.
    // Car mode in Belgium has ~52% inaccessible nodes (pedestrian paths, one-way, etc.)
    // Shortcuts cascade: if either leg is unreachable, shortcut is unreachable.
    // P(both legs reachable) ≈ 0.48² = 23%, so ~77% unreachable shortcuts is normal.
    if max_pct > 95.0 {
        anyhow::bail!(
            "CRITICAL: {}% of edges are unreachable (u32::MAX). This indicates a bug!",
            max_pct
        );
    }

    // Write output
    std::fs::create_dir_all(&config.outdir)?;
    let output_path = config.outdir.join(format!("cch.w.{}.u32", mode_name));

    println!("\nWriting output...");
    write_cch_weights(&output_path, &up_weights, &down_weights, config.mode)?;
    println!("  ✓ Written {}", output_path.display());

    let customize_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step8Result {
        output_path,
        mode: config.mode,
        n_up_edges: n_up as u64,
        n_down_edges: n_down as u64,
        customize_time_ms,
    })
}

/// Compute weight for an original edge (deprecated - use rank-aligned version)
///
/// CCH operates in filtered node space, but weights are indexed by original node ID.
/// We map filtered_v → original_v for weight lookup.
/// Turn penalties are indexed by original arc index (from filtered_ebg.original_arc_idx).
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
    // Map filtered v to original v for weight lookup
    let original_v = filtered_to_original[v] as usize;
    let w_v = node_weights[original_v];

    // If target node is inaccessible, edge is inaccessible
    if w_v == 0 {
        return u32::MAX;
    }

    // Find original arc index to get turn penalty
    // Turn penalties are indexed by original arc_idx
    match sorted_ebg.find_original_arc_index(u, v as u32) {
        Some(orig_arc_idx) => {
            let penalty = turn_penalties[orig_arc_idx as usize];
            w_v.saturating_add(penalty)
        }
        None => {
            // Edge not found in filtered EBG - should not happen for original edges
            u32::MAX
        }
    }
}

/// Compute weight for an original edge in rank-aligned CCH
///
/// In rank-aligned CCH, node IDs are rank positions, not filtered IDs.
/// We need to convert: rank → filtered_id → original_id for weight lookup.
/// Turn penalties are indexed by original arc index (from filtered_ebg.original_arc_idx).
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
    // Convert rank positions to filtered IDs
    let u_filtered = rank_to_filtered[u_rank] as usize;
    let v_filtered = rank_to_filtered[v_rank] as usize;

    // Map filtered v to original v for weight lookup
    let original_v = filtered_to_original[v_filtered] as usize;
    let w_v = node_weights[original_v];

    // If target node is inaccessible, edge is inaccessible
    if w_v == 0 {
        return u32::MAX;
    }

    // Find original arc index to get turn penalty
    // sorted_ebg uses filtered IDs, not ranks
    match sorted_ebg.find_original_arc_index(u_filtered, v_filtered as u32) {
        Some(orig_arc_idx) => {
            let penalty = turn_penalties[orig_arc_idx as usize];
            w_v.saturating_add(penalty)
        }
        None => {
            // Edge not found in filtered EBG - should not happen for original edges
            u32::MAX
        }
    }
}

/// Find edge weight using binary search in CCH CSR
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

    if start >= end {
        return u32::MAX;
    }

    let slice = &targets[start..end];
    let v32 = v as u32;

    match slice.binary_search(&v32) {
        Ok(idx) => weights[start + idx],
        Err(_) => u32::MAX,
    }
}

/// Find edge index using binary search in CCH CSR
/// Returns Some(global_index) if edge exists, None otherwise
#[inline]
fn find_edge_index(
    u: usize,
    v: usize,
    offsets: &[u64],
    targets: &[u32],
) -> Option<usize> {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;

    if start >= end {
        return None;
    }

    let slice = &targets[start..end];
    let v32 = v as u32;

    match slice.binary_search(&v32) {
        Ok(idx) => Some(start + idx),
        Err(_) => None,
    }
}

/// Write CCH weights to file
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
    let padding = [0u8; 8]; // Pad to 32 bytes

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

    // Up weights
    for &w in up_weights {
        let bytes = w.to_le_bytes();
        writer.write_all(&bytes)?;
        crc_digest.update(&bytes);
    }

    // Down weights
    for &w in down_weights {
        let bytes = w.to_le_bytes();
        writer.write_all(&bytes)?;
        crc_digest.update(&bytes);
    }

    // Footer
    let crc = crc_digest.finalize();
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(&crc.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}
