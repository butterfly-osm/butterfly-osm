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
//! Since we process bottom-up, when computing a shortcut's weight, the weights
//! of its constituent edges have already been computed.
//!
//! # Performance
//!
//! - Edge lookup uses binary search (edges sorted by target)
//! - Original edge arc lookup uses linear scan (could be optimized with hashmap)
//! - Parallel processing of independent nodes (TODO)

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;

use crate::formats::{mod_turns, mod_weights, CchTopoFile, EbgCsrFile, OrderEbgFile};
use crate::profile_abi::Mode;

/// Configuration for Step 8
pub struct Step8Config {
    pub cch_topo_path: PathBuf,
    pub ebg_csr_path: PathBuf,
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

    // Load EBG CSR (for arc→turn_idx mapping)
    println!("Loading EBG CSR...");
    let ebg_csr = EbgCsrFile::read(&config.ebg_csr_path)?;
    println!("  ✓ {} arcs", ebg_csr.n_arcs);

    // Load ordering
    println!("Loading ordering...");
    let order = OrderEbgFile::read(&config.order_path)?;
    println!("  ✓ {} nodes", order.n_nodes);

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

    // Build arc lookup: precompute for faster original edge weight computation
    // For each EBG edge u→v, store the arc index
    println!("\nBuilding arc index...");
    let arc_lookup = build_arc_lookup(&ebg_csr);
    println!("  ✓ Built arc lookup");

    // Process in contraction order (bottom-up by rank)
    println!("\nCustomizing weights (bottom-up)...");
    let perm = &order.perm;
    let inv_perm = &order.inv_perm;

    let report_interval = (n_nodes / 20).max(1);

    for rank in 0..n_nodes {
        if rank % report_interval == 0 {
            let pct = (rank as f64 / n_nodes as f64) * 100.0;
            println!("  {:5.1}% customized", pct);
        }

        let u = inv_perm[rank] as usize;

        // Process UP edges from u (to higher-ranked nodes)
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;

        for i in up_start..up_end {
            let v = topo.up_targets[i] as usize;

            if !topo.up_is_shortcut[i] {
                // Original edge: weight = w[v] + turn_penalty
                let weight = compute_original_weight(u, v, &weights.weights, &turns.penalties, &arc_lookup);
                up_weights[i] = weight;
            } else {
                // Shortcut via m: weight = weight(u→m) + weight(m→v)
                let m = topo.up_middle[i] as usize;

                // u→m: rank(m) < rank(u), so it's a DOWN edge from u
                let w_um = find_edge_weight(
                    u,
                    m,
                    &topo.down_offsets,
                    &topo.down_targets,
                    &down_weights,
                );

                // m→v: rank(v) > rank(m), so it's an UP edge from m
                let w_mv =
                    find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);

                up_weights[i] = w_um.saturating_add(w_mv);
            }
        }

        // Process DOWN edges from u (to lower-ranked nodes)
        let down_start = topo.down_offsets[u] as usize;
        let down_end = topo.down_offsets[u + 1] as usize;

        for i in down_start..down_end {
            let v = topo.down_targets[i] as usize;

            if !topo.down_is_shortcut[i] {
                // Original edge: weight = w[v] + turn_penalty
                let weight = compute_original_weight(u, v, &weights.weights, &turns.penalties, &arc_lookup);
                down_weights[i] = weight;
            } else {
                // Shortcut via m: weight = weight(u→m) + weight(m→v)
                let m = topo.down_middle[i] as usize;

                // For down shortcut u→v via m:
                // rank(m) < rank(v) < rank(u)
                // u→m: DOWN edge from u (rank(m) < rank(u))
                let w_um = find_edge_weight(
                    u,
                    m,
                    &topo.down_offsets,
                    &topo.down_targets,
                    &down_weights,
                );

                // m→v: UP edge from m (rank(v) > rank(m))
                let w_mv =
                    find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);

                down_weights[i] = w_um.saturating_add(w_mv);
            }
        }
    }

    println!("  ✓ Customization complete");

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

/// Build arc lookup: for each (u, v) pair in EBG, store the arc index
/// Returns a nested structure: arc_lookup[u] = sorted vec of (target, arc_idx)
fn build_arc_lookup(ebg_csr: &crate::formats::EbgCsr) -> Vec<Vec<(u32, usize)>> {
    let n_nodes = ebg_csr.n_nodes as usize;
    let mut lookup: Vec<Vec<(u32, usize)>> = vec![Vec::new(); n_nodes];

    for u in 0..n_nodes {
        let start = ebg_csr.offsets[u] as usize;
        let end = ebg_csr.offsets[u + 1] as usize;

        for arc_idx in start..end {
            let v = ebg_csr.heads[arc_idx];
            lookup[u].push((v, arc_idx));
        }

        // Sort by target for binary search
        lookup[u].sort_unstable_by_key(|(v, _)| *v);
    }

    lookup
}

/// Find arc index for edge u→v using binary search
fn find_arc_index(arc_lookup: &[Vec<(u32, usize)>], u: usize, v: usize) -> Option<usize> {
    let targets = &arc_lookup[u];
    let v32 = v as u32;

    match targets.binary_search_by_key(&v32, |(t, _)| *t) {
        Ok(idx) => Some(targets[idx].1),
        Err(_) => None,
    }
}

/// Compute weight for an original edge
fn compute_original_weight(
    u: usize,
    v: usize,
    node_weights: &[u32],
    turn_penalties: &[u32],
    arc_lookup: &[Vec<(u32, usize)>],
) -> u32 {
    let w_v = node_weights[v];

    // If target node is inaccessible, edge is inaccessible
    if w_v == 0 {
        return u32::MAX;
    }

    // Find arc index to get turn penalty
    match find_arc_index(arc_lookup, u, v) {
        Some(arc_idx) => {
            let penalty = turn_penalties[arc_idx];
            w_v.saturating_add(penalty)
        }
        None => {
            // Edge not found in EBG - should not happen for original edges
            // This might indicate a self-loop that was excluded
            u32::MAX
        }
    }
}

/// Find edge weight using binary search in CSR
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
