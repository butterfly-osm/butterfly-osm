//! Step 6 (Lifted): CCH ordering via NBG ND + lift to EBG
//!
//! Key insight from CCH theory:
//! - Compute ND ordering on the PHYSICAL node graph (NBG) - good separators
//! - Lift to edge-state graph (EBG) with block ranks
//! - All states of a physical node get consecutive ranks
//!
//! This avoids the problem of ND on the edge-state graph destroying
//! separator quality due to turn-based cross-links.

use anyhow::Result;
use std::path::PathBuf;

use crate::formats::{
    EbgNodesFile, EbgCsrFile, FilteredEbgFile, NbgCsrFile, NbgGeoFile,
    OrderEbg, OrderEbgFile,
};
use crate::nbg_ch::{compute_nbg_ordering, lift_ordering_to_ebg};
use crate::profile_abi::Mode;

/// Configuration for Step 6 Lifted
pub struct Step6LiftedConfig {
    pub nbg_csr_path: PathBuf,
    pub nbg_geo_path: PathBuf,
    pub ebg_nodes_path: PathBuf,
    pub ebg_csr_path: PathBuf,
    pub filtered_ebg_path: PathBuf,
    pub mode: Mode,
    pub outdir: PathBuf,
    pub leaf_threshold: usize,
}

/// Result of Step 6 Lifted
#[derive(Debug)]
pub struct Step6LiftedResult {
    pub order_path: PathBuf,
    pub mode: Mode,
    pub n_nbg_nodes: u32,
    pub n_ebg_states: u32,
    pub n_filtered_states: u32,
    pub nbg_ordering_time_ms: u64,
    pub lift_time_ms: u64,
    pub total_time_ms: u64,
}

/// Generate CCH ordering via lifted NBG ordering
pub fn generate_lifted_ordering(config: Step6LiftedConfig) -> Result<Step6LiftedResult> {
    let start_time = std::time::Instant::now();
    let mode_name = match config.mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };

    println!("\n📐 Step 6 (Lifted): CCH ordering for {} via NBG lift\n", mode_name);

    // Step 1: Load NBG and compute ND ordering on physical graph
    println!("[1/3] Computing ND ordering on physical node graph (NBG)...");
    let nbg_start = std::time::Instant::now();

    let nbg_csr = NbgCsrFile::read(&config.nbg_csr_path)?;
    println!("  Loaded NBG: {} nodes, {} edges", nbg_csr.n_nodes, nbg_csr.n_edges_und);

    let nbg_geo = NbgGeoFile::read(&config.nbg_geo_path)?;

    let nbg_ordering = compute_nbg_ordering(
        &nbg_csr,
        &nbg_geo,
        config.leaf_threshold,
        0.05, // balance_eps
    )?;
    let nbg_ordering_time = nbg_start.elapsed().as_millis() as u64;
    println!("  NBG ordering: {} nodes, {} components, depth {}",
             nbg_ordering.n_nodes, nbg_ordering.n_components, nbg_ordering.max_depth);
    println!("  Time: {} ms", nbg_ordering_time);

    // Step 2: Load EBG and lift ordering
    println!("\n[2/3] Lifting ordering to edge-state graph (EBG)...");
    let lift_start = std::time::Instant::now();

    let ebg_nodes = EbgNodesFile::read(&config.ebg_nodes_path)?;
    let ebg_csr = EbgCsrFile::read(&config.ebg_csr_path)?;
    println!("  Loaded EBG: {} states, {} arcs", ebg_csr.n_nodes, ebg_csr.n_arcs);

    let lifted = lift_ordering_to_ebg(&nbg_ordering, &ebg_nodes, &ebg_csr)?;
    let lift_time = lift_start.elapsed().as_millis() as u64;
    println!("  Lifted to {} edge-states", lifted.n_states);
    println!("  Time: {} ms", lift_time);

    // Step 3: Map to filtered EBG space and write output
    println!("\n[3/3] Mapping to filtered EBG and writing output...");

    let filtered_ebg = FilteredEbgFile::read(&config.filtered_ebg_path)?;
    println!("  Filtered EBG: {} nodes (of {} original)",
             filtered_ebg.n_filtered_nodes, filtered_ebg.n_original_nodes);

    // Create filtered ordering: only include nodes that pass the filter
    // filtered_to_orig maps filtered_id -> original_id
    // We need to create perm/inv_perm for filtered space

    let n_filtered = filtered_ebg.n_filtered_nodes as usize;
    let mut filtered_perm: Vec<u32> = vec![0; n_filtered];
    let mut filtered_inv_perm: Vec<u32> = vec![0; n_filtered];

    // Collect (filtered_id, original_rank) pairs and sort by rank
    let mut rank_pairs: Vec<(u32, u32)> = Vec::with_capacity(n_filtered);
    for filtered_id in 0..n_filtered {
        let orig_id = filtered_ebg.filtered_to_original[filtered_id];
        let orig_rank = lifted.perm[orig_id as usize];
        rank_pairs.push((filtered_id as u32, orig_rank));
    }
    rank_pairs.sort_by_key(|(_, rank)| *rank);

    // Assign new ranks in filtered space
    for (new_rank, (filtered_id, _)) in rank_pairs.iter().enumerate() {
        filtered_perm[*filtered_id as usize] = new_rank as u32;
        filtered_inv_perm[new_rank] = *filtered_id;
    }

    // Write output
    let order = OrderEbg {
        n_nodes: n_filtered as u32,
        inputs_sha: [0u8; 32], // TODO: compute proper SHA
        perm: filtered_perm,
        inv_perm: filtered_inv_perm,
    };

    let order_path = config.outdir.join(format!("order.lifted.{}.bin", mode_name));
    OrderEbgFile::write(&order_path, &order)?;
    println!("  Written: {}", order_path.display());

    let total_time = start_time.elapsed().as_millis() as u64;

    println!("\n✅ Step 6 (Lifted) complete!");
    println!("  NBG ordering: {} ms", nbg_ordering_time);
    println!("  Lift to EBG:  {} ms", lift_time);
    println!("  Total:        {} ms", total_time);

    Ok(Step6LiftedResult {
        order_path,
        mode: config.mode,
        n_nbg_nodes: nbg_ordering.n_nodes,
        n_ebg_states: lifted.n_states,
        n_filtered_states: n_filtered as u32,
        nbg_ordering_time_ms: nbg_ordering_time,
        lift_time_ms: lift_time,
        total_time_ms: total_time,
    })
}
