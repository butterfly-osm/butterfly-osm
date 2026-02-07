//! Step 6 validation - CCH ordering lock conditions (per-mode on filtered EBG)

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::path::Path;

use crate::formats::{FilteredEbgFile, OrderEbgFile};
use crate::step6::Step6Result;
use crate::step6_lifted::Step6LiftedResult;

#[derive(Debug, Serialize, Deserialize)]
pub struct Step6LockFile {
    pub mode: String,
    pub inputs_sha256: String,
    pub order_sha256: String,
    pub n_nodes: u32,
    pub n_components: usize,
    pub tree_depth: usize,
    pub build_time_ms: u64,
    pub created_at_utc: String,
}

/// Validate Step 6 outputs and generate lock file
pub fn validate_step6(
    result: &Step6Result,
    filtered_ebg_path: &Path,
) -> Result<Step6LockFile> {
    let mode_name = match result.mode {
        crate::profile_abi::Mode::Car => "car",
        crate::profile_abi::Mode::Bike => "bike",
        crate::profile_abi::Mode::Foot => "foot",
    };
    println!("\n🔐 Running Step 6 validation for {} mode...\n", mode_name);

    // Load data
    let order = OrderEbgFile::read(&result.order_path)?;
    let filtered_ebg = FilteredEbgFile::read(filtered_ebg_path)?;

    // Lock Condition A: Structural integrity
    println!("A. Structural integrity checks...");
    verify_permutation(&order)?;
    println!("  ✓ perm is a valid permutation");
    verify_inverse(&order)?;
    println!("  ✓ inv_perm is correct inverse");
    verify_node_count(&order, &filtered_ebg)?;
    println!("  ✓ node count matches filtered EBG");

    // Compute SHA-256
    let inputs_sha256 = hex::encode(order.inputs_sha);
    let order_sha256 = compute_file_sha256(&result.order_path)?;

    println!("\n✅ Step 6 validation passed for {} mode!", mode_name);

    Ok(Step6LockFile {
        mode: mode_name.to_string(),
        inputs_sha256,
        order_sha256,
        n_nodes: result.n_nodes,
        n_components: result.n_components,
        tree_depth: result.tree_depth,
        build_time_ms: result.build_time_ms,
        created_at_utc: chrono::Utc::now().to_rfc3339(),
    })
}

/// Verify perm is a valid permutation of [0..n)
fn verify_permutation(order: &crate::formats::OrderEbg) -> Result<()> {
    let n = order.n_nodes as usize;
    let mut seen = vec![false; n];

    for (i, &p) in order.perm.iter().enumerate() {
        anyhow::ensure!(
            (p as usize) < n,
            "perm[{}] = {} out of range [0, {})",
            i, p, n
        );
        anyhow::ensure!(
            !seen[p as usize],
            "perm contains duplicate value {} at index {}",
            p, i
        );
        seen[p as usize] = true;
    }

    Ok(())
}

/// Verify inv_perm[perm[i]] == i
fn verify_inverse(order: &crate::formats::OrderEbg) -> Result<()> {
    let n = order.n_nodes as usize;

    for i in 0..n {
        let p = order.perm[i] as usize;
        let inv = order.inv_perm[p] as usize;
        anyhow::ensure!(
            inv == i,
            "inv_perm mismatch: inv_perm[perm[{}]] = inv_perm[{}] = {}, expected {}",
            i, p, inv, i
        );
    }

    Ok(())
}

/// Verify node count matches filtered EBG
fn verify_node_count(order: &crate::formats::OrderEbg, filtered_ebg: &crate::formats::FilteredEbg) -> Result<()> {
    anyhow::ensure!(
        order.n_nodes == filtered_ebg.n_filtered_nodes,
        "order.n_nodes ({}) != filtered_ebg.n_filtered_nodes ({})",
        order.n_nodes, filtered_ebg.n_filtered_nodes
    );
    Ok(())
}

fn compute_file_sha256(path: &Path) -> Result<String> {
    use sha2::{Digest, Sha256};
    let data = std::fs::read(path)?;
    let hash = Sha256::digest(&data);
    Ok(hex::encode(hash))
}

/// Lock file for Step 6 Lifted ordering
#[derive(Debug, Serialize, Deserialize)]
pub struct Step6LiftedLockFile {
    pub mode: String,
    pub order_sha256: String,
    pub n_nbg_nodes: u32,
    pub n_ebg_states: u32,
    pub n_filtered_states: u32,
    pub nbg_ordering_time_ms: u64,
    pub lift_time_ms: u64,
    pub total_time_ms: u64,
    pub created_at_utc: String,
}

/// Validate Step 6 Lifted outputs and generate lock file
pub fn validate_step6_lifted(
    result: &Step6LiftedResult,
    filtered_ebg_path: &Path,
) -> Result<Step6LiftedLockFile> {
    let mode_name = match result.mode {
        crate::profile_abi::Mode::Car => "car",
        crate::profile_abi::Mode::Bike => "bike",
        crate::profile_abi::Mode::Foot => "foot",
    };
    println!("\n🔐 Running Step 6 (Lifted) validation for {} mode...\n", mode_name);

    // Load data
    let order = OrderEbgFile::read(&result.order_path)?;
    let filtered_ebg = FilteredEbgFile::read(filtered_ebg_path)?;

    // Lock Condition A: Structural integrity
    println!("A. Structural integrity checks...");
    verify_permutation(&order)?;
    println!("  ✓ perm is a valid permutation");
    verify_inverse(&order)?;
    println!("  ✓ inv_perm is correct inverse");
    verify_node_count(&order, &filtered_ebg)?;
    println!("  ✓ node count matches filtered EBG");

    // Compute SHA-256
    let order_sha256 = compute_file_sha256(&result.order_path)?;

    println!("\n✅ Step 6 (Lifted) validation passed for {} mode!", mode_name);

    Ok(Step6LiftedLockFile {
        mode: mode_name.to_string(),
        order_sha256,
        n_nbg_nodes: result.n_nbg_nodes,
        n_ebg_states: result.n_ebg_states,
        n_filtered_states: result.n_filtered_states,
        nbg_ordering_time_ms: result.nbg_ordering_time_ms,
        lift_time_ms: result.lift_time_ms,
        total_time_ms: result.total_time_ms,
        created_at_utc: chrono::Utc::now().to_rfc3339(),
    })
}
