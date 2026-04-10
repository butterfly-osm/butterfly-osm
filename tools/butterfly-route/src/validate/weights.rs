//! Step 5 validation - Per-mode weights lock conditions

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::formats::*;
use crate::profile_abi::Mode;
use crate::weights::Step5Result;

#[derive(Debug, Serialize, Deserialize)]
pub struct Step5LockFile {
    pub inputs_sha256: String,
    pub modes: HashMap<String, ModeLockData>,
    pub node_count: u32,
    pub arc_count: u64,
    pub created_at_utc: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct ModeLockData {
    pub w_sha256: String,
    pub t_sha256: String,
    pub mask_sha256: String,
}

/// Validate Step 5 outputs and generate lock file.
///
/// `way_attrs_by_name` maps mode_name -> way_attrs path (e.g. "car" -> "/data/step2/way_attrs.car.bin").
pub fn validate_step5(
    result: &Step5Result,
    ebg_nodes_path: &Path,
    ebg_csr_path: &Path,
    turn_table_path: &Path,
    nbg_geo_path: &Path,
    way_attrs_by_name: &HashMap<String, std::path::PathBuf>,
) -> Result<Step5LockFile> {
    println!("\n  Running Step 5 validation lock conditions...\n");

    // Load all data
    let ebg_nodes = EbgNodesFile::read(ebg_nodes_path)?;
    let ebg_csr = EbgCsrFile::read(ebg_csr_path)?;
    let turn_table = TurnTableFile::read(turn_table_path)?;
    let nbg_geo = NbgGeoFile::read(nbg_geo_path)?;

    let mut modes_lock = HashMap::new();

    for mode_output in &result.modes {
        let mode_name = &mode_output.mode_name;
        let mode = Mode(mode_output.mode_index);

        // Load generated weights/turns/masks for this mode
        let weights = mod_weights::read_all(&mode_output.weights_path)?;
        let turns = mod_turns::read_all(&mode_output.turns_path)?;
        let mask = mod_mask::read_all(&mode_output.mask_path)?;

        // Lock Condition A: Structural integrity
        println!("A. Structural integrity checks for '{}'...", mode_name);
        let t0 = std::time::Instant::now();
        verify_lock_a_structural(&ebg_nodes, &ebg_csr, &weights, &turns, &mask)?;
        println!(
            "  Passed structural checks for '{}' ({:.3}s)",
            mode_name,
            t0.elapsed().as_secs_f64()
        );

        // Lock Condition B: Math parity
        if let Some(way_attrs_path) = way_attrs_by_name.get(mode_name) {
            println!("\nB. Math parity checks for '{}'...", mode_name);
            let t0 = std::time::Instant::now();
            let way_attrs_data = way_attrs::read_all(way_attrs_path)?;
            let way_index = build_way_index(&way_attrs_data);
            verify_lock_b_math(&ebg_nodes, &nbg_geo, &weights, &mask, &way_index, mode)?;
            println!(
                "  Passed math parity checks for '{}' ({:.3}s)",
                mode_name,
                t0.elapsed().as_secs_f64()
            );
        }

        // Lock Condition C: Arc/turn consistency
        println!("\nC. Arc/turn consistency checks for '{}'...", mode_name);
        let t0 = std::time::Instant::now();
        verify_lock_c_turns(&ebg_csr, &turn_table, &turns, mode)?;
        println!(
            "  Passed turn consistency checks for '{}' ({:.3}s)",
            mode_name,
            t0.elapsed().as_secs_f64()
        );

        // Lock Condition E: Sanity & bounds
        println!("\nE. Sanity & bounds checks for '{}'...", mode_name);
        let t0 = std::time::Instant::now();
        let max_weight = match mode_name.as_str() {
            "car" => 10_000_000u32,
            _ => 5_000_000u32,
        };
        verify_lock_e_bounds(&weights, &mask, 1, max_weight, mode_name)?;
        println!(
            "  Passed sanity & bounds checks for '{}' ({:.3}s)",
            mode_name,
            t0.elapsed().as_secs_f64()
        );

        // Calculate SHA-256 hashes for lock file
        let lock_data = ModeLockData {
            w_sha256: super::compute_sha256(&mode_output.weights_path)?,
            t_sha256: super::compute_sha256(&mode_output.turns_path)?,
            mask_sha256: super::compute_sha256(&mode_output.mask_path)?,
        };
        modes_lock.insert(mode_name.clone(), lock_data);
    }

    // Lock Condition D: Graph-level parity (Dijkstra reachability)
    println!("\nD. Graph-level parity (Dijkstra reachability)...");
    println!("  -- Deferred: requires CCH from steps 6-8, validated post-step8 instead");

    Ok(Step5LockFile {
        inputs_sha256: hex::encode(ebg_nodes.inputs_sha),
        modes: modes_lock,
        node_count: result.n_nodes,
        arc_count: result.n_arcs,
        created_at_utc: chrono::Utc::now().to_rfc3339(),
    })
}

/// Lock A.1-A.3: Structural integrity
fn verify_lock_a_structural(
    ebg_nodes: &EbgNodes,
    ebg_csr: &EbgCsr,
    weights: &ModWeights,
    turns: &ModTurns,
    mask: &ModMask,
) -> Result<()> {
    // A.1: Sizes must match
    anyhow::ensure!(
        weights.weights.len() == ebg_nodes.n_nodes as usize,
        "Weight count mismatch: {} != {}",
        weights.weights.len(),
        ebg_nodes.n_nodes
    );

    anyhow::ensure!(
        turns.penalties.len() == ebg_csr.n_arcs as usize,
        "Turn penalty count mismatch: {} != {}",
        turns.penalties.len(),
        ebg_csr.n_arcs
    );

    anyhow::ensure!(
        mask.n_nodes == ebg_nodes.n_nodes,
        "Mask node count mismatch: {} != {}",
        mask.n_nodes,
        ebg_nodes.n_nodes
    );

    Ok(())
}

/// Lock B.4-B.6: Math parity checks
fn verify_lock_b_math(
    ebg_nodes: &EbgNodes,
    nbg_geo: &NbgGeo,
    weights: &ModWeights,
    mask: &ModMask,
    way_index: &HashMap<i64, WayAttr>,
    mode: Mode,
) -> Result<()> {
    let n_nodes = ebg_nodes.n_nodes as usize;
    let sample_size = std::cmp::min(100_000, n_nodes);

    let mut sampled = 0;
    for i in 0..sample_size {
        let ebg_id = (i * 7919) % n_nodes;
        let ebg_node = &ebg_nodes.nodes[ebg_id];
        let nbg_edge = &nbg_geo.edges[ebg_node.geom_idx as usize];

        // Look up way attributes
        let way_attr = match way_index.get(&nbg_edge.first_osm_way_id) {
            Some(attr) => attr,
            None => {
                // Not in index - should be inaccessible
                anyhow::ensure!(
                    weights.weights[ebg_id] == 0 && !mask.get(ebg_id as u32),
                    "Node {} not in way_index but has weight={} mask={}",
                    ebg_id,
                    weights.weights[ebg_id],
                    mask.get(ebg_id as u32)
                );
                continue;
            }
        };

        // Determine direction
        let is_forward =
            ebg_node.tail_nbg == nbg_edge.u_node && ebg_node.head_nbg == nbg_edge.v_node;
        let has_access = if is_forward {
            way_attr.output.access_fwd
        } else {
            way_attr.output.access_rev
        };

        if !has_access || way_attr.output.base_speed_mmps == 0 {
            // Should be inaccessible: weight must be 0 and mask bit must be clear
            anyhow::ensure!(
                weights.weights[ebg_id] == 0 && !mask.get(ebg_id as u32),
                "Node {} has no access but weight={} mask={}",
                ebg_id,
                weights.weights[ebg_id],
                mask.get(ebg_id as u32)
            );
            continue;
        }

        // Should be accessible - verify mask
        anyhow::ensure!(
            mask.get(ebg_id as u32),
            "Node {} has access but mask bit not set",
            ebg_id
        );

        // Recompute weight using exact formulas
        let length_mm = ebg_node.length_mm;
        let base_speed_mmps = way_attr.output.base_speed_mmps;

        let travel_time_ds = (length_mm as u64 * 10).div_ceil(base_speed_mmps as u64) as u32;
        let per_km_extra_ds = (length_mm as u64 * way_attr.output.per_km_penalty_ds as u64)
            .div_ceil(1_000_000) as u32;
        let expected_weight = travel_time_ds
            .saturating_add(per_km_extra_ds)
            .saturating_add(way_attr.output.const_penalty_ds)
            .max(1);

        anyhow::ensure!(
            weights.weights[ebg_id] == expected_weight,
            "Weight mismatch at node {}: expected {} got {} (mode={:?}, length_mm={}, speed={}, travel_time={}, per_km={}, const={})",
            ebg_id,
            expected_weight,
            weights.weights[ebg_id],
            mode,
            length_mm,
            base_speed_mmps,
            travel_time_ds,
            per_km_extra_ds,
            way_attr.output.const_penalty_ds
        );

        sampled += 1;
    }

    println!("    {:?}: verified {} node weights", mode, sampled);
    Ok(())
}

/// Lock C.7-C.8: Turn consistency checks
fn verify_lock_c_turns(
    ebg_csr: &EbgCsr,
    turn_table: &TurnTable,
    turns: &ModTurns,
    mode: Mode,
) -> Result<()> {
    let mode_bit = mode.bit();

    let n_arcs = ebg_csr.n_arcs as usize;
    let sample_size = std::cmp::min(100_000, n_arcs);

    let mut sampled = 0;
    for i in 0..sample_size {
        let arc_idx = (i * 7919) % n_arcs;
        let turn_idx = ebg_csr.turn_idx[arc_idx] as usize;
        let turn_entry = &turn_table.entries[turn_idx];

        // Check mode mask coherence
        let mode_allowed = (turn_entry.mode_mask & mode_bit) != 0;

        if !mode_allowed {
            // Mode not allowed - penalty should be 0
            anyhow::ensure!(
                turns.penalties[arc_idx] == 0,
                "Arc {} mode not allowed but penalty={}",
                arc_idx,
                turns.penalties[arc_idx]
            );
        } else {
            // Mode allowed - check penalty mapping via dynamic penalty_ds array
            let expected_penalty = turn_entry.penalty_ds[mode.index()];
            anyhow::ensure!(
                turns.penalties[arc_idx] == expected_penalty,
                "Arc {} penalty mismatch: expected {} got {}",
                arc_idx,
                expected_penalty,
                turns.penalties[arc_idx]
            );
        }

        sampled += 1;
    }

    println!("    {:?}: verified {} turn penalties", mode, sampled);
    Ok(())
}

/// Lock E.10-E.12: Sanity & bounds checks
fn verify_lock_e_bounds(
    weights: &ModWeights,
    mask: &ModMask,
    min_weight: u32,
    max_weight: u32,
    mode_name: &str,
) -> Result<()> {
    for (i, &weight) in weights.weights.iter().enumerate() {
        if mask.get(i as u32) {
            // Accessible node - must be in bounds
            anyhow::ensure!(
                weight >= min_weight && weight <= max_weight,
                "{} node {} weight {} out of bounds [{}, {}]",
                mode_name,
                i,
                weight,
                min_weight,
                max_weight
            );
        } else {
            // Inaccessible node — must be 0
            anyhow::ensure!(
                weight == 0,
                "{} node {} inaccessible but weight={}",
                mode_name,
                i,
                weight
            );
        }
    }

    println!("    {}: all weights in bounds", mode_name);
    Ok(())
}

/// Build way_id -> WayAttr index
fn build_way_index(attrs: &[WayAttr]) -> HashMap<i64, WayAttr> {
    attrs.iter().map(|a| (a.way_id, a.clone())).collect()
}
