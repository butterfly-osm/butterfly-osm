//! Step 5 validation - Per-mode weights lock conditions

use anyhow::Result;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

use crate::formats::*;
use crate::profile_abi::Mode;
use crate::step5;

#[derive(Debug, Serialize, Deserialize)]
pub struct Step5LockFile {
    pub inputs_sha256: String,
    pub car: ModeLockData,
    pub bike: ModeLockData,
    pub foot: ModeLockData,
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

/// Validate Step 5 outputs and generate lock file
#[allow(clippy::too_many_arguments)]
pub fn validate_step5(
    result: &step5::Step5Result,
    ebg_nodes_path: &Path,
    ebg_csr_path: &Path,
    turn_table_path: &Path,
    nbg_geo_path: &Path,
    way_attrs_car_path: &Path,
    way_attrs_bike_path: &Path,
    way_attrs_foot_path: &Path,
) -> Result<Step5LockFile> {
    println!("\n🔐 Running Step 5 validation lock conditions...\n");

    // Load all data
    let ebg_nodes = EbgNodesFile::read(ebg_nodes_path)?;
    let ebg_csr = EbgCsrFile::read(ebg_csr_path)?;
    let turn_table = TurnTableFile::read(turn_table_path)?;
    let nbg_geo = NbgGeoFile::read(nbg_geo_path)?;

    let way_attrs_car = way_attrs::read_all(way_attrs_car_path)?;
    let way_attrs_bike = way_attrs::read_all(way_attrs_bike_path)?;
    let way_attrs_foot = way_attrs::read_all(way_attrs_foot_path)?;

    // Load generated weights/turns/masks
    let car_weights = mod_weights::read_all(&result.car_weights)?;
    let car_turns = mod_turns::read_all(&result.car_turns)?;
    let car_mask = mod_mask::read_all(&result.car_mask)?;

    let bike_weights = mod_weights::read_all(&result.bike_weights)?;
    let bike_turns = mod_turns::read_all(&result.bike_turns)?;
    let bike_mask = mod_mask::read_all(&result.bike_mask)?;

    let foot_weights = mod_weights::read_all(&result.foot_weights)?;
    let foot_turns = mod_turns::read_all(&result.foot_turns)?;
    let foot_mask = mod_mask::read_all(&result.foot_mask)?;

    // Lock Condition A: Structural integrity
    println!("A. Structural integrity checks...");
    let t0 = std::time::Instant::now();
    verify_lock_a_structural(&ebg_nodes, &ebg_csr, &car_weights, &car_turns, &car_mask)?;
    verify_lock_a_structural(&ebg_nodes, &ebg_csr, &bike_weights, &bike_turns, &bike_mask)?;
    verify_lock_a_structural(&ebg_nodes, &ebg_csr, &foot_weights, &foot_turns, &foot_mask)?;
    println!(
        "  ✓ Passed structural checks ({:.3}s)",
        t0.elapsed().as_secs_f64()
    );

    // Lock Condition B: Math parity
    println!("\nB. Math parity checks...");
    let t0 = std::time::Instant::now();
    let car_index = build_way_index(&way_attrs_car);
    let bike_index = build_way_index(&way_attrs_bike);
    let foot_index = build_way_index(&way_attrs_foot);

    verify_lock_b_math(
        &ebg_nodes,
        &nbg_geo,
        &car_weights,
        &car_mask,
        &car_index,
        Mode::Car,
    )?;
    verify_lock_b_math(
        &ebg_nodes,
        &nbg_geo,
        &bike_weights,
        &bike_mask,
        &bike_index,
        Mode::Bike,
    )?;
    verify_lock_b_math(
        &ebg_nodes,
        &nbg_geo,
        &foot_weights,
        &foot_mask,
        &foot_index,
        Mode::Foot,
    )?;
    println!(
        "  ✓ Passed math parity checks ({:.3}s)",
        t0.elapsed().as_secs_f64()
    );

    // Lock Condition C: Arc/turn consistency
    println!("\nC. Arc/turn consistency checks...");
    let t0 = std::time::Instant::now();
    verify_lock_c_turns(&ebg_csr, &turn_table, &car_turns, Mode::Car)?;
    verify_lock_c_turns(&ebg_csr, &turn_table, &bike_turns, Mode::Bike)?;
    verify_lock_c_turns(&ebg_csr, &turn_table, &foot_turns, Mode::Foot)?;
    println!(
        "  ✓ Passed turn consistency checks ({:.3}s)",
        t0.elapsed().as_secs_f64()
    );

    // Lock Condition D: Graph-level parity (SKIPPED - too expensive for now)
    println!("\nD. Graph-level parity (Dijkstra reachability)...");
    println!("  ⚠️  SKIPPED (too expensive, implement later if needed)");

    // Lock Condition E: Sanity & bounds
    println!("\nE. Sanity & bounds checks...");
    let t0 = std::time::Instant::now();
    verify_lock_e_bounds(&car_weights, &car_mask, 1, 10_000_000, "car")?;
    verify_lock_e_bounds(&bike_weights, &bike_mask, 1, 5_000_000, "bike")?;
    verify_lock_e_bounds(&foot_weights, &foot_mask, 1, 5_000_000, "foot")?;
    println!(
        "  ✓ Passed sanity & bounds checks ({:.3}s)",
        t0.elapsed().as_secs_f64()
    );

    // Calculate SHA-256 hashes for lock file
    let car_lock = ModeLockData {
        w_sha256: "TODO".to_string(),
        t_sha256: "TODO".to_string(),
        mask_sha256: "TODO".to_string(),
    };
    let bike_lock = ModeLockData {
        w_sha256: "TODO".to_string(),
        t_sha256: "TODO".to_string(),
        mask_sha256: "TODO".to_string(),
    };
    let foot_lock = ModeLockData {
        w_sha256: "TODO".to_string(),
        t_sha256: "TODO".to_string(),
        mask_sha256: "TODO".to_string(),
    };

    Ok(Step5LockFile {
        inputs_sha256: hex::encode(ebg_nodes.inputs_sha),
        car: car_lock,
        bike: bike_lock,
        foot: foot_lock,
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
            // Should be inaccessible
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
    let mode_bit = match mode {
        Mode::Car => 1u8 << 0,
        Mode::Bike => 1u8 << 1,
        Mode::Foot => 1u8 << 2,
    };

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
            // Mode allowed - check penalty mapping
            // All turns now have geometry-based penalties stored in the turn table
            // regardless of kind (None, Ban, Only, or Penalty)
            let expected_penalty = match mode {
                Mode::Car => turn_entry.penalty_ds_car,
                Mode::Bike => turn_entry.penalty_ds_bike,
                Mode::Foot => turn_entry.penalty_ds_foot,
            };
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
            // Inaccessible node - must be 0
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

/// Build way_id → WayAttr index
fn build_way_index(attrs: &[WayAttr]) -> HashMap<i64, WayAttr> {
    attrs.iter().map(|a| (a.way_id, a.clone())).collect()
}
