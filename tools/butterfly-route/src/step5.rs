///! Step 5: Per-mode weights & masks (car | bike | foot)

use anyhow::{Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::formats::*;
use crate::profile_abi::Mode;

/// Result of Step 5 weight generation
#[derive(Debug)]
pub struct Step5Result {
    pub car_weights: PathBuf,
    pub car_turns: PathBuf,
    pub car_mask: PathBuf,
    pub bike_weights: PathBuf,
    pub bike_turns: PathBuf,
    pub bike_mask: PathBuf,
    pub foot_weights: PathBuf,
    pub foot_turns: PathBuf,
    pub foot_mask: PathBuf,
    pub n_nodes: u32,
    pub n_arcs: u64,
}

/// Generate per-mode weights, turns, and masks
pub fn generate_weights(
    ebg_nodes_path: &Path,
    ebg_csr_path: &Path,
    turn_table_path: &Path,
    nbg_geo_path: &Path,
    way_attrs_car_path: &Path,
    way_attrs_bike_path: &Path,
    way_attrs_foot_path: &Path,
    outdir: &Path,
) -> Result<Step5Result> {
    println!("\n🏋️  Step 5: Generating per-mode weights & masks...\n");

    // Load EBG
    println!("Loading EBG nodes...");
    let ebg_nodes = EbgNodesFile::read(ebg_nodes_path)?;
    println!("  ✓ {} nodes", ebg_nodes.n_nodes);

    println!("Loading EBG CSR...");
    let ebg_csr = EbgCsrFile::read(ebg_csr_path)?;
    println!("  ✓ {} arcs", ebg_csr.n_arcs);

    println!("Loading turn table...");
    let turn_table = TurnTableFile::read(turn_table_path)?;
    println!("  ✓ {} turn entries", turn_table.entries.len());

    println!("Loading NBG geo...");
    let nbg_geo = NbgGeoFile::read(nbg_geo_path)?;
    println!("  ✓ {} edges", nbg_geo.edges.len());

    // Load way_attrs for all modes
    println!("\nLoading way attributes...");
    let way_attrs_car = way_attrs::read_all(way_attrs_car_path)?;
    let way_attrs_bike = way_attrs::read_all(way_attrs_bike_path)?;
    let way_attrs_foot = way_attrs::read_all(way_attrs_foot_path)?;
    println!("  ✓ car: {} ways", way_attrs_car.len());
    println!("  ✓ bike: {} ways", way_attrs_bike.len());
    println!("  ✓ foot: {} ways", way_attrs_foot.len());

    // Build way_id → WayAttr indices for fast lookup
    let car_index = build_way_index(&way_attrs_car);
    let bike_index = build_way_index(&way_attrs_bike);
    let foot_index = build_way_index(&way_attrs_foot);

    // Calculate inputs hash (simplified - just use first 16 bytes of ebg inputs_sha)
    let mut inputs_sha = [0u8; 16];
    inputs_sha.copy_from_slice(&ebg_nodes.inputs_sha[0..16]);
    let mut inputs_sha_8 = [0u8; 8];
    inputs_sha_8.copy_from_slice(&ebg_nodes.inputs_sha[0..8]);

    // Generate weights, turns, and masks for each mode
    println!("\n🚗 Generating car weights...");
    let (car_weights, car_turns, car_mask) = generate_mode_data(
        Mode::Car,
        &ebg_nodes,
        &ebg_csr,
        &turn_table,
        &nbg_geo,
        &car_index,
        inputs_sha,
        inputs_sha_8,
    )?;

    println!("🚴 Generating bike weights...");
    let (bike_weights, bike_turns, bike_mask) = generate_mode_data(
        Mode::Bike,
        &ebg_nodes,
        &ebg_csr,
        &turn_table,
        &nbg_geo,
        &bike_index,
        inputs_sha,
        inputs_sha_8,
    )?;

    println!("🚶 Generating foot weights...");
    let (foot_weights, foot_turns, foot_mask) = generate_mode_data(
        Mode::Foot,
        &ebg_nodes,
        &ebg_csr,
        &turn_table,
        &nbg_geo,
        &foot_index,
        inputs_sha,
        inputs_sha_8,
    )?;

    // Write to files
    std::fs::create_dir_all(outdir)?;

    let car_weights_path = outdir.join("w.car.u32");
    let car_turns_path = outdir.join("t.car.u32");
    let car_mask_path = outdir.join("mask.car.bitset");

    let bike_weights_path = outdir.join("w.bike.u32");
    let bike_turns_path = outdir.join("t.bike.u32");
    let bike_mask_path = outdir.join("mask.bike.bitset");

    let foot_weights_path = outdir.join("w.foot.u32");
    let foot_turns_path = outdir.join("t.foot.u32");
    let foot_mask_path = outdir.join("mask.foot.bitset");

    println!("\nWriting output files...");
    mod_weights::write(&car_weights_path, &car_weights)?;
    mod_turns::write(&car_turns_path, &car_turns)?;
    mod_mask::write(&car_mask_path, &car_mask)?;

    mod_weights::write(&bike_weights_path, &bike_weights)?;
    mod_turns::write(&bike_turns_path, &bike_turns)?;
    mod_mask::write(&bike_mask_path, &bike_mask)?;

    mod_weights::write(&foot_weights_path, &foot_weights)?;
    mod_turns::write(&foot_turns_path, &foot_turns)?;
    mod_mask::write(&foot_mask_path, &foot_mask)?;

    println!("  ✓ Written 9 files (3 modes × 3 types)");

    Ok(Step5Result {
        car_weights: car_weights_path,
        car_turns: car_turns_path,
        car_mask: car_mask_path,
        bike_weights: bike_weights_path,
        bike_turns: bike_turns_path,
        bike_mask: bike_mask_path,
        foot_weights: foot_weights_path,
        foot_turns: foot_turns_path,
        foot_mask: foot_mask_path,
        n_nodes: ebg_nodes.n_nodes,
        n_arcs: ebg_csr.n_arcs,
    })
}

/// Build way_id → WayAttr index
fn build_way_index(attrs: &[WayAttr]) -> HashMap<i64, WayAttr> {
    attrs.iter().map(|a| (a.way_id, a.clone())).collect()
}

/// Generate weights, turns, and mask for a single mode
fn generate_mode_data(
    mode: Mode,
    ebg_nodes: &EbgNodes,
    ebg_csr: &EbgCsr,
    turn_table: &TurnTable,
    nbg_geo: &NbgGeo,
    way_index: &HashMap<i64, WayAttr>,
    inputs_sha: [u8; 16],
    inputs_sha_8: [u8; 8],
) -> Result<(ModWeights, ModTurns, ModMask)> {
    let mode_bit = match mode {
        Mode::Car => 1u8 << 0,
        Mode::Bike => 1u8 << 1,
        Mode::Foot => 1u8 << 2,
    };

    let n_nodes = ebg_nodes.n_nodes;
    let n_arcs = ebg_csr.n_arcs as usize;

    let mut weights = vec![0u32; n_nodes as usize];
    let mut penalties = vec![0u32; n_arcs];
    let mut mask = ModMask::new(mode, n_nodes, inputs_sha_8);

    // Compute node weights and accessibility mask
    for (ebg_id, ebg_node) in ebg_nodes.nodes.iter().enumerate() {
        let nbg_edge = &nbg_geo.edges[ebg_node.geom_idx as usize];
        let way_id = nbg_edge.first_osm_way_id;

        // Look up way attributes
        let way_attr = match way_index.get(&way_id) {
            Some(attr) => attr,
            None => {
                // Way not in this mode's index - inaccessible
                weights[ebg_id] = 0;
                continue;
            }
        };

        // Determine direction: forward or reverse
        let is_forward = ebg_node.tail_nbg == nbg_edge.u_node && ebg_node.head_nbg == nbg_edge.v_node;

        // Get access for this direction
        let has_access = if is_forward {
            way_attr.output.access_fwd
        } else {
            way_attr.output.access_rev
        };

        if !has_access {
            // Not accessible in this direction
            weights[ebg_id] = 0;
            continue;
        }

        // Node is accessible - set mask bit
        mask.set(ebg_id as u32);

        // Compute weight
        let length_mm = ebg_node.length_mm;
        let base_speed_mmps = way_attr.output.base_speed_mmps;

        if base_speed_mmps == 0 {
            // Inaccessible (zero speed)
            weights[ebg_id] = 0;
            continue;
        }

        // Compute travel_time_ds using integer math (ceiling division)
        // travel_time_ds = ceil(length_mm / base_speed_mmps * 10)
        //                = (length_mm * 10 + base_speed_mmps - 1) / base_speed_mmps
        let travel_time_ds = ((length_mm as u64 * 10 + base_speed_mmps as u64 - 1) / base_speed_mmps as u64) as u32;

        // Compute per_km_extra_ds using integer math
        // per_km_extra_ds = ceil(length_km * per_km_penalty_ds)
        //                 = (length_mm * per_km_penalty_ds + 1_000_000 - 1) / 1_000_000
        let per_km_extra_ds = ((length_mm as u64 * way_attr.output.per_km_penalty_ds as u64 + 1_000_000 - 1) / 1_000_000) as u32;

        // Total weight = travel_time + per_km_extra + const_penalty (saturating)
        let weight_ds = travel_time_ds
            .saturating_add(per_km_extra_ds)
            .saturating_add(way_attr.output.const_penalty_ds);

        // Enforce minimum weight of 1 for accessible nodes
        weights[ebg_id] = weight_ds.max(1);
    }

    // Compute turn penalties
    for arc_idx in 0..n_arcs {
        let turn_idx = ebg_csr.turn_idx[arc_idx] as usize;
        let turn_entry = &turn_table.entries[turn_idx];

        // Check if this mode is allowed on this arc
        if (turn_entry.mode_mask & mode_bit) == 0 {
            // Mode not allowed - penalty is 0 (traversal forbidden by mask)
            penalties[arc_idx] = 0;
            continue;
        }

        // Check if this is a Penalty turn
        if turn_entry.kind == TurnKind::Penalty {
            // Extract per-mode penalty
            let penalty_ds = match mode {
                Mode::Car => turn_entry.penalty_ds_car,
                Mode::Bike => turn_entry.penalty_ds_bike,
                Mode::Foot => turn_entry.penalty_ds_foot,
            };
            penalties[arc_idx] = penalty_ds;
        } else {
            // Ban or Only (allowed) - no penalty (ban enforced by bitmask)
            penalties[arc_idx] = 0;
        }
    }

    let weights_data = ModWeights {
        mode,
        weights,
        inputs_sha,
    };

    let turns_data = ModTurns {
        mode,
        penalties,
        inputs_sha,
    };

    Ok((weights_data, turns_data, mask))
}
