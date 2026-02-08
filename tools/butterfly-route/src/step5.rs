//! Step 5: Per-mode weights, masks, and filtered EBGs (car | bike | foot)

use anyhow::Result;
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
    pub car_filtered_ebg: PathBuf,
    pub bike_weights: PathBuf,
    pub bike_turns: PathBuf,
    pub bike_mask: PathBuf,
    pub bike_filtered_ebg: PathBuf,
    pub foot_weights: PathBuf,
    pub foot_turns: PathBuf,
    pub foot_mask: PathBuf,
    pub foot_filtered_ebg: PathBuf,
    pub n_nodes: u32,
    pub n_arcs: u64,
}

/// Generate per-mode weights, turns, and masks
#[allow(clippy::too_many_arguments)]
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

    // Debug: check access flags in loaded way_attrs
    let car_with_access = way_attrs_car
        .iter()
        .filter(|a| a.output.access_fwd || a.output.access_rev)
        .count();
    let car_fwd_only = way_attrs_car
        .iter()
        .filter(|a| a.output.access_fwd && !a.output.access_rev)
        .count();
    let car_rev_only = way_attrs_car
        .iter()
        .filter(|a| !a.output.access_fwd && a.output.access_rev)
        .count();
    let car_both = way_attrs_car
        .iter()
        .filter(|a| a.output.access_fwd && a.output.access_rev)
        .count();
    println!("  DEBUG: Car access flags:");
    println!("    With any access: {}", car_with_access);
    println!("    Fwd only: {}", car_fwd_only);
    println!("    Rev only: {}", car_rev_only);
    println!("    Both: {}", car_both);

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
    let car_filtered_path = outdir.join("filtered.car.ebg");

    let bike_weights_path = outdir.join("w.bike.u32");
    let bike_turns_path = outdir.join("t.bike.u32");
    let bike_mask_path = outdir.join("mask.bike.bitset");
    let bike_filtered_path = outdir.join("filtered.bike.ebg");

    let foot_weights_path = outdir.join("w.foot.u32");
    let foot_turns_path = outdir.join("t.foot.u32");
    let foot_mask_path = outdir.join("mask.foot.bitset");
    let foot_filtered_path = outdir.join("filtered.foot.ebg");

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

    // Build and write filtered EBGs for each mode
    println!("\n🔍 Building per-mode filtered EBGs...");

    // Compute inputs SHA for filtered EBG (includes all inputs)
    let filtered_inputs_sha = compute_filtered_inputs_sha(
        ebg_nodes_path,
        ebg_csr_path,
        way_attrs_car_path,
        way_attrs_bike_path,
        way_attrs_foot_path,
    )?;

    // Extract mode_masks from turn_table for arc filtering
    // This is CRITICAL for enforcing turn restrictions!
    let arc_mode_masks: Vec<u8> = turn_table.entries.iter().map(|e| e.mode_mask).collect();
    println!(
        "  Extracted {} turn entry mode masks for arc filtering",
        arc_mode_masks.len()
    );

    // Car filtered EBG
    println!("  Building car filtered EBG...");
    let car_filtered = FilteredEbg::build_with_arc_filter(
        Mode::Car,
        &ebg_csr.offsets,
        &ebg_csr.heads,
        &car_mask.mask,
        Some(&ebg_csr.turn_idx),
        Some(&arc_mode_masks),
        ebg_nodes.n_nodes,
        filtered_inputs_sha,
    );
    println!(
        "    ✓ {} nodes (of {}), {} arcs",
        car_filtered.n_filtered_nodes, car_filtered.n_original_nodes, car_filtered.n_filtered_arcs
    );
    FilteredEbgFile::write(&car_filtered_path, &car_filtered)?;

    // Bike filtered EBG
    println!("  Building bike filtered EBG...");
    let bike_filtered = FilteredEbg::build_with_arc_filter(
        Mode::Bike,
        &ebg_csr.offsets,
        &ebg_csr.heads,
        &bike_mask.mask,
        Some(&ebg_csr.turn_idx),
        Some(&arc_mode_masks),
        ebg_nodes.n_nodes,
        filtered_inputs_sha,
    );
    println!(
        "    ✓ {} nodes (of {}), {} arcs",
        bike_filtered.n_filtered_nodes,
        bike_filtered.n_original_nodes,
        bike_filtered.n_filtered_arcs
    );
    FilteredEbgFile::write(&bike_filtered_path, &bike_filtered)?;

    // Foot filtered EBG
    println!("  Building foot filtered EBG...");
    let foot_filtered = FilteredEbg::build_with_arc_filter(
        Mode::Foot,
        &ebg_csr.offsets,
        &ebg_csr.heads,
        &foot_mask.mask,
        Some(&ebg_csr.turn_idx),
        Some(&arc_mode_masks),
        ebg_nodes.n_nodes,
        filtered_inputs_sha,
    );
    println!(
        "    ✓ {} nodes (of {}), {} arcs",
        foot_filtered.n_filtered_nodes,
        foot_filtered.n_original_nodes,
        foot_filtered.n_filtered_arcs
    );
    FilteredEbgFile::write(&foot_filtered_path, &foot_filtered)?;

    println!("  ✓ Written 3 filtered EBG files");

    Ok(Step5Result {
        car_weights: car_weights_path,
        car_turns: car_turns_path,
        car_mask: car_mask_path,
        car_filtered_ebg: car_filtered_path,
        bike_weights: bike_weights_path,
        bike_turns: bike_turns_path,
        bike_mask: bike_mask_path,
        bike_filtered_ebg: bike_filtered_path,
        foot_weights: foot_weights_path,
        foot_turns: foot_turns_path,
        foot_mask: foot_mask_path,
        foot_filtered_ebg: foot_filtered_path,
        n_nodes: ebg_nodes.n_nodes,
        n_arcs: ebg_csr.n_arcs,
    })
}

/// Build way_id → WayAttr index
fn build_way_index(attrs: &[WayAttr]) -> HashMap<i64, WayAttr> {
    attrs.iter().map(|a| (a.way_id, a.clone())).collect()
}

/// Generate weights, turns, and mask for a single mode
#[allow(clippy::too_many_arguments)]
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

    // Debug counters
    let mut dbg_no_way = 0usize;
    let mut dbg_no_access = 0usize;
    let mut dbg_zero_speed = 0usize;
    let mut dbg_accessible = 0usize;

    // Compute node weights and accessibility mask
    for (ebg_id, ebg_node) in ebg_nodes.nodes.iter().enumerate() {
        let nbg_edge = &nbg_geo.edges[ebg_node.geom_idx as usize];
        let way_id = nbg_edge.first_osm_way_id;

        // Look up way attributes
        let way_attr = match way_index.get(&way_id) {
            Some(attr) => attr,
            None => {
                // Way not in this mode's index - inaccessible
                dbg_no_way += 1;
                weights[ebg_id] = 0;
                continue;
            }
        };

        // Determine direction: forward or reverse
        let is_forward =
            ebg_node.tail_nbg == nbg_edge.u_node && ebg_node.head_nbg == nbg_edge.v_node;

        // Get access for this direction
        let has_access = if is_forward {
            way_attr.output.access_fwd
        } else {
            way_attr.output.access_rev
        };

        if !has_access {
            // Not accessible in this direction
            dbg_no_access += 1;
            weights[ebg_id] = 0;
            continue;
        }

        // Compute weight
        let length_mm = ebg_node.length_mm;
        let base_speed_mmps = way_attr.output.base_speed_mmps;

        if base_speed_mmps == 0 {
            // Inaccessible (zero speed) — do NOT set mask bit
            dbg_zero_speed += 1;
            weights[ebg_id] = 0;
            continue;
        }

        // Node is accessible - set mask bit (after zero-speed check)
        mask.set(ebg_id as u32);

        dbg_accessible += 1;

        // Compute travel_time_ds using integer math (ceiling division)
        // travel_time_ds = ceil(length_mm / base_speed_mmps * 10)
        //                = (length_mm * 10 + base_speed_mmps - 1) / base_speed_mmps
        let travel_time_ds = (length_mm as u64 * 10).div_ceil(base_speed_mmps as u64) as u32;

        // Compute per_km_extra_ds using integer math
        // per_km_extra_ds = ceil(length_km * per_km_penalty_ds)
        //                 = (length_mm * per_km_penalty_ds + 1_000_000 - 1) / 1_000_000
        let per_km_extra_ds = (length_mm as u64 * way_attr.output.per_km_penalty_ds as u64)
            .div_ceil(1_000_000) as u32;

        // Total weight = travel_time + per_km_extra + const_penalty (saturating)
        let weight_ds = travel_time_ds
            .saturating_add(per_km_extra_ds)
            .saturating_add(way_attr.output.const_penalty_ds);

        // Enforce minimum weight of 1 for accessible nodes
        weights[ebg_id] = weight_ds.max(1);
    }

    // Compute turn penalties
    // Turn penalties are now geometry-based (computed in step4 EBG construction)
    // Every turn has a penalty stored in the turn table based on:
    // - Turn angle (straight/right/left/u-turn)
    // - Intersection complexity (degree)
    // - Traffic signals (future: not yet implemented)
    let mut total_penalty_ds = 0u64;
    let mut arcs_with_penalty = 0usize;

    for (arc_idx, penalty) in penalties.iter_mut().enumerate() {
        let turn_idx = ebg_csr.turn_idx[arc_idx] as usize;
        let turn_entry = &turn_table.entries[turn_idx];

        // Check if this mode is allowed on this arc
        if (turn_entry.mode_mask & mode_bit) == 0 {
            // Mode not allowed - penalty is 0 (traversal forbidden by mask)
            *penalty = 0;
            continue;
        }

        // Extract per-mode penalty (geometry-based + explicit from OSM rules)
        let penalty_ds = match mode {
            Mode::Car => turn_entry.penalty_ds_car,
            Mode::Bike => turn_entry.penalty_ds_bike,
            Mode::Foot => turn_entry.penalty_ds_foot,
        };
        *penalty = penalty_ds;

        if penalty_ds > 0 {
            total_penalty_ds += penalty_ds as u64;
            arcs_with_penalty += 1;
        }
    }

    // Print turn penalty statistics
    if arcs_with_penalty > 0 {
        println!(
            "  Turn penalties: {} arcs ({:.1}%), avg {:.1}s",
            arcs_with_penalty,
            arcs_with_penalty as f64 * 100.0 / n_arcs as f64,
            total_penalty_ds as f64 / arcs_with_penalty as f64 / 10.0
        );
    }

    // Debug output
    let mode_name = match mode {
        Mode::Car => "car",
        Mode::Bike => "bike",
        Mode::Foot => "foot",
    };
    println!(
        "  DEBUG {}: no_way={}, no_access={}, zero_speed={}, accessible={} ({:.1}%)",
        mode_name,
        dbg_no_way,
        dbg_no_access,
        dbg_zero_speed,
        dbg_accessible,
        dbg_accessible as f64 * 100.0 / n_nodes as f64
    );

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

/// Compute SHA256 hash of inputs for filtered EBG
fn compute_filtered_inputs_sha(
    ebg_nodes_path: &Path,
    ebg_csr_path: &Path,
    way_attrs_car_path: &Path,
    way_attrs_bike_path: &Path,
    way_attrs_foot_path: &Path,
) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(&std::fs::read(ebg_nodes_path)?);
    hasher.update(&std::fs::read(ebg_csr_path)?);
    hasher.update(&std::fs::read(way_attrs_car_path)?);
    hasher.update(&std::fs::read(way_attrs_bike_path)?);
    hasher.update(&std::fs::read(way_attrs_foot_path)?);

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}
