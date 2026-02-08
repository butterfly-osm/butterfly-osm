//! Step 5: Per-mode weights, masks, and filtered EBGs (dynamic modes)

use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::formats::*;
use crate::profile_abi::Mode;

/// Input descriptor for a single mode to be processed by Step 5.
#[derive(Debug, Clone)]
pub struct Step5ModeInput {
    pub mode_name: String,
    pub mode_index: u8,
    pub way_attrs_path: PathBuf,
}

/// Output paths and metadata for a single mode produced by Step 5.
#[derive(Debug)]
pub struct ModeStep5Output {
    pub mode_name: String,
    pub mode_index: u8,
    pub weights_path: PathBuf,
    pub turns_path: PathBuf,
    pub mask_path: PathBuf,
    pub filtered_ebg_path: PathBuf,
}

/// Result of Step 5 weight generation (dynamic: one entry per mode).
#[derive(Debug)]
pub struct Step5Result {
    pub modes: Vec<ModeStep5Output>,
    pub n_nodes: u32,
    pub n_arcs: u64,
}

/// Generate per-mode weights, turns, and masks for all provided modes.
pub fn generate_weights(
    ebg_nodes_path: &Path,
    ebg_csr_path: &Path,
    turn_table_path: &Path,
    nbg_geo_path: &Path,
    mode_inputs: &[Step5ModeInput],
    outdir: &Path,
) -> Result<Step5Result> {
    println!("\n  Step 5: Generating per-mode weights & masks...\n");

    anyhow::ensure!(
        !mode_inputs.is_empty(),
        "Step 5 requires at least one mode input"
    );

    // Load EBG
    println!("Loading EBG nodes...");
    let ebg_nodes = EbgNodesFile::read(ebg_nodes_path)?;
    println!("  {} nodes", ebg_nodes.n_nodes);

    println!("Loading EBG CSR...");
    let ebg_csr = EbgCsrFile::read(ebg_csr_path)?;
    println!("  {} arcs", ebg_csr.n_arcs);

    println!("Loading turn table...");
    let turn_table = TurnTableFile::read(turn_table_path)?;
    println!("  {} turn entries", turn_table.entries.len());

    println!("Loading NBG geo...");
    let nbg_geo = NbgGeoFile::read(nbg_geo_path)?;
    println!("  {} edges", nbg_geo.edges.len());

    // Calculate inputs hash (simplified - use first bytes of ebg inputs_sha)
    let mut inputs_sha = [0u8; 16];
    inputs_sha.copy_from_slice(&ebg_nodes.inputs_sha[0..16]);
    let mut inputs_sha_8 = [0u8; 8];
    inputs_sha_8.copy_from_slice(&ebg_nodes.inputs_sha[0..8]);

    std::fs::create_dir_all(outdir)?;

    // Extract mode_masks from turn_table for arc filtering
    // This is CRITICAL for enforcing turn restrictions!
    let arc_mode_masks: Vec<u8> = turn_table.entries.iter().map(|e| e.mode_mask).collect();

    // Compute inputs SHA for filtered EBG (includes all inputs)
    let way_attrs_paths: Vec<&Path> = mode_inputs
        .iter()
        .map(|m| m.way_attrs_path.as_path())
        .collect();
    let filtered_inputs_sha =
        compute_filtered_inputs_sha(ebg_nodes_path, ebg_csr_path, &way_attrs_paths)?;

    let mut mode_outputs = Vec::with_capacity(mode_inputs.len());

    for mode_input in mode_inputs {
        let mode_name = &mode_input.mode_name;
        let mode_index = mode_input.mode_index;
        let mode = Mode(mode_index);

        // Load way attributes for this mode
        println!("\nLoading way attributes for '{}'...", mode_name);
        let way_attrs = way_attrs::read_all(&mode_input.way_attrs_path)?;
        println!("  {} ways", way_attrs.len());

        // Debug: check access flags
        let with_access = way_attrs
            .iter()
            .filter(|a| a.output.access_fwd || a.output.access_rev)
            .count();
        let fwd_only = way_attrs
            .iter()
            .filter(|a| a.output.access_fwd && !a.output.access_rev)
            .count();
        let rev_only = way_attrs
            .iter()
            .filter(|a| !a.output.access_fwd && a.output.access_rev)
            .count();
        let both = way_attrs
            .iter()
            .filter(|a| a.output.access_fwd && a.output.access_rev)
            .count();
        println!("  DEBUG: {} access flags:", mode_name);
        println!("    With any access: {}", with_access);
        println!("    Fwd only: {}", fwd_only);
        println!("    Rev only: {}", rev_only);
        println!("    Both: {}", both);

        // Build way_id index
        let way_index = build_way_index(&way_attrs);

        // Generate weights, turns, and mask
        println!("Generating {} weights...", mode_name);
        let (weights_data, turns_data, mask_data) = generate_mode_data(
            mode,
            mode_name,
            &ebg_nodes,
            &ebg_csr,
            &turn_table,
            &nbg_geo,
            &way_index,
            inputs_sha,
            inputs_sha_8,
        )?;

        // Write output files
        let weights_path = outdir.join(format!("w.{}.u32", mode_name));
        let turns_path = outdir.join(format!("t.{}.u32", mode_name));
        let mask_path = outdir.join(format!("mask.{}.bitset", mode_name));
        let filtered_path = outdir.join(format!("filtered.{}.ebg", mode_name));

        println!("Writing {} output files...", mode_name);
        mod_weights::write(&weights_path, &weights_data)?;
        mod_turns::write(&turns_path, &turns_data)?;
        mod_mask::write(&mask_path, &mask_data)?;
        println!("  Written 3 files for '{}'", mode_name);

        // Build and write filtered EBG
        println!("Building {} filtered EBG...", mode_name);
        let filtered = FilteredEbg::build_with_arc_filter(
            mode,
            &ebg_csr.offsets,
            &ebg_csr.heads,
            &mask_data.mask,
            Some(&ebg_csr.turn_idx),
            Some(&arc_mode_masks),
            ebg_nodes.n_nodes,
            filtered_inputs_sha,
        );
        println!(
            "    {} nodes (of {}), {} arcs",
            filtered.n_filtered_nodes, filtered.n_original_nodes, filtered.n_filtered_arcs
        );
        FilteredEbgFile::write(&filtered_path, &filtered)?;

        mode_outputs.push(ModeStep5Output {
            mode_name: mode_name.clone(),
            mode_index,
            weights_path,
            turns_path,
            mask_path,
            filtered_ebg_path: filtered_path,
        });
    }

    println!(
        "\n  Written {} modes x 4 files = {} files total",
        mode_outputs.len(),
        mode_outputs.len() * 4
    );

    Ok(Step5Result {
        modes: mode_outputs,
        n_nodes: ebg_nodes.n_nodes,
        n_arcs: ebg_csr.n_arcs,
    })
}

/// Build way_id -> WayAttr index
fn build_way_index(attrs: &[WayAttr]) -> HashMap<i64, WayAttr> {
    attrs.iter().map(|a| (a.way_id, a.clone())).collect()
}

/// Generate weights, turns, and mask for a single mode
#[allow(clippy::too_many_arguments)]
fn generate_mode_data(
    mode: Mode,
    mode_name: &str,
    ebg_nodes: &EbgNodes,
    ebg_csr: &EbgCsr,
    turn_table: &TurnTable,
    nbg_geo: &NbgGeo,
    way_index: &HashMap<i64, WayAttr>,
    inputs_sha: [u8; 16],
    inputs_sha_8: [u8; 8],
) -> Result<(ModWeights, ModTurns, ModMask)> {
    let mode_bit = mode.bit();

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
            // Inaccessible (zero speed) -- do NOT set mask bit
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

        // Extract per-mode penalty from the dynamic array indexed by mode index
        let penalty_ds = turn_entry.penalty_ds[mode.index()];
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
    way_attrs_paths: &[&Path],
) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();
    hasher.update(&std::fs::read(ebg_nodes_path)?);
    hasher.update(&std::fs::read(ebg_csr_path)?);
    for path in way_attrs_paths {
        hasher.update(&std::fs::read(path)?);
    }

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}
