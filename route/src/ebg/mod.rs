//! Edge-Based Graph (EBG) construction - Step 4
//!
//! Builds a turn-expanded graph where:
//! - Nodes = directed NBG edges (u→v)
//! - Arcs = legal turn transitions at intersections
//! - Mode specificity encoded as bitmasks

use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::formats::*;
use crate::profile_abi::{MAX_MODES, Mode};

pub mod turn_penalty;
pub mod turn_processor;

use turn_penalty::{TurnGeometry, TurnPenaltyConfig, compute_turn_penalty};

/// Per-mode input paths for EBG construction
#[derive(Debug, Clone)]
pub struct EbgModeConfig {
    pub mode_name: String,
    pub mode_index: u8,
    pub way_attrs_path: PathBuf,
    pub turn_rules_path: PathBuf,
}

#[derive(Debug)]
pub struct EbgConfig {
    pub nbg_csr_path: PathBuf,
    pub nbg_geo_path: PathBuf,
    pub nbg_node_map_path: PathBuf,
    pub node_signals_path: PathBuf,
    pub modes: Vec<EbgModeConfig>,
    pub outdir: PathBuf,
}

#[derive(Debug)]
pub struct EbgResult {
    pub nodes_path: PathBuf,
    pub csr_path: PathBuf,
    pub turn_table_path: PathBuf,
    pub n_nodes: u32,
    pub n_arcs: u64,
    pub build_time_ms: u64,
}

/// Canonical turn rule key
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct TurnRuleKey {
    pub via_node_osm: i64,
    pub from_way_id: i64,
    pub to_way_id: i64,
}

/// Canonical turn rule with dynamic per-mode penalties
#[derive(Debug, Clone)]
pub struct CanonicalTurnRule {
    pub mode_mask: u8, // Which modes this rule applies to
    pub kind: TurnKind,
    pub penalty_ds: [u32; MAX_MODES], // Indexed by mode index
    pub has_time_dep: bool,
}

pub fn build_ebg(config: EbgConfig) -> Result<EbgResult> {
    use std::time::Instant;
    let start_time = Instant::now();

    println!("🦋 Starting Step 4: Edge-Based Graph Construction");
    println!("📂 NBG CSR: {}", config.nbg_csr_path.display());
    println!("📂 NBG Geo: {}", config.nbg_geo_path.display());
    println!("📂 Output: {}", config.outdir.display());
    println!(
        "  Modes: {}",
        config
            .modes
            .iter()
            .map(|m| m.mode_name.as_str())
            .collect::<Vec<_>>()
            .join(", ")
    );
    println!();

    // 1. Load NBG data
    println!("Loading NBG files...");
    let nbg_csr = NbgCsrFile::read(&config.nbg_csr_path)?;
    let nbg_geo = NbgGeoFile::read(&config.nbg_geo_path)?;
    let nbg_node_map = NbgNodeMapFile::read_map(&config.nbg_node_map_path)?;
    println!(
        "  ✓ NBG loaded: {} nodes, {} edges",
        nbg_csr.n_nodes, nbg_geo.n_edges_und
    );

    // 1b. Load traffic signal nodes
    let node_signals = if config.node_signals_path.exists() {
        let signals = NodeSignalsFile::read(&config.node_signals_path)?;
        println!("  ✓ Loaded {} traffic signal nodes", signals.len());
        signals
    } else {
        println!("  ⚠ No node_signals.bin found, traffic signals disabled");
        NodeSignals::new(vec![])
    };

    // 2. Load way attributes per mode (dynamic list)
    println!("Loading way attributes...");
    let mut way_attrs_by_mode: Vec<HashMap<i64, WayAttr>> = Vec::with_capacity(MAX_MODES);
    // Pre-fill with empty maps for all slots
    for _ in 0..MAX_MODES {
        way_attrs_by_mode.push(HashMap::new());
    }
    for mc in &config.modes {
        let attrs = load_way_attrs(&mc.way_attrs_path)?;
        println!("  ✓ {}: {} ways", mc.mode_name, attrs.len());
        way_attrs_by_mode[mc.mode_index as usize] = attrs;
    }

    // Compute active mode mask (which bits are present)
    let active_mode_mask: u8 = config
        .modes
        .iter()
        .fold(0u8, |acc, mc| acc | Mode(mc.mode_index).bit());

    // 3. Load and process turn rules (dynamic)
    println!("Processing turn rules...");
    let mode_turn_inputs: Vec<(u8, &Path)> = config
        .modes
        .iter()
        .map(|mc| (mc.mode_index, mc.turn_rules_path.as_path()))
        .collect();
    let canonical_rules = turn_processor::build_canonical_turn_rules(
        &mode_turn_inputs,
        &nbg_csr,
        &nbg_geo,
        &nbg_node_map,
    )?;
    println!(
        "  ✓ Processed {} canonical turn rules",
        canonical_rules.len()
    );

    // 4. Enumerate EBG nodes (2 per NBG edge)
    println!("Enumerating EBG nodes...");
    let ebg_nodes = enumerate_ebg_nodes(&nbg_geo)?;
    println!("  ✓ Created {} EBG nodes", ebg_nodes.len());

    // 5. Build adjacency lists with turn rule application
    println!("Building turn-expanded adjacency...");

    // Build turn penalty configs per mode from model JSON files
    let mut penalty_configs: [TurnPenaltyConfig; MAX_MODES] =
        std::array::from_fn(|_| TurnPenaltyConfig::default_identity());
    for mc in &config.modes {
        penalty_configs[mc.mode_index as usize] = TurnPenaltyConfig::for_mode(&mc.mode_name);
    }

    // Determine which mode (if any) to use for highway class lookup in turn geometry.
    // Use the first available mode's way_attrs for highway class info.
    let highway_class_mode_idx = config
        .modes
        .first()
        .map(|mc| mc.mode_index as usize)
        .unwrap_or(0);

    let (adjacency, turn_table) = build_adjacency(
        &nbg_csr,
        &nbg_geo,
        &nbg_node_map,
        &node_signals,
        &ebg_nodes,
        &canonical_rules,
        &way_attrs_by_mode,
        active_mode_mask,
        &penalty_configs,
        highway_class_mode_idx,
        &config.modes,
    )?;
    let n_arcs: u64 = adjacency.values().map(|v| v.len() as u64).sum();
    println!(
        "  ✓ Generated {} arcs with {} turn table entries",
        n_arcs,
        turn_table.len()
    );

    // 6. Materialize CSR
    println!("Materializing CSR...");
    let ebg_csr = materialize_csr(&adjacency, ebg_nodes.len() as u32, n_arcs)?;
    println!("  ✓ CSR assembled");

    // 7. Write output files
    println!();
    println!("Writing output files...");
    std::fs::create_dir_all(&config.outdir)?;

    let nodes_path = config.outdir.join("ebg.nodes");
    let csr_path = config.outdir.join("ebg.csr");
    let turn_table_path = config.outdir.join("ebg.turn_table");

    let created_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    // Compute inputs SHA
    let inputs_sha = compute_inputs_sha(&config)?;

    let ebg_nodes_data = EbgNodes {
        n_nodes: ebg_nodes.len() as u32,
        created_unix,
        inputs_sha,
        nodes: ebg_nodes,
    };
    EbgNodesFile::write(&nodes_path, &ebg_nodes_data)?;
    println!("  ✓ Wrote {}", nodes_path.display());

    EbgCsrFile::write(&csr_path, &ebg_csr)?;
    println!("  ✓ Wrote {}", csr_path.display());

    let turn_table_data = TurnTable {
        n_entries: turn_table.len() as u32,
        inputs_sha,
        entries: turn_table,
    };
    TurnTableFile::write(&turn_table_path, &turn_table_data)?;
    println!("  ✓ Wrote {}", turn_table_path.display());

    println!();
    println!("✅ EBG construction complete!");
    println!("  Nodes: {}", ebg_nodes_data.n_nodes);
    println!("  Arcs: {}", n_arcs);
    println!("  Time: {:.2}s", start_time.elapsed().as_secs_f64());

    let build_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(EbgResult {
        nodes_path,
        csr_path,
        turn_table_path,
        n_nodes: ebg_nodes_data.n_nodes,
        n_arcs,
        build_time_ms,
    })
}

/// Enumerate EBG nodes (2 per NBG undirected edge)
fn enumerate_ebg_nodes(nbg_geo: &NbgGeo) -> Result<Vec<EbgNode>> {
    let mut nodes = Vec::with_capacity((nbg_geo.n_edges_und * 2) as usize);

    for (geom_idx, edge) in nbg_geo.edges.iter().enumerate() {
        // Forward direction: u → v
        nodes.push(EbgNode {
            tail_nbg: edge.u_node,
            head_nbg: edge.v_node,
            geom_idx: geom_idx as u32,
            length_mm: edge.length_mm,
            class_bits: edge.flags,
            primary_way: (edge.first_osm_way_id & 0xFFFFFFFF) as u32,
        });

        // Reverse direction: v → u
        nodes.push(EbgNode {
            tail_nbg: edge.v_node,
            head_nbg: edge.u_node,
            geom_idx: geom_idx as u32,
            length_mm: edge.length_mm,
            class_bits: edge.flags,
            primary_way: (edge.first_osm_way_id & 0xFFFFFFFF) as u32,
        });
    }

    Ok(nodes)
}

/// Build adjacency lists with turn rule application and geometry-based penalties.
/// All modes are processed dynamically based on discovered model files.
#[allow(clippy::too_many_arguments)]
#[allow(clippy::type_complexity)]
fn build_adjacency(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    nbg_node_map: &NbgNodeMap,
    node_signals: &NodeSignals,
    ebg_nodes: &[EbgNode],
    canonical_rules: &HashMap<TurnRuleKey, CanonicalTurnRule>,
    way_attrs_by_mode: &[HashMap<i64, WayAttr>],
    active_mode_mask: u8,
    penalty_configs: &[TurnPenaltyConfig; MAX_MODES],
    highway_class_mode_idx: usize,
    modes: &[EbgModeConfig],
) -> Result<(HashMap<u32, Vec<(u32, u32)>>, Vec<TurnEntry>)> {
    let mut adjacency: HashMap<u32, Vec<(u32, u32)>> = HashMap::new();
    let mut turn_table = Vec::new();
    let mut turn_table_index: HashMap<TurnEntry, u32> = HashMap::new();

    // Build index: NBG node -> incoming/outgoing EBG nodes
    let mut incoming_by_nbg: HashMap<u32, Vec<u32>> = HashMap::new();
    let mut outgoing_by_nbg: HashMap<u32, Vec<u32>> = HashMap::new();

    for (ebg_id, ebg_node) in ebg_nodes.iter().enumerate() {
        outgoing_by_nbg
            .entry(ebg_node.tail_nbg)
            .or_default()
            .push(ebg_id as u32);
        incoming_by_nbg
            .entry(ebg_node.head_nbg)
            .or_default()
            .push(ebg_id as u32);
    }

    // Determine which modes have u-turn restrictions (modes with oneway rules, i.e., vehicular)
    // Use a simple heuristic: modes whose model JSON has respect_oneway=true should ban u-turns.
    // For now, any mode that has turn penalty u_turn_penalty_ds > 0 restricts u-turns.
    let mut uturn_restricted_mask = 0u8;
    for mc in modes {
        let idx = mc.mode_index as usize;
        if penalty_configs[idx].u_turn_penalty_ds > 0 {
            uturn_restricted_mask |= Mode(mc.mode_index).bit();
        }
    }

    // Debug counters
    let mut total_arcs = 0u64;
    let mut arcs_with_penalty = 0u64;
    let mut total_penalty_ds = 0u64;

    // For each NBG intersection node
    for nbg_node in 0..nbg_csr.n_nodes {
        let incoming = incoming_by_nbg.get(&nbg_node).cloned().unwrap_or_default();
        let outgoing = outgoing_by_nbg.get(&nbg_node).cloned().unwrap_or_default();

        // Intersection degree for complexity penalty
        let via_degree = (incoming.len() + outgoing.len()) as u8;

        // Check if via node has traffic signal
        let via_node_osm_for_signal = nbg_node_to_osm_id(nbg_node, nbg_node_map);
        let via_has_signal = node_signals.has_signal(via_node_osm_for_signal);

        // For each incoming EBG edge (a = u→nbg_node)
        for &a_id in &incoming {
            let a_node = &ebg_nodes[a_id as usize];
            let from_edge = &nbg_geo.edges[a_node.geom_idx as usize];

            // For each outgoing EBG edge (b = nbg_node→w)
            for &b_id in &outgoing {
                let b_node = &ebg_nodes[b_id as usize];
                let to_edge = &nbg_geo.edges[b_node.geom_idx as usize];

                // Skip if not a valid turn (tail of b must equal head of a)
                if a_node.head_nbg != b_node.tail_nbg {
                    continue;
                }

                // Handle U-turns with mode-specific policy
                let is_uturn = a_node.tail_nbg == b_node.head_nbg;
                let is_dead_end = outgoing.len() == 1;

                // Determine mode accessibility
                let from_way_id = from_edge.first_osm_way_id;
                let to_way_id = to_edge.first_osm_way_id;

                // Get OSM node ID for via node
                let via_node_osm = nbg_node_to_osm_id(nbg_node, nbg_node_map);

                // Check turn rules
                let rule_key = TurnRuleKey {
                    via_node_osm,
                    from_way_id,
                    to_way_id,
                };

                // Start with all active modes allowed
                let mut mode_mask = active_mode_mask;

                // Apply turn rules if they exist
                if let Some(rule) = canonical_rules.get(&rule_key) {
                    match rule.kind {
                        TurnKind::Ban => {
                            // Remove banned modes
                            mode_mask &= !rule.mode_mask;
                        }
                        TurnKind::Only => {
                            // Only allowed for specified modes
                            mode_mask &= rule.mode_mask;
                        }
                        _ => {}
                    }
                }

                // Filter by way accessibility (dynamic: check each active mode)
                mode_mask &= get_way_mode_mask(from_way_id, way_attrs_by_mode, active_mode_mask);
                mode_mask &= get_way_mode_mask(to_way_id, way_attrs_by_mode, active_mode_mask);

                // Apply U-turn policy: restrict u-turn-restricted modes at non-dead-ends
                if is_uturn && !is_dead_end {
                    mode_mask &= !uturn_restricted_mask;
                }

                // If no modes can use this turn, skip it
                if mode_mask == 0 {
                    continue;
                }

                // === COMPUTE TURN GEOMETRY AND PENALTIES ===
                let from_bearing = if a_node.tail_nbg == from_edge.u_node {
                    from_edge.bearing_deci_deg
                } else if from_edge.bearing_deci_deg == 65535 {
                    65535
                } else {
                    (from_edge.bearing_deci_deg + 1800) % 3600
                };

                let to_bearing = if b_node.tail_nbg == to_edge.u_node {
                    to_edge.bearing_deci_deg
                } else if to_edge.bearing_deci_deg == 65535 {
                    65535
                } else {
                    (to_edge.bearing_deci_deg + 1800) % 3600
                };

                // Get highway classes for road class transition penalty
                let from_highway_class = way_attrs_by_mode[highway_class_mode_idx]
                    .get(&from_way_id)
                    .map(|a| a.output.highway_class)
                    .unwrap_or(0);
                let to_highway_class = way_attrs_by_mode[highway_class_mode_idx]
                    .get(&to_way_id)
                    .map(|a| a.output.highway_class)
                    .unwrap_or(0);

                // Compute turn geometry
                let geom = TurnGeometry::compute(
                    from_bearing,
                    to_bearing,
                    via_has_signal,
                    via_degree,
                    from_highway_class,
                    to_highway_class,
                );

                // Compute per-mode penalties dynamically
                let mut penalty_ds = [0u32; MAX_MODES];
                for mc in modes {
                    let idx = mc.mode_index as usize;
                    if (mode_mask & Mode(mc.mode_index).bit()) != 0 {
                        penalty_ds[idx] = compute_turn_penalty(&geom, &penalty_configs[idx]);
                    }
                }

                // Add explicit penalties from turn rules if any
                if let Some(rule) = canonical_rules.get(&rule_key)
                    && rule.kind == TurnKind::Penalty {
                        for mc in modes {
                            let idx = mc.mode_index as usize;
                            penalty_ds[idx] = penalty_ds[idx].saturating_add(rule.penalty_ds[idx]);
                        }
                    }

                // Statistics
                total_arcs += 1;
                let first_penalty = modes
                    .first()
                    .map(|mc| penalty_ds[mc.mode_index as usize])
                    .unwrap_or(0);
                if first_penalty > 0 {
                    arcs_with_penalty += 1;
                    total_penalty_ds += first_penalty as u64;
                }

                // Get or create turn table entry
                let turn_entry = TurnEntry {
                    mode_mask,
                    kind: canonical_rules
                        .get(&rule_key)
                        .map(|r| r.kind)
                        .unwrap_or(TurnKind::None),
                    has_time_dep: canonical_rules
                        .get(&rule_key)
                        .map(|r| r.has_time_dep)
                        .unwrap_or(false),
                    penalty_ds,
                    attrs_idx: 0,
                };

                let turn_idx = if let Some(&idx) = turn_table_index.get(&turn_entry) {
                    idx
                } else {
                    let idx = turn_table.len() as u32;
                    turn_table.push(turn_entry.clone());
                    turn_table_index.insert(turn_entry, idx);
                    idx
                };

                debug_assert!(
                    (turn_idx as usize) < turn_table.len(),
                    "turn_idx {} out of bounds (turn_table.len()={})",
                    turn_idx,
                    turn_table.len()
                );

                // Add arc
                adjacency.entry(a_id).or_default().push((b_id, turn_idx));
            }
        }
    }

    // Print turn penalty statistics
    println!("  Turn penalty statistics:");
    println!("    Total arcs: {}", total_arcs);
    println!(
        "    Arcs with penalty: {} ({:.1}%)",
        arcs_with_penalty,
        arcs_with_penalty as f64 * 100.0 / total_arcs.max(1) as f64
    );
    if arcs_with_penalty > 0 {
        println!(
            "    Avg penalty: {:.1}s",
            total_penalty_ds as f64 / arcs_with_penalty as f64 / 10.0
        );
    }

    Ok((adjacency, turn_table))
}

/// Materialize CSR from adjacency lists
fn materialize_csr(
    adjacency: &HashMap<u32, Vec<(u32, u32)>>,
    n_nodes: u32,
    n_arcs: u64,
) -> Result<EbgCsr> {
    let mut offsets = vec![0u64; (n_nodes + 1) as usize];
    let mut heads = Vec::with_capacity(n_arcs as usize);
    let mut turn_idx = Vec::with_capacity(n_arcs as usize);

    let mut current_offset = 0u64;
    for node_id in 0..n_nodes {
        offsets[node_id as usize] = current_offset;

        if let Some(neighbors) = adjacency.get(&node_id) {
            for &(head, tidx) in neighbors {
                debug_assert!(
                    (head as usize) < n_nodes as usize,
                    "head {} out of bounds (n_nodes={})",
                    head,
                    n_nodes
                );

                heads.push(head);
                turn_idx.push(tidx);
                current_offset += 1;
            }
        }
    }
    offsets[n_nodes as usize] = current_offset;

    let created_unix = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)?
        .as_secs();

    Ok(EbgCsr {
        n_nodes,
        n_arcs,
        created_unix,
        inputs_sha: [0u8; 32],
        offsets,
        heads,
        turn_idx,
    })
}

/// Helper: Get mode mask for a way based on per-mode way attributes.
/// Checks all active modes dynamically.
fn get_way_mode_mask(
    way_id: i64,
    way_attrs_by_mode: &[HashMap<i64, WayAttr>],
    active_mode_mask: u8,
) -> u8 {
    let mut mask = 0u8;
    for (mode_idx, attrs) in way_attrs_by_mode.iter().enumerate().take(MAX_MODES) {
        let mode_bit = 1u8 << mode_idx;
        if (active_mode_mask & mode_bit) == 0 {
            continue; // Mode not active
        }
        if attrs
            .get(&way_id)
            .map(|a| a.output.access_fwd || a.output.access_rev)
            .unwrap_or(false)
        {
            mask |= mode_bit;
        }
    }
    mask
}

/// Helper: Convert NBG compact node ID to OSM node ID
fn nbg_node_to_osm_id(compact_id: u32, node_map: &NbgNodeMap) -> i64 {
    node_map
        .mappings
        .get(compact_id as usize)
        .map(|m| m.osm_node_id)
        .unwrap_or(0)
}

/// Helper: Load way attributes into HashMap
fn load_way_attrs(path: &Path) -> Result<HashMap<i64, WayAttr>> {
    let attrs = way_attrs::read_all(path)?;
    let mut map = HashMap::with_capacity(attrs.len());
    for attr in attrs {
        map.insert(attr.way_id, attr);
    }
    Ok(map)
}

/// Compute combined SHA-256 of all inputs
fn compute_inputs_sha(config: &EbgConfig) -> Result<[u8; 32]> {
    use sha2::{Digest, Sha256};

    let mut hasher = Sha256::new();

    // Hash all input file paths (deterministic)
    hasher.update(std::fs::read(&config.nbg_csr_path)?);
    hasher.update(std::fs::read(&config.nbg_geo_path)?);
    hasher.update(std::fs::read(&config.nbg_node_map_path)?);

    let result = hasher.finalize();
    let mut sha = [0u8; 32];
    sha.copy_from_slice(&result);
    Ok(sha)
}
