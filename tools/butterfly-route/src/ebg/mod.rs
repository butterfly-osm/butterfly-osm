///! Edge-Based Graph (EBG) construction - Step 4
///!
///! Builds a turn-expanded graph where:
///! - Nodes = directed NBG edges (u→v)
///! - Arcs = legal turn transitions at intersections
///! - Mode specificity encoded as bitmasks

use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::formats::*;

pub mod turn_processor;

// Mode bit flags
pub const MODE_CAR: u8 = 0b001;
pub const MODE_BIKE: u8 = 0b010;
pub const MODE_FOOT: u8 = 0b100;
pub const MODE_ALL: u8 = 0b111;

#[derive(Debug)]
pub struct EbgConfig {
    pub nbg_csr_path: PathBuf,
    pub nbg_geo_path: PathBuf,
    pub nbg_node_map_path: PathBuf,
    pub way_attrs_car_path: PathBuf,
    pub way_attrs_bike_path: PathBuf,
    pub way_attrs_foot_path: PathBuf,
    pub turn_rules_car_path: PathBuf,
    pub turn_rules_bike_path: PathBuf,
    pub turn_rules_foot_path: PathBuf,
    pub outdir: PathBuf,
}

#[derive(Debug)]
pub struct EbgResult {
    pub nodes_path: PathBuf,
    pub csr_path: PathBuf,
    pub turn_table_path: PathBuf,
    pub n_nodes: u32,
    pub n_arcs: u64,
}

/// Canonical turn rule key
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) struct TurnRuleKey {
    pub via_node_osm: i64,
    pub from_way_id: i64,
    pub to_way_id: i64,
}

/// Canonical turn rule
#[derive(Debug, Clone)]
pub(crate) struct CanonicalTurnRule {
    pub mode_mask: u8,      // Which modes this rule applies to
    pub kind: TurnKind,
    pub penalty_ds_car: u32,
    pub penalty_ds_bike: u32,
    pub penalty_ds_foot: u32,
    pub has_time_dep: bool,
}

pub fn build_ebg(config: EbgConfig) -> Result<EbgResult> {
    use std::time::Instant;
    let start_time = Instant::now();

    println!("🦋 Starting Step 4: Edge-Based Graph Construction");
    println!("📂 NBG CSR: {}", config.nbg_csr_path.display());
    println!("📂 NBG Geo: {}", config.nbg_geo_path.display());
    println!("📂 Output: {}", config.outdir.display());
    println!();

    // 1. Load NBG data
    println!("Loading NBG files...");
    let nbg_csr = NbgCsrFile::read(&config.nbg_csr_path)?;
    let nbg_geo = NbgGeoFile::read(&config.nbg_geo_path)?;
    let nbg_node_map = NbgNodeMapFile::read_map(&config.nbg_node_map_path)?;
    println!("  ✓ NBG loaded: {} nodes, {} edges", nbg_csr.n_nodes, nbg_geo.n_edges_und);

    // 2. Load way attributes (to determine access per mode)
    println!("Loading way attributes...");
    let way_attrs_car = load_way_attrs(&config.way_attrs_car_path)?;
    let way_attrs_bike = load_way_attrs(&config.way_attrs_bike_path)?;
    let way_attrs_foot = load_way_attrs(&config.way_attrs_foot_path)?;
    println!("  ✓ Loaded way attributes for 3 modes");

    // 3. Load and process turn rules
    println!("Processing turn rules...");
    let canonical_rules = turn_processor::build_canonical_turn_rules(
        &config.turn_rules_car_path,
        &config.turn_rules_bike_path,
        &config.turn_rules_foot_path,
        &nbg_csr,
        &nbg_geo,
        &nbg_node_map,
    )?;
    println!("  ✓ Processed {} canonical turn rules", canonical_rules.len());

    // 4. Enumerate EBG nodes (2 per NBG edge)
    println!("Enumerating EBG nodes...");
    let ebg_nodes = enumerate_ebg_nodes(&nbg_geo)?;
    println!("  ✓ Created {} EBG nodes", ebg_nodes.len());

    // 5. Build adjacency lists with turn rule application
    println!("Building turn-expanded adjacency...");
    let (adjacency, turn_table) = build_adjacency(
        &nbg_csr,
        &nbg_geo,
        &nbg_node_map,
        &ebg_nodes,
        &canonical_rules,
        &way_attrs_car,
        &way_attrs_bike,
        &way_attrs_foot,
    )?;
    let n_arcs: u64 = adjacency.values().map(|v| v.len() as u64).sum();
    println!("  ✓ Generated {} arcs with {} turn table entries", n_arcs, turn_table.len());

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

    Ok(EbgResult {
        nodes_path,
        csr_path,
        turn_table_path,
        n_nodes: ebg_nodes_data.n_nodes,
        n_arcs,
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

/// Build adjacency lists with turn rule application
fn build_adjacency(
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    nbg_node_map: &NbgNodeMap,
    ebg_nodes: &[EbgNode],
    canonical_rules: &HashMap<TurnRuleKey, CanonicalTurnRule>,
    way_attrs_car: &HashMap<i64, WayAttr>,
    way_attrs_bike: &HashMap<i64, WayAttr>,
    way_attrs_foot: &HashMap<i64, WayAttr>,
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

    // For each NBG intersection node
    for nbg_node in 0..nbg_csr.n_nodes {
        let incoming = incoming_by_nbg.get(&nbg_node).cloned().unwrap_or_default();
        let outgoing = outgoing_by_nbg.get(&nbg_node).cloned().unwrap_or_default();

        // For each incoming EBG edge (a = u→nbg_node)
        for &a_id in &incoming {
            let a_node = &ebg_nodes[a_id as usize];

            // For each outgoing EBG edge (b = nbg_node→w)
            for &b_id in &outgoing {
                let b_node = &ebg_nodes[b_id as usize];

                // Skip if not a valid turn (tail of b must equal head of a)
                if a_node.head_nbg != b_node.tail_nbg {
                    continue;
                }

                // Handle U-turns with mode-specific policy
                let is_uturn = a_node.tail_nbg == b_node.head_nbg;
                if is_uturn {
                    // Check if this is a dead-end (degree == 1 for outgoing)
                    let is_dead_end = outgoing.len() == 1;

                    // Car: forbid U-turns except at dead-ends
                    // Bike/Foot: allow U-turns unless explicitly banned
                    // We'll handle this by filtering mode_mask below
                    if !is_dead_end {
                        // At non-dead-ends, remove car from allowed modes for U-turns
                        // Bike and foot can still make U-turns
                        // This will be applied after checking way accessibility
                    }
                }

                // Determine mode accessibility
                let from_way_id = nbg_geo.edges[a_node.geom_idx as usize].first_osm_way_id;
                let to_way_id = nbg_geo.edges[b_node.geom_idx as usize].first_osm_way_id;

                // Get OSM node ID for via node
                let via_node_osm = nbg_node_to_osm_id(nbg_node, nbg_node_map);

                // Check turn rules
                let rule_key = TurnRuleKey {
                    via_node_osm,
                    from_way_id,
                    to_way_id,
                };

                // Start with all modes allowed
                let mut mode_mask = MODE_ALL;

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

                // Filter by way accessibility
                mode_mask &= get_way_mode_mask(from_way_id, way_attrs_car, way_attrs_bike, way_attrs_foot);
                mode_mask &= get_way_mode_mask(to_way_id, way_attrs_car, way_attrs_bike, way_attrs_foot);

                // Apply U-turn policy
                if is_uturn {
                    let is_dead_end = outgoing.len() == 1;
                    if !is_dead_end {
                        // Remove car mode from U-turns at non-dead-ends
                        mode_mask &= !MODE_CAR;
                    }
                }

                // If no modes can use this turn, skip it
                if mode_mask == 0 {
                    continue;
                }

                // Get or create turn table entry
                let turn_entry = TurnEntry {
                    mode_mask,
                    kind: canonical_rules.get(&rule_key).map(|r| r.kind).unwrap_or(TurnKind::None),
                    has_time_dep: canonical_rules.get(&rule_key).map(|r| r.has_time_dep).unwrap_or(false),
                    penalty_ds_car: canonical_rules.get(&rule_key).map(|r| r.penalty_ds_car).unwrap_or(0),
                    penalty_ds_bike: canonical_rules.get(&rule_key).map(|r| r.penalty_ds_bike).unwrap_or(0),
                    penalty_ds_foot: canonical_rules.get(&rule_key).map(|r| r.penalty_ds_foot).unwrap_or(0),
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

                // Add arc
                adjacency
                    .entry(a_id)
                    .or_default()
                    .push((b_id, turn_idx));
            }
        }
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
        inputs_sha: [0u8; 32], // Will be set by caller
        offsets,
        heads,
        turn_idx,
    })
}

/// Helper: Get mode mask for a way based on attributes
fn get_way_mode_mask(
    way_id: i64,
    car_attrs: &HashMap<i64, WayAttr>,
    bike_attrs: &HashMap<i64, WayAttr>,
    foot_attrs: &HashMap<i64, WayAttr>,
) -> u8 {
    let mut mask = 0u8;

    if car_attrs.get(&way_id).map(|a| a.output.access_fwd || a.output.access_rev).unwrap_or(false) {
        mask |= MODE_CAR;
    }
    if bike_attrs.get(&way_id).map(|a| a.output.access_fwd || a.output.access_rev).unwrap_or(false) {
        mask |= MODE_BIKE;
    }
    if foot_attrs.get(&way_id).map(|a| a.output.access_fwd || a.output.access_rev).unwrap_or(false) {
        mask |= MODE_FOOT;
    }

    mask
}

/// Helper: Convert NBG compact node ID to OSM node ID
fn nbg_node_to_osm_id(compact_id: u32, node_map: &NbgNodeMap) -> i64 {
    node_map.mappings.get(compact_id as usize)
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
    use sha2::{Sha256, Digest};

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
