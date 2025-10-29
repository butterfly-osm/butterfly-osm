///! Turn rule processing - via=way expansion, merging, ONLY conversion

use anyhow::Result;
use std::collections::{HashMap, HashSet};
use std::path::Path;

use crate::formats::*;
use super::{TurnRuleKey, CanonicalTurnRule, MODE_CAR, MODE_BIKE, MODE_FOOT};

/// Build canonical turn rule table from per-mode turn rules
pub fn build_canonical_turn_rules(
    car_path: &Path,
    bike_path: &Path,
    foot_path: &Path,
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    nbg_node_map: &NbgNodeMap,
) -> Result<HashMap<TurnRuleKey, CanonicalTurnRule>> {
    // Load per-mode turn rules
    let car_rules = load_turn_rules(car_path)?;
    let bike_rules = load_turn_rules(bike_path)?;
    let foot_rules = load_turn_rules(foot_path)?;

    let mut canonical_rules: HashMap<TurnRuleKey, CanonicalTurnRule> = HashMap::new();

    // Process car rules
    for rule in car_rules {
        process_rule(rule, MODE_CAR, &mut canonical_rules, nbg_csr, nbg_geo, nbg_node_map)?;
    }

    // Process bike rules
    for rule in bike_rules {
        process_rule(rule, MODE_BIKE, &mut canonical_rules, nbg_csr, nbg_geo, nbg_node_map)?;
    }

    // Process foot rules
    for rule in foot_rules {
        process_rule(rule, MODE_FOOT, &mut canonical_rules, nbg_csr, nbg_geo, nbg_node_map)?;
    }

    // Convert ONLY rules to implicit Bans
    convert_only_to_bans(&mut canonical_rules, nbg_csr, nbg_geo, nbg_node_map)?;

    Ok(canonical_rules)
}

/// Process a single turn rule and add to canonical table
fn process_rule(
    rule: TurnRule,
    mode_bit: u8,
    canonical_rules: &mut HashMap<TurnRuleKey, CanonicalTurnRule>,
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    nbg_node_map: &NbgNodeMap,
) -> Result<()> {
    // Convert rule kind from profile_abi TurnRuleKind to ebg TurnKind
    use crate::profile_abi::TurnRuleKind as PRK;
    let kind = match rule.kind {
        PRK::None => TurnKind::None,
        PRK::Ban => TurnKind::Ban,
        PRK::Only => TurnKind::Only,
        PRK::Penalty => TurnKind::Penalty,
    };

    // For via=way rules (is_time_dep == 2), expand to via=node rules
    if rule.is_time_dep == 2 {
        let via_way_id = rule.via_node_id; // Actually via_way_id in this case
        let via_nodes = find_nodes_on_way(via_way_id, nbg_geo, nbg_node_map)?;

        // Create a rule for each node on the via way
        for via_node_osm in via_nodes {
            add_canonical_rule(
                via_node_osm,
                rule.from_way_id,
                rule.to_way_id,
                mode_bit,
                kind,
                rule.penalty_ds,
                false, // Not time-dependent after expansion
                canonical_rules,
            );
        }
        return Ok(());
    }

    // Normal via=node rule
    add_canonical_rule(
        rule.via_node_id,
        rule.from_way_id,
        rule.to_way_id,
        mode_bit,
        kind,
        rule.penalty_ds,
        rule.is_time_dep == 1,
        canonical_rules,
    );

    Ok(())
}

/// Add or merge a canonical turn rule
fn add_canonical_rule(
    via_node_osm: i64,
    from_way_id: i64,
    to_way_id: i64,
    mode_bit: u8,
    kind: TurnKind,
    penalty_ds: u32,
    has_time_dep: bool,
    canonical_rules: &mut HashMap<TurnRuleKey, CanonicalTurnRule>,
) {
    let key = TurnRuleKey {
        via_node_osm,
        from_way_id,
        to_way_id,
    };

    // Merge with existing rule if present
    if let Some(existing) = canonical_rules.get_mut(&key) {
        existing.mode_mask |= mode_bit;

        // Update penalties
        if mode_bit == MODE_CAR {
            existing.penalty_ds_car = penalty_ds;
        } else if mode_bit == MODE_BIKE {
            existing.penalty_ds_bike = penalty_ds;
        } else if mode_bit == MODE_FOOT {
            existing.penalty_ds_foot = penalty_ds;
        }

        existing.has_time_dep |= has_time_dep;
    } else {
        // Create new canonical rule
        let mut penalty_car = 0;
        let mut penalty_bike = 0;
        let mut penalty_foot = 0;

        if mode_bit == MODE_CAR {
            penalty_car = penalty_ds;
        } else if mode_bit == MODE_BIKE {
            penalty_bike = penalty_ds;
        } else if mode_bit == MODE_FOOT {
            penalty_foot = penalty_ds;
        }

        canonical_rules.insert(key, CanonicalTurnRule {
            mode_mask: mode_bit,
            kind,
            penalty_ds_car: penalty_car,
            penalty_ds_bike: penalty_bike,
            penalty_ds_foot: penalty_foot,
            has_time_dep,
        });
    }
}

/// Find all NBG nodes that are part of a given way
fn find_nodes_on_way(
    way_id: i64,
    nbg_geo: &NbgGeo,
    nbg_node_map: &NbgNodeMap,
) -> Result<Vec<i64>> {
    let mut nodes = Vec::new();

    // Scan all NBG edges to find those that belong to this way
    for edge in &nbg_geo.edges {
        if edge.first_osm_way_id == way_id {
            // Get OSM node IDs for both endpoints
            let u_osm = nbg_node_map.mappings.get(edge.u_node as usize)
                .map(|m| m.osm_node_id)
                .unwrap_or(0);
            let v_osm = nbg_node_map.mappings.get(edge.v_node as usize)
                .map(|m| m.osm_node_id)
                .unwrap_or(0);

            if u_osm != 0 && !nodes.contains(&u_osm) {
                nodes.push(u_osm);
            }
            if v_osm != 0 && !nodes.contains(&v_osm) {
                nodes.push(v_osm);
            }
        }
    }

    Ok(nodes)
}

/// Convert ONLY rules to implicit Bans
fn convert_only_to_bans(
    canonical_rules: &mut HashMap<TurnRuleKey, CanonicalTurnRule>,
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
    nbg_node_map: &NbgNodeMap,
) -> Result<()> {
    // Collect all ONLY rules grouped by (via_node, from_way)
    let mut only_groups: HashMap<(i64, i64), Vec<(TurnRuleKey, CanonicalTurnRule)>> = HashMap::new();

    for (key, rule) in canonical_rules.iter() {
        if rule.kind == TurnKind::Only {
            only_groups
                .entry((key.via_node_osm, key.from_way_id))
                .or_default()
                .push((*key, rule.clone()));
        }
    }

    // For each ONLY group, find all possible to_ways and create bans
    for ((via_node_osm, from_way_id), only_rules) in only_groups {
        // Find the NBG compact node ID for this OSM node
        let via_node_compact = osm_node_to_compact(via_node_osm, nbg_node_map);
        if via_node_compact.is_none() {
            continue; // Node not in NBG
        }
        let via_node = via_node_compact.unwrap();

        // Find all possible to_ways at this intersection
        let all_to_ways = find_outgoing_ways_from_intersection(via_node, from_way_id, nbg_csr, nbg_geo);

        // Collect allowed to_ways from ONLY rules, per mode
        let mut allowed_by_mode: HashMap<u8, std::collections::HashSet<i64>> = HashMap::new();
        for (key, rule) in &only_rules {
            // For each mode bit in the rule's mask
            for mode_shift in 0..3 {
                let mode_bit = 1u8 << mode_shift;
                if (rule.mode_mask & mode_bit) != 0 {
                    allowed_by_mode
                        .entry(mode_bit)
                        .or_default()
                        .insert(key.to_way_id);
                }
            }
        }

        // Create Ban rules for disallowed to_ways
        for to_way_id in all_to_ways {
            for mode_shift in 0..3 {
                let mode_bit = 1u8 << mode_shift;

                // If this mode has ONLY rules at this intersection
                if let Some(allowed_ways) = allowed_by_mode.get(&mode_bit) {
                    // And this to_way is NOT in the allowed set
                    if !allowed_ways.contains(&to_way_id) {
                        // Create implicit ban
                        add_canonical_rule(
                            via_node_osm,
                            from_way_id,
                            to_way_id,
                            mode_bit,
                            TurnKind::Ban,
                            0, // No penalty, just banned
                            false,
                            canonical_rules,
                        );
                    }
                }
            }
        }
    }

    Ok(())
}

/// Convert OSM node ID to NBG compact node ID
fn osm_node_to_compact(osm_id: i64, node_map: &NbgNodeMap) -> Option<u32> {
    node_map.mappings.iter()
        .position(|m| m.osm_node_id == osm_id)
        .map(|idx| idx as u32)
}

/// Find all outgoing ways from an intersection given an incoming way
fn find_outgoing_ways_from_intersection(
    via_node: u32,
    from_way_id: i64,
    nbg_csr: &NbgCsr,
    nbg_geo: &NbgGeo,
) -> Vec<i64> {
    let mut to_ways = Vec::new();

    // Get outgoing edges from via_node
    let start = nbg_csr.offsets[via_node as usize];
    let end = nbg_csr.offsets[via_node as usize + 1];

    for i in start..end {
        let edge_idx = nbg_csr.edge_idx[i as usize];
        let edge = &nbg_geo.edges[edge_idx as usize];

        // Skip if this is the from_way (U-turn back)
        if edge.first_osm_way_id == from_way_id {
            continue;
        }

        if !to_ways.contains(&edge.first_osm_way_id) {
            to_ways.push(edge.first_osm_way_id);
        }
    }

    to_ways
}

/// Load turn rules from file
fn load_turn_rules(path: &Path) -> Result<Vec<TurnRule>> {
    turn_rules::read_all(path)
}
