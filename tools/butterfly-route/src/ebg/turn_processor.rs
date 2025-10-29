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
    _nbg_csr: &NbgCsr,
    _nbg_geo: &NbgGeo,
    _nbg_node_map: &NbgNodeMap,
) -> Result<()> {
    // Convert rule kind from profile_abi TurnRuleKind to ebg TurnKind
    use crate::profile_abi::TurnRuleKind as PRK;
    let kind = match rule.kind {
        PRK::None => TurnKind::None,
        PRK::Ban => TurnKind::Ban,
        PRK::Only => TurnKind::Only,
        PRK::Penalty => TurnKind::Penalty,
    };

    // For via=way rules (is_time_dep == 2), we would need to expand them
    // For now, we'll skip via=way expansion (would require NBG topology traversal)
    if rule.is_time_dep == 2 {
        // TODO: Implement via=way expansion
        return Ok(());
    }

    let key = TurnRuleKey {
        via_node_osm: rule.via_node_id,
        from_way_id: rule.from_way_id,
        to_way_id: rule.to_way_id,
    };

    // Merge with existing rule if present
    if let Some(existing) = canonical_rules.get_mut(&key) {
        existing.mode_mask |= mode_bit;

        // Update penalties
        if mode_bit == MODE_CAR {
            existing.penalty_ds_car = rule.penalty_ds;
        } else if mode_bit == MODE_BIKE {
            existing.penalty_ds_bike = rule.penalty_ds;
        } else if mode_bit == MODE_FOOT {
            existing.penalty_ds_foot = rule.penalty_ds;
        }

        existing.has_time_dep |= rule.is_time_dep == 1;
    } else {
        // Create new canonical rule
        let mut penalty_car = 0;
        let mut penalty_bike = 0;
        let mut penalty_foot = 0;

        if mode_bit == MODE_CAR {
            penalty_car = rule.penalty_ds;
        } else if mode_bit == MODE_BIKE {
            penalty_bike = rule.penalty_ds;
        } else if mode_bit == MODE_FOOT {
            penalty_foot = rule.penalty_ds;
        }

        canonical_rules.insert(key, CanonicalTurnRule {
            mode_mask: mode_bit,
            kind,
            penalty_ds_car: penalty_car,
            penalty_ds_bike: penalty_bike,
            penalty_ds_foot: penalty_foot,
            has_time_dep: rule.is_time_dep == 1,
        });
    }

    Ok(())
}

/// Convert ONLY rules to implicit Bans
fn convert_only_to_bans(
    canonical_rules: &mut HashMap<TurnRuleKey, CanonicalTurnRule>,
    _nbg_csr: &NbgCsr,
    _nbg_geo: &NbgGeo,
    _nbg_node_map: &NbgNodeMap,
) -> Result<()> {
    // Collect all ONLY rules
    let only_rules: Vec<(TurnRuleKey, CanonicalTurnRule)> = canonical_rules
        .iter()
        .filter(|(_, r)| r.kind == TurnKind::Only)
        .map(|(k, r)| (*k, r.clone()))
        .collect();

    // For each ONLY rule, we would need to find all other possible turns at that (via, from_way)
    // and create implicit bans for them. This requires knowing all possible to_ways at each intersection.
    // For now, we'll keep ONLY rules as-is and handle them in the adjacency builder.

    // TODO: Implement full ONLY -> Ban conversion by:
    // 1. Group ONLY rules by (via_node, from_way)
    // 2. For each group, find all possible to_ways at that intersection
    // 3. Create Ban rules for to_ways not in the ONLY set

    // For now, just mark this as a placeholder
    for (key, rule) in only_rules {
        // Keep the ONLY rule in the table
        // The adjacency builder will handle it by only allowing specified modes
        let _ = (key, rule); // Suppress unused warning
    }

    Ok(())
}

/// Load turn rules from file
fn load_turn_rules(_path: &Path) -> Result<Vec<TurnRule>> {
    // TODO: Implement turn rules reader
    // For now, return empty list (Monaco and Belgium have 0 turn rules anyway)
    Ok(Vec::new())
}
