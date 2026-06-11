//! Model evaluator - evaluate compiled models against way/turn inputs
//!
//! Uses dense table lookups for O(1) evaluation per way.

use crate::profile_abi::{TurnRuleKind, WayOutput};

use super::compile::{CompiledClassBitRule, CompiledModel};

/// Evaluate a way through a compiled model.
///
/// `kv_keys` and `kv_vals` are parallel arrays of dictionary IDs for this way's tags.
/// `val_dict` is needed for highway_suffix checks.
pub fn evaluate_way(
    model: &CompiledModel,
    kv_keys: &[u32],
    kv_vals: &[u32],
    val_dict: &std::collections::HashMap<u32, String>,
) -> WayOutput {
    let mut output = WayOutput::default();

    // Find highway value_id
    let highway_key_id = match model.highway_key_id {
        Some(kid) => kid,
        None => return output,
    };

    let highway_val_id = match find_value_for_key(kv_keys, kv_vals, highway_key_id) {
        Some(vid) => vid,
        None => return output, // No highway tag = not routable
    };

    let hw_idx = highway_val_id as usize;

    // Hard legal class ban (#470): e.g. pedestrians/cyclists on
    // motorway/motorway_link (Vienna-convention semantics). Highest
    // precedence — cannot be overridden by `access.highway`, `allow_if`
    // rules, or way tags (`foot=yes`, `sidewalk=*`, ...).
    if hw_idx < model.hard_deny_table.len() && model.hard_deny_table[hw_idx] {
        return output;
    }

    // Check access
    let highway_allowed = hw_idx < model.access_table.len() && model.access_table[hw_idx];

    if !highway_allowed {
        // Highway type denied by default — check allow_if overrides
        let mut allowed_by_rule = false;
        for rule in &model.allow_if_rules {
            if check_all_conditions(&rule.conditions, kv_keys, kv_vals) {
                allowed_by_rule = true;
                // Override the speed for this allowed road
                output.base_speed_mmps = rule.speed_mmps;
                break;
            }
        }
        if !allowed_by_rule {
            return output; // Not accessible
        }
    }

    // Check deny rules (explicit access tag overrides)
    for deny in &model.deny_rules {
        if let Some(val_id) = find_value_for_key(kv_keys, kv_vals, deny.key_id) {
            let vidx = val_id as usize;
            if vidx < deny.denied_values.len() && deny.denied_values[vidx] {
                return output; // Explicitly denied
            }
        }
    }

    // Set access flags
    output.access_fwd = true;
    output.access_rev = true;

    // Handle oneway
    if model.respect_oneway {
        if let Some(oneway_key_id) = model.oneway_key_id
            && let Some(oneway_val_id) = find_value_for_key(kv_keys, kv_vals, oneway_key_id)
        {
            if model.forward_value_ids.contains(&oneway_val_id) {
                output.access_rev = false;
                output.oneway = 1;
            } else if model.reverse_value_ids.contains(&oneway_val_id) {
                output.access_fwd = false;
                output.oneway = 2;
            }
        }

        // Default oneways (e.g., motorways)
        if output.oneway == 0 && model.default_oneway_highway_ids.contains(&highway_val_id) {
            output.access_rev = false;
            output.oneway = 1;
        }
    } else if let Some(oneway_key_id) = model.oneway_key_id {
        // Mode doesn't respect car oneways but may have its own oneway tag (e.g., oneway:bicycle)
        if let Some(oneway_val_id) = find_value_for_key(kv_keys, kv_vals, oneway_key_id)
            && model.forward_value_ids.contains(&oneway_val_id)
        {
            output.access_rev = false;
            output.oneway = 1;
        }
    }

    // Speed (skip if already set by allow_if rule)
    if output.base_speed_mmps == 0 {
        let base_speed_mmps = if hw_idx < model.speed_table.len() {
            model.speed_table[hw_idx]
        } else {
            0
        };
        output.base_speed_mmps = base_speed_mmps;
    }

    // Speed overrides
    for ovr in &model.speed_overrides {
        if check_all_conditions(&ovr.conditions, kv_keys, kv_vals) {
            output.base_speed_mmps = ovr.limit_to_mmps;
        }
    }

    // Highway class
    output.highway_class = if hw_idx < model.highway_class_table.len() {
        model.highway_class_table[hw_idx]
    } else {
        0
    };

    // Class bits
    for &(bit_pos, ref rule) in &model.class_bit_rules {
        if check_class_bit_rule(rule, kv_keys, kv_vals, highway_val_id, val_dict) {
            output.class_bits |= 1 << bit_pos;
        }
    }

    // Priority rules (compute per_km_penalty_ds)
    for rule in &model.priority_rules {
        if check_priority_conditions(&rule.conditions, kv_keys, kv_vals) {
            // multiply_by < 1.0 means slower -> penalty
            // The penalty adds cost so the road is less preferred
            // penalty = travel_time * (1/multiply_by - 1)
            // Since we need per_km_penalty_ds: we compute a base at reference speed
            // For simplicity: use a fixed reference (50 km/h -> 720 ds/km)
            // This matches the plan's additive penalty approach
            if rule.multiply_by > 0.0 && rule.multiply_by < 1.0 {
                let factor = (1.0 / rule.multiply_by) - 1.0;
                // Reference: 1 km at current speed in ds = 1_000_000 mm / speed_mmps * 10
                // But per_km_penalty_ds is u16, so cap at 65535
                let ref_time_ds_per_km = if output.base_speed_mmps > 0 {
                    (1_000_000u64 * 10) / output.base_speed_mmps as u64
                } else {
                    720 // default: 50 km/h
                };
                let penalty = (ref_time_ds_per_km as f64 * factor).round() as u16;
                output.per_km_penalty_ds = output.per_km_penalty_ds.saturating_add(penalty);
            }
        }
    }

    output
}

/// Determine if a mode is a motor vehicle mode (applies generic turn restrictions)
fn is_motor_vehicle_mode(mode_name: &str) -> bool {
    matches!(mode_name, "car" | "truck" | "bus" | "taxi" | "motorcycle")
}

/// Parse restriction value into TurnRuleKind
fn parse_restriction_kind(restriction: &str) -> TurnRuleKind {
    if restriction.starts_with("no_") {
        TurnRuleKind::Ban
    } else if restriction.starts_with("only_") {
        TurnRuleKind::Only
    } else {
        TurnRuleKind::None
    }
}

/// Find the value_id for a given key_id in parallel arrays
#[inline]
fn find_value_for_key(keys: &[u32], vals: &[u32], key_id: u32) -> Option<u32> {
    for (i, &k) in keys.iter().enumerate() {
        if k == key_id {
            return Some(vals[i]);
        }
    }
    None
}

/// Check all conditions match (AND logic)
#[inline]
fn check_all_conditions(conditions: &[(u32, u32)], keys: &[u32], vals: &[u32]) -> bool {
    conditions
        .iter()
        .all(|&(key_id, value_id)| find_value_for_key(keys, vals, key_id) == Some(value_id))
}

/// Check priority conditions (each key must match at least one of its values)
#[inline]
fn check_priority_conditions(conditions: &[(u32, Vec<u32>)], keys: &[u32], vals: &[u32]) -> bool {
    conditions.iter().all(|(key_id, value_ids)| {
        find_value_for_key(keys, vals, *key_id)
            .map(|vid| value_ids.contains(&vid))
            .unwrap_or(false)
    })
}

/// Check a class bit rule
fn check_class_bit_rule(
    rule: &CompiledClassBitRule,
    kv_keys: &[u32],
    kv_vals: &[u32],
    highway_val_id: u32,
    val_dict: &std::collections::HashMap<u32, String>,
) -> bool {
    match rule {
        CompiledClassBitRule::TagValue { key_id, value_id } => {
            find_value_for_key(kv_keys, kv_vals, *key_id) == Some(*value_id)
        }
        CompiledClassBitRule::Highway { value_id } => highway_val_id == *value_id,
        CompiledClassBitRule::HighwaySuffix { suffix } => {
            if let Some(hw_str) = val_dict.get(&highway_val_id) {
                hw_str.ends_with(suffix.as_str())
            } else {
                false
            }
        }
        CompiledClassBitRule::HighwayAny { value_ids } => value_ids.contains(&highway_val_id),
    }
}

/// Extended evaluate_turn that takes key_dict for full except handling
pub fn evaluate_turn_full(
    model: &CompiledModel,
    tags_keys: &[u32],
    tags_vals: &[u32],
    key_dict: &std::collections::HashMap<u32, String>,
    val_dict: &std::collections::HashMap<u32, String>,
) -> (TurnRuleKind, bool, u32, bool) {
    if !model.respect_turn_restrictions {
        return (TurnRuleKind::None, false, 0, false);
    }

    // Build reverse key map for this relation's tags
    let rev_key: std::collections::HashMap<&str, u32> =
        key_dict.iter().map(|(id, s)| (s.as_str(), *id)).collect();

    // Check mode-specific restriction tag first (takes precedence)
    if let Some(mode_key_id) = model.mode_restriction_key_id
        && let Some(restriction_val_id) = find_value_for_key(tags_keys, tags_vals, mode_key_id)
        && let Some(val_str) = val_dict.get(&restriction_val_id)
    {
        let kind = parse_restriction_kind(val_str);
        if kind != TurnRuleKind::None {
            return (kind, true, 0, false);
        }
    }

    // Fall back to generic restriction tag
    if let Some(restriction_key_id) = model.restriction_key_id
        && let Some(restriction_val_id) =
            find_value_for_key(tags_keys, tags_vals, restriction_key_id)
        && let Some(val_str) = val_dict.get(&restriction_val_id)
    {
        let kind = parse_restriction_kind(val_str);
        if kind != TurnRuleKind::None {
            // Check for exceptions
            if let Some(&except_key_id) = rev_key.get("except")
                && let Some(except_val_id) = find_value_for_key(tags_keys, tags_vals, except_key_id)
                && let Some(except_str) = val_dict.get(&except_val_id)
            {
                for &exc_vid in &model.exception_value_ids {
                    if let Some(exc_str) = val_dict.get(&exc_vid)
                        && except_str.contains(exc_str.as_str())
                    {
                        return (TurnRuleKind::None, false, 0, false);
                    }
                }
            }

            // Non-motor-vehicle modes: generic restrictions don't apply
            if !is_motor_vehicle_mode(&model.name) {
                return (TurnRuleKind::None, false, 0, false);
            }

            // Check conditional
            let is_time_dep = check_conditional_with_key_dict(tags_keys, key_dict);

            return (kind, true, 0, is_time_dep);
        }
    }

    (TurnRuleKind::None, false, 0, false)
}

/// Check for conditional restriction tags using key_dict
fn check_conditional_with_key_dict(
    tags_keys: &[u32],
    key_dict: &std::collections::HashMap<u32, String>,
) -> bool {
    for &kid in tags_keys {
        if let Some(key_str) = key_dict.get(&kid)
            && key_str.contains("conditional")
        {
            return true;
        }
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::{ModelSchema, compile_model};
    use std::collections::HashMap;

    // Dictionary ids used by every test below.
    const K_HIGHWAY: u32 = 1;
    const K_FOOT: u32 = 2;
    const K_BICYCLE: u32 = 3;
    const K_SIDEWALK: u32 = 4;

    const V_MOTORWAY: u32 = 1;
    const V_MOTORWAY_LINK: u32 = 2;
    const V_RESIDENTIAL: u32 = 3;
    const V_YES: u32 = 4;
    const V_BOTH: u32 = 5;

    fn dicts() -> (HashMap<u32, String>, HashMap<u32, String>) {
        let key_dict: HashMap<u32, String> = [
            (K_HIGHWAY, "highway"),
            (K_FOOT, "foot"),
            (K_BICYCLE, "bicycle"),
            (K_SIDEWALK, "sidewalk"),
        ]
        .into_iter()
        .map(|(id, s)| (id, s.to_string()))
        .collect();
        let val_dict: HashMap<u32, String> = [
            (V_MOTORWAY, "motorway"),
            (V_MOTORWAY_LINK, "motorway_link"),
            (V_RESIDENTIAL, "residential"),
            (V_YES, "yes"),
            (V_BOTH, "both"),
        ]
        .into_iter()
        .map(|(id, s)| (id, s.to_string()))
        .collect();
        (key_dict, val_dict)
    }

    /// Compile a SHIPPED model (models/<name>.model.json) against the
    /// test dictionaries — exactly the path step2 profiling takes.
    fn compile_shipped(name: &str) -> (CompiledModel, HashMap<u32, String>) {
        let path = format!(
            "{}/../models/{}.model.json",
            env!("CARGO_MANIFEST_DIR"),
            name
        );
        let json = std::fs::read_to_string(&path).unwrap();
        let schema: ModelSchema = serde_json::from_str(&json).unwrap();
        let (key_dict, val_dict) = dicts();
        let compiled = compile_model(&schema, 0, [0u8; 32], &key_dict, &val_dict);
        (compiled, val_dict)
    }

    fn assert_no_access(out: &WayOutput) {
        assert!(!out.access_fwd, "access_fwd must be false");
        assert!(!out.access_rev, "access_rev must be false");
        assert_eq!(out.base_speed_mmps, 0, "speed must be 0 for denied way");
    }

    /// #470: `highway=motorway` + `foot=yes` must still evaluate to
    /// no-access for foot. Pedestrians are banned on motorways
    /// regardless of way tags (Vienna-convention semantics).
    #[test]
    fn foot_motorway_foot_yes_still_denied() {
        let (model, val_dict) = compile_shipped("foot");
        let out = evaluate_way(
            &model,
            &[K_HIGHWAY, K_FOOT],
            &[V_MOTORWAY, V_YES],
            &val_dict,
        );
        assert_no_access(&out);
    }

    /// #470: `highway=motorway_link` + `foot=yes` + `sidewalk=both`
    /// must still evaluate to no-access for foot.
    #[test]
    fn foot_motorway_link_sidewalk_still_denied() {
        let (model, val_dict) = compile_shipped("foot");
        let out = evaluate_way(
            &model,
            &[K_HIGHWAY, K_FOOT, K_SIDEWALK],
            &[V_MOTORWAY_LINK, V_YES, V_BOTH],
            &val_dict,
        );
        assert_no_access(&out);
    }

    /// #470: same ban class for bike — `highway=motorway` +
    /// `bicycle=yes` must still evaluate to no-access.
    #[test]
    fn bike_motorway_bicycle_yes_still_denied() {
        let (model, val_dict) = compile_shipped("bike");
        let out = evaluate_way(
            &model,
            &[K_HIGHWAY, K_BICYCLE],
            &[V_MOTORWAY, V_YES],
            &val_dict,
        );
        assert_no_access(&out);

        let out = evaluate_way(
            &model,
            &[K_HIGHWAY, K_BICYCLE],
            &[V_MOTORWAY_LINK, V_YES],
            &val_dict,
        );
        assert_no_access(&out);
    }

    /// #470 precedence proof: even a model that explicitly maps
    /// `motorway` to accessible AND carries a matching `allow_if` rule
    /// is overridden by `hard_deny_highways`. This is the structural
    /// guarantee — no future model edit or rule can re-grant access to
    /// a hard-denied highway class.
    #[test]
    fn hard_deny_beats_class_allow_and_allow_if() {
        let json = r#"{
            "name": "adversarial",
            "version": 1,
            "speed": {"unit": "km/h", "highway": {"motorway": 5}, "overrides": []},
            "access": {
                "highway": {"motorway": true, "motorway_link": true},
                "allow_if": [{"if": {"foot": "yes"}, "speed_kmh": 5}],
                "hard_deny_highways": ["motorway", "motorway_link"]
            },
            "oneway": {"respect": false, "tag": "oneway", "forward_values": [], "reverse_values": [], "default_oneway_highways": []},
            "priority": [],
            "highway_class": {},
            "class_bits": {},
            "turn_penalties": {"turn_penalty_s": 0, "turn_bias": 1.0, "u_turn_penalty_s": 0, "min_degree_for_penalty": 3, "signal_delay_s": 0, "class_change_penalty_s_per_diff": 0, "max_class_diff_for_penalty": 0},
            "turn_restrictions": {"respect": false, "restriction_tag": "restriction", "exception_values": []}
        }"#;
        let schema: ModelSchema = serde_json::from_str(json).unwrap();
        let (key_dict, val_dict) = dicts();
        let model = compile_model(&schema, 0, [0u8; 32], &key_dict, &val_dict);

        for hw in [V_MOTORWAY, V_MOTORWAY_LINK] {
            let out = evaluate_way(&model, &[K_HIGHWAY, K_FOOT], &[hw, V_YES], &val_dict);
            assert_no_access(&out);
        }
    }

    /// Positive control: the hard deny must not leak onto ordinary
    /// roads — `highway=residential` stays accessible for foot/bike.
    #[test]
    fn residential_still_allowed_for_foot_and_bike() {
        for name in ["foot", "bike"] {
            let (model, val_dict) = compile_shipped(name);
            let out = evaluate_way(&model, &[K_HIGHWAY], &[V_RESIDENTIAL], &val_dict);
            assert!(out.access_fwd, "{name}: residential must stay accessible");
            assert!(out.access_rev, "{name}: residential must stay accessible");
            assert!(out.base_speed_mmps > 0, "{name}: residential speed > 0");
        }
    }

    /// Regression control: car has no hard deny — motorway stays
    /// accessible for car.
    #[test]
    fn car_motorway_still_allowed() {
        let (model, val_dict) = compile_shipped("car");
        let out = evaluate_way(&model, &[K_HIGHWAY], &[V_MOTORWAY], &val_dict);
        assert!(out.access_fwd, "car must keep motorway access");
        assert!(out.base_speed_mmps > 0);
    }
}
