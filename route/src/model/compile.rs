//! Model compiler - compile JSON schema against tag dictionaries into dense tables
//!
//! The compiled model uses dense Vec<T> arrays indexed by dictionary value_id,
//! giving O(1) evaluation at runtime with zero hashing overhead.

use std::collections::HashMap;

use crate::ebg::turn_penalty::TurnPenaltyConfig;

use super::schema::*;

/// Compiled speed override
#[derive(Debug, Clone)]
pub struct CompiledSpeedOverride {
    /// (key_id, value_id) pairs that must ALL match
    pub conditions: Vec<(u32, u32)>,
    /// Speed to set in mm/s
    pub limit_to_mmps: u32,
}

/// Compiled deny rule: a key_id and a set of denied value_ids
#[derive(Debug, Clone)]
pub struct CompiledDenyRule {
    pub key_id: u32,
    /// Dense bool array indexed by value_id — true means denied
    pub denied_values: Vec<bool>,
}

/// Compiled priority rule
#[derive(Debug, Clone)]
pub struct CompiledPriorityRule {
    /// (key_id, list of matching value_ids) — all keys must match at least one value
    pub conditions: Vec<(u32, Vec<u32>)>,
    /// Per-km penalty in deciseconds (additive, derived from multiply_by)
    /// penalty = base_penalty_per_km * (1/multiply_by - 1)
    /// We store the multiply_by and compute at evaluation time since base speed varies
    pub multiply_by: f64,
}

/// Compiled allow_if rule: grants access to otherwise-denied highway types
#[derive(Debug, Clone)]
pub struct CompiledAllowRule {
    /// (key_id, value_id) pairs that must ALL match
    pub conditions: Vec<(u32, u32)>,
    /// Speed in mm/s to assign when this rule matches
    pub speed_mmps: u32,
}

/// Compiled class bit rule
#[derive(Debug, Clone)]
pub enum CompiledClassBitRule {
    TagValue { key_id: u32, value_id: u32 },
    Highway { value_id: u32 },
    HighwaySuffix { suffix: String },
    HighwayAny { value_ids: Vec<u32> },
}

/// Fully compiled model ready for fast evaluation
#[derive(Debug, Clone)]
pub struct CompiledModel {
    pub name: String,
    pub version: u32,
    pub mode_index: u8,
    pub model_sha256: [u8; 32],

    // Speed: dense array indexed by highway value_id -> speed_mmps (0 = not accessible)
    pub speed_table: Vec<u32>,
    pub speed_cap_mmps: u32,
    pub speed_overrides: Vec<CompiledSpeedOverride>,

    // Access: dense array indexed by highway value_id -> accessible
    pub access_table: Vec<bool>,
    pub deny_rules: Vec<CompiledDenyRule>,
    pub allow_if_rules: Vec<CompiledAllowRule>,
    /// Unconditional legal class bans (#470), dense by highway value_id.
    /// Checked FIRST in evaluation — beats `access_table`, `allow_if_rules`,
    /// and every way tag.
    pub hard_deny_table: Vec<bool>,

    // Oneway
    pub respect_oneway: bool,
    pub oneway_key_id: Option<u32>,
    pub forward_value_ids: Vec<u32>,
    pub reverse_value_ids: Vec<u32>,
    pub default_oneway_highway_ids: Vec<u32>,

    // Priority
    pub priority_rules: Vec<CompiledPriorityRule>,

    // Highway class: dense array indexed by highway value_id -> class_u16
    pub highway_class_table: Vec<u16>,

    // Class bits: (bit_position, rule)
    pub class_bit_rules: Vec<(u32, CompiledClassBitRule)>,

    // Highway key_id for fast lookup
    pub highway_key_id: Option<u32>,

    // Turn penalties
    pub turn_penalty_config: TurnPenaltyConfig,

    // Turn restrictions
    pub respect_turn_restrictions: bool,
    pub restriction_key_id: Option<u32>,
    pub mode_restriction_key_id: Option<u32>,
    pub exception_value_ids: Vec<u32>,
}

impl CompiledModel {
    /// Create a minimal empty model for testing.
    /// All tables are empty, so no highway tag matches → no access granted.
    pub fn empty_for_test() -> Self {
        Self {
            name: "test".to_string(),
            version: 1,
            mode_index: 0,
            model_sha256: [0u8; 32],
            speed_table: vec![],
            speed_cap_mmps: 0,
            speed_overrides: vec![],
            access_table: vec![],
            deny_rules: vec![],
            allow_if_rules: vec![],
            hard_deny_table: vec![],
            respect_oneway: false,
            oneway_key_id: None,
            forward_value_ids: vec![],
            reverse_value_ids: vec![],
            default_oneway_highway_ids: vec![],
            priority_rules: vec![],
            highway_class_table: vec![],
            class_bit_rules: vec![],
            highway_key_id: None,
            turn_penalty_config: TurnPenaltyConfig::default_identity(),
            respect_turn_restrictions: false,
            restriction_key_id: None,
            mode_restriction_key_id: None,
            exception_value_ids: vec![],
        }
    }
}

/// Convert km/h to mm/s (integer), capped
pub fn kmh_to_mmps(kmh: f64, cap_mmps: u32) -> u32 {
    ((kmh * 1000.0 / 3.6).round() as u32).min(cap_mmps)
}

/// Compile a model schema against tag dictionaries
pub fn compile_model(
    schema: &ModelSchema,
    mode_index: u8,
    model_sha256: [u8; 32],
    key_dict: &HashMap<u32, String>,
    val_dict: &HashMap<u32, String>,
) -> CompiledModel {
    // Build reverse dictionaries: string -> id
    let rev_key: HashMap<&str, u32> = key_dict.iter().map(|(id, s)| (s.as_str(), *id)).collect();
    let rev_val: HashMap<&str, u32> = val_dict.iter().map(|(id, s)| (s.as_str(), *id)).collect();

    let max_val_id = val_dict.keys().copied().max().unwrap_or(0) as usize;
    let table_len = max_val_id + 1;

    let highway_key_id = rev_key.get("highway").copied();

    // --- Speed table ---
    let speed_cap_mmps = kmh_to_mmps(schema.speed.speed_cap_kmh, u32::MAX);
    let mut speed_table = vec![0u32; table_len];
    for (highway_type, &speed_kmh) in &schema.speed.highway {
        if let Some(&vid) = rev_val.get(highway_type.as_str()) {
            speed_table[vid as usize] = kmh_to_mmps(speed_kmh, speed_cap_mmps);
        }
    }

    // Speed overrides
    let speed_overrides: Vec<CompiledSpeedOverride> = schema
        .speed
        .overrides
        .iter()
        .map(|ovr| {
            let conditions = compile_tag_conditions(&ovr.condition, &rev_key, &rev_val);
            CompiledSpeedOverride {
                conditions,
                limit_to_mmps: kmh_to_mmps(ovr.limit_to, speed_cap_mmps),
            }
        })
        .collect();

    // --- Access table ---
    let mut access_table = vec![false; table_len];
    for (highway_type, &accessible) in &schema.access.highway {
        if let Some(&vid) = rev_val.get(highway_type.as_str()) {
            access_table[vid as usize] = accessible;
        }
    }

    // Deny rules
    let deny_rules: Vec<CompiledDenyRule> = schema
        .access
        .deny_if
        .iter()
        .filter_map(|rule| {
            let key_id = *rev_key.get(rule.tag.as_str())?;
            let mut denied_values = vec![false; table_len];
            for val_str in &rule.values {
                if let Some(&vid) = rev_val.get(val_str.as_str()) {
                    denied_values[vid as usize] = true;
                }
            }
            Some(CompiledDenyRule {
                key_id,
                denied_values,
            })
        })
        .collect();

    // --- Hard-deny table (#470: unconditional legal class bans) ---
    let mut hard_deny_table = vec![false; table_len];
    for highway_type in &schema.access.hard_deny_highways {
        if let Some(&vid) = rev_val.get(highway_type.as_str()) {
            hard_deny_table[vid as usize] = true;
        }
    }

    // --- Allow-if rules (conditional access overrides) ---
    let allow_if_rules: Vec<CompiledAllowRule> = schema
        .access
        .allow_if
        .iter()
        .map(|rule| {
            let conditions: Vec<(u32, u32)> = rule
                .condition
                .iter()
                .filter_map(|(key, val)| {
                    let kid = *rev_key.get(key.as_str())?;
                    let val_str = val.as_str()?;
                    let vid = *rev_val.get(val_str)?;
                    Some((kid, vid))
                })
                .collect();
            let speed_mmps = (rule.speed_kmh * 1_000_000.0 / 3_600.0) as u32;
            CompiledAllowRule {
                conditions,
                speed_mmps,
            }
        })
        .collect();

    // --- Oneway ---
    let oneway_key_id = rev_key.get(schema.oneway.tag.as_str()).copied();

    let forward_value_ids: Vec<u32> = schema
        .oneway
        .forward_values
        .iter()
        .filter_map(|v| rev_val.get(v.as_str()).copied())
        .collect();

    let reverse_value_ids: Vec<u32> = schema
        .oneway
        .reverse_values
        .iter()
        .filter_map(|v| rev_val.get(v.as_str()).copied())
        .collect();

    let default_oneway_highway_ids: Vec<u32> = schema
        .oneway
        .default_oneway_highways
        .iter()
        .filter_map(|h| rev_val.get(h.as_str()).copied())
        .collect();

    // --- Priority rules ---
    let priority_rules: Vec<CompiledPriorityRule> = schema
        .priority
        .iter()
        .map(|rule| {
            let conditions = compile_priority_conditions(&rule.condition, &rev_key, &rev_val);
            CompiledPriorityRule {
                conditions,
                multiply_by: rule.multiply_by,
            }
        })
        .collect();

    // --- Highway class table ---
    let mut highway_class_table = vec![0u16; table_len];
    for (highway_type, &class) in &schema.highway_class {
        if let Some(&vid) = rev_val.get(highway_type.as_str()) {
            highway_class_table[vid as usize] = class;
        }
    }

    // --- Class bits ---
    let class_bit_rules: Vec<(u32, CompiledClassBitRule)> = schema
        .class_bits
        .iter()
        .filter_map(|(bit_name, rule)| {
            let bit_pos = class_bit_position(bit_name)?;
            let compiled = compile_class_bit_rule(rule, &rev_key, &rev_val)?;
            Some((bit_pos, compiled))
        })
        .collect();

    // --- Turn penalty config (post-#297: values in seconds) ---
    let tp = &schema.turn_penalties;
    let turn_penalty_config = TurnPenaltyConfig {
        turn_penalty_s: tp.turn_penalty_s,
        turn_bias: tp.turn_bias,
        u_turn_penalty_s: tp.u_turn_penalty_s,
        min_degree_for_penalty: tp.min_degree_for_penalty,
        signal_delay_s: tp.signal_delay_s,
        class_change_penalty_s_per_diff: tp.class_change_penalty_s_per_diff,
        max_class_diff_for_penalty: tp.max_class_diff_for_penalty,
    };

    // --- Turn restrictions ---
    let restriction_key_id = rev_key
        .get(schema.turn_restrictions.restriction_tag.as_str())
        .copied();
    let mode_restriction_key_id = schema
        .turn_restrictions
        .mode_specific_tag
        .as_ref()
        .and_then(|t| rev_key.get(t.as_str()).copied());

    let exception_value_ids: Vec<u32> = schema
        .turn_restrictions
        .exception_values
        .iter()
        .filter_map(|v| rev_val.get(v.as_str()).copied())
        .collect();

    CompiledModel {
        name: schema.name.clone(),
        version: schema.version,
        mode_index,
        model_sha256,

        speed_table,
        speed_cap_mmps,
        speed_overrides,

        access_table,
        deny_rules,
        allow_if_rules,
        hard_deny_table,

        respect_oneway: schema.oneway.respect,
        oneway_key_id,
        forward_value_ids,
        reverse_value_ids,
        default_oneway_highway_ids,

        priority_rules,

        highway_class_table,

        class_bit_rules,
        highway_key_id,

        turn_penalty_config,

        respect_turn_restrictions: schema.turn_restrictions.respect,
        restriction_key_id,
        mode_restriction_key_id,
        exception_value_ids,
    }
}

/// Map class bit name to bit position
fn class_bit_position(name: &str) -> Option<u32> {
    use crate::profile_abi::class_bits;
    match name {
        "toll" => Some(class_bits::TOLL),
        "ferry" => Some(class_bits::FERRY),
        "tunnel" => Some(class_bits::TUNNEL),
        "bridge" => Some(class_bits::BRIDGE),
        "link" => Some(class_bits::LINK),
        "residential" => Some(class_bits::RESIDENTIAL),
        "track" => Some(class_bits::TRACK),
        "cycleway" => Some(class_bits::CYCLEWAY),
        "footway" => Some(class_bits::FOOTWAY),
        "living_street" => Some(class_bits::LIVING_STREET),
        "service" => Some(class_bits::SERVICE),
        "construction" => Some(class_bits::CONSTRUCTION),
        _ => None,
    }
}

/// Compile a class bit rule
fn compile_class_bit_rule(
    rule: &ClassBitRule,
    rev_key: &HashMap<&str, u32>,
    rev_val: &HashMap<&str, u32>,
) -> Option<CompiledClassBitRule> {
    match rule {
        ClassBitRule::TagValue { tag, value } => {
            let key_id = *rev_key.get(tag.as_str())?;
            let value_id = *rev_val.get(value.as_str())?;
            Some(CompiledClassBitRule::TagValue { key_id, value_id })
        }
        ClassBitRule::Highway { highway } => {
            let value_id = *rev_val.get(highway.as_str())?;
            Some(CompiledClassBitRule::Highway { value_id })
        }
        ClassBitRule::HighwaySuffix { highway_suffix } => {
            Some(CompiledClassBitRule::HighwaySuffix {
                suffix: highway_suffix.clone(),
            })
        }
        ClassBitRule::HighwayAny { highway_any } => {
            let value_ids: Vec<u32> = highway_any
                .iter()
                .filter_map(|h| rev_val.get(h.as_str()).copied())
                .collect();
            if value_ids.is_empty() {
                return None;
            }
            Some(CompiledClassBitRule::HighwayAny { value_ids })
        }
    }
}

/// Compile tag conditions from a HashMap<String, Value> into (key_id, value_id) pairs
fn compile_tag_conditions(
    conditions: &HashMap<String, serde_json::Value>,
    rev_key: &HashMap<&str, u32>,
    rev_val: &HashMap<&str, u32>,
) -> Vec<(u32, u32)> {
    let mut compiled = Vec::new();
    for (key_str, val_json) in conditions {
        if let Some(&key_id) = rev_key.get(key_str.as_str())
            && let Some(val_str) = val_json.as_str()
            && let Some(&val_id) = rev_val.get(val_str)
        {
            compiled.push((key_id, val_id));
        }
    }
    compiled
}

/// Compile priority conditions (key -> [values]) into (key_id, [value_ids])
fn compile_priority_conditions(
    conditions: &HashMap<String, serde_json::Value>,
    rev_key: &HashMap<&str, u32>,
    rev_val: &HashMap<&str, u32>,
) -> Vec<(u32, Vec<u32>)> {
    let mut compiled = Vec::new();
    for (key_str, val_json) in conditions {
        if let Some(&key_id) = rev_key.get(key_str.as_str()) {
            let value_ids: Vec<u32> = match val_json {
                serde_json::Value::Array(arr) => arr
                    .iter()
                    .filter_map(|v| v.as_str())
                    .filter_map(|s| rev_val.get(s).copied())
                    .collect(),
                serde_json::Value::String(s) => {
                    rev_val.get(s.as_str()).copied().into_iter().collect()
                }
                _ => Vec::new(),
            };
            if !value_ids.is_empty() {
                compiled.push((key_id, value_ids));
            }
        }
    }
    compiled
}
