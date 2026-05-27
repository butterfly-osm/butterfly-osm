//! JSON model schema - serde structs for declarative routing profiles

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Root schema for a model JSON file
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ModelSchema {
    pub name: String,
    pub version: u32,

    pub speed: SpeedConfig,
    pub access: AccessConfig,
    pub oneway: OnewayConfig,
    pub priority: Vec<PriorityRule>,
    pub highway_class: HashMap<String, u16>,
    pub class_bits: HashMap<String, ClassBitRule>,
    pub turn_penalties: TurnPenaltySchema,
    pub turn_restrictions: TurnRestrictionConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedConfig {
    pub unit: String,
    pub highway: HashMap<String, f64>,
    #[serde(default = "default_speed_cap")]
    pub speed_cap_kmh: f64,
    #[serde(default)]
    pub overrides: Vec<SpeedOverride>,
}

fn default_speed_cap() -> f64 {
    288.0
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SpeedOverride {
    #[serde(rename = "if")]
    pub condition: HashMap<String, serde_json::Value>,
    pub limit_to: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AccessConfig {
    pub highway: HashMap<String, bool>,
    #[serde(default)]
    pub deny_if: Vec<DenyRule>,
    /// Conditional access overrides: grant access even if highway type is denied.
    /// Evaluated after highway lookup but before deny_if.
    /// Example: allow track if tracktype=grade1 or tracktype=grade2.
    #[serde(default)]
    pub allow_if: Vec<AllowRule>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct AllowRule {
    /// Tag conditions that must ALL match (AND logic)
    #[serde(rename = "if")]
    pub condition: HashMap<String, serde_json::Value>,
    /// Speed to assign when this rule matches (km/h). Required for allowing otherwise-denied roads.
    pub speed_kmh: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DenyRule {
    pub tag: String,
    pub values: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct OnewayConfig {
    pub respect: bool,
    pub tag: String,
    #[serde(default)]
    pub forward_values: Vec<String>,
    #[serde(default)]
    pub reverse_values: Vec<String>,
    #[serde(default)]
    pub default_oneway_highways: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PriorityRule {
    #[serde(rename = "if")]
    pub condition: HashMap<String, serde_json::Value>,
    pub multiply_by: f64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(untagged)]
pub enum ClassBitRule {
    TagValue { tag: String, value: String },
    Highway { highway: String },
    HighwaySuffix { highway_suffix: String },
    HighwayAny { highway_any: Vec<String> },
}

/// Turn-penalty section of a model JSON. Values are whole seconds
/// (post-#297; v1 used deciseconds). The `deny_unknown_fields` attribute
/// makes the loader REJECT pre-#297 JSON files (which used `_ds` keys),
/// pointing the user at the migration.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct TurnPenaltySchema {
    /// Maximum turn penalty in seconds (was deciseconds in pre-#297 models).
    pub turn_penalty_s: u32,
    pub turn_bias: f64,
    /// Additional U-turn penalty in seconds (was deciseconds).
    pub u_turn_penalty_s: u32,
    pub min_degree_for_penalty: u8,
    /// Traffic-signal delay in seconds (was deciseconds).
    pub signal_delay_s: u32,
    /// Per-class-step transition penalty in seconds (was deciseconds).
    pub class_change_penalty_s_per_diff: u32,
    pub max_class_diff_for_penalty: u8,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TurnRestrictionConfig {
    pub respect: bool,
    pub restriction_tag: String,
    #[serde(default)]
    pub mode_specific_tag: Option<String>,
    #[serde(default)]
    pub exception_values: Vec<String>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_car_model() {
        let json = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../models/car.model.json"
        ))
        .unwrap();
        let model: ModelSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(model.name, "car");
        assert_eq!(model.version, 1);
        // Speed table re-reverted to OSM legal limits in #390 (motorway
        // 90→120, primary 65→90 etc.). Test the schema parses + key
        // presence rather than pinning a specific speed value; the
        // numeric values evolve as the realistic-friction work moves
        // from base table into profile multipliers.
        assert!(model.speed.highway.contains_key("motorway"));
        assert!(model.access.highway.get("motorway") == Some(&true));
        assert!(model.access.highway.get("footway") == Some(&false));
        assert!(model.oneway.respect);
        // car turn_penalty: 75 ds → 8 s (round-half-to-even: 7.5 → 8 since 7 is odd → 8).
        assert_eq!(model.turn_penalties.turn_penalty_s, 8);
    }

    #[test]
    fn test_parse_bike_model() {
        let json = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../models/bike.model.json"
        ))
        .unwrap();
        let model: ModelSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(model.name, "bike");
        assert!(!model.oneway.respect);
        assert_eq!(model.turn_penalties.turn_bias, 1.4);
    }

    #[test]
    fn test_parse_foot_model() {
        let json = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../models/foot.model.json"
        ))
        .unwrap();
        let model: ModelSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(model.name, "foot");
        assert!(!model.oneway.respect);
        assert_eq!(model.turn_penalties.turn_bias, 1.0);
        assert_eq!(model.turn_penalties.u_turn_penalty_s, 0);
    }

    #[test]
    fn test_parse_truck_model() {
        let json = std::fs::read_to_string(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/../models/truck.model.json"
        ))
        .unwrap();
        let model: ModelSchema = serde_json::from_str(&json).unwrap();
        assert_eq!(model.name, "truck");
        assert_eq!(model.speed.highway.get("motorway"), Some(&90.0));
        assert!(!model.priority.is_empty());
        assert_eq!(
            model.turn_restrictions.mode_specific_tag,
            Some("restriction:hgv".to_string())
        );
    }
}
