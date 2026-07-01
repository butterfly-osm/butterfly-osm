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
    // #490: the `turn_penalties` block is intentionally NOT parsed here. It is
    // the single-sourced by `ebg::turn_penalty` (`for_mode`), which re-reads
    // the model JSON at step 4 and is the only consumer that feeds the turn
    // table. ModelSchema has no `deny_unknown_fields`, so the block in the JSON
    // is simply ignored on the compile path.
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
    /// Unconditional highway-type bans (#470) — legal class bans that no
    /// way tag or rule may override. Highest precedence in evaluation:
    /// a highway type listed here is denied even if `highway` maps it to
    /// `true`, even if an `allow_if` rule matches, and regardless of way
    /// tags (`foot=yes`, `sidewalk=*`, ...). Example: pedestrians and
    /// cyclists are banned on `motorway`/`motorway_link` under
    /// Vienna-convention semantics no matter how the way is tagged.
    #[serde(default)]
    pub hard_deny_highways: Vec<String>,
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
    /// #478: OSM access-hierarchy escape — the deny is SKIPPED when this
    /// more-specific tag carries one of these values (e.g. the generic
    /// `access=no` must not deny a car when `motor_vehicle=yes`: per the
    /// OSM hierarchy, specific transport-mode tags override `access`).
    #[serde(default)]
    pub unless: Option<DenyUnless>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DenyUnless {
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

// #490: `TurnPenaltySchema` moved to `ebg::turn_penalty` as the single typed
// source for the `turn_penalties` block. The compile path no longer parses it.

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
        // Speed table reverted to OSM legal limits in #390 (motorway
        // 90→120, primary 65→90 etc.). Test the schema parses + key
        // presence rather than pinning a specific speed value; the
        // numeric values evolve as the realistic-friction work moves
        // from base table into profile multipliers.
        assert!(model.speed.highway.contains_key("motorway"));
        assert!(model.access.highway.get("motorway") == Some(&true));
        assert!(model.access.highway.get("footway") == Some(&false));
        assert!(model.oneway.respect);
        // #490: turn_penalties no longer parsed here — tested in ebg::turn_penalty.
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
        // #470: cyclists are hard-banned from motorways regardless of tags.
        assert_eq!(
            model.access.hard_deny_highways,
            vec!["motorway", "motorway_link"]
        );
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
        // #470: pedestrians are hard-banned from motorways regardless of
        // tags (foot=yes / sidewalk=* must not override the class ban).
        assert_eq!(
            model.access.hard_deny_highways,
            vec!["motorway", "motorway_link"]
        );
    }

    #[test]
    fn test_pedestrian_class_models_carry_motorway_hard_deny() {
        // #470: every non-motorized / moped-class model ships the
        // unconditional motorway ban. Car/truck/motorcycle must NOT.
        for name in ["foot", "bike", "wheelchair", "scooter"] {
            let json = std::fs::read_to_string(format!(
                "{}/../models/{}.model.json",
                env!("CARGO_MANIFEST_DIR"),
                name
            ))
            .unwrap();
            let model: ModelSchema = serde_json::from_str(&json).unwrap();
            assert_eq!(
                model.access.hard_deny_highways,
                vec!["motorway", "motorway_link"],
                "{name} must hard-deny motorway + motorway_link"
            );
        }
        for name in ["car", "truck", "motorcycle"] {
            let json = std::fs::read_to_string(format!(
                "{}/../models/{}.model.json",
                env!("CARGO_MANIFEST_DIR"),
                name
            ))
            .unwrap();
            let model: ModelSchema = serde_json::from_str(&json).unwrap();
            assert!(
                model.access.hard_deny_highways.is_empty(),
                "{name} must not carry a motorway hard deny"
            );
        }
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
