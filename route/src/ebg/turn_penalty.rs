//! Turn penalty cost model - OSRM-compatible sigmoid-based turn costs
//!
//! Implements OSRM's exact turn penalty formula from car.lua:
//! - Sigmoid function mapping angle to penalty
//! - turn_penalty = 7.5 seconds (max) — stored as 8 s post-#297 (round-half-to-even)
//! - turn_bias = 1.075 (right-turn preference for right-hand traffic)
//! - u_turn_penalty = 20 seconds additional
//!
//! Reference: https://github.com/Project-OSRM/osrm-backend/blob/master/profiles/car.lua

/// Schema for parsing model JSON files (turn_penalties section).
/// Values are whole seconds (post-#297; v1 used deciseconds with `_ds` keys
/// which are now rejected).
#[derive(Debug, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct TurnPenaltySchema {
    pub turn_penalty_s: u32,
    #[serde(default = "default_turn_bias")]
    pub turn_bias: f64,
    #[serde(default)]
    pub u_turn_penalty_s: u32,
    #[serde(default = "default_min_degree")]
    pub min_degree_for_penalty: u8,
    #[serde(default)]
    pub signal_delay_s: u32,
    #[serde(default)]
    pub class_change_penalty_s_per_diff: u32,
    #[serde(default)]
    pub max_class_diff_for_penalty: u8,
    // Class-scaled per-junction friction: at every real junction, add
    // `min(max(0, max(from_class,to_class) - ref_class), max_levels) * per_level`
    // seconds. Unlike the angle penalty it applies to straight-throughs too, so
    // it accumulates along a chain of minor junctions (rat-run deterrence).
    // per_level == 0 disables it. See `compute_turn_penalty`.
    #[serde(default)]
    pub junction_class_penalty_s_per_level: u32,
    #[serde(default)]
    pub junction_class_penalty_ref_class: u16,
    #[serde(default)]
    pub junction_class_penalty_max_levels: u8,
}
fn default_turn_bias() -> f64 {
    1.0
}
fn default_min_degree() -> u8 {
    3
}

/// Top-level model JSON schema (only the fields we need).
#[derive(Debug, serde::Deserialize)]
struct ModelSchema {
    turn_penalties: TurnPenaltySchema,
}

/// Turn geometry for a single turn (a → b at intersection)
#[derive(Debug, Clone)]
pub struct TurnGeometry {
    pub from_bearing_deci: u16, // 0-3599 (deci-degrees, 0 = North)
    pub to_bearing_deci: u16,
    pub angle_deg: i16, // Signed turn angle (-180 to +180)
    pub is_uturn: bool,
    pub via_degree: u8,          // Intersection complexity
    pub via_has_signal: bool,    // Traffic signal at intersection
    pub from_highway_class: u16, // Highway class of incoming edge
    pub to_highway_class: u16,   // Highway class of outgoing edge
}

impl TurnGeometry {
    /// Compute turn geometry from bearings
    ///
    /// from_bearing: bearing of incoming edge at intersection (deci-degrees 0-3599)
    /// to_bearing: bearing of outgoing edge at intersection (deci-degrees 0-3599)
    /// via_has_signal: whether the via node has a traffic signal
    /// via_degree: in_degree + out_degree at via node
    /// from_highway_class: highway class of incoming edge (0-99)
    /// to_highway_class: highway class of outgoing edge (0-99)
    pub fn compute(
        from_bearing_deci: u16,
        to_bearing_deci: u16,
        via_has_signal: bool,
        via_degree: u8,
        from_highway_class: u16,
        to_highway_class: u16,
    ) -> Self {
        // Handle NA bearings (65535)
        if from_bearing_deci == 65535 || to_bearing_deci == 65535 {
            return Self {
                from_bearing_deci,
                to_bearing_deci,
                angle_deg: 0,
                is_uturn: false,
                via_degree,
                via_has_signal,
                from_highway_class,
                to_highway_class,
            };
        }

        // Convert deci-degrees to degrees for calculation
        let from_deg = from_bearing_deci as f64 / 10.0;
        let to_deg = to_bearing_deci as f64 / 10.0;

        // Turn angle: how much we turn from current heading
        // Positive = right turn, Negative = left turn
        // We need: outgoing_bearing - incoming_bearing, wrapped to [-180, 180]
        let mut delta = to_deg - from_deg;

        // Wrap to [-180, 180]
        while delta > 180.0 {
            delta -= 360.0;
        }
        while delta < -180.0 {
            delta += 360.0;
        }

        let angle_deg = delta.round() as i16;
        let is_uturn = angle_deg.abs() >= 170;

        Self {
            from_bearing_deci,
            to_bearing_deci,
            angle_deg,
            is_uturn,
            via_degree,
            via_has_signal,
            from_highway_class,
            to_highway_class,
        }
    }
}

/// Turn penalty configuration (mode-specific, OSRM-compatible).
/// All values are whole seconds (post-#297; was deciseconds in v1).
#[derive(Debug, Clone)]
pub struct TurnPenaltyConfig {
    /// Maximum turn penalty in seconds (OSRM: 7.5 s → rounded to 8 s).
    pub turn_penalty_s: u32,

    /// Turn bias for asymmetric left/right costs (OSRM: 1.075)
    /// >1.0 = prefer right turns (right-hand traffic countries)
    pub turn_bias: f64,

    /// Additional U-turn penalty in seconds (OSRM: 20 s).
    pub u_turn_penalty_s: u32,

    /// Minimum intersection degree to apply turn penalty
    /// (OSRM only applies at complex intersections with >2 roads)
    pub min_degree_for_penalty: u8,

    /// Traffic signal delay in seconds (typical: 2-8 s).
    /// OSRM uses variable signal penalties based on intersection complexity
    pub signal_delay_s: u32,

    /// Road class transition penalty in seconds per class difference.
    /// Applied when transitioning between different highway classes
    pub class_change_penalty_s_per_diff: u32,

    /// Maximum class difference to apply penalty (larger diffs capped)
    pub max_class_diff_for_penalty: u8,

    /// Class-scaled per-junction friction: seconds added per hierarchy level
    /// below `junction_class_penalty_ref_class`, at every real junction (not
    /// just turns). `0` disables it. Governing class = the least-important
    /// (highest-code) of the two edges, so a movement involving a minor road
    /// pays minor-road junction friction; accumulates along minor-road chains.
    pub junction_class_penalty_s_per_level: u32,

    /// Reference (arterial) class code: roads at or above this importance
    /// (code ≤ ref) pay zero junction friction. E.g. 5 = primary.
    pub junction_class_penalty_ref_class: u16,

    /// Cap on the number of levels-below-ref charged (larger drops capped).
    pub junction_class_penalty_max_levels: u8,
}

impl TurnPenaltyConfig {
    /// Identity/zero config — no penalties at all. Used as placeholder for inactive mode slots.
    pub fn default_identity() -> Self {
        Self {
            turn_penalty_s: 0,
            turn_bias: 1.0,
            u_turn_penalty_s: 0,
            min_degree_for_penalty: 255,
            signal_delay_s: 0,
            class_change_penalty_s_per_diff: 0,
            max_class_diff_for_penalty: 0,
            junction_class_penalty_s_per_level: 0,
            junction_class_penalty_ref_class: 0,
            junction_class_penalty_max_levels: 0,
        }
    }

    /// Load turn penalty config from a model JSON file for an ACTIVE mode.
    ///
    /// #491: resolves the models dir from `BUTTERFLY_MODELS_DIR` at runtime,
    /// falling back to the compile-time checkout path for local dev. HARD-ERRORS
    /// if the model file is missing or unparseable (including pre-#297 `_ds`
    /// keys, rejected by `deny_unknown_fields`) — an active mode with no model
    /// is a build error, not a silent zero-penalty fallback. `default_identity`
    /// stays for genuinely-inactive mode slots, which never call this.
    pub fn for_mode(mode_name: &str) -> anyhow::Result<Self> {
        use anyhow::Context;
        let models_dir = std::env::var_os("BUTTERFLY_MODELS_DIR")
            .map(std::path::PathBuf::from)
            .unwrap_or_else(|| {
                std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../models"))
            });
        let model_path = models_dir.join(format!("{mode_name}.model.json"));
        let content = std::fs::read_to_string(&model_path).with_context(|| {
            format!(
                "turn-penalty model for active mode '{mode_name}' not found at {} \
                 (set BUTTERFLY_MODELS_DIR to the models directory)",
                model_path.display()
            )
        })?;
        let schema: ModelSchema = serde_json::from_str(&content).with_context(|| {
            format!(
                "failed to parse turn_penalties from {}",
                model_path.display()
            )
        })?;
        Ok(Self::from_model_schema(&schema.turn_penalties))
    }

    /// Build config from model schema turn_penalties section
    fn from_model_schema(tp: &TurnPenaltySchema) -> Self {
        Self {
            turn_penalty_s: tp.turn_penalty_s,
            turn_bias: tp.turn_bias,
            u_turn_penalty_s: tp.u_turn_penalty_s,
            min_degree_for_penalty: tp.min_degree_for_penalty,
            signal_delay_s: tp.signal_delay_s,
            class_change_penalty_s_per_diff: tp.class_change_penalty_s_per_diff,
            max_class_diff_for_penalty: tp.max_class_diff_for_penalty,
            junction_class_penalty_s_per_level: tp.junction_class_penalty_s_per_level,
            junction_class_penalty_ref_class: tp.junction_class_penalty_ref_class,
            junction_class_penalty_max_levels: tp.junction_class_penalty_max_levels,
        }
    }

    /// Car mode turn penalties - matches OSRM car.lua exactly (rounded to seconds).
    pub fn car() -> Self {
        // ds → s with round-half-to-even: 75→8, 200→20, 80→8, 5→0.
        Self {
            turn_penalty_s: 8,
            turn_bias: 1.075,
            u_turn_penalty_s: 20,
            min_degree_for_penalty: 3,
            signal_delay_s: 8,
            class_change_penalty_s_per_diff: 0,
            max_class_diff_for_penalty: 6,
            junction_class_penalty_s_per_level: 0,
            junction_class_penalty_ref_class: 0,
            junction_class_penalty_max_levels: 0,
        }
    }

    /// Bike mode turn penalties
    pub fn bike() -> Self {
        // ds → s with round-half-to-even: 40→4, 50→6 (5.0→4 even? 5.0 is exact half, 5 is odd → 6),
        // wait — 50/10 = 5.0 exactly. 5 is odd so round to 6? No — banker's rounds 0.5 to nearest
        // even integer. 5.0 isn't half, it's exact. 5/10 = 0.5 only when remainder of 50/10 is 5.
        // 50/10 = 5 remainder 0 → 5 (exact). 40/10 = 4 (exact). 3→0 (rounded down from 0.3).
        Self {
            turn_penalty_s: 4,
            turn_bias: 1.4,
            u_turn_penalty_s: 5,
            min_degree_for_penalty: 3,
            signal_delay_s: 5,
            class_change_penalty_s_per_diff: 0,
            max_class_diff_for_penalty: 4,
            junction_class_penalty_s_per_level: 0,
            junction_class_penalty_ref_class: 0,
            junction_class_penalty_max_levels: 0,
        }
    }

    /// Foot mode turn penalties
    pub fn foot() -> Self {
        // ds → s: 20→2, 0→0, 40→4, 0→0.
        Self {
            turn_penalty_s: 2,
            turn_bias: 1.0,
            u_turn_penalty_s: 0,
            min_degree_for_penalty: 4,
            signal_delay_s: 4,
            class_change_penalty_s_per_diff: 0,
            max_class_diff_for_penalty: 0,
            junction_class_penalty_s_per_level: 0,
            junction_class_penalty_ref_class: 0,
            junction_class_penalty_max_levels: 0,
        }
    }
}

/// Compute turn penalty using OSRM's sigmoid formula
///
/// OSRM formula from car.lua:
/// ```lua
/// penalty = turn_penalty / (1 + math.exp(-((13 / turn_bias) * -turn.angle/180 - 6.5*turn_bias)))
/// ```
/// This is a sigmoid that:
/// - Returns ~0 for angle=0 (going straight)
/// - Returns ~turn_penalty for angle=±180 (U-turn)
/// - Is asymmetric based on turn_bias (right turns slightly cheaper)
///
/// For pedestrians (turn_bias == 1.0), we use a flat crossing penalty instead.
///
/// Traffic signals add an additional delay (typically 8 seconds for cars).
pub fn compute_turn_penalty(geom: &TurnGeometry, config: &TurnPenaltyConfig) -> u32 {
    // Only apply at intersections (not simple road continuations)
    if geom.via_degree < config.min_degree_for_penalty {
        return 0;
    }

    // Skip if no penalty configured
    if config.turn_penalty_s == 0
        && config.signal_delay_s == 0
        && config.junction_class_penalty_s_per_level == 0
    {
        return 0;
    }

    let mut penalty = 0u32;

    // For pedestrians (turn_bias == 1.0), use flat crossing penalty.
    // Pedestrians don't care about turn angle, just intersection complexity.
    if (config.turn_bias - 1.0).abs() < 0.001 {
        penalty = config.turn_penalty_s;
    } else if config.turn_penalty_s > 0 {
        let angle = geom.angle_deg as f64;
        let turn_bias = config.turn_bias;

        // OSRM sigmoid formula. The formula uses -angle because OSRM
        // convention is opposite. Positive angle = left turn in OSRM,
        // right turn in our convention.
        let exponent = -((13.0 / turn_bias) * (-angle / 180.0) - 6.5 * turn_bias);
        let sigmoid = 1.0 / (1.0 + exponent.exp());

        penalty = (config.turn_penalty_s as f64 * sigmoid).round() as u32;

        // Add U-turn penalty
        if geom.is_uturn {
            penalty = penalty.saturating_add(config.u_turn_penalty_s);
        }
    }

    // Add traffic signal delay
    if geom.via_has_signal {
        penalty = penalty.saturating_add(config.signal_delay_s);
    }

    // Add road class transition penalty
    if config.class_change_penalty_s_per_diff > 0
        && geom.from_highway_class > 0
        && geom.to_highway_class > 0
    {
        let from_class = geom.from_highway_class as i32;
        let to_class = geom.to_highway_class as i32;
        let class_diff = (from_class - to_class).unsigned_abs();
        let capped_diff = class_diff.min(config.max_class_diff_for_penalty as u32);
        let class_penalty = capped_diff * config.class_change_penalty_s_per_diff;
        penalty = penalty.saturating_add(class_penalty);
    }

    // Class-scaled per-junction friction. Applies to EVERY movement at this
    // junction (including straight-throughs the angle penalty misses), scaled
    // by how far below the arterial reference the least-important road is —
    // so a chain of minor junctions accumulates realistic give-way/crossing
    // cost, deterring rat-runs that thread through residential streets.
    if config.junction_class_penalty_s_per_level > 0
        && geom.from_highway_class > 0
        && geom.to_highway_class > 0
    {
        // Least-important road at the movement (higher code = less important).
        let governing = geom.from_highway_class.max(geom.to_highway_class);
        let levels_below = governing.saturating_sub(config.junction_class_penalty_ref_class);
        let capped = (levels_below as u32).min(config.junction_class_penalty_max_levels as u32);
        let junction_penalty = capped * config.junction_class_penalty_s_per_level;
        penalty = penalty.saturating_add(junction_penalty);
    }

    penalty
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_osrm_sigmoid_penalties() {
        let config = TurnPenaltyConfig::car();

        // Straight: ~0 penalty (same highway class so no class change penalty).
        // Values are now in seconds (post-#297).
        let geom = TurnGeometry::compute(0, 0, false, 4, 5, 5);
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(penalty < 2, "straight should be ~0, got {}s", penalty);

        // 90 degree right turn: very low penalty in right-hand traffic.
        let geom = TurnGeometry::compute(0, 900, false, 4, 5, 5);
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(
            penalty < 2,
            "90° right should be ~0 in right-hand traffic, got {}s",
            penalty
        );

        // 90 degree left turn: ~2-3 s (crossing traffic).
        let geom = TurnGeometry::compute(0, 2700, false, 4, 5, 5); // 270° bearing = -90° left turn
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(
            (1..=4).contains(&penalty),
            "90° left should be a few s, got {}s",
            penalty
        );

        // Left U-turn: ~7.5 s + 20 s = ~27.5 s (maximum penalty).
        let geom = TurnGeometry::compute(0, 1800, false, 4, 5, 5); // 180° = U-turn
        let penalty = compute_turn_penalty(&geom, &config);
        // Note: 180° could be left or right depending on interpretation.
        // The formula should give high penalty for any U-turn.
        assert!(penalty >= 20, "U-turn should be ~20+s, got {}s", penalty);

        // No penalty at simple road (degree < 3).
        let geom = TurnGeometry::compute(0, 900, false, 2, 5, 5);
        let penalty = compute_turn_penalty(&geom, &config);
        assert_eq!(penalty, 0, "no penalty at simple road continuation");
    }

    #[test]
    fn test_foot_crossing_penalty() {
        let config = TurnPenaltyConfig::foot();

        // Pedestrians get small crossing penalty at complex intersections.
        let geom = TurnGeometry::compute(0, 900, false, 5, 0, 0); // 5-way intersection
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(
            penalty > 0,
            "pedestrians should get crossing penalty at complex intersections"
        );
        assert!(
            penalty <= 4,
            "pedestrian penalty should be small, got {}s",
            penalty
        );

        // No penalty at simple intersections.
        let geom = TurnGeometry::compute(0, 900, false, 3, 0, 0);
        let penalty = compute_turn_penalty(&geom, &config);
        assert_eq!(penalty, 0, "no penalty at simple 3-way intersection");
    }

    #[test]
    fn test_left_right_asymmetry() {
        let config = TurnPenaltyConfig::car();

        // 90° right turn
        let right = TurnGeometry::compute(0, 900, false, 4, 5, 5);
        let right_penalty = compute_turn_penalty(&right, &config);

        // 90° left turn (bearing 270° = -90°)
        let left = TurnGeometry::compute(0, 2700, false, 4, 5, 5);
        let left_penalty = compute_turn_penalty(&left, &config);

        // Left turns should cost more than right turns (right-hand traffic).
        assert!(
            left_penalty > right_penalty,
            "left turn ({}s) should cost more than right turn ({}s)",
            left_penalty,
            right_penalty
        );
    }

    #[test]
    fn test_traffic_signal_delay() {
        let config = TurnPenaltyConfig::car();

        // Straight at signalized intersection: signal delay only.
        let geom_no_signal = TurnGeometry::compute(0, 0, false, 4, 5, 5);
        let geom_with_signal = TurnGeometry::compute(0, 0, true, 4, 5, 5);

        let penalty_no = compute_turn_penalty(&geom_no_signal, &config);
        let penalty_with = compute_turn_penalty(&geom_with_signal, &config);

        // With signal should add signal_delay_s.
        assert_eq!(
            penalty_with - penalty_no,
            config.signal_delay_s,
            "signal should add {}s delay, got {} vs {}",
            config.signal_delay_s,
            penalty_with,
            penalty_no
        );

        // Left turn at signalized intersection: turn penalty + signal delay.
        let left_no_signal = TurnGeometry::compute(0, 2700, false, 4, 5, 5);
        let left_with_signal = TurnGeometry::compute(0, 2700, true, 4, 5, 5);

        let penalty_left_no = compute_turn_penalty(&left_no_signal, &config);
        let penalty_left_with = compute_turn_penalty(&left_with_signal, &config);

        assert_eq!(
            penalty_left_with - penalty_left_no,
            config.signal_delay_s,
            "signal should add consistent delay to left turn"
        );
    }

    #[test]
    fn test_road_class_transition_penalty() {
        // Use a synthetic config where class-change > 0 in seconds so the test
        // can prove the additive behavior. The default car() config has
        // class_change_penalty_s_per_diff = 0 (the original 5 ds rounded to 0 s).
        let config = TurnPenaltyConfig {
            class_change_penalty_s_per_diff: 1,
            ..TurnPenaltyConfig::car()
        };

        // Same highway class: no class change penalty.
        let same_class = TurnGeometry::compute(0, 0, false, 4, 5, 5); // primary -> primary
        let penalty_same = compute_turn_penalty(&same_class, &config);

        // Different highway class: class change penalty added.
        // primary (5) -> residential (12) = diff of 7, capped to 6.
        let diff_class = TurnGeometry::compute(0, 0, false, 4, 5, 12);
        let penalty_diff = compute_turn_penalty(&diff_class, &config);

        // Should add capped_diff * class_change_penalty_s_per_diff.
        let expected_class_penalty =
            config.max_class_diff_for_penalty as u32 * config.class_change_penalty_s_per_diff;
        assert_eq!(
            penalty_diff - penalty_same,
            expected_class_penalty,
            "class transition should add {}s penalty, got {} vs {}",
            expected_class_penalty,
            penalty_diff,
            penalty_same
        );

        // Smaller class diff: motorway_link (2) -> trunk (3) = diff of 1.
        let small_diff = TurnGeometry::compute(0, 0, false, 4, 2, 3);
        let penalty_small = compute_turn_penalty(&small_diff, &config);
        let expected_small_penalty = config.class_change_penalty_s_per_diff;
        assert_eq!(
            penalty_small - penalty_same,
            expected_small_penalty,
            "small class diff should add {}s penalty",
            expected_small_penalty
        );

        // Pedestrians (turn_bias=1.0) should not get class penalty.
        let foot_config = TurnPenaltyConfig::foot();
        let foot_same = TurnGeometry::compute(0, 0, false, 5, 5, 5);
        let foot_diff = TurnGeometry::compute(0, 0, false, 5, 5, 12);
        let foot_penalty_same = compute_turn_penalty(&foot_same, &foot_config);
        let foot_penalty_diff = compute_turn_penalty(&foot_diff, &foot_config);
        assert_eq!(
            foot_penalty_same, foot_penalty_diff,
            "pedestrians should not get class change penalty"
        );
    }

    #[test]
    fn test_class_scaled_junction_friction() {
        // per_level=1, ref=primary(5), cap=6: friction = min(max(0, gov-5), 6).
        let config = TurnPenaltyConfig {
            junction_class_penalty_s_per_level: 1,
            junction_class_penalty_ref_class: 5,
            junction_class_penalty_max_levels: 6,
            // isolate the junction term: no angle/class-change here (straight-through).
            turn_penalty_s: 0,
            signal_delay_s: 0,
            class_change_penalty_s_per_diff: 0,
            ..TurnPenaltyConfig::car()
        };
        // Straight-through (angle 0) at a real junction (degree 4). governing =
        // max(from,to) — the least-important road at the movement.
        let at = |from: u16, to: u16, deg: u8| {
            compute_turn_penalty(&TurnGeometry::compute(0, 0, false, deg, from, to), &config)
        };
        // Arterials (≤ ref) pay nothing even at a junction.
        assert_eq!(at(5, 5, 4), 0, "primary-primary junction: 0");
        assert_eq!(at(1, 1, 4), 0, "motorway: 0");
        // Minor roads accumulate, scaling with the drop below the arterial ref.
        assert_eq!(at(7, 7, 4), 2, "secondary: (7-5)=2");
        assert_eq!(at(9, 9, 4), 4, "tertiary: (9-5)=4");
        assert_eq!(at(12, 12, 4), 6, "residential: min(12-5,6)=6");
        // Governing = least-important road: entering residential from primary
        // still pays residential-junction friction.
        assert_eq!(
            at(5, 12, 4),
            6,
            "primary->residential governed by residential"
        );
        // Below the min-degree gate (not a real junction): no friction.
        assert_eq!(at(12, 12, 2), 0, "degree<3: not a junction, 0");
    }
}
