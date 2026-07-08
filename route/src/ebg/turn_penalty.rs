//! Turn penalty cost model - OSRM-compatible sigmoid-based turn costs
//!
//! Implements OSRM's exact turn penalty formula from car.lua:
//! - Sigmoid function mapping angle to penalty
//! - turn_penalty = 7.5 seconds (max) — stored as 8 s post-#297 (round-half-to-even)
//! - turn_bias = 1.075 (right-turn preference for right-hand traffic)
//! - u_turn_penalty = 20 seconds additional
//!
//! Reference: https://github.com/Project-OSRM/osrm-backend/blob/master/profiles/car.lua

/// #490: the `turn_penalties` JSON block is parsed by the SHARED
/// `crate::model::schema::TurnPenaltySchema` — one struct, one parse contract.
/// (Previously a private duplicate here had to be kept field-for-field in sync
/// with the compile-path struct.)
use crate::model::ModelSchema;
use crate::model::schema::TurnPenaltySchema;

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
        }
    }

    /// Load turn penalty config for an ACTIVE mode from `<models_dir>/<mode>.model.json`.
    ///
    /// #491: this is a HARD ERROR on a missing or unparseable model file. The
    /// previous `for_mode` read a compile-time-baked path and silently returned
    /// `default_identity()` when it was absent (e.g. in containers), producing
    /// artifacts with ZERO turn penalties. Identity is reserved for genuinely
    /// inactive mode slots, which never reach this call.
    pub fn from_models_dir(models_dir: &std::path::Path, mode_name: &str) -> anyhow::Result<Self> {
        use anyhow::Context;
        let model_path = models_dir.join(format!("{}.model.json", mode_name));
        let content = std::fs::read_to_string(&model_path).with_context(|| {
            format!(
                "cannot read model file for active mode '{}': {} (#491 — refusing to build with zero turn penalties)",
                mode_name,
                model_path.display()
            )
        })?;
        let schema: ModelSchema = serde_json::from_str(&content)
            .with_context(|| format!("unparseable model file: {}", model_path.display()))?;
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
    if config.turn_penalty_s == 0 && config.signal_delay_s == 0 {
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

    // #491: loading from an explicit models dir must parse the shipped car
    // model (real penalties), and a missing model for an active mode must be
    // a hard error — never a silent identity fallback.
    #[test]
    fn from_models_dir_loads_shipped_car() {
        let dir = std::path::PathBuf::from(concat!(env!("CARGO_MANIFEST_DIR"), "/../models"));
        let tp = TurnPenaltyConfig::from_models_dir(&dir, "car").unwrap();
        assert_eq!(tp.turn_penalty_s, 8);
        assert_eq!(tp.u_turn_penalty_s, 20);
        assert!(
            tp.turn_penalty_s > 0,
            "car must never build with zero turn penalties (#491)"
        );
    }

    #[test]
    fn from_models_dir_missing_mode_is_hard_error() {
        // Fresh empty dir (not the shared temp root, which may hold unrelated files).
        let dir = std::env::temp_dir().join(format!("bf_tp_missing_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let err = TurnPenaltyConfig::from_models_dir(&dir, "no_such_mode").unwrap_err();
        let _ = std::fs::remove_dir_all(&dir);
        assert!(
            err.to_string().contains("no_such_mode"),
            "error must name the missing mode: {err}"
        );
    }
}
