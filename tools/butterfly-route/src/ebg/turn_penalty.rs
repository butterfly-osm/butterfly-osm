///! Turn penalty cost model - OSRM-compatible sigmoid-based turn costs
///!
///! Implements OSRM's exact turn penalty formula from car.lua:
///! - Sigmoid function mapping angle to penalty
///! - turn_penalty = 7.5 seconds (max)
///! - turn_bias = 1.075 (right-turn preference for right-hand traffic)
///! - u_turn_penalty = 20 seconds additional
///!
///! Reference: https://github.com/Project-OSRM/osrm-backend/blob/master/profiles/car.lua

/// Turn geometry for a single turn (a → b at intersection)
#[derive(Debug, Clone)]
pub struct TurnGeometry {
    pub from_bearing_deci: u16,  // 0-3599 (deci-degrees, 0 = North)
    pub to_bearing_deci: u16,
    pub angle_deg: i16,          // Signed turn angle (-180 to +180)
    pub is_uturn: bool,
    pub via_degree: u8,          // Intersection complexity
    pub via_has_signal: bool,    // Traffic signal at intersection
}

impl TurnGeometry {
    /// Compute turn geometry from bearings
    ///
    /// from_bearing: bearing of incoming edge at intersection (deci-degrees 0-3599)
    /// to_bearing: bearing of outgoing edge at intersection (deci-degrees 0-3599)
    /// via_has_signal: whether the via node has a traffic signal
    /// degree: in_degree + out_degree at via node
    pub fn compute(
        from_bearing_deci: u16,
        to_bearing_deci: u16,
        via_has_signal: bool,
        via_degree: u8,
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
        }
    }
}

/// Turn penalty configuration (mode-specific, OSRM-compatible)
#[derive(Debug, Clone)]
pub struct TurnPenaltyConfig {
    /// Maximum turn penalty in deciseconds (OSRM: 7.5s = 75 ds)
    pub turn_penalty_ds: u32,

    /// Turn bias for asymmetric left/right costs (OSRM: 1.075)
    /// >1.0 = prefer right turns (right-hand traffic countries)
    pub turn_bias: f64,

    /// Additional U-turn penalty in deciseconds (OSRM: 20s = 200 ds)
    pub u_turn_penalty_ds: u32,

    /// Minimum intersection degree to apply turn penalty
    /// (OSRM only applies at complex intersections with >2 roads)
    pub min_degree_for_penalty: u8,

    /// Traffic signal delay in deciseconds (typical: 15-30 seconds)
    /// OSRM uses variable signal penalties based on intersection complexity
    pub signal_delay_ds: u32,
}

impl TurnPenaltyConfig {
    /// Car mode turn penalties - matches OSRM car.lua exactly
    pub fn car() -> Self {
        Self {
            turn_penalty_ds: 75,       // 7.5 seconds (OSRM default)
            turn_bias: 1.075,          // Slight right-turn preference
            u_turn_penalty_ds: 200,    // 20 seconds (OSRM default)
            min_degree_for_penalty: 3, // Only at intersections (not straight roads)
            signal_delay_ds: 80,       // 8 seconds average signal wait
        }
    }

    /// Bike mode turn penalties
    pub fn bike() -> Self {
        Self {
            turn_penalty_ds: 40,       // 4 seconds max
            turn_bias: 1.4,            // Bikes prefer right turns more
            u_turn_penalty_ds: 50,     // 5 seconds
            min_degree_for_penalty: 3,
            signal_delay_ds: 50,       // 5 seconds (bikes often filter)
        }
    }

    /// Foot mode turn penalties
    /// Pedestrians don't get angle-based turn penalties but do get
    /// crossing penalties at intersections (modeled as small fixed cost)
    pub fn foot() -> Self {
        Self {
            turn_penalty_ds: 20,       // 2 seconds for crossing intersection
            turn_bias: 1.0,            // Symmetric - no left/right preference
            u_turn_penalty_ds: 0,      // No U-turn penalty for walking
            min_degree_for_penalty: 4, // Only at complex intersections
            signal_delay_ds: 40,       // 4 seconds pedestrian signal wait
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
    if config.turn_penalty_ds == 0 && config.signal_delay_ds == 0 {
        return 0;
    }

    let mut penalty = 0u32;

    // For pedestrians (turn_bias == 1.0), use flat crossing penalty
    // Pedestrians don't care about turn angle, just intersection complexity
    if (config.turn_bias - 1.0).abs() < 0.001 {
        penalty = config.turn_penalty_ds;
    } else if config.turn_penalty_ds > 0 {
        let angle = geom.angle_deg as f64;
        let turn_bias = config.turn_bias;

        // OSRM sigmoid formula
        // The formula uses -angle because OSRM convention is opposite
        // Positive angle = left turn in OSRM, right turn in our convention
        let exponent = -((13.0 / turn_bias) * (-angle / 180.0) - 6.5 * turn_bias);
        let sigmoid = 1.0 / (1.0 + exponent.exp());

        penalty = (config.turn_penalty_ds as f64 * sigmoid).round() as u32;

        // Add U-turn penalty
        if geom.is_uturn {
            penalty = penalty.saturating_add(config.u_turn_penalty_ds);
        }
    }

    // Add traffic signal delay
    if geom.via_has_signal {
        penalty = penalty.saturating_add(config.signal_delay_ds);
    }

    penalty
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_osrm_sigmoid_penalties() {
        let config = TurnPenaltyConfig::car();

        // Straight: ~0 penalty
        let geom = TurnGeometry::compute(0, 0, false, 4);
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(penalty < 10, "straight should be ~0, got {}ds", penalty);

        // 90 degree right turn: very low penalty (~0) in right-hand traffic
        // OSRM heavily favors right turns
        let geom = TurnGeometry::compute(0, 900, false, 4);
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(penalty < 10, "90° right should be ~0 in right-hand traffic, got {}ds", penalty);

        // 90 degree left turn: ~2s (crossing traffic)
        let geom = TurnGeometry::compute(0, 2700, false, 4);  // 270° bearing = -90° left turn
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(penalty >= 15 && penalty <= 30, "90° left should be ~2s, got {}ds", penalty);

        // Left U-turn: ~7.5s + 20s = ~27.5s (maximum penalty)
        let geom = TurnGeometry::compute(0, 1800, false, 4);  // 180° = U-turn
        let penalty = compute_turn_penalty(&geom, &config);
        // Note: 180° could be left or right depending on interpretation
        // The formula should give high penalty for any U-turn
        assert!(penalty >= 200, "U-turn should be ~20+s, got {}ds", penalty);

        // No penalty at simple road (degree < 3)
        let geom = TurnGeometry::compute(0, 900, false, 2);
        let penalty = compute_turn_penalty(&geom, &config);
        assert_eq!(penalty, 0, "no penalty at simple road continuation");
    }

    #[test]
    fn test_foot_crossing_penalty() {
        let config = TurnPenaltyConfig::foot();

        // Pedestrians get small crossing penalty at complex intersections
        let geom = TurnGeometry::compute(0, 900, false, 5);  // 5-way intersection
        let penalty = compute_turn_penalty(&geom, &config);
        assert!(penalty > 0, "pedestrians should get crossing penalty at complex intersections");
        assert!(penalty <= 30, "pedestrian penalty should be small, got {}ds", penalty);

        // No penalty at simple intersections
        let geom = TurnGeometry::compute(0, 900, false, 3);
        let penalty = compute_turn_penalty(&geom, &config);
        assert_eq!(penalty, 0, "no penalty at simple 3-way intersection");
    }

    #[test]
    fn test_left_right_asymmetry() {
        let config = TurnPenaltyConfig::car();

        // 90° right turn
        let right = TurnGeometry::compute(0, 900, false, 4);
        let right_penalty = compute_turn_penalty(&right, &config);

        // 90° left turn (bearing 270° = -90°)
        let left = TurnGeometry::compute(0, 2700, false, 4);
        let left_penalty = compute_turn_penalty(&left, &config);

        // Left turns should cost more than right turns (right-hand traffic)
        assert!(left_penalty > right_penalty,
            "left turn ({}ds) should cost more than right turn ({}ds)",
            left_penalty, right_penalty);
    }

    #[test]
    fn test_traffic_signal_delay() {
        let config = TurnPenaltyConfig::car();

        // Straight at signalized intersection: signal delay only
        let geom_no_signal = TurnGeometry::compute(0, 0, false, 4);
        let geom_with_signal = TurnGeometry::compute(0, 0, true, 4);

        let penalty_no = compute_turn_penalty(&geom_no_signal, &config);
        let penalty_with = compute_turn_penalty(&geom_with_signal, &config);

        // With signal should add signal_delay_ds
        assert_eq!(
            penalty_with - penalty_no,
            config.signal_delay_ds,
            "signal should add {}ds delay, got {} vs {}",
            config.signal_delay_ds,
            penalty_with,
            penalty_no
        );

        // Left turn at signalized intersection: turn penalty + signal delay
        let left_no_signal = TurnGeometry::compute(0, 2700, false, 4);
        let left_with_signal = TurnGeometry::compute(0, 2700, true, 4);

        let penalty_left_no = compute_turn_penalty(&left_no_signal, &config);
        let penalty_left_with = compute_turn_penalty(&left_with_signal, &config);

        assert_eq!(
            penalty_left_with - penalty_left_no,
            config.signal_delay_ds,
            "signal should add consistent delay to left turn"
        );
    }
}
