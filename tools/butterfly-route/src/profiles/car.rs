///! Car routing profile - Tag semantics for automobile routing
///!
///! Implements access rules, speed limits, and preferences for cars.

use crate::profile_abi::*;
use crate::profiles::tag_lookup::TagLookup;

pub struct CarProfile;

impl Profile for CarProfile {
    fn version() -> u32 {
        1
    }

    fn process_way(input: WayInput) -> WayOutput {
        let tags = TagLookup::from_input(input.kv_keys, input.kv_vals, input.key_dict, input.val_dict);

        // Default: no access
        let mut output = WayOutput::default();

        // Get highway type
        let highway = tags.get_str("highway");
        if highway.is_none() {
            return output; // No highway tag = not routable
        }

        let highway = highway.unwrap();

        // Determine base accessibility and speed based on highway type
        let (access_default, base_speed_kmh, highway_class) = match highway {
            // Motorways
            "motorway" => (true, 110.0, 1),
            "motorway_link" => (true, 60.0, 2),

            // Trunk roads
            "trunk" => (true, 90.0, 3),
            "trunk_link" => (true, 50.0, 4),

            // Primary roads
            "primary" => (true, 70.0, 5),
            "primary_link" => (true, 40.0, 6),

            // Secondary roads
            "secondary" => (true, 60.0, 7),
            "secondary_link" => (true, 40.0, 8),

            // Tertiary roads
            "tertiary" => (true, 50.0, 9),
            "tertiary_link" => (true, 30.0, 10),

            // Unclassified and residential
            "unclassified" => (true, 50.0, 11),
            "residential" => (true, 30.0, 12),

            // Service roads
            "service" => (true, 20.0, 13),
            "living_street" => (true, 10.0, 14),

            // Tracks (limited access)
            "track" => (false, 15.0, 15), // Usually not for cars

            // Pedestrian/cyclist infrastructure (no car access)
            "footway" | "path" | "cycleway" | "pedestrian" | "steps" => (false, 0.0, 20),

            // Construction
            "construction" => (false, 0.0, 99),

            _ => (false, 0.0, 0), // Unknown highway type
        };

        if !access_default {
            return output; // Not accessible by default
        }

        // Check explicit access tags (simplified)
        let motor_vehicle = tags.get_str("motor_vehicle");
        let vehicle = tags.get_str("vehicle");
        let access = tags.get_str("access");

        if is_denied(motor_vehicle) || is_denied(vehicle) || is_denied(access) {
            return output; // Explicitly denied
        }

        // Set access flags
        output.access_fwd = true;
        output.access_rev = true;

        // Handle oneway
        if let Some(oneway) = tags.get_str("oneway") {
            match oneway {
                "yes" | "1" | "true" => {
                    output.access_rev = false;
                    output.oneway = 1;
                }
                "-1" | "reverse" => {
                    output.access_fwd = false;
                    output.oneway = 2;
                }
                _ => {}
            }
        }

        // Motorways and motorway_links are oneway by default
        if highway == "motorway" || highway == "motorway_link" {
            if output.oneway == 0 {
                output.access_rev = false;
                output.oneway = 1;
            }
        }

        // Convert speed to mm/s
        output.base_speed_mmps = kmh_to_mmps(base_speed_kmh);
        output.highway_class = highway_class;

        // Set class bits
        if highway.ends_with("_link") {
            output.class_bits |= 1 << class_bits::LINK;
        }
        if highway == "residential" {
            output.class_bits |= 1 << class_bits::RESIDENTIAL;
        }
        if highway == "service" {
            output.class_bits |= 1 << class_bits::SERVICE;
        }
        if highway == "living_street" {
            output.class_bits |= 1 << class_bits::LIVING_STREET;
        }

        // Check for toll
        if tags.get_str("toll") == Some("yes") {
            output.class_bits |= 1 << class_bits::TOLL;
        }

        // Check for tunnel/bridge
        if tags.get_str("tunnel") == Some("yes") {
            output.class_bits |= 1 << class_bits::TUNNEL;
        }
        if tags.get_str("bridge") == Some("yes") {
            output.class_bits |= 1 << class_bits::BRIDGE;
        }

        // Check for ferry (route=ferry)
        if tags.get_str("route") == Some("ferry") {
            output.class_bits |= 1 << class_bits::FERRY;
            output.base_speed_mmps = kmh_to_mmps(20.0); // Slow ferry speed
        }

        output
    }

    fn process_turn(input: TurnInput) -> TurnOutput {
        let tags = TagLookup::from_input(input.tags_keys, input.tags_vals, input.key_dict, input.val_dict);

        let mut output = TurnOutput::default();

        // Check restriction type
        let restriction = tags.get_str("restriction");
        if restriction.is_none() {
            return output;
        }

        let restriction = restriction.unwrap();

        // Parse restriction type
        if restriction.starts_with("no_") {
            output.kind = TurnRuleKind::Ban;
            output.applies = 1 << 0; // bit0 = car
        } else if restriction.starts_with("only_") {
            output.kind = TurnRuleKind::Only;
            output.applies = 1 << 0;
        } else if restriction == "restriction" {
            // Generic restriction, check for specific type tag
            output.kind = TurnRuleKind::Ban;
            output.applies = 1 << 0;
        }

        // Check for conditional restrictions
        for key in ["restriction:conditional", "restriction:hgv:conditional"] {
            if tags.has(key) {
                output.is_time_dependent = true;
                break;
            }
        }

        // Check for exceptions
        if let Some(except) = tags.get_str("except") {
            // If except mentions "motorcar" or "motor_vehicle", this doesn't apply to cars
            if except.contains("motorcar") || except.contains("motor_vehicle") {
                output.applies = 0;
                output.kind = TurnRuleKind::None;
            }
        }

        output
    }
}

/// Helper: check if access is denied
/// Note: "destination" means accessible to reach local destinations (not for through traffic)
/// We allow these roads but could add a penalty to discourage through traffic
fn is_denied(value: Option<&str>) -> bool {
    matches!(value, Some("no") | Some("private"))
}

/// Convert km/h to mm/s (integer)
fn kmh_to_mmps(kmh: f64) -> u32 {
    ((kmh * 1000.0 / 3.6).round() as u32).min(80_000) // Cap at 80 m/s (288 km/h)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kmh_to_mmps() {
        assert_eq!(kmh_to_mmps(36.0), 10_000); // 36 km/h = 10 m/s = 10000 mm/s
        assert_eq!(kmh_to_mmps(90.0), 25_000); // 90 km/h = 25 m/s
    }

    #[test]
    fn test_motorway_access() {
        let keys = vec![];
        let vals = vec![];
        let input = WayInput {
            kv_keys: &keys,
            kv_vals: &vals,
        };
        // This will fail without highway tag, but tests the function compiles
        let _output = CarProfile::process_way(input);
    }
}
