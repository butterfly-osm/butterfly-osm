///! Pedestrian routing profile - Tag semantics for walking

use crate::profile_abi::*;
use crate::profiles::tag_lookup::TagLookup;

pub struct FootProfile;

impl Profile for FootProfile {
    fn version() -> u32 {
        1
    }

    fn process_way(input: WayInput) -> WayOutput {
        let tags = TagLookup::from_input(input.kv_keys, input.kv_vals, input.key_dict, input.val_dict);

        let mut output = WayOutput::default();

        let highway = tags.get_str("highway");
        if highway.is_none() {
            return output;
        }

        let highway = highway.unwrap();

        // Determine accessibility and speed for pedestrians
        let (access_default, base_speed_kmh, highway_class) = match highway {
            // Dedicated pedestrian infrastructure
            "footway" | "pedestrian" | "steps" => (true, 5.0, 12),

            // Shared paths
            "path" | "cycleway" => (true, 4.5, 20),

            // Roads with sidewalks (assume accessible)
            "residential" | "living_street" | "unclassified" => (true, 5.0, 12),
            "tertiary" | "secondary" | "primary" => (true, 4.5, 9),

            // Service roads
            "service" => (true, 4.5, 13),

            // Tracks
            "track" => (true, 4.0, 15),

            // Generally not for pedestrians
            "motorway" | "motorway_link" | "trunk" | "trunk_link" => (false, 0.0, 1),

            _ => (false, 0.0, 0),
        };

        if !access_default {
            return output;
        }

        // Check explicit foot access
        let foot = tags.get_str("foot");
        if foot == Some("no") || foot == Some("private") {
            return output;
        }

        output.access_fwd = true;
        output.access_rev = true;
        // Pedestrians generally ignore oneway restrictions

        output.base_speed_mmps = kmh_to_mmps(base_speed_kmh);
        output.highway_class = highway_class;

        if highway == "footway" || highway == "pedestrian" {
            output.class_bits |= 1 << class_bits::FOOTWAY;
        }

        output
    }

    fn process_turn(input: TurnInput) -> TurnOutput {
        let tags = TagLookup::from_input(input.tags_keys, input.tags_vals, input.key_dict, input.val_dict);

        let mut output = TurnOutput::default();

        // Check foot-specific restrictions
        if let Some(r) = tags.get_str("restriction:foot") {
            if r.starts_with("no_") {
                output.kind = TurnRuleKind::Ban;
                output.applies = 1 << 2; // bit2 = foot
            } else if r.starts_with("only_") {
                output.kind = TurnRuleKind::Only;
                output.applies = 1 << 2;
            }
        }

        // Motor vehicle restrictions don't apply to pedestrians
        output
    }
}

fn kmh_to_mmps(kmh: f64) -> u32 {
    ((kmh * 1000.0 / 3.6).round() as u32).min(2_800) // Cap at ~10 km/h
}
