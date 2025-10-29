///! Bicycle routing profile - Tag semantics for bicycle routing

use crate::profile_abi::*;
use crate::profiles::tag_lookup::TagLookup;

pub struct BikeProfile;

impl Profile for BikeProfile {
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

        // Determine accessibility and speed for bikes
        let (access_default, base_speed_kmh, highway_class) = match highway {
            // Dedicated cycle infrastructure
            "cycleway" => (true, 20.0, 11),

            // Shared with pedestrians
            "path" | "footway" => (true, 15.0, 20),

            // Roads generally accessible to bikes
            "residential" | "unclassified" | "tertiary" | "secondary" | "primary" => {
                (true, 18.0, 12)
            }

            // Trunk and motorways - generally no bikes
            "trunk" | "motorway" => (false, 0.0, 1),

            // Service roads
            "service" | "living_street" => (true, 15.0, 13),

            // Tracks
            "track" => (true, 12.0, 15),

            _ => (false, 0.0, 0),
        };

        if !access_default {
            return output;
        }

        // Check explicit bicycle access
        let bicycle = tags.get_str("bicycle");
        if bicycle == Some("no") || bicycle == Some("dismount") {
            return output;
        }

        output.access_fwd = true;
        output.access_rev = true;

        // Bikes are not affected by oneway for cars unless explicitly stated
        if let Some(oneway_bicycle) = tags.get_str("oneway:bicycle") {
            if oneway_bicycle == "yes" {
                output.access_rev = false;
                output.oneway = 1;
            }
        }

        output.base_speed_mmps = kmh_to_mmps(base_speed_kmh);
        output.highway_class = highway_class;

        if highway == "cycleway" {
            output.class_bits |= 1 << class_bits::CYCLEWAY;
        }

        output
    }

    fn process_turn(input: TurnInput) -> TurnOutput {
        let tags = TagLookup::from_input(input.tags_keys, input.tags_vals, input.key_dict, input.val_dict);

        let mut output = TurnOutput::default();

        let restriction = tags.get_str("restriction");
        let restriction_bicycle = tags.get_str("restriction:bicycle");

        // Check bicycle-specific restrictions first
        if let Some(r) = restriction_bicycle {
            if r.starts_with("no_") {
                output.kind = TurnRuleKind::Ban;
                output.applies = 1 << 1; // bit1 = bike
            } else if r.starts_with("only_") {
                output.kind = TurnRuleKind::Only;
                output.applies = 1 << 1;
            }
        }
        // Check if exception mentions bicycle
        else if let Some(r) = restriction {
            if r.starts_with("no_") || r.starts_with("only_") {
                // Check for exceptions
                if let Some(except) = tags.get_str("except") {
                    if except.contains("bicycle") {
                        return output; // Exception for bikes
                    }
                }
                // Motor vehicle restrictions don't apply to bikes
                return output;
            }
        }

        output
    }
}

fn kmh_to_mmps(kmh: f64) -> u32 {
    ((kmh * 1000.0 / 3.6).round() as u32).min(16_700) // Cap at ~60 km/h
}
