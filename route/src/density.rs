//! Urban density classification for traffic-aware routing.
//!
//! Each way is assigned a `DensityClass` during step 2 profiling. The class is
//! stored in `way_attrs.*.bin` (format v2) and consumed at step 8 by the
//! traffic recustomization pass which multiplies edge weights by per-class
//! speed factors loaded from a `traffic/*.traffic.json` profile.
//!
//! ## Classifiers
//!
//! - [`DensityClassifier::OsmTag`] — synthetic, tag-driven, deterministic. The
//!   only classifier currently implemented. Uses highway type, surface,
//!   `lit=*`, `maxspeed=*`, urban/rural hints, and class_bits to bucket each
//!   way into one of the five density classes. Runs in O(n_ways), no spatial
//!   index, no extra I/O.
//!
//! - [`DensityClassifier::CdisParquet`] — reserved plug-in point for the
//!   Sirius proprietary CDIS sector geometries + rurban archetypes. Not
//!   implemented in this repo; selecting it returns an error so operators
//!   know they need a private build.
//!
//! ## Mapping rules (OsmTag classifier)
//!
//! | Highway class                    | Default class | Notes |
//! |----------------------------------|---------------|-------|
//! | residential, living_street       | UrbanHigh     | inherently dense |
//! | service, footway, pedestrian     | UrbanHigh     | tight street network |
//! | tertiary, tertiary_link          | UrbanMedium   | typical city/town arteries |
//! | secondary, secondary_link        | UrbanLow      | urban-suburban mix |
//! | primary, primary_link            | Suburban      | regional connector |
//! | trunk, trunk_link, motorway, motorway_link | Rural | grade-separated |
//! | unclassified                     | Suburban      | usually rural country roads |
//! | track                            | Rural         | farm/forest |
//!
//! Hints that promote toward the urban side:
//! - `lit=yes` → bump up one urban level (caps at UrbanHigh)
//! - `maxspeed <= 30` → UrbanHigh
//! - `maxspeed <= 50` → at least UrbanMedium
//! - `traffic_calming=*` → bump up
//!
//! Hints that demote toward rural:
//! - `maxspeed >= 90` → at least Suburban
//! - `maxspeed >= 110` → Rural

use serde::{Deserialize, Serialize};

/// Density class assigned to each way.
#[repr(u8)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default, Serialize, Deserialize)]
pub enum DensityClass {
    /// Dense urban core: residential streets, living streets, service roads.
    UrbanHigh = 0,
    /// Mixed urban: tertiary roads, in-town arteries, lit secondaries.
    UrbanMedium = 1,
    /// Outer urban: secondaries, primaries within town limits.
    UrbanLow = 2,
    /// Suburban / regional: primaries between towns, unclassified.
    /// Default — neutral mid-bucket.
    #[default]
    Suburban = 3,
    /// Rural / inter-urban: motorways, trunks, country tracks.
    Rural = 4,
}

impl DensityClass {
    /// All variants in canonical order.
    pub const ALL: [DensityClass; 5] = [
        DensityClass::UrbanHigh,
        DensityClass::UrbanMedium,
        DensityClass::UrbanLow,
        DensityClass::Suburban,
        DensityClass::Rural,
    ];

    /// Stable string label, used by JSON traffic profiles.
    pub fn as_str(&self) -> &'static str {
        match self {
            DensityClass::UrbanHigh => "urban_high",
            DensityClass::UrbanMedium => "urban_medium",
            DensityClass::UrbanLow => "urban_low",
            DensityClass::Suburban => "suburban",
            DensityClass::Rural => "rural",
        }
    }

    /// Parse from a label (case-insensitive).
    pub fn parse(s: &str) -> Option<Self> {
        match s.to_ascii_lowercase().as_str() {
            "urban_high" | "urbanhigh" => Some(DensityClass::UrbanHigh),
            "urban_medium" | "urbanmedium" => Some(DensityClass::UrbanMedium),
            "urban_low" | "urbanlow" => Some(DensityClass::UrbanLow),
            "suburban" => Some(DensityClass::Suburban),
            "rural" => Some(DensityClass::Rural),
            _ => None,
        }
    }

    /// Encode to the on-disk u8.
    #[inline]
    pub fn to_u8(self) -> u8 {
        self as u8
    }

    /// Decode from on-disk u8. Falls back to `Suburban` for unknown codes
    /// (forward compatibility with older binaries that wrote 0xFF padding).
    #[inline]
    pub fn from_u8(v: u8) -> Self {
        match v {
            0 => DensityClass::UrbanHigh,
            1 => DensityClass::UrbanMedium,
            2 => DensityClass::UrbanLow,
            3 => DensityClass::Suburban,
            4 => DensityClass::Rural,
            _ => DensityClass::Suburban,
        }
    }

    /// Promote toward urban (UrbanHigh is the cap).
    fn bump_urban(self) -> Self {
        match self {
            DensityClass::Rural => DensityClass::Suburban,
            DensityClass::Suburban => DensityClass::UrbanLow,
            DensityClass::UrbanLow => DensityClass::UrbanMedium,
            DensityClass::UrbanMedium => DensityClass::UrbanHigh,
            DensityClass::UrbanHigh => DensityClass::UrbanHigh,
        }
    }

    /// Push toward rural (Rural is the cap).
    fn bump_rural(self) -> Self {
        match self {
            DensityClass::UrbanHigh => DensityClass::UrbanMedium,
            DensityClass::UrbanMedium => DensityClass::UrbanLow,
            DensityClass::UrbanLow => DensityClass::Suburban,
            DensityClass::Suburban => DensityClass::Rural,
            DensityClass::Rural => DensityClass::Rural,
        }
    }
}

/// Strategy used to assign `DensityClass` to each way during step 2.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DensityClassifier {
    /// Tag-driven, no external data. Default.
    OsmTag,
    /// Sirius proprietary CDIS parquet — not implemented in this repo.
    CdisParquet,
}

impl DensityClassifier {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.to_ascii_lowercase().as_str() {
            "osm-tag" | "osm_tag" | "osmtag" => Ok(DensityClassifier::OsmTag),
            "cdis-parquet" | "cdis_parquet" | "cdisparquet" => Ok(DensityClassifier::CdisParquet),
            other => anyhow::bail!(
                "unknown density classifier '{}': supported: osm-tag, cdis-parquet",
                other
            ),
        }
    }
}

/// One way's tag view, in the form already available during step 2 profiling
/// (parallel `(key_id, val_id)` arrays + the value dictionary).
#[derive(Debug, Clone, Copy)]
pub struct WayTagsView<'a> {
    pub keys: &'a [u32],
    pub vals: &'a [u32],
    pub val_dict: &'a std::collections::HashMap<u32, String>,
    pub key_dict: &'a std::collections::HashMap<u32, String>,
}

impl<'a> WayTagsView<'a> {
    fn lookup(&self, key_name: &str) -> Option<&'a str> {
        // Reverse-find the key id, then locate the value.
        for (k_id, k_str) in self.key_dict.iter() {
            if k_str == key_name {
                for (i, kid) in self.keys.iter().enumerate() {
                    if kid == k_id {
                        let vid = self.vals[i];
                        return self.val_dict.get(&vid).map(|s| s.as_str());
                    }
                }
                return None;
            }
        }
        None
    }
}

/// Map a highway class string to its starting density bucket.
fn highway_default_density(highway: &str) -> DensityClass {
    match highway {
        "residential" | "living_street" | "service" | "footway" | "pedestrian" | "cycleway"
        | "path" | "steps" => DensityClass::UrbanHigh,
        "tertiary" | "tertiary_link" => DensityClass::UrbanMedium,
        "secondary" | "secondary_link" => DensityClass::UrbanLow,
        "primary" | "primary_link" => DensityClass::Suburban,
        "unclassified" => DensityClass::Suburban,
        "trunk" | "trunk_link" | "motorway" | "motorway_link" => DensityClass::Rural,
        "track" => DensityClass::Rural,
        _ => DensityClass::Suburban,
    }
}

/// Parse an OSM `maxspeed=*` value into km/h. Returns None if the tag is
/// absent or non-numeric (e.g. `signals`, `walk`, `none`).
fn parse_maxspeed_kmh(s: &str) -> Option<u32> {
    let trimmed = s.trim().to_ascii_lowercase();
    // Strip mph suffix if present and convert.
    if let Some(rest) = trimmed.strip_suffix(" mph") {
        return rest
            .trim()
            .parse::<u32>()
            .ok()
            .map(|m| (m as f32 * 1.609_344).round() as u32);
    }
    if let Some(rest) = trimmed.strip_suffix("mph") {
        return rest
            .trim()
            .parse::<u32>()
            .ok()
            .map(|m| (m as f32 * 1.609_344).round() as u32);
    }
    // Strip the kmh suffix variants.
    let bare = trimmed
        .strip_suffix(" kmh")
        .or_else(|| trimmed.strip_suffix("kmh"))
        .or_else(|| trimmed.strip_suffix(" km/h"))
        .or_else(|| trimmed.strip_suffix("km/h"))
        .unwrap_or(&trimmed);
    bare.trim().parse::<u32>().ok()
}

/// Tag-driven classifier — runs once per way during step 2.
///
/// Inputs are the same parallel key/val id arrays already iterated by the
/// step 2 evaluator, plus the dictionaries to resolve them to strings.
pub fn classify_osm_tag(
    classifier: DensityClassifier,
    highway: &str,
    tags: &WayTagsView<'_>,
) -> DensityClass {
    if classifier != DensityClassifier::OsmTag {
        // The CDIS classifier is plumbed at the CLI level; if we ever reach
        // this point with another variant the caller forgot to dispatch.
        return DensityClass::Suburban;
    }

    let mut class = highway_default_density(highway);

    // maxspeed-driven adjustments
    if let Some(maxspeed_str) = tags.lookup("maxspeed")
        && let Some(kmh) = parse_maxspeed_kmh(maxspeed_str)
    {
        if kmh <= 30 {
            class = DensityClass::UrbanHigh;
        } else if kmh <= 50 {
            // Cap at UrbanMedium minimum (i.e. don't push it more rural than urban_medium).
            class = match class {
                DensityClass::Rural | DensityClass::Suburban | DensityClass::UrbanLow => {
                    DensityClass::UrbanMedium
                }
                other => other,
            };
        } else if kmh >= 110 {
            class = DensityClass::Rural;
        } else if kmh >= 90 {
            // 90 km/h+ rules out urban classes — settle at Suburban or Rural.
            class = match class {
                DensityClass::UrbanHigh
                | DensityClass::UrbanMedium
                | DensityClass::UrbanLow
                | DensityClass::Suburban => DensityClass::Suburban,
                DensityClass::Rural => DensityClass::Rural,
            };
        }
    }

    // `lit=yes` is a strong urban indicator (lit roads outside towns are rare)
    if let Some(lit) = tags.lookup("lit")
        && (lit == "yes" || lit == "24/7")
    {
        class = class.bump_urban();
    }

    // `traffic_calming=*` → urban
    if tags.lookup("traffic_calming").is_some() {
        class = class.bump_urban();
    }

    // sidewalk presence → urban (only if not already capped)
    if let Some(sidewalk) = tags.lookup("sidewalk")
        && sidewalk != "no"
        && sidewalk != "none"
    {
        class = match class {
            DensityClass::Rural => DensityClass::Suburban,
            other => other,
        };
    }

    // `motorroad=yes` is a strong rural/inter-urban indicator
    if let Some(motorroad) = tags.lookup("motorroad")
        && motorroad == "yes"
    {
        class = class.bump_rural();
    }

    class
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn make_view<'a>(
        pairs: &'a [(u32, u32)],
        key_dict: &'a HashMap<u32, String>,
        val_dict: &'a HashMap<u32, String>,
    ) -> (Vec<u32>, Vec<u32>, WayTagsView<'a>) {
        let keys: Vec<u32> = pairs.iter().map(|(k, _)| *k).collect();
        let vals: Vec<u32> = pairs.iter().map(|(_, v)| *v).collect();
        // Construct a view that borrows from the function-level slices we own.
        let v = WayTagsView {
            keys: &[],
            vals: &[],
            key_dict,
            val_dict,
        };
        (keys, vals, v)
    }

    #[test]
    fn density_class_round_trip() {
        for c in DensityClass::ALL {
            assert_eq!(DensityClass::from_u8(c.to_u8()), c);
            assert_eq!(DensityClass::parse(c.as_str()), Some(c));
        }
    }

    #[test]
    fn unknown_byte_falls_back_to_suburban() {
        assert_eq!(DensityClass::from_u8(0xff), DensityClass::Suburban);
        assert_eq!(DensityClass::from_u8(99), DensityClass::Suburban);
    }

    #[test]
    fn classifier_parsing() {
        assert_eq!(
            DensityClassifier::parse("osm-tag").unwrap(),
            DensityClassifier::OsmTag
        );
        assert_eq!(
            DensityClassifier::parse("CDIS-Parquet").unwrap(),
            DensityClassifier::CdisParquet
        );
        assert!(DensityClassifier::parse("nope").is_err());
    }

    #[test]
    fn highway_defaults_match_spec() {
        assert_eq!(highway_default_density("motorway"), DensityClass::Rural);
        assert_eq!(
            highway_default_density("residential"),
            DensityClass::UrbanHigh
        );
        assert_eq!(highway_default_density("primary"), DensityClass::Suburban);
        assert_eq!(
            highway_default_density("tertiary"),
            DensityClass::UrbanMedium
        );
        assert_eq!(highway_default_density("secondary"), DensityClass::UrbanLow);
    }

    #[test]
    fn maxspeed_parser_handles_units() {
        assert_eq!(parse_maxspeed_kmh("50"), Some(50));
        assert_eq!(parse_maxspeed_kmh("50 km/h"), Some(50));
        assert_eq!(parse_maxspeed_kmh("30 mph"), Some(48));
        assert_eq!(parse_maxspeed_kmh("walk"), None);
        assert_eq!(parse_maxspeed_kmh(" 110 "), Some(110));
    }

    #[test]
    fn classify_no_tags_uses_default() {
        let key_dict: HashMap<u32, String> = HashMap::new();
        let val_dict: HashMap<u32, String> = HashMap::new();
        let pairs: [(u32, u32); 0] = [];
        let (keys, vals, _) = make_view(&pairs, &key_dict, &val_dict);
        let view = WayTagsView {
            keys: &keys,
            vals: &vals,
            key_dict: &key_dict,
            val_dict: &val_dict,
        };
        assert_eq!(
            classify_osm_tag(DensityClassifier::OsmTag, "motorway", &view),
            DensityClass::Rural
        );
        assert_eq!(
            classify_osm_tag(DensityClassifier::OsmTag, "residential", &view),
            DensityClass::UrbanHigh
        );
    }

    #[test]
    fn classify_lit_promotes_urban() {
        let mut key_dict: HashMap<u32, String> = HashMap::new();
        key_dict.insert(1, "lit".to_string());
        let mut val_dict: HashMap<u32, String> = HashMap::new();
        val_dict.insert(1, "yes".to_string());

        let keys = vec![1u32];
        let vals = vec![1u32];
        let view = WayTagsView {
            keys: &keys,
            vals: &vals,
            key_dict: &key_dict,
            val_dict: &val_dict,
        };

        // primary (Suburban) + lit=yes → UrbanLow
        assert_eq!(
            classify_osm_tag(DensityClassifier::OsmTag, "primary", &view),
            DensityClass::UrbanLow
        );
    }

    #[test]
    fn classify_maxspeed_30_caps_to_urban_high() {
        let mut key_dict: HashMap<u32, String> = HashMap::new();
        key_dict.insert(1, "maxspeed".to_string());
        let mut val_dict: HashMap<u32, String> = HashMap::new();
        val_dict.insert(1, "30".to_string());

        let keys = vec![1u32];
        let vals = vec![1u32];
        let view = WayTagsView {
            keys: &keys,
            vals: &vals,
            key_dict: &key_dict,
            val_dict: &val_dict,
        };

        assert_eq!(
            classify_osm_tag(DensityClassifier::OsmTag, "primary", &view),
            DensityClass::UrbanHigh
        );
    }

    #[test]
    fn classify_maxspeed_120_forces_rural() {
        let mut key_dict: HashMap<u32, String> = HashMap::new();
        key_dict.insert(1, "maxspeed".to_string());
        let mut val_dict: HashMap<u32, String> = HashMap::new();
        val_dict.insert(1, "120".to_string());

        let keys = vec![1u32];
        let vals = vec![1u32];
        let view = WayTagsView {
            keys: &keys,
            vals: &vals,
            key_dict: &key_dict,
            val_dict: &val_dict,
        };

        assert_eq!(
            classify_osm_tag(DensityClassifier::OsmTag, "tertiary", &view),
            DensityClass::Rural
        );
    }
}
