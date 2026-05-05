//! OSM PBF address-tag extractor.
//!
//! Two-pass: first pass collects node coordinates and emits node
//! addresses; second pass resolves way addresses by averaging
//! resolved node coordinates (centroid proxy).
//!
//! ## Per-country OSM tag overrides (#96)
//!
//! Most countries follow the standard OSM tagging convention
//! (`addr:street` + `addr:housenumber`). A few don't — Japan publishes
//! whole addresses through `addr:full` because its addressing model is
//! block-based; some countries override `addr:city` etc. The country
//! pack carries those overrides via [`crate::routing::pack::OsmTags`];
//! [`extract_addresses_with_tags`] threads them through.
//!
//! [`extract_addresses`] is the legacy zero-config entrypoint that
//! uses the standard OSM keys.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};

use crate::routing::pack::OsmTags;
use crate::shard::{AddressRecord, SourceTag};

/// Standard OSM `addr:*` tag mapping. Used by [`extract_addresses`]
/// when no country pack is provided.
fn default_tags() -> OsmTags {
    OsmTags {
        postcode: "addr:postcode".to_string(),
        street: "addr:street".to_string(),
        housenumber: "addr:housenumber".to_string(),
        city: "addr:city".to_string(),
    }
}

#[derive(Debug, Clone, Copy)]
pub enum ExtractProgress {
    Phase {
        phase: &'static str,
    },
    NodePass {
        nodes_seen: u64,
        addresses_emitted: u64,
    },
    WayPass {
        ways_seen: u64,
        addresses_emitted: u64,
    },
}

pub fn extract_addresses<P: AsRef<Path>>(
    pbf_path: P,
    progress: impl FnMut(ExtractProgress),
) -> Result<Vec<AddressRecord>> {
    let tags = default_tags();
    extract_addresses_with_tags(pbf_path, &tags, progress)
}

/// Like [`extract_addresses`] but consults `tags` (per-country pack
/// override) when resolving the canonical street/postcode/etc. tags.
/// `tags.street` falls back through `addr:full` and `addr:place`
/// regardless of the override so block-based / place-based addresses
/// continue to work.
pub fn extract_addresses_with_tags<P: AsRef<Path>>(
    pbf_path: P,
    tags: &OsmTags,
    mut progress: impl FnMut(ExtractProgress),
) -> Result<Vec<AddressRecord>> {
    let path = pbf_path.as_ref();

    progress(ExtractProgress::Phase {
        phase: "scanning nodes",
    });

    let reader =
        ElementReader::from_path(path).with_context(|| format!("opening {}", path.display()))?;

    let mut node_coords: HashMap<i64, (f64, f64)> = HashMap::with_capacity(2_000_000);
    let mut records: Vec<AddressRecord> = Vec::with_capacity(1_000_000);
    let mut nodes_seen = 0u64;
    let mut node_addr_records = 0u64;

    reader
        .for_each(|el| match el {
            Element::Node(node) => {
                nodes_seen += 1;
                node_coords.insert(node.id(), (node.lat(), node.lon()));
                if let Some(rec) = tags_to_address(node.lat(), node.lon(), node.tags(), tags) {
                    records.push(rec);
                    node_addr_records += 1;
                }
            }
            Element::DenseNode(node) => {
                nodes_seen += 1;
                node_coords.insert(node.id(), (node.lat(), node.lon()));
                if let Some(rec) = tags_to_address(node.lat(), node.lon(), node.tags(), tags) {
                    records.push(rec);
                    node_addr_records += 1;
                }
            }
            _ => {}
        })
        .context("error scanning nodes")?;

    progress(ExtractProgress::NodePass {
        nodes_seen,
        addresses_emitted: node_addr_records,
    });

    progress(ExtractProgress::Phase {
        phase: "scanning ways",
    });
    let reader =
        ElementReader::from_path(path).with_context(|| format!("re-opening {}", path.display()))?;
    let mut ways_seen = 0u64;
    let mut way_addr_records = 0u64;
    reader
        .for_each(|el| {
            if let Element::Way(way) = el {
                ways_seen += 1;
                if let Some(rec) = way_to_address(&way, &node_coords, tags) {
                    records.push(rec);
                    way_addr_records += 1;
                }
            }
        })
        .context("error scanning ways")?;

    progress(ExtractProgress::WayPass {
        ways_seen,
        addresses_emitted: way_addr_records,
    });

    Ok(records)
}

/// Where the resolved street value came from. The block-based
/// fallback (`addr:full` / `addr:place`) is treated more permissively
/// by [`has_minimum_signal`] — countries like Japan don't reliably
/// carry housenumbers separately, so a non-empty `addr:full` plus
/// locality OR postcode is enough to anchor a record. The strict
/// `addr:street` path still requires a housenumber.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StreetSource {
    /// Came from the canonical street tag (`addr:street` or pack-overridden).
    Strict,
    /// Came from `addr:full` (block-based) or `addr:place` (place-based).
    Fallback,
    /// No street value resolved.
    None,
}

/// Decide whether a tag bag carries enough address signal to be worth
/// storing.
///
/// Conventional (`Strict`) address: street + housenumber must both be
/// present (the European/US/AU pattern).
///
/// Block-based / place-based (`Fallback`) address (Japan, Korea, parts
/// of Latin America): the `addr:full` tag carries the whole address as
/// a single string and neither `addr:street` nor `addr:housenumber` is
/// reliably set. We accept records where the fallback street is
/// non-empty as long as locality OR postcode is also present.
///
/// The previous version conflated the two: a strict `addr:street`
/// without a housenumber would slip through if locality or postcode
/// was set, which let bare-street POIs leak into the shard. The
/// `source` parameter restores the documented split.
fn has_minimum_signal(
    source: StreetSource,
    housenumber: &str,
    postcode: &str,
    locality: &str,
) -> bool {
    match source {
        StreetSource::None => false,
        StreetSource::Strict => !housenumber.is_empty(),
        StreetSource::Fallback => !postcode.is_empty() || !locality.is_empty(),
    }
}

fn tags_to_address<'a, I>(lat: f64, lon: f64, raw_tags: I, cfg: &OsmTags) -> Option<AddressRecord>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let (street, source, housenumber, postcode, locality) = pull_addr_tags(raw_tags, cfg);
    if !has_minimum_signal(source, &housenumber, &postcode, &locality) {
        return None;
    }
    Some(AddressRecord {
        lat,
        lon,
        street,
        housenumber,
        postcode,
        locality,
        source: SourceTag::Osm,
        source_id: None,
    })
}

fn way_to_address(
    way: &osmpbf::Way<'_>,
    coords: &HashMap<i64, (f64, f64)>,
    cfg: &OsmTags,
) -> Option<AddressRecord> {
    let (street, source, housenumber, postcode, locality) = pull_addr_tags(way.tags(), cfg);
    if !has_minimum_signal(source, &housenumber, &postcode, &locality) {
        return None;
    }
    let mut sum_lat = 0.0_f64;
    let mut sum_lon = 0.0_f64;
    let mut n = 0u32;
    for nid in way.refs() {
        if let Some(&(la, lo)) = coords.get(&nid) {
            sum_lat += la;
            sum_lon += lo;
            n += 1;
        }
    }
    if n == 0 {
        return None;
    }
    let lat = sum_lat / n as f64;
    let lon = sum_lon / n as f64;
    Some(AddressRecord {
        lat,
        lon,
        street,
        housenumber,
        postcode,
        locality,
        source: SourceTag::Osm,
        source_id: None,
    })
}

fn pull_addr_tags<'a, I>(tags: I, cfg: &OsmTags) -> (String, StreetSource, String, String, String)
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let mut street = String::new();
    let mut place = String::new();
    let mut full = String::new();
    let mut housenumber = String::new();
    let mut postcode = String::new();
    let mut city = String::new();
    let mut province = String::new();
    let mut quarter = String::new();
    let mut block_number = String::new();

    // Pack-overridable keys. The pack carries country-specific tag
    // names (e.g. JP packs use `addr:full` as the canonical street).
    // Standard keys still flow through their dedicated arms — the
    // pack overrides only change the "primary" channel for each field.
    for (k, v) in tags {
        if k == cfg.street {
            street = v.to_string();
            continue;
        }
        if k == cfg.housenumber {
            housenumber = v.to_string();
            continue;
        }
        if k == cfg.postcode {
            postcode = v.to_string();
            continue;
        }
        if k == cfg.city {
            city = v.to_string();
            continue;
        }
        match k {
            "addr:place" => place = v.to_string(),
            // `addr:full` is the standard fallback for countries where
            // street+number doesn't decompose cleanly (Japan blocks,
            // Korean addresses, freeform rural addressing).
            "addr:full" => full = v.to_string(),
            // `addr:block_number` is the Japanese chōme block id —
            // promoted into housenumber when housenumber is empty.
            "addr:block_number" => block_number = v.to_string(),
            // City fallbacks for Japanese / non-Western admin levels.
            "addr:province" => province = v.to_string(),
            "addr:quarter" => quarter = v.to_string(),
            _ => {}
        }
    }

    // Resolve the canonical street + remember which source it came
    // from. The fallback path (`addr:full` / `addr:place`) drives the
    // relaxed signal threshold below.
    //
    // Country packs may override the canonical `street` key — Japan's
    // pack remaps `street` to `addr:full` because Japanese addresses
    // don't decompose into street + housenumber. When the pack's
    // override IS one of the documented fallback tags, the captured
    // value is semantically a Fallback signal even though it entered
    // via the pack's `street` channel; treating it as Strict would
    // require a housenumber and silently drop every JP record without
    // a separate `addr:housenumber` tag (a vanishingly rare condition
    // in OSM JP data, cf. Copilot review on PR #183).
    let pack_street_is_fallback_tag = cfg.street == "addr:full" || cfg.street == "addr:place";
    let (resolved_street, source) = if !street.is_empty() {
        let s = if pack_street_is_fallback_tag {
            StreetSource::Fallback
        } else {
            StreetSource::Strict
        };
        (street, s)
    } else if !full.is_empty() {
        (full, StreetSource::Fallback)
    } else if !place.is_empty() {
        (place, StreetSource::Fallback)
    } else {
        (String::new(), StreetSource::None)
    };
    let resolved_housenumber = if !housenumber.is_empty() {
        housenumber
    } else {
        block_number
    };
    let resolved_locality = if !city.is_empty() {
        city
    } else if !quarter.is_empty() {
        quarter
    } else {
        province
    };
    (
        resolved_street,
        source,
        resolved_housenumber,
        postcode,
        resolved_locality,
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tags(pairs: &[(&'static str, &'static str)]) -> Vec<(String, String)> {
        pairs
            .iter()
            .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
            .collect()
    }

    fn run(
        pairs: Vec<(String, String)>,
        cfg: &OsmTags,
    ) -> (String, StreetSource, String, String, String) {
        let refs: Vec<(&str, &str)> = pairs
            .iter()
            .map(|(k, v)| (k.as_str(), v.as_str()))
            .collect();
        pull_addr_tags(refs, cfg)
    }

    #[test]
    fn strict_path_requires_housenumber() {
        let cfg = default_tags();
        let (street, source, hn, pc, loc) = run(
            tags(&[
                ("addr:street", "Rue Wayez"),
                ("addr:postcode", "1070"),
                ("addr:city", "Anderlecht"),
            ]),
            &cfg,
        );
        assert_eq!(street, "Rue Wayez");
        assert_eq!(source, StreetSource::Strict);
        assert!(hn.is_empty());
        assert!(!has_minimum_signal(source, &hn, &pc, &loc));
    }

    #[test]
    fn fallback_path_accepts_postcode_only() {
        let cfg = default_tags();
        let (street, source, hn, pc, loc) = run(
            tags(&[
                ("addr:full", "東京都千代田区千代田1-1"),
                ("addr:postcode", "100-0001"),
            ]),
            &cfg,
        );
        assert_eq!(street, "東京都千代田区千代田1-1");
        assert_eq!(source, StreetSource::Fallback);
        assert!(hn.is_empty());
        assert!(has_minimum_signal(source, &hn, &pc, &loc));
    }

    #[test]
    fn pack_override_to_addr_full_treats_value_as_fallback_signal() {
        // JP pack publishes street=addr:full. The value DOES enter
        // through the pack's `street` channel, but `addr:full` is
        // semantically a fallback tag (block-based addressing) and
        // must NOT be subjected to the strict-path housenumber
        // requirement. Otherwise every JP record without a separate
        // `addr:housenumber` is silently dropped — the bug Copilot
        // flagged on PR #183.
        let cfg = OsmTags {
            postcode: "addr:postcode".to_string(),
            street: "addr:full".to_string(),
            housenumber: "addr:housenumber".to_string(),
            city: "addr:city".to_string(),
        };
        let (street, source, hn, pc, loc) = run(
            tags(&[
                ("addr:full", "東京都千代田区千代田1-1"),
                ("addr:postcode", "100-0001"),
            ]),
            &cfg,
        );
        assert_eq!(street, "東京都千代田区千代田1-1");
        assert_eq!(
            source,
            StreetSource::Fallback,
            "pack-override of street to addr:full must surface as Fallback"
        );
        assert!(hn.is_empty());
        assert!(
            has_minimum_signal(source, &hn, &pc, &loc),
            "JP-style addr:full + postcode must be accepted (relaxed signal)"
        );
    }

    #[test]
    fn pack_override_to_addr_place_treats_value_as_fallback_signal() {
        // `addr:place` is the second documented fallback tag (used in
        // some Latin American + rural contexts where the address has
        // a place name but no street). Any pack that elects to remap
        // `street` to `addr:place` must trigger the same relaxed
        // signal path as a pack-overridden `addr:full`.
        let cfg = OsmTags {
            postcode: "addr:postcode".to_string(),
            street: "addr:place".to_string(),
            housenumber: "addr:housenumber".to_string(),
            city: "addr:city".to_string(),
        };
        let (_, source, hn, pc, loc) = run(
            tags(&[("addr:place", "Some Place"), ("addr:city", "City")]),
            &cfg,
        );
        assert_eq!(source, StreetSource::Fallback);
        assert!(hn.is_empty());
        assert!(has_minimum_signal(source, &hn, &pc, &loc));
    }

    #[test]
    fn standard_pack_with_addr_street_still_strict() {
        // Sanity: the BE/FR/DE/etc. shipped packs use street=addr:street.
        // Records via the standard channel must remain Strict and
        // demand a housenumber.
        let cfg = default_tags();
        let (street, source, hn, _, _) = run(
            tags(&[("addr:street", "Rue Wayez"), ("addr:housenumber", "122")]),
            &cfg,
        );
        assert_eq!(street, "Rue Wayez");
        assert_eq!(source, StreetSource::Strict);
        assert_eq!(hn, "122");
    }

    #[test]
    fn no_street_no_record() {
        let cfg = default_tags();
        let (_, source, _, _, _) = run(
            tags(&[("addr:postcode", "1070"), ("addr:city", "Anderlecht")]),
            &cfg,
        );
        assert_eq!(source, StreetSource::None);
        assert!(!has_minimum_signal(source, "", "1070", "Anderlecht"));
    }
}
