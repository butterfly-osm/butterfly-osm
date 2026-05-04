//! OSM PBF address-tag extractor.
//!
//! Two-pass: first pass collects node coordinates and emits node
//! addresses; second pass resolves way addresses by averaging
//! resolved node coordinates (centroid proxy).

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};

use crate::shard::{AddressRecord, SourceTag};

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
                if let Some(rec) = tags_to_address(node.lat(), node.lon(), node.tags()) {
                    records.push(rec);
                    node_addr_records += 1;
                }
            }
            Element::DenseNode(node) => {
                nodes_seen += 1;
                node_coords.insert(node.id(), (node.lat(), node.lon()));
                if let Some(rec) = tags_to_address(node.lat(), node.lon(), node.tags()) {
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
                if let Some(rec) = way_to_address(&way, &node_coords) {
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

/// Decide whether a tag bag carries enough address signal to be worth
/// storing.
///
/// Conventional address: street + housenumber must both be present
/// (the European/US/AU pattern).
///
/// Block-based address (Japan, Korea, parts of Latin America): the
/// `addr:full` tag carries the whole address as a single string and
/// neither `addr:street` nor `addr:housenumber` is reliably set. We
/// accept records where `addr:full` is non-empty as a pack-agnostic
/// fallback; the geocoder treats the `street` field as the queryable
/// canonical form regardless of which OSM tag fed it.
fn has_minimum_signal(street: &str, housenumber: &str, postcode: &str, locality: &str) -> bool {
    if !street.is_empty() && !housenumber.is_empty() {
        return true;
    }
    // Block-based / place-based fallback: a non-empty `street` (which
    // may have come from `addr:full` or `addr:place`) plus locality
    // is enough to anchor a record.
    if !street.is_empty() && (!postcode.is_empty() || !locality.is_empty()) {
        return true;
    }
    false
}

fn tags_to_address<'a, I>(lat: f64, lon: f64, tags: I) -> Option<AddressRecord>
where
    I: IntoIterator<Item = (&'a str, &'a str)>,
{
    let (street, housenumber, postcode, locality) = pull_addr_tags(tags);
    if !has_minimum_signal(&street, &housenumber, &postcode, &locality) {
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
) -> Option<AddressRecord> {
    let (street, housenumber, postcode, locality) = pull_addr_tags(way.tags());
    if !has_minimum_signal(&street, &housenumber, &postcode, &locality) {
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

fn pull_addr_tags<'a, I>(tags: I) -> (String, String, String, String)
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

    for (k, v) in tags {
        match k {
            "addr:street" => street = v.to_string(),
            "addr:place" => place = v.to_string(),
            // `addr:full` is the standard fallback for countries where
            // street+number doesn't decompose cleanly (Japan blocks,
            // Korean addresses, freeform rural addressing).
            "addr:full" => full = v.to_string(),
            "addr:housenumber" => housenumber = v.to_string(),
            // `addr:block_number` is the Japanese chōme block id —
            // promoted into housenumber when housenumber is empty.
            "addr:block_number" => block_number = v.to_string(),
            "addr:postcode" => postcode = v.to_string(),
            "addr:city" => city = v.to_string(),
            // City fallbacks for Japanese / non-Western admin levels.
            "addr:province" => province = v.to_string(),
            "addr:quarter" => quarter = v.to_string(),
            _ => {}
        }
    }

    // Resolve the canonical street: prefer the explicit street tag,
    // then `addr:full`, then `addr:place`. The shard schema only has
    // one street field, so this collapses the diversity at ingest.
    let resolved_street = if !street.is_empty() {
        street
    } else if !full.is_empty() {
        full
    } else {
        place
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
        resolved_housenumber,
        postcode,
        resolved_locality,
    )
}
