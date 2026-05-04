//! Read OSM PBF, extract addr-tagged nodes, return GoldRecords.
//!
//! Belgium-specific note: BE addr nodes from OSM are a subset of authoritative
//! BOSA. They cover a meaningful sample but lack BOSA's coverage of new
//! subdivisions and recent splits. For research-grade corpus this is fine —
//! the parallel data-prep agent will add the actual BOSA shard later.

use anyhow::{Context, Result};
use osmpbf::{Element, ElementReader};
use std::path::Path;

/// Iterator over OSM tag (key, value) pairs as borrowed string slices.
type TagIter<'a> = Box<dyn Iterator<Item = (&'a str, &'a str)> + 'a>;

#[derive(Debug, Clone)]
pub struct GoldRecord {
    pub osm_id: i64,
    pub country: String,
    pub street: Option<String>,
    pub housenumber: Option<String>,
    pub postcode: Option<String>,
    pub city: Option<String>,
    /// Free-form unit/floor/etc. — kept separate so the BIO labeler can decide
    /// whether to render it. Most BE OSM nodes don't have one.
    pub unit: Option<String>,
    /// Coordinates from the OSM node. Used by the bench query generator —
    /// not used by training (the parser doesn't see coordinates as input).
    pub lat: f64,
    pub lon: f64,
}

impl GoldRecord {
    /// Useful gold records have at least street + (housenumber OR postcode).
    /// Anything weaker is too noisy to train on.
    pub fn is_usable(&self) -> bool {
        self.street.is_some() && (self.housenumber.is_some() || self.postcode.is_some())
    }
}

pub fn read_pbf(path: &Path, country: &str, limit: usize) -> Result<Vec<GoldRecord>> {
    if !path.exists() {
        anyhow::bail!(
            "PBF not found at {}. The parallel data-prep agent owns dl/regions/ and data/ — \
             run that pipeline first, or pass --pbf with an existing PBF for testing.",
            path.display()
        );
    }
    let reader = ElementReader::from_path(path)
        .with_context(|| format!("opening PBF {}", path.display()))?;
    let mut out: Vec<GoldRecord> = Vec::new();
    let mut limit_hit = false;

    reader.for_each(|el| {
        if limit_hit {
            return;
        }
        // We accept Node and DenseNode (and Way for BAG-like polygons later).
        // For the MVP we only ingest Nodes/DenseNodes — those are 99% of `addr:*`
        // tagged elements in Belgium OSM.
        let (id, lat, lon, tags): (i64, f64, f64, TagIter<'_>) = match el {
            Element::Node(n) => (n.id(), n.lat(), n.lon(), Box::new(n.tags())),
            Element::DenseNode(n) => (n.id(), n.lat(), n.lon(), Box::new(n.tags())),
            _ => return,
        };

        let mut g = GoldRecord {
            osm_id: id,
            country: country.to_string(),
            street: None,
            housenumber: None,
            postcode: None,
            city: None,
            unit: None,
            lat,
            lon,
        };
        let mut has_addr_tag = false;
        for (k, v) in tags {
            match k {
                "addr:street" => {
                    g.street = Some(v.to_string());
                    has_addr_tag = true;
                }
                "addr:housenumber" => {
                    g.housenumber = Some(v.to_string());
                    has_addr_tag = true;
                }
                "addr:postcode" => {
                    g.postcode = Some(v.to_string());
                    has_addr_tag = true;
                }
                "addr:city" => {
                    g.city = Some(v.to_string());
                    has_addr_tag = true;
                }
                "addr:unit" | "addr:flats" => {
                    g.unit = Some(v.to_string());
                    has_addr_tag = true;
                }
                _ => {}
            }
        }
        if has_addr_tag && g.is_usable() {
            out.push(g);
            if limit > 0 && out.len() >= limit {
                limit_hit = true;
            }
        }
    })?;

    Ok(out)
}
