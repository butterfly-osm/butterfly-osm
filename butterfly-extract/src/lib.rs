//! OSM data processing and extraction for butterfly-osm

pub mod pbf;
pub mod sieve;

/// OSM data extraction and processing pipeline
#[derive(Default)]
pub struct Extractor {}

impl Extractor {
    pub fn new() -> Self {
        Self {}
    }
}
