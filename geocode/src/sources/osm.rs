//! OSM PBF wrapper that adapts the existing `osm_extract` two-pass
//! extractor to the [`Source`] trait. This is the global fallback for
//! countries without a BOSA/BAN/BAG-style authoritative dataset.
//!
//! Implementation detail: `osm_extract::extract_addresses` collects
//! all records into a `Vec` before returning, so the Source-trait
//! "streaming" guarantee is best-effort here — for OSM PBF the entire
//! address-tag set fits comfortably in memory anyway (Belgium ~170 K
//! records, global ~200 M but always one-country-at-a-time per the
//! shard model). A future change to `osm_extract` to support
//! streaming would make this loader bounded-memory automatically.

use std::path::{Path, PathBuf};

use anyhow::Result;

use crate::osm_extract::{ExtractProgress, extract_addresses};
use crate::routing::CountryId;
use crate::shard::{AddressRecord, SourceTag};

use super::{Source, SourceProgress};

#[derive(Debug, Clone)]
pub struct OsmPbfSource {
    pbf_path: PathBuf,
    /// Country tag for the records emitted. `osm_extract` does not
    /// look at country boundaries — it trusts the PBF to be a
    /// per-country extract (Geofabrik regional extracts, e.g.
    /// `belgium-latest.osm.pbf`).
    country: CountryId,
}

impl OsmPbfSource {
    pub fn new(pbf_path: impl AsRef<Path>, country: CountryId) -> Self {
        Self {
            pbf_path: pbf_path.as_ref().to_path_buf(),
            country,
        }
    }

    #[must_use]
    pub fn country(&self) -> CountryId {
        self.country
    }
}

impl Source for OsmPbfSource {
    fn tag(&self) -> SourceTag {
        SourceTag::Osm
    }

    fn stream(
        &self,
        progress: &mut dyn FnMut(SourceProgress),
        emit: &mut dyn FnMut(AddressRecord),
    ) -> Result<()> {
        // Adapt the existing two-pass progress shape to the unified
        // `SourceProgress` enum. We forward node and way passes as
        // separate phases plus mid-run record counts.
        let recs = extract_addresses(&self.pbf_path, |evt| match evt {
            ExtractProgress::Phase { phase } => progress(SourceProgress::Phase { phase }),
            ExtractProgress::NodePass {
                nodes_seen,
                addresses_emitted,
            } => progress(SourceProgress::Records {
                rows_seen: nodes_seen,
                records_emitted: addresses_emitted,
            }),
            ExtractProgress::WayPass {
                ways_seen,
                addresses_emitted,
            } => progress(SourceProgress::Records {
                rows_seen: ways_seen,
                records_emitted: addresses_emitted,
            }),
        })?;

        // `osm_extract` already sets `source = Osm` and `source_id =
        // None` per its tag-extractor contract; we re-emit each record
        // unchanged. The country field is informational only — the
        // shard header carries the per-shard country and OSM PBFs are
        // already country-bounded.
        for r in recs {
            emit(r);
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tag_is_osm() {
        let s = OsmPbfSource::new("/nonexistent.pbf", CountryId::BE);
        assert_eq!(s.tag(), SourceTag::Osm);
        assert_eq!(s.country(), CountryId::BE);
    }
}
