//! Authoritative-source ingestion (#96 §"Data Sources").
//!
//! Each source maps an upstream open-data dataset to the same
//! normalised [`crate::shard::AddressRecord`] shape so the shard
//! builder is source-agnostic.
//!
//! ## Coverage today
//!
//! - [`osm`] — wraps `crate::osm_extract` so the existing OSM PBF
//!   path goes through the same `Source` interface.
//! - [`openaddresses`] — OpenAddresses streaming loader. Reads the
//!   canonical gzipped GeoJSON-seq published at
//!   `https://v2.openaddresses.io/batch-prod/job/<id>/source.geojson.gz`,
//!   plus zipped/CSV variants.
//!
//! Adding a new source = new module + new `Source` impl + new
//! `SourceTag` variant + new `[[address]]` entry in the relevant
//! `dl/regions/<country>.toml`. No core code change.
//!
//! ## Why OpenAddresses, not per-country authoritative datasets
//!
//! BOSA (Belgium), BAN (France), BAG (Netherlands), BD-Adresses
//! (Luxembourg), BEV (Austria), G-NAF (Australia), and several dozen
//! state-level datasets for the US/DE/AT/CH all live **upstream** of
//! OpenAddresses. OA ingests each one via a per-source manifest and
//! republishes a normalised feed with the same set of properties
//! (`number`, `street`, `unit`, `city`, `postcode`, `id`, …). Going
//! through OA gives butterfly-geocode one ingestion code path
//! (instead of seven heterogeneous loaders), one normalised schema,
//! and one update cadence (weekly).
//!
//! Operators wanting maximum recency for a single country can still
//! point the loader directly at the upstream dataset (e.g. a fresh
//! BOSA ZIP) — OA's CSV format intentionally mirrors BOSA's column
//! names where they overlap, so the same loader works.
//!
//! ## Merge dedup
//!
//! [`merge_records`] combines records from multiple sources into one
//! shard. Conflicts (same `(postcode, normalized street, housenumber)`
//! within ~30 m) resolve toward the highest-priority authoritative
//! source — see [`merge_priority`]. OpenAddresses wins over OSM. The
//! OSM fallback survives for addresses no authoritative source covers
//! (rare buildings, recent mappings, countries OA doesn't yet index).

pub mod openaddresses;
pub mod osm;

use std::path::Path;

use anyhow::Result;

use crate::shard::{AddressRecord, SourceTag};

/// A pluggable address-source loader.
///
/// Implementations stream records out of an upstream dataset and emit
/// them through the callback. Streaming is required because
/// OpenAddresses at hundreds of millions of records exceeds
/// comfortable in-memory buffering.
///
/// The callback returns `()` and is expected to push into a
/// caller-owned `Vec` or directly into a shard builder. A future
/// follow-up may switch the builder to a streaming sink so the entire
/// path stays bounded-memory; today the callback fills a `Vec`
/// because the shard builder needs all records up-front for sorting.
pub trait Source {
    /// Stable [`SourceTag`] this source emits.
    fn tag(&self) -> SourceTag;

    /// Stream every record. The callback is called once per record.
    /// Errors propagate; partial output is the caller's problem to
    /// reset.
    fn stream(
        &self,
        progress: &mut dyn FnMut(SourceProgress),
        emit: &mut dyn FnMut(AddressRecord),
    ) -> Result<()>;
}

/// Progress event for long-running source loaders. Emitted at fixed
/// intervals (every ~100k records) so callers can render a progress
/// line without flooding the log.
#[derive(Debug, Clone, Copy)]
pub enum SourceProgress {
    /// Phase boundary: useful for two-pass loaders that want to log
    /// "scanning nodes" then "scanning ways" separately.
    Phase { phase: &'static str },
    /// Mid-run progress: how many input rows have been seen and how
    /// many records have been emitted so far. The two diverge when
    /// the loader filters (OA rows missing street/housenumber, OSM
    /// nodes without `addr:*` tags, etc.).
    Records {
        rows_seen: u64,
        records_emitted: u64,
    },
}

/// Load every source in `sources` sequentially and concatenate their
/// records. The output is the input to the shard builder.
///
/// Callers wanting per-source records (for the `--merge` CLI path,
/// or for source-specific metrics) should call each [`Source::stream`]
/// directly — this helper is the convenience function for the common
/// "single-source build" case.
pub fn collect_all<S: Source + ?Sized>(
    source: &S,
    mut progress: impl FnMut(SourceProgress),
) -> Result<Vec<AddressRecord>> {
    let mut out = Vec::new();
    source.stream(&mut |p| progress(p), &mut |r| out.push(r))?;
    Ok(out)
}

/// Sort key used by [`merge_records`] to group records that should
/// be deduped against each other. Goes through the shared
/// [`crate::parser::normalize::normalize`] so accent folding,
/// punctuation collapse, and whitespace collapse all match the
/// shard's inverted-index keys. Without this, `Chaussée de Mons`
/// (OA, normalized via OSM upstream) and `Chaussee de Mons` (OSM)
/// end up in different dedup groups and ship as duplicate records.
fn dedup_key(rec: &AddressRecord) -> (String, String, String) {
    (
        crate::parser::normalize::normalize(rec.postcode.trim()),
        crate::parser::normalize::normalize(rec.street.trim()),
        crate::parser::normalize::normalize(rec.housenumber.trim()),
    )
}

/// Spatial proximity threshold for merge dedup, in degrees. ~30 m at
/// Belgium's latitude (lat=50.83). When two records share the same
/// `dedup_key` AND fall within this radius, they are considered the
/// same physical address and the higher-priority source wins.
///
/// Bigger threshold = more aggressive merge (drops near-duplicates
/// even when they're slightly off). Smaller threshold = more
/// conservative (keeps both records when the upstream sources
/// disagree on geolocation by more than ~30 m). 30 m matches the
/// "snapped road point" semantics used elsewhere in the codebase.
const MERGE_RADIUS_DEG: f64 = 0.0003;

/// Source-priority ranking used by [`merge_records`]. Authoritative
/// sources (OpenAddresses) outrank OSM. Higher number = higher
/// priority.
fn merge_priority(tag: SourceTag) -> u8 {
    match tag {
        SourceTag::OpenAddresses => 100,
        SourceTag::Osm => 10,
    }
}

/// Merge multiple per-source record vectors into one. Records sharing
/// the same `dedup_key` AND within `MERGE_RADIUS_DEG` are deduped to
/// the highest-priority source. Records from different physical
/// locations or with different street/housenumber values survive
/// independently.
///
/// Order-stable for ties: when two sources have equal priority, the
/// earlier vector in `inputs` wins. This is rare in the MVP (one
/// authoritative source per country) but matters for callers that
/// merge multiple OSM extracts or multiple OA per-state shards.
#[must_use]
pub fn merge_records(inputs: Vec<Vec<AddressRecord>>) -> Vec<AddressRecord> {
    use std::collections::HashMap;

    // Group records by (postcode, street, housenumber). Within a
    // group, dedup by spatial proximity.
    let mut by_key: HashMap<(String, String, String), Vec<AddressRecord>> = HashMap::new();
    for vec in inputs {
        for rec in vec {
            let k = dedup_key(&rec);
            by_key.entry(k).or_default().push(rec);
        }
    }

    let mut out = Vec::with_capacity(by_key.len());
    for (_, group) in by_key {
        let merged = dedup_group(group);
        out.extend(merged);
    }

    // Stable order so shard byte-comparison reproducibility holds.
    out.sort_by(|a, b| {
        a.postcode
            .cmp(&b.postcode)
            .then(a.street.cmp(&b.street))
            .then(a.housenumber.cmp(&b.housenumber))
            .then(
                a.lat
                    .partial_cmp(&b.lat)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
            .then(
                a.lon
                    .partial_cmp(&b.lon)
                    .unwrap_or(std::cmp::Ordering::Equal),
            )
    });
    out
}

/// Dedup a `dedup_key`-equivalent group by spatial proximity.
/// Returns the survivors. Algorithm: O(n²) within the group (groups
/// are typically size 1-3), pick the highest-priority source within
/// each spatial cluster.
fn dedup_group(group: Vec<AddressRecord>) -> Vec<AddressRecord> {
    if group.len() <= 1 {
        return group;
    }
    let n = group.len();
    let mut taken = vec![false; n];
    let mut survivors = Vec::with_capacity(n);

    for i in 0..n {
        if taken[i] {
            continue;
        }
        // Cluster: every record within radius of `group[i]`.
        let mut cluster: Vec<usize> = vec![i];
        for j in (i + 1)..n {
            if taken[j] {
                continue;
            }
            let dlat = (group[i].lat - group[j].lat).abs();
            let dlon = (group[i].lon - group[j].lon).abs();
            if dlat < MERGE_RADIUS_DEG && dlon < MERGE_RADIUS_DEG {
                cluster.push(j);
            }
        }
        // Pick the highest-priority record from the cluster.
        // First-wins on tie: when two cluster members share the same
        // priority (e.g. two OSM records, or two `--merge` inputs at
        // equal authoritative priority), keep the one that arrived
        // earliest in `inputs`. `max_by_key` returns the LAST equal
        // max, so we can't use it directly; iterate manually keeping
        // the first time the maximum priority is observed.
        let mut winner_idx: usize = cluster[0];
        let mut winner_prio: u8 = merge_priority(group[winner_idx].source);
        for &k in cluster.iter().skip(1) {
            let prio = merge_priority(group[k].source);
            if prio > winner_prio {
                winner_idx = k;
                winner_prio = prio;
            }
        }
        survivors.push(group[winner_idx].clone());
        for k in cluster {
            taken[k] = true;
        }
    }
    survivors
}

/// Dispatch table used by the CLI: parse a `(format, path)` pair and
/// load the source. Unknown formats return an error.
///
/// Today: `openaddresses` / `oa` / `csv` → OpenAddresses loader.
/// `pbf` / `osm` → OSM loader. New formats land here.
pub fn load_by_format(
    format: &str,
    path: &Path,
    country: crate::routing::CountryId,
) -> Result<Vec<AddressRecord>> {
    match format {
        "openaddresses" | "open-addresses" | "oa" | "csv" | "geojson" | "geojsonseq"
        | "geojson-gz" | "ndjson" => {
            let loader = openaddresses::OpenAddressesSource::new(path, country);
            collect_all(&loader, |_| {})
        }
        "pbf" | "osm" => {
            let loader = osm::OsmPbfSource::new(path, country);
            collect_all(&loader, |_| {})
        }
        other => Err(anyhow::anyhow!(
            "unknown source format '{other}' (supported: openaddresses|oa|csv|geojson, pbf|osm)"
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(
        postcode: &str,
        street: &str,
        house: &str,
        lat: f64,
        lon: f64,
        source: SourceTag,
    ) -> AddressRecord {
        AddressRecord {
            lat,
            lon,
            street: street.into(),
            housenumber: house.into(),
            postcode: postcode.into(),
            locality: "X".into(),
            source,
            source_id: None,
        }
    }

    #[test]
    fn merge_dedups_within_radius_keeps_higher_priority() {
        // OA + OSM at the same physical address — OA wins.
        let oa = vec![rec(
            "1070",
            "Rue Wayez",
            "122",
            50.834,
            4.314,
            SourceTag::OpenAddresses,
        )];
        let osm = vec![rec(
            "1070",
            "rue wayez",
            "122",
            50.8341,
            4.3141,
            SourceTag::Osm,
        )];
        let merged = merge_records(vec![oa, osm]);
        assert_eq!(merged.len(), 1, "expected single record after dedup");
        assert_eq!(merged[0].source, SourceTag::OpenAddresses);
    }

    #[test]
    fn merge_keeps_unique_records() {
        // Two different addresses survive.
        let a = vec![rec(
            "1070",
            "Rue Wayez",
            "122",
            50.834,
            4.314,
            SourceTag::OpenAddresses,
        )];
        let b = vec![rec(
            "2000",
            "Grote Markt",
            "1",
            51.221,
            4.401,
            SourceTag::OpenAddresses,
        )];
        let merged = merge_records(vec![a, b]);
        assert_eq!(merged.len(), 2);
    }

    #[test]
    fn merge_keeps_records_outside_radius() {
        // Same postcode/street/housenumber but ~5 km apart — keep
        // both because spatial split says they're not the same place.
        let a = vec![rec("1070", "Rue X", "1", 50.8, 4.3, SourceTag::Osm)];
        let b = vec![rec("1070", "Rue X", "1", 50.85, 4.35, SourceTag::Osm)];
        let merged = merge_records(vec![a, b]);
        assert_eq!(
            merged.len(),
            2,
            "spatial split should keep both records when >>30m apart"
        );
    }

    #[test]
    fn merge_osm_only_keeps_all() {
        // Two OSM-only records that are coincidentally at the same
        // address: tie on priority, dedup keeps the first by stable
        // sort.
        let a = vec![rec("1070", "Rue X", "1", 50.834, 4.314, SourceTag::Osm)];
        let b = vec![rec("1070", "Rue X", "1", 50.8341, 4.3141, SourceTag::Osm)];
        let merged = merge_records(vec![a, b]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].source, SourceTag::Osm);
    }

    #[test]
    fn dedup_key_normalises_whitespace_and_case() {
        let a = rec("1070", "Rue Wayez", "122", 50.0, 4.0, SourceTag::Osm);
        let b = rec(" 1070 ", "rue wayez", "122", 50.0, 4.0, SourceTag::Osm);
        assert_eq!(dedup_key(&a), dedup_key(&b));
    }

    #[test]
    fn priority_order_openaddresses_beats_osm() {
        assert!(merge_priority(SourceTag::OpenAddresses) > merge_priority(SourceTag::Osm));
    }

    #[test]
    fn dedup_key_normalizes_diacritics() {
        // OpenAddresses: "Chaussée de Mons", OSM: "Chaussee de Mons" —
        // must land in the same group so the merge dedup sees them.
        let oa = rec(
            "1070",
            "Chaussée de Mons",
            "122",
            50.834,
            4.314,
            SourceTag::OpenAddresses,
        );
        let osm = rec(
            "1070",
            "Chaussee de Mons",
            "122",
            50.8341,
            4.3141,
            SourceTag::Osm,
        );
        assert_eq!(dedup_key(&oa), dedup_key(&osm));
        let merged = merge_records(vec![vec![oa], vec![osm]]);
        assert_eq!(merged.len(), 1, "diacritic dedup should collapse the pair");
        assert_eq!(merged[0].source, SourceTag::OpenAddresses);
    }

    #[test]
    fn winner_first_wins_on_priority_tie() {
        // Two `--merge` inputs at equal priority. The earlier vector
        // (representing the earlier `--merge` arg) must win.
        let earlier = rec("1070", "Rue X", "1", 50.8340, 4.3140, SourceTag::Osm);
        let later = rec("1070", "Rue X", "1", 50.83405, 4.31405, SourceTag::Osm);
        // Tag the earlier with a stable distinguishing source_id to
        // assert which one survived — both are OSM so source priority
        // is identical.
        let mut earlier_marked = earlier.clone();
        earlier_marked.source_id = Some("EARLIER".to_string());
        let mut later_marked = later.clone();
        later_marked.source_id = Some("LATER".to_string());
        let merged = merge_records(vec![vec![earlier_marked], vec![later_marked]]);
        assert_eq!(merged.len(), 1);
        assert_eq!(merged[0].source_id.as_deref(), Some("EARLIER"));
    }
}
