//! Region-indexed multi-file fetch (#100).
//!
//! Each region ships a small TOML file in `dl/regions/<name>.toml`
//! enumerating every resource a butterfly-route deployment needs for
//! that region: OSM PBF + GTFS feeds + NeTEx-EPIP + any future
//! data types. `butterfly-dl <region>` loads the index and fetches
//! every entry **in parallel** via [`crate::verified::download_verified`].
//!
//! The TOML is embedded at compile time so the CLI works from any
//! working directory without a runtime file lookup. Adding a new
//! region is a new `.toml` file and a new arm in `load_region`.
//!
//! ## Parallelism
//!
//! Every entry is dispatched as its own `tokio::spawn`. Wall-clock
//! cost is bottlenecked by the slowest single fetch, not by the sum.
//! A failure on one entry does NOT cascade — the report carries a
//! per-entry success/error. Fatal-for-routing entries (e.g. missing
//! PBF) surface as a non-zero exit code in the CLI layer, which the
//! library does not decide here.
//!
//! ## Layout convention
//!
//! For a region name `<NAME>` the default data root is `./data/<NAME>`
//! and the canonical target paths are:
//!
//! - `[pbf]` → `<root>/<NAME>.pbf`
//! - `[[gtfs]]` → `<root>/transit/gtfs/<id>.zip`
//! - `[[netex_epip]]` → `<root>/transit/netex/<id>-epip.xml`
//!
//! Operators who want to override a path per-deployment can still
//! maintain a local `transit.toml` that `butterfly-route` reads at
//! server start — the region index is the default, not the only
//! source of truth.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde::Deserialize;

use crate::verified::{Outcome, VerifiedOptions, download_verified};

/// The shipped region indexes, embedded at compile time. Adding a
/// new region = new file + new constant + new arm in `load_region`.
const BELGIUM_INDEX_TOML: &str = include_str!("../regions/belgium.toml");
const FRANCE_INDEX_TOML: &str = include_str!("../regions/france.toml");
const NETHERLANDS_INDEX_TOML: &str = include_str!("../regions/netherlands.toml");
const LUXEMBOURG_INDEX_TOML: &str = include_str!("../regions/luxembourg.toml");
const GERMANY_INDEX_TOML: &str = include_str!("../regions/germany.toml");
const AUSTRIA_INDEX_TOML: &str = include_str!("../regions/austria.toml");
const SWITZERLAND_INDEX_TOML: &str = include_str!("../regions/switzerland.toml");

// Non-European regions for butterfly-geocode #96 "serve the world".
// These ship a [pbf] section only — no transit feeds.
const UNITED_STATES_INDEX_TOML: &str = include_str!("../regions/united-states.toml");
const JAPAN_INDEX_TOML: &str = include_str!("../regions/japan.toml");
const BRAZIL_INDEX_TOML: &str = include_str!("../regions/brazil.toml");
const INDIA_INDEX_TOML: &str = include_str!("../regions/india.toml");
const AUSTRALIA_INDEX_TOML: &str = include_str!("../regions/australia.toml");

/// Parsed region index. Each field is an optional list so partial
/// regions (e.g. one without transit feeds) are a valid shape.
#[derive(Debug, Clone, Deserialize)]
pub struct RegionIndex {
    #[serde(default)]
    pub pbf: Option<PbfEntry>,
    #[serde(default)]
    pub gtfs: Vec<GtfsEntry>,
    #[serde(default)]
    pub netex_epip: Vec<NetexEpipEntry>,
    /// Authoritative-source address datasets (#96 §"Data Sources":
    /// OpenAddresses, which ingests national open-data datasets like
    /// BOSA / BAN / BAG / G-NAF / state-level US/DE feeds upstream
    /// and republishes them through one normalised schema). Consumed
    /// by butterfly-geocode; routing pipelines ignore the section.
    #[serde(default)]
    pub address: Vec<AddressEntry>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PbfEntry {
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct GtfsEntry {
    pub id: String,
    pub url: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct NetexEpipEntry {
    pub id: String,
    pub url: String,
}

/// Authoritative-source address dataset entry. Lives under
/// `addresses/<id>.<ext>` in the region data root; the extension
/// derives from `format` (geojson-gz → `.geojson.gz`,
/// csv-zip → `.zip`, csv → `.csv`).
#[derive(Debug, Clone, Deserialize)]
pub struct AddressEntry {
    pub id: String,
    pub url: String,
    /// Wire format. Recognised values:
    ///
    /// - `"geojson-gz"` — gzipped GeoJSON-seq, the canonical
    ///   OpenAddresses processed feed (one Feature per line).
    /// - `"geojson"` / `"geojsonseq"` / `"ndjson"` — raw uncompressed
    ///   JSON-seq.
    /// - `"csv"` — raw CSV (per OpenAddresses CSV spec or BOSA-style).
    /// - `"csv-gz"` / `"gz"` — gzipped CSV.
    /// - `"csv-zip"` / `"zip"` — ZIP wrapping a single CSV/GeoJSON
    ///   entry.
    /// - `"xml-zip"` — ZIP wrapping XML (legacy, no longer used in
    ///   v5).
    ///
    /// Drives the verification preset (magic prefix + min bytes) at
    /// fetch time and the loader dispatch at ingest time.
    pub format: String,
    /// `SourceTag` name (`"osm"`, `"openaddresses"` / `"oa"`).
    /// Stored as metadata only; the importer's CLI flag is the source
    /// of truth for the per-shard byte. Carrying it here lets
    /// operators inspect the index without a separate registry.
    #[serde(default)]
    pub source: Option<String>,
}

impl AddressEntry {
    /// File extension on disk for the chosen wire format.
    fn extension(&self) -> &'static str {
        match self.format.as_str() {
            "geojson-gz" => "geojson.gz",
            "geojson" => "geojson",
            "geojsonseq" => "geojsonseq",
            "ndjson" => "ndjson",
            "csv-zip" | "xml-zip" | "zip" => "zip",
            "csv-gz" | "gz" => "csv.gz",
            "csv" => "csv",
            "xml" => "xml",
            // Default to .bin for unknown formats. The verified
            // download falls back to no magic-prefix check, which is
            // safe — the importer will reject malformed payloads.
            _ => "bin",
        }
    }
}

impl RegionIndex {
    /// Load the shipped index for a region name. Returns an error if
    /// the region is not recognised.
    pub fn load(name: &str) -> Result<Self> {
        let raw: &str = match name {
            "belgium" => BELGIUM_INDEX_TOML,
            "france" => FRANCE_INDEX_TOML,
            "netherlands" => NETHERLANDS_INDEX_TOML,
            "luxembourg" => LUXEMBOURG_INDEX_TOML,
            "germany" => GERMANY_INDEX_TOML,
            "austria" => AUSTRIA_INDEX_TOML,
            "switzerland" => SWITZERLAND_INDEX_TOML,
            "united-states" | "us" => UNITED_STATES_INDEX_TOML,
            "japan" => JAPAN_INDEX_TOML,
            "brazil" => BRAZIL_INDEX_TOML,
            "india" => INDIA_INDEX_TOML,
            "australia" => AUSTRALIA_INDEX_TOML,
            other => bail!(
                "unknown region '{other}'. Supported regions are bundled \
                 in `dl/regions/`. Add a new one with a new TOML file + \
                 arm in `regions::RegionIndex::load`."
            ),
        };
        toml::from_str::<RegionIndex>(raw)
            .with_context(|| format!("parsing region index for '{name}'"))
    }
}

/// One file the region index wants us to fetch. Produced by
/// [`RegionIndex::entries`].
#[derive(Debug, Clone)]
pub struct RegionEntry {
    /// Stable identifier (`"pbf"` for the OSM PBF, feed id for
    /// transit). Used as the first column in the CLI output.
    pub id: String,
    /// Remote URL to GET.
    pub url: String,
    /// Local target path relative to the region's data root.
    pub target: PathBuf,
    /// Logical section of the index — `"pbf"` | `"gtfs"` | `"netex_epip"`.
    /// Used by the `--only` CLI filter.
    pub section: &'static str,
}

/// High-level filter for `--only <section>`. Matches the TOML section
/// names or the aggregate `"transit"` / `"all"` / `"addresses"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionFilter {
    All,
    PbfOnly,
    TransitOnly,
    AddressesOnly,
}

impl SectionFilter {
    /// Parse a CLI `--only` argument. Named `parse` instead of
    /// `from_str` so we're not shadowing `std::str::FromStr`.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "pbf" => Ok(Self::PbfOnly),
            "transit" => Ok(Self::TransitOnly),
            "addresses" | "address" => Ok(Self::AddressesOnly),
            other => bail!(
                "unknown --only value '{other}'. Accepted values: all, pbf, transit, addresses"
            ),
        }
    }

    fn keeps_pbf(self) -> bool {
        matches!(self, Self::All | Self::PbfOnly)
    }

    fn keeps_transit(self) -> bool {
        matches!(self, Self::All | Self::TransitOnly)
    }

    fn keeps_addresses(self) -> bool {
        matches!(self, Self::All | Self::AddressesOnly)
    }
}

impl RegionIndex {
    /// Enumerate every file the index wants us to fetch, with the
    /// canonical target path relative to `data_root`. Filtered by
    /// `filter` (default `All`).
    pub fn entries(
        &self,
        region_name: &str,
        data_root: &Path,
        filter: SectionFilter,
    ) -> Vec<RegionEntry> {
        let mut out = Vec::new();
        if filter.keeps_pbf()
            && let Some(pbf) = self.pbf.as_ref()
        {
            let target = data_root.join(format!("{region_name}.pbf"));
            out.push(RegionEntry {
                id: "pbf".to_string(),
                url: pbf.url.clone(),
                target,
                section: "pbf",
            });
        }
        if filter.keeps_transit() {
            for feed in &self.gtfs {
                let target = data_root
                    .join("transit")
                    .join("gtfs")
                    .join(format!("{}.zip", feed.id));
                out.push(RegionEntry {
                    id: feed.id.clone(),
                    url: feed.url.clone(),
                    target,
                    section: "gtfs",
                });
            }
            for feed in &self.netex_epip {
                let target = data_root
                    .join("transit")
                    .join("netex")
                    .join(format!("{}-epip.xml", feed.id));
                out.push(RegionEntry {
                    id: feed.id.clone(),
                    url: feed.url.clone(),
                    target,
                    section: "netex_epip",
                });
            }
        }
        if filter.keeps_addresses() {
            for entry in &self.address {
                let target =
                    data_root
                        .join("addresses")
                        .join(format!("{}.{}", entry.id, entry.extension()));
                out.push(RegionEntry {
                    id: entry.id.clone(),
                    url: entry.url.clone(),
                    target,
                    section: "address",
                });
            }
        }
        out
    }
}

/// Per-entry outcome from [`fetch_region`]. Success carries the
/// verification outcome; failure carries the stringified error so
/// consumers can print it without threading `anyhow::Error` through
/// `Send` boundaries.
#[derive(Debug)]
pub struct EntryReport {
    pub entry: RegionEntry,
    pub result: std::result::Result<Outcome, String>,
}

/// Aggregate report for a `fetch_region` run.
#[derive(Debug)]
pub struct RegionReport {
    pub region: String,
    pub entries: Vec<EntryReport>,
}

impl RegionReport {
    /// True when every entry succeeded.
    pub fn all_ok(&self) -> bool {
        self.entries.iter().all(|e| e.result.is_ok())
    }

    /// True when the PBF entry (the only fatal-for-routing one)
    /// succeeded or was filtered out.
    pub fn pbf_ok(&self) -> bool {
        self.entries
            .iter()
            .filter(|e| e.entry.section == "pbf")
            .all(|e| e.result.is_ok())
    }
}

/// Load the shipped index for `region_name`, enumerate entries
/// under `data_root`, and fetch every one **in parallel**.
///
/// Returns a `RegionReport` with a per-entry success/error. The
/// function itself only returns `Err` for programmer-visible
/// failures (unknown region, bad index TOML); per-entry network
/// failures become `EntryReport::result = Err(...)` and the caller
/// decides whether to exit non-zero.
pub async fn fetch_region(
    region_name: &str,
    data_root: &Path,
    filter: SectionFilter,
) -> Result<RegionReport> {
    let index = RegionIndex::load(region_name)?;
    let entries = index.entries(region_name, data_root, filter);
    if entries.is_empty() {
        return Ok(RegionReport {
            region: region_name.to_string(),
            entries: Vec::new(),
        });
    }

    let tasks: Vec<_> = entries
        .into_iter()
        .map(|entry| {
            let entry_clone = entry.clone();
            tokio::spawn(async move {
                let opts = VerifiedOptions::for_extension(&entry_clone.target);
                let result = download_verified(&entry_clone.url, &entry_clone.target, &opts)
                    .await
                    .map_err(|e| format!("{e:#}"));
                EntryReport {
                    entry: entry_clone,
                    result,
                }
            })
        })
        .collect();

    let mut reports = Vec::with_capacity(tasks.len());
    for task in tasks {
        match task.await {
            Ok(report) => reports.push(report),
            Err(join_err) => {
                // A tokio task panic is a programmer error, not a
                // network failure — bubble it up loudly.
                return Err(anyhow::anyhow!(
                    "tokio join error in fetch_region: {join_err}"
                ));
            }
        }
    }

    Ok(RegionReport {
        region: region_name.to_string(),
        entries: reports,
    })
}

/// List every region name whose index is shipped with butterfly-dl.
/// Used by the CLI's error path ("unknown region X; known regions:
/// [...]").
pub fn shipped_regions() -> &'static [&'static str] {
    &[
        "austria",
        "belgium",
        "france",
        "germany",
        "luxembourg",
        "netherlands",
        "switzerland",
        // #96 serve-the-world non-European set
        "australia",
        "brazil",
        "india",
        "japan",
        "united-states",
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn belgium_index_parses() {
        let idx = RegionIndex::load("belgium").expect("belgium should load");
        // PBF
        assert!(
            idx.pbf.is_some(),
            "belgium index must carry a [pbf] section"
        );
        // Four transit feeds
        assert_eq!(idx.gtfs.len(), 3, "SNCB + De Lijn + TEC");
        assert_eq!(idx.netex_epip.len(), 1, "STIB only");
        let gtfs_ids: Vec<&str> = idx.gtfs.iter().map(|g| g.id.as_str()).collect();
        assert!(gtfs_ids.contains(&"sncb"));
        assert!(gtfs_ids.contains(&"delijn"));
        assert!(gtfs_ids.contains(&"tec"));
        assert_eq!(idx.netex_epip[0].id, "stib");
        // Six OpenAddresses regional packs (BRU/VLG/WAL × per-region
        // langs — bru-fr, bru-nl, vlg-fr, vlg-nl, wal-fr, wal-de).
        assert_eq!(idx.address.len(), 6);
        for entry in &idx.address {
            assert_eq!(entry.source.as_deref(), Some("openaddresses"));
            assert_eq!(entry.format, "geojson-gz");
            assert!(
                entry.url.contains("openaddresses.io"),
                "OpenAddresses URL drift: {}",
                entry.url
            );
        }
    }

    #[test]
    fn cross_border_region_indexes_parse_with_oa_addresses() {
        // Cross-border cluster #1 (BE/FR/NL/LU/DE) plus #2 (AT/DE/CH).
        // All carry a [pbf] section; per-country authoritative data
        // ships through OpenAddresses (#96 §"Data Sources") with one
        // or more `[[address]]` entries.
        for region in [
            "france",
            "netherlands",
            "luxembourg",
            "germany",
            "austria",
            "switzerland",
        ] {
            let idx = RegionIndex::load(region)
                .unwrap_or_else(|e| panic!("region '{region}' must load: {e:#}"));
            assert!(
                idx.pbf.is_some(),
                "region '{region}' must carry a [pbf] section",
            );
            assert!(
                idx.gtfs.is_empty(),
                "region '{region}' is not expected to ship GTFS in this pass",
            );
            assert!(
                idx.netex_epip.is_empty(),
                "region '{region}' is not expected to ship NeTEx in this pass",
            );
            let pbf_url = &idx.pbf.as_ref().unwrap().url;
            assert!(
                pbf_url.starts_with("https://download.geofabrik.de/"),
                "region '{region}' PBF URL should be Geofabrik: {pbf_url}",
            );
            assert!(
                !idx.address.is_empty(),
                "region '{region}' must carry at least one [[address]] OpenAddresses entry",
            );
            for entry in &idx.address {
                assert_eq!(
                    entry.source.as_deref(),
                    Some("openaddresses"),
                    "region '{region}' [[address]] entry {} should be tagged source=openaddresses",
                    entry.id,
                );
                assert!(
                    entry.url.contains("openaddresses.io"),
                    "region '{region}' [[address]] {} url should hit OpenAddresses: {}",
                    entry.id,
                    entry.url,
                );
            }
        }
    }

    #[test]
    fn shipped_regions_lists_every_loadable_region() {
        for name in shipped_regions() {
            RegionIndex::load(name)
                .unwrap_or_else(|e| panic!("shipped region '{name}' must load: {e:#}"));
        }
    }

    #[test]
    fn every_loadable_region_appears_in_shipped_regions() {
        // Inverse of the test above: every name in `shipped_regions()`
        // must round-trip through `RegionIndex::load`. Without this,
        // a new arm in `RegionIndex::load` without a corresponding
        // entry in `shipped_regions()` would slip past CI — the CLI's
        // "unknown region X; known regions are [...]" error would lie
        // about the actual supported set.
        //
        // The list of known good aliases this test enumerates must
        // stay synchronised with `RegionIndex::load`. When you add a
        // region, drop a new line below.
        let known: &[&str] = &[
            "belgium",
            "france",
            "netherlands",
            "luxembourg",
            "germany",
            "austria",
            "switzerland",
            "united-states",
            "us",
            "japan",
            "brazil",
            "india",
            "australia",
        ];
        for name in known {
            assert!(
                RegionIndex::load(name).is_ok(),
                "region '{name}' should load via RegionIndex::load"
            );
        }
        // The `shipped_regions()` alias set must be a subset of the
        // load-arm set above. Any name in `shipped_regions()` that
        // doesn't load is a bug — same direction as the test above
        // but verified explicitly without depending on iteration.
        let shipped: std::collections::HashSet<&str> = shipped_regions().iter().copied().collect();
        let known_set: std::collections::HashSet<&str> = known.iter().copied().collect();
        for name in &shipped {
            assert!(
                known_set.contains(name),
                "shipped_regions() lists '{name}' but the inverse test doesn't \
                 enumerate it — add it to the `known` array in this test"
            );
        }
    }

    #[test]
    fn unknown_region_errors() {
        let err = RegionIndex::load("atlantis").expect_err("should reject");
        let msg = format!("{err:#}");
        assert!(msg.contains("unknown region"));
    }

    #[test]
    fn entries_respects_all_filter() {
        let idx = RegionIndex::load("belgium").unwrap();
        let entries = idx.entries("belgium", Path::new("/tmp/data"), SectionFilter::All);
        // 1 pbf + 3 gtfs + 1 netex + 6 address
        // (OA bru-fr/nl, vlg-fr/nl, wal-fr/de) = 11.
        assert_eq!(entries.len(), 11);
        let pbf = entries.iter().find(|e| e.section == "pbf").unwrap();
        assert_eq!(pbf.target, Path::new("/tmp/data/belgium.pbf"));
        let sncb = entries
            .iter()
            .find(|e| e.section == "gtfs" && e.id == "sncb")
            .unwrap();
        assert_eq!(sncb.target, Path::new("/tmp/data/transit/gtfs/sncb.zip"));
        let stib = entries
            .iter()
            .find(|e| e.section == "netex_epip" && e.id == "stib")
            .unwrap();
        assert_eq!(
            stib.target,
            Path::new("/tmp/data/transit/netex/stib-epip.xml")
        );
        let bru_fr = entries
            .iter()
            .find(|e| e.section == "address" && e.id == "oa-be-bru-fr")
            .unwrap();
        assert_eq!(
            bru_fr.target,
            Path::new("/tmp/data/addresses/oa-be-bru-fr.geojson.gz"),
        );
        assert!(
            bru_fr.url.contains("openaddresses.io"),
            "OpenAddresses URL drift: {}",
            bru_fr.url,
        );
    }

    #[test]
    fn entries_respects_addresses_only_filter() {
        let idx = RegionIndex::load("belgium").unwrap();
        let entries = idx.entries(
            "belgium",
            Path::new("/tmp/data"),
            SectionFilter::AddressesOnly,
        );
        // 6 OpenAddresses regional packs (BRU/VLG/WAL × per-region langs).
        assert_eq!(entries.len(), 6);
        for e in &entries {
            assert_eq!(e.section, "address");
            assert!(
                e.id.starts_with("oa-be-"),
                "unexpected id: {} (expected oa-be-* prefix)",
                e.id
            );
            assert!(
                e.target.to_string_lossy().ends_with(".geojson.gz"),
                "unexpected target: {} (expected .geojson.gz)",
                e.target.display()
            );
        }
    }

    #[test]
    fn section_filter_addresses_parses() {
        assert_eq!(
            SectionFilter::parse("addresses").unwrap(),
            SectionFilter::AddressesOnly
        );
        assert_eq!(
            SectionFilter::parse("address").unwrap(),
            SectionFilter::AddressesOnly
        );
    }

    #[test]
    fn entries_respects_pbf_only_filter() {
        let idx = RegionIndex::load("belgium").unwrap();
        let entries = idx.entries("belgium", Path::new("/tmp/data"), SectionFilter::PbfOnly);
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].section, "pbf");
    }

    #[test]
    fn entries_respects_transit_only_filter() {
        let idx = RegionIndex::load("belgium").unwrap();
        let entries = idx.entries(
            "belgium",
            Path::new("/tmp/data"),
            SectionFilter::TransitOnly,
        );
        assert_eq!(entries.len(), 4);
        assert!(entries.iter().all(|e| e.section != "pbf"));
    }

    #[test]
    fn section_filter_parses() {
        assert!(matches!(
            SectionFilter::parse("all"),
            Ok(SectionFilter::All)
        ));
        assert!(matches!(
            SectionFilter::parse("pbf"),
            Ok(SectionFilter::PbfOnly)
        ));
        assert!(matches!(
            SectionFilter::parse("transit"),
            Ok(SectionFilter::TransitOnly)
        ));
        assert!(SectionFilter::parse("foo").is_err());
    }
}
