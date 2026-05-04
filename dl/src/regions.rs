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

/// The shipped Belgium index, embedded at compile time. Adding a new
/// region = new file + new constant + new arm in `load_region`.
const BELGIUM_INDEX_TOML: &str = include_str!("../regions/belgium.toml");

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

impl RegionIndex {
    /// Load the shipped index for a region name. Returns an error if
    /// the region is not recognised.
    pub fn load(name: &str) -> Result<Self> {
        let raw: &str = match name {
            "belgium" => BELGIUM_INDEX_TOML,
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
/// names or the aggregate `"transit"` / `"all"`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SectionFilter {
    All,
    PbfOnly,
    TransitOnly,
}

impl SectionFilter {
    /// Parse a CLI `--only` argument. Named `parse` instead of
    /// `from_str` so we're not shadowing `std::str::FromStr`.
    pub fn parse(s: &str) -> Result<Self> {
        match s {
            "all" => Ok(Self::All),
            "pbf" => Ok(Self::PbfOnly),
            "transit" => Ok(Self::TransitOnly),
            other => bail!("unknown --only value '{other}'. Accepted values: all, pbf, transit"),
        }
    }

    fn keeps_pbf(self) -> bool {
        matches!(self, Self::All | Self::PbfOnly)
    }

    fn keeps_transit(self) -> bool {
        matches!(self, Self::All | Self::TransitOnly)
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
    &["belgium"]
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
        // 1 pbf + 3 gtfs + 1 netex = 5
        assert_eq!(entries.len(), 5);
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
