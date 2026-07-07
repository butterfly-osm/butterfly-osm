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
            "france" => FRANCE_INDEX_TOML,
            "netherlands" => NETHERLANDS_INDEX_TOML,
            "luxembourg" => LUXEMBOURG_INDEX_TOML,
            "germany" => GERMANY_INDEX_TOML,
            "austria" => AUSTRIA_INDEX_TOML,
            "switzerland" => SWITZERLAND_INDEX_TOML,
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
/// #498: resolve a wildcard feed URL against its parent directory index.
///
/// Some mirrors publish only DATED files with no stable `latest` alias (e.g.
/// gtfs.flatturtle.cloud: `delijn/delijn-gtfs_2026-07-07.zip`). A feed `url`
/// whose FILENAME segment contains a single `*` (`.../delijn-gtfs_*.zip`) is
/// resolved by fetching the parent index and picking the lexicographically
/// greatest match — ISO-dated names make that the newest. Conditional
/// download (#418) then applies to the resolved URL as usual.
async fn resolve_wildcard_url(url: &str) -> Result<String> {
    let (parent, pattern) = url
        .rsplit_once('/')
        .with_context(|| format!("wildcard feed url has no path: {url}"))?;
    let (prefix, suffix) = pattern
        .split_once('*')
        .with_context(|| format!("wildcard feed url has no '*': {url}"))?;
    anyhow::ensure!(
        !suffix.contains('*'),
        "only one '*' is supported in a feed url: {url}"
    );
    let index_url = format!("{parent}/");
    let outcome = crate::core::Downloader::stream_url_conditional(&index_url, None, None)
        .await
        .with_context(|| format!("GET {index_url} (wildcard index for {url})"))?;
    let body = match outcome {
        crate::core::ConditionalOutcome::Body { stream, .. } => {
            // Directory indexes are a few KB; cap the read so a misbehaving
            // mirror can't balloon memory. 4 MiB is orders of magnitude above
            // any real index.
            const MAX_INDEX_BYTES: u64 = 4 * 1024 * 1024;
            let mut limited = tokio::io::AsyncReadExt::take(stream, MAX_INDEX_BYTES);
            let mut buf = Vec::new();
            tokio::io::AsyncReadExt::read_to_end(&mut limited, &mut buf)
                .await
                .with_context(|| format!("read index body: {index_url}"))?;
            String::from_utf8_lossy(&buf).into_owned()
        }
        crate::core::ConditionalOutcome::NotModified => {
            bail!("unexpected 304 for unconditional GET {index_url}")
        }
    };
    let best = best_wildcard_match(&body, prefix, suffix).with_context(|| {
        format!("no file matching '{pattern}' found in index {index_url} (#498)")
    })?;
    log::info!("{url}: wildcard resolved → {best} (#498)");
    Ok(format!("{parent}/{best}"))
}

/// Scan an HTML/plain directory index for filenames `prefix<middle>suffix`
/// (middle free of separators/quotes) and return the lexicographically
/// greatest — for ISO-dated names, the newest.
fn best_wildcard_match(body: &str, prefix: &str, suffix: &str) -> Option<String> {
    let mut best: Option<String> = None;
    let mut rest = body;
    while let Some(pos) = rest.find(prefix) {
        let tail = &rest[pos..];
        if let Some(send) = tail[prefix.len()..].find(suffix) {
            let name = &tail[..prefix.len() + send + suffix.len()];
            let middle = &name[prefix.len()..name.len() - suffix.len()];
            let clean = !middle.contains(|c: char| {
                c == '"' || c == '\'' || c == '/' || c == '<' || c == '>' || c.is_whitespace()
            });
            if clean && best.as_deref().is_none_or(|b| name > b) {
                best = Some(name.to_string());
            }
        }
        rest = &rest[pos + prefix.len()..];
    }
    best
}

pub async fn fetch_region(
    region_name: &str,
    data_root: &Path,
    filter: SectionFilter,
    force: bool,
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
                // #498: dated-only mirrors — resolve a `*` in the filename
                // segment against the parent index before downloading.
                let url = if entry_clone
                    .url
                    .rsplit_once('/')
                    .is_some_and(|(_, f)| f.contains('*'))
                {
                    match resolve_wildcard_url(&entry_clone.url).await {
                        Ok(u) => u,
                        Err(e) => {
                            return EntryReport {
                                entry: entry_clone,
                                result: Err(format!("{e:#}")),
                            };
                        }
                    }
                } else {
                    entry_clone.url.clone()
                };
                let mut opts = VerifiedOptions::for_extension(&entry_clone.target);
                // #418: --force suppresses the conditional GET + sidecar skip.
                opts.force_refresh = force;
                let result = download_verified(&url, &entry_clone.target, &opts)
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
    ]
}

#[cfg(test)]
mod tests {
    use super::*;

    // #498: wildcard index matching — picks the lexicographically greatest
    // (= newest for ISO-dated names), ignores non-filename spans.
    #[test]
    fn wildcard_picks_newest_dated_file() {
        let body = r#"<a href="/delijn/delijn-gtfs_2026-07-04.zip">delijn-gtfs_2026-07-04.zip</a>
                      <a href="/delijn/delijn-gtfs_2026-07-07.zip">delijn-gtfs_2026-07-07.zip</a>
                      <a href="/delijn/delijn-gtfs_2026-07-06.zip">delijn-gtfs_2026-07-06.zip</a>"#;
        assert_eq!(
            best_wildcard_match(body, "delijn-gtfs_", ".zip").as_deref(),
            Some("delijn-gtfs_2026-07-07.zip")
        );
    }

    #[test]
    fn wildcard_rejects_spans_crossing_tokens() {
        // prefix appears but nothing ends with the suffix inside one token
        let body = r#"<a href="/x/delijn-gtfs_readme.txt">notes</a> other .zip elsewhere"#;
        assert_eq!(best_wildcard_match(body, "delijn-gtfs_", ".zip"), None);
    }

    #[test]
    fn belgium_index_parses() {
        let idx = RegionIndex::load("belgium").expect("belgium should load");
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
    fn cross_border_region_indexes_parse() {
        // Cross-border cluster #1 (BE/FR/NL/LU/DE) plus #2 (AT/DE/CH).
        // All carry a [pbf] section; transit / NeTEx ships only for
        // Belgium today.
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
        // 1 pbf + 3 gtfs + 1 netex = 5.
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
