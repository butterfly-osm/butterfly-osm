//! Transit configuration (`transit.toml`).
//!
//! Operational model: transit feeds are refreshed *at rebuild time*, not
//! continuously by the running server — exactly like the OSM PBF. The
//! `butterfly-route transit-fetch` CLI command downloads every configured
//! feed into `<data>/transit/gtfs/<id>.zip`. The server then loads whatever
//! is on disk at startup. When the operator wants a fresh schedule, they
//! re-run `transit-fetch` (usually alongside the PBF refresh cron) and
//! restart the server. No background pollers. No hot-swapping.
//!
//! With no `transit.toml` the feed list comes from the region index
//! shipped with the downloader (`dl/regions/<region>.toml`) — the same
//! list `butterfly-dl <region>` fetches, so a feed URL is written down
//! in exactly one place. A `transit.toml` overrides it:
//!
//! ```toml
//! max_walk_m        = 2000
//! transfer_radius_m = 2000
//! max_access_stops  = 20
//!
//! [[feeds]]
//! id     = "sncb"
//! url    = "https://example.org/sncb-gtfs.zip"
//! format = "gtfs"          # or "netex-epip"
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// One GTFS feed source. Refreshed at rebuild time by `transit-fetch`,
/// not by the running server. Optional `rt_url` captures a one-shot
/// GTFS-RT trip-update snapshot for the rebuild; the server applies it
/// once at startup and never polls.
/// On-disk file format for a transit feed. Operators who publish
/// plain GTFS zips use `Gtfs` (the default); operators who have
/// migrated to NeTEx-EPIP (notably STIB) use `NetexEpip`. The
/// loader dispatches on this field.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "kebab-case")]
pub enum FeedFormat {
    #[default]
    Gtfs,
    NetexEpip,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedConfig {
    /// Stable identifier (used as the local filename prefix, e.g.
    /// `<id>.zip` for GTFS or `<id>.xml` for NeTEx-EPIP).
    pub id: String,
    /// URL for the static feed (zip for GTFS, XML for NeTEx-EPIP).
    pub url: String,
    /// Optional URL for a GTFS-RT trip-updates snapshot (protobuf).
    #[serde(default)]
    pub rt_url: Option<String>,
    /// Feed format (`gtfs` or `netex-epip`). Defaults to `gtfs` for
    /// backward compatibility with pre-#101 configs.
    #[serde(default)]
    pub format: FeedFormat,
}

/// Top-level transit configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitConfig {
    /// Maximum walking distance from origin/destination to any stop (meters).
    #[serde(default = "default_max_walk")]
    pub max_walk_m: u32,
    /// Radius for precomputed stop-to-stop walking transfers (meters).
    #[serde(default = "default_transfer_radius")]
    pub transfer_radius_m: u32,
    /// Number of nearest stops to fan out to at origin/destination. Default: 20.
    #[serde(default = "default_max_access_stops")]
    pub max_access_stops: usize,
    /// Static feed specifications.
    #[serde(default)]
    pub feeds: Vec<FeedConfig>,
    /// Data root (set after parsing, not serialised).
    #[serde(skip)]
    pub data_dir: PathBuf,
}

fn default_max_walk() -> u32 {
    2_000
}
fn default_transfer_radius() -> u32 {
    2_000
}
fn default_max_access_stops() -> usize {
    // Sentinel: 0 means "use the per-mode default from the handler"
    // (foot=20, bike=60, car=500). Operators who set a concrete value
    // in `transit.toml` override it for every mode; operators who leave
    // it out get mode-aware defaults automatically. See issue #110.
    0
}

impl TransitConfig {
    /// Directory that holds the transit state (`<data>/transit`).
    pub fn transit_dir(&self) -> PathBuf {
        self.data_dir.join("transit")
    }

    /// Directory that stores downloaded GTFS zips (`<data>/transit/gtfs`).
    pub fn gtfs_dir(&self) -> PathBuf {
        self.transit_dir().join("gtfs")
    }

    /// Cache file for the precomputed transfer graph.
    pub fn transfers_cache_path(&self) -> PathBuf {
        self.transit_dir().join("transfers.bin")
    }

    /// Local path for a particular feed's static archive. GTFS feeds
    /// land under `transit/gtfs/<id>.zip`; NeTEx-EPIP feeds under
    /// `transit/netex/<id>-epip.xml`.
    pub fn feed_zip_path(&self, feed: &FeedConfig) -> PathBuf {
        match feed.format {
            FeedFormat::Gtfs => self.gtfs_dir().join(format!("{}.zip", feed.id)),
            FeedFormat::NetexEpip => self
                .transit_dir()
                .join("netex")
                .join(format!("{}-epip.xml", feed.id)),
        }
    }

    /// Local path for the one-shot GTFS-RT snapshot blob for a feed.
    pub fn feed_rt_path(&self, feed: &FeedConfig) -> PathBuf {
        self.transit_dir()
            .join("rt")
            .join(format!("{}.pb", feed.id))
    }
}

impl Default for TransitConfig {
    fn default() -> Self {
        Self {
            max_walk_m: default_max_walk(),
            transfer_radius_m: default_transfer_radius(),
            max_access_stops: default_max_access_stops(),
            feeds: Vec::new(),
            data_dir: PathBuf::new(),
        }
    }
}

/// Default feed set for a region, taken from the region index shipped
/// with the downloader — the SAME list `butterfly-dl <region>` fetches.
///
/// It used to be a second hand-written copy of that list, and the two
/// drifted: a De Lijn URL fix landed in the index and never reached
/// here, so a `transit-fetch` run kept asking for a path that had been
/// gone for months. There is now one list; a URL fix lands once, and
/// `defaults_match_the_shipped_region_index` fails if a copy reappears.
///
/// Feeds map to formats by which section of the index they came from:
/// `[[gtfs]]` entries are [`FeedFormat::Gtfs`], `[[netex_epip]]`
/// entries are [`FeedFormat::NetexEpip`]. Coverage for Belgium:
///
/// * **SNCB** — national rail (GTFS)
/// * **De Lijn** — Flanders bus + tram (GTFS)
/// * **TEC** — Wallonia bus (GTFS)
/// * **STIB** — Brussels metro/bus/tram, NeTEx-EPIP: STIB deprecated
///   GTFS under EU Delegated Regulation 2017/1926, and the loader
///   (`crate::transit::netex_epip`) parses the EPIP publication into
///   the same [`Timetable`] shape as the GTFS loader, reprojecting
///   Lambert-93 to WGS84 on the way in.
///
/// Returns an error for an unknown region, naming the ones that ship.
pub fn default_feeds(region: &str) -> Result<Vec<FeedConfig>> {
    let index = butterfly_dl::regions::RegionIndex::load(region)
        .with_context(|| format!("no shipped feed list for region '{region}'"))?;
    let gtfs = index.gtfs.iter().map(|e| (&e.id, &e.url, FeedFormat::Gtfs));
    let epip = index
        .netex_epip
        .iter()
        .map(|e| (&e.id, &e.url, FeedFormat::NetexEpip));
    Ok(gtfs
        .chain(epip)
        .map(|(id, url, format)| FeedConfig {
            id: id.clone(),
            url: url.clone(),
            rt_url: None,
            format,
        })
        .collect())
}

/// The region whose feed list is used when `transit/` exists but
/// `transit.toml` does not.
pub const DEFAULT_REGION: &str = "belgium";

/// [`default_feeds`] for [`DEFAULT_REGION`]. The shipped index is
/// embedded at compile time and is parsed by a test, so a failure here
/// would mean the binary was built from a broken index.
pub fn default_belgium_feeds() -> Vec<FeedConfig> {
    default_feeds(DEFAULT_REGION).expect("the shipped belgium region index must parse")
}

/// Load `transit.toml` from the data directory, if present.
///
/// Returns `Ok(None)` if `transit/` does not exist at all. Returns
/// `Ok(Some(default_with_sncb))` if `transit/` exists but no TOML file does.
/// Returns `Ok(Some(parsed))` if the TOML file parsed successfully.
pub fn load(data_dir: &Path) -> Result<Option<TransitConfig>> {
    let transit_dir = data_dir.join("transit");
    if !transit_dir.is_dir() {
        return Ok(None);
    }

    let toml_path = transit_dir.join("transit.toml");
    if !toml_path.is_file() {
        // No config — provide the default Belgium feed set so the
        // operator only has to `mkdir transit && butterfly-route
        // transit-fetch` to enable transit.
        let mut cfg = TransitConfig {
            feeds: default_belgium_feeds(),
            ..TransitConfig::default()
        };
        cfg.data_dir = data_dir.to_path_buf();
        return Ok(Some(cfg));
    }

    let text = std::fs::read_to_string(&toml_path)
        .with_context(|| format!("reading {}", toml_path.display()))?;
    let mut cfg: TransitConfig =
        toml::from_str(&text).with_context(|| format!("parsing {}", toml_path.display()))?;
    cfg.data_dir = data_dir.to_path_buf();
    if cfg.feeds.is_empty() {
        cfg.feeds = default_belgium_feeds();
    }
    Ok(Some(cfg))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn parses_full_config() {
        let dir = tempdir().unwrap();
        let td = dir.path().join("transit");
        std::fs::create_dir_all(&td).unwrap();
        std::fs::write(
            td.join("transit.toml"),
            r#"
max_walk_m = 800
transfer_radius_m = 900
max_access_stops = 12

[[feeds]]
id = "sncb"
url = "https://example.com/sncb.zip"
rt_url = "https://example.com/sncb.rt"
"#,
        )
        .unwrap();

        let cfg = load(dir.path()).unwrap().unwrap();
        assert_eq!(cfg.max_walk_m, 800);
        assert_eq!(cfg.transfer_radius_m, 900);
        assert_eq!(cfg.max_access_stops, 12);
        assert_eq!(cfg.feeds.len(), 1);
        assert_eq!(cfg.feeds[0].id, "sncb");
        assert_eq!(
            cfg.feeds[0].rt_url.as_deref(),
            Some("https://example.com/sncb.rt")
        );
    }

    #[test]
    fn returns_none_when_transit_dir_absent() {
        let dir = tempdir().unwrap();
        assert!(load(dir.path()).unwrap().is_none());
    }

    /// #537: the feed list existed twice — once here, once in the region
    /// index the downloader fetches — and a URL fix landed in one and not
    /// the other, so `transit-fetch` kept requesting a path that had been
    /// dead for months. There is one list now; this fails the moment a
    /// second copy appears.
    #[test]
    fn defaults_match_the_shipped_region_index() {
        let index = butterfly_dl::regions::RegionIndex::load(DEFAULT_REGION)
            .expect("the shipped region index must parse");
        let feeds = default_belgium_feeds();

        let mut expected: Vec<(&str, &str, FeedFormat)> = index
            .gtfs
            .iter()
            .map(|e| (e.id.as_str(), e.url.as_str(), FeedFormat::Gtfs))
            .collect();
        expected.extend(
            index
                .netex_epip
                .iter()
                .map(|e| (e.id.as_str(), e.url.as_str(), FeedFormat::NetexEpip)),
        );
        let got: Vec<(&str, &str, FeedFormat)> = feeds
            .iter()
            .map(|f| (f.id.as_str(), f.url.as_str(), f.format))
            .collect();
        assert_eq!(got, expected, "the defaults are no longer the shipped list");

        // A feed URL that the fetcher cannot resolve is the failure mode
        // this ticket is about: the region index supports a `*` wildcard
        // resolved against a directory listing, the transit fetcher does
        // not, and that mismatch is how the two lists came apart.
        for f in &feeds {
            assert!(
                !f.url.contains('*'),
                "feed {} has a wildcard URL the transit fetcher cannot resolve: {}",
                f.id,
                f.url
            );
            assert!(
                f.url.starts_with("https://"),
                "feed {} must be fetched over TLS: {}",
                f.id,
                f.url
            );
        }
    }

    /// An unknown region names itself rather than silently producing an
    /// empty feed list (which would boot a server with no transit and no
    /// explanation).
    #[test]
    fn an_unknown_region_has_no_silent_empty_feed_set() {
        let err = default_feeds("atlantis").expect_err("unknown region must be an error");
        assert!(
            format!("{err:#}").contains("atlantis"),
            "the error must name the region: {err:#}"
        );
    }

    #[test]
    fn default_belgium_feed_set_when_toml_missing() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("transit")).unwrap();
        let cfg = load(dir.path()).unwrap().unwrap();
        let ids: Vec<&str> = cfg.feeds.iter().map(|f| f.id.as_str()).collect();
        // STIB is now included via its NeTEx-EPIP feed (#101 loader
        // landed; `format = "netex-epip"`). The three GTFS operators
        // keep their default shape.
        assert_eq!(ids, vec!["sncb", "delijn", "tec", "stib"]);
        // STIB must carry the NetexEpip format discriminator; the
        // others must stay on Gtfs.
        let stib = cfg
            .feeds
            .iter()
            .find(|f| f.id == "stib")
            .expect("stib feed present");
        assert_eq!(stib.format, FeedFormat::NetexEpip);
        let sncb = cfg
            .feeds
            .iter()
            .find(|f| f.id == "sncb")
            .expect("sncb feed present");
        assert_eq!(sncb.format, FeedFormat::Gtfs);
    }

    #[test]
    fn default_max_access_stops_is_sentinel_zero() {
        // Issue #110: the default must be 0 so the handler picks the
        // per-mode default (foot=20 / bike=60 / car=500). A concrete
        // default here would silently shadow the per-mode values.
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("transit")).unwrap();
        let cfg = load(dir.path()).unwrap().unwrap();
        assert_eq!(cfg.max_access_stops, 0);
    }
}
