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
//! Example:
//!
//! ```toml
//! max_walk_m        = 2000
//! transfer_radius_m = 2000
//! max_access_stops  = 20
//!
//! [[feeds]]
//! id  = "sncb"
//! url = "https://gtfs.irail.be/nmbs/gtfs/latest.zip"
//!
//! [[feeds]]
//! id  = "delijn"
//! url = "https://gtfs.irail.be/de-lijn/de_lijn-gtfs.zip"
//!
//! [[feeds]]
//! id  = "tec"
//! url = "https://opendata.tec-wl.be/Current%20GTFS/TEC-GTFS.zip"
//!
//! [[feeds]]
//! id  = "stib"
//! url = "https://gtfs.irail.be/mivb/mivb-gtfs.zip"
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// One GTFS feed source. Refreshed at rebuild time by `transit-fetch`,
/// not by the running server. Optional `rt_url` captures a one-shot
/// GTFS-RT trip-update snapshot for the rebuild; the server applies it
/// once at startup and never polls.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedConfig {
    /// Stable identifier (used as the local filename: `<id>.zip`).
    pub id: String,
    /// URL for the static GTFS zip.
    pub url: String,
    /// Optional URL for a GTFS-RT trip-updates snapshot (protobuf).
    #[serde(default)]
    pub rt_url: Option<String>,
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

    /// Local path for a particular feed's static zip.
    pub fn feed_zip_path(&self, feed: &FeedConfig) -> PathBuf {
        self.gtfs_dir().join(format!("{}.zip", feed.id))
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

/// Default Belgium feed set used when no `transit.toml` is present.
///
/// This currently lists the three GTFS-publishing operators that cover
/// the vast majority of scheduled public transport in Belgium:
///
/// * **SNCB** — national rail (~14 MB GTFS, ~2,750 stops, ~3,900 active trips/day)
/// * **De Lijn** — Flanders bus + tram (~190 MB GTFS, ~30,500 stops)
/// * **TEC** — Wallonia bus (~85 MB GTFS, ~31,200 stops)
///
/// **STIB (Brussels metro/bus/tram) is intentionally not in the default
/// set.** STIB has deprecated GTFS and migrated to NeTEx (the EU-mandated
/// format under Delegated Regulation 2017/1926). Their public GTFS
/// endpoints all return either 404s or domain-squat HTML masquerading
/// as `application/zip`. Loading STIB requires the EPIP NeTEx loader
/// tracked in butterfly-osm/butterfly-osm#101. Once that lands, an
/// operator can opt in by adding STIB to their `transit.toml` with
/// `format = "netex-epip"`.
pub fn default_belgium_feeds() -> Vec<FeedConfig> {
    vec![
        FeedConfig {
            id: "sncb".to_string(),
            url: "https://gtfs.irail.be/nmbs/gtfs/latest.zip".to_string(),
            rt_url: None,
        },
        FeedConfig {
            id: "delijn".to_string(),
            url: "https://gtfs.irail.be/de-lijn/de_lijn-gtfs.zip".to_string(),
            rt_url: None,
        },
        FeedConfig {
            id: "tec".to_string(),
            url: "https://opendata.tec-wl.be/Current%20GTFS/TEC-GTFS.zip".to_string(),
            rt_url: None,
        },
    ]
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

    #[test]
    fn default_belgium_feed_set_when_toml_missing() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("transit")).unwrap();
        let cfg = load(dir.path()).unwrap().unwrap();
        let ids: Vec<&str> = cfg.feeds.iter().map(|f| f.id.as_str()).collect();
        // STIB is intentionally excluded — see default_belgium_feeds()
        // doc comment + butterfly-osm/butterfly-osm#101.
        assert_eq!(ids, vec!["sncb", "delijn", "tec"]);
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
