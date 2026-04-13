//! Transit configuration (`transit.toml`).
//!
//! Example:
//!
//! ```toml
//! refresh_static_secs = 86400
//! refresh_rt_secs = 60
//! max_walk_m = 1000
//! transfer_radius_m = 1000
//!
//! [[feeds]]
//! id = "sncb"
//! url = "https://gtfs.irail.be/nmbs/gtfs/latest.zip"
//!
//! [[feeds]]
//! id = "delijn"
//! url = "https://gtfs.irail.be/de-lijn/de_lijn-gtfs.zip"
//! rt_url = "https://gtfs.irail.be/de-lijn/de_lijn-gtfs-realtime.bin"
//! ```

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

/// One GTFS feed source (static + optional GTFS-RT trip updates).
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FeedConfig {
    /// Stable identifier (used as the local filename: `<id>.zip`).
    pub id: String,
    /// URL for the static GTFS zip.
    pub url: String,
    /// Optional URL for GTFS-RT trip updates (protobuf).
    #[serde(default)]
    pub rt_url: Option<String>,
}

/// Top-level transit configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransitConfig {
    /// Refresh interval for static GTFS feeds (seconds). Default: 86400 (24h).
    #[serde(default = "default_refresh_static")]
    pub refresh_static_secs: u64,
    /// Refresh interval for GTFS-RT trip updates (seconds). Default: 60.
    #[serde(default = "default_refresh_rt")]
    pub refresh_rt_secs: u64,
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

fn default_refresh_static() -> u64 {
    86_400
}
fn default_refresh_rt() -> u64 {
    60
}
fn default_max_walk() -> u32 {
    1_000
}
fn default_transfer_radius() -> u32 {
    1_000
}
fn default_max_access_stops() -> usize {
    20
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

    /// Local path for the last-seen GTFS-RT protobuf blob for a feed.
    pub fn feed_rt_path(&self, feed: &FeedConfig) -> PathBuf {
        self.transit_dir().join(format!("{}.rt.bin", feed.id))
    }
}

impl Default for TransitConfig {
    fn default() -> Self {
        Self {
            refresh_static_secs: default_refresh_static(),
            refresh_rt_secs: default_refresh_rt(),
            max_walk_m: default_max_walk(),
            transfer_radius_m: default_transfer_radius(),
            max_access_stops: default_max_access_stops(),
            feeds: Vec::new(),
            data_dir: PathBuf::new(),
        }
    }
}

/// Default SNCB feed used when no `transit.toml` is present but a `transit/`
/// directory exists. This mirrors the only feed we exercise in end-to-end
/// tests on the Belgium dataset.
pub fn default_sncb_feed() -> FeedConfig {
    FeedConfig {
        id: "sncb".to_string(),
        url: "https://gtfs.irail.be/nmbs/gtfs/latest.zip".to_string(),
        rt_url: None,
    }
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
        // No config — provide a sensible default (SNCB only) so the
        // operator only has to `mkdir transit` to enable transit.
        let mut cfg = TransitConfig {
            feeds: vec![default_sncb_feed()],
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
        cfg.feeds.push(default_sncb_feed());
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
refresh_static_secs = 3600
refresh_rt_secs = 30
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
        assert_eq!(cfg.refresh_static_secs, 3600);
        assert_eq!(cfg.refresh_rt_secs, 30);
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
    fn default_with_sncb_when_toml_missing() {
        let dir = tempdir().unwrap();
        std::fs::create_dir_all(dir.path().join("transit")).unwrap();
        let cfg = load(dir.path()).unwrap().unwrap();
        assert_eq!(cfg.feeds.len(), 1);
        assert_eq!(cfg.feeds[0].id, "sncb");
    }
}
