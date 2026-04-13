//! Feed manager: download, hash, rebuild, hot-swap.
//!
//! On server start we load whatever is on disk, then spawn a background
//! Tokio task that:
//!   1. polls each static feed URL at `refresh_static_secs`;
//!   2. polls each RT feed URL at `refresh_rt_secs`;
//!   3. on static-feed change (sha256 differs from last seen) rebuilds
//!      the timetable + transfer graph and hot-swaps the snapshot;
//!   4. on RT-feed change patches the current static timetable and
//!      hot-swaps a cheap snapshot (new `Timetable` clone).
//!
//! Network failures never crash the server: they're logged and retried.

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Local;
use sha2::{Digest, Sha256};
use tokio::time::{MissedTickBehavior, interval};

use crate::server::spatial::SpatialIndex;
use crate::server::state::{ModeData, ServerState};

use super::config::TransitConfig;
use super::gtfs::{self, ServiceFilter};
use super::realtime;
use super::transfers::{self, TransferBuildOptions};
use super::{TransitSnapshot, TransitState};

/// Synchronously download a URL to a target file, verifying SHA-256.
///
/// Returns the new SHA-256 if the content changed (or if the file was
/// absent), or `None` if the content matches the `previous_sha`.
///
/// Network errors are wrapped with context and returned — callers are
/// expected to treat them as transient.
pub async fn download_if_changed(
    client: &reqwest::Client,
    url: &str,
    target: &Path,
    previous_sha: Option<[u8; 32]>,
) -> Result<Option<[u8; 32]>> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("GET {url} returned {}", resp.status());
    }
    let body = resp
        .bytes()
        .await
        .with_context(|| format!("reading body of {url}"))?;

    let mut hasher = Sha256::new();
    hasher.update(&body);
    let mut sha = [0u8; 32];
    sha.copy_from_slice(hasher.finalize().as_slice());

    if let Some(prev) = previous_sha {
        if prev == sha {
            return Ok(None);
        }
    }

    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    // Atomic-ish: write to a .tmp, then rename.
    let tmp = target.with_extension("tmp");
    tokio::fs::write(&tmp, &body)
        .await
        .with_context(|| format!("writing {}", tmp.display()))?;
    tokio::fs::rename(&tmp, target)
        .await
        .with_context(|| format!("renaming {} -> {}", tmp.display(), target.display()))?;

    Ok(Some(sha))
}

/// Hash an existing local file, if present, to seed the `previous_sha` cache.
pub fn hash_file_if_exists(path: &Path) -> Option<[u8; 32]> {
    let bytes = std::fs::read(path).ok()?;
    let mut h = Sha256::new();
    h.update(&bytes);
    Some(h.finalize().into())
}

/// Ensure the static feed(s) are present on disk (downloading if needed).
///
/// Called synchronously at server start so that transit queries can answer
/// the very first request correctly. Network errors here are degraded to
/// warnings: if nothing is on disk yet, the transit subsystem refuses to
/// load and the server carries on with road-only queries.
pub async fn ensure_static_feeds(config: &TransitConfig) -> Result<()> {
    let client = build_http_client()?;
    for feed in &config.feeds {
        let target = config.feed_zip_path(feed);
        if target.exists() {
            tracing::info!(
                feed = feed.id.as_str(),
                path = %target.display(),
                "static GTFS feed already cached"
            );
            continue;
        }
        tracing::info!(
            feed = feed.id.as_str(),
            url = feed.url.as_str(),
            "downloading static GTFS feed"
        );
        match download_if_changed(&client, &feed.url, &target, None).await {
            Ok(_) => {
                tracing::info!(
                    feed = feed.id.as_str(),
                    path = %target.display(),
                    "static GTFS feed downloaded"
                );
            }
            Err(e) => {
                tracing::warn!(
                    feed = feed.id.as_str(),
                    url = feed.url.as_str(),
                    error = %e,
                    "download failed — transit will use on-disk snapshot if available"
                );
            }
        }
    }
    Ok(())
}

fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("butterfly-route/2.0 transit-feed-manager")
        .timeout(Duration::from_secs(120))
        .build()
        .context("building reqwest client for transit feeds")
}

/// Build the initial [`TransitSnapshot`] from whatever static feeds are on disk.
///
/// Returns an error only if *no* feed could be loaded at all.
pub fn load_initial_snapshot(
    config: &TransitConfig,
    foot: &ModeData,
    spatial: &SpatialIndex,
) -> Result<TransitSnapshot> {
    let service_date = Local::now().date_naive();
    let filter = ServiceFilter::new(service_date);

    // We load the first feed that exists on disk. Multi-feed merge is
    // deferred; a typical deployment has only one feed (SNCB for Belgium).
    let mut last_error: Option<anyhow::Error> = None;
    for feed in &config.feeds {
        let path = config.feed_zip_path(feed);
        if !path.exists() {
            continue;
        }
        tracing::info!(feed = feed.id.as_str(), path = %path.display(), "loading GTFS zip");
        let tt = match gtfs::load_zip(&path, filter) {
            Ok(tt) => tt,
            Err(e) => {
                tracing::warn!(
                    feed = feed.id.as_str(),
                    error = %e,
                    "failed to load GTFS zip"
                );
                last_error = Some(e);
                continue;
            }
        };
        let opts = TransferBuildOptions {
            radius_m: config.transfer_radius_m,
            ..TransferBuildOptions::default()
        };
        let cache_path = config.transfers_cache_path();
        let transfers = transfers::load_or_build(&tt, foot, spatial, &opts, &cache_path)?;
        return Ok(TransitSnapshot::new(tt, transfers));
    }
    Err(last_error.unwrap_or_else(|| anyhow::anyhow!("no static GTFS feeds on disk")))
}

/// Spawn the background feed manager task.
///
/// The manager takes an `Arc<ServerState>` so it can access the foot mode
/// and spatial index and hot-swap transit snapshots. It runs until the
/// server shuts down.
pub fn spawn_manager(state: Arc<ServerState>) {
    tokio::spawn(async move {
        if let Err(e) = run_manager(state).await {
            tracing::error!(error = %e, "transit feed manager exited with error");
        }
    });
}

async fn run_manager(state: Arc<ServerState>) -> Result<()> {
    let client = build_http_client()?;

    let transit = state
        .transit
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("feed manager started without transit state"))?;

    // Per-feed cached hashes (static + RT).
    let mut static_sha: Vec<Option<[u8; 32]>> = transit
        .config
        .feeds
        .iter()
        .map(|f| hash_file_if_exists(&transit.config.feed_zip_path(f)))
        .collect();
    let mut rt_sha: Vec<Option<[u8; 32]>> = vec![None; transit.config.feeds.len()];

    let mut static_tick = interval(Duration::from_secs(transit.config.refresh_static_secs));
    let mut rt_tick = interval(Duration::from_secs(transit.config.refresh_rt_secs));
    static_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    rt_tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
    // Skip the initial immediate tick so we don't re-download on startup.
    static_tick.tick().await;
    rt_tick.tick().await;

    loop {
        tokio::select! {
            _ = static_tick.tick() => {
                for (idx, feed) in transit.config.feeds.iter().enumerate() {
                    let path = transit.config.feed_zip_path(feed);
                    match download_if_changed(&client, &feed.url, &path, static_sha[idx]).await {
                        Ok(Some(new_sha)) => {
                            static_sha[idx] = Some(new_sha);
                            tracing::info!(
                                feed = feed.id.as_str(),
                                "static feed changed — rebuilding timetable"
                            );
                            rebuild_static_snapshot(&state);
                        }
                        Ok(None) => {
                            tracing::debug!(feed = feed.id.as_str(), "static feed unchanged");
                        }
                        Err(e) => {
                            tracing::warn!(
                                feed = feed.id.as_str(),
                                url = feed.url.as_str(),
                                error = %e,
                                "static feed download failed — will retry"
                            );
                        }
                    }
                }
            }
            _ = rt_tick.tick() => {
                for (idx, feed) in transit.config.feeds.iter().enumerate() {
                    let Some(rt_url) = feed.rt_url.clone() else { continue };
                    let target = transit.config.feed_rt_path(feed);
                    match download_if_changed(&client, &rt_url, &target, rt_sha[idx]).await {
                        Ok(Some(new_sha)) => {
                            rt_sha[idx] = Some(new_sha);
                            apply_rt_to_snapshot(transit, &target).await;
                        }
                        Ok(None) => {}
                        Err(e) => {
                            tracing::warn!(
                                feed = feed.id.as_str(),
                                url = rt_url.as_str(),
                                error = %e,
                                "GTFS-RT download failed — will retry"
                            );
                        }
                    }
                }
            }
        }
    }
}

fn rebuild_static_snapshot(state: &Arc<ServerState>) {
    let Some(transit) = state.transit.as_ref() else {
        return;
    };
    let Some(&foot_idx) = state.mode_lookup.get("foot") else {
        tracing::error!("foot mode missing — cannot rebuild transit snapshot");
        return;
    };
    let foot = &state.modes[foot_idx as usize];
    let spatial = &state.spatial_index;
    match build_snapshot(&transit.config, foot, spatial) {
        Ok(snap) => {
            tracing::info!(
                stops = snap.timetable.n_stops(),
                routes = snap.timetable.n_routes(),
                "hot-swapping new transit snapshot"
            );
            transit.swap(snap);
        }
        Err(e) => {
            tracing::error!(error = %e, "failed to rebuild transit snapshot");
        }
    }
}

fn build_snapshot(
    config: &TransitConfig,
    foot: &ModeData,
    spatial: &SpatialIndex,
) -> Result<TransitSnapshot> {
    let service_date = Local::now().date_naive();
    let filter = ServiceFilter::new(service_date);

    // Reuse load_initial_snapshot logic: try feeds in order.
    for feed in &config.feeds {
        let path = config.feed_zip_path(feed);
        if !path.exists() {
            continue;
        }
        let tt = gtfs::load_zip(&path, filter)?;
        let opts = TransferBuildOptions {
            radius_m: config.transfer_radius_m,
            ..TransferBuildOptions::default()
        };
        let cache_path = config.transfers_cache_path();
        let transfers = transfers::load_or_build(&tt, foot, spatial, &opts, &cache_path)?;
        return Ok(TransitSnapshot::new(tt, transfers));
    }
    anyhow::bail!("no GTFS feeds available to rebuild snapshot");
}

async fn apply_rt_to_snapshot(transit: &TransitState, path: &Path) {
    let bytes = match tokio::fs::read(path).await {
        Ok(b) => b,
        Err(e) => {
            tracing::warn!(path = %path.display(), error = %e, "cannot read RT file");
            return;
        }
    };
    let feed = match realtime::decode(&bytes) {
        Ok(f) => f,
        Err(e) => {
            tracing::warn!(error = %e, "cannot decode GTFS-RT");
            return;
        }
    };
    let current = transit.snapshot();
    let (patched, stats) = realtime::apply_trip_updates(&current.timetable, &feed);
    tracing::info!(
        entities = stats.entities_seen,
        matched = stats.trips_matched,
        unknown = stats.trips_unknown,
        patched = stats.stop_times_patched,
        "applied GTFS-RT update"
    );
    let new_snap = TransitSnapshot {
        timetable: Arc::new(patched),
        transfers: Arc::clone(&current.transfers),
    };
    transit.swap(new_snap);
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn hash_file_works() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("blob.bin");
        std::fs::write(&p, b"hello world").unwrap();
        let h = hash_file_if_exists(&p).unwrap();
        // SHA-256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
        assert_eq!(
            hex::encode(h),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[tokio::test]
    async fn download_mocked_via_wiremock() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"payload-v1".to_vec()))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.zip");
        let client = build_http_client().unwrap();
        let sha = download_if_changed(&client, &server.uri(), &target, None)
            .await
            .unwrap()
            .expect("first download should return Some");
        assert_eq!(std::fs::read(&target).unwrap(), b"payload-v1".to_vec());

        // Second call with same hash → Ok(None).
        let second = download_if_changed(&client, &server.uri(), &target, Some(sha))
            .await
            .unwrap();
        assert!(second.is_none());
    }
}
