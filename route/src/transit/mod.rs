//! Transit (public transport) routing module.
//!
//! Implements multimodal routing combining:
//!   * walking legs via Butterfly's foot-mode CCH, and
//!   * transit legs via a RAPTOR search over a GTFS timetable.
//!
//! The transit sub-system is strictly optional: if no `transit/` directory
//! (or no `transit.toml`) is present under the data directory, the server
//! starts normally without transit support. When present, transit is loaded
//! alongside road modes and exposed via `GET /transit`.
//!
//! ## Refresh model
//!
//! Transit feeds are refreshed *at rebuild time* via the `transit-fetch`
//! CLI command, then the server loads whatever is on disk at startup.
//! There is no background polling; the operator refreshes feeds the same
//! way they refresh the PBF — re-run the scraper and restart the server.
//! See [`feeds::fetch_all`] for the scraper entry point.
//!
//! ## Sub-modules
//!
//! * [`config`]          — `transit.toml` parser and defaults.
//! * [`feeds`]           — Feed scraper used by the `transit-fetch` CLI.
//! * [`gtfs`]            — GTFS (static) zip loader using `gtfs-structures`.
//! * [`timetable`]       — RAPTOR-shaped arrays (routes / trips / stops / stop_times).
//! * [`raptor`]          — Round-based RAPTOR earliest-arrival search.
//! * [`transfers`]       — ULTRA-style stop-to-stop foot transfer precompute with
//!   triangle dominance restriction.
//! * [`transfers_cache`] — Binary cache format for the transfer graph.
//! * [`realtime`]        — GTFS-RT protobuf ingestion, applied once at startup.

pub mod config;
pub mod feeds;
pub mod gtfs;
pub mod netex_epip;
pub mod raptor;
pub mod realtime;
pub mod stop_index;
pub mod timetable;
pub mod transfers;
pub mod transfers_cache;

use std::sync::Arc;

pub use config::{FeedConfig, TransitConfig};
pub use raptor::{RaptorJourney, RaptorLeg};
pub use stop_index::StopSpatialIndex;
pub use timetable::{RouteIdx, Stop, StopIdx, Timetable, TripIdx};
pub use transfers::TransferGraph;

/// A transit-enabled snapshot: timetable + transfer graph + stop
/// spatial index. Immutable for the lifetime of the process — to
/// refresh, the operator runs `transit-fetch` and restarts the server.
#[derive(Clone)]
pub struct TransitSnapshot {
    pub timetable: Arc<Timetable>,
    pub transfers: Arc<TransferGraph>,
    /// R-tree over transit stops for O(log N) `k_nearest` candidate
    /// selection. Built once per snapshot, shared read-only across
    /// every query. See issue #102.
    pub stop_index: Arc<StopSpatialIndex>,
}

impl TransitSnapshot {
    pub fn new(timetable: Timetable, transfers: TransferGraph) -> Self {
        let stop_index = StopSpatialIndex::build(&timetable);
        Self {
            timetable: Arc::new(timetable),
            transfers: Arc::new(transfers),
            stop_index: Arc::new(stop_index),
        }
    }
}

/// Top-level transit state stored on `ServerState`. Holds the immutable
/// snapshot plus the effective `TransitConfig` (for query defaults).
pub struct TransitState {
    pub config: TransitConfig,
    pub snapshot: TransitSnapshot,
}

impl TransitState {
    pub fn new(config: TransitConfig, snapshot: TransitSnapshot) -> Self {
        Self { config, snapshot }
    }

    /// Obtain the current snapshot for a query.
    pub fn snapshot(&self) -> &TransitSnapshot {
        &self.snapshot
    }
}

/// Load a transit snapshot from whatever GTFS zips are present on disk.
///
/// Called at server startup. The behaviour is:
///
/// 1. Collect every feed in `config.feeds` that has a corresponding
///    `<data>/transit/gtfs/<id>.zip` on disk. Missing feeds are logged
///    and skipped — this is normal if the operator hasn't yet run
///    `butterfly-route transit-fetch` for a particular operator.
/// 2. If **zero** feeds are present, return an error (caller logs it
///    and continues in road-only mode).
/// 3. Otherwise load every present feed into a single [`Timetable`]
///    with feed-namespaced ids (when more than one feed is loaded) or
///    raw ids (single-feed).
/// 4. Build (or reuse the cache for) the transfer graph using the
///    effective `transfer_radius_m` and the ULTRA dominance restriction.
/// 5. If any feed has a one-shot GTFS-RT snapshot at
///    `<data>/transit/rt/<id>.pb`, decode and apply it once. This is the
///    extent of GTFS-RT integration — there is no polling loop.
pub fn load_from_disk(
    config: &TransitConfig,
    foot: &crate::server::state::ModeData,
    spatial: &crate::server::spatial::SpatialIndex,
) -> anyhow::Result<TransitSnapshot> {
    use chrono::Local;

    use crate::transit::config::FeedFormat;
    use crate::transit::gtfs::{FeedSource, ServiceFilter};
    use crate::transit::timetable::TimetableBuilder;
    use crate::transit::transfers::{TransferBuildOptions, load_or_build};

    // Gather on-disk feeds, split by format. GTFS feeds collect into
    // a `Vec<FeedSource>` for the existing `gtfs::load_into_builder`
    // path; NeTEx-EPIP feeds (#101, STIB) go through
    // `netex_epip::load_into_builder`. Both sides write into the same
    // `TimetableBuilder`, which is finalised once at the end so the
    // resulting `Timetable` is a single merged multi-feed view.
    let mut gtfs_sources: Vec<FeedSource> = Vec::new();
    let mut epip_paths: Vec<(std::path::PathBuf, String)> = Vec::new();
    let mut present_feeds: Vec<&FeedConfig> = Vec::new();
    for feed in &config.feeds {
        let path = config.feed_zip_path(feed);
        if !path.exists() {
            tracing::warn!(
                feed = feed.id.as_str(),
                format = ?feed.format,
                path = %path.display(),
                "transit feed not on disk — skipping (run `butterfly-route transit-fetch` or place manually)"
            );
            continue;
        }
        present_feeds.push(feed);
        match feed.format {
            FeedFormat::Gtfs => {
                gtfs_sources.push(FeedSource::namespaced(path, feed.id.clone()));
            }
            FeedFormat::NetexEpip => {
                epip_paths.push((path, feed.id.clone()));
            }
        }
    }
    if gtfs_sources.is_empty() && epip_paths.is_empty() {
        anyhow::bail!(
            "no transit feeds present on disk under {}",
            config.transit_dir().display()
        );
    }

    // When exactly one GTFS feed is present AND no NeTEx-EPIP feeds
    // are loaded, drop the namespace prefix so existing single-feed
    // code paths and tests keep seeing raw GTFS ids. With any mix of
    // feeds, namespacing is required.
    let multi_feed = gtfs_sources.len() + epip_paths.len() > 1;
    if gtfs_sources.len() == 1 && epip_paths.is_empty() && !multi_feed {
        gtfs_sources[0].feed_id = None;
    }

    let service_date = Local::now().date_naive();
    let filter = ServiceFilter::new(service_date);

    let mut builder = TimetableBuilder::new();
    if !gtfs_sources.is_empty() {
        crate::transit::gtfs::load_into_builder(&gtfs_sources, filter, &mut builder)?;
    }
    for (path, feed_id) in &epip_paths {
        let prefix = if multi_feed {
            Some(feed_id.as_str())
        } else {
            None
        };
        crate::transit::netex_epip::load_into_builder(path, prefix, &mut builder)?;
    }
    let timetable = builder
        .build()
        .map_err(|e| anyhow::anyhow!("building merged Timetable (GTFS + NeTEx-EPIP): {e}"))?;

    let opts = TransferBuildOptions {
        radius_m: config.transfer_radius_m,
        ..TransferBuildOptions::default()
    };
    let cache_path = config.transfers_cache_path();
    let transfers = load_or_build(&timetable, foot, spatial, &opts, &cache_path)?;

    // Apply GTFS-RT one-shot snapshots, if any. We never fail the load
    // on a bad RT blob — the static feed is still usable, and the
    // operator will see a warning in the logs.
    let mut patched = timetable;
    for feed in &present_feeds {
        let rt_path = config.feed_rt_path(feed);
        if !rt_path.exists() {
            continue;
        }
        match std::fs::read(&rt_path) {
            Ok(bytes) => match crate::transit::realtime::decode(&bytes) {
                Ok(feed_msg) => {
                    let (next, stats) =
                        crate::transit::realtime::apply_trip_updates(&patched, &feed_msg);
                    tracing::info!(
                        feed = feed.id.as_str(),
                        entities = stats.entities_seen,
                        matched = stats.trips_matched,
                        unknown = stats.trips_unknown,
                        patched = stats.stop_times_patched,
                        "applied GTFS-RT snapshot at startup"
                    );
                    patched = next;
                }
                Err(e) => {
                    tracing::warn!(
                        feed = feed.id.as_str(),
                        error = %e,
                        "failed to decode GTFS-RT snapshot — keeping static timetable"
                    );
                }
            },
            Err(e) => {
                tracing::warn!(
                    feed = feed.id.as_str(),
                    path = %rt_path.display(),
                    error = %e,
                    "failed to read GTFS-RT snapshot file"
                );
            }
        }
    }

    Ok(TransitSnapshot::new(patched, transfers))
}
