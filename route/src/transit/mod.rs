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
pub mod raptor;
pub mod realtime;
pub mod timetable;
pub mod transfers;
pub mod transfers_cache;

use std::sync::Arc;

pub use config::{FeedConfig, TransitConfig};
pub use raptor::{RaptorJourney, RaptorLeg};
pub use timetable::{RouteIdx, Stop, StopIdx, Timetable, TripIdx};
pub use transfers::TransferGraph;

/// A transit-enabled snapshot: timetable + transfer graph. Immutable for
/// the lifetime of the process — to refresh, the operator runs
/// `transit-fetch` and restarts the server.
#[derive(Clone)]
pub struct TransitSnapshot {
    pub timetable: Arc<Timetable>,
    pub transfers: Arc<TransferGraph>,
}

impl TransitSnapshot {
    pub fn new(timetable: Timetable, transfers: TransferGraph) -> Self {
        Self {
            timetable: Arc::new(timetable),
            transfers: Arc::new(transfers),
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

    use crate::transit::gtfs::{FeedSource, ServiceFilter};
    use crate::transit::transfers::{TransferBuildOptions, load_or_build};

    // Gather on-disk feeds.
    let mut sources: Vec<FeedSource> = Vec::new();
    let mut present_feeds: Vec<&FeedConfig> = Vec::new();
    for feed in &config.feeds {
        let path = config.feed_zip_path(feed);
        if path.exists() {
            sources.push(FeedSource::namespaced(path, feed.id.clone()));
            present_feeds.push(feed);
        } else {
            tracing::warn!(
                feed = feed.id.as_str(),
                path = %path.display(),
                "GTFS feed not on disk — skipping (run `butterfly-route transit-fetch`)"
            );
        }
    }
    if sources.is_empty() {
        anyhow::bail!(
            "no GTFS feeds present on disk under {}",
            config.gtfs_dir().display()
        );
    }

    // When only a single feed is present, drop the namespace prefix so
    // existing single-feed code paths and tests keep seeing raw GTFS ids.
    if sources.len() == 1 {
        sources[0].feed_id = None;
    }

    let service_date = Local::now().date_naive();
    let filter = ServiceFilter::new(service_date);
    let timetable = crate::transit::gtfs::load_many(&sources, filter)?;

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
