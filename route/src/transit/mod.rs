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
//! ## Sub-modules
//!
//! * [`config`]   — `transit.toml` parser and defaults.
//! * [`gtfs`]     — GTFS (static) zip loader using `gtfs-structures`.
//! * [`timetable`]— RAPTOR-shaped arrays (routes / trips / stops / stop_times).
//! * [`raptor`]   — Round-based RAPTOR earliest-arrival search.
//! * [`transfers`]— ULTRA-style stop-to-stop foot transfer precompute.
//! * [`transfers_cache`] — Binary cache format for the transfer graph.
//! * [`realtime`] — GTFS-RT protobuf ingestion and in-place trip patching.
//! * [`feeds`]    — Background feed manager: download, hash, rebuild, swap.

pub mod config;
pub mod feeds;
pub mod gtfs;
pub mod raptor;
pub mod realtime;
pub mod timetable;
pub mod transfers;
pub mod transfers_cache;

use std::sync::Arc;

use arc_swap::ArcSwap;

pub use config::{FeedConfig, TransitConfig};
pub use raptor::{RaptorJourney, RaptorLeg};
pub use timetable::{RouteIdx, Stop, StopIdx, Timetable, TripIdx};
pub use transfers::TransferGraph;

/// A transit-enabled snapshot: timetable + transfer graph.
///
/// Held in an `ArcSwap` so the feed manager can hot-swap it when the static
/// feed or GTFS-RT updates change the schedule.
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

/// Top-level transit state stored on `ServerState`.
pub struct TransitState {
    pub config: TransitConfig,
    pub snapshot: ArcSwap<TransitSnapshot>,
}

impl TransitState {
    pub fn new(config: TransitConfig, snapshot: TransitSnapshot) -> Self {
        Self {
            config,
            snapshot: ArcSwap::from(Arc::new(snapshot)),
        }
    }

    /// Obtain a consistent snapshot for a single query.
    pub fn snapshot(&self) -> Arc<TransitSnapshot> {
        self.snapshot.load_full()
    }

    /// Hot-swap to a new snapshot (called by the feed manager).
    pub fn swap(&self, snapshot: TransitSnapshot) {
        self.snapshot.store(Arc::new(snapshot));
    }
}
