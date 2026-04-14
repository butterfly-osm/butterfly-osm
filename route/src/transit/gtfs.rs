//! GTFS static feed loader.
//!
//! Uses [`gtfs_structures`] to parse each zip, then compiles the result
//! into a [`Timetable`] keyed on canonical stop patterns. Multi-feed
//! loading is supported — stops, trips and routes from every feed are
//! merged into a single RAPTOR timetable with namespaced IDs so two
//! operators using the same raw GTFS id (e.g. `"1"`) never collide.
//!
//! ## Service filtering
//!
//! RAPTOR is a *single-day* algorithm. We filter trips to a specific
//! `service_date` (defaulting to "today" in `Europe/Brussels`) using the
//! GTFS `calendar.txt` / `calendar_dates.txt` rules provided by the
//! `gtfs-structures` crate.
//!
//! ## ID namespacing
//!
//! When multiple feeds are loaded simultaneously, every GTFS id that ends
//! up in the [`Timetable`] is prefixed with `<feed_id>:` to guarantee
//! uniqueness across operators. A single-feed load keeps the legacy
//! behaviour (no prefix) so existing tests and callers are unaffected.

use std::collections::HashSet;
use std::path::Path;

use anyhow::{Context, Result};
use chrono::{Datelike, NaiveDate, Weekday};
use gtfs_structures::{DirectionType, Gtfs, Stop as GtfsStop};

use super::timetable::{StopIdx, StopTime, Timetable, TimetableBuilder};

/// Source describing which trips should populate the timetable.
#[derive(Debug, Clone, Copy)]
pub struct ServiceFilter {
    pub date: NaiveDate,
}

impl ServiceFilter {
    pub fn new(date: NaiveDate) -> Self {
        Self { date }
    }
}

/// A single GTFS feed to load: a zip path and an optional namespace prefix.
#[derive(Debug, Clone)]
pub struct FeedSource {
    /// Path to the static GTFS zip.
    pub path: std::path::PathBuf,
    /// Feed identifier used to namespace stop / trip / route ids when
    /// multiple feeds are merged. `None` preserves raw GTFS ids and is
    /// only legitimate for single-feed loads.
    pub feed_id: Option<String>,
}

impl FeedSource {
    pub fn single(path: impl Into<std::path::PathBuf>) -> Self {
        Self {
            path: path.into(),
            feed_id: None,
        }
    }

    pub fn namespaced(path: impl Into<std::path::PathBuf>, feed_id: impl Into<String>) -> Self {
        Self {
            path: path.into(),
            feed_id: Some(feed_id.into()),
        }
    }
}

/// Load a single GTFS zip (no namespacing). Equivalent to
/// [`load_many`] with one unnamed source.
pub fn load_zip(path: &Path, filter: ServiceFilter) -> Result<Timetable> {
    load_many(&[FeedSource::single(path.to_path_buf())], filter)
}

/// Load one or more GTFS feeds and merge them into a single [`Timetable`].
///
/// When more than one feed is provided, every `feed.feed_id` must be
/// `Some(_)` (a bare single-feed load is only allowed when there is
/// exactly one source). Stops / trips / routes from different feeds are
/// merged into one namespace using `<feed_id>:<raw_id>` so collisions are
/// impossible and per-stop provenance is recoverable from the id.
pub fn load_many(sources: &[FeedSource], filter: ServiceFilter) -> Result<Timetable> {
    let mut builder = TimetableBuilder::new();
    load_into_builder(sources, filter, &mut builder)?;
    builder.build()
}

/// Append one or more GTFS feeds into an existing [`TimetableBuilder`].
///
/// Like [`load_many`] but does not finalise the builder — used by
/// `transit::load_from_disk` to merge GTFS feeds and NeTEx-EPIP feeds
/// (#101) into the same timetable, since the two formats write into
/// the same builder and we call `builder.build()` once at the end.
pub fn load_into_builder(
    sources: &[FeedSource],
    filter: ServiceFilter,
    builder: &mut TimetableBuilder,
) -> Result<()> {
    if sources.is_empty() {
        // Empty is fine — the caller may have NeTEx-EPIP feeds but
        // zero GTFS feeds. The builder still yields a valid Timetable
        // if the other loaders wrote stops into it.
        return Ok(());
    }
    if sources.len() > 1 {
        for s in sources {
            if s.feed_id.is_none() {
                anyhow::bail!(
                    "multi-feed load requires every source to carry a feed_id prefix ({} missing)",
                    s.path.display()
                );
            }
        }
    }

    let mut total_trips_kept = 0usize;
    let mut total_trips_seen = 0usize;
    let mut total_stops_seen = 0usize;

    for source in sources {
        tracing::info!(
            path = %source.path.display(),
            feed = source.feed_id.as_deref().unwrap_or("<single>"),
            "parsing GTFS zip"
        );
        let gtfs = Gtfs::new(source.path.to_str().context("non-utf8 path")?)
            .map_err(|e| anyhow::anyhow!("parsing GTFS {}: {}", source.path.display(), e))?;
        tracing::info!(
            stops = gtfs.stops.len(),
            trips = gtfs.trips.len(),
            routes = gtfs.routes.len(),
            "GTFS feed parsed"
        );
        total_stops_seen += gtfs.stops.len();
        total_trips_seen += gtfs.trips.len();

        let prefix = source.feed_id.as_deref();
        let active_services = active_service_ids(&gtfs, filter.date);
        tracing::info!(
            n_active = active_services.len(),
            feed = prefix.unwrap_or("<single>"),
            "active GTFS services for {}",
            filter.date
        );

        let kept = compile_into(&gtfs, filter, prefix, &active_services, builder)?;
        total_trips_kept += kept;
    }

    tracing::info!(
        feeds = sources.len(),
        total_stops = total_stops_seen,
        total_trips_kept,
        total_trips_seen,
        "GTFS multi-feed load complete"
    );
    Ok(())
}

/// Compile an in-memory [`Gtfs`] struct into a [`Timetable`] (single-feed
/// convenience — used by tests).
pub fn compile(gtfs: &Gtfs, filter: ServiceFilter) -> Result<Timetable> {
    let active_services = active_service_ids(gtfs, filter.date);
    let mut builder = TimetableBuilder::new();
    compile_into(gtfs, filter, None, &active_services, &mut builder)?;
    builder.build()
}

/// Append one parsed GTFS feed into an existing [`TimetableBuilder`].
///
/// This is the core merge routine shared by `load_many` and `compile`.
/// Namespacing is controlled by `prefix` — `Some("sncb")` turns GTFS id
/// `"8814001"` into `"sncb:8814001"`, while `None` passes ids through.
fn compile_into(
    gtfs: &Gtfs,
    _filter: ServiceFilter,
    prefix: Option<&str>,
    active_services: &std::collections::HashSet<String>,
    builder: &mut TimetableBuilder,
) -> Result<usize> {
    let prefix_fn = |raw: &str| -> String {
        match prefix {
            Some(p) => format!("{p}:{raw}"),
            None => raw.to_string(),
        }
    };

    // First pass: register every stop so StopIdx is stable for parent
    // stations and for stops that never appear in an active trip.
    //
    // **Determinism**: `gtfs.stops` is a HashMap whose iteration order is
    // randomised per process. The transfer-graph cache keys edges by
    // StopIdx (Vec position), so a non-deterministic stop ordering would
    // silently corrupt the cached graph on reload. We sort stops by
    // GTFS id before registering them, locking in a stable ordering.
    let mut sorted_stops: Vec<&GtfsStop> = gtfs.stops.values().map(|arc| arc.as_ref()).collect();
    sorted_stops.sort_by(|a, b| a.id.cmp(&b.id));

    for stop in &sorted_stops {
        let (lon, lat) = stop_coords(stop);
        let namespaced = prefix_fn(&stop.id);
        builder.add_stop(
            &namespaced,
            stop.name.as_deref().unwrap_or(""),
            lon,
            lat,
            None,
        );
    }
    // Second pass: wire up parent_station pointers (using namespaced ids).
    for stop in &sorted_stops {
        if let Some(parent_id) = &stop.parent_station {
            let child_ns = prefix_fn(&stop.id);
            let parent_ns = prefix_fn(parent_id);
            if let (Some(&child_idx), Some(&parent_idx)) = (
                builder.stop_id_to_idx.get(&child_ns),
                builder.stop_id_to_idx.get(&parent_ns),
            ) {
                builder.set_parent_station(child_idx, parent_idx);
            }
        }
    }

    // Walk trips, filter by service, feed into the builder.
    // Sort by trip id for the same determinism reason as stops above.
    let mut n_trips_kept = 0usize;
    let mut sorted_trips: Vec<&gtfs_structures::Trip> = gtfs.trips.values().collect();
    sorted_trips.sort_by(|a, b| a.id.cmp(&b.id));
    for trip in sorted_trips {
        if !active_services.contains(&trip.service_id) {
            continue;
        }
        if trip.stop_times.len() < 2 {
            continue;
        }

        let mut pattern: Vec<StopIdx> = Vec::with_capacity(trip.stop_times.len());
        let mut times: Vec<StopTime> = Vec::with_capacity(trip.stop_times.len());
        let mut skip = false;
        for st in &trip.stop_times {
            let ns_stop_id = prefix_fn(&st.stop.id);
            let Some(&idx) = builder.stop_id_to_idx.get(&ns_stop_id) else {
                skip = true;
                break;
            };
            pattern.push(idx);
            let arrival = st
                .arrival_time
                .unwrap_or_else(|| st.departure_time.unwrap_or(0));
            let departure = st.departure_time.unwrap_or(arrival);
            times.push(StopTime { arrival, departure });
        }
        if skip {
            continue;
        }

        let route = gtfs.routes.get(&trip.route_id);
        let short_name = route.and_then(|r| r.short_name.clone()).unwrap_or_default();
        let long_name = route.and_then(|r| r.long_name.clone()).unwrap_or_default();
        let headsign = trip
            .trip_headsign
            .clone()
            .unwrap_or_else(|| match trip.direction_id {
                Some(DirectionType::Outbound) => "outbound".to_string(),
                Some(DirectionType::Inbound) => "inbound".to_string(),
                None => String::new(),
            });

        let ns_trip_id = prefix_fn(&trip.id);
        builder.add_trip(
            &ns_trip_id,
            &short_name,
            &long_name,
            &headsign,
            pattern,
            times,
        );
        n_trips_kept += 1;
    }

    tracing::info!(
        kept = n_trips_kept,
        total = gtfs.trips.len(),
        feed = prefix.unwrap_or("<single>"),
        "GTFS trips filtered to service date"
    );
    Ok(n_trips_kept)
}

/// Resolve the set of service_ids active on the given date using
/// `calendar.txt` + `calendar_dates.txt`.
fn active_service_ids(gtfs: &Gtfs, date: NaiveDate) -> HashSet<String> {
    let mut active: HashSet<String> = HashSet::new();

    for (service_id, calendar) in &gtfs.calendar {
        if date < calendar.start_date || date > calendar.end_date {
            continue;
        }
        let valid = match date.weekday() {
            Weekday::Mon => calendar.monday,
            Weekday::Tue => calendar.tuesday,
            Weekday::Wed => calendar.wednesday,
            Weekday::Thu => calendar.thursday,
            Weekday::Fri => calendar.friday,
            Weekday::Sat => calendar.saturday,
            Weekday::Sun => calendar.sunday,
        };
        if valid {
            active.insert(service_id.clone());
        }
    }

    // Apply calendar_dates.txt exceptions: Added or Deleted on specific dates.
    for (service_id, exceptions) in &gtfs.calendar_dates {
        for ex in exceptions {
            if ex.date != date {
                continue;
            }
            match ex.exception_type {
                gtfs_structures::Exception::Added => {
                    active.insert(service_id.clone());
                }
                gtfs_structures::Exception::Deleted => {
                    active.remove(service_id);
                }
            }
        }
    }

    active
}

fn stop_coords(stop: &GtfsStop) -> (f64, f64) {
    let lon = stop.longitude.unwrap_or(0.0);
    let lat = stop.latitude.unwrap_or(0.0);
    (lon, lat)
}

/// Find the haversine distance (meters) between a query point and a stop.
/// Used by the `/transit` handler to fan out to nearby access/egress stops.
pub fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let dphi = (lat2 - lat1).to_radians();
    let dlambda = (lon2 - lon1).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlambda / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().asin()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_sanity() {
        // Brussels-Midi ≈ (4.3571, 50.8358), Ghent-Sint-Pieters ≈ (3.7107, 51.0353)
        let d = haversine_m(4.3571, 50.8358, 3.7107, 51.0353);
        // Straight-line should be roughly 50-55 km.
        assert!((48_000.0..=58_000.0).contains(&d), "got {d}");
    }
}
