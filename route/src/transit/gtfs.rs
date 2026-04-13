//! GTFS static feed loader.
//!
//! Uses [`gtfs_structures`] to parse the zip, then compiles the result
//! into a [`Timetable`] keyed on canonical stop patterns.
//!
//! ## Service filtering
//!
//! RAPTOR is a *single-day* algorithm. We filter trips to a specific
//! `service_date` (defaulting to "today" in `Europe/Brussels`) using the
//! GTFS `calendar.txt` / `calendar_dates.txt` rules provided by the
//! `gtfs-structures` crate.

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

/// Load a GTFS zip from disk and compile it into a [`Timetable`]
/// containing only trips that run on `filter.date`.
pub fn load_zip(path: &Path, filter: ServiceFilter) -> Result<Timetable> {
    tracing::info!(path = %path.display(), "parsing GTFS zip");
    let gtfs = Gtfs::new(path.to_str().context("non-utf8 path")?)
        .map_err(|e| anyhow::anyhow!("parsing GTFS {}: {}", path.display(), e))?;
    tracing::info!(
        stops = gtfs.stops.len(),
        trips = gtfs.trips.len(),
        routes = gtfs.routes.len(),
        "GTFS parsed"
    );

    compile(&gtfs, filter)
}

/// Compile an in-memory [`Gtfs`] struct into a [`Timetable`].
pub fn compile(gtfs: &Gtfs, filter: ServiceFilter) -> Result<Timetable> {
    // Determine the set of service_ids active on the target date.
    let active_services = active_service_ids(gtfs, filter.date);
    tracing::info!(
        n_active = active_services.len(),
        "active GTFS services for {}",
        filter.date
    );

    let mut builder = TimetableBuilder::new();

    // Register stops up-front so StopIdx is stable even for stops that
    // never appear in any active trip (parent stations in particular).
    // First pass: add all stops without parent links so they get indices.
    for stop in gtfs.stops.values() {
        let (lon, lat) = stop_coords(stop);
        builder.add_stop(&stop.id, stop.name.as_deref().unwrap_or(""), lon, lat, None);
    }
    // Second pass: wire up parent_station pointers.
    for stop in gtfs.stops.values() {
        if let Some(parent_id) = &stop.parent_station {
            if let (Some(&child_idx), Some(&parent_idx)) = (
                builder.stop_id_to_idx.get(&stop.id),
                builder.stop_id_to_idx.get(parent_id),
            ) {
                builder.set_parent_station(child_idx, parent_idx);
            }
        }
    }

    // Walk trips, filter by service, and feed them into the builder.
    let mut n_trips_kept = 0usize;
    for trip in gtfs.trips.values() {
        if !active_services.contains(&trip.service_id) {
            continue;
        }
        if trip.stop_times.len() < 2 {
            continue;
        }

        // Pattern is the ordered list of StopIdx along this trip.
        let mut pattern: Vec<StopIdx> = Vec::with_capacity(trip.stop_times.len());
        let mut times: Vec<StopTime> = Vec::with_capacity(trip.stop_times.len());
        let mut skip = false;
        for st in &trip.stop_times {
            let stop_id = &st.stop.id;
            let Some(&idx) = builder.stop_id_to_idx.get(stop_id) else {
                skip = true;
                break;
            };
            pattern.push(idx);
            // Use arrival/departure seconds; fall back to the other field
            // if one side is missing (common for first/last stops).
            let arrival = st
                .arrival_time
                .unwrap_or_else(|| st.departure_time.unwrap_or(0));
            let departure = st.departure_time.unwrap_or(arrival);
            times.push(StopTime { arrival, departure });
        }
        if skip {
            continue;
        }

        // Lookup route metadata.
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

        // Direction differentiates opposite-direction patterns.
        // We already encode direction implicitly via the pattern order, so
        // no separate key is needed — trips going A→B→C and C→B→A map to
        // different patterns naturally.
        builder.add_trip(&trip.id, &short_name, &long_name, &headsign, pattern, times);
        n_trips_kept += 1;
    }

    tracing::info!(
        kept = n_trips_kept,
        total = gtfs.trips.len(),
        "GTFS trips filtered to service date"
    );
    builder.build()
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
