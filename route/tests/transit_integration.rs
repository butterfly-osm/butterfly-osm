//! End-to-end transit integration test.
//!
//! Loads the SNCB GTFS feed (already on disk under
//! `data/belgium/transit/gtfs/sncb.zip` — downloaded out-of-band to
//! respect the sandbox) and runs a pure-RAPTOR query against it.
//!
//! This test does NOT exercise the foot-CCH transfer precompute, which
//! takes tens of seconds on Belgium — see the Docker smoke test for that
//! end-to-end path. It does exercise the GTFS compile, RAPTOR round loop,
//! and RAPTOR path reconstruction on real-world scale (~600 stops,
//! thousands of trips).

use std::collections::HashMap;
use std::path::PathBuf;

use butterfly_route::transit::gtfs::{ServiceFilter, load_zip};
use butterfly_route::transit::raptor::{RaptorLeg, RaptorQuery, run_raptor};
use butterfly_route::transit::transfers::TransferGraph;
use chrono::{Datelike, Duration as ChronoDuration, Local, NaiveDate, Weekday};

fn belgium_data_root() -> PathBuf {
    // Integration tests run from the package directory, so `../data/...`
    // is the workspace-relative path.
    PathBuf::from("../data/belgium")
}

fn gtfs_zip_path() -> Option<PathBuf> {
    let p = belgium_data_root().join("transit/gtfs/sncb.zip");
    if p.exists() { Some(p) } else { None }
}

/// Find a weekday on or after `date` — SNCB weekday and weekend patterns
/// differ significantly; sticking to weekdays gives a more predictable
/// set of IC services.
fn next_weekday(mut date: NaiveDate) -> NaiveDate {
    while matches!(date.weekday(), Weekday::Sat | Weekday::Sun) {
        date += ChronoDuration::days(1);
    }
    date
}

#[test]
#[ignore = "requires SNCB GTFS zip under data/belgium/transit/gtfs/sncb.zip"]
fn sncb_raptor_brussels_to_ghent() {
    let Some(zip) = gtfs_zip_path() else {
        panic!("SNCB GTFS zip missing: download from https://gtfs.irail.be/nmbs/gtfs/latest.zip");
    };

    // Use a weekday so normal IC services are running.
    let date = next_weekday(Local::now().date_naive());
    eprintln!("loading SNCB GTFS for service date {date}");
    let tt = load_zip(&zip, ServiceFilter::new(date)).expect("GTFS load");
    eprintln!(
        "loaded {} stops, {} routes, {} trips",
        tt.n_stops(),
        tt.n_routes(),
        tt.n_total_trips
    );
    assert!(
        tt.n_stops() >= 400,
        "expected >= 400 SNCB stops, got {}",
        tt.n_stops()
    );
    assert!(
        tt.n_total_trips >= 500,
        "expected >= 500 trips on a weekday, got {}",
        tt.n_total_trips
    );

    // Find Brussels-Midi ("Bruxelles-Midi" / "Brussel-Zuid") and
    // Ghent-Sint-Pieters ("Gent-Sint-Pieters") by name substring.
    let (bxlm, _) = tt
        .stops
        .iter()
        .enumerate()
        .find(|(_, s)| {
            let n = s.name.to_lowercase();
            n.contains("bruxelles-midi")
                || n.contains("brussel-zuid")
                || n.contains("brussels-south")
        })
        .unwrap_or_else(|| panic!("Brussels-Midi not found in SNCB stops"));
    let (gsp, _) = tt
        .stops
        .iter()
        .enumerate()
        .find(|(_, s)| {
            let n = s.name.to_lowercase();
            n.contains("gent-sint-pieters") || n.contains("gand-saint-pierre")
        })
        .unwrap_or_else(|| panic!("Gent-Sint-Pieters not found in SNCB stops"));

    eprintln!(
        "origin stop {} ({}), destination stop {} ({})",
        bxlm, tt.stops[bxlm].name, gsp, tt.stops[gsp].name
    );
    eprintln!(
        "  origin parent_station={:?}, routes={}",
        tt.stops[bxlm].parent_station,
        tt.routes_for_stop(bxlm as u32).len()
    );
    eprintln!(
        "  destination parent_station={:?}, routes={}",
        tt.stops[gsp].parent_station,
        tt.routes_for_stop(gsp as u32).len()
    );

    // Expand source/destination to include all station children so that
    // we model "boarding at any platform of the station" correctly.
    // This matches real traveller semantics and how the /transit handler
    // fans out via walking proximity.
    let expand = |s: u32| -> Vec<u32> {
        // If the stop is a parent station, add all children.
        if let Some(children) = tt.station_children.get(&s) {
            return children.clone();
        }
        // Otherwise add the stop + its siblings under the same parent.
        if let Some(parent) = tt.stops[s as usize].parent_station {
            if let Some(children) = tt.station_children.get(&parent) {
                return children.clone();
            }
        }
        vec![s]
    };
    let src_stops = expand(bxlm as u32);
    let dst_stops = expand(gsp as u32);
    eprintln!("expanded src={:?} dst={:?}", src_stops, dst_stops);

    // Run RAPTOR with a single source and single target at 08:00:00.
    // No transfer graph is needed for a direct IC journey.
    let transfers = TransferGraph::empty(tt.n_stops());
    let depart_s: u32 = 8 * 3600;
    let sources: Vec<(u32, u32)> = src_stops.iter().map(|s| (*s, depart_s)).collect();
    let mut targets = HashMap::new();
    for s in &dst_stops {
        targets.insert(*s, 0u32);
    }
    let q = RaptorQuery {
        sources: &sources,
        target_weights: &targets,
    };

    let journey = run_raptor(&tt, &transfers, &q).expect("RAPTOR journey must exist");
    let duration = journey.arrival_time - depart_s;
    eprintln!(
        "journey: depart {}, arrive {}, duration {}s, legs {}",
        depart_s,
        journey.arrival_time,
        duration,
        journey.legs.len()
    );
    for leg in &journey.legs {
        match leg {
            RaptorLeg::Ride {
                route,
                from_stop,
                to_stop,
                board_time,
                alight_time,
                ..
            } => {
                let meta = &tt.route_meta[*route as usize];
                eprintln!(
                    "  ride route={:?} [{}→{}] {} → {} ({}..{})",
                    meta.short_name,
                    tt.stops[*from_stop as usize].name,
                    tt.stops[*to_stop as usize].name,
                    board_time,
                    alight_time,
                    board_time,
                    alight_time
                );
            }
            RaptorLeg::Walk {
                from_stop,
                to_stop,
                duration_s,
            } => {
                eprintln!(
                    "  walk {} → {} ({}s)",
                    tt.stops[*from_stop as usize].name,
                    tt.stops[*to_stop as usize].name,
                    duration_s
                );
            }
        }
    }

    // Brussels→Ghent direct IC takes 30–60 minutes; allow a generous
    // window to account for service variance.
    assert!(
        (20 * 60..=90 * 60).contains(&(duration as i32)),
        "journey duration {duration}s is outside the 20–90 min window"
    );
    // At least one transit leg.
    assert!(
        journey
            .legs
            .iter()
            .any(|l| matches!(l, RaptorLeg::Ride { .. })),
        "journey must contain at least one ride leg"
    );
}
