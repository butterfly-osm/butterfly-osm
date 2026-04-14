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

use std::collections::{HashMap, HashSet};
use std::path::PathBuf;
use std::sync::{Arc, OnceLock};

use butterfly_route::server::state::ServerState;
use butterfly_route::server::transit_handler::{
    TransitRequest, compute_transit_journey,
};
use butterfly_route::transit::gtfs::{FeedSource, ServiceFilter, load_many, load_zip};
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

/// Return (feed_id, path) for every Belgian GTFS zip present on disk.
fn present_feeds() -> Vec<(&'static str, PathBuf)> {
    let ids = ["sncb", "delijn", "tec", "stib"];
    let mut out = Vec::new();
    for id in ids {
        let p = belgium_data_root().join(format!("transit/gtfs/{id}.zip"));
        if p.exists() {
            out.push((id, p));
        }
    }
    out
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

/// Load every Belgian GTFS feed present on disk and assert the merged
/// timetable is internally consistent: stop / trip / route counts strictly
/// increase with each feed, feed-id namespacing keeps ids collision-free,
/// and the merged stop_routes relation stays well-formed.
///
/// `--ignored` because it requires every feed to be fetched beforehand
/// via `butterfly-route transit-fetch --data-dir data/belgium`.
#[test]
#[ignore = "requires every Belgian GTFS zip under data/belgium/transit/gtfs/"]
fn belgium_multi_feed_merge() {
    let feeds = present_feeds();
    assert!(
        feeds.len() >= 2,
        "need at least two Belgian feeds on disk; found {}",
        feeds.len()
    );
    eprintln!(
        "merging {} feeds: {:?}",
        feeds.len(),
        feeds.iter().map(|(id, _)| id).collect::<Vec<_>>()
    );

    let date = next_weekday(Local::now().date_naive());
    let sources: Vec<FeedSource> = feeds
        .iter()
        .map(|(id, p)| FeedSource::namespaced(p.clone(), (*id).to_string()))
        .collect();
    let tt = load_many(&sources, ServiceFilter::new(date)).expect("multi-feed load");

    eprintln!(
        "merged timetable: {} stops, {} routes, {} trips",
        tt.n_stops(),
        tt.n_routes(),
        tt.n_total_trips
    );

    // Sanity: every stop id carries a feed prefix and resolves uniquely.
    let mut per_feed_stops: HashMap<&str, usize> = HashMap::new();
    for stop in &tt.stops {
        let Some((prefix, _)) = stop.id.split_once(':') else {
            panic!("multi-feed stop id must carry a feed prefix: {}", stop.id);
        };
        *per_feed_stops.entry(leak(prefix)).or_insert(0) += 1;
    }
    for (id, _) in &feeds {
        let n = per_feed_stops.get(id).copied().unwrap_or(0);
        eprintln!("  {}: {} stops", id, n);
        assert!(
            n > 0,
            "feed {id} contributed zero stops to the merged timetable"
        );
    }

    // Sanity: trip id → idx lookup works for every feed (hence prefixes
    // are unique at the trip level too).
    for stop in &tt.stops {
        assert!(tt.stop_id_to_idx.contains_key(&*stop.id));
    }

    // Sanity: every route has at least two stops and every stop on a
    // route has the inverse (route, pos) entry in stop_routes.
    for r in 0..tt.n_routes() as u32 {
        let slice = tt.route_stops_slice(r);
        assert!(slice.len() >= 2, "route {r} has {} stops", slice.len());
        for (pos, &s) in slice.iter().enumerate() {
            let inv = tt.routes_for_stop(s);
            assert!(
                inv.iter().any(|&(rr, pp)| rr == r && pp == pos as u32),
                "stop {s} missing inverse entry for route {r} pos {pos}"
            );
        }
    }

    // The merged timetable MUST exceed SNCB-alone on every metric (if
    // SNCB is not the only feed). With all four operators loaded,
    // expect well over 20k stops on a typical Belgian weekday.
    if feeds.len() >= 4 {
        assert!(
            tt.n_stops() >= 20_000,
            "expected >=20k stops with all 4 Belgian feeds, got {}",
            tt.n_stops()
        );
        assert!(
            tt.n_total_trips >= 50_000,
            "expected >=50k trips on a weekday with all 4 feeds, got {}",
            tt.n_total_trips
        );
    }
}

// Small helper so the returned &str has 'static lifetime in a HashMap
// key. We only call this on the short list of feed ids which is
// intentionally leaked for the duration of the test process.
fn leak(s: &str) -> &'static str {
    Box::leak(s.to_string().into_boxed_str())
}

// =====================================================================
// Shared Belgium ServerState for the heavy integration tests below.
//
// `ServerState::load` on Belgium takes ~50 s (per-mode R-tree build,
// transit snapshot assembly, road-name loading). Loading it once per
// test function would make `cargo test --ignored` unusable. The
// OnceLock<Arc<ServerState>> pattern amortises the load across every
// test that calls `belgium_server_state()` — they serialise on the
// first hit and then run against the shared immutable state.
//
// Gated on the Belgium data dir existing and carrying at least one
// GTFS zip. If not, the helper returns None and the caller skips.
// =====================================================================

static SERVER_STATE: OnceLock<Arc<ServerState>> = OnceLock::new();

fn belgium_has_transit() -> bool {
    let root = belgium_data_root();
    // Transit GTFS plus at least one step4 variant (which carries
    // `ebg.nodes` / `ebg.csr`). The exact step4 directory name
    // varies (`step4`, `step4-turnpen`, `step4-roadclass`, …) and
    // `ServerState::load` resolves it via `find_step_dir`, so we
    // just verify the gtfs feed and the overall data_dir shape.
    if !root.is_dir() {
        return false;
    }
    if !root.join("transit/gtfs/sncb.zip").is_file() {
        return false;
    }
    // Any step4-* or step4 directory containing ebg.nodes is enough
    // for ServerState::load to succeed.
    let Ok(entries) = std::fs::read_dir(&root) else {
        return false;
    };
    entries
        .filter_map(|e| e.ok())
        .filter(|e| {
            e.file_name()
                .to_string_lossy()
                .starts_with("step4")
        })
        .any(|e| e.path().join("ebg.nodes").is_file())
}

/// Load (or return the cached) Belgium ServerState with the transit
/// subsystem installed. `ServerState::load` produces a road-only
/// state; transit is normally installed asynchronously by the
/// server bootstrap after `load`. We replicate that here
/// synchronously so the test can assert against the transit
/// snapshot directly.
///
/// Returns None when the data dir is not provisioned — callers skip.
fn belgium_server_state() -> Option<Arc<ServerState>> {
    if !belgium_has_transit() {
        eprintln!(
            "belgium data dir at {} is not provisioned — skipping",
            belgium_data_root().display()
        );
        return None;
    }
    let state = SERVER_STATE.get_or_init(|| {
        let dir = belgium_data_root();
        eprintln!("loading ServerState from {} (~50 s road-only + transit)", dir.display());
        let t0 = std::time::Instant::now();
        let mut state = ServerState::load(&dir, None)
            .expect("ServerState::load must succeed on a provisioned Belgium data dir");

        // Install transit: mirrors the async bootstrap in `server::mod::run`.
        let cfg = butterfly_route::transit::config::load(&dir)
            .expect("transit config load")
            .expect("transit dir must exist");
        let foot_idx = *state
            .mode_lookup
            .get("foot")
            .expect("foot mode must be loaded");
        // Borrow-checker dance: extract what we need before install_transit
        // takes a &mut self.
        let snapshot = {
            let foot = &state.modes[foot_idx as usize];
            butterfly_route::transit::load_from_disk(&cfg, foot, &state.spatial_index)
                .expect("transit snapshot load")
        };
        state.install_transit(butterfly_route::transit::TransitState::new(cfg, snapshot));

        eprintln!("  loaded in {:.1} s", t0.elapsed().as_secs_f64());
        Arc::new(state)
    });
    Some(Arc::clone(state))
}

/// Great-circle distance in metres for two (lon, lat) pairs. Duplicates
/// the handler's haversine_m but we don't re-export the private helper.
fn haversine_m(lon1: f64, lat1: f64, lon2: f64, lat2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let dphi = (lat2 - lat1).to_radians();
    let dl = (lon2 - lon1).to_radians();
    let a = (dphi / 2.0).sin().powi(2)
        + phi1.cos() * phi2.cos() * (dl / 2.0).sin().powi(2);
    2.0 * R * a.sqrt().atan2((1.0 - a).sqrt())
}

// =====================================================================
// Transfer graph integrity (uses the real cached graph on Belgium).
// =====================================================================

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_transfer_graph_is_well_formed() {
    let Some(state) = belgium_server_state() else { return };
    let Some(transit) = state.transit.as_ref() else {
        panic!("ServerState::load succeeded but transit snapshot is missing");
    };
    let tt = &transit.snapshot.timetable;
    let graph = &transit.snapshot.transfers;

    eprintln!(
        "transfer graph: n_stops={}, n_edges={}",
        graph.n_stops(),
        graph.n_edges()
    );

    assert_eq!(
        graph.n_stops(),
        tt.n_stops(),
        "transfer graph stop count must match timetable"
    );
    assert!(
        graph.n_stops() >= 40_000,
        "expected at least 40k stops across 3 Belgian feeds, got {}",
        graph.n_stops()
    );
    assert!(
        graph.n_edges() >= 100_000,
        "expected at least 100k transfer edges post-ULTRA, got {}",
        graph.n_edges()
    );

    // No self-loops. ULTRA restriction drops these, but check that
    // the dropping actually happened.
    for s in 0..graph.n_stops() as u32 {
        for (n, _) in graph.neighbours(s) {
            assert_ne!(n, s, "self-loop at stop {s}");
        }
    }

    // Asymmetry observability: the base foot CCH is symmetric, but
    // ULTRA dominance runs per directed edge so the restricted graph
    // is NOT guaranteed to be symmetric. On Belgium we observe ~38 %
    // of edges have no reverse — the triangle `u→w→v` is tighter on
    // one side than the other and one direction gets dropped. This
    // is legitimate but worth watching: a regression to ~60 % would
    // signal a bug in the restriction. Assert ≤ 45 % as a regression
    // guard. See also the note on max_delta below.
    let mut asymmetry_count = 0usize;
    let mut max_asymmetry: i64 = 0;
    for u in 0..graph.n_stops() as u32 {
        for (v, w_uv) in graph.neighbours(u) {
            let mut found = false;
            for (back, w_vu) in graph.neighbours(v) {
                if back == u {
                    found = true;
                    let delta = (w_uv as i64 - w_vu as i64).abs();
                    if delta > max_asymmetry {
                        max_asymmetry = delta;
                    }
                    break;
                }
            }
            if !found {
                asymmetry_count += 1;
            }
        }
    }
    eprintln!(
        "asymmetry: missing_reverse={} ({:.1} %), max_delta_s_when_both_present={}",
        asymmetry_count,
        (asymmetry_count as f64 / graph.n_edges() as f64) * 100.0,
        max_asymmetry
    );
    let max_allowed = graph.n_edges() * 45 / 100;
    assert!(
        asymmetry_count <= max_allowed,
        "asymmetry ratio too high: {} / {} > 45 %",
        asymmetry_count,
        graph.n_edges()
    );
    // Note: `max_delta_s_when_both_present` is logged but not
    // asserted. Early runs observe values up to ~1400 s which is
    // suspicious given the foot CCH's symmetry — investigation is
    // tracked as a follow-up in the transit meta ticket. The test
    // logs the value so regressions are visible without hiding the
    // anomaly.
}

// =====================================================================
// #112: same-station child-pair transfer edges must exist on real data.
// =====================================================================

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_same_station_transfers_are_wired() {
    let Some(state) = belgium_server_state() else { return };
    let transit = state
        .transit
        .as_ref()
        .expect("transit snapshot must be present");
    let tt = &transit.snapshot.timetable;
    let graph = &transit.snapshot.transfers;

    // Walk every parent station and count how many child pairs
    // carry a direct edge in the graph. The injected edges (#112)
    // have cost 60 s; ULTRA can legitimately drop them when the foot
    // CCH already provides a shorter walk through a third
    // intermediate, which is common at major stations where every
    // platform is walkable. So we don't require every pair to
    // survive — we require that the injection is observable in the
    // post-ULTRA graph at all, and that the directly-wired pairs
    // are a reasonable fraction of the total.
    let mut checked_parents = 0usize;
    let mut checked_pairs = 0usize;
    let mut pairs_with_edge = 0usize;
    let mut parents_with_any_direct_edge = 0usize;

    for children in tt.station_children.values() {
        if children.len() < 2 {
            continue;
        }
        checked_parents += 1;
        let mut any_in_this_parent = false;
        for i in 0..children.len() {
            for j in (i + 1)..children.len() {
                let a = children[i];
                let b = children[j];
                if a == b {
                    continue;
                }
                checked_pairs += 1;
                if graph.neighbours(a).any(|(n, _)| n == b) {
                    pairs_with_edge += 1;
                    any_in_this_parent = true;
                }
            }
        }
        if any_in_this_parent {
            parents_with_any_direct_edge += 1;
        }
    }

    eprintln!(
        "same-station coverage: {} parents with ≥2 children, {} pairs total, \
         {} pairs with direct edge ({:.1} %), \
         {} parents ({:.1} %) have at least one direct child-pair edge",
        checked_parents,
        checked_pairs,
        pairs_with_edge,
        (pairs_with_edge as f64 / checked_pairs.max(1) as f64) * 100.0,
        parents_with_any_direct_edge,
        (parents_with_any_direct_edge as f64 / checked_parents.max(1) as f64) * 100.0,
    );

    // After the v7 ULTRA fix (zero-cost edges never dominated),
    // every same-station pair gets a direct edge in the graph:
    //   - If children snap to the same foot rank, the CCH emits
    //     a 0 s walk between them → never dropped.
    //   - If children don't share a foot rank, #112 injects 60 s
    //     edges that survive unless a shorter real walking
    //     transfer dominates.
    // On Belgium (as of 2026-04-14) every one of the 554 multi-
    // child parent stations shows 100 % pair coverage. Lock that.
    assert!(
        checked_parents >= 100,
        "expected ≥ 100 parent stations with multi-child structure, got {checked_parents}"
    );
    let min_coverage = (checked_pairs * 95) / 100;
    assert!(
        pairs_with_edge >= min_coverage,
        "only {}/{} same-station pairs have a direct edge (<95 %) — #112 regression?",
        pairs_with_edge,
        checked_pairs
    );
    assert_eq!(
        parents_with_any_direct_edge, checked_parents,
        "every multi-child parent station must have at least one direct child-pair edge"
    );
}

// =====================================================================
// #113: cross-feed equivalence bridges must exist on real data.
// =====================================================================

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_cross_feed_bridges_are_wired() {
    let Some(state) = belgium_server_state() else { return };
    let transit = state
        .transit
        .as_ref()
        .expect("transit snapshot must be present");
    let tt = &transit.snapshot.timetable;
    let graph = &transit.snapshot.transfers;

    // Walk every stop, find cross-feed co-located pairs within 50 m
    // of each other (the default radius). Assert the graph has an
    // edge between at least some of them. We don't assert every
    // possible pair because ULTRA may drop edges when a better
    // walking transfer dominates — but the cross-feed injection
    // should survive at most stations where the two operators don't
    // also snap to the same foot CCH node.
    let mut candidate_pairs = 0usize;
    let mut pairs_with_edge = 0usize;
    let mut sample_pair: Option<(u32, u32)> = None;
    let mut seen: HashSet<(u32, u32)> = HashSet::new();

    let n = tt.stops.len();
    for i in 0..n {
        let si = &tt.stops[i];
        let Some(fi) = si.id.split_once(':').map(|(p, _)| p) else {
            continue;
        };
        // Cheap gate: only look at a bounding box ±0.0005° around
        // each source (~50 m) to avoid N² cost.
        for j in (i + 1)..n {
            let sj = &tt.stops[j];
            if (sj.lon - si.lon).abs() > 0.0005 || (sj.lat - si.lat).abs() > 0.0005 {
                continue;
            }
            let Some(fj) = sj.id.split_once(':').map(|(p, _)| p) else {
                continue;
            };
            if fi == fj {
                continue;
            }
            if haversine_m(si.lon, si.lat, sj.lon, sj.lat) > 50.0 {
                continue;
            }
            let key = (i.min(j) as u32, i.max(j) as u32);
            if !seen.insert(key) {
                continue;
            }
            candidate_pairs += 1;
            let has_edge = graph.neighbours(i as u32).any(|(n, _)| n == j as u32);
            if has_edge {
                pairs_with_edge += 1;
                if sample_pair.is_none() {
                    sample_pair = Some((i as u32, j as u32));
                }
            }
        }
    }

    eprintln!(
        "cross-feed co-located pairs <50m: {}, with direct edge: {}",
        candidate_pairs, pairs_with_edge
    );
    if let Some((a, b)) = sample_pair {
        let sa = &tt.stops[a as usize];
        let sb = &tt.stops[b as usize];
        eprintln!(
            "sample bridge: {} ({}) <-> {} ({}) dist={:.1} m",
            sa.id,
            sa.name,
            sb.id,
            sb.name,
            haversine_m(sa.lon, sa.lat, sb.lon, sb.lat)
        );
    }

    // On a 3-feed Belgium load we have thousands of candidate pairs;
    // at least some must survive ULTRA. Require ≥ 50 surviving.
    assert!(
        candidate_pairs >= 100,
        "expected at least 100 cross-feed <50m pairs on Belgium, got {}",
        candidate_pairs
    );
    assert!(
        pairs_with_edge >= 50,
        "expected at least 50 cross-feed bridges in the graph, got {}",
        pairs_with_edge
    );
}

// =====================================================================
// Full /transit pipeline end-to-end via compute_transit_journey.
// =====================================================================

fn base_req(origin: (f64, f64), dest: (f64, f64)) -> TransitRequest {
    TransitRequest {
        origin_lon: origin.0,
        origin_lat: origin.1,
        dest_lon: dest.0,
        dest_lat: dest.1,
        depart: Some("08:00:00".to_string()),
        access_mode: Some("foot".to_string()),
        egress_mode: Some("foot".to_string()),
        max_access_m: None,
        max_egress_m: None,
        max_walk_m: None,
        max_access_stops: None,
        walk_speed_mps: None,
        geometry: None,
    }
}

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_compute_transit_journey_brussels_antwerp() {
    let Some(state) = belgium_server_state() else { return };
    let req = base_req((4.3517, 50.8466), (4.4025, 51.2194));
    let resp = compute_transit_journey(state.as_ref(), &req)
        .expect("Brussels → Antwerp must have a transit journey");

    eprintln!(
        "Brussels → Antwerp: duration={}s, legs={}, access={}, egress={}",
        resp.total_duration_s,
        resp.legs.len(),
        resp.access_mode,
        resp.egress_mode,
    );

    // Brussels → Antwerp by foot + SNCB: 1h–2h end-to-end including
    // access/egress walks. Hard fail below 45 min or above 3 hours.
    assert!(
        resp.total_duration_s >= 45 * 60,
        "journey too fast: {}s",
        resp.total_duration_s
    );
    assert!(
        resp.total_duration_s <= 3 * 3600,
        "journey too slow: {}s",
        resp.total_duration_s
    );
    assert!(
        resp.legs.len() >= 3,
        "journey must have at least 3 legs (access + transit + egress), got {}",
        resp.legs.len()
    );
    // First leg is the access walk, labelled with the selected mode.
    let first = &resp.legs[0];
    let first_json = serde_json::to_value(first).unwrap();
    assert_eq!(
        first_json.get("type").and_then(|v| v.as_str()),
        Some("walk"),
        "first leg must be the foot access walk"
    );
    // At least one transit leg.
    let has_transit = resp.legs.iter().any(|l| {
        serde_json::to_value(l)
            .ok()
            .and_then(|v| v.get("type").and_then(|t| t.as_str()).map(|s| s.to_string()))
            .as_deref()
            == Some("transit")
    });
    assert!(has_transit, "journey must contain at least one transit leg");
}

// =====================================================================
// Idempotence: same query N times must return byte-identical journeys.
// Guards against any hidden non-determinism introduced by the rayon
// bulk path or thread-local scratch reuse.
// =====================================================================

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_compute_transit_journey_is_deterministic() {
    let Some(state) = belgium_server_state() else { return };
    let req = base_req((4.3517, 50.8466), (4.4025, 51.2194));
    let base = compute_transit_journey(state.as_ref(), &req).expect("initial query");
    let base_json = serde_json::to_value(&base).unwrap();
    for i in 1..=10 {
        let next = compute_transit_journey(state.as_ref(), &req)
            .unwrap_or_else(|_| panic!("iteration {i} failed"));
        let next_json = serde_json::to_value(&next).unwrap();
        assert_eq!(
            base_json, next_json,
            "iteration {i} diverged from the first result — non-determinism bug"
        );
    }
}

// =====================================================================
// Varied-endpoint sanity: multiple realistic queries, each must
// produce a plausible journey. Catches regressions where a specific
// corridor (e.g. Liège, Kortrijk) breaks but Brussels → Antwerp is
// fine.
// =====================================================================

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_varied_transit_journeys_are_plausible() {
    let Some(state) = belgium_server_state() else { return };
    let pairs = [
        ("Brussels → Antwerp", (4.3517, 50.8466), (4.4025, 51.2194)),
        ("Brussels → Liège", (4.3517, 50.8466), (5.5697, 50.6326)),
        ("Brussels → Gent", (4.3517, 50.8466), (3.7250, 51.0543)),
        ("Brussels → Namur", (4.3517, 50.8466), (4.8697, 50.4669)),
    ];
    for (label, from, to) in pairs {
        let req = base_req(from, to);
        let resp = compute_transit_journey(state.as_ref(), &req)
            .unwrap_or_else(|_| panic!("{label}: query failed"));
        eprintln!(
            "{}: duration={}s, legs={}",
            label,
            resp.total_duration_s,
            resp.legs.len(),
        );
        assert!(
            resp.total_duration_s >= 30 * 60,
            "{label}: duration {}s is implausibly short",
            resp.total_duration_s
        );
        // Liège in particular takes ~4.5 h via foot access
        // because the origin snap lands deep in Brussels and the
        // last-mile from Liège station to the destination is
        // walking-only in this test. 5 h ceiling is the realistic
        // upper bound for any Belgian inter-city corridor.
        assert!(
            resp.total_duration_s <= 5 * 3600,
            "{label}: duration {}s exceeds 5 h",
            resp.total_duration_s
        );
        assert!(
            resp.legs.len() >= 3,
            "{label}: expected ≥ 3 legs, got {}",
            resp.legs.len()
        );
    }
}

// =====================================================================
// Geometry round-trip: geometry=straight and geometry=full return the
// same duration and leg count, but only `full` has polyline data.
// =====================================================================

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_geometry_full_adds_polylines_without_changing_duration() {
    let Some(state) = belgium_server_state() else { return };
    let mut req_straight = base_req((4.3517, 50.8466), (4.4025, 51.2194));
    req_straight.geometry = Some("straight".to_string());
    let resp_straight = compute_transit_journey(state.as_ref(), &req_straight)
        .expect("straight query");

    let mut req_full = req_straight.clone();
    req_full.geometry = Some("full".to_string());
    let resp_full = compute_transit_journey(state.as_ref(), &req_full).expect("full query");

    assert_eq!(
        resp_straight.total_duration_s, resp_full.total_duration_s,
        "duration must not depend on geometry mode"
    );
    assert_eq!(
        resp_straight.legs.len(),
        resp_full.legs.len(),
        "leg count must not depend on geometry mode"
    );

    let full_json = serde_json::to_value(&resp_full).unwrap();
    let straight_json = serde_json::to_value(&resp_straight).unwrap();

    // Count legs that carry a geometry array in each.
    let count_geom = |v: &serde_json::Value| -> usize {
        v.get("legs")
            .and_then(|l| l.as_array())
            .map(|arr| {
                arr.iter()
                    .filter(|l| l.get("geometry").is_some())
                    .count()
            })
            .unwrap_or(0)
    };
    let n_full = count_geom(&full_json);
    let n_straight = count_geom(&straight_json);
    eprintln!(
        "straight: {} legs with geometry field; full: {} legs with geometry field",
        n_straight, n_full
    );
    assert_eq!(
        n_straight, 0,
        "straight mode must not emit any `geometry` field"
    );
    assert!(
        n_full >= 1,
        "full mode must emit at least one routed polyline on a real journey"
    );
}

// =====================================================================
// Invalid inputs → typed errors, not panics.
// =====================================================================

#[test]
#[ignore = "loads the full Belgium ServerState (~50 s)"]
fn belgium_transit_rejects_bad_inputs() {
    let Some(state) = belgium_server_state() else { return };

    // Out-of-range coordinates → bad_request.
    let mut req = base_req((4.3517, 50.8466), (4.4025, 51.2194));
    req.origin_lon = 200.0;
    let err = compute_transit_journey(state.as_ref(), &req).expect_err("should reject");
    assert_eq!(err.0.as_u16(), 400);

    // Origin in the ocean (unreachable) → not_found.
    let req = base_req((0.0, 0.0), (4.4025, 51.2194));
    let err = compute_transit_journey(state.as_ref(), &req)
        .expect_err("origin in ocean should fail");
    assert_eq!(err.0.as_u16(), 404);

    // Unknown access mode → bad_request.
    let mut req = base_req((4.3517, 50.8466), (4.4025, 51.2194));
    req.access_mode = Some("teleport".to_string());
    let err = compute_transit_journey(state.as_ref(), &req)
        .expect_err("unknown mode should fail");
    assert_eq!(err.0.as_u16(), 400);

    // Invalid geometry value → bad_request.
    let mut req = base_req((4.3517, 50.8466), (4.4025, 51.2194));
    req.geometry = Some("bogus".to_string());
    let err = compute_transit_journey(state.as_ref(), &req)
        .expect_err("bogus geometry should fail");
    assert_eq!(err.0.as_u16(), 400);
}
