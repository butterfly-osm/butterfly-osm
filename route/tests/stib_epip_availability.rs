//! STIB NeTEx-EPIP availability smoke test.
//!
//! STIB is the only major Belgian operator that has deprecated GTFS
//! in favour of NeTEx-EPIP via the Belgian National Access Point
//! (see `butterfly_route::transit::config::default_belgium_feeds`
//! for the background). A full NeTEx-EPIP loader is tracked in
//! butterfly-osm/butterfly-osm#101 — this test does *not* implement
//! parsing; it validates three operational claims that the #101
//! loader will depend on:
//!
//! 1. The published EPIP XML file exists at the expected path on
//!    disk (or at the Belgian NAP URL, when `STIB_EPIP_NETWORK` is
//!    set).
//! 2. It is well-formed XML with the NeTEx 1.x root element.
//! 3. Its element counts are in the ballpark we expect for the STIB
//!    network — ~11 k routing stops, ~90 lines, ~100 k trips. If any
//!    of these collapse by an order of magnitude the file has been
//!    replaced with something else and the #101 loader needs to be
//!    re-validated against the new shape.
//!
//! Both variants of this test are `#[ignore]`'d by default because
//! the file is 720 MB and not something CI should fetch. Run
//! manually:
//!
//! ```
//! # Local file (after downloading once to this path).
//! cargo test --test stib_epip_availability -- --ignored \
//!     stib_epip_local_file
//!
//! # Network fetch (hits the Belgian NAP). Slow.
//! STIB_EPIP_NETWORK=1 cargo test --test stib_epip_availability -- \
//!     --ignored stib_epip_network
//! ```

use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::time::Instant;

const EPIP_NETEX_URL: &str =
    "https://belgianmobility.blob.core.windows.net/epip-production/epip-stibmivb-bmc-latest.xml";

/// Expected element count ranges (min, max). These are order-of-
/// magnitude sanity bounds, not exact numbers — STIB publishes the
/// file monthly and individual counts drift. A count outside the
/// range means the file shape has changed materially and someone
/// should investigate before the #101 loader re-runs against it.
struct ExpectedCounts {
    scheduled_stop_point: (usize, usize),
    stop_place: (usize, usize),
    line: (usize, usize),
    service_journey: (usize, usize),
    timetabled_passing_time: (usize, usize),
}

fn expected() -> ExpectedCounts {
    ExpectedCounts {
        // 11,757 observed 2026-04-14. Bound generously — ±50 %.
        scheduled_stop_point: (5_000, 20_000),
        // 1,994 observed. Parent stations; relatively stable.
        stop_place: (1_000, 5_000),
        // 90 observed. Line count moves slowly.
        line: (50, 200),
        // 110,045 observed. Varies significantly by calendar period.
        service_journey: (50_000, 250_000),
        // 2,216,323 observed. Scales with ServiceJourney × stops/trip.
        timetabled_passing_time: (1_000_000, 5_000_000),
    }
}

/// Stream a NeTEx-EPIP file and count the element openings we care
/// about. Uses a line-based byte scan — 720 MB / line-based BufReader
/// on an SSD takes ~10 s, well under the 60 s default test timeout.
/// No XML parser dependency because the real parser is #101's job
/// and this test just needs a structural sanity check.
fn count_elements(path: &std::path::Path) -> std::io::Result<ActualCounts> {
    let f = File::open(path)?;
    // 4 MB read buffer — big enough to amortise syscalls, small
    // enough that we never materialise more than a tiny fraction of
    // the 720 MB file in memory at once.
    let reader = BufReader::with_capacity(4 * 1024 * 1024, f);
    let mut counts = ActualCounts::default();
    // NOTE: line-based iteration is fine here because NeTEx-EPIP
    // from the Belgian NAP pretty-prints one element opening per
    // line (verified manually with `head -30`). If a future publisher
    // one-lines the file, switch to a byte-wise state machine.
    for line in reader.lines() {
        let line = line?;
        let bytes = line.as_bytes();
        // Open-tag matching: we look for `<Name` where the next byte
        // is either `>`, ` `, `/`, or end-of-line. `.contains()` on a
        // bare substring would over-match on `<NameOther`.
        if tag_opens(bytes, b"ScheduledStopPoint") {
            counts.scheduled_stop_point += 1;
        }
        if tag_opens(bytes, b"StopPlace") {
            counts.stop_place += 1;
        }
        if tag_opens(bytes, b"Line") {
            counts.line += 1;
        }
        if tag_opens(bytes, b"ServiceJourney") {
            counts.service_journey += 1;
        }
        if tag_opens(bytes, b"TimetabledPassingTime") {
            counts.timetabled_passing_time += 1;
        }
    }
    Ok(counts)
}

/// True if `haystack` contains a NeTEx open-tag for `tag_name`: a
/// `<` byte followed by `tag_name` followed by a word boundary
/// (space, `>`, `/`). Rejects partial matches like `<Line` when
/// searching for `ServiceJourney`, and `<LineRef` when searching
/// for `Line`.
fn tag_opens(haystack: &[u8], tag_name: &[u8]) -> bool {
    let mut i = 0;
    while i + 1 + tag_name.len() <= haystack.len() {
        if haystack[i] == b'<' && haystack[i + 1..i + 1 + tag_name.len()] == *tag_name {
            let next = haystack.get(i + 1 + tag_name.len()).copied();
            if matches!(next, Some(b' ') | Some(b'>') | Some(b'/') | None) {
                return true;
            }
        }
        i += 1;
    }
    false
}

#[derive(Default, Debug)]
struct ActualCounts {
    scheduled_stop_point: usize,
    stop_place: usize,
    line: usize,
    service_journey: usize,
    timetabled_passing_time: usize,
}

fn assert_in_range(name: &str, actual: usize, (lo, hi): (usize, usize)) {
    assert!(
        actual >= lo && actual <= hi,
        "{name}: {actual} is outside the expected range [{lo}, {hi}]. \
         Either the STIB NeTEx publication shape has drifted and the \
         #101 loader needs re-validation, or this test is wrong and \
         the bounds need tightening."
    );
}

fn local_epip_path() -> Option<PathBuf> {
    // Env override wins.
    if let Ok(p) = std::env::var("STIB_EPIP_PATH") {
        let path = PathBuf::from(p);
        if path.is_file() {
            return Some(path);
        }
    }
    // Default location under the Belgium data dir, mirroring the
    // `gtfs/` sibling that the other feeds use.
    let default = PathBuf::from("../data/belgium/transit/netex/stib-epip.xml");
    if default.is_file() {
        return Some(default);
    }
    None
}

fn assert_is_netex_root(path: &std::path::Path) {
    let mut f = File::open(path).expect("open stib epip");
    let mut head = vec![0u8; 512];
    use std::io::Read;
    let n = f.read(&mut head).expect("read stib epip head");
    let head_str = std::str::from_utf8(&head[..n]).expect("utf-8 head");
    assert!(
        head_str.starts_with("<?xml version=\"1.0\""),
        "file does not start with an XML declaration: first bytes = {head_str:?}"
    );
    assert!(
        head_str.contains("<PublicationDelivery"),
        "file is not a NeTEx PublicationDelivery: no opening tag in head = {head_str:?}"
    );
    assert!(
        head_str.contains("http://www.netex.org.uk/netex"),
        "file is not in the NeTEx namespace: {head_str:?}"
    );
}

#[test]
#[ignore = "requires the ~720 MB STIB NeTEx-EPIP file on disk — \
            set STIB_EPIP_PATH or place at data/belgium/transit/netex/stib-epip.xml"]
fn stib_epip_local_file() {
    let Some(path) = local_epip_path() else {
        panic!(
            "STIB NeTEx-EPIP file not found. Download once:\n  \
             mkdir -p data/belgium/transit/netex\n  \
             curl -sSL -o data/belgium/transit/netex/stib-epip.xml \\\n    \
             '{EPIP_NETEX_URL}'\n\
             Or set STIB_EPIP_PATH to a local copy."
        );
    };
    eprintln!("using STIB EPIP at {}", path.display());

    assert_is_netex_root(&path);

    let t0 = Instant::now();
    let counts = count_elements(&path).expect("count elements");
    let dt_s = t0.elapsed().as_secs_f64();
    eprintln!(
        "STIB EPIP element counts (scan {:.2}s):\n  \
         ScheduledStopPoint    = {}\n  \
         StopPlace             = {}\n  \
         Line                  = {}\n  \
         ServiceJourney        = {}\n  \
         TimetabledPassingTime = {}",
        dt_s,
        counts.scheduled_stop_point,
        counts.stop_place,
        counts.line,
        counts.service_journey,
        counts.timetabled_passing_time
    );

    let expected = expected();
    assert_in_range(
        "ScheduledStopPoint",
        counts.scheduled_stop_point,
        expected.scheduled_stop_point,
    );
    assert_in_range("StopPlace", counts.stop_place, expected.stop_place);
    assert_in_range("Line", counts.line, expected.line);
    assert_in_range(
        "ServiceJourney",
        counts.service_journey,
        expected.service_journey,
    );
    assert_in_range(
        "TimetabledPassingTime",
        counts.timetabled_passing_time,
        expected.timetabled_passing_time,
    );
}

/// End-to-end parse test: drive the real NeTEx-EPIP loader against
/// the STIB file and assert the resulting Timetable has the shape
/// we expect. This exercises `butterfly_route::transit::netex_epip::load_epip_xml`
/// in its first real deployment target.
#[test]
#[ignore = "parses the 720 MB STIB NeTEx-EPIP file (~30 s) — \
            set STIB_EPIP_PATH or place at data/belgium/transit/netex/stib-epip.xml"]
fn stib_epip_loader_produces_timetable() {
    use butterfly_route::transit::netex_epip::load_epip_xml;

    let Some(path) = local_epip_path() else {
        panic!(
            "STIB NeTEx-EPIP file not found — see stib_epip_local_file for the download command"
        );
    };
    eprintln!("parsing STIB EPIP from {}", path.display());

    let t0 = Instant::now();
    let tt = load_epip_xml(&path, Some("stib")).expect("EPIP load must succeed");
    let dt = t0.elapsed().as_secs_f64();
    eprintln!(
        "parsed in {:.1}s: {} stops, {} routes, {} trips",
        dt,
        tt.n_stops(),
        tt.n_routes(),
        tt.n_total_trips,
    );

    // STIB shape expectations. Bounds are ±50 % of observed values
    // so that a modest publication drift doesn't break the test.
    //
    // Note: the loader deduplicates per-pattern ScheduledStopPoints
    // by quantised coordinate, so the physical stop count is roughly
    // ~3,500 even though the raw file has 11,757 SSPs. The bound is
    // generous so the test passes both before and after dedup tuning.
    assert!(
        tt.n_stops() >= 1_500 && tt.n_stops() <= 20_000,
        "expected 1.5k–20k physical stops, got {}",
        tt.n_stops()
    );
    assert!(
        tt.n_routes() >= 100 && tt.n_routes() <= 2_000,
        "expected 100–2k routes (RAPTOR canonical patterns), got {}",
        tt.n_routes()
    );
    // After calendar filtering, ~14k trips run on a typical weekday
    // (down from ~110k raw ServiceJourneys spanning every day class).
    // Bounds are wide enough to absorb weekday vs weekend skew and
    // the stale-publication remap fallback.
    assert!(
        tt.n_total_trips >= 5_000 && tt.n_total_trips <= 250_000,
        "expected 5k–250k trips, got {}",
        tt.n_total_trips
    );

    // Every stop must carry the `stib:` feed prefix.
    let mut n_bad_prefix = 0usize;
    for stop in &tt.stops {
        if !stop.id.starts_with("stib:") {
            n_bad_prefix += 1;
        }
    }
    assert_eq!(n_bad_prefix, 0, "stop id missing 'stib:' prefix");

    // Coordinates must be Brussels-area after reprojection. Lambert-93
    // → WGS84 for the STIB coverage should land lon ≈ 4.2–4.5,
    // lat ≈ 50.75–50.95. Check the distribution.
    let mut in_brussels = 0usize;
    let mut out_of_bounds = 0usize;
    for stop in &tt.stops {
        let brussels = (4.0..=5.0).contains(&stop.lon) && (50.5..=51.1).contains(&stop.lat);
        if brussels {
            in_brussels += 1;
        } else {
            out_of_bounds += 1;
        }
    }
    eprintln!(
        "coordinates: {} in Brussels bbox, {} out of bounds",
        in_brussels, out_of_bounds
    );
    // Allow a tiny fraction (< 5 %) to land outside for satellite
    // bus stops or reprojection rounding at the edges — the vast
    // majority must be inside.
    let min_inside = (tt.n_stops() * 95) / 100;
    assert!(
        in_brussels >= min_inside,
        "only {}/{} stops in Brussels bbox — reprojection may be broken",
        in_brussels,
        tt.n_stops()
    );
}

#[test]
#[ignore = "hits the live Belgian NAP (720 MB download) — \
            set STIB_EPIP_NETWORK=1 to enable"]
fn stib_epip_network_availability() {
    if std::env::var("STIB_EPIP_NETWORK").ok().as_deref() != Some("1") {
        eprintln!("STIB_EPIP_NETWORK=1 not set, skipping live fetch");
        return;
    }

    // We don't download the full 720 MB — just range-GET the first
    // 8 KB and assert it's a NeTEx PublicationDelivery. That's
    // enough to prove the endpoint is alive and still publishing.
    let out = std::process::Command::new("curl")
        .args([
            "-sSfL",
            "-H",
            "Range: bytes=0-8191",
            "--max-time",
            "30",
            EPIP_NETEX_URL,
        ])
        .output()
        .expect("curl invocation");
    assert!(
        out.status.success(),
        "curl failed: stderr = {}",
        String::from_utf8_lossy(&out.stderr)
    );
    let head = String::from_utf8_lossy(&out.stdout);
    assert!(
        head.starts_with("<?xml"),
        "response did not start with an XML declaration: {head:?}"
    );
    assert!(
        head.contains("<PublicationDelivery"),
        "response missing NeTEx root element"
    );
    assert!(
        head.contains("http://www.netex.org.uk/netex"),
        "response missing NeTEx namespace"
    );
    eprintln!(
        "STIB EPIP endpoint is live; first 8 KB start with PublicationDelivery / NeTEx namespace"
    );
}
