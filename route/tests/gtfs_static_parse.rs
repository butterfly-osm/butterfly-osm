//! GTFS static parse regression test on a committed fixture (#598).
//!
//! Every other transit test needs a real feed on disk, so on a bare
//! runner they all skip and nothing checks the GTFS static parse at
//! all. That is not a safety net, it is a claim nobody verifies — and
//! it had a concrete cost: a dependency clean-up that deduplicates the
//! macro-parsing crate could not be landed, because the same bump moves
//! the hashing and archive crates used inside this loader and nothing
//! could prove the parse output was unchanged.
//!
//! This test needs no artifact, no network and no licensed feed. It
//! parses `tests/fixtures/gtfs_mini/` — an invented six-stop network —
//! through the real loader and pins the result exactly.
//!
//! ## What the fixture pins
//!
//! * **Stop registration order** — `stops.txt` rows are deliberately
//!   shuffled; the loader sorts by GTFS id, so `StopIdx` must come out
//!   `S1, S1A, S1B, S2, S3, S4` whatever order the parser's `HashMap`
//!   yields. The transfer-graph cache keys edges by `StopIdx`, so this
//!   ordering is load-bearing, not cosmetic.
//! * **Parent stations** — `S1` is a station (`location_type=1`) with
//!   platforms `S1A` / `S1B`; `station_children` must list both plus
//!   the parent itself.
//! * **Calendar** — `WEEKDAY` / `WEEKEND` / `NEVER` service windows and
//!   weekday bitmaps.
//! * **Calendar exceptions** — on 2024-07-03 (a Wednesday, so normally
//!   a `WEEKDAY` day) `calendar_dates.txt` *deletes* `WEEKDAY` and
//!   *adds* `NEVER`. The active set must flip completely.
//! * **`stop_sequence` ordering** — `T2_LATE`'s stop-times are written
//!   out of order in the file and must come back in sequence order.
//! * **Times crossing midnight** — `T2_LATE` runs 23:50 → 25:12, i.e.
//!   past 86400 s, and must not wrap.
//! * **Missing arrival / departure fallback** — `T1_EARLY` omits the
//!   arrival at `S3` and the departure at `S4`; each must fall back to
//!   the other field.
//! * **Trips dropped** — `T5_SINGLE` has one stop-time (< 2) and must
//!   never reach the timetable.
//! * **Trip ordering within a pattern** — `T2_LATE` is listed first in
//!   `trips.txt` but departs last, and must be sorted after `T1_EARLY`
//!   by departure at the first stop (RAPTOR's `earliest_trip` relies on
//!   it).
//! * **Route metadata fallbacks** — `R2` has empty short/long names and
//!   `T4_EXCEPTION` has no headsign, so the headsign falls back to the
//!   `direction_id` word.
//! * **Feed namespacing** — loading the same feed twice under two feed
//!   ids must produce disjoint, prefixed stop / trip ids.
//! * **Archive + hashing path** — the fixture is also zipped at runtime
//!   (stored, fixed timestamps ⇒ byte-deterministic) and parsed through
//!   the zip reader. The zip parse must produce the *same* timetable
//!   digest as the directory parse, and the feed sha256 the parser
//!   reports is pinned to a constant.

use std::fmt::Write as _;
use std::path::{Path, PathBuf};

use butterfly_route::transit::gtfs::{FeedSource, ServiceFilter, load_many, load_zip};
use butterfly_route::transit::timetable::Timetable;
use chrono::NaiveDate;
use sha2::{Digest, Sha256};

/// Tuesday — `WEEKDAY` runs, no exception applies.
const PLAIN_WEEKDAY: (i32, u32, u32) = (2024, 7, 2);
/// Wednesday — `calendar_dates.txt` deletes `WEEKDAY` and adds `NEVER`.
const EXCEPTION_DAY: (i32, u32, u32) = (2024, 7, 3);

/// SHA-256 of the byte-deterministic zip built from the fixture. Pins
/// the hashing crate the loader's dependency chain uses: the parser
/// hashes the whole archive, so any change in that crate's output shows
/// up here immediately.
const FIXTURE_ZIP_SHA256: &str = "0f89f74de60bc8a08e381df4a8c925ced6d9de0533bd6130a4b8f9e5b1ff3afe";

/// Digest of the timetable compiled from the fixture on `PLAIN_WEEKDAY`.
/// The individual assertions below say *what* is pinned; this constant
/// makes sure nothing else moved either.
const WEEKDAY_TIMETABLE_DIGEST: &str =
    "4d33760c3db4d7fe57db82cb35fe62f02de1628e1894147c83218ca4217bbcc6";

fn date(d: (i32, u32, u32)) -> NaiveDate {
    NaiveDate::from_ymd_opt(d.0, d.1, d.2).expect("valid fixture date")
}

fn fixture_dir() -> PathBuf {
    Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/gtfs_mini")
}

// ---------------------------------------------------------------------
// Minimal deterministic zip writer
// ---------------------------------------------------------------------
//
// The fixture is committed as plain CSV so a reviewer can read it, but
// the loader's archive path has to be exercised too. Rather than commit
// a second, binary copy that can silently drift from the CSV, we build
// the archive from the CSV at test time. Entries are STORED (no
// compression) with a fixed DOS timestamp and a fixed file order, so
// the bytes — and therefore the feed sha256 — are reproducible.

/// CRC-32 (IEEE, reflected), as required by the zip local/central headers.
fn crc32(data: &[u8]) -> u32 {
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        crc ^= u32::from(byte);
        for _ in 0..8 {
            let mask = (crc & 1).wrapping_neg();
            crc = (crc >> 1) ^ (0xEDB8_8320 & mask);
        }
    }
    !crc
}

/// 1980-01-01 00:00:00 in DOS date/time form — a fixed timestamp keeps
/// the archive bytes identical from run to run and host to host.
const DOS_TIME: u16 = 0;
const DOS_DATE: u16 = (1 << 5) | 1; // year 1980, month 1, day 1

fn push_u16(out: &mut Vec<u8>, v: u16) {
    out.extend_from_slice(&v.to_le_bytes());
}

fn push_u32(out: &mut Vec<u8>, v: u32) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// Zip the fixture directory into `dest` using STORED entries.
fn write_fixture_zip(dest: &Path) {
    let mut names: Vec<String> = std::fs::read_dir(fixture_dir())
        .expect("fixture directory")
        .map(|e| {
            e.expect("fixture entry")
                .file_name()
                .to_string_lossy()
                .into_owned()
        })
        .collect();
    names.sort();

    let mut out: Vec<u8> = Vec::new();
    let mut central: Vec<u8> = Vec::new();
    let mut n_entries = 0u16;

    for name in &names {
        let body = std::fs::read(fixture_dir().join(name)).expect("fixture file");
        let crc = crc32(&body);
        let size = u32::try_from(body.len()).expect("fixture file fits in u32");
        let local_offset = u32::try_from(out.len()).expect("fixture archive fits in u32");

        // Local file header.
        push_u32(&mut out, 0x0403_4b50);
        push_u16(&mut out, 20); // version needed
        push_u16(&mut out, 0); // flags
        push_u16(&mut out, 0); // method: stored
        push_u16(&mut out, DOS_TIME);
        push_u16(&mut out, DOS_DATE);
        push_u32(&mut out, crc);
        push_u32(&mut out, size); // compressed size
        push_u32(&mut out, size); // uncompressed size
        push_u16(
            &mut out,
            u16::try_from(name.len()).expect("short file name"),
        );
        push_u16(&mut out, 0); // extra length
        out.extend_from_slice(name.as_bytes());
        out.extend_from_slice(&body);

        // Central directory header.
        push_u32(&mut central, 0x0201_4b50);
        push_u16(&mut central, 20); // version made by
        push_u16(&mut central, 20); // version needed
        push_u16(&mut central, 0); // flags
        push_u16(&mut central, 0); // method: stored
        push_u16(&mut central, DOS_TIME);
        push_u16(&mut central, DOS_DATE);
        push_u32(&mut central, crc);
        push_u32(&mut central, size);
        push_u32(&mut central, size);
        push_u16(
            &mut central,
            u16::try_from(name.len()).expect("short file name"),
        );
        push_u16(&mut central, 0); // extra length
        push_u16(&mut central, 0); // comment length
        push_u16(&mut central, 0); // disk number start
        push_u16(&mut central, 0); // internal attributes
        push_u32(&mut central, 0); // external attributes
        push_u32(&mut central, local_offset);
        central.extend_from_slice(name.as_bytes());

        n_entries += 1;
    }

    let central_offset = u32::try_from(out.len()).expect("fixture archive fits in u32");
    let central_size = u32::try_from(central.len()).expect("fixture archive fits in u32");
    out.extend_from_slice(&central);

    // End of central directory.
    push_u32(&mut out, 0x0605_4b50);
    push_u16(&mut out, 0); // this disk
    push_u16(&mut out, 0); // disk with central directory
    push_u16(&mut out, n_entries);
    push_u16(&mut out, n_entries);
    push_u32(&mut out, central_size);
    push_u32(&mut out, central_offset);
    push_u16(&mut out, 0); // comment length

    std::fs::write(dest, &out).expect("write fixture zip");
}

// ---------------------------------------------------------------------
// Canonical timetable rendering
// ---------------------------------------------------------------------

/// Render every field of the compiled timetable that the loader is
/// responsible for, in a fixed order. Hashed into the digest constant
/// so a change nobody thought to assert on still fails the test.
fn canonical_render(tt: &Timetable) -> String {
    let mut s = String::new();
    writeln!(s, "stops {}", tt.n_stops()).unwrap();
    for (idx, stop) in tt.stops.iter().enumerate() {
        writeln!(
            s,
            "  stop {idx} id={} name={} lon={:.6} lat={:.6} parent={:?}",
            stop.id, stop.name, stop.lon, stop.lat, stop.parent_station
        )
        .unwrap();
    }

    let mut stations: Vec<_> = tt.station_children.iter().collect();
    stations.sort_by_key(|(parent, _)| **parent);
    writeln!(s, "stations {}", stations.len()).unwrap();
    for (parent, children) in stations {
        writeln!(s, "  station {parent} children={children:?}").unwrap();
    }

    writeln!(s, "routes {}", tt.n_routes()).unwrap();
    for r in 0..tt.n_routes() {
        let r = r as u32;
        let meta = &tt.route_meta[r as usize];
        writeln!(
            s,
            "  route {r} short={} long={} headsign={} stops={:?}",
            meta.short_name,
            meta.long_name,
            meta.headsign,
            tt.route_stops_slice(r)
        )
        .unwrap();
    }

    writeln!(s, "trips {}", tt.n_total_trips).unwrap();
    for (global, trip_id) in tt.trip_ids.iter().enumerate() {
        let (r, t) = tt.trip_to_route[global];
        write!(s, "  trip {global} id={trip_id} route={r} pos={t} times=").unwrap();
        for i in 0..tt.n_stops_on_route(r) as u32 {
            let st = tt.stop_time(r, t, i);
            write!(s, "({},{})", st.arrival, st.departure).unwrap();
        }
        writeln!(s).unwrap();
    }
    s
}

fn digest(tt: &Timetable) -> String {
    let render = canonical_render(tt);
    let mut hasher = Sha256::new();
    hasher.update(render.as_bytes());
    hex(&hasher.finalize())
}

fn hex(bytes: &[u8]) -> String {
    let mut s = String::with_capacity(bytes.len() * 2);
    for b in bytes {
        write!(s, "{b:02x}").unwrap();
    }
    s
}

// ---------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------

#[test]
fn fixture_weekday_parse_is_exact() {
    let tt = load_zip(&fixture_dir(), ServiceFilter::new(date(PLAIN_WEEKDAY)))
        .expect("fixture parses on a plain weekday");

    // Stops: every stop in the feed is registered, sorted by GTFS id,
    // regardless of the order they appear in stops.txt.
    let ids: Vec<&str> = tt.stops.iter().map(|s| &*s.id).collect();
    assert_eq!(ids, ["S1", "S1A", "S1B", "S2", "S3", "S4"]);
    assert_eq!(&*tt.stops[0].name, "Aurora Central");
    assert_eq!(&*tt.stops[5].name, "Draco Halt");
    assert_eq!(tt.stops[3].lon, 4.1);
    assert_eq!(tt.stops[3].lat, 50.1);

    // Parent stations: S1A and S1B hang off S1; S1's children list
    // includes the station itself.
    assert_eq!(tt.stops[0].parent_station, None);
    assert_eq!(tt.stops[1].parent_station, Some(0));
    assert_eq!(tt.stops[2].parent_station, Some(0));
    assert_eq!(tt.station_children.get(&0), Some(&vec![0, 1, 2]));
    assert_eq!(tt.station_children.len(), 1);

    // Services: only WEEKDAY runs, so the WEEKEND trip and the
    // exception-only trip are gone, and T5_SINGLE (one stop-time) is
    // dropped before it ever reaches the builder.
    assert_eq!(tt.n_total_trips, 2);
    assert_eq!(tt.n_routes(), 1);
    assert!(!tt.trip_id_to_idx.contains_key("T3_WEEKEND"));
    assert!(!tt.trip_id_to_idx.contains_key("T4_EXCEPTION"));
    assert!(!tt.trip_id_to_idx.contains_key("T5_SINGLE"));

    // Both surviving trips share a pattern, so they are one RAPTOR
    // route: S1A → S2 → S3 → S4.
    assert_eq!(tt.route_stops_slice(0), &[1, 3, 4, 5]);
    assert_eq!(tt.route_meta[0].short_name.as_ref(), "A1");
    assert_eq!(tt.route_meta[0].long_name.as_ref(), "Aurora - Draco");
    assert_eq!(tt.route_meta[0].headsign.as_ref(), "Draco Halt");

    // Trip order inside the route is by departure at the first stop,
    // not by the order trips.txt happens to list them.
    assert_eq!(tt.trip_ids, ["T1_EARLY", "T2_LATE"]);

    // T1_EARLY: 08:00/08:02, 08:15/08:15:30, then the two fallbacks —
    // the missing arrival at S3 takes the departure, the missing
    // departure at S4 takes the arrival.
    let early: Vec<(u32, u32)> = (0..4)
        .map(|i| {
            let st = tt.stop_time(0, 0, i);
            (st.arrival, st.departure)
        })
        .collect();
    assert_eq!(
        early,
        [
            (28800, 28920),
            (29700, 29730),
            (30600, 30600),
            (31500, 31500)
        ]
    );

    // T2_LATE: written out of stop_sequence order in the fixture, and
    // running past midnight — 24:41 and 25:12 must stay above 86400.
    let late: Vec<(u32, u32)> = (0..4)
        .map(|i| {
            let st = tt.stop_time(0, 1, i);
            (st.arrival, st.departure)
        })
        .collect();
    assert_eq!(
        late,
        [
            (85800, 85920),
            (87000, 87060),
            (88800, 88860),
            (90600, 90720)
        ]
    );

    // RAPTOR's boarding lookup agrees with the stored order.
    assert_eq!(tt.earliest_trip(0, 0, 0), Some(0));
    assert_eq!(tt.earliest_trip(0, 0, 28921), Some(1));
    assert_eq!(tt.earliest_trip(0, 0, 85921), None);

    assert_eq!(
        digest(&tt),
        WEEKDAY_TIMETABLE_DIGEST,
        "weekday timetable changed:\n{}",
        canonical_render(&tt)
    );
}

#[test]
fn calendar_exceptions_flip_the_active_set() {
    // 2024-07-03 is a Wednesday, so calendar.txt alone would run
    // WEEKDAY. calendar_dates.txt deletes it and adds NEVER instead.
    let tt = load_zip(&fixture_dir(), ServiceFilter::new(date(EXCEPTION_DAY)))
        .expect("fixture parses on the exception day");

    assert_eq!(tt.n_total_trips, 1);
    assert_eq!(tt.trip_ids, ["T4_EXCEPTION"]);
    assert_eq!(tt.n_routes(), 1);

    // A different pattern (S2 → S1B) than the weekday route.
    assert_eq!(tt.route_stops_slice(0), &[3, 2]);

    // R2 has no short or long name, and the trip has no headsign, so
    // the headsign falls back to the direction_id word.
    assert_eq!(tt.route_meta[0].short_name.as_ref(), "");
    assert_eq!(tt.route_meta[0].long_name.as_ref(), "");
    assert_eq!(tt.route_meta[0].headsign.as_ref(), "inbound");

    assert_eq!(tt.stop_time(0, 0, 0).departure, 25200);
    assert_eq!(tt.stop_time(0, 0, 1).arrival, 27000);
    assert_eq!(tt.stop_time(0, 0, 1).departure, 27060);

    // Every stop is still registered even though most see no trip.
    assert_eq!(tt.n_stops(), 6);
}

#[test]
fn a_never_running_service_yields_no_trips() {
    // A Saturday inside the calendar window: WEEKEND runs, and it has
    // exactly one trip (S1B → S2).
    let saturday = date((2024, 7, 6));
    let tt = load_zip(&fixture_dir(), ServiceFilter::new(saturday)).expect("weekend parse");
    assert_eq!(tt.trip_ids, ["T3_WEEKEND"]);
    assert_eq!(tt.route_stops_slice(0), &[2, 3]);

    // A date outside every calendar window: no service at all, but the
    // stops still load.
    let out_of_window = date((2025, 7, 2));
    let empty = load_zip(&fixture_dir(), ServiceFilter::new(out_of_window)).expect("empty parse");
    assert_eq!(empty.n_total_trips, 0);
    assert_eq!(empty.n_routes(), 0);
    assert_eq!(empty.n_stops(), 6);
}

#[test]
fn two_feeds_merge_without_colliding() {
    let sources = [
        FeedSource::namespaced(fixture_dir(), "alpha"),
        FeedSource::namespaced(fixture_dir(), "beta"),
    ];
    let tt = load_many(&sources, ServiceFilter::new(date(PLAIN_WEEKDAY))).expect("multi-feed load");

    assert_eq!(tt.n_stops(), 12);
    let ids: Vec<&str> = tt.stops.iter().map(|s| &*s.id).collect();
    assert_eq!(
        ids,
        [
            "alpha:S1",
            "alpha:S1A",
            "alpha:S1B",
            "alpha:S2",
            "alpha:S3",
            "alpha:S4",
            "beta:S1",
            "beta:S1A",
            "beta:S1B",
            "beta:S2",
            "beta:S3",
            "beta:S4",
        ]
    );

    // Two disjoint patterns, one per feed, four trips total.
    assert_eq!(tt.n_routes(), 2);
    assert_eq!(tt.n_total_trips, 4);
    assert_eq!(tt.route_stops_slice(0), &[1, 3, 4, 5]);
    assert_eq!(tt.route_stops_slice(1), &[7, 9, 10, 11]);
    assert_eq!(
        tt.trip_ids,
        [
            "alpha:T1_EARLY",
            "alpha:T2_LATE",
            "beta:T1_EARLY",
            "beta:T2_LATE"
        ]
    );

    // Parent stations stay inside their own feed.
    assert_eq!(tt.stops[1].parent_station, Some(0));
    assert_eq!(tt.stops[7].parent_station, Some(6));
}

#[test]
fn the_zip_path_parses_to_the_same_timetable() {
    let dir = tempfile::tempdir().expect("tempdir");
    let zip_path = dir.path().join("gtfs_mini.zip");
    write_fixture_zip(&zip_path);

    // The archive the loader reads is the one the hashing crate sees:
    // pin the feed sha256 the parser reports over the whole file.
    let raw = gtfs_structures::RawGtfs::from_path(&zip_path).expect("raw zip parse");
    assert_eq!(raw.sha256.as_deref(), Some(FIXTURE_ZIP_SHA256));

    let from_zip = load_zip(&zip_path, ServiceFilter::new(date(PLAIN_WEEKDAY))).expect("zip parse");
    let from_dir =
        load_zip(&fixture_dir(), ServiceFilter::new(date(PLAIN_WEEKDAY))).expect("dir parse");

    assert_eq!(
        canonical_render(&from_zip),
        canonical_render(&from_dir),
        "zip and directory parses disagree"
    );
    assert_eq!(digest(&from_zip), WEEKDAY_TIMETABLE_DIGEST);
}
