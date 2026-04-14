//! NeTEx-EPIP loader for butterfly-route (#101, STIB).
//!
//! Streams an EPIP NeTEx XML file and compiles it into the same
//! [`Timetable`] struct the GTFS loader produces, so every downstream
//! consumer (RAPTOR, transfer graph build, multi-feed merger, /transit
//! handler) stays format-blind.
//!
//! ## Scope of this MVP
//!
//! - Target shape: STIB's Belgian NAP publication
//!   (`https://belgianmobility.blob.core.windows.net/epip-production/epip-stibmivb-bmc-latest.xml`).
//!   Verified 2026-04-14: 722 MB, 11,757 ScheduledStopPoints, 1,994
//!   StopPlaces, 2,504 Quays, 90 Lines, 812 ServiceJourneyPatterns,
//!   110,045 ServiceJourneys, 2.2M TimetabledPassingTimes. See
//!   `route/tests/stib_epip_availability.rs` for the regression guard
//!   on those counts.
//! - Element types handled: `ScheduledStopPoint`, `StopPlace`, `Quay`,
//!   `PassengerStopAssignment`, `Line`, `ServiceJourneyPattern`,
//!   `StopPointInJourneyPattern`, `ServiceJourney`,
//!   `TimetabledPassingTime`.
//! - Coordinates: STIB publishes everything in **EPSG:2154**
//!   (Lambert-93, the French Réseau Géodésique). butterfly-route uses
//!   WGS84 everywhere, so we reproject via [`proj4rs`] (pure Rust, no
//!   system PROJ dependency).
//!
//! ## Known limitations
//!
//! - **Calendar filtering is not applied.** The STIB EPIP publication
//!   as fetched 2026-04-14 covers a three-week window from 2025-03.
//!   DayType / UicOperatingPeriod / ValidDayBits all exist in the file
//!   but the active window is 11+ months stale, so any date filter
//!   would drop every trip. The MVP loads every ServiceJourney
//!   unconditionally and logs a warning. A calendar remapping or a
//!   fresh publication upstream will lift the limitation without
//!   parser changes.
//! - **Fares, accessibility, bookings** are not parsed — out of scope
//!   for route planning.
//! - **Non-EPIP NeTEx profiles** (NeTEx-FR, MMTIS, general NeTEx) are
//!   not handled. Explicitly out of scope per #101.
//!
//! ## Cross-reference resolution
//!
//! A NeTEx EPIP file is a graph of cross-references, not a tree. The
//! loader builds four lookup tables during a single streaming pass and
//! resolves the references at the end:
//!
//! 1. `scheduled_stop_points: id → SSP record` (per-pattern stop with
//!    its own Lambert-93 `Location`).
//! 2. `stop_places: id → StopPlace record` (physical station with
//!    `Name`, transport mode, parent-site ref).
//! 3. `quays: id → Quay record` (platform-level stop with `Name`).
//! 4. `passenger_stop_assignments: SSP_id → (StopPlace_ref, Quay_ref)`
//!    — the bridge that gives each SSP a human name via its containing
//!    station/quay.
//!
//! Each `ServiceJourneyPattern` stores an ordered list of SSP refs.
//! Each `ServiceJourney` carries passing times referenced positionally
//! against its pattern's stop sequence. The TimetableBuilder is
//! populated at the end of the stream, once every record is in memory.
//!
//! Memory cost for STIB: ~100 MB of aggregated records while parsing
//! the 722 MB file, dropped after `TimetableBuilder::build()`.

use std::collections::{HashMap, HashSet};
use std::fs::File;
use std::io::BufReader;
use std::path::Path;

use anyhow::{Context, Result, bail};
use chrono::{Datelike, Local, NaiveDate, NaiveDateTime};
use quick_xml::Reader;
use quick_xml::events::Event;

use super::timetable::{StopIdx, StopTime, Timetable, TimetableBuilder};

/// Parse a STIB-shaped NeTEx-EPIP XML file into a [`Timetable`].
///
/// `feed_id` is the namespace prefix used for stop and trip ids in
/// the merged multi-feed `Timetable`; pass `Some("stib")` to match
/// the convention used by the GTFS loader.
pub fn load_epip_xml(path: &Path, feed_id: Option<&str>) -> Result<Timetable> {
    let mut builder = TimetableBuilder::new();
    load_into_builder(path, feed_id, &mut builder)?;
    builder
        .build()
        .context("building Timetable from NeTEx-EPIP")
}

/// Stream a NeTEx-EPIP file into an existing [`TimetableBuilder`]
/// without finalising it — the twin of
/// [`crate::transit::gtfs::load_into_builder`]. Used by
/// `transit::load_from_disk` to merge GTFS feeds and NeTEx-EPIP
/// feeds into one timetable.
pub fn load_into_builder(
    path: &Path,
    feed_id: Option<&str>,
    builder: &mut TimetableBuilder,
) -> Result<()> {
    tracing::info!(
        path = %path.display(),
        feed = feed_id.unwrap_or("<raw>"),
        "parsing NeTEx-EPIP XML"
    );
    let file = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    // Large read buffer — the file is 720 MB and we want to minimise
    // syscalls over the linear scan.
    let reader = BufReader::with_capacity(4 * 1024 * 1024, file);
    let mut xml = Reader::from_reader(reader);
    xml.config_mut().trim_text(true);

    let mut state = ParseState::default();
    let mut buf = Vec::with_capacity(4096);

    loop {
        match xml.read_event_into(&mut buf).context("reading EPIP XML event")? {
            Event::Start(ref e) => handle_start(&mut state, e, &mut xml)?,
            Event::Empty(ref e) => handle_empty(&mut state, e)?,
            Event::End(ref e) => handle_end(&mut state, e)?,
            Event::Text(ref t) => handle_text(&mut state, t)?,
            Event::Eof => break,
            _ => {}
        }
        buf.clear();
    }

    tracing::info!(
        stop_places = state.stop_places.len(),
        quays = state.quays.len(),
        scheduled_stop_points = state.scheduled_stop_points.len(),
        passenger_stop_assignments = state.passenger_stop_assignments.len(),
        lines = state.lines.len(),
        journey_patterns = state.journey_patterns.len(),
        service_journeys = state.service_journeys.len(),
        "NeTEx-EPIP parse complete; resolving into builder"
    );
    emit_into_builder(state, feed_id, builder)
}

// =====================================================================
// Parser state (collected during the streaming pass).
// =====================================================================

#[derive(Default)]
struct ParseState {
    // Raw element records.
    stop_places: HashMap<String, StopPlaceRec>,
    quays: HashMap<String, QuayRec>,
    scheduled_stop_points: HashMap<String, ScheduledStopPointRec>,
    passenger_stop_assignments: HashMap<String, PsaRec>,
    lines: HashMap<String, LineRec>,
    journey_patterns: HashMap<String, JourneyPatternRec>,
    service_journeys: Vec<ServiceJourneyRec>,
    /// Operating periods keyed by id (#101 calendar follow-up).
    operating_periods: HashMap<String, UicOperatingPeriodRec>,
    /// `(day_type_ref, operating_period_ref)` pairs from
    /// `<DayTypeAssignment>` (#101 calendar follow-up).
    day_type_assignments: Vec<(String, String)>,

    // Streaming cursor: we track the currently-open element stack so
    // that nested text events (<Name>, <gml:pos>, <ArrivalTime>, …)
    // can be routed to the right record.
    stack: Vec<ElementKind>,
    current_stop_place: Option<StopPlaceRec>,
    current_stop_place_id: Option<String>,
    current_quay: Option<QuayRec>,
    current_quay_id: Option<String>,
    current_ssp: Option<ScheduledStopPointRec>,
    current_ssp_id: Option<String>,
    current_psa_ssp_ref: Option<String>,
    current_psa_stop_place_ref: Option<String>,
    current_psa_quay_ref: Option<String>,
    current_psa_id: Option<String>,
    current_line: Option<LineRec>,
    current_line_id: Option<String>,
    current_jp: Option<JourneyPatternRec>,
    current_jp_id: Option<String>,
    current_jp_stop_point_ref: Option<String>,
    current_sj: Option<ServiceJourneyRec>,
    current_passing_time: Option<PassingTimeRec>,
    // Calendar parsing scratch (#101 follow-up).
    current_op_id: Option<String>,
    current_op: Option<UicOperatingPeriodRec>,
    current_dta_day_type_ref: Option<String>,
    current_dta_op_ref: Option<String>,
    // Pending text destination — when we see a `<Name>`, `<gml:pos>`,
    // `<ArrivalTime>`, etc., this tells the text handler which field
    // to populate on the current record.
    text_target: TextTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ElementKind {
    StopPlace,
    Quay,
    ScheduledStopPoint,
    PassengerStopAssignment,
    Line,
    JourneyPattern,
    StopPointInJourneyPattern,
    ServiceJourney,
    TimetabledPassingTime,
    UicOperatingPeriod,
    DayTypeAssignment,
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
enum TextTarget {
    #[default]
    None,
    StopPlaceName,
    QuayName,
    SspLocation,
    LineName,
    LinePublicCode,
    LineTransportMode,
    PtArrivalTime,
    PtDepartureTime,
    PtArrivalDayOffset,
    PtDepartureDayOffset,
    OpFromDate,
    OpToDate,
    OpValidDayBits,
}

#[derive(Debug, Default, Clone)]
struct StopPlaceRec {
    name: Option<String>,
    parent_site_ref: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct QuayRec {
    name: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct ScheduledStopPointRec {
    /// Raw Lambert-93 coordinates from `<gml:pos>` (easting, northing).
    lambert: Option<(f64, f64)>,
}

#[derive(Debug, Default, Clone)]
struct PsaRec {
    stop_place_ref: Option<String>,
    quay_ref: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct LineRec {
    name: String,
    public_code: String,
    transport_mode: String,
}

#[derive(Debug, Default, Clone)]
struct JourneyPatternRec {
    /// Ordered list of `ScheduledStopPoint` ids this pattern visits,
    /// in the sequence given by the nested `<StopPointInJourneyPattern>`
    /// elements' document order (NeTEx also stamps an `order` attribute
    /// but document order is the canonical ordering in EPIP).
    stop_point_refs: Vec<String>,
    /// `<RouteRef ref="FR:Route:..."/>` from this pattern. Used to
    /// resolve to a `Line` via id stem matching (the EPIP file has no
    /// standalone `Route` declarations — the route id maps directly
    /// to a `Line` id with the same stem, plus an optional `_R`
    /// suffix for the reverse direction).
    route_ref: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct ServiceJourneyRec {
    pattern_ref: Option<String>,
    /// `<dayTypes>` references — one ServiceJourney can carry several
    /// DayTypeRefs and is active when *any* of them resolves to a
    /// running calendar day. Empty list = unknown / always active
    /// (we treat this as active to avoid silently dropping trips).
    day_type_refs: Vec<String>,
    // `passingTimes` ordered list. Each entry is a single stop on the
    // journey, matched positionally to `journey_patterns[pattern_ref].stop_point_refs[i]`.
    passing_times: Vec<PassingTimeRec>,
}

#[derive(Debug, Default, Clone)]
struct UicOperatingPeriodRec {
    /// `FromDate` text, ISO-8601 e.g. `2025-03-07T00:00:00+00:00`.
    from_date: Option<String>,
    /// `ToDate` text.
    to_date: Option<String>,
    /// `ValidDayBits` text — a string of `'0'` and `'1'` characters
    /// where each char represents one day starting at `FromDate`,
    /// `'1'` meaning "active that day".
    valid_day_bits: Option<String>,
}

#[derive(Debug, Default, Clone)]
struct PassingTimeRec {
    arrival: Option<u32>,   // seconds since midnight, with day offset applied
    departure: Option<u32>, // ditto
    arrival_day_offset: u32,
    departure_day_offset: u32,
    // Temporary text buffers — the day offset element can appear
    // before the time element, so we finalise at end-of-element.
    raw_arrival: Option<String>,
    raw_departure: Option<String>,
}

// =====================================================================
// Streaming event handlers.
// =====================================================================

fn handle_start(
    state: &mut ParseState,
    e: &quick_xml::events::BytesStart<'_>,
    _xml: &mut Reader<BufReader<File>>,
) -> Result<()> {
    let qn = e.name();
    let name = element_local_name(qn.as_ref());
    match name {
        b"StopPlace" => {
            let id = attr(e, b"id").unwrap_or_default();
            state.current_stop_place = Some(StopPlaceRec::default());
            state.current_stop_place_id = Some(id);
            state.stack.push(ElementKind::StopPlace);
        }
        b"Quay" => {
            let id = attr(e, b"id").unwrap_or_default();
            state.current_quay = Some(QuayRec::default());
            state.current_quay_id = Some(id);
            state.stack.push(ElementKind::Quay);
        }
        b"ScheduledStopPoint" => {
            let id = attr(e, b"id").unwrap_or_default();
            state.current_ssp = Some(ScheduledStopPointRec::default());
            state.current_ssp_id = Some(id);
            state.stack.push(ElementKind::ScheduledStopPoint);
        }
        b"PassengerStopAssignment" => {
            let id = attr(e, b"id").unwrap_or_default();
            state.current_psa_id = Some(id);
            state.current_psa_ssp_ref = None;
            state.current_psa_stop_place_ref = None;
            state.current_psa_quay_ref = None;
            state.stack.push(ElementKind::PassengerStopAssignment);
        }
        b"Line" => {
            let id = attr(e, b"id").unwrap_or_default();
            state.current_line = Some(LineRec::default());
            state.current_line_id = Some(id);
            state.stack.push(ElementKind::Line);
        }
        b"ServiceJourneyPattern" => {
            let id = attr(e, b"id").unwrap_or_default();
            state.current_jp = Some(JourneyPatternRec::default());
            state.current_jp_id = Some(id);
            state.stack.push(ElementKind::JourneyPattern);
        }
        b"StopPointInJourneyPattern" => {
            state.current_jp_stop_point_ref = None;
            state.stack.push(ElementKind::StopPointInJourneyPattern);
        }
        b"ServiceJourney" => {
            state.current_sj = Some(ServiceJourneyRec::default());
            state.stack.push(ElementKind::ServiceJourney);
        }
        b"TimetabledPassingTime" => {
            state.current_passing_time = Some(PassingTimeRec::default());
            state.stack.push(ElementKind::TimetabledPassingTime);
        }
        b"UicOperatingPeriod" => {
            let id = attr(e, b"id").unwrap_or_default();
            state.current_op_id = Some(id);
            state.current_op = Some(UicOperatingPeriodRec::default());
            state.stack.push(ElementKind::UicOperatingPeriod);
        }
        b"DayTypeAssignment" => {
            state.current_dta_day_type_ref = None;
            state.current_dta_op_ref = None;
            state.stack.push(ElementKind::DayTypeAssignment);
        }
        b"FromDate" => {
            state.text_target = if state.stack.last() == Some(&ElementKind::UicOperatingPeriod) {
                TextTarget::OpFromDate
            } else {
                TextTarget::None
            };
        }
        b"ToDate" => {
            state.text_target = if state.stack.last() == Some(&ElementKind::UicOperatingPeriod) {
                TextTarget::OpToDate
            } else {
                TextTarget::None
            };
        }
        b"ValidDayBits" => {
            state.text_target = if state.stack.last() == Some(&ElementKind::UicOperatingPeriod) {
                TextTarget::OpValidDayBits
            } else {
                TextTarget::None
            };
        }
        b"Name" => {
            // Which record to attach this Name to depends on the
            // currently-open element.
            state.text_target = match state.stack.last() {
                Some(ElementKind::StopPlace) => TextTarget::StopPlaceName,
                Some(ElementKind::Quay) => TextTarget::QuayName,
                Some(ElementKind::Line) => TextTarget::LineName,
                _ => TextTarget::None,
            };
        }
        b"pos" => {
            // <gml:pos> — only meaningful inside a ScheduledStopPoint's
            // Location; we ignore it for StopPlace / Quay because we
            // pull StopPlace coordinates via the SSP chain.
            if state.stack.last() == Some(&ElementKind::ScheduledStopPoint) {
                state.text_target = TextTarget::SspLocation;
            } else {
                state.text_target = TextTarget::None;
            }
        }
        b"PublicCode" => {
            state.text_target = if state.stack.last() == Some(&ElementKind::Line) {
                TextTarget::LinePublicCode
            } else {
                TextTarget::None
            };
        }
        b"TransportMode" => {
            state.text_target = if state.stack.last() == Some(&ElementKind::Line) {
                TextTarget::LineTransportMode
            } else {
                TextTarget::None
            };
        }
        b"ArrivalTime" => state.text_target = TextTarget::PtArrivalTime,
        b"DepartureTime" => state.text_target = TextTarget::PtDepartureTime,
        b"ArrivalDayOffset" => state.text_target = TextTarget::PtArrivalDayOffset,
        b"DepartureDayOffset" => state.text_target = TextTarget::PtDepartureDayOffset,
        // Reference elements: STIB's EPIP file emits these as
        // `<Tag ref="..."></Tag>` (non-empty `Start` + `End`), not
        // `<Tag ref="..." />` (self-closing `Empty`). Capture the
        // `ref` attribute here as well as in `handle_empty` so we
        // don't lose it when the source uses the verbose shape.
        b"ParentSiteRef" => {
            if state.stack.last() == Some(&ElementKind::StopPlace) {
                if let Some(sp) = state.current_stop_place.as_mut() {
                    sp.parent_site_ref = attr(e, b"ref");
                }
            }
        }
        b"ScheduledStopPointRef" => {
            match state.stack.last() {
                Some(ElementKind::StopPointInJourneyPattern) => {
                    let r = attr(e, b"ref");
                    if r.is_some() {
                        state.current_jp_stop_point_ref = r;
                    }
                }
                Some(ElementKind::PassengerStopAssignment) => {
                    state.current_psa_ssp_ref = attr(e, b"ref");
                }
                _ => {}
            }
        }
        b"StopPlaceRef" => {
            if state.stack.last() == Some(&ElementKind::PassengerStopAssignment) {
                state.current_psa_stop_place_ref = attr(e, b"ref");
            }
        }
        b"QuayRef" => {
            if state.stack.last() == Some(&ElementKind::PassengerStopAssignment) {
                state.current_psa_quay_ref = attr(e, b"ref");
            }
        }
        b"ServiceJourneyPatternRef" => {
            if state.stack.last() == Some(&ElementKind::ServiceJourney) {
                if let Some(sj) = state.current_sj.as_mut() {
                    sj.pattern_ref = attr(e, b"ref");
                }
            }
        }
        b"RouteRef" => {
            if state.stack.last() == Some(&ElementKind::JourneyPattern) {
                if let Some(jp) = state.current_jp.as_mut() {
                    jp.route_ref = attr(e, b"ref");
                }
            }
        }
        b"DayTypeRef" => {
            match state.stack.last() {
                Some(ElementKind::ServiceJourney) => {
                    if let Some(r) = attr(e, b"ref") {
                        if let Some(sj) = state.current_sj.as_mut() {
                            sj.day_type_refs.push(r);
                        }
                    }
                }
                Some(ElementKind::DayTypeAssignment) => {
                    state.current_dta_day_type_ref = attr(e, b"ref");
                }
                _ => {}
            }
        }
        b"OperatingPeriodRef" => {
            if state.stack.last() == Some(&ElementKind::DayTypeAssignment) {
                state.current_dta_op_ref = attr(e, b"ref");
            }
        }
        _ => {}
    }
    Ok(())
}

fn handle_empty(
    state: &mut ParseState,
    e: &quick_xml::events::BytesStart<'_>,
) -> Result<()> {
    let qn = e.name();
    let name = element_local_name(qn.as_ref());
    match name {
        b"ParentSiteRef" => {
            if state.stack.last() == Some(&ElementKind::StopPlace) {
                if let Some(sp) = state.current_stop_place.as_mut() {
                    sp.parent_site_ref = attr(e, b"ref");
                }
            }
        }
        b"ScheduledStopPointRef" => {
            match state.stack.last() {
                Some(ElementKind::StopPointInJourneyPattern) => {
                    let r = attr(e, b"ref");
                    if r.is_some() {
                        state.current_jp_stop_point_ref = r;
                    }
                }
                Some(ElementKind::PassengerStopAssignment) => {
                    state.current_psa_ssp_ref = attr(e, b"ref");
                }
                _ => {}
            }
        }
        b"StopPlaceRef" => {
            if state.stack.last() == Some(&ElementKind::PassengerStopAssignment) {
                state.current_psa_stop_place_ref = attr(e, b"ref");
            }
        }
        b"QuayRef" => {
            if state.stack.last() == Some(&ElementKind::PassengerStopAssignment) {
                state.current_psa_quay_ref = attr(e, b"ref");
            }
        }
        b"ServiceJourneyPatternRef" => {
            if state.stack.last() == Some(&ElementKind::ServiceJourney) {
                if let Some(sj) = state.current_sj.as_mut() {
                    sj.pattern_ref = attr(e, b"ref");
                }
            }
        }
        b"StopPointInJourneyPatternRef" => {
            // Positional ref inside a <TimetabledPassingTime>. We
            // don't need to capture the ref value — the positional
            // matching is implicit via the document order of the
            // sibling <TimetabledPassingTime> elements.
        }
        b"RouteRef" => {
            if state.stack.last() == Some(&ElementKind::JourneyPattern) {
                if let Some(jp) = state.current_jp.as_mut() {
                    jp.route_ref = attr(e, b"ref");
                }
            }
        }
        b"DayTypeRef" => {
            match state.stack.last() {
                Some(ElementKind::ServiceJourney) => {
                    if let Some(r) = attr(e, b"ref") {
                        if let Some(sj) = state.current_sj.as_mut() {
                            sj.day_type_refs.push(r);
                        }
                    }
                }
                Some(ElementKind::DayTypeAssignment) => {
                    state.current_dta_day_type_ref = attr(e, b"ref");
                }
                _ => {}
            }
        }
        b"OperatingPeriodRef" => {
            if state.stack.last() == Some(&ElementKind::DayTypeAssignment) {
                state.current_dta_op_ref = attr(e, b"ref");
            }
        }
        _ => {}
    }
    Ok(())
}

fn handle_end(
    state: &mut ParseState,
    e: &quick_xml::events::BytesEnd<'_>,
) -> Result<()> {
    let qn = e.name();
    let name = element_local_name(qn.as_ref());
    match name {
        b"StopPlace" => {
            if let (Some(id), Some(rec)) = (
                state.current_stop_place_id.take(),
                state.current_stop_place.take(),
            ) {
                state.stop_places.insert(id, rec);
            }
            pop_stack(state, ElementKind::StopPlace);
        }
        b"Quay" => {
            if let (Some(id), Some(rec)) = (state.current_quay_id.take(), state.current_quay.take()) {
                state.quays.insert(id, rec);
            }
            pop_stack(state, ElementKind::Quay);
        }
        b"ScheduledStopPoint" => {
            if let (Some(id), Some(rec)) = (state.current_ssp_id.take(), state.current_ssp.take())
            {
                state.scheduled_stop_points.insert(id, rec);
            }
            pop_stack(state, ElementKind::ScheduledStopPoint);
        }
        b"PassengerStopAssignment" => {
            if let (Some(_psa_id), Some(ssp)) =
                (state.current_psa_id.take(), state.current_psa_ssp_ref.take())
            {
                let stop_place_ref = state.current_psa_stop_place_ref.take();
                let quay_ref = state.current_psa_quay_ref.take();
                state.passenger_stop_assignments.insert(
                    ssp,
                    PsaRec {
                        stop_place_ref,
                        quay_ref,
                    },
                );
            }
            pop_stack(state, ElementKind::PassengerStopAssignment);
        }
        b"Line" => {
            if let (Some(id), Some(rec)) = (state.current_line_id.take(), state.current_line.take())
            {
                state.lines.insert(id, rec);
            }
            pop_stack(state, ElementKind::Line);
        }
        b"ServiceJourneyPattern" => {
            if let (Some(id), Some(rec)) = (state.current_jp_id.take(), state.current_jp.take())
            {
                state.journey_patterns.insert(id, rec);
            }
            pop_stack(state, ElementKind::JourneyPattern);
        }
        b"StopPointInJourneyPattern" => {
            if let Some(ref_) = state.current_jp_stop_point_ref.take() {
                if let Some(jp) = state.current_jp.as_mut() {
                    jp.stop_point_refs.push(ref_);
                }
            }
            pop_stack(state, ElementKind::StopPointInJourneyPattern);
        }
        b"ServiceJourney" => {
            if let Some(sj) = state.current_sj.take() {
                if sj.pattern_ref.is_some() && !sj.passing_times.is_empty() {
                    state.service_journeys.push(sj);
                }
            }
            pop_stack(state, ElementKind::ServiceJourney);
        }
        b"TimetabledPassingTime" => {
            if let Some(mut pt) = state.current_passing_time.take() {
                finalise_passing_time(&mut pt);
                if let Some(sj) = state.current_sj.as_mut() {
                    sj.passing_times.push(pt);
                }
            }
            pop_stack(state, ElementKind::TimetabledPassingTime);
        }
        b"UicOperatingPeriod" => {
            if let (Some(id), Some(rec)) = (state.current_op_id.take(), state.current_op.take()) {
                state.operating_periods.insert(id, rec);
            }
            pop_stack(state, ElementKind::UicOperatingPeriod);
        }
        b"DayTypeAssignment" => {
            if let (Some(dt), Some(op)) = (
                state.current_dta_day_type_ref.take(),
                state.current_dta_op_ref.take(),
            ) {
                state.day_type_assignments.push((dt, op));
            }
            pop_stack(state, ElementKind::DayTypeAssignment);
        }
        _ => {}
    }
    Ok(())
}

fn handle_text(state: &mut ParseState, t: &quick_xml::events::BytesText<'_>) -> Result<()> {
    if state.text_target == TextTarget::None {
        return Ok(());
    }
    let s = t
        .decode()
        .context("decoding text in NeTEx-EPIP")?
        .into_owned();
    match state.text_target {
        TextTarget::StopPlaceName => {
            if let Some(sp) = state.current_stop_place.as_mut() {
                sp.name = Some(s);
            }
        }
        TextTarget::QuayName => {
            if let Some(q) = state.current_quay.as_mut() {
                q.name = Some(s);
            }
        }
        TextTarget::SspLocation => {
            let mut parts = s.split_whitespace();
            let x = parts.next().and_then(|p| p.parse::<f64>().ok());
            let y = parts.next().and_then(|p| p.parse::<f64>().ok());
            if let (Some(x), Some(y)) = (x, y) {
                if let Some(ssp) = state.current_ssp.as_mut() {
                    ssp.lambert = Some((x, y));
                }
            }
        }
        TextTarget::LineName => {
            if let Some(line) = state.current_line.as_mut() {
                line.name = s;
            }
        }
        TextTarget::LinePublicCode => {
            if let Some(line) = state.current_line.as_mut() {
                line.public_code = s;
            }
        }
        TextTarget::LineTransportMode => {
            if let Some(line) = state.current_line.as_mut() {
                line.transport_mode = s;
            }
        }
        TextTarget::PtArrivalTime => {
            if let Some(pt) = state.current_passing_time.as_mut() {
                pt.raw_arrival = Some(s);
            }
        }
        TextTarget::PtDepartureTime => {
            if let Some(pt) = state.current_passing_time.as_mut() {
                pt.raw_departure = Some(s);
            }
        }
        TextTarget::PtArrivalDayOffset => {
            if let Some(pt) = state.current_passing_time.as_mut() {
                pt.arrival_day_offset = s.trim().parse().unwrap_or(0);
            }
        }
        TextTarget::PtDepartureDayOffset => {
            if let Some(pt) = state.current_passing_time.as_mut() {
                pt.departure_day_offset = s.trim().parse().unwrap_or(0);
            }
        }
        TextTarget::OpFromDate => {
            if let Some(op) = state.current_op.as_mut() {
                op.from_date = Some(s);
            }
        }
        TextTarget::OpToDate => {
            if let Some(op) = state.current_op.as_mut() {
                op.to_date = Some(s);
            }
        }
        TextTarget::OpValidDayBits => {
            if let Some(op) = state.current_op.as_mut() {
                op.valid_day_bits = Some(s);
            }
        }
        TextTarget::None => {}
    }
    state.text_target = TextTarget::None;
    Ok(())
}

fn pop_stack(state: &mut ParseState, expected: ElementKind) {
    if state.stack.last() == Some(&expected) {
        state.stack.pop();
    }
}

fn element_local_name(qualified: &[u8]) -> &[u8] {
    if let Some(pos) = qualified.iter().position(|&b| b == b':') {
        &qualified[pos + 1..]
    } else {
        qualified
    }
}

fn attr(e: &quick_xml::events::BytesStart<'_>, key: &[u8]) -> Option<String> {
    for a in e.attributes().flatten() {
        if a.key.as_ref() == key {
            return Some(String::from_utf8_lossy(&a.value).into_owned());
        }
    }
    None
}

fn finalise_passing_time(pt: &mut PassingTimeRec) {
    if let Some(raw) = pt.raw_arrival.take() {
        pt.arrival = parse_hms(&raw).map(|s| s + pt.arrival_day_offset * 86_400);
    }
    if let Some(raw) = pt.raw_departure.take() {
        pt.departure = parse_hms(&raw).map(|s| s + pt.departure_day_offset * 86_400);
    }
}

/// Parse `HH:MM:SS` → seconds since midnight. Returns `None` on
/// malformed input.
fn parse_hms(s: &str) -> Option<u32> {
    let mut parts = s.trim().split(':');
    let h: u32 = parts.next()?.parse().ok()?;
    let m: u32 = parts.next()?.parse().ok()?;
    let sec: u32 = parts.next().unwrap_or("0").parse().ok().unwrap_or(0);
    Some(h * 3600 + m * 60 + sec)
}

// =====================================================================
// Cross-reference resolution & TimetableBuilder emission.
// =====================================================================

fn emit_into_builder(
    state: ParseState,
    feed_id: Option<&str>,
    builder: &mut TimetableBuilder,
) -> Result<()> {
    // Reprojection pipeline: Lambert-93 (EPSG:2154) → WGS84 lon/lat.
    // proj4rs returns radians for geographic CRSes, so we convert to
    // degrees at the tail. The source and destination Proj objects
    // are created once and reused across every SSP conversion.
    let lambert = proj4rs::Proj::from_proj_string(
        "+proj=lcc +lat_0=46.5 +lon_0=3 +lat_1=49 +lat_2=44 \
         +x_0=700000 +y_0=6600000 +ellps=GRS80 +units=m +no_defs +type=crs",
    )
    .context("building Lambert-93 projection")?;
    let wgs84 = proj4rs::Proj::from_proj_string(
        "+proj=longlat +datum=WGS84 +no_defs +type=crs",
    )
    .context("building WGS84 projection")?;

    let namespaced = |raw: &str| -> String {
        match feed_id {
            Some(p) => format!("{p}:{raw}"),
            None => raw.to_string(),
        }
    };

    // Pass 1: register every ScheduledStopPoint as a stop in the
    // TimetableBuilder.
    //
    // Critical optimization: STIB's EPIP file emits **per-pattern** SSPs.
    // A single physical Brussels stop served by 5 lines has 5+ SSP
    // entries, all at the exact same Lambert-93 coordinates. If we
    // registered each SSP as its own TimetableBuilder stop, the merged
    // multi-feed timetable would have ~76k physical stops with many
    // hundreds of duplicate-coordinate stops at major hubs — and the
    // transfer build's 2 km neighbour grid would degenerate into a
    // quadratic blowup at every dense cell.
    //
    // Instead, deduplicate by quantised (lon, lat). Two SSPs whose
    // reprojected coordinates round to the same 6-decimal cell (~11 cm)
    // collapse to a single TimetableBuilder stop. The
    // `ssp_id_to_idx` table then maps every duplicate SSP id to that
    // single StopIdx, so the per-trip pattern resolution still works
    // (every pattern that visits this SSP threads through the same
    // physical stop in the timetable).
    //
    // Effect on STIB: 11,757 raw SSPs → ~3,500–4,000 physical stops
    // after dedup, matching the order of `Quay` (2,504) and
    // `StopPlace` (1,994) counts in the source file.
    let mut ssp_id_to_idx: HashMap<String, u32> = HashMap::new();
    let mut coord_to_idx: HashMap<(i64, i64), u32> = HashMap::new();
    let mut n_unresolved = 0usize;

    for (ssp_id, ssp_rec) in &state.scheduled_stop_points {
        let Some((x, y)) = ssp_rec.lambert else {
            n_unresolved += 1;
            continue;
        };
        let mut coord = (x, y, 0.0_f64);
        proj4rs::transform::transform(&lambert, &wgs84, &mut coord)
            .with_context(|| format!("reprojecting SSP {ssp_id}"))?;
        let lon = coord.0.to_degrees();
        let lat = coord.1.to_degrees();

        // Quantise to ~11 cm. Two SSPs that land within this cell
        // share a single TimetableBuilder stop.
        let key = (
            (lon * 1_000_000.0).round() as i64,
            (lat * 1_000_000.0).round() as i64,
        );
        let idx = if let Some(&existing) = coord_to_idx.get(&key) {
            existing
        } else {
            let name = resolve_stop_name(&state, ssp_id);
            // The TimetableBuilder id is the namespaced SSP id of the
            // *first* SSP at this coordinate; any subsequent
            // duplicates fold into the same StopIdx via ssp_id_to_idx
            // (so trip-time resolution still works for every SSP id).
            let ns_id = namespaced(ssp_id);
            let new_idx = builder.add_stop(&ns_id, &name, lon, lat, None);
            coord_to_idx.insert(key, new_idx);
            new_idx
        };
        ssp_id_to_idx.insert(ssp_id.clone(), idx);
    }

    if n_unresolved > 0 {
        tracing::warn!(
            n_unresolved,
            "NeTEx-EPIP: {n_unresolved} ScheduledStopPoints have no Location — skipped"
        );
    }
    tracing::info!(
        ssps = ssp_id_to_idx.len(),
        physical_stops = coord_to_idx.len(),
        dedup_ratio = format!(
            "{:.1}x",
            ssp_id_to_idx.len() as f64 / coord_to_idx.len().max(1) as f64
        ),
        "NeTEx-EPIP: deduplicated per-pattern SSPs to physical stops"
    );

    // Pass 1b: parent station hierarchy. Walk every SSP we registered
    // and resolve its parent StopPlace's `ParentSiteRef`. If the
    // umbrella parent has its own representative SSP in the
    // timetable (often the case when one of the umbrella's child
    // stops shares the same coordinates), wire `parent_station` to
    // that StopIdx. Otherwise, leave it None — #112's same-station
    // bridges still cover most of the practical "transfer at the
    // same station" benefit via the foot CCH.
    //
    // We also build a `stop_place_idx_table` keyed by StopPlace id
    // so two SSPs that resolve to the same umbrella StopPlace land
    // in a shared `parent_station`. The umbrella StopPlace itself
    // is mapped to *one* of its child SSPs' StopIdx (the first one
    // the iteration encounters).
    let mut stop_place_to_idx: HashMap<&str, StopIdx> = HashMap::new();
    for (ssp_id, &stop_idx) in &ssp_id_to_idx {
        if let Some(psa) = state.passenger_stop_assignments.get(ssp_id) {
            if let Some(spref) = psa.stop_place_ref.as_deref() {
                stop_place_to_idx.entry(spref).or_insert(stop_idx);
                if let Some(sp) = state.stop_places.get(spref) {
                    if let Some(parent_ref) = sp.parent_site_ref.as_deref() {
                        stop_place_to_idx.entry(parent_ref).or_insert(stop_idx);
                    }
                }
            }
        }
    }
    let mut n_parents_set = 0usize;
    for (ssp_id, &stop_idx) in &ssp_id_to_idx {
        if let Some(psa) = state.passenger_stop_assignments.get(ssp_id) {
            if let Some(spref) = psa.stop_place_ref.as_deref() {
                if let Some(sp) = state.stop_places.get(spref) {
                    if let Some(parent_ref) = sp.parent_site_ref.as_deref() {
                        if let Some(&parent_idx) = stop_place_to_idx.get(parent_ref) {
                            if parent_idx != stop_idx {
                                builder.set_parent_station(stop_idx, parent_idx);
                                n_parents_set += 1;
                                continue;
                            }
                        }
                    }
                    // No grandparent — but the immediate StopPlace
                    // itself still acts as the umbrella for its
                    // platform-level Quays. Use it as the parent
                    // when it differs from this SSP's own StopIdx.
                    if let Some(&umbrella_idx) = stop_place_to_idx.get(spref) {
                        if umbrella_idx != stop_idx {
                            builder.set_parent_station(stop_idx, umbrella_idx);
                            n_parents_set += 1;
                        }
                    }
                }
            }
        }
    }
    tracing::info!(
        n_parents_set,
        "NeTEx-EPIP: wired parent_station via StopPlace.ParentSiteRef hierarchy"
    );

    // Resolve calendar (#101 follow-up): build the set of DayType ids
    // that are active for "today". When the publication is stale
    // (the case for STIB whose 2025-03 file is loaded in 2026-04),
    // remap today to the same weekday in the latest covered period
    // so we still load a coherent weekday/weekend slice instead of
    // dropping every trip.
    let active_day_types = compute_active_day_types(&state);
    let calendar_active = !active_day_types.is_empty();
    if !calendar_active {
        tracing::warn!(
            "NeTEx-EPIP: no DayTypes active for today and no stale-window remap \
             succeeded — falling back to loading every ServiceJourney unconditionally"
        );
    } else {
        tracing::info!(
            n_active_day_types = active_day_types.len(),
            n_total_day_types = state.day_type_assignments.len(),
            "NeTEx-EPIP: calendar resolved for service date"
        );
    }

    // Resolve Pattern → Line metadata. STIB's EPIP file has no
    // standalone `<Route>` declarations — the RouteRef id maps
    // directly to a Line id with the same stem (with an optional
    // `_R` suffix for the reverse direction). We compute that
    // mapping once so each ServiceJourney can pull the line's
    // public_code + name into the RAPTOR RouteMeta short / long
    // name fields.

    // Pass 2: emit every ServiceJourney as a trip via the builder.
    // The pattern_ref resolves to a JourneyPatternRec whose
    // stop_point_refs list is the canonical SSP sequence. The
    // journey's passing_times match positionally against that list.
    let mut n_trips_ok = 0usize;
    let mut n_trips_skipped_no_pattern = 0usize;
    let mut n_trips_skipped_mismatch = 0usize;
    let mut n_trips_skipped_unknown_stop = 0usize;
    let mut n_trips_skipped_calendar = 0usize;

    for (sj_idx, sj) in state.service_journeys.iter().enumerate() {
        // Calendar filter — only when the calendar resolution
        // produced a non-empty active set. Otherwise we load all
        // journeys (the stale-publication fallback).
        if calendar_active
            && !sj.day_type_refs.is_empty()
            && !sj
                .day_type_refs
                .iter()
                .any(|d| active_day_types.contains(d.as_str()))
        {
            n_trips_skipped_calendar += 1;
            continue;
        }
        let Some(pattern_ref) = sj.pattern_ref.as_deref() else {
            n_trips_skipped_no_pattern += 1;
            continue;
        };
        let Some(pattern) = state.journey_patterns.get(pattern_ref) else {
            n_trips_skipped_no_pattern += 1;
            continue;
        };
        if pattern.stop_point_refs.len() != sj.passing_times.len() {
            n_trips_skipped_mismatch += 1;
            continue;
        }
        // Resolve every SSP ref to a StopIdx in the builder.
        let mut pattern_idxs: Vec<u32> = Vec::with_capacity(pattern.stop_point_refs.len());
        let mut stop_times: Vec<StopTime> = Vec::with_capacity(pattern.stop_point_refs.len());
        let mut any_unknown = false;
        for (i, ssp_ref) in pattern.stop_point_refs.iter().enumerate() {
            let Some(&stop_idx) = ssp_id_to_idx.get(ssp_ref) else {
                any_unknown = true;
                break;
            };
            pattern_idxs.push(stop_idx);
            let pt = &sj.passing_times[i];
            // Fall back: if arrival is missing, use departure; vice
            // versa. If both are missing, synthesise a 0-duration
            // stop time at a propagating value. The real file always
            // has both, but we stay defensive.
            let arrival = pt.arrival.or(pt.departure).unwrap_or(0);
            let departure = pt.departure.or(pt.arrival).unwrap_or(arrival);
            stop_times.push(StopTime { arrival, departure });
        }
        if any_unknown {
            n_trips_skipped_unknown_stop += 1;
            continue;
        }
        // Need ≥ 2 stops for RAPTOR to consider the pattern a route.
        if pattern_idxs.len() < 2 {
            continue;
        }
        let trip_id = namespaced(&format!("sj{sj_idx}"));
        let (short_name, long_name) = resolve_pattern_line_meta(&state, pattern);
        builder.add_trip(&trip_id, &short_name, &long_name, "", pattern_idxs, stop_times);
        n_trips_ok += 1;
    }

    tracing::info!(
        trips_ok = n_trips_ok,
        trips_skipped_no_pattern = n_trips_skipped_no_pattern,
        trips_skipped_mismatch = n_trips_skipped_mismatch,
        trips_skipped_unknown_stop = n_trips_skipped_unknown_stop,
        trips_skipped_calendar = n_trips_skipped_calendar,
        calendar_active,
        "NeTEx-EPIP: service journeys emitted"
    );
    if n_trips_ok == 0 {
        bail!("NeTEx-EPIP: zero service journeys passed validation — file is malformed or schema changed");
    }
    Ok(())
}

/// Resolve the active DayType set for today, with a stale-publication
/// fallback. STIB's EPIP file is republished monthly but the active
/// window may be ~weeks behind the current calendar date, so a strict
/// "today's date in this period" filter would drop every trip on most
/// days. The fallback strategy: if today's date isn't covered by any
/// `UicOperatingPeriod`, find the latest period whose ToDate is in the
/// past and remap today to the **same weekday** inside that period.
/// This preserves the weekday/weekend semantics the user actually cares
/// about ("does the Tuesday timetable run today?") while still
/// producing a coherent slice of trips.
///
/// Returns an empty set on any failure (the caller logs a warning and
/// loads every trip unconditionally as the ultimate fallback).
fn compute_active_day_types(state: &ParseState) -> HashSet<&str> {
    let today = Local::now().date_naive();
    let active = active_day_types_for_date(state, today);
    if !active.is_empty() {
        return active;
    }
    // Stale publication — find the latest period and remap by weekday.
    let Some(remap_date) = remap_to_published_window(state, today) else {
        return HashSet::new();
    };
    tracing::info!(
        today = %today,
        remap = %remap_date,
        "NeTEx-EPIP: today's date is outside the published window; remapping to same-weekday in the latest period"
    );
    active_day_types_for_date(state, remap_date)
}

fn active_day_types_for_date<'a>(state: &'a ParseState, date: NaiveDate) -> HashSet<&'a str> {
    let mut active: HashSet<&'a str> = HashSet::new();
    for (dt_ref, op_ref) in &state.day_type_assignments {
        let Some(op) = state.operating_periods.get(op_ref) else {
            continue;
        };
        if period_covers_date(op, date) {
            active.insert(dt_ref.as_str());
        }
    }
    active
}

/// True if `op` covers `date` AND its ValidDayBits has a `'1'` at the
/// offset `(date - from_date)` days.
fn period_covers_date(op: &UicOperatingPeriodRec, date: NaiveDate) -> bool {
    let Some(from) = op.from_date.as_deref().and_then(parse_iso_date) else {
        return false;
    };
    let Some(to) = op.to_date.as_deref().and_then(parse_iso_date) else {
        return false;
    };
    if date < from || date > to {
        return false;
    }
    let bits = op.valid_day_bits.as_deref().unwrap_or("");
    let offset = (date - from).num_days();
    if offset < 0 {
        return false;
    }
    let offset = offset as usize;
    bits.as_bytes().get(offset) == Some(&b'1')
}

/// Parse the EPIP date format. STIB uses two shapes:
///
/// - `2025-03-07T00:00:00+00:00` (with timezone)
/// - `2025-03-30T23:59:59` (without timezone)
///
/// We only need the date part.
fn parse_iso_date(s: &str) -> Option<NaiveDate> {
    if let Ok(d) = NaiveDate::parse_from_str(s.get(..10)?, "%Y-%m-%d") {
        return Some(d);
    }
    if let Ok(dt) = NaiveDateTime::parse_from_str(s, "%Y-%m-%dT%H:%M:%S") {
        return Some(dt.date());
    }
    None
}

/// Find the latest published period and remap `today` to the same
/// weekday inside it. Returns `None` when the file has no parseable
/// operating periods.
fn remap_to_published_window(state: &ParseState, today: NaiveDate) -> Option<NaiveDate> {
    let mut latest_to: Option<NaiveDate> = None;
    let mut latest_from: Option<NaiveDate> = None;
    for op in state.operating_periods.values() {
        let from = op.from_date.as_deref().and_then(parse_iso_date)?;
        let to = op.to_date.as_deref().and_then(parse_iso_date)?;
        if latest_to.is_none() || to > latest_to.unwrap() {
            latest_to = Some(to);
            latest_from = Some(from);
        }
    }
    let to = latest_to?;
    let from = latest_from?;
    let target_weekday = today.weekday().num_days_from_monday();
    // Walk the period from `to` backward to find a same-weekday date.
    // Worst case: 7 days.
    let mut cursor = to;
    while cursor >= from {
        if cursor.weekday().num_days_from_monday() == target_weekday {
            return Some(cursor);
        }
        cursor = cursor.pred_opt()?;
    }
    None
}

/// Compute `(short_name, long_name)` for a `ServiceJourneyPattern` by
/// resolving its `RouteRef` to the matching `Line` via id-stem
/// matching.
///
/// STIB's EPIP file has no `<Route>` declarations — the RouteRef id
/// `FR:Route:gr_stibmivb_14:` maps to Line id `FR:Line:gr_stibmivb_14:`
/// (same stem). The reverse direction has a `_R` suffix
/// (`FR:Route:gr_stibmivb_14_R:`) which we strip to find the line.
fn resolve_pattern_line_meta(state: &ParseState, pattern: &JourneyPatternRec) -> (String, String) {
    let Some(route_ref) = pattern.route_ref.as_deref() else {
        return (String::new(), String::new());
    };
    // Replace `FR:Route:` with `FR:Line:` and strip an optional `_R`
    // direction suffix.
    let line_id = route_ref.replace(":Route:", ":Line:");
    let stripped = line_id
        .strip_suffix("_R:")
        .map(|s| format!("{s}:"))
        .unwrap_or(line_id);
    let Some(line) = state.lines.get(&stripped) else {
        return (String::new(), String::new());
    };
    // GTFS short_name is the public-facing line number (e.g. "1" for
    // metro line 1, "T7" for tram 7); long_name is the descriptive
    // route ("ERASME — STOCKEL"). EPIP encodes these as PublicCode
    // and Name respectively.
    (line.public_code.clone(), line.name.clone())
}

/// Resolve a ScheduledStopPoint's human-readable name via the
/// PassengerStopAssignment chain. Falls back to the SSP id if no
/// chain is available.
fn resolve_stop_name(state: &ParseState, ssp_id: &str) -> String {
    if let Some(psa) = state.passenger_stop_assignments.get(ssp_id) {
        // Prefer Quay name (platform-level, more specific).
        if let Some(qref) = &psa.quay_ref {
            if let Some(q) = state.quays.get(qref) {
                if let Some(n) = q.name.as_ref() {
                    if !n.is_empty() {
                        return n.clone();
                    }
                }
            }
        }
        // Fall back to StopPlace name (station-level).
        if let Some(spref) = &psa.stop_place_ref {
            if let Some(sp) = state.stop_places.get(spref) {
                if let Some(n) = sp.name.as_ref() {
                    if !n.is_empty() {
                        return n.clone();
                    }
                }
            }
        }
    }
    // Last resort: reuse the SSP id as the name so the stop is at
    // least identifiable.
    ssp_id.to_string()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn hms_parser_handles_common_shapes() {
        assert_eq!(parse_hms("00:00:00"), Some(0));
        assert_eq!(parse_hms("05:19:00"), Some(5 * 3600 + 19 * 60));
        assert_eq!(parse_hms("23:59:59"), Some(23 * 3600 + 59 * 60 + 59));
        // Two-component shape is tolerated.
        assert_eq!(parse_hms("12:34"), Some(12 * 3600 + 34 * 60));
        // Bad input → None.
        assert_eq!(parse_hms(""), None);
        assert_eq!(parse_hms("abc"), None);
    }

    #[test]
    fn element_local_name_strips_ns() {
        assert_eq!(element_local_name(b"gml:pos"), b"pos");
        assert_eq!(element_local_name(b"Line"), b"Line");
        assert_eq!(element_local_name(b"ns0:StopPlace"), b"StopPlace");
    }
}
