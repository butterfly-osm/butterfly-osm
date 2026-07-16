//! Arrow Flight gRPC server for Butterfly routing engine
//!
//! Provides high-performance Arrow-native streaming for:
//! - `matrix` — distance/duration matrix via Bucket M2M or PHAST tiling
//! - `route_batch` — batch point-to-point routing with WKB geometry
//! - `isochrone` — reachability polygons as WKB per interval
//!
//! Ticket format: `action:profile:params_json`

// tonic::Status is 176 bytes — the canonical error type for gRPC services.
// Boxing it would add indirection with zero benefit since every gRPC return type uses it.
// This lint is suppressed module-wide via the mod declaration in mod.rs.

use std::io::Write;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::*;
use arrow::datatypes::*;
use arrow::ipc::writer::StreamWriter;
use arrow::record_batch::RecordBatch;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use rayon::prelude::*;
// Rename arrow_flight::Result to avoid conflict with std::result::Result
use arrow_flight::Result as FlightResult;
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use serde::Deserialize;
use tonic::{Request, Response, Status, Streaming};

use crate::matrix::bucket_ch::table_bucket_full_flat;
use crate::matrix::neighbors::{RadiusParam, auto_radius_km, build_neighbors, parse_radius};
use crate::profile_abi::Mode;
use crate::range::contour::ContourResult;
use crate::range::wkb_stream::encode_polygon_wkb;

use super::geometry::{Point, build_isochrone_geometry};
use super::isochrone_handler::{run_phast_bounded_fast, run_phast_bounded_fast_reverse};
use super::query::CchQuery;
use super::state::ServerState;

/// Butterfly Arrow Flight service. Holds an `Arc<RegionsState>` and
/// resolves the primary `ServerState` lazily per request — this lets
/// the multi-region default-lazy boot keep all regions in Pending
/// state until the first query (#292 Phase 3).
///
/// **Multi-region limitation**: today Flight always serves the
/// *primary* (first) region. Cross-region routing on Flight is filed
/// as future work (PR C in the #91 chain). Until then, multi-region
/// REST works correctly via the per-request dispatcher; Flight is
/// single-region-only.
pub struct ButterflyFlight {
    regions: Arc<super::regions::RegionsState>,
}

impl ButterflyFlight {
    pub fn new(regions: Arc<super::regions::RegionsState>) -> Self {
        Self { regions }
    }

    /// Resolve the primary region's state on demand. Triggers lazy
    /// load (~30 s container load) the first time a Flight handler
    /// reaches this method on a Pending region.
    ///
    /// Used only by handlers that don't carry coordinates (e.g. the
    /// transit subsystem readiness check) — coordinate-bearing actions
    /// dispatch to the right region via
    /// [`ButterflyFlight::dispatch_for_point`] / [`dispatch_for_pair`].
    #[inline]
    fn state(&self) -> Arc<ServerState> {
        self.regions.primary()
    }

    /// #336: snap a single coordinate to the right region and return
    /// `(state, region_id)`. Maps the regions-layer
    /// [`super::regions::DispatchError`] into a gRPC `Status` so each
    /// action handler stays a single statement.
    fn dispatch_for_point(
        &self,
        lon: f64,
        lat: f64,
        profile: &str,
    ) -> std::result::Result<(Arc<ServerState>, String), Status> {
        self.regions
            .dispatch_single_id(lon, lat, profile)
            .map_err(dispatch_to_status)
    }

    /// #336: snap a src/dst pair to the right region. Returns
    /// `(state, region_id)` when both endpoints share a region, or a
    /// `FAILED_PRECONDITION` status for cross-region pairs (mirrors
    /// the REST 501 with the same wording).
    fn dispatch_for_pair(
        &self,
        origin_lon: f64,
        origin_lat: f64,
        destination_lon: f64,
        destination_lat: f64,
        profile: &str,
    ) -> std::result::Result<(Arc<ServerState>, String), Status> {
        self.regions
            .dispatch_p2p_id(
                origin_lon,
                origin_lat,
                destination_lon,
                destination_lat,
                profile,
            )
            .map_err(dispatch_to_status)
    }
}

/// Map a [`super::regions::DispatchError`] into a gRPC `Status` that
/// matches the REST-side semantics (400 → InvalidArgument, 501 →
/// FailedPrecondition). Flight has no native 501 so we use
/// FailedPrecondition (status code 9) for cross-region, which is also
/// what the REST handler returns under the hood for "spans regions".
/// Find the first row's (store_lon, store_lat) across a sequence of
/// catchment input batches so [`ButterflyFlight::do_exchange`] can
/// dispatch to the right region.  Returns `None` if no batch has any
/// rows; the caller renders an InvalidArgument in that case.
fn first_store_lonlat(batches: &[arrow::record_batch::RecordBatch]) -> Option<(f64, f64)> {
    for batch in batches {
        if batch.num_rows() == 0 {
            continue;
        }
        let slon = batch
            .column_by_name("store_lon")?
            .as_any()
            .downcast_ref::<Float64Array>()?;
        let slat = batch
            .column_by_name("store_lat")?
            .as_any()
            .downcast_ref::<Float64Array>()?;
        return Some((slon.value(0), slat.value(0)));
    }
    None
}

fn dispatch_to_status(err: super::regions::DispatchError) -> Status {
    use super::regions::DispatchError;
    match err {
        DispatchError::NoRegion {
            endpoint,
            lon,
            lat,
            mode,
            ..
        } => Status::not_found(format!(
            "No road found within snap distance for {} ({}, {}) mode={}",
            endpoint.label(),
            lon,
            lat,
            mode
        )),
        DispatchError::InvalidMode { mode, available } => Status::invalid_argument(format!(
            "Invalid mode '{}'. Available across loaded regions: {}.",
            mode,
            available.join(", ")
        )),
        DispatchError::CrossRegion {
            src_region,
            dst_region,
        } => Status::failed_precondition(format!(
            "request spans regions {} \u{2192} {}; cross-region Flight not yet implemented (#336 follow-up)",
            src_region, dst_region
        )),
        DispatchError::Empty => Status::invalid_argument("no coordinates supplied to dispatcher"),
    }
}

/// Build a configured FlightServiceServer. Takes `Arc<RegionsState>`
/// instead of `Arc<ServerState>` so lazy region boot doesn't get
/// forced into eager construction at boot time.
pub fn build_flight_server(
    regions: Arc<super::regions::RegionsState>,
) -> FlightServiceServer<ButterflyFlight> {
    FlightServiceServer::new(ButterflyFlight::new(regions))
        .max_encoding_message_size(64 * 1024 * 1024)
        .max_decoding_message_size(64 * 1024 * 1024)
}

// =============================================================================
// Ticket parsing
// =============================================================================

struct ParsedTicket {
    action: String,
    profile: String,
    params_json: String,
}

fn parse_ticket(ticket: &Ticket) -> std::result::Result<ParsedTicket, Status> {
    let s = std::str::from_utf8(&ticket.ticket)
        .map_err(|_| Status::invalid_argument("Ticket must be UTF-8"))?;

    let first_colon = s
        .find(':')
        .ok_or_else(|| Status::invalid_argument("Ticket format: action:profile:params_json"))?;
    let rest = &s[first_colon + 1..];
    let second_colon = rest
        .find(':')
        .ok_or_else(|| Status::invalid_argument("Ticket format: action:profile:params_json"))?;

    Ok(ParsedTicket {
        action: s[..first_colon].to_string(),
        profile: rest[..second_colon].to_string(),
        params_json: rest[second_colon + 1..].to_string(),
    })
}

fn resolve_mode(profile: &str, state: &ServerState) -> std::result::Result<Mode, Status> {
    let lower = profile.to_lowercase();
    match state.mode_lookup.get(&lower) {
        Some(&idx) => Ok(Mode(idx)),
        None => {
            let mut available: Vec<&str> = state.mode_lookup.keys().map(|s| s.as_str()).collect();
            available.sort();
            Err(Status::invalid_argument(format!(
                "Unknown profile '{}'. Available: {}",
                profile,
                available.join(", ")
            )))
        }
    }
}

fn validate_coord(lon: f64, lat: f64, label: &str) -> std::result::Result<(), Status> {
    if !(-180.0..=180.0).contains(&lon) {
        return Err(Status::invalid_argument(format!(
            "{} longitude {} outside [-180, 180]",
            label, lon
        )));
    }
    if !(-90.0..=90.0).contains(&lat) {
        return Err(Status::invalid_argument(format!(
            "{} latitude {} outside [-90, 90]",
            label, lat
        )));
    }
    if lon.is_nan() || lat.is_nan() {
        return Err(Status::invalid_argument(format!(
            "{} coordinates contain NaN",
            label
        )));
    }
    Ok(())
}

// =============================================================================
// Schemas
// =============================================================================

fn matrix_schema() -> Schema {
    Schema::new(vec![
        Field::new("source_idx", DataType::UInt32, false),
        Field::new("target_idx", DataType::UInt32, false),
        Field::new("duration_ms", DataType::UInt32, false),
        Field::new("distance_m", DataType::UInt32, false),
    ])
}

fn route_batch_schema() -> Schema {
    // #482: `pair_idx` is the index of the pair in the input `pairs`
    // array. With `max_meters` set, over-bound pairs are DROPPED — so
    // emitted rows are sparse and pair_idx is the only way to map a row
    // back to its input. It is the FIRST column and never null.
    //
    // `duration_s` and `geometry_wkb` are nullable: the bounded prune
    // runs a DISTANCE-only query (no time, no path geometry), so those
    // two columns are null for every row in a bounded request. In the
    // unbounded path they are always populated (the full time-optimal
    // route + WKB), exactly as before. `distance_m` is always non-null.
    Schema::new(vec![
        Field::new("pair_idx", DataType::UInt32, false),
        Field::new("origin_lon", DataType::Float64, false),
        Field::new("origin_lat", DataType::Float64, false),
        Field::new("destination_lon", DataType::Float64, false),
        Field::new("destination_lat", DataType::Float64, false),
        Field::new("duration_s", DataType::Float32, true),
        Field::new("distance_m", DataType::Float32, false),
        Field::new("geometry_wkb", DataType::Binary, true),
    ])
}

fn isochrone_schema() -> Schema {
    Schema::new(vec![
        Field::new("interval_s", DataType::UInt32, false),
        Field::new("polygon_wkb", DataType::Binary, false),
    ])
}

/// Schema for `edges_batch` Flight action (#125).
///
/// One row per traversed edge per pair. Unnested on purpose — the
/// output is meant for polars/duckdb/arrow-native flow analytics
/// pipelines that `GROUP BY osm_node_to` or pipe to a traffic
/// assignment solver. Nested `list<struct>` is explicitly rejected
/// per the ticket: it fights every downstream tool.
///
/// Unreachable pairs emit a single row with `edge_seq` / `osm_node_*`
/// / `duration_ms` / `distance_m` all null. Clients filter on
/// `edge_seq IS NULL` cleanly. Sentinels like `u32::MAX` are the kind
/// of decision that bites consumers six months later.
///
/// #462: snap misses are the same failure class — a pair whose source
/// or destination has no road within snap distance emits the same
/// all-null row and NEVER fails the request, including when the miss
/// happens at the region-dispatch stage (e.g. a coordinate far outside
/// every loaded region). Request-level errors are reserved for
/// coordinate VALIDATION (NaN / out-of-range), unknown profiles,
/// cross-region pairs, and the pair-count cap.
///
/// Continuity invariant: for every `(query_idx, target_idx)` pair,
/// consecutive rows satisfy `osm_node_to[i] == osm_node_from[i+1]`.
/// This is what flow-assignment pipelines rely on to walk paths.
pub fn edges_batch_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_idx", DataType::UInt32, false),
        Field::new("target_idx", DataType::UInt32, false),
        Field::new("edge_seq", DataType::UInt32, true),
        Field::new("osm_node_from", DataType::Int64, true),
        Field::new("osm_node_to", DataType::Int64, true),
        Field::new("duration_ms", DataType::UInt32, true),
        Field::new("distance_m", DataType::UInt32, true),
    ])
}

/// Schema for the `edges_flow` DoExchange action (#460).
///
/// ONE row per `(group, directed edge)` with the accumulated flow —
/// the server-side replacement for streaming per-pair edge lists that the
/// consumer immediately re-aggregates. A trailing empty FlightData carries
/// the conservation summary as JSON `app_metadata`:
/// `{"n_pairs", "n_unreachable", "total_weight_in", "total_weight_assigned"}`.
pub fn edges_flow_schema() -> Schema {
    Schema::new(vec![
        Field::new("group", DataType::UInt32, false),
        Field::new("osm_node_from", DataType::Int64, false),
        Field::new("osm_node_to", DataType::Int64, false),
        Field::new("flow", DataType::Float64, false),
    ])
}

/// Schema for `transit_bulk` Flight action (#119).
///
/// One row per query in the batch. Successful queries carry the full
/// transit response metadata and a JSON-encoded `legs` array; failed
/// queries carry the HTTP-style `(status, error)` pair with the
/// metadata columns null.
///
/// Why JSON for `legs_json` instead of native Arrow `List<Struct>`?
/// The transit leg schema is a tagged enum with four variants
/// (`walk` / `drive` / `road` / `transit`) and the `transit` variant
/// has 12 nullable fields including `Arc<str>` references to stop
/// names. Encoding that natively is a multi-week schema project. JSON
/// is honest, dictionary-compresses well at scale, and round-trips
/// through every Arrow consumer (pyarrow, polars, DuckDB) without
/// custom decoding. The metadata columns are still natively typed,
/// which is the actual win for query / aggregation workloads.
pub fn transit_bulk_schema() -> Schema {
    Schema::new(vec![
        Field::new("query_idx", DataType::UInt32, false),
        Field::new("status", DataType::Utf8, false), // "ok" | "err"
        Field::new("http_status", DataType::UInt16, false), // 200 / 4xx / 5xx
        Field::new("error", DataType::Utf8, true),
        Field::new("origin_lon", DataType::Float64, false),
        Field::new("origin_lat", DataType::Float64, false),
        Field::new("destination_lon", DataType::Float64, false),
        Field::new("destination_lat", DataType::Float64, false),
        Field::new("depart_time", DataType::Utf8, true),
        Field::new("arrival_time", DataType::Utf8, true),
        Field::new("total_duration_s", DataType::UInt32, true),
        Field::new("access_mode", DataType::Utf8, true),
        Field::new("egress_mode", DataType::Utf8, true),
        Field::new("legs_json", DataType::Utf8, true),
    ])
}

// =============================================================================
// Matrix endpoint
// =============================================================================

#[derive(Deserialize)]
#[serde(deny_unknown_fields)] // #415: reject unsupported params (e.g. max_minutes on an old build) instead of silently ignoring
struct MatrixParams {
    origins: Vec<[f64; 2]>,
    destinations: Vec<[f64; 2]>,
    /// Optional Euclidean pre-filter radius in kilometres. Accepts a
    /// positive number, the string "auto", or null/0 to disable.
    #[serde(default)]
    radius_km: Option<serde_json::Value>,
    /// Optional drive-time bound in minutes (#415). When set, only cells whose
    /// travel time ≤ `max_minutes` are returned, and the search early-stops at
    /// the bound (compute ∝ reachable region). Orthogonal to radius_km.
    ///
    /// Returned values are exact for every reachable cell. NOTE: under a bound
    /// the Flight path skips the K-best snap-gap rescue (a bounded MAX cell is
    /// overwhelmingly "beyond the bound", not a snap gap), so a rare in-bound
    /// cell whose PRIMARY snap lands on a disconnected micro-component may be
    /// returned null instead of rescued. Use POST /table for full snap-gap
    /// fidelity under a bound. Values are never wrong — only conservatively
    /// dropped.
    #[serde(default)]
    max_minutes: Option<f64>,
}

/// Build matrix RecordBatch from flat u32 distances.
#[allow(clippy::too_many_arguments)]
fn build_matrix_batch(
    matrix: &[u32],
    lat_matrix: Option<&[u32]>,
    n_valid_origin: usize,
    n_valid_dst: usize,
    valid_src_indices: &[usize],
    valid_dst_indices: &[usize],
    schema: Arc<Schema>,
    neighbor_mask: Option<&[Vec<u32>]>,
) -> std::result::Result<RecordBatch, Status> {
    let capacity = n_valid_origin * n_valid_dst;
    let mut src_idx = UInt32Builder::with_capacity(capacity);
    let mut tgt_idx = UInt32Builder::with_capacity(capacity);
    let mut dur_ms = UInt32Builder::with_capacity(capacity);
    let mut dist_m = UInt32Builder::with_capacity(capacity);

    for (si, &orig_src) in valid_src_indices.iter().enumerate() {
        let neighbors: Option<&[u32]> = neighbor_mask.map(|nm| nm[orig_src].as_slice());
        for (di, &orig_dst) in valid_dst_indices.iter().enumerate() {
            let pruned = if let Some(ns) = neighbors {
                ns.binary_search(&(orig_dst as u32)).is_err()
            } else {
                false
            };
            let cell_idx = si * n_valid_dst + di;
            let d = if pruned { u32::MAX } else { matrix[cell_idx] };
            src_idx.append_value(orig_src as u32);
            tgt_idx.append_value(orig_dst as u32);
            if d == u32::MAX {
                dur_ms.append_value(u32::MAX);
                dist_m.append_value(u32::MAX);
            } else {
                // dur_ms is the time matrix value scaled to ms (post-#297
                // weights are in seconds).
                dur_ms.append_value(d.saturating_mul(1000));
                // #372: when the 2-channel run produced a lat matrix,
                // emit it as distance_m. Otherwise emit u32::MAX (the
                // pre-#372 behaviour — old containers without cch.lat).
                let dist_val = lat_matrix.map(|lm| lm[cell_idx]).unwrap_or(u32::MAX);
                dist_m.append_value(dist_val);
            }
        }
    }

    RecordBatch::try_new(
        schema,
        vec![
            Arc::new(src_idx.finish()) as ArrayRef,
            Arc::new(tgt_idx.finish()),
            Arc::new(dur_ms.finish()),
            Arc::new(dist_m.finish()),
        ],
    )
    .map_err(|e| Status::internal(format!("Arrow error: {}", e)))
}

pub type BatchStream =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<RecordBatch, Status>> + Send>>;

/// Execute the matrix Flight action.
fn do_matrix(
    state: &Arc<ServerState>,
    mode: Mode,
    params: MatrixParams,
) -> std::result::Result<BatchStream, Status> {
    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    // #415 max_minutes: time bound in CCH seconds. `u32::MAX` = unbounded.
    let threshold = super::table::parse_max_minutes(params.max_minutes)
        .map_err(Status::invalid_argument)?
        .unwrap_or(u32::MAX);
    let bounded = threshold != u32::MAX;

    use super::types::SnapRole;
    // K-best snap candidates per src/dst with directional role + the
    // SCC-aware connectivity filter (via mode_data.has_outbound /
    // has_inbound). The first candidate per slot feeds the bucket
    // M2M primary pass; the rest power the per-cell P2P fallback for
    // INF cells in the small-matrix branch below.
    const SNAP_K: usize = 64;
    use rayon::prelude::*;
    // Lazy snap (#368 pattern): K=1 primary upfront, K=64 escalation
    // lives in the INF-cell fallback below.
    // #502: primary + phantom seed set per endpoint. Seeds cover both
    // directed twins of up to 3 near-equidistant physical edges; the small-
    // matrix branch expands them into extra rows/columns (SeedExpansion) so
    // the bucket engine stays untouched. The large PHAST-tiled branch keeps
    // the primary-only legacy (engine-level multi-seed is the follow-up).
    type PrimarySnap = Option<((u32, f64, f64, f64), u32, Vec<(u32, u32, u32)>)>;
    let snap_endpoint = |lon: f64, lat: f64, role: SnapRole| -> PrimarySnap {
        let k = state.snap_index.snap_k_with_info_filtered_role(
            lon,
            lat,
            mode.0,
            8,
            None,
            role.role_filter(&mode_data),
        );
        if let Some(pe) =
            super::phantom::phantom_from_candidates(state, &mode_data, &k, lon, lat, role, None)
        {
            let seeds: Vec<(u32, u32, u32)> = pe
                .seeds
                .iter()
                .map(|x| (x.rank, x.part_time, x.part_len))
                .collect();
            let primary_rank = {
                let r = mode_data.orig_to_rank[pe.primary_ebg as usize];
                if r != u32::MAX { r } else { seeds[0].0 }
            };
            return Some((
                (
                    pe.primary_ebg,
                    pe.snapped_lon,
                    pe.snapped_lat,
                    pe.snap_distance_m,
                ),
                primary_rank,
                seeds,
            ));
        }
        super::snap_kbest::snap_primary_role(state, &mode_data, mode, lon, lat, role, None)
            .map(|(t, r)| (t, r, vec![(r, 0, 0)]))
    };
    let src_primary: Vec<PrimarySnap> = params
        .origins
        .par_iter()
        .map(|&[lon, lat]| snap_endpoint(lon, lat, SnapRole::Src))
        .collect();
    let dst_primary: Vec<PrimarySnap> = params
        .destinations
        .par_iter()
        .map(|&[lon, lat]| snap_endpoint(lon, lat, SnapRole::Dst))
        .collect();

    let mut origins_rank = Vec::with_capacity(params.origins.len());
    let mut valid_origin = Vec::with_capacity(params.origins.len());
    let mut origins_snapped = Vec::with_capacity(params.origins.len());
    let mut src_seedsets: Vec<Vec<(u32, u32, u32)>> = Vec::new();
    for (i, snap) in src_primary.iter().enumerate() {
        if let Some(((_, plon, plat, _), rank, seeds)) = snap {
            origins_rank.push(*rank);
            valid_origin.push(i);
            origins_snapped.push((*plon, *plat));
            src_seedsets.push(seeds.clone());
        } else {
            let [lon, lat] = params.origins[i];
            origins_snapped.push((lon, lat));
        }
    }
    let mut targets_rank = Vec::with_capacity(params.destinations.len());
    let mut valid_dst = Vec::with_capacity(params.destinations.len());
    let mut targets_snapped = Vec::with_capacity(params.destinations.len());
    let mut tgt_seedsets: Vec<Vec<(u32, u32, u32)>> = Vec::new();
    for (i, snap) in dst_primary.iter().enumerate() {
        if let Some(((_, plon, plat, _), rank, seeds)) = snap {
            targets_rank.push(*rank);
            valid_dst.push(i);
            targets_snapped.push((*plon, *plat));
            tgt_seedsets.push(seeds.clone());
        } else {
            let [lon, lat] = params.destinations[i];
            targets_snapped.push((lon, lat));
        }
    }

    if origins_rank.is_empty() || targets_rank.is_empty() {
        let schema = Arc::new(matrix_schema());
        let empty = RecordBatch::new_empty(schema);
        return Ok(Box::pin(stream::once(async move { Ok(empty) })));
    }

    let radius_param = parse_radius(params.radius_km.as_ref());
    let neighbor_mask: Option<Arc<Vec<Vec<u32>>>> = match radius_param {
        RadiusParam::None => None,
        RadiusParam::Km(r) => Some(Arc::new(build_neighbors(
            &origins_snapped,
            &targets_snapped,
            r,
        ))),
        RadiusParam::Auto => {
            let r = auto_radius_km(&origins_snapped, &targets_snapped);
            if r > 0.0 {
                Some(Arc::new(build_neighbors(
                    &origins_snapped,
                    &targets_snapped,
                    r,
                )))
            } else {
                None
            }
        }
    };

    let n_origin = params.origins.len();
    let n_dst = params.destinations.len();
    let n_valid_origin = origins_rank.len();
    let n_valid_dst = targets_rank.len();

    // Bucket-M2M handles up to ~1M cells comfortably (4 MB matrix + a
    // few MB of bucket scratch). The pre-#386 threshold of 50_000
    // bounced 250×250+ matrices into the slow PHAST tiled streaming
    // path, which made apples-to-apples Flight bench against libosrm
    // look ~5× worse than reality. Above 1M cells, the streamed PHAST
    // path still wins on memory.
    const BUCKET_M2M_THRESHOLD: usize = 1_000_000;

    if n_origin * n_dst <= BUCKET_M2M_THRESHOLD {
        // ---- SMALL MATRIX: Bucket M2M, single batch ----
        let use_parallel = n_valid_origin * n_valid_dst >= 2500;
        let up = &mode_data.up_adj_flat;
        let down = &mode_data.down_rev_flat;

        // #372: when cch_weights_len_along_time is loaded, run the
        // 2-channel bucket-M2M to populate distance_m correctly (length
        // along the time-shortest path). Falls back to single-channel
        // time-only when the LAT flats aren't available — old containers
        // built before PR #379 emit u32::MAX in distance_m, same as
        // pre-#372 behaviour.
        let lat_flats = mode_data
            .up_adj_flat_len_along_time
            .as_ref()
            .zip(mode_data.down_rev_flat_len_along_time.as_ref());
        // #502: expansion (see phantom.rs SeedExpansion)
        let src_exp = super::phantom::SeedExpansion::build(&src_seedsets);
        let tgt_exp = super::phantom::SeedExpansion::build(&tgt_seedsets);
        let exp_threshold = if threshold == u32::MAX {
            u32::MAX
        } else {
            threshold.saturating_add(src_exp.slack() + tgt_exp.slack())
        };
        let (mut matrix, mut lat_matrix_opt, _stats) = if let Some((up_lat, dn_lat)) = lat_flats {
            // Always use parallel for 2-channel: the sequential path
            // calls `SearchState2::new(n_nodes)` per call which is
            // ~60 MB on Belgium and dominates small-N latency. The
            // parallel path reuses thread-local SearchState2 via
            // BACKWARD_STATE_LAT, so even 10×10 amortises the alloc
            // away. Rayon spawn cost is in microseconds and fine.
            // #415: `_bounded` early-stops the time sweeps at `threshold`
            // (u32::MAX = unbounded, byte-identical to the prior call).
            let (m, lm, st) =
                crate::matrix::bucket_ch::table_bucket_parallel_len_along_time_bounded(
                    n_nodes,
                    up,
                    down,
                    up_lat,
                    dn_lat,
                    &src_exp.exp_ranks,
                    &tgt_exp.exp_ranks,
                    exp_threshold,
                );
            let (m, lm_opt) = src_exp.reduce_time(&tgt_exp, &m, Some(&lm));
            (m, lm_opt, st)
        } else {
            // #415: when bounded, always take the parallel `_bounded` path
            // (the sequential `table_bucket_full_flat` has no bound hook).
            let (m, st) = if use_parallel || bounded {
                crate::matrix::bucket_ch::table_bucket_parallel_bounded(
                    n_nodes,
                    up,
                    down,
                    &src_exp.exp_ranks,
                    &tgt_exp.exp_ranks,
                    exp_threshold,
                )
            } else {
                table_bucket_full_flat(n_nodes, up, down, &src_exp.exp_ranks, &tgt_exp.exp_ranks)
            };
            let (m, _) = src_exp.reduce_time(&tgt_exp, &m, None);
            (m, None, st)
        };

        // Per-cell K-best fallback for INF cells (mirrors /table POST).
        // With SCC-aware role masks this is now a rare per-cell rescue
        // for geometric-ambiguity / dynamic-recustomisation pairs.
        // K=64 escalation runs only for src/dst indices that have at
        // least one INF cell.
        //
        // #415: skip the fallback entirely when a minutes bound is in effect.
        // A MAX cell under a bound is overwhelmingly "beyond the time bound"
        // (legitimately excluded), not a snap gap — rescuing every one with
        // an UNBOUNDED P2P would do large wasted work and then be masked away
        // below anyway. Bounded matrices trade the rare in-bound snap-gap
        // rescue for correctness + speed; /table POST keeps the full rescue
        // for the high-fidelity, typically-smaller request shape.
        if !bounded && matrix.contains(&u32::MAX) {
            use rayon::prelude::*;
            use std::collections::HashSet;
            let query = super::query::CchQuery::new(&mode_data);

            let mut work: Vec<(usize, usize)> = Vec::new();
            let mut needed_src: HashSet<usize> = HashSet::new();
            let mut needed_dst: HashSet<usize> = HashSet::new();
            for (i, _) in valid_origin.iter().enumerate() {
                for (j, _) in valid_dst.iter().enumerate() {
                    if matrix[i * n_valid_dst + j] == u32::MAX {
                        work.push((i, j));
                        needed_src.insert(valid_origin[i]);
                        needed_dst.insert(valid_dst[j]);
                    }
                }
            }
            // Lazy K=64 snap for only the failing src/dst originals.
            let needed_src_vec: Vec<usize> = needed_src.into_iter().collect();
            let needed_dst_vec: Vec<usize> = needed_dst.into_iter().collect();
            let mut src_kbest_ranks: std::collections::HashMap<usize, Vec<u32>> =
                std::collections::HashMap::new();
            for (orig_idx, ranks) in needed_src_vec
                .par_iter()
                .map(|&oi| {
                    let [lon, lat] = params.origins[oi];
                    let snap = super::snap_kbest::snap_k_pair_role(
                        state,
                        &mode_data,
                        mode,
                        lon,
                        lat,
                        SnapRole::Src,
                        None,
                        SNAP_K,
                    );
                    (oi, snap.ranks)
                })
                .collect::<Vec<_>>()
            {
                src_kbest_ranks.insert(orig_idx, ranks);
            }
            let mut dst_kbest_ranks: std::collections::HashMap<usize, Vec<u32>> =
                std::collections::HashMap::new();
            for (orig_idx, ranks) in needed_dst_vec
                .par_iter()
                .map(|&oi| {
                    let [lon, lat] = params.destinations[oi];
                    let snap = super::snap_kbest::snap_k_pair_role(
                        state,
                        &mode_data,
                        mode,
                        lon,
                        lat,
                        SnapRole::Dst,
                        None,
                        SNAP_K,
                    );
                    (oi, snap.ranks)
                })
                .collect::<Vec<_>>()
            {
                dst_kbest_ranks.insert(orig_idx, ranks);
            }

            let empty: Vec<u32> = Vec::new();
            let patches: Vec<(usize, usize, u32)> = work
                .par_iter()
                .filter_map(|&(i, j)| {
                    let src_orig_idx = valid_origin[i];
                    let dst_orig_idx = valid_dst[j];
                    let src_ranks = src_kbest_ranks.get(&src_orig_idx).unwrap_or(&empty);
                    let dst_ranks = dst_kbest_ranks.get(&dst_orig_idx).unwrap_or(&empty);
                    super::snap_kbest::p2p_with_kbest_fallback(
                        &query,
                        src_ranks,
                        dst_ranks,
                        super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
                    )
                    .map(|(_, _, r)| (i, j, r.distance))
                })
                .collect();
            for (i, j, dist) in patches {
                matrix[i * n_valid_dst + j] = dist;
            }
        }

        // #415: exact-output mask. The bounded join can leave a non-minimal
        // value > threshold for an out-of-bound pair; null both channels
        // there so the emitted set is exactly the unbounded matrix filtered
        // to ≤ threshold.
        if bounded {
            for c in 0..matrix.len() {
                if matrix[c] > threshold {
                    matrix[c] = u32::MAX;
                    if let Some(lm) = lat_matrix_opt.as_mut() {
                        lm[c] = u32::MAX;
                    }
                }
            }
        }

        // #372: if the K-best fallback patched any cells above, the
        // lat_matrix is now stale for those cells — they were updated
        // from a per-pair P2P, not the bucket-M2M. For now we keep the
        // 2-channel lat values for cells that bucket-M2M reached, and
        // emit u32::MAX in `distance_m` for the rescued cells (whose
        // `dur_ms` came from p2p_with_kbest_fallback). The Flight
        // schema callers already treat u32::MAX as "no distance".
        let schema = Arc::new(matrix_schema());
        let batch = build_matrix_batch(
            &matrix,
            lat_matrix_opt.as_deref(),
            n_valid_origin,
            n_valid_dst,
            &valid_origin,
            &valid_dst,
            schema,
            neighbor_mask.as_ref().map(|v| v.as_slice()),
        )?;
        let _ = &mut lat_matrix_opt; // silence unused-mut if later refactored

        Ok(Box::pin(stream::once(async move { Ok(batch) })))
    } else {
        // ---- LARGE MATRIX: PHAST tiling, streamed ----
        let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<RecordBatch, Status>>(8);
        let cancelled = Arc::new(AtomicBool::new(false));
        let cancelled_bg = cancelled.clone();

        let up_adj = mode_data.up_adj_flat.clone();
        let down_rev = mode_data.down_rev_flat.clone();
        let schema = Arc::new(matrix_schema());
        let neighbor_mask_bg = neighbor_mask.clone();

        let src_tile_size = 1000usize.min(n_origin).max(1);

        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;

            let src_blocks: Vec<(usize, usize)> = (0..n_origin)
                .step_by(src_tile_size)
                .map(|s| (s, (s + src_tile_size).min(n_origin)))
                .collect();

            src_blocks.par_iter().for_each(|&(src_start, src_end)| {
                if cancelled_bg.load(Ordering::Relaxed) {
                    return;
                }

                let mut block_src_ranks = Vec::new();
                let mut block_src_orig = Vec::new();
                for (vi, &oi) in valid_origin.iter().enumerate() {
                    if oi >= src_start && oi < src_end {
                        block_src_ranks.push(origins_rank[vi]);
                        block_src_orig.push(oi);
                    }
                }

                if block_src_ranks.is_empty() {
                    return;
                }

                // #415: bound both sweeps at the time threshold so the large
                // (streamed) path is also time-proportional, not just the
                // small bucket-M2M path. u32::MAX = unbounded.
                let buckets = Arc::new(crate::matrix::bucket_ch::forward_build_buckets_bounded(
                    n_nodes,
                    &up_adj,
                    &block_src_ranks,
                    threshold,
                ));

                let tile_matrix = crate::matrix::bucket_ch::backward_join_with_buckets_bounded(
                    n_nodes,
                    &down_rev,
                    &buckets,
                    &targets_rank,
                    threshold,
                );

                let n_block_src = block_src_ranks.len();
                let n_block_dst = targets_rank.len();
                let capacity = n_block_src * n_block_dst;
                let mut si_arr = UInt32Builder::with_capacity(capacity);
                let mut di_arr = UInt32Builder::with_capacity(capacity);
                let mut dur_arr = UInt32Builder::with_capacity(capacity);
                let mut dist_arr = UInt32Builder::with_capacity(capacity);

                for (bsi, &orig_si) in block_src_orig.iter().enumerate() {
                    let neighbors: Option<&[u32]> =
                        neighbor_mask_bg.as_ref().map(|nm| nm[orig_si].as_slice());
                    for (bdi, &orig_di) in valid_dst.iter().enumerate() {
                        let pruned = if let Some(ns) = neighbors {
                            ns.binary_search(&(orig_di as u32)).is_err()
                        } else {
                            false
                        };
                        let d = if pruned {
                            u32::MAX
                        } else {
                            let v = tile_matrix[bsi * n_block_dst + bdi];
                            // #415: null cells beyond the time bound. When
                            // unbounded, threshold == u32::MAX so this never
                            // fires (v > u32::MAX is impossible).
                            if v > threshold { u32::MAX } else { v }
                        };
                        si_arr.append_value(orig_si as u32);
                        di_arr.append_value(orig_di as u32);
                        if d == u32::MAX {
                            dur_arr.append_value(u32::MAX);
                        } else {
                            // #415 review: seconds → milliseconds (×1000) to
                            // match the small-matrix branch and the `duration_ms`
                            // schema name (was ×100, i.e. 10× too small here).
                            dur_arr.append_value(d.saturating_mul(1000));
                        }
                        dist_arr.append_value(u32::MAX);
                    }
                }

                let batch = match RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(si_arr.finish()) as ArrayRef,
                        Arc::new(di_arr.finish()),
                        Arc::new(dur_arr.finish()),
                        Arc::new(dist_arr.finish()),
                    ],
                ) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(Status::internal(format!("Arrow: {}", e))));
                        cancelled_bg.store(true, Ordering::Relaxed);
                        return;
                    }
                };

                if tx.blocking_send(Ok(batch)).is_err() {
                    cancelled_bg.store(true, Ordering::Relaxed);
                }
            });
        });

        let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
        Ok(Box::pin(stream))
    }
}

// =============================================================================
// Route batch endpoint
// =============================================================================

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RouteBatchParams {
    pairs: Vec<[f64; 4]>, // [origin_lon, origin_lat, destination_lon, destination_lat]
    /// #482: optional DISTANCE prune (meters). When set, only pairs whose
    /// distance-metric shortest route is `<= max_meters` emit a row; the
    /// rest are DROPPED. The bounded CCH search early-terminates at the
    /// ball, so over-bound pairs are also cheaper to compute. Unbounded
    /// (absent) behaves exactly as before. There is intentionally no
    /// `max_seconds` companion — distance is the only bound the reporting
    /// use case (drive-distance catchment) needs, and adding a time bound
    /// would force a second metric query per pair for no consumer.
    ///
    /// `deny_unknown_fields` (above) is also part of #482: today the
    /// action silently accepts `max_km`, `prune_max_km`, `radius_km`,
    /// `zzz`, … with no effect — a footgun the ticket calls out. Unknown
    /// params now error instead of being ignored.
    #[serde(default)]
    max_meters: Option<f64>,
}

/// #273: in-place WKB encoder — appends bytes to `out`. Clears `out`
/// first so callers can reuse the same buffer across pairs.
fn encode_linestring_wkb_into(points: &[Point], out: &mut Vec<u8>) {
    out.clear();
    let n = points.len();
    out.reserve(1 + 4 + 4 + n * 16);

    out.push(1u8); // little-endian
    let _ = out.write_all(&2u32.to_le_bytes()); // LineString type
    let _ = out.write_all(&(n as u32).to_le_bytes());
    for p in points {
        let _ = out.write_all(&p.lon.to_le_bytes());
        let _ = out.write_all(&p.lat.to_le_bytes());
    }
}

/// #273 per-worker scratch buffers. Reused across pairs in the same
/// `std::thread::scope` worker. Each pair calls `compute_route_pair`
/// passing `&mut RouteScratch`; the buffers are cleared at the start
/// of each call. The final WKB is moved out via `std::mem::replace`
/// (handed off to the Arrow `BinaryBuilder`) and the slot is replaced
/// with `Vec::with_capacity(prev_capacity)` so the next pair's encode
/// reuses the allocation. (#301 review: `mem::take` would leave
/// `Vec::new()` with zero capacity, forcing a fresh allocation per
/// pair and defeating the scratch reuse this struct exists for.)
#[derive(Default)]
struct RouteScratch {
    rank_path: Vec<u32>,
    ebg_path: Vec<u32>,
    points: Vec<Point>,
    wkb: Vec<u8>,
}

/// Per-pair output for the route_batch parallel loop. One row per
/// (source, destination) pair. Named fields — the previous tuple alias
/// indexed by position (`r.6` for WKB) was brittle.
struct RoutePairRow {
    /// #482: index of this pair in the input `pairs` array. In bounded
    /// mode, over-bound pairs are dropped (no row), so this is the only
    /// way to map a sparse output back to the request.
    pair_idx: u32,
    origin_lon: f64,
    origin_lat: f64,
    destination_lon: f64,
    destination_lat: f64,
    /// `None` in bounded mode (distance-only query, no time). `Some` in
    /// the unbounded path (full time-optimal route).
    duration_s: Option<f32>,
    distance_m: f32,
    /// `None` in bounded mode (no path geometry computed). `Some` in the
    /// unbounded path.
    wkb: Option<Vec<u8>>,
}

/// Compute a single pair's `(duration, distance, WKB linestring)`.
///
/// Two-tier snap strategy: K=1 fast path first (covers most pairs per
/// #197 connectivity-aware role masks); on miss, escalate to K=64 +
/// (i+j)-combo fallback. Eliminates the K=64 tax on the hot path:
/// 5.79 ms/pair down to roughly 0.5 ms/pair on Belgium.
///
/// Reads only `&state` + `&mode_data`; safe to call from many worker
/// threads in parallel. `CchQueryState` is thread-local so the
/// bidirectional search never contends.
#[allow(clippy::too_many_arguments)]
fn compute_route_pair(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    query: &CchQuery<'_>,
    slon: f64,
    slat: f64,
    dlon: f64,
    dlat: f64,
    fallback_count: &std::sync::atomic::AtomicU64,
    scratch: &mut RouteScratch,
) -> Option<(f32, f32, Vec<u8>)> {
    use super::types::SnapRole;

    // Fast path (#502): phantom-seeded single query — both directed twins of
    // up to 3 near-equidistant physical edges per endpoint, exact partial
    // costs. Replaces the K=1 single-directed-edge commitment that caused 4x
    // fwd/rev asymmetry on long rural chains.
    let src_k = state.snap_index.snap_k_with_info_filtered_role(
        slon,
        slat,
        mode.0,
        8,
        None,
        SnapRole::Src.role_filter(mode_data),
    );
    let dst_k = state.snap_index.snap_k_with_info_filtered_role(
        dlon,
        dlat,
        mode.0,
        8,
        None,
        SnapRole::Dst.role_filter(mode_data),
    );
    if let (Some(sp), Some(dp)) = (
        super::phantom::phantom_from_candidates(
            state,
            mode_data,
            &src_k,
            slon,
            slat,
            SnapRole::Src,
            None,
        ),
        super::phantom::phantom_from_candidates(
            state,
            mode_data,
            &dst_k,
            dlon,
            dlat,
            SnapRole::Dst,
            None,
        ),
    ) {
        let (src_seeds, _) = sp.query_seeds_and_shift(SnapRole::Src);
        let (dst_seeds, dst_shift) = dp.query_seeds_and_shift(SnapRole::Dst);
        if let Some(r) = query.query_seeded(&src_seeds, &dst_seeds, false) {
            let result = super::query::QueryResult {
                distance: r.distance.saturating_sub(dst_shift),
                meeting_node: r.meeting_node,
                forward_parent: r.forward_parent,
                backward_parent: r.backward_parent,
            };
            return Some(build_route_output(
                state, mode_data, &result, r.src_root, r.dst_root, scratch,
            ));
        }
    }

    // #275-bench: increment fallback counter — K=1 fast path missed.
    fallback_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);

    // Slow path: K=64 K-best snap + (i+j)-combo fallback.
    const SNAP_K: usize = 64;
    let src_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        slon,
        slat,
        SnapRole::Src,
        None,
        SNAP_K,
    );
    let dst_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        dlon,
        dlat,
        SnapRole::Dst,
        None,
        SNAP_K,
    );

    if src_snap.ranks.is_empty() || dst_snap.ranks.is_empty() {
        return None;
    }

    super::snap_kbest::p2p_with_kbest_fallback(
        query,
        &src_snap.ranks,
        &dst_snap.ranks,
        super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
    )
    .map(|(src_rank, dst_rank, result)| {
        build_route_output(state, mode_data, &result, src_rank, dst_rank, scratch)
    })
}

/// Common output builder for a successful CCH P2P result. Returns
/// (duration_s, distance_m, WKB linestring). #273: uses `scratch` for
/// rank_path / ebg_path / points / wkb buffers — clears them on entry
/// and hands ownership of the final WKB to the caller via
/// `std::mem::replace` (the slot is replaced with a same-capacity
/// `Vec`, preserving the allocation for the next call's encode).
/// #487: unpack the route + compute its distance (meters) WITHOUT encoding
/// WKB. Leaves the geometry points in `scratch.points`. This is the exact
/// same distance metric `build_route_output` returns (it now calls this),
/// so the `max_meters` prune is a true drop-in on the unbounded
/// `route_batch` distance — no metric shift, unlike the dropped
/// distance-optimal approach.
fn build_route_distance(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    result: &super::query::QueryResult,
    src_rank: u32,
    scratch: &mut RouteScratch,
) -> f32 {
    super::unpack::unpack_path_into(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &result.forward_parent,
        &result.backward_parent,
        src_rank,
        &mut scratch.rank_path,
    );
    scratch.ebg_path.clear();
    scratch.ebg_path.reserve(scratch.rank_path.len());
    for &rank in &scratch.rank_path {
        let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        scratch
            .ebg_path
            .push(mode_data.filtered_to_original[filt_id as usize]);
    }
    super::geometry::build_raw_points_into(
        &scratch.ebg_path,
        &state.ebg_nodes,
        &state.edge_geom,
        &mut scratch.points,
    ) as f32
}

fn build_route_output(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    result: &super::query::QueryResult,
    src_rank: u32,
    dst_rank: u32,
    scratch: &mut RouteScratch,
) -> (f32, f32, Vec<u8>) {
    // #297: result.distance is now seconds (v2 CCH weights).
    let duration_s = result.distance as f64;
    let _ = dst_rank; // unpack derives the path from forward+backward parents

    // distance + geometry points (scratch.points filled).
    let distance_m = build_route_distance(state, mode_data, result, src_rank, scratch);

    encode_linestring_wkb_into(&scratch.points, &mut scratch.wkb);
    // #301: `mem::take` leaves `Vec::new()` behind (zero capacity),
    // forcing a fresh allocation on the next pair and defeating the
    // scratch-buffer reuse this struct exists for. Instead replace
    // with a same-capacity Vec — the returned WKB still owns its
    // bytes, and the scratch slot stays sized for the next pair so
    // subsequent encodes reuse the allocation.
    let cap = scratch.wkb.capacity();
    let wkb = std::mem::replace(&mut scratch.wkb, Vec::with_capacity(cap));

    (duration_s as f32, distance_m, wkb)
}

/// #487: compute the TIME-optimal route's distance (meters) and keep the
/// pair only if it is `<= max_m`, else `None` (over-bound or unreachable
/// → dropped, no row). This is the SAME metric the unbounded `route_batch`
/// returns in `distance_m`, so the prune is a true drop-in (no
/// re-validation), and it uses the fast time CCH — not the distance metric
/// (the dropped #482 approach was ~12x slower AND a different metric, #487).
///
/// Snap MIRRORS `compute_route_pair`: K=1 fast path, then K=64 + 16-combo
/// fallback ONLY when the K=1 snap misses or the snapped ranks do not
/// connect — NOT when the route simply exceeds the bound. An over-bound
/// pair whose endpoints snapped fine is dropped immediately (one time
/// query), which is the whole point of a prune; the original #482 code's
/// escalation-on-over-bound made the pruned pairs the SLOWEST (the 12x).
#[allow(clippy::too_many_arguments)]
fn compute_route_distance_bounded(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    query: &CchQuery<'_>,
    slon: f64,
    slat: f64,
    dlon: f64,
    dlat: f64,
    max_m: u32,
    fallback_count: &std::sync::atomic::AtomicU64,
    scratch: &mut RouteScratch,
) -> Option<f32> {
    use super::types::SnapRole;
    let bound = max_m as f32;

    // K=1 fast path.
    let src_role = SnapRole::Src.role_filter(mode_data);
    let dst_role = SnapRole::Dst.role_filter(mode_data);
    if let (Some(src_id), Some(dst_id)) = (
        state
            .snap_index
            .snap_filtered_role(slon, slat, mode.0, None, src_role),
        state
            .snap_index
            .snap_filtered_role(dlon, dlat, mode.0, None, dst_role),
    ) {
        let (src_rank, dst_rank) = match (
            mode_data.orig_to_rank.get(src_id as usize).copied(),
            mode_data.orig_to_rank.get(dst_id as usize).copied(),
        ) {
            (Some(s), Some(d)) => (s, d),
            _ => (u32::MAX, u32::MAX),
        };
        if src_rank != u32::MAX && dst_rank != u32::MAX {
            // Both endpoints snapped to valid ranks: the time query is
            // authoritative for reachability. If it connects, the bound
            // decides — over-bound DROPS here, no K=64 escalation.
            if let Some(result) = query.query(src_rank, dst_rank) {
                let dist = build_route_distance(state, mode_data, &result, src_rank, scratch);
                return if dist <= bound { Some(dist) } else { None };
            }
            // valid ranks but no path between them → fall through to K=64
            // (same reachability escalation compute_route_pair does).
        }
    }

    // K=64 escalation — only reached on a K=1 snap miss / non-connecting
    // ranks, NOT on over-bound. Find the time-optimal route over combos,
    // then apply the bound ONCE.
    fallback_count.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    const SNAP_K: usize = 64;
    let src_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        slon,
        slat,
        SnapRole::Src,
        None,
        SNAP_K,
    );
    let dst_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        dlon,
        dlat,
        SnapRole::Dst,
        None,
        SNAP_K,
    );
    if src_snap.ranks.is_empty() || dst_snap.ranks.is_empty() {
        return None;
    }
    super::snap_kbest::p2p_with_kbest_fallback(
        query,
        &src_snap.ranks,
        &dst_snap.ranks,
        super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
    )
    .and_then(|(src_rank, _dst_rank, result)| {
        let dist = build_route_distance(state, mode_data, &result, src_rank, scratch);
        if dist <= bound { Some(dist) } else { None }
    })
}

fn route_batch_worker_threads(n_pairs: usize) -> usize {
    if n_pairs < 512 {
        return 1;
    }

    // Cap at logical CPU count: even if BUTTERFLY_ROUTE_BATCH_THREADS
    // is set above available_parallelism, oversubscribing hurts
    // latency/throughput. Default to min(available, 8) when env unset.
    let max_threads = std::thread::available_parallelism()
        .map(|n| n.get())
        .unwrap_or(4);
    let default_threads = max_threads.min(8);
    let configured = std::env::var("BUTTERFLY_ROUTE_BATCH_THREADS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(default_threads);
    configured
        .min(max_threads)
        .min(n_pairs.div_ceil(128))
        .max(1)
}

/// Per-record-batch pair count. Each emitted Arrow `RecordBatch` carries
/// at most this many `(src, dst, dur, dist, wkb)` rows.
///
/// Sized for the gRPC `max_encoding_message_size` of 64 MiB. Default 1000:
///   - Belgium WKB avg ~6-7 KiB → 1000 pairs ≈ 6-7 MiB per batch
///   - Transcontinental WKB ~50 KiB → 1000 pairs ≈ 50 MiB, still under cap
///   - Fixed-size columns (src/dst/dur/dist = 32 bytes/row) negligible
///
/// PR #294/#295 review (#TBD follow-up): a byte-aware adaptive flusher
/// that splits a chunk into multiple RecordBatches when accumulated WKB
/// exceeds a soft cap (e.g. 32 MiB) is the proper correctness fix.
/// Tracking that as a separate change so this PR's perf claims stay
/// reviewable.
///
/// Override via `BUTTERFLY_ROUTE_BATCH_SIZE` if you know your WKB sizes
/// fit in a higher cap, or set it lower for very long routes.
fn route_batch_batch_size() -> usize {
    std::env::var("BUTTERFLY_ROUTE_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(1_000)
}

/// Convert a `compute_route_pair` result into a `RoutePairRow`,
/// filling NaN + empty WKB on failure (snap miss, unreachable).
#[inline]
fn row_of(
    pair_idx: u32,
    slon: f64,
    slat: f64,
    dlon: f64,
    dlat: f64,
    result: Option<(f32, f32, Vec<u8>)>,
) -> RoutePairRow {
    // Unbounded path: duration + geometry always populated (`Some`), and
    // EVERY pair emits a row — a snap miss / unreachable pair becomes a
    // NaN-distance row with empty (but non-null) WKB, preserving the
    // pre-#482 wire shape exactly (modulo the new leading pair_idx column).
    match result {
        Some((dur, dist, wkb)) => RoutePairRow {
            pair_idx,
            origin_lon: slon,
            origin_lat: slat,
            destination_lon: dlon,
            destination_lat: dlat,
            duration_s: Some(dur),
            distance_m: dist,
            wkb: Some(wkb),
        },
        None => RoutePairRow {
            pair_idx,
            origin_lon: slon,
            origin_lat: slat,
            destination_lon: dlon,
            destination_lat: dlat,
            duration_s: Some(f32::NAN),
            distance_m: f32::NAN,
            wkb: Some(Vec::new()),
        },
    }
}

/// Outcome of building + sending an Arrow `RecordBatch`. The variants
/// distinguish a clean send (`Sent`) from an Arrow build failure
/// (`ArrowError`, already forwarded as `Status::internal` on `tx` —
/// caller bails) from a client disconnect (`Disconnected`).
///
/// PR #318 Copilot review: the previous `bool` return collapsed the
/// Arrow-error and disconnect paths; call sites then logged
/// disconnects when an Arrow error actually fired, which was
/// misleading.
enum EmitOutcome {
    Sent,
    Disconnected,
    ArrowError,
}

/// Build + send an Arrow `RecordBatch` from a fully-computed chunk of
/// `RoutePairRow`s.
fn emit_route_batch(
    tx: &tokio::sync::mpsc::Sender<std::result::Result<RecordBatch, Status>>,
    schema: &Arc<arrow::datatypes::Schema>,
    n: usize,
    results: Vec<RoutePairRow>,
) -> EmitOutcome {
    let mut pair_idx_arr = UInt32Builder::with_capacity(n);
    let mut origin_lon_arr = Float64Builder::with_capacity(n);
    let mut origin_lat_arr = Float64Builder::with_capacity(n);
    let mut destination_lon_arr = Float64Builder::with_capacity(n);
    let mut destination_lat_arr = Float64Builder::with_capacity(n);
    let mut dur_arr = Float32Builder::with_capacity(n);
    let mut dist_arr = Float32Builder::with_capacity(n);
    // #482: `wkb` is now `Option<Vec<u8>>` (null in bounded mode).
    let geom_bytes = results
        .iter()
        .map(|r| r.wkb.as_ref().map_or(0, |w| w.len()))
        .sum();
    let mut geom_arr = BinaryBuilder::with_capacity(n, geom_bytes);

    for row in results {
        pair_idx_arr.append_value(row.pair_idx);
        origin_lon_arr.append_value(row.origin_lon);
        origin_lat_arr.append_value(row.origin_lat);
        destination_lon_arr.append_value(row.destination_lon);
        destination_lat_arr.append_value(row.destination_lat);
        // #482: duration + geometry are nullable (null in bounded mode).
        dur_arr.append_option(row.duration_s);
        dist_arr.append_value(row.distance_m);
        geom_arr.append_option(row.wkb.as_deref());
    }

    let batch = match RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(pair_idx_arr.finish()) as ArrayRef,
            Arc::new(origin_lon_arr.finish()),
            Arc::new(origin_lat_arr.finish()),
            Arc::new(destination_lon_arr.finish()),
            Arc::new(destination_lat_arr.finish()),
            Arc::new(dur_arr.finish()),
            Arc::new(dist_arr.finish()),
            Arc::new(geom_arr.finish()),
        ],
    ) {
        Ok(b) => b,
        Err(e) => {
            // PR #322 review: if the receiver is already gone, the
            // outcome is really a disconnect — don't claim ArrowError
            // for a status that never reached the client.
            if tx
                .blocking_send(Err(Status::internal(format!("Arrow: {}", e))))
                .is_err()
            {
                return EmitOutcome::Disconnected;
            }
            return EmitOutcome::ArrowError;
        }
    };

    if tx.blocking_send(Ok(batch)).is_ok() {
        EmitOutcome::Sent
    } else {
        EmitOutcome::Disconnected
    }
}

/// #290: persistent worker pool. Spawns `n_workers` threads ONCE at
/// the start of the request so each thread initialises its
/// thread-local `CchQueryState` + per-worker `RouteScratch` exactly
/// once (the previous loop re-spawned per chunk, paying ~80 MB TLS
/// init each time on Belgium).
fn do_route_batch_blocking(
    state: Arc<ServerState>,
    mode: Mode,
    params: RouteBatchParams,
    tx: tokio::sync::mpsc::Sender<std::result::Result<RecordBatch, Status>>,
) {
    let mode_data = state.get_mode(mode);
    let schema = Arc::new(route_batch_schema());
    let batch_size = route_batch_batch_size();
    let total_pairs = params.pairs.len();
    let n_workers = route_batch_worker_threads(total_pairs);

    // #482: distance bound (meters → u32). `None` means the unbounded
    // path (full time-optimal route + geometry, every pair emits a row).
    // The dispatcher already validated `> 0` and finite, so this rounds
    // cleanly; the `clamp(0.0, u32::MAX)` is belt-and-suspenders.
    let max_m: Option<u32> = params
        .max_meters
        .map(|m| m.round().clamp(0.0, u32::MAX as f64) as u32);

    // Small-batch fast path: no pool overhead for tiny calls.
    if n_workers == 1 {
        let query = CchQuery::new(&mode_data);
        let mut scratch = RouteScratch::default();
        for (chunk_idx, chunk) in params.pairs.chunks(batch_size).enumerate() {
            // #482: global input index of this chunk's first pair.
            let base = chunk_idx * batch_size;
            // #275-bench: per-chunk counter incremented by
            // `compute_route_pair` each time the K=1 fast path misses
            // and escalates to the K=64 + (i+j)-combo fallback.
            let fallback_count = std::sync::atomic::AtomicU64::new(0);
            let fb = &fallback_count;
            let results: Vec<RoutePairRow> = chunk
                .iter()
                .enumerate()
                .filter_map(|(i, pair)| {
                    let pair_idx = (base + i) as u32;
                    let (slon, slat, dlon, dlat) = (pair[0], pair[1], pair[2], pair[3]);
                    match max_m {
                        // Bounded (#487): keep the pair only if the
                        // TIME-optimal route's distance is <= max_m; emit a
                        // distance-only row. Over-bound / unreachable → drop.
                        Some(m) => compute_route_distance_bounded(
                            &state,
                            &mode_data,
                            mode,
                            &query,
                            slon,
                            slat,
                            dlon,
                            dlat,
                            m,
                            fb,
                            &mut scratch,
                        )
                        .map(|dist| RoutePairRow {
                            pair_idx,
                            origin_lon: slon,
                            origin_lat: slat,
                            destination_lon: dlon,
                            destination_lat: dlat,
                            duration_s: None,
                            distance_m: dist,
                            wkb: None,
                        }),
                        // Unbounded: full route, every pair emits a row.
                        _ => {
                            let r = compute_route_pair(
                                &state,
                                &mode_data,
                                mode,
                                &query,
                                slon,
                                slat,
                                dlon,
                                dlat,
                                fb,
                                &mut scratch,
                            );
                            Some(row_of(pair_idx, slon, slat, dlon, dlat, r))
                        }
                    }
                })
                .collect();
            let n = results.len();
            // Bounded mode can drop every pair in a chunk — skip empty
            // RecordBatches (an empty batch is valid Arrow but pointless
            // wire traffic).
            if n == 0 {
                continue;
            }
            let fb_count = fallback_count.load(std::sync::atomic::Ordering::Relaxed);
            // #315 Copilot review: drop to DEBUG so production
            // /route_batch traffic doesn't spam logs (this is bench
            // instrumentation, not request audit).
            tracing::debug!(
                n_pairs = n,
                fallback = fb_count,
                fallback_pct = (fb_count as f64) * 100.0 / (n.max(1) as f64),
                "route_batch chunk fallback rate"
            );
            match emit_route_batch(&tx, &schema, n, results) {
                EmitOutcome::Sent => {}
                EmitOutcome::Disconnected | EmitOutcome::ArrowError => return,
            }
        }
        return;
    }

    // Multi-worker: persistent pool. #293 review feedback addressed:
    //   1. Per-worker sync_channel + round-robin dispatch (was
    //      Arc<Mutex<Receiver>>, which serialised all worker recvs
    //      through one lock — defeated parallelism).
    //   2. catch_unwind in each worker so a panic between recv and
    //      send sends a poison Done back instead of deadlocking the
    //      coordinator on done_rx.recv().
    //   3. Early returns close all work channels before returning so
    //      workers exit cleanly via recv() → Err; no scope-join deadlock.
    // #318 Copilot review: slot is `usize` so unusually large batch
    // sizes (BUTTERFLY_ROUTE_BATCH_SIZE > u32::MAX) can't truncate and
    // out-of-bounds index when writing back to `slots`.
    #[derive(Clone, Copy)]
    struct Work {
        slot: usize,
        /// #482: global index in the input `pairs` array. Distinct from
        /// `slot`, which is only this pair's position WITHIN its chunk
        /// (used to reorder results before emit). `pair_idx` is what the
        /// caller maps rows back by, and is the only handle on a sparse
        /// (bounded-mode) output.
        pair_idx: u32,
        slon: f64,
        slat: f64,
        dlon: f64,
        dlat: f64,
    }
    enum DoneKind {
        /// #482: `None` means the pair was DROPPED (over-bound in bounded
        /// mode). `Some` carries the emitted row.
        Row(Option<RoutePairRow>),
        WorkerPanic(String),
    }
    struct Done {
        slot: usize,
        kind: DoneKind,
    }

    // Shared fallback counter across the whole call so workers and
    // coordinator can quantify K=1 fast-path miss rate.
    let fallback_count = Arc::new(std::sync::atomic::AtomicU64::new(0));

    // Per-worker channels for true MPMC dispatch without a shared lock.
    let mut work_txs: Vec<std::sync::mpsc::SyncSender<Work>> = Vec::with_capacity(n_workers);
    let mut work_rxs: Vec<std::sync::mpsc::Receiver<Work>> = Vec::with_capacity(n_workers);
    for _ in 0..n_workers {
        let (wtx, wrx) = std::sync::mpsc::sync_channel::<Work>(32);
        work_txs.push(wtx);
        work_rxs.push(wrx);
    }
    let (done_tx, done_rx) = std::sync::mpsc::channel::<Done>();

    let join_result: std::thread::Result<()> = std::thread::scope(|scope| {
        let mut handles = Vec::with_capacity(n_workers);
        for rx in work_rxs.into_iter() {
            let state = Arc::clone(&state);
            let done_tx = done_tx.clone();
            let fallback_count = Arc::clone(&fallback_count);
            handles.push(scope.spawn(move || {
                let mode_data = state.get_mode(mode);
                let query = CchQuery::new(&mode_data);
                let mut scratch = RouteScratch::default();
                let fb = fallback_count.as_ref();
                while let Ok(work) = rx.recv() {
                    let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        match max_m {
                            // Bounded (#487): keep the pair only if the
                            // TIME-optimal route's distance is <= max_m.
                            // Over-bound / unreachable → drop.
                            Some(m) => compute_route_distance_bounded(
                                &state,
                                &mode_data,
                                mode,
                                &query,
                                work.slon,
                                work.slat,
                                work.dlon,
                                work.dlat,
                                m,
                                fb,
                                &mut scratch,
                            )
                            .map(|dist| RoutePairRow {
                                pair_idx: work.pair_idx,
                                origin_lon: work.slon,
                                origin_lat: work.slat,
                                destination_lon: work.dlon,
                                destination_lat: work.dlat,
                                duration_s: None,
                                distance_m: dist,
                                wkb: None,
                            }),
                            // Unbounded: full route, every pair emits a row.
                            _ => {
                                let r = compute_route_pair(
                                    &state,
                                    &mode_data,
                                    mode,
                                    &query,
                                    work.slon,
                                    work.slat,
                                    work.dlon,
                                    work.dlat,
                                    fb,
                                    &mut scratch,
                                );
                                Some(row_of(
                                    work.pair_idx,
                                    work.slon,
                                    work.slat,
                                    work.dlon,
                                    work.dlat,
                                    r,
                                ))
                            }
                        }
                    }));
                    let kind = match result {
                        Ok(row) => DoneKind::Row(row),
                        Err(panic_payload) => {
                            let msg = panic_payload
                                .downcast_ref::<String>()
                                .cloned()
                                .or_else(|| {
                                    panic_payload
                                        .downcast_ref::<&'static str>()
                                        .map(|s| s.to_string())
                                })
                                .unwrap_or_else(|| "<non-string panic>".to_string());
                            DoneKind::WorkerPanic(msg)
                        }
                    };
                    if done_tx
                        .send(Done {
                            slot: work.slot,
                            kind,
                        })
                        .is_err()
                    {
                        return;
                    }
                }
            }));
        }
        drop(done_tx);

        let mut next_worker = 0usize;
        let mut first_panic_msg: Option<String> = None;
        let mut bail_msg: Option<&'static str> = None;

        'chunks: for (chunk_idx, chunk) in params.pairs.chunks(batch_size).enumerate() {
            let n = chunk.len();
            // #482: global input index of this chunk's first pair.
            let base = chunk_idx * batch_size;
            let fb_before = fallback_count.load(std::sync::atomic::Ordering::Relaxed);
            for (i, pair) in chunk.iter().enumerate() {
                let work = Work {
                    slot: i,
                    pair_idx: (base + i) as u32,
                    slon: pair[0],
                    slat: pair[1],
                    dlon: pair[2],
                    dlat: pair[3],
                };
                if work_txs[next_worker].send(work).is_err() {
                    bail_msg = Some("route_batch workers exited unexpectedly");
                    break 'chunks;
                }
                next_worker = (next_worker + 1) % n_workers;
            }
            // #482: outer Option = "slot filled" (every dispatched pair
            // gets a Done back), inner Option = "row present" vs "dropped
            // (over-bound)". `slot` is the within-chunk position so the
            // emitted rows keep input order after flattening.
            let mut slots: Vec<Option<Option<RoutePairRow>>> = (0..n).map(|_| None).collect();
            let mut received = 0usize;
            while received < n {
                match done_rx.recv() {
                    Ok(d) => {
                        received += 1;
                        match d.kind {
                            DoneKind::Row(row) => slots[d.slot] = Some(row),
                            DoneKind::WorkerPanic(msg) => {
                                if first_panic_msg.is_none() {
                                    first_panic_msg = Some(msg);
                                }
                                slots[d.slot] = Some(Some(RoutePairRow {
                                    pair_idx: (base + d.slot) as u32,
                                    origin_lon: f64::NAN,
                                    origin_lat: f64::NAN,
                                    destination_lon: f64::NAN,
                                    destination_lat: f64::NAN,
                                    duration_s: Some(f32::NAN),
                                    distance_m: f32::NAN,
                                    wkb: Some(Vec::new()),
                                }));
                            }
                        }
                    }
                    Err(_) => {
                        bail_msg = Some("route_batch result channel closed early");
                        break 'chunks;
                    }
                }
            }
            if first_panic_msg.is_some() {
                break 'chunks;
            }
            // Flatten: drop slots that resolved to `None` (over-bound,
            // bounded mode). Order preserved by `slot`.
            let results: Vec<RoutePairRow> = slots
                .into_iter()
                .filter_map(|s| s.expect("slot filled"))
                .collect();
            let emit_n = results.len();
            // Bounded mode can drop every pair in a chunk — skip empty
            // RecordBatches.
            if emit_n == 0 {
                continue;
            }
            let fb_chunk = fallback_count.load(std::sync::atomic::Ordering::Relaxed) - fb_before;
            // #315 Copilot review: drop to DEBUG so production logs
            // aren't spammed by bench instrumentation.
            tracing::debug!(
                n_pairs = n,
                fallback = fb_chunk,
                fallback_pct = (fb_chunk as f64) * 100.0 / (n.max(1) as f64),
                "route_batch chunk fallback rate"
            );
            match emit_route_batch(&tx, &schema, emit_n, results) {
                EmitOutcome::Sent => {}
                EmitOutcome::Disconnected => {
                    bail_msg = Some("client disconnected");
                    break 'chunks;
                }
                EmitOutcome::ArrowError => {
                    // emit_route_batch already forwarded the error
                    // status on tx; bail without setting bail_msg so
                    // we don't double-send.
                    break 'chunks;
                }
            }
        }

        // Close all work channels BEFORE joining so workers exit
        // cleanly via recv → Err. Without this, an early break would
        // leave work_txs open and scope.join would deadlock.
        work_txs.clear();
        for h in handles {
            h.join()?;
        }

        if let Some(msg) = first_panic_msg {
            let _ = tx.blocking_send(Err(Status::internal(format!(
                "route_batch worker panicked: {}",
                msg
            ))));
        } else if let Some(msg) = bail_msg
            && msg != "client disconnected"
        {
            let _ = tx.blocking_send(Err(Status::internal(msg)));
        }
        Ok(())
    });

    if let Err(panic_payload) = join_result {
        let msg = panic_payload
            .downcast_ref::<String>()
            .cloned()
            .or_else(|| {
                panic_payload
                    .downcast_ref::<&'static str>()
                    .map(|s| s.to_string())
            })
            .unwrap_or_else(|| "<non-string panic>".to_string());
        let _ = tx.blocking_send(Err(Status::internal(format!(
            "route_batch worker panicked: {}",
            msg
        ))));
    }
}

fn do_route_batch(
    state: &Arc<ServerState>,
    mode: Mode,
    params: RouteBatchParams,
) -> std::result::Result<BatchStream, Status> {
    if params.pairs.is_empty() {
        let schema = Arc::new(route_batch_schema());
        let empty = RecordBatch::new_empty(schema);
        return Ok(Box::pin(stream::once(async move { Ok(empty) })));
    }

    let state = Arc::clone(state);
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<RecordBatch, Status>>(8);

    tokio::task::spawn_blocking(move || {
        do_route_batch_blocking(state, mode, params, tx);
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(stream))
}

// =============================================================================
// Isochrone endpoint
// =============================================================================

#[derive(Deserialize)]
struct IsochroneParams {
    lon: f64,
    lat: f64,
    intervals: Vec<u32>, // seconds
    #[serde(default = "default_direction")]
    direction: String,
}

fn default_direction() -> String {
    "depart".to_string()
}

fn do_isochrone(
    state: &Arc<ServerState>,
    mode: Mode,
    params: IsochroneParams,
) -> std::result::Result<BatchStream, Status> {
    if params.intervals.is_empty() {
        return Err(Status::invalid_argument("intervals must not be empty"));
    }
    if params.intervals.len() > 10 {
        return Err(Status::invalid_argument("max 10 intervals"));
    }
    for &iv in &params.intervals {
        if iv == 0 || iv > 7200 {
            return Err(Status::invalid_argument(
                "each interval must be 1..=7200 seconds",
            ));
        }
    }

    let mode_data = state.get_mode(mode);
    let mode_name = &state.mode_names[mode.index()];

    let is_reverse = params.direction.to_lowercase() == "arrive";
    // #197 directional snap: depart → center is a source (needs outbound),
    // arrive → center is a destination (needs inbound).
    let center_role = if is_reverse {
        super::types::SnapRole::Dst
    } else {
        super::types::SnapRole::Src
    };
    let orig_id = state
        .snap_index
        .snap_filtered_role(
            params.lon,
            params.lat,
            mode.0,
            None,
            center_role.role_filter(&mode_data),
        )
        .ok_or_else(|| Status::not_found("Could not snap to road network"))?;
    let origin_rank = mode_data.orig_to_rank[orig_id as usize];
    if origin_rank == u32::MAX {
        return Err(Status::not_found(
            "Snapped node not accessible for this mode",
        ));
    }
    // Intervals are user-input seconds; weights are also seconds (post-#297),
    // so the threshold passes through unchanged.
    let max_threshold_s = *params.intervals.iter().max().unwrap();

    let settled = if is_reverse {
        run_phast_bounded_fast_reverse(
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            origin_rank,
            max_threshold_s,
            mode,
        )
    } else {
        run_phast_bounded_fast(
            &mode_data.up_adj_flat,
            &mode_data.down_adj_flat,
            origin_rank,
            max_threshold_s,
            mode,
        )
    };

    // Map settled ranks back to original EBG IDs
    let settled_original: Vec<(u32, u32)> = settled
        .iter()
        .map(|&(rank, dist)| {
            let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
            let orig_id = mode_data.filtered_to_original[filt_id as usize];
            (orig_id, dist)
        })
        .collect();

    let node_weights = &mode_data.node_weights;

    let mut intervals_s = Vec::with_capacity(params.intervals.len());
    let mut wkb_data: Vec<Vec<u8>> = Vec::with_capacity(params.intervals.len());

    for &interval_s in &params.intervals {
        // No scaling: thresholds and weights are both in seconds (post-#297).
        let polygon_points = build_isochrone_geometry(
            &settled_original,
            interval_s,
            node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
            mode_name,
            None,
        );

        let coords: Vec<(f64, f64)> = polygon_points.iter().map(|p| (p.lon, p.lat)).collect();
        let contour = ContourResult {
            outer_ring: coords,
            holes: vec![],
            stats: Default::default(),
        };

        let wkb = encode_polygon_wkb(&contour).unwrap_or_default();
        intervals_s.push(interval_s);
        wkb_data.push(wkb);
    }

    let schema = Arc::new(isochrone_schema());
    let n = intervals_s.len();

    let interval_arr = UInt32Array::from(intervals_s);
    let mut wkb_builder = BinaryBuilder::with_capacity(n, wkb_data.iter().map(|w| w.len()).sum());
    for wkb in &wkb_data {
        wkb_builder.append_value(wkb);
    }

    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(interval_arr) as ArrayRef,
            Arc::new(wkb_builder.finish()),
        ],
    )
    .map_err(|e| Status::internal(format!("Arrow error: {}", e)))?;

    Ok(Box::pin(stream::once(async move { Ok(batch) })))
}

// =============================================================================
// edges_batch endpoint (#125)
// =============================================================================

/// Parameters for the `edges_batch` Flight action. MVP accepts the
/// flat `pairs` shape (same as `route_batch`). The source-batched
/// `queries` shape from the ticket is a follow-up that depends on a
/// predecessor-tracking batched PHAST variant.
#[derive(Deserialize)]
pub struct EdgesBatchParams {
    pub pairs: Vec<[f64; 4]>, // [origin_lon, origin_lat, destination_lon, destination_lat]
}

/// One unpacked edge row for `edges_batch` (one per EBG node in a path).
pub struct EdgeRow {
    pub edge_seq: u32,
    pub osm_from: i64,
    pub osm_to: i64,
    pub dur_ms: u32,
    pub dist_m: u32,
}

/// Per-pair result: the query index plus its unpacked edge rows.
/// `rows.is_empty()` ⇒ unreachable (the caller emits one all-null edge row).
pub struct PairEdges {
    pub query_idx: u32,
    pub rows: Vec<EdgeRow>,
    /// #468: the OPTIMIZED CCH metric (`QueryResult::distance` /
    /// `TreePath::distance`) for this pair; `None` ⇒ unreachable. Bench /
    /// internal only — the searches optimize CCH weights while the row
    /// `dur_ms` values come from post-unpack `node_weights`, so equal-CCH-cost
    /// ties can legitimately differ in summed row durations. The equivalence
    /// oracle asserts on THIS field. NOT part of the `edges_batch` wire
    /// schema; the Arrow emission never reads it.
    pub cch_distance: Option<u32>,
}

/// Compute the unpacked edge sequence for a single (src,dst) pair.
///
/// #436: factored out of `do_edges_batch` so the chunk loop can fan it
/// across rayon workers. Mirrors `compute_route_pair`'s lean K=1 snap
/// fast path (direct `snap_filtered_role`, no K=64 collect) and only
/// escalates to the K=64 + combo fallback when the fast path misses —
/// the dominant per-pair cost cut, since most realistic pairs snap on
/// the first try. `CchQuery::new` is free (just references + a
/// thread-local scratch reused across pairs on the same worker), so
/// constructing it per pair carries no allocation.
pub(crate) fn edges_for_pair(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    query: &super::query::CchQuery<'_>,
    query_idx: u32,
    pair: &[f64; 4],
) -> PairEdges {
    match route_for_pair(state, mode_data, mode, query, pair) {
        Some((src_rank, dst_rank, result)) => {
            emit_pair_rows(state, mode_data, src_rank, dst_rank, &result, query_idx)
        }
        None => PairEdges {
            query_idx,
            rows: Vec::new(),
            cch_distance: None,
        },
    }
}

/// #460: the routing core of [`edges_for_pair`] WITHOUT row emission —
/// K=1 fast path + K=64/16-combo escalation, returning the raw
/// `(src_rank, dst_rank, QueryResult)` so flow accumulation can fold
/// ranks instead of materialized rows. `None` ⇒ unreachable.
pub(crate) fn route_for_pair(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    query: &super::query::CchQuery<'_>,
    pair: &[f64; 4],
) -> Option<(u32, u32, super::query::QueryResult)> {
    use super::types::SnapRole;
    let (slon, slat, dlon, dlat) = (pair[0], pair[1], pair[2], pair[3]);

    // K=1 fast path (same shape as compute_route_pair).
    let src_role = SnapRole::Src.role_filter(mode_data);
    let dst_role = SnapRole::Dst.role_filter(mode_data);
    if let (Some(src_id), Some(dst_id)) = (
        state
            .snap_index
            .snap_filtered_role(slon, slat, mode.0, None, src_role),
        state
            .snap_index
            .snap_filtered_role(dlon, dlat, mode.0, None, dst_role),
    ) && let (Some(s), Some(d)) = (
        mode_data.orig_to_rank.get(src_id as usize).copied(),
        mode_data.orig_to_rank.get(dst_id as usize).copied(),
    ) && s != u32::MAX
        && d != u32::MAX
        && let Some(r) = query.query(s, d)
    {
        return Some((s, d, r));
    }
    // K=1 didn't connect → K=64 escalation.
    escalate_route(state, mode_data, mode, query, pair)
}

/// #438: K=64 + 16-combo escalation for a pair the K=1 fast path could not
/// connect. Snaps both ends to 64 candidates and tries the closest-sum-first
/// combos. Routing only (#460 split — callers emit rows via
/// [`emit_pair_rows`] or fold ranks); `None` ⇒ unreachable.
///
/// Split out of `edges_for_pair` so the source-grouped per-pair path
/// (`process_per_pair_work`) can escalate WITHOUT re-doing the K=1 snap+query
/// it already attempted with the precomputed ranks (#438-review: avoids the
/// redundant K=1 work that made the all-singleton workload regress).
fn escalate_route(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    query: &super::query::CchQuery<'_>,
    pair: &[f64; 4],
) -> Option<(u32, u32, super::query::QueryResult)> {
    use super::types::SnapRole;
    let (slon, slat, dlon, dlat) = (pair[0], pair[1], pair[2], pair[3]);
    const SNAP_K: usize = 64;
    // Rescues K=1 misses, including pairs whose closest snap had a u32::MAX
    // rank (not in this mode's CCH) — K=64 looks further out for a contracted
    // node.
    let src_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        slon,
        slat,
        SnapRole::Src,
        None,
        SNAP_K,
    );
    let dst_snap = super::snap_kbest::snap_k_pair_role(
        state,
        mode_data,
        mode,
        dlon,
        dlat,
        SnapRole::Dst,
        None,
        SNAP_K,
    );
    if !src_snap.ranks.is_empty() && !dst_snap.ranks.is_empty() {
        // Bound the escalation at 16 combos rather than the default 200.
        // Reachable pairs connect on the closest-sum-first combos almost
        // immediately (the #197 connectivity masks keep candidates on the main
        // component), so 16 loses no reachable pairs on the Belgium benchmark
        // (reachable count identical at 8 / 16 / 200) while bounding the
        // worst-case cost of a pair whose K=64 candidates don't connect.
        const EDGES_BATCH_MAX_COMBOS: usize = 16;
        if let Some(hit) = super::snap_kbest::p2p_with_kbest_fallback(
            query,
            &src_snap.ranks,
            &dst_snap.ranks,
            EDGES_BATCH_MAX_COMBOS,
        ) {
            return Some(hit);
        }
    }
    None // unreachable
}

/// #438: shared unpack + per-edge row emit. Used by BOTH the per-pair
/// `edges_for_pair` and the source-grouped path, so the row contract
/// (osm_from/to, dur_ms, dist_m, edge_seq) is byte-identical regardless of
/// how the `QueryResult` was produced.
///
/// Unpacks the CCH result to the full EBG rank sequence, then maps each rank
/// → original EBG node id and emits one row per node. Each EBG node is a
/// directed NBG edge (tail → head), so osm_node_from = osm(tail),
/// osm_node_to = osm(head); consecutive rows satisfy osm_to[i] == osm_from[i+1].
fn emit_pair_rows(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    src_rank: u32,
    dst_rank: u32,
    result: &super::query::QueryResult,
    query_idx: u32,
) -> PairEdges {
    let rank_path = super::unpack::unpack_path(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &result.forward_parent,
        &result.backward_parent,
        src_rank,
        dst_rank,
        result.meeting_node,
    );

    let mut rows = Vec::with_capacity(rank_path.len());
    for (edge_seq, &rank) in rank_path.iter().enumerate() {
        let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        let ebg_id = mode_data.filtered_to_original[filt_id as usize];
        let node = &state.ebg_nodes.nodes[ebg_id as usize];
        let osm_from = state
            .nbg_node_to_osm
            .get(node.tail_nbg as usize)
            .copied()
            .unwrap_or(0);
        let osm_to = state
            .nbg_node_to_osm
            .get(node.head_nbg as usize)
            .copied()
            .unwrap_or(0);
        // node_weights is per-EBG-node travel time in seconds → ms.
        let dur_ms = mode_data
            .node_weights
            .get(ebg_id as usize)
            .copied()
            .unwrap_or(0)
            .saturating_mul(1000);
        rows.push(EdgeRow {
            edge_seq: edge_seq as u32,
            osm_from,
            osm_to,
            dur_ms,
            dist_m: node.length_m,
        });
    }
    PairEdges {
        query_idx,
        rows,
        cch_distance: Some(result.distance),
    }
}

/// #438: resolve a coordinate to its K=1 (closest, role-filtered) CCH rank —
/// the same fast-path snap `edges_for_pair` uses. Returns `None` when the
/// closest role-valid node is not in this mode's contracted graph (the case
/// that today triggers the K=64 escalation). Used to GROUP pairs by source
/// rank so one forward search can be shared across a source's targets.
fn resolve_k1_rank(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    lon: f64,
    lat: f64,
    role: super::types::SnapRole,
) -> Option<u32> {
    let role_filter = role.role_filter(mode_data);
    let id = state
        .snap_index
        .snap_filtered_role(lon, lat, mode.0, None, role_filter)?;
    let rank = mode_data.orig_to_rank.get(id as usize).copied()?;
    if rank == u32::MAX { None } else { Some(rank) }
}

/// #438: one target of a source group: its original flat `query_idx`, the raw
/// coords (for the per-pair escalation fallback), and its K=1 destination rank
/// (`None` ⇒ the K=1 dst snap missed → must fall back to per-pair).
pub(crate) struct GroupedTarget {
    pub(crate) query_idx: u32,
    pub(crate) pair: [f64; 4],
    pub(crate) dst_rank: Option<u32>,
}

/// #438: process one source group — settle the forward CCH search ONCE for
/// `src_rank`, then for each target do only the (cheaper) backward search +
/// meeting + unpack. This is where the ~30× forward-recompute is amortised.
///
/// Targets that the shared K=1 forward can't serve (K=1 dst missed, or no path
/// via the K=1 src) fall back to the full per-pair `edges_for_pair` (K=64 + 16
/// combos) — but those fallbacks call `query.query()`, which bumps the forward
/// epoch and ERASES the frozen tree, so they MUST run AFTER all shared-forward
/// targets of the group. Output distances are identical to per-pair; equal-cost
/// paths may differ on ties (both are valid time-shortest paths).
fn process_source_group(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    src_rank: u32,
    targets: &[GroupedTarget],
) -> Vec<PairEdges> {
    let query = super::query::CchQuery::new(mode_data);

    let mut out = Vec::with_capacity(targets.len());
    let mut fallbacks: Vec<&GroupedTarget> = Vec::new();
    // #438: settle + ALL per-target meets under ONE thread-local borrow
    // (per-call EvictableCell ceremony was ~26% of profile samples).
    query.with_meet_group(src_rank, |g| {
        for t in targets {
            match t.dst_rank {
                Some(dst_rank) => match g.meet(dst_rank) {
                    Some(result) => {
                        out.push(emit_pair_rows(
                            state,
                            mode_data,
                            src_rank,
                            dst_rank,
                            &result,
                            t.query_idx,
                        ));
                    }
                    None => fallbacks.push(t),
                },
                None => fallbacks.push(t),
            }
        }
    });
    // Fallbacks LAST — they destroy the frozen forward via query.query().
    for t in fallbacks {
        out.push(edges_for_pair(
            state,
            mode_data,
            mode,
            &query,
            t.query_idx,
            &t.pair,
        ));
    }
    out
}

/// #438: a pair handled OUTSIDE a shared-forward group — either a singleton
/// source (K=1 resolved but only 1 target, so no forward-sharing benefit) or a
/// K=1-source-miss (needs K=64 escalation). Carries any already-resolved K=1
/// ranks so `process_per_pair_work` can skip the redundant snap.
pub(crate) struct PerPairWork {
    pub(crate) query_idx: u32,
    pair: [f64; 4],
    /// K=1 source rank, if it resolved (singleton); `None` ⇒ K=1 src missed.
    src_rank: Option<u32>,
    /// K=1 destination rank, if it resolved.
    dst_rank: Option<u32>,
}

/// #438: process one per-pair work item. When BOTH K=1 ranks are already
/// resolved (the singleton case), try the direct K=1 query — skipping the
/// redundant snap that `edges_for_pair` would otherwise repeat (the #438-review
/// double-snap fix). Anything that misses (no precomputed ranks, or the K=1
/// query doesn't connect) falls through to the full per-pair `edges_for_pair`
/// (K=1 snap + K=64 + 16-combo escalation), byte-identical to today.
pub(crate) fn process_per_pair_work(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    query: &super::query::CchQuery<'_>,
    work: &PerPairWork,
) -> PairEdges {
    match route_per_pair_work(state, mode_data, mode, query, work) {
        Some((src_rank, dst_rank, result)) => emit_pair_rows(
            state,
            mode_data,
            src_rank,
            dst_rank,
            &result,
            work.query_idx,
        ),
        None => PairEdges {
            query_idx: work.query_idx,
            rows: Vec::new(),
            cch_distance: None,
        },
    }
}

/// #460: routing core of [`process_per_pair_work`] WITHOUT row emission —
/// carried-K=1-ranks query, then K=64 escalation. `None` ⇒ unreachable.
pub(crate) fn route_per_pair_work(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    query: &super::query::CchQuery<'_>,
    work: &PerPairWork,
) -> Option<(u32, u32, super::query::QueryResult)> {
    if let (Some(src_rank), Some(dst_rank)) = (work.src_rank, work.dst_rank)
        && let Some(result) = query.query(src_rank, dst_rank)
    {
        return Some((src_rank, dst_rank, result));
    }
    // K=1 missed (or didn't connect) → escalate WITHOUT re-doing the K=1 that
    // the precomputed-rank query above already covered.
    escalate_route(state, mode_data, mode, query, &work.pair)
}

/// #438: resolve each pair's K=1 source/dest rank and partition into
/// `(multi_target_groups, per_pair)`:
/// - sources with **≥2** targets become a shared-forward GROUP (the win);
/// - sources with exactly **1** target, and pairs whose K=1 SOURCE snap missed
///   (need K=64 escalation), go to the per-pair list.
///
/// The singleton split is the #438-review MAJOR fix: a 1-target source can't
/// amortise the forward-settle-to-exhaustion, so routing it per-pair preserves
/// the early-terminated bidirectional cost and avoids a regression on
/// distinct-pair (1 target/source) workloads.
#[allow(clippy::type_complexity)]
pub(crate) fn group_pairs(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    pairs: &[[f64; 4]],
    parallel: bool,
) -> (Vec<(u32, Vec<GroupedTarget>)>, Vec<PerPairWork>) {
    use super::types::SnapRole;

    // Resolve K=1 src/dst ranks for every pair FIRST — embarrassingly parallel,
    // and on a zero-sharing (all-singleton) workload ~half the total work, so
    // running it sequentially before the parallel process phase was an Amdahl
    // bottleneck (#438-review perf cliff). Parallelise it when the batch is
    // parallel; the cheap HashMap grouping afterwards stays sequential.
    let resolve = |idx: usize, pair: &[f64; 4]| {
        let s = resolve_k1_rank(state, mode_data, mode, pair[0], pair[1], SnapRole::Src);
        let d = resolve_k1_rank(state, mode_data, mode, pair[2], pair[3], SnapRole::Dst);
        (idx as u32, *pair, s, d)
    };
    let resolved: Vec<(u32, [f64; 4], Option<u32>, Option<u32>)> = if parallel {
        pairs
            .par_iter()
            .enumerate()
            .map(|(idx, pair)| resolve(idx, pair))
            .collect()
    } else {
        pairs
            .iter()
            .enumerate()
            .map(|(idx, pair)| resolve(idx, pair))
            .collect()
    };

    let mut groups: std::collections::HashMap<u32, Vec<GroupedTarget>> =
        std::collections::HashMap::new();
    let mut per_pair: Vec<PerPairWork> = Vec::new();
    for (query_idx, pair, src, dst) in resolved {
        match src {
            Some(src_rank) => {
                groups.entry(src_rank).or_default().push(GroupedTarget {
                    query_idx,
                    pair,
                    dst_rank: dst,
                });
            }
            // K=1 source miss → per-pair with no precomputed ranks (full snap).
            None => per_pair.push(PerPairWork {
                query_idx,
                pair,
                src_rank: None,
                dst_rank: None,
            }),
        }
    }
    // Singleton groups don't amortise the shared forward → run them per-pair,
    // but CARRY the already-resolved K=1 ranks so the per-pair path skips the
    // redundant snap (#438-review double-snap fix).
    let mut multi: Vec<(u32, Vec<GroupedTarget>)> = Vec::with_capacity(groups.len());
    for (src_rank, targets) in groups {
        if targets.len() >= 2 {
            multi.push((src_rank, targets));
        } else {
            for t in targets {
                per_pair.push(PerPairWork {
                    query_idx: t.query_idx,
                    pair: t.pair,
                    src_rank: Some(src_rank),
                    dst_rank: t.dst_rank,
                });
            }
        }
    }
    // Deterministic work order (Copilot review): HashMap iteration order is
    // random per run; sorting keeps the streamed batch order stable without
    // affecting results (each batch is internally sorted by query_idx already).
    multi.sort_unstable_by_key(|(src_rank, _)| *src_rank);
    per_pair.sort_unstable_by_key(|w| w.query_idx);
    (multi, per_pair)
}

/// #438: resolve + source-group + process every pair into per-pair edge lists,
/// sorted by `query_idx`. The OSRM-gap fix — the forward CCH search depends only
/// on the SOURCE, so pairs are grouped by their resolved K=1 source rank and
/// share ONE forward settle across the source's targets ([`process_source_group`]);
/// singletons + K=1-source-misses run per-pair ([`process_per_pair_work`]).
/// `parallel` fans the resolve, the groups, and the per-pair work across rayon.
///
/// Shared by [`do_edges_batch`] (which streams the result in chunks) and the
/// equivalence test / bench. Returns the SAME `PairEdges` shape as the per-pair
/// path; min-cost distance is identical, equal-cost paths may differ on ties.
pub fn compute_edges_grouped(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    pairs: &[[f64; 4]],
    parallel: bool,
) -> Vec<PairEdges> {
    let (group_vec, per_pair_work) = group_pairs(state, mode_data, mode, pairs, parallel);

    // Source GROUPS are the parallel unit (one shared forward settle per rayon
    // task); per-pair work runs individually. flat_map keeps each group's
    // settle_forward + its targets on a single thread so the frozen forward
    // tree (thread-local) is valid for the whole group.
    let mut per_pair: Vec<PairEdges> = if parallel {
        let mut v: Vec<PairEdges> = group_vec
            .par_iter()
            .flat_map(|(src_rank, targets)| {
                process_source_group(state, mode_data, mode, *src_rank, targets)
            })
            .collect();
        let from_per_pair: Vec<PairEdges> = per_pair_work
            .par_iter()
            .map(|w| {
                let query = super::query::CchQuery::new(mode_data);
                process_per_pair_work(state, mode_data, mode, &query, w)
            })
            .collect();
        v.extend(from_per_pair);
        v
    } else {
        let mut v: Vec<PairEdges> = Vec::with_capacity(pairs.len());
        for (src_rank, targets) in &group_vec {
            v.extend(process_source_group(
                state, mode_data, mode, *src_rank, targets,
            ));
        }
        let query = super::query::CchQuery::new(mode_data);
        for w in &per_pair_work {
            v.push(process_per_pair_work(state, mode_data, mode, &query, w));
        }
        v
    };
    per_pair.sort_unstable_by_key(|p| p.query_idx);
    per_pair
}

/// #438: the pre-grouping per-pair path (each pair an independent bidirectional
/// CCH query). Kept as the equivalence oracle + bench baseline.
pub fn compute_edges_flat(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    pairs: &[[f64; 4]],
    parallel: bool,
) -> Vec<PairEdges> {
    let mut per_pair: Vec<PairEdges> = if parallel {
        pairs
            .par_iter()
            .enumerate()
            .map(|(i, pair)| {
                let query = super::query::CchQuery::new(mode_data);
                edges_for_pair(state, mode_data, mode, &query, i as u32, pair)
            })
            .collect()
    } else {
        let query = super::query::CchQuery::new(mode_data);
        pairs
            .iter()
            .enumerate()
            .map(|(i, pair)| edges_for_pair(state, mode_data, mode, &query, i as u32, pair))
            .collect()
    };
    per_pair.sort_unstable_by_key(|p| p.query_idx);
    per_pair
}

/// #438 Phase 1: per-source predecessor-tracking PHAST TREE. One bounded tree
/// settle per source replaces the per-target backward searches entirely — each
/// target's path is a backtrack + the existing shortcut unpack
/// ([`crate::range::tree_phast`]).
///
/// Bounding: the threshold per source group is `max great-circle distance to
/// any of its targets × DETOUR / SPEED_FLOOR` (a duration metric has no
/// principled geometric bound — codex's #1 risk). Targets the bounded tree
/// misses are retried once at 4× the bound, then fall back to the per-pair
/// path. The equivalence oracle (reachability + total duration identical)
/// gates correctness; retry rate is the number to watch in the bench.
pub fn compute_edges_tree(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    pairs: &[[f64; 4]],
    parallel: bool,
) -> Vec<PairEdges> {
    let (group_vec, per_pair_work) = group_pairs(state, mode_data, mode, pairs, parallel);

    // Locality batching: CCH ranks come from nested dissection, so
    // rank-adjacent sources are spatially close. Sorting groups by source
    // rank before chunking keeps each K-lane batch's union selection tight
    // (an arrival-order batch can mix sources from different cities, and
    // every lane then pays the scan over the union's full ancestry).
    let mut group_refs: Vec<&(u32, Vec<GroupedTarget>)> = group_vec.iter().collect();
    group_refs.sort_unstable_by_key(|(src, _)| *src);
    let batches: Vec<Vec<&(u32, Vec<GroupedTarget>)>> = group_refs
        .chunks(crate::range::tree_phast::TREE_LANES)
        .map(|c| c.to_vec())
        .collect();
    let mut per_pair: Vec<PairEdges> = if parallel {
        let mut v: Vec<PairEdges> = batches
            .par_iter()
            .flat_map(|batch| process_tree_batch(state, mode_data, mode, batch.as_slice()))
            .collect();
        let from_per_pair: Vec<PairEdges> = per_pair_work
            .par_iter()
            .map(|w| {
                let query = super::query::CchQuery::new(mode_data);
                process_per_pair_work(state, mode_data, mode, &query, w)
            })
            .collect();
        v.extend(from_per_pair);
        v
    } else {
        let mut v: Vec<PairEdges> = Vec::with_capacity(pairs.len());
        for batch in &batches {
            v.extend(process_tree_batch(state, mode_data, mode, batch.as_slice()));
        }
        let query = super::query::CchQuery::new(mode_data);
        for w in &per_pair_work {
            v.push(process_per_pair_work(state, mode_data, mode, &query, w));
        }
        v
    };
    per_pair.sort_unstable_by_key(|p| p.query_idx);
    per_pair
}

/// #438 K-lane: process up to TREE_LANES source groups per task — one union
/// selection + ONE restricted scan shared by all lanes (the scan was 73% of
/// tree CPU). Per lane: backtrack its targets off the UP-tree snapshot
/// taken at settle time (no resweep — PR #461).
/// Misses fall back to the per-pair path (separate scratch, safe after the
/// lane work).
fn process_tree_batch(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    mode: Mode,
    batch: &[&(u32, Vec<GroupedTarget>)],
) -> Vec<PairEdges> {
    use crate::range::tree_phast::{TreeSettle, tree_lane_backtrack, tree_settle_restricted_batch};
    let sources: Vec<u32> = batch.iter().map(|(s, _)| *s).collect();
    let union_targets: Vec<u32> = batch
        .iter()
        .flat_map(|(_, ts)| ts.iter().filter_map(|t| t.dst_rank))
        .collect();
    let n_pairs: usize = batch.iter().map(|(_, ts)| ts.len()).sum();
    let mut out = Vec::with_capacity(n_pairs);
    let mut fallbacks: Vec<&GroupedTarget> = Vec::new();

    let settled = !union_targets.is_empty()
        && tree_settle_restricted_batch(
            &mode_data.cch_topo,
            &mode_data.cch_weights,
            &mode_data.down_rev_flat,
            &sources,
            &union_targets,
        ) == TreeSettle::Ok;

    for (k, (src_rank, targets)) in batch.iter().enumerate() {
        for t in targets {
            let hit = if settled {
                t.dst_rank
                    .and_then(|dst| tree_lane_backtrack(&mode_data.cch_topo, k, *src_rank, dst))
            } else {
                None
            };
            match (hit, t.dst_rank) {
                (Some(tp), Some(dst_rank)) => {
                    let result = super::query::QueryResult {
                        distance: tp.distance,
                        meeting_node: tp.apex,
                        forward_parent: tp.forward_parent,
                        backward_parent: tp.backward_parent,
                    };
                    out.push(emit_pair_rows(
                        state,
                        mode_data,
                        *src_rank,
                        dst_rank,
                        &result,
                        t.query_idx,
                    ));
                }
                _ => fallbacks.push(t),
            }
        }
    }
    let query = super::query::CchQuery::new(mode_data);
    for t in fallbacks {
        out.push(edges_for_pair(
            state,
            mode_data,
            mode,
            &query,
            t.query_idx,
            &t.pair,
        ));
    }
    out
}

/// #462: pick the dispatch result of the FIRST pair whose endpoints both
/// snap into a region. One unsnappable pair must not poison the batch:
/// `NoRegion` pairs are skipped here and emit the documented all-null
/// unreachable row downstream — their per-pair snap against the picked
/// region misses the same way the region dispatch did (both are bounded
/// by `MAX_SNAP_DISTANCE_M`). `InvalidMode` and `CrossRegion` keep their
/// request-level semantics: the former is a caller typo on the whole
/// request (and fires on the first pair regardless of snapping), the
/// latter marks a genuinely cross-region workload (#336 follow-up) that
/// silently emitting nulls would hide.
///
/// Returns `Ok(None)` when EVERY pair fails to snap into any region —
/// the caller emits the all-null row for all pairs instead of erroring.
///
/// Generic over the dispatch closure so the skip/stop decision table is
/// unit-testable without a loaded `RegionsState`.
fn first_dispatchable_pair<T>(
    pairs: &[[f64; 4]],
    mut dispatch: impl FnMut(&[f64; 4]) -> std::result::Result<T, super::regions::DispatchError>,
) -> std::result::Result<Option<T>, Status> {
    for pair in pairs {
        match dispatch(pair) {
            Ok(hit) => return Ok(Some(hit)),
            Err(super::regions::DispatchError::NoRegion { .. }) => continue,
            Err(e) => return Err(dispatch_to_status(e)),
        }
    }
    Ok(None)
}

/// #462: one all-null unreachable row per pair, chunked into row-bounded
/// RecordBatches. Used when NO pair in an `edges_batch` request could be
/// dispatched to any region (e.g. every coordinate outside coverage):
/// the per-pair contract from [`edges_batch_schema`] ("unreachable pairs
/// emit a single row with the edge columns null") applies to the whole
/// batch instead of failing the request.
fn all_null_edges_batches(n_pairs: usize) -> Vec<std::result::Result<RecordBatch, Status>> {
    let schema = Arc::new(edges_batch_schema());
    const ROWS_PER_BATCH: usize = 20_000;
    let mut batches = Vec::with_capacity(n_pairs.div_ceil(ROWS_PER_BATCH));
    let mut start = 0usize;
    while start < n_pairs {
        let end = (start + ROWS_PER_BATCH).min(n_pairs);
        let n = end - start;
        let mut query_idx_b = UInt32Builder::with_capacity(n);
        let mut target_idx_b = UInt32Builder::with_capacity(n);
        let mut edge_seq_b = UInt32Builder::with_capacity(n);
        let mut osm_from_b = Int64Builder::with_capacity(n);
        let mut osm_to_b = Int64Builder::with_capacity(n);
        let mut dur_ms_b = UInt32Builder::with_capacity(n);
        let mut dist_m_b = UInt32Builder::with_capacity(n);
        for query_idx in start..end {
            query_idx_b.append_value(query_idx as u32);
            target_idx_b.append_value(0);
            edge_seq_b.append_null();
            osm_from_b.append_null();
            osm_to_b.append_null();
            dur_ms_b.append_null();
            dist_m_b.append_null();
        }
        batches.push(
            RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(query_idx_b.finish()) as ArrayRef,
                    Arc::new(target_idx_b.finish()),
                    Arc::new(edge_seq_b.finish()),
                    Arc::new(osm_from_b.finish()),
                    Arc::new(osm_to_b.finish()),
                    Arc::new(dur_ms_b.finish()),
                    Arc::new(dist_m_b.finish()),
                ],
            )
            .map_err(|e| Status::internal(format!("edges_batch all-null Arrow build: {e}"))),
        );
        start = end;
    }
    batches
}

pub fn do_edges_batch(
    state: &Arc<ServerState>,
    mode: Mode,
    params: EdgesBatchParams,
) -> std::result::Result<BatchStream, Status> {
    if params.pairs.is_empty() {
        let schema = Arc::new(edges_batch_schema());
        let empty = RecordBatch::new_empty(schema);
        return Ok(Box::pin(stream::once(async move { Ok(empty) })));
    }
    if params.pairs.len() > 500_000 {
        return Err(Status::invalid_argument(
            "max 500,000 pairs per edges_batch request",
        ));
    }
    for (i, pair) in params.pairs.iter().enumerate() {
        validate_coord(pair[0], pair[1], &format!("pair[{i}].src"))?;
        validate_coord(pair[2], pair[3], &format!("pair[{i}].dst"))?;
    }

    let state = Arc::clone(state);
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<RecordBatch, Status>>(8);

    tokio::task::spawn_blocking(move || {
        let mode_data = state.get_mode(mode);
        let schema = Arc::new(edges_batch_schema());
        const MIN_PARALLEL_PAIRS: usize = 256;
        let parallel = params.pairs.len() >= MIN_PARALLEL_PAIRS;

        // #438: partition into multi-target source GROUPS (shared forward) +
        // per-pair pairs (singletons + K=1-src misses), then process + STREAM in
        // work-chunks of ~CHUNK_PAIRS. Chunked streaming keeps resident memory
        // bounded to one chunk even at the 500k-pair cap — the #438-review
        // BLOCKER fix: we no longer materialise EVERY pair's edge rows before
        // emitting (which was unbounded by the pair cap for long routes).
        let (group_vec, per_pair_work) =
            group_pairs(&state, &mode_data, mode, &params.pairs, parallel);
        // Work-chunk size in PAIRS. Bounds peak ≈ CHUNK_PAIRS × path_len × 32B
        // (~2 MB typical, ~200 MB worst-case long routes) vs unbounded before.
        const CHUNK_PAIRS: usize = 2048;
        const ROWS_PER_BATCH: usize = 20_000;

        // Build + stream row-bounded RecordBatches from one work-chunk's results
        // (sorted by query_idx). Returns false on client disconnect / Arrow
        // error so the caller stops. `per_pair` is dropped after emit, freeing
        // the chunk's rows before the next chunk is computed.
        let emit = |mut per_pair: Vec<PairEdges>| -> bool {
            per_pair.sort_unstable_by_key(|p| p.query_idx);
            let mut idx = 0usize;
            while idx < per_pair.len() {
                let mut end = idx;
                let mut rows_in_batch = 0usize;
                while end < per_pair.len() && rows_in_batch < ROWS_PER_BATCH {
                    rows_in_batch += per_pair[end].rows.len().max(1);
                    end += 1;
                }
                let mut query_idx_b = UInt32Builder::with_capacity(rows_in_batch);
                let mut target_idx_b = UInt32Builder::with_capacity(rows_in_batch);
                let mut edge_seq_b = UInt32Builder::with_capacity(rows_in_batch);
                let mut osm_from_b = Int64Builder::with_capacity(rows_in_batch);
                let mut osm_to_b = Int64Builder::with_capacity(rows_in_batch);
                let mut dur_ms_b = UInt32Builder::with_capacity(rows_in_batch);
                let mut dist_m_b = UInt32Builder::with_capacity(rows_in_batch);

                for pe in &per_pair[idx..end] {
                    if pe.rows.is_empty() {
                        query_idx_b.append_value(pe.query_idx);
                        target_idx_b.append_value(0);
                        edge_seq_b.append_null();
                        osm_from_b.append_null();
                        osm_to_b.append_null();
                        dur_ms_b.append_null();
                        dist_m_b.append_null();
                    } else {
                        for row in &pe.rows {
                            query_idx_b.append_value(pe.query_idx);
                            target_idx_b.append_value(0);
                            edge_seq_b.append_value(row.edge_seq);
                            osm_from_b.append_value(row.osm_from);
                            osm_to_b.append_value(row.osm_to);
                            dur_ms_b.append_value(row.dur_ms);
                            dist_m_b.append_value(row.dist_m);
                        }
                    }
                }
                idx = end;

                let batch = match RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(query_idx_b.finish()) as ArrayRef,
                        Arc::new(target_idx_b.finish()),
                        Arc::new(edge_seq_b.finish()),
                        Arc::new(osm_from_b.finish()),
                        Arc::new(osm_to_b.finish()),
                        Arc::new(dur_ms_b.finish()),
                        Arc::new(dist_m_b.finish()),
                    ],
                ) {
                    Ok(b) => b,
                    Err(e) => {
                        let _ = tx.blocking_send(Err(Status::internal(format!(
                            "edges_batch Arrow build: {e}"
                        ))));
                        return false;
                    }
                };
                if tx.blocking_send(Ok(batch)).is_err() {
                    return false; // Client disconnected.
                }
            }
            true
        };

        // Phase A: multi-target groups, chunked by accumulated target count so
        // each chunk holds ~CHUNK_PAIRS pairs. flat_map keeps each group's
        // each group's settle (K-lane tree — the adaptive dispatch is retired) +
        // its targets on one rayon thread (thread-local state stays valid).
        let mut gi = 0usize;
        while gi < group_vec.len() {
            let mut pairs_in_chunk = 0usize;
            let mut gj = gi;
            while gj < group_vec.len() && pairs_in_chunk < CHUNK_PAIRS {
                pairs_in_chunk += group_vec[gj].1.len();
                gj += 1;
            }
            let chunk = &group_vec[gi..gj];
            gi = gj;
            // #438: every group rides the K-lane tree — post-interleave/
            // snapshot/locality the tree beats the grouped path on BOTH
            // workload shapes (B 2.55×, C 1.40× vs grouped, oracle green),
            // so the old 15 km adaptive dispatch is retired. Rank-sort for
            // locality (ND ranks are spatially coherent → tight unions).
            let mut group_refs: Vec<&(u32, Vec<GroupedTarget>)> = chunk.iter().collect();
            group_refs.sort_unstable_by_key(|(src, _)| *src);
            let batches: Vec<Vec<&(u32, Vec<GroupedTarget>)>> = group_refs
                .chunks(crate::range::tree_phast::TREE_LANES)
                .map(|c| c.to_vec())
                .collect();
            let per_pair: Vec<PairEdges> = if parallel {
                batches
                    .par_iter()
                    .flat_map_iter(|b| process_tree_batch(&state, &mode_data, mode, b))
                    .collect()
            } else {
                batches
                    .iter()
                    .flat_map(|b| process_tree_batch(&state, &mode_data, mode, b))
                    .collect()
            };
            if !emit(per_pair) {
                return;
            }
        }

        // Phase B: per-pair work (singletons with cached ranks + K=1-src
        // misses), chunked.
        for chunk in per_pair_work.chunks(CHUNK_PAIRS) {
            let per_pair: Vec<PairEdges> = if parallel {
                chunk
                    .par_iter()
                    .map(|w| {
                        let query = super::query::CchQuery::new(&mode_data);
                        process_per_pair_work(&state, &mode_data, mode, &query, w)
                    })
                    .collect()
            } else {
                let query = super::query::CchQuery::new(&mode_data);
                chunk
                    .iter()
                    .map(|w| process_per_pair_work(&state, &mode_data, mode, &query, w))
                    .collect()
            };
            if !emit(per_pair) {
                return;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(stream))
}

// =============================================================================
// transit_bulk endpoint (#119)
// =============================================================================

/// Parameters for the `transit_bulk` Flight action. Mirror the JSON
/// shape of `TransitBulkRequest` from the Axum REST endpoint so REST
/// and Flight clients share the same request schema.
#[derive(Deserialize)]
pub struct TransitBulkParams {
    pub queries: Vec<super::transit_handler::TransitRequest>,
    /// Optional batch defaults — applied to any query that omits the
    /// field. Same semantics as `TransitBulkRequest`.
    #[serde(default)]
    pub max_walk_m: Option<u32>,
    #[serde(default)]
    pub access_mode: Option<String>,
    #[serde(default)]
    pub egress_mode: Option<String>,
}

pub fn do_transit_bulk(
    state: &Arc<ServerState>,
    params: TransitBulkParams,
) -> std::result::Result<BatchStream, Status> {
    if state.transit.is_none() {
        return Err(Status::failed_precondition(
            "transit subsystem is not loaded",
        ));
    }
    if params.queries.is_empty() {
        let schema = Arc::new(transit_bulk_schema());
        let empty = RecordBatch::new_empty(schema);
        return Ok(Box::pin(stream::once(async move { Ok(empty) })));
    }
    // Soft cap: 500k queries — Flight streaming has no URL-length
    // bottleneck so we can go larger than the JSON `/transit/bulk`
    // limit (100k).
    if params.queries.len() > 500_000 {
        return Err(Status::invalid_argument(
            "max 500,000 queries per transit_bulk request",
        ));
    }

    let state = Arc::clone(state);
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<RecordBatch, Status>>(8);

    tokio::task::spawn_blocking(move || {
        // Apply per-batch defaults to every query that omits the field.
        let mut queries = params.queries;
        let batch_max_walk_m = params.max_walk_m;
        let batch_access_mode = params.access_mode.clone();
        let batch_egress_mode = params.egress_mode.clone();
        for q in &mut queries {
            if q.max_walk_m.is_none() && batch_max_walk_m.is_some() {
                q.max_walk_m = batch_max_walk_m;
            }
            if q.access_mode.is_none() && batch_access_mode.is_some() {
                q.access_mode = batch_access_mode.clone();
            }
            if q.egress_mode.is_none() && batch_egress_mode.is_some() {
                q.egress_mode = batch_egress_mode.clone();
            }
        }

        let schema = Arc::new(transit_bulk_schema());
        // Chunk size: 1024 rows per RecordBatch is the sweet spot for
        // Arrow streaming — small enough for low latency-to-first-byte
        // on slow networks, large enough to amortise per-batch
        // serialisation overhead.
        const CHUNK: usize = 1024;

        // Indexed parallel evaluation: every query keeps its position
        // so the output rows are stable across Rayon thread reordering.
        // Process one CHUNK at a time so we can stream RecordBatches
        // incrementally instead of waiting for the whole batch.
        for (chunk_start, chunk) in queries
            .chunks(CHUNK)
            .enumerate()
            .map(|(ci, c)| (ci * CHUNK, c))
        {
            // Per-query results in original order. Each entry is
            // either Ok(response) or Err((http_status, error_msg)).
            let chunk_results: Vec<
                std::result::Result<super::transit_handler::TransitResponse, (u16, String)>,
            > = chunk
                .par_iter()
                .map(|q| {
                    super::transit_handler::compute_transit_journey(state.as_ref(), q)
                        .map_err(|(sc, err)| (sc.as_u16(), err.0.error.clone()))
                })
                .collect();

            let n = chunk_results.len();
            let mut query_idx_b = UInt32Builder::with_capacity(n);
            let mut status_b = StringBuilder::with_capacity(n, n * 4);
            let mut http_status_b = UInt16Builder::with_capacity(n);
            let mut error_b = StringBuilder::with_capacity(n, n * 16);
            let mut origin_lon_b = Float64Builder::with_capacity(n);
            let mut origin_lat_b = Float64Builder::with_capacity(n);
            let mut destination_lon_b = Float64Builder::with_capacity(n);
            let mut destination_lat_b = Float64Builder::with_capacity(n);
            let mut depart_b = StringBuilder::with_capacity(n, n * 8);
            let mut arrival_b = StringBuilder::with_capacity(n, n * 8);
            let mut total_dur_b = UInt32Builder::with_capacity(n);
            let mut access_mode_b = StringBuilder::with_capacity(n, n * 8);
            let mut egress_mode_b = StringBuilder::with_capacity(n, n * 8);
            let mut legs_json_b = StringBuilder::with_capacity(n, n * 256);

            for (i, result) in chunk_results.iter().enumerate() {
                let qi = (chunk_start + i) as u32;
                let req = &chunk[i];
                query_idx_b.append_value(qi);
                origin_lon_b.append_value(req.origin_lon);
                origin_lat_b.append_value(req.origin_lat);
                destination_lon_b.append_value(req.destination_lon);
                destination_lat_b.append_value(req.destination_lat);
                match result {
                    Ok(resp) => {
                        status_b.append_value("ok");
                        http_status_b.append_value(200);
                        error_b.append_null();
                        depart_b.append_value(&resp.depart_time);
                        arrival_b.append_value(&resp.arrival_time);
                        total_dur_b.append_value(resp.total_duration_s);
                        access_mode_b.append_value(&resp.access_mode);
                        egress_mode_b.append_value(&resp.egress_mode);
                        // Serialize just the `legs` field — the
                        // metadata columns above already carry the
                        // top-level response fields. Falling back to
                        // an empty array on a (theoretically
                        // impossible) serde error so the row stays
                        // emit-able.
                        match serde_json::to_string(&resp.legs) {
                            Ok(s) => legs_json_b.append_value(&s),
                            Err(_) => legs_json_b.append_value("[]"),
                        }
                    }
                    Err((sc, msg)) => {
                        status_b.append_value("err");
                        http_status_b.append_value(*sc);
                        error_b.append_value(msg);
                        depart_b.append_null();
                        arrival_b.append_null();
                        total_dur_b.append_null();
                        access_mode_b.append_null();
                        egress_mode_b.append_null();
                        legs_json_b.append_null();
                    }
                }
            }

            let batch = match RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(query_idx_b.finish()) as ArrayRef,
                    Arc::new(status_b.finish()),
                    Arc::new(http_status_b.finish()),
                    Arc::new(error_b.finish()),
                    Arc::new(origin_lon_b.finish()),
                    Arc::new(origin_lat_b.finish()),
                    Arc::new(destination_lon_b.finish()),
                    Arc::new(destination_lat_b.finish()),
                    Arc::new(depart_b.finish()),
                    Arc::new(arrival_b.finish()),
                    Arc::new(total_dur_b.finish()),
                    Arc::new(access_mode_b.finish()),
                    Arc::new(egress_mode_b.finish()),
                    Arc::new(legs_json_b.finish()),
                ],
            ) {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx.blocking_send(Err(Status::internal(format!(
                        "transit_bulk Arrow build: {e}"
                    ))));
                    return;
                }
            };

            if tx.blocking_send(Ok(batch)).is_err() {
                // Client disconnected — bail.
                return;
            }
        }
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(stream))
}

// =============================================================================
// Helper: encode RecordBatch stream to FlightData stream
// =============================================================================

type FlightDataStream =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<FlightData, Status>> + Send>>;

fn batches_to_flight_data(schema: SchemaRef, batch_stream: BatchStream) -> FlightDataStream {
    let flight_stream = FlightDataEncoderBuilder::new().with_schema(schema).build(
        batch_stream.map(|r| r.map_err(|e| arrow_flight::error::FlightError::Tonic(Box::new(e)))),
    );

    Box::pin(flight_stream.map(|item| {
        item.map_err(|e| match e {
            arrow_flight::error::FlightError::Tonic(s) => *s,
            other => Status::internal(other.to_string()),
        })
    }))
}

/// Encode an Arrow Schema to IPC bytes for SchemaResult
fn schema_to_ipc_bytes(schema: &Schema) -> std::result::Result<Bytes, Status> {
    let mut buf = Vec::new();
    {
        let schema_ref = Arc::new(schema.clone());
        let mut writer = StreamWriter::try_new(&mut buf, &schema_ref)
            .map_err(|e| Status::internal(format!("IPC write error: {}", e)))?;
        writer
            .finish()
            .map_err(|e| Status::internal(format!("IPC finish error: {}", e)))?;
    }
    Ok(Bytes::from(buf))
}

// =============================================================================
// Catchment via DoExchange
// =============================================================================

fn catchment_schema() -> Schema {
    super::catchment::catchment_arrow_schema()
}

/// Process a catchment DoExchange request.
///
/// Input: flat denormalized Arrow table (store_id, store_lon, store_lat, client_lon, client_lat).
/// Output: per-store × per-percentile polygon results.
/// #460 `edges_flow` DoExchange: weighted OD pairs in, accumulated
/// per-edge flow out. Input columns: `src_lon, src_lat, dst_lon, dst_lat`
/// (f64, required), `weight` (f64, optional — default 1.0), `group`
/// (u32, optional — default 0). Output: [`edges_flow_schema`] rows sorted
/// by `(group, osm_node_from, osm_node_to)`, then one empty FlightData
/// whose `app_metadata` is the JSON conservation summary.
async fn do_exchange_edges_flow(
    state: Arc<ServerState>,
    mode: Mode,
    batches: &[RecordBatch],
) -> std::result::Result<
    Response<Pin<Box<dyn futures::Stream<Item = std::result::Result<FlightData, Status>> + Send>>>,
    Status,
> {
    const MAX_FLOW_PAIRS: usize = 2_000_000;

    let mut pairs: Vec<[f64; 4]> = Vec::new();
    let mut weights: Vec<f64> = Vec::new();
    let mut groups: Vec<u32> = Vec::new();

    for batch in batches {
        let f64_col = |name: &str| -> std::result::Result<&Float64Array, Status> {
            batch
                .column_by_name(name)
                .ok_or_else(|| Status::invalid_argument(format!("missing '{name}'")))?
                .as_any()
                .downcast_ref::<Float64Array>()
                .ok_or_else(|| Status::invalid_argument(format!("{name} must be f64")))
        };
        let slon = f64_col("src_lon")?;
        let slat = f64_col("src_lat")?;
        let dlon = f64_col("dst_lon")?;
        let dlat = f64_col("dst_lat")?;
        // weight / group are optional with documented defaults.
        let weight = match batch.column_by_name("weight") {
            Some(c) => Some(
                c.as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| Status::invalid_argument("weight must be f64"))?,
            ),
            None => None,
        };
        let group = match batch.column_by_name("group") {
            Some(c) => Some(
                c.as_any()
                    .downcast_ref::<arrow::array::UInt32Array>()
                    .ok_or_else(|| Status::invalid_argument("group must be u32"))?,
            ),
            None => None,
        };

        // Null handling (Copilot review on #463): coordinate columns must
        // be null-free (Arrow `.value()` on a null slot returns garbage);
        // a null weight/group falls back to its documented default.
        for (name, arr) in [
            ("src_lon", slon),
            ("src_lat", slat),
            ("dst_lon", dlon),
            ("dst_lat", dlat),
        ] {
            if arr.null_count() > 0 {
                return Err(Status::invalid_argument(format!(
                    "{name} must not contain nulls ({} null rows)",
                    arr.null_count()
                )));
            }
        }
        for i in 0..batch.num_rows() {
            let pair = [slon.value(i), slat.value(i), dlon.value(i), dlat.value(i)];
            validate_coord(pair[0], pair[1], &format!("row[{i}].src"))?;
            validate_coord(pair[2], pair[3], &format!("row[{i}].dst"))?;
            let w = match weight {
                Some(a) if !a.is_null(i) => a.value(i),
                _ => 1.0,
            };
            if !w.is_finite() || w < 0.0 {
                return Err(Status::invalid_argument(format!(
                    "row[{i}].weight must be finite and >= 0 (got {w})"
                )));
            }
            pairs.push(pair);
            weights.push(w);
            groups.push(match group {
                Some(a) if !a.is_null(i) => a.value(i),
                _ => 0,
            });
        }
        if pairs.len() > MAX_FLOW_PAIRS {
            return Err(Status::invalid_argument(format!(
                "max {MAX_FLOW_PAIRS} pairs per edges_flow request"
            )));
        }
    }
    if pairs.is_empty() {
        return Err(Status::invalid_argument("no input rows"));
    }

    let schema = Arc::new(edges_flow_schema());
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<RecordBatch, Status>>(32);
    let (sum_tx, sum_rx) = tokio::sync::oneshot::channel::<String>();

    let schema_clone = schema.clone();
    tokio::task::spawn_blocking(move || {
        let start = std::time::Instant::now();
        let n_pairs = pairs.len();
        let mode_data = state.get_mode(mode);
        let (rows, summary) = super::flow::compute_edges_flow(
            &state, &mode_data, mode, &pairs, &weights, &groups, true,
        );
        tracing::info!(
            n_pairs,
            n_rows = rows.len(),
            n_unreachable = summary.n_unreachable,
            total_weight_in = summary.total_weight_in,
            total_weight_assigned = summary.total_weight_assigned,
            elapsed_s = start.elapsed().as_secs_f64(),
            "do_exchange edges_flow"
        );

        const ROWS_PER_BATCH: usize = 65_536;
        for chunk in rows.chunks(ROWS_PER_BATCH) {
            let mut g_b = UInt32Builder::with_capacity(chunk.len());
            let mut from_b = Int64Builder::with_capacity(chunk.len());
            let mut to_b = Int64Builder::with_capacity(chunk.len());
            let mut flow_b = Float64Builder::with_capacity(chunk.len());
            for r in chunk {
                g_b.append_value(r.group);
                from_b.append_value(r.osm_from);
                to_b.append_value(r.osm_to);
                flow_b.append_value(r.flow);
            }
            let batch = RecordBatch::try_new(
                schema_clone.clone(),
                vec![
                    Arc::new(g_b.finish()),
                    Arc::new(from_b.finish()),
                    Arc::new(to_b.finish()),
                    Arc::new(flow_b.finish()),
                ],
            )
            .map_err(|e| Status::internal(format!("batch build error: {e}")));
            if tx.blocking_send(batch).is_err() {
                return; // client went away — cooperative cancel
            }
        }
        let summary_json = format!(
            r#"{{"n_pairs":{},"n_unreachable":{},"total_weight_in":{},"total_weight_assigned":{}}}"#,
            summary.n_pairs,
            summary.n_unreachable,
            summary.total_weight_in,
            summary.total_weight_assigned
        );
        let _ = sum_tx.send(summary_json);
    });

    let batch_stream: BatchStream = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    let flight_stream = batches_to_flight_data(schema, batch_stream);
    // Trailing summary message: empty body, JSON app_metadata.
    let trailer = futures::stream::once(async move {
        let json = sum_rx.await.unwrap_or_default();
        Ok(FlightData {
            app_metadata: json.into_bytes().into(),
            ..Default::default()
        })
    });
    Ok(Response::new(Box::pin(flight_stream.chain(trailer))))
}

async fn do_exchange_catchment(
    state: Arc<ServerState>,
    mode: Mode,
    params: super::catchment::CatchmentParams,
    batches: &[RecordBatch],
) -> std::result::Result<
    Response<Pin<Box<dyn futures::Stream<Item = std::result::Result<FlightData, Status>> + Send>>>,
    Status,
> {
    // Extract columns from all batches
    let mut store_ids_all: Vec<String> = Vec::new();
    let mut store_lons_all: Vec<f64> = Vec::new();
    let mut store_lats_all: Vec<f64> = Vec::new();
    let mut client_lons_all: Vec<f64> = Vec::new();
    let mut client_lats_all: Vec<f64> = Vec::new();

    for batch in batches {
        let sid = batch
            .column_by_name("store_id")
            .ok_or_else(|| Status::invalid_argument("missing 'store_id'"))?
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Status::invalid_argument("store_id must be utf8"))?;
        let slon = batch
            .column_by_name("store_lon")
            .ok_or_else(|| Status::invalid_argument("missing 'store_lon'"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Status::invalid_argument("store_lon must be f64"))?;
        let slat = batch
            .column_by_name("store_lat")
            .ok_or_else(|| Status::invalid_argument("missing 'store_lat'"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Status::invalid_argument("store_lat must be f64"))?;
        let clon = batch
            .column_by_name("client_lon")
            .ok_or_else(|| Status::invalid_argument("missing 'client_lon'"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Status::invalid_argument("client_lon must be f64"))?;
        let clat = batch
            .column_by_name("client_lat")
            .ok_or_else(|| Status::invalid_argument("missing 'client_lat'"))?
            .as_any()
            .downcast_ref::<Float64Array>()
            .ok_or_else(|| Status::invalid_argument("client_lat must be f64"))?;

        for i in 0..batch.num_rows() {
            store_ids_all.push(sid.value(i).to_string());
            store_lons_all.push(slon.value(i));
            store_lats_all.push(slat.value(i));
            client_lons_all.push(clon.value(i));
            client_lats_all.push(clat.value(i));
        }
    }

    let total_links = store_ids_all.len();

    // Group by store_id -> (store_lon, store_lat, Vec<(client_lon, client_lat)>)
    type StoreEntry = (f64, f64, Vec<(f64, f64)>);
    let mut store_map: std::collections::HashMap<String, StoreEntry> =
        std::collections::HashMap::new();
    for i in 0..total_links {
        let entry = store_map.entry(store_ids_all[i].clone()).or_insert((
            store_lons_all[i],
            store_lats_all[i],
            Vec::new(),
        ));
        entry.2.push((client_lons_all[i], client_lats_all[i]));
    }

    type StoreRecord = (String, f64, f64, Vec<(f64, f64)>);
    let mut store_list: Vec<StoreRecord> = store_map
        .into_iter()
        .map(|(id, (lon, lat, clients))| (id, lon, lat, clients))
        .collect();
    store_list.sort_by(|a, b| a.0.cmp(&b.0)); // deterministic order by store_id
    let n_stores = store_list.len();

    let schema = Arc::new(catchment_schema());
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<RecordBatch, Status>>(32);

    let schema_clone = schema.clone();
    tokio::task::spawn_blocking(move || {
        let start = std::time::Instant::now();
        tracing::info!(
            n_stores = n_stores,
            total_links = total_links,
            "do_exchange catchment"
        );

        let mode_data = state.get_mode(mode);
        let n_nodes = mode_data.cch_topo.n_nodes as usize;

        // Process stores sequentially (each does a Bucket M2M + catchment)
        let mut store_idx_b = UInt32Builder::new();
        let mut store_id_b = StringBuilder::new();
        let mut pct_b = Float32Builder::new();
        let mut thresh_b = Float32Builder::new();
        let mut covered_b = UInt32Builder::new();
        let mut total_b = UInt32Builder::new();
        let mut wkb_b = BinaryBuilder::new();

        for (si, (sid, slon, slat, client_coords)) in store_list.iter().enumerate() {
            if client_coords.is_empty() {
                continue;
            }

            // Lazy snap (#368 pattern): K=1 primary upfront for store
            // and all clients; K=64 escalation lives in the INF-cell
            // fallback below.
            const SNAP_K: usize = 64;
            let store_rank = match super::snap_kbest::snap_primary_role(
                &state,
                &mode_data,
                mode,
                *slon,
                *slat,
                super::types::SnapRole::Src,
                None,
            ) {
                Some((_, r)) => r,
                None => continue,
            };

            let mut client_ranks: Vec<u32> = Vec::with_capacity(client_coords.len());
            let mut client_valid: Vec<usize> = Vec::with_capacity(client_coords.len());
            for (ci, &(clon, clat)) in client_coords.iter().enumerate() {
                if let Some((_, r)) = super::snap_kbest::snap_primary_role(
                    &state,
                    &mode_data,
                    mode,
                    clon,
                    clat,
                    super::types::SnapRole::Dst,
                    None,
                ) {
                    client_ranks.push(r);
                    client_valid.push(ci);
                }
            }

            if client_ranks.is_empty() {
                continue;
            }

            // 1-to-N matrix
            let (mut matrix, _stats) = table_bucket_full_flat(
                n_nodes,
                &mode_data.up_adj_flat,
                &mode_data.down_rev_flat,
                &[store_rank],
                &client_ranks,
            );

            // Per-cell K-best fallback for INF cells (mirrors /table POST).
            // K=64 escalation runs only for client indices whose 1-to-N
            // cell came back u32::MAX.
            if matrix.contains(&u32::MAX) {
                use rayon::prelude::*;
                let query = super::query::CchQuery::new(&mode_data);
                let store_kbest = super::snap_kbest::snap_k_pair_role(
                    &state,
                    &mode_data,
                    mode,
                    *slon,
                    *slat,
                    super::types::SnapRole::Src,
                    None,
                    SNAP_K,
                );
                let failing: Vec<usize> = (0..client_valid.len())
                    .filter(|&ti| matrix[ti] == u32::MAX)
                    .collect();
                let client_kbest: Vec<(usize, Vec<u32>)> = failing
                    .par_iter()
                    .map(|&ti| {
                        let ci = client_valid[ti];
                        let (clon, clat) = client_coords[ci];
                        let snap = super::snap_kbest::snap_k_pair_role(
                            &state,
                            &mode_data,
                            mode,
                            clon,
                            clat,
                            super::types::SnapRole::Dst,
                            None,
                            SNAP_K,
                        );
                        (ti, snap.ranks)
                    })
                    .collect();
                let patches: Vec<(usize, u32)> = client_kbest
                    .par_iter()
                    .filter_map(|(ti, dst_ranks)| {
                        super::snap_kbest::p2p_with_kbest_fallback(
                            &query,
                            &store_kbest.ranks,
                            dst_ranks,
                            super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
                        )
                        .map(|(_, _, r)| (*ti, r.distance))
                    })
                    .collect();
                for (ti, dist) in patches {
                    matrix[ti] = dist;
                }
            }

            let mut clients_with_dt: Vec<super::catchment::Client> = Vec::new();
            for (ti, &ci) in client_valid.iter().enumerate() {
                let d = matrix[ti];
                if d != u32::MAX {
                    // d is already seconds (post-#297).
                    let duration_s = d as f32;
                    clients_with_dt.push(super::catchment::Client {
                        lon: client_coords[ci].0,
                        lat: client_coords[ci].1,
                        duration_s,
                    });
                }
            }

            let store_coord = (*slon, *slat);
            let mut catch_results = super::catchment::compute_catchment(
                &state,
                mode,
                store_coord,
                &clients_with_dt,
                &params,
            );

            for r in &mut catch_results {
                r.store_idx = si as u32;
            }

            for r in &catch_results {
                store_idx_b.append_value(r.store_idx);
                store_id_b.append_value(sid);
                pct_b.append_value(r.percentile);
                thresh_b.append_value(r.threshold_s);
                covered_b.append_value(r.clients_covered);
                total_b.append_value(r.clients_total);
                wkb_b.append_value(&r.polygon_wkb);
            }
        }

        if store_idx_b.len() > 0
            && let Ok(batch) = RecordBatch::try_new(
                schema_clone,
                vec![
                    Arc::new(store_idx_b.finish()),
                    Arc::new(store_id_b.finish()),
                    Arc::new(pct_b.finish()),
                    Arc::new(thresh_b.finish()),
                    Arc::new(covered_b.finish()),
                    Arc::new(total_b.finish()),
                    Arc::new(wkb_b.finish()),
                ],
            )
        {
            let _ = tx.blocking_send(Ok(batch));
        }

        tracing::info!(
            elapsed_s = start.elapsed().as_secs_f64(),
            "do_exchange catchment done"
        );
    });

    let batch_stream: BatchStream = Box::pin(tokio_stream::wrappers::ReceiverStream::new(rx));
    let flight_stream = batches_to_flight_data(schema, batch_stream);
    Ok(Response::new(flight_stream))
}

// =============================================================================
// FlightService trait implementation
// =============================================================================

#[tonic::async_trait]
impl FlightService for ButterflyFlight {
    type HandshakeStream =
        Pin<Box<dyn futures::Stream<Item = std::result::Result<HandshakeResponse, Status>> + Send>>;
    type ListFlightsStream =
        Pin<Box<dyn futures::Stream<Item = std::result::Result<FlightInfo, Status>> + Send>>;
    type DoGetStream = FlightDataStream;
    type DoPutStream =
        Pin<Box<dyn futures::Stream<Item = std::result::Result<PutResult, Status>> + Send>>;
    type DoExchangeStream =
        Pin<Box<dyn futures::Stream<Item = std::result::Result<FlightData, Status>> + Send>>;
    type DoActionStream =
        Pin<Box<dyn futures::Stream<Item = std::result::Result<FlightResult, Status>> + Send>>;
    type ListActionsStream =
        Pin<Box<dyn futures::Stream<Item = std::result::Result<ActionType, Status>> + Send>>;

    async fn do_get(
        &self,
        request: Request<Ticket>,
    ) -> std::result::Result<Response<Self::DoGetStream>, Status> {
        let ticket = request.into_inner();
        let parsed = parse_ticket(&ticket)?;

        // #336: per-action region dispatch. Each coordinate-bearing
        // action snaps an input point to pick the region; transit_bulk
        // is single-state today (blocked on #334) and continues to use
        // the primary state until multi-region transit lands.
        match parsed.action.as_str() {
            "matrix" => {
                let params: MatrixParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid matrix params: {}", e))
                    })?;

                if params.origins.is_empty() || params.destinations.is_empty() {
                    return Err(Status::invalid_argument(
                        "sources and destinations must not be empty",
                    ));
                }
                for (i, [lon, lat]) in params.origins.iter().enumerate() {
                    validate_coord(*lon, *lat, &format!("source[{}]", i))?;
                }
                for (i, [lon, lat]) in params.destinations.iter().enumerate() {
                    validate_coord(*lon, *lat, &format!("dest[{}]", i))?;
                }

                // Snap the first (source, destination) pair. If both
                // sides snap to the same region we proceed; otherwise
                // the dispatcher returns CrossRegion → 9 (FAILED_PRECONDITION).
                let [s_lon, s_lat] = params.origins[0];
                let [d_lon, d_lat] = params.destinations[0];
                let (state, _region) =
                    self.dispatch_for_pair(s_lon, s_lat, d_lon, d_lat, &parsed.profile)?;
                let mode = resolve_mode(&parsed.profile, &state)?;

                let batch_stream = do_matrix(&state, mode, params)?;
                let schema = Arc::new(matrix_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            "route_batch" => {
                let params: RouteBatchParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid route_batch params: {}", e))
                    })?;

                if params.pairs.is_empty() {
                    return Err(Status::invalid_argument("pairs must not be empty"));
                }
                if params.pairs.len() > 100_000 {
                    return Err(Status::invalid_argument("max 100,000 pairs per request"));
                }
                // #482: validate the distance bound up front — a negative,
                // zero, or NaN/inf `max_meters` is a client error, not a
                // silently-clamped value (the old silent-ignore behaviour
                // is exactly what the ticket flags).
                if let Some(m) = params.max_meters
                    && (!m.is_finite() || m <= 0.0)
                {
                    return Err(Status::invalid_argument(
                        "max_meters must be a finite number > 0",
                    ));
                }
                for (i, pair) in params.pairs.iter().enumerate() {
                    validate_coord(pair[0], pair[1], &format!("pair[{}].src", i))?;
                    validate_coord(pair[2], pair[3], &format!("pair[{}].dst", i))?;
                }

                // First pair picks the region; subsequent pairs sharing
                // that region run within it. Mixed-region pairs are
                // rejected up front by dispatch_for_pair on the FIRST
                // pair; per-pair cross-region in a multi-pair batch is
                // a known follow-up (see #336).
                let p0 = params.pairs[0];
                let (state, _region) =
                    self.dispatch_for_pair(p0[0], p0[1], p0[2], p0[3], &parsed.profile)?;
                let mode = resolve_mode(&parsed.profile, &state)?;

                let batch_stream = do_route_batch(&state, mode, params)?;
                let schema = Arc::new(route_batch_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            "isochrone" => {
                let params: IsochroneParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid isochrone params: {}", e))
                    })?;
                validate_coord(params.lon, params.lat, "origin")?;

                let (state, _region) =
                    self.dispatch_for_point(params.lon, params.lat, &parsed.profile)?;
                let mode = resolve_mode(&parsed.profile, &state)?;

                let batch_stream = do_isochrone(&state, mode, params)?;
                let schema = Arc::new(isochrone_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            "transit_bulk" => {
                // The transit_bulk action ignores the `profile` part of
                // the ticket — every query carries its own
                // `access_mode`/`egress_mode`. Transit in multi-region
                // mode is blocked on #334 (subsystem not loaded across
                // regions); for now continue to dispatch on the primary
                // and let `do_transit_bulk` return FailedPrecondition if
                // transit isn't loaded.
                let state = self.state();
                let params: TransitBulkParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid transit_bulk params: {}", e))
                    })?;
                let batch_stream = do_transit_bulk(&state, params)?;
                let schema = Arc::new(transit_bulk_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            "edges_batch" => {
                let params: EdgesBatchParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid edges_batch params: {}", e))
                    })?;
                if params.pairs.is_empty() {
                    return Err(Status::invalid_argument("pairs must not be empty"));
                }
                for (i, pair) in params.pairs.iter().enumerate() {
                    validate_coord(pair[0], pair[1], &format!("pair[{}].src", i))?;
                    validate_coord(pair[2], pair[3], &format!("pair[{}].dst", i))?;
                }
                // #462: one unsnappable pair must not fail the other
                // 499,999. The region is picked by the FIRST pair that
                // dispatches; pairs that snap into no region fall through
                // to do_edges_batch, where their per-pair snap miss emits
                // the documented all-null unreachable row.
                let dispatched = first_dispatchable_pair(&params.pairs, |p| {
                    self.regions
                        .dispatch_p2p_id(p[0], p[1], p[2], p[3], &parsed.profile)
                })?;
                let Some((state, _region)) = dispatched else {
                    // No pair snapped into any region: every pair is
                    // unreachable. Same per-pair contract, applied to the
                    // whole batch instead of a request-level error.
                    let schema = Arc::new(edges_batch_schema());
                    let batch_stream: BatchStream =
                        Box::pin(stream::iter(all_null_edges_batches(params.pairs.len())));
                    let flight_stream = batches_to_flight_data(schema, batch_stream);
                    return Ok(Response::new(flight_stream));
                };
                let mode = resolve_mode(&parsed.profile, &state)?;

                let batch_stream = do_edges_batch(&state, mode, params)?;
                let schema = Arc::new(edges_batch_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            other => Err(Status::invalid_argument(format!(
                "Unknown action '{}'. Available: matrix, route_batch, isochrone, transit_bulk, edges_batch",
                other
            ))),
        }
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let cmd = std::str::from_utf8(&descriptor.cmd)
            .map_err(|_| Status::invalid_argument("descriptor cmd must be UTF-8"))?;

        let schema = match cmd {
            "matrix" => matrix_schema(),
            "route_batch" => route_batch_schema(),
            "isochrone" => isochrone_schema(),
            "catchment" => catchment_schema(),
            "transit_bulk" => transit_bulk_schema(),
            "edges_batch" => edges_batch_schema(),
            "edges_flow" => edges_flow_schema(),
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown action '{}'. Available: matrix, route_batch, isochrone, catchment, transit_bulk, edges_batch, edges_flow",
                    other
                )));
            }
        };

        let info = FlightInfo::new()
            .with_descriptor(descriptor)
            .try_with_schema(&schema)
            .map_err(|e| Status::internal(format!("Schema encoding error: {}", e)))?;

        Ok(Response::new(info))
    }

    async fn poll_flight_info(
        &self,
        _request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<PollInfo>, Status> {
        Err(Status::unimplemented("PollFlightInfo not supported"))
    }

    async fn get_schema(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<SchemaResult>, Status> {
        let descriptor = request.into_inner();
        let cmd = std::str::from_utf8(&descriptor.cmd)
            .map_err(|_| Status::invalid_argument("descriptor cmd must be UTF-8"))?;

        let schema = match cmd {
            "matrix" => matrix_schema(),
            "route_batch" => route_batch_schema(),
            "isochrone" => isochrone_schema(),
            "catchment" => catchment_schema(),
            "transit_bulk" => transit_bulk_schema(),
            "edges_batch" => edges_batch_schema(),
            "edges_flow" => edges_flow_schema(),
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown action '{}'. Available: matrix, route_batch, isochrone, catchment, transit_bulk, edges_batch, edges_flow",
                    other
                )));
            }
        };

        let schema_bytes = schema_to_ipc_bytes(&schema)?;
        Ok(Response::new(SchemaResult {
            schema: schema_bytes,
        }))
    }

    async fn list_actions(
        &self,
        _request: Request<Empty>,
    ) -> std::result::Result<Response<Self::ListActionsStream>, Status> {
        let actions = vec![
            ActionType {
                r#type: "matrix".into(),
                description: "Distance/duration matrix. Ticket: matrix:<profile>:{\"sources\":[[lon,lat],...],\"destinations\":[[lon,lat],...]}".into(),
            },
            ActionType {
                r#type: "route_batch".into(),
                description: "Batch P2P routing with WKB geometry. Ticket: route_batch:<profile>:{\"pairs\":[[origin_lon,origin_lat,destination_lon,destination_lat],...], \"max_meters\": optional}. Every row carries pair_idx = its index in the input pairs array. Without max_meters: one row per pair (full time-optimal route, duration_s + geometry_wkb populated). With max_meters: a DISTANCE prune — pairs whose shortest road distance exceeds the bound are DROPPED (no row, gaps visible via pair_idx), the bounded search early-terminates, and emitted rows carry distance_m only (duration_s + geometry_wkb null). Unknown params are rejected (#482).".into(),
            },
            ActionType {
                r#type: "isochrone".into(),
                description: "Reachability polygons as WKB. Ticket: isochrone:<profile>:{\"lon\":4.35,\"lat\":50.85,\"intervals\":[300,600]}".into(),
            },
            ActionType {
                r#type: "catchment".into(),
                description: "Catchment areas via DoExchange. Input: (store_id:utf8, store_lon:f64, store_lat:f64, client_lon:f64, client_lat:f64). Descriptor cmd: catchment:<profile>:{\"percentiles\":[50,80],\"hull_shape\":\"isochrone\",\"remove_outliers\":true}".into(),
            },
            ActionType {
                r#type: "transit_bulk".into(),
                description: "Multimodal transit batch routing with Arrow IPC streaming (#119). Ticket: transit_bulk:<profile>:{\"queries\":[{\"origin_lon\":...,\"origin_lat\":...,\"destination_lon\":...,\"destination_lat\":...,\"depart\":\"08:00:00\",...},...]}. The profile is ignored — every query carries its own access_mode/egress_mode. Schema: query_idx, status, http_status, error, origin/dest lon/lat, depart_time, arrival_time, total_duration_s, access/egress_mode, legs_json (JSON-encoded leg array). Up to 500k queries per call.".into(),
            },
            ActionType {
                r#type: "edges_batch".into(),
                description: "Unnested per-edge path output for bulk flow analytics (#125). Ticket: edges_batch:<profile>:{\"pairs\":[[origin_lon,origin_lat,destination_lon,destination_lat],...]}. Unlike route_batch (which returns WKB polyline geometry), edges_batch emits one row per traversed EBG edge with columns: query_idx, target_idx, edge_seq, osm_node_from, osm_node_to, duration_ms, distance_m. Unreachable pairs emit a single row with null edge columns. Continuity invariant: consecutive rows within a query satisfy osm_node_to[i] == osm_node_from[i+1]. Up to 500k pairs per call.".into(),
            },
        ];

        let stream = stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream) as Self::ListActionsStream))
    }

    // ---- Unimplemented methods ----

    async fn handshake(
        &self,
        _request: Request<Streaming<HandshakeRequest>>,
    ) -> std::result::Result<Response<Self::HandshakeStream>, Status> {
        Err(Status::unimplemented("Handshake not supported"))
    }

    async fn list_flights(
        &self,
        _request: Request<Criteria>,
    ) -> std::result::Result<Response<Self::ListFlightsStream>, Status> {
        Err(Status::unimplemented("ListFlights not supported"))
    }

    async fn do_put(
        &self,
        _request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoPutStream>, Status> {
        Err(Status::unimplemented("DoPut not supported"))
    }

    async fn do_exchange(
        &self,
        request: Request<Streaming<FlightData>>,
    ) -> std::result::Result<Response<Self::DoExchangeStream>, Status> {
        let mut stream = request.into_inner();

        // Collect all FlightData messages, extract descriptor from first
        let mut all_fds: Vec<FlightData> = Vec::new();
        let mut descriptor_cmd: Vec<u8> = Vec::new();

        while let Some(fd) = stream.message().await? {
            if descriptor_cmd.is_empty()
                && let Some(ref desc) = fd.flight_descriptor
            {
                descriptor_cmd = desc.cmd.to_vec();
            }
            all_fds.push(fd);
        }

        if descriptor_cmd.is_empty() {
            return Err(Status::invalid_argument(
                "first message must have flight_descriptor with cmd",
            ));
        }

        let cmd = std::str::from_utf8(&descriptor_cmd)
            .map_err(|_| Status::invalid_argument("cmd must be UTF-8"))?;

        // Parse: <action>:profile:params_json
        let parts: Vec<&str> = cmd.splitn(3, ':').collect();
        if parts.is_empty() || !matches!(parts[0], "catchment" | "edges_flow") {
            return Err(Status::invalid_argument(
                "do_exchange supports 'catchment:profile[:params_json]' and 'edges_flow:profile'",
            ));
        }
        let action = parts[0];
        let profile = parts.get(1).copied().unwrap_or("car");
        let params_json = parts.get(2).copied().unwrap_or("{}");

        // Decode FlightData into RecordBatches
        let ipc_messages: Vec<FlightData> = all_fds
            .into_iter()
            .filter(|fd| !fd.data_header.is_empty())
            .map(|mut fd| {
                fd.flight_descriptor = None;
                fd
            })
            .collect();

        let batches = arrow_flight::utils::flight_data_to_batches(&ipc_messages)
            .map_err(|e| Status::invalid_argument(format!("decode error: {}", e)))?;

        if batches.is_empty() {
            return Err(Status::invalid_argument("no data received"));
        }

        match action {
            "edges_flow" => {
                // #336: first src coordinate picks the region.
                let (lon, lat) = batches
                    .iter()
                    .find(|b| b.num_rows() > 0)
                    .and_then(|b| {
                        let lon = b
                            .column_by_name("src_lon")?
                            .as_any()
                            .downcast_ref::<Float64Array>()?;
                        let lat = b
                            .column_by_name("src_lat")?
                            .as_any()
                            .downcast_ref::<Float64Array>()?;
                        Some((lon.value(0), lat.value(0)))
                    })
                    .ok_or_else(|| {
                        Status::invalid_argument("need at least one row with (src_lon, src_lat)")
                    })?;
                let (state, _region) = self.dispatch_for_point(lon, lat, profile)?;
                let mode = resolve_mode(profile, &state)?;
                do_exchange_edges_flow(state, mode, &batches).await
            }
            _ => {
                let cp = super::catchment::parse_exchange_params(params_json)
                    .map_err(Status::invalid_argument)?;

                // #336: snap the first store coordinate to pick the region.
                // Catchment input schema: (store_id, store_lon, store_lat,
                // client_lon, client_lat). Mixed-region inputs in a single
                // batch are a follow-up — for now the first row picks.
                let (store_lon, store_lat) = first_store_lonlat(&batches).ok_or_else(|| {
                    Status::invalid_argument(
                        "no rows in input batches — need at least one (store_lon, store_lat)",
                    )
                })?;
                let (state, _region) = self.dispatch_for_point(store_lon, store_lat, profile)?;
                let mode = resolve_mode(profile, &state)?;

                do_exchange_catchment(state, mode, cp, &batches).await
            }
        }
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "DoAction not supported. Use DoGet with tickets.",
        ))
    }
}

#[cfg(test)]
mod edges_batch_null_row_tests {
    use super::{all_null_edges_batches, edges_batch_schema, first_dispatchable_pair};
    use crate::server::regions::{DispatchError, Endpoint};
    use arrow::array::{Array, Int64Array, UInt32Array};

    fn no_region(lon: f64, lat: f64) -> DispatchError {
        DispatchError::NoRegion {
            endpoint: Endpoint::Source,
            lon,
            lat,
            mode: "car".to_string(),
            tried: vec!["BE".to_string()],
        }
    }

    /// #462: the production failure shape — pair 0 unsnappable (geocoder
    /// sentinel deep in France), pair 1 fine. The scan must skip pair 0
    /// and pick pair 1's region instead of failing the request.
    #[test]
    fn skips_noregion_pair_and_picks_next() {
        let pairs = vec![
            [2.3059, 49.2942, 4.4025, 51.2194], // src ~100 km outside coverage
            [4.3517, 50.8503, 4.4025, 51.2194], // Brussels → Antwerp
        ];
        let mut calls = 0usize;
        let got = first_dispatchable_pair(&pairs, |p| {
            calls += 1;
            if p[0] < 3.0 {
                Err(no_region(p[0], p[1]))
            } else {
                Ok("region-of-pair-1")
            }
        })
        .expect("NoRegion on one pair must not error the request");
        assert_eq!(got, Some("region-of-pair-1"));
        assert_eq!(calls, 2, "scan stops at the first dispatchable pair");
    }

    /// #462: every pair unsnappable → `Ok(None)` (caller emits the
    /// all-null row for the whole batch), NOT a request-level error.
    #[test]
    fn all_noregion_returns_none() {
        let pairs = vec![[0.0, 0.0, 1.0, 1.0]; 3];
        let mut calls = 0usize;
        let got: Option<()> = first_dispatchable_pair(&pairs, |p| {
            calls += 1;
            Err(no_region(p[0], p[1]))
        })
        .expect("all-NoRegion must not error");
        assert_eq!(got, None);
        assert_eq!(calls, 3, "every pair gets a dispatch attempt");
    }

    /// CrossRegion keeps its request-level semantics (the #336
    /// follow-up): silently null-rowing a genuinely cross-region pair
    /// would hide a capability gap, not a bad coordinate.
    #[test]
    fn cross_region_stays_request_level() {
        let pairs = vec![[0.0, 0.0, 1.0, 1.0], [4.35, 50.85, 6.13, 49.61]];
        let err = first_dispatchable_pair::<()>(&pairs, |p| {
            if p[0] < 1.0 {
                Err(no_region(p[0], p[1]))
            } else {
                Err(DispatchError::CrossRegion {
                    src_region: "BE".to_string(),
                    dst_region: "LU".to_string(),
                })
            }
        })
        .expect_err("cross-region must stay a request-level error");
        assert_eq!(err.code(), tonic::Code::FailedPrecondition);
    }

    /// InvalidMode keeps its request-level semantics — a profile typo
    /// applies to the whole request, never to a single pair.
    #[test]
    fn invalid_mode_stays_request_level() {
        let pairs = vec![[4.35, 50.85, 4.40, 51.22]];
        let err = first_dispatchable_pair::<()>(&pairs, |_| {
            Err(DispatchError::InvalidMode {
                mode: "cra".to_string(),
                available: vec!["car".to_string()],
            })
        })
        .expect_err("invalid mode must stay a request-level error");
        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    /// #462: the all-null fallback batch has exactly the documented
    /// unreachable-row shape — one row per pair, `query_idx` dense,
    /// `target_idx` 0, every edge column null.
    #[test]
    fn all_null_batches_shape() {
        let batches = all_null_edges_batches(5);
        assert_eq!(batches.len(), 1);
        let batch = batches.into_iter().next().unwrap().expect("arrow build");
        assert_eq!(batch.schema().as_ref(), &edges_batch_schema());
        assert_eq!(batch.num_rows(), 5);

        let query_idx = batch
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        let target_idx = batch
            .column(1)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(query_idx.null_count(), 0);
        assert_eq!(target_idx.null_count(), 0);
        for i in 0..5 {
            assert_eq!(query_idx.value(i), i as u32);
            assert_eq!(target_idx.value(i), 0);
        }
        // Every edge column is all-null: the `edge_seq IS NULL` filter
        // documented on the schema matches every row.
        for (col, name) in [(2, "edge_seq"), (5, "duration_ms"), (6, "distance_m")] {
            let arr = batch
                .column(col)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            assert_eq!(arr.null_count(), 5, "{name} must be all-null");
        }
        for (col, name) in [(3, "osm_node_from"), (4, "osm_node_to")] {
            let arr = batch
                .column(col)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            assert_eq!(arr.null_count(), 5, "{name} must be all-null");
        }
    }

    /// #462: chunking — rows are bounded per RecordBatch and `query_idx`
    /// stays dense across batch boundaries; zero pairs build zero batches.
    #[test]
    fn all_null_batches_chunking() {
        assert!(all_null_edges_batches(0).is_empty());

        let batches = all_null_edges_batches(20_001);
        assert_eq!(batches.len(), 2);
        let sizes: Vec<usize> = batches
            .iter()
            .map(|b| b.as_ref().expect("arrow build").num_rows())
            .collect();
        assert_eq!(sizes, vec![20_000, 1]);
        let second = batches[1].as_ref().expect("arrow build");
        let query_idx = second
            .column(0)
            .as_any()
            .downcast_ref::<UInt32Array>()
            .unwrap();
        assert_eq!(query_idx.value(0), 20_000, "query_idx dense across chunks");
    }
}

#[cfg(test)]
mod edges_batch_grouping_tests {
    use super::{compute_edges_flat, compute_edges_grouped};
    use crate::model::types::Mode;
    use crate::server::state::{LoadOptions, ServerState};
    use std::path::PathBuf;

    /// #438: equivalence oracle — the source-GROUPED edges path must produce
    /// the SAME result as the per-pair FLAT path: identical reachability and
    /// identical per-pair CCH distance (the optimised metric, #468). Equal-cost
    /// ties may pick a different (still-shortest) edge sequence — so byte-
    /// identity is asserted as a high rate, not 100%, and the summed row
    /// durations (post-unpack `node_weights` basis) are NOT asserted at all:
    /// equal-CCH-cost alternatives legitimately differ on them (#468).
    ///
    /// Skipped unless `BT_EDGES_CONTAINER` points at a Belgium `.butterfly`
    /// (the step4-7 CCH is large and not committed). Run with:
    /// ```text
    /// BT_EDGES_CONTAINER=/path/belgium.butterfly \
    ///   cargo test -p butterfly-route edges_grouped_matches_flat -- --nocapture
    /// ```
    #[test]
    fn edges_grouped_matches_flat() {
        let Some(path) = std::env::var("BT_EDGES_CONTAINER").ok().map(PathBuf::from) else {
            eprintln!(
                "skipping edges_grouped_matches_flat: set BT_EDGES_CONTAINER to a Belgium .butterfly"
            );
            return;
        };

        let state = ServerState::load_from_container_with_options(
            &path,
            Some(&["car".to_string()]),
            &LoadOptions {
                eager_verify: false,
                warmup_on_boot: false,
            },
        )
        .expect("load container");
        let mode_idx = *state.mode_lookup.get("car").expect("car mode loaded");
        let mode = Mode(mode_idx);
        let mode_data = state.get_mode(mode);

        // Source-sharing workload: 60 sources × 25 nearby targets, plus a few
        // deliberate edge cases (source==target, a non-contiguous interleave).
        let mut rng_state: u64 = 0x9E3779B97F4A7C15;
        let mut next = || {
            rng_state ^= rng_state << 13;
            rng_state ^= rng_state >> 7;
            rng_state ^= rng_state << 17;
            rng_state
        };
        let unit = |r: u64| (r as f64) / (u64::MAX as f64);
        let mut pairs: Vec<[f64; 4]> = Vec::new();
        for _ in 0..60 {
            let slon = 2.6 + unit(next()) * 3.7;
            let slat = 49.6 + unit(next()) * 1.8;
            for _ in 0..25 {
                let dlon = (slon + (unit(next()) - 0.5) * 0.5).clamp(2.55, 6.40);
                let dlat = (slat + (unit(next()) - 0.5) * 0.5).clamp(49.50, 51.50);
                pairs.push([slon, slat, dlon, dlat]);
            }
            // source==target edge case for this source.
            pairs.push([slon, slat, slon, slat]);
        }

        for parallel in [false, true] {
            let flat = compute_edges_flat(&state, &mode_data, mode, &pairs, parallel);
            let grouped = compute_edges_grouped(&state, &mode_data, mode, &pairs, parallel);
            assert_eq!(flat.len(), pairs.len());
            assert_eq!(grouped.len(), pairs.len());

            let mut byte_identical = 0usize;
            for (f, g) in flat.iter().zip(grouped.iter()) {
                assert_eq!(
                    f.query_idx, g.query_idx,
                    "query_idx order (parallel={parallel})"
                );
                assert_eq!(
                    f.rows.is_empty(),
                    g.rows.is_empty(),
                    "reachability for query_idx {} (parallel={parallel})",
                    f.query_idx
                );
                // #468: assert on the OPTIMIZED metric (CCH distance), not the
                // post-unpack node_weights row sum — ties differ on the latter.
                assert_eq!(
                    f.cch_distance, g.cch_distance,
                    "CCH distance for query_idx {} (parallel={parallel})",
                    f.query_idx
                );
                let same = f.rows.len() == g.rows.len()
                    && f.rows.iter().zip(g.rows.iter()).all(|(a, b)| {
                        a.edge_seq == b.edge_seq
                            && a.osm_from == b.osm_from
                            && a.osm_to == b.osm_to
                            && a.dur_ms == b.dur_ms
                            && a.dist_m == b.dist_m
                    });
                if same {
                    byte_identical += 1;
                }
            }
            let rate = byte_identical as f64 / pairs.len() as f64;
            eprintln!(
                "edges_grouped_matches_flat parallel={parallel}: byte-identical {byte_identical}/{} ({:.2}%)",
                pairs.len(),
                rate * 100.0
            );
            // Reachability + CCH distance are exact (asserted above). Edge
            // sequences match for all but rare equal-cost ties.
            assert!(
                rate > 0.95,
                "byte-identity {:.2}% too low (parallel={parallel}) — ties should be rare",
                rate * 100.0
            );
        }
    }
}

#[cfg(test)]
mod route_batch_prune_tests {
    use super::{RouteBatchParams, route_batch_schema};
    use arrow::datatypes::DataType;

    /// #482: the route_batch schema gained a leading `pair_idx` column and
    /// made `duration_s` / `geometry_wkb` nullable (null in bounded mode).
    /// `distance_m` stays non-null. Column ORDER is load-bearing — the
    /// emit builders push in this exact order, and clients index by
    /// position.
    #[test]
    fn schema_has_pair_idx_and_nullable_duration_geometry() {
        let schema = route_batch_schema();
        let fields = schema.fields();
        assert_eq!(fields.len(), 8, "pair_idx prepended to the 7 prior cols");

        // pair_idx: FIRST, UInt32, non-null.
        let pair_idx = &fields[0];
        assert_eq!(pair_idx.name(), "pair_idx");
        assert_eq!(pair_idx.data_type(), &DataType::UInt32);
        assert!(!pair_idx.is_nullable(), "pair_idx must never be null");

        // Coords stay non-null Float64 in cols 1..=4.
        for (i, name) in [
            "origin_lon",
            "origin_lat",
            "destination_lon",
            "destination_lat",
        ]
        .iter()
        .enumerate()
        {
            let f = &fields[i + 1];
            assert_eq!(f.name(), name);
            assert_eq!(f.data_type(), &DataType::Float64);
            assert!(!f.is_nullable(), "{name} must stay non-null");
        }

        // duration_s: now NULLABLE Float32 (null in bounded mode).
        let dur = &fields[5];
        assert_eq!(dur.name(), "duration_s");
        assert_eq!(dur.data_type(), &DataType::Float32);
        assert!(dur.is_nullable(), "duration_s must be nullable (#482)");

        // distance_m: stays NON-null Float32.
        let dist = &fields[6];
        assert_eq!(dist.name(), "distance_m");
        assert_eq!(dist.data_type(), &DataType::Float32);
        assert!(!dist.is_nullable(), "distance_m stays non-null");

        // geometry_wkb: now NULLABLE Binary (null in bounded mode).
        let geom = &fields[7];
        assert_eq!(geom.name(), "geometry_wkb");
        assert_eq!(geom.data_type(), &DataType::Binary);
        assert!(geom.is_nullable(), "geometry_wkb must be nullable (#482)");
    }

    /// #482: `max_meters` defaults to `None` (unbounded) when absent, so
    /// every pre-existing caller keeps the old full-route behaviour.
    #[test]
    fn max_meters_defaults_to_none() {
        let json = r#"{"pairs":[[4.35,50.85,4.40,51.22]]}"#;
        let p: RouteBatchParams = serde_json::from_str(json).expect("parse without max_meters");
        assert!(p.max_meters.is_none());
        assert_eq!(p.pairs.len(), 1);
    }

    /// #482: `max_meters` parses when present.
    #[test]
    fn max_meters_parses_when_present() {
        let json = r#"{"pairs":[[4.35,50.85,4.40,51.22]],"max_meters":3000}"#;
        let p: RouteBatchParams = serde_json::from_str(json).expect("parse with max_meters");
        assert_eq!(p.max_meters, Some(3000.0));
    }

    /// #482: `deny_unknown_fields` — the ticket's core complaint is that
    /// today `max_km`, `prune_max_km`, `radius_km`, `zzz`, … are accepted
    /// with NO effect. They must now error instead of being ignored.
    #[test]
    fn unknown_fields_are_rejected() {
        for json in [
            r#"{"pairs":[[4.35,50.85,4.40,51.22]],"max_km":3}"#,
            r#"{"pairs":[[4.35,50.85,4.40,51.22]],"prune_max_km":3}"#,
            r#"{"pairs":[[4.35,50.85,4.40,51.22]],"radius_km":3}"#,
            r#"{"pairs":[[4.35,50.85,4.40,51.22]],"max_minutes":5}"#,
            r#"{"pairs":[[4.35,50.85,4.40,51.22]],"zzz":1}"#,
        ] {
            assert!(
                serde_json::from_str::<RouteBatchParams>(json).is_err(),
                "unknown-field params must be rejected, not silently ignored: {json}"
            );
        }
    }
}
