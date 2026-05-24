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

use crate::matrix::bucket_ch::{
    backward_join_with_buckets, forward_build_buckets, table_bucket_full_flat,
    table_bucket_parallel,
};
use crate::matrix::neighbors::{RadiusParam, auto_radius_km, build_neighbors, parse_radius};
use crate::profile_abi::Mode;
use crate::range::contour::ContourResult;
use crate::range::wkb_stream::encode_polygon_wkb;

use super::geometry::{Point, build_isochrone_geometry};
use super::isochrone_handler::{run_phast_bounded_fast, run_phast_bounded_fast_reverse};
use super::query::CchQuery;
use super::state::ServerState;

/// Butterfly Arrow Flight service — wraps shared ServerState
pub struct ButterflyFlight {
    state: Arc<ServerState>,
}

impl ButterflyFlight {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

/// Build a configured FlightServiceServer
pub fn build_flight_server(state: Arc<ServerState>) -> FlightServiceServer<ButterflyFlight> {
    FlightServiceServer::new(ButterflyFlight::new(state))
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
    Schema::new(vec![
        Field::new("src_lon", DataType::Float64, false),
        Field::new("src_lat", DataType::Float64, false),
        Field::new("dst_lon", DataType::Float64, false),
        Field::new("dst_lat", DataType::Float64, false),
        Field::new("duration_s", DataType::Float32, false),
        Field::new("distance_m", DataType::Float32, false),
        Field::new("geometry_wkb", DataType::Binary, false),
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
        Field::new("dest_lon", DataType::Float64, false),
        Field::new("dest_lat", DataType::Float64, false),
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
struct MatrixParams {
    sources: Vec<[f64; 2]>,
    destinations: Vec<[f64; 2]>,
    /// Optional Euclidean pre-filter radius in kilometres. Accepts a
    /// positive number, the string "auto", or null/0 to disable.
    #[serde(default)]
    radius_km: Option<serde_json::Value>,
}

/// Build matrix RecordBatch from flat u32 distances.
#[allow(clippy::too_many_arguments)]
fn build_matrix_batch(
    matrix: &[u32],
    n_valid_src: usize,
    n_valid_dst: usize,
    valid_src_indices: &[usize],
    valid_dst_indices: &[usize],
    schema: Arc<Schema>,
    neighbor_mask: Option<&[Vec<u32>]>,
) -> std::result::Result<RecordBatch, Status> {
    let capacity = n_valid_src * n_valid_dst;
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
            let d = if pruned {
                u32::MAX
            } else {
                matrix[si * n_valid_dst + di]
            };
            src_idx.append_value(orig_src as u32);
            tgt_idx.append_value(orig_dst as u32);
            if d == u32::MAX {
                dur_ms.append_value(u32::MAX);
                dist_m.append_value(u32::MAX);
            } else {
                dur_ms.append_value(d.saturating_mul(100));
                dist_m.append_value(u32::MAX); // distance not computed in time metric
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

    use super::types::SnapRole;
    // K-best snap candidates per src/dst with directional role + the
    // SCC-aware connectivity filter (via mode_data.has_outbound /
    // has_inbound). The first candidate per slot feeds the bucket
    // M2M primary pass; the rest power the per-cell P2P fallback for
    // INF cells in the small-matrix branch below.
    const SNAP_K: usize = 64;
    use rayon::prelude::*;
    let src_kbest: Vec<super::snap_kbest::KBestSnap> = params
        .sources
        .par_iter()
        .map(|&[lon, lat]| {
            super::snap_kbest::snap_k_pair_role(
                state,
                mode_data,
                mode,
                lon,
                lat,
                SnapRole::Src,
                None,
                SNAP_K,
            )
        })
        .collect();
    let dst_kbest: Vec<super::snap_kbest::KBestSnap> = params
        .destinations
        .par_iter()
        .map(|&[lon, lat]| {
            super::snap_kbest::snap_k_pair_role(
                state,
                mode_data,
                mode,
                lon,
                lat,
                SnapRole::Dst,
                None,
                SNAP_K,
            )
        })
        .collect();

    let mut sources_rank = Vec::with_capacity(params.sources.len());
    let mut valid_src = Vec::with_capacity(params.sources.len());
    let mut sources_snapped = Vec::with_capacity(params.sources.len());
    for (i, snap) in src_kbest.iter().enumerate() {
        if let Some(r) = snap.primary_rank() {
            sources_rank.push(r);
            valid_src.push(i);
            if let Some((_, plon, plat, _)) = snap.primary {
                sources_snapped.push((plon, plat));
            } else {
                let [lon, lat] = params.sources[i];
                sources_snapped.push((lon, lat));
            }
        } else {
            let [lon, lat] = params.sources[i];
            sources_snapped.push((lon, lat));
        }
    }
    let mut targets_rank = Vec::with_capacity(params.destinations.len());
    let mut valid_dst = Vec::with_capacity(params.destinations.len());
    let mut targets_snapped = Vec::with_capacity(params.destinations.len());
    for (i, snap) in dst_kbest.iter().enumerate() {
        if let Some(r) = snap.primary_rank() {
            targets_rank.push(r);
            valid_dst.push(i);
            if let Some((_, plon, plat, _)) = snap.primary {
                targets_snapped.push((plon, plat));
            } else {
                let [lon, lat] = params.destinations[i];
                targets_snapped.push((lon, lat));
            }
        } else {
            let [lon, lat] = params.destinations[i];
            targets_snapped.push((lon, lat));
        }
    }

    if sources_rank.is_empty() || targets_rank.is_empty() {
        let schema = Arc::new(matrix_schema());
        let empty = RecordBatch::new_empty(schema);
        return Ok(Box::pin(stream::once(async move { Ok(empty) })));
    }

    let radius_param = parse_radius(params.radius_km.as_ref());
    let neighbor_mask: Option<Arc<Vec<Vec<u32>>>> = match radius_param {
        RadiusParam::None => None,
        RadiusParam::Km(r) => Some(Arc::new(build_neighbors(
            &sources_snapped,
            &targets_snapped,
            r,
        ))),
        RadiusParam::Auto => {
            let r = auto_radius_km(&sources_snapped, &targets_snapped);
            if r > 0.0 {
                Some(Arc::new(build_neighbors(
                    &sources_snapped,
                    &targets_snapped,
                    r,
                )))
            } else {
                None
            }
        }
    };

    let n_src = params.sources.len();
    let n_dst = params.destinations.len();
    let n_valid_src = sources_rank.len();
    let n_valid_dst = targets_rank.len();

    const BUCKET_M2M_THRESHOLD: usize = 50_000;

    if n_src * n_dst <= BUCKET_M2M_THRESHOLD {
        // ---- SMALL MATRIX: Bucket M2M, single batch ----
        let use_parallel = n_valid_src * n_valid_dst >= 2500;
        let up = &mode_data.up_adj_flat;
        let down = &mode_data.down_rev_flat;

        let (mut matrix, _stats) = if use_parallel {
            table_bucket_parallel(n_nodes, up, down, &sources_rank, &targets_rank)
        } else {
            table_bucket_full_flat(n_nodes, up, down, &sources_rank, &targets_rank)
        };

        // Per-cell K-best fallback for INF cells (mirrors /table POST).
        // With SCC-aware role masks this is now a rare per-cell rescue
        // for geometric-ambiguity / dynamic-recustomisation pairs.
        if matrix.contains(&u32::MAX) {
            use rayon::prelude::*;
            let query = super::query::CchQuery::new(state, mode);

            // Map back from matrix index (over valid src/dst) to the
            // original src/dst index so we can look up the K-best ranks.
            let mut work: Vec<(usize, usize)> = Vec::new();
            for (i, _) in valid_src.iter().enumerate() {
                for (j, _) in valid_dst.iter().enumerate() {
                    if matrix[i * n_valid_dst + j] == u32::MAX {
                        work.push((i, j));
                    }
                }
            }
            let patches: Vec<(usize, usize, u32)> = work
                .par_iter()
                .filter_map(|&(i, j)| {
                    let src_orig_idx = valid_src[i];
                    let dst_orig_idx = valid_dst[j];
                    let src_ranks = &src_kbest[src_orig_idx].ranks;
                    let dst_ranks = &dst_kbest[dst_orig_idx].ranks;
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

        let schema = Arc::new(matrix_schema());
        let batch = build_matrix_batch(
            &matrix,
            n_valid_src,
            n_valid_dst,
            &valid_src,
            &valid_dst,
            schema,
            neighbor_mask.as_ref().map(|v| v.as_slice()),
        )?;

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

        let src_tile_size = 1000usize.min(n_src).max(1);

        tokio::task::spawn_blocking(move || {
            use rayon::prelude::*;

            let src_blocks: Vec<(usize, usize)> = (0..n_src)
                .step_by(src_tile_size)
                .map(|s| (s, (s + src_tile_size).min(n_src)))
                .collect();

            src_blocks.par_iter().for_each(|&(src_start, src_end)| {
                if cancelled_bg.load(Ordering::Relaxed) {
                    return;
                }

                let mut block_src_ranks = Vec::new();
                let mut block_src_orig = Vec::new();
                for (vi, &oi) in valid_src.iter().enumerate() {
                    if oi >= src_start && oi < src_end {
                        block_src_ranks.push(sources_rank[vi]);
                        block_src_orig.push(oi);
                    }
                }

                if block_src_ranks.is_empty() {
                    return;
                }

                let buckets = Arc::new(forward_build_buckets(n_nodes, &up_adj, &block_src_ranks));

                let tile_matrix =
                    backward_join_with_buckets(n_nodes, &down_rev, &buckets, &targets_rank);

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
                            tile_matrix[bsi * n_block_dst + bdi]
                        };
                        si_arr.append_value(orig_si as u32);
                        di_arr.append_value(orig_di as u32);
                        if d == u32::MAX {
                            dur_arr.append_value(u32::MAX);
                        } else {
                            dur_arr.append_value(d.saturating_mul(100));
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
struct RouteBatchParams {
    pairs: Vec<[f64; 4]>, // [src_lon, src_lat, dst_lon, dst_lat]
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
/// of each call. The final WKB is moved out via `std::mem::take`
/// (handed off to the Arrow `BinaryBuilder`) and replaced with a
/// fresh empty `Vec`, so the next pair starts clean.
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
    src_lon: f64,
    src_lat: f64,
    dst_lon: f64,
    dst_lat: f64,
    duration_s: f32,
    distance_m: f32,
    wkb: Vec<u8>,
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
    scratch: &mut RouteScratch,
) -> Option<(f32, f32, Vec<u8>)> {
    use super::types::SnapRole;

    // Fast path: K=1 single snap + single CCH query. Avoids the K=64
    // collect overhead that costs ~1 ms/pair just for the snap.
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
        let src_rank = mode_data.orig_to_rank[src_id as usize];
        let dst_rank = mode_data.orig_to_rank[dst_id as usize];
        if src_rank != u32::MAX
            && dst_rank != u32::MAX
            && let Some(result) = query.query(src_rank, dst_rank)
        {
            return Some(build_route_output(
                state, mode_data, &result, src_rank, dst_rank, scratch,
            ));
        }
    }

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
/// and hands ownership of the final WKB to the caller via mem::take
/// (replaced with an empty `Vec` so the next call starts clean).
fn build_route_output(
    state: &ServerState,
    mode_data: &super::state::ModeData,
    result: &super::query::QueryResult,
    src_rank: u32,
    dst_rank: u32,
    scratch: &mut RouteScratch,
) -> (f32, f32, Vec<u8>) {
    let duration_s = result.distance as f64 / 10.0;

    super::unpack::unpack_path_into(
        &mode_data.cch_topo,
        &mode_data.cch_weights,
        &result.forward_parent,
        &result.backward_parent,
        src_rank,
        &mut scratch.rank_path,
    );
    let _ = dst_rank; // unpack derives the path from forward+backward parents

    // Translate CCH-rank ids → original EBG ids, reusing scratch.ebg_path.
    scratch.ebg_path.clear();
    scratch.ebg_path.reserve(scratch.rank_path.len());
    for &rank in &scratch.rank_path {
        let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
        scratch.ebg_path.push(mode_data.filtered_to_original[filt_id as usize]);
    }

    let distance_m = super::geometry::build_raw_points_into(
        &scratch.ebg_path,
        &state.ebg_nodes,
        &state.edge_geom,
        &mut scratch.points,
    );

    encode_linestring_wkb_into(&scratch.points, &mut scratch.wkb);
    let wkb = std::mem::take(&mut scratch.wkb);

    (duration_s as f32, distance_m as f32, wkb)
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
/// Sized for the gRPC `max_encoding_message_size` of 64 MiB. WKB
/// geometry on a Belgium route averages ~6–7 KiB; 2000 pairs ≈ 12 MiB,
/// leaving headroom for unusually long routes (50+ KiB transcontinental
/// shapes are not Belgium, but the cap must hold in the worst case).
/// Override via `BUTTERFLY_ROUTE_BATCH_BATCH_SIZE` if needed.
fn route_batch_batch_size() -> usize {
    std::env::var("BUTTERFLY_ROUTE_BATCH_BATCH_SIZE")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(2_000)
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
        let mode_data = state.get_mode(mode);
        let schema = Arc::new(route_batch_schema());
        let batch_size = route_batch_batch_size();

        let row_of = |slon: f64,
                      slat: f64,
                      dlon: f64,
                      dlat: f64,
                      result: Option<(f32, f32, Vec<u8>)>|
         -> RoutePairRow {
            match result {
                Some((dur, dist, wkb)) => RoutePairRow {
                    src_lon: slon,
                    src_lat: slat,
                    dst_lon: dlon,
                    dst_lat: dlat,
                    duration_s: dur,
                    distance_m: dist,
                    wkb,
                },
                None => RoutePairRow {
                    src_lon: slon,
                    src_lat: slat,
                    dst_lon: dlon,
                    dst_lat: dlat,
                    duration_s: f32::NAN,
                    distance_m: f32::NAN,
                    wkb: Vec::new(),
                },
            }
        };

        for chunk in params.pairs.chunks(batch_size) {
            let n = chunk.len();

            // Compute every pair in parallel. Use scoped OS threads
            // instead of rayon so each thread-local `CchQueryState`
            // drops after this chunk; keeping it in the global rayon
            // pool would permanently retain ~160 MB per worker on
            // Belgium after the first route_batch call.
            let n_workers = route_batch_worker_threads(n);
            let results: Vec<RoutePairRow> = if n_workers == 1 {
                let query = CchQuery::new(&state, mode);
                let mut scratch = RouteScratch::default();
                chunk
                    .iter()
                    .map(|pair| {
                        let (slon, slat, dlon, dlat) = (pair[0], pair[1], pair[2], pair[3]);
                        let r = compute_route_pair(
                            &state,
                            mode_data,
                            mode,
                            &query,
                            slon,
                            slat,
                            dlon,
                            dlat,
                            &mut scratch,
                        );
                        row_of(slon, slat, dlon, dlat, r)
                    })
                    .collect()
            } else {
                let chunk_size = n.div_ceil(n_workers);
                let join_result: std::thread::Result<Vec<Vec<RoutePairRow>>> =
                    std::thread::scope(|scope| {
                        let handles: Vec<_> = chunk
                            .chunks(chunk_size)
                            .map(|sub_chunk| {
                                let state = &state;
                                scope.spawn(move || {
                                    let query = CchQuery::new(state, mode);
                                    let mut scratch = RouteScratch::default();
                                    sub_chunk
                                        .iter()
                                        .map(|pair| {
                                            let (slon, slat, dlon, dlat) =
                                                (pair[0], pair[1], pair[2], pair[3]);
                                            let r = compute_route_pair(
                                                state,
                                                mode_data,
                                                mode,
                                                &query,
                                                slon,
                                                slat,
                                                dlon,
                                                dlat,
                                                &mut scratch,
                                            );
                                            row_of(slon, slat, dlon, dlat, r)
                                        })
                                        .collect::<Vec<RoutePairRow>>()
                                })
                            })
                            .collect();
                        handles.into_iter().map(|h| h.join()).collect()
                    });
                match join_result {
                    Ok(parts) => parts.into_iter().flatten().collect(),
                    Err(panic_payload) => {
                        // Convert a worker panic into a Status::internal
                        // sent on tx so a single bad pair cannot take
                        // down the gRPC server.
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
                        return;
                    }
                }
            };

            // Sequentially fill builders + emit batch. Builders are
            // not Send so the fill cannot be parallelised; the heavy
            // CPU work has already happened above.
            let mut src_lon_arr = Float64Builder::with_capacity(n);
            let mut src_lat_arr = Float64Builder::with_capacity(n);
            let mut dst_lon_arr = Float64Builder::with_capacity(n);
            let mut dst_lat_arr = Float64Builder::with_capacity(n);
            let mut dur_arr = Float32Builder::with_capacity(n);
            let mut dist_arr = Float32Builder::with_capacity(n);
            let geom_bytes = results.iter().map(|r| r.wkb.len()).sum();
            let mut geom_arr = BinaryBuilder::with_capacity(n, geom_bytes);

            for row in results {
                src_lon_arr.append_value(row.src_lon);
                src_lat_arr.append_value(row.src_lat);
                dst_lon_arr.append_value(row.dst_lon);
                dst_lat_arr.append_value(row.dst_lat);
                dur_arr.append_value(row.duration_s);
                dist_arr.append_value(row.distance_m);
                geom_arr.append_value(&row.wkb);
            }

            let batch = match RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(src_lon_arr.finish()) as ArrayRef,
                    Arc::new(src_lat_arr.finish()),
                    Arc::new(dst_lon_arr.finish()),
                    Arc::new(dst_lat_arr.finish()),
                    Arc::new(dur_arr.finish()),
                    Arc::new(dist_arr.finish()),
                    Arc::new(geom_arr.finish()),
                ],
            ) {
                Ok(b) => b,
                Err(e) => {
                    let _ = tx.blocking_send(Err(Status::internal(format!("Arrow: {}", e))));
                    return;
                }
            };

            if tx.blocking_send(Ok(batch)).is_err() {
                return; // Client disconnected
            }
        }
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
            center_role.role_filter(mode_data),
        )
        .ok_or_else(|| Status::not_found("Could not snap to road network"))?;
    let origin_rank = mode_data.orig_to_rank[orig_id as usize];
    if origin_rank == u32::MAX {
        return Err(Status::not_found(
            "Snapped node not accessible for this mode",
        ));
    }
    let max_interval = *params.intervals.iter().max().unwrap();
    let max_threshold_ds = max_interval * 10;

    let settled = if is_reverse {
        run_phast_bounded_fast_reverse(
            &mode_data.up_adj_flat,
            &mode_data.down_rev_flat,
            origin_rank,
            max_threshold_ds,
            mode,
        )
    } else {
        run_phast_bounded_fast(
            &mode_data.up_adj_flat,
            &mode_data.down_adj_flat,
            origin_rank,
            max_threshold_ds,
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
        let threshold_ds = interval_s * 10;

        let polygon_points = build_isochrone_geometry(
            &settled_original,
            threshold_ds,
            node_weights,
            &state.ebg_nodes,
            &state.edge_geom,
            mode_name,
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
    pub pairs: Vec<[f64; 4]>, // [src_lon, src_lat, dst_lon, dst_lat]
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
        // Chunk by PAIR count — each pair expands to ~20 edges on
        // Belgium (average path length), so 256 pairs ≈ 5k rows per
        // RecordBatch, a comfortable amortisation window.
        const CHUNK_PAIRS: usize = 256;

        for (chunk_start, chunk) in params
            .pairs
            .chunks(CHUNK_PAIRS)
            .enumerate()
            .map(|(ci, c)| (ci * CHUNK_PAIRS, c))
        {
            // Pre-size row builders generously; they grow as needed.
            let estimated_rows = chunk.len() * 32;
            let mut query_idx_b = UInt32Builder::with_capacity(estimated_rows);
            let mut target_idx_b = UInt32Builder::with_capacity(estimated_rows);
            let mut edge_seq_b = UInt32Builder::with_capacity(estimated_rows);
            let mut osm_from_b = Int64Builder::with_capacity(estimated_rows);
            let mut osm_to_b = Int64Builder::with_capacity(estimated_rows);
            let mut dur_ms_b = UInt32Builder::with_capacity(estimated_rows);
            let mut dist_m_b = UInt32Builder::with_capacity(estimated_rows);

            for (local_i, pair) in chunk.iter().enumerate() {
                let global_idx = (chunk_start + local_i) as u32;
                let target_idx = 0u32; // placeholder for source-batched shape

                // Emit an "unreachable" row by pushing one row with
                // all edge columns null. Query_idx / target_idx are
                // always non-null so the row is still uniquely
                // identifiable.
                let emit_unreachable =
                    |query_idx_b: &mut UInt32Builder,
                     target_idx_b: &mut UInt32Builder,
                     edge_seq_b: &mut UInt32Builder,
                     osm_from_b: &mut Int64Builder,
                     osm_to_b: &mut Int64Builder,
                     dur_ms_b: &mut UInt32Builder,
                     dist_m_b: &mut UInt32Builder| {
                        query_idx_b.append_value(global_idx);
                        target_idx_b.append_value(target_idx);
                        edge_seq_b.append_null();
                        osm_from_b.append_null();
                        osm_to_b.append_null();
                        dur_ms_b.append_null();
                        dist_m_b.append_null();
                    };

                // K-best snap + bounded combo fallback for the residual
                // geometric-ambiguity / dynamic-recustomisation cases.
                // The connectivity-aware role masks already drop
                // disconnected-component snap traps before we get here.
                const SNAP_K: usize = 64;
                let src_snap = super::snap_kbest::snap_k_pair_role(
                    &state,
                    mode_data,
                    mode,
                    pair[0],
                    pair[1],
                    super::types::SnapRole::Src,
                    None,
                    SNAP_K,
                );
                let dst_snap = super::snap_kbest::snap_k_pair_role(
                    &state,
                    mode_data,
                    mode,
                    pair[2],
                    pair[3],
                    super::types::SnapRole::Dst,
                    None,
                    SNAP_K,
                );
                if src_snap.ranks.is_empty() || dst_snap.ranks.is_empty() {
                    emit_unreachable(
                        &mut query_idx_b,
                        &mut target_idx_b,
                        &mut edge_seq_b,
                        &mut osm_from_b,
                        &mut osm_to_b,
                        &mut dur_ms_b,
                        &mut dist_m_b,
                    );
                    continue;
                }

                // Run CchQuery against the default time weights (no
                // avoid/exclude support in MVP; add later as a param
                // if the first consumer needs it).
                let query = super::query::CchQuery::with_custom_weights(
                    &mode_data.cch_topo,
                    &mode_data.up_adj_flat,
                    &mode_data.down_rev_flat,
                    &mode_data.cch_weights,
                );
                let Some((src_rank, dst_rank, result)) = super::snap_kbest::p2p_with_kbest_fallback(
                    &query,
                    &src_snap.ranks,
                    &dst_snap.ranks,
                    super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
                ) else {
                    emit_unreachable(
                        &mut query_idx_b,
                        &mut target_idx_b,
                        &mut edge_seq_b,
                        &mut osm_from_b,
                        &mut osm_to_b,
                        &mut dur_ms_b,
                        &mut dist_m_b,
                    );
                    continue;
                };

                // Unpack to the full EBG rank sequence.
                let rank_path = super::unpack::unpack_path(
                    &mode_data.cch_topo,
                    &mode_data.cch_weights,
                    &result.forward_parent,
                    &result.backward_parent,
                    src_rank,
                    dst_rank,
                    result.meeting_node,
                );
                // Convert rank path → original EBG node ids.
                let ebg_path: Vec<u32> = rank_path
                    .iter()
                    .map(|&rank| {
                        let filt_id = mode_data.cch_topo.rank_to_filtered[rank as usize];
                        mode_data.filtered_to_original[filt_id as usize]
                    })
                    .collect();

                if ebg_path.is_empty() {
                    emit_unreachable(
                        &mut query_idx_b,
                        &mut target_idx_b,
                        &mut edge_seq_b,
                        &mut osm_from_b,
                        &mut osm_to_b,
                        &mut dur_ms_b,
                        &mut dist_m_b,
                    );
                    continue;
                }

                // Emit one row per EBG node visited. Each EBG node
                // represents a directed edge between two NBG nodes
                // (tail → head), so `osm_node_from = osm(tail)`,
                // `osm_node_to = osm(head)`. The continuity invariant
                // `osm_to[i] == osm_from[i+1]` holds because
                // consecutive EBG nodes in a path share a junction.
                for (edge_seq, &ebg_id) in ebg_path.iter().enumerate() {
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
                    // Per-edge duration: node_weights is in
                    // deciseconds; convert to ms.
                    let duration_ds = mode_data
                        .node_weights
                        .get(ebg_id as usize)
                        .copied()
                        .unwrap_or(0);
                    let duration_ms = duration_ds.saturating_mul(100);
                    // Per-edge distance: length_mm on the EbgNode is
                    // a copy of nbg.geo.length_mm; convert to metres.
                    let distance_m = node.length_mm / 1000;

                    query_idx_b.append_value(global_idx);
                    target_idx_b.append_value(target_idx);
                    edge_seq_b.append_value(edge_seq as u32);
                    osm_from_b.append_value(osm_from);
                    osm_to_b.append_value(osm_to);
                    dur_ms_b.append_value(duration_ms);
                    dist_m_b.append_value(distance_m);
                }
            }

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
                    return;
                }
            };

            if tx.blocking_send(Ok(batch)).is_err() {
                return; // Client disconnected.
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
            let mut dest_lon_b = Float64Builder::with_capacity(n);
            let mut dest_lat_b = Float64Builder::with_capacity(n);
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
                dest_lon_b.append_value(req.dest_lon);
                dest_lat_b.append_value(req.dest_lat);
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
                    Arc::new(dest_lon_b.finish()),
                    Arc::new(dest_lat_b.finish()),
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

            // K-best snap store as source (#197 Src role).
            const SNAP_K: usize = 64;
            let store_snap = super::snap_kbest::snap_k_pair_role(
                &state,
                mode_data,
                mode,
                *slon,
                *slat,
                super::types::SnapRole::Src,
                None,
                SNAP_K,
            );
            let store_rank = match store_snap.primary_rank() {
                Some(r) => r,
                None => continue,
            };

            // K-best snap all clients as destinations (#197 Dst role).
            let client_snaps: Vec<super::snap_kbest::KBestSnap> = client_coords
                .iter()
                .map(|&(clon, clat)| {
                    super::snap_kbest::snap_k_pair_role(
                        &state,
                        mode_data,
                        mode,
                        clon,
                        clat,
                        super::types::SnapRole::Dst,
                        None,
                        SNAP_K,
                    )
                })
                .collect();
            let mut client_ranks: Vec<u32> = Vec::with_capacity(client_coords.len());
            let mut client_valid: Vec<usize> = Vec::with_capacity(client_coords.len());
            for (ci, snap) in client_snaps.iter().enumerate() {
                if let Some(r) = snap.primary_rank() {
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
            if matrix.contains(&u32::MAX) {
                use rayon::prelude::*;
                let query = super::query::CchQuery::new(&state, mode);
                let patches: Vec<(usize, u32)> = (0..client_valid.len())
                    .filter(|&ti| matrix[ti] == u32::MAX)
                    .collect::<Vec<_>>()
                    .par_iter()
                    .filter_map(|&ti| {
                        let ci = client_valid[ti];
                        let dst_ranks = &client_snaps[ci].ranks;
                        super::snap_kbest::p2p_with_kbest_fallback(
                            &query,
                            &store_snap.ranks,
                            dst_ranks,
                            super::snap_kbest::DEFAULT_MAX_FALLBACK_COMBOS,
                        )
                        .map(|(_, _, r)| (ti, r.distance))
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
                    let duration_s = d as f32 / 10.0;
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
        let mode = resolve_mode(&parsed.profile, &self.state)?;

        match parsed.action.as_str() {
            "matrix" => {
                let params: MatrixParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid matrix params: {}", e))
                    })?;

                for (i, [lon, lat]) in params.sources.iter().enumerate() {
                    validate_coord(*lon, *lat, &format!("source[{}]", i))?;
                }
                for (i, [lon, lat]) in params.destinations.iter().enumerate() {
                    validate_coord(*lon, *lat, &format!("dest[{}]", i))?;
                }
                if params.sources.is_empty() || params.destinations.is_empty() {
                    return Err(Status::invalid_argument(
                        "sources and destinations must not be empty",
                    ));
                }

                let batch_stream = do_matrix(&self.state, mode, params)?;
                let schema = Arc::new(matrix_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            "route_batch" => {
                let params: RouteBatchParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid route_batch params: {}", e))
                    })?;

                for (i, pair) in params.pairs.iter().enumerate() {
                    validate_coord(pair[0], pair[1], &format!("pair[{}].src", i))?;
                    validate_coord(pair[2], pair[3], &format!("pair[{}].dst", i))?;
                }
                if params.pairs.len() > 100_000 {
                    return Err(Status::invalid_argument("max 100,000 pairs per request"));
                }

                let batch_stream = do_route_batch(&self.state, mode, params)?;
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

                let batch_stream = do_isochrone(&self.state, mode, params)?;
                let schema = Arc::new(isochrone_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            "transit_bulk" => {
                // The transit_bulk action ignores the `profile` part
                // of the ticket — every query carries its own
                // `access_mode` / `egress_mode`. The mode resolved
                // above is unused but parsing it would be a hard
                // error if the profile were missing, so we accept any
                // valid loaded mode here.
                let _ = mode;
                let params: TransitBulkParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid transit_bulk params: {}", e))
                    })?;
                let batch_stream = do_transit_bulk(&self.state, params)?;
                let schema = Arc::new(transit_bulk_schema());
                let flight_stream = batches_to_flight_data(schema, batch_stream);
                Ok(Response::new(flight_stream))
            }
            "edges_batch" => {
                let params: EdgesBatchParams =
                    serde_json::from_str(&parsed.params_json).map_err(|e| {
                        Status::invalid_argument(format!("Invalid edges_batch params: {}", e))
                    })?;
                let batch_stream = do_edges_batch(&self.state, mode, params)?;
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
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown action '{}'. Available: matrix, route_batch, isochrone, catchment, transit_bulk, edges_batch",
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
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown action '{}'. Available: matrix, route_batch, isochrone, catchment, transit_bulk, edges_batch",
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
                description: "Batch P2P routing with WKB geometry. Ticket: route_batch:<profile>:{\"pairs\":[[src_lon,src_lat,dst_lon,dst_lat],...]}".into(),
            },
            ActionType {
                r#type: "isochrone".into(),
                description: "Reachability polygons as WKB. Ticket: isochrone:<profile>:{\"lon\":4.35,\"lat\":50.85,\"intervals\":[300,600]}".into(),
            },
            ActionType {
                r#type: "catchment".into(),
                description: "Catchment areas via DoExchange. Input: (store_id:utf8, store_lon:f64, store_lat:f64, client_lon:f64, client_lat:f64). Descriptor cmd: catchment:<profile>:{\"percentiles\":[50,80],\"hull_mode\":\"isochrone\",\"remove_outliers\":true}".into(),
            },
            ActionType {
                r#type: "transit_bulk".into(),
                description: "Multimodal transit batch routing with Arrow IPC streaming (#119). Ticket: transit_bulk:<profile>:{\"queries\":[{\"origin_lon\":...,\"origin_lat\":...,\"dest_lon\":...,\"dest_lat\":...,\"depart\":\"08:00:00\",...},...]}. The profile is ignored — every query carries its own access_mode/egress_mode. Schema: query_idx, status, http_status, error, origin/dest lon/lat, depart_time, arrival_time, total_duration_s, access/egress_mode, legs_json (JSON-encoded leg array). Up to 500k queries per call.".into(),
            },
            ActionType {
                r#type: "edges_batch".into(),
                description: "Unnested per-edge path output for bulk flow analytics (#125). Ticket: edges_batch:<profile>:{\"pairs\":[[src_lon,src_lat,dst_lon,dst_lat],...]}. Unlike route_batch (which returns WKB polyline geometry), edges_batch emits one row per traversed EBG edge with columns: query_idx, target_idx, edge_seq, osm_node_from, osm_node_to, duration_ms, distance_m. Unreachable pairs emit a single row with null edge columns. Continuity invariant: consecutive rows within a query satisfy osm_node_to[i] == osm_node_from[i+1]. Up to 500k pairs per call.".into(),
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
        let state = Arc::clone(&self.state);
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

        // Parse: catchment:profile:params_json
        let parts: Vec<&str> = cmd.splitn(3, ':').collect();
        if parts.is_empty() || parts[0] != "catchment" {
            return Err(Status::invalid_argument(
                "do_exchange supports 'catchment:profile[:params_json]'",
            ));
        }
        let profile = parts.get(1).copied().unwrap_or("car");
        let params_json = parts.get(2).copied().unwrap_or("{}");
        let mode = resolve_mode(profile, &state)?;

        let cp = super::catchment::parse_exchange_params(params_json)
            .map_err(Status::invalid_argument)?;

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

        do_exchange_catchment(state, mode, cp, &batches).await
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
