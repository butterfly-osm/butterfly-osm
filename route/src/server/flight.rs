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

use super::geometry::{Point, build_isochrone_geometry, build_raw_points};
use super::isochrone_handler::{run_phast_bounded_fast, run_phast_bounded_fast_reverse};
use super::query::CchQuery;
use super::state::ServerState;
use super::unpack::unpack_path;

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

/// Snap coordinates to ranks, returning (ranks, valid_indices).
fn snap_to_ranks(
    coords: &[[f64; 2]],
    state: &ServerState,
    mode_data: &super::state::ModeData,
) -> (Vec<u32>, Vec<usize>) {
    let mut ranks = Vec::with_capacity(coords.len());
    let mut valid = Vec::with_capacity(coords.len());
    for (i, [lon, lat]) in coords.iter().enumerate() {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &mode_data.mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                let rank = mode_data.order.perm[filtered as usize];
                ranks.push(rank);
                valid.push(i);
            }
        }
    }
    (ranks, valid)
}

/// Build a full-length (n = coords.len()) snapped coordinate vector. Points
/// that fail to snap keep their original lon/lat — they will still be marked
/// invalid in the matrix, but having them in the vector keeps indexing
/// straightforward for the Euclidean pre-filter.
fn snapped_coords_full(
    coords: &[[f64; 2]],
    state: &ServerState,
    mode_data: &super::state::ModeData,
) -> Vec<(f64, f64)> {
    let mut out = Vec::with_capacity(coords.len());
    for [lon, lat] in coords {
        if let Some(orig_id) = state.spatial_index.snap(*lon, *lat, &mode_data.mask, 10) {
            let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
            if filtered != u32::MAX {
                let loc = super::types::get_node_location(state, orig_id);
                out.push((loc[0], loc[1]));
                continue;
            }
        }
        out.push((*lon, *lat));
    }
    out
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

type BatchStream =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<RecordBatch, Status>> + Send>>;

/// Execute the matrix Flight action.
fn do_matrix(
    state: &Arc<ServerState>,
    mode: Mode,
    params: MatrixParams,
) -> std::result::Result<BatchStream, Status> {
    let mode_data = state.get_mode(mode);
    let n_nodes = mode_data.cch_topo.n_nodes as usize;

    let (sources_rank, valid_src) = snap_to_ranks(&params.sources, state, mode_data);
    let (targets_rank, valid_dst) = snap_to_ranks(&params.destinations, state, mode_data);

    if sources_rank.is_empty() || targets_rank.is_empty() {
        let schema = Arc::new(matrix_schema());
        let empty = RecordBatch::new_empty(schema);
        return Ok(Box::pin(stream::once(async move { Ok(empty) })));
    }

    // Full-length snapped coordinates — for the Euclidean pre-filter.
    let sources_snapped = snapped_coords_full(&params.sources, state, mode_data);
    let targets_snapped = snapped_coords_full(&params.destinations, state, mode_data);

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

        let (matrix, _stats) = if use_parallel {
            table_bucket_parallel(n_nodes, up, down, &sources_rank, &targets_rank)
        } else {
            table_bucket_full_flat(n_nodes, up, down, &sources_rank, &targets_rank)
        };

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

/// Encode a LineString as WKB (Well-Known Binary), little-endian.
fn encode_linestring_wkb(points: &[Point]) -> Vec<u8> {
    let n = points.len();
    let buf_size = 1 + 4 + 4 + n * 16;
    let mut buf = Vec::with_capacity(buf_size);

    buf.push(1u8); // little-endian
    let _ = buf.write_all(&2u32.to_le_bytes()); // LineString type
    let _ = buf.write_all(&(n as u32).to_le_bytes());
    for p in points {
        let _ = buf.write_all(&p.lon.to_le_bytes());
        let _ = buf.write_all(&p.lat.to_le_bytes());
    }
    buf
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
        let batch_size = 1000usize;

        for chunk in params.pairs.chunks(batch_size) {
            let n = chunk.len();
            let mut src_lon_arr = Float64Builder::with_capacity(n);
            let mut src_lat_arr = Float64Builder::with_capacity(n);
            let mut dst_lon_arr = Float64Builder::with_capacity(n);
            let mut dst_lat_arr = Float64Builder::with_capacity(n);
            let mut dur_arr = Float32Builder::with_capacity(n);
            let mut dist_arr = Float32Builder::with_capacity(n);
            let mut geom_arr = BinaryBuilder::with_capacity(n, n * 256);

            for pair in chunk {
                let (slon, slat, dlon, dlat) = (pair[0], pair[1], pair[2], pair[3]);
                src_lon_arr.append_value(slon);
                src_lat_arr.append_value(slat);
                dst_lon_arr.append_value(dlon);
                dst_lat_arr.append_value(dlat);

                let src_snap = state.spatial_index.snap(slon, slat, &mode_data.mask, 10);
                let dst_snap = state.spatial_index.snap(dlon, dlat, &mode_data.mask, 10);

                match (src_snap, dst_snap) {
                    (Some(src_orig), Some(dst_orig)) => {
                        let src_filt =
                            mode_data.filtered_ebg.original_to_filtered[src_orig as usize];
                        let dst_filt =
                            mode_data.filtered_ebg.original_to_filtered[dst_orig as usize];

                        if src_filt == u32::MAX || dst_filt == u32::MAX {
                            dur_arr.append_value(f32::NAN);
                            dist_arr.append_value(f32::NAN);
                            geom_arr.append_value(&[] as &[u8]);
                            continue;
                        }

                        let src_rank = mode_data.order.perm[src_filt as usize];
                        let dst_rank = mode_data.order.perm[dst_filt as usize];

                        let query = CchQuery::new(&state, mode);
                        match query.query(src_rank, dst_rank) {
                            Some(result) => {
                                let duration_s = result.distance as f64 / 10.0;

                                let rank_path = unpack_path(
                                    &mode_data.cch_topo,
                                    &mode_data.cch_weights,
                                    &result.forward_parent,
                                    &result.backward_parent,
                                    src_rank,
                                    dst_rank,
                                    result.meeting_node,
                                );
                                let ebg_path: Vec<u32> = rank_path
                                    .iter()
                                    .map(|&rank| {
                                        let filt_id =
                                            mode_data.cch_topo.rank_to_filtered[rank as usize];
                                        mode_data.filtered_ebg.filtered_to_original
                                            [filt_id as usize]
                                    })
                                    .collect();

                                let (points, distance_m) =
                                    build_raw_points(&ebg_path, &state.ebg_nodes, &state.nbg_geo);

                                let wkb = encode_linestring_wkb(&points);

                                dur_arr.append_value(duration_s as f32);
                                dist_arr.append_value(distance_m as f32);
                                geom_arr.append_value(&wkb);
                            }
                            None => {
                                dur_arr.append_value(f32::NAN);
                                dist_arr.append_value(f32::NAN);
                                geom_arr.append_value(&[] as &[u8]);
                            }
                        }
                    }
                    _ => {
                        dur_arr.append_value(f32::NAN);
                        dist_arr.append_value(f32::NAN);
                        geom_arr.append_value(&[] as &[u8]);
                    }
                }
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

    let orig_id = state
        .spatial_index
        .snap(params.lon, params.lat, &mode_data.mask, 10)
        .ok_or_else(|| Status::not_found("Could not snap to road network"))?;
    let filtered = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
    if filtered == u32::MAX {
        return Err(Status::not_found(
            "Snapped node not accessible for this mode",
        ));
    }
    let origin_rank = mode_data.order.perm[filtered as usize];

    let is_reverse = params.direction.to_lowercase() == "arrive";
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
            &mode_data.cch_topo,
            &mode_data.cch_weights,
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
            let orig_id = mode_data.filtered_ebg.filtered_to_original[filt_id as usize];
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
            &state.nbg_geo,
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

            // Snap store
            let store_snap = state.spatial_index.snap(*slon, *slat, &mode_data.mask, 10);
            let store_orig = match store_snap {
                Some(id) => id,
                None => continue,
            };
            let store_filt = mode_data.filtered_ebg.original_to_filtered[store_orig as usize];
            if store_filt == u32::MAX {
                continue;
            }
            let store_rank = mode_data.order.perm[store_filt as usize];

            // Snap all clients
            let mut client_ranks: Vec<u32> = Vec::with_capacity(client_coords.len());
            let mut client_valid: Vec<usize> = Vec::with_capacity(client_coords.len());
            for (ci, &(clon, clat)) in client_coords.iter().enumerate() {
                if let Some(orig_id) = state.spatial_index.snap(clon, clat, &mode_data.mask, 10) {
                    let filt = mode_data.filtered_ebg.original_to_filtered[orig_id as usize];
                    if filt != u32::MAX {
                        let rank = mode_data.order.perm[filt as usize];
                        client_ranks.push(rank);
                        client_valid.push(ci);
                    }
                }
            }

            if client_ranks.is_empty() {
                continue;
            }

            // 1-to-N matrix
            let (matrix, _stats) = table_bucket_full_flat(
                n_nodes,
                &mode_data.up_adj_flat,
                &mode_data.down_rev_flat,
                &[store_rank],
                &client_ranks,
            );

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

        if store_idx_b.len() > 0 {
            if let Ok(batch) = RecordBatch::try_new(
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
            ) {
                let _ = tx.blocking_send(Ok(batch));
            }
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
            other => Err(Status::invalid_argument(format!(
                "Unknown action '{}'. Available: matrix, route_batch, isochrone",
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
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown action '{}'. Available: matrix, route_batch, isochrone, catchment",
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
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown action '{}'. Available: matrix, route_batch, isochrone, catchment",
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
            if descriptor_cmd.is_empty() {
                if let Some(ref desc) = fd.flight_descriptor {
                    descriptor_cmd = desc.cmd.to_vec();
                }
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
