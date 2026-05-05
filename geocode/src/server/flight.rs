//! Arrow Flight gRPC server for butterfly-geocode (#145).
//!
//! Provides a single batch action — `geocode_batch` — that accepts a
//! RecordBatch of `(query, country)` rows and streams a RecordBatch of
//! geocoded results, one row per input query (preserved order via
//! `query_idx`).
//!
//! Why Flight: bulk geocoding workloads (data-pipeline backfills,
//! address-book uploads, ETL jobs) hit REST limits hard — URL length,
//! JSON parsing overhead, no native batching. Flight gives us
//! columnar Arrow IPC with backpressure, cancellation on disconnect,
//! and 10-100× throughput vs equivalent REST loops at scale.
//!
//! Action protocol:
//! - DoGet ticket format: `geocode_batch[:<json>]` where the JSON
//!   carries both the params and the inline query list — used for
//!   smoke testing where the query list fits in the ticket.
//! - DoExchange (descriptor cmd `geocode_batch[:<params_json>]`):
//!   client uploads input RecordBatches over the request stream,
//!   server streams output RecordBatches. This is the canonical bulk
//!   path.

use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use arrow::array::{
    Array, ArrayRef, Float32Array, Float32Builder, Float64Array, Float64Builder, ListArray,
    ListBuilder, RecordBatch, StringArray, StringBuilder, UInt32Array, UInt32Builder,
};
use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use arrow::ipc::writer::StreamWriter;
use arrow_flight::Result as FlightResult;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_server::{FlightService, FlightServiceServer};
use arrow_flight::{
    Action, ActionType, Criteria, Empty, FlightData, FlightDescriptor, FlightInfo,
    HandshakeRequest, HandshakeResponse, PollInfo, PutResult, SchemaResult, Ticket,
};
use bytes::Bytes;
use futures::StreamExt;
use futures::stream;
use rayon::prelude::*;
use serde::{Deserialize, Serialize};
use tonic::{Request, Response, Status, Streaming};

use crate::control::budget::compute_budget;
use crate::geocoder::executor::{GeocodedResult, apply_rerank, execute_with_control};
use crate::routing::CountryId;

use super::state::ServerState;

// =============================================================================
// Types
// =============================================================================

/// Action body for the `geocode_batch` DoGet/DoExchange request.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct GeocodeBatchParams {
    #[serde(default = "default_limit")]
    pub limit: u32,
    #[serde(default)]
    pub include_debug: bool,
    #[serde(default)]
    pub group_by_country: bool,
}

fn default_limit() -> u32 {
    5
}

impl Default for GeocodeBatchParams {
    fn default() -> Self {
        Self {
            limit: default_limit(),
            include_debug: false,
            group_by_country: false,
        }
    }
}

/// Per-query input row decoded from the input RecordBatch.
#[derive(Debug, Clone)]
struct InputRow {
    /// Original index into the inbound RecordBatch (preserved through
    /// rayon shuffling so output rows can be re-ordered to input
    /// order).
    idx: u32,
    query: String,
    country: Option<String>,
}

/// Outcome of geocoding a single input row.
#[derive(Debug, Clone)]
struct OutputRow {
    idx: u32,
    /// Top-1 candidate, if any.
    top: Option<GeocodedResult>,
    /// Confidence tier of the top result. Stored as `&'static str`
    /// instead of [`Confidence`] so we can encode the "no result"
    /// outcome as `"empty"` — the existing `Confidence` enum only
    /// covers the four scoring tiers.
    confidence: &'static str,
    /// Country the row was actually dispatched to. `None` when no
    /// shard could be selected.
    country: Option<CountryId>,
    /// Reason codes when `include_debug` is set; empty otherwise.
    reason_codes: Vec<String>,
}

// =============================================================================
// Boot
// =============================================================================

/// Butterfly-geocode Flight service — wraps the shared [`ServerState`].
#[derive(Debug)]
pub struct GeocodeFlight {
    state: Arc<ServerState>,
}

impl GeocodeFlight {
    pub fn new(state: Arc<ServerState>) -> Self {
        Self { state }
    }
}

/// Build a configured `FlightServiceServer`.
pub fn build_flight_server(state: Arc<ServerState>) -> FlightServiceServer<GeocodeFlight> {
    FlightServiceServer::new(GeocodeFlight::new(state))
        .max_encoding_message_size(64 * 1024 * 1024)
        .max_decoding_message_size(64 * 1024 * 1024)
}

// =============================================================================
// Schemas
// =============================================================================

/// Input RecordBatch schema for the `geocode_batch` action.
///
/// - `query`: required UTF-8
/// - `country`: nullable UTF-8 (ISO 3166-1 alpha-2)
pub fn geocode_batch_input_schema() -> Schema {
    Schema::new(vec![
        Field::new("query", DataType::Utf8, false),
        Field::new("country", DataType::Utf8, true),
    ])
}

/// Output RecordBatch schema. One row per input query, preserved
/// order via `query_idx`.
pub fn geocode_batch_output_schema() -> Schema {
    // The inner item field is marked nullable to match what
    // `ListBuilder<StringBuilder>` emits — null inner items never
    // actually appear in our payload, but Arrow's typecheck rejects
    // a non-null field declaration against a nullable-array builder.
    let reason_item = Field::new("item", DataType::Utf8, true);
    Schema::new(vec![
        Field::new("query_idx", DataType::UInt32, false),
        Field::new("lat", DataType::Float64, true),
        Field::new("lon", DataType::Float64, true),
        Field::new("score", DataType::Float32, true),
        Field::new("confidence", DataType::Utf8, false),
        Field::new("street", DataType::Utf8, true),
        Field::new("housenumber", DataType::Utf8, true),
        Field::new("postcode", DataType::Utf8, true),
        Field::new("locality", DataType::Utf8, true),
        Field::new("country", DataType::Utf8, true),
        Field::new("reason_codes", DataType::List(Arc::new(reason_item)), false),
    ])
}

// =============================================================================
// Flight stream plumbing
// =============================================================================

pub type BatchStream =
    Pin<Box<dyn futures::Stream<Item = std::result::Result<RecordBatch, Status>> + Send>>;

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
// Ticket / params parsing
// =============================================================================

fn parse_descriptor_params(cmd: &[u8]) -> std::result::Result<GeocodeBatchParams, Status> {
    let s = std::str::from_utf8(cmd)
        .map_err(|_| Status::invalid_argument("descriptor cmd must be UTF-8"))?;
    let (action, rest) = match s.find(':') {
        Some(idx) => (&s[..idx], &s[idx + 1..]),
        None => (s, ""),
    };
    if action != "geocode_batch" {
        return Err(Status::invalid_argument(format!(
            "Unknown action '{}'. Available: geocode_batch",
            action
        )));
    }
    if rest.trim().is_empty() {
        return Ok(GeocodeBatchParams::default());
    }
    serde_json::from_str(rest)
        .map_err(|e| Status::invalid_argument(format!("Invalid params JSON: {}", e)))
}

#[cfg(test)]
fn parse_ticket_params(ticket: &Ticket) -> std::result::Result<GeocodeBatchParams, Status> {
    parse_descriptor_params(&ticket.ticket)
}

// =============================================================================
// Input decoding
// =============================================================================

fn decode_input_batches(batches: &[RecordBatch]) -> std::result::Result<Vec<InputRow>, Status> {
    let mut rows: Vec<InputRow> = Vec::new();
    let mut next_idx: u32 = 0;

    for batch in batches {
        let schema = batch.schema();

        let query_field = schema.column_with_name("query").ok_or_else(|| {
            Status::invalid_argument("input RecordBatch must have a `query` column")
        })?;
        if query_field.1.data_type() != &DataType::Utf8 {
            return Err(Status::invalid_argument("`query` column must be Utf8"));
        }

        let query_arr = batch
            .column(query_field.0)
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| Status::internal("query column not a StringArray"))?;

        let country_arr: Option<&StringArray> = match schema.column_with_name("country") {
            Some((idx, field)) => {
                if field.data_type() != &DataType::Utf8 {
                    return Err(Status::invalid_argument("`country` column must be Utf8"));
                }
                Some(
                    batch
                        .column(idx)
                        .as_any()
                        .downcast_ref::<StringArray>()
                        .ok_or_else(|| Status::internal("country column not a StringArray"))?,
                )
            }
            None => None,
        };

        for i in 0..batch.num_rows() {
            if query_arr.is_null(i) {
                return Err(Status::invalid_argument(format!(
                    "row {} has null `query` (column is required-non-null)",
                    next_idx
                )));
            }
            let query = query_arr.value(i).to_string();
            let country = country_arr.and_then(|a| {
                if a.is_null(i) {
                    None
                } else {
                    Some(a.value(i).to_string())
                }
            });
            rows.push(InputRow {
                idx: next_idx,
                query,
                country,
            });
            next_idx = next_idx
                .checked_add(1)
                .ok_or_else(|| Status::invalid_argument("input row count exceeds u32"))?;
        }
    }

    Ok(rows)
}

// =============================================================================
// Per-query processing
// =============================================================================

/// Resolve the dispatch country for an input row. Pinned country wins
/// when a shard is loaded for it; otherwise the cheap lexical
/// classifier picks the top loaded shard. `None` means no shard could
/// be selected.
fn resolve_dispatch(query: &str, pinned: Option<&str>, state: &ServerState) -> Option<CountryId> {
    if let Some(code) = pinned
        && let Some(c) = CountryId::from_iso2(code)
        && state.shards.contains_key(&c)
    {
        return Some(c);
    }
    let posterior = state.classifier.classify(query);
    state.pick_shard(&posterior).map(|(c, _)| c)
}

/// Run the full geocode pipeline against one input row, mirroring the
/// REST `forward` handler exactly.
fn process_row(row: &InputRow, state: &ServerState, limit: u32, include_debug: bool) -> OutputRow {
    let limit_usize = (limit as usize).clamp(1, 50);

    if row.query.trim().is_empty() || row.query.chars().count() > 512 {
        return OutputRow {
            idx: row.idx,
            top: None,
            confidence: "reject",
            country: None,
            reason_codes: if include_debug {
                vec!["INPUT_REJECTED".to_string()]
            } else {
                Vec::new()
            },
        };
    }

    let dispatch_country = match resolve_dispatch(&row.query, row.country.as_deref(), state) {
        Some(c) => c,
        None => {
            return OutputRow {
                idx: row.idx,
                top: None,
                confidence: "reject",
                country: None,
                reason_codes: if include_debug {
                    vec!["NO_SHARD_LOADED".to_string()]
                } else {
                    Vec::new()
                },
            };
        }
    };
    let shard = state
        .shards
        .get(&dispatch_country)
        .expect("dispatch_country must be loaded");

    let mut parsed = match state.parser.parse(&row.query, dispatch_country, shard) {
        Ok(p) => p,
        Err(_) => {
            return OutputRow {
                idx: row.idx,
                top: None,
                confidence: "reject",
                country: Some(dispatch_country),
                reason_codes: if include_debug {
                    vec!["PARSER_ERROR".to_string()]
                } else {
                    Vec::new()
                },
            };
        }
    };
    parsed.country_candidates = vec![(dispatch_country, 1.0)];

    let stats = shard.stats();
    parsed.execution_budget = compute_budget(&parsed, stats, state.control.budget_policy);

    let raw = match execute_with_control(&parsed, shard, limit_usize, &state.control) {
        Ok(r) => r,
        Err(_) => {
            return OutputRow {
                idx: row.idx,
                top: None,
                confidence: "reject",
                country: Some(dispatch_country),
                reason_codes: if include_debug {
                    vec!["ADMISSION_REJECTED".to_string()]
                } else {
                    Vec::new()
                },
            };
        }
    };

    let (mut ranked, action) = apply_rerank(
        raw,
        &parsed,
        shard,
        state.rerank_model.as_ref(),
        &state.confidence_config,
    );

    let top = if ranked.is_empty() {
        None
    } else {
        Some(ranked.swap_remove(0))
    };

    let reason_codes = if include_debug {
        match &top {
            Some(t) => t.reason_codes.iter().map(|c| c.to_string()).collect(),
            None => Vec::new(),
        }
    } else {
        Vec::new()
    };

    let confidence = if top.is_some() {
        action.as_str()
    } else {
        "empty"
    };

    OutputRow {
        idx: row.idx,
        top,
        confidence,
        country: Some(dispatch_country),
        reason_codes,
    }
}

// =============================================================================
// Output encoding
// =============================================================================

fn encode_output_chunk(
    schema: SchemaRef,
    rows: &[OutputRow],
) -> std::result::Result<RecordBatch, Status> {
    let n = rows.len();

    let mut idx_b = UInt32Builder::with_capacity(n);
    let mut lat_b = Float64Builder::with_capacity(n);
    let mut lon_b = Float64Builder::with_capacity(n);
    let mut score_b = Float32Builder::with_capacity(n);
    let mut confidence_b = StringBuilder::with_capacity(n, n * 8);
    let mut street_b = StringBuilder::with_capacity(n, n * 16);
    let mut housenumber_b = StringBuilder::with_capacity(n, n * 8);
    let mut postcode_b = StringBuilder::with_capacity(n, n * 6);
    let mut locality_b = StringBuilder::with_capacity(n, n * 16);
    let mut country_b = StringBuilder::with_capacity(n, n * 4);
    let inner_string_b = StringBuilder::new();
    let mut reason_b: ListBuilder<StringBuilder> = ListBuilder::new(inner_string_b);

    for row in rows {
        idx_b.append_value(row.idx);
        confidence_b.append_value(row.confidence);

        // The country column reflects the candidate's own country
        // (set by `execute_multi` when results span multiple shards)
        // when present, falling back to the dispatch country
        // (which shard answered) otherwise. This way the column is
        // populated whether or not a candidate was found.
        let country_str = row
            .top
            .as_ref()
            .and_then(|t| t.country)
            .or_else(|| row.country.map(|c| c.iso2()));
        match country_str {
            Some(s) => country_b.append_value(s),
            None => country_b.append_null(),
        }

        match &row.top {
            Some(t) => {
                lat_b.append_value(t.lat);
                lon_b.append_value(t.lon);
                score_b.append_value(t.score);
                if t.street.is_empty() {
                    street_b.append_null();
                } else {
                    street_b.append_value(&t.street);
                }
                if t.housenumber.is_empty() {
                    housenumber_b.append_null();
                } else {
                    housenumber_b.append_value(&t.housenumber);
                }
                if t.postcode.is_empty() {
                    postcode_b.append_null();
                } else {
                    postcode_b.append_value(&t.postcode);
                }
                if t.locality.is_empty() {
                    locality_b.append_null();
                } else {
                    locality_b.append_value(&t.locality);
                }
            }
            None => {
                lat_b.append_null();
                lon_b.append_null();
                score_b.append_null();
                street_b.append_null();
                housenumber_b.append_null();
                postcode_b.append_null();
                locality_b.append_null();
            }
        }

        let inner = reason_b.values();
        for code in &row.reason_codes {
            inner.append_value(code);
        }
        reason_b.append(true);
    }

    let columns: Vec<ArrayRef> = vec![
        Arc::new(idx_b.finish()) as ArrayRef,
        Arc::new(lat_b.finish()),
        Arc::new(lon_b.finish()),
        Arc::new(score_b.finish()),
        Arc::new(confidence_b.finish()),
        Arc::new(street_b.finish()),
        Arc::new(housenumber_b.finish()),
        Arc::new(postcode_b.finish()),
        Arc::new(locality_b.finish()),
        Arc::new(country_b.finish()),
        Arc::new(reason_b.finish()),
    ];

    RecordBatch::try_new(schema, columns)
        .map_err(|e| Status::internal(format!("output RecordBatch build: {}", e)))
}

// =============================================================================
// Core: do_geocode_batch
// =============================================================================

fn do_geocode_batch(
    state: Arc<ServerState>,
    rows: Vec<InputRow>,
    params: GeocodeBatchParams,
) -> std::result::Result<BatchStream, Status> {
    if rows.len() > 500_000 {
        return Err(Status::invalid_argument(
            "max 500,000 queries per geocode_batch request",
        ));
    }

    let schema = Arc::new(geocode_batch_output_schema());

    if rows.is_empty() {
        let empty = RecordBatch::new_empty(schema);
        return Ok(Box::pin(stream::once(async move { Ok(empty) })));
    }

    // Bounded channel — small buffer pushes back on the producer when
    // the consumer (the gRPC stream over the wire) lags, preventing
    // unbounded memory growth on slow clients.
    let (tx, rx) = tokio::sync::mpsc::channel::<std::result::Result<RecordBatch, Status>>(8);

    let cancelled = Arc::new(AtomicBool::new(false));

    tokio::task::spawn_blocking(move || {
        run_batch_blocking(state, rows, params, schema, tx, cancelled);
    });

    let stream = tokio_stream::wrappers::ReceiverStream::new(rx);
    Ok(Box::pin(stream))
}

fn run_batch_blocking(
    state: Arc<ServerState>,
    rows: Vec<InputRow>,
    params: GeocodeBatchParams,
    schema: SchemaRef,
    tx: tokio::sync::mpsc::Sender<std::result::Result<RecordBatch, Status>>,
    cancelled: Arc<AtomicBool>,
) {
    const CHUNK: usize = 1024;
    let limit = params.limit;
    let include_debug = params.include_debug;

    let processed: Vec<OutputRow> = if params.group_by_country {
        let mut grouped: std::collections::BTreeMap<String, Vec<InputRow>> =
            std::collections::BTreeMap::new();
        for r in rows {
            let key = r.country.clone().unwrap_or_default();
            grouped.entry(key).or_default().push(r);
        }
        grouped
            .into_values()
            .flat_map(|group| {
                group
                    .par_iter()
                    .map(|r| process_row(r, state.as_ref(), limit, include_debug))
                    .collect::<Vec<_>>()
            })
            .collect()
    } else {
        rows.par_iter()
            .map(|r| process_row(r, state.as_ref(), limit, include_debug))
            .collect()
    };

    let mut processed = processed;
    processed.sort_by_key(|r| r.idx);

    for chunk in processed.chunks(CHUNK) {
        if cancelled.load(Ordering::Relaxed) {
            return;
        }
        let batch = match encode_output_chunk(schema.clone(), chunk) {
            Ok(b) => b,
            Err(e) => {
                let _ = tx.blocking_send(Err(e));
                return;
            }
        };
        if tx.blocking_send(Ok(batch)).is_err() {
            cancelled.store(true, Ordering::Relaxed);
            return;
        }
    }
}

// =============================================================================
// FlightService impl
// =============================================================================

#[tonic::async_trait]
impl FlightService for GeocodeFlight {
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
        let s = std::str::from_utf8(&ticket.ticket)
            .map_err(|_| Status::invalid_argument("Ticket must be UTF-8"))?;
        let (action, rest) = match s.find(':') {
            Some(idx) => (&s[..idx], &s[idx + 1..]),
            None => (s, ""),
        };
        if action != "geocode_batch" {
            return Err(Status::invalid_argument(format!(
                "Unknown action '{}'. Available: geocode_batch",
                action
            )));
        }

        #[derive(Deserialize)]
        struct DoGetBody {
            #[serde(default)]
            params: GeocodeBatchParams,
            queries: Vec<DoGetQuery>,
        }
        #[derive(Deserialize)]
        struct DoGetQuery {
            query: String,
            #[serde(default)]
            country: Option<String>,
        }

        let body: DoGetBody = serde_json::from_str(rest)
            .map_err(|e| Status::invalid_argument(format!("Invalid DoGet body JSON: {}", e)))?;

        let rows: Vec<InputRow> = body
            .queries
            .into_iter()
            .enumerate()
            .map(|(i, q)| InputRow {
                idx: u32::try_from(i).unwrap_or(u32::MAX),
                query: q.query,
                country: q.country,
            })
            .collect();

        let batch_stream = do_geocode_batch(Arc::clone(&self.state), rows, body.params)?;
        let schema = Arc::new(geocode_batch_output_schema());
        let flight_stream = batches_to_flight_data(schema, batch_stream);
        Ok(Response::new(flight_stream))
    }

    async fn get_flight_info(
        &self,
        request: Request<FlightDescriptor>,
    ) -> std::result::Result<Response<FlightInfo>, Status> {
        let descriptor = request.into_inner();
        let _params = parse_descriptor_params(&descriptor.cmd)?;
        let schema = geocode_batch_output_schema();
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
            s if s.starts_with("geocode_batch") => geocode_batch_output_schema(),
            "geocode_batch_input" => geocode_batch_input_schema(),
            other => {
                return Err(Status::invalid_argument(format!(
                    "Unknown schema name '{}'. Available: geocode_batch, geocode_batch_input",
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
        let actions = vec![ActionType {
            r#type: "geocode_batch".into(),
            description:
                "Batch forward geocoding via Arrow IPC. DoExchange cmd: geocode_batch[:params_json]. \
                 Input: RecordBatch(query: Utf8, country: Utf8?). \
                 Output: RecordBatch(query_idx, lat?, lon?, score?, confidence, street?, housenumber?, \
                 postcode?, locality?, country?, reason_codes: List<Utf8>). Up to 500k queries per call."
                    .into(),
        }];
        let stream = stream::iter(actions.into_iter().map(Ok));
        Ok(Response::new(Box::pin(stream) as Self::ListActionsStream))
    }

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

        let params = parse_descriptor_params(&descriptor_cmd)?;

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
            let schema = Arc::new(geocode_batch_output_schema());
            let empty = RecordBatch::new_empty(schema.clone());
            let batch_stream: BatchStream = Box::pin(stream::once(async move { Ok(empty) }));
            let flight_stream = batches_to_flight_data(schema, batch_stream);
            return Ok(Response::new(flight_stream));
        }

        let rows = decode_input_batches(&batches)?;
        let batch_stream = do_geocode_batch(state, rows, params)?;
        let schema = Arc::new(geocode_batch_output_schema());
        let flight_stream = batches_to_flight_data(schema, batch_stream);
        Ok(Response::new(flight_stream))
    }

    async fn do_action(
        &self,
        _request: Request<Action>,
    ) -> std::result::Result<Response<Self::DoActionStream>, Status> {
        Err(Status::unimplemented(
            "DoAction not supported. Use DoGet/DoExchange with action 'geocode_batch'.",
        ))
    }
}

// =============================================================================
// Public client-side helpers (used by the CLI subcommand and tests).
// =============================================================================

/// Build an input RecordBatch from a slice of (query, country) tuples.
pub fn build_input_batch(
    queries: &[(String, Option<String>)],
) -> std::result::Result<RecordBatch, arrow::error::ArrowError> {
    let queries_arr: StringArray = queries.iter().map(|(q, _)| Some(q.as_str())).collect();
    let country_arr: StringArray = queries.iter().map(|(_, c)| c.as_deref()).collect();
    let schema = Arc::new(geocode_batch_input_schema());
    RecordBatch::try_new(schema, vec![Arc::new(queries_arr), Arc::new(country_arr)])
}

/// Decoded result row produced by [`decode_output_batch`].
#[derive(Debug, Clone, Serialize)]
pub struct DecodedResult {
    pub query_idx: u32,
    pub lat: Option<f64>,
    pub lon: Option<f64>,
    pub score: Option<f32>,
    pub confidence: String,
    pub street: Option<String>,
    pub housenumber: Option<String>,
    pub postcode: Option<String>,
    pub locality: Option<String>,
    pub country: Option<String>,
    pub reason_codes: Vec<String>,
}

/// Decode an output RecordBatch into a flat `Vec<DecodedResult>`.
pub fn decode_output_batch(
    batch: &RecordBatch,
) -> std::result::Result<Vec<DecodedResult>, arrow::error::ArrowError> {
    let n = batch.num_rows();
    let mk_err = arrow::error::ArrowError::SchemaError;

    let idx = batch
        .column_by_name("query_idx")
        .and_then(|c| c.as_any().downcast_ref::<UInt32Array>())
        .ok_or_else(|| mk_err("missing or wrong-typed query_idx column".into()))?;
    let lat = batch
        .column_by_name("lat")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| mk_err("missing or wrong-typed lat column".into()))?;
    let lon = batch
        .column_by_name("lon")
        .and_then(|c| c.as_any().downcast_ref::<Float64Array>())
        .ok_or_else(|| mk_err("missing or wrong-typed lon column".into()))?;
    let score = batch
        .column_by_name("score")
        .and_then(|c| c.as_any().downcast_ref::<Float32Array>())
        .ok_or_else(|| mk_err("missing or wrong-typed score column".into()))?;
    let confidence = batch
        .column_by_name("confidence")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| mk_err("missing or wrong-typed confidence column".into()))?;
    let street = batch
        .column_by_name("street")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| mk_err("missing or wrong-typed street column".into()))?;
    let housenumber = batch
        .column_by_name("housenumber")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| mk_err("missing or wrong-typed housenumber column".into()))?;
    let postcode = batch
        .column_by_name("postcode")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| mk_err("missing or wrong-typed postcode column".into()))?;
    let locality = batch
        .column_by_name("locality")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| mk_err("missing or wrong-typed locality column".into()))?;
    let country = batch
        .column_by_name("country")
        .and_then(|c| c.as_any().downcast_ref::<StringArray>())
        .ok_or_else(|| mk_err("missing or wrong-typed country column".into()))?;
    let reason_codes = batch
        .column_by_name("reason_codes")
        .and_then(|c| c.as_any().downcast_ref::<ListArray>())
        .ok_or_else(|| mk_err("missing or wrong-typed reason_codes column".into()))?;

    let mut out = Vec::with_capacity(n);
    for i in 0..n {
        let codes_array = reason_codes.value(i);
        let codes_str = codes_array
            .as_any()
            .downcast_ref::<StringArray>()
            .ok_or_else(|| mk_err("reason_codes inner not Utf8".into()))?;
        let mut codes_vec = Vec::with_capacity(codes_str.len());
        for j in 0..codes_str.len() {
            if !codes_str.is_null(j) {
                codes_vec.push(codes_str.value(j).to_string());
            }
        }

        out.push(DecodedResult {
            query_idx: idx.value(i),
            lat: if lat.is_null(i) {
                None
            } else {
                Some(lat.value(i))
            },
            lon: if lon.is_null(i) {
                None
            } else {
                Some(lon.value(i))
            },
            score: if score.is_null(i) {
                None
            } else {
                Some(score.value(i))
            },
            confidence: confidence.value(i).to_string(),
            street: if street.is_null(i) {
                None
            } else {
                Some(street.value(i).to_string())
            },
            housenumber: if housenumber.is_null(i) {
                None
            } else {
                Some(housenumber.value(i).to_string())
            },
            postcode: if postcode.is_null(i) {
                None
            } else {
                Some(postcode.value(i).to_string())
            },
            locality: if locality.is_null(i) {
                None
            } else {
                Some(locality.value(i).to_string())
            },
            country: if country.is_null(i) {
                None
            } else {
                Some(country.value(i).to_string())
            },
            reason_codes: codes_vec,
        });
    }
    Ok(out)
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn input_schema_matches_spec() {
        let s = geocode_batch_input_schema();
        assert_eq!(s.fields().len(), 2);
        let q = s.field(0);
        assert_eq!(q.name(), "query");
        assert_eq!(q.data_type(), &DataType::Utf8);
        assert!(!q.is_nullable());
        let c = s.field(1);
        assert_eq!(c.name(), "country");
        assert_eq!(c.data_type(), &DataType::Utf8);
        assert!(c.is_nullable());
    }

    #[test]
    fn output_schema_matches_spec() {
        let s = geocode_batch_output_schema();
        let names: Vec<&str> = s.fields().iter().map(|f| f.name().as_str()).collect();
        assert_eq!(
            names,
            vec![
                "query_idx",
                "lat",
                "lon",
                "score",
                "confidence",
                "street",
                "housenumber",
                "postcode",
                "locality",
                "country",
                "reason_codes",
            ]
        );

        let f = |name: &str| s.field_with_name(name).unwrap();
        assert!(!f("query_idx").is_nullable());
        assert!(!f("confidence").is_nullable());
        assert!(!f("reason_codes").is_nullable());
        assert!(f("lat").is_nullable());
        assert!(f("lon").is_nullable());
        assert!(f("score").is_nullable());
        assert!(f("street").is_nullable());
        assert!(f("housenumber").is_nullable());
        assert!(f("postcode").is_nullable());
        assert!(f("locality").is_nullable());
        assert!(f("country").is_nullable());

        assert_eq!(f("query_idx").data_type(), &DataType::UInt32);
        assert_eq!(f("lat").data_type(), &DataType::Float64);
        assert_eq!(f("lon").data_type(), &DataType::Float64);
        assert_eq!(f("score").data_type(), &DataType::Float32);
        match f("reason_codes").data_type() {
            DataType::List(inner) => {
                assert_eq!(inner.data_type(), &DataType::Utf8);
            }
            other => panic!("reason_codes type wrong: {:?}", other),
        }
    }

    #[test]
    fn build_input_batch_round_trips() {
        let queries = vec![
            (
                "Rue de la Loi 16, 1000 Bruxelles".to_string(),
                Some("BE".to_string()),
            ),
            ("Grote Markt 1, Antwerpen".to_string(), None),
        ];
        let batch = build_input_batch(&queries).unwrap();
        assert_eq!(batch.num_rows(), 2);
        let rows = decode_input_batches(&[batch]).unwrap();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].idx, 0);
        assert_eq!(rows[0].query, "Rue de la Loi 16, 1000 Bruxelles");
        assert_eq!(rows[0].country.as_deref(), Some("BE"));
        assert_eq!(rows[1].idx, 1);
        assert_eq!(rows[1].country, None);
    }

    #[test]
    fn parse_ticket_params_round_trip() {
        let t = Ticket {
            ticket: Bytes::from_static(b"geocode_batch"),
        };
        let p = parse_ticket_params(&t).unwrap();
        assert_eq!(p.limit, 5);
        assert!(!p.include_debug);
        assert!(!p.group_by_country);

        let t = Ticket {
            ticket: Bytes::from_static(
                br#"geocode_batch:{"limit":10,"include_debug":true,"group_by_country":true}"#,
            ),
        };
        let p = parse_ticket_params(&t).unwrap();
        assert_eq!(p.limit, 10);
        assert!(p.include_debug);
        assert!(p.group_by_country);

        let t = Ticket {
            ticket: Bytes::from_static(b"unknown"),
        };
        assert!(parse_ticket_params(&t).is_err());
    }

    #[test]
    fn output_chunk_handles_empty_and_full_rows() {
        let schema = Arc::new(geocode_batch_output_schema());
        let rows = vec![
            OutputRow {
                idx: 0,
                top: None,
                confidence: "empty",
                country: Some(CountryId::BE),
                reason_codes: Vec::new(),
            },
            OutputRow {
                idx: 1,
                top: Some(GeocodedResult {
                    lat: 50.85,
                    lon: 4.35,
                    street: "Rue de la Loi".to_string(),
                    housenumber: "16".to_string(),
                    postcode: "1000".to_string(),
                    locality: "Bruxelles".to_string(),
                    score: 0.95,
                    country: Some("BE"),
                    reason_codes: vec![std::borrow::Cow::Borrowed("STREET_EXACT")],
                }),
                confidence: "accept",
                country: Some(CountryId::BE),
                reason_codes: vec!["STREET_EXACT".to_string()],
            },
        ];
        let batch = encode_output_chunk(schema, &rows).unwrap();
        assert_eq!(batch.num_rows(), 2);

        let decoded = decode_output_batch(&batch).unwrap();
        assert_eq!(decoded.len(), 2);
        assert_eq!(decoded[0].query_idx, 0);
        assert_eq!(decoded[0].lat, None);
        assert_eq!(decoded[0].confidence, "empty");
        assert_eq!(decoded[1].query_idx, 1);
        assert_eq!(decoded[1].lat, Some(50.85));
        assert_eq!(decoded[1].lon, Some(4.35));
        assert_eq!(decoded[1].street.as_deref(), Some("Rue de la Loi"));
        assert_eq!(decoded[1].confidence, "accept");
        assert_eq!(decoded[1].reason_codes, vec!["STREET_EXACT".to_string()]);
    }
}
