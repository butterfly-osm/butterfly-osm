//! Arrow Flight — ONE client per URI, blocking wrappers over a private tokio
//! runtime. `do_get` returns the decoded batches AND the trailing
//! app_metadata (the #533 completeness trailer travels in a data-less
//! message that `read_all()`-style consumers drop); `do_exchange` sends a
//! table under a command descriptor and drains the reply the same way.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use arrow::array::{
    Array, ArrayRef, BinaryArray, Float32Array, Float64Array, Int32Array, Int64Array, RecordBatch,
    StringArray, UInt16Array, UInt32Array, UInt64Array,
};
use arrow::datatypes::{DataType, Field, Schema};
use arrow_flight::decode::{DecodedPayload, FlightDataDecoder};
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::error::FlightError;
use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::{FlightData, FlightDescriptor, Ticket};
use futures_util::StreamExt;
use serde_json::Value;
use tonic::transport::Channel;

use super::http::{GResult, GateErr};

pub struct Flights {
    rt: tokio::runtime::Runtime,
    clients: Mutex<HashMap<String, Channel>>,
}

/// Decoded chunks of one stream: the batches with rows, and the last
/// app_metadata seen (parsed as JSON when it is JSON).
pub struct Decoded {
    pub batches: Vec<RecordBatch>,
    pub rows: usize,
    pub meta: Option<Value>,
}

impl Decoded {
    pub fn num_rows(&self) -> usize {
        self.rows
    }
}

fn flight_err(e: FlightError) -> GateErr {
    match e {
        FlightError::Tonic(status) => GateErr::Other(format!(
            "Flight returned {} error, with message: {}",
            pyarrow_code_words(status.code()),
            status.message()
        )),
        other => GateErr::Other(format!("Flight: {other}")),
    }
}

/// `pyarrow.flight` spells a gRPC status code in words in its error text
/// ("Flight returned invalid argument error, with message: …"); the gate's
/// detail strings quote that text, so the Rust gate spells it the same way.
fn pyarrow_code_words(code: tonic::Code) -> &'static str {
    use tonic::Code::*;
    match code {
        InvalidArgument => "invalid argument",
        NotFound => "not found",
        FailedPrecondition => "failed precondition",
        Unavailable => "unavailable",
        Unimplemented => "unimplemented",
        Internal => "internal",
        ResourceExhausted => "resource exhausted",
        DeadlineExceeded => "deadline exceeded",
        Cancelled => "cancelled",
        Unauthenticated => "unauthenticated",
        PermissionDenied => "permission denied",
        AlreadyExists => "already exists",
        Aborted => "aborted",
        OutOfRange => "out of range",
        DataLoss => "data loss",
        Unknown => "unknown",
        Ok => "ok",
    }
}

impl Flights {
    pub fn new() -> Self {
        let rt = tokio::runtime::Builder::new_multi_thread()
            .worker_threads(2)
            .enable_all()
            .build()
            .expect("tokio runtime for Flight");
        Flights {
            rt,
            clients: Mutex::new(HashMap::new()),
        }
    }

    fn channel(&self, uri: &str) -> GResult<Channel> {
        if let Some(c) = self.clients.lock().unwrap().get(uri) {
            return Ok(c.clone());
        }
        // tonic wants http://; the gate's convention is grpc://host:port.
        let http_uri = uri
            .replacen("grpc+tcp://", "http://", 1)
            .replacen("grpc://", "http://", 1);
        let ch = self.rt.block_on(async {
            Channel::from_shared(http_uri.clone())
                .map_err(|e| GateErr::Other(format!("bad Flight URI {http_uri}: {e}")))?
                .connect()
                .await
                .map_err(|e| GateErr::Other(format!("Flight connect {http_uri}: {e}")))
        })?;
        self.clients
            .lock()
            .unwrap()
            .insert(uri.to_string(), ch.clone());
        Ok(ch)
    }

    /// `do_get(f"{action}:{mode}:{json}")`.
    pub fn do_get(&self, uri: &str, action: &str, mode: &str, params: &Value) -> GResult<Decoded> {
        let ch = self.channel(uri)?;
        let ticket = format!("{action}:{mode}:{}", serde_json::to_string(params)?);
        self.rt.block_on(async move {
            let mut client = FlightServiceClient::new(ch).max_decoding_message_size(usize::MAX);
            let resp = client
                .do_get(Ticket::new(ticket.into_bytes()))
                .await
                .map_err(|s| flight_err(FlightError::Tonic(Box::new(s))))?;
            drain(resp.into_inner()).await
        })
    }

    /// `do_exchange(FlightDescriptor.for_command(cmd))` with one table.
    pub fn do_exchange(&self, uri: &str, cmd: &[u8], table: RecordBatch) -> GResult<Decoded> {
        let ch = self.channel(uri)?;
        let cmd = cmd.to_vec();
        self.rt.block_on(async move {
            let mut client = FlightServiceClient::new(ch).max_decoding_message_size(usize::MAX);
            let schema = table.schema();
            let input = FlightDataEncoderBuilder::new()
                .with_schema(schema)
                .with_flight_descriptor(Some(FlightDescriptor::new_cmd(cmd)))
                .build(futures_util::stream::iter(vec![Ok(table)]))
                .map(|r| r.expect("encoding an in-memory batch cannot fail"));
            let resp = client
                .do_exchange(input)
                .await
                .map_err(|s| flight_err(FlightError::Tonic(Box::new(s))))?;
            drain(resp.into_inner()).await
        })
    }
}

impl Default for Flights {
    fn default() -> Self {
        Self::new()
    }
}

/// Drain a raw FlightData stream the way `pyarrow.flight` does: a message
/// with app_metadata and NO IPC header is the #533 completeness trailer
/// (data=None on the Python side); it is read for its metadata and kept out
/// of the IPC decoder, which would otherwise reject the empty header.
async fn drain(mut stream: tonic::Streaming<FlightData>) -> GResult<Decoded> {
    let mut out = Decoded {
        batches: Vec::new(),
        rows: 0,
        meta: None,
    };
    let mut with_body: Vec<Result<FlightData, FlightError>> = Vec::new();
    while let Some(item) = stream.next().await {
        let fd = item.map_err(|s| flight_err(FlightError::Tonic(Box::new(s))))?;
        if !fd.app_metadata.is_empty()
            && let Ok(v) = serde_json::from_slice::<Value>(&fd.app_metadata)
        {
            out.meta = Some(v);
        }
        if !fd.data_header.is_empty() {
            with_body.push(Ok(fd));
        }
    }
    let mut dec = FlightDataDecoder::new(futures_util::stream::iter(with_body));
    while let Some(item) = dec.next().await {
        let d = item.map_err(flight_err)?;
        if let DecodedPayload::RecordBatch(b) = d.payload {
            out.rows += b.num_rows();
            out.batches.push(b);
        }
    }
    Ok(out)
}

// ---------------------------------------------------------------------------
// Column access, `as_py()`-style: None for a null, integers widened, floats
// widened, bytes borrowed.
// ---------------------------------------------------------------------------
pub fn col<'a>(b: &'a RecordBatch, name: &str) -> Option<&'a ArrayRef> {
    b.column_by_name(name)
}

pub fn has_col(b: &RecordBatch, name: &str) -> bool {
    b.schema().column_with_name(name).is_some()
}

pub fn col_u64(b: &RecordBatch, name: &str, i: usize) -> Option<u64> {
    let a = col(b, name)?;
    if a.is_null(i) {
        return None;
    }
    let any = a.as_any();
    if let Some(x) = any.downcast_ref::<UInt32Array>() {
        return Some(x.value(i) as u64);
    }
    if let Some(x) = any.downcast_ref::<UInt64Array>() {
        return Some(x.value(i));
    }
    if let Some(x) = any.downcast_ref::<UInt16Array>() {
        return Some(x.value(i) as u64);
    }
    if let Some(x) = any.downcast_ref::<Int64Array>() {
        return u64::try_from(x.value(i)).ok();
    }
    if let Some(x) = any.downcast_ref::<Int32Array>() {
        return u64::try_from(x.value(i)).ok();
    }
    None
}

pub fn col_f64(b: &RecordBatch, name: &str, i: usize) -> Option<f64> {
    let a = col(b, name)?;
    if a.is_null(i) {
        return None;
    }
    let any = a.as_any();
    if let Some(x) = any.downcast_ref::<Float32Array>() {
        return Some(x.value(i) as f64);
    }
    if let Some(x) = any.downcast_ref::<Float64Array>() {
        return Some(x.value(i));
    }
    col_u64(b, name, i).map(|v| v as f64)
}

pub fn col_bytes<'a>(b: &'a RecordBatch, name: &str, i: usize) -> Option<&'a [u8]> {
    let a = col(b, name)?;
    if a.is_null(i) {
        return None;
    }
    a.as_any().downcast_ref::<BinaryArray>().map(|x| x.value(i))
}

pub fn col_str<'a>(b: &'a RecordBatch, name: &str, i: usize) -> Option<&'a str> {
    let a = col(b, name)?;
    if a.is_null(i) {
        return None;
    }
    a.as_any().downcast_ref::<StringArray>().map(|x| x.value(i))
}

/// Iterate every row of every batch as (batch, row index).
pub fn rows(d: &Decoded) -> impl Iterator<Item = (&RecordBatch, usize)> {
    d.batches
        .iter()
        .flat_map(|b| (0..b.num_rows()).map(move |i| (b, i)))
}

/// Column names of the stream (from the first batch).
pub fn column_names(d: &Decoded) -> Vec<String> {
    d.batches
        .first()
        .map(|b| {
            b.schema()
                .fields()
                .iter()
                .map(|f| f.name().clone())
                .collect()
        })
        .unwrap_or_default()
}

/// A Float64 table from named columns (`pa.table({...})`).
pub fn f64_table(cols: &[(&str, Vec<f64>)], strings: &[(&str, Vec<String>)]) -> RecordBatch {
    let mut fields = Vec::new();
    let mut arrays: Vec<ArrayRef> = Vec::new();
    for (name, v) in strings {
        fields.push(Field::new(*name, DataType::Utf8, false));
        arrays.push(Arc::new(StringArray::from(v.clone())));
    }
    for (name, v) in cols {
        fields.push(Field::new(*name, DataType::Float64, false));
        arrays.push(Arc::new(Float64Array::from(v.clone())));
    }
    RecordBatch::try_new(Arc::new(Schema::new(fields)), arrays).expect("table")
}
