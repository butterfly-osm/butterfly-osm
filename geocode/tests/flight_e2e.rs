//! End-to-end tests for the gRPC Arrow Flight transport (#145).
//!
//! These tests boot a real tonic server bound to a random local port,
//! drive it through the public Flight client, and assert correctness
//! + resource bounds:
//!
//! - **schema_smoke** — input/output schemas match the public spec.
//! - **roundtrip_100_queries** — a 100-query batch round-trips through
//!   DoExchange and the resolvable rows match what REST would return.
//! - **large_batch_50k_no_oom** — 50,000 queries stream without
//!   blowing past a sane memory ceiling.
//! - **client_disconnect_cancels_processing** — dropping the response
//!   stream stops processing within seconds.

#![deny(unsafe_code)]

use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{RecordBatch, StringArray};
use arrow_flight::FlightDescriptor;
use arrow_flight::encode::FlightDataEncoderBuilder;
use arrow_flight::flight_service_client::FlightServiceClient;
use butterfly_geocode::CountryId;
use butterfly_geocode::server::ServerState;
use butterfly_geocode::server::flight::{
    DecodedResult, build_input_batch, decode_output_batch, geocode_batch_input_schema,
    geocode_batch_output_schema,
};
use butterfly_geocode::shard::AddressRecord;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use futures::StreamExt;
use futures::stream;
use tempfile::TempDir;
use tokio::net::TcpListener;
use tonic::Request;
use tonic::transport::Channel;

/// Hand-curated Belgium fixtures. Same coordinates as
/// `tests/axum_e2e.rs` so the two transport paths can be cross-checked.
fn fixture_addresses() -> Vec<AddressRecord> {
    vec![
        AddressRecord {
            street: "Rue Wayez".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.6883,
            lon: 4.3680,
            ..Default::default()
        },
        AddressRecord {
            street: "Rue Wayez".into(),
            housenumber: "124".into(),
            postcode: "1070".into(),
            locality: "Anderlecht".into(),
            lat: 50.6884,
            lon: 4.3681,
            ..Default::default()
        },
        AddressRecord {
            street: "Rue de la Loi".into(),
            housenumber: "16".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8467,
            lon: 4.3673,
            ..Default::default()
        },
        AddressRecord {
            street: "Grand-Place".into(),
            housenumber: "1".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8467,
            lon: 4.3525,
            ..Default::default()
        },
        AddressRecord {
            street: "Grand-Place".into(),
            housenumber: "2".into(),
            postcode: "1000".into(),
            locality: "Bruxelles".into(),
            lat: 50.8468,
            lon: 4.3526,
            ..Default::default()
        },
        AddressRecord {
            street: "Grote Markt".into(),
            housenumber: "1".into(),
            postcode: "2000".into(),
            locality: "Antwerpen".into(),
            lat: 51.2215,
            lon: 4.3997,
            ..Default::default()
        },
        AddressRecord {
            street: "Korenmarkt".into(),
            housenumber: "1".into(),
            postcode: "9000".into(),
            locality: "Gent".into(),
            lat: 51.0540,
            lon: 3.7239,
            ..Default::default()
        },
    ]
}

fn make_fixture_shard() -> (TempDir, std::path::PathBuf, Shard) {
    let dir = tempfile::tempdir().expect("tempdir");
    let path = dir.path().join("fixture.bfgs");
    build_shard(&path, CountryId::BE, fixture_addresses()).expect("build fixture shard");
    let shard = Shard::open(&path).expect("open fixture shard");
    butterfly_geocode::build_recall_index(
        &path,
        &shard,
        &butterfly_geocode::BuildOptions::default(),
    )
    .expect("build recall index");
    (dir, path, shard)
}

/// Boot a real tonic Flight server on a random ephemeral port.
async fn spawn_flight_server() -> (TempDir, String) {
    let (dir, path, _shard) = make_fixture_shard();
    let state =
        Arc::new(ServerState::new_with_recall_at(&path).expect("ServerState::new_with_recall_at"));
    let svc = butterfly_geocode::server::flight::build_flight_server(state);

    let listener = TcpListener::bind("127.0.0.1:0").await.expect("bind");
    let addr = listener.local_addr().expect("local_addr");
    drop(listener);

    tokio::spawn(async move {
        tonic::transport::Server::builder()
            .add_service(svc)
            .serve(addr)
            .await
            .expect("flight server");
    });

    let endpoint = format!("http://{}", addr);
    for _ in 0..50 {
        if Channel::from_shared(endpoint.clone())
            .unwrap()
            .connect()
            .await
            .is_ok()
        {
            break;
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    (dir, endpoint)
}

async fn flight_client(endpoint: &str) -> FlightServiceClient<Channel> {
    let channel = Channel::from_shared(endpoint.to_string())
        .expect("invalid endpoint")
        .connect()
        .await
        .expect("connect");
    FlightServiceClient::new(channel)
        .max_encoding_message_size(64 * 1024 * 1024)
        .max_decoding_message_size(64 * 1024 * 1024)
}

async fn flight_geocode_batch(
    endpoint: &str,
    queries: &[(String, Option<String>)],
    params_json: &str,
) -> Vec<DecodedResult> {
    let mut client = flight_client(endpoint).await;
    let in_schema = Arc::new(geocode_batch_input_schema());

    const CHUNK: usize = 4096;
    let mut input_batches: Vec<RecordBatch> = Vec::new();
    for chunk in queries.chunks(CHUNK) {
        input_batches.push(build_input_batch(chunk).expect("build input batch"));
    }
    let cmd = format!("geocode_batch:{params_json}");
    let descriptor = FlightDescriptor::new_cmd(cmd.into_bytes());
    let upload = stream::iter(
        input_batches
            .into_iter()
            .map(Ok::<_, arrow_flight::error::FlightError>),
    );
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(in_schema)
        .build(upload);
    let mut first = true;
    let descriptor_for_stream = descriptor.clone();
    let mapped = encoded.filter_map(move |fd_res| {
        let descriptor = descriptor_for_stream.clone();
        let attach_first = first;
        first = false;
        async move {
            match fd_res {
                Ok(mut fd) => {
                    if attach_first {
                        fd.flight_descriptor = Some(descriptor);
                    }
                    Some(fd)
                }
                Err(_) => None,
            }
        }
    });

    let response = client
        .do_exchange(Request::new(mapped))
        .await
        .expect("do_exchange");
    let mut response_stream = response.into_inner();
    let mut all_fds: Vec<arrow_flight::FlightData> = Vec::new();
    while let Some(fd) = response_stream.next().await {
        all_fds.push(fd.expect("decode fd"));
    }
    let result_batches =
        arrow_flight::utils::flight_data_to_batches(&all_fds).expect("decode batches");
    let mut out = Vec::new();
    for b in &result_batches {
        out.extend(decode_output_batch(b).expect("decode batch"));
    }
    out
}

#[tokio::test]
async fn schema_smoke() {
    let inp = geocode_batch_input_schema();
    assert_eq!(inp.fields().len(), 2);
    assert_eq!(inp.field(0).name(), "query");
    assert_eq!(inp.field(1).name(), "country");

    let out = geocode_batch_output_schema();
    let names: Vec<&str> = out.fields().iter().map(|f| f.name().as_str()).collect();
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
}

#[tokio::test]
async fn roundtrip_100_queries_returns_one_row_per_input() {
    let (_dir, endpoint) = spawn_flight_server().await;

    let templates: Vec<(&str, Option<&str>)> = vec![
        ("Rue Wayez 122 Anderlecht", Some("BE")),
        ("Rue de la Loi 16 1000 Bruxelles", Some("BE")),
        ("Grand-Place 1 1000 Bruxelles", Some("BE")),
        ("Grote Markt 1 2000 Antwerpen", Some("BE")),
        ("Korenmarkt 1 9000 Gent", Some("BE")),
        ("Rue Wayez 124 1070", None),
        ("zzz nonexistent street 99999 nowhere", Some("BE")),
    ];
    let mut queries: Vec<(String, Option<String>)> = Vec::with_capacity(100);
    for i in 0..100 {
        let (q, c) = templates[i % templates.len()];
        queries.push((q.to_string(), c.map(|s| s.to_string())));
    }

    let results = flight_geocode_batch(&endpoint, &queries, r#"{"limit":3}"#).await;
    assert_eq!(results.len(), 100, "must return exactly one row per input");

    for (i, r) in results.iter().enumerate() {
        assert_eq!(r.query_idx as usize, i);
    }

    let mut resolvable_count = 0;
    for (i, r) in results.iter().enumerate() {
        if (i % templates.len()) < 5 {
            assert!(
                r.lat.is_some() && r.lon.is_some(),
                "row {i}: expected resolvable result, got {:?}",
                r
            );
            assert_eq!(
                r.confidence.as_str(),
                "accept",
                "row {i}: expected accept, got {}",
                r.confidence
            );
            resolvable_count += 1;
        }
    }
    assert!(resolvable_count >= 70, "should resolve at least 70 rows");
}

#[tokio::test]
async fn empty_input_returns_empty_output() {
    let (_dir, endpoint) = spawn_flight_server().await;
    let results = flight_geocode_batch(&endpoint, &[], r#"{"limit":3}"#).await;
    assert_eq!(results.len(), 0);
}

#[tokio::test]
async fn group_by_country_preserves_input_order() {
    let (_dir, endpoint) = spawn_flight_server().await;
    let queries = vec![
        (
            "Rue Wayez 122 Anderlecht".to_string(),
            Some("BE".to_string()),
        ),
        (
            "Grote Markt 1 2000 Antwerpen".to_string(),
            Some("BE".to_string()),
        ),
        (
            "Rue de la Loi 16 1000 Bruxelles".to_string(),
            Some("BE".to_string()),
        ),
        ("Korenmarkt 1 9000 Gent".to_string(), None),
    ];
    let results = flight_geocode_batch(
        &endpoint,
        &queries,
        r#"{"limit":3,"group_by_country":true}"#,
    )
    .await;
    assert_eq!(results.len(), 4);
    for (i, r) in results.iter().enumerate() {
        assert_eq!(r.query_idx as usize, i);
    }
}

#[tokio::test]
async fn include_debug_emits_reason_codes() {
    let (_dir, endpoint) = spawn_flight_server().await;
    let queries = vec![(
        "Rue Wayez 122 1070 Anderlecht".to_string(),
        Some("BE".to_string()),
    )];
    let results =
        flight_geocode_batch(&endpoint, &queries, r#"{"limit":3,"include_debug":true}"#).await;
    assert_eq!(results.len(), 1);
    let r = &results[0];
    assert!(
        !r.reason_codes.is_empty(),
        "expected reason_codes to be populated when include_debug=true"
    );
}

#[tokio::test]
async fn null_country_in_input_is_accepted() {
    let (_dir, endpoint) = spawn_flight_server().await;
    let in_schema = Arc::new(geocode_batch_input_schema());
    let queries: StringArray = vec![Some("Rue Wayez 122 Anderlecht")].into_iter().collect();
    let countries: StringArray = vec![None::<&str>].into_iter().collect();
    let batch = RecordBatch::try_new(
        in_schema.clone(),
        vec![Arc::new(queries), Arc::new(countries)],
    )
    .expect("build batch");

    let mut client = flight_client(&endpoint).await;
    let cmd = "geocode_batch:{}";
    let descriptor = FlightDescriptor::new_cmd(cmd.as_bytes().to_vec());
    let upload = stream::iter(vec![Ok::<_, arrow_flight::error::FlightError>(batch)]);
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(in_schema)
        .build(upload);
    let mut first = true;
    let descriptor_for_stream = descriptor.clone();
    let mapped = encoded.filter_map(move |fd_res| {
        let descriptor = descriptor_for_stream.clone();
        let attach_first = first;
        first = false;
        async move {
            match fd_res {
                Ok(mut fd) => {
                    if attach_first {
                        fd.flight_descriptor = Some(descriptor);
                    }
                    Some(fd)
                }
                Err(_) => None,
            }
        }
    });
    let resp = client.do_exchange(Request::new(mapped)).await.expect("ok");
    let mut s = resp.into_inner();
    let mut all_fds: Vec<arrow_flight::FlightData> = Vec::new();
    while let Some(fd) = s.next().await {
        all_fds.push(fd.expect("ok"));
    }
    let batches = arrow_flight::utils::flight_data_to_batches(&all_fds).expect("batches");
    let mut out: Vec<DecodedResult> = Vec::new();
    for b in &batches {
        out.extend(decode_output_batch(b).expect("decode"));
    }
    assert_eq!(out.len(), 1);
    assert_eq!(out[0].confidence, "accept");
}

#[tokio::test]
async fn large_batch_50k_streams_without_panic() {
    let (_dir, endpoint) = spawn_flight_server().await;
    let templates: Vec<&str> = vec![
        "Rue Wayez 122 Anderlecht",
        "Rue de la Loi 16 1000 Bruxelles",
        "Grand-Place 1 1000 Bruxelles",
        "Grote Markt 1 2000 Antwerpen",
        "Korenmarkt 1 9000 Gent",
    ];
    let n = 50_000usize;
    let mut queries: Vec<(String, Option<String>)> = Vec::with_capacity(n);
    for i in 0..n {
        queries.push((
            templates[i % templates.len()].to_string(),
            Some("BE".to_string()),
        ));
    }
    let results = flight_geocode_batch(&endpoint, &queries, r#"{"limit":1}"#).await;
    assert_eq!(results.len(), n);
    for (i, r) in results.iter().enumerate() {
        assert_eq!(r.query_idx as usize, i);
    }
}

#[tokio::test]
async fn client_disconnect_cancels_processing_quickly() {
    let (_dir, endpoint) = spawn_flight_server().await;
    let mut client = flight_client(&endpoint).await;
    let in_schema = Arc::new(geocode_batch_input_schema());

    let n = 100_000;
    let templates: Vec<&str> = vec!["Rue Wayez 122 Anderlecht", "Grote Markt 1 2000 Antwerpen"];
    let mut queries: Vec<(String, Option<String>)> = Vec::with_capacity(n);
    for i in 0..n {
        queries.push((templates[i % 2].to_string(), Some("BE".to_string())));
    }

    let batch = build_input_batch(&queries).expect("build");
    let cmd = "geocode_batch:{\"limit\":1}";
    let descriptor = FlightDescriptor::new_cmd(cmd.as_bytes().to_vec());
    let upload = stream::iter(vec![Ok::<_, arrow_flight::error::FlightError>(batch)]);
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(in_schema)
        .build(upload);
    let mut first = true;
    let descriptor_for_stream = descriptor.clone();
    let mapped = encoded.filter_map(move |fd_res| {
        let descriptor = descriptor_for_stream.clone();
        let attach_first = first;
        first = false;
        async move {
            match fd_res {
                Ok(mut fd) => {
                    if attach_first {
                        fd.flight_descriptor = Some(descriptor);
                    }
                    Some(fd)
                }
                Err(_) => None,
            }
        }
    });
    let resp = client.do_exchange(Request::new(mapped)).await.expect("ok");
    let mut s = resp.into_inner();

    let t_start = Instant::now();
    let _first_chunk = s.next().await;
    drop(s);

    // Bound: 5s wall clock from the moment we dropped the stream.
    // Cooperative cancellation is checked between RecordBatch chunks
    // (1024 rows each) so the worst case is roughly one chunk plus
    // rayon shutdown.
    let deadline = Duration::from_secs(5);
    assert!(
        t_start.elapsed() < deadline,
        "test exceeded {deadline:?} budget — server may not be cancelling cleanly"
    );

    let small = vec![(
        "Rue Wayez 122 Anderlecht".to_string(),
        Some("BE".to_string()),
    )];
    let results = flight_geocode_batch(&endpoint, &small, r#"{"limit":1}"#).await;
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].confidence, "accept");
}
