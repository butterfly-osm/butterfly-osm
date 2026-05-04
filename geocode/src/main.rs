//! butterfly-geocode CLI: build shards, train models, and serve the API.

#![deny(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use tracing::{Level, info, warn};
use tracing_subscriber::EnvFilter;

use butterfly_geocode::CountryId;
use butterfly_geocode::confidence::{
    GbdtModel, TrainConfig, build_training_groups, build_training_rows, dump_training_rows,
    evaluate, load_corpus, synthesise_corpus_from_shard, train_pointwise,
};
use butterfly_geocode::osm_extract::{ExtractProgress, extract_addresses};
use butterfly_geocode::server::{
    DEFAULT_GRPC_PORT, DEFAULT_REST_PORT, ServerConfig, ServerState, Transport,
    build_router_with_config, start_grpc_server,
};
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use butterfly_geocode::shard::{AddressRecord, SourceTag};
use butterfly_geocode::sources::{SourceProgress, bosa::BosaCsvSource, collect_all, merge_records};
use butterfly_geocode::{HeuristicBackend, NeuralBackend, NeuralParser, ParserBackend};

#[derive(Parser, Debug)]
#[command(
    name = "butterfly-geocode",
    about = "Geocoder for the butterfly-osm toolkit"
)]
struct Cli {
    /// Logging format: `text` (default) or `json`.
    #[arg(long, default_value = "text", global = true)]
    log_format: String,

    #[command(subcommand)]
    cmd: Command,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, ValueEnum)]
enum ParserKind {
    /// Deterministic regex-driven parser (Phase 0 baseline).
    #[default]
    Heuristic,
    /// Byte-level transformer parser (#96 §Tagger + #98 Phase 1).
    /// Requires `--model` to point at a safetensors file produced by
    /// the `train` subcommand. If the model fails to load, the server
    /// falls back to the heuristic parser with a warning.
    Neural,
}

#[derive(Subcommand, Debug)]
enum Command {
    /// Build a single-country shard from one or more authoritative
    /// sources (OSM PBF tags, BOSA BeSt CSV, ...).
    ///
    /// Three usage modes:
    ///
    /// 1. Single OSM PBF:    `--pbf <PATH> [--source osm]`
    /// 2. Single BOSA CSV:   `--csv <PATH> --source bosa`
    /// 3. Merge two shards:  `--merge a.bfgs --merge b.bfgs`
    ///
    /// Modes 1+2 are mutually exclusive. Mode 3 reads existing BFGS
    /// shards (built via mode 1 or 2) and merges them, deduping
    /// records by spatial proximity + housenumber match. The
    /// authoritative source wins on conflict (#96 §"Data Sources":
    /// "country packs choose channel weighting and policy").
    BuildShard {
        /// Source PBF (any Geofabrik regional/country extract).
        /// Mutually exclusive with `--csv` / `--merge`.
        #[arg(long, conflicts_with_all = ["csv", "merge"])]
        pbf: Option<PathBuf>,
        /// Source CSV (BOSA BeSt openaddress-be{vlg,wal,bru}.zip
        /// or unzipped CSV). Mutually exclusive with `--pbf` /
        /// `--merge`.
        #[arg(long, conflicts_with_all = ["pbf", "merge"])]
        csv: Option<PathBuf>,
        /// Merge multiple existing shards into one (deduped). Repeat
        /// the flag for each input shard. Mutually exclusive with
        /// `--pbf` / `--csv`.
        #[arg(long, conflicts_with_all = ["pbf", "csv"])]
        merge: Vec<PathBuf>,
        /// Output BFGS v4 shard file.
        #[arg(long)]
        out: PathBuf,
        /// ISO 3166-1 alpha-2 country code for this shard. Stored
        /// in the BFGS v4 header and verified at server load.
        #[arg(long)]
        country: String,
        /// Authoritative-source tag for the records in this shard
        /// (`osm`, `bosa`, `ban`, `bag`, `gnaf`, `bev`, `swisstopo`).
        /// Required for `--csv`; optional for `--pbf` (defaults to
        /// `osm`); ignored for `--merge` (each input shard already
        /// carries its own per-record tag).
        #[arg(long)]
        source: Option<String>,
    },
    /// Build every country shard the server can deploy in one pass.
    /// Looks for `<country>.pbf` (or `<iso2>.pbf`) inside `--pbf-dir`
    /// for each ISO2 in the supported list, and emits
    /// `<out_dir>/<iso2>.bfgs`. Missing PBFs are skipped with a
    /// warning — operators can deploy the subset they have data for.
    BuildShardsAll {
        /// Directory containing per-country PBFs.
        #[arg(long)]
        pbf_dir: PathBuf,
        /// Output directory for BFGS shards.
        #[arg(long)]
        out_dir: PathBuf,
        /// Limit to a comma-separated subset (e.g. `BE,FR,NL`). If
        /// unset, every supported country is attempted.
        #[arg(long)]
        only: Option<String>,
    },
    /// Train a byte-level transformer tagger (#96 §Tagger). When
    /// `--corpus` is omitted, an inline synthetic Belgium corpus is
    /// generated — useful as a smoke test for the training loop and
    /// for shipping the proof-of-life model.
    Train {
        /// Output safetensors path. A sidecar `.config.json` with the
        /// model architecture is written next to it.
        #[arg(long)]
        out: PathBuf,
        /// Path to a JSONL corpus. Each line: `{"text", "country", "spans": [{"field","start","end"}, ...]}`.
        #[arg(long)]
        corpus: Option<PathBuf>,
        /// Number of synthetic examples to generate when no corpus is
        /// provided. Default 4096.
        #[arg(long, default_value_t = 4096)]
        synthetic: usize,
        /// Number of training epochs.
        #[arg(long, default_value_t = 8)]
        epochs: usize,
        /// Mini-batch size.
        #[arg(long, default_value_t = 16)]
        batch_size: usize,
        /// Learning rate for AdamW.
        #[arg(long, default_value_t = 2e-3)]
        learning_rate: f64,
        /// Random seed.
        #[arg(long, default_value_t = 0xB17EBAD0)]
        seed: u64,
    },
    /// Run the geocode server (REST and/or gRPC Arrow Flight).
    ///
    /// Per #145 (transport policy) the geocoder ships both transports.
    /// Use `--transport=both` (default) for production, or pick one
    /// for testing. The legacy `--port` flag still works as the REST
    /// port; new deployments should set `--rest-port` and
    /// `--grpc-port` explicitly.
    Serve {
        /// Single-shard mode: load this shard. Mutually exclusive
        /// with `--shard-dir`.
        #[arg(long, conflicts_with = "shard_dir")]
        shard: Option<PathBuf>,
        /// Multi-shard mode: load every `*.bfgs` in this directory.
        /// Each shard is keyed by its on-disk country code (BFGS v3
        /// header).
        #[arg(long, conflicts_with = "shard")]
        shard_dir: Option<PathBuf>,
        /// Legacy alias for `--rest-port`. Kept so existing run scripts
        /// keep working when transport defaults to `both`.
        #[arg(long, default_value_t = 3003)]
        port: u16,
        /// REST port. Overrides `--port` when set. Used when transport
        /// is `rest` or `both`.
        #[arg(long)]
        rest_port: Option<u16>,
        /// gRPC Arrow Flight port (default 3004). Used when transport
        /// is `grpc` or `both`.
        #[arg(long)]
        grpc_port: Option<u16>,
        /// Transport selection: `rest`, `grpc`, or `both` (default).
        #[arg(long, default_value = "both")]
        transport: String,
        #[arg(long, default_value = "0.0.0.0")]
        host: String,
        /// Optional path to a trained GBDT confidence reranker
        /// (`butterfly-geocode train-rerank` output). When omitted,
        /// the server returns raw executor scores untouched
        /// (no-model fallback path).
        #[arg(long)]
        rerank_model: Option<PathBuf>,
        /// Parser backend.
        #[arg(long, value_enum, default_value_t = ParserKind::Heuristic)]
        parser: ParserKind,
        /// Path to the safetensors model. Required when `--parser=neural`.
        #[arg(long)]
        model: Option<PathBuf>,
        /// Per-IP HTTP rate limit (requests per second steady state).
        #[arg(long, default_value_t = 100)]
        rate_limit_per_sec: u32,
        /// Per-IP HTTP rate-limit burst size.
        #[arg(long, default_value_t = 200)]
        rate_limit_burst: u32,
        /// Per-request server-side timeout (seconds).
        #[arg(long, default_value_t = 30)]
        request_timeout_secs: u64,
        /// Maximum number of seconds to wait for in-flight requests
        /// to complete after SIGTERM/SIGINT. Beyond this, the process
        /// exits even if requests are still running.
        #[arg(long, default_value_t = 30)]
        shutdown_timeout_secs: u64,
        /// Maximum POST/PUT body size in bytes (4 KB default —
        /// future Flight endpoints will tighten this).
        #[arg(long, default_value_t = 4096)]
        max_body_bytes: usize,
    },
    /// Run a batch of queries against a remote geocoder via gRPC
    /// Arrow Flight (#145). Reads JSONL queries (one per line) from
    /// `--queries`, posts them as a single `geocode_batch` DoExchange
    /// call, and writes the streamed Arrow output to `--output`.
    ///
    /// Each input line is either `{"query": "..."}` or
    /// `{"query": "...", "country": "BE"}`.
    FlightBatch {
        /// Flight endpoint (e.g. `http://localhost:3004`).
        #[arg(long)]
        endpoint: String,
        /// JSONL file with one query per line.
        #[arg(long)]
        queries: PathBuf,
        /// Output file (Arrow IPC stream format).
        #[arg(long)]
        output: PathBuf,
        /// Top-k limit per query (server-side).
        #[arg(long, default_value_t = 5)]
        limit: u32,
        /// Include reason codes in the output.
        #[arg(long)]
        include_debug: bool,
        /// Group by country before dispatching to rayon (improves
        /// per-country cache locality).
        #[arg(long)]
        group_by_country: bool,
    },
    /// Train the GBDT confidence reranker (#96 §Confidence Model).
    ///
    /// Reads a JSONL labelled corpus, runs the executor against the
    /// provided shard to materialise candidates, computes features,
    /// labels each (query, candidate) pair, and trains a pointwise
    /// logistic-loss GBDT.
    ///
    /// If `--corpus` is omitted, the trainer synthesises a tiny
    /// labelled corpus by sampling records directly from the shard —
    /// the Phase-0 bootstrap when no real labelled data exists yet.
    TrainRerank {
        #[arg(long)]
        shard: PathBuf,
        #[arg(long)]
        corpus: Option<PathBuf>,
        #[arg(long)]
        out: PathBuf,
        #[arg(long, default_value_t = 100)]
        iterations: usize,
        #[arg(long, default_value_t = 6)]
        max_depth: u32,
        #[arg(long, default_value_t = 20)]
        limit_per_query: usize,
        #[arg(long, default_value_t = 5000)]
        synth_size: usize,
        #[arg(long)]
        dump_rows: Option<PathBuf>,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_logging(&cli.log_format);

    match cli.cmd {
        Command::BuildShard {
            pbf,
            csv,
            merge,
            out,
            country,
            source,
        } => build_shard_cmd(
            pbf.as_deref(),
            csv.as_deref(),
            &merge,
            &out,
            &country,
            source.as_deref(),
        ),
        Command::BuildShardsAll {
            pbf_dir,
            out_dir,
            only,
        } => build_shards_all_cmd(&pbf_dir, &out_dir, only.as_deref()),
        Command::Train {
            out,
            corpus,
            synthetic,
            epochs,
            batch_size,
            learning_rate,
            seed,
        } => train_cmd(
            out,
            corpus,
            synthetic,
            epochs,
            batch_size,
            learning_rate,
            seed,
        ),
        Command::Serve {
            shard,
            shard_dir,
            port,
            rest_port,
            grpc_port,
            transport,
            host,
            rerank_model,
            parser,
            model,
            rate_limit_per_sec,
            rate_limit_burst,
            request_timeout_secs,
            shutdown_timeout_secs,
            max_body_bytes,
        } => {
            let server_cfg = ServerConfig {
                rate_limit_per_sec,
                rate_limit_burst,
                request_timeout: std::time::Duration::from_secs(request_timeout_secs),
                max_request_body_bytes: max_body_bytes,
            };
            // Port resolution precedence:
            // 1. explicit `--rest-port` / `--grpc-port`
            // 2. legacy `--port` aliases the REST port (default 3003)
            // 3. named defaults (DEFAULT_REST_PORT / DEFAULT_GRPC_PORT)
            let rest = rest_port.unwrap_or(port);
            let grpc = grpc_port.unwrap_or(DEFAULT_GRPC_PORT);
            let _ = DEFAULT_REST_PORT; // referenced for doc/lint visibility
            let transport_enum = Transport::parse(&transport).context("parsing --transport")?;
            serve_cmd(
                shard.as_deref(),
                shard_dir.as_deref(),
                &host,
                rest,
                grpc,
                transport_enum,
                rerank_model.as_deref(),
                parser,
                model.as_deref(),
                server_cfg,
                std::time::Duration::from_secs(shutdown_timeout_secs),
            )
            .await
        }
        Command::FlightBatch {
            endpoint,
            queries,
            output,
            limit,
            include_debug,
            group_by_country,
        } => {
            flight_batch_cmd(
                &endpoint,
                &queries,
                &output,
                limit,
                include_debug,
                group_by_country,
            )
            .await
        }
        Command::TrainRerank {
            shard,
            corpus,
            out,
            iterations,
            max_depth,
            limit_per_query,
            synth_size,
            dump_rows,
        } => train_rerank_cmd(
            &shard,
            corpus.as_deref(),
            &out,
            iterations,
            max_depth,
            limit_per_query,
            synth_size,
            dump_rows.as_deref(),
        ),
    }
}

fn init_logging(format: &str) {
    let filter = EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| EnvFilter::new("info,butterfly_geocode=debug"));
    if format.eq_ignore_ascii_case("json") {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_max_level(Level::DEBUG)
            .json()
            .init();
    } else {
        tracing_subscriber::fmt()
            .with_env_filter(filter)
            .with_max_level(Level::DEBUG)
            .init();
    }
}

fn build_shard_cmd(
    pbf: Option<&std::path::Path>,
    csv: Option<&std::path::Path>,
    merge_inputs: &[PathBuf],
    out: &std::path::Path,
    country_iso2: &str,
    source: Option<&str>,
) -> Result<()> {
    let country = CountryId::from_iso2(country_iso2).ok_or_else(|| {
        anyhow!("'{country_iso2}' is not a valid ISO 3166-1 alpha-2 country code")
    })?;
    if butterfly_geocode::routing::PackRegistry::shipped()
        .ok()
        .and_then(|r| r.get(country).cloned())
        .is_none()
    {
        warn!(
            country = country.iso2(),
            "no shipped country pack for {} — building without pack-driven OSM tag overrides",
            country.iso2()
        );
    }
    info!(
        pbf = ?pbf.map(|p| p.display().to_string()),
        csv = ?csv.map(|p| p.display().to_string()),
        merge_inputs = merge_inputs.len(),
        out = %out.display(),
        country = country.iso2(),
        "building shard"
    );
    let start = std::time::Instant::now();

    if let Some(parent) = out.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)
            .with_context(|| format!("creating output dir {}", parent.display()))?;
    }

    // Branch on which input mode the user picked. clap conflicts_with
    // already enforces mutual exclusion, but we still validate that
    // exactly one mode is set so error messages are clean.
    let addresses: Vec<AddressRecord> = if !merge_inputs.is_empty() {
        info!(
            shards = merge_inputs.len(),
            country = country.iso2(),
            "merging existing shards"
        );
        merge_existing_shards(merge_inputs, country)?
    } else if let Some(csv_path) = csv {
        let tag_str = source.ok_or_else(|| {
            anyhow!("--csv requires --source <bosa|...> so the shard byte is set explicitly")
        })?;
        let tag = SourceTag::from_name(tag_str).ok_or_else(|| {
            anyhow!(
                "unknown --source '{tag_str}' (supported: osm, bosa, ban, bag, gnaf, bev, swisstopo)"
            )
        })?;
        info!(
            csv = %csv_path.display(),
            country = country.iso2(),
            source = tag.name(),
            "loading authoritative-source CSV"
        );
        load_csv_source(csv_path, country, tag)?
    } else if let Some(pbf_path) = pbf {
        // OSM PBF path. `source` defaults to `osm`.
        let tag_str = source.unwrap_or("osm");
        let tag = SourceTag::from_name(tag_str).ok_or_else(|| {
            anyhow!(
                "unknown --source '{tag_str}' (supported: osm, bosa, ban, bag, gnaf, bev, swisstopo)"
            )
        })?;
        if tag != SourceTag::Osm {
            bail!(
                "--pbf is OSM-only; pass --source osm (or omit --source). Got --source={}",
                tag.name()
            );
        }
        info!(
            pbf = %pbf_path.display(),
            out = %out.display(),
            country = country.iso2(),
            source = tag.name(),
            "extracting OSM addresses"
        );
        extract_addresses(pbf_path, |evt| match evt {
            ExtractProgress::Phase { phase } => info!("phase: {phase}"),
            ExtractProgress::NodePass {
                nodes_seen,
                addresses_emitted,
            } => info!(nodes_seen, addresses_emitted, "nodes pass complete"),
            ExtractProgress::WayPass {
                ways_seen,
                addresses_emitted,
            } => info!(ways_seen, addresses_emitted, "ways pass complete"),
        })?
    } else {
        bail!(
            "build-shard needs exactly one of --pbf, --csv, or --merge. \
             For OSM tags use --pbf <PBF>; for BOSA BeSt use --csv <ZIP|CSV> --source bosa; \
             for combining shards use --merge a.bfgs --merge b.bfgs."
        );
    };

    info!(
        count = addresses.len(),
        secs = start.elapsed().as_secs_f64(),
        "extracted addresses"
    );

    let stats = build_shard(out, country, addresses).context("writing shard")?;
    info!(
        records = stats.record_count,
        unique_postcodes = stats.unique_postcodes,
        unique_streets = stats.unique_streets,
        strings_bytes = stats.strings_bytes,
        records_bytes = stats.records_bytes,
        index_bytes = stats.index_bytes,
        country = stats.country.iso2(),
        secs = start.elapsed().as_secs_f64(),
        "shard built"
    );

    let s = Shard::open(out).context("verifying shard CRC after build")?;
    if s.country() != country {
        bail!(
            "shard country mismatch after build: header says {} but we expected {}",
            s.country().iso2(),
            country.iso2()
        );
    }
    info!("shard verified");

    Ok(())
}

/// Load a CSV authoritative source. Today only BOSA BeSt is wired
/// (`SourceTag::Bosa`). Other CSV sources land here as new arms.
fn load_csv_source(
    path: &std::path::Path,
    country: CountryId,
    tag: SourceTag,
) -> Result<Vec<AddressRecord>> {
    match tag {
        SourceTag::Bosa => {
            let loader = BosaCsvSource::new(path, country);
            collect_all(&loader, |evt| match evt {
                SourceProgress::Phase { phase } => info!("phase: {phase}"),
                SourceProgress::Records {
                    rows_seen,
                    records_emitted,
                } => info!(rows_seen, records_emitted, "BOSA progress"),
            })
            .context("BOSA CSV ingest")
        }
        other => bail!(
            "CSV ingest for source {} is not wired yet (only BOSA today). \
             Add a loader to geocode/src/sources/ and a new arm here.",
            other.name()
        ),
    }
}

/// Read existing BFGS shards, materialise their records into
/// `AddressRecord`s (preserving each record's source byte), and merge
/// via [`merge_records`].
fn merge_existing_shards(inputs: &[PathBuf], country: CountryId) -> Result<Vec<AddressRecord>> {
    let mut groups: Vec<Vec<AddressRecord>> = Vec::with_capacity(inputs.len());
    for p in inputs {
        info!(shard = %p.display(), "reading shard for merge");
        let s = Shard::open(p).with_context(|| format!("opening merge input {}", p.display()))?;
        if s.country() != country {
            bail!(
                "merge input {} has country {} but target shard is {}",
                p.display(),
                s.country().iso2(),
                country.iso2()
            );
        }
        let mut recs = Vec::with_capacity(s.record_count());
        for i in 0..s.record_count() as u32 {
            let Some(r) = s.record(i) else { continue };
            recs.push(AddressRecord {
                lat: r.lat,
                lon: r.lon,
                street: r.street.to_string(),
                locality: r.locality.to_string(),
                housenumber: r.housenumber.to_string(),
                postcode: r.postcode.to_string(),
                source: r.source,
                source_id: None,
            });
        }
        info!(records = recs.len(), shard = %p.display(), "shard records loaded");
        groups.push(recs);
    }
    let total_in: usize = groups.iter().map(|g| g.len()).sum();
    let merged = merge_records(groups);
    info!(
        in_records = total_in,
        merged_records = merged.len(),
        deduped = total_in - merged.len(),
        "merge complete"
    );
    Ok(merged)
}

fn build_shards_all_cmd(
    pbf_dir: &std::path::Path,
    out_dir: &std::path::Path,
    only: Option<&str>,
) -> Result<()> {
    if !pbf_dir.is_dir() {
        bail!(
            "pbf-dir does not exist or is not a directory: {}",
            pbf_dir.display()
        );
    }
    std::fs::create_dir_all(out_dir)
        .with_context(|| format!("creating output dir {}", out_dir.display()))?;

    let allowed: Option<Vec<CountryId>> = only.map(|s| {
        s.split(',')
            .filter_map(|t| CountryId::from_iso2(t.trim()))
            .collect()
    });

    // Iterate every shipped country pack — adding a country to the
    // build sweep is dropping a pack TOML, no Rust changes (#96 "serve
    // the world").
    let registry = butterfly_geocode::routing::PackRegistry::shipped()
        .context("loading shipped country packs")?;
    let mut built = Vec::<CountryId>::new();
    let mut skipped = Vec::<(CountryId, String)>::new();
    for c in registry.countries() {
        if let Some(ref a) = allowed
            && !a.contains(&c)
        {
            continue;
        }
        let candidates = candidate_pbf_names(c);
        let pbf = candidates
            .iter()
            .map(|n| pbf_dir.join(n))
            .find(|p| p.is_file());
        let Some(pbf) = pbf else {
            warn!(
                country = c.iso2(),
                "no PBF found in {} (looked for {:?}); skipping",
                pbf_dir.display(),
                candidates
            );
            skipped.push((c, "no PBF".to_string()));
            continue;
        };
        let out = out_dir.join(format!("{}.bfgs", c.iso2().to_ascii_lowercase()));
        match build_shard_cmd(Some(&pbf), None, &[], &out, c.iso2(), Some("osm")) {
            Ok(_) => built.push(c),
            Err(e) => {
                warn!(country = c.iso2(), error = %e, "shard build failed; continuing");
                skipped.push((c, e.to_string()));
            }
        }
    }
    info!(
        built = built.len(),
        skipped = skipped.len(),
        "build-shards-all complete"
    );
    for (c, why) in &skipped {
        info!(country = c.iso2(), reason = %why, "skipped");
    }
    Ok(())
}

fn candidate_pbf_names(c: CountryId) -> Vec<String> {
    // Friendly long names per country (matches the butterfly-dl
    // region index naming). Adding a country: append a row here,
    // it's the only ISO2 → long-name lookup the binary uses.
    let long = match &c.as_bytes() {
        b"BE" => "belgium",
        b"FR" => "france",
        b"NL" => "netherlands",
        b"LU" => "luxembourg",
        b"DE" => "germany",
        b"AT" => "austria",
        b"CH" => "switzerland",
        b"GB" => "united-kingdom",
        b"ES" => "spain",
        b"IT" => "italy",
        b"US" => "united-states",
        b"JP" => "japan",
        b"BR" => "brazil",
        b"IN" => "india",
        b"AU" => "australia",
        // Any country without a long name falls through to ISO2-only
        // filename probing.
        _ => "",
    };
    let mut v = Vec::new();
    if !long.is_empty() {
        v.push(format!("{long}.pbf"));
        v.push(format!("{long}.osm.pbf"));
        v.push(format!("{long}-latest.osm.pbf"));
    }
    v.push(format!("{}.pbf", c.iso2().to_ascii_lowercase()));
    v.push(format!("{}.osm.pbf", c.iso2().to_ascii_lowercase()));
    v
}

fn train_cmd(
    out: PathBuf,
    corpus_path: Option<PathBuf>,
    synthetic_n: usize,
    epochs: usize,
    batch_size: usize,
    learning_rate: f64,
    seed: u64,
) -> Result<()> {
    use butterfly_geocode::tagger::training::{
        TrainConfig, generate_belgium_synthetic, read_jsonl_corpus, train_and_save,
    };
    use butterfly_geocode::tagger::transformer::ModelConfig;

    let corpus = if let Some(path) = corpus_path {
        info!(path = %path.display(), "loading corpus");
        let c = read_jsonl_corpus(&path)?;
        info!(examples = c.len(), "corpus loaded");
        if c.is_empty() {
            bail!("corpus at {} is empty", path.display());
        }
        c
    } else {
        info!(n = synthetic_n, "generating synthetic Belgium corpus");
        let c = generate_belgium_synthetic(synthetic_n, seed);
        info!(examples = c.len(), "synthetic corpus generated");
        c
    };

    let cfg = ModelConfig::tiny();
    let train_cfg = TrainConfig {
        epochs,
        batch_size,
        learning_rate,
        seed,
        ..Default::default()
    };

    let metrics = train_and_save(cfg, train_cfg, &corpus, &out)?;
    info!("training complete");
    if let Some(last) = metrics.last() {
        info!(
            final_train_loss = last.train_loss,
            final_eval_loss = last.eval_loss,
            final_bio_acc = last.eval_bio_acc,
            final_country_acc = last.eval_country_acc,
            "final metrics"
        );
    }
    info!(model_path = %out.display(), "model written");
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn serve_cmd(
    shard_path: Option<&std::path::Path>,
    shard_dir: Option<&std::path::Path>,
    host: &str,
    rest_port: u16,
    grpc_port: u16,
    transport: Transport,
    rerank_model_path: Option<&std::path::Path>,
    parser_kind: ParserKind,
    model_path: Option<&std::path::Path>,
    server_cfg: ServerConfig,
    shutdown_timeout: std::time::Duration,
) -> Result<()> {
    // Pick the parser backend first — the neural parser is wired via
    // `ParserBackend` (#98 Phase 1), the heuristic backend is always
    // available as a deterministic fallback (Phase 0 baseline). The
    // GBDT reranker layer composes on top regardless of parser choice.
    let parser_backend: Arc<dyn ParserBackend> = match parser_kind {
        ParserKind::Heuristic => {
            info!("using heuristic parser backend");
            Arc::new(HeuristicBackend)
        }
        ParserKind::Neural => {
            let model_path = model_path.ok_or_else(|| {
                anyhow!("--parser=neural requires --model <path/to/model.safetensors>")
            })?;
            match NeuralParser::load(model_path) {
                Ok(p) => {
                    info!(
                        model = %model_path.display(),
                        "neural parser loaded — using #98 Phase 1 retrieval-aware decoding"
                    );
                    Arc::new(NeuralBackend::new(p))
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        model = %model_path.display(),
                        "neural model failed to load; falling back to heuristic parser"
                    );
                    Arc::new(HeuristicBackend)
                }
            }
        }
    };

    let mut state = match (shard_path, shard_dir) {
        (Some(p), None) => {
            info!(shard = %p.display(), "loading shard (single-country mode)");
            let shard = Shard::open(p).context("opening shard")?;
            info!(
                country = shard.country().iso2(),
                record_count = shard.record_count(),
                "shard loaded"
            );
            ServerState::new(shard)
        }
        (None, Some(d)) => {
            info!(dir = %d.display(), "loading shards from directory (multi-country mode)");
            let s = ServerState::load_from_dir(d).context("loading shards")?;
            info!(
                countries = ?s.loaded_countries(),
                total_records = s.total_record_count(),
                "shards loaded"
            );
            s
        }
        (Some(_), Some(_)) => unreachable!("clap's conflicts_with prevents this"),
        (None, None) => {
            bail!("missing --shard <PATH> or --shard-dir <DIR>; specify one");
        }
    };
    state = state.with_parser(parser_backend);
    if let Some(p) = rerank_model_path {
        info!(model = %p.display(), "loading GBDT reranker");
        let model = GbdtModel::load(p).context("loading reranker")?;
        state = state.with_rerank_model(model);
    }
    let state = Arc::new(state);

    // Bind the REST listener up-front (when REST is enabled) so a port
    // conflict surfaces before we spawn anything.
    let rest_listener = if matches!(transport, Transport::Rest | Transport::Both) {
        let addr = format!("{host}:{rest_port}");
        let listener = tokio::net::TcpListener::bind(&addr)
            .await
            .with_context(|| format!("binding REST {addr}"))?;
        info!(addr = %addr, "REST server listening");
        Some(listener)
    } else {
        None
    };

    info!(
        transport = ?transport,
        rest_port = rest_port,
        grpc_port = grpc_port,
        "starting transports"
    );

    // Graceful shutdown shape:
    //
    //   1. A single signal task awaits SIGINT/SIGTERM. When it fires,
    //      it logs "shutdown initiated", broadcasts via a `Notify`,
    //      then sleeps for `shutdown_timeout`.
    //   2. Both axum::serve and tonic::serve_with_shutdown listen to
    //      the same `Notify` for graceful shutdown — they stop
    //      accepting new connections and drain in-flight requests.
    //   3. The deadline timer races the drain. If drain wins we log
    //      "shutdown complete (graceful)". If the timer wins we log a
    //      warning and exit forcefully.
    let shutdown = Arc::new(tokio::sync::Notify::new());

    let signal_shutdown = Arc::clone(&shutdown);
    let drain_deadline_secs = shutdown_timeout.as_secs();
    let drain_deadline_task = tokio::spawn(async move {
        wait_for_shutdown_signal().await;
        info!(
            shutdown_timeout_secs = drain_deadline_secs,
            "shutdown initiated"
        );
        signal_shutdown.notify_waiters();
        tokio::time::sleep(shutdown_timeout).await;
    });

    // REST future: only present when transport selects REST or Both.
    let rest_fut: std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> =
        if let Some(listener) = rest_listener {
            let app = build_router_with_config(Arc::clone(&state), server_cfg);
            let serve_shutdown = Arc::clone(&shutdown);
            Box::pin(async move {
                axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .with_graceful_shutdown(async move {
                    serve_shutdown.notified().await;
                })
                .await
                .context("REST server crashed")
            })
        } else {
            Box::pin(async { std::future::pending::<Result<()>>().await })
        };

    // gRPC future: only present when transport selects gRPC or Both.
    let grpc_fut: std::pin::Pin<Box<dyn std::future::Future<Output = Result<()>> + Send>> =
        if matches!(transport, Transport::Grpc | Transport::Both) {
            let grpc_state = Arc::clone(&state);
            let grpc_shutdown = Arc::clone(&shutdown);
            let host = host.to_string();
            Box::pin(async move {
                start_grpc_server(grpc_state, &host, grpc_port, async move {
                    grpc_shutdown.notified().await;
                })
                .await
                .context("gRPC server crashed")
            })
        } else {
            Box::pin(async { std::future::pending::<Result<()>>().await })
        };

    tokio::pin!(rest_fut);
    tokio::pin!(grpc_fut);
    tokio::pin!(drain_deadline_task);
    tokio::select! {
        result = &mut rest_fut => {
            result?;
            info!("REST shutdown complete (graceful)");
            drain_deadline_task.abort();
        }
        result = &mut grpc_fut => {
            result?;
            info!("gRPC shutdown complete (graceful)");
            drain_deadline_task.abort();
        }
        _ = &mut drain_deadline_task => {
            tracing::warn!(
                shutdown_timeout_secs = shutdown_timeout.as_secs(),
                "graceful shutdown deadline exceeded; exiting forcefully"
            );
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
fn train_rerank_cmd(
    shard_path: &std::path::Path,
    corpus_path: Option<&std::path::Path>,
    out: &std::path::Path,
    iterations: usize,
    max_depth: u32,
    limit_per_query: usize,
    synth_size: usize,
    dump_rows: Option<&std::path::Path>,
) -> Result<()> {
    info!(shard = %shard_path.display(), "loading shard");
    let shard = Shard::open(shard_path).context("opening shard")?;
    info!(record_count = shard.record_count(), "shard loaded");

    let corpus = match corpus_path {
        Some(p) => {
            info!(corpus = %p.display(), "loading labelled corpus");
            let c = load_corpus(p).context("loading corpus")?;
            info!(rows = c.len(), "corpus loaded");
            c
        }
        None => {
            info!(
                synth_size,
                "no corpus provided — synthesising from shard records"
            );
            synthesise_corpus_from_shard(&shard, synth_size)
        }
    };
    if corpus.is_empty() {
        return Err(anyhow::anyhow!("empty corpus — nothing to train on"));
    }

    let start = std::time::Instant::now();
    let (rows, balance) =
        build_training_rows(&shard, &corpus, limit_per_query).context("building training rows")?;
    info!(
        rows = rows.len(),
        positives = balance.positives,
        negatives = balance.negatives,
        secs = start.elapsed().as_secs_f64(),
        "training rows materialised"
    );
    if rows.is_empty() {
        return Err(anyhow::anyhow!(
            "no training rows produced — executor returned no candidates for any corpus entry"
        ));
    }
    if balance.positives == 0 {
        return Err(anyhow::anyhow!(
            "no positive labels in corpus — model would degenerate"
        ));
    }

    if let Some(p) = dump_rows {
        dump_training_rows(&rows, p).context("dumping training rows")?;
        info!(dump = %p.display(), "training rows written");
    }

    let cfg = TrainConfig {
        n_trees: iterations,
        max_depth,
        ..TrainConfig::default()
    };
    let t = std::time::Instant::now();
    let model = train_pointwise(&rows, cfg).context("training GBDT")?;
    info!(
        secs = t.elapsed().as_secs_f64(),
        n_trees = iterations,
        max_depth,
        "GBDT trained"
    );

    let groups =
        build_training_groups(&shard, &corpus, limit_per_query).context("building eval groups")?;
    let report = evaluate(&model, &groups);
    info!(
        binary_accuracy = report.binary_accuracy,
        rank_1_hit_rate = report.rank_1_hit_rate,
        n_groups = report.n_groups,
        n_groups_with_positive = report.n_groups_with_positive,
        rank_1_hits = report.rank_1_hits,
        "training-set eval"
    );

    model.save(out).context("saving GBDT model")?;
    info!(out = %out.display(), "GBDT model written");

    let _ = GbdtModel::load(out).context("verifying saved model loads cleanly")?;
    info!("saved model verified (load-back succeeded)");

    Ok(())
}

/// Resolve on the first SIGINT or SIGTERM. Caller is responsible for
/// any post-signal logging — we keep this function minimal so callers
/// can broadcast the event through a `Notify` and start their drain
/// budget at the right wall-clock moment.
async fn wait_for_shutdown_signal() {
    use tokio::signal;
    let ctrl_c = async {
        signal::ctrl_c().await.expect("install Ctrl+C handler");
    };
    #[cfg(unix)]
    let terminate = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("install SIGTERM handler")
            .recv()
            .await;
    };
    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        _ = ctrl_c => {
            info!("received SIGINT");
        },
        _ = terminate => {
            info!("received SIGTERM");
        },
    }
}

// =============================================================================
// flight-batch CLI subcommand (#145)
// =============================================================================

async fn flight_batch_cmd(
    endpoint: &str,
    queries_path: &std::path::Path,
    output_path: &std::path::Path,
    limit: u32,
    include_debug: bool,
    group_by_country: bool,
) -> Result<()> {
    use std::fs::File;
    use std::io::{BufRead, BufReader, BufWriter};

    use arrow::ipc::writer::StreamWriter;
    use arrow::record_batch::RecordBatch;
    use arrow_flight::FlightDescriptor;
    use arrow_flight::encode::FlightDataEncoderBuilder;
    use arrow_flight::flight_service_client::FlightServiceClient;
    use butterfly_geocode::server::flight::{
        build_input_batch, decode_output_batch, geocode_batch_output_schema,
    };
    use futures::StreamExt;
    use futures::stream;
    use tonic::Request;
    use tonic::transport::Channel;

    info!(endpoint = %endpoint, queries = %queries_path.display(), "loading queries");

    #[derive(serde::Deserialize)]
    struct QLine {
        query: String,
        #[serde(default)]
        country: Option<String>,
    }

    let f =
        File::open(queries_path).with_context(|| format!("opening {}", queries_path.display()))?;
    let reader = BufReader::new(f);
    let mut queries: Vec<(String, Option<String>)> = Vec::new();
    for (i, line) in reader.lines().enumerate() {
        let line = line.context("reading queries")?;
        if line.trim().is_empty() {
            continue;
        }
        let q: QLine = serde_json::from_str(&line)
            .with_context(|| format!("parsing line {} as JSONL", i + 1))?;
        queries.push((q.query, q.country));
    }
    info!(n_queries = queries.len(), "queries loaded");
    if queries.is_empty() {
        bail!("no queries to send");
    }

    let channel = Channel::from_shared(endpoint.to_string())
        .with_context(|| format!("invalid endpoint {endpoint}"))?
        .connect()
        .await
        .with_context(|| format!("connecting to {endpoint}"))?;
    let mut client = FlightServiceClient::new(channel)
        .max_encoding_message_size(64 * 1024 * 1024)
        .max_decoding_message_size(64 * 1024 * 1024);

    // Build input RecordBatches in 8192-row chunks so the upload
    // streams instead of buffering the whole input client-side.
    const CHUNK: usize = 8192;
    let chunks: Vec<Vec<(String, Option<String>)>> =
        queries.chunks(CHUNK).map(|c| c.to_vec()).collect();

    let params_json = serde_json::json!({
        "limit": limit,
        "include_debug": include_debug,
        "group_by_country": group_by_country,
    })
    .to_string();
    let cmd = format!("geocode_batch:{params_json}");

    let descriptor = FlightDescriptor::new_cmd(cmd.clone().into_bytes());
    let mut input_batches: Vec<RecordBatch> = Vec::with_capacity(chunks.len());
    for chunk in chunks {
        let batch = build_input_batch(&chunk).context("building input RecordBatch")?;
        input_batches.push(batch);
    }
    let in_schema = input_batches[0].schema();
    let batch_stream = stream::iter(
        input_batches
            .into_iter()
            .map(Ok::<_, arrow_flight::error::FlightError>),
    );
    let encoded = FlightDataEncoderBuilder::new()
        .with_schema(in_schema)
        .build(batch_stream);
    // Attach the descriptor to the first FlightData by mapping the
    // stream — Arrow Flight convention.
    let mut first = true;
    let descriptor_for_stream = descriptor.clone();
    let upload = encoded.map(move |fd_res| match fd_res {
        Ok(mut fd) => {
            if first {
                fd.flight_descriptor = Some(descriptor_for_stream.clone());
                first = false;
            }
            Ok::<_, arrow_flight::error::FlightError>(fd)
        }
        Err(e) => Err(e),
    });
    let upload = upload.filter_map(|x| async move {
        match x {
            Ok(fd) => Some(fd),
            Err(e) => {
                tracing::error!(error = %e, "upload encoding failed");
                None
            }
        }
    });

    let request = Request::new(upload);
    let t0 = std::time::Instant::now();
    let response = client
        .do_exchange(request)
        .await
        .context("DoExchange RPC failed")?;
    let mut response_stream = response.into_inner();

    let mut all_fds: Vec<arrow_flight::FlightData> = Vec::new();
    while let Some(fd) = response_stream.next().await {
        let fd = fd.context("decoding response stream")?;
        all_fds.push(fd);
    }
    let result_batches = arrow_flight::utils::flight_data_to_batches(&all_fds)
        .context("decoding response RecordBatches")?;

    let total_rows: usize = result_batches.iter().map(|b| b.num_rows()).sum();
    let elapsed = t0.elapsed();
    info!(
        rows = total_rows,
        secs = elapsed.as_secs_f64(),
        rps = if elapsed.as_secs_f64() > 0.0 {
            total_rows as f64 / elapsed.as_secs_f64()
        } else {
            0.0
        },
        "flight batch complete"
    );

    // Write Arrow IPC stream output.
    let out_schema = std::sync::Arc::new(geocode_batch_output_schema());
    let f =
        File::create(output_path).with_context(|| format!("creating {}", output_path.display()))?;
    let mut w = BufWriter::new(f);
    {
        let mut writer =
            StreamWriter::try_new(&mut w, &out_schema).context("creating IPC stream writer")?;
        for batch in &result_batches {
            writer.write(batch).context("writing batch")?;
        }
        writer.finish().context("finalising IPC stream")?;
    }
    info!(out = %output_path.display(), "results written");

    if let Some(first_batch) = result_batches.first() {
        let _decoded = decode_output_batch(first_batch).context("decoding first batch")?;
    }

    Ok(())
}
