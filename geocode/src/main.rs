//! butterfly-geocode CLI: build shards, train models, and serve the API.

#![deny(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use tracing::{Level, info, warn};
use tracing_subscriber::EnvFilter;

use butterfly_geocode::CountryId;
use butterfly_geocode::confidence::GbdtModel;
use butterfly_geocode::index::{BuildOptions, build_recall_index};
use butterfly_geocode::osm_extract::{ExtractProgress, extract_addresses_with_tags};
use butterfly_geocode::server::{
    DEFAULT_GRPC_PORT, DEFAULT_REST_PORT, ServerConfig, ServerState, Transport,
    build_router_with_config, start_grpc_server,
};
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use butterfly_geocode::shard::{AddressRecord, SourceTag};
use butterfly_geocode::sources::{
    SourceProgress, collect_all, merge_records, openaddresses::OpenAddressesSource,
};
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
    /// sources (OSM PBF tags, OpenAddresses GeoJSON-seq, …).
    ///
    /// Three usage modes:
    ///
    /// 1. Single OSM PBF:        `--pbf <PATH> [--source osm]`
    /// 2. Single OpenAddresses:  `--csv <PATH> --source openaddresses`
    /// 3. Merge two shards:      `--merge a.bfgs --merge b.bfgs`
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
        /// Source OpenAddresses file. Accepts `.geojson.gz` (the
        /// canonical processed format), `.zip` (legacy wrapping or
        /// raw upstream pack), or `.csv` / `.geojson` /
        /// `.geojsonseq` / `.ndjson`. Mutually exclusive with
        /// `--pbf` / `--merge`. Despite the historical `--csv` flag
        /// name, the loader auto-detects the format from the magic
        /// bytes — gzip, zip, or raw — and dispatches accordingly.
        #[arg(long, conflicts_with_all = ["pbf", "merge"])]
        csv: Option<PathBuf>,
        /// Merge multiple existing shards into one (deduped). Repeat
        /// the flag for each input shard. Mutually exclusive with
        /// `--pbf` / `--csv`.
        #[arg(long, conflicts_with_all = ["pbf", "csv"])]
        merge: Vec<PathBuf>,
        /// Output BFGS v5 shard file.
        #[arg(long)]
        out: PathBuf,
        /// ISO 3166-1 alpha-2 country code for this shard. Stored
        /// in the BFGS v5 header and verified at server load.
        #[arg(long)]
        country: String,
        /// Authoritative-source tag for the records in this shard
        /// (`osm`, `openaddresses` / `oa`). Required for `--csv`;
        /// optional for `--pbf` (defaults to `osm`); ignored for
        /// `--merge` (each input shard already carries its own
        /// per-record tag).
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
        /// model architecture + country vocab is written next to it.
        #[arg(long)]
        out: PathBuf,
        /// Path to a JSONL corpus. Two formats are accepted (auto-detected):
        ///
        /// 1. Spans format (legacy): `{"text", "country", "spans": [{"field","start","end"}, ...]}`.
        /// 2. corpus-gen BIO format: `{"text", "country", "bio_labels": [...], "augmentation": "..."}`.
        #[arg(long)]
        corpus: Option<PathBuf>,
        /// Number of synthetic examples to generate when no corpus is
        /// provided. Default 4096.
        #[arg(long, default_value_t = 4096)]
        synthetic: usize,
        /// Comma-separated list of ISO 3166-1 alpha-2 country codes that
        /// the model's country head will be sized for. Order does not
        /// matter — the vocab is internally lex-sorted.
        #[arg(long, default_value = "BE")]
        countries: String,
        /// Architecture profile. `tiny` (the proof-of-life shipped model)
        /// or `production` (#96 Fork A+: d=128, l=4, h=8, ~825k params).
        #[arg(long, default_value = "tiny")]
        architecture: String,
        /// Number of training epochs.
        #[arg(long, default_value_t = 8)]
        epochs: usize,
        /// Mini-batch size.
        #[arg(long, default_value_t = 64)]
        batch_size: usize,
        /// Peak learning rate for AdamW.
        #[arg(long, default_value_t = 1e-3)]
        learning_rate: f64,
        /// LR schedule kind: `cosine`, `linear`, or `constant`.
        /// All variants do a linear warmup ramp first.
        #[arg(long, default_value = "cosine")]
        lr_schedule: String,
        /// AdamW weight-decay coefficient.
        #[arg(long, default_value_t = 0.01)]
        weight_decay: f64,
        /// Max global gradient L2-norm. Set to 0 to disable clipping.
        #[arg(long, default_value_t = 1.0)]
        gradient_clip: f64,
        /// Number of linear-warmup steps. `0` disables warmup.
        #[arg(long, default_value_t = 1000)]
        warmup_steps: usize,
        /// Eval split fraction in `[0.0, 1.0)`. Default 0.1.
        #[arg(long, default_value_t = 0.1)]
        eval_split: f32,
        /// Random seed.
        #[arg(long, default_value_t = 0xB17EBAD0)]
        seed: u64,
        /// Compute device. `auto` picks CUDA when available + the binary
        /// was compiled with `--features cuda`, otherwise CPU. `cuda`
        /// errors loudly if the GPU isn't reachable. `cpu` is the
        /// CPU-only path.
        #[arg(long, default_value = "auto")]
        device: String,
        /// Compute dtype. `f32` (default) or `bf16` (mixed precision —
        /// requires CUDA + Ada/Ampere/Hopper). When dtype=bf16 the
        /// trainer warns and falls back to F32 if the model layers
        /// can't honour BF16 yet.
        #[arg(long, default_value = "f32")]
        dtype: String,
        /// Wall-clock training budget (seconds). When elapsed exceeds
        /// this at the start of an epoch, training writes a checkpoint
        /// and exits with status code 2 (more work possible). `0` =
        /// unlimited (default).
        #[arg(long, default_value_t = 0)]
        max_train_seconds: u64,
        /// Stop if eval_loss has not improved by `--early-stop-min-delta`
        /// for this many consecutive epochs. `0` disables (default).
        #[arg(long, default_value_t = 0)]
        early_stop_patience: usize,
        /// Minimum eval_loss improvement (lower is better) considered
        /// a real improvement for early stopping.
        #[arg(long, default_value_t = 1e-3)]
        early_stop_min_delta: f32,
        /// Append per-epoch JSONL telemetry to this path. One row per
        /// epoch with epoch, train_loss, eval_loss, bio_acc, country_acc,
        /// lr, wall_seconds_elapsed, plateau_signal, plateau_streak,
        /// best_eval_loss, global_step, device, n_countries, d_model,
        /// n_layers.
        #[arg(long)]
        metrics_out: Option<PathBuf>,
        /// Resume from an existing safetensors checkpoint. The
        /// architecture must match.
        #[arg(long)]
        resume: Option<PathBuf>,
        /// When resuming, the optimizer step count to start from for
        /// the LR schedule. The previous run's last logged
        /// `global_step` from `--metrics-out` is the right value.
        #[arg(long, default_value_t = 0)]
        resume_step: usize,
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
        /// Disable admission control entirely. Use when a fronting
        /// reverse proxy / load balancer handles rate limiting, or
        /// for benchmarks that want raw service throughput. When
        /// set, neither the global nor the per-IP token bucket is
        /// consulted on the request path. Default: false.
        #[arg(long, default_value_t = false)]
        admission_disable: bool,
        /// Per-IP admission token-bucket steady-state refill rate
        /// (requests per second). Set to a high value (e.g.
        /// 1_000_000) for benchmarks talking from a single client
        /// IP. Default: 25, matching the production-hardening
        /// preset. Range: 1 - 10_000_000.
        #[arg(long, default_value_t = 25)]
        admission_per_ip_per_sec: u32,
        /// Per-IP admission token-bucket capacity (max burst).
        /// Default: 50. Range: 1 - 10_000_000.
        #[arg(long, default_value_t = 50)]
        admission_per_ip_burst: u32,
        /// Global admission token-bucket steady-state refill rate
        /// (requests per second). Default: 500. Range: 1 -
        /// 10_000_000.
        #[arg(long, default_value_t = 500)]
        admission_global_per_sec: u32,
        /// Global admission token-bucket capacity (max burst).
        /// Default: 1000. Range: 1 - 10_000_000.
        #[arg(long, default_value_t = 1_000)]
        admission_global_burst: u32,
        /// Maximum simultaneously-tracked client IPs in the
        /// admission table. Beyond this an amortised sweep evicts
        /// idle entries. Default: 10_000.
        #[arg(long, default_value_t = 10_000)]
        admission_max_tracked_ips: usize,
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
        /// Optional directory of country pack TOMLs that overlay
        /// the shipped packs (#96 §"Country Routing"). Each TOML is
        /// loaded after the shipped set and replaces the embedded
        /// pack for that ISO2; missing files leave the shipped pack
        /// in place. Useful for hot-patching a postcode regex or
        /// adding lexical cues without rebuilding the binary.
        #[arg(long)]
        pack_dir: Option<PathBuf>,
        /// Comma-separated CIDR allowlist of trusted reverse proxies.
        /// When set, the per-IP rate limiter pulls the client IP
        /// from `X-Forwarded-For` (rightmost non-trusted entry per
        /// RFC 7239) for connections coming from these CIDRs.
        /// Without it, all requests behind a reverse proxy share the
        /// proxy IP and rate-limiting is global rather than per-client.
        #[arg(long)]
        trusted_proxies: Option<String>,
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
    /// Train the rerank GBDT (#205 step 2).
    ///
    /// Reads one or more shards + recall indexes, synthesises perturbed
    /// gold queries from each shard, runs recall against the matching
    /// index, labels candidates by proximity to the gold record,
    /// computes [`RerankFeatures`] and trains a SINGLE pointwise
    /// logistic-loss GBDT on the pooled rows. Saves a versioned
    /// envelope file the server loads with `--rerank-model`.
    ///
    /// Use `--shard` for single-country training (legacy / smoke).
    /// Use `--shards-dir` to pool training rows across every shard
    /// in a directory — this is the multi-country production path
    /// (#89). The two flags are mutually exclusive.
    TrainRerank {
        /// Single BFGS shard (with sibling `.recall.fst` /
        /// `.recall.postings`). Mutually exclusive with `--shards-dir`.
        #[arg(long, conflicts_with = "shards_dir")]
        shard: Option<PathBuf>,
        /// Multi-shard mode: pool training rows from every `*.bfgs`
        /// shard in this directory. Each shard must have sibling
        /// `.recall.fst` / `.recall.postings` files. Synth rows are
        /// drawn proportionally to shard size up to `--synth-size`
        /// total queries, with a per-shard floor so even small
        /// shards contribute. Mutually exclusive with `--shard`.
        #[arg(long, conflicts_with = "shard")]
        shards_dir: Option<PathBuf>,
        /// Output GBDT model path.
        #[arg(long)]
        out: PathBuf,
        /// Number of boosting iterations.
        #[arg(long, default_value_t = 100)]
        iterations: usize,
        /// Tree max depth.
        #[arg(long, default_value_t = 6)]
        max_depth: u32,
        /// Number of synthetic queries to generate. In single-shard
        /// mode this is the per-shard count. In multi-shard mode this
        /// is the GLOBAL pooled count split proportionally across
        /// shards (with a floor of 200 queries per shard).
        #[arg(long, default_value_t = 5000)]
        synth_size: usize,
        /// Top-K candidates from recall scored per query.
        #[arg(long, default_value_t = 20)]
        limit_per_query: usize,
        /// Random seed.
        #[arg(long, default_value_t = 0xB17EBAD0)]
        seed: u64,
    },
    /// Build the recall FST + postings + stats sidecar for an
    /// existing BFGS shard. Idempotent — overwrites existing
    /// sidecars. Useful when an operator built shards before #205
    /// landed.
    BuildRecallIndex {
        /// BFGS shard to index.
        #[arg(long)]
        shard: PathBuf,
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
            countries,
            architecture,
            epochs,
            batch_size,
            learning_rate,
            lr_schedule,
            weight_decay,
            gradient_clip,
            warmup_steps,
            eval_split,
            seed,
            device,
            dtype,
            max_train_seconds,
            early_stop_patience,
            early_stop_min_delta,
            metrics_out,
            resume,
            resume_step,
        } => train_cmd(TrainCmdArgs {
            out,
            corpus_path: corpus,
            synthetic_n: synthetic,
            countries_csv: countries,
            architecture,
            epochs,
            batch_size,
            learning_rate,
            lr_schedule,
            weight_decay,
            gradient_clip,
            warmup_steps,
            eval_split,
            seed,
            device,
            dtype,
            max_train_seconds,
            early_stop_patience,
            early_stop_min_delta,
            metrics_out,
            resume,
            resume_step,
        }),
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
            admission_disable,
            admission_per_ip_per_sec,
            admission_per_ip_burst,
            admission_global_per_sec,
            admission_global_burst,
            admission_max_tracked_ips,
            request_timeout_secs,
            shutdown_timeout_secs,
            max_body_bytes,
            pack_dir,
            trusted_proxies,
        } => {
            let server_cfg = ServerConfig {
                rate_limit_per_sec,
                rate_limit_burst,
                request_timeout: std::time::Duration::from_secs(request_timeout_secs),
                max_request_body_bytes: max_body_bytes,
                trusted_proxies: butterfly_geocode::server::parse_trusted_proxies(
                    trusted_proxies.as_deref(),
                )
                .map_err(|e| anyhow!("parsing --trusted-proxies: {e}"))?,
            };
            // Admission policy is constructed here from CLI knobs
            // and then plumbed through `serve_cmd` into
            // `ServerState`. Defaults match the production-hardening
            // preset (per_ip 50 burst / 25/sec). The `--admission-*`
            // flags exist so deployments that front the geocoder
            // with a reverse proxy (or single-tenant benchmarks) can
            // relax or fully disable the gate without recompiling.
            let admission_policy = butterfly_geocode::control::AdmissionPolicy {
                disabled: admission_disable,
                global_capacity: admission_global_burst,
                global_refill_per_sec: admission_global_per_sec,
                per_ip_capacity: admission_per_ip_burst,
                per_ip_refill_per_sec: admission_per_ip_per_sec,
                max_tracked_ips: admission_max_tracked_ips,
                ..butterfly_geocode::control::AdmissionPolicy::default()
            };
            // Load + validate the pack registry. Either shipped-only
            // (default) or shipped + override directory. Done at CLI
            // level so a bad pack drop fails the boot loudly rather
            // than silently degrading classifier accuracy.
            let pack_registry = std::sync::Arc::new(match pack_dir.as_deref() {
                Some(d) => butterfly_geocode::routing::PackRegistry::shipped_with_overrides(d)
                    .with_context(|| format!("loading pack overrides from {}", d.display()))?,
                None => butterfly_geocode::routing::PackRegistry::shipped()
                    .context("loading shipped country packs")?,
            });
            info!(
                packs = pack_registry.len(),
                pack_dir = ?pack_dir,
                "country pack registry ready"
            );
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
                admission_policy,
                std::time::Duration::from_secs(shutdown_timeout_secs),
                pack_registry,
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
            shards_dir,
            out,
            iterations,
            max_depth,
            synth_size,
            limit_per_query,
            seed,
        } => match (shard, shards_dir) {
            (Some(p), None) => train_rerank_cmd(
                std::slice::from_ref(&p),
                &out,
                iterations,
                max_depth,
                synth_size,
                limit_per_query,
                seed,
            ),
            (None, Some(d)) => {
                let mut shards: Vec<PathBuf> = std::fs::read_dir(&d)
                    .with_context(|| format!("reading shards dir {}", d.display()))?
                    .filter_map(|e| e.ok())
                    .map(|e| e.path())
                    .filter(|p| p.extension().and_then(|s| s.to_str()) == Some("bfgs"))
                    .collect();
                shards.sort();
                if shards.is_empty() {
                    bail!("no *.bfgs shards found in {}", d.display());
                }
                info!(n = shards.len(), dir = %d.display(), "multi-shard rerank training");
                train_rerank_cmd(
                    &shards,
                    &out,
                    iterations,
                    max_depth,
                    synth_size,
                    limit_per_query,
                    seed,
                )
            }
            (Some(_), Some(_)) => bail!("--shard and --shards-dir are mutually exclusive"),
            (None, None) => bail!("specify exactly one of --shard or --shards-dir"),
        },
        Command::BuildRecallIndex { shard } => build_recall_index_cmd(&shard),
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
            anyhow!(
                "--csv requires --source <openaddresses|oa> so the shard byte is set explicitly. \
                 OSM data lives in PBF and is consumed via --pbf, not --csv — there is no \
                 OSM-CSV ingest path."
            )
        })?;
        let tag = SourceTag::from_name(tag_str).ok_or_else(|| {
            anyhow!("unknown --source '{tag_str}' (supported: openaddresses (alias oa))")
        })?;
        // The previous wording listed `osm` as a valid `--source` for
        // `--csv`, but `load_addr_source` only knows OpenAddresses
        // and aborts on every other tag. Rejecting the combination
        // upfront produces a usable error before any work starts.
        if tag != SourceTag::OpenAddresses {
            bail!(
                "--csv only supports --source openaddresses (or alias oa). \
                 Got --source={}. For OSM data use --pbf <PATH> instead — there is no \
                 OSM-CSV ingest path today.",
                tag.name()
            );
        }
        info!(
            csv = %csv_path.display(),
            country = country.iso2(),
            source = tag.name(),
            "loading authoritative-source feed"
        );
        load_addr_source(csv_path, country, tag)?
    } else if let Some(pbf_path) = pbf {
        // OSM PBF path. `source` defaults to `osm`.
        let tag_str = source.unwrap_or("osm");
        let tag = SourceTag::from_name(tag_str).ok_or_else(|| {
            anyhow!("unknown --source '{tag_str}' (supported: osm, openaddresses (alias oa))")
        })?;
        if tag != SourceTag::Osm {
            bail!(
                "--pbf is OSM-only; pass --source osm (or omit --source). Got --source={}",
                tag.name()
            );
        }
        // Per-country OSM tag overrides via the country pack
        // (#96 §"Per-Country Shard Contents"). Falls back to the
        // standard `addr:*` keys when no pack is shipped for `country`
        // — every shipped pack today carries the [osm_tags] section,
        // but we don't fail the build over a missing pack.
        let pack_registry = butterfly_geocode::routing::PackRegistry::shipped()
            .context("loading shipped country packs for OSM tag mapping")?;
        let osm_tags = pack_registry
            .get(country)
            .map(|p| p.osm_tags.clone())
            .unwrap_or_else(|| butterfly_geocode::routing::pack::OsmTags {
                postcode: "addr:postcode".to_string(),
                street: "addr:street".to_string(),
                housenumber: "addr:housenumber".to_string(),
                city: "addr:city".to_string(),
            });
        info!(
            pbf = %pbf_path.display(),
            out = %out.display(),
            country = country.iso2(),
            source = tag.name(),
            street_tag = osm_tags.street,
            "extracting OSM addresses (pack-driven tag mapping)"
        );
        extract_addresses_with_tags(pbf_path, &osm_tags, |evt| match evt {
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
             For OSM tags use --pbf <PBF>; for OpenAddresses use \
             --csv <GZ|ZIP|CSV|GEOJSON> --source openaddresses; \
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

    // Emit the recall FST + postings + stats sidecars next to the
    // shard. Operators no longer need a separate `build-recall-index`
    // run for fresh shards.
    let report =
        build_recall_index(out, &s, &BuildOptions::default()).context("building recall index")?;
    info!(
        fst = %report.fst_path.display(),
        postings = %report.postings_path.display(),
        stats = %report.stats_path.display(),
        vocab_size = report.stats.vocab_size,
        p50_postings = report.stats.p50_postings,
        p95_postings = report.stats.p95_postings,
        "recall index emitted"
    );

    Ok(())
}

/// Load an authoritative-source address feed. Today only
/// OpenAddresses is wired (`SourceTag::OpenAddresses`); other
/// authoritative sources land here as new arms once OpenAddresses no
/// longer covers them adequately.
fn load_addr_source(
    path: &std::path::Path,
    country: CountryId,
    tag: SourceTag,
) -> Result<Vec<AddressRecord>> {
    match tag {
        SourceTag::OpenAddresses => {
            let loader = OpenAddressesSource::new(path, country);
            collect_all(&loader, |evt| match evt {
                SourceProgress::Phase { phase } => info!("phase: {phase}"),
                SourceProgress::Records {
                    rows_seen,
                    records_emitted,
                } => info!(rows_seen, records_emitted, "OpenAddresses progress"),
            })
            .context("OpenAddresses ingest")
        }
        other => bail!(
            "address-feed ingest for source {} is not wired yet (only OpenAddresses today). \
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

    let allowed: Option<Vec<CountryId>> = match only {
        Some(s) => {
            let tokens: Vec<&str> = s
                .split(',')
                .map(str::trim)
                .filter(|t| !t.is_empty())
                .collect();
            if tokens.is_empty() {
                bail!(
                    "--only is empty (got {:?}); pass a comma-separated list of ISO2 codes \
                     like --only BE,FR,NL or omit the flag to build every shipped pack",
                    s
                );
            }
            let parsed: Result<Vec<CountryId>> = tokens
                .iter()
                .map(|t| {
                    CountryId::from_iso2(t).ok_or_else(|| {
                        anyhow!(
                            "--only contains invalid ISO 3166-1 alpha-2 code '{}' \
                             (must be exactly 2 letters)",
                            t
                        )
                    })
                })
                .collect();
            Some(parsed?)
        }
        None => None,
    };

    let mut built = Vec::<CountryId>::new();
    let mut skipped = Vec::<(CountryId, String)>::new();
    let pack_registry = butterfly_geocode::routing::PackRegistry::shipped()
        .context("loading shipped country packs")?;
    let all_countries: Vec<CountryId> = pack_registry.countries();
    for &c in &all_countries {
        if let Some(ref a) = allowed
            && !a.contains(&c)
        {
            continue;
        }
        // Authoritative-source preference: if the region index ships
        // any `[[address]]` entries for this country AND the operator
        // has staged the corresponding files under `<pbf_dir>/addresses/`,
        // prefer the OpenAddresses dump over OSM PBF tags. Belgium
        // ships three regional OA jobs (Flanders/Wallonia/Brussels);
        // when all three are present we build per-region shards in a
        // tmp dir and merge them into the country shard.
        match try_authoritative_build(c, pbf_dir, out_dir) {
            Ok(true) => {
                built.push(c);
                continue;
            }
            Ok(false) => {
                // No authoritative source available; fall through to PBF.
            }
            Err(e) => {
                warn!(country = c.iso2(), error = %e, "authoritative-source build failed; falling back to PBF");
            }
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

/// Per-country coverage manifest: the set of `[[address]]` entry IDs
/// that together provide complete national coverage. When `Some(ids)`
/// is returned, `try_authoritative_build` REQUIRES every id to be
/// staged on disk; otherwise the build falls back to PBF (which has
/// always-complete national coverage).
///
/// `None` means "use whatever is staged" — historic behaviour for
/// countries where the partial/full distinction doesn't apply.
///
/// Why declared in code rather than in `dl/regions/*.toml`:
/// `butterfly-dl::regions` doesn't currently model coverage policy
/// (issue out of scope here); the policy lives next to the build
/// command that consumes it. When `dl/` grows a `coverage_complete`
/// field per region this function becomes a one-line lookup.
fn coverage_complete_ids(c: CountryId) -> Option<&'static [&'static str]> {
    if c == CountryId::BE {
        // BOSA / OpenAddresses: each region (Brussels, Flanders,
        // Wallonia) is published in two languages — picking one
        // language per region gives full national coverage. Any one
        // of these IDs missing means the country is not fully
        // represented; we'd silently drop entire regions if we built
        // an authoritative-only shard.
        Some(&["oa-be-bru-fr", "oa-be-vlg-nl", "oa-be-wal-fr"])
    } else if c == CountryId::DE {
        // 14 German state OA packs (Berlin and Bayern are not in the
        // shipped index today — the OA upstream doesn't publish a
        // single-pack feed for them, so the index treats their
        // territory as PBF-only).
        Some(&[
            "oa-de-nw", "oa-de-bw", "oa-de-ni", "oa-de-he", "oa-de-rp", "oa-de-sn", "oa-de-bb",
            "oa-de-sh", "oa-de-st", "oa-de-th", "oa-de-mv", "oa-de-sl", "oa-de-hh", "oa-de-hb",
        ])
    } else {
        None
    }
}

/// If the region index for `c` ships `[[address]]` entries with
/// staged files under `<pbf_dir>/addresses/`, build the country shard
/// from the authoritative source(s). Returns `Ok(true)` when a shard
/// was written; `Ok(false)` when no authoritative source is available
/// **or** when coverage is partial and the caller should fall back
/// to PBF; `Err` on a build failure (caller should fall back to
/// PBF/OSM).
///
/// Coverage policy: when [`coverage_complete_ids`] declares a
/// complete-coverage set for the country, EVERY id in that set must
/// be staged on disk before we use the authoritative-only path —
/// otherwise we'd silently drop entire regions (Belgium minus
/// Wallonia, Germany minus 13 of 14 states, …). Partial sets log a
/// WARN and return `Ok(false)` so the caller falls through to PBF.
///
/// Today only BOSA / OpenAddresses ingestion is wired — the BOSA
/// loader is the authoritative source the geocode crate knows how
/// to ingest, alongside the OpenAddresses streaming loader (#96
/// §"Data Sources"). As BAN/BAG/etc. land they get a new arm here
/// AND a new arm in `load_csv_source` (geocode/src/main.rs:
/// `build_shard_cmd`).
fn try_authoritative_build(
    c: CountryId,
    pbf_dir: &std::path::Path,
    out_dir: &std::path::Path,
) -> Result<bool> {
    let addresses_dir = pbf_dir.join("addresses");
    if !addresses_dir.is_dir() {
        return Ok(false);
    }
    let region_name = if c == CountryId::BE {
        "belgium"
    } else if c == CountryId::FR {
        "france"
    } else if c == CountryId::NL {
        "netherlands"
    } else if c == CountryId::LU {
        "luxembourg"
    } else if c == CountryId::DE {
        "germany"
    } else if c == CountryId::AT {
        "austria"
    } else if c == CountryId::CH {
        "switzerland"
    } else if c == CountryId::US {
        "united-states"
    } else if c == CountryId::JP {
        "japan"
    } else if c == CountryId::BR {
        "brazil"
    } else if c == CountryId::IN {
        "india"
    } else if c == CountryId::AU {
        "australia"
    } else {
        return Ok(false);
    };
    let region_index = match butterfly_dl::regions::RegionIndex::load(region_name) {
        Ok(idx) => idx,
        Err(_) => return Ok(false),
    };

    // Walk the [[address]] section, collect every entry with both a
    // known loader AND a staged file on disk.
    let mut staged: Vec<(PathBuf, &'static str)> = Vec::new();
    let mut staged_ids: Vec<String> = Vec::new();
    for entry in &region_index.address {
        let tag = entry.source.as_deref().unwrap_or("");
        let static_tag: &'static str = match tag {
            "bosa" => "bosa",
            "openaddresses" | "oa" => "openaddresses",
            // BAN/BAG/G-NAF/BEV/swisstopo lack loaders today; ignore.
            _ => continue,
        };
        let candidate = addresses_dir.join(format!("{}.{}", entry.id, address_extension(entry)));
        if candidate.is_file() {
            staged.push((candidate, static_tag));
            staged_ids.push(entry.id.clone());
        }
    }
    if staged.is_empty() {
        return Ok(false);
    }

    // Coverage policy: if the country has a declared complete-set, every
    // ID in that set must be present on disk; otherwise we'd silently
    // build an authoritative-only shard with regional gaps (e.g. all of
    // Wallonia missing, or 13 of 14 German states missing). Falling
    // back to PBF is the safe choice.
    if let Some(required) = coverage_complete_ids(c) {
        let staged_set: std::collections::HashSet<&str> =
            staged_ids.iter().map(String::as_str).collect();
        let missing: Vec<&&str> = required
            .iter()
            .filter(|id| !staged_set.contains(*id))
            .collect();
        if !missing.is_empty() {
            warn!(
                country = c.iso2(),
                staged_ids = ?staged_ids,
                missing_ids = ?missing,
                required = ?required,
                "authoritative-source coverage is incomplete for {}; falling back to PBF (set --pbf-dir/addresses with the full coverage set to use authoritative ingest)",
                c.iso2()
            );
            return Ok(false);
        }
    }

    let out = out_dir.join(format!("{}.bfgs", c.iso2().to_ascii_lowercase()));

    // Single staged file: build directly from it.
    if staged.len() == 1 {
        let (csv_path, tag) = &staged[0];
        info!(
            country = c.iso2(),
            source = *tag,
            csv = %csv_path.display(),
            "using authoritative source for shard build"
        );
        build_shard_cmd(None, Some(csv_path), &[], &out, c.iso2(), Some(*tag))?;
        return Ok(true);
    }

    // Multiple staged files (e.g. BE's three BOSA regional ZIPs):
    // build a tmp shard per file, then merge into the country shard.
    let tmp = tempfile::tempdir().context("creating tmp dir for authoritative-source build")?;
    let mut per_region_shards: Vec<PathBuf> = Vec::with_capacity(staged.len());
    for (i, (csv_path, tag)) in staged.iter().enumerate() {
        let stem = csv_path
            .file_stem()
            .and_then(|s| s.to_str())
            .unwrap_or("source");
        let part = tmp.path().join(format!("{i:02}-{stem}.bfgs"));
        info!(
            country = c.iso2(),
            source = *tag,
            csv = %csv_path.display(),
            tmp = %part.display(),
            "ingesting authoritative-source shard fragment"
        );
        build_shard_cmd(None, Some(csv_path), &[], &part, c.iso2(), Some(*tag))?;
        per_region_shards.push(part);
    }
    info!(
        country = c.iso2(),
        fragments = per_region_shards.len(),
        out = %out.display(),
        "merging authoritative-source fragments into country shard"
    );
    build_shard_cmd(None, None, &per_region_shards, &out, c.iso2(), None)?;
    Ok(true)
}

/// Extension for an `AddressEntry`. Mirrors `RegionIndex` internals
/// but locally-typed since the field is private upstream.
fn address_extension(entry: &butterfly_dl::regions::AddressEntry) -> &'static str {
    match entry.format.as_str() {
        "csv-zip" | "xml-zip" | "zip" => "zip",
        "csv-gz" | "gz" => "csv.gz",
        "csv" => "csv",
        "xml" => "xml",
        _ => "bin",
    }
}

fn candidate_pbf_names(c: CountryId) -> Vec<String> {
    // Map every shipped pack-country to the long Geofabrik filename
    // form. CountryId is now a newtype (no enum), so we can't
    // exhaustively match on the type itself; instead we list each code
    // explicitly here. Adding a pack to PackRegistry::shipped() and
    // forgetting to wire a long name lands the country at ISO2-only
    // probing — still functional, just less likely to find user files
    // named `<country-name>.osm.pbf`.
    let long = if c == CountryId::BE {
        "belgium"
    } else if c == CountryId::FR {
        "france"
    } else if c == CountryId::NL {
        "netherlands"
    } else if c == CountryId::LU {
        "luxembourg"
    } else if c == CountryId::DE {
        "germany"
    } else if c == CountryId::AT {
        "austria"
    } else if c == CountryId::CH {
        "switzerland"
    } else if c == CountryId::GB {
        "great-britain"
    } else if c == CountryId::ES {
        "spain"
    } else if c == CountryId::IT {
        "italy"
    } else if c == CountryId::US {
        // butterfly-dl's region index stages the file as
        // `united-states.pbf`. The shorter `us.pbf` form was the old
        // convention; keep both via the alias list below so manually
        // staged files under either name still get picked up.
        "united-states"
    } else if c == CountryId::JP {
        "japan"
    } else if c == CountryId::BR {
        "brazil"
    } else if c == CountryId::IN {
        "india"
    } else if c == CountryId::AU {
        "australia"
    } else {
        ""
    };
    let mut v = Vec::new();
    if !long.is_empty() {
        v.push(format!("{long}.pbf"));
        v.push(format!("{long}.osm.pbf"));
        v.push(format!("{long}-latest.osm.pbf"));
    }
    // Country-specific aliases for filenames that don't follow the
    // single canonical Geofabrik long name. `united-states` is the
    // butterfly-dl region index canonical; `us` is the historical
    // operator shorthand. Keep both in the probe list.
    if c == CountryId::US {
        v.push("us.pbf".to_string());
        v.push("us.osm.pbf".to_string());
        v.push("us-latest.osm.pbf".to_string());
    }
    v.push(format!("{}.pbf", c.iso2().to_ascii_lowercase()));
    v.push(format!("{}.osm.pbf", c.iso2().to_ascii_lowercase()));
    v
}

#[allow(clippy::too_many_arguments)]
/// Bundle of `train` subcommand arguments. Beats a 22-positional fn
/// signature and lets clippy's `too_many_arguments` lint stay on.
struct TrainCmdArgs {
    out: PathBuf,
    corpus_path: Option<PathBuf>,
    synthetic_n: usize,
    countries_csv: String,
    architecture: String,
    epochs: usize,
    batch_size: usize,
    learning_rate: f64,
    lr_schedule: String,
    weight_decay: f64,
    gradient_clip: f64,
    warmup_steps: usize,
    eval_split: f32,
    seed: u64,
    device: String,
    dtype: String,
    max_train_seconds: u64,
    early_stop_patience: usize,
    early_stop_min_delta: f32,
    metrics_out: Option<PathBuf>,
    resume: Option<PathBuf>,
    resume_step: usize,
}

fn train_cmd(args: TrainCmdArgs) -> Result<()> {
    use butterfly_geocode::tagger::training::{
        CountryVocab, DevicePref, LrSchedule, StopReason, TrainConfig, generate_belgium_synthetic,
        read_jsonl_corpus, train_and_save_with_outcome,
    };
    use butterfly_geocode::tagger::transformer::ModelConfig;
    use candle_core::DType;

    let TrainCmdArgs {
        out,
        corpus_path,
        synthetic_n,
        countries_csv,
        architecture,
        epochs,
        batch_size,
        learning_rate,
        lr_schedule,
        weight_decay,
        gradient_clip,
        warmup_steps,
        eval_split,
        seed,
        device,
        dtype,
        max_train_seconds,
        early_stop_patience,
        early_stop_min_delta,
        metrics_out,
        resume,
        resume_step,
    } = args;

    let vocab = CountryVocab::from_csv(&countries_csv)?;
    info!(
        countries = vocab.countries().join(",").as_str(),
        n = vocab.len(),
        "country vocab"
    );

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

    let cfg = match architecture.trim().to_ascii_lowercase().as_str() {
        "tiny" => {
            // tiny is BE-only by definition; reject multi-country with tiny.
            if vocab.len() != 1 {
                bail!(
                    "architecture=tiny is single-country; got vocab len={}. Use --architecture production for multi-country.",
                    vocab.len()
                );
            }
            ModelConfig::tiny()
        }
        "production" => ModelConfig::production(vocab.len()),
        "large" => ModelConfig::large(vocab.len()),
        other => bail!(
            "unknown --architecture {:?} (use tiny|production|large)",
            other
        ),
    };
    info!(
        architecture = architecture.as_str(),
        d_model = cfg.d_model,
        n_layers = cfg.n_layers,
        n_heads = cfg.n_heads,
        d_ff = cfg.d_ff,
        n_countries = cfg.n_countries,
        params_approx = cfg.approx_param_count(),
        "model config"
    );

    let schedule = LrSchedule::parse(&lr_schedule)?;
    let grad_clip = if gradient_clip > 0.0 {
        Some(gradient_clip)
    } else {
        None
    };
    let device_pref = DevicePref::parse(&device)?;
    let dtype_parsed = match dtype.trim().to_ascii_lowercase().as_str() {
        "f32" | "fp32" => DType::F32,
        "bf16" | "bfloat16" => DType::BF16,
        other => bail!("unknown --dtype {:?} (use f32|bf16)", other),
    };
    let max_seconds = if max_train_seconds == 0 {
        None
    } else {
        Some(max_train_seconds)
    };

    let train_cfg = TrainConfig {
        epochs,
        batch_size,
        learning_rate,
        weight_decay,
        gradient_clip: grad_clip,
        warmup_steps,
        lr_schedule: schedule,
        eval_split,
        seed,
        device_pref,
        dtype: dtype_parsed,
        max_train_seconds: max_seconds,
        early_stop_patience,
        early_stop_min_delta,
        metrics_out: metrics_out.clone(),
        resume_from: resume.clone(),
        resume_optimizer_step: resume_step,
        ..Default::default()
    };

    let outcome = train_and_save_with_outcome(cfg, train_cfg, &vocab, &corpus, &out)?;
    info!("training complete");
    if let Some(last) = outcome.metrics.last() {
        info!(
            final_train_loss = last.train_loss,
            final_eval_loss = last.eval_loss,
            final_bio_acc = last.eval_bio_acc,
            final_country_acc = last.eval_country_acc,
            wall_seconds = last.wall_seconds_elapsed,
            best_eval_loss = last.best_eval_loss,
            plateau_streak = last.plateau_streak,
            stop_reason = ?outcome.stop_reason,
            "final metrics"
        );
    }
    info!(model_path = %out.display(), "model written");

    // Status code 2 means "more work possible" — chunked training driver
    // can pick the run back up with --resume + --resume-step.
    if outcome.stop_reason == StopReason::WallClockBudgetExhausted {
        info!("wall-clock budget hit; exiting with code 2 (more work possible)");
        std::process::exit(2);
    }
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
    admission_policy: butterfly_geocode::control::AdmissionPolicy,
    shutdown_timeout: std::time::Duration,
    pack_registry: Arc<butterfly_geocode::routing::PackRegistry>,
) -> Result<()> {
    // Pick the parser backend. The neural backend emits real BIO
    // logits + per-country posterior; the heuristic backend emits
    // neutral signals (cheap classifier only). The recall + rerank
    // pipeline runs identically on either.
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
                        "neural tagger loaded — emitting per-byte BIO logits + country posterior"
                    );
                    Arc::new(NeuralBackend::new(p))
                }
                Err(e) => {
                    warn!(
                        error = %e,
                        model = %model_path.display(),
                        "neural model failed to load; falling back to heuristic backend"
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
            let country = shard.country();
            let recall = butterfly_geocode::index::RecallIndex::open(p).with_context(|| {
                format!(
                    "opening recall index for shard at {} — rebuild via \
                     `butterfly-geocode build-recall-index --shard {}`",
                    p.display(),
                    p.display()
                )
            })?;
            info!(
                country = country.iso2(),
                record_count = shard.record_count(),
                recall_keys = recall.key_count(),
                "shard + recall index loaded"
            );
            let mut shards = std::collections::HashMap::new();
            shards.insert(country, std::sync::Arc::new(shard));
            let mut recaller = butterfly_geocode::geocoder::recall::Recaller::new();
            recaller.insert(country, recall);
            ServerState::from_shards_with_recaller(shards, recaller)
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
    state = state.with_pack_registry(pack_registry);
    state = state.with_admission_policy(admission_policy);
    info!(
        admission_disabled = admission_policy.disabled,
        per_ip_per_sec = admission_policy.per_ip_refill_per_sec,
        per_ip_burst = admission_policy.per_ip_capacity,
        global_per_sec = admission_policy.global_refill_per_sec,
        global_burst = admission_policy.global_capacity,
        max_tracked_ips = admission_policy.max_tracked_ips,
        "admission policy applied"
    );
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
            let (app, gc_handle) = build_router_with_config(Arc::clone(&state), server_cfg.clone());
            // Spawn the per-IP map garbage-collector inside the Tokio
            // runtime. The returned `StartedGovernorGc` aborts the GC
            // task on drop, so we move it into the REST future so the
            // task lives exactly as long as the server does. No more
            // process-wide OnceLock — every router built in this
            // process gets its own GC, every one cleans up on drop.
            let started_gc = butterfly_geocode::server::spawn_governor_gc(gc_handle);
            let serve_shutdown = Arc::clone(&shutdown);
            Box::pin(async move {
                let result = axum::serve(
                    listener,
                    app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
                )
                .with_graceful_shutdown(async move {
                    serve_shutdown.notified().await;
                })
                .await
                .context("REST server crashed");
                // Drop the GC AFTER the server is fully shut down so
                // we don't abort it mid-request. `drop` is explicit
                // here for documentation, even though falling out of
                // scope would do the same.
                drop(started_gc);
                result
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

/// Train the rerank GBDT (#205 step 2 / #89).
///
/// Pools synthetic perturbed-gold training rows across one or more
/// shards, runs recall against each shard's sibling FST, labels
/// candidates by proximity to the gold record, computes
/// [`butterfly_geocode::RerankFeatures`] per (query, candidate), and
/// trains a SINGLE pointwise log-likelihood GBDT on the pooled rows.
///
/// Multi-shard pooling: the `synth_size` budget is the GLOBAL query
/// count, allocated proportionally to shard size with a 200-query
/// floor per shard (so even small countries contribute training
/// signal, while large countries get a representative slice).
#[allow(clippy::too_many_arguments)]
fn train_rerank_cmd(
    shard_paths: &[PathBuf],
    out: &std::path::Path,
    iterations: usize,
    max_depth: u32,
    synth_size: usize,
    limit_per_query: usize,
    seed: u64,
) -> Result<()> {
    use butterfly_geocode::geocoder::recall::{RecallBudget, Recaller, TaggerSignals};
    use butterfly_geocode::geocoder::rerank::SourcePriors;
    use butterfly_geocode::index::RecallIndex;
    use butterfly_geocode::shard::reader::haversine_m;
    use gbdt::config::Config;
    use gbdt::decision_tree::{Data, DataVec};
    use gbdt::gradient_boost::GBDT;

    if shard_paths.is_empty() {
        bail!("no shard paths provided to train_rerank_cmd");
    }

    // Open every shard + its recall index up front. Pool record counts
    // for proportional query allocation. Each shard contributes:
    //   per_shard_budget = max(MIN_PER_SHARD,
    //                          synth_size * shard_size / total_size)
    // capped so the sum stays close to `synth_size`.
    const MIN_PER_SHARD: usize = 200;
    let mut shards: Vec<(butterfly_geocode::routing::CountryId, Shard)> = Vec::new();
    let mut recall_indexes: Vec<(butterfly_geocode::routing::CountryId, RecallIndex)> = Vec::new();
    for p in shard_paths {
        info!(shard = %p.display(), "loading shard + recall index");
        let shard = Shard::open(p).with_context(|| format!("opening shard {}", p.display()))?;
        let ri = RecallIndex::open(p).with_context(|| {
            format!(
                "opening recall index for shard at {} — rebuild via \
                 `butterfly-geocode build-recall-index --shard {}`",
                p.display(),
                p.display()
            )
        })?;
        let c = shard.country();
        info!(
            country = c.iso2(),
            record_count = shard.record_count(),
            recall_keys = ri.key_count(),
            "shard + recall index loaded"
        );
        shards.push((c, shard));
        recall_indexes.push((c, ri));
    }

    // Build the multi-country recaller.
    let mut recaller = Recaller::new();
    for (c, ri) in recall_indexes {
        recaller.insert(c, ri);
    }

    // Per-shard query budget allocation.
    let total_records: usize = shards.iter().map(|(_, s)| s.record_count()).sum();
    if total_records == 0 {
        bail!("all shards have zero records — cannot train rerank");
    }
    // Apply the floor; if proportional + floor exceeds the budget we
    // scale the proportional part back. The floor is honoured even
    // when budget is tight (small shards still get MIN_PER_SHARD).
    let mut per_shard_budgets: Vec<usize> = shards
        .iter()
        .map(|(_, s)| {
            let p = (synth_size as u128 * s.record_count() as u128 / total_records as u128) as usize;
            p.max(MIN_PER_SHARD)
        })
        .collect();
    // Clamp so the sum doesn't blow past 2x the requested size.
    let sum: usize = per_shard_budgets.iter().sum();
    let cap = synth_size.saturating_mul(2).max(synth_size + MIN_PER_SHARD * shards.len());
    if sum > cap {
        let scale = cap as f64 / sum as f64;
        for b in per_shard_budgets.iter_mut() {
            *b = ((*b as f64) * scale).round().max(MIN_PER_SHARD as f64) as usize;
        }
    }

    // Synthesise queries per shard, run recall, label, append to pool.
    let signals = TaggerSignals::default();
    let priors = SourcePriors::default();

    fn lcg(s: &mut u64) -> u32 {
        *s = s
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (*s >> 32) as u32
    }
    // Initial state seeded once; per-shard streams are derived by
    // mixing in the shard index so different shards produce different
    // queries even for the same record id distribution.
    let base_seed = seed;

    let mut rows: Vec<(Vec<f32>, f32)> = Vec::new();
    let mut per_country_pos: Vec<(String, usize, usize)> = Vec::new();
    for (idx, ((country, shard), budget)) in shards
        .iter()
        .zip(per_shard_budgets.iter().copied())
        .enumerate()
    {
        let n = shard.record_count() as u32;
        if n == 0 {
            warn!(country = country.iso2(), "shard has zero records — skipping");
            continue;
        }
        let mut state = base_seed.wrapping_add((idx as u64).wrapping_mul(0x9E3779B97F4A7C15));
        let mut queries: Vec<(u32, String)> = Vec::with_capacity(budget);
        for _ in 0..budget {
            let id = lcg(&mut state) % n;
            let Some(rec) = shard.record(id) else {
                continue;
            };
            let mode = lcg(&mut state) % 5;
            let q = match mode {
                0 => format!(
                    "{} {} {} {}",
                    rec.street, rec.housenumber, rec.postcode, rec.locality
                ),
                1 => format!("{} {} {}", rec.street, rec.postcode, rec.locality),
                2 => format!("{} {} {}", rec.street, rec.housenumber, rec.locality),
                3 => {
                    let mut s = rec.street.to_string();
                    if s.len() >= 4 {
                        s = unsafe_safe_swap(&s, 1);
                    }
                    format!(
                        "{} {} {} {}",
                        s, rec.housenumber, rec.postcode, rec.locality
                    )
                }
                _ => rec.locality.to_string(),
            };
            queries.push((id, q));
        }
        info!(
            country = country.iso2(),
            queries = queries.len(),
            budget,
            "synthetic queries generated for shard"
        );
        let budget_recall = RecallBudget::default().adapt_to_stats(
            recaller
                .stats_for(*country)
                .map(|s| s.p95_postings)
                .unwrap_or(256),
        );

        let pre_pos = rows.iter().filter(|r| r.1 > 0.5).count();
        let pre_total = rows.len();
        for (gold_id, q) in &queries {
            let cands = recaller.query(q, &signals, &[*country], &budget_recall);
            let cands_top: Vec<_> = cands.into_iter().take(limit_per_query).collect();
            if cands_top.is_empty() {
                continue;
            }
            let Some(gold_rec) = shard.record(*gold_id) else {
                continue;
            };
            let mut group_has_positive = false;
            let mut group_rows: Vec<(Vec<f32>, f32)> =
                Vec::with_capacity(cands_top.len());
            for cand in cands_top {
                let Some(rec) = shard.record(cand.address_id as u32) else {
                    continue;
                };
                let d = haversine_m(gold_rec.lat, gold_rec.lon, rec.lat, rec.lon);
                let label = if d <= 50.0 { 1.0 } else { 0.0 };
                if label > 0.5 {
                    group_has_positive = true;
                }
                let feat = compute_rerank_row(q, &signals, &cand, &rec, &priors);
                group_rows.push((feat, label));
            }
            if group_has_positive {
                rows.extend(group_rows);
            }
        }
        let post_pos = rows.iter().filter(|r| r.1 > 0.5).count();
        let post_total = rows.len();
        per_country_pos.push((
            country.iso2().to_string(),
            post_pos - pre_pos,
            post_total - pre_total,
        ));
    }

    info!(rows = rows.len(), shards = shards.len(), "pooled training rows materialised");
    for (iso, pos, total) in &per_country_pos {
        info!(country = iso, positives = pos, total = total, "per-country label contribution");
    }
    if rows.is_empty() {
        bail!(
            "no training rows with positive labels produced — synth_size or recall budget too small"
        );
    }
    let n_pos = rows.iter().filter(|r| r.1 > 0.5).count();
    let n_neg = rows.len() - n_pos;
    info!(positives = n_pos, negatives = n_neg, "pooled label balance");
    if n_pos == 0 || n_neg == 0 {
        bail!("degenerate labels: pos={n_pos} neg={n_neg}");
    }

    let mut data: DataVec = rows
        .into_iter()
        .map(|(feature, label)| Data {
            feature,
            target: label,
            weight: 1.0,
            label,
            residual: 0.0,
            initial_guess: 0.0,
        })
        .collect();
    let n_features = butterfly_geocode::RerankFeatures::N;
    let mut cfg = Config::new();
    cfg.set_feature_size(n_features);
    cfg.set_max_depth(max_depth);
    cfg.set_iterations(iterations);
    cfg.set_shrinkage(0.1);
    cfg.set_loss("LogLikelyhood");
    cfg.set_data_sample_ratio(1.0);
    cfg.set_feature_sample_ratio(1.0);
    cfg.set_training_optimization_level(2);
    let t0 = std::time::Instant::now();
    let mut g = GBDT::new(&cfg);
    g.fit(&mut data);
    info!(
        secs = t0.elapsed().as_secs_f64(),
        n_trees = iterations,
        max_depth,
        "GBDT trained"
    );

    let model = GbdtModel::from_inner(g);
    model.save(out).context("saving rerank GBDT")?;
    info!(out = %out.display(), "rerank GBDT model written");

    let _ = GbdtModel::load(out).context("verifying saved model loads cleanly")?;
    info!("saved model verified (load-back succeeded)");
    Ok(())
}

/// Compute a [`RerankFeatures`] row inline. Mirrors the production
/// pipeline's feature shape.
fn compute_rerank_row(
    query: &str,
    signals: &butterfly_geocode::TaggerSignals,
    cand: &butterfly_geocode::Candidate,
    rec: &butterfly_geocode::shard::reader::ShardRecord,
    priors: &butterfly_geocode::geocoder::rerank::SourcePriors,
) -> Vec<f32> {
    use butterfly_geocode::geocoder::recall::lexical_alignment_score;
    use butterfly_geocode::geocoder::rerank::extract_postcode;
    use butterfly_geocode::parser::normalize::normalize;

    let normalized_input = normalize(query);
    let canonical = format!(
        "{} {} {} {}",
        normalize(&rec.street),
        normalize(&rec.housenumber),
        normalize(&rec.postcode),
        normalize(&rec.locality)
    );
    let canonical = canonical.split_whitespace().collect::<Vec<_>>().join(" ");

    let lexical = lexical_alignment_score(&normalized_input, &canonical);

    let country_p = signals
        .country_posterior
        .iter()
        .find(|(c, _)| *c == cand.country)
        .map(|(_, p)| *p)
        .unwrap_or(0.0);

    let postcode_agreement = match extract_postcode(query) {
        Some(pc) if !rec.postcode.is_empty() => {
            let a = pc.to_ascii_lowercase().replace(' ', "");
            let b = rec.postcode.to_ascii_lowercase().replace(' ', "");
            if a == b {
                1.0
            } else if b.contains(&a) || a.contains(&b) {
                0.5
            } else {
                0.0
            }
        }
        _ => -1.0,
    };

    let source_prior = priors.for_source(cand.source_tag);
    let mut completeness = 0.0_f32;
    let mut total = 0.0_f32;
    for f in [&rec.street, &rec.housenumber, &rec.postcode, &rec.locality] {
        total += 1.0;
        if !f.is_empty() {
            completeness += 1.0;
        }
    }
    let completeness = completeness / total.max(1.0);

    vec![
        lexical,
        0.0, // tagger_bio_agreement: synthetic queries have no BIO logits
        country_p,
        postcode_agreement,
        source_prior,
        cand.recall_score,
        completeness,
    ]
}

/// UTF-8 safe single-character swap helper (typo perturbation).
fn unsafe_safe_swap(s: &str, idx: usize) -> String {
    let chars: Vec<char> = s.chars().collect();
    if idx + 1 >= chars.len() {
        return s.to_string();
    }
    let mut out: Vec<char> = chars.clone();
    out.swap(idx, idx + 1);
    out.into_iter().collect()
}

/// Build the recall FST + postings + stats sidecar for an existing
/// BFGS shard.
fn build_recall_index_cmd(shard_path: &std::path::Path) -> Result<()> {
    info!(shard = %shard_path.display(), "loading shard");
    let shard = Shard::open(shard_path).context("opening shard")?;
    info!(
        country = shard.country().iso2(),
        record_count = shard.record_count(),
        "shard loaded"
    );
    let report = build_recall_index(shard_path, &shard, &BuildOptions::default())
        .context("building recall index")?;
    info!(
        fst = %report.fst_path.display(),
        postings = %report.postings_path.display(),
        stats = %report.stats_path.display(),
        vocab_size = report.stats.vocab_size,
        avg_key_len = report.stats.avg_key_len,
        p50_postings = report.stats.p50_postings,
        p95_postings = report.stats.p95_postings,
        total_postings = report.stats.total_postings,
        "recall index built"
    );
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
