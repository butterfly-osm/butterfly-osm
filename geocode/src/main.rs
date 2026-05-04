//! butterfly-geocode CLI: build shards, train models, and serve the API.

#![deny(unsafe_code)]

use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use clap::{Parser, Subcommand, ValueEnum};
use tracing::{Level, info, warn};
use tracing_subscriber::EnvFilter;

use butterfly_geocode::confidence::{
    GbdtModel, TrainConfig, build_training_groups, build_training_rows, dump_training_rows,
    evaluate, load_corpus, synthesise_corpus_from_shard, train_pointwise,
};
use butterfly_geocode::osm_extract::{ExtractProgress, extract_addresses};
use butterfly_geocode::server::{ServerState, build_router};
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
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
    /// Build a shard from an OSM PBF.
    BuildShard {
        #[arg(long)]
        pbf: PathBuf,
        #[arg(long)]
        out: PathBuf,
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
    /// Run the HTTP server.
    Serve {
        #[arg(long)]
        shard: PathBuf,
        #[arg(long, default_value_t = 3003)]
        port: u16,
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
        Command::BuildShard { pbf, out } => build_shard_cmd(&pbf, &out),
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
            port,
            host,
            rerank_model,
            parser,
            model,
        } => {
            serve_cmd(
                &shard,
                &host,
                port,
                rerank_model.as_deref(),
                parser,
                model.as_deref(),
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

fn build_shard_cmd(pbf: &PathBuf, out: &PathBuf) -> Result<()> {
    info!(pbf = %pbf.display(), out = %out.display(), "extracting OSM addresses");
    let start = std::time::Instant::now();

    let addresses = extract_addresses(pbf, |evt| match evt {
        ExtractProgress::Phase { phase } => info!("phase: {phase}"),
        ExtractProgress::NodePass {
            nodes_seen,
            addresses_emitted,
        } => info!(nodes_seen, addresses_emitted, "nodes pass complete"),
        ExtractProgress::WayPass {
            ways_seen,
            addresses_emitted,
        } => info!(ways_seen, addresses_emitted, "ways pass complete"),
    })?;

    info!(
        count = addresses.len(),
        secs = start.elapsed().as_secs_f64(),
        "extracted addresses"
    );

    let stats = build_shard(out, addresses).context("writing shard")?;
    info!(
        records = stats.record_count,
        unique_postcodes = stats.unique_postcodes,
        unique_streets = stats.unique_streets,
        strings_bytes = stats.strings_bytes,
        records_bytes = stats.records_bytes,
        index_bytes = stats.index_bytes,
        secs = start.elapsed().as_secs_f64(),
        "shard built"
    );

    let _ = Shard::open(out).context("verifying shard CRC after build")?;
    info!("shard verified");

    Ok(())
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

async fn serve_cmd(
    shard_path: &PathBuf,
    host: &str,
    port: u16,
    rerank_model_path: Option<&std::path::Path>,
    parser_kind: ParserKind,
    model_path: Option<&std::path::Path>,
) -> Result<()> {
    info!(shard = %shard_path.display(), "loading shard");
    let shard = Shard::open(shard_path).context("opening shard")?;
    info!(record_count = shard.record_count(), "shard loaded");

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

    let mut state = ServerState::new(shard).with_parser(parser_backend);
    if let Some(p) = rerank_model_path {
        info!(model = %p.display(), "loading GBDT reranker");
        let model = GbdtModel::load(p).context("loading reranker")?;
        state = state.with_rerank_model(model);
    }
    let state = Arc::new(state);
    let app = build_router(state);

    let addr = format!("{host}:{port}");
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("binding {addr}"))?;
    info!(addr = %addr, "serving");

    axum::serve(
        listener,
        app.into_make_service_with_connect_info::<std::net::SocketAddr>(),
    )
    .with_graceful_shutdown(shutdown_signal())
    .await
    .context("server crashed")?;
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

async fn shutdown_signal() {
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
        _ = ctrl_c => {},
        _ = terminate => {},
    }
    info!("shutdown signal received");
}
