//! Training loop for the byte-level tagger (#96 §Tagger, #98 Phase 2 prerequisite).
//!
//! ## Pipeline
//!
//! 1. **Corpus** — JSONL, one example per line. **Two formats are
//!    accepted**, auto-detected per record:
//!
//!    - **Spans format** (legacy, hand-authored fixtures, the trainer's
//!      original schema):
//!      ```json
//!      {
//!        "text": "Rue Wayez 122 1070 Anderlecht",
//!        "country": "BE",
//!        "spans": [
//!          {"field": "street",   "start": 0,  "end": 9},
//!          {"field": "house",    "start": 10, "end": 13},
//!          {"field": "postcode", "start": 14, "end": 18},
//!          {"field": "locality", "start": 19, "end": 29}
//!        ]
//!      }
//!      ```
//!
//!    - **BIO-labels format** emitted by `geocode-training/corpus-gen`
//!      (one label per byte of `text`):
//!      ```json
//!      {
//!        "text": "Rue Wayez 122, 1070 Anderlecht",
//!        "country": "BE",
//!        "bio_labels": [1,2,2,2,2,2,2,2,2,0,3,4,4,0,0,5,6,6,6,0,7,8,8,8,8,8,8,8,8,8],
//!        "augmentation": "canonical",
//!        "source_record_id": "osm:n12345"
//!      }
//!      ```
//!      The corpus-gen scheme uses `{O=0, B/I-Street=1/2, B/I-Hnum=3/4,
//!      B/I-Post=5/6, B/I-City=7/8, B/I-Unit=9/10}`. Labels 0-8 align
//!      one-to-one with the trainer's BIO scheme; labels 9-10 (Unit)
//!      are folded to `O` (the trainer has no Unit field).
//!
//!    `augmentation` is optional (defaults to "canonical"). It is used
//!    by the country-head loss masking heuristic — variants like
//!    `drop_postcode` / `drop_city` make the country ambiguous, so we
//!    zero out their contribution to the country-head loss (per the
//!    codex review in `geocode-research/CORPUS_DESIGN_NOTES.md`).
//!
//! 2. **Synthetic generator** — when no corpus is provided, the
//!    [`generate_belgium_synthetic`] function emits N synthetic Belgium
//!    addresses by sampling Cartesian products of street/house/postcode/
//!    locality pools. This is the proof-of-life corpus shipped with
//!    the tiny model.
//!
//! 3. **Loss** — weighted CE on BIO + CE on country. Default weights
//!    `bio=1.0, country=0.3` reflect the relative supervision strength
//!    (BIO is per-token, country is per-sequence — country head needs
//!    less weight). Per-example country-loss masking is applied based
//!    on `augmentation` (drop-field variants get country-loss masked
//!    out — the country can't be inferred from a missing field, so
//!    supervising on it injects noise).
//!
//! 4. **Optimizer** — AdamW from `candle_nn::AdamW`. Defaults:
//!    `lr=1e-3, weight_decay=0.01, gradient_clip=1.0`. Learning-rate
//!    schedule: cosine annealing with linear warmup (configurable to
//!    linear decay or constant).
//!
//! 5. **Country vocabulary** — built from the `--countries BE,FR,NL,DE,US`
//!    CLI flag (or equivalent programmatic [`CountryVocab::new`]).
//!    Each country gets a deterministic id (lex-ordered uppercase ISO
//!    codes), plumbed through to `ModelConfig.n_countries` and the
//!    sidecar config JSON. Inference at runtime reads the vocab back
//!    so the country-head posterior decodes to ISO codes.
//!
//! 6. **Checkpoint** — `VarMap::save(path)` writes safetensors. A
//!    sidecar JSON next to the safetensors holds the [`ModelConfig`]
//!    plus the [`CountryVocab`] so [`load_model`] can reconstruct the
//!    architecture and decode country posteriors back to ISO codes.

use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};
use std::time::Instant;

use anyhow::{Context, Result, anyhow, bail};
use candle_core::{DType, Device, Tensor};
use candle_nn::{AdamW, Optimizer, ParamsAdamW, VarBuilder, VarMap, loss};
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::{Rng, SeedableRng};
use serde::{Deserialize, Serialize};

use super::tokenizer::ByteTokenizer;
use super::transformer::{
    BIO_B_HOUSE, BIO_B_LOCALITY, BIO_B_POSTCODE, BIO_B_STREET, BIO_I_HOUSE, BIO_I_LOCALITY,
    BIO_I_POSTCODE, BIO_I_STREET, BIO_O, ModelConfig, TaggerModel,
};

/// Compute device preference for training and inference.
///
/// `Auto` picks CUDA when the binary was compiled with the `cuda` feature
/// AND a GPU is available, otherwise falls back to CPU with a warning
/// (the user can force `Cpu` to silence the warning, or `Cuda` to
/// require a GPU).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum DevicePref {
    #[default]
    Auto,
    Cuda,
    Cpu,
}

impl DevicePref {
    /// Parse `auto|cuda|gpu|cpu` (case-insensitive). `gpu` is an alias
    /// for `cuda` so users used to other frameworks aren't surprised.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "auto" => Ok(Self::Auto),
            "cuda" | "gpu" => Ok(Self::Cuda),
            "cpu" => Ok(Self::Cpu),
            other => bail!("unknown device {:?} (use auto|cuda|cpu)", other),
        }
    }
}

/// Resolve a [`DevicePref`] to a concrete [`Device`].
///
/// `Cuda` errors loudly if the build wasn't compiled with the `cuda`
/// feature or if no CUDA device is reachable. `Auto` prefers CUDA when
/// available, falls back to CPU with a warning. `Cpu` always returns
/// [`Device::Cpu`].
pub fn select_device(pref: DevicePref) -> Result<Device> {
    match pref {
        DevicePref::Cpu => Ok(Device::Cpu),
        DevicePref::Cuda => {
            let dev = Device::new_cuda(0).map_err(|e| {
                anyhow!(
                    "CUDA device requested but not available: {e}. \
                     Build with `--features cuda` and ensure nvidia-smi works."
                )
            })?;
            eprintln!("[device] using CUDA device 0");
            Ok(dev)
        }
        DevicePref::Auto => match Device::new_cuda(0) {
            Ok(dev) => {
                eprintln!("[device] auto: using CUDA device 0");
                Ok(dev)
            }
            Err(e) => {
                eprintln!(
                    "[device] auto: CUDA unavailable ({e}); falling back to CPU. \
                     Pass --device cuda to require GPU, or --device cpu to silence this warning."
                );
                Ok(Device::Cpu)
            }
        },
    }
}

/// Country vocabulary — deterministic uppercase-ISO-2 → id mapping.
///
/// The id is the index into [`countries`] (which is sorted lexicographically
/// for determinism). The `Default` impl returns a single-country `["BE"]`
/// vocab — preserves the previous BE-only behaviour for callers that don't
/// need multi-country.
///
/// [`countries`]: CountryVocab::countries
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct CountryVocab {
    /// Uppercase ISO 3166-1 alpha-2 codes, lex-sorted.
    countries: Vec<String>,
}

impl Default for CountryVocab {
    fn default() -> Self {
        Self {
            countries: vec!["BE".to_string()],
        }
    }
}

impl CountryVocab {
    /// Build from a slice of ISO codes. Codes are uppercased, trimmed,
    /// deduplicated, and lex-sorted. Returns `Err` if any code isn't
    /// 2 ASCII alphabetic chars or if the slice is empty.
    pub fn new<S: AsRef<str>>(codes: &[S]) -> Result<Self> {
        if codes.is_empty() {
            bail!("country vocab must contain at least one country");
        }
        let mut out = Vec::with_capacity(codes.len());
        for c in codes {
            let s = c.as_ref().trim().to_ascii_uppercase();
            if s.len() != 2 || !s.bytes().all(|b| b.is_ascii_alphabetic()) {
                bail!(
                    "country code {:?} is not a 2-letter ISO 3166-1 alpha-2 code",
                    c.as_ref()
                );
            }
            if !out.contains(&s) {
                out.push(s);
            }
        }
        out.sort();
        Ok(Self { countries: out })
    }

    /// Parse a comma-separated list (`"BE,FR,NL,DE,US"`).
    pub fn from_csv(s: &str) -> Result<Self> {
        let parts: Vec<&str> = s
            .split(',')
            .map(str::trim)
            .filter(|p| !p.is_empty())
            .collect();
        Self::new(&parts)
    }

    /// Look up the id of an ISO code (case-insensitive). Returns
    /// `None` if the code isn't in the vocab.
    #[must_use]
    pub fn id_of(&self, iso2: &str) -> Option<u32> {
        let s = iso2.trim().to_ascii_uppercase();
        self.countries
            .iter()
            .position(|c| *c == s)
            .map(|i| i as u32)
    }

    /// ISO code at id `idx`, or `None` if out of range.
    #[must_use]
    pub fn iso_of(&self, idx: usize) -> Option<&str> {
        self.countries.get(idx).map(String::as_str)
    }

    /// All countries in the vocab, lex-sorted.
    #[must_use]
    pub fn countries(&self) -> &[String] {
        &self.countries
    }

    /// Number of countries (== `ModelConfig.n_countries`).
    #[must_use]
    pub fn len(&self) -> usize {
        self.countries.len()
    }

    /// Whether the vocab is empty. Always false for a vocab built via
    /// the constructors (they reject empty input).
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.countries.is_empty()
    }
}

/// Stored alongside the safetensors file so [`load_model`] can
/// reconstruct the architecture without hard-coding it. Filename:
/// `<model_path>.config.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointSidecar {
    config: ModelConfig,
    /// Country vocabulary used at training time. Defaults to a single-
    /// country `["BE"]` for backwards compatibility with older sidecars
    /// that pre-date the multi-country head.
    #[serde(default)]
    country_vocab: CountryVocab,
}

/// Field id literal in the corpus JSONL.
fn field_name_to_id(name: &str) -> Option<u8> {
    match name {
        "street" => Some(0),
        "house" | "housenumber" | "house_number" => Some(1),
        "postcode" | "zip" => Some(2),
        "locality" | "city" | "town" => Some(3),
        _ => None,
    }
}

fn b_label(field: u8) -> usize {
    match field {
        0 => BIO_B_STREET,
        1 => BIO_B_HOUSE,
        2 => BIO_B_POSTCODE,
        3 => BIO_B_LOCALITY,
        _ => BIO_O,
    }
}

fn i_label(field: u8) -> usize {
    match field {
        0 => BIO_I_STREET,
        1 => BIO_I_HOUSE,
        2 => BIO_I_POSTCODE,
        3 => BIO_I_LOCALITY,
        _ => BIO_O,
    }
}

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CorpusSpan {
    pub field: String,
    pub start: usize,
    pub end: usize,
}

/// One training example, in either of the two on-disk formats.
///
/// `spans` and `bio_labels` are mutually-exclusive on the JSONL side
/// but the in-memory struct carries both — we collapse to a normalized
/// form when read. If `bio_labels` is non-empty, it takes precedence
/// (it is exact byte-aligned supervision from corpus-gen). If empty,
/// `spans` is used.
#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CorpusExample {
    pub text: String,
    #[serde(default = "default_country")]
    pub country: String,
    #[serde(default)]
    pub spans: Vec<CorpusSpan>,
    /// Per-byte BIO labels (one byte per byte of `text`). Emitted by
    /// `geocode-training/corpus-gen`. Trainer scheme is 0=O,
    /// {1,2}=B/I-Street, {3,4}=B/I-Hnum, {5,6}=B/I-Post, {7,8}=B/I-City.
    /// corpus-gen also defines {9,10} for B/I-Unit; the trainer folds
    /// those down to O.
    #[serde(default)]
    pub bio_labels: Vec<u8>,
    /// Augmentation tag — `"canonical"`, `"drop_postcode"`, etc.
    /// Used by the trainer for ambiguity-aware country-loss masking.
    #[serde(default = "default_augmentation")]
    pub augmentation: String,
}

fn default_country() -> String {
    "BE".to_string()
}

fn default_augmentation() -> String {
    "canonical".to_string()
}

/// Augmentation kinds for which the country head can't be reliably
/// supervised — the source string is country-ambiguous (a missing
/// postcode could match BE/FR/NL/LU, etc).
///
/// Per the codex review (`geocode-research/CORPUS_DESIGN_NOTES.md`):
/// "don't force fake certainty on ambiguous positives".
#[must_use]
pub fn country_loss_weight_for(augmentation: &str) -> f32 {
    match augmentation {
        // Drop-field variants are country-ambiguous — supervising on
        // the country head injects noise.
        "drop_postcode" => 0.0,
        "drop_city" => 0.0,
        // All other variants are fully informative for the country
        // head (canonical, abbreviations, casing, whitespace, typos,
        // reorderings, canary cross-shard rewrites).
        _ => 1.0,
    }
}

/// `(token_ids, attention_mask, loss_mask, bio_labels, country_id)`
/// returned by [`example_to_tensors`].
///
/// - `attention_mask` is 1 for BOS, body bytes, and EOS, 0 for PAD —
///   the transformer needs to attend over BOS/EOS for sequence-level
///   pooling to be sensible.
/// - `loss_mask` is 1 ONLY for body bytes — BIO-tagging supervision
///   does not exist for the synthetic BOS/EOS positions, so they must
///   be excluded from CE loss and accuracy. Including them inflates
///   the metric and biases gradient updates toward "predict O at the
///   sentinels".
pub type ExampleTensors = (Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>, u32);

/// Map corpus-gen's BIO label byte (0..=10) onto the trainer's BIO label
/// (0..=8). Labels 0..=8 are identity. corpus-gen's UNIT B/I (9,10) collapse
/// to O — the trainer has no UNIT field. Anything else also folds to O so
/// a malformed corpus can't crash training; it just loses supervision on
/// that byte.
fn corpus_gen_to_trainer_bio(b: u8) -> u32 {
    match b {
        0..=8 => b as u32,
        // 9, 10 = B/I-Unit in corpus-gen → O in the trainer.
        _ => BIO_O as u32,
    }
}

/// Build per-byte trainer BIO labels from a corpus example. Prefers
/// `bio_labels` (corpus-gen format) over `spans` (legacy hand-authored
/// fixtures). Length of the returned vec equals `text.len()` bytes.
fn build_byte_labels(ex: &CorpusExample) -> Result<Vec<u32>> {
    let n_bytes = ex.text.len();
    if !ex.bio_labels.is_empty() {
        // corpus-gen guarantees one label per byte; enforce the
        // invariant rather than silently truncating.
        if ex.bio_labels.len() != n_bytes {
            bail!(
                "bio_labels length {} != text byte length {}",
                ex.bio_labels.len(),
                n_bytes
            );
        }
        return Ok(ex
            .bio_labels
            .iter()
            .map(|&b| corpus_gen_to_trainer_bio(b))
            .collect());
    }
    // Legacy spans format.
    let mut byte_labels = vec![BIO_O as u32; n_bytes];
    for span in &ex.spans {
        let Some(field) = field_name_to_id(&span.field) else {
            continue;
        };
        if span.end <= span.start || span.end > n_bytes {
            continue;
        }
        byte_labels[span.start] = b_label(field) as u32;
        for slot in byte_labels.iter_mut().take(span.end).skip(span.start + 1) {
            *slot = i_label(field) as u32;
        }
    }
    Ok(byte_labels)
}

/// Convert a corpus example to
/// (token_ids, attention_mask, loss_mask, bio_labels, country_id).
///
/// The BIO label tensor uses [`BIO_O`] for BOS, EOS, and pad positions
/// — the cross-entropy mask (the dedicated `loss_mask`, NOT the
/// attention `mask`) zeroes those out, see [`bio_loss_masked`].
pub fn example_to_tensors(
    ex: &CorpusExample,
    pad_to: usize,
    vocab: &CountryVocab,
) -> Result<ExampleTensors> {
    let (ids, attention_mask) = ByteTokenizer.encode_padded(&ex.text, pad_to);

    let byte_labels = build_byte_labels(ex)?;

    // Map back into token-stream positions: ids = [BOS, b0, b1, ..., EOS, PAD...]
    // BOS gets BIO_O, byte i gets byte_labels[i] (truncated by the tokenizer).
    let mut bio = vec![BIO_O as u32; ids.len()];
    let body_start = 1;
    let mut body_end = ids.len();
    for (i, &id) in ids.iter().enumerate().skip(1) {
        if id == super::tokenizer::EOS {
            body_end = i;
            break;
        }
    }
    let n_body = body_end.saturating_sub(body_start);
    let n_kept = n_body.min(byte_labels.len());
    bio[body_start..body_start + n_kept].copy_from_slice(&byte_labels[..n_kept]);

    // Loss mask: 1 only for body byte positions [body_start, body_end).
    // BOS at position 0, EOS at body_end, and any PAD beyond are zero.
    let mut loss_mask = vec![0u32; ids.len()];
    for slot in loss_mask
        .iter_mut()
        .take(body_end.min(ids.len()))
        .skip(body_start)
    {
        *slot = 1;
    }

    let country_id = vocab.id_of(&ex.country).ok_or_else(|| {
        anyhow!(
            "country {:?} is not in the trained country vocabulary {:?}",
            ex.country,
            vocab.countries()
        )
    })?;

    Ok((ids, attention_mask, loss_mask, bio, country_id))
}

/// Read a JSONL corpus file.
pub fn read_jsonl_corpus<P: AsRef<Path>>(path: P) -> Result<Vec<CorpusExample>> {
    let path = path.as_ref();
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let r = BufReader::new(f);
    let mut out = Vec::new();
    for (i, line) in r.lines().enumerate() {
        let line = line.with_context(|| format!("read line {i}"))?;
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let ex: CorpusExample =
            serde_json::from_str(line).with_context(|| format!("parse line {i}: {line}"))?;
        out.push(ex);
    }
    Ok(out)
}

/// Learning-rate schedule kind.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum LrSchedule {
    /// Linear warmup → cosine annealing to 0.
    Cosine,
    /// Linear warmup → linear decay to 0.
    Linear,
    /// Linear warmup → constant. Equivalent to a flat LR after warmup.
    Constant,
}

impl LrSchedule {
    /// Parse from a CLI string. Accepts `cosine`, `linear`, `constant`.
    pub fn parse(s: &str) -> Result<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "cosine" => Ok(Self::Cosine),
            "linear" => Ok(Self::Linear),
            "constant" => Ok(Self::Constant),
            _ => bail!("unknown lr schedule {:?} (use cosine|linear|constant)", s),
        }
    }
}

#[derive(Debug, Clone)]
pub struct TrainConfig {
    pub epochs: usize,
    pub batch_size: usize,
    pub learning_rate: f64,
    pub weight_decay: f64,
    pub bio_loss_weight: f32,
    pub country_loss_weight: f32,
    pub seed: u64,
    pub eval_split: f32,
    /// Max global gradient norm. Each step renormalizes the variables'
    /// gradients to at most this L2-norm. `None` disables clipping.
    pub gradient_clip: Option<f64>,
    /// Number of warmup steps (linear ramp from 0 → `learning_rate`).
    /// `0` disables warmup. Default 1000.
    pub warmup_steps: usize,
    /// Learning-rate schedule applied AFTER warmup.
    pub lr_schedule: LrSchedule,
    /// Device preference (auto/cuda/cpu).
    pub device_pref: DevicePref,
    /// Compute dtype. `F32` is the default; `BF16` enables mixed-
    /// precision training on Ada/Ampere/Hopper GPUs. Master weights
    /// stay in F32 inside `VarMap` either way; this only declares
    /// intent at the API surface (the trainer warns + pins to F32 if
    /// the model layers can't honour BF16 yet).
    pub dtype: DType,
    /// Optional wall-clock cap (seconds). When elapsed time exceeds
    /// this at the start of an epoch, the loop writes a checkpoint
    /// and exits early. `None` = unlimited.
    pub max_train_seconds: Option<u64>,
    /// Stop if eval_loss has not improved by `early_stop_min_delta` for
    /// this many consecutive epochs. `0` disables early stopping.
    pub early_stop_patience: usize,
    /// Minimum eval_loss improvement (lower is better) considered a
    /// "real" improvement for early stopping. Default 1e-3.
    pub early_stop_min_delta: f32,
    /// Append per-epoch JSONL telemetry to this path.
    pub metrics_out: Option<PathBuf>,
    /// Resume training from an existing safetensors checkpoint. The
    /// architecture must match.
    pub resume_from: Option<PathBuf>,
    /// Optimizer step count to start from when resuming (for the LR
    /// schedule). 0 if not resuming.
    pub resume_optimizer_step: usize,
}

impl Default for TrainConfig {
    fn default() -> Self {
        Self {
            epochs: 8,
            batch_size: 64,
            learning_rate: 1e-3,
            weight_decay: 0.01,
            bio_loss_weight: 1.0,
            country_loss_weight: 0.3,
            seed: 0xB17EBAD0,
            eval_split: 0.1,
            gradient_clip: Some(1.0),
            warmup_steps: 1000,
            lr_schedule: LrSchedule::Cosine,
            device_pref: DevicePref::Auto,
            dtype: DType::F32,
            max_train_seconds: None,
            early_stop_patience: 0,
            early_stop_min_delta: 1e-3,
            metrics_out: None,
            resume_from: None,
            resume_optimizer_step: 0,
        }
    }
}

/// Compute the LR for a given global step, given total steps + cfg.
///
/// Public so the train_cmd CLI plumbing can log the schedule profile
/// for observability.
#[must_use]
pub fn lr_at_step(cfg: &TrainConfig, step: usize, total_steps: usize) -> f64 {
    let lr_max = cfg.learning_rate;
    if cfg.warmup_steps > 0 && step < cfg.warmup_steps {
        // Linear warmup.
        let t = (step as f64 + 1.0) / cfg.warmup_steps as f64;
        return lr_max * t;
    }
    let post_warmup = step.saturating_sub(cfg.warmup_steps);
    let post_warmup_total = total_steps.saturating_sub(cfg.warmup_steps).max(1);
    let progress = (post_warmup as f64 / post_warmup_total as f64).clamp(0.0, 1.0);
    match cfg.lr_schedule {
        LrSchedule::Constant => lr_max,
        LrSchedule::Linear => lr_max * (1.0 - progress),
        LrSchedule::Cosine => {
            // Cosine annealing to 0.
            lr_max * 0.5 * (1.0 + (std::f64::consts::PI * progress).cos())
        }
    }
}

#[derive(Debug, Clone)]
pub struct EpochMetrics {
    pub epoch: usize,
    pub train_loss: f32,
    pub eval_loss: f32,
    pub eval_bio_acc: f32,
    pub eval_country_acc: f32,
    /// Wall-clock seconds elapsed since training started.
    pub wall_seconds_elapsed: f64,
    /// LR at the *start* of this epoch.
    pub lr_at_epoch_start: f64,
    /// `true` when eval_loss did not improve by
    /// `early_stop_min_delta` versus the best-so-far.
    pub plateau_signal: bool,
    /// Best eval_loss across all epochs up to and including this one.
    pub best_eval_loss: f32,
    /// Number of consecutive plateau epochs (resets on a real
    /// improvement). `early_stop_patience` triggers exit when this
    /// reaches the threshold.
    pub plateau_streak: usize,
}

/// Why the training loop exited.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StopReason {
    /// Reached `cfg.epochs`.
    EpochsCompleted,
    /// `cfg.max_train_seconds` exceeded.
    WallClockBudgetExhausted,
    /// `cfg.early_stop_patience` consecutive plateau epochs.
    EarlyStop,
}

/// Outcome of a training run.
#[derive(Debug, Clone)]
pub struct TrainOutcome {
    pub metrics: Vec<EpochMetrics>,
    pub stop_reason: StopReason,
    /// Total optimizer steps taken across this invocation.
    pub global_step_end: usize,
    /// Training set size after eval split.
    pub train_examples: usize,
}

/// Train the model and write weights + sidecar config to `out`.
///
/// The country vocab is plumbed through to:
/// - `cfg.n_countries` (must equal `vocab.len()`)
/// - the country head's output dimension (set by [`TaggerModel::new`])
/// - per-example country id encoding
/// - the safetensors sidecar (so inference can decode posteriors back
///   to ISO codes without a parallel config file)
pub fn train_and_save<P: AsRef<Path>>(
    cfg: ModelConfig,
    train_cfg: TrainConfig,
    vocab: &CountryVocab,
    corpus: &[CorpusExample],
    out: P,
) -> Result<Vec<EpochMetrics>> {
    let outcome = train_and_save_with_outcome(cfg, train_cfg, vocab, corpus, out)?;
    Ok(outcome.metrics)
}

/// Full training entry point with structured outcome (stop reason,
/// step count). The `train_and_save` wrapper is preserved for
/// backwards compatibility with the existing tests.
pub fn train_and_save_with_outcome<P: AsRef<Path>>(
    cfg: ModelConfig,
    train_cfg: TrainConfig,
    vocab: &CountryVocab,
    corpus: &[CorpusExample],
    out: P,
) -> Result<TrainOutcome> {
    let out = out.as_ref();
    if corpus.is_empty() {
        bail!("training corpus is empty");
    }
    cfg.validate().map_err(|e| anyhow!("invalid config: {e}"))?;
    if cfg.n_countries != vocab.len() {
        bail!(
            "ModelConfig.n_countries ({}) must equal CountryVocab len ({})",
            cfg.n_countries,
            vocab.len()
        );
    }

    let device = select_device(train_cfg.device_pref)?;
    if train_cfg.dtype != DType::F32 {
        eprintln!(
            "[train] WARN: dtype={:?} requested but TaggerModel currently runs F32 only — \
             pinning to F32 for this run.",
            train_cfg.dtype
        );
    }
    let master_dtype = DType::F32;
    let mut varmap = VarMap::new();
    let vb = VarBuilder::from_varmap(&varmap, master_dtype, &device);
    let model = TaggerModel::new(cfg, vb).map_err(|e| anyhow!("build model: {e}"))?;

    // Resume from checkpoint if requested. Loaded weights are mapped
    // into the existing varmap shapes (so the architecture must match).
    if let Some(ref resume_path) = train_cfg.resume_from {
        varmap
            .load(resume_path)
            .map_err(|e| anyhow!("loading resume checkpoint {}: {e}", resume_path.display()))?;
        eprintln!(
            "[train] resumed weights from {} (step offset = {})",
            resume_path.display(),
            train_cfg.resume_optimizer_step
        );
    }

    // Eval split.
    let mut rng = StdRng::seed_from_u64(train_cfg.seed);
    let mut idx: Vec<usize> = (0..corpus.len()).collect();
    idx.shuffle(&mut rng);
    let n_eval = ((corpus.len() as f32) * train_cfg.eval_split).round() as usize;
    let n_eval = n_eval.clamp(1, corpus.len().saturating_sub(1).max(1));
    let (eval_idx, train_idx) = idx.split_at(n_eval);
    let eval_idx: Vec<usize> = eval_idx.to_vec();
    let train_idx: Vec<usize> = train_idx.to_vec();
    if train_idx.is_empty() {
        bail!("training split is empty after eval split");
    }

    let pad_to = cfg.max_seq_len;
    let train_vars = varmap.all_vars();
    let mut opt = AdamW::new(
        train_vars.clone(),
        ParamsAdamW {
            lr: train_cfg.learning_rate,
            weight_decay: train_cfg.weight_decay,
            ..Default::default()
        },
    )
    .map_err(|e| anyhow!("adamw: {e}"))?;

    let mut metrics = Vec::with_capacity(train_cfg.epochs);

    let n_batches_per_epoch = train_idx.len().div_ceil(train_cfg.batch_size);
    // total_steps counts ALL steps planned for the LR schedule, including
    // any resume-offset previously consumed. This keeps the cosine
    // schedule continuous across resumes.
    let total_steps = (n_batches_per_epoch * train_cfg.epochs)
        .saturating_add(train_cfg.resume_optimizer_step)
        .max(1);
    let mut global_step: usize = train_cfg.resume_optimizer_step;

    // Open the JSONL telemetry file in append mode so resumes append to
    // the same log file rather than truncating prior chunks.
    let mut metrics_writer: Option<std::io::BufWriter<File>> =
        if let Some(ref p) = train_cfg.metrics_out {
            if let Some(parent) = p.parent() {
                std::fs::create_dir_all(parent).ok();
            }
            let f = OpenOptions::new()
                .create(true)
                .append(true)
                .open(p)
                .with_context(|| format!("opening metrics file {}", p.display()))?;
            Some(std::io::BufWriter::new(f))
        } else {
            None
        };

    eprintln!(
        "[train] device={:?} dtype={:?} vocab={} epochs={} batch={} steps_per_epoch={} total_steps={} lr_max={} schedule={:?} warmup={} weight_decay={} grad_clip={:?} max_seconds={:?} early_stop_patience={} resume_step={}",
        train_cfg.device_pref,
        train_cfg.dtype,
        vocab.countries().join(","),
        train_cfg.epochs,
        train_cfg.batch_size,
        n_batches_per_epoch,
        total_steps,
        train_cfg.learning_rate,
        train_cfg.lr_schedule,
        train_cfg.warmup_steps,
        train_cfg.weight_decay,
        train_cfg.gradient_clip,
        train_cfg.max_train_seconds,
        train_cfg.early_stop_patience,
        train_cfg.resume_optimizer_step,
    );

    let start = Instant::now();
    let mut best_eval_loss = f32::INFINITY;
    let mut plateau_streak: usize = 0;
    let mut stop_reason = StopReason::EpochsCompleted;

    for epoch in 0..train_cfg.epochs {
        // Wall-clock budget check at the *start* of each epoch — we
        // never abort mid-epoch (state would be inconsistent).
        if let Some(budget) = train_cfg.max_train_seconds
            && start.elapsed().as_secs_f64() >= budget as f64
        {
            eprintln!(
                "[train] wall-clock budget {}s exceeded at start of epoch {} — stopping early",
                budget, epoch
            );
            stop_reason = StopReason::WallClockBudgetExhausted;
            break;
        }

        let lr_at_epoch_start = lr_at_step(&train_cfg, global_step, total_steps);

        // Shuffle training indices each epoch.
        let mut order = train_idx.clone();
        order.shuffle(&mut rng);

        let mut train_loss_sum = 0.0_f32;
        let mut train_n = 0usize;
        for chunk in order.chunks(train_cfg.batch_size) {
            // Update the LR before stepping.
            let lr = lr_at_step(&train_cfg, global_step, total_steps);
            opt.set_learning_rate(lr);

            let batch: Vec<&CorpusExample> = chunk.iter().map(|&i| &corpus[i]).collect();
            let (ids_t, attn_t, loss_t, bio_t, country_t, country_loss_w_t) =
                build_batch(&batch, pad_to, vocab, &device)?;
            let (bio_logits, country_logits) = model
                .forward(&ids_t, &attn_t)
                .map_err(|e| anyhow!("forward: {e}"))?;
            // Loss mask excludes BOS/EOS; supervision is body-only.
            let l_bio = bio_loss_masked(&bio_logits, &bio_t, &loss_t)?;
            let l_country = country_loss_masked(&country_logits, &country_t, &country_loss_w_t)?;
            let l_bio_w = (l_bio * train_cfg.bio_loss_weight as f64)
                .map_err(|e| anyhow!("scale bio: {e}"))?;
            let l_country_w = (l_country * train_cfg.country_loss_weight as f64)
                .map_err(|e| anyhow!("scale country: {e}"))?;
            let total = (l_bio_w + l_country_w).map_err(|e| anyhow!("sum: {e}"))?;
            // Backward → optionally clip → step.
            let mut grads = total.backward().map_err(|e| anyhow!("backward: {e}"))?;
            if let Some(max_norm) = train_cfg.gradient_clip {
                clip_grads_in_place(&train_vars, &mut grads, max_norm)?;
            }
            opt.step(&grads).map_err(|e| anyhow!("opt step: {e}"))?;

            train_loss_sum += total
                .to_scalar::<f32>()
                .map_err(|e| anyhow!("scalar: {e}"))?;
            train_n += 1;
            global_step += 1;
        }

        let train_loss = if train_n > 0 {
            train_loss_sum / train_n as f32
        } else {
            f32::NAN
        };

        // Eval.
        let (eval_loss, eval_bio_acc, eval_country_acc, per_country_counts) = evaluate(
            &model,
            corpus,
            &eval_idx,
            pad_to,
            vocab,
            train_cfg.bio_loss_weight,
            train_cfg.country_loss_weight,
            &device,
        )?;

        let improved =
            eval_loss.is_finite() && (best_eval_loss - eval_loss) > train_cfg.early_stop_min_delta;
        if improved {
            best_eval_loss = eval_loss;
            plateau_streak = 0;
        } else if eval_loss.is_finite() {
            plateau_streak += 1;
        }
        let plateau_signal = !improved;

        let wall_seconds_elapsed = start.elapsed().as_secs_f64();
        let m = EpochMetrics {
            epoch,
            train_loss,
            eval_loss,
            eval_bio_acc,
            eval_country_acc,
            wall_seconds_elapsed,
            lr_at_epoch_start,
            plateau_signal,
            best_eval_loss,
            plateau_streak,
        };
        eprintln!(
            "epoch={} train_loss={:.4} eval_loss={:.4} bio_acc={:.4} country_acc={:.4} \
             wall={:.1}s lr={:.2e} plateau={} streak={} best_eval={:.4}",
            m.epoch,
            m.train_loss,
            m.eval_loss,
            m.eval_bio_acc,
            m.eval_country_acc,
            m.wall_seconds_elapsed,
            m.lr_at_epoch_start,
            m.plateau_signal,
            m.plateau_streak,
            m.best_eval_loss,
        );

        // JSONL telemetry — one row per epoch.
        if let Some(ref mut w) = metrics_writer {
            // Per-country bio_acc breakdown — keys are ISO codes
            // from the vocab, values are accuracy in [0, 1]. Empty
            // (no eval examples for that country) → 0.0.
            let mut per_country_bio = serde_json::Map::with_capacity(vocab.len());
            for ci in 0..vocab.len() {
                let total = per_country_counts.bio_total.get(ci).copied().unwrap_or(0);
                let correct = per_country_counts.bio_correct.get(ci).copied().unwrap_or(0);
                let acc = if total > 0 {
                    correct as f32 / total as f32
                } else {
                    0.0
                };
                let iso = vocab.iso_of(ci).unwrap_or("");
                per_country_bio.insert(
                    iso.to_string(),
                    serde_json::json!({"bio_acc": acc, "total": total}),
                );
            }
            let row = serde_json::json!({
                "epoch": m.epoch,
                "train_loss": m.train_loss,
                "eval_loss": m.eval_loss,
                "bio_acc": m.eval_bio_acc,
                "country_acc": m.eval_country_acc,
                "lr": m.lr_at_epoch_start,
                "wall_seconds_elapsed": m.wall_seconds_elapsed,
                "plateau_signal": m.plateau_signal,
                "plateau_streak": m.plateau_streak,
                "best_eval_loss": m.best_eval_loss,
                "global_step": global_step,
                "device": match &device {
                    candle_core::Device::Cpu => "cpu",
                    candle_core::Device::Cuda(_) => "cuda",
                    candle_core::Device::Metal(_) => "metal",
                },
                "device_pref": format!("{:?}", train_cfg.device_pref),
                "n_countries": cfg.n_countries,
                "d_model": cfg.d_model,
                "n_layers": cfg.n_layers,
                "per_country_bio_acc": per_country_bio,
            });
            writeln!(w, "{row}").context("writing metrics row")?;
            w.flush().ok();
        }
        metrics.push(m);

        if train_cfg.early_stop_patience > 0 && plateau_streak >= train_cfg.early_stop_patience {
            eprintln!(
                "[train] early stop: eval_loss has not improved by >{} for {} epochs (best={:.4})",
                train_cfg.early_stop_min_delta, plateau_streak, best_eval_loss
            );
            stop_reason = StopReason::EarlyStop;
            break;
        }
    }

    // Persist weights (always, even on early stop or budget exhaustion —
    // that's the whole point of chunked training).
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    varmap.save(out).map_err(|e| anyhow!("save weights: {e}"))?;

    // Sidecar config so loaders don't need to hardcode the architecture.
    let sidecar_path = sidecar_path_for(out);
    let sidecar = CheckpointSidecar {
        config: cfg,
        country_vocab: vocab.clone(),
    };
    let mut f = File::create(&sidecar_path)
        .with_context(|| format!("creating sidecar {}", sidecar_path.display()))?;
    f.write_all(serde_json::to_string_pretty(&sidecar)?.as_bytes())?;

    Ok(TrainOutcome {
        metrics,
        stop_reason,
        global_step_end: global_step,
        train_examples: train_idx.len(),
    })
}

/// Renormalize all per-variable gradients to a global L2-norm of at
/// most `max_norm`. Reads the gradient from `grads`, scales, and
/// re-inserts. No-op if the global norm is already below the threshold.
fn clip_grads_in_place(
    vars: &[candle_core::Var],
    grads: &mut candle_core::backprop::GradStore,
    max_norm: f64,
) -> Result<()> {
    // Compute global L2 norm.
    let mut sq_sum = 0.0f64;
    for v in vars {
        if let Some(g) = grads.get(v.as_tensor()) {
            let n = g
                .sqr()
                .map_err(|e| anyhow!("grad sqr: {e}"))?
                .sum_all()
                .map_err(|e| anyhow!("grad sum: {e}"))?
                .to_scalar::<f32>()
                .map_err(|e| anyhow!("grad scalar: {e}"))?;
            sq_sum += n as f64;
        }
    }
    let global_norm = sq_sum.sqrt();
    if !global_norm.is_finite() {
        bail!("non-finite gradient norm ({global_norm}) — training divergence");
    }
    if global_norm <= max_norm || global_norm == 0.0 {
        return Ok(());
    }
    let scale = max_norm / global_norm;
    for v in vars {
        let t = v.as_tensor();
        if let Some(g) = grads.get(t) {
            let scaled = (g * scale).map_err(|e| anyhow!("grad scale: {e}"))?;
            grads.insert(t, scaled);
        }
    }
    Ok(())
}

fn sidecar_path_for(model_path: &Path) -> std::path::PathBuf {
    let mut p = model_path.to_path_buf();
    let name = match p.file_name() {
        Some(n) => format!("{}.config.json", n.to_string_lossy()),
        None => "model.config.json".to_string(),
    };
    p.set_file_name(name);
    p
}

/// Load a previously saved model. Returns the model, its config, the
/// country vocabulary it was trained on, and the device.
///
/// Older sidecars (pre-multi-country) lack the `country_vocab` field;
/// the deserializer falls back to the [`CountryVocab::default`]
/// (`["BE"]`), preserving backwards compatibility with the shipped
/// `belgium-tiny.safetensors`.
pub fn load_model<P: AsRef<Path>>(
    path: P,
) -> Result<(TaggerModel, ModelConfig, CountryVocab, Device)> {
    load_model_on(path, DevicePref::Auto)
}

/// Same as [`load_model`] but with explicit device control. Inference
/// servers wire `--device` through here so a single binary can serve
/// CPU-only or GPU-backed traffic depending on deployment.
pub fn load_model_on<P: AsRef<Path>>(
    path: P,
    device_pref: DevicePref,
) -> Result<(TaggerModel, ModelConfig, CountryVocab, Device)> {
    let path = path.as_ref();
    let sidecar_path = sidecar_path_for(path);
    let sidecar_str = std::fs::read_to_string(&sidecar_path)
        .with_context(|| format!("reading sidecar at {}", sidecar_path.display()))?;
    let sidecar: CheckpointSidecar =
        serde_json::from_str(&sidecar_str).context("parsing sidecar config")?;
    let cfg = sidecar.config;
    let vocab = sidecar.country_vocab;

    let device = select_device(device_pref)?;
    let mut varmap = VarMap::new();
    // Build the model first so VarMap allocates the right shapes,
    // then load weights from disk.
    let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
    let model = TaggerModel::new(cfg, vb).map_err(|e| anyhow!("build model: {e}"))?;
    varmap
        .load(path)
        .map_err(|e| anyhow!("load weights: {e}"))?;
    Ok((model, cfg, vocab, device))
}

/// Build a batch tensor from references into the corpus.
///
/// Returns `(ids, attention_mask, loss_mask, bio, country, country_loss_w)`.
/// `attention_mask` is fed to the transformer; `loss_mask` excludes
/// BOS/EOS as well as PAD from BIO supervision; `country_loss_w` is a
/// per-example weight in `[0.0, 1.0]` that masks out country-head
/// supervision for ambiguous augmentations (drop-field variants).
#[allow(clippy::type_complexity)]
fn build_batch(
    batch: &[&CorpusExample],
    pad_to: usize,
    vocab: &CountryVocab,
    device: &Device,
) -> Result<(Tensor, Tensor, Tensor, Tensor, Tensor, Tensor)> {
    let b = batch.len();
    let mut ids_buf = Vec::with_capacity(b * pad_to);
    let mut attn_buf = Vec::with_capacity(b * pad_to);
    let mut loss_buf = Vec::with_capacity(b * pad_to);
    let mut bio_buf = Vec::with_capacity(b * pad_to);
    let mut country_buf = Vec::with_capacity(b);
    let mut country_w_buf = Vec::with_capacity(b);
    for ex in batch {
        let (ids, attn_mask, loss_mask, bio, country) = example_to_tensors(ex, pad_to, vocab)?;
        ids_buf.extend_from_slice(&ids);
        attn_buf.extend_from_slice(&attn_mask);
        loss_buf.extend_from_slice(&loss_mask);
        bio_buf.extend_from_slice(&bio);
        country_buf.push(country);
        country_w_buf.push(country_loss_weight_for(&ex.augmentation));
    }
    let ids_t =
        Tensor::from_vec(ids_buf, (b, pad_to), device).map_err(|e| anyhow!("ids tensor: {e}"))?;
    let attn_t = Tensor::from_vec(attn_buf, (b, pad_to), device)
        .map_err(|e| anyhow!("attention mask tensor: {e}"))?;
    let loss_t = Tensor::from_vec(loss_buf, (b, pad_to), device)
        .map_err(|e| anyhow!("loss mask tensor: {e}"))?;
    let bio_t =
        Tensor::from_vec(bio_buf, (b, pad_to), device).map_err(|e| anyhow!("bio tensor: {e}"))?;
    let country_t =
        Tensor::from_vec(country_buf, (b,), device).map_err(|e| anyhow!("country tensor: {e}"))?;
    let country_w_t = Tensor::from_vec(country_w_buf, (b,), device)
        .map_err(|e| anyhow!("country loss weight tensor: {e}"))?;
    Ok((ids_t, attn_t, loss_t, bio_t, country_t, country_w_t))
}

/// Cross-entropy loss over the country head with per-example masking.
///
/// `logits: (B, n_countries)`, `targets: (B,)`, `mask: (B,)` f32.
/// Computes mean CE over examples weighted by the mask. If the mask
/// is all zero (very rare — would mean the entire batch is drop-field
/// variants), the loss is zero with no gradient, which is correct.
fn country_loss_masked(logits: &Tensor, targets: &Tensor, mask: &Tensor) -> Result<Tensor> {
    let logp = candle_nn::ops::log_softmax(logits, candle_core::D::Minus1)
        .map_err(|e| anyhow!("country log_softmax: {e}"))?;
    let nll = logp
        .gather(&targets.unsqueeze(1).map_err(|e| anyhow!("unsq: {e}"))?, 1)
        .map_err(|e| anyhow!("gather: {e}"))?
        .squeeze(1)
        .map_err(|e| anyhow!("squeeze: {e}"))?
        .neg()
        .map_err(|e| anyhow!("neg: {e}"))?;
    let weighted = nll.mul(mask).map_err(|e| anyhow!("mul: {e}"))?;
    let total = weighted.sum_all().map_err(|e| anyhow!("sum: {e}"))?;
    let denom = mask
        .sum_all()
        .map_err(|e| anyhow!("sum mask: {e}"))?
        .clamp(1.0, f64::INFINITY)
        .map_err(|e| anyhow!("clamp: {e}"))?;
    total.div(&denom).map_err(|e| anyhow!("div: {e}"))
}

/// Cross-entropy loss over BIO with masking on padded tokens.
///
/// `bio_logits: (B, T, K)`, `bio_targets: (B, T)` u32, `mask: (B, T)` u32.
/// Computes mean CE over positions where mask==1.
fn bio_loss_masked(bio_logits: &Tensor, bio_targets: &Tensor, mask: &Tensor) -> Result<Tensor> {
    let (b, t, k) = bio_logits.dims3().map_err(|e| anyhow!("dims: {e}"))?;
    let logits_flat = bio_logits
        .reshape((b * t, k))
        .map_err(|e| anyhow!("reshape: {e}"))?;
    let targets_flat = bio_targets
        .reshape((b * t,))
        .map_err(|e| anyhow!("reshape t: {e}"))?;
    let mask_flat = mask
        .reshape((b * t,))
        .map_err(|e| anyhow!("reshape m: {e}"))?
        .to_dtype(DType::F32)
        .map_err(|e| anyhow!("dtype: {e}"))?;

    // Compute per-position CE, multiply by mask, sum, divide by sum(mask).
    let logp = candle_nn::ops::log_softmax(&logits_flat, candle_core::D::Minus1)
        .map_err(|e| anyhow!("logsoftmax: {e}"))?;
    // Gather along last axis at target indices: equivalent to NLL.
    let nll_per_pos = logp
        .gather(
            &targets_flat
                .unsqueeze(1)
                .map_err(|e| anyhow!("unsq: {e}"))?,
            1,
        )
        .map_err(|e| anyhow!("gather: {e}"))?
        .squeeze(1)
        .map_err(|e| anyhow!("squeeze: {e}"))?
        .neg()
        .map_err(|e| anyhow!("neg: {e}"))?;
    let masked = nll_per_pos
        .mul(&mask_flat)
        .map_err(|e| anyhow!("mul: {e}"))?;
    let total = masked.sum_all().map_err(|e| anyhow!("sum: {e}"))?;
    let denom = mask_flat.sum_all().map_err(|e| anyhow!("sum mask: {e}"))?;
    // safe divide; if all positions masked (impossible in practice) clamp to 1.
    let denom = denom
        .clamp(1.0, f64::INFINITY)
        .map_err(|e| anyhow!("clamp: {e}"))?;
    let loss = total.div(&denom).map_err(|e| anyhow!("div: {e}"))?;
    Ok(loss)
}

/// Per-country evaluation breakdown. `bio_correct[ci]` and
/// `bio_total[ci]` index by the same country id encoded in
/// `CountryVocab`. Returned alongside the aggregate metrics by
/// [`evaluate`] so the trainer can log per-country bio_acc to the
/// JSONL telemetry stream.
#[derive(Debug, Clone, Default)]
pub struct PerCountryEvalCounts {
    pub bio_correct: Vec<u64>,
    pub bio_total: Vec<u64>,
}

#[allow(clippy::too_many_arguments)]
fn evaluate(
    model: &TaggerModel,
    corpus: &[CorpusExample],
    eval_idx: &[usize],
    pad_to: usize,
    vocab: &CountryVocab,
    bio_w: f32,
    country_w: f32,
    device: &Device,
) -> Result<(f32, f32, f32, PerCountryEvalCounts)> {
    let n_countries = vocab.len();
    let mut per_country = PerCountryEvalCounts {
        bio_correct: vec![0; n_countries],
        bio_total: vec![0; n_countries],
    };
    if eval_idx.is_empty() {
        return Ok((f32::NAN, f32::NAN, f32::NAN, per_country));
    }
    let mut total_loss = 0.0_f32;
    let mut count = 0usize;
    let mut bio_correct = 0u64;
    let mut bio_total = 0u64;
    let mut country_correct = 0u64;
    let mut country_total = 0u64;

    // Batched eval — process EVAL_BATCH examples per forward to amortize
    // tensor construction + forward overhead. With 149k eval examples
    // (10% of a 1.5M-record corpus) the per-example loop took longer
    // than training itself; batching brings eval back in line with
    // training throughput.
    const EVAL_BATCH: usize = 128;
    for chunk in eval_idx.chunks(EVAL_BATCH) {
        let batch: Vec<&CorpusExample> = chunk.iter().map(|&i| &corpus[i]).collect();
        let (ids_t, attn_t, loss_t, bio_t, country_t, _country_w_t) =
            build_batch(&batch, pad_to, vocab, device)?;
        let (bio_logits, country_logits) = model
            .forward(&ids_t, &attn_t)
            .map_err(|e| anyhow!("eval forward: {e}"))?;
        let l_bio = bio_loss_masked(&bio_logits, &bio_t, &loss_t)?;
        // Eval uses unmasked country CE so the metric is comparable
        // across runs (eval set isn't filtered for ambiguity).
        let l_country = loss::cross_entropy(&country_logits, &country_t)
            .map_err(|e| anyhow!("eval country ce: {e}"))?;
        let total = (l_bio * bio_w as f64).map_err(|e| anyhow!("scale: {e}"))?
            + (l_country * country_w as f64).map_err(|e| anyhow!("scale: {e}"))?;
        let total = total.map_err(|e| anyhow!("sum: {e}"))?;
        total_loss += total
            .to_scalar::<f32>()
            .map_err(|e| anyhow!("scalar: {e}"))?;
        count += 1;

        // Argmax of BIO + country head. BIO accuracy is computed
        // against the loss mask (body bytes only), NOT the attention
        // mask — synthetic BOS/EOS positions have no ground-truth
        // tag so including them would inflate accuracy.
        let bio_argmax = bio_logits
            .argmax(candle_core::D::Minus1)
            .map_err(|e| anyhow!("argmax bio: {e}"))?
            .to_dtype(DType::U32)
            .map_err(|e| anyhow!("dtype: {e}"))?
            .to_vec2::<u32>()
            .map_err(|e| anyhow!("to_vec2: {e}"))?;
        let loss_v = loss_t
            .to_vec2::<u32>()
            .map_err(|e| anyhow!("loss mask to_vec2: {e}"))?;
        let bio_v = bio_t
            .to_vec2::<u32>()
            .map_err(|e| anyhow!("bio to_vec2: {e}"))?;
        // Per-row country id (same length as the eval batch). Used
        // to attribute BIO body-byte hits/misses to the source
        // country for the per-country breakdown.
        let row_country_v = country_t
            .to_vec1::<u32>()
            .map_err(|e| anyhow!("country target vec for bio breakdown: {e}"))?;
        for r in 0..bio_argmax.len() {
            let ci = row_country_v.get(r).copied().unwrap_or(0) as usize;
            for c in 0..bio_argmax[r].len() {
                if loss_v[r][c] == 1 {
                    bio_total += 1;
                    if let Some(slot) = per_country.bio_total.get_mut(ci) {
                        *slot += 1;
                    }
                    if bio_argmax[r][c] == bio_v[r][c] {
                        bio_correct += 1;
                        if let Some(slot) = per_country.bio_correct.get_mut(ci) {
                            *slot += 1;
                        }
                    }
                }
            }
        }
        let country_pred = country_logits
            .argmax(candle_core::D::Minus1)
            .map_err(|e| anyhow!("country argmax: {e}"))?
            .to_dtype(DType::U32)
            .map_err(|e| anyhow!("dtype: {e}"))?
            .to_vec1::<u32>()
            .map_err(|e| anyhow!("country vec: {e}"))?;
        let country_v = country_t
            .to_vec1::<u32>()
            .map_err(|e| anyhow!("country target vec: {e}"))?;
        for (p, t) in country_pred.iter().zip(country_v.iter()) {
            country_total += 1;
            if p == t {
                country_correct += 1;
            }
        }
    }

    let avg_loss = if count > 0 {
        total_loss / count as f32
    } else {
        f32::NAN
    };
    let bio_acc = if bio_total > 0 {
        bio_correct as f32 / bio_total as f32
    } else {
        f32::NAN
    };
    let country_acc = if country_total > 0 {
        country_correct as f32 / country_total as f32
    } else {
        f32::NAN
    };
    Ok((avg_loss, bio_acc, country_acc, per_country))
}

/// Belgium-only synthetic corpus generator.
///
/// Cartesian-product samples from small pools of streets/houses/postcodes/
/// localities, with optional reorderings (street first vs locality first)
/// and synthetic typos. **Not** the shard-agnostic augmentation strategy
/// from #96 §Tagger — that requires real OSM-derived structured data
/// and is filed as Phase 2 of #98.
pub fn generate_belgium_synthetic(n: usize, seed: u64) -> Vec<CorpusExample> {
    const STREETS: &[&str] = &[
        "Rue Wayez",
        "Rue de la Loi",
        "Avenue Louise",
        "Chaussee de Wavre",
        "Boulevard Anspach",
        "Rue Royale",
        "Grote Markt",
        "Meir",
        "Steenweg op Mol",
        "Vrijheidstraat",
        "Korenmarkt",
        "Sint-Pietersnieuwstraat",
    ];
    const LOCALITIES: &[(&str, &str)] = &[
        ("Anderlecht", "1070"),
        ("Bruxelles", "1000"),
        ("Brussel", "1000"),
        ("Antwerpen", "2000"),
        ("Gent", "9000"),
        ("Leuven", "3000"),
        ("Liege", "4000"),
        ("Namur", "5000"),
        ("Charleroi", "6000"),
        ("Mons", "7000"),
        ("Brugge", "8000"),
        ("Hasselt", "3500"),
    ];

    let mut rng = StdRng::seed_from_u64(seed);
    let mut out = Vec::with_capacity(n);
    for _ in 0..n {
        let s_idx = rng.random_range(0..STREETS.len());
        let l_idx = rng.random_range(0..LOCALITIES.len());
        let street = STREETS[s_idx];
        let (locality, postcode) = LOCALITIES[l_idx];
        let house: u32 = rng.random_range(1..=300);

        // 4 shapes: full forward / drop locality / drop postcode / locality-first
        let shape = rng.random_range(0..4);
        let (text, spans) = match shape {
            0 => {
                // "<street> <house> <postcode> <locality>"
                let mut t = String::new();
                let mut spans = Vec::new();
                let s_start = t.len();
                t.push_str(street);
                spans.push(CorpusSpan {
                    field: "street".into(),
                    start: s_start,
                    end: t.len(),
                });
                t.push(' ');
                let h_start = t.len();
                t.push_str(&house.to_string());
                spans.push(CorpusSpan {
                    field: "house".into(),
                    start: h_start,
                    end: t.len(),
                });
                t.push(' ');
                let p_start = t.len();
                t.push_str(postcode);
                spans.push(CorpusSpan {
                    field: "postcode".into(),
                    start: p_start,
                    end: t.len(),
                });
                t.push(' ');
                let l_start = t.len();
                t.push_str(locality);
                spans.push(CorpusSpan {
                    field: "locality".into(),
                    start: l_start,
                    end: t.len(),
                });
                (t, spans)
            }
            1 => {
                // "<street> <house> <postcode>" (no locality)
                let mut t = String::new();
                let mut spans = Vec::new();
                let s_start = t.len();
                t.push_str(street);
                spans.push(CorpusSpan {
                    field: "street".into(),
                    start: s_start,
                    end: t.len(),
                });
                t.push(' ');
                let h_start = t.len();
                t.push_str(&house.to_string());
                spans.push(CorpusSpan {
                    field: "house".into(),
                    start: h_start,
                    end: t.len(),
                });
                t.push(' ');
                let p_start = t.len();
                t.push_str(postcode);
                spans.push(CorpusSpan {
                    field: "postcode".into(),
                    start: p_start,
                    end: t.len(),
                });
                (t, spans)
            }
            2 => {
                // "<street> <house> <locality>" (no postcode)
                let mut t = String::new();
                let mut spans = Vec::new();
                let s_start = t.len();
                t.push_str(street);
                spans.push(CorpusSpan {
                    field: "street".into(),
                    start: s_start,
                    end: t.len(),
                });
                t.push(' ');
                let h_start = t.len();
                t.push_str(&house.to_string());
                spans.push(CorpusSpan {
                    field: "house".into(),
                    start: h_start,
                    end: t.len(),
                });
                t.push(' ');
                let l_start = t.len();
                t.push_str(locality);
                spans.push(CorpusSpan {
                    field: "locality".into(),
                    start: l_start,
                    end: t.len(),
                });
                (t, spans)
            }
            _ => {
                // "<postcode> <locality>, <street> <house>"
                let mut t = String::new();
                let mut spans = Vec::new();
                let p_start = t.len();
                t.push_str(postcode);
                spans.push(CorpusSpan {
                    field: "postcode".into(),
                    start: p_start,
                    end: t.len(),
                });
                t.push(' ');
                let l_start = t.len();
                t.push_str(locality);
                spans.push(CorpusSpan {
                    field: "locality".into(),
                    start: l_start,
                    end: t.len(),
                });
                t.push_str(", ");
                let s_start = t.len();
                t.push_str(street);
                spans.push(CorpusSpan {
                    field: "street".into(),
                    start: s_start,
                    end: t.len(),
                });
                t.push(' ');
                let h_start = t.len();
                t.push_str(&house.to_string());
                spans.push(CorpusSpan {
                    field: "house".into(),
                    start: h_start,
                    end: t.len(),
                });
                (t, spans)
            }
        };

        out.push(CorpusExample {
            text,
            country: "BE".to_string(),
            spans,
            bio_labels: Vec::new(),
            augmentation: "canonical".to_string(),
        });
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn synthetic_corpus_is_well_formed() {
        let corpus = generate_belgium_synthetic(50, 0xC0DE);
        assert_eq!(corpus.len(), 50);
        for ex in &corpus {
            assert!(!ex.spans.is_empty());
            for s in &ex.spans {
                assert!(s.start < s.end);
                assert!(s.end <= ex.text.len());
            }
        }
    }

    #[test]
    fn example_to_tensors_assigns_bio_correctly() {
        let ex = CorpusExample {
            text: "Rue X 12".to_string(),
            country: "BE".to_string(),
            spans: vec![
                CorpusSpan {
                    field: "street".into(),
                    start: 0,
                    end: 5,
                },
                CorpusSpan {
                    field: "house".into(),
                    start: 6,
                    end: 8,
                },
            ],
            bio_labels: Vec::new(),
            augmentation: "canonical".to_string(),
        };
        let vocab = CountryVocab::new(&["BE"]).unwrap();
        let (ids, _attn, loss_mask, bio, _) = example_to_tensors(&ex, 16, &vocab).unwrap();
        // BOS is index 0 → BIO_O.
        assert_eq!(bio[0] as usize, BIO_O);
        assert_eq!(bio[1] as usize, BIO_B_STREET); // 'R'
        assert_eq!(bio[2] as usize, BIO_I_STREET); // 'u'
        assert_eq!(bio[3] as usize, BIO_I_STREET); // 'e'
        assert_eq!(bio[4] as usize, BIO_I_STREET); // ' '
        assert_eq!(bio[5] as usize, BIO_I_STREET); // 'X'
        assert_eq!(bio[6] as usize, BIO_O); // ' '
        assert_eq!(bio[7] as usize, BIO_B_HOUSE); // '1'
        assert_eq!(bio[8] as usize, BIO_I_HOUSE); // '2'
        // Position 9 was where EOS lands → BIO_O.
        assert_eq!(ids[9], crate::tagger::tokenizer::EOS);
        assert_eq!(bio[9] as usize, BIO_O);
        // Loss mask must exclude BOS (idx 0), EOS (idx 9), and PAD
        // (10..16) — only body bytes (1..9) are supervised.
        assert_eq!(loss_mask[0], 0, "BOS must not contribute to loss");
        for (i, &m) in loss_mask.iter().enumerate().take(9).skip(1) {
            assert_eq!(m, 1, "body byte {i} must contribute to loss");
        }
        for (i, &m) in loss_mask.iter().enumerate().take(16).skip(9) {
            assert_eq!(m, 0, "EOS/PAD position {i} must not contribute to loss");
        }
    }

    #[test]
    fn training_loop_decreases_loss_on_synthetic() {
        let corpus = generate_belgium_synthetic(64, 0xBEEF);
        // Use a higher learning rate + no warmup for the smoke test —
        // the default warmup_steps=1000 dwarfs the 24 batches in this
        // tiny harness, leaving the model essentially untrained.
        let tcfg = TrainConfig {
            epochs: 3,
            batch_size: 8,
            learning_rate: 2e-3,
            warmup_steps: 0,
            lr_schedule: LrSchedule::Constant,
            gradient_clip: None,
            ..Default::default()
        };
        let dir = tempdir().unwrap();
        let out = dir.path().join("tiny.safetensors");
        let cfg = ModelConfig::tiny();
        let vocab = CountryVocab::new(&["BE"]).unwrap();
        let metrics = train_and_save(cfg, tcfg, &vocab, &corpus, &out).unwrap();
        assert_eq!(metrics.len(), 3);
        // Training loss must trend downward — last epoch < first epoch.
        assert!(
            metrics.last().unwrap().train_loss < metrics.first().unwrap().train_loss,
            "train loss did not decrease: {:?}",
            metrics
        );
        // Reload and run inference.
        let (model, _cfg, _vocab, device) = load_model(&out).unwrap();
        let infer_out =
            super::super::infer(&model, "Rue Wayez 122 1070 Anderlecht", &device).unwrap();
        assert!(!infer_out.bio_label_top1.is_empty());
    }

    #[test]
    fn country_vocab_round_trips_iso_codes() {
        let v = CountryVocab::new(&["be", "FR", "nl ", " DE", "us"]).unwrap();
        // Lex-sorted, uppercased, deduped.
        assert_eq!(v.countries(), &["BE", "DE", "FR", "NL", "US"]);
        assert_eq!(v.id_of("be"), Some(0));
        assert_eq!(v.id_of("FR"), Some(2));
        assert_eq!(v.id_of("XX"), None);
        assert_eq!(v.iso_of(2), Some("FR"));
        assert_eq!(v.iso_of(99), None);
        assert_eq!(v.len(), 5);
    }

    #[test]
    fn country_vocab_rejects_invalid_codes() {
        assert!(CountryVocab::new::<&str>(&[]).is_err());
        assert!(CountryVocab::new(&["BEL"]).is_err());
        assert!(CountryVocab::new(&["B1"]).is_err());
        assert!(CountryVocab::from_csv("").is_err());
        assert!(CountryVocab::from_csv("BE,").is_ok()); // trailing comma trimmed
    }

    #[test]
    fn country_vocab_csv_parsing() {
        let v = CountryVocab::from_csv("BE,FR,NL,DE,US").unwrap();
        assert_eq!(v.countries(), &["BE", "DE", "FR", "NL", "US"]);
    }

    #[test]
    fn corpus_gen_bio_format_is_decoded() {
        // "Rue X 12" — corpus-gen scheme:
        //   B-Street(1) I-Street(2) I-Street(2) O(0) B-Street(1) [oops, no — single-letter "X" is street]
        // Use realistic example: text "Rue 1" with bio_labels matching corpus-gen.
        // Corpus-gen scheme: 0=O, 1=B-Street, 2=I-Street, 3=B-Hnum, 4=I-Hnum
        let ex = CorpusExample {
            text: "Rue 1".to_string(),
            country: "BE".to_string(),
            spans: Vec::new(),
            bio_labels: vec![
                1, // B-Street 'R'
                2, // I-Street 'u'
                2, // I-Street 'e'
                0, // O ' '
                3, // B-Hnum '1'
            ],
            augmentation: "canonical".to_string(),
        };
        let vocab = CountryVocab::new(&["BE"]).unwrap();
        let (ids, _attn, _loss, bio, country) = example_to_tensors(&ex, 16, &vocab).unwrap();
        // Trainer scheme aligned 1:1 with corpus-gen 0..=8.
        assert_eq!(bio[0] as usize, BIO_O); // BOS
        assert_eq!(bio[1] as usize, BIO_B_STREET);
        assert_eq!(bio[2] as usize, BIO_I_STREET);
        assert_eq!(bio[3] as usize, BIO_I_STREET);
        assert_eq!(bio[4] as usize, BIO_O);
        assert_eq!(bio[5] as usize, BIO_B_HOUSE);
        // EOS at position 6.
        assert_eq!(ids[6], crate::tagger::tokenizer::EOS);
        assert_eq!(country, 0);
    }

    #[test]
    fn corpus_gen_unit_labels_fold_to_o() {
        // corpus-gen UNIT (9, 10) collapses to O in the trainer.
        let ex = CorpusExample {
            text: "ab".to_string(),
            country: "BE".to_string(),
            spans: Vec::new(),
            bio_labels: vec![9, 10],
            augmentation: "canonical".to_string(),
        };
        let vocab = CountryVocab::new(&["BE"]).unwrap();
        let (_ids, _attn, _loss, bio, _) = example_to_tensors(&ex, 16, &vocab).unwrap();
        assert_eq!(bio[1] as usize, BIO_O); // 'a' (was UNIT B → O)
        assert_eq!(bio[2] as usize, BIO_O); // 'b' (was UNIT I → O)
    }

    #[test]
    fn bio_labels_length_mismatch_is_rejected() {
        let ex = CorpusExample {
            text: "Rue 1".to_string(),
            country: "BE".to_string(),
            spans: Vec::new(),
            bio_labels: vec![1, 2, 0], // length 3, text is 5 bytes
            augmentation: "canonical".to_string(),
        };
        let vocab = CountryVocab::new(&["BE"]).unwrap();
        assert!(example_to_tensors(&ex, 16, &vocab).is_err());
    }

    #[test]
    fn unknown_country_in_corpus_is_rejected() {
        let ex = CorpusExample {
            text: "abc".to_string(),
            country: "XX".to_string(),
            spans: Vec::new(),
            bio_labels: vec![0, 0, 0],
            augmentation: "canonical".to_string(),
        };
        let vocab = CountryVocab::new(&["BE", "FR"]).unwrap();
        assert!(example_to_tensors(&ex, 16, &vocab).is_err());
    }

    #[test]
    fn jsonl_corpus_reads_both_formats() {
        use std::io::Write;
        let dir = tempdir().unwrap();
        let path = dir.path().join("mixed.jsonl");
        let mut f = File::create(&path).unwrap();
        // bio_labels variant.
        writeln!(
            f,
            r#"{{"text":"Rue 1","country":"BE","bio_labels":[1,2,2,0,3]}}"#
        )
        .unwrap();
        // spans variant (legacy).
        writeln!(
            f,
            r#"{{"text":"Rue 1","country":"BE","spans":[{{"field":"street","start":0,"end":3}},{{"field":"house","start":4,"end":5}}]}}"#
        )
        .unwrap();
        drop(f);
        let corpus = read_jsonl_corpus(&path).unwrap();
        assert_eq!(corpus.len(), 2);
        assert_eq!(corpus[0].bio_labels.len(), 5);
        assert!(corpus[0].spans.is_empty());
        assert!(corpus[1].bio_labels.is_empty());
        assert_eq!(corpus[1].spans.len(), 2);
    }

    #[test]
    fn drop_postcode_zeros_country_loss_weight() {
        assert_eq!(country_loss_weight_for("canonical"), 1.0);
        assert_eq!(country_loss_weight_for("drop_postcode"), 0.0);
        assert_eq!(country_loss_weight_for("drop_city"), 0.0);
        assert_eq!(country_loss_weight_for("typo_injection"), 1.0);
        assert_eq!(country_loss_weight_for("case_upper"), 1.0);
    }

    #[test]
    fn lr_schedule_warmup_then_anneal() {
        let cfg = TrainConfig {
            learning_rate: 1.0,
            warmup_steps: 100,
            lr_schedule: LrSchedule::Cosine,
            ..Default::default()
        };
        // During warmup: linear ramp.
        let lr0 = lr_at_step(&cfg, 0, 1000);
        assert!(lr0 > 0.0 && lr0 < 0.05);
        let lr50 = lr_at_step(&cfg, 49, 1000);
        assert!((lr50 - 0.5).abs() < 0.02, "lr50 = {lr50}");
        // After warmup: cosine descends.
        let lr_post = lr_at_step(&cfg, 100, 1000);
        assert!(
            (lr_post - 1.0).abs() < 1e-6,
            "should peak right at end of warmup, got {lr_post}"
        );
        let lr_end = lr_at_step(&cfg, 1000, 1000);
        assert!(lr_end < 1e-6, "should anneal to ~0, got {lr_end}");
        // Constant schedule stays flat.
        let cfg2 = TrainConfig {
            lr_schedule: LrSchedule::Constant,
            ..cfg
        };
        assert!((lr_at_step(&cfg2, 500, 1000) - 1.0).abs() < 1e-6);
    }

    #[test]
    fn lr_schedule_parses() {
        assert_eq!(LrSchedule::parse("cosine").unwrap(), LrSchedule::Cosine);
        assert_eq!(LrSchedule::parse("LINEAR").unwrap(), LrSchedule::Linear);
        assert_eq!(LrSchedule::parse("constant").unwrap(), LrSchedule::Constant);
        assert!(LrSchedule::parse("bogus").is_err());
    }

    #[test]
    fn sidecar_round_trips_country_vocab() {
        let corpus = generate_belgium_synthetic(32, 0xCAFE);
        let tcfg = TrainConfig {
            epochs: 1,
            batch_size: 8,
            warmup_steps: 0,
            lr_schedule: LrSchedule::Constant,
            ..Default::default()
        };
        let dir = tempdir().unwrap();
        let out = dir.path().join("rt.safetensors");
        let vocab = CountryVocab::new(&["BE"]).unwrap();
        let cfg = ModelConfig::tiny();
        train_and_save(cfg, tcfg, &vocab, &corpus, &out).unwrap();
        // Sidecar contains the vocab.
        let (_model, cfg2, vocab2, _device) = load_model(&out).unwrap();
        assert_eq!(cfg2.n_countries, 1);
        assert_eq!(vocab2.countries(), vocab.countries());
    }
}
