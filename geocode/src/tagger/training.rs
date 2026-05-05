//! Training loop for the byte-level tagger (#96 §Tagger, #98 Phase 2 prerequisite).
//!
//! ## Pipeline
//!
//! 1. **Corpus** — JSONL, one example per line. Format:
//!    ```json
//!    {
//!      "text": "Rue Wayez 122 1070 Anderlecht",
//!      "country": "BE",
//!      "spans": [
//!        {"field": "street",   "start": 0,  "end": 9},
//!        {"field": "house",    "start": 10, "end": 13},
//!        {"field": "postcode", "start": 14, "end": 18},
//!        {"field": "locality", "start": 19, "end": 29}
//!      ]
//!    }
//!    ```
//!    `start`/`end` are byte offsets into `text`. Each example is
//!    converted to a per-byte BIO label sequence at training time.
//!
//! 2. **Synthetic generator** — when no corpus is provided, the
//!    [`generate_belgium_synthetic`] function emits N synthetic Belgium
//!    addresses by sampling Cartesian products of street/house/postcode/
//!    locality pools. This is the proof-of-life corpus shipped with
//!    the tiny model. **Not** the shard-agnostic augmentation strategy
//!    from #96 §Tagger — that is filed as Phase 2 of #98.
//!
//! 3. **Loss** — weighted CE on BIO + CE on country. Default weights
//!    `bio=1.0, country=0.3` reflect the relative supervision strength
//!    (BIO is per-token, country is per-sequence — country head needs
//!    less weight).
//!
//! 4. **Optimizer** — AdamW from `candle_nn::AdamW`. Default lr=1e-3,
//!    weight_decay=1e-4.
//!
//! 5. **Checkpoint** — `VarMap::save(path)` writes safetensors. A
//!    sidecar JSON next to the safetensors holds the [`ModelConfig`]
//!    so [`load_model`] can reconstruct the architecture.

use std::fs::File;
use std::io::{BufRead, BufReader, Write};
use std::path::Path;

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

/// Stored alongside the safetensors file so [`load_model`] can
/// reconstruct the architecture without hard-coding it. Filename:
/// `<model_path>.config.json`.
#[derive(Debug, Clone, Serialize, Deserialize)]
struct CheckpointSidecar {
    config: ModelConfig,
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

#[derive(Debug, Clone, Deserialize, Serialize)]
pub struct CorpusExample {
    pub text: String,
    #[serde(default = "default_country")]
    pub country: String,
    #[serde(default)]
    pub spans: Vec<CorpusSpan>,
}

fn default_country() -> String {
    "BE".to_string()
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

/// Convert a corpus example to
/// (token_ids, attention_mask, loss_mask, bio_labels, country_id).
///
/// The BIO label tensor uses [`BIO_O`] for BOS, EOS, and pad positions
/// — the cross-entropy mask (the dedicated `loss_mask`, NOT the
/// attention `mask`) zeroes those out, see [`bio_loss_masked`].
pub fn example_to_tensors(
    ex: &CorpusExample,
    pad_to: usize,
    n_countries: usize,
) -> Result<ExampleTensors> {
    let (ids, attention_mask) = ByteTokenizer.encode_padded(&ex.text, pad_to);

    // Build per-byte BIO labels for the original text.
    let n_bytes = ex.text.len();
    let mut byte_labels = vec![BIO_O; n_bytes];
    for span in &ex.spans {
        let Some(field) = field_name_to_id(&span.field) else {
            continue;
        };
        if span.end <= span.start || span.end > n_bytes {
            continue;
        }
        byte_labels[span.start] = b_label(field);
        for slot in byte_labels.iter_mut().take(span.end).skip(span.start + 1) {
            *slot = i_label(field);
        }
    }

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
    for i in 0..n_kept {
        bio[body_start + i] = byte_labels[i] as u32;
    }

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

    let country_id = match ex.country.to_uppercase().as_str() {
        "BE" => 0,
        _ => 0, // MVP only ships BE; multi-country is #96
    };
    if (country_id as usize) >= n_countries {
        bail!("country id {country_id} out of range for n_countries={n_countries}");
    }

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

#[derive(Debug, Clone, Copy)]
pub struct TrainConfig {
    pub epochs: usize,
    pub batch_size: usize,
    pub learning_rate: f64,
    pub weight_decay: f64,
    pub bio_loss_weight: f32,
    pub country_loss_weight: f32,
    pub seed: u64,
    pub eval_split: f32,
}

impl Default for TrainConfig {
    fn default() -> Self {
        Self {
            epochs: 8,
            batch_size: 16,
            learning_rate: 2e-3,
            weight_decay: 1e-4,
            bio_loss_weight: 1.0,
            country_loss_weight: 0.3,
            seed: 0xB17EBAD0,
            eval_split: 0.1,
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
}

/// Train the model and write weights + sidecar config to `out`.
pub fn train_and_save<P: AsRef<Path>>(
    cfg: ModelConfig,
    train_cfg: TrainConfig,
    corpus: &[CorpusExample],
    out: P,
) -> Result<Vec<EpochMetrics>> {
    let out = out.as_ref();
    if corpus.is_empty() {
        bail!("training corpus is empty");
    }
    cfg.validate().map_err(|e| anyhow!("invalid config: {e}"))?;

    let device = Device::Cpu;
    let varmap = VarMap::new();
    let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
    let model = TaggerModel::new(cfg, vb).map_err(|e| anyhow!("build model: {e}"))?;

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
    let mut opt = AdamW::new(
        varmap.all_vars(),
        ParamsAdamW {
            lr: train_cfg.learning_rate,
            weight_decay: train_cfg.weight_decay,
            ..Default::default()
        },
    )
    .map_err(|e| anyhow!("adamw: {e}"))?;

    let mut metrics = Vec::with_capacity(train_cfg.epochs);

    for epoch in 0..train_cfg.epochs {
        // Shuffle training indices each epoch.
        let mut order = train_idx.clone();
        order.shuffle(&mut rng);

        let mut train_loss_sum = 0.0_f32;
        let mut train_n = 0usize;
        for chunk in order.chunks(train_cfg.batch_size) {
            let batch: Vec<&CorpusExample> = chunk.iter().map(|&i| &corpus[i]).collect();
            let (ids_t, attn_t, loss_t, bio_t, country_t) =
                build_batch(&batch, pad_to, cfg.n_countries, &device)?;
            let (bio_logits, country_logits) = model
                .forward(&ids_t, &attn_t)
                .map_err(|e| anyhow!("forward: {e}"))?;
            // Loss mask excludes BOS/EOS; supervision is body-only.
            let l_bio = bio_loss_masked(&bio_logits, &bio_t, &loss_t)?;
            let l_country = loss::cross_entropy(&country_logits, &country_t)
                .map_err(|e| anyhow!("country ce: {e}"))?;
            let l_bio_w = (l_bio * train_cfg.bio_loss_weight as f64)
                .map_err(|e| anyhow!("scale bio: {e}"))?;
            let l_country_w = (l_country * train_cfg.country_loss_weight as f64)
                .map_err(|e| anyhow!("scale country: {e}"))?;
            let total = (l_bio_w + l_country_w).map_err(|e| anyhow!("sum: {e}"))?;
            opt.backward_step(&total)
                .map_err(|e| anyhow!("backward: {e}"))?;
            train_loss_sum += total
                .to_scalar::<f32>()
                .map_err(|e| anyhow!("scalar: {e}"))?;
            train_n += 1;
        }

        let train_loss = if train_n > 0 {
            train_loss_sum / train_n as f32
        } else {
            f32::NAN
        };

        // Eval.
        let (eval_loss, eval_bio_acc, eval_country_acc) = evaluate(
            &model,
            corpus,
            &eval_idx,
            pad_to,
            cfg.n_countries,
            train_cfg.bio_loss_weight,
            train_cfg.country_loss_weight,
            &device,
        )?;

        let m = EpochMetrics {
            epoch,
            train_loss,
            eval_loss,
            eval_bio_acc,
            eval_country_acc,
        };
        eprintln!(
            "epoch={} train_loss={:.4} eval_loss={:.4} bio_acc={:.4} country_acc={:.4}",
            m.epoch, m.train_loss, m.eval_loss, m.eval_bio_acc, m.eval_country_acc
        );
        metrics.push(m);
    }

    // Persist weights.
    if let Some(parent) = out.parent() {
        std::fs::create_dir_all(parent).ok();
    }
    varmap.save(out).map_err(|e| anyhow!("save weights: {e}"))?;

    // Sidecar config so loaders don't need to hardcode the architecture.
    let sidecar_path = sidecar_path_for(out);
    let sidecar = CheckpointSidecar { config: cfg };
    let mut f = File::create(&sidecar_path)
        .with_context(|| format!("creating sidecar {}", sidecar_path.display()))?;
    f.write_all(serde_json::to_string_pretty(&sidecar)?.as_bytes())?;

    Ok(metrics)
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

/// Load a previously saved model. Returns the model and its config.
pub fn load_model<P: AsRef<Path>>(path: P) -> Result<(TaggerModel, ModelConfig, Device)> {
    let path = path.as_ref();
    let sidecar_path = sidecar_path_for(path);
    let sidecar_str = std::fs::read_to_string(&sidecar_path)
        .with_context(|| format!("reading sidecar at {}", sidecar_path.display()))?;
    let sidecar: CheckpointSidecar =
        serde_json::from_str(&sidecar_str).context("parsing sidecar config")?;
    let cfg = sidecar.config;

    let device = Device::Cpu;
    let mut varmap = VarMap::new();
    // Build the model first so VarMap allocates the right shapes,
    // then load weights from disk.
    let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
    let model = TaggerModel::new(cfg, vb).map_err(|e| anyhow!("build model: {e}"))?;
    varmap
        .load(path)
        .map_err(|e| anyhow!("load weights: {e}"))?;
    Ok((model, cfg, device))
}

/// Build a batch tensor from references into the corpus.
///
/// Returns `(ids, attention_mask, loss_mask, bio, country)`.
/// `attention_mask` is fed to the transformer; `loss_mask` excludes
/// BOS/EOS as well as PAD from supervision.
fn build_batch(
    batch: &[&CorpusExample],
    pad_to: usize,
    n_countries: usize,
    device: &Device,
) -> Result<(Tensor, Tensor, Tensor, Tensor, Tensor)> {
    let b = batch.len();
    let mut ids_buf = Vec::with_capacity(b * pad_to);
    let mut attn_buf = Vec::with_capacity(b * pad_to);
    let mut loss_buf = Vec::with_capacity(b * pad_to);
    let mut bio_buf = Vec::with_capacity(b * pad_to);
    let mut country_buf = Vec::with_capacity(b);
    for ex in batch {
        let (ids, attn_mask, loss_mask, bio, country) =
            example_to_tensors(ex, pad_to, n_countries)?;
        ids_buf.extend_from_slice(&ids);
        attn_buf.extend_from_slice(&attn_mask);
        loss_buf.extend_from_slice(&loss_mask);
        bio_buf.extend_from_slice(&bio);
        country_buf.push(country);
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
    Ok((ids_t, attn_t, loss_t, bio_t, country_t))
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

#[allow(clippy::too_many_arguments)]
fn evaluate(
    model: &TaggerModel,
    corpus: &[CorpusExample],
    eval_idx: &[usize],
    pad_to: usize,
    n_countries: usize,
    bio_w: f32,
    country_w: f32,
    device: &Device,
) -> Result<(f32, f32, f32)> {
    if eval_idx.is_empty() {
        return Ok((f32::NAN, f32::NAN, f32::NAN));
    }
    let mut total_loss = 0.0_f32;
    let mut count = 0usize;
    let mut bio_correct = 0u64;
    let mut bio_total = 0u64;
    let mut country_correct = 0u64;
    let mut country_total = 0u64;

    for &i in eval_idx {
        let ex = &corpus[i];
        let batch = vec![ex];
        let (ids_t, attn_t, loss_t, bio_t, country_t) =
            build_batch(&batch, pad_to, n_countries, device)?;
        let (bio_logits, country_logits) = model
            .forward(&ids_t, &attn_t)
            .map_err(|e| anyhow!("eval forward: {e}"))?;
        let l_bio = bio_loss_masked(&bio_logits, &bio_t, &loss_t)?;
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
        for r in 0..bio_argmax.len() {
            for c in 0..bio_argmax[r].len() {
                if loss_v[r][c] == 1 {
                    bio_total += 1;
                    if bio_argmax[r][c] == bio_v[r][c] {
                        bio_correct += 1;
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
    Ok((avg_loss, bio_acc, country_acc))
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
        };
        let (ids, _attn, loss_mask, bio, _) = example_to_tensors(&ex, 16, 1).unwrap();
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
        let tcfg = TrainConfig {
            epochs: 3,
            batch_size: 8,
            ..Default::default()
        };
        let dir = tempdir().unwrap();
        let out = dir.path().join("tiny.safetensors");
        let cfg = ModelConfig::tiny();
        let metrics = train_and_save(cfg, tcfg, &corpus, &out).unwrap();
        assert_eq!(metrics.len(), 3);
        // Training loss must trend downward — last epoch < first epoch.
        assert!(
            metrics.last().unwrap().train_loss < metrics.first().unwrap().train_loss,
            "train loss did not decrease: {:?}",
            metrics
        );
        // Reload and run inference.
        let (model, _cfg, device) = load_model(&out).unwrap();
        let infer_out =
            super::super::infer(&model, "Rue Wayez 122 1070 Anderlecht", &device).unwrap();
        assert!(!infer_out.bio_label_top1.is_empty());
    }
}
