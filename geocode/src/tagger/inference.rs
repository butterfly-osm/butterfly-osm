//! Inference path: tokenize → forward → BIO span extraction +
//! country posterior softmax.
//!
//! ## Output shape
//!
//! [`InferenceOutput`] is the structured form the parser wraps into a
//! [`crate::types::ParsedQuery`]. It contains:
//!
//! - per-position `bio_label_top1` (the argmax label) — used for
//!   span extraction
//! - per-position `bio_logprobs` (full distribution over BIO labels)
//!   — used by [`crate::parser::decoding`] for adaptive beam width
//!   and entropy estimation
//! - `country_posterior` — softmax over `n_countries`. The cheap
//!   classifier in [`crate::routing::classifier`] is the prior; this
//!   is the model's refinement.
//!
//! ## Span extraction
//!
//! Standard BIO decoding:
//!
//! - A `B-X` opens a new span on field X.
//! - An `I-X` extends the currently open span if and only if the
//!   currently open span has the same field X. Otherwise the I- is
//!   demoted to start-of-span (this is consistent with the relaxed
//!   "BIO/IOB1" decoder used in production NER libraries — strict
//!   BIO would drop these tokens, which silently loses recall).
//! - `O` closes any open span.

use anyhow::{Context, Result, anyhow};
use candle_core::{DType, Device, Tensor};
use candle_nn::ops::softmax_last_dim;

use super::tokenizer::{ByteTokenizer, EOS};
use super::transformer::{NUM_BIO_LABELS, TaggerModel, bio_to_field, is_b};

/// One BIO span extracted from the model output.
#[derive(Debug, Clone)]
pub struct BioSpan {
    /// Field id: 0=street, 1=house, 2=postcode, 3=locality.
    pub field: u8,
    /// Byte range in the **input string** (not the token stream).
    /// BOS/EOS are stripped before mapping back.
    pub byte_range: std::ops::Range<usize>,
    /// Mean per-byte probability of the assigned BIO label across the span.
    /// Used by [`crate::parser::decoding`] for anchor confidence.
    pub mean_label_prob: f32,
    /// Extracted text content (UTF-8). May lose trailing/leading
    /// whitespace stripped from the input bytes inside the span.
    pub text: String,
}

/// Output of a single inference call.
#[derive(Debug, Clone)]
pub struct InferenceOutput {
    /// Per-input-byte argmax BIO label.
    pub bio_label_top1: Vec<usize>,
    /// Per-input-byte log-probabilities over BIO labels (length = bytes,
    /// inner length = NUM_BIO_LABELS). Outer index is byte position
    /// in the input (BOS/EOS stripped).
    pub bio_logprobs: Vec<[f32; NUM_BIO_LABELS]>,
    /// Country posterior (softmax). Index by country id.
    pub country_posterior: Vec<f32>,
    /// Decoded BIO spans, one per field occurrence.
    pub spans: Vec<BioSpan>,
    /// Per-position entropy of the BIO distribution. Bytes only
    /// (BOS/EOS stripped). Used by [`crate::parser::decoding`] for
    /// adaptive beam width.
    pub entropy_per_byte: Vec<f32>,
}

/// Run the model on a single string.
pub fn infer(model: &TaggerModel, text: &str, device: &Device) -> Result<InferenceOutput> {
    let cfg = &model.config;
    let pad_to = cfg.max_seq_len;
    let (ids, mask) = ByteTokenizer.encode_padded(text, pad_to);
    let n = ids.len();

    let ids_t = Tensor::from_vec(ids.clone(), (1, n), device).context("creating ids tensor")?;
    let mask_t = Tensor::from_vec(mask.clone(), (1, n), device).context("creating mask tensor")?;

    let (bio_logits, country_logits) = model
        .forward(&ids_t, &mask_t)
        .map_err(|e| anyhow!("forward pass failed: {e}"))?;

    let bio_logprobs_t = candle_nn::ops::log_softmax(&bio_logits, candle_core::D::Minus1)
        .map_err(|e| anyhow!("log_softmax failed: {e}"))?;
    let bio_probs_t = softmax_last_dim(&bio_logits).map_err(|e| anyhow!("softmax failed: {e}"))?;
    let country_probs_t =
        softmax_last_dim(&country_logits).map_err(|e| anyhow!("country softmax failed: {e}"))?;

    let bio_logprobs_v = bio_logprobs_t
        .to_dtype(DType::F32)
        .map_err(|e| anyhow!("dtype: {e}"))?
        .to_vec3::<f32>()
        .map_err(|e| anyhow!("to_vec3: {e}"))?;
    let bio_probs_v = bio_probs_t
        .to_dtype(DType::F32)
        .map_err(|e| anyhow!("dtype: {e}"))?
        .to_vec3::<f32>()
        .map_err(|e| anyhow!("to_vec3: {e}"))?;
    let country_v = country_probs_t
        .to_dtype(DType::F32)
        .map_err(|e| anyhow!("dtype: {e}"))?
        .to_vec2::<f32>()
        .map_err(|e| anyhow!("to_vec2: {e}"))?;

    let bio_logprobs_seq = &bio_logprobs_v[0]; // (T, K)
    let bio_probs_seq = &bio_probs_v[0];

    // Strip BOS at index 0 and EOS at the position where ids[i] == EOS
    // (or at end if no EOS visible due to truncation). The token stream
    // we built was [BOS, b0, b1, ..., EOS, PAD, PAD...].
    let body_start = 1usize; // skip BOS
    let mut body_end = ids.len(); // exclusive
    for (i, &id) in ids.iter().enumerate().skip(1) {
        if id == EOS {
            body_end = i;
            break;
        }
    }
    // For real input, body_start..body_end maps 1-to-1 to text.as_bytes()
    // (truncated to fit). Compute a guard so we never read beyond.
    let n_body = body_end.saturating_sub(body_start);
    let n_text_bytes = text.len();
    let n_kept = n_body.min(n_text_bytes);

    let mut bio_label_top1 = Vec::with_capacity(n_kept);
    let mut bio_logprobs_out: Vec<[f32; NUM_BIO_LABELS]> = Vec::with_capacity(n_kept);
    let mut entropy_per_byte = Vec::with_capacity(n_kept);
    for i in 0..n_kept {
        let pos = body_start + i;
        let probs = &bio_probs_seq[pos];
        let logp = &bio_logprobs_seq[pos];
        let mut best = 0usize;
        let mut best_p = f32::NEG_INFINITY;
        for (k, &p) in probs.iter().enumerate() {
            if p > best_p {
                best = k;
                best_p = p;
            }
        }
        bio_label_top1.push(best);
        let mut row = [0.0_f32; NUM_BIO_LABELS];
        for (k, slot) in row.iter_mut().enumerate().take(NUM_BIO_LABELS) {
            *slot = logp.get(k).copied().unwrap_or(f32::NEG_INFINITY);
        }
        bio_logprobs_out.push(row);

        // Entropy in nats.
        let mut h = 0.0_f32;
        for (&p, &lp) in probs.iter().zip(logp.iter()) {
            if p > 0.0 {
                h -= p * lp;
            }
        }
        entropy_per_byte.push(h);
    }

    let spans = extract_spans(text, &bio_label_top1, &bio_probs_seq[body_start..body_end]);

    Ok(InferenceOutput {
        bio_label_top1,
        bio_logprobs: bio_logprobs_out,
        country_posterior: country_v.into_iter().next().unwrap_or_default(),
        spans,
        entropy_per_byte,
    })
}

/// Extract BIO spans from the per-byte argmax sequence, mapping back
/// to byte ranges in the original text.
///
/// Implementation: scan left-to-right. State = currently open span
/// (field, byte_start, accumulated_prob). On `B-X` close current span
/// (if any) and open a new one. On `I-X` matching the open field,
/// extend. On mismatch, close and open. On `O`, close.
fn extract_spans(text: &str, labels: &[usize], probs: &[Vec<f32>]) -> Vec<BioSpan> {
    let bytes = text.as_bytes();
    let n = labels.len().min(bytes.len()).min(probs.len());
    let mut out: Vec<BioSpan> = Vec::new();
    let mut open: Option<(u8, usize, f32, usize)> = None; // (field, start, prob_sum, count)

    let close = |open: &mut Option<(u8, usize, f32, usize)>,
                 end: usize,
                 bytes: &[u8],
                 out: &mut Vec<BioSpan>| {
        if let Some((field, start, ps, count)) = open.take()
            && end > start
            && count > 0
        {
            let raw = &bytes[start..end.min(bytes.len())];
            let text = String::from_utf8_lossy(raw).into_owned();
            out.push(BioSpan {
                field,
                byte_range: start..end,
                mean_label_prob: ps / count as f32,
                text,
            });
        }
    };

    for i in 0..n {
        let label = labels[i];
        let p_at = probs[i].get(label).copied().unwrap_or(0.0);
        match bio_to_field(label) {
            None => close(&mut open, i, bytes, &mut out),
            Some(field) => {
                let opens_new = is_b(label) || open.as_ref().is_none_or(|(f, _, _, _)| *f != field);
                if opens_new {
                    close(&mut open, i, bytes, &mut out);
                    open = Some((field, i, p_at, 1));
                } else if let Some((_, _, ps, c)) = open.as_mut() {
                    *ps += p_at;
                    *c += 1;
                }
            }
        }
    }
    close(&mut open, n, bytes, &mut out);
    out
}

#[cfg(test)]
mod tests {
    use super::super::transformer::ModelConfig;
    use super::super::transformer::{
        BIO_B_HOUSE, BIO_B_STREET, BIO_I_POSTCODE, BIO_I_STREET, BIO_O,
    };
    use super::*;
    use candle_core::Device;
    use candle_nn::{VarBuilder, VarMap};

    #[test]
    fn extract_spans_basic_bio_decoding() {
        // text = "Rue 1"
        let text = "Rue 1";
        let mut labels = vec![BIO_O; text.len()];
        labels[0] = BIO_B_STREET;
        labels[1] = BIO_I_STREET;
        labels[2] = BIO_I_STREET;
        labels[3] = BIO_O;
        labels[4] = BIO_B_HOUSE;
        let probs: Vec<Vec<f32>> = labels
            .iter()
            .map(|&l| {
                let mut row = vec![0.01_f32; NUM_BIO_LABELS];
                row[l] = 0.9;
                row
            })
            .collect();
        let spans = extract_spans(text, &labels, &probs);
        assert_eq!(spans.len(), 2);
        assert_eq!(spans[0].field, 0);
        assert_eq!(&text[spans[0].byte_range.clone()], "Rue");
        assert_eq!(spans[1].field, 1);
        assert_eq!(&text[spans[1].byte_range.clone()], "1");
    }

    #[test]
    fn extract_spans_handles_field_change_without_b_prefix() {
        // I- of a different field should still close the previous span.
        let text = "ABCD";
        let labels = vec![BIO_B_STREET, BIO_I_STREET, BIO_I_POSTCODE, BIO_I_POSTCODE];
        let probs: Vec<Vec<f32>> = labels
            .iter()
            .map(|&l| {
                let mut row = vec![0.01_f32; NUM_BIO_LABELS];
                row[l] = 0.9;
                row
            })
            .collect();
        let spans = extract_spans(text, &labels, &probs);
        assert_eq!(spans.len(), 2, "got {spans:?}");
        assert_eq!(spans[0].field, 0);
        assert_eq!(spans[1].field, 2);
        assert_eq!(&text[spans[0].byte_range.clone()], "AB");
        assert_eq!(&text[spans[1].byte_range.clone()], "CD");
    }

    #[test]
    fn random_init_inference_runs_end_to_end() {
        let varmap = VarMap::new();
        let device = Device::Cpu;
        let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
        let cfg = ModelConfig::tiny();
        let model = TaggerModel::new(cfg, vb).unwrap();
        let out = infer(&model, "Rue Wayez 122 1070", &device).unwrap();
        assert_eq!(out.bio_label_top1.len(), "Rue Wayez 122 1070".len());
        assert_eq!(out.country_posterior.len(), cfg.n_countries);
        assert_eq!(out.entropy_per_byte.len(), out.bio_label_top1.len());
        // Probabilities are valid distribution.
        let s: f32 = out.country_posterior.iter().sum();
        assert!(
            (s - 1.0).abs() < 1e-3,
            "country posterior didn't sum to 1: {s}"
        );
    }
}
