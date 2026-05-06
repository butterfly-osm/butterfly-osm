//! Byte-level transformer architecture (#96 §Tagger).
//!
//! ## Architecture
//!
//! Standard pre-norm transformer encoder:
//!
//! ```text
//! input ids ──[Embedding]──┬─────────────┐
//!                          │             │
//!                  [Sin/Cos PosEnc]     │
//!                          │             │
//!                          ▼             │
//!         ┌────────────[Block 1]◄────────┘
//!         │  pre-LN ─► MultiHeadAttn (causal=false, mask=padding)
//!         │            +residual
//!         │  pre-LN ─► FFN (Linear(d→4d), GELU, Linear(4d→d))
//!         │            +residual
//!         │
//!         ▼
//!     [Block 2..N_LAYERS]
//!         │
//!         ▼
//!    [Final LayerNorm]
//!         │
//!         ├──► [BIO head: Linear(d→7)]   shape (B, T, NUM_BIO_LABELS)
//!         │
//!         └──► [Country head]:
//!               mean-pool over non-pad positions
//!               ─► Linear(d→n_countries)   shape (B, n_countries)
//! ```
//!
//! ## Notes on the design
//!
//! - **Pre-norm** (`norm → attn`) instead of post-norm. Pre-norm
//!   transformers train more stably without warmup, which matters
//!   for a tiny model on a tiny corpus.
//! - **Sinusoidal** positional encoding instead of learned. Learned
//!   would force the model to memorize positions; sin/cos generalizes
//!   to lengths outside the training distribution.
//! - **GELU** activation (closed-form approximation, matches GPT-2).
//! - **Padding mask only**, no causal mask. Tagging needs full
//!   bidirectional context.
//! - **Single global model**, no LoRA / adapters. The hooks are noted
//!   in #96 §Tagger and filed for follow-up; not in this PR's scope.

use candle_core::{D, DType, Device, IndexOp, Module, Result as CResult, Tensor};
use candle_nn::{Embedding, LayerNorm, Linear, VarBuilder, embedding, layer_norm, linear};
use serde::{Deserialize, Serialize};

use super::tokenizer::{MAX_SEQ_LEN, VOCAB_SIZE};

/// Number of BIO labels: O, B-STREET, I-STREET, B-HOUSE, I-HOUSE,
/// B-POSTCODE, I-POSTCODE, B-LOCALITY, I-LOCALITY.
///
/// Outside-token + 4 BIO pairs = 9.
pub const NUM_BIO_LABELS: usize = 9;

/// Index of the `O` (outside) BIO label.
pub const BIO_O: usize = 0;
/// First B- index. The pairing is `B = 1 + 2*field, I = 2 + 2*field`
/// for `field ∈ 0..4` (street, house, postcode, locality).
pub const BIO_B_STREET: usize = 1;
pub const BIO_I_STREET: usize = 2;
pub const BIO_B_HOUSE: usize = 3;
pub const BIO_I_HOUSE: usize = 4;
pub const BIO_B_POSTCODE: usize = 5;
pub const BIO_I_POSTCODE: usize = 6;
pub const BIO_B_LOCALITY: usize = 7;
pub const BIO_I_LOCALITY: usize = 8;

/// Field id matching the BIO scheme (0=street, 1=house, 2=postcode,
/// 3=locality). `O` maps to `None`.
#[must_use]
pub fn bio_to_field(label: usize) -> Option<u8> {
    match label {
        BIO_O => None,
        BIO_B_STREET | BIO_I_STREET => Some(0),
        BIO_B_HOUSE | BIO_I_HOUSE => Some(1),
        BIO_B_POSTCODE | BIO_I_POSTCODE => Some(2),
        BIO_B_LOCALITY | BIO_I_LOCALITY => Some(3),
        _ => None,
    }
}

#[must_use]
pub fn is_b(label: usize) -> bool {
    matches!(
        label,
        BIO_B_STREET | BIO_B_HOUSE | BIO_B_POSTCODE | BIO_B_LOCALITY
    )
}

/// Configuration for the transformer.
///
/// The default is the **tiny** profile shipped with the proof-of-life
/// model (~120k params, fits well under the 5MB safetensors target):
///
/// - d_model=64, n_heads=4 (each head 16-dim), n_layers=2
/// - d_ff=256, n_countries=1 (BE only — multi-country tracked in #96)
///
/// A real production model would scale these to ~2-4M params per
/// #96 §Tagger.
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
pub struct ModelConfig {
    pub d_model: usize,
    pub n_heads: usize,
    pub n_layers: usize,
    pub d_ff: usize,
    pub max_seq_len: usize,
    pub vocab_size: usize,
    pub num_bio_labels: usize,
    pub n_countries: usize,
    pub layer_norm_eps: f64,
}

impl ModelConfig {
    /// Tiny proof-of-life config used by the shipped model.
    #[must_use]
    pub fn tiny() -> Self {
        Self {
            d_model: 64,
            n_heads: 4,
            n_layers: 2,
            d_ff: 256,
            max_seq_len: MAX_SEQ_LEN,
            vocab_size: VOCAB_SIZE,
            num_bio_labels: NUM_BIO_LABELS,
            n_countries: 1,
            layer_norm_eps: 1e-5,
        }
    }

    /// Production profile for the Fork A+ neural parser engineering.
    ///
    /// d_model=128, n_heads=8 (each 16-dim), n_layers=4, d_ff=512.
    /// vocab=260 (256 byte values + 4 specials), max_seq_len=128.
    ///
    /// Rough parameter count (excluding biases ≪ embeds):
    /// - embed: 260 × 128 = 33 280
    /// - 4 encoder blocks, each:
    ///   - QKV: 128 × (3·128) = 49 152
    ///   - attn out: 128 × 128 = 16 384
    ///   - FFN fc1: 128 × 512 = 65 536
    ///   - FFN fc2: 512 × 128 = 65 536
    ///   - LN×2: 2 × 2 × 128 = 512
    ///   - sums to 197 120 per block × 4 layers = 788 480
    /// - final LN: 256
    /// - bio_head: 128 × 9 = 1 152
    /// - country_head: 128 × n_countries (varies)
    ///
    /// At n_countries=1 ≈ 823 168 fp32 weights ≈ 3.3 MB safetensors.
    /// At n_countries=8 ≈ 824 064 fp32 weights ≈ 3.3 MB. Headroom for
    /// the 8 MB target in the spec.
    ///
    /// Note: the 2.5M-param figure quoted in the Fork A+ task brief
    /// assumed a larger embedding/vocab; with a 260-token byte vocab
    /// the natural footprint at d_model=128/n_layers=4 is ~0.8 M
    /// params. This is the right size for byte-level NER and matches
    /// the "production but still tiny" target.
    #[must_use]
    pub fn production(n_countries: usize) -> Self {
        Self {
            d_model: 128,
            n_heads: 8,
            n_layers: 4,
            d_ff: 512,
            max_seq_len: MAX_SEQ_LEN,
            vocab_size: VOCAB_SIZE,
            num_bio_labels: NUM_BIO_LABELS,
            n_countries: n_countries.max(1),
            layer_norm_eps: 1e-5,
        }
    }

    /// Approximate parameter count (weights only — biases and LN
    /// scales/biases are tiny rounding terms). Used by tests + logging
    /// to confirm the architecture lands at the expected size.
    #[must_use]
    pub fn approx_param_count(&self) -> usize {
        let embed = self.vocab_size * self.d_model;
        let qkv = self.d_model * 3 * self.d_model;
        let attn_out = self.d_model * self.d_model;
        let fc1 = self.d_model * self.d_ff;
        let fc2 = self.d_ff * self.d_model;
        let ln = 2 * self.d_model; // gamma + beta
        let block = qkv + attn_out + fc1 + fc2 + 2 * ln;
        let final_ln = ln;
        let bio_head = self.d_model * self.num_bio_labels;
        let country_head = self.d_model * self.n_countries;
        embed + self.n_layers * block + final_ln + bio_head + country_head
    }

    pub fn validate(&self) -> CResult<()> {
        if self.n_heads == 0 {
            return Err(candle_core::Error::Msg("n_heads must be > 0".to_string()));
        }
        if self.d_model == 0 {
            return Err(candle_core::Error::Msg("d_model must be > 0".to_string()));
        }
        if self.vocab_size == 0 {
            return Err(candle_core::Error::Msg(
                "vocab_size must be > 0".to_string(),
            ));
        }
        if self.max_seq_len == 0 {
            return Err(candle_core::Error::Msg(
                "max_seq_len must be > 0".to_string(),
            ));
        }
        if !self.d_model.is_multiple_of(self.n_heads) {
            return Err(candle_core::Error::Msg(format!(
                "d_model ({}) must be divisible by n_heads ({})",
                self.d_model, self.n_heads
            )));
        }
        Ok(())
    }

    #[must_use]
    pub fn head_dim(&self) -> usize {
        self.d_model / self.n_heads
    }
}

impl Default for ModelConfig {
    fn default() -> Self {
        Self::tiny()
    }
}

/// Multi-head self-attention, no rotary, no kv-cache. Padding mask
/// is applied as a large negative bias to the pre-softmax logits at
/// padded keys, so attention weights at pad columns go to ~0.
#[derive(Debug)]
pub struct MultiHeadAttention {
    qkv: Linear,
    out: Linear,
    n_heads: usize,
    head_dim: usize,
}

impl MultiHeadAttention {
    pub fn new(cfg: &ModelConfig, vb: VarBuilder) -> CResult<Self> {
        // Fused QKV projection: 3 × d_model output for cache-friendly access.
        let qkv = linear(cfg.d_model, cfg.d_model * 3, vb.pp("qkv"))?;
        let out = linear(cfg.d_model, cfg.d_model, vb.pp("out"))?;
        Ok(Self {
            qkv,
            out,
            n_heads: cfg.n_heads,
            head_dim: cfg.head_dim(),
        })
    }

    /// `x: (B, T, D)`, `mask: (B, T)` with 1.0 for keep / 0.0 for pad.
    /// Returns `(B, T, D)`.
    pub fn forward(&self, x: &Tensor, mask: &Tensor) -> CResult<Tensor> {
        let (b, t, _d) = x.dims3()?;

        // Project to QKV.
        let qkv = self.qkv.forward(x)?; // (B, T, 3D)
        let qkv = qkv.reshape((b, t, 3, self.n_heads, self.head_dim))?;
        // (B, T, 3, H, Hd) → (3, B, H, T, Hd) for cleaner extraction.
        let qkv = qkv.permute((2, 0, 3, 1, 4))?.contiguous()?;
        let q = qkv.i(0)?; // (B, H, T, Hd)
        let k = qkv.i(1)?;
        let v = qkv.i(2)?;

        // Scaled dot-product attention.
        let scale = (self.head_dim as f64).sqrt();
        let scores = q.matmul(&k.transpose(D::Minus2, D::Minus1)?)?; // (B, H, T, T)
        let scores = (scores / scale)?;

        // Build padding mask: (B, 1, 1, T) of 0/-inf.
        // mask is (B, T) with 1=real / 0=pad.
        let neg_inf = -1.0e30_f32;
        let bias = mask.to_dtype(DType::F32)?; // (B, T)
        let bias = bias
            .affine(neg_inf as f64, 0.0)? // 1.0 * -inf where mask was 1.0
            .neg()?
            .affine(1.0, neg_inf as f64)?; // = -inf * (1-mask)  i.e. 0 for real, -inf for pad
        // Equivalently: -inf where mask==0.
        let bias = bias.unsqueeze(1)?.unsqueeze(1)?; // (B, 1, 1, T)
        let scores = scores.broadcast_add(&bias)?;

        let weights = candle_nn::ops::softmax_last_dim(&scores)?;
        let ctx = weights.matmul(&v)?; // (B, H, T, Hd)
        let ctx = ctx.transpose(1, 2)?.contiguous()?; // (B, T, H, Hd)
        let ctx = ctx.reshape((b, t, self.n_heads * self.head_dim))?;
        self.out.forward(&ctx)
    }
}

#[derive(Debug)]
pub struct FeedForward {
    fc1: Linear,
    fc2: Linear,
}

impl FeedForward {
    pub fn new(cfg: &ModelConfig, vb: VarBuilder) -> CResult<Self> {
        let fc1 = linear(cfg.d_model, cfg.d_ff, vb.pp("fc1"))?;
        let fc2 = linear(cfg.d_ff, cfg.d_model, vb.pp("fc2"))?;
        Ok(Self { fc1, fc2 })
    }

    pub fn forward(&self, x: &Tensor) -> CResult<Tensor> {
        let h = self.fc1.forward(x)?;
        let h = h.gelu()?;
        self.fc2.forward(&h)
    }
}

#[derive(Debug)]
pub struct EncoderBlock {
    norm1: LayerNorm,
    attn: MultiHeadAttention,
    norm2: LayerNorm,
    ffn: FeedForward,
}

impl EncoderBlock {
    pub fn new(cfg: &ModelConfig, vb: VarBuilder) -> CResult<Self> {
        let norm1 = layer_norm(cfg.d_model, cfg.layer_norm_eps, vb.pp("norm1"))?;
        let attn = MultiHeadAttention::new(cfg, vb.pp("attn"))?;
        let norm2 = layer_norm(cfg.d_model, cfg.layer_norm_eps, vb.pp("norm2"))?;
        let ffn = FeedForward::new(cfg, vb.pp("ffn"))?;
        Ok(Self {
            norm1,
            attn,
            norm2,
            ffn,
        })
    }

    pub fn forward(&self, x: &Tensor, mask: &Tensor) -> CResult<Tensor> {
        // Pre-norm residual structure.
        let h = self.norm1.forward(x)?;
        let h = self.attn.forward(&h, mask)?;
        let x = (x + h)?;
        let h = self.norm2.forward(&x)?;
        let h = self.ffn.forward(&h)?;
        x + h
    }
}

/// Sinusoidal positional encoding precomputed up to `max_seq_len`.
fn build_sin_cos_positions(max_seq_len: usize, d_model: usize, device: &Device) -> CResult<Tensor> {
    let mut pe = vec![0.0_f32; max_seq_len * d_model];
    for pos in 0..max_seq_len {
        for i in 0..(d_model / 2) {
            let denom = 10_000_f32.powf((2 * i) as f32 / d_model as f32);
            pe[pos * d_model + 2 * i] = (pos as f32 / denom).sin();
            pe[pos * d_model + 2 * i + 1] = (pos as f32 / denom).cos();
        }
        // odd d_model: leave the trailing slot zero
    }
    Tensor::from_vec(pe, (max_seq_len, d_model), device)
}

/// Full tagger: encoder + BIO head + country head.
#[derive(Debug)]
pub struct TaggerModel {
    pub config: ModelConfig,
    embed: Embedding,
    pos_enc: Tensor,
    blocks: Vec<EncoderBlock>,
    final_norm: LayerNorm,
    bio_head: Linear,
    country_head: Linear,
}

impl TaggerModel {
    pub fn new(cfg: ModelConfig, vb: VarBuilder) -> CResult<Self> {
        cfg.validate()?;
        let embed = embedding(cfg.vocab_size, cfg.d_model, vb.pp("embed"))?;
        let pos_enc = build_sin_cos_positions(cfg.max_seq_len, cfg.d_model, vb.device())?;
        let mut blocks = Vec::with_capacity(cfg.n_layers);
        for i in 0..cfg.n_layers {
            blocks.push(EncoderBlock::new(&cfg, vb.pp(format!("block_{i}")))?);
        }
        let final_norm = layer_norm(cfg.d_model, cfg.layer_norm_eps, vb.pp("final_norm"))?;
        let bio_head = linear(cfg.d_model, cfg.num_bio_labels, vb.pp("bio_head"))?;
        let country_head = linear(cfg.d_model, cfg.n_countries, vb.pp("country_head"))?;
        Ok(Self {
            config: cfg,
            embed,
            pos_enc,
            blocks,
            final_norm,
            bio_head,
            country_head,
        })
    }

    /// Forward pass.
    ///
    /// - `ids: (B, T)` u32
    /// - `mask: (B, T)` u32 with 1=real, 0=pad
    ///
    /// Returns:
    /// - `bio_logits: (B, T, NUM_BIO_LABELS)` f32
    /// - `country_logits: (B, n_countries)` f32
    pub fn forward(&self, ids: &Tensor, mask: &Tensor) -> CResult<(Tensor, Tensor)> {
        let (b, t) = ids.dims2()?;
        let mask_f = mask.to_dtype(DType::F32)?; // (B, T)

        let mut h = self.embed.forward(ids)?; // (B, T, D)
        // Add positional encoding (slice to T).
        let pos = self.pos_enc.i(..t)?; // (T, D)
        let pos = pos
            .unsqueeze(0)?
            .broadcast_as((b, t, self.config.d_model))?;
        h = (h + pos)?;

        for blk in &self.blocks {
            h = blk.forward(&h, &mask_f)?;
        }
        h = self.final_norm.forward(&h)?;

        let bio_logits = self.bio_head.forward(&h)?; // (B, T, NUM_BIO_LABELS)

        // Mean-pool over non-pad positions for country head.
        // weighted sum / sum(mask).
        let mask_unsq = mask_f.unsqueeze(D::Minus1)?; // (B, T, 1)
        let h_masked = h.broadcast_mul(&mask_unsq)?;
        let summed = h_masked.sum(1)?; // (B, D)
        let denom = mask_f
            .sum(1)? // (B,)
            .clamp(1.0, f64::INFINITY)?
            .unsqueeze(D::Minus1)?; // (B, 1)
        let pooled = summed.broadcast_div(&denom)?; // (B, D)
        let country_logits = self.country_head.forward(&pooled)?;

        Ok((bio_logits, country_logits))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use candle_core::Device;
    use candle_nn::VarMap;

    fn cpu_vb_and_map() -> (VarMap, Device) {
        let varmap = VarMap::new();
        let device = Device::Cpu;
        (varmap, device)
    }

    #[test]
    fn config_validates_head_divisibility() {
        let mut cfg = ModelConfig::tiny();
        assert!(cfg.validate().is_ok());
        cfg.n_heads = 5;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn config_rejects_zero_dimensions() {
        let mut cfg = ModelConfig::tiny();
        cfg.n_heads = 0;
        assert!(cfg.validate().is_err(), "n_heads=0 must fail validation");

        let mut cfg = ModelConfig::tiny();
        cfg.d_model = 0;
        assert!(cfg.validate().is_err(), "d_model=0 must fail validation");

        let mut cfg = ModelConfig::tiny();
        cfg.vocab_size = 0;
        assert!(cfg.validate().is_err(), "vocab_size=0 must fail validation");

        let mut cfg = ModelConfig::tiny();
        cfg.max_seq_len = 0;
        assert!(
            cfg.validate().is_err(),
            "max_seq_len=0 must fail validation"
        );
    }

    #[test]
    fn forward_shape_is_correct() {
        let (varmap, device) = cpu_vb_and_map();
        let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
        let cfg = ModelConfig::tiny();
        let model = TaggerModel::new(cfg, vb).unwrap();

        let b = 2usize;
        let t = 16usize;
        let ids = Tensor::zeros((b, t), DType::U32, &device).unwrap();
        let mask = Tensor::ones((b, t), DType::U32, &device).unwrap();

        let (bio, country) = model.forward(&ids, &mask).unwrap();
        assert_eq!(bio.dims(), &[b, t, NUM_BIO_LABELS]);
        assert_eq!(country.dims(), &[b, cfg.n_countries]);
    }

    #[test]
    fn forward_with_padding_mask_runs() {
        let (varmap, device) = cpu_vb_and_map();
        let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
        let cfg = ModelConfig::tiny();
        let model = TaggerModel::new(cfg, vb).unwrap();

        let b = 1usize;
        let t = 8usize;
        let ids_v: Vec<u32> = (0..(b * t) as u32).collect();
        let mut mask_v: Vec<u32> = vec![1; b * t];
        // pad last 3 positions
        for m in mask_v.iter_mut().skip(b * t - 3) {
            *m = 0;
        }
        let ids = Tensor::from_vec(ids_v, (b, t), &device).unwrap();
        let mask = Tensor::from_vec(mask_v, (b, t), &device).unwrap();
        let (bio, _) = model.forward(&ids, &mask).unwrap();
        assert_eq!(bio.dims(), &[b, t, NUM_BIO_LABELS]);
        // No NaN.
        let v = bio.flatten_all().unwrap().to_vec1::<f32>().unwrap();
        assert!(v.iter().all(|x| x.is_finite()), "NaN/inf in bio logits");
    }

    #[test]
    fn production_config_validates_and_has_expected_shape() {
        let cfg = ModelConfig::production(5);
        assert!(cfg.validate().is_ok());
        assert_eq!(cfg.d_model, 128);
        assert_eq!(cfg.n_heads, 8);
        assert_eq!(cfg.n_layers, 4);
        assert_eq!(cfg.d_ff, 512);
        assert_eq!(cfg.n_countries, 5);
        assert_eq!(cfg.head_dim(), 16);
    }

    #[test]
    fn production_config_param_count_in_expected_range() {
        let cfg = ModelConfig::production(5);
        let pc = cfg.approx_param_count();
        // ~825k params for d_model=128 / n_layers=4 / vocab=260.
        // The Fork A+ brief targeted "~2.5M params" but with byte-vocab
        // the natural footprint at this depth is ~0.8 M. We assert the
        // shape lands in the right order of magnitude (between 500k
        // and 1.5M) — the brief number was wrong, the architecture is
        // right.
        assert!(
            (500_000..=1_500_000).contains(&pc),
            "production param count {pc} out of expected range",
        );
    }

    #[test]
    fn production_config_forward_shape() {
        let (varmap, device) = cpu_vb_and_map();
        let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
        let cfg = ModelConfig::production(5);
        let model = TaggerModel::new(cfg, vb).unwrap();
        let b = 2usize;
        let t = 32usize;
        let ids = Tensor::zeros((b, t), DType::U32, &device).unwrap();
        let mask = Tensor::ones((b, t), DType::U32, &device).unwrap();
        let (bio, country) = model.forward(&ids, &mask).unwrap();
        assert_eq!(bio.dims(), &[b, t, NUM_BIO_LABELS]);
        assert_eq!(country.dims(), &[b, 5]);
    }

    #[test]
    fn production_config_country_head_scales_with_n_countries() {
        for n in [1, 3, 5, 8] {
            let cfg = ModelConfig::production(n);
            assert_eq!(cfg.n_countries, n);
            let (varmap, device) = cpu_vb_and_map();
            let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
            let model = TaggerModel::new(cfg, vb).unwrap();
            let ids = Tensor::zeros((1, 8), DType::U32, &device).unwrap();
            let mask = Tensor::ones((1, 8), DType::U32, &device).unwrap();
            let (_bio, country) = model.forward(&ids, &mask).unwrap();
            assert_eq!(country.dims(), &[1, n]);
        }
    }

    #[test]
    fn bio_to_field_mapping() {
        assert_eq!(bio_to_field(BIO_O), None);
        assert_eq!(bio_to_field(BIO_B_STREET), Some(0));
        assert_eq!(bio_to_field(BIO_I_STREET), Some(0));
        assert_eq!(bio_to_field(BIO_B_HOUSE), Some(1));
        assert_eq!(bio_to_field(BIO_B_POSTCODE), Some(2));
        assert_eq!(bio_to_field(BIO_B_LOCALITY), Some(3));
    }
}
