//! Minimal byte-level transformer forward pass on candle-rs.
//!
//! Purpose: measure single-thread CPU inference latency for a small (~1-2M param)
//! transformer encoder shaped like the geocode tagger from issue #96. We are NOT
//! training here, NOT running BIO heads end-to-end — just the forward pass through
//! the encoder stack on a 64-byte synthetic input. That is the dominant cost.
//!
//! The numbers from this binary feed `geocode-research/ML_STACK_DECISION.md`.

use anyhow::Result;
use candle_core::{D, DType, Device, Tensor};
use candle_nn::{
    Embedding, LayerNorm, Linear, Module, VarBuilder, VarMap, embedding, layer_norm, linear,
};
use std::time::Instant;

/// Geocode tagger shape: byte-level vocab (256), short sequence (64), small d_model.
/// Sized to land in the 1-2M-param range without ferrying values from Python.
const VOCAB: usize = 256;
const SEQ: usize = 64;
const D_MODEL: usize = 96;
const N_HEADS: usize = 4;
const FFN_HIDDEN: usize = 256;
const N_LAYERS: usize = 4;

/// Multi-head self-attention block. Hand-rolled because candle does not ship a
/// configurable `MultiHeadAttention` we can subclass — we have to assemble it from
/// `Linear` and tensor ops. The geocode parser implementer should expect to do
/// the same.
struct SelfAttention {
    qkv: Linear,
    out: Linear,
    n_heads: usize,
    head_dim: usize,
}

impl SelfAttention {
    fn new(vb: VarBuilder, d_model: usize, n_heads: usize) -> Result<Self> {
        let head_dim = d_model / n_heads;
        let qkv = linear(d_model, d_model * 3, vb.pp("qkv"))?;
        let out = linear(d_model, d_model, vb.pp("out"))?;
        Ok(Self {
            qkv,
            out,
            n_heads,
            head_dim,
        })
    }

    fn forward(&self, x: &Tensor) -> Result<Tensor> {
        let (b, t, d) = x.dims3()?;
        let qkv = self.qkv.forward(x)?;
        // (b, t, 3*d) -> 3 x (b, h, t, head_dim)
        let qkv = qkv
            .reshape((b, t, 3, self.n_heads, self.head_dim))?
            .permute((2, 0, 3, 1, 4))?
            .contiguous()?;
        let q = qkv.get(0)?;
        let k = qkv.get(1)?;
        let v = qkv.get(2)?;
        let scale = 1.0_f64 / (self.head_dim as f64).sqrt();
        let scores = (q.matmul(&k.transpose(D::Minus2, D::Minus1)?)? * scale)?;
        let attn = candle_nn::ops::softmax(&scores, D::Minus1)?;
        let ctx = attn.matmul(&v)?;
        // (b, h, t, head_dim) -> (b, t, d)
        let ctx = ctx.transpose(1, 2)?.reshape((b, t, d))?;
        Ok(self.out.forward(&ctx)?)
    }
}

struct EncoderLayer {
    ln1: LayerNorm,
    attn: SelfAttention,
    ln2: LayerNorm,
    ffn1: Linear,
    ffn2: Linear,
}

impl EncoderLayer {
    fn new(vb: VarBuilder, d_model: usize, n_heads: usize, ffn_hidden: usize) -> Result<Self> {
        let ln1 = layer_norm(d_model, 1e-5, vb.pp("ln1"))?;
        let attn = SelfAttention::new(vb.pp("attn"), d_model, n_heads)?;
        let ln2 = layer_norm(d_model, 1e-5, vb.pp("ln2"))?;
        let ffn1 = linear(d_model, ffn_hidden, vb.pp("ffn1"))?;
        let ffn2 = linear(ffn_hidden, d_model, vb.pp("ffn2"))?;
        Ok(Self {
            ln1,
            attn,
            ln2,
            ffn1,
            ffn2,
        })
    }

    fn forward(&self, x: &Tensor) -> Result<Tensor> {
        let h = self.ln1.forward(x)?;
        let h = self.attn.forward(&h)?;
        let x = (x + &h)?;
        let h = self.ln2.forward(&x)?;
        let h = self.ffn1.forward(&h)?.gelu()?;
        let h = self.ffn2.forward(&h)?;
        Ok((x + h)?)
    }
}

struct ByteTransformer {
    tok_emb: Embedding,
    pos_emb: Embedding,
    layers: Vec<EncoderLayer>,
    ln_final: LayerNorm,
    bio_head: Linear,
    country_head: Linear,
}

impl ByteTransformer {
    fn new(vb: VarBuilder) -> Result<Self> {
        let tok_emb = embedding(VOCAB, D_MODEL, vb.pp("tok_emb"))?;
        let pos_emb = embedding(SEQ, D_MODEL, vb.pp("pos_emb"))?;
        let mut layers = Vec::with_capacity(N_LAYERS);
        for i in 0..N_LAYERS {
            layers.push(EncoderLayer::new(
                vb.pp(format!("layer.{}", i)),
                D_MODEL,
                N_HEADS,
                FFN_HIDDEN,
            )?);
        }
        let ln_final = layer_norm(D_MODEL, 1e-5, vb.pp("ln_final"))?;
        // 9 BIO labels (B/I-{street,house,postcode,locality} + O)
        let bio_head = linear(D_MODEL, 9, vb.pp("bio_head"))?;
        // 200 country IDs (placeholder for ISO 3166-1)
        let country_head = linear(D_MODEL, 200, vb.pp("country_head"))?;
        Ok(Self {
            tok_emb,
            pos_emb,
            layers,
            ln_final,
            bio_head,
            country_head,
        })
    }

    /// Forward pass. Returns (bio_logits, country_logits).
    /// bio_logits: (b, t, 9). country_logits: (b, 200) — pooled by mean.
    fn forward(&self, tokens: &Tensor, positions: &Tensor) -> Result<(Tensor, Tensor)> {
        let tok = self.tok_emb.forward(tokens)?;
        let pos = self.pos_emb.forward(positions)?;
        let mut x = (tok + pos)?;
        for layer in &self.layers {
            x = layer.forward(&x)?;
        }
        let x = self.ln_final.forward(&x)?;
        let bio = self.bio_head.forward(&x)?;
        // mean-pool across the sequence dim for the country head
        let pooled = x.mean(1)?;
        let country = self.country_head.forward(&pooled)?;
        Ok((bio, country))
    }
}

fn count_params(varmap: &VarMap) -> usize {
    varmap
        .all_vars()
        .iter()
        .map(|v| v.elem_count())
        .sum::<usize>()
}

fn main() -> Result<()> {
    let device = Device::Cpu;
    let varmap = VarMap::new();
    let vb = VarBuilder::from_varmap(&varmap, DType::F32, &device);
    let model = ByteTransformer::new(vb)?;
    let total = count_params(&varmap);
    println!(
        "model params: {} (~{:.2} MB f32)",
        total,
        (total * 4) as f64 / 1024.0 / 1024.0
    );

    // Synthetic batch=1, seq=64.
    let tokens = Tensor::from_slice(&[42u32; SEQ], (1, SEQ), &device)?;
    let positions_data: Vec<u32> = (0..SEQ as u32).collect();
    let positions = Tensor::from_slice(&positions_data, (1, SEQ), &device)?;

    // Warm-up: BLAS / kernel selection, page faults, allocator priming.
    for _ in 0..50 {
        let (bio, country) = model.forward(&tokens, &positions)?;
        let _ = bio.dtype();
        let _ = country.dtype();
    }

    // Measure single-query latency (no batching, no parallelism — single CPU thread).
    let n_iter = 1000;
    let mut samples = Vec::with_capacity(n_iter);
    for _ in 0..n_iter {
        let t0 = Instant::now();
        let (bio, country) = model.forward(&tokens, &positions)?;
        // Force materialisation — candle is lazy in places.
        let _ = bio.to_vec3::<f32>()?;
        let _ = country.to_vec2::<f32>()?;
        samples.push(t0.elapsed().as_micros() as u64);
    }
    samples.sort_unstable();
    let p50 = samples[n_iter / 2];
    let p90 = samples[(n_iter * 90) / 100];
    let p99 = samples[(n_iter * 99) / 100];
    let mean = samples.iter().sum::<u64>() as f64 / n_iter as f64;
    println!(
        "candle byte-transformer forward (CPU, single-thread, batch=1, seq={}, d={}, layers={}):",
        SEQ, D_MODEL, N_LAYERS
    );
    println!("  p50: {} µs", p50);
    println!("  p90: {} µs", p90);
    println!("  p99: {} µs", p99);
    println!("  mean: {:.1} µs", mean);
    println!("  iterations: {}", n_iter);
    Ok(())
}
