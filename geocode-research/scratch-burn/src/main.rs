//! Minimal byte-level transformer forward pass on burn.
//!
//! Sized to match `scratch-candle/` for an apples-to-apples latency comparison.
//! Uses the ndarray backend (no native BLAS) so the comparison is "pure-Rust
//! matmul on CPU" against candle's default CPU backend.

use anyhow::Result;
use burn::backend::NdArray;
use burn::nn::transformer::{
    TransformerEncoder, TransformerEncoderConfig, TransformerEncoderInput,
};
use burn::nn::{Embedding, EmbeddingConfig, LayerNorm, LayerNormConfig, Linear, LinearConfig};
use burn::prelude::*;
use burn::tensor::{Distribution, Tensor};
use std::time::Instant;

const VOCAB: usize = 256;
const SEQ: usize = 64;
const D_MODEL: usize = 96;
const N_HEADS: usize = 4;
const FFN_HIDDEN: usize = 256;
const N_LAYERS: usize = 4;

#[derive(Config, Debug)]
struct ByteTransformerConfig {
    transformer: TransformerEncoderConfig,
    vocab: usize,
    seq: usize,
    d_model: usize,
}

#[derive(Module, Debug)]
struct ByteTransformer<B: Backend> {
    transformer: TransformerEncoder<B>,
    tok_emb: Embedding<B>,
    pos_emb: Embedding<B>,
    ln_final: LayerNorm<B>,
    bio_head: Linear<B>,
    country_head: Linear<B>,
}

impl ByteTransformerConfig {
    fn init<B: Backend>(&self, device: &B::Device) -> ByteTransformer<B> {
        ByteTransformer {
            transformer: self.transformer.init(device),
            tok_emb: EmbeddingConfig::new(self.vocab, self.d_model).init(device),
            pos_emb: EmbeddingConfig::new(self.seq, self.d_model).init(device),
            ln_final: LayerNormConfig::new(self.d_model).init(device),
            bio_head: LinearConfig::new(self.d_model, 9).init(device),
            country_head: LinearConfig::new(self.d_model, 200).init(device),
        }
    }
}

impl<B: Backend> ByteTransformer<B> {
    fn forward(
        &self,
        tokens: Tensor<B, 2, Int>,
        positions: Tensor<B, 2, Int>,
    ) -> (Tensor<B, 3>, Tensor<B, 2>) {
        let tok = self.tok_emb.forward(tokens);
        let pos = self.pos_emb.forward(positions);
        let x = tok + pos;
        let encoded = self.transformer.forward(TransformerEncoderInput::new(x));
        let encoded = self.ln_final.forward(encoded);
        let bio = self.bio_head.forward(encoded.clone());
        // Mean-pool over seq dim.
        // mean_dim keeps the squashed dim as size-1, so we need a `squeeze_dim` style op.
        // In burn 0.20, `squeeze_dims::<2>([1])` collapses dim 1 specifically.
        let pooled: Tensor<B, 2> = encoded.mean_dim(1).squeeze_dims(&[1]);
        let country = self.country_head.forward(pooled);
        (bio, country)
    }
}

fn count_params<B: Backend>(model: &ByteTransformer<B>) -> usize {
    use burn::module::Module;
    model.num_params()
}

fn main() -> Result<()> {
    type B = NdArray<f32>;
    let device = Default::default();
    let cfg = ByteTransformerConfig {
        transformer: TransformerEncoderConfig::new(D_MODEL, FFN_HIDDEN, N_HEADS, N_LAYERS),
        vocab: VOCAB,
        seq: SEQ,
        d_model: D_MODEL,
    };
    let model: ByteTransformer<B> = cfg.init(&device);
    let total = count_params(&model);
    println!(
        "model params: {} (~{:.2} MB f32)",
        total,
        (total * 4) as f64 / 1024.0 / 1024.0
    );

    let tokens_data: Vec<i32> = (0..SEQ).map(|i| (i as i32 * 7) % VOCAB as i32).collect();
    let tokens: Tensor<B, 2, Int> = Tensor::from_data(
        burn::tensor::TensorData::new(tokens_data, [1, SEQ]),
        &device,
    );
    // silence unused-import on Distribution
    let _ = Distribution::Uniform(0.0, 1.0);
    let positions_data: Vec<i32> = (0..SEQ as i32).collect();
    let positions: Tensor<B, 2, Int> = Tensor::from_data(
        burn::tensor::TensorData::new(positions_data, [1, SEQ]),
        &device,
    );

    // Warm-up.
    for _ in 0..50 {
        let (bio, country) = model.forward(tokens.clone(), positions.clone());
        let _ = bio.dims();
        let _ = country.dims();
    }

    let n_iter = 1000;
    let mut samples = Vec::with_capacity(n_iter);
    for _ in 0..n_iter {
        let t0 = Instant::now();
        let (bio, country) = model.forward(tokens.clone(), positions.clone());
        // Force materialisation. NdArray is eager but call to_data anyway to be sure.
        let _ = bio.into_data();
        let _ = country.into_data();
        samples.push(t0.elapsed().as_micros() as u64);
    }
    samples.sort_unstable();
    let p50 = samples[n_iter / 2];
    let p90 = samples[(n_iter * 90) / 100];
    let p99 = samples[(n_iter * 99) / 100];
    let mean = samples.iter().sum::<u64>() as f64 / n_iter as f64;
    println!(
        "burn byte-transformer forward (CPU/ndarray, single-thread, batch=1, seq={}, d={}, layers={}):",
        SEQ, D_MODEL, N_LAYERS
    );
    println!("  p50: {} µs", p50);
    println!("  p90: {} µs", p90);
    println!("  p99: {} µs", p99);
    println!("  mean: {:.1} µs", mean);
    println!("  iterations: {}", n_iter);
    Ok(())
}
