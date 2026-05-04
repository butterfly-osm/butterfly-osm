//! Single-sample GBDT inference latency benchmark.
//!
//! Models the geocode reranker hot path: a 100-tree, depth-6 ensemble scoring
//! a candidate with ~20 numerical features. The reranker runs on every query
//! and we are budgeted at ~10 µs end-to-end per query, so GBDT inference must
//! land under 1 µs per candidate.
//!
//! We benchmark `gbdt` (pure-Rust). Native bindings (lightgbm3, xgboost-rs)
//! are documented in GBDT_DECISION.md but not benchmarked here because their
//! C++ build dependency (cmake, OpenMP) breaks the single-binary deploy story
//! committed in #96.

use anyhow::Result;
use gbdt::config::Config;
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;
use rand::prelude::*;
use std::time::Instant;

const N_FEATURES: usize = 20;
const N_TREES: usize = 100;
const TREE_DEPTH: u32 = 6;
const N_TRAIN: usize = 5000;

fn synth_dataset(n: usize, seed: u64) -> DataVec {
    let mut rng = StdRng::seed_from_u64(seed);
    (0..n)
        .map(|_| {
            let feature: Vec<f32> = (0..N_FEATURES).map(|_| rng.random::<f32>()).collect();
            // Synthetic label: positive iff feature[0] + feature[5] > 1.0 (with noise).
            let raw = feature[0] + feature[5] + 0.1 * rng.random::<f32>() - 0.55;
            let label = if raw > 0.0 { 1.0 } else { 0.0 };
            Data {
                feature,
                target: label,
                weight: 1.0,
                label,
                residual: 0.0,
                initial_guess: 0.0,
            }
        })
        .collect()
}

fn main() -> Result<()> {
    println!(
        "training {}-tree depth-{} GBDT on {} synthetic samples...",
        N_TREES, TREE_DEPTH, N_TRAIN
    );
    let train = synth_dataset(N_TRAIN, 42);
    let mut cfg = Config::new();
    cfg.set_feature_size(N_FEATURES);
    cfg.set_max_depth(TREE_DEPTH);
    cfg.set_iterations(N_TREES);
    cfg.set_shrinkage(0.1);
    cfg.set_loss("LogLikelyhood");
    cfg.set_debug(false);
    cfg.set_data_sample_ratio(1.0);
    cfg.set_feature_sample_ratio(1.0);
    cfg.set_training_optimization_level(2);

    let t0 = Instant::now();
    let mut model = GBDT::new(&cfg);
    model.fit(&mut train.clone());
    println!("train: {:?} (model size: {} trees)", t0.elapsed(), N_TREES);

    // Build query set — 1000 random samples for inference benchmark.
    let queries = synth_dataset(1000, 99);

    // Warm-up.
    for q in queries.iter().take(50) {
        let _ = model.predict(&vec![q.clone()]);
    }

    // Single-sample latency: predict one row at a time, measure each.
    let n_iter = queries.len();
    let mut samples_ns = Vec::with_capacity(n_iter);
    for q in &queries {
        let single = vec![q.clone()];
        let t = Instant::now();
        let _ = model.predict(&single);
        samples_ns.push(t.elapsed().as_nanos() as u64);
    }
    samples_ns.sort_unstable();
    let p50 = samples_ns[n_iter / 2];
    let p90 = samples_ns[(n_iter * 90) / 100];
    let p99 = samples_ns[(n_iter * 99) / 100];
    let mean = samples_ns.iter().sum::<u64>() as f64 / n_iter as f64;
    println!("gbdt single-sample predict (CPU, single-thread, 100 trees, depth 6, 20 features):");
    println!("  p50: {} ns ({:.2} µs)", p50, p50 as f64 / 1000.0);
    println!("  p90: {} ns ({:.2} µs)", p90, p90 as f64 / 1000.0);
    println!("  p99: {} ns ({:.2} µs)", p99, p99 as f64 / 1000.0);
    println!("  mean: {:.0} ns ({:.2} µs)", mean, mean / 1000.0);
    println!("  iterations: {}", n_iter);

    // Batched: predict 1000 in one call.
    let t = Instant::now();
    let _ = model.predict(&queries);
    let batched = t.elapsed();
    println!(
        "gbdt batched predict (1000 rows): {:?} ({:.0} ns/row)",
        batched,
        batched.as_nanos() as f64 / 1000.0
    );
    Ok(())
}
