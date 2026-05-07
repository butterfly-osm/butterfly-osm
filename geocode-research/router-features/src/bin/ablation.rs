//! Phase 4 — Structural ablation at the chosen knee (step 5).
//!
//! At the knee feature set (marker + digit + bigram + length + punct;
//! 356 features active; ~624 ns extraction cost), compare three
//! inference structures on accuracy and latency:
//!
//!   A. Tree: 15 one-vs-rest GBDT trees (1 estimator, depth 12).
//!      Already trained in Phase 3 conceptually — we retrain here for
//!      a clean, isolated bench.
//!
//!   B. Signature table: discretise each feature into a small number
//!      of buckets, hash the bucket vector to u32, look up empirical
//!      posterior counts. Inference = extract → quantise → hash →
//!      table lookup.
//!
//!   C. Cost-aware cascade: hand-tuned ordering of cheapest checks
//!      first, with early termination on locks (a feature value that
//!      uniquely identifies one country in training).  Falls through
//!      to a residual table for ambiguous queries.
//!
//! Output `ablation.tsv` with: structure, mean_inference_ns, p50_ns,
//! p99_ns, memory_kb, macro_acc, min_country_acc, min_family_acc.

use anyhow::{Context, Result};
use clap::Parser;
use gbdt::config::Config;
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;
use rand::SeedableRng;
use rand::seq::SliceRandom;
use router_features::{family_for, ExtractorSpec, FeatureClass, COUNTRIES};
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "geocode-research/router-features/artifacts/extractor.spec.json")]
    extractor_spec: PathBuf,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/features-subset.bin")]
    features_bin: PathBuf,
    /// At what step (knee) to evaluate.  Step 5 = marker+digit+bigram+length+punct.
    #[arg(long, default_value = "5")]
    knee_step: usize,
    #[arg(long, default_value = "20000")]
    cap_per_country: usize,
    #[arg(long, default_value = "12")]
    max_depth: u32,
    #[arg(long, default_value = "42")]
    seed: u64,
    /// Number of latency-bench iterations (per structure).
    #[arg(long, default_value = "20000")]
    bench_iters: usize,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/ablation.tsv")]
    out: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let spec = ExtractorSpec::load(&args.extractor_spec)?;
    let ex = spec.build();
    let n_total = ex.n_features();

    // Build the cost-aware class addition order (same ranking as Phase 3).
    // Knee step = first N classes from MI/ns ranking.
    let knee_classes: Vec<FeatureClass> = vec![
        FeatureClass::Marker,
        FeatureClass::Digit,
        FeatureClass::Bigram,
        FeatureClass::Length,
        FeatureClass::Punct,
        FeatureClass::Script,
        FeatureClass::Postcode,
    ];
    if args.knee_step == 0 || args.knee_step > knee_classes.len() {
        anyhow::bail!("knee_step out of range");
    }
    let active_classes: Vec<FeatureClass> = knee_classes[..args.knee_step].to_vec();
    let class_ranges = ex.class_ranges();
    let mut active_indices: Vec<usize> = Vec::new();
    for c in &active_classes {
        for (cc, s, e) in &class_ranges {
            if *cc == *c {
                for i in *s..*e {
                    active_indices.push(i);
                }
            }
        }
    }
    eprintln!(
        "[knee] step={}, classes={:?}, n_features={}",
        args.knee_step,
        active_classes,
        active_indices.len()
    );

    eprintln!("[load] feature subset");
    let (rows_features, rows_country, n_records) = load_subset(&args.features_bin, n_total)?;
    eprintln!(
        "[load] {} records × {} features (per-country cap = {})",
        n_records, n_total, args.cap_per_country
    );

    let (sf, sc) = cap_per_country(&rows_features, &rows_country, n_total, args.cap_per_country);
    let n = sc.len();
    let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
    let mut perm: Vec<usize> = (0..n).collect();
    perm.shuffle(&mut rng);
    let n_train = (n as f32 * 0.8) as usize;
    let train_idx: Vec<usize> = perm[..n_train].to_vec();
    let test_idx: Vec<usize> = perm[n_train..].to_vec();

    // Build train/test feature views (only active columns).
    let train_feats: Vec<Vec<f32>> = train_idx
        .iter()
        .map(|&r| {
            let off = r * n_total;
            active_indices.iter().map(|&c| sf[off + c]).collect()
        })
        .collect();
    let train_lbls: Vec<u8> = train_idx.iter().map(|&i| sc[i]).collect();
    let test_feats: Vec<Vec<f32>> = test_idx
        .iter()
        .map(|&r| {
            let off = r * n_total;
            active_indices.iter().map(|&c| sf[off + c]).collect()
        })
        .collect();
    let test_lbls: Vec<u8> = test_idx.iter().map(|&i| sc[i]).collect();

    eprintln!("[split] train={}, test={}", train_feats.len(), test_feats.len());

    // ---- Structure A: tree ensemble (1 tree per country, OvR) ----
    eprintln!("\n=== A. Tree (15 OvR, depth={}) ===", args.max_depth);
    let t0 = Instant::now();
    let trees = train_ovr_trees(&train_feats, &train_lbls, args.max_depth);
    let train_seconds_a = t0.elapsed().as_secs_f64();
    eprintln!("    trained in {:.2}s", train_seconds_a);

    let acc_a = eval_trees(&trees, &test_feats, &test_lbls);
    let lat_a = bench_trees(&trees, &test_feats, args.bench_iters);
    let mem_a = estimate_tree_memory(&trees);
    eprintln!(
        "    macro_acc={:.4}, min_country={:.4}, min_family={:.4}, mean={:.0}ns p50={:.0}ns p99={:.0}ns mem={}KB",
        acc_a.macro_acc,
        acc_a.min_country_acc,
        acc_a.min_family_acc,
        lat_a.mean,
        lat_a.p50,
        lat_a.p99,
        mem_a
    );

    // ---- Structure B: signature table ----
    eprintln!("\n=== B. Signature table ===");
    let t0 = Instant::now();
    // Quantize: continuous features into 4 buckets each (already binary
    // for most). Build u64 hash of the bucket vector → posterior over
    // countries.
    let n_active = active_indices.len();
    let (mins, maxs) = compute_min_max(&train_feats, n_active);
    let table = build_signature_table(&train_feats, &train_lbls, &mins, &maxs);
    let train_seconds_b = t0.elapsed().as_secs_f64();
    eprintln!(
        "    table size = {} unique signatures, train {:.2}s",
        table.map.len(),
        train_seconds_b
    );

    let acc_b = eval_table(&table, &test_feats, &test_lbls, &mins, &maxs);
    let lat_b = bench_table(&table, &test_feats, &mins, &maxs, args.bench_iters);
    let mem_b = estimate_table_memory(&table);
    eprintln!(
        "    macro_acc={:.4}, min_country={:.4}, min_family={:.4}, mean={:.0}ns p50={:.0}ns p99={:.0}ns mem={}KB",
        acc_b.macro_acc,
        acc_b.min_country_acc,
        acc_b.min_family_acc,
        lat_b.mean,
        lat_b.p50,
        lat_b.p99,
        mem_b
    );

    // ---- Structure C: cost-aware cascade ----
    eprintln!("\n=== C. Cost-aware cascade ===");
    // Cascade: identify "lock" features in training (a feature value
    // that only appears with one country). At inference, check
    // cheapest class first, then escalate. Fall back to a tree on
    // residual ambiguous cases.
    //
    // Implementation: for each binary feature, count countries seeing
    // value 1.  If exactly 1, that feature's presence locks that
    // country with high confidence (the empirical posterior).
    let cascade = build_cascade(&train_feats, &train_lbls, &active_classes, &active_indices, &class_ranges);
    let acc_c = eval_cascade(&cascade, &test_feats, &test_lbls, &active_classes, &active_indices, &class_ranges);
    let lat_c = bench_cascade(&cascade, &test_feats, &active_classes, &active_indices, &class_ranges, args.bench_iters);
    let mem_c = estimate_cascade_memory(&cascade);
    eprintln!(
        "    macro_acc={:.4}, min_country={:.4}, min_family={:.4}, mean={:.0}ns p50={:.0}ns p99={:.0}ns mem={}KB",
        acc_c.macro_acc,
        acc_c.min_country_acc,
        acc_c.min_family_acc,
        lat_c.mean,
        lat_c.p50,
        lat_c.p99,
        mem_c
    );

    eprintln!("\n[out] writing {}", args.out.display());
    let mut w = std::io::BufWriter::new(std::fs::File::create(&args.out)?);
    writeln!(
        w,
        "structure\tmean_inference_ns\tp50_ns\tp99_ns\tmemory_kb\tmacro_acc\tmin_country_acc\tmin_country\tmin_family_acc\tmin_family"
    )?;
    write_row(&mut w, "tree", &acc_a, &lat_a, mem_a)?;
    write_row(&mut w, "signature_table", &acc_b, &lat_b, mem_b)?;
    write_row(&mut w, "cascade", &acc_c, &lat_c, mem_c)?;

    Ok(())
}

fn write_row(
    w: &mut impl Write,
    name: &str,
    acc: &AccResult,
    lat: &LatencyStats,
    mem_kb: usize,
) -> std::io::Result<()> {
    writeln!(
        w,
        "{}\t{:.0}\t{:.0}\t{:.0}\t{}\t{:.6}\t{:.6}\t{}\t{:.6}\t{}",
        name,
        lat.mean,
        lat.p50,
        lat.p99,
        mem_kb,
        acc.macro_acc,
        acc.min_country_acc,
        acc.min_country,
        acc.min_family_acc,
        acc.min_family,
    )
}

// ---------- common ----------

struct AccResult {
    macro_acc: f64,
    min_country_acc: f64,
    min_country: String,
    min_family_acc: f64,
    min_family: String,
}

struct LatencyStats {
    mean: f64,
    p50: f64,
    p99: f64,
}

fn compute_acc(predictions: &[u8], truth: &[u8]) -> AccResult {
    let mut correct = vec![0u64; COUNTRIES.len()];
    let mut total = vec![0u64; COUNTRIES.len()];
    for (p, t) in predictions.iter().zip(truth.iter()) {
        total[*t as usize] += 1;
        if p == t {
            correct[*t as usize] += 1;
        }
    }
    let macro_acc = (0..COUNTRIES.len())
        .map(|c| {
            if total[c] == 0 {
                0.0
            } else {
                correct[c] as f64 / total[c] as f64
            }
        })
        .sum::<f64>()
        / COUNTRIES.len() as f64;
    let mut min_country_acc = 1.0;
    let mut min_country_idx = 0usize;
    for c in 0..COUNTRIES.len() {
        let a = if total[c] == 0 {
            0.0
        } else {
            correct[c] as f64 / total[c] as f64
        };
        if a < min_country_acc {
            min_country_acc = a;
            min_country_idx = c;
        }
    }
    // family
    let mut fam: HashMap<&'static str, (u64, u64)> = HashMap::new();
    for c in 0..COUNTRIES.len() {
        let f = family_for(COUNTRIES[c]);
        let e = fam.entry(f).or_default();
        e.0 += correct[c];
        e.1 += total[c];
    }
    let mut min_family_acc = 1.0;
    let mut min_family_name = String::new();
    for (f, (cor, tot)) in &fam {
        let a = if *tot == 0 { 0.0 } else { *cor as f64 / *tot as f64 };
        if a < min_family_acc {
            min_family_acc = a;
            min_family_name = (*f).to_string();
        }
    }
    AccResult {
        macro_acc,
        min_country_acc,
        min_country: COUNTRIES[min_country_idx].to_string(),
        min_family_acc,
        min_family: min_family_name,
    }
}

fn percentiles(mut samples: Vec<f64>) -> LatencyStats {
    samples.sort_by(|a, b| a.partial_cmp(b).unwrap());
    let n = samples.len();
    let mean = samples.iter().sum::<f64>() / n.max(1) as f64;
    let p50 = samples[n / 2];
    let p99 = samples[(n as f64 * 0.99) as usize];
    LatencyStats { mean, p50, p99 }
}

// ---------- A: tree ----------

fn train_ovr_trees(
    train_feats: &[Vec<f32>],
    train_lbls: &[u8],
    max_depth: u32,
) -> Vec<GBDT> {
    let n_feat = train_feats[0].len();
    let mut models = Vec::with_capacity(COUNTRIES.len());
    for c in 0..COUNTRIES.len() {
        let mut cfg = Config::new();
        cfg.set_feature_size(n_feat);
        cfg.set_max_depth(max_depth);
        cfg.set_iterations(1);
        cfg.set_shrinkage(0.1);
        cfg.set_loss("LogLikelyhood");
        cfg.set_training_optimization_level(2);
        cfg.set_min_leaf_size(20);
        let mut dv: DataVec = Vec::with_capacity(train_feats.len());
        for (i, row) in train_feats.iter().enumerate() {
            let label = if train_lbls[i] as usize == c { 1.0 } else { -1.0 };
            dv.push(Data::new_training_data(row.clone(), 1.0, label, None));
        }
        let mut m = GBDT::new(&cfg);
        m.fit(&mut dv);
        models.push(m);
    }
    models
}

fn eval_trees(models: &[GBDT], test_feats: &[Vec<f32>], test_lbls: &[u8]) -> AccResult {
    let mut preds = vec![0u8; test_feats.len()];
    for c in 0..COUNTRIES.len() {
        let mut tdv: DataVec = Vec::with_capacity(test_feats.len());
        for row in test_feats {
            tdv.push(Data::new_test_data(row.clone(), None));
        }
        let pp = models[c].predict(&tdv);
        if c == 0 {
            // initialise with first
            for (i, p) in pp.iter().enumerate() {
                preds[i] = (*p as f32).to_bits() as u8 ^ 255; // placeholder; will overwrite below
                let _ = preds[i];
                preds[i] = 0;
            }
        }
        // For each test row, accumulate score; pick argmax later.
        // Simpler: do all predicts in 2D.
        // Actually we'll do it the simple way:
        if c == 0 {
            // store all scores in flat structure
        }
    }
    // Redo with 2D collection
    let mut all_scores: Vec<Vec<f32>> = vec![vec![0.0; test_feats.len()]; COUNTRIES.len()];
    for c in 0..COUNTRIES.len() {
        let mut tdv: DataVec = Vec::with_capacity(test_feats.len());
        for row in test_feats {
            tdv.push(Data::new_test_data(row.clone(), None));
        }
        let pp = models[c].predict(&tdv);
        for (i, p) in pp.iter().enumerate() {
            all_scores[c][i] = *p as f32;
        }
    }
    for i in 0..test_feats.len() {
        let mut best_c = 0;
        let mut best_s = f32::NEG_INFINITY;
        for c in 0..COUNTRIES.len() {
            if all_scores[c][i] > best_s {
                best_s = all_scores[c][i];
                best_c = c;
            }
        }
        preds[i] = best_c as u8;
    }
    compute_acc(&preds, test_lbls)
}

fn bench_trees(models: &[GBDT], test_feats: &[Vec<f32>], iters: usize) -> LatencyStats {
    // Single-row inference latency
    let mut samples: Vec<f64> = Vec::with_capacity(iters);
    let n_test = test_feats.len();
    let mut idx = 0usize;
    // Warmup
    for _ in 0..1024 {
        let row = &test_feats[idx % n_test];
        idx += 1;
        let mut tdv: DataVec = vec![Data::new_test_data(row.clone(), None)];
        for c in 0..COUNTRIES.len() {
            let p = models[c].predict(&tdv);
            std::hint::black_box(p);
            tdv[0] = Data::new_test_data(row.clone(), None);
        }
    }
    for _ in 0..iters {
        let row = &test_feats[idx % n_test];
        idx += 1;
        let t0 = Instant::now();
        let tdv: DataVec = vec![Data::new_test_data(row.clone(), None)];
        let mut best_s = f32::NEG_INFINITY;
        let mut best_c = 0u8;
        for c in 0..COUNTRIES.len() {
            let p = models[c].predict(&tdv);
            if p[0] as f32 > best_s {
                best_s = p[0] as f32;
                best_c = c as u8;
            }
        }
        let dt = t0.elapsed().as_nanos() as f64;
        std::hint::black_box(best_c);
        samples.push(dt);
    }
    percentiles(samples)
}

fn estimate_tree_memory(models: &[GBDT]) -> usize {
    // Rough: serialise to JSON and divide by 1024.
    // gbdt 0.1.3 has save_model to file.
    let path = std::env::temp_dir().join("ablation_tree.tmp");
    if let Err(_) = models[0].save_model(path.to_str().unwrap()) {
        return 0;
    }
    let mut sum = 0usize;
    for (i, m) in models.iter().enumerate() {
        let p = std::env::temp_dir().join(format!("ablation_tree_{}.tmp", i));
        if m.save_model(p.to_str().unwrap()).is_ok() {
            if let Ok(meta) = std::fs::metadata(&p) {
                sum += meta.len() as usize;
            }
            let _ = std::fs::remove_file(&p);
        }
    }
    sum / 1024
}

// ---------- B: signature table ----------

#[derive(Default)]
struct SignatureTable {
    /// signature → posterior counts per country (size 15)
    map: HashMap<u64, [u32; 15]>,
    /// fallback prior (overall country distribution)
    prior: [u32; 15],
}

const SIG_BUCKETS_CONT: u32 = 4;

fn quantise_row(row: &[f32], mins: &[f32], maxs: &[f32]) -> u64 {
    // u64 hash via FNV-1a-like over per-feature small integers.
    // We mix in the bucket index (0..3 for continuous, 0/1 for binary).
    let mut h: u64 = 0xcbf29ce484222325;
    for (i, &v) in row.iter().enumerate() {
        let mn = mins[i];
        let mx = maxs[i];
        let bucket: u32 = if mn >= mx {
            0
        } else if (mx - mn).abs() < 1.5 && mn >= 0.0 {
            // binary
            if v >= 0.5 { 1 } else { 0 }
        } else {
            let t = ((v - mn) / (mx - mn)).clamp(0.0, 1.0);
            let b = (t * SIG_BUCKETS_CONT as f32).floor() as u32;
            b.min(SIG_BUCKETS_CONT - 1)
        };
        h ^= bucket as u64;
        h = h.wrapping_mul(0x100000001b3);
    }
    h
}

fn compute_min_max(rows: &[Vec<f32>], n: usize) -> (Vec<f32>, Vec<f32>) {
    let mut mn = vec![f32::INFINITY; n];
    let mut mx = vec![f32::NEG_INFINITY; n];
    for row in rows {
        for (i, &v) in row.iter().enumerate() {
            if v < mn[i] {
                mn[i] = v;
            }
            if v > mx[i] {
                mx[i] = v;
            }
        }
    }
    (mn, mx)
}

fn build_signature_table(
    rows: &[Vec<f32>],
    labels: &[u8],
    mins: &[f32],
    maxs: &[f32],
) -> SignatureTable {
    let mut t = SignatureTable::default();
    for (i, row) in rows.iter().enumerate() {
        let sig = quantise_row(row, mins, maxs);
        let entry = t.map.entry(sig).or_insert([0u32; 15]);
        entry[labels[i] as usize] = entry[labels[i] as usize].saturating_add(1);
        t.prior[labels[i] as usize] = t.prior[labels[i] as usize].saturating_add(1);
    }
    t
}

fn predict_table(t: &SignatureTable, row: &[f32], mins: &[f32], maxs: &[f32]) -> u8 {
    let sig = quantise_row(row, mins, maxs);
    if let Some(counts) = t.map.get(&sig) {
        argmax_15(counts)
    } else {
        argmax_15(&t.prior)
    }
}

fn argmax_15(counts: &[u32; 15]) -> u8 {
    let mut best = 0usize;
    let mut bv = counts[0];
    for i in 1..15 {
        if counts[i] > bv {
            bv = counts[i];
            best = i;
        }
    }
    best as u8
}

fn eval_table(
    t: &SignatureTable,
    test: &[Vec<f32>],
    truth: &[u8],
    mins: &[f32],
    maxs: &[f32],
) -> AccResult {
    let mut preds = Vec::with_capacity(test.len());
    for row in test {
        preds.push(predict_table(t, row, mins, maxs));
    }
    compute_acc(&preds, truth)
}

fn bench_table(
    t: &SignatureTable,
    test: &[Vec<f32>],
    mins: &[f32],
    maxs: &[f32],
    iters: usize,
) -> LatencyStats {
    let n_test = test.len();
    let mut idx = 0;
    // Warmup
    for _ in 0..1024 {
        let p = predict_table(t, &test[idx % n_test], mins, maxs);
        std::hint::black_box(p);
        idx += 1;
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let row = &test[idx % n_test];
        idx += 1;
        let t0 = Instant::now();
        let p = predict_table(t, row, mins, maxs);
        let dt = t0.elapsed().as_nanos() as f64;
        std::hint::black_box(p);
        samples.push(dt);
    }
    percentiles(samples)
}

fn estimate_table_memory(t: &SignatureTable) -> usize {
    // 8 (key) + 60 (values) per entry = 68 bytes; HashMap overhead ~+50%.
    let per_entry = 8 + std::mem::size_of::<[u32; 15]>();
    let raw = t.map.len() * per_entry;
    (raw + raw / 2) / 1024
}

// ---------- C: cost-aware cascade ----------

struct Cascade {
    /// For each binary feature index (in the active-feature space),
    /// (country, posterior) if presence locks that country.  Map from
    /// active-feature-index → (country_idx, p_country_given_feature).
    locks: HashMap<usize, (u8, f64)>,
    /// Prior over countries for the residual case.
    prior: [u32; 15],
    /// Order in which active classes are checked (cheapest first).
    /// Each entry contains the active-feature-index range.
    class_order: Vec<(FeatureClass, usize, usize)>,
    /// Residual fallback: signature table on the FULL knee feature set.
    fallback: SignatureTable,
    fallback_mins: Vec<f32>,
    fallback_maxs: Vec<f32>,
}

fn build_cascade(
    train: &[Vec<f32>],
    labels: &[u8],
    active_classes: &[FeatureClass],
    active_indices: &[usize],
    class_ranges: &[(FeatureClass, usize, usize)],
) -> Cascade {
    // Build a map from full-feature-index → active-feature-index
    let mut active_pos: HashMap<usize, usize> = HashMap::new();
    for (i, &ai) in active_indices.iter().enumerate() {
        active_pos.insert(ai, i);
    }
    // Order active classes by ascending Phase-1 cost (cheapest first).
    // We use the same fixed mapping as our measured Phase-1 numbers.
    let class_cost = |c: FeatureClass| -> f64 {
        match c {
            FeatureClass::Length => 54.3,
            FeatureClass::Marker => 58.9,
            FeatureClass::Script => 78.0,
            FeatureClass::Digit => 124.5,
            FeatureClass::Punct => 134.3,
            FeatureClass::Bigram => 252.4,
            FeatureClass::Postcode => 1670.1,
        }
    };
    let mut class_order: Vec<(FeatureClass, usize, usize)> = Vec::new();
    let mut sorted = active_classes.to_vec();
    sorted.sort_by(|a, b| class_cost(*a).partial_cmp(&class_cost(*b)).unwrap());
    for c in sorted {
        // Find the range in the FULL feature space
        for (cc, s, e) in class_ranges {
            if *cc == c {
                // Map to active-feature range. Active ranges are
                // contiguous because we add classes in their ext order.
                let ams = active_pos[&*s];
                let ame = active_pos
                    .get(&(e - 1))
                    .map(|&v| v + 1)
                    .unwrap_or(*e - *s + ams);
                class_order.push((c, ams, ame));
                break;
            }
        }
    }

    // For each binary feature in the active space (mn=0, mx=1 typically),
    // count how many countries see it set to 1.
    let n_active = active_indices.len();
    let mut country_seen: Vec<[u32; 15]> = vec![[0u32; 15]; n_active];
    let mut total_seen: Vec<u32> = vec![0u32; n_active];
    let mut prior = [0u32; 15];
    for (i, row) in train.iter().enumerate() {
        for (j, &v) in row.iter().enumerate() {
            if v > 0.5 {
                country_seen[j][labels[i] as usize] += 1;
                total_seen[j] += 1;
            }
        }
        prior[labels[i] as usize] += 1;
    }
    let n_train_total: u32 = prior.iter().sum();
    let mut locks: HashMap<usize, (u8, f64)> = HashMap::new();
    for j in 0..n_active {
        if total_seen[j] < 50 {
            continue; // not enough evidence
        }
        let mut nonzero_countries = 0;
        let mut best_c = 0u8;
        let mut best_count = 0u32;
        for c in 0..15 {
            if country_seen[j][c] > 0 {
                nonzero_countries += 1;
            }
            if country_seen[j][c] > best_count {
                best_count = country_seen[j][c];
                best_c = c as u8;
            }
        }
        let posterior = best_count as f64 / total_seen[j] as f64;
        // "Lock" if posterior ≥ 0.95 AND seen at least 50 times
        if posterior >= 0.95 && nonzero_countries <= 3 {
            locks.insert(j, (best_c, posterior));
        }
    }
    let _ = n_train_total;
    eprintln!("    cascade locks: {} active-feature indices", locks.len());

    // Fallback: signature table over the active features.
    let (mns, mxs) = compute_min_max(train, n_active);
    let fallback = build_signature_table(train, labels, &mns, &mxs);
    let _ = active_pos; // suppress unused warning when class_ranges path taken

    Cascade {
        locks,
        prior,
        class_order,
        fallback,
        fallback_mins: mns,
        fallback_maxs: mxs,
    }
}

fn predict_cascade(c: &Cascade, row: &[f32]) -> u8 {
    // Walk active features in the cheapest-class-first order.
    // If a lock fires (binary feature == 1 and that index is in `locks`),
    // return that country immediately.
    for (_cls, s, e) in &c.class_order {
        for j in *s..*e {
            if row[j] > 0.5 {
                if let Some(&(country, _)) = c.locks.get(&j) {
                    return country;
                }
            }
        }
    }
    // Fallback to signature table on the full row
    predict_table(&c.fallback, row, &c.fallback_mins, &c.fallback_maxs)
}

fn eval_cascade(
    c: &Cascade,
    test: &[Vec<f32>],
    truth: &[u8],
    _ac: &[FeatureClass],
    _ai: &[usize],
    _cr: &[(FeatureClass, usize, usize)],
) -> AccResult {
    let mut preds = Vec::with_capacity(test.len());
    for row in test {
        preds.push(predict_cascade(c, row));
    }
    compute_acc(&preds, truth)
}

fn bench_cascade(
    c: &Cascade,
    test: &[Vec<f32>],
    _ac: &[FeatureClass],
    _ai: &[usize],
    _cr: &[(FeatureClass, usize, usize)],
    iters: usize,
) -> LatencyStats {
    // Warmup
    let n_test = test.len();
    let mut idx = 0;
    for _ in 0..1024 {
        let p = predict_cascade(c, &test[idx % n_test]);
        std::hint::black_box(p);
        idx += 1;
    }
    let mut samples = Vec::with_capacity(iters);
    for _ in 0..iters {
        let row = &test[idx % n_test];
        idx += 1;
        let t0 = Instant::now();
        let p = predict_cascade(c, row);
        let dt = t0.elapsed().as_nanos() as f64;
        std::hint::black_box(p);
        samples.push(dt);
    }
    percentiles(samples)
}

fn estimate_cascade_memory(c: &Cascade) -> usize {
    let locks_kb = c.locks.len() * (8 + 16) / 1024;
    let table_kb = estimate_table_memory(&c.fallback);
    locks_kb + table_kb
}

// ---------- subset bin loader (shared with Phase 3) ----------

fn load_subset(
    path: &PathBuf,
    n_features_expected: usize,
) -> Result<(Vec<f32>, Vec<u8>, usize)> {
    let mut f = std::fs::File::open(path).context("open subset bin")?;
    let mut hdr = [0u8; 12];
    f.read_exact(&mut hdr)?;
    let nf = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
    let _nc = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as usize;
    let nr = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
    if nf != n_features_expected {
        anyhow::bail!(
            "feature count mismatch: spec={} subset_bin={}",
            n_features_expected,
            nf
        );
    }
    let mut features = Vec::with_capacity(nr * nf);
    let mut countries = Vec::with_capacity(nr);
    let mut buf = vec![0u8; 1 + nf * 4];
    for _ in 0..nr {
        f.read_exact(&mut buf)?;
        countries.push(buf[0]);
        for k in 0..nf {
            let off = 1 + k * 4;
            features.push(f32::from_le_bytes(buf[off..off + 4].try_into().unwrap()));
        }
    }
    Ok((features, countries, nr))
}

fn cap_per_country(
    flat: &[f32],
    country: &[u8],
    n_features: usize,
    cap: usize,
) -> (Vec<f32>, Vec<u8>) {
    let mut counts = vec![0usize; COUNTRIES.len()];
    let mut keep = Vec::with_capacity(country.len());
    for (i, &c) in country.iter().enumerate() {
        if (c as usize) < counts.len() && counts[c as usize] < cap {
            keep.push(i);
            counts[c as usize] += 1;
        }
    }
    let mut sf = Vec::with_capacity(keep.len() * n_features);
    let mut sc = Vec::with_capacity(keep.len());
    for &i in &keep {
        let off = i * n_features;
        sf.extend_from_slice(&flat[off..off + n_features]);
        sc.push(country[i]);
    }
    (sf, sc)
}
