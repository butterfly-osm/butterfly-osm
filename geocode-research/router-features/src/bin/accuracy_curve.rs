//! Phase 3 — Accuracy vs feature classes added in cost-aware order.
//!
//! Strategy:
//!   1. Sort feature classes by total-MI / class-cost-ns descending.
//!      The selection unit is the CLASS (paying for one byte-pass
//!      produces every feature in the class).
//!   2. Iteratively add classes. After each addition, train a 15-way
//!      classifier (one-vs-rest with gbdt's LogLikelyhood binary
//!      loss; n_estimators=1, max_depth=12) and evaluate top-1
//!      accuracy on a held-out test set:
//!        - macro-averaged across the 15 countries
//!        - per-country breakdown
//!        - per-family breakdown
//!   3. Stop when accuracy plateaus (Δ macro < 0.5 % for 3 consecutive
//!      additions) OR all classes added.
//!
//! Loads the per-country balanced subset written by `measure-features`
//! (300 k records × 391 f32 features). 80/20 train/test split,
//! deterministic by seeded shuffle.

use anyhow::{Context, Result};
use clap::Parser;
use gbdt::config::Config;
use gbdt::decision_tree::{Data, DataVec};
use gbdt::gradient_boost::GBDT;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use router_features::{country_index, family_for, ExtractorSpec, FeatureClass, COUNTRIES};
use serde::Serialize;
use std::collections::HashMap;
use std::io::{Read, Write};
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long, default_value = "geocode-research/router-features/artifacts/extractor.spec.json")]
    extractor_spec: PathBuf,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/features-subset.bin")]
    features_bin: PathBuf,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/features.tsv")]
    features_tsv: PathBuf,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/bench-extraction.json")]
    bench_json: PathBuf,
    /// Output curve TSV (overall macro accuracy).
    #[arg(long, default_value = "geocode-research/router-features/artifacts/accuracy_vs_features.tsv")]
    out_overall: PathBuf,
    /// Output curve TSV (per-country accuracy).
    #[arg(long, default_value = "geocode-research/router-features/artifacts/accuracy_vs_features_per_country.tsv")]
    out_per_country: PathBuf,
    /// Output curve TSV (per-family accuracy).
    #[arg(long, default_value = "geocode-research/router-features/artifacts/accuracy_vs_features_per_family.tsv")]
    out_per_family: PathBuf,
    /// Save the chosen-knee classifier set so Phase 4 can reuse it.
    #[arg(long, default_value = "geocode-research/router-features/artifacts/knee-classifier.json")]
    out_knee_meta: PathBuf,
    /// 80/20 split — fraction of data used for training.
    #[arg(long, default_value = "0.8")]
    train_frac: f32,
    /// Cap rows used per country to bound runtime (set high to use all).
    #[arg(long, default_value = "20000")]
    cap_per_country: usize,
    #[arg(long, default_value = "12")]
    max_depth: u32,
    #[arg(long, default_value = "42")]
    seed: u64,
    /// Plateau detection: stop after N consecutive additions with
    /// macro accuracy delta below `plateau_delta`.
    #[arg(long, default_value = "3")]
    plateau_window: usize,
    #[arg(long, default_value = "0.005")]
    plateau_delta: f64,
}

#[derive(Serialize)]
struct CurveRow {
    step: u32,
    classes_added: String,
    n_features: usize,
    cumulative_cost_ns: f64,
    macro_top1: f64,
    train_rows: usize,
    test_rows: usize,
    train_seconds: f64,
}

fn main() -> Result<()> {
    let args = Args::parse();
    eprintln!("[load] extractor spec");
    let spec = ExtractorSpec::load(&args.extractor_spec)?;
    let ex = spec.build();
    let n_total = ex.n_features();

    eprintln!("[load] feature subset {}", args.features_bin.display());
    let (rows_features, rows_country, n_records) = load_subset(&args.features_bin, n_total)?;
    eprintln!(
        "[load] {} records × {} features (assumed; capping {} per country)",
        n_records, n_total, args.cap_per_country
    );

    // Load class costs from bench-extraction.json
    let bench: serde_json::Value =
        serde_json::from_str(&std::fs::read_to_string(&args.bench_json)?)?;
    let mut class_cost: HashMap<String, f64> = HashMap::new();
    for entry in bench["per_class"].as_array().unwrap() {
        class_cost.insert(
            entry["class"].as_str().unwrap().to_string(),
            entry["ns_per_call"].as_f64().unwrap(),
        );
    }

    // Compute per-class total MI from features.tsv (need to sort classes).
    eprintln!("[rank] reading features.tsv");
    let mut class_mi: HashMap<String, f64> = HashMap::new();
    let tsv = std::fs::read_to_string(&args.features_tsv)?;
    for (i, line) in tsv.lines().enumerate() {
        if i == 0 {
            continue;
        }
        let cols: Vec<&str> = line.split('\t').collect();
        if cols.len() < 5 {
            continue;
        }
        let cls = cols[1].to_string();
        let mi: f64 = cols[4].parse().unwrap_or(0.0);
        // Use the maximum per-feature MI summed: actually we want total class MI which
        // we already computed in Phase 2 (class summary). Reconstruct by summing.
        *class_mi.entry(cls).or_default() += mi;
    }
    let mut classes: Vec<FeatureClass> = FeatureClass::all().to_vec();
    classes.sort_by(|a, b| {
        let ka = class_mi.get(a.as_str()).copied().unwrap_or(0.0)
            / class_cost.get(a.as_str()).copied().unwrap_or(1.0);
        let kb = class_mi.get(b.as_str()).copied().unwrap_or(0.0)
            / class_cost.get(b.as_str()).copied().unwrap_or(1.0);
        kb.partial_cmp(&ka).unwrap()
    });
    eprintln!("[rank] class addition order:");
    for c in &classes {
        let mi = class_mi.get(c.as_str()).copied().unwrap_or(0.0);
        let cost = class_cost.get(c.as_str()).copied().unwrap_or(0.0);
        let r = if cost > 0.0 { mi / cost } else { 0.0 };
        eprintln!(
            "       {:8}  total-MI={:.4}  cost={:.1} ns  MI/ns={:.4e}",
            c.as_str(),
            mi,
            cost,
            r
        );
    }

    // Apply per-country cap (downsample for speed; full 300k is fine but
    // 20k × 15 = 300k anyway, no actual cap if subset already balanced).
    let (sampled_features, sampled_country) =
        cap_per_country(&rows_features, &rows_country, n_total, args.cap_per_country);
    eprintln!(
        "[sample] {} records after per-country cap (each country ≤ {})",
        sampled_country.len(),
        args.cap_per_country
    );

    // Train/test split
    let mut rng = rand::rngs::StdRng::seed_from_u64(args.seed);
    let n = sampled_country.len();
    let mut perm: Vec<usize> = (0..n).collect();
    perm.shuffle(&mut rng);
    let n_train = (n as f32 * args.train_frac) as usize;
    let train_idx: Vec<usize> = perm[..n_train].to_vec();
    let test_idx: Vec<usize> = perm[n_train..].to_vec();
    eprintln!("[split] train={}, test={}", train_idx.len(), test_idx.len());

    // Class-range map
    let class_ranges = ex.class_ranges();
    let class_range_of = |c: FeatureClass| -> (usize, usize) {
        for (cc, s, e) in &class_ranges {
            if *cc == c {
                return (*s, *e);
            }
        }
        unreachable!()
    };

    // Iteratively add classes
    let mut overall_rows: Vec<CurveRow> = Vec::new();
    let mut per_country_rows: Vec<(u32, String, &'static str, f64)> = Vec::new();
    let mut per_family_rows: Vec<(u32, String, String, f64, usize)> = Vec::new();

    let mut active_classes: Vec<FeatureClass> = Vec::new();
    let mut active_indices: Vec<usize> = Vec::new();
    let mut cumulative_cost = 0.0f64;
    let mut last_acc = 0.0f64;
    let mut plateau_run = 0usize;
    let mut step: u32 = 0;
    let mut chosen_knee_step = 0u32;
    let mut chosen_knee_features: Vec<usize> = Vec::new();
    let mut best_acc = 0.0f64;

    for class in &classes {
        active_classes.push(*class);
        let (s, e) = class_range_of(*class);
        for i in s..e {
            active_indices.push(i);
        }
        cumulative_cost += class_cost.get(class.as_str()).copied().unwrap_or(0.0);
        step += 1;

        eprintln!(
            "\n=== step {} (class={:?}, {} features active, cost={:.1} ns) ===",
            step,
            active_classes.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
            active_indices.len(),
            cumulative_cost
        );

        let t0 = std::time::Instant::now();
        // Build training DataVec for one-vs-rest (we'll do 15 separate fits inside)
        let train_data = build_view(&sampled_features, &train_idx, &active_indices, n_total);
        let train_labels: Vec<i32> = train_idx
            .iter()
            .map(|&i| sampled_country[i] as i32)
            .collect();
        let test_data = build_view(&sampled_features, &test_idx, &active_indices, n_total);
        let test_labels: Vec<i32> = test_idx
            .iter()
            .map(|&i| sampled_country[i] as i32)
            .collect();
        let n_feats_now = active_indices.len();

        // Train 15 one-vs-rest binary trees (1 tree each, depth=max_depth)
        let mut models: Vec<GBDT> = Vec::with_capacity(COUNTRIES.len());
        for c in 0..COUNTRIES.len() {
            let mut cfg = Config::new();
            cfg.set_feature_size(n_feats_now);
            cfg.set_max_depth(args.max_depth);
            cfg.set_iterations(1);
            cfg.set_shrinkage(0.1);
            cfg.set_loss("LogLikelyhood");
            cfg.set_training_optimization_level(2);
            cfg.set_min_leaf_size(20);

            let mut dv: DataVec = Vec::with_capacity(train_data.len());
            for (row_i, row) in train_data.iter().enumerate() {
                let label = if train_labels[row_i] == c as i32 {
                    1.0
                } else {
                    -1.0
                };
                dv.push(Data::new_training_data(row.clone(), 1.0, label, None));
            }
            let mut model = GBDT::new(&cfg);
            model.fit(&mut dv);
            models.push(model);
        }
        let train_seconds = t0.elapsed().as_secs_f64();

        // Predict — for each test row, get score from each model, take argmax.
        let mut correct_per_country = vec![0u64; COUNTRIES.len()];
        let mut total_per_country = vec![0u64; COUNTRIES.len()];
        let mut correct_total = 0u64;

        // Build a single PredVec for each model in turn.
        let mut all_scores: Vec<Vec<f32>> = vec![vec![0.0; test_data.len()]; COUNTRIES.len()];
        for c in 0..COUNTRIES.len() {
            let mut tdv: DataVec = Vec::with_capacity(test_data.len());
            for row in &test_data {
                tdv.push(Data::new_test_data(row.clone(), None));
            }
            let preds = models[c].predict(&tdv);
            for (i, p) in preds.iter().enumerate() {
                all_scores[c][i] = *p as f32;
            }
        }
        for i in 0..test_data.len() {
            let mut best_c = 0;
            let mut best_s = f32::NEG_INFINITY;
            for c in 0..COUNTRIES.len() {
                let s = all_scores[c][i];
                if s > best_s {
                    best_s = s;
                    best_c = c;
                }
            }
            let true_c = test_labels[i] as usize;
            total_per_country[true_c] += 1;
            if best_c == true_c {
                correct_per_country[true_c] += 1;
                correct_total += 1;
            }
        }

        let macro_acc: f64 = (0..COUNTRIES.len())
            .map(|c| {
                if total_per_country[c] == 0 {
                    0.0
                } else {
                    correct_per_country[c] as f64 / total_per_country[c] as f64
                }
            })
            .sum::<f64>()
            / COUNTRIES.len() as f64;
        let micro_acc = correct_total as f64 / test_data.len() as f64;

        eprintln!(
            "    macro top-1 = {:.4}, micro top-1 = {:.4}, train {:.2}s",
            macro_acc, micro_acc, train_seconds
        );

        for c in 0..COUNTRIES.len() {
            let acc = if total_per_country[c] == 0 {
                0.0
            } else {
                correct_per_country[c] as f64 / total_per_country[c] as f64
            };
            eprintln!("       {} acc={:.4} (n={})", COUNTRIES[c], acc, total_per_country[c]);
            per_country_rows.push((step, format!("{:?}", active_classes), COUNTRIES[c], acc));
        }
        // Per-family breakdown
        let mut fam_correct: HashMap<&'static str, (u64, u64)> = HashMap::new();
        for c in 0..COUNTRIES.len() {
            let fam = family_for(COUNTRIES[c]);
            let e = fam_correct.entry(fam).or_default();
            e.0 += correct_per_country[c];
            e.1 += total_per_country[c];
        }
        for (fam, (cor, tot)) in &fam_correct {
            let a = if *tot == 0 { 0.0 } else { *cor as f64 / *tot as f64 };
            eprintln!("       fam={} acc={:.4} (n={})", fam, a, tot);
            per_family_rows.push((step, format!("{:?}", active_classes), (*fam).to_string(), a, *tot as usize));
        }

        overall_rows.push(CurveRow {
            step,
            classes_added: active_classes.iter().map(|c| c.as_str()).collect::<Vec<_>>().join(","),
            n_features: active_indices.len(),
            cumulative_cost_ns: cumulative_cost,
            macro_top1: macro_acc,
            train_rows: train_data.len(),
            test_rows: test_data.len(),
            train_seconds,
        });

        // Plateau detection
        let delta = macro_acc - last_acc;
        if delta < args.plateau_delta {
            plateau_run += 1;
        } else {
            plateau_run = 0;
        }
        if macro_acc > best_acc {
            best_acc = macro_acc;
            chosen_knee_step = step;
            chosen_knee_features = active_indices.clone();
        }
        last_acc = macro_acc;
        if plateau_run >= args.plateau_window {
            eprintln!(
                "\n[plateau] macro accuracy plateau detected after step {} (Δ < {} for {} consecutive)",
                step, args.plateau_delta, args.plateau_window
            );
            break;
        }
    }

    // Write outputs
    eprintln!("\n[out] writing {}", args.out_overall.display());
    let mut w = std::io::BufWriter::new(std::fs::File::create(&args.out_overall)?);
    writeln!(
        w,
        "step\tclasses_added\tn_features\tcumulative_cost_ns\tmacro_top1\ttrain_rows\ttest_rows\ttrain_seconds"
    )?;
    for r in &overall_rows {
        writeln!(
            w,
            "{}\t{}\t{}\t{:.2}\t{:.6}\t{}\t{}\t{:.3}",
            r.step,
            r.classes_added,
            r.n_features,
            r.cumulative_cost_ns,
            r.macro_top1,
            r.train_rows,
            r.test_rows,
            r.train_seconds
        )?;
    }
    drop(w);

    let mut w = std::io::BufWriter::new(std::fs::File::create(&args.out_per_country)?);
    writeln!(w, "step\tclasses_active\tcountry\tacc")?;
    for (s, c, country, acc) in &per_country_rows {
        writeln!(w, "{}\t{}\t{}\t{:.6}", s, c, country, acc)?;
    }
    drop(w);

    let mut w = std::io::BufWriter::new(std::fs::File::create(&args.out_per_family)?);
    writeln!(w, "step\tclasses_active\tfamily\tacc\tn")?;
    for (s, c, fam, a, n) in &per_family_rows {
        writeln!(w, "{}\t{}\t{}\t{:.6}\t{}", s, c, fam, a, n)?;
    }
    drop(w);

    // Save knee meta for Phase 4
    let knee_meta = serde_json::json!({
        "knee_step": chosen_knee_step,
        "knee_n_features": chosen_knee_features.len(),
        "knee_feature_indices": chosen_knee_features,
        "knee_macro_acc": best_acc,
        "class_addition_order": classes.iter().map(|c| c.as_str()).collect::<Vec<_>>(),
    });
    std::fs::write(&args.out_knee_meta, serde_json::to_string_pretty(&knee_meta)?)?;
    eprintln!(
        "[knee] step {} with {} features, macro_acc={:.4}; saved to {}",
        chosen_knee_step,
        chosen_knee_features.len(),
        best_acc,
        args.out_knee_meta.display()
    );

    Ok(())
}

fn load_subset(
    path: &PathBuf,
    n_features_expected: usize,
) -> Result<(Vec<f32>, Vec<u8>, usize)> {
    let mut f = std::fs::File::open(path).context("open subset bin")?;
    let mut hdr = [0u8; 12];
    f.read_exact(&mut hdr)?;
    let nf = u32::from_le_bytes(hdr[0..4].try_into().unwrap()) as usize;
    let nc = u32::from_le_bytes(hdr[4..8].try_into().unwrap()) as usize;
    let nr = u32::from_le_bytes(hdr[8..12].try_into().unwrap()) as usize;
    eprintln!(
        "[load] subset bin header: {} features × {} countries × {} records",
        nf, nc, nr
    );
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
            features.push(f32::from_le_bytes(
                buf[off..off + 4].try_into().unwrap(),
            ));
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

fn build_view(
    flat: &[f32],
    rows: &[usize],
    cols: &[usize],
    n_total: usize,
) -> Vec<Vec<f32>> {
    rows.iter()
        .map(|&r| {
            let off = r * n_total;
            cols.iter().map(|&c| flat[off + c]).collect()
        })
        .collect()
}

// helper for the country labels on the country code
#[allow(dead_code)]
fn _country_iso(idx: u8) -> &'static str {
    COUNTRIES.get(idx as usize).copied().unwrap_or("??")
}

// Avoid unused import if ever
#[allow(dead_code)]
fn _force_use(s: &str) -> Option<usize> {
    country_index(s)
}
