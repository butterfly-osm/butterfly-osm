//! Phase 2 — Per-feature mutual information + cost-aware ranking.
//!
//! For each candidate feature:
//!   - read all corpus records (3.5M)
//!   - extract features
//!   - bin continuous features into 10 buckets (per-feature quantiles)
//!   - compute MI(country, feature) using empirical contingency tables
//!   - cost is the per-class extraction cost from Phase 1, divided
//!     across the class's features (one byte-pass produces all of them)
//!
//! Output: features.tsv with columns
//!   feature_name | class | n_features_in_class | mutual_information |
//!   class_cost_ns | per_feature_class_cost_ns | ig_per_class_ns | rank
//!
//! NB: ranking is by IG-per-class because the unit of selection is the
//! class. Within a class, features are ranked by raw MI (since paying
//! for the class produces them all).

use anyhow::{Context, Result};
use clap::Parser;
use router_features::corpus::CorpusReader;
use router_features::{ExtractorSpec, FeatureClass, country_index};
use std::path::PathBuf;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    corpus: PathBuf,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/extractor.spec.json")]
    extractor_spec: PathBuf,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/bench-extraction.json")]
    bench_json: PathBuf,
    /// Cap on records read (debug). 0 = full corpus.
    #[arg(long, default_value = "0")]
    max_records: usize,
    /// Number of bins for continuous features.
    #[arg(long, default_value = "10")]
    bins: usize,
    #[arg(long, default_value = "geocode-research/router-features/artifacts/features.tsv")]
    out: PathBuf,
    /// Save a per-country-balanced subset of feature vectors + labels
    /// for Phase 3 reuse (avoids re-extracting). Empty = skip.
    #[arg(long, default_value = "geocode-research/router-features/artifacts/features-subset.bin")]
    save_features: PathBuf,
    /// Per-country cap for the saved subset (each country contributes up
    /// to this many records, balanced).
    #[arg(long, default_value = "20000")]
    save_per_country: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let spec = ExtractorSpec::load(&args.extractor_spec).context("load spec")?;
    let ex = spec.build();
    eprintln!(
        "[measure] {} features across {} classes",
        ex.n_features(),
        FeatureClass::all().len()
    );

    let bench: serde_json::Value = serde_json::from_str(
        &std::fs::read_to_string(&args.bench_json).context("read bench json")?,
    )?;
    let mut class_cost: std::collections::HashMap<String, f64> = std::collections::HashMap::new();
    for entry in bench["per_class"].as_array().context("per_class")? {
        let name = entry["class"].as_str().unwrap().to_string();
        let cost = entry["ns_per_call"].as_f64().unwrap();
        class_cost.insert(name, cost);
    }

    // PASS 1: stream corpus, extract, accumulate (a) per-feature
    // continuous values for quantile binning, (b) running country
    // marginal counts.
    eprintln!("[pass-1] streaming corpus to extract feature matrix...");
    let n = ex.n_features();
    let n_countries = router_features::COUNTRIES.len();

    // We need quantile bins for continuous features.  Strategy:
    // sample up to 200k rows for quantile computation, then a second
    // pass over the full corpus for the contingency tables.
    //
    // Rather than two passes through the 720MB file, do a single
    // streaming pass: collect ALL features and labels in memory.
    // 3.5M × 391 × 4B = 5.4 GB — too much.  Use a single pass with
    // an early subsample for quantiles, then a second pass for MI.
    //
    // Simpler: stream once, build per-feature histograms with 256
    // fixed-width bins after determining per-feature [min,max] from
    // first 100k records.  Two passes total.

    // PASS A — sample first 200k records to compute per-feature min/max for binning.
    let sample_cap: usize = 200_000;
    let mut feat_min = vec![f32::INFINITY; n];
    let mut feat_max = vec![f32::NEG_INFINITY; n];
    let mut sampled = 0usize;
    let reader = CorpusReader::open(&args.corpus)?;
    let mut total_seen = 0u64;
    for rec in reader.into_iter() {
        let r = rec?;
        if country_index(&r.country).is_none() {
            continue;
        }
        let f = ex.extract(&r.text);
        for (i, &v) in f.iter().enumerate() {
            if v < feat_min[i] {
                feat_min[i] = v;
            }
            if v > feat_max[i] {
                feat_max[i] = v;
            }
        }
        sampled += 1;
        total_seen += 1;
        if sampled >= sample_cap {
            break;
        }
    }
    eprintln!(
        "[pass-1A] sampled {} records for min/max",
        sampled
    );

    // For binary features (min=max=0 or 1), keep 2 bins.
    // For continuous: bins = args.bins, equal-width.
    // Edge case: feature is constant (min==max) → 1 bin.
    let bins = args.bins.max(2);
    let mut n_bins = vec![bins as u32; n];
    for i in 0..n {
        if !feat_min[i].is_finite() || !feat_max[i].is_finite() || feat_min[i] >= feat_max[i] {
            n_bins[i] = 1;
        } else if (feat_max[i] - feat_min[i]).abs() < 1.5 && feat_min[i] >= 0.0 {
            // Likely binary (0/1) — 2 bins suffice.
            n_bins[i] = 2;
        }
    }
    let total_binary = n_bins.iter().filter(|&&b| b <= 2).count();
    eprintln!(
        "[pass-1A] {} binary, {} continuous features",
        total_binary,
        n - total_binary
    );

    // PASS B — full corpus stream, build joint country-feature contingency.
    // For each feature: counts[country][bin] (small: 15 × bins).
    eprintln!("[pass-1B] streaming corpus for contingency tables...");
    let max_bins = bins as usize;
    // Flat layout: counts[feat * n_countries * max_bins + c * max_bins + b]
    let mut counts: Vec<u64> = vec![0u64; n * n_countries * max_bins];
    let mut country_total: Vec<u64> = vec![0u64; n_countries];
    let mut total: u64 = 0;

    // Optional: write features-subset.bin (raw per-record feature vectors + label).
    // Layout header: [u32 n_features][u32 n_countries][u32 n_records]
    // Then per record: [u8 country_idx][f32 × n_features]   per record.
    // Per-country balanced: at most `save_per_country` per country.
    let save_path = args.save_features.clone();
    let mut saver = if !save_path.as_os_str().is_empty() {
        let f = std::fs::File::create(&save_path)?;
        let mut bw = std::io::BufWriter::with_capacity(8 * 1024 * 1024, f);
        // Placeholder header; we'll seek and rewrite at the end.
        use std::io::Write;
        bw.write_all(&(n as u32).to_le_bytes())?;
        bw.write_all(&(n_countries as u32).to_le_bytes())?;
        bw.write_all(&0u32.to_le_bytes())?; // n_records placeholder
        Some(bw)
    } else {
        None
    };
    let mut saved_per_country = vec![0usize; n_countries];
    let mut saved_total: u32 = 0;

    let reader = CorpusReader::open(&args.corpus)?;
    let mut processed = 0u64;
    for rec in reader.into_iter() {
        let r = match rec {
            Ok(r) => r,
            Err(_) => continue,
        };
        let cidx = match country_index(&r.country) {
            Some(i) => i,
            None => continue,
        };
        if args.max_records > 0 && processed as usize >= args.max_records {
            break;
        }
        let f = ex.extract(&r.text);
        for (i, &v) in f.iter().enumerate() {
            let bin = bucket_for(v, feat_min[i], feat_max[i], n_bins[i]);
            let ofs = i * n_countries * max_bins + cidx * max_bins + bin;
            counts[ofs] = counts[ofs].saturating_add(1);
        }
        country_total[cidx] += 1;
        total += 1;
        processed += 1;

        if let Some(s) = saver.as_mut() {
            if saved_per_country[cidx] < args.save_per_country {
                use std::io::Write;
                s.write_all(&[cidx as u8])?;
                for v in &f {
                    s.write_all(&v.to_le_bytes())?;
                }
                saved_per_country[cidx] += 1;
                saved_total += 1;
            }
        }

        if processed % 250_000 == 0 {
            eprintln!("[pass-1B] processed {}", processed);
        }
    }
    if let Some(mut s) = saver {
        use std::io::{Seek, SeekFrom, Write};
        s.flush()?;
        // Rewrite header n_records.
        let mut f = s.into_inner()?;
        f.seek(SeekFrom::Start(8))?;
        f.write_all(&saved_total.to_le_bytes())?;
        f.flush()?;
        eprintln!(
            "[saver] wrote {} records ({} per-country cap) to {}",
            saved_total,
            args.save_per_country,
            save_path.display()
        );
    }
    eprintln!("[pass-1B] processed {} records total (initial sample {})", processed, total_seen);
    let _ = total_seen;

    // Compute MI per feature.
    // H(C) = -Σ p(c) log p(c)
    // H(C|F) = Σ p(f) Σ_c p(c|f) log p(c|f)  =  Σ_f Σ_c p(c,f) log p(c) - p(c,f) log p(c|f) ... easier:
    // I(C;F) = Σ_c Σ_f p(c,f) log [ p(c,f) / (p(c) p(f)) ]
    eprintln!("[mi] computing mutual information per feature...");
    let total_f = total as f64;
    let mut feature_mi = vec![0.0f64; n];
    for i in 0..n {
        let nb = n_bins[i] as usize;
        // p(f=b) = sum over countries
        let mut feat_marginal = vec![0u64; nb];
        for c in 0..n_countries {
            for b in 0..nb {
                let v = counts[i * n_countries * max_bins + c * max_bins + b];
                feat_marginal[b] += v;
            }
        }
        let mut mi = 0.0f64;
        for c in 0..n_countries {
            let pc = country_total[c] as f64 / total_f;
            if pc <= 0.0 {
                continue;
            }
            for b in 0..nb {
                let v = counts[i * n_countries * max_bins + c * max_bins + b] as f64;
                if v == 0.0 {
                    continue;
                }
                let pcb = v / total_f;
                let pf = feat_marginal[b] as f64 / total_f;
                if pf <= 0.0 {
                    continue;
                }
                mi += pcb * (pcb / (pc * pf)).ln() / std::f64::consts::LN_2;
            }
        }
        feature_mi[i] = mi;
    }

    // Write features.tsv
    eprintln!("[out] writing {}", args.out.display());
    use std::io::Write;
    let mut w = std::io::BufWriter::new(std::fs::File::create(&args.out)?);
    writeln!(
        w,
        "feature_name\tclass\tn_features_in_class\tn_bins\tmutual_information_bits\tclass_cost_ns\tclass_share_cost_ns\tig_per_class_ns\trank_overall\trank_in_class"
    )?;
    // Compute per-class total MI for ranking.
    let mut per_class_mi: std::collections::HashMap<&str, f64> =
        std::collections::HashMap::new();
    let mut per_class_count: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for (i, _) in feature_mi.iter().enumerate() {
        let cls = ex.feature_classes[i].as_str();
        *per_class_mi.entry(cls).or_default() += feature_mi[i];
        *per_class_count.entry(cls).or_default() += 1;
    }

    // Rank features overall (by raw MI desc — useful but not the
    // primary ranking; class-level ranking is the actionable one).
    let mut order: Vec<usize> = (0..n).collect();
    order.sort_by(|&a, &b| feature_mi[b].partial_cmp(&feature_mi[a]).unwrap());
    let mut rank_overall = vec![0u32; n];
    for (rank, &i) in order.iter().enumerate() {
        rank_overall[i] = (rank + 1) as u32;
    }
    // Rank in class
    let mut rank_in_class = vec![0u32; n];
    for cls in FeatureClass::all() {
        let cs = cls.as_str();
        let mut idxs: Vec<usize> = (0..n)
            .filter(|&i| ex.feature_classes[i].as_str() == cs)
            .collect();
        idxs.sort_by(|&a, &b| feature_mi[b].partial_cmp(&feature_mi[a]).unwrap());
        for (rank, &i) in idxs.iter().enumerate() {
            rank_in_class[i] = (rank + 1) as u32;
        }
    }

    for i in 0..n {
        let cls = ex.feature_classes[i].as_str();
        let cost_class = *class_cost.get(cls).unwrap_or(&0.0);
        let share = cost_class / per_class_count[cls] as f64;
        // ig_per_class_ns: total class MI / class cost
        let class_mi = per_class_mi[cls];
        let ig_per_class_ns = if cost_class > 0.0 {
            class_mi / cost_class
        } else {
            0.0
        };
        writeln!(
            w,
            "{}\t{}\t{}\t{}\t{:.6}\t{:.2}\t{:.2}\t{:.6e}\t{}\t{}",
            ex.feature_names[i],
            cls,
            per_class_count[cls],
            n_bins[i],
            feature_mi[i],
            cost_class,
            share,
            ig_per_class_ns,
            rank_overall[i],
            rank_in_class[i],
        )?;
    }
    drop(w);

    // Print class-level summary
    eprintln!("\n=== Class summary (sorted by IG-per-ns descending) ===");
    let mut class_summary: Vec<(&str, f64, f64, usize)> = Vec::new();
    for &c in FeatureClass::all() {
        let cs = c.as_str();
        let mi = per_class_mi[cs];
        let cost = *class_cost.get(cs).unwrap_or(&0.0);
        let count = per_class_count[cs];
        class_summary.push((cs, mi, cost, count));
    }
    class_summary.sort_by(|a, b| {
        let ka = if a.2 > 0.0 { a.1 / a.2 } else { 0.0 };
        let kb = if b.2 > 0.0 { b.1 / b.2 } else { 0.0 };
        kb.partial_cmp(&ka).unwrap()
    });
    eprintln!("class      | total MI (bits) | cost (ns) | features | MI/ns");
    eprintln!("-----------|-----------------|-----------|----------|---------------");
    for (name, mi, cost, count) in &class_summary {
        let r = if *cost > 0.0 { mi / cost } else { 0.0 };
        eprintln!(
            "{:10} | {:>15.4} | {:>9.1} | {:>8} | {:.4e}",
            name, mi, cost, count, r
        );
    }

    // Top-20 features overall
    eprintln!("\n=== Top-20 features by raw MI ===");
    for (rank, &i) in order.iter().take(20).enumerate() {
        eprintln!(
            "{:>2}. {:6.4} bits  [{:8}]  {}",
            rank + 1,
            feature_mi[i],
            ex.feature_classes[i].as_str(),
            ex.feature_names[i]
        );
    }

    eprintln!(
        "\nDONE. {} records processed; {} features ranked.",
        total, n
    );
    Ok(())
}

fn bucket_for(v: f32, mn: f32, mx: f32, n_bins: u32) -> usize {
    if n_bins <= 1 {
        return 0;
    }
    if !v.is_finite() || mn >= mx {
        return 0;
    }
    if v <= mn {
        return 0;
    }
    if v >= mx {
        return (n_bins - 1) as usize;
    }
    let t = (v - mn) / (mx - mn);
    let b = (t * n_bins as f32).floor() as i32;
    b.clamp(0, n_bins as i32 - 1) as usize
}
