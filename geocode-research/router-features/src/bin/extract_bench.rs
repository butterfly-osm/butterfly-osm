//! Phase 1 — Per-feature-class extraction cost (criterion-light).
//!
//! Loads the 15 country packs, builds a representative-input set
//! (sampled from the training corpus or fallback embedded examples),
//! computes top-100 byte 2-grams from a corpus sample, then times
//! each feature-class extraction over many iterations.
//!
//! Output:
//!   - `bench-extraction.json` next to a `--out` path with one entry
//!     per class: name, per_call_ns, samples, n_features
//!   - `extractor.spec.json` so phase-2 can rebuild the same extractor
//!     deterministically.

use anyhow::{Context, Result};
use clap::Parser;
use router_features::corpus::CorpusReader;
use router_features::packs::load_packs;
use router_features::{ExtractorSpec, FeatureClass, country_index};
use std::collections::HashMap;
use std::path::PathBuf;
use std::time::Instant;

#[derive(Parser, Debug)]
struct Args {
    #[arg(long)]
    corpus: PathBuf,
    #[arg(long, default_value = "geocode/data/packs")]
    packs_dir: PathBuf,
    /// How many corpus samples to draw for the timing input set.
    #[arg(long, default_value = "20000")]
    sample: usize,
    /// How many times to iterate the timing loop per class.
    #[arg(long, default_value = "5")]
    rounds: usize,
    /// How many top byte-bigrams to use as bigram features.
    #[arg(long, default_value = "100")]
    top_bigrams: usize,
    /// Output dir for bench-extraction.json + extractor.spec.json.
    #[arg(long, default_value = "geocode-research/router-features/artifacts")]
    out: PathBuf,
}

fn main() -> Result<()> {
    let args = Args::parse();
    std::fs::create_dir_all(&args.out)?;

    eprintln!("[1/5] loading {} country packs...", router_features::COUNTRIES.len());
    let packs = load_packs(&args.packs_dir).context("load packs")?;
    eprintln!(
        "      → {} markers, {} postcode regexes",
        packs.markers.len(),
        packs.postcode_regexes.len()
    );

    eprintln!("[2/5] sampling corpus + computing top bigrams...");
    let (samples, bigrams) = sample_corpus_and_bigrams(&args.corpus, args.sample, args.top_bigrams)?;
    eprintln!(
        "      → {} samples, top-{} bigrams",
        samples.len(),
        bigrams.len()
    );

    let spec = ExtractorSpec {
        markers: packs.markers.clone(),
        postcode_patterns: load_postcode_patterns(&args.packs_dir)?,
        bigrams: bigrams.clone(),
    };
    let spec_path = args.out.join("extractor.spec.json");
    spec.save(&spec_path)?;
    eprintln!("      → saved {}", spec_path.display());

    eprintln!("[3/5] building extractor...");
    let ex = spec.build();
    eprintln!(
        "      → {} features total ({} script, {} digit, {} punct, {} length, {} postcode, {} marker, {} bigram)",
        ex.n_features(),
        ex.n_script_feats,
        ex.n_digit_feats,
        ex.n_punct_feats,
        ex.n_length_feats,
        ex.n_postcode_feats,
        ex.n_marker_feats,
        ex.n_bigram_feats
    );

    eprintln!("[4/5] timing per-class extraction...");
    let mut buf = vec![0.0f32; ex.n_features().max(1024)];

    let mut entries: Vec<BenchEntry> = Vec::new();
    for &class in FeatureClass::all() {
        let n = ex.class_count(class);
        // Warmup
        for s in samples.iter().take(1024) {
            ex.extract_class(s, class, &mut buf[..n]);
        }
        // Measure
        let mut best_ns_per_call = f64::INFINITY;
        for _ in 0..args.rounds {
            let t0 = Instant::now();
            for s in &samples {
                ex.extract_class(s, class, &mut buf[..n]);
                std::hint::black_box(&buf[..n]);
            }
            let dt = t0.elapsed();
            let per_call = dt.as_nanos() as f64 / samples.len() as f64;
            if per_call < best_ns_per_call {
                best_ns_per_call = per_call;
            }
        }
        eprintln!(
            "      {:?}: {:.1} ns/call ({} features)",
            class, best_ns_per_call, n
        );
        entries.push(BenchEntry {
            class: class.as_str().to_string(),
            n_features: n,
            ns_per_call: best_ns_per_call,
            samples: samples.len(),
        });
    }

    // Total full-extract cost
    let mut best_full = f64::INFINITY;
    for _ in 0..args.rounds {
        let t0 = Instant::now();
        for s in &samples {
            let v = ex.extract(s);
            std::hint::black_box(v);
        }
        let dt = t0.elapsed();
        let per_call = dt.as_nanos() as f64 / samples.len() as f64;
        if per_call < best_full {
            best_full = per_call;
        }
    }
    eprintln!("      FULL extract: {:.1} ns/call", best_full);

    let bench_json = serde_json::json!({
        "samples": samples.len(),
        "rounds": args.rounds,
        "per_class": entries,
        "full_extract_ns": best_full,
    });
    let bench_path = args.out.join("bench-extraction.json");
    std::fs::write(&bench_path, serde_json::to_string_pretty(&bench_json)?)?;
    eprintln!("[5/5] wrote {}", bench_path.display());

    Ok(())
}

#[derive(serde::Serialize)]
struct BenchEntry {
    class: String,
    n_features: usize,
    ns_per_call: f64,
    samples: usize,
}

/// Streams the first `sample` records from the corpus, returning the
/// strings AND the top-N byte 2-grams.
///
/// We sample stratified by country to avoid Latin-heavy averages biasing
/// the representative-input cost. Per-country budget = sample/15.
fn sample_corpus_and_bigrams(
    path: &std::path::Path,
    target: usize,
    top_n: usize,
) -> Result<(Vec<String>, Vec<[u8; 2]>)> {
    let per_country = (target / router_features::COUNTRIES.len()).max(1);
    let mut buckets: HashMap<String, Vec<String>> = HashMap::new();
    let mut bigram_counts: HashMap<u16, u64> = HashMap::with_capacity(65536);

    let reader = CorpusReader::open(path)?;
    for rec in reader.into_iter() {
        let r = rec?;
        if country_index(&r.country).is_none() {
            continue;
        }
        let bucket = buckets.entry(r.country.clone()).or_default();
        if bucket.len() < per_country {
            // count bigrams from this string (only for samples we keep, to
            // avoid full-corpus pass; this is approximate but representative)
            let bytes = r.text.as_bytes();
            for w in bytes.windows(2) {
                let key = (w[0] as u16) | ((w[1] as u16) << 8);
                *bigram_counts.entry(key).or_default() += 1;
            }
            bucket.push(r.text);
        }
        // Early exit when all buckets full
        if buckets.len() == router_features::COUNTRIES.len()
            && buckets.values().all(|v| v.len() >= per_country)
        {
            break;
        }
    }

    let mut samples: Vec<String> = Vec::with_capacity(target);
    for c in router_features::COUNTRIES {
        if let Some(bucket) = buckets.get(*c) {
            samples.extend(bucket.iter().cloned());
        }
    }

    // Top bigrams
    let mut sorted: Vec<(u16, u64)> = bigram_counts.into_iter().collect();
    sorted.sort_by(|a, b| b.1.cmp(&a.1));
    let top: Vec<[u8; 2]> = sorted
        .into_iter()
        .take(top_n)
        .map(|(k, _)| [(k & 0xff) as u8, (k >> 8) as u8])
        .collect();

    Ok((samples, top))
}

fn load_postcode_patterns(packs_dir: &std::path::Path) -> Result<Vec<String>> {
    use serde::Deserialize;
    #[derive(Deserialize)]
    struct PackPostcodeOnly {
        postcode: P,
    }
    #[derive(Deserialize)]
    struct P {
        regex: String,
    }
    let mut out = Vec::with_capacity(router_features::COUNTRIES.len());
    for c in router_features::COUNTRIES {
        let path = packs_dir.join(format!("{}.toml", c.to_ascii_lowercase()));
        let body = std::fs::read_to_string(&path)?;
        let p: PackPostcodeOnly = toml::from_str(&body)?;
        out.push(p.postcode.regex);
    }
    Ok(out)
}
