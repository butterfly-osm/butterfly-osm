//! Training corpus generator for the byte-level geocode tagger (#96, #98 Phase 2).
//!
//! Reads an OSM PBF, finds addr-tagged nodes, and emits JSONL training samples.
//! Each gold address is rendered into a canonical text + BIO byte-span
//! supervision tuple, then expanded into N retrieval-success-invariant
//! augmentations (typos, abbreviation flips, reorderings, case noise, etc).
//!
//! See `geocode-research/PROMPT_CORPUS.txt` for design rationale and the
//! external review in `geocode-research/EXTERNAL_REVIEW.md`.

mod augment;
mod bench_queries;
mod bio;
mod canary;
mod gold;
mod morphology;
mod output;

use anyhow::Result;
use clap::Parser;
use rand::SeedableRng;
use rand_chacha::ChaCha20Rng;
use std::path::PathBuf;

#[derive(Parser, Debug)]
#[command(name = "corpus-gen", about = "Geocode training corpus generator")]
struct Args {
    /// Input PBF file (e.g. data/belgium.pbf).
    #[arg(short, long)]
    pbf: PathBuf,

    /// Output JSONL path. Augmented training samples go here.
    #[arg(short, long, default_value = "corpus.jsonl")]
    out: PathBuf,

    /// Cross-shard canary output path. Source-country addresses rewritten
    /// with `--canary-targets` countries' conventions for held-out
    /// regression testing of shard memorization.
    #[arg(long, default_value = "canary.jsonl")]
    canary: PathBuf,

    /// Country code stamped on every record. The PBF is country-specific.
    #[arg(long, default_value = "BE")]
    country: String,

    /// Comma-separated ISO 3166-1 alpha-2 codes of the canary target
    /// countries. Each target country's morphology is used to rewrite
    /// source-country streets with its conventions, generating held-out
    /// counterfactual examples. Empty to skip canary generation.
    #[arg(long, default_value = "FR,NL")]
    canary_targets: String,

    /// Directory containing morphology TOML files (one per ISO code,
    /// e.g. `morphology/be.toml`).
    #[arg(long, default_value = "morphology")]
    morphology_dir: PathBuf,

    /// Number of augmented variants per gold record (default 8 per
    /// codex review). Each gold record contributes 1 canonical record
    /// + N augmented variants to the training corpus.
    #[arg(short = 'n', long, default_value_t = 8)]
    augmentations: u32,

    /// Optional cap on number of gold records read from the PBF
    /// (for fast iteration; 0 = no cap).
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Seed for deterministic output. Same seed + same PBF = byte-identical corpus.
    #[arg(long, default_value_t = 42)]
    seed: u64,

    /// Sample-only mode: don't write the full corpus, just the first `sample`
    /// records to `--out`. Used for committing a fixture for code review.
    #[arg(long, default_value_t = 0)]
    sample: usize,

    /// Optional bench-query TSV output. If set, writes a 1000-row sample
    /// of (query_id, query_text, gold_lat, gold_lon, quality_class) for
    /// the Nominatim/Photon comparison bench. One row per quality class
    /// per stride so the set is balanced.
    #[arg(long)]
    bench_queries: Option<PathBuf>,

    /// Number of bench queries to emit (split across 5 quality classes).
    #[arg(long, default_value_t = 1000)]
    bench_queries_count: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    let mut rng = ChaCha20Rng::seed_from_u64(args.seed);

    // Load source-country morphology + canary-target morphologies.
    let source_iso = args.country.trim().to_ascii_uppercase();
    let canary_isos: Vec<String> = args
        .canary_targets
        .split(',')
        .map(|s| s.trim().to_ascii_uppercase())
        .filter(|s| !s.is_empty() && s != &source_iso)
        .collect();

    let source_morph = morphology::Morphology::load_from_dir(&args.morphology_dir, &source_iso)?;
    eprintln!(
        "[corpus-gen] source morphology: {} ({})",
        source_morph.country.iso2, source_morph.country.name,
    );

    let canary_targets: Vec<morphology::Morphology> = canary_isos
        .iter()
        .map(|iso| morphology::Morphology::load_from_dir(&args.morphology_dir, iso))
        .collect::<Result<Vec<_>>>()?;
    if !canary_targets.is_empty() {
        eprintln!(
            "[corpus-gen] canary targets: {}",
            canary_targets
                .iter()
                .map(|m| m.country.iso2.as_str())
                .collect::<Vec<_>>()
                .join(",")
        );
    }

    eprintln!("[corpus-gen] reading PBF: {}", args.pbf.display());
    let golds = gold::read_pbf(&args.pbf, &source_iso, args.limit)?;
    eprintln!("[corpus-gen] gold records: {}", golds.len());

    let mut writer = output::JsonlWriter::new(&args.out)?;
    let mut canary_writer = output::JsonlWriter::new(&args.canary)?;
    let mut written = 0usize;
    let mut canary_written = 0usize;

    // Fixed canary fraction — every Nth gold record gets the cross-shard rewrite
    // and is written to the canary file ONLY (it is a held-out test set, not
    // training data).
    let canary_stride = 50;

    for (i, g) in golds.iter().enumerate() {
        // Skip the canary stride from the training corpus and route them to canary instead.
        if i % canary_stride == 0 {
            for tgt in &canary_targets {
                if let Some(record) = canary::rewrite_with(g, &source_morph, tgt, &mut rng) {
                    canary_writer.write(&record)?;
                    canary_written += 1;
                }
            }
            continue;
        }

        // Canonical (text, BIO, country) record always goes in.
        let canonical = bio::render_canonical(g);
        writer.write(&output::TrainRecord {
            text: canonical.text.clone(),
            bio_labels: canonical.bio_labels.clone(),
            country: g.country.clone(),
            source_record_id: format!("osm:n{}", g.osm_id),
            augmentation: "canonical".to_string(),
        })?;
        written += 1;

        // N augmented variants. Each picks one or more rewrite strategies.
        for k in 0..args.augmentations {
            let variant = augment::apply(g, &canonical, &source_morph, &mut rng, k);
            writer.write(&output::TrainRecord {
                text: variant.text,
                bio_labels: variant.bio_labels,
                country: g.country.clone(),
                source_record_id: format!("osm:n{}", g.osm_id),
                augmentation: variant.kind,
            })?;
            written += 1;
        }

        if args.sample > 0 && written >= args.sample {
            eprintln!("[corpus-gen] sample mode: stopping at {} records", written);
            break;
        }
    }

    writer.finish()?;
    canary_writer.finish()?;
    eprintln!(
        "[corpus-gen] wrote {} training records to {}",
        written,
        args.out.display()
    );
    eprintln!(
        "[corpus-gen] wrote {} canary records to {}",
        canary_written,
        args.canary.display()
    );

    if let Some(bench_path) = &args.bench_queries {
        let n = bench_queries::write_bench_tsv(
            &golds,
            bench_path,
            args.bench_queries_count,
            &source_morph,
            &mut rng,
        )?;
        eprintln!(
            "[corpus-gen] wrote {} bench queries to {}",
            n,
            bench_path.display()
        );
    }
    Ok(())
}
