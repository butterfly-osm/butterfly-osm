//! `phase2-corpus` — Phase 2 corpus generator.
//!
//! Iterates a BFGS shard, emits canonical + N augmented queries per
//! record with the gold record id + lat/lon retained. The output JSONL
//! is consumed by `phase2-label`.
//!
//! Per #98 Phase 2 prompt:
//!
//! > Default N=8 augmentations per gold record. For Belgium-merged
//! > 13.3M records × 8 = ~100M rows. Sample down to 5M for the
//! > labeling step (full corpus is overkill for a GBDT with ~30
//! > features).

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]

use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use butterfly_geocode::{CountryId, Shard};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use phase2_pipeline::augment::{DEFAULT_AUGMENTATIONS, Fields, apply, render_canonical};
use phase2_pipeline::sample::{AugmentationKind, PHASE2_SAMPLE_SCHEMA_VERSION, Phase2Sample};
use rand::SeedableRng;
use rand::seq::SliceRandom;
use rand_chacha::ChaCha20Rng;

#[derive(Parser, Debug)]
#[command(
    name = "phase2-corpus",
    about = "Phase 2 retrieval-aware corpus generator"
)]
struct Args {
    /// Input BFGS shard path. The shard's country header is used as
    /// the country tag on every emitted sample.
    #[arg(long)]
    shard: PathBuf,

    /// Output JSONL path (uncompressed; gzip later if needed).
    #[arg(long)]
    out: PathBuf,

    /// Number of augmentations per gold record (canonical is always
    /// emitted, augmentations are in addition).
    #[arg(long, default_value_t = 8)]
    augmentations: usize,

    /// Optional cap on total records to read from the shard. `0` = no cap.
    /// Used for fast iteration during development.
    #[arg(long, default_value_t = 0)]
    limit: usize,

    /// Optional sample fraction in `(0, 1]`. After generating all
    /// (canonical + augmented) rows, randomly sample this fraction
    /// to write out. Use `--max-rows` for an absolute cap.
    #[arg(long, default_value_t = 1.0)]
    sample_fraction: f64,

    /// Hard cap on the number of rows written. `0` = no cap. Applied
    /// AFTER `--sample-fraction` so a (small fraction, large cap) pair
    /// produces a fraction-sized output.
    #[arg(long, default_value_t = 0)]
    max_rows: usize,

    /// Random seed for deterministic augmentation + sampling.
    #[arg(long, default_value_t = 0xB17EBAD0)]
    seed: u64,

    /// Skip records whose street, postcode, or locality is empty.
    /// Empty fields produce ambiguous queries that contaminate the
    /// label distribution.
    #[arg(long, default_value_t = true)]
    skip_incomplete: bool,
}

fn main() -> Result<()> {
    let args = Args::parse();
    eprintln!("[phase2-corpus] opening shard {}", args.shard.display());
    let shard =
        Shard::open(&args.shard).with_context(|| format!("opening {}", args.shard.display()))?;
    let n_records = shard.record_count();
    let country = shard.country();
    eprintln!(
        "[phase2-corpus] shard country={} records={}",
        country.iso2(),
        n_records
    );
    if args.augmentations > DEFAULT_AUGMENTATIONS.len() {
        eprintln!(
            "[phase2-corpus] warn: --augmentations={} > available default kinds ({}), \
             will cycle the strategy list",
            args.augmentations,
            DEFAULT_AUGMENTATIONS.len()
        );
    }

    let cap_records = if args.limit == 0 {
        n_records
    } else {
        args.limit.min(n_records)
    };

    let pb = ProgressBar::new(cap_records as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner} [{elapsed_precise}] {bar:40} {pos}/{len} ({per_sec}) eta {eta}",
        )
        .unwrap(),
    );

    let out_path: &Path = &args.out;
    if let Some(parent) = out_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }

    // Two-pass: first generate everything in memory, then sample.
    // For Belgium 4M × 8 = 32M rows × ~100 bytes = ~3.2 GB. Streaming
    // with reservoir sampling avoids the in-memory blow-up.
    //
    // Implementation: reservoir sample of `target_size` indexed by
    // (record_idx, aug_idx). When `--sample-fraction == 1.0` and
    // `--max-rows == 0` we stream-write directly.
    let target_size = compute_target_size(
        cap_records,
        args.augmentations + 1,
        args.sample_fraction,
        args.max_rows,
    );
    eprintln!(
        "[phase2-corpus] aug-per-record={} sample-fraction={} max-rows={} → target ~{} rows",
        args.augmentations, args.sample_fraction, args.max_rows, target_size
    );

    let mut rng = ChaCha20Rng::seed_from_u64(args.seed);

    let f = std::fs::File::create(out_path)
        .with_context(|| format!("creating {}", out_path.display()))?;
    let mut w = BufWriter::with_capacity(1 << 20, f);

    let mut written = 0usize;
    let mut skipped = 0usize;
    let mut seen_aug_rows = 0u64;

    // Reservoir sample for (record_idx, kind).
    let do_reservoir = target_size > 0
        && target_size as u64
            != (cap_records as u64).saturating_mul((args.augmentations + 1) as u64);
    let mut reservoir: Vec<(u32, AugmentationKind)> = if do_reservoir {
        Vec::with_capacity(target_size)
    } else {
        Vec::new()
    };

    let kinds: Vec<AugmentationKind> = (0..args.augmentations)
        .map(|i| DEFAULT_AUGMENTATIONS[i % DEFAULT_AUGMENTATIONS.len()])
        .collect();

    for idx in 0..cap_records {
        let Some(rec) = shard.record(idx as u32) else {
            continue;
        };
        if args.skip_incomplete
            && (rec.street.is_empty() || rec.postcode.is_empty() || rec.locality.is_empty())
        {
            skipped += 1;
            pb.inc(1);
            continue;
        }

        let mut all_kinds: Vec<AugmentationKind> = Vec::with_capacity(1 + kinds.len());
        all_kinds.push(AugmentationKind::Canonical);
        all_kinds.extend(kinds.iter().copied());

        for k in all_kinds {
            seen_aug_rows += 1;
            if do_reservoir {
                if reservoir.len() < target_size {
                    reservoir.push((idx as u32, k));
                } else {
                    // Reservoir sample: replace at random index with
                    // probability target_size / seen.
                    let j = rng.gen_range(0..seen_aug_rows);
                    if (j as usize) < target_size {
                        reservoir[j as usize] = (idx as u32, k);
                    }
                }
            } else {
                let row = build_sample(&shard, &rec, k, country, &mut rng);
                if let Some(s) = row {
                    let line = serde_json::to_string(&s)?;
                    w.write_all(line.as_bytes())?;
                    w.write_all(b"\n")?;
                    written += 1;
                    if args.max_rows > 0 && written >= args.max_rows {
                        break;
                    }
                }
            }
        }

        if args.max_rows > 0 && written >= args.max_rows && !do_reservoir {
            break;
        }

        pb.inc(1);
    }

    if do_reservoir {
        // Materialise the reservoir into rows, with per-row RNG state
        // for stochastic kinds (typo/ws_noise).
        eprintln!(
            "[phase2-corpus] reservoir sampled {} rows from {} candidate rows",
            reservoir.len(),
            seen_aug_rows
        );
        // Shuffle the reservoir so the output isn't strictly ordered
        // by record_id (helps the trainer's random splits).
        reservoir.shuffle(&mut rng);
        for (rec_id, kind) in reservoir {
            let Some(rec) = shard.record(rec_id) else {
                continue;
            };
            if let Some(s) = build_sample(&shard, &rec, kind, country, &mut rng) {
                let line = serde_json::to_string(&s)?;
                w.write_all(line.as_bytes())?;
                w.write_all(b"\n")?;
                written += 1;
            }
        }
    }

    pb.finish_and_clear();
    w.flush()?;

    eprintln!(
        "[phase2-corpus] wrote {} rows to {}; skipped {} incomplete records",
        written,
        out_path.display(),
        skipped
    );
    Ok(())
}

fn build_sample(
    _shard: &Shard,
    rec: &butterfly_geocode::shard::reader::ShardRecord,
    kind: AugmentationKind,
    country: CountryId,
    rng: &mut ChaCha20Rng,
) -> Option<Phase2Sample> {
    let fields = Fields {
        street: &rec.street,
        housenumber: &rec.housenumber,
        postcode: &rec.postcode,
        locality: &rec.locality,
    };
    let query = match kind {
        AugmentationKind::Canonical => render_canonical(&fields, country),
        other => apply(other, &fields, country, rng),
    };
    if query.trim().is_empty() {
        return None;
    }
    let house = if rec.housenumber.is_empty() {
        None
    } else {
        Some(rec.housenumber.to_string())
    };
    Some(Phase2Sample {
        schema_version: PHASE2_SAMPLE_SCHEMA_VERSION,
        query,
        gold_record_id: rec.id,
        gold_lat: rec.lat,
        gold_lon: rec.lon,
        gold_housenumber: house,
        augmentation: kind,
        country: country.iso2().to_string(),
    })
}

fn compute_target_size(
    cap_records: usize,
    rows_per_record: usize,
    sample_fraction: f64,
    max_rows: usize,
) -> usize {
    let total = (cap_records as u64).saturating_mul(rows_per_record as u64);
    let after_fraction = (total as f64 * sample_fraction).round() as u64;
    let after_fraction = after_fraction as usize;
    if max_rows == 0 {
        after_fraction
    } else {
        after_fraction.min(max_rows)
    }
}

// Shadow `rand::Rng::gen_range` reference for older clippy configs.
trait RngRange {
    fn gen_range(&mut self, range: std::ops::Range<u64>) -> u64;
}
impl RngRange for ChaCha20Rng {
    fn gen_range(&mut self, range: std::ops::Range<u64>) -> u64 {
        use rand::Rng;
        self.random_range(range)
    }
}
