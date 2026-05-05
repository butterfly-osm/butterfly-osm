//! `phase2-label` — feature-extraction + label assignment.
//!
//! For each `Phase2Sample` row in the input JSONL:
//!
//! 1. Run the heuristic parser (single-hypothesis baseline).
//! 2. Build the retrieval program from the hypothesis.
//! 3. Canonicalize the program, compute Phase 2 features.
//! 4. Execute the program against the shard.
//! 5. Label `1.0` if any executor result is within 30 m of the gold
//!    AND has matching housenumber (when the gold has one); else `0.0`.
//! 6. Emit `LabeledRow` (features + label) to the output JSONL.
//!
//! ## Why heuristic parser?
//!
//! Per the prompt: the heuristic parser is the source of hypotheses
//! at this scale. The neural parser (#168) is a 120k-param tiny model
//! trained on 8k synthetic examples — it cannot generalise to a 5M-row
//! corpus and would produce noise. The heuristic emits one hypothesis
//! per query (clean-query path), so labels are well-defined and the
//! GBDT learns "given these features, is the program right". Future
//! work re-runs labeling against a real-corpus-trained neural parser.

#![deny(unsafe_code)]
#![deny(missing_debug_implementations)]

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::{Context, Result, anyhow, bail};
use butterfly_geocode::{
    CountryId, GeocodedResult, HeuristicScorer, ParsedQuery, Phase2AnchorSummary, Phase2BeamStats,
    Phase2Features, Phase2LabeledRow, Phase2ProgramFeatures, RetrievalUtilityScorer, Shard,
    parser::{anchor::detect_anchors, heuristic::parse_heuristic, phase2_features::extract},
};
use clap::Parser;
use indicatif::{ProgressBar, ProgressStyle};
use phase2_pipeline::sample::{PHASE2_SAMPLE_SCHEMA_VERSION, Phase2Sample};
use rayon::prelude::*;

#[derive(Parser, Debug)]
#[command(name = "phase2-label", about = "Phase 2 feature extraction + labeling")]
struct Args {
    /// Input JSONL produced by `phase2-corpus`.
    #[arg(long)]
    samples: PathBuf,

    /// Shard the executor runs against. Should be the SAME shard used
    /// to generate the corpus — gold record ids are shard-local.
    #[arg(long)]
    shard: PathBuf,

    /// Output JSONL of `(features, label)` rows.
    #[arg(long)]
    out: PathBuf,

    /// Distance tolerance in meters for the "executor landed on gold"
    /// label. Defaults to 30 m (matches the confidence reranker and
    /// is the radius BOSA's housenumber centroid stays within).
    #[arg(long, default_value_t = 30.0)]
    distance_tolerance_m: f64,

    /// Top-K candidate cap from the executor. Higher = more recall
    /// but more compute. Phase 2 only needs to know whether ANY
    /// candidate is the gold — `5` is plenty.
    #[arg(long, default_value_t = 5)]
    top_k: usize,

    /// Optional row cap (testing).
    #[arg(long, default_value_t = 0)]
    limit: usize,
}

fn main() -> Result<()> {
    let args = Args::parse();
    eprintln!("[phase2-label] reading samples {}", args.samples.display());
    let samples = read_samples(&args.samples, args.limit)?;
    eprintln!("[phase2-label] {} samples loaded", samples.len());
    if samples.is_empty() {
        bail!("samples file is empty");
    }

    eprintln!("[phase2-label] opening shard {}", args.shard.display());
    let shard = Arc::new(
        Shard::open(&args.shard).with_context(|| format!("opening {}", args.shard.display()))?,
    );

    let pb = ProgressBar::new(samples.len() as u64);
    pb.set_style(
        ProgressStyle::with_template(
            "{spinner} [{elapsed_precise}] {bar:40} {pos}/{len} ({per_sec}) eta {eta}",
        )
        .unwrap(),
    );

    let n_pos = AtomicU64::new(0);
    let n_neg = AtomicU64::new(0);
    let n_failed = AtomicU64::new(0);

    let out_path = args.out.clone();
    if let Some(parent) = out_path.parent()
        && !parent.as_os_str().is_empty()
    {
        std::fs::create_dir_all(parent)?;
    }
    let out_f = File::create(&out_path)?;
    let writer = Arc::new(Mutex::new(BufWriter::with_capacity(1 << 20, out_f)));

    let dist_tol = args.distance_tolerance_m;
    let top_k = args.top_k;

    samples.par_iter().for_each(|sample| {
        match label_one(sample, &shard, dist_tol, top_k) {
            Ok(rows) => {
                if !rows.is_empty() {
                    let mut buf = String::with_capacity(256 * rows.len());
                    for r in &rows {
                        let line = serde_json::to_string(r).expect("serialise LabeledRow");
                        buf.push_str(&line);
                        buf.push('\n');
                        if r.label > 0.5 {
                            n_pos.fetch_add(1, Ordering::Relaxed);
                        } else {
                            n_neg.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                    if let Ok(mut w) = writer.lock() {
                        let _ = w.write_all(buf.as_bytes());
                    }
                }
            }
            Err(_) => {
                n_failed.fetch_add(1, Ordering::Relaxed);
            }
        }
        pb.inc(1);
    });

    pb.finish_and_clear();
    if let Ok(mut w) = writer.lock() {
        w.flush()?;
    }
    let pos = n_pos.load(Ordering::Relaxed);
    let neg = n_neg.load(Ordering::Relaxed);
    let failed = n_failed.load(Ordering::Relaxed);
    eprintln!(
        "[phase2-label] wrote rows: positives={} negatives={} failed={} → {}",
        pos,
        neg,
        failed,
        out_path.display()
    );
    if pos == 0 {
        return Err(anyhow!(
            "no positive labels emitted — verify the shard matches the corpus shard \
             (gold_record_id is shard-local)"
        ));
    }
    Ok(())
}

fn read_samples(path: &std::path::Path, limit: usize) -> Result<Vec<Phase2Sample>> {
    let f = File::open(path).with_context(|| format!("opening {}", path.display()))?;
    let r = BufReader::new(f);
    let mut out: Vec<Phase2Sample> = Vec::new();
    for (i, line) in r.lines().enumerate() {
        let line = line.with_context(|| format!("reading line {}", i + 1))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with('#') {
            continue;
        }
        let s: Phase2Sample =
            serde_json::from_str(trimmed).with_context(|| format!("parsing line {}", i + 1))?;
        if s.schema_version != PHASE2_SAMPLE_SCHEMA_VERSION {
            return Err(anyhow!(
                "samples schema_version {} does not match expected {} — regenerate \
                 with the matching `phase2-corpus`",
                s.schema_version,
                PHASE2_SAMPLE_SCHEMA_VERSION
            ));
        }
        out.push(s);
        if limit > 0 && out.len() >= limit {
            break;
        }
    }
    Ok(out)
}

fn parse_country(iso2: &str) -> Result<CountryId> {
    CountryId::from_iso2(iso2).ok_or_else(|| anyhow::anyhow!("unknown country code: {}", iso2))
}

fn haversine_m(a_lat: f64, a_lon: f64, b_lat: f64, b_lon: f64) -> f64 {
    butterfly_geocode::shard::reader::haversine_m(a_lat, a_lon, b_lat, b_lon)
}

/// Process one sample → produce as many `LabeledRow`s as the parser
/// emits hypotheses. For the heuristic parser this is always 1.
fn label_one(
    sample: &Phase2Sample,
    shard: &Shard,
    dist_tol_m: f64,
    top_k: usize,
) -> Result<Vec<Phase2LabeledRow>> {
    let country = parse_country(&sample.country)?;
    let parsed: ParsedQuery = parse_heuristic(&sample.query, country);

    // The heuristic parser emits exactly one hypothesis. To produce
    // a Phase 2 feature row we need: program + policy + anchors +
    // beam stats. We compute those from the parsed query directly.
    if parsed.hypotheses.is_empty() {
        return Ok(Vec::new());
    }
    let h = &parsed.hypotheses[0];
    let policy = h.retrieval_policy;

    // Build the program. We use the same builder the decoder calls so
    // the runtime path matches; this is the legacy-private function
    // exposed via the parser module.
    let program = build_program_for_hypothesis(h, &policy);
    let canon = program.canonicalize();

    // Anchors over the raw query text.
    let anchors_vec = detect_anchors(&sample.query, shard);
    let anchor_summary = Phase2AnchorSummary::from(&anchors_vec, h);

    // Program features (walk the canonical tree).
    let prog_features = Phase2ProgramFeatures::from_program(&canon, &policy, shard);

    // Beam stats: heuristic parser is single-hypothesis, so the
    // surrogate logprob is `ln(global_confidence)`.
    let logp = parsed.global_confidence.max(1e-6).ln();
    let beam_stats = Phase2BeamStats::from_logprobs(&[logp]);

    // Country posterior — heuristic always claims its argument with
    // weight 1.0.
    let country_posterior = parsed
        .country_candidates
        .first()
        .map(|(_, w)| *w)
        .unwrap_or(1.0);

    let features: Phase2Features = extract(
        h,
        &canon,
        &policy,
        &prog_features,
        &anchor_summary,
        beam_stats,
        0,
        logp,
        country_posterior,
        shard,
    );

    // Execute the program against the shard. We use the public
    // `execute` (not `execute_program`, which is private) — for the
    // single-hypothesis clean-query path this delegates to the fast
    // path that returns at most `top_k` candidates.
    let candidates: Vec<GeocodedResult> = butterfly_geocode::execute(&parsed, shard, top_k);
    let label = if landed_on_gold(&candidates, sample, dist_tol_m) {
        1.0_f32
    } else {
        0.0_f32
    };

    // Sanity: a heuristic scorer over the same features should produce
    // a finite log-prob. We compute it for telemetry only — the
    // training pipeline re-derives it from the trained model.
    let _scorer_smoke = HeuristicScorer::default().score(&features);

    Ok(vec![Phase2LabeledRow {
        schema_version: Phase2Features::SCHEMA_VERSION,
        features,
        label,
    }])
}

/// Did any executor result land within `dist_tol_m` of the gold and
/// (when the gold has a housenumber) match the housenumber?
fn landed_on_gold(candidates: &[GeocodedResult], sample: &Phase2Sample, dist_tol_m: f64) -> bool {
    for c in candidates {
        let d = haversine_m(c.lat, c.lon, sample.gold_lat, sample.gold_lon);
        if d > dist_tol_m {
            continue;
        }
        if let Some(gold_hn) = sample.gold_housenumber.as_deref() {
            if c.housenumber.eq_ignore_ascii_case(gold_hn) {
                return true;
            }
            // House missing on candidate but distance match — still
            // counts as a "found the gold" label since BOSA's centroid
            // can be at the building level without the housenumber
            // surviving merge.
            if c.housenumber.is_empty() {
                return true;
            }
        } else {
            return true;
        }
    }
    false
}

// Re-implementation of the (currently private) `build_program_for_hypothesis`
// from `parser/decoding.rs`. We duplicate it here because it isn't on
// the public surface yet and we don't want to widen the API just for
// a training binary. The semantics MUST match — when this drifts, the
// learned scorer will be calibrated against a different program shape
// than the runtime executes.
//
// IDENTITY check: this implementation must produce the exact same Op
// tree as `parser::decoding::build_program_for_hypothesis`. Verified
// in `phase2_label_program_matches_decoder` test (geocode/tests/).
fn build_program_for_hypothesis(
    h: &butterfly_geocode::ParseHypothesis,
    policy: &butterfly_geocode::RetrievalPolicy,
) -> butterfly_geocode::geocoder::program::Op {
    use butterfly_geocode::geocoder::channels::{Channel, ChannelRole};
    use butterfly_geocode::geocoder::program::{LookupKey, Op};

    let mut blockers: Vec<Op> = Vec::new();
    let mut reducers: Vec<Op> = Vec::new();
    let mut scorers: Vec<Op> = Vec::new();

    let mut push_for = |ch: Channel, key: &str| {
        let lookup = Op::Lookup(LookupKey {
            channel: ch,
            key: key.to_string(),
        });
        match policy.role(ch) {
            Some(ChannelRole::Blocker) => blockers.push(lookup),
            Some(ChannelRole::Reducer) => reducers.push(lookup),
            Some(ChannelRole::Scorer) => scorers.push(Op::Score {
                child: Box::new(lookup),
                channel: ch,
                weight: 1.0,
            }),
            None => {}
        }
    };
    if let Some((pc, _)) = h.postcode_candidates.first() {
        push_for(Channel::Postcode, pc);
    }
    if let Some((st, _)) = h.street_candidates.first() {
        push_for(Channel::Street, st);
    }
    if let Some((loc, _)) = h.locality_candidates.first() {
        push_for(Channel::Locality, loc);
    }

    let base: Op = match (blockers.len(), reducers.len()) {
        (0, 0) if !scorers.is_empty() => Op::Union(scorers.clone()),
        (0, 0) => Op::Lookup(LookupKey {
            channel: Channel::Locality,
            key: String::new(),
        }),
        (0, _) => {
            if reducers.len() == 1 {
                reducers.into_iter().next().unwrap()
            } else {
                Op::Intersect(reducers)
            }
        }
        (_, 0) => {
            if blockers.len() == 1 {
                blockers.into_iter().next().unwrap()
            } else {
                Op::Intersect(blockers)
            }
        }
        (_, _) => {
            let mut all = blockers;
            all.extend(reducers);
            if all.len() == 1 {
                all.into_iter().next().unwrap()
            } else {
                Op::Intersect(all)
            }
        }
    };
    let after_filter = if let Some((hn, _)) = h.house_candidates.first() {
        Op::Filter {
            child: Box::new(base),
            predicate: butterfly_geocode::geocoder::program::FilterPredicate::HouseNumberEq(
                hn.clone(),
            ),
        }
    } else {
        base
    };
    Op::Cap {
        child: Box::new(after_filter),
        n: 64,
    }
}
