//! Bench query TSV emitter.
//!
//! Builds a balanced 1000-row TSV (query_id, query_text, gold_lat, gold_lon,
//! quality_class) suitable for the Nominatim / Photon / butterfly-geocode
//! competitive bench in `bench/geocode/`.
//!
//! Quality classes (roughly equal share):
//!   - clean: canonical "Street House, Postcode City"
//!   - abbreviated: street type contracted ("Rue" -> "R.")
//!   - typo: one-character edit injected
//!   - reordered: postcode-first order
//!   - partial: postcode dropped
//!
//! Coordinates come from the source OSM node — they are gold for recall@1
//! evaluation. They will not match every geocoder's coordinate system
//! (some snap to building polygon centroids, others to street centerline)
//! so the recall threshold should be 100 m, not 10 m.

use crate::augment::{self, Augmented};
use crate::bio::Labeled;
use crate::gold::GoldRecord;
use crate::morphology::Morphology;
use anyhow::{Result, bail};
use rand::Rng;
use rand_chacha::ChaCha20Rng;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

const CLASSES: &[&str] = &["clean", "abbreviated", "typo", "reordered", "partial"];

pub fn write_bench_tsv(
    golds: &[GoldRecord],
    path: &Path,
    target: usize,
    morph: &Morphology,
    rng: &mut ChaCha20Rng,
) -> Result<usize> {
    let f = File::create(path)?;
    let mut w = BufWriter::with_capacity(64 * 1024, f);
    writeln!(w, "query_id\tquery_text\tgold_lat\tgold_lon\tquality_class")?;

    let per_class = target / CLASSES.len();
    let mut written = 0usize;

    // Filter to records with usable city/postcode for richer queries.
    let usable: Vec<&GoldRecord> = golds
        .iter()
        .filter(|g| {
            g.lat.is_finite() && g.lon.is_finite() && g.city.is_some() && g.postcode.is_some()
        })
        .collect();

    if usable.is_empty() {
        bail!(
            "no usable gold records (need finite lat/lon, city, and postcode); \
             {} input records were filtered out",
            golds.len()
        );
    }

    for (class_idx, class) in CLASSES.iter().enumerate() {
        let mut emitted = 0usize;
        let mut tries = 0usize;
        while emitted < per_class && tries < per_class * 10 {
            tries += 1;
            let g_idx = rng.random_range(0..usable.len());
            let g = usable[g_idx];
            let canonical = crate::bio::render_canonical(g);
            let text = match *class {
                "clean" => canonical.text.clone(),
                "abbreviated" => {
                    render_one_kind(g, &canonical, morph, rng, kind_offset_for_class(class))
                }
                "typo" => {
                    render_one_kind(g, &canonical, morph, rng, kind_offset_for_class(class))
                }
                "reordered" => {
                    render_one_kind(g, &canonical, morph, rng, kind_offset_for_class(class))
                }
                "partial" => {
                    render_one_kind(g, &canonical, morph, rng, kind_offset_for_class(class))
                }
                _ => unreachable!(),
            };
            // Skip degenerate (empty / too short) outputs.
            if text.len() < 8 {
                continue;
            }
            let qid = format!("be-{}-{:05}", class, class_idx * per_class + emitted);
            // Strip TSV-hostile characters from the rendered text.
            let safe = text.replace(['\t', '\n', '\r'], " ");
            writeln!(
                w,
                "{}\t{}\t{:.7}\t{:.7}\t{}",
                qid, safe, g.lat, g.lon, class
            )?;
            emitted += 1;
            written += 1;
        }
    }
    w.flush()?;
    Ok(written)
}

/// Map a quality class to one of the augmentation strategies in `augment::apply`.
fn kind_offset_for_class(class: &&str) -> u32 {
    match *class {
        "abbreviated" => 3, // abbr_contract
        "typo" => 8,        // typo_injection
        "reordered" => 0,   // reorder_postcode_first
        "partial" => 1,     // drop_postcode
        _ => 0,
    }
}

fn render_one_kind(
    g: &GoldRecord,
    canonical: &Labeled,
    morph: &Morphology,
    rng: &mut ChaCha20Rng,
    k: u32,
) -> String {
    let Augmented { text, .. } = augment::apply(g, canonical, morph, rng, k);
    text
}
