//! Cross-shard regression canary.
//!
//! BE addresses rewritten with FR or NL conventions, kept OUT of the
//! training corpus and emitted to a separate JSONL file. The goal: at eval
//! time, run the trained parser on the canary file and verify it still
//! tags / routes BE correctly despite seeing FR/NL surface forms. If it
//! drops accuracy, the training corpus is leaking shard quirks.
//!
//! The country label stays "BE" — that's the whole point. The parser must
//! learn geography over orthography.

use crate::gold::GoldRecord;
use crate::output::TrainRecord;
use rand_chacha::ChaCha20Rng;

const FR_STREET_TYPE_FLIPS: &[(&str, &str)] = &[
    // NL → FR (so an NL-named street looks French)
    ("straat", "rue"),
    ("Straat", "Rue"),
    ("laan", "avenue"),
    ("Laan", "Avenue"),
    ("plein", "place"),
    ("Plein", "Place"),
];

const NL_STREET_TYPE_FLIPS: &[(&str, &str)] = &[
    // FR → NL (so a FR-named street looks Dutch)
    ("rue", "straat"),
    ("Rue", "Straat"),
    ("avenue", "laan"),
    ("Avenue", "Laan"),
    ("place", "plein"),
    ("Place", "Plein"),
    ("boulevard", "laan"),
    ("Boulevard", "Laan"),
];

fn rewrite_street(s: &str, flips: &[(&str, &str)]) -> Option<String> {
    for (from, to) in flips {
        if let Some(idx) = s.find(from) {
            let mut out = String::with_capacity(s.len() + 4);
            out.push_str(&s[..idx]);
            out.push_str(to);
            out.push_str(&s[idx + from.len()..]);
            return Some(out);
        }
    }
    None
}

pub fn rewrite_be_as_fr(g: &GoldRecord, _rng: &mut ChaCha20Rng) -> Option<TrainRecord> {
    let new_street = rewrite_street(g.street.as_deref()?, FR_STREET_TYPE_FLIPS)?;
    let mut g2 = g.clone();
    g2.street = Some(new_street);
    Some(rendered_record(&g2, "canary_be_as_fr"))
}

pub fn rewrite_be_as_nl(g: &GoldRecord, _rng: &mut ChaCha20Rng) -> Option<TrainRecord> {
    let new_street = rewrite_street(g.street.as_deref()?, NL_STREET_TYPE_FLIPS)?;
    let mut g2 = g.clone();
    g2.street = Some(new_street);
    Some(rendered_record(&g2, "canary_be_as_nl"))
}

fn rendered_record(g: &GoldRecord, kind: &str) -> TrainRecord {
    let labeled = crate::bio::render_canonical(g);
    TrainRecord {
        text: labeled.text,
        bio_labels: labeled.bio_labels,
        country: g.country.clone(),
        source_record_id: format!("osm:n{}", g.osm_id),
        augmentation: kind.to_string(),
    }
}
