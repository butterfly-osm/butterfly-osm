//! Retrieval-success-invariant augmentations.
//!
//! Each augmentation rewrites the canonical record into a new (text, BIO)
//! pair. Crucially, BIO labels are RE-DERIVED against the rewritten text
//! using span-tracked rendering — we never try to mutate labels separately
//! from text and have them drift out of alignment.
//!
//! Strategies (mixed via the `kind` index for diversity per gold record):
//!   0  reorder: postcode-first
//!   1  reorder: drop postcode
//!   2  reorder: drop locality
//!   3  abbreviation contraction (e.g. "Rue" -> "R.")
//!   4  abbreviation expansion (e.g. "Bd." -> "Boulevard")
//!   5  case: UPPERCASE
//!   6  case: lowercase
//!   7  whitespace/punctuation noise
//!   8  typo injection (1 char edit)
//!   9  combined (case + abbr)
//!  10+  cycles back through the strategies.

use crate::bio::{Field, Labeled, Span, bio_from_spans};
use crate::gold::GoldRecord;
use rand::Rng;
use rand_chacha::ChaCha20Rng;

#[derive(Debug, Clone)]
pub struct Augmented {
    pub text: String,
    pub bio_labels: Vec<u8>,
    pub kind: String,
}

pub fn apply(g: &GoldRecord, _canonical: &Labeled, rng: &mut ChaCha20Rng, k: u32) -> Augmented {
    match k % 10 {
        0 => reorder_postcode_first(g),
        1 => drop_field(g, DropTarget::Postcode),
        2 => drop_field(g, DropTarget::City),
        3 => abbr_contract(g),
        4 => abbr_expand(g),
        5 => case_transform(g, CaseStyle::Upper),
        6 => case_transform(g, CaseStyle::Lower),
        7 => whitespace_noise(g, rng),
        8 => typo_injection(g, rng),
        _ => combined_case_abbr(g, rng),
    }
}

#[derive(Copy, Clone)]
enum DropTarget {
    Postcode,
    City,
}

#[derive(Copy, Clone)]
enum CaseStyle {
    Upper,
    Lower,
}

/// Re-render with components in a new order. Returns (text, spans).
fn render_with(components: &[(Field, &str)], separators: &[&str]) -> (String, Vec<Span>) {
    let mut text = String::new();
    let mut spans: Vec<Span> = Vec::new();
    for (i, (field, value)) in components.iter().enumerate() {
        if i > 0 && i - 1 < separators.len() {
            text.push_str(separators[i - 1]);
        }
        let start = text.len();
        text.push_str(value);
        spans.push(Span {
            field: *field,
            start,
            end: text.len(),
        });
    }
    (text, spans)
}

fn reorder_postcode_first(g: &GoldRecord) -> Augmented {
    // "1070 Anderlecht Rue Wayez 122"
    let mut comps: Vec<(Field, &str)> = Vec::new();
    let mut seps: Vec<&str> = Vec::new();
    if let Some(p) = &g.postcode {
        comps.push((Field::Post, p));
    }
    if let Some(c) = &g.city {
        if !comps.is_empty() {
            seps.push(" ");
        }
        comps.push((Field::City, c));
    }
    if let Some(s) = &g.street {
        if !comps.is_empty() {
            seps.push(" ");
        }
        comps.push((Field::Street, s));
    }
    if let Some(h) = &g.housenumber {
        if !comps.is_empty() {
            seps.push(" ");
        }
        comps.push((Field::Hnum, h));
    }
    let (text, spans) = render_with(&comps, &seps);
    Augmented {
        bio_labels: bio_from_spans(&text, &spans),
        text,
        kind: "reorder_postcode_first".to_string(),
    }
}

fn drop_field(g: &GoldRecord, target: DropTarget) -> Augmented {
    let mut comps: Vec<(Field, &str)> = Vec::new();
    let mut seps: Vec<&str> = Vec::new();

    if let Some(s) = &g.street {
        comps.push((Field::Street, s));
    }
    if let Some(h) = &g.housenumber {
        if !comps.is_empty() {
            seps.push(" ");
        }
        comps.push((Field::Hnum, h));
    }
    let mut had_first = !comps.is_empty();
    if !matches!(target, DropTarget::Postcode)
        && let Some(p) = &g.postcode
    {
        if had_first {
            seps.push(", ");
            had_first = false;
        }
        comps.push((Field::Post, p));
    }
    if !matches!(target, DropTarget::City)
        && let Some(c) = &g.city
    {
        if had_first {
            seps.push(", ");
        } else if !comps.is_empty() && comps.last().map(|x| x.0) != Some(Field::Hnum) {
            seps.push(" ");
        }
        comps.push((Field::City, c));
    }

    let (text, spans) = render_with(&comps, &seps);
    let kind = match target {
        DropTarget::Postcode => "drop_postcode",
        DropTarget::City => "drop_city",
    };
    Augmented {
        bio_labels: bio_from_spans(&text, &spans),
        text,
        kind: kind.to_string(),
    }
}

/// French/Dutch abbreviation contractions used in BE addressing.
const ABBR_CONTRACT: &[(&str, &str)] = &[
    // FR
    ("Rue", "R."),
    ("rue", "r."),
    ("Boulevard", "Bd"),
    ("boulevard", "bd"),
    ("Avenue", "Av."),
    ("avenue", "av."),
    ("Place", "Pl."),
    ("place", "pl."),
    ("Chaussée", "Ch."),
    ("chaussée", "ch."),
    ("Saint", "St"),
    ("saint", "st"),
    // NL
    ("Straat", "Str."),
    ("straat", "str."),
    ("Laan", "Ln."),
    ("laan", "ln."),
];

/// Reverse: contracted → expanded.
const ABBR_EXPAND: &[(&str, &str)] = &[
    ("R.", "Rue"),
    ("r.", "rue"),
    ("Bd.", "Boulevard"),
    ("bd.", "boulevard"),
    ("Bd ", "Boulevard "),
    ("bd ", "boulevard "),
    ("Av.", "Avenue"),
    ("av.", "avenue"),
    ("Pl.", "Place"),
    ("pl.", "place"),
    ("Ch.", "Chaussée"),
    ("ch.", "chaussée"),
    ("St ", "Saint "),
    ("st ", "saint "),
    ("Str.", "Straat"),
    ("str.", "straat"),
    ("Ln.", "Laan"),
    ("ln.", "laan"),
];

fn abbr_contract(g: &GoldRecord) -> Augmented {
    let mut g2 = g.clone();
    if let Some(s) = g2.street.as_mut() {
        for (long, short) in ABBR_CONTRACT {
            if s.starts_with(long) {
                *s = format!("{}{}", short, &s[long.len()..]);
                break;
            }
        }
    }
    let labeled = crate::bio::render_canonical(&g2);
    Augmented {
        text: labeled.text,
        bio_labels: labeled.bio_labels,
        kind: "abbr_contract".to_string(),
    }
}

fn abbr_expand(g: &GoldRecord) -> Augmented {
    let mut g2 = g.clone();
    if let Some(s) = g2.street.as_mut() {
        for (short, long) in ABBR_EXPAND {
            if s.starts_with(short) {
                *s = format!("{}{}", long, &s[short.len()..]);
                break;
            }
        }
    }
    let labeled = crate::bio::render_canonical(&g2);
    Augmented {
        text: labeled.text,
        bio_labels: labeled.bio_labels,
        kind: "abbr_expand".to_string(),
    }
}

/// Case transformation. Critical: char-by-char case change preserves byte
/// length only for ASCII. For non-ASCII (é/ç/ü), uppercase can change byte
/// length (one codepoint → multiple). The safe path is to re-render the
/// gold record with cased components, then derive BIO from spans on the
/// rewritten text. We do NOT try to maintain offsets across case mutation
/// of a flat string.
fn case_transform(g: &GoldRecord, style: CaseStyle) -> Augmented {
    let f = |s: &str| match style {
        CaseStyle::Upper => s.to_uppercase(),
        CaseStyle::Lower => s.to_lowercase(),
    };
    let g2 = GoldRecord {
        osm_id: g.osm_id,
        country: g.country.clone(),
        street: g.street.as_deref().map(f),
        housenumber: g.housenumber.as_deref().map(f),
        postcode: g.postcode.clone(), // postcodes have no case to flip
        city: g.city.as_deref().map(f),
        unit: g.unit.as_deref().map(f),
        lat: g.lat,
        lon: g.lon,
    };
    let labeled = crate::bio::render_canonical(&g2);
    let kind = match style {
        CaseStyle::Upper => "case_upper",
        CaseStyle::Lower => "case_lower",
    };
    Augmented {
        text: labeled.text,
        bio_labels: labeled.bio_labels,
        kind: kind.to_string(),
    }
}

/// Whitespace and punctuation noise. We only mutate SEPARATORS between
/// fields, never inside spans, so labels stay aligned by re-derivation.
fn whitespace_noise(g: &GoldRecord, rng: &mut ChaCha20Rng) -> Augmented {
    // Re-render with random separator choices.
    let mut comps: Vec<(Field, &str)> = Vec::new();
    let mut seps: Vec<String> = Vec::new();
    if let Some(s) = &g.street {
        comps.push((Field::Street, s));
    }
    if let Some(h) = &g.housenumber {
        if !comps.is_empty() {
            seps.push(noisy_sep(rng, " "));
        }
        comps.push((Field::Hnum, h));
    }
    if let Some(p) = &g.postcode {
        if !comps.is_empty() {
            seps.push(noisy_sep(rng, ", "));
        }
        comps.push((Field::Post, p));
    }
    if let Some(c) = &g.city {
        if !comps.is_empty() {
            seps.push(noisy_sep(rng, " "));
        }
        comps.push((Field::City, c));
    }

    let mut text = String::new();
    let mut spans: Vec<Span> = Vec::new();
    for (i, (field, value)) in comps.iter().enumerate() {
        if i > 0 {
            text.push_str(&seps[i - 1]);
        }
        let start = text.len();
        text.push_str(value);
        spans.push(Span {
            field: *field,
            start,
            end: text.len(),
        });
    }
    let bio_labels = bio_from_spans(&text, &spans);
    Augmented {
        text,
        bio_labels,
        kind: "ws_noise".to_string(),
    }
}

fn noisy_sep(rng: &mut ChaCha20Rng, default: &str) -> String {
    match rng.random_range(0..6) {
        0 => "  ".to_string(),
        1 => " - ".to_string(),
        2 => format!("{}{}", default, " "),
        3 => default.trim().to_string(), // possibly empty
        4 => " , ".to_string(),
        _ => default.to_string(),
    }
}

/// Typo injection. We mutate WITHIN a single span (chosen at random) so we
/// can keep span boundaries aligned at the byte level by tracking the
/// length delta. We never split a multi-byte char (we mutate ASCII bytes
/// only — non-ASCII runs are skipped).
fn typo_injection(g: &GoldRecord, rng: &mut ChaCha20Rng) -> Augmented {
    let labeled = crate::bio::render_canonical(g);
    if labeled.spans.is_empty() {
        return Augmented {
            text: labeled.text.clone(),
            bio_labels: labeled.bio_labels.clone(),
            kind: "typo_skipped".to_string(),
        };
    }
    // Pick a span; pick a byte position inside it that is ASCII (not the start
    // of a multi-byte char). Apply one of: substitute, delete, transpose, insert.
    let span_idx = rng.random_range(0..labeled.spans.len());
    let span = labeled.spans[span_idx].clone();
    let bytes = labeled.text.as_bytes();
    if span.end - span.start < 2 {
        return Augmented {
            text: labeled.text.clone(),
            bio_labels: labeled.bio_labels.clone(),
            kind: "typo_skipped".to_string(),
        };
    }
    // Find an ASCII byte to mutate.
    let mut pos = None;
    for _ in 0..10 {
        let p = rng.random_range(span.start..span.end);
        if bytes[p] < 128 && bytes[p].is_ascii_alphabetic() {
            pos = Some(p);
            break;
        }
    }
    let Some(pos) = pos else {
        return Augmented {
            text: labeled.text.clone(),
            bio_labels: labeled.bio_labels.clone(),
            kind: "typo_skipped".to_string(),
        };
    };

    let op = rng.random_range(0..4);
    let mut new_bytes: Vec<u8> = bytes.to_vec();
    let delta: i32 = match op {
        0 => {
            // substitute: change to another lowercase ASCII letter.
            let c = b'a' + (rng.random_range(0..26) as u8);
            new_bytes[pos] = c;
            0
        }
        1 => {
            // delete: only if pos > span.start (so the B-tag stays).
            if pos > span.start {
                new_bytes.remove(pos);
                -1
            } else {
                0
            }
        }
        2 => {
            // transpose with previous byte (must also be ASCII).
            if pos > span.start && new_bytes[pos - 1] < 128 {
                new_bytes.swap(pos - 1, pos);
            }
            0
        }
        _ => {
            // insert: random ASCII letter at pos.
            let c = b'a' + (rng.random_range(0..26) as u8);
            new_bytes.insert(pos, c);
            1
        }
    };

    // Apply delta to all spans whose start >= pos (and end >= pos).
    let mut new_spans: Vec<Span> = Vec::with_capacity(labeled.spans.len());
    for s in &labeled.spans {
        let mut s2 = s.clone();
        if s2.start > pos {
            s2.start = (s2.start as i32 + delta).max(0) as usize;
        }
        if s2.end > pos || (s2.end == pos + 1 && delta == 1) {
            s2.end = (s2.end as i32 + delta).max(s2.start as i32) as usize;
        }
        new_spans.push(s2);
    }
    // Specifically: the mutated span (span_idx) must extend its end by delta.
    if delta != 0 {
        let s = &mut new_spans[span_idx];
        s.end = (labeled.spans[span_idx].end as i32 + delta).max(s.start as i32) as usize;
    }

    let new_text = match String::from_utf8(new_bytes) {
        Ok(t) => t,
        Err(_) => {
            // Mutation broke UTF-8 — discard and fall back to canonical.
            return Augmented {
                text: labeled.text.clone(),
                bio_labels: labeled.bio_labels.clone(),
                kind: "typo_invalid".to_string(),
            };
        }
    };
    let bio_labels = bio_from_spans(&new_text, &new_spans);
    Augmented {
        text: new_text,
        bio_labels,
        kind: "typo_injection".to_string(),
    }
}

fn combined_case_abbr(g: &GoldRecord, rng: &mut ChaCha20Rng) -> Augmented {
    // Apply contraction first, then random case.
    let contracted = abbr_contract(g);
    // Re-derive a temporary GoldRecord from contracted text — too lossy.
    // Instead, take the canonical form, contract street, then case-flip.
    let style = if rng.random::<bool>() {
        CaseStyle::Upper
    } else {
        CaseStyle::Lower
    };
    let mut g2 = g.clone();
    if let Some(s) = g2.street.as_mut() {
        for (long, short) in ABBR_CONTRACT {
            if s.starts_with(long) {
                *s = format!("{}{}", short, &s[long.len()..]);
                break;
            }
        }
    }
    let cased = case_transform(&g2, style);
    let _ = contracted;
    Augmented {
        text: cased.text,
        bio_labels: cased.bio_labels,
        kind: "combined_case_abbr".to_string(),
    }
}
