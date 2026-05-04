//! BIO byte-level labeling.
//!
//! Tag scheme: B-{field} / I-{field} / O. Tags are byte-indexed. `text` is
//! UTF-8; multi-byte characters get one BIO tag per byte (the model is
//! byte-level so this matches its tokenization).
//!
//! Field set: STREET, HNUM, POST, CITY, UNIT. Five fields × 2 (B/I) + O = 11
//! possible labels (we use indices, not strings, in the training output).
//!
//! IMPORTANT: when augmentation rewrites the canonical text, the BIO labels
//! must be re-derived against the new byte string. We never try to
//! "synthesize" labels from offsets — we always tag spans against the
//! rewritten string. This is the design that survives typo injection without
//! drift (per the codex/gemini review on label preservation).

use crate::gold::GoldRecord;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Field {
    O,
    Street,
    Hnum,
    Post,
    City,
    Unit,
}

impl Field {
    pub fn b_label(self) -> u8 {
        match self {
            Field::O => 0,
            Field::Street => 1,
            Field::Hnum => 3,
            Field::Post => 5,
            Field::City => 7,
            Field::Unit => 9,
        }
    }
    pub fn i_label(self) -> u8 {
        match self {
            Field::O => 0,
            Field::Street => 2,
            Field::Hnum => 4,
            Field::Post => 6,
            Field::City => 8,
            Field::Unit => 10,
        }
    }
}

/// One labeled span in the rendered text. `start..end` is a byte range in
/// `text`. The labeler walks these spans and emits BIO bytes.
#[derive(Debug, Clone)]
pub struct Span {
    pub field: Field,
    pub start: usize,
    pub end: usize,
}

#[derive(Debug, Clone)]
pub struct Labeled {
    pub text: String,
    pub bio_labels: Vec<u8>,
    pub spans: Vec<Span>,
}

/// Render the canonical address string and label it. The canonical form for
/// Belgium is "<street> <housenumber>, <postcode> <city>".
pub fn render_canonical(g: &GoldRecord) -> Labeled {
    // Build the components in order, recording byte offsets as we append.
    let mut text = String::new();
    let mut spans: Vec<Span> = Vec::new();

    if let Some(street) = &g.street {
        let start = text.len();
        text.push_str(street);
        spans.push(Span {
            field: Field::Street,
            start,
            end: text.len(),
        });
    }
    if let Some(hnum) = &g.housenumber {
        if !text.is_empty() {
            text.push(' ');
        }
        let start = text.len();
        text.push_str(hnum);
        spans.push(Span {
            field: Field::Hnum,
            start,
            end: text.len(),
        });
    }
    if (g.postcode.is_some() || g.city.is_some()) && !text.is_empty() {
        text.push_str(", ");
    }
    if let Some(post) = &g.postcode {
        let start = text.len();
        text.push_str(post);
        spans.push(Span {
            field: Field::Post,
            start,
            end: text.len(),
        });
    }
    if let Some(city) = &g.city {
        if g.postcode.is_some() {
            text.push(' ');
        }
        let start = text.len();
        text.push_str(city);
        spans.push(Span {
            field: Field::City,
            start,
            end: text.len(),
        });
    }
    if let Some(unit) = &g.unit {
        text.push_str(" /");
        let start = text.len();
        text.push_str(unit);
        spans.push(Span {
            field: Field::Unit,
            start,
            end: text.len(),
        });
    }

    let bio_labels = bio_from_spans(&text, &spans);
    Labeled {
        text,
        bio_labels,
        spans,
    }
}

/// Compute BIO labels from a labeled text + spans. One label per byte.
pub fn bio_from_spans(text: &str, spans: &[Span]) -> Vec<u8> {
    let mut labels = vec![Field::O.b_label(); text.len()];
    for span in spans {
        if span.start >= text.len() || span.end > text.len() || span.start >= span.end {
            continue;
        }
        // First byte = B-tag, subsequent bytes inside span = I-tag.
        labels[span.start] = span.field.b_label();
        for label in labels.iter_mut().take(span.end).skip(span.start + 1) {
            *label = span.field.i_label();
        }
    }
    labels
}
