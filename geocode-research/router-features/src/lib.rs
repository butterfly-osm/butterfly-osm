//! Feature extraction module for issue #227 measurement phase.
//!
//! This is research code, NOT production. It exposes every candidate
//! feature individually so we can:
//!   - bench extraction cost per feature class
//!   - compute mutual information per feature
//!   - ablate cost-aware feature selection
//!
//! The cost unit is "feature class" (script flags, digit features,
//! length features, postcode DFAs, lexical markers, n-grams). Once
//! you've paid for a class's byte-pass, every feature in that class
//! is effectively free — that's the point of single-byte-pass design.

use aho_corasick::{AhoCorasick, AhoCorasickKind, MatchKind};
use regex::Regex;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;

pub mod corpus;
pub mod packs;

/// Stable feature class identifier used for cost amortisation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum FeatureClass {
    Script,
    Digit,
    Punct,
    Length,
    Postcode,
    Marker,
    Bigram,
}

impl FeatureClass {
    pub fn as_str(self) -> &'static str {
        match self {
            FeatureClass::Script => "script",
            FeatureClass::Digit => "digit",
            FeatureClass::Punct => "punct",
            FeatureClass::Length => "length",
            FeatureClass::Postcode => "postcode",
            FeatureClass::Marker => "marker",
            FeatureClass::Bigram => "bigram",
        }
    }

    pub fn all() -> &'static [FeatureClass] {
        &[
            FeatureClass::Script,
            FeatureClass::Digit,
            FeatureClass::Punct,
            FeatureClass::Length,
            FeatureClass::Postcode,
            FeatureClass::Marker,
            FeatureClass::Bigram,
        ]
    }
}

/// Country labels used by the 15-pack measurement corpus.
/// Indices are stable and used as `i32` labels for gbdt training.
pub const COUNTRIES: &[&str] = &[
    "AT", "AU", "BE", "BR", "CH", "DE", "ES", "FR", "GB", "IN", "IT", "JP", "LU", "NL", "US",
];

pub fn country_index(iso2: &str) -> Option<usize> {
    let up = iso2.to_ascii_uppercase();
    COUNTRIES.iter().position(|c| *c == up)
}

/// Address-format family for per-family breakdown in REPORT.md.
/// Latin-heavy by design of the 15-country test set; flagged as a
/// sampling limitation in the report.
pub fn family_for(iso2: &str) -> &'static str {
    match iso2 {
        // Western-Latin (street + house + postcode + locality)
        "AT" | "AU" | "BE" | "CH" | "DE" | "FR" | "GB" | "IT" | "LU" | "NL" | "US" => "western_latin",
        // Hispanic-Latin (overlaps with Western, but uses different street markers)
        "BR" | "ES" => "hispanic_latin",
        // CJK
        "JP" => "cjk",
        // Devanagari + Latin mix
        "IN" => "devanagari_mixed",
        _ => "other",
    }
}

/// Feature extractor — built once at startup, used many times.
pub struct FeatureExtractor {
    pub markers_aho: AhoCorasick,
    pub marker_strings: Vec<String>,
    /// Per-country postcode DFAs in fixed `COUNTRIES` order.
    pub postcode_regexes: Vec<Regex>,
    /// Top-N most frequent byte 2-grams (LE-packed) on the corpus.
    pub bigrams: Vec<[u8; 2]>,
    pub bigram_index: HashMap<u16, usize>,
    /// Names for each emitted feature in order.
    pub feature_names: Vec<String>,
    /// Class for each feature in order.
    pub feature_classes: Vec<FeatureClass>,
    pub n_script_feats: usize,
    pub n_digit_feats: usize,
    pub n_punct_feats: usize,
    pub n_length_feats: usize,
    pub n_postcode_feats: usize,
    pub n_marker_feats: usize,
    pub n_bigram_feats: usize,
}

impl FeatureExtractor {
    /// Total number of features (the Vec<f32> length returned by `extract`).
    pub fn n_features(&self) -> usize {
        self.feature_names.len()
    }

    /// Slice of (start, end) feature-index ranges for each class, in
    /// the canonical order returned by `extract`.
    pub fn class_ranges(&self) -> Vec<(FeatureClass, usize, usize)> {
        let mut out = Vec::new();
        let mut start = 0;
        for &c in FeatureClass::all() {
            let n = match c {
                FeatureClass::Script => self.n_script_feats,
                FeatureClass::Digit => self.n_digit_feats,
                FeatureClass::Punct => self.n_punct_feats,
                FeatureClass::Length => self.n_length_feats,
                FeatureClass::Postcode => self.n_postcode_feats,
                FeatureClass::Marker => self.n_marker_feats,
                FeatureClass::Bigram => self.n_bigram_feats,
            };
            out.push((c, start, start + n));
            start += n;
        }
        out
    }

    /// Build the extractor.
    /// `markers` are the Aho-Corasick patterns (lower-cased before insertion),
    /// `postcode_per_country` is one regex per country in COUNTRIES order,
    /// `bigrams` is the top-N byte 2-grams.
    pub fn new(
        markers: Vec<String>,
        postcode_per_country: Vec<Regex>,
        bigrams: Vec<[u8; 2]>,
    ) -> Self {
        let aho = AhoCorasick::builder()
            .kind(Some(AhoCorasickKind::DFA))
            .match_kind(MatchKind::Standard)
            .ascii_case_insensitive(true)
            .build(&markers)
            .expect("aho-corasick build");

        // Build bigram index for O(1) lookup.
        let mut bigram_index = HashMap::with_capacity(bigrams.len());
        for (i, b) in bigrams.iter().enumerate() {
            let key = (b[0] as u16) | ((b[1] as u16) << 8);
            bigram_index.insert(key, i);
        }

        // Feature names + classes
        let mut feature_names = Vec::new();
        let mut feature_classes = Vec::new();

        // Script: 9 scripts × (flag + ratio) + has_mixed_script + multi_script_count = 20
        const SCRIPTS: &[&str] = &[
            "latin", "cjk", "cyrillic", "arabic", "devanagari", "hangul", "thai", "hebrew", "greek",
        ];
        for s in SCRIPTS {
            feature_names.push(format!("script_{}_flag", s));
            feature_classes.push(FeatureClass::Script);
        }
        for s in SCRIPTS {
            feature_names.push(format!("script_{}_ratio", s));
            feature_classes.push(FeatureClass::Script);
        }
        feature_names.push("script_mixed_flag".into());
        feature_classes.push(FeatureClass::Script);
        feature_names.push("script_distinct_count".into());
        feature_classes.push(FeatureClass::Script);
        let n_script_feats = SCRIPTS.len() * 2 + 2;

        // Digit features
        let digit_names = [
            "digit_total_count",
            "digit_longest_run",
            "digit_run_2",
            "digit_run_3",
            "digit_run_4",
            "digit_run_5",
            "digit_run_6",
            "digit_run_7p",
            "digit_has_4",
            "digit_has_5",
            "digit_has_7",
            "digit_block_dash",
            "digit_block_slash",
            "digit_block_space",
            "digit_token_count",
        ];
        for n in &digit_names {
            feature_names.push(n.to_string());
            feature_classes.push(FeatureClass::Digit);
        }
        let n_digit_feats = digit_names.len();

        // Punctuation features
        let punct_names = [
            "punct_comma",
            "punct_period",
            "punct_hyphen",
            "punct_cjk",
            "punct_rtl",
            "case_title_ratio",
            "case_allcaps_ratio",
            "case_nocase_ratio",
            "diacritic_present",
            "diacritic_count",
        ];
        for n in &punct_names {
            feature_names.push(n.to_string());
            feature_classes.push(FeatureClass::Punct);
        }
        let n_punct_feats = punct_names.len();

        // Length features
        let length_names = [
            "len_bytes",
            "len_chars",
            "len_tokens",
            "len_avg_token",
            "len_max_token",
        ];
        for n in &length_names {
            feature_names.push(n.to_string());
            feature_classes.push(FeatureClass::Length);
        }
        let n_length_feats = length_names.len();

        // Postcode features
        for c in COUNTRIES {
            feature_names.push(format!("postcode_{}", c.to_ascii_lowercase()));
            feature_classes.push(FeatureClass::Postcode);
        }
        let n_postcode_feats = COUNTRIES.len();

        // Lexical markers
        for m in &markers {
            feature_names.push(format!("marker_{}", sanitize_for_name(m)));
            feature_classes.push(FeatureClass::Marker);
        }
        let n_marker_feats = markers.len();

        // Bigrams
        for b in &bigrams {
            feature_names.push(format!(
                "bigram_{}_{}",
                escape_byte(b[0]),
                escape_byte(b[1])
            ));
            feature_classes.push(FeatureClass::Bigram);
        }
        let n_bigram_feats = bigrams.len();

        FeatureExtractor {
            markers_aho: aho,
            marker_strings: markers,
            postcode_regexes: postcode_per_country,
            bigrams,
            bigram_index,
            feature_names,
            feature_classes,
            n_script_feats,
            n_digit_feats,
            n_punct_feats,
            n_length_feats,
            n_postcode_feats,
            n_marker_feats,
            n_bigram_feats,
        }
    }

    /// Extract one feature class (used for cost-amortised cascade
    /// inference and for bench timing per class).
    pub fn extract_class(&self, s: &str, class: FeatureClass, out: &mut [f32]) {
        match class {
            FeatureClass::Script => self.extract_script(s, out),
            FeatureClass::Digit => self.extract_digit(s, out),
            FeatureClass::Punct => self.extract_punct(s, out),
            FeatureClass::Length => self.extract_length(s, out),
            FeatureClass::Postcode => self.extract_postcode(s, out),
            FeatureClass::Marker => self.extract_markers(s, out),
            FeatureClass::Bigram => self.extract_bigrams(s, out),
        }
    }

    /// Extract all features in canonical order.
    pub fn extract(&self, s: &str) -> Vec<f32> {
        let mut out = vec![0.0f32; self.n_features()];
        let mut cursor = 0;
        for &c in FeatureClass::all() {
            let n = self.class_count(c);
            self.extract_class(s, c, &mut out[cursor..cursor + n]);
            cursor += n;
        }
        out
    }

    pub fn class_count(&self, c: FeatureClass) -> usize {
        match c {
            FeatureClass::Script => self.n_script_feats,
            FeatureClass::Digit => self.n_digit_feats,
            FeatureClass::Punct => self.n_punct_feats,
            FeatureClass::Length => self.n_length_feats,
            FeatureClass::Postcode => self.n_postcode_feats,
            FeatureClass::Marker => self.n_marker_feats,
            FeatureClass::Bigram => self.n_bigram_feats,
        }
    }

    // -----------------------------------------------------------------
    // Script extractor — single codepoint pass.
    // Order: latin, cjk, cyrillic, arabic, devanagari, hangul, thai, hebrew, greek
    fn extract_script(&self, s: &str, out: &mut [f32]) {
        let mut counts = [0u32; 9];
        let mut total = 0u32;
        for ch in s.chars() {
            let code = ch as u32;
            // skip ASCII non-letters (digits/space/punct don't count)
            if !ch.is_alphabetic() {
                continue;
            }
            total += 1;
            let idx = match code {
                0x41..=0x5A | 0x61..=0x7A | 0xC0..=0x024F | 0x1E00..=0x1EFF => 0, // Latin (incl. extended)
                0x4E00..=0x9FFF | 0x3040..=0x309F | 0x30A0..=0x30FF | 0x3400..=0x4DBF | 0xF900..=0xFAFF => 1, // CJK + Kana
                0x0400..=0x04FF | 0x0500..=0x052F => 2, // Cyrillic
                0x0600..=0x06FF | 0x0750..=0x077F | 0x08A0..=0x08FF | 0xFB50..=0xFDFF | 0xFE70..=0xFEFF => 3, // Arabic
                0x0900..=0x097F => 4,                       // Devanagari
                0xAC00..=0xD7AF | 0x1100..=0x11FF => 5,     // Hangul
                0x0E00..=0x0E7F => 6,                       // Thai
                0x0590..=0x05FF | 0xFB1D..=0xFB4F => 7,     // Hebrew
                0x0370..=0x03FF | 0x1F00..=0x1FFF => 8,     // Greek
                _ => continue,
            };
            counts[idx] += 1;
        }
        for i in 0..9 {
            out[i] = if counts[i] > 0 { 1.0 } else { 0.0 };
        }
        let total_f = total.max(1) as f32;
        for i in 0..9 {
            out[9 + i] = counts[i] as f32 / total_f;
        }
        let distinct = counts.iter().filter(|&&c| c > 0).count() as f32;
        out[18] = if distinct > 1.0 { 1.0 } else { 0.0 }; // mixed
        out[19] = distinct;
    }

    // Digit features (single byte-pass over `s.as_bytes()`).
    fn extract_digit(&self, s: &str, out: &mut [f32]) {
        let bytes = s.as_bytes();
        let mut total = 0u32;
        let mut cur_run = 0u32;
        let mut longest = 0u32;
        let mut runs = [0u32; 7]; // index 0..6 -> lengths 2..7+
        let mut block_dash = 0u32; // \d-\d
        let mut block_slash = 0u32;
        let mut block_space = 0u32;
        let mut token_count = 0u32; // distinct digit-runs

        let mut prev_digit = false;
        for (i, &b) in bytes.iter().enumerate() {
            let is_d = b.is_ascii_digit();
            if is_d {
                total += 1;
                cur_run += 1;
                if !prev_digit {
                    token_count += 1;
                }
            } else {
                if cur_run > 0 {
                    if cur_run > longest {
                        longest = cur_run;
                    }
                    let bin = (cur_run.min(7) as usize).saturating_sub(2);
                    if cur_run >= 2 {
                        runs[bin] += 1;
                    }
                    if cur_run >= 7 {
                        runs[5] += 1; // index 5 = run_7p (using bin shift below)
                    }
                }
                cur_run = 0;

                // \d{X}-\d{Y} block detection: previous char was digit, this is delim, next is digit
                if prev_digit {
                    let next_is_d = bytes
                        .get(i + 1)
                        .map(|c| c.is_ascii_digit())
                        .unwrap_or(false);
                    if next_is_d {
                        match b {
                            b'-' => block_dash += 1,
                            b'/' => block_slash += 1,
                            b' ' => block_space += 1,
                            _ => {}
                        }
                    }
                }
            }
            prev_digit = is_d;
        }
        if cur_run > 0 {
            if cur_run > longest {
                longest = cur_run;
            }
            let bin = (cur_run.min(7) as usize).saturating_sub(2);
            if cur_run >= 2 {
                runs[bin] += 1;
            }
        }

        out[0] = total as f32;
        out[1] = longest as f32;
        // run_2..run_7p
        out[2] = runs[0] as f32;
        out[3] = runs[1] as f32;
        out[4] = runs[2] as f32;
        out[5] = runs[3] as f32;
        out[6] = runs[4] as f32;
        // run_7p — count of runs ≥ 7
        let run_7p = bytes_iter_runs_geq(bytes, 7);
        out[7] = run_7p as f32;
        // has_X for exact-length runs anywhere
        out[8] = if has_exact_run(bytes, 4) { 1.0 } else { 0.0 };
        out[9] = if has_exact_run(bytes, 5) { 1.0 } else { 0.0 };
        out[10] = if has_exact_run(bytes, 7) { 1.0 } else { 0.0 };
        out[11] = block_dash as f32;
        out[12] = block_slash as f32;
        out[13] = block_space as f32;
        out[14] = token_count as f32;
    }

    fn extract_punct(&self, s: &str, out: &mut [f32]) {
        let bytes = s.as_bytes();
        let mut comma = 0u32;
        let mut period = 0u32;
        let mut hyphen = 0u32;

        for &b in bytes {
            match b {
                b',' => comma += 1,
                b'.' => period += 1,
                b'-' => hyphen += 1,
                _ => {}
            }
        }

        // Non-ASCII pieces require codepoint pass.
        let mut cjk_punct = 0u32;
        let mut rtl_marker = 0u32;
        let mut diacritic = 0u32;
        let mut letters_total = 0u32;
        let mut letters_upper = 0u32;
        let mut letters_lower = 0u32;
        let mut tokens_seen = 0u32;
        let mut titled_tokens = 0u32;
        let mut allcaps_tokens = 0u32;
        let mut nocase_tokens = 0u32;

        // Walk tokens (whitespace-separated) once for casing ratios.
        let mut in_token = false;
        let mut token_first_upper = false;
        let mut token_rest_lower = true;
        let mut token_has_letters = false;
        let mut token_all_upper = true;
        let mut token_all_nocase = true;
        let mut token_first_processed = false;

        for ch in s.chars() {
            // diacritic / latin extended chars
            let code = ch as u32;
            if (0x00C0..=0x024F).contains(&code) || (0x1E00..=0x1EFF).contains(&code) {
                diacritic += 1;
            }
            // CJK punctuation block
            if (0x3000..=0x303F).contains(&code) || (0xFF00..=0xFFEF).contains(&code) {
                if matches!(ch, '、' | '。' | '〒' | '（' | '）' | '・') {
                    cjk_punct += 1;
                }
            }
            // RTL marker LRM/RLM/PDF/LRE/RLE/LRO/RLO/LRI/RLI/FSI/PDI
            if matches!(code, 0x200E | 0x200F | 0x202A..=0x202E | 0x2066..=0x2069) {
                rtl_marker += 1;
            }

            if ch.is_alphabetic() {
                letters_total += 1;
                if ch.is_uppercase() {
                    letters_upper += 1;
                } else if ch.is_lowercase() {
                    letters_lower += 1;
                }

                // token machine
                if !in_token {
                    in_token = true;
                    token_first_upper = ch.is_uppercase();
                    token_first_processed = true;
                    token_rest_lower = true;
                    token_has_letters = true;
                    token_all_upper = ch.is_uppercase();
                    token_all_nocase = !(ch.is_uppercase() || ch.is_lowercase());
                } else {
                    if !token_first_processed {
                        token_first_upper = ch.is_uppercase();
                        token_first_processed = true;
                    } else if !ch.is_lowercase() {
                        token_rest_lower = false;
                    }
                    if !ch.is_uppercase() {
                        token_all_upper = false;
                    }
                    if ch.is_uppercase() || ch.is_lowercase() {
                        token_all_nocase = false;
                    }
                    token_has_letters = true;
                }
            } else if ch.is_whitespace() {
                if in_token {
                    if token_has_letters {
                        tokens_seen += 1;
                        if token_first_upper && token_rest_lower {
                            titled_tokens += 1;
                        }
                        if token_all_upper {
                            allcaps_tokens += 1;
                        }
                        if token_all_nocase {
                            nocase_tokens += 1;
                        }
                    }
                    in_token = false;
                    token_first_processed = false;
                    token_has_letters = false;
                    token_all_upper = true;
                    token_all_nocase = true;
                    token_rest_lower = true;
                    token_first_upper = false;
                }
            }
            // any non-letter non-whitespace char doesn't reset the token
            // (e.g. "'" inside "O'Neill"); but we don't add to letter pass.
        }
        // close last token
        if in_token && token_has_letters {
            tokens_seen += 1;
            if token_first_upper && token_rest_lower {
                titled_tokens += 1;
            }
            if token_all_upper {
                allcaps_tokens += 1;
            }
            if token_all_nocase {
                nocase_tokens += 1;
            }
        }

        out[0] = comma as f32;
        out[1] = period as f32;
        out[2] = hyphen as f32;
        out[3] = cjk_punct as f32;
        out[4] = rtl_marker as f32;
        let tk = tokens_seen.max(1) as f32;
        out[5] = titled_tokens as f32 / tk;
        out[6] = allcaps_tokens as f32 / tk;
        out[7] = nocase_tokens as f32 / tk;
        out[8] = if diacritic > 0 { 1.0 } else { 0.0 };
        out[9] = diacritic as f32;
        let _ = (letters_total, letters_upper, letters_lower);
    }

    fn extract_length(&self, s: &str, out: &mut [f32]) {
        let bytes_len = s.len() as f32;
        let mut chars = 0u32;
        let mut tokens = 0u32;
        let mut in_token = false;
        let mut tok_chars = 0u32;
        let mut tok_chars_total = 0u32;
        let mut tok_max = 0u32;
        for ch in s.chars() {
            chars += 1;
            if ch.is_whitespace() {
                if in_token {
                    tokens += 1;
                    tok_chars_total += tok_chars;
                    if tok_chars > tok_max {
                        tok_max = tok_chars;
                    }
                }
                in_token = false;
                tok_chars = 0;
            } else {
                in_token = true;
                tok_chars += 1;
            }
        }
        if in_token {
            tokens += 1;
            tok_chars_total += tok_chars;
            if tok_chars > tok_max {
                tok_max = tok_chars;
            }
        }
        out[0] = bytes_len;
        out[1] = chars as f32;
        out[2] = tokens as f32;
        out[3] = if tokens > 0 {
            tok_chars_total as f32 / tokens as f32
        } else {
            0.0
        };
        out[4] = tok_max as f32;
    }

    fn extract_postcode(&self, s: &str, out: &mut [f32]) {
        for (i, re) in self.postcode_regexes.iter().enumerate() {
            out[i] = if re.is_match(s) { 1.0 } else { 0.0 };
        }
    }

    fn extract_markers(&self, s: &str, out: &mut [f32]) {
        for v in out.iter_mut() {
            *v = 0.0;
        }
        // ascii_case_insensitive=true, so lowercase the input.
        // BUT lowercasing allocs; instead AhoCorasick was built with
        // ascii_case_insensitive — Aho-Corasick handles ASCII case
        // folding internally. Non-ASCII markers (like 丁目) are matched
        // exactly. For the morphology markers in scope this is fine.
        for m in self.markers_aho.find_iter(s.as_bytes()) {
            let id = m.pattern().as_usize();
            if id < out.len() {
                out[id] = 1.0;
            }
        }
    }

    fn extract_bigrams(&self, s: &str, out: &mut [f32]) {
        for v in out.iter_mut() {
            *v = 0.0;
        }
        let bytes = s.as_bytes();
        if bytes.len() < 2 {
            return;
        }
        for w in bytes.windows(2) {
            let key = (w[0] as u16) | ((w[1] as u16) << 8);
            if let Some(&idx) = self.bigram_index.get(&key) {
                out[idx] = 1.0;
            }
        }
    }
}

// ---- Helpers ----

fn has_exact_run(bytes: &[u8], target: u32) -> bool {
    let mut run = 0u32;
    let mut prev_d = false;
    let mut found = false;
    for (i, &b) in bytes.iter().enumerate() {
        let d = b.is_ascii_digit();
        if d {
            run += 1;
        } else if prev_d {
            // run ended
            // but only count if this run's length == target AND boundaries are non-digit on both sides.
            let _ = i;
            if run == target {
                found = true;
                break;
            }
            run = 0;
        }
        prev_d = d;
    }
    if !found && run == target {
        // also true if string ends with the run
        return true;
    }
    found
}

fn bytes_iter_runs_geq(bytes: &[u8], target: u32) -> u32 {
    let mut run = 0u32;
    let mut count = 0u32;
    for &b in bytes {
        if b.is_ascii_digit() {
            run += 1;
        } else {
            if run >= target {
                count += 1;
            }
            run = 0;
        }
    }
    if run >= target {
        count += 1;
    }
    count
}

fn sanitize_for_name(s: &str) -> String {
    s.chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c
            } else {
                '_'
            }
        })
        .collect()
}

fn escape_byte(b: u8) -> String {
    if b.is_ascii_alphanumeric() {
        format!("{}", b as char)
    } else {
        format!("x{:02x}", b)
    }
}

/// Saved feature extractor state (markers + bigrams + postcode patterns).
/// The Regex objects are not Serialize, so we store pattern strings.
#[derive(Serialize, Deserialize, Debug)]
pub struct ExtractorSpec {
    pub markers: Vec<String>,
    pub postcode_patterns: Vec<String>,
    pub bigrams: Vec<[u8; 2]>,
}

impl ExtractorSpec {
    pub fn build(&self) -> FeatureExtractor {
        let regexes = self
            .postcode_patterns
            .iter()
            .map(|p| Regex::new(p).expect("postcode pattern"))
            .collect();
        FeatureExtractor::new(self.markers.clone(), regexes, self.bigrams.clone())
    }

    pub fn save(&self, path: &Path) -> std::io::Result<()> {
        let json = serde_json::to_string(self).expect("serde");
        std::fs::write(path, json)
    }

    pub fn load(path: &Path) -> std::io::Result<Self> {
        let s = std::fs::read_to_string(path)?;
        Ok(serde_json::from_str(&s).expect("serde deserialize"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn small_extractor() -> FeatureExtractor {
        let markers = vec![
            "rue".to_string(),
            "straat".to_string(),
            "丁目".to_string(),
            "ул".to_string(),
        ];
        // Per-country postcode: just a couple of trivial samples
        let postcodes: Vec<Regex> = COUNTRIES
            .iter()
            .map(|_| Regex::new(r"^\d{4}$").unwrap())
            .collect();
        let bigrams = vec![*b"st", *b"ru", *b"de"];
        FeatureExtractor::new(markers, postcodes, bigrams)
    }

    #[test]
    fn script_flags_detect_cjk() {
        let ex = small_extractor();
        let s = "東京都千代田区";
        let mut buf = vec![0.0f32; ex.n_script_feats];
        ex.extract_script(s, &mut buf);
        // CJK is index 1 in flag block
        assert!(buf[1] > 0.5, "cjk flag should be set: {:?}", buf);
        // Latin index 0 should be 0
        assert!(buf[0] < 0.5);
    }

    #[test]
    fn digit_runs_basic() {
        let ex = small_extractor();
        let s = "Rue 12345 / 67";
        let mut buf = vec![0.0f32; ex.n_digit_feats];
        ex.extract_digit(s, &mut buf);
        // total 7 digits, longest run 5
        assert_eq!(buf[0], 7.0);
        assert_eq!(buf[1], 5.0);
    }

    #[test]
    fn marker_aho_finds() {
        let ex = small_extractor();
        let s = "Rue de la Paix";
        let mut buf = vec![0.0f32; ex.n_marker_feats];
        ex.extract_markers(s, &mut buf);
        // marker[0] = "rue"
        assert_eq!(buf[0], 1.0);
        assert_eq!(buf[1], 0.0);
    }

    #[test]
    fn bigram_lookup() {
        let ex = small_extractor();
        let s = "rue st germain";
        let mut buf = vec![0.0f32; ex.n_bigram_feats];
        ex.extract_bigrams(s, &mut buf);
        // bigrams: st(0), ru(1), de(2). Expect "st" and "ru" present.
        assert_eq!(buf[0], 1.0); // st
        assert_eq!(buf[1], 1.0); // ru
    }

    #[test]
    fn full_extract_consistent_length() {
        let ex = small_extractor();
        let f = ex.extract("Rue de la Paix");
        assert_eq!(f.len(), ex.n_features());
        assert_eq!(ex.feature_names.len(), ex.feature_classes.len());
    }
}
