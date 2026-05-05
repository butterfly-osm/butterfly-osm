//! Data-driven country classifier (#96 §Country Routing).
//!
//! The classifier loads every [`super::CountryPack`] at startup and
//! scores incoming text against every pack. Cheap, deterministic,
//! Bayesian-flavoured: `P(country | text) ∝ P(text | country) × P(country)`,
//! softmaxed across all loaded packs.
//!
//! ## Why "data-driven" not enum-bound
//!
//! The previous classifier (PR #169) hardcoded BE/FR/NL/LU/DE/AT/CH
//! signal logic. That model didn't generalise to Japan, Brazil, India,
//! the US, or Australia — the symbols were European. The new
//! architecture pushes the per-country scoring into the pack itself
//! ([`super::CountryPack::score`]) and the classifier becomes a pure
//! orchestrator: collect per-pack log-evidence, softmax, return the
//! posterior.
//!
//! Adding a country = drop a TOML pack. Zero classifier code changes.

use std::sync::{Arc, OnceLock};

use super::{CountryId, CountryPack, PackRegistry};

/// Classifier holding a [`PackRegistry`]. Two construction modes are
/// supported:
///
/// 1. [`Classifier::shipped`] — process-wide singleton backed by the
///    binary's embedded shipped packs. Used by call sites that don't
///    have access to a `ServerState` (heuristic parser tests, the
///    legacy `classify_country()` free function, neural decoder
///    fast-path lookup).
/// 2. [`Classifier::from_registry`] — owned classifier backed by an
///    arbitrary registry (typically built from
///    `PackRegistry::shipped_with_overrides`). The HTTP server uses
///    this so `--pack-dir` overrides actually reach query-time
///    classification + bbox dispatch.
#[derive(Debug)]
pub struct Classifier {
    registry: ClassifierRegistry,
}

#[derive(Debug)]
enum ClassifierRegistry {
    /// Borrowed from a process-wide static. The shipped singleton.
    Static(&'static PackRegistry),
    /// Owned by an `Arc`. The pack-dir-aware path.
    Owned(Arc<PackRegistry>),
}

impl Classifier {
    /// Singleton classifier backed by the binary's shipped packs.
    /// Initialised on first call; subsequent calls are a static load.
    #[must_use]
    pub fn shipped() -> &'static Classifier {
        static C: OnceLock<Classifier> = OnceLock::new();
        C.get_or_init(|| {
            static REG: OnceLock<PackRegistry> = OnceLock::new();
            let reg = REG.get_or_init(|| {
                PackRegistry::shipped().expect("shipped country packs must compile")
            });
            Classifier {
                registry: ClassifierRegistry::Static(reg),
            }
        })
    }

    /// Construct an owned classifier from an arbitrary registry. The
    /// server uses this with `PackRegistry::shipped_with_overrides`
    /// so `--pack-dir` overrides reach query-time classification.
    #[must_use]
    pub fn from_registry(registry: Arc<PackRegistry>) -> Self {
        Self {
            registry: ClassifierRegistry::Owned(registry),
        }
    }

    /// Classify the country distribution implied by `text`. Returns a
    /// `(country, weight)` list whose weights sum to 1.0, sorted
    /// descending by weight.
    ///
    /// If no signal fires across any pack, returns a uniform
    /// distribution over all loaded packs.
    #[must_use]
    pub fn classify(&self, text: &str) -> Vec<(CountryId, f32)> {
        let lower = text.to_lowercase();
        let scores: Vec<(CountryId, f32)> = self
            .registry()
            .iter()
            .map(|p: &std::sync::Arc<CountryPack>| (p.country, p.score(text, &lower)))
            .collect();
        normalize_scores(scores)
    }

    #[must_use]
    pub fn registry(&self) -> &PackRegistry {
        match &self.registry {
            ClassifierRegistry::Static(r) => r,
            ClassifierRegistry::Owned(r) => r.as_ref(),
        }
    }

    /// Return every country whose bbox contains the point. Mirrors the
    /// free function [`super::supported_countries_for_point`] but uses
    /// THIS classifier's registry — the difference matters when an
    /// operator launched the server with `--pack-dir` overrides that
    /// patch a shipped pack's bbox.
    #[must_use]
    pub fn supported_for_point(&self, lat: f64, lon: f64) -> Vec<CountryId> {
        self.registry()
            .iter()
            .filter(|p| p.bbox.contains(lat, lon))
            .map(|p| p.country)
            .collect()
    }

    /// Smallest-bbox country containing the point. Mirrors the free
    /// function [`super::country_for_point`].
    #[must_use]
    pub fn country_for_point(&self, lat: f64, lon: f64) -> Option<CountryId> {
        self.registry()
            .iter()
            .filter(|p| p.bbox.contains(lat, lon))
            .min_by(|a, b| {
                a.bbox
                    .area_deg2()
                    .partial_cmp(&b.bbox.area_deg2())
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .map(|p| p.country)
    }
}

fn normalize_scores(mut scores: Vec<(CountryId, f32)>) -> Vec<(CountryId, f32)> {
    if scores.is_empty() {
        return scores;
    }
    if scores.iter().all(|(_, s)| *s == 0.0) {
        let n = scores.len() as f32;
        for (_, s) in &mut scores {
            *s = 1.0 / n;
        }
        scores.sort_by(|a, b| a.0.as_str().cmp(b.0.as_str()));
        return scores;
    }
    let max = scores
        .iter()
        .map(|(_, s)| *s)
        .fold(f32::NEG_INFINITY, f32::max);
    let exps: Vec<f32> = scores.iter().map(|(_, s)| (*s - max).exp()).collect();
    let sum: f32 = exps.iter().sum();
    for ((_, s), e) in scores.iter_mut().zip(exps.iter()) {
        *s = *e / sum;
    }
    scores.sort_by(|a, b| {
        b.1.partial_cmp(&a.1)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| a.0.as_str().cmp(b.0.as_str()))
    });
    scores
}

/// Backwards-compatible top-level entry point. Equivalent to
/// `Classifier::shipped().classify(text)`.
#[must_use]
pub fn classify_country(text: &str) -> Vec<(CountryId, f32)> {
    Classifier::shipped().classify(text)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn top(v: &[(CountryId, f32)]) -> CountryId {
        v[0].0
    }
    fn weight(v: &[(CountryId, f32)], c: CountryId) -> f32 {
        v.iter()
            .find(|(x, _)| *x == c)
            .map(|(_, w)| *w)
            .unwrap_or(0.0)
    }

    #[test]
    fn weights_sum_to_one_across_15_packs() {
        for q in [
            "Rue Wayez 122 1070 Anderlecht",
            "Damrak 1 1012 LP Amsterdam",
            "Friedrichstraße 100 10117 Berlin",
            "Stephansplatz 1 1010 Wien",
            "Bahnhofstrasse 1 8001 Zürich",
            "L-2453 Luxembourg",
            "10 rue de la Paix 75001 Paris",
            "東京都千代田区千代田1-1",
            "1600 Pennsylvania Ave NW Washington DC 20500",
            "Avenida Paulista 1578 São Paulo 01310-200",
            "Rajpath New Delhi 110001",
            "1 Macquarie Street Sydney NSW 2000",
            "10 Downing Street SW1A 2AA London",
            "Calle Mayor 1 28013 Madrid",
            "Via Roma 1 00184 Roma",
            "",
            "no markers here just gibberish",
        ] {
            let r = classify_country(q);
            let s: f32 = r.iter().map(|(_, w)| w).sum();
            assert!((s - 1.0).abs() < 1e-3, "weights for {q:?} sum to {s}");
        }
    }

    #[test]
    fn top_country_be_for_brussels_query() {
        assert_eq!(
            top(&classify_country("Rue Wayez 122 1070 Anderlecht")),
            CountryId::BE
        );
    }

    #[test]
    fn top_country_fr_for_paris_query() {
        assert_eq!(
            top(&classify_country("10 rue de la Paix 75001 Paris")),
            CountryId::FR
        );
    }

    #[test]
    fn top_country_nl_for_amsterdam_query() {
        assert_eq!(
            top(&classify_country("Damrak 1 1012 LP Amsterdam")),
            CountryId::NL
        );
    }

    #[test]
    fn top_country_de_for_berlin_query() {
        assert_eq!(
            top(&classify_country("Friedrichstraße 100 10117 Berlin")),
            CountryId::DE
        );
    }

    #[test]
    fn top_country_at_for_vienna_query() {
        assert_eq!(
            top(&classify_country("Stephansplatz 1 1010 Wien")),
            CountryId::AT
        );
    }

    #[test]
    fn top_country_ch_for_zurich_query() {
        assert_eq!(
            top(&classify_country("Bahnhofstrasse 1 8001 Zürich")),
            CountryId::CH
        );
    }

    #[test]
    fn top_country_lu_for_lprefixed() {
        assert_eq!(top(&classify_country("L-2453 Luxembourg")), CountryId::LU);
    }

    // ===== Non-European tests — the architectural pivot proof =====

    #[test]
    fn top_country_jp_for_tokyo_kanji() {
        assert_eq!(
            top(&classify_country("東京都千代田区千代田1-1")),
            CountryId::JP
        );
    }

    #[test]
    fn top_country_us_for_dc_query() {
        assert_eq!(
            top(&classify_country(
                "1600 Pennsylvania Ave NW Washington DC 20500"
            )),
            CountryId::US
        );
    }

    #[test]
    fn top_country_br_for_sao_paulo() {
        assert_eq!(
            top(&classify_country(
                "Avenida Paulista 1578 São Paulo 01310-200"
            )),
            CountryId::BR
        );
    }

    #[test]
    fn top_country_in_for_delhi() {
        assert_eq!(
            top(&classify_country("Rajpath New Delhi 110001")),
            CountryId::IN
        );
    }

    #[test]
    fn top_country_au_for_sydney() {
        assert_eq!(
            top(&classify_country("1 Macquarie Street Sydney NSW 2000")),
            CountryId::AU
        );
    }

    #[test]
    fn top_country_gb_for_london() {
        assert_eq!(
            top(&classify_country("10 Downing Street SW1A 2AA London")),
            CountryId::GB
        );
    }

    #[test]
    fn top_country_es_for_madrid() {
        assert_eq!(
            top(&classify_country("Calle Mayor 1 28013 Madrid")),
            CountryId::ES
        );
    }

    #[test]
    fn top_country_it_for_rome() {
        assert_eq!(
            top(&classify_country("Via Roma 1 00184 Roma")),
            CountryId::IT
        );
    }

    #[test]
    fn empty_falls_back_to_uniform_over_all_packs() {
        let r = classify_country("");
        let n = r.len();
        assert!(n > 0);
        let expected = 1.0 / n as f32;
        for (_, w) in &r {
            assert!(
                (w - expected).abs() < 1e-3,
                "expected uniform {expected}, got {w}"
            );
        }
    }

    #[test]
    fn ambiguous_4digit_postcode_ranks_multiple_countries() {
        // "1070" matches the BE/LU/AT/CH/AU 4-digit shape (and IT 5-dig
        // does NOT — but BR 8-dig with no context also doesn't). Without
        // lexical disambiguation the classifier distributes weight.
        let r = classify_country("1070");
        for c in [
            CountryId::BE,
            CountryId::LU,
            CountryId::AT,
            CountryId::CH,
            CountryId::AU,
        ] {
            assert!(weight(&r, c) > 0.0, "expected positive weight for {c}");
        }
    }
}
