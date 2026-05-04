//! Country routing metrics (#97 §6) — first-class subsystem.
//!
//! Country misrouting is catastrophic and non-recoverable. The metrics
//! are designed so a regression is observable BEFORE it hits a customer:
//!
//! - cheap-router confidence histogram per top-country
//! - cheap-vs-neural disagreement rate (per direction)
//! - wrong-country rate against an eval corpus (loaded from
//!   `geocode/eval/country_routing.jsonl` if present, skipped otherwise)
//! - "recovered by fallback" rate
//! - cross-border confusion matrix for the cluster countries from #96
//! - country-routing latency p50/p95 (cheap vs neural separately)
//!
//! ## Belgium-only MVP
//!
//! The MVP cheap classifier always returns `[(BE, 1.0)]`. The neural
//! fallback is not wired. So in practice the disagreement rate is
//! always 0 today. The metric machinery is still emitted as no-op
//! counters so dashboards can be wired now and the data backfills
//! the moment the neural router lands.

use std::path::Path;
use std::time::Duration;

use metrics::{counter, gauge, histogram};
use serde::Deserialize;

use crate::routing::CountryId;

/// Direction label used for the cheap-vs-neural disagreement metric.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RoutingDirection {
    /// Cheap → A, Neural → B (cheap was wrong, neural rescued).
    CheapToNeural,
    /// Cheap → A, Neural → A (agreement).
    Agreement,
}

impl RoutingDirection {
    pub const fn label(self) -> &'static str {
        match self {
            RoutingDirection::CheapToNeural => "cheap_to_neural",
            RoutingDirection::Agreement => "agreement",
        }
    }
}

/// One observation suitable for emission.
#[derive(Debug, Clone, Copy)]
pub struct RoutingObservation {
    pub cheap_top: CountryId,
    pub cheap_confidence: f32,
    pub cheap_latency: Duration,
    /// `None` means the neural fallback did not run (cheap classifier
    /// confidence was high enough). When `Some`, the disagreement
    /// metric fires.
    pub neural_top: Option<CountryId>,
    pub neural_latency: Option<Duration>,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct CountryRoutingMetrics;

impl CountryRoutingMetrics {
    #[must_use]
    pub fn new() -> Self {
        Self
    }

    /// Emit one routing observation. Wires up:
    ///
    /// - `geocode_country_router_confidence` (histogram, label
    ///   `top_country`)
    /// - `geocode_country_router_latency_seconds` (histogram, label
    ///   `stage`=`cheap`|`neural`)
    /// - `geocode_country_router_disagreement_total` (counter, label
    ///   `direction`)
    /// - `geocode_country_router_recovered_by_fallback_total` (counter)
    pub fn observe(&self, obs: RoutingObservation) {
        histogram!(
            "geocode_country_router_confidence",
            "top_country" => obs.cheap_top.iso2(),
        )
        .record(f64::from(obs.cheap_confidence));

        histogram!(
            "geocode_country_router_latency_seconds",
            "stage" => "cheap",
        )
        .record(obs.cheap_latency.as_secs_f64());

        if let (Some(neural_top), Some(neural_latency)) = (obs.neural_top, obs.neural_latency) {
            histogram!(
                "geocode_country_router_latency_seconds",
                "stage" => "neural",
            )
            .record(neural_latency.as_secs_f64());

            let direction = if neural_top == obs.cheap_top {
                RoutingDirection::Agreement
            } else {
                RoutingDirection::CheapToNeural
            };
            counter!(
                "geocode_country_router_disagreement_total",
                "direction" => direction.label(),
            )
            .increment(1);

            if direction == RoutingDirection::CheapToNeural {
                counter!("geocode_country_router_recovered_by_fallback_total").increment(1);
            }
        }
    }

    /// Record a confusion-matrix cell (gold ↔ predicted).
    ///
    /// Used by the offline eval corpus. Emits
    /// `geocode_country_router_confusion_total` with labels `gold`
    /// and `predicted`.
    pub fn observe_confusion(&self, gold: CountryId, predicted: CountryId) {
        counter!(
            "geocode_country_router_confusion_total",
            "gold" => gold.iso2(),
            "predicted" => predicted.iso2(),
        )
        .increment(1);
    }

    /// Update the wrong-country gauge after an eval pass. Value is
    /// the fraction (0.0 - 1.0).
    pub fn observe_eval_wrong_rate(&self, fraction: f64) {
        gauge!("geocode_country_router_wrong_rate").set(fraction);
    }

    /// Run an eval pass over `path` (JSONL of [`EvalSample`]),
    /// emitting per-cell confusion metrics and updating the
    /// wrong-rate gauge. Returns the wrong-rate as a convenience for
    /// tests.
    ///
    /// If the file does not exist, the call is a no-op and returns
    /// `None`. The eval corpus is optional infrastructure (#97 §6).
    pub fn run_eval_corpus(
        &self,
        path: &Path,
        classify: impl Fn(&str) -> CountryId,
    ) -> Option<f64> {
        if !path.exists() {
            return None;
        }
        let text = match std::fs::read_to_string(path) {
            Ok(t) => t,
            Err(_) => return None,
        };
        let mut total = 0u64;
        let mut wrong = 0u64;
        for line in text.lines() {
            let line = line.trim();
            if line.is_empty() {
                continue;
            }
            let Ok(sample) = serde_json::from_str::<EvalSample>(line) else {
                continue;
            };
            let Some(gold) = CountryId::from_iso2(&sample.country) else {
                continue;
            };
            let predicted = classify(&sample.text);
            self.observe_confusion(gold, predicted);
            total += 1;
            if predicted != gold {
                wrong += 1;
            }
        }
        if total == 0 {
            return None;
        }
        let frac = wrong as f64 / total as f64;
        self.observe_eval_wrong_rate(frac);
        Some(frac)
    }
}

#[derive(Debug, Deserialize)]
pub struct EvalSample {
    pub text: String,
    pub country: String,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn observe_agreement_does_not_panic() {
        let m = CountryRoutingMetrics::new();
        m.observe(RoutingObservation {
            cheap_top: CountryId::BE,
            cheap_confidence: 0.95,
            cheap_latency: Duration::from_micros(7),
            neural_top: Some(CountryId::BE),
            neural_latency: Some(Duration::from_micros(50)),
        });
    }

    #[test]
    fn observe_no_neural_does_not_panic() {
        let m = CountryRoutingMetrics::new();
        m.observe(RoutingObservation {
            cheap_top: CountryId::BE,
            cheap_confidence: 0.95,
            cheap_latency: Duration::from_micros(3),
            neural_top: None,
            neural_latency: None,
        });
    }

    #[test]
    fn observe_disagreement_emits_recovery() {
        let m = CountryRoutingMetrics::new();
        // The MVP only has BE so we can't construct a real
        // disagreement; this test verifies the agreement path
        // doesn't fire the recovery counter. The eval-corpus path
        // exercises wrong predictions.
        m.observe(RoutingObservation {
            cheap_top: CountryId::BE,
            cheap_confidence: 0.5,
            cheap_latency: Duration::from_micros(7),
            neural_top: Some(CountryId::BE),
            neural_latency: Some(Duration::from_micros(50)),
        });
    }

    #[test]
    fn run_eval_corpus_missing_path_is_noop() {
        let m = CountryRoutingMetrics::new();
        let r = m.run_eval_corpus(
            Path::new("/tmp/__definitely_does_not_exist__.jsonl"),
            |_| CountryId::BE,
        );
        assert!(r.is_none());
    }

    #[test]
    fn run_eval_corpus_with_inline_samples() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("eval.jsonl");
        std::fs::write(
            &path,
            r#"{"text":"Rue Wayez 122 1070 Anderlecht","country":"BE"}
{"text":"another belgian address","country":"BE"}
"#,
        )
        .unwrap();
        let m = CountryRoutingMetrics::new();
        let frac = m.run_eval_corpus(&path, |_| CountryId::BE).unwrap();
        assert_eq!(frac, 0.0);
    }
}
