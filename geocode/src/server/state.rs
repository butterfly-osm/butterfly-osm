//! Shared server state.
//!
//! ## Multi-country (#96)
//!
//! The server holds **N shards**, one per country. Each shard is a
//! BFGS v3 file tagged with its [`CountryId`] in the file header. At
//! load time the server reads the country code from each shard and
//! keys it into [`ServerState::shards`].
//!
//! Forward queries route via the [`crate::routing::classify_country`]
//! posterior, top-K by [`crate::types::ExecutionBudget::max_countries`].
//! Reverse queries route via [`crate::routing::country_for_point`]
//! (lat/lon bbox membership). Both fall back to "search every loaded
//! shard" if the routing signal is empty.
//!
//! ## What if a country's shard is missing?
//!
//! Per the task spec: graceful degradation. The forward executor
//! filters its target-country list to the intersection with the loaded
//! shards. If the intersection is empty, the executor falls back to
//! the highest-posterior loaded shard, then (if even that fails) fans
//! out to every loaded shard. The HTTP handler turns "country pinned
//! but no shard for it" into a 503.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};

use crate::confidence::{ConfidenceConfig, GbdtModel};
use crate::control::admission::AdmissionState;
use crate::control::{AdmissionPolicy, BudgetPolicy, FanoutConfig, GeneralMetrics};
use crate::geocoder::executor::ControlPlane;
use crate::parser::{HeuristicBackend, ParserBackend};
use crate::routing::{Classifier, CountryId, PackRegistry};
use crate::shard::reader::Shard;

#[derive(Debug)]
pub struct ServerState {
    /// Shards keyed by country. Multi-country deployments load
    /// multiple shards; single-country deployments load one. The
    /// server is symmetric across the two cases — there is no
    /// dedicated "single-shard" code path.
    pub shards: HashMap<CountryId, Shard>,
    pub started_at: Instant,
    pub version: &'static str,
    pub control: Arc<ControlPlane>,
    pub admission: AdmissionState,
    /// Optional GBDT confidence reranker (#96 §Confidence Model). When
    /// `None`, the executor returns its raw scores untouched (no-model
    /// fallback path).
    pub rerank_model: Option<GbdtModel>,
    pub confidence_config: ConfidenceConfig,
    /// Active parser backend. Defaults to [`HeuristicBackend`] when no
    /// neural model is loaded; replaced via [`Self::with_parser`] to
    /// dispatch through the neural pipeline (#96 §Tagger + #98 Phase 1).
    pub parser: Arc<dyn ParserBackend>,
    /// Country classifier (forward routing) + bbox dispatcher
    /// (reverse routing). Built from the [`PackRegistry`] passed at
    /// construction time so `--pack-dir` overrides reach query-time
    /// classification. Defaults to the shipped registry when the
    /// constructor doesn't take an explicit one.
    pub classifier: Arc<Classifier>,
}

impl ServerState {
    /// Single-country constructor. The shard's country is read from
    /// its BFGS v3 header.
    pub fn new(shard: Shard) -> Self {
        Self::with_config(
            shard,
            BudgetPolicy::default(),
            FanoutConfig::default(),
            AdmissionPolicy::default(),
        )
    }

    pub fn with_config(
        shard: Shard,
        budget_policy: BudgetPolicy,
        fanout: FanoutConfig,
        admission_policy: AdmissionPolicy,
    ) -> Self {
        let mut shards = HashMap::with_capacity(1);
        shards.insert(shard.country(), shard);
        Self::with_shards_and_config(shards, budget_policy, fanout, admission_policy)
    }

    /// Construct from a pre-built map of shards.
    pub fn from_shards(shards: HashMap<CountryId, Shard>) -> Self {
        Self::with_shards_and_config(
            shards,
            BudgetPolicy::default(),
            FanoutConfig::default(),
            AdmissionPolicy::default(),
        )
    }

    fn with_shards_and_config(
        shards: HashMap<CountryId, Shard>,
        budget_policy: BudgetPolicy,
        fanout: FanoutConfig,
        admission_policy: AdmissionPolicy,
    ) -> Self {
        let metrics = GeneralMetrics::new();
        let control = Arc::new(ControlPlane {
            general: metrics,
            channels: crate::control::ChannelMetrics::new(),
            cost_calib: crate::control::CostCalibrationMetrics::new(),
            recomb: crate::control::RecombinationMetrics::new(),
            clean: crate::control::CleanQueryMetrics::new(),
            fanout,
            budget_policy,
        });
        let admission = AdmissionState::new(admission_policy, metrics);
        // Default classifier wraps the process-wide singleton over the
        // shipped registry. Callers that boot via `--pack-dir` replace
        // this with `with_pack_registry(...)` so the override packs
        // reach query-time dispatch.
        let shipped_registry =
            Arc::new(PackRegistry::shipped().expect("shipped country packs must compile"));
        let classifier = Arc::new(Classifier::from_registry(shipped_registry));
        Self {
            shards,
            started_at: Instant::now(),
            version: env!("CARGO_PKG_VERSION"),
            control,
            admission,
            rerank_model: None,
            confidence_config: ConfidenceConfig::default(),
            parser: Arc::new(HeuristicBackend),
            classifier,
        }
    }

    /// Load every `*.bfgs` file in `dir` and return a `ServerState`.
    /// Each shard's country is read from its BFGS v3 header — the
    /// filename is informational (we recommend `<iso2>.bfgs` /
    /// `<country>.bfgs` but the loader does not enforce it).
    ///
    /// Errors:
    /// - the directory cannot be read (Err)
    /// - no `.bfgs` files at all (Err — operator misconfiguration is
    ///   never a silent success)
    /// - two shards declare the same country (Err — duplicate routing
    ///   target)
    /// - a shard fails CRC / version check (Err — bubble up so the
    ///   operator sees it at boot, not on first query)
    pub fn load_from_dir<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        let mut shards: HashMap<CountryId, Shard> = HashMap::new();
        let entries = std::fs::read_dir(dir)
            .with_context(|| format!("reading shard directory {}", dir.display()))?;
        for entry in entries {
            let entry = entry.context("iterating shard directory")?;
            let path = entry.path();
            if path
                .extension()
                .and_then(|s| s.to_str())
                .map(|s| s.eq_ignore_ascii_case("bfgs"))
                != Some(true)
            {
                continue;
            }
            let shard = Shard::open(&path)
                .with_context(|| format!("loading shard at {}", path.display()))?;
            let country = shard.country();
            if shards.contains_key(&country) {
                bail!(
                    "two shards declare country {}: existing one in {} clashes with {}",
                    country.iso2(),
                    dir.display(),
                    path.display()
                );
            }
            tracing::info!(
                country = country.iso2(),
                records = shard.record_count(),
                path = %path.display(),
                "loaded shard"
            );
            shards.insert(country, shard);
        }
        if shards.is_empty() {
            bail!(
                "no *.bfgs shards found in {} — build at least one with \
                 `butterfly-geocode build-shard --pbf <pbf> --out <dir>/<iso2>.bfgs --country <ISO2>`",
                dir.display()
            );
        }
        Ok(Self::from_shards(shards))
    }

    /// Builder-style constructor for use with a trained reranker.
    #[must_use]
    pub fn with_rerank_model(mut self, model: GbdtModel) -> Self {
        self.rerank_model = Some(model);
        self
    }

    /// Override the threshold knobs (defaults are #96 BE Phase-0).
    #[must_use]
    pub fn with_confidence_config(mut self, cfg: ConfidenceConfig) -> Self {
        self.confidence_config = cfg;
        self
    }

    /// Replace the parser backend (e.g. with a [`crate::parser::NeuralBackend`]
    /// constructed from a loaded safetensors file).
    #[must_use]
    pub fn with_parser(mut self, parser: Arc<dyn ParserBackend>) -> Self {
        self.parser = parser;
        self
    }

    /// Replace the classifier with one backed by the supplied
    /// [`PackRegistry`]. Used by the `serve --pack-dir` boot path so
    /// override packs reach query-time forward + reverse dispatch.
    #[must_use]
    pub fn with_pack_registry(mut self, registry: Arc<PackRegistry>) -> Self {
        self.classifier = Arc::new(Classifier::from_registry(registry));
        self
    }

    /// Total number of records across all loaded shards.
    #[must_use]
    pub fn total_record_count(&self) -> usize {
        self.shards.values().map(|s| s.record_count()).sum()
    }

    /// Sorted list of loaded countries (ISO2). Used by `/health` and
    /// `/metrics` to surface what is mounted.
    #[must_use]
    pub fn loaded_countries(&self) -> Vec<&'static str> {
        let mut v: Vec<&'static str> = self.shards.keys().map(|c| c.iso2()).collect();
        v.sort_unstable();
        v
    }

    /// Pick a shard for the country candidates in `posterior`, in
    /// descending posterior order. Returns the first matching shard
    /// alongside its country code, or `None` if none of the
    /// candidates is loaded. Used by the handler for the
    /// control-plane single-shard code path.
    #[must_use]
    pub fn pick_shard(&self, posterior: &[(CountryId, f32)]) -> Option<(CountryId, &Shard)> {
        for (c, _) in posterior {
            if let Some(s) = self.shards.get(c) {
                return Some((*c, s));
            }
        }
        // No posterior overlap. Fall back to whichever shard is loaded
        // (any one — the handler treats this as a fan-out path).
        self.shards.iter().next().map(|(c, s)| (*c, s))
    }
}
