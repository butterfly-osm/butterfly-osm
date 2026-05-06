//! Shared server state (#205).
//!
//! ## Multi-country
//!
//! The server holds **N shards**, one per country, each paired with a
//! [`crate::index::RecallIndex`] sidecar. Forward queries route via
//! the [`crate::routing::classify_country`] posterior. Reverse queries
//! route via [`crate::routing::country_for_point`] (lat/lon bbox
//! membership).

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;

use anyhow::{Context, Result, bail};

use crate::confidence::{ConfidenceConfig, GbdtModel};
use crate::control::admission::AdmissionState;
use crate::control::{AdmissionPolicy, GeneralMetrics};
use crate::geocoder::recall::Recaller;
use crate::geocoder::rerank::Reranker;
use crate::index::RecallIndex;
use crate::parser::{HeuristicBackend, ParserBackend};
use crate::routing::{Classifier, CountryId, PackRegistry};
use crate::shard::reader::Shard;

#[derive(Debug)]
pub struct ServerState {
    /// Shards keyed by country.
    pub shards: HashMap<CountryId, Arc<Shard>>,
    /// Recall service. One [`RecallIndex`] per loaded shard. Populated
    /// at boot — every loaded shard must have a sibling
    /// `<base>.recall.fst`.
    pub recaller: Arc<Recaller>,
    /// Rerank service. Wraps the optional rerank GBDT model.
    pub reranker: Arc<Reranker>,
    pub started_at: Instant,
    pub version: &'static str,
    pub admission: AdmissionState,
    pub confidence_config: ConfidenceConfig,
    /// Active parser backend. Defaults to [`HeuristicBackend`] (neutral
    /// `TaggerSignals`); replaced via [`Self::with_parser`] to dispatch
    /// through the neural pipeline.
    pub parser: Arc<dyn ParserBackend>,
    pub classifier: Arc<Classifier>,
    pub general_metrics: GeneralMetrics,
}

impl ServerState {
    /// Single-country constructor. Tries to open a sibling recall
    /// index at `<shard_path>.recall.fst`. If the shard came from a
    /// `Shard` opened in-memory and there is no on-disk path, the
    /// recaller starts empty — operators must either build the index
    /// alongside the shard (the default `build-shard` path now does
    /// this) or load via [`Self::load_from_dir`].
    pub fn new(shard: Shard) -> Self {
        Self::with_config(shard, AdmissionPolicy::default())
    }

    /// Single-country constructor that also opens the sibling recall
    /// index at the given shard path. Used by tests + the single-shard
    /// CLI mode.
    pub fn new_with_recall_at(shard_path: &Path) -> Result<Self> {
        let shard = Shard::open(shard_path)
            .with_context(|| format!("opening shard at {}", shard_path.display()))?;
        let recall = RecallIndex::open(shard_path)
            .with_context(|| format!("opening recall index for {}", shard_path.display()))?;
        let country = shard.country();
        let mut shards: HashMap<CountryId, Arc<Shard>> = HashMap::with_capacity(1);
        shards.insert(country, Arc::new(shard));
        let mut recaller = Recaller::new();
        recaller.insert(country, recall);
        Ok(Self::with_shards_and_config(
            shards,
            AdmissionPolicy::default(),
            recaller,
        ))
    }

    pub fn with_config(shard: Shard, admission_policy: AdmissionPolicy) -> Self {
        let mut shards: HashMap<CountryId, Arc<Shard>> = HashMap::with_capacity(1);
        shards.insert(shard.country(), Arc::new(shard));
        Self::with_shards_and_config(shards, admission_policy, Recaller::new())
    }

    pub fn from_shards(shards: HashMap<CountryId, Arc<Shard>>) -> Self {
        Self::with_shards_and_config(shards, AdmissionPolicy::default(), Recaller::new())
    }

    pub fn from_shards_with_recaller(
        shards: HashMap<CountryId, Arc<Shard>>,
        recaller: Recaller,
    ) -> Self {
        Self::with_shards_and_config(shards, AdmissionPolicy::default(), recaller)
    }

    fn with_shards_and_config(
        shards: HashMap<CountryId, Arc<Shard>>,
        admission_policy: AdmissionPolicy,
        recaller: Recaller,
    ) -> Self {
        let metrics = GeneralMetrics::new();
        let admission = AdmissionState::new(admission_policy, metrics);
        let shipped_registry =
            Arc::new(PackRegistry::shipped().expect("shipped country packs must compile"));
        let classifier = Arc::new(Classifier::from_registry(shipped_registry));
        Self {
            shards,
            recaller: Arc::new(recaller),
            reranker: Arc::new(Reranker::new_no_model()),
            started_at: Instant::now(),
            version: env!("CARGO_PKG_VERSION"),
            admission,
            confidence_config: ConfidenceConfig::default(),
            parser: Arc::new(HeuristicBackend),
            classifier,
            general_metrics: metrics,
        }
    }

    /// Load every `*.bfgs` file in `dir`, plus its sibling recall
    /// index. Each shard's country comes from the BFGS header.
    pub fn load_from_dir<P: AsRef<Path>>(dir: P) -> Result<Self> {
        let dir = dir.as_ref();
        let mut shards: HashMap<CountryId, Arc<Shard>> = HashMap::new();
        let mut recaller = Recaller::new();
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
            // Try to open the sibling recall index. If it's missing,
            // surface a precise error pointing operators at the
            // `build-shard` command — there is no fallback path.
            let idx = RecallIndex::open(&path).with_context(|| {
                format!(
                    "opening recall index for shard at {} — rebuild via \
                     `butterfly-geocode build-shard --country {} ...`",
                    path.display(),
                    country.iso2()
                )
            })?;
            tracing::info!(
                country = country.iso2(),
                records = shard.record_count(),
                recall_keys = idx.key_count(),
                path = %path.display(),
                "loaded shard + recall index"
            );
            shards.insert(country, Arc::new(shard));
            recaller.insert(country, idx);
        }
        if shards.is_empty() {
            bail!(
                "no *.bfgs shards found in {} — build at least one with \
                 `butterfly-geocode build-shard --pbf <pbf> --out <dir>/<iso2>.bfgs --country <ISO2>`",
                dir.display()
            );
        }
        Ok(Self::from_shards_with_recaller(shards, recaller))
    }

    #[must_use]
    pub fn with_rerank_model(mut self, model: GbdtModel) -> Self {
        self.reranker =
            Arc::new(Reranker::new(model).with_confidence_config(self.confidence_config));
        self
    }

    #[must_use]
    pub fn with_confidence_config(mut self, cfg: ConfidenceConfig) -> Self {
        self.confidence_config = cfg;
        self
    }

    #[must_use]
    pub fn with_parser(mut self, parser: Arc<dyn ParserBackend>) -> Self {
        self.parser = parser;
        self
    }

    #[must_use]
    pub fn with_pack_registry(mut self, registry: Arc<PackRegistry>) -> Self {
        self.classifier = Arc::new(Classifier::from_registry(registry));
        self
    }

    #[must_use]
    pub fn with_admission_policy(mut self, policy: AdmissionPolicy) -> Self {
        let metrics = *self.admission.metrics();
        self.admission = AdmissionState::new(policy, metrics);
        self
    }

    #[must_use]
    pub fn total_record_count(&self) -> usize {
        self.shards.values().map(|s| s.record_count()).sum()
    }

    #[must_use]
    pub fn loaded_countries(&self) -> Vec<&'static str> {
        let mut v: Vec<&'static str> = self.shards.keys().map(|c| c.iso2()).collect();
        v.sort_unstable();
        v
    }

    #[must_use]
    pub fn pick_shard(&self, posterior: &[(CountryId, f32)]) -> Option<(CountryId, &Arc<Shard>)> {
        for (c, _) in posterior {
            if let Some(s) = self.shards.get(c) {
                return Some((*c, s));
            }
        }
        self.shards.iter().next().map(|(c, s)| (*c, s))
    }
}
