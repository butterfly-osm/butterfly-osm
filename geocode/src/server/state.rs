//! Shared server state.

use std::sync::Arc;
use std::time::Instant;

use crate::confidence::{ConfidenceConfig, GbdtModel};
use crate::control::admission::AdmissionState;
use crate::control::{AdmissionPolicy, BudgetPolicy, FanoutConfig, GeneralMetrics};
use crate::geocoder::executor::ControlPlane;
use crate::parser::{HeuristicBackend, ParserBackend};
use crate::shard::reader::Shard;

#[derive(Debug)]
pub struct ServerState {
    pub shard: Shard,
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
}

impl ServerState {
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
        Self {
            shard,
            started_at: Instant::now(),
            version: env!("CARGO_PKG_VERSION"),
            control,
            admission,
            rerank_model: None,
            confidence_config: ConfidenceConfig::default(),
            parser: Arc::new(HeuristicBackend),
        }
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
}
