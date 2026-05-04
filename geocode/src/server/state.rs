//! Shared server state.

use std::sync::Arc;
use std::time::Instant;

use crate::control::admission::AdmissionState;
use crate::control::{AdmissionPolicy, BudgetPolicy, FanoutConfig, GeneralMetrics};
use crate::geocoder::executor::ControlPlane;
use crate::shard::reader::Shard;

#[derive(Debug)]
pub struct ServerState {
    pub shard: Shard,
    pub started_at: Instant,
    pub version: &'static str,
    pub control: Arc<ControlPlane>,
    pub admission: AdmissionState,
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
        }
    }
}
