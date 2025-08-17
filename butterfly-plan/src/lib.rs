//! Autopilot memory planning and resource allocation for butterfly-osm

pub mod adaptive;
pub mod cli;
pub mod config;
pub mod fuzzing;
pub mod memory;
pub mod planner;

pub use adaptive::{
    AdaptivePlanner, AdaptiveBuildPlan, DensityDistribution, WorkerScaling,
    MemoryFactors, ChunkSizes, TelemetrySummary, ComplexityRating
};
pub use cli::PlanCli;
pub use config::PlanConfig;
pub use fuzzing::{
    PlanFuzzer, TelemetryFuzzer, FuzzingConfig, DistributionType,
    FuzzingResults, InvariantViolation, ViolationType, Severity
};
pub use memory::MemoryBudget;
pub use planner::AutopilotPlanner;

/// Compile-time RAM cap in MB
pub const BFLY_MAX_RAM_MB: u32 = 16_384;
