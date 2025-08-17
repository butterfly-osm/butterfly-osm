//! Autopilot memory planning and resource allocation for butterfly-osm

pub mod adaptive;
pub mod cli;
pub mod config;
pub mod fuzzing;
pub mod memory;
pub mod planner;

pub use adaptive::{
    AdaptiveBuildPlan, AdaptivePlanner, ChunkSizes, ComplexityRating, DensityDistribution,
    MemoryFactors, TelemetrySummary, WorkerScaling,
};
pub use cli::PlanCli;
pub use config::PlanConfig;
pub use fuzzing::{
    DistributionType, FuzzingConfig, FuzzingResults, InvariantViolation, PlanFuzzer, Severity,
    TelemetryFuzzer, ViolationType,
};
pub use memory::MemoryBudget;
pub use planner::AutopilotPlanner;

/// Compile-time RAM cap in MB
pub const BFLY_MAX_RAM_MB: u32 = 16_384;
