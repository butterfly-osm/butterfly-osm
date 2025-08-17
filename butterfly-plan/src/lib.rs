//! Autopilot memory planning and resource allocation for butterfly-osm

pub mod cli;
pub mod config;
pub mod memory;
pub mod planner;

pub use cli::PlanCli;
pub use config::PlanConfig;
pub use memory::MemoryBudget;
pub use planner::AutopilotPlanner;

/// Compile-time RAM cap in MB
pub const BFLY_MAX_RAM_MB: u32 = 16_384;
