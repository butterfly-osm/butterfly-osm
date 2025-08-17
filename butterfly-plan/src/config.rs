use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PlanConfig {
    /// Maximum RAM usage in MB
    pub max_ram_mb: u32,
    /// Number of worker threads
    pub workers: u32,
    /// Deterministic mode (fixed parameters)
    pub deterministic: bool,
    /// Debug output enabled
    pub debug: bool,
}

impl Default for PlanConfig {
    fn default() -> Self {
        Self {
            max_ram_mb: crate::BFLY_MAX_RAM_MB,
            workers: num_cpus::get() as u32,
            deterministic: false,
            debug: false,
        }
    }
}
