//! CLI interface for butterfly-plan
//!
//! Provides command-line tools for memory planning and validation.

use crate::{AutopilotPlanner, PlanConfig};
use std::env;
use std::path::Path;

/// Command-line interface for autopilot planning
pub struct PlanCli {
    config: PlanConfig,
}

impl PlanCli {
    /// Create CLI with default configuration
    pub fn new() -> Self {
        Self {
            config: PlanConfig::default(),
        }
    }

    /// Create CLI from command line arguments
    pub fn from_args(args: Vec<String>) -> Result<Self, String> {
        let mut config = PlanConfig::default();
        let mut i = 1; // Skip program name

        while i < args.len() {
            match args[i].as_str() {
                "--max-ram" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--max-ram requires a value".to_string());
                    }
                    config.max_ram_mb = args[i].parse().map_err(|_| "Invalid RAM value")?;
                }
                "--workers" => {
                    i += 1;
                    if i >= args.len() {
                        return Err("--workers requires a value".to_string());
                    }
                    config.workers = args[i].parse().map_err(|_| "Invalid worker count")?;
                }
                "--deterministic" => {
                    config.deterministic = true;
                }
                "--debug-plan" => {
                    config.debug = true;
                }
                arg => {
                    return Err(format!("Unknown argument: {}", arg));
                }
            }
            i += 1;
        }

        Ok(Self { config })
    }

    /// Load configuration from environment variables
    pub fn load_env(&mut self) {
        if let Ok(ram_str) = env::var("BFLY_MAX_RAM_MB") {
            if let Ok(ram_mb) = ram_str.parse() {
                self.config.max_ram_mb = ram_mb;
            }
        }

        if let Ok(workers_str) = env::var("BFLY_WORKERS") {
            if let Ok(workers) = workers_str.parse() {
                self.config.workers = workers;
            }
        }

        if env::var("BFLY_DETERMINISTIC").is_ok() {
            self.config.deterministic = true;
        }

        if env::var("BFLY_DEBUG").is_ok() {
            self.config.debug = true;
        }
    }

    /// Load configuration from TOML file
    pub fn load_toml<P: AsRef<Path>>(&mut self, path: P) -> Result<(), String> {
        let content = std::fs::read_to_string(path)
            .map_err(|e| format!("Failed to read config file: {}", e))?;

        let config: PlanConfig =
            toml::from_str(&content).map_err(|e| format!("Failed to parse TOML: {}", e))?;

        self.config = config;
        Ok(())
    }

    /// Execute --validate-plan command
    pub fn validate_plan(&self) -> Result<(), String> {
        let planner = AutopilotPlanner::new(self.config.clone());
        let budget = planner.create_budget();

        // Always show the validation inequality with numbers
        println!("Memory Budget Validation:");
        println!("  Total RAM cap: {} MB", budget.cap_mb);
        println!("  Usable RAM (78% safety): {} MB", budget.usable_mb);
        println!();
        println!("Budget Breakdown:");
        println!("  Fixed overhead: {} MB", budget.fixed_overhead_mb);
        println!(
            "  Workers: {} × {} MB = {} MB",
            self.config.workers,
            budget.per_worker_mb,
            self.config.workers * budget.per_worker_mb
        );
        println!("  I/O buffers: {} MB", budget.io_buffers_mb);
        println!("  Merge heaps: {} MB", budget.merge_heaps_mb);

        let total_usage = budget.fixed_overhead_mb
            + (self.config.workers * budget.per_worker_mb)
            + budget.io_buffers_mb
            + budget.merge_heaps_mb;

        println!();
        println!("Constraint Check:");
        println!(
            "  {} + {} + {} + {} = {} MB",
            budget.fixed_overhead_mb,
            self.config.workers * budget.per_worker_mb,
            budget.io_buffers_mb,
            budget.merge_heaps_mb,
            total_usage
        );
        println!(
            "  {} ≤ {} MB: {}",
            total_usage,
            budget.usable_mb,
            if total_usage <= budget.usable_mb {
                "✅ PASS"
            } else {
                "❌ FAIL"
            }
        );

        if total_usage > budget.usable_mb {
            println!();
            println!(
                "⚠️  Memory budget exceeded by {} MB",
                total_usage - budget.usable_mb
            );
            return Err("Memory budget validation failed".to_string());
        }

        println!();
        println!("✅ Memory budget validation passed");
        println!("   Available margin: {} MB", budget.usable_mb - total_usage);

        Ok(())
    }

    /// Execute --debug-plan command
    pub fn debug_plan(&self) {
        println!("Autopilot Plan Debug Information");
        println!("================================");
        println!();

        println!("Configuration:");
        println!("  max_ram_mb: {}", self.config.max_ram_mb);
        println!("  workers: {}", self.config.workers);
        println!("  deterministic: {}", self.config.deterministic);
        println!("  debug: {}", self.config.debug);
        println!();

        if self.config.deterministic {
            println!("Deterministic Mode Settings:");
            println!("  ✓ Fixed zstd dictionaries disabled");
            println!("  ✓ Fixed worker count: {}", self.config.workers);
            println!("  ✓ Fixed run size and fan-in");
            println!("  ✓ Auto-throttle disabled");
            println!();
        }

        let planner = AutopilotPlanner::new(self.config.clone());
        let budget = planner.create_budget();

        println!("Memory Budget Details:");
        println!("  RAM Cap: {} MB", budget.cap_mb);
        println!(
            "  Safety Margin: {}% (leaves {} MB usable)",
            78, budget.usable_mb
        );
        println!(
            "  Fixed Overhead: {} MB (JVM, OS buffers)",
            budget.fixed_overhead_mb
        );
        println!(
            "  Per-Worker: {} MB × {} workers",
            budget.per_worker_mb, self.config.workers
        );
        println!(
            "  I/O Buffers: {} MB (read/write caching)",
            budget.io_buffers_mb
        );
        println!(
            "  Merge Heaps: {} MB (k-way merge structures)",
            budget.merge_heaps_mb
        );
        println!();

        println!("Calculated Constraints:");
        println!("  workers × per_worker_mb + io_buffers_mb + merge_heaps_mb ≤ usable_mb");
        println!(
            "  {} × {} + {} + {} ≤ {}",
            self.config.workers,
            budget.per_worker_mb,
            budget.io_buffers_mb,
            budget.merge_heaps_mb,
            budget.usable_mb
        );

        let result = self.validate_plan();
        match result {
            Ok(_) => println!("  Status: ✅ Valid"),
            Err(e) => println!("  Status: ❌ {}", e),
        }
    }

    /// Get the current configuration
    pub fn config(&self) -> &PlanConfig {
        &self.config
    }
}

impl Default for PlanCli {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_cli_args_parsing() {
        let args = vec![
            "butterfly-plan".to_string(),
            "--max-ram".to_string(),
            "8192".to_string(),
            "--workers".to_string(),
            "4".to_string(),
            "--deterministic".to_string(),
        ];

        let cli = PlanCli::from_args(args).expect("Failed to parse args");
        assert_eq!(cli.config.max_ram_mb, 8192);
        assert_eq!(cli.config.workers, 4);
        assert!(cli.config.deterministic);
    }

    #[test]
    fn test_validate_plan() {
        let mut cli = PlanCli::new();
        cli.config.max_ram_mb = 4096; // 4GB should be enough for validation
        cli.config.workers = 2;

        // This should pass validation
        let result = cli.validate_plan();
        assert!(result.is_ok());
    }
}
