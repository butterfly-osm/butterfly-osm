use crate::{MemoryBudget, PlanConfig};

pub struct AutopilotPlanner {
    config: PlanConfig,
}

impl AutopilotPlanner {
    pub fn new(config: PlanConfig) -> Self {
        Self { config }
    }

    /// Create a memory budget for the given configuration
    pub fn create_budget(&self) -> MemoryBudget {
        MemoryBudget::new(self.config.max_ram_mb, self.config.workers)
    }

    /// Validate the current plan and print diagnostics
    pub fn validate_plan(&self) -> bool {
        let budget = self.create_budget();

        if self.config.debug {
            println!("Memory Budget Validation:");
            println!("  cap_mb = {} MB", budget.cap_mb);
            println!(
                "  usable_mb = {} MB ({}%)",
                budget.usable_mb,
                (budget.usable_mb as f64 / budget.cap_mb as f64 * 100.0) as u32
            );
            println!(
                "  Constraint: {} + {} × {} + {} + {} ≤ {} MB",
                budget.fixed_overhead_mb,
                self.config.workers,
                budget.per_worker_mb,
                budget.io_buffers_mb,
                budget.merge_heaps_mb,
                budget.usable_mb
            );
        }

        budget.validate(self.config.workers)
    }
}
