/// Memory budget calculations and enforcement
#[derive(Debug, Clone)]
pub struct MemoryBudget {
    /// Total cap in MB
    pub cap_mb: u32,
    /// Usable amount (75-80% safety margin)
    pub usable_mb: u32,
    /// Per-worker allocation
    pub per_worker_mb: u32,
    /// I/O buffer allocation
    pub io_buffers_mb: u32,
    /// Merge heap allocation
    pub merge_heaps_mb: u32,
    /// Fixed overhead
    pub fixed_overhead_mb: u32,
}

impl MemoryBudget {
    pub fn new(cap_mb: u32, _workers: u32) -> Self {
        let usable_mb = (cap_mb as f64 * 0.78).floor() as u32;

        Self {
            cap_mb,
            usable_mb,
            per_worker_mb: 8,   // Initial estimate
            io_buffers_mb: 64,  // Initial estimate
            merge_heaps_mb: 32, // Initial estimate
            fixed_overhead_mb: 256,
        }
    }

    /// Validate that the budget fits within constraints
    pub fn validate(&self) -> bool {
        let total =
            self.fixed_overhead_mb + self.per_worker_mb + self.io_buffers_mb + self.merge_heaps_mb;
        total <= self.usable_mb
    }
}
