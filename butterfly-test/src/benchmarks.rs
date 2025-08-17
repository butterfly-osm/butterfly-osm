//! Benchmarking infrastructure

use std::time::Instant;

/// Simple benchmark runner
#[derive(Default)]
pub struct BenchmarkRunner {}

impl BenchmarkRunner {
    pub fn new() -> Self {
        Self {}
    }

    /// Run a simple timing benchmark
    pub fn time_function<F, R>(name: &str, f: F) -> R
    where
        F: FnOnce() -> R,
    {
        let start = Instant::now();
        let result = f();
        let elapsed = start.elapsed();
        println!("Benchmark {}: {:?}", name, elapsed);
        result
    }
}
