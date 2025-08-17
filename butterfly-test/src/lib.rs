//! Test infrastructure and synthetic data generation for butterfly-osm

pub mod benchmarks;
pub mod corpus;
pub mod generators;

/// Test data generator
#[derive(Default)]
pub struct TestDataGenerator {}

impl TestDataGenerator {
    pub fn new() -> Self {
        Self {}
    }
}
