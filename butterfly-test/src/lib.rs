//! Test infrastructure and synthetic data generation for butterfly-osm

pub mod benchmarks;
pub mod corpus;
pub mod generators;

pub use benchmarks::{BenchmarkResults, BenchmarkRunner, GeometryBenchmarks};
pub use corpus::{fetch_monaco_data, load_sample_pbf_data, DatasetInfo, PbfInfo, TestCorpus};
pub use generators::{
    generate_test_linestring, KnownProblematicRegions, ProblematicRegionGenerator,
};

/// Test data generator
#[derive(Default)]
pub struct TestDataGenerator {}

impl TestDataGenerator {
    pub fn new() -> Self {
        Self {}
    }
}
