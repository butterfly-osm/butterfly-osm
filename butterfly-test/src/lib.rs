//! Test infrastructure and synthetic data generation for butterfly-osm

pub mod benchmarks;
pub mod corpus;
pub mod generators;

pub use benchmarks::{BenchmarkRunner, BenchmarkResults, GeometryBenchmarks};
pub use corpus::{TestCorpus, PbfInfo, DatasetInfo, fetch_monaco_data, load_sample_pbf_data};
pub use generators::{ProblematicRegionGenerator, KnownProblematicRegions, generate_test_linestring};

/// Test data generator
#[derive(Default)]
pub struct TestDataGenerator {}

impl TestDataGenerator {
    pub fn new() -> Self {
        Self {}
    }
}
