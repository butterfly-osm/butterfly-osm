//! Core routing algorithms and graph processing for butterfly-osm

pub mod dijkstra;
pub mod graph;
pub mod profiles;

// Re-export main types
pub use profiles::{
    TransportProfile, AccessLevel, HighwayType, WayAccess, AccessTruthTable,
    ProfileMask, MultiProfileMask, EdgeId, MaskingStats, MaskValidationResult,
    ComponentAnalyzer, Component, ComponentId, ComponentType, ComponentStats
};

/// Core routing engine
#[derive(Default)]
pub struct Router {}

impl Router {
    pub fn new() -> Self {
        Self {}
    }
}
