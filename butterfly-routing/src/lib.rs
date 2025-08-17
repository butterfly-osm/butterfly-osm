//! Core routing algorithms and graph processing for butterfly-osm

pub mod dijkstra;
pub mod graph;

/// Core routing engine
#[derive(Default)]
pub struct Router {}

impl Router {
    pub fn new() -> Self {
        Self {}
    }
}
