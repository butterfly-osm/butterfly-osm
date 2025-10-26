/// PHAST: Partitioned Highway-centric A* Search Technique
///
/// Two-level hierarchical routing system:
/// - L0: Geographic tiles for local routing (A*)
/// - L1: Highway-only network with CH for long-distance queries
pub mod tile;
pub mod builder;
pub mod query;
pub mod highway;

pub use tile::{Tile, TileBounds, TileGrid, TileId};
