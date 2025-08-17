//! Geometry processing and 3-pass pipeline for butterfly-osm

pub mod delta;
pub mod resample;
pub mod simplify;
pub mod traits;

pub use traits::{DeltaEncode, ResampleArcLen, SimplifyNav};
