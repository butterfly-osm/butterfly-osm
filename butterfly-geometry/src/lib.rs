//! Geometry processing and 3-pass pipeline for butterfly-osm

pub mod delta;
pub mod pipeline;
pub mod resample;
pub mod simplify;
pub mod traits;

pub use delta::{DeltaEncoder, DeltaPoint, FullFidelityGeometry};
pub use pipeline::{GeometryPipeline, GeometryPipelineResult, PipelineConfig, ProcessingStats};
pub use resample::{ArcLengthResampler, HeadingSample, Point2D, SnapSkeleton};
pub use simplify::{AnchorPoint, AnchorType, NavigationGeometry, NavigationSimplifier};
pub use traits::{DeltaEncode, ResampleArcLen, SimplifyNav};
