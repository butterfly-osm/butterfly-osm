//! Geometry processing and 3-pass pipeline for butterfly-osm

pub mod delta;
pub mod resample;
pub mod simplify;
pub mod traits;
pub mod pipeline;

pub use traits::{DeltaEncode, ResampleArcLen, SimplifyNav};
pub use resample::{Point2D, SnapSkeleton, ArcLengthResampler, HeadingSample};
pub use delta::{DeltaPoint, FullFidelityGeometry, DeltaEncoder};
pub use simplify::{AnchorPoint, AnchorType, NavigationGeometry, NavigationSimplifier};
pub use pipeline::{PipelineConfig, GeometryPipelineResult, ProcessingStats, GeometryPipeline};
