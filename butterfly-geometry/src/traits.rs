//! Geometry processing trait definitions

/// Arc-length resampling for Pass A (snap skeleton)
pub trait ResampleArcLen {
    type Point;
    type Error;

    /// Resample geometry with arc-length spacing and angle guards
    fn resample_arc_length(
        &self,
        spacing: f64,
        angle_threshold: f64,
    ) -> Result<Vec<Self::Point>, Self::Error>;
}

/// Navigation-grade simplification for Pass B
pub trait SimplifyNav {
    type Point;
    type Error;

    /// Simplify geometry for turn-by-turn navigation
    fn simplify_nav(&self, epsilon: f64) -> Result<Vec<Self::Point>, Self::Error>;
}

/// Delta encoding for Pass C (full fidelity)
pub trait DeltaEncode {
    type Point;
    type Error;

    /// Delta encode geometry with minimal noise removal
    fn delta_encode(&self, noise_threshold: f64) -> Result<Vec<Self::Point>, Self::Error>;
}
