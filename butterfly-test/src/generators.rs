//! Synthetic data generators

use rand::Rng;

/// Generate synthetic geometry for testing
pub fn generate_test_linestring(points: usize) -> Vec<(f64, f64)> {
    let mut rng = rand::thread_rng();
    (0..points)
        .map(|_| (rng.gen_range(-180.0..180.0), rng.gen_range(-90.0..90.0)))
        .collect()
}
