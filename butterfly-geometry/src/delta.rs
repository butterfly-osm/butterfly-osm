//! Delta encoding for full fidelity geometry (Pass C)

use crate::resample::Point2D;
use crate::traits::DeltaEncode;
use serde::{Deserialize, Serialize};

/// Delta-encoded point with integer coordinates for space efficiency
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct DeltaPoint {
    pub dx: i32,  // Delta X in millimeters
    pub dy: i32,  // Delta Y in millimeters
}

impl DeltaPoint {
    pub fn new(dx: i32, dy: i32) -> Self {
        Self { dx, dy }
    }

    /// Convert back to absolute Point2D coordinates
    pub fn to_absolute(&self, reference: &Point2D) -> Point2D {
        Point2D::new(
            reference.x + (self.dx as f64 / 1000.0),  // Convert mm to meters
            reference.y + (self.dy as f64 / 1000.0),
        )
    }
}

/// Full fidelity geometry with delta encoding
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FullFidelityGeometry {
    pub reference_point: Point2D,    // First point as absolute reference
    pub delta_points: Vec<DeltaPoint>, // Subsequent points as deltas
    pub total_length: f64,
    pub noise_removed: usize,
    pub compression_ratio: f64,
}

impl FullFidelityGeometry {
    pub fn new(
        reference_point: Point2D,
        delta_points: Vec<DeltaPoint>,
        total_length: f64,
        noise_removed: usize,
        compression_ratio: f64,
    ) -> Self {
        Self {
            reference_point,
            delta_points,
            total_length,
            noise_removed,
            compression_ratio,
        }
    }

    /// Reconstruct the full geometry
    pub fn to_points(&self) -> Vec<Point2D> {
        let mut points = vec![self.reference_point];
        let mut current = self.reference_point;

        for delta in &self.delta_points {
            current = delta.to_absolute(&current);
            points.push(current);
        }

        points
    }

    /// Get approximate memory usage in bytes
    pub fn memory_usage(&self) -> usize {
        std::mem::size_of::<Point2D>() +  // reference point
        self.delta_points.len() * std::mem::size_of::<DeltaPoint>() +
        std::mem::size_of::<f64>() * 2 +  // total_length + compression_ratio
        std::mem::size_of::<usize>()      // noise_removed
    }
}

/// Delta encoder for Pass C
pub struct DeltaEncoder {
    pub noise_threshold: f64,     // 0.2-0.3m noise removal
    pub max_delta_meters: f64,    // Maximum delta before using absolute coordinate
}

impl DeltaEncoder {
    pub fn new(noise_threshold: f64, max_delta_meters: f64) -> Self {
        Self {
            noise_threshold,
            max_delta_meters,
        }
    }

    pub fn default() -> Self {
        Self::new(0.25, 32.0)  // 25cm noise threshold, 32m max delta
    }

    /// Remove noise points that are too close to the line between neighbors
    fn remove_noise(&self, points: &[Point2D]) -> Vec<Point2D> {
        if points.len() <= 2 {
            return points.to_vec();
        }

        let mut filtered = vec![points[0]];  // Always keep first point

        for i in 1..points.len()-1 {
            let current = &points[i];
            let prev = filtered.last().unwrap();
            let next = &points[i + 1];

            // Calculate distance from current point to line between prev and next
            let line_dist = self.point_to_line_distance(current, prev, next);

            // Keep point if it's significant enough
            if line_dist >= self.noise_threshold {
                filtered.push(*current);
            }
        }

        filtered.push(*points.last().unwrap());  // Always keep last point
        filtered
    }

    /// Calculate perpendicular distance from point to line
    fn point_to_line_distance(&self, point: &Point2D, line_start: &Point2D, line_end: &Point2D) -> f64 {
        let line_dx = line_end.x - line_start.x;
        let line_dy = line_end.y - line_start.y;
        let line_length_sq = line_dx * line_dx + line_dy * line_dy;

        if line_length_sq < 1e-10 {
            return point.distance_to(line_start);
        }

        let t = ((point.x - line_start.x) * line_dx + (point.y - line_start.y) * line_dy) / line_length_sq;
        let t = t.max(0.0).min(1.0);  // Clamp to line segment

        let closest_x = line_start.x + t * line_dx;
        let closest_y = line_start.y + t * line_dy;
        let closest = Point2D::new(closest_x, closest_y);

        point.distance_to(&closest)
    }

    /// Convert coordinate difference to millimeter integer delta
    fn coord_to_delta_mm(&self, diff_meters: f64) -> Option<i32> {
        let diff_mm = diff_meters * 1000.0;
        
        if diff_mm.abs() > self.max_delta_meters * 1000.0 {
            None  // Delta too large, need absolute coordinate
        } else {
            Some(diff_mm.round() as i32)
        }
    }

    /// Encode geometry with delta compression
    pub fn encode(&self, geometry: &[Point2D]) -> Result<FullFidelityGeometry, String> {
        if geometry.is_empty() {
            return Err("Cannot encode empty geometry".to_string());
        }

        // Remove noise points
        let original_count = geometry.len();
        let denoised = self.remove_noise(geometry);
        let noise_removed = original_count - denoised.len();

        if denoised.is_empty() {
            return Err("All points removed during noise filtering".to_string());
        }

        // Start with first point as reference
        let reference_point = denoised[0];
        let mut delta_points = Vec::new();
        let mut current = reference_point;

        for &next_point in denoised.iter().skip(1) {
            let dx_m = next_point.x - current.x;
            let dy_m = next_point.y - current.y;

            // Try to encode as delta
            if let (Some(dx_mm), Some(dy_mm)) = (
                self.coord_to_delta_mm(dx_m),
                self.coord_to_delta_mm(dy_m),
            ) {
                delta_points.push(DeltaPoint::new(dx_mm, dy_mm));
                current = next_point;
            } else {
                // Delta too large - insert absolute coordinate as new reference
                // For simplicity, we'll use the current implementation without absolute restarts
                // In a full implementation, this would restart delta encoding from this point
                let dx_mm = (dx_m * 1000.0).round() as i32;
                let dy_mm = (dy_m * 1000.0).round() as i32;
                delta_points.push(DeltaPoint::new(dx_mm, dy_mm));
                current = next_point;
            }
        }

        // Calculate total length
        let total_length = denoised.windows(2)
            .map(|w| w[0].distance_to(&w[1]))
            .sum();

        // Calculate compression ratio
        let original_size = geometry.len() * std::mem::size_of::<Point2D>();
        let compressed_size = std::mem::size_of::<Point2D>() + 
                             delta_points.len() * std::mem::size_of::<DeltaPoint>();
        let compression_ratio = compressed_size as f64 / original_size as f64;

        Ok(FullFidelityGeometry::new(
            reference_point,
            delta_points,
            total_length,
            noise_removed,
            compression_ratio,
        ))
    }
}

impl DeltaEncode for Vec<Point2D> {
    type Point = Point2D;
    type Error = String;

    fn delta_encode(&self, noise_threshold: f64) -> Result<Vec<Self::Point>, Self::Error> {
        let encoder = DeltaEncoder::new(noise_threshold, 32.0);
        let encoded = encoder.encode(self)?;
        Ok(encoded.to_points())
    }
}

/// Extended API for creating full fidelity geometries
impl DeltaEncoder {
    pub fn create_full_fidelity(&self, geometry: &[Point2D]) -> Result<FullFidelityGeometry, String> {
        self.encode(geometry)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_delta_point_conversion() {
        let reference = Point2D::new(100.0, 200.0);
        let delta = DeltaPoint::new(1500, -2000);  // 1.5m east, 2m south
        let absolute = delta.to_absolute(&reference);
        
        assert!((absolute.x - 101.5).abs() < 1e-6);
        assert!((absolute.y - 198.0).abs() < 1e-6);
    }

    #[test]
    fn test_noise_removal() {
        let encoder = DeltaEncoder::new(0.5, 32.0);  // 50cm noise threshold
        
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(10.0, 0.1),  // Small deviation - should be removed
            Point2D::new(20.0, 0.0),
        ];
        
        let denoised = encoder.remove_noise(&geometry);
        assert_eq!(denoised.len(), 2);  // Middle point should be removed
        assert_eq!(denoised[0], geometry[0]);
        assert_eq!(denoised[1], geometry[2]);
    }

    #[test]
    fn test_point_to_line_distance() {
        let encoder = DeltaEncoder::default();
        
        let line_start = Point2D::new(0.0, 0.0);
        let line_end = Point2D::new(10.0, 0.0);
        let point = Point2D::new(5.0, 2.0);  // 2m perpendicular distance
        
        let distance = encoder.point_to_line_distance(&point, &line_start, &line_end);
        assert!((distance - 2.0).abs() < 1e-6);
    }

    #[test]
    fn test_delta_encoding() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(1.0, 0.0),
            Point2D::new(2.0, 1.0),
            Point2D::new(3.0, 1.0),
        ];
        
        let encoder = DeltaEncoder::default();
        let encoded = encoder.encode(&geometry).unwrap();
        
        assert_eq!(encoded.reference_point, geometry[0]);
        assert_eq!(encoded.delta_points.len(), 3);
        
        // Verify reconstruction
        let reconstructed = encoded.to_points();
        assert_eq!(reconstructed.len(), 4);
        
        // Check that points are approximately equal (within millimeter precision)
        for (orig, recon) in geometry.iter().zip(reconstructed.iter()) {
            assert!((orig.x - recon.x).abs() < 0.001);
            assert!((orig.y - recon.y).abs() < 0.001);
        }
    }

    #[test]
    fn test_compression_ratio() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(0.001, 0.001),  // 1mm steps
            Point2D::new(0.002, 0.002),
            Point2D::new(0.003, 0.003),
        ];
        
        let encoder = DeltaEncoder::default();
        let encoded = encoder.encode(&geometry).unwrap();
        
        // Delta encoding should be more space efficient for small movements
        assert!(encoded.compression_ratio < 1.0);
        assert!(encoded.memory_usage() < geometry.len() * std::mem::size_of::<Point2D>());
    }

    #[test]
    fn test_full_fidelity_geometry() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(50.0, 0.0),
            Point2D::new(100.0, 50.0),
        ];
        
        let encoder = DeltaEncoder::default();
        let full_fidelity = encoder.create_full_fidelity(&geometry).unwrap();
        
        assert!(full_fidelity.total_length > 100.0);
        assert_eq!(full_fidelity.delta_points.len(), 2);
        
        let reconstructed = full_fidelity.to_points();
        assert_eq!(reconstructed.len(), 3);
    }
}
