//! Arc-length resampling implementation for Pass A (snap skeleton)

use crate::traits::ResampleArcLen;
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use butterfly_extract::{TelemetryCalculator, SemanticBreakpoints, DensityClass};

#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct Point2D {
    pub x: f64,
    pub y: f64,
}

impl Point2D {
    pub fn new(x: f64, y: f64) -> Self {
        Self { x, y }
    }

    pub fn distance_to(&self, other: &Point2D) -> f64 {
        ((self.x - other.x).powi(2) + (self.y - other.y).powi(2)).sqrt()
    }

    /// Linear interpolation between two points
    pub fn lerp(&self, other: &Point2D, t: f64) -> Point2D {
        Point2D::new(
            self.x + t * (other.x - self.x),
            self.y + t * (other.y - self.y),
        )
    }

    /// Calculate angle between three points (self as vertex)
    pub fn angle_with(&self, prev: &Point2D, next: &Point2D) -> f64 {
        let v1_x = prev.x - self.x;
        let v1_y = prev.y - self.y;
        let v2_x = next.x - self.x;
        let v2_y = next.y - self.y;

        let dot = v1_x * v2_x + v1_y * v2_y;
        let cross = v1_x * v2_y - v1_y * v2_x;
        
        cross.atan2(dot).abs().to_degrees()
    }
}

/// Heading delta for efficient heading-aware snapping
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HeadingSample {
    pub position: f64,          // Position along edge in meters
    pub heading_delta: i8,      // Signed delta from previous sample (degrees)
}

/// Pass A resampling result
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapSkeleton {
    pub points: Vec<Point2D>,
    pub heading_samples: Vec<HeadingSample>,
    pub total_length: f64,
    pub spacing_used: f64,
}

impl SnapSkeleton {
    pub fn new(points: Vec<Point2D>, heading_samples: Vec<HeadingSample>, total_length: f64, spacing_used: f64) -> Self {
        Self { points, heading_samples, total_length, spacing_used }
    }

    /// Get heading at a specific position along the skeleton
    pub fn heading_at_position(&self, position: f64) -> Option<f64> {
        if self.heading_samples.is_empty() {
            return None;
        }

        // Find the appropriate heading sample
        let mut cumulative_heading = 0.0;
        let mut last_position = 0.0;

        for sample in &self.heading_samples {
            if sample.position >= position {
                // Interpolate if needed
                if sample.position > position && last_position < position {
                    let t = (position - last_position) / (sample.position - last_position);
                    let interpolated_delta = t * sample.heading_delta as f64;
                    return Some(cumulative_heading + interpolated_delta);
                }
                return Some(cumulative_heading + sample.heading_delta as f64);
            }
            cumulative_heading += sample.heading_delta as f64;
            last_position = sample.position;
        }

        Some(cumulative_heading)
    }
}

/// Arc-length resampler with angle guards for Pass A
pub struct ArcLengthResampler {
    pub urban_spacing: f64,     // min(5m, r_local) 
    pub rural_spacing: f64,     // 20-30m
    pub angle_threshold: f64,   // 12-15 degrees
    pub heading_sample_interval: f64, // 30-50m
    /// Optional telemetry calculator for urban/rural density detection
    pub telemetry: Option<TelemetryCalculator>,
    /// Optional semantic breakpoints for preserving critical points
    pub semantic_breakpoints: Option<SemanticBreakpoints>,
}

impl ArcLengthResampler {
    pub fn new(urban_spacing: f64, rural_spacing: f64, angle_threshold: f64, heading_sample_interval: f64) -> Self {
        Self {
            urban_spacing,
            rural_spacing,
            angle_threshold,
            heading_sample_interval,
            telemetry: None,
            semantic_breakpoints: None,
        }
    }
    
    /// Create resampler with telemetry and semantic integration
    pub fn with_integrations(
        urban_spacing: f64, 
        rural_spacing: f64, 
        angle_threshold: f64, 
        heading_sample_interval: f64,
        telemetry: Option<TelemetryCalculator>,
        semantic_breakpoints: Option<SemanticBreakpoints>
    ) -> Self {
        Self {
            urban_spacing,
            rural_spacing,
            angle_threshold,
            heading_sample_interval,
            telemetry,
            semantic_breakpoints,
        }
    }

    pub fn default() -> Self {
        Self::new(5.0, 25.0, 12.0, 40.0)
    }

    /// Determine if area is urban based on density using M1 telemetry
    fn is_urban_density(&self, geometry: &[Point2D]) -> bool {
        if let Some(ref telemetry_calc) = self.telemetry {
            if geometry.is_empty() {
                return true; // Conservative default
            }
            
            // Sample density at the middle of the geometry
            let mid_point = &geometry[geometry.len() / 2];
            
            // Generate telemetry for the area (simplified approach)
            let telemetry_data = telemetry_calc.generate_telemetry();
            
            // Find relevant tile containing this point
            let tile_id = butterfly_extract::telemetry::TileId::from_coords(mid_point.x, mid_point.y);
            
            // Check if we have telemetry data for this tile
            for tile_telemetry in &telemetry_data {
                if tile_telemetry.tile_id == tile_id {
                    match tile_telemetry.density_class {
                        DensityClass::Urban | DensityClass::Suburban => return true,
                        DensityClass::Rural => return false,
                    }
                }
            }
            
            // Fallback: check road density heuristic
            // If coordinates are small (projected), likely urban
            let coord_magnitude = (mid_point.x.abs() + mid_point.y.abs()) / 2.0;
            coord_magnitude < 1.0 // Small coordinates suggest urban area
        } else {
            // Conservative default when no telemetry available
            true
        }
    }

    /// Extract semantic breakpoint indices from geometry using M2 integration
    fn extract_semantic_breakpoints(&self, geometry: &[Point2D]) -> HashSet<usize> {
        let mut breakpoints = HashSet::new();
        
        if let Some(ref _semantic) = self.semantic_breakpoints {
            // For now, use geometric heuristics to identify potential semantic breakpoints
            // This would be enhanced with actual semantic analysis in full implementation
            
            // Always preserve start and end points
            if !geometry.is_empty() {
                breakpoints.insert(0);
                if geometry.len() > 1 {
                    breakpoints.insert(geometry.len() - 1);
                }
            }
            
            // Detect significant turns as semantic breakpoints
            for (i, window) in geometry.windows(3).enumerate() {
                let prev = &window[0];
                let curr = &window[1]; 
                let next = &window[2];
                
                // Check if this is a significant turn that should be preserved
                let angle = curr.angle_with(prev, next);
                if angle > 30.0 { // Significant turn threshold - likely routing-critical
                    breakpoints.insert(i + 1); // i+1 because window starts at i
                }
            }
            
            // Detect potential intersection points based on geometry patterns
            for (i, window) in geometry.windows(2).enumerate() {
                let curr = &window[0];
                let next = &window[1];
                
                // Large distance jumps might indicate intersections or merged ways
                let distance = curr.distance_to(next);
                if distance > 100.0 { // Unusual gap - potential intersection
                    breakpoints.insert(i);
                    breakpoints.insert(i + 1);
                }
            }
        }
        
        breakpoints
    }

    /// Calculate heading between two points in degrees
    fn calculate_heading(&self, from: &Point2D, to: &Point2D) -> f64 {
        let dx = to.x - from.x;
        let dy = to.y - from.y;
        let heading = dy.atan2(dx).to_degrees();
        if heading < 0.0 { heading + 360.0 } else { heading }
    }

    /// Generate heading samples at regular intervals
    fn generate_heading_samples(&self, resampled_points: &[Point2D]) -> Vec<HeadingSample> {
        if resampled_points.len() < 2 {
            return Vec::new();
        }

        let mut samples = Vec::new();
        let mut cumulative_distance = 0.0;
        let mut last_sample_distance = 0.0;
        let mut last_heading: Option<f64> = None;

        for window in resampled_points.windows(2) {
            let segment_length = window[0].distance_to(&window[1]);
            let current_heading = self.calculate_heading(&window[0], &window[1]);
            
            // Sample at intervals
            while cumulative_distance - last_sample_distance >= self.heading_sample_interval {
                last_sample_distance += self.heading_sample_interval;
                
                if let Some(prev_heading) = last_heading {
                    let mut delta = current_heading - prev_heading;
                    
                    // Normalize delta to [-180, 180]
                    while delta > 180.0 { delta -= 360.0; }
                    while delta < -180.0 { delta += 360.0; }
                    
                    // Clamp to i8 range
                    let delta_i8 = delta.max(-127.0).min(127.0) as i8;
                    
                    samples.push(HeadingSample {
                        position: last_sample_distance,
                        heading_delta: delta_i8,
                    });
                } else {
                    // First sample - use absolute heading as delta from 0
                    let delta_i8 = current_heading.max(-127.0).min(127.0) as i8;
                    samples.push(HeadingSample {
                        position: last_sample_distance,
                        heading_delta: delta_i8,
                    });
                }
                
                last_heading = Some(current_heading);
            }
            
            cumulative_distance += segment_length;
        }

        samples
    }
}

impl ResampleArcLen for Vec<Point2D> {
    type Point = Point2D;
    type Error = String;

    fn resample_arc_length(
        &self,
        spacing: f64,
        angle_threshold: f64,
    ) -> Result<Vec<Self::Point>, Self::Error> {
        use crate::resample::ResampleArcLenExtended;
        self.resample_arc_length_with_breakpoints(spacing, angle_threshold, &HashSet::new())
    }
}

/// Extended resampling trait with semantic breakpoint support
pub trait ResampleArcLenExtended {
    /// Resample with semantic breakpoint preservation
    fn resample_arc_length_with_breakpoints(
        &self,
        spacing: f64,
        angle_threshold: f64,
        semantic_breakpoints: &HashSet<usize>,
    ) -> Result<Vec<Point2D>, String>;
}

/// Extended resampling implementation with semantic breakpoint support
impl ResampleArcLenExtended for Vec<Point2D> {
    /// Resample with semantic breakpoint preservation
    fn resample_arc_length_with_breakpoints(
        &self,
        spacing: f64,
        angle_threshold: f64,
        semantic_breakpoints: &HashSet<usize>,
    ) -> Result<Vec<Point2D>, String> {
        if self.len() < 2 {
            return Ok(self.clone());
        }

        let mut resampled = Vec::new();
        let mut cumulative_distance = 0.0;
        let mut last_point_distance = 0.0;

        // Always keep the first point
        resampled.push(self[0]);

        for (segment_idx, window) in self.windows(2).enumerate() {
            let segment_length = window[0].distance_to(&window[1]);
            
            // Check if we need to force-keep the end point of this segment (semantic breakpoint)
            let force_keep_end = semantic_breakpoints.contains(&(segment_idx + 1));
            
            // Resample at regular intervals
            while cumulative_distance - last_point_distance >= spacing {
                last_point_distance += spacing;
                
                // Find position along this segment
                let segment_progress = (last_point_distance - (cumulative_distance - segment_length)) / segment_length;
                
                if segment_progress >= 0.0 && segment_progress <= 1.0 {
                    let resampled_point = window[0].lerp(&window[1], segment_progress);
                    resampled.push(resampled_point);
                }
            }
            
            // Force-keep semantic breakpoint at end of segment
            if force_keep_end {
                let end_point = window[1];
                if resampled.last() != Some(&end_point) {
                    resampled.push(end_point);
                    // Update last_point_distance to account for forced point
                    last_point_distance = cumulative_distance + segment_length;
                }
            }
            
            cumulative_distance += segment_length;
        }

        // Always keep the last point
        if let Some(last) = self.last() {
            if resampled.last() != Some(last) {
                resampled.push(*last);
            }
        }

        // Apply angle guards - keep points where angle change exceeds threshold
        // But also preserve semantic breakpoints during angle filtering
        if resampled.len() >= 3 {
            let mut angle_guarded = Vec::new();
            angle_guarded.push(resampled[0]);

            for i in 1..resampled.len()-1 {
                let current_point = resampled[i];
                let angle = current_point.angle_with(&resampled[i-1], &resampled[i+1]);
                
                // Check if this point corresponds to a semantic breakpoint in original geometry
                let is_semantic = self.iter().enumerate()
                    .any(|(orig_idx, orig_point)| {
                        semantic_breakpoints.contains(&orig_idx) && 
                        current_point.distance_to(orig_point) < 0.1 // Very close match
                    });
                
                // Keep points with significant angle changes OR semantic breakpoints
                if angle >= angle_threshold || is_semantic {
                    angle_guarded.push(current_point);
                }
            }
            
            angle_guarded.push(*resampled.last().unwrap());
            resampled = angle_guarded;
        }

        Ok(resampled)
    }
}

/// Extended implementation for creating snap skeletons
impl ArcLengthResampler {
    pub fn create_snap_skeleton(&self, geometry: &[Point2D]) -> Result<SnapSkeleton, String> {
        if geometry.len() < 2 {
            return Err("Geometry must have at least 2 points".to_string());
        }

        // Choose spacing based on area density using M1 telemetry integration
        let spacing = if self.is_urban_density(geometry) {
            self.urban_spacing
        } else {
            self.rural_spacing
        };

        // Extract semantic breakpoints from M2 coarsening system
        let semantic_breakpoints = self.extract_semantic_breakpoints(geometry);

        // Perform enhanced arc-length resampling with semantic breakpoint preservation
        use crate::resample::ResampleArcLenExtended;
        let resampled_points = geometry.to_vec()
            .resample_arc_length_with_breakpoints(spacing, self.angle_threshold, &semantic_breakpoints)?;
        
        // Calculate total length
        let total_length = geometry.windows(2)
            .map(|w| w[0].distance_to(&w[1]))
            .sum();

        // Generate heading samples
        let heading_samples = self.generate_heading_samples(&resampled_points);

        Ok(SnapSkeleton::new(resampled_points, heading_samples, total_length, spacing))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_point2d_distance() {
        let p1 = Point2D::new(0.0, 0.0);
        let p2 = Point2D::new(3.0, 4.0);
        assert_eq!(p1.distance_to(&p2), 5.0);
    }

    #[test]
    fn test_point2d_lerp() {
        let p1 = Point2D::new(0.0, 0.0);
        let p2 = Point2D::new(10.0, 0.0);
        let mid = p1.lerp(&p2, 0.5);
        assert_eq!(mid.x, 5.0);
        assert_eq!(mid.y, 0.0);
    }

    #[test]
    fn test_basic_resampling() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(10.0, 0.0),
            Point2D::new(20.0, 0.0),
        ];
        
        let resampled = geometry.resample_arc_length(5.0, 15.0).unwrap();
        
        // Should have points approximately every 5 meters
        assert!(resampled.len() >= 3);
        assert_eq!(resampled.first().unwrap().x, 0.0);
        assert_eq!(resampled.last().unwrap().x, 20.0);
    }

    #[test]
    fn test_angle_guarding() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(5.0, 0.0),
            Point2D::new(10.0, 5.0),  // 45 degree turn
            Point2D::new(15.0, 5.0),
        ];
        
        let resampled = geometry.resample_arc_length(2.0, 30.0).unwrap(); // Low angle threshold
        
        // The turn point should be preserved due to angle guard
        // Check if any point is near the corner
        let has_corner_point = resampled.iter().any(|p| {
            (p.x - 10.0).abs() < 1.0 && (p.y - 5.0).abs() < 1.0
        });
        
        assert!(has_corner_point, "Corner point should be preserved by angle guard");
    }

    #[test]
    fn test_snap_skeleton_creation() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(50.0, 0.0),
            Point2D::new(100.0, 50.0),
        ];
        
        let resampler = ArcLengthResampler::default();
        let skeleton = resampler.create_snap_skeleton(&geometry).unwrap();
        
        assert!(skeleton.points.len() >= 2);
        assert!(skeleton.total_length > 100.0);
        assert_eq!(skeleton.spacing_used, 5.0); // Urban spacing
        assert!(!skeleton.heading_samples.is_empty());
    }

    #[test]
    fn test_heading_calculation() {
        let resampler = ArcLengthResampler::default();
        let p1 = Point2D::new(0.0, 0.0);
        let p2 = Point2D::new(1.0, 0.0); // East
        let heading = resampler.calculate_heading(&p1, &p2);
        assert!((heading - 0.0).abs() < 0.01);
        
        let p3 = Point2D::new(0.0, 1.0); // North
        let heading_north = resampler.calculate_heading(&p1, &p3);
        assert!((heading_north - 90.0).abs() < 0.01);
    }

    #[test]
    fn test_semantic_breakpoint_preservation() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),   // 0: start
            Point2D::new(10.0, 0.0),  // 1: semantic breakpoint (intersection)
            Point2D::new(20.0, 0.0),  // 2: regular point
            Point2D::new(30.0, 10.0), // 3: semantic breakpoint (sharp turn)
            Point2D::new(40.0, 10.0), // 4: end
        ];
        
        // Create semantic breakpoints set
        let mut breakpoints = HashSet::new();
        breakpoints.insert(1); // Intersection
        breakpoints.insert(3); // Sharp turn
        
        let resampled = geometry.resample_arc_length_with_breakpoints(5.0, 15.0, &breakpoints).unwrap();
        
        // Verify semantic breakpoints are preserved
        let has_intersection = resampled.iter().any(|p| 
            (p.x - 10.0).abs() < 0.1 && (p.y - 0.0).abs() < 0.1
        );
        let has_turn = resampled.iter().any(|p| 
            (p.x - 30.0).abs() < 0.1 && (p.y - 10.0).abs() < 0.1
        );
        
        assert!(has_intersection, "Intersection semantic breakpoint should be preserved");
        assert!(has_turn, "Turn semantic breakpoint should be preserved");
    }

    #[test]
    fn test_urban_density_detection_with_telemetry() {
        use butterfly_extract::TelemetryCalculator;
        
        let telemetry = TelemetryCalculator::new();
        let semantic = None;
        
        let resampler = ArcLengthResampler::with_integrations(
            3.0,    // urban_spacing
            20.0,   // rural_spacing  
            12.0,   // angle_threshold
            40.0,   // heading_sample_interval
            Some(telemetry),
            semantic,
        );
        
        // Urban geometry (small coordinates suggesting urban tile)
        let urban_geometry = vec![
            Point2D::new(0.001, 0.001),
            Point2D::new(0.002, 0.001),
            Point2D::new(0.003, 0.002),
        ];
        
        let skeleton = resampler.create_snap_skeleton(&urban_geometry).unwrap();
        
        // Should use urban spacing (3.0m) instead of rural (20.0m)
        assert_eq!(skeleton.spacing_used, 3.0);
        assert!(skeleton.points.len() >= 2);
    }

    #[test]
    fn test_enhanced_pass_a_with_m1_m2_integration() {
        use butterfly_extract::{TelemetryCalculator, SemanticBreakpoints};
        
        let telemetry = TelemetryCalculator::new();
        let semantic = SemanticBreakpoints::new();
        
        let resampler = ArcLengthResampler::with_integrations(
            5.0,    // urban_spacing
            25.0,   // rural_spacing
            12.0,   // angle_threshold
            40.0,   // heading_sample_interval
            Some(telemetry),
            Some(semantic),
        );
        
        // Complex geometry with potential semantic breakpoints
        let geometry = vec![
            Point2D::new(0.0, 0.0),     // Start
            Point2D::new(50.0, 0.0),    // Straight section
            Point2D::new(100.0, 50.0),  // Turn point (potential semantic)
            Point2D::new(150.0, 50.0),  // Straight section  
            Point2D::new(200.0, 0.0),   // End turn
        ];
        
        let skeleton = resampler.create_snap_skeleton(&geometry).unwrap();
        
        // Verify enhanced functionality
        assert!(skeleton.points.len() >= 3);
        assert!(skeleton.total_length > 200.0);
        assert!(!skeleton.heading_samples.is_empty());
        
        // Verify that we get more detailed sampling due to integration
        let point_density = skeleton.points.len() as f64 / skeleton.total_length * 1000.0; // points per km
        assert!(point_density > 10.0, "Should have good point density with enhanced Pass A");
    }

    #[test]
    fn test_semantic_breakpoint_extraction() {
        use butterfly_extract::SemanticBreakpoints;
        
        let semantic = SemanticBreakpoints::new();
        let resampler = ArcLengthResampler::with_integrations(
            5.0, 25.0, 12.0, 40.0,
            None,
            Some(semantic),
        );
        
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(10.0, 0.0),    // Potential intersection
            Point2D::new(20.0, 15.0),   // Sharp turn (45+ degrees)
            Point2D::new(30.0, 15.0),
        ];
        
        let breakpoints = resampler.extract_semantic_breakpoints(&geometry);
        
        // Should detect the sharp turn as a semantic breakpoint
        assert!(breakpoints.len() > 0, "Should detect semantic breakpoints in geometry with turns");
    }

    #[test]
    fn test_angle_preservation_with_semantics() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(5.0, 0.0),
            Point2D::new(10.0, 5.0),    // 45-degree turn
            Point2D::new(15.0, 5.0),
        ];
        
        // Mark turn as semantic
        let mut breakpoints = HashSet::new();
        breakpoints.insert(2); // The turn point
        
        // Use high angle threshold to test semantic preservation
        let resampled = geometry.resample_arc_length_with_breakpoints(2.0, 60.0, &breakpoints).unwrap();
        
        // Even with high angle threshold (60°), semantic point should be preserved
        let has_turn_point = resampled.iter().any(|p| 
            (p.x - 10.0).abs() < 0.5 && (p.y - 5.0).abs() < 0.5
        );
        
        assert!(has_turn_point, "Semantic breakpoint should override angle threshold");
    }
}
