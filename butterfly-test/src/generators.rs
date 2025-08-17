//! Synthetic data generators for problematic regions and edge cases

use rand::Rng;
use std::f64::consts::PI;

/// Generate synthetic geometry for testing
pub fn generate_test_linestring(points: usize) -> Vec<(f64, f64)> {
    let mut rng = rand::thread_rng();
    (0..points)
        .map(|_| (rng.gen_range(-180.0..180.0), rng.gen_range(-90.0..90.0)))
        .collect()
}

/// Generator for problematic geographical regions
pub struct ProblematicRegionGenerator {
    rng: rand::rngs::ThreadRng,
}

impl ProblematicRegionGenerator {
    pub fn new() -> Self {
        Self {
            rng: rand::thread_rng(),
        }
    }
    
    /// Generate shapes near the International Date Line (problematic for longitude wrapping)
    pub fn generate_dateline_crossing(&mut self, points: usize) -> Vec<(f64, f64)> {
        (0..points)
            .map(|_| {
                let lon = if self.rng.gen_bool(0.5) {
                    self.rng.gen_range(175.0..180.0)  // East side
                } else {
                    self.rng.gen_range(-180.0..-175.0) // West side  
                };
                let lat = self.rng.gen_range(-85.0..85.0);
                (lon, lat)
            })
            .collect()
    }
    
    /// Generate shapes near the poles (problematic for mercator projection)
    pub fn generate_polar_regions(&mut self, points: usize) -> Vec<(f64, f64)> {
        (0..points)
            .map(|_| {
                let lat = if self.rng.gen_bool(0.5) {
                    self.rng.gen_range(80.0..90.0)   // North pole
                } else {
                    self.rng.gen_range(-90.0..-80.0) // South pole
                };
                let lon = self.rng.gen_range(-180.0..180.0);
                (lon, lat)
            })
            .collect()
    }
    
    /// Generate degenerate geometries (same points, zero-length segments)
    pub fn generate_degenerate_shapes(&mut self, points: usize) -> Vec<(f64, f64)> {
        let base_point = (
            self.rng.gen_range(-180.0..180.0),
            self.rng.gen_range(-90.0..90.0),
        );
        
        // Generate points very close to each other or identical
        (0..points)
            .map(|_| {
                if self.rng.gen_bool(0.3) {
                    // Identical point
                    base_point
                } else {
                    // Very close point
                    (
                        base_point.0 + self.rng.gen_range(-0.00001..0.00001),
                        base_point.1 + self.rng.gen_range(-0.00001..0.00001),
                    )
                }
            })
            .collect()
    }
    
    /// Generate extremely dense point clusters
    pub fn generate_dense_cluster(&mut self, points: usize) -> Vec<(f64, f64)> {
        let center = (
            self.rng.gen_range(-180.0..180.0),
            self.rng.gen_range(-90.0..90.0),
        );
        let radius = self.rng.gen_range(0.001..0.01); // Very small radius
        
        (0..points)
            .map(|_| {
                let angle = self.rng.gen_range(0.0..2.0 * PI);
                let r = self.rng.gen_range(0.0..radius);
                (
                    center.0 + r * angle.cos(),
                    center.1 + r * angle.sin(),
                )
            })
            .collect()
    }
    
    /// Generate shapes with extreme coordinate precision
    pub fn generate_precision_stress(&mut self, points: usize) -> Vec<(f64, f64)> {
        (0..points)
            .map(|_| {
                // Generate coordinates with up to 15 decimal places
                let lon = self.rng.gen_range(-180.0..180.0);
                let lat = self.rng.gen_range(-90.0..90.0);
                (
                    (lon * 1e15_f64).round() / 1e15_f64,
                    (lat * 1e15_f64).round() / 1e15_f64,
                )
            })
            .collect()
    }
    
    /// Generate coordinates that stress floating-point edge cases
    pub fn generate_floating_point_edge_cases(&mut self) -> Vec<(f64, f64)> {
        vec![
            // Exact zero
            (0.0, 0.0),
            // Very small numbers
            (f64::MIN_POSITIVE, f64::MIN_POSITIVE),
            (-f64::MIN_POSITIVE, -f64::MIN_POSITIVE),
            // Numbers near coordinate limits
            (179.9999999, 89.9999999),
            (-179.9999999, -89.9999999),
            // Infinity and NaN handling
            (180.0, 90.0),   // Exact limits
            (-180.0, -90.0), // Exact limits
        ]
    }
    
    /// Generate large-scale continental shapes
    pub fn generate_continental_scale(&mut self, points: usize) -> Vec<(f64, f64)> {
        // Approximate Europe boundary for stress testing
        let europe_bounds = (-10.0, 30.0, 40.0, 70.0); // west, east, south, north
        
        (0..points)
            .map(|_| {
                (
                    self.rng.gen_range(europe_bounds.0..europe_bounds.1),
                    self.rng.gen_range(europe_bounds.2..europe_bounds.3),
                )
            })
            .collect()
    }
}

impl Default for ProblematicRegionGenerator {
    fn default() -> Self {
        Self::new()
    }
}

/// Generate test data for specific known problematic regions
pub struct KnownProblematicRegions;

impl KnownProblematicRegions {
    /// Generate points around Null Island (0,0) - common edge case
    pub fn null_island_region(points: usize) -> Vec<(f64, f64)> {
        let mut rng = rand::thread_rng();
        (0..points)
            .map(|_| {
                (
                    rng.gen_range(-1.0..1.0),
                    rng.gen_range(-1.0..1.0),
                )
            })
            .collect()
    }
    
    /// Generate points in the Arctic Ocean (high latitude, low precision)
    pub fn arctic_ocean_region(points: usize) -> Vec<(f64, f64)> {
        let mut rng = rand::thread_rng();
        (0..points)
            .map(|_| {
                (
                    rng.gen_range(-180.0..180.0),
                    rng.gen_range(85.0..90.0),
                )
            })
            .collect()
    }
    
    /// Generate points in the Sahara Desert (large sparse region)
    pub fn sahara_desert_region(points: usize) -> Vec<(f64, f64)> {
        let mut rng = rand::thread_rng();
        (0..points)
            .map(|_| {
                (
                    rng.gen_range(-10.0..30.0),  // Roughly Sahara longitude
                    rng.gen_range(15.0..30.0),   // Roughly Sahara latitude
                )
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    
    #[test]
    fn test_basic_linestring_generation() {
        let points = generate_test_linestring(10);
        assert_eq!(points.len(), 10);
        
        // Verify all points are within valid coordinate bounds
        for (lon, lat) in points {
            assert!((-180.0..=180.0).contains(&lon));
            assert!((-90.0..=90.0).contains(&lat));
        }
    }
    
    #[test]
    fn test_problematic_region_generators() {
        let mut gen = ProblematicRegionGenerator::new();
        
        let dateline_points = gen.generate_dateline_crossing(5);
        assert_eq!(dateline_points.len(), 5);
        
        let polar_points = gen.generate_polar_regions(5);
        assert_eq!(polar_points.len(), 5);
        
        let degenerate_points = gen.generate_degenerate_shapes(5);
        assert_eq!(degenerate_points.len(), 5);
        
        let cluster_points = gen.generate_dense_cluster(5);
        assert_eq!(cluster_points.len(), 5);
    }
    
    #[test]
    fn test_known_problematic_regions() {
        let null_island = KnownProblematicRegions::null_island_region(10);
        assert_eq!(null_island.len(), 10);
        
        // Verify points are near (0,0)
        for (lon, lat) in null_island {
            assert!(lon.abs() <= 1.0);
            assert!(lat.abs() <= 1.0);
        }
    }
}
