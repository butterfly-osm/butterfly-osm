//! Spatial indexing for geometry and snapping

use rstar::{RTree, RTreeObject, AABB};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use crate::profiles::EdgeId;

/// Point in 2D space with coordinates
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize, bytemuck::Pod, bytemuck::Zeroable)]
#[repr(C)]
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
}

/// Bounding box for spatial objects
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct BBox {
    pub min_x: f64,
    pub min_y: f64,
    pub max_x: f64,
    pub max_y: f64,
}

impl BBox {
    pub fn new(min_x: f64, min_y: f64, max_x: f64, max_y: f64) -> Self {
        Self { min_x, min_y, max_x, max_y }
    }

    pub fn from_points(points: &[Point2D]) -> Self {
        if points.is_empty() {
            return Self::new(0.0, 0.0, 0.0, 0.0);
        }

        let mut min_x = points[0].x;
        let mut min_y = points[0].y;
        let mut max_x = points[0].x;
        let mut max_y = points[0].y;

        for point in points.iter().skip(1) {
            min_x = min_x.min(point.x);
            min_y = min_y.min(point.y);
            max_x = max_x.max(point.x);
            max_y = max_y.max(point.y);
        }

        Self::new(min_x, min_y, max_x, max_y)
    }

    pub fn expand(&self, margin: f64) -> Self {
        Self::new(
            self.min_x - margin,
            self.min_y - margin,
            self.max_x + margin,
            self.max_y + margin,
        )
    }

    pub fn contains_point(&self, point: &Point2D) -> bool {
        point.x >= self.min_x && point.x <= self.max_x && 
        point.y >= self.min_y && point.y <= self.max_y
    }

    pub fn area(&self) -> f64 {
        (self.max_x - self.min_x) * (self.max_y - self.min_y)
    }
}

/// Super-edge with spatial information for R-tree indexing
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct SpatialEdge {
    pub edge_id: EdgeId,
    pub bbox: BBox,
    pub geometry: Vec<Point2D>,
    pub length: f64,
}

impl SpatialEdge {
    pub fn new(edge_id: EdgeId, geometry: Vec<Point2D>) -> Self {
        let bbox = BBox::from_points(&geometry);
        let length = Self::calculate_length(&geometry);
        Self { edge_id, bbox, geometry, length }
    }

    fn calculate_length(geometry: &[Point2D]) -> f64 {
        geometry.windows(2)
            .map(|window| window[0].distance_to(&window[1]))
            .sum()
    }

    /// Get the closest point on this edge to the given point
    pub fn closest_point_on_edge(&self, query_point: &Point2D) -> (Point2D, f64, f64) {
        let mut min_distance = f64::INFINITY;
        let mut closest_point = self.geometry[0];
        let mut position_along_edge = 0.0;
        let mut cumulative_distance = 0.0;

        for segment in self.geometry.windows(2) {
            let (proj_point, dist, t) = Self::point_to_segment_distance(query_point, &segment[0], &segment[1]);
            
            if dist < min_distance {
                min_distance = dist;
                closest_point = proj_point;
                position_along_edge = cumulative_distance + t * segment[0].distance_to(&segment[1]);
            }
            
            cumulative_distance += segment[0].distance_to(&segment[1]);
        }

        (closest_point, min_distance, position_along_edge)
    }

    fn point_to_segment_distance(point: &Point2D, seg_start: &Point2D, seg_end: &Point2D) -> (Point2D, f64, f64) {
        let dx = seg_end.x - seg_start.x;
        let dy = seg_end.y - seg_start.y;
        
        if dx == 0.0 && dy == 0.0 {
            return (*seg_start, point.distance_to(seg_start), 0.0);
        }

        let t = ((point.x - seg_start.x) * dx + (point.y - seg_start.y) * dy) / (dx * dx + dy * dy);
        let t = t.max(0.0).min(1.0);

        let proj_point = Point2D::new(
            seg_start.x + t * dx,
            seg_start.y + t * dy,
        );

        (proj_point, point.distance_to(&proj_point), t)
    }
}

impl RTreeObject for SpatialEdge {
    type Envelope = AABB<[f64; 2]>;

    fn envelope(&self) -> Self::Envelope {
        AABB::from_corners(
            [self.bbox.min_x, self.bbox.min_y],
            [self.bbox.max_x, self.bbox.max_y],
        )
    }
}


/// Spatial index for snap operations
#[derive(Debug)]
pub struct SnapIndex {
    rtree: RTree<SpatialEdge>,
    edge_lookup: HashMap<EdgeId, usize>,
}

impl SnapIndex {
    pub fn new() -> Self {
        Self {
            rtree: RTree::new(),
            edge_lookup: HashMap::new(),
        }
    }

    /// Build R-tree from super-edge bboxes
    pub fn from_super_edges(edges: Vec<SpatialEdge>) -> Self {
        let mut edge_lookup = HashMap::new();
        for (idx, edge) in edges.iter().enumerate() {
            edge_lookup.insert(edge.edge_id, idx);
        }

        let rtree = RTree::bulk_load(edges);
        
        Self { rtree, edge_lookup }
    }

    /// Find nearest edges within radius
    pub fn nearest_edges(&self, point: &Point2D, max_distance: f64) -> Vec<&SpatialEdge> {
        let search_bbox = BBox::new(
            point.x - max_distance,
            point.y - max_distance,
            point.x + max_distance,
            point.y + max_distance,
        );

        self.rtree
            .locate_in_envelope(&AABB::from_corners(
                [search_bbox.min_x, search_bbox.min_y],
                [search_bbox.max_x, search_bbox.max_y],
            ))
            .filter(|edge| {
                let (_, distance, _) = edge.closest_point_on_edge(point);
                distance <= max_distance
            })
            .collect()
    }

    /// Find the single nearest edge
    pub fn nearest_edge(&self, point: &Point2D) -> Option<&SpatialEdge> {
        // Use a more manual approach for finding nearest edge
        let candidates = self.nearest_edges(point, 1000.0); // Search in 1km radius
        
        if candidates.is_empty() {
            return None;
        }

        let mut closest_edge: Option<&SpatialEdge> = None;
        let mut min_distance = f64::INFINITY;

        for edge in candidates {
            let (_, distance, _) = edge.closest_point_on_edge(point);
            if distance < min_distance {
                min_distance = distance;
                closest_edge = Some(edge);
            }
        }

        closest_edge
    }

    /// Get edge by ID
    pub fn get_edge(&self, edge_id: &EdgeId) -> Option<&SpatialEdge> {
        self.edge_lookup.get(edge_id)
            .and_then(|&idx| self.rtree.iter().nth(idx))
    }

    /// Number of indexed edges
    pub fn size(&self) -> usize {
        self.rtree.size()
    }

    /// Save index to binary format
    pub fn save_to_file(&self, path: &str) -> Result<(), Box<dyn std::error::Error>> {
        let data = bincode::serialize(&self.rtree.iter().collect::<Vec<_>>())?;
        std::fs::write(path, data)?;
        Ok(())
    }

    /// Load index from binary format
    pub fn load_from_file(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        let data = std::fs::read(path)?;
        let edges: Vec<SpatialEdge> = bincode::deserialize(&data)?;
        Ok(Self::from_super_edges(edges))
    }
}

/// Snapping result with edge and position information
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SnapResult {
    pub edge_id: EdgeId,
    pub snapped_point: Point2D,
    pub distance: f64,
    pub position_along_edge: f64,
    pub heading_degrees: Option<f64>,
}

impl SnapResult {
    pub fn new(
        edge_id: EdgeId,
        snapped_point: Point2D,
        distance: f64,
        position_along_edge: f64,
        heading_degrees: Option<f64>,
    ) -> Self {
        Self {
            edge_id,
            snapped_point,
            distance,
            position_along_edge,
            heading_degrees,
        }
    }
}

/// Snap engine for universal snapping operations
pub struct SnapEngine {
    index: SnapIndex,
    max_snap_distance: f64,
    heading_tolerance_degrees: f64,
}

impl SnapEngine {
    pub fn new(index: SnapIndex, max_snap_distance: f64, heading_tolerance_degrees: f64) -> Self {
        Self {
            index,
            max_snap_distance,
            heading_tolerance_degrees,
        }
    }

    /// Snap a point to the nearest appropriate edge
    pub fn snap_point(&self, point: &Point2D, query_heading: Option<f64>) -> Option<SnapResult> {
        let candidates = self.index.nearest_edges(point, self.max_snap_distance);
        
        if candidates.is_empty() {
            return None;
        }

        let mut best_candidate: Option<SnapResult> = None;
        let mut best_score = f64::INFINITY;

        for edge in candidates {
            let (snapped_point, distance, position) = edge.closest_point_on_edge(point);
            
            if distance > self.max_snap_distance {
                continue;
            }

            // Calculate edge heading at snap point
            let edge_heading = self.calculate_edge_heading_at_position(edge, position);
            
            // Apply heading filter if provided
            if let (Some(query_h), Some(edge_h)) = (query_heading, edge_heading) {
                let heading_diff = self.normalize_heading_difference(query_h - edge_h);
                if heading_diff > self.heading_tolerance_degrees {
                    continue;
                }
            }

            // Score combines distance and heading alignment
            let heading_penalty = if let (Some(query_h), Some(edge_h)) = (query_heading, edge_heading) {
                let heading_diff = self.normalize_heading_difference(query_h - edge_h);
                heading_diff / self.heading_tolerance_degrees
            } else {
                0.0
            };

            let score = distance + heading_penalty * 10.0; // Heading penalty in meters equivalent
            
            if score < best_score {
                best_score = score;
                best_candidate = Some(SnapResult::new(
                    edge.edge_id,
                    snapped_point,
                    distance,
                    position,
                    edge_heading,
                ));
            }
        }

        best_candidate
    }

    fn calculate_edge_heading_at_position(&self, edge: &SpatialEdge, position: f64) -> Option<f64> {
        if edge.geometry.len() < 2 {
            return None;
        }

        let mut cumulative_distance = 0.0;
        
        for segment in edge.geometry.windows(2) {
            let segment_length = segment[0].distance_to(&segment[1]);
            
            if cumulative_distance + segment_length >= position {
                let dx = segment[1].x - segment[0].x;
                let dy = segment[1].y - segment[0].y;
                let heading = dy.atan2(dx).to_degrees();
                return Some(if heading < 0.0 { heading + 360.0 } else { heading });
            }
            
            cumulative_distance += segment_length;
        }

        // Fallback to last segment
        let last_segment = &edge.geometry[edge.geometry.len()-2..];
        let dx = last_segment[1].x - last_segment[0].x;
        let dy = last_segment[1].y - last_segment[0].y;
        let heading = dy.atan2(dx).to_degrees();
        Some(if heading < 0.0 { heading + 360.0 } else { heading })
    }

    fn normalize_heading_difference(&self, diff: f64) -> f64 {
        let mut normalized = diff.abs();
        if normalized > 180.0 {
            normalized = 360.0 - normalized;
        }
        normalized
    }

    /// Snap multiple points efficiently
    pub fn snap_points(&self, points: &[(Point2D, Option<f64>)]) -> Vec<Option<SnapResult>> {
        points.iter()
            .map(|(point, heading)| self.snap_point(point, *heading))
            .collect()
    }

    /// Get statistics about the spatial index
    pub fn get_stats(&self) -> SnapIndexStats {
        SnapIndexStats {
            total_edges: self.index.size(),
            max_snap_distance: self.max_snap_distance,
            heading_tolerance_degrees: self.heading_tolerance_degrees,
        }
    }
}

/// Statistics about the spatial index
#[derive(Debug, Serialize, Deserialize)]
pub struct SnapIndexStats {
    pub total_edges: usize,
    pub max_snap_distance: f64,
    pub heading_tolerance_degrees: f64,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bbox_from_points() {
        let points = vec![
            Point2D::new(1.0, 2.0),
            Point2D::new(3.0, 4.0),
            Point2D::new(0.0, 5.0),
        ];
        let bbox = BBox::from_points(&points);
        assert_eq!(bbox.min_x, 0.0);
        assert_eq!(bbox.min_y, 2.0);
        assert_eq!(bbox.max_x, 3.0);
        assert_eq!(bbox.max_y, 5.0);
    }

    #[test]
    fn test_spatial_edge_creation() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(1.0, 0.0),
            Point2D::new(1.0, 1.0),
        ];
        let edge = SpatialEdge::new(EdgeId(1), geometry);
        assert_eq!(edge.edge_id, EdgeId(1));
        assert!(edge.length > 1.9 && edge.length < 2.1); // Approximately 2.0
    }

    #[test]
    fn test_closest_point_on_edge() {
        let geometry = vec![
            Point2D::new(0.0, 0.0),
            Point2D::new(2.0, 0.0),
        ];
        let edge = SpatialEdge::new(EdgeId(1), geometry);
        let query_point = Point2D::new(1.0, 1.0);
        
        let (closest, distance, position) = edge.closest_point_on_edge(&query_point);
        assert_eq!(closest.x, 1.0);
        assert_eq!(closest.y, 0.0);
        assert_eq!(distance, 1.0);
        assert_eq!(position, 1.0);
    }

    #[test]
    fn test_snap_index_creation() {
        let edges = vec![
            SpatialEdge::new(EdgeId(1), vec![Point2D::new(0.0, 0.0), Point2D::new(1.0, 0.0)]),
            SpatialEdge::new(EdgeId(2), vec![Point2D::new(0.0, 1.0), Point2D::new(1.0, 1.0)]),
        ];
        let index = SnapIndex::from_super_edges(edges);
        assert_eq!(index.size(), 2);
    }

    #[test]
    fn test_snap_engine_basic() {
        let edges = vec![
            SpatialEdge::new(EdgeId(1), vec![Point2D::new(0.0, 0.0), Point2D::new(2.0, 0.0)]),
        ];
        let index = SnapIndex::from_super_edges(edges);
        let engine = SnapEngine::new(index, 5.0, 35.0);
        
        let query_point = Point2D::new(1.0, 0.5);
        let result = engine.snap_point(&query_point, None).unwrap();
        
        assert_eq!(result.edge_id, EdgeId(1));
        assert_eq!(result.distance, 0.5);
        assert!(result.position_along_edge > 0.9 && result.position_along_edge < 1.1);
    }
}