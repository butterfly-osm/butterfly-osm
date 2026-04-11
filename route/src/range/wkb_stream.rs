//! WKB (Well-Known Binary) Streaming for Isochrone Polygons
//!
//! Outputs isochrone polygons in WKB format for efficient streaming
//! and consumption by GIS tools (PostGIS, QGIS, Shapely, GeoPandas, etc.)
//!
//! ## Output Format
//!
//! Each isochrone includes:
//! - origin_id: u32 (the origin node)
//! - threshold_ds: u32 (threshold in deciseconds)
//! - wkb: Binary (WKB-encoded polygon)
//! - n_vertices: u32 (number of vertices in outer ring)
//!
//! ## WKB Polygon Format
//!
//! ```text
//! byte order: 1 byte (little-endian = 1)
//! type: 4 bytes (polygon = 3)
//! num_rings: 4 bytes
//! for each ring:
//!   num_points: 4 bytes
//!   for each point:
//!     x: 8 bytes (f64, longitude)
//!     y: 8 bytes (f64, latitude)
//! ```

use super::contour::ContourResult;
use std::io::Write;

/// Compute 2x signed area of a ring (handles both open and closed rings).
/// Positive = CW, negative = CCW (in standard x-right, y-up coordinates).
fn signed_area_2(ring: &[(f64, f64)]) -> f64 {
    if ring.len() < 3 {
        return 0.0;
    }
    let n = ring.len();
    let mut sum = 0.0;
    for i in 0..n {
        let (x1, y1) = ring[i];
        let (x2, y2) = ring[(i + 1) % n];
        sum += (x2 - x1) * (y2 + y1);
    }
    sum
}

/// Ensure a ring is counter-clockwise (CCW) per RFC 7946 GeoJSON convention.
/// Positive signed area = CW → reverse to CCW.
/// Negative signed area = already CCW → no change.
pub fn ensure_ccw(ring: &mut [(f64, f64)]) {
    if signed_area_2(ring) > 0.0 {
        ring.reverse();
    }
}

/// Ensure a ring is clockwise (CW) — for WKB holes.
pub fn ensure_cw(ring: &mut [(f64, f64)]) {
    if signed_area_2(ring) < 0.0 {
        ring.reverse();
    }
}

/// Encode a polygon as WKB (Well-Known Binary)
///
/// Outer ring is normalized to CCW (RFC 7946), holes to CW.
/// Returns None if the polygon is empty.
pub fn encode_polygon_wkb(contour: &ContourResult) -> Option<Vec<u8>> {
    if contour.outer_ring.is_empty() {
        return None;
    }

    let n_rings = 1 + contour.holes.len();
    let mut outer_ring = contour.outer_ring.clone();

    // Normalize outer ring to CCW
    ensure_ccw(&mut outer_ring);

    // Ensure ring is closed (first point == last point)
    if let (Some(first), Some(last)) = (outer_ring.first(), outer_ring.last()) {
        if first != last {
            outer_ring.push(*first);
        }
    }

    // Calculate buffer size
    let mut total_points = outer_ring.len();
    for hole in &contour.holes {
        total_points += hole.len() + 1; // +1 for closing point
    }

    // WKB header: 1 (byte order) + 4 (type) + 4 (num_rings)
    // Each ring: 4 (num_points) + n_points * 16 (x,y as f64)
    let buf_size = 1 + 4 + 4 + (n_rings * 4) + (total_points * 16);
    let mut buf = Vec::with_capacity(buf_size);

    // Byte order: 1 = little-endian
    buf.push(1u8);

    // Type: 3 = Polygon
    buf.write_all(&3u32.to_le_bytes()).ok()?;

    // Number of rings
    buf.write_all(&(n_rings as u32).to_le_bytes()).ok()?;

    // Write outer ring
    buf.write_all(&(outer_ring.len() as u32).to_le_bytes())
        .ok()?;
    for &(lon, lat) in &outer_ring {
        buf.write_all(&lon.to_le_bytes()).ok()?;
        buf.write_all(&lat.to_le_bytes()).ok()?;
    }

    // Write holes (CW orientation per convention)
    for hole in &contour.holes {
        let mut closed_hole = hole.clone();
        ensure_cw(&mut closed_hole);
        if let (Some(first), Some(last)) = (closed_hole.first(), closed_hole.last()) {
            if first != last {
                closed_hole.push(*first);
            }
        }
        buf.write_all(&(closed_hole.len() as u32).to_le_bytes())
            .ok()?;
        for &(lon, lat) in &closed_hole {
            buf.write_all(&lon.to_le_bytes()).ok()?;
            buf.write_all(&lat.to_le_bytes()).ok()?;
        }
    }

    Some(buf)
}

/// A single isochrone result ready for streaming
#[derive(Debug, Clone)]
pub struct IsochroneRecord {
    /// Origin node ID
    pub origin_id: u32,
    /// Threshold in deciseconds
    pub threshold_ds: u32,
    /// WKB-encoded polygon
    pub wkb: Vec<u8>,
    /// Number of vertices in outer ring
    pub n_vertices: u32,
    /// Elapsed time in microseconds
    pub elapsed_us: u64,
}

impl IsochroneRecord {
    /// Create from a ContourResult
    pub fn from_contour(
        origin_id: u32,
        threshold_ds: u32,
        contour: &ContourResult,
    ) -> Option<Self> {
        let wkb = encode_polygon_wkb(contour)?;
        Some(Self {
            origin_id,
            threshold_ds,
            wkb,
            n_vertices: contour.outer_ring.len() as u32,
            elapsed_us: contour.stats.elapsed_ms * 1000,
        })
    }
}

/// Batch of isochrone records for Arrow output
#[derive(Debug, Default)]
pub struct IsochroneBatch {
    pub origin_ids: Vec<u32>,
    pub threshold_ds: Vec<u32>,
    pub wkb_data: Vec<Vec<u8>>,
    pub n_vertices: Vec<u32>,
    pub elapsed_us: Vec<u64>,
}

impl IsochroneBatch {
    /// Create a new empty batch with given capacity
    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            origin_ids: Vec::with_capacity(capacity),
            threshold_ds: Vec::with_capacity(capacity),
            wkb_data: Vec::with_capacity(capacity),
            n_vertices: Vec::with_capacity(capacity),
            elapsed_us: Vec::with_capacity(capacity),
        }
    }

    /// Add a record to the batch
    pub fn push(&mut self, record: IsochroneRecord) {
        self.origin_ids.push(record.origin_id);
        self.threshold_ds.push(record.threshold_ds);
        self.wkb_data.push(record.wkb);
        self.n_vertices.push(record.n_vertices);
        self.elapsed_us.push(record.elapsed_us);
    }

    /// Number of records in batch
    pub fn len(&self) -> usize {
        self.origin_ids.len()
    }

    /// Check if batch is empty
    pub fn is_empty(&self) -> bool {
        self.origin_ids.is_empty()
    }

    /// Total size of WKB data in bytes
    pub fn wkb_bytes(&self) -> usize {
        self.wkb_data.iter().map(|w| w.len()).sum()
    }
}

/// Convert batch to Arrow RecordBatch
#[cfg(any())] // Arrow feature not enabled - keep code for reference
pub fn batch_to_arrow(
    batch: &IsochroneBatch,
) -> Result<arrow::record_batch::RecordBatch, arrow::error::ArrowError> {
    use arrow::array::{ArrayRef, BinaryArray, UInt32Array, UInt64Array};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    use std::sync::Arc;

    let schema = Arc::new(Schema::new(vec![
        Field::new("origin_id", DataType::UInt32, false),
        Field::new("threshold_ds", DataType::UInt32, false),
        Field::new("wkb", DataType::Binary, false),
        Field::new("n_vertices", DataType::UInt32, false),
        Field::new("elapsed_us", DataType::UInt64, false),
    ]));

    let origin_ids: ArrayRef = Arc::new(UInt32Array::from(batch.origin_ids.clone()));
    let thresholds: ArrayRef = Arc::new(UInt32Array::from(batch.threshold_ds.clone()));
    let wkb: ArrayRef = Arc::new(BinaryArray::from_iter_values(
        batch.wkb_data.iter().map(|v| v.as_slice()),
    ));
    let n_vertices: ArrayRef = Arc::new(UInt32Array::from(batch.n_vertices.clone()));
    let elapsed: ArrayRef = Arc::new(UInt64Array::from(batch.elapsed_us.clone()));

    RecordBatch::try_new(
        schema,
        vec![origin_ids, thresholds, wkb, n_vertices, elapsed],
    )
}

/// Write multiple isochrones to a bytes buffer in newline-delimited JSON (NDJSON) format
/// This is simpler than Arrow and works well for moderate volumes
pub fn write_ndjson(records: &[IsochroneRecord]) -> Vec<u8> {
    let mut buf = Vec::new();
    for record in records {
        // Base64-encode WKB for JSON compatibility
        let wkb_b64 = base64_encode(&record.wkb);
        let line = format!(
            r#"{{"origin_id":{},"threshold_ds":{},"wkb":"{}","n_vertices":{},"elapsed_us":{}}}"#,
            record.origin_id, record.threshold_ds, wkb_b64, record.n_vertices, record.elapsed_us
        );
        buf.extend_from_slice(line.as_bytes());
        buf.push(b'\n');
    }
    buf
}

/// Simple base64 encoding (no padding)
fn base64_encode(data: &[u8]) -> String {
    const ALPHABET: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut result = String::with_capacity(data.len().div_ceil(3) * 4);

    for chunk in data.chunks(3) {
        let b0 = chunk[0] as usize;
        let b1 = chunk.get(1).copied().unwrap_or(0) as usize;
        let b2 = chunk.get(2).copied().unwrap_or(0) as usize;

        result.push(ALPHABET[b0 >> 2] as char);
        result.push(ALPHABET[((b0 & 0x03) << 4) | (b1 >> 4)] as char);

        if chunk.len() > 1 {
            result.push(ALPHABET[((b1 & 0x0F) << 2) | (b2 >> 6)] as char);
        }
        if chunk.len() > 2 {
            result.push(ALPHABET[b2 & 0x3F] as char);
        }
    }

    result
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_wkb_triangle() {
        // Create a simple triangle
        let contour = ContourResult {
            outer_ring: vec![(0.0, 0.0), (1.0, 0.0), (0.5, 1.0)],
            holes: vec![],
            stats: Default::default(),
        };

        let wkb = encode_polygon_wkb(&contour).unwrap();

        // Check header
        assert_eq!(wkb[0], 1); // little-endian
        assert_eq!(u32::from_le_bytes([wkb[1], wkb[2], wkb[3], wkb[4]]), 3); // polygon
        assert_eq!(u32::from_le_bytes([wkb[5], wkb[6], wkb[7], wkb[8]]), 1); // 1 ring

        // 4 points (3 + closing point)
        assert_eq!(u32::from_le_bytes([wkb[9], wkb[10], wkb[11], wkb[12]]), 4);
    }

    #[test]
    fn test_empty_polygon() {
        let contour = ContourResult {
            outer_ring: vec![],
            holes: vec![],
            stats: Default::default(),
        };

        assert!(encode_polygon_wkb(&contour).is_none());
    }

    #[test]
    fn test_base64() {
        assert_eq!(base64_encode(b""), "");
        assert_eq!(base64_encode(b"f"), "Zg");
        assert_eq!(base64_encode(b"fo"), "Zm8");
        assert_eq!(base64_encode(b"foo"), "Zm9v");
    }

    #[test]
    fn test_ensure_ccw() {
        // CW square: (0,0) → (0,1) → (1,1) → (1,0) → goes up, right, down = CW
        let mut cw = vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)];
        assert!(signed_area_2(&cw) > 0.0, "should be CW before ensure_ccw");
        ensure_ccw(&mut cw);
        assert!(signed_area_2(&cw) < 0.0, "should be CCW after ensure_ccw");

        // Already CCW: reversed order
        let mut ccw = vec![(1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        let ccw_original = ccw.clone();
        assert!(signed_area_2(&ccw) < 0.0, "should already be CCW");
        ensure_ccw(&mut ccw);
        assert_eq!(ccw, ccw_original); // unchanged
    }

    #[test]
    fn test_ensure_cw() {
        // CCW ring → ensure_cw should reverse it
        let mut ccw = vec![(1.0, 0.0), (1.0, 1.0), (0.0, 1.0), (0.0, 0.0)];
        assert!(signed_area_2(&ccw) < 0.0, "should be CCW before ensure_cw");
        ensure_cw(&mut ccw);
        assert!(signed_area_2(&ccw) > 0.0, "should be CW after ensure_cw");
    }

    #[test]
    fn test_wkb_determinism() {
        // Same input twice should produce identical WKB bytes
        let contour = ContourResult {
            outer_ring: vec![
                (4.3517, 50.8503),
                (4.4017, 50.8503),
                (4.4017, 50.8803),
                (4.3517, 50.8803),
            ],
            holes: vec![],
            stats: Default::default(),
        };

        let wkb1 = encode_polygon_wkb(&contour).unwrap();
        let wkb2 = encode_polygon_wkb(&contour).unwrap();
        assert_eq!(wkb1, wkb2, "WKB output must be deterministic");
    }

    #[test]
    fn test_wkb_outer_ring_is_ccw() {
        // Provide a CW ring (0,0)→(0,1)→(1,1)→(1,0) is CW, verify WKB output is CCW
        let cw_ring = vec![(0.0, 0.0), (0.0, 1.0), (1.0, 1.0), (1.0, 0.0)];
        assert!(
            signed_area_2(&cw_ring) > 0.0,
            "input ring must be CW for this test"
        );
        let contour = ContourResult {
            outer_ring: cw_ring,
            holes: vec![],
            stats: Default::default(),
        };

        let wkb = encode_polygon_wkb(&contour).unwrap();

        // Parse WKB: skip header (1+4+4=9 bytes), num_points (4 bytes), then read points
        let n_pts = u32::from_le_bytes([wkb[9], wkb[10], wkb[11], wkb[12]]) as usize;
        let mut ring: Vec<(f64, f64)> = Vec::new();
        for i in 0..n_pts {
            let off = 13 + i * 16;
            let x = f64::from_le_bytes(wkb[off..off + 8].try_into().unwrap());
            let y = f64::from_le_bytes(wkb[off + 8..off + 16].try_into().unwrap());
            ring.push((x, y));
        }

        // Verify CCW: signed area should be negative
        let sa = signed_area_2(&ring);
        assert!(sa < 0.0, "Outer ring must be CCW (signed_area_2={sa})");
    }
}
