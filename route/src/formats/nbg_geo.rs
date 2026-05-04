//! nbg.geo format - Edge geometry and metrics for NBG

use anyhow::Result;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;

use super::crc;

const MAGIC: u32 = 0x4E424747; // "NBGG"
const VERSION: u16 = 1;

#[derive(Debug, Clone)]
pub struct NbgEdge {
    pub u_node: u32,
    pub v_node: u32,
    pub length_mm: u32,
    pub bearing_deci_deg: u16, // 0-3599, 65535 if NA
    pub n_poly_pts: u16,
    pub poly_off: u64,
    pub first_osm_way_id: i64,
    pub flags: u32, // bit0=ferry, bit1=bridge, bit2=tunnel, bit3=roundabout, bit4=ford, bit5=layer_boundary
}

#[derive(Debug, Clone)]
pub struct PolyLine {
    pub lat_fxp: Vec<i32>, // 1e-7 deg
    pub lon_fxp: Vec<i32>,
}

pub struct NbgGeo {
    pub n_edges_und: u64,
    pub edges: Vec<NbgEdge>,
    pub polylines: Vec<PolyLine>,
}

pub struct NbgGeoFile;

impl NbgGeoFile {
    /// Write NBG geo to file
    pub fn write<P: AsRef<Path>>(path: P, geo: &NbgGeo) -> Result<()> {
        let mut writer = BufWriter::new(File::create(path)?);
        let mut crc_digest = crc::Digest::new();

        // Calculate poly_bytes
        let mut poly_bytes = 0u64;
        for poly in &geo.polylines {
            poly_bytes += (poly.lat_fxp.len() * 4 + poly.lon_fxp.len() * 4) as u64;
        }

        // Header (64 bytes)
        let magic_bytes = MAGIC.to_le_bytes();
        let version_bytes = VERSION.to_le_bytes();
        let reserved_bytes = 0u16.to_le_bytes();
        let n_edges_und_bytes = geo.n_edges_und.to_le_bytes();
        let poly_bytes_bytes = poly_bytes.to_le_bytes();
        let padding = [0u8; 40]; // Pad to 64 bytes

        writer.write_all(&magic_bytes)?;
        writer.write_all(&version_bytes)?;
        writer.write_all(&reserved_bytes)?;
        writer.write_all(&n_edges_und_bytes)?;
        writer.write_all(&poly_bytes_bytes)?;
        writer.write_all(&padding)?;

        crc_digest.update(&magic_bytes);
        crc_digest.update(&version_bytes);
        crc_digest.update(&reserved_bytes);
        crc_digest.update(&n_edges_und_bytes);
        crc_digest.update(&poly_bytes_bytes);
        crc_digest.update(&padding);

        // Edge records (36 bytes each)
        for edge in &geo.edges {
            let u_node_bytes = edge.u_node.to_le_bytes();
            let v_node_bytes = edge.v_node.to_le_bytes();
            let length_mm_bytes = edge.length_mm.to_le_bytes();
            let bearing_bytes = edge.bearing_deci_deg.to_le_bytes();
            let n_poly_pts_bytes = edge.n_poly_pts.to_le_bytes();
            let poly_off_bytes = edge.poly_off.to_le_bytes();
            let way_id_bytes = edge.first_osm_way_id.to_le_bytes();
            let flags_bytes = edge.flags.to_le_bytes();

            writer.write_all(&u_node_bytes)?;
            writer.write_all(&v_node_bytes)?;
            writer.write_all(&length_mm_bytes)?;
            writer.write_all(&bearing_bytes)?;
            writer.write_all(&n_poly_pts_bytes)?;
            writer.write_all(&poly_off_bytes)?;
            writer.write_all(&way_id_bytes)?;
            writer.write_all(&flags_bytes)?;

            crc_digest.update(&u_node_bytes);
            crc_digest.update(&v_node_bytes);
            crc_digest.update(&length_mm_bytes);
            crc_digest.update(&bearing_bytes);
            crc_digest.update(&n_poly_pts_bytes);
            crc_digest.update(&poly_off_bytes);
            crc_digest.update(&way_id_bytes);
            crc_digest.update(&flags_bytes);
        }

        // Polyline blob
        for poly in &geo.polylines {
            for &lat in &poly.lat_fxp {
                let bytes = lat.to_le_bytes();
                writer.write_all(&bytes)?;
                crc_digest.update(&bytes);
            }
            for &lon in &poly.lon_fxp {
                let bytes = lon.to_le_bytes();
                writer.write_all(&bytes)?;
                crc_digest.update(&bytes);
            }
        }

        // Footer
        let body_crc = crc_digest.finalize();
        let file_crc = body_crc;
        writer.write_all(&body_crc.to_le_bytes())?;
        writer.write_all(&file_crc.to_le_bytes())?;
        writer.flush()?;

        Ok(())
    }

    /// Read NBG geo from file
    pub fn read<P: AsRef<Path>>(path: P) -> Result<NbgGeo> {
        use std::io::BufReader;
        Self::read_from_reader(BufReader::new(std::fs::File::open(path)?))
    }

    pub fn read_from_bytes(bytes: &[u8]) -> Result<NbgGeo> {
        Self::read_from_reader(std::io::Cursor::new(bytes))
    }

    /// Read just the header + the per-edge metadata array, skipping the
    /// polyline blob. The returned `NbgGeo` has `polylines` left as empty
    /// `PolyLine` placeholders (one per edge, all with empty lat_fxp /
    /// lon_fxp vecs).
    ///
    /// Used by the serve path on containers that carry the flat
    /// `shared/edge_geom_*` sections (#155): the polyline bytes live in
    /// the new sections instead, and this metadata-only reader avoids
    /// materialising the heap `Vec<Vec<i32>>` shape.
    ///
    /// CRC over header + edges + polyline body is still validated — the
    /// polyline-body bytes are streamed through the digest without being
    /// retained in memory.
    pub fn read_edges_only_from_bytes(bytes: &[u8]) -> Result<NbgGeo> {
        Self::read_edges_only_from_reader(std::io::Cursor::new(bytes))
    }

    fn read_edges_only_from_reader<R: std::io::Read>(mut reader: R) -> Result<NbgGeo> {
        let mut crc_digest = crc::Digest::new();

        let mut header = vec![0u8; 64];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let n_edges_und = u64::from_le_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
        ]);

        let mut edges = Vec::with_capacity(n_edges_und as usize);
        for _ in 0..n_edges_und {
            let mut record = [0u8; 36];
            reader.read_exact(&mut record)?;
            crc_digest.update(&record);

            edges.push(NbgEdge {
                u_node: u32::from_le_bytes([record[0], record[1], record[2], record[3]]),
                v_node: u32::from_le_bytes([record[4], record[5], record[6], record[7]]),
                length_mm: u32::from_le_bytes([record[8], record[9], record[10], record[11]]),
                bearing_deci_deg: u16::from_le_bytes([record[12], record[13]]),
                n_poly_pts: u16::from_le_bytes([record[14], record[15]]),
                poly_off: u64::from_le_bytes([
                    record[16], record[17], record[18], record[19], record[20], record[21],
                    record[22], record[23],
                ]),
                first_osm_way_id: i64::from_le_bytes([
                    record[24], record[25], record[26], record[27], record[28], record[29],
                    record[30], record[31],
                ]),
                flags: u32::from_le_bytes([record[32], record[33], record[34], record[35]]),
            });
        }

        // Stream the polyline body through the CRC digest without
        // retaining it. Each edge contributes 8 bytes per polyline
        // vertex (4-byte lat + 4-byte lon).
        let mut buf = [0u8; 4096];
        for edge in &edges {
            let mut remaining = (edge.n_poly_pts as usize) * 8;
            while remaining > 0 {
                let take = remaining.min(buf.len());
                reader.read_exact(&mut buf[..take])?;
                crc_digest.update(&buf[..take]);
                remaining -= take;
            }
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in nbg.geo (edges-only): computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        // Empty PolyLine placeholders — they exist so any legacy reader
        // that asks for `polylines.len()` still gets the right count;
        // attempting to read points returns empty arrays.
        let polylines = (0..edges.len())
            .map(|_| PolyLine {
                lat_fxp: Vec::new(),
                lon_fxp: Vec::new(),
            })
            .collect();

        Ok(NbgGeo {
            n_edges_und,
            edges,
            polylines,
        })
    }

    fn read_from_reader<R: std::io::Read>(mut reader: R) -> Result<NbgGeo> {
        let mut crc_digest = crc::Digest::new();

        let mut header = vec![0u8; 64];
        reader.read_exact(&mut header)?;
        crc_digest.update(&header);

        let n_edges_und = u64::from_le_bytes([
            header[8], header[9], header[10], header[11], header[12], header[13], header[14],
            header[15],
        ]);

        // Read edges (36 bytes each)
        let mut edges = Vec::with_capacity(n_edges_und as usize);
        for _ in 0..n_edges_und {
            let mut record = [0u8; 36];
            reader.read_exact(&mut record)?;
            crc_digest.update(&record);

            edges.push(NbgEdge {
                u_node: u32::from_le_bytes([record[0], record[1], record[2], record[3]]),
                v_node: u32::from_le_bytes([record[4], record[5], record[6], record[7]]),
                length_mm: u32::from_le_bytes([record[8], record[9], record[10], record[11]]),
                bearing_deci_deg: u16::from_le_bytes([record[12], record[13]]),
                n_poly_pts: u16::from_le_bytes([record[14], record[15]]),
                poly_off: u64::from_le_bytes([
                    record[16], record[17], record[18], record[19], record[20], record[21],
                    record[22], record[23],
                ]),
                first_osm_way_id: i64::from_le_bytes([
                    record[24], record[25], record[26], record[27], record[28], record[29],
                    record[30], record[31],
                ]),
                flags: u32::from_le_bytes([record[32], record[33], record[34], record[35]]),
            });
        }

        // Read polylines - stored sequentially: for each edge, all lats then all lons
        let mut polylines = Vec::with_capacity(n_edges_und as usize);
        for edge in &edges {
            let n_pts = edge.n_poly_pts as usize;
            if n_pts == 0 {
                polylines.push(PolyLine {
                    lat_fxp: Vec::new(),
                    lon_fxp: Vec::new(),
                });
                continue;
            }

            // Read lat values
            let mut lat_fxp = Vec::with_capacity(n_pts);
            for _ in 0..n_pts {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                crc_digest.update(&buf);
                lat_fxp.push(i32::from_le_bytes(buf));
            }

            // Read lon values
            let mut lon_fxp = Vec::with_capacity(n_pts);
            for _ in 0..n_pts {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                crc_digest.update(&buf);
                lon_fxp.push(i32::from_le_bytes(buf));
            }

            polylines.push(PolyLine { lat_fxp, lon_fxp });
        }

        // Verify CRC64
        let computed_crc = crc_digest.finalize();
        let mut footer = [0u8; 16];
        reader.read_exact(&mut footer)?;
        let stored_crc = u64::from_le_bytes(footer[0..8].try_into().unwrap());
        anyhow::ensure!(
            computed_crc == stored_crc,
            "CRC64 mismatch in nbg.geo: computed 0x{:016X}, stored 0x{:016X}",
            computed_crc,
            stored_crc
        );

        Ok(NbgGeo {
            n_edges_und,
            edges,
            polylines,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    fn fixture() -> NbgGeo {
        let edges = vec![
            NbgEdge {
                u_node: 1,
                v_node: 2,
                length_mm: 1234,
                bearing_deci_deg: 900,
                n_poly_pts: 3,
                poly_off: 0,
                first_osm_way_id: 42,
                flags: 0,
            },
            NbgEdge {
                u_node: 3,
                v_node: 4,
                length_mm: 5678,
                bearing_deci_deg: 1800,
                n_poly_pts: 0, // empty polyline
                poly_off: 24,
                first_osm_way_id: 43,
                flags: 1,
            },
            NbgEdge {
                u_node: 5,
                v_node: 6,
                length_mm: 9000,
                bearing_deci_deg: 2700,
                n_poly_pts: 5,
                poly_off: 24,
                first_osm_way_id: 44,
                flags: 2,
            },
        ];
        let polylines = vec![
            PolyLine {
                lat_fxp: vec![100, 200, 300],
                lon_fxp: vec![1000, 2000, 3000],
            },
            PolyLine {
                lat_fxp: vec![],
                lon_fxp: vec![],
            },
            PolyLine {
                lat_fxp: vec![400, 500, 600, 700, 800],
                lon_fxp: vec![4000, 5000, 6000, 7000, 8000],
            },
        ];
        NbgGeo {
            n_edges_und: 3,
            edges,
            polylines,
        }
    }

    fn encode_to_bytes(geo: &NbgGeo) -> Vec<u8> {
        let mut buf = Vec::new();
        // Reuse the writer by going through a temp file-like buffer.
        // The simplest way is to write to a file and read it back; but
        // for the unit test we just inline the same encoding logic.
        // Instead, use the public write API to a temp file.
        let temp = tempfile::NamedTempFile::new().unwrap();
        NbgGeoFile::write(temp.path(), geo).unwrap();
        std::io::Read::read_to_end(&mut std::fs::File::open(temp.path()).unwrap(), &mut buf)
            .unwrap();
        buf
    }

    #[test]
    fn edges_only_reader_matches_full_reader_on_edges() {
        let geo = fixture();
        let bytes = encode_to_bytes(&geo);
        let full = NbgGeoFile::read_from_bytes(&bytes).unwrap();
        let lite = NbgGeoFile::read_edges_only_from_bytes(&bytes).unwrap();
        assert_eq!(full.n_edges_und, lite.n_edges_und);
        assert_eq!(full.edges.len(), lite.edges.len());
        for (a, b) in full.edges.iter().zip(lite.edges.iter()) {
            assert_eq!(a.u_node, b.u_node);
            assert_eq!(a.v_node, b.v_node);
            assert_eq!(a.length_mm, b.length_mm);
            assert_eq!(a.bearing_deci_deg, b.bearing_deci_deg);
            assert_eq!(a.n_poly_pts, b.n_poly_pts);
            assert_eq!(a.poly_off, b.poly_off);
            assert_eq!(a.first_osm_way_id, b.first_osm_way_id);
            assert_eq!(a.flags, b.flags);
        }
    }

    #[test]
    fn edges_only_reader_returns_empty_polylines() {
        let geo = fixture();
        let bytes = encode_to_bytes(&geo);
        let lite = NbgGeoFile::read_edges_only_from_bytes(&bytes).unwrap();
        assert_eq!(lite.polylines.len(), geo.edges.len());
        for poly in &lite.polylines {
            assert!(poly.lat_fxp.is_empty());
            assert!(poly.lon_fxp.is_empty());
        }
    }

    #[test]
    fn edges_only_reader_validates_crc() {
        let geo = fixture();
        let mut bytes = encode_to_bytes(&geo);
        // Stomp a polyline byte to invalidate the CRC.
        // Header 64 bytes + 3 edges × 36 bytes = 172. Polyline body
        // starts at offset 172.
        let body_offset = 64 + 3 * 36;
        bytes[body_offset] ^= 0xFF;
        match NbgGeoFile::read_edges_only_from_bytes(&bytes) {
            Ok(_) => panic!("expected CRC validation to fail"),
            Err(e) => assert!(
                e.to_string().contains("CRC64 mismatch"),
                "wrong error: {}",
                e
            ),
        }
    }

    #[test]
    fn edges_only_reader_handles_cursor() {
        let geo = fixture();
        let bytes = encode_to_bytes(&geo);
        let lite = NbgGeoFile::read_edges_only_from_reader(Cursor::new(&bytes)).unwrap();
        assert_eq!(lite.n_edges_und, 3);
        assert_eq!(lite.edges.len(), 3);
    }
}
