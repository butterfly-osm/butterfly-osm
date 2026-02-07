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
    pub bearing_deci_deg: u16,  // 0-3599, 65535 if NA
    pub n_poly_pts: u16,
    pub poly_off: u64,
    pub first_osm_way_id: i64,
    pub flags: u32,  // bit0=ferry, bit1=bridge, bit2=tunnel, bit3=roundabout, bit4=ford, bit5=layer_boundary
}

#[derive(Debug, Clone)]
pub struct PolyLine {
    pub lat_fxp: Vec<i32>,  // 1e-7 deg
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

        // Edge records (40 bytes each)
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
        use std::io::{BufReader, Read};

        let mut reader = BufReader::new(std::fs::File::open(path)?);
        let mut header = vec![0u8; 64];
        reader.read_exact(&mut header)?;

        let n_edges_und = u64::from_le_bytes([
            header[8], header[9], header[10], header[11],
            header[12], header[13], header[14], header[15],
        ]);

        // Read edges (36 bytes each)
        let mut edges = Vec::with_capacity(n_edges_und as usize);
        for _ in 0..n_edges_und {
            let mut record = [0u8; 36];
            reader.read_exact(&mut record)?;

            edges.push(NbgEdge {
                u_node: u32::from_le_bytes([record[0], record[1], record[2], record[3]]),
                v_node: u32::from_le_bytes([record[4], record[5], record[6], record[7]]),
                length_mm: u32::from_le_bytes([record[8], record[9], record[10], record[11]]),
                bearing_deci_deg: u16::from_le_bytes([record[12], record[13]]),
                n_poly_pts: u16::from_le_bytes([record[14], record[15]]),
                poly_off: u64::from_le_bytes([
                    record[16], record[17], record[18], record[19],
                    record[20], record[21], record[22], record[23],
                ]),
                first_osm_way_id: i64::from_le_bytes([
                    record[24], record[25], record[26], record[27],
                    record[28], record[29], record[30], record[31],
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
                lat_fxp.push(i32::from_le_bytes(buf));
            }

            // Read lon values
            let mut lon_fxp = Vec::with_capacity(n_pts);
            for _ in 0..n_pts {
                let mut buf = [0u8; 4];
                reader.read_exact(&mut buf)?;
                lon_fxp.push(i32::from_le_bytes(buf));
            }

            polylines.push(PolyLine { lat_fxp, lon_fxp });
        }

        Ok(NbgGeo {
            n_edges_und,
            edges,
            polylines,
        })
    }
}
