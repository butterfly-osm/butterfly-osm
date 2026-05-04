//! Shard reader. Loads a `BFGS` v1 file into heap-resident structures
//! and exposes the lookup primitives the executor needs.
//!
//! ## MVP design choice — heap, not mmap
//!
//! The Belgium shard is small (~50-100 MB). A heap-resident reader is
//! simpler, has zero `unsafe` exposure, and keeps the executor free
//! of lifetime constraints. A future ticket can switch to
//! `memmap2`-backed zero-copy reads through butterfly-route's
//! `formats/mmap.rs` wrappers (the only sanctioned `unsafe` carveout
//! in the workspace).

use std::collections::HashMap;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use crc::{CRC_64_XZ, Crc};
use rstar::{AABB, PointDistance, RTree, RTreeObject};

use super::{FOOTER_BYTES, HEADER_BYTES, MAGIC, RECORD_BYTES, VERSION};
use crate::geocoder::cost::ShardStats;
use crate::parser::normalize::normalize;

const CRC_ENGINE: Crc<u64> = Crc::<u64>::new(&CRC_64_XZ);

#[derive(Debug, Clone)]
pub struct ShardRecord {
    pub id: u32,
    pub lat: f64,
    pub lon: f64,
    pub street: Arc<str>,
    pub locality: Arc<str>,
    pub housenumber: Arc<str>,
    pub postcode: Arc<str>,
}

#[derive(Debug, Clone, Copy)]
struct SpatialPoint {
    record_id: u32,
    lat: f64,
    lon: f64,
}

impl RTreeObject for SpatialPoint {
    type Envelope = AABB<[f64; 2]>;
    fn envelope(&self) -> Self::Envelope {
        AABB::from_point([self.lat, self.lon])
    }
}

impl PointDistance for SpatialPoint {
    fn distance_2(&self, point: &[f64; 2]) -> f64 {
        let dx = self.lat - point[0];
        let dy = self.lon - point[1];
        dx * dx + dy * dy
    }
}

#[derive(Debug)]
pub struct Shard {
    records: Vec<ShardRecord>,
    by_postcode: HashMap<String, Vec<u32>>,
    by_locality: HashMap<String, Vec<u32>>,
    by_street: HashMap<String, Vec<u32>>,
    by_pc_street: HashMap<String, Vec<u32>>,
    rtree: RTree<SpatialPoint>,
}

impl Shard {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mut f =
            File::open(path).with_context(|| format!("opening shard at {}", path.display()))?;
        let mut buf = Vec::new();
        f.read_to_end(&mut buf)
            .with_context(|| format!("reading shard at {}", path.display()))?;
        if buf.len() < HEADER_BYTES + FOOTER_BYTES {
            bail!("shard file too short ({} bytes)", buf.len());
        }

        let body_end = buf.len() - FOOTER_BYTES;
        let body = &buf[..body_end];
        let body_crc_bytes: [u8; 8] = buf[body_end..body_end + 8]
            .try_into()
            .map_err(|_| anyhow!("malformed footer body crc"))?;
        let file_crc_bytes: [u8; 8] = buf[body_end + 8..body_end + 16]
            .try_into()
            .map_err(|_| anyhow!("malformed footer file crc"))?;
        let body_crc = u64::from_le_bytes(body_crc_bytes);
        let file_crc = u64::from_le_bytes(file_crc_bytes);
        let computed = CRC_ENGINE.checksum(body);
        if body_crc != computed || file_crc != computed {
            bail!(
                "shard CRC mismatch (body={body_crc:#x}, file={file_crc:#x}, computed={computed:#x})"
            );
        }

        let header = &buf[..HEADER_BYTES];
        let magic = u32::from_le_bytes(header[0..4].try_into().expect("4 bytes"));
        if magic != MAGIC {
            bail!("bad shard magic: {magic:#x} (expected {MAGIC:#x})");
        }
        let version = u16::from_le_bytes(header[4..6].try_into().expect("2 bytes"));
        if version != VERSION {
            bail!("unsupported shard version: {version} (expected {VERSION})");
        }
        let record_count = u32::from_le_bytes(header[8..12].try_into().expect("4 bytes"));
        let strings_off = u64::from_le_bytes(header[16..24].try_into().expect("8 bytes")) as usize;
        let strings_len = u64::from_le_bytes(header[24..32].try_into().expect("8 bytes")) as usize;
        let records_off = u64::from_le_bytes(header[32..40].try_into().expect("8 bytes")) as usize;
        let records_len = u64::from_le_bytes(header[40..48].try_into().expect("8 bytes")) as usize;
        let index_off = u64::from_le_bytes(header[48..56].try_into().expect("8 bytes")) as usize;
        let index_len = u64::from_le_bytes(header[56..64].try_into().expect("8 bytes")) as usize;

        let strings_end = strings_off
            .checked_add(strings_len)
            .ok_or_else(|| anyhow!("strings overflow"))?;
        let records_end = records_off
            .checked_add(records_len)
            .ok_or_else(|| anyhow!("records overflow"))?;
        let index_end = index_off
            .checked_add(index_len)
            .ok_or_else(|| anyhow!("index overflow"))?;
        if strings_end > body.len() || records_end > body.len() || index_end > body.len() {
            bail!("shard section out of bounds");
        }

        let strings = &buf[strings_off..strings_end];
        let records_bytes = &buf[records_off..records_end];
        if records_bytes.len() != record_count as usize * RECORD_BYTES {
            bail!(
                "records length mismatch: {} bytes for {} records",
                records_bytes.len(),
                record_count
            );
        }

        let mut records = Vec::with_capacity(record_count as usize);
        for i in 0..record_count as usize {
            let r = &records_bytes[i * RECORD_BYTES..(i + 1) * RECORD_BYTES];
            let lat_e7 = i32::from_le_bytes(r[0..4].try_into().expect("4 bytes"));
            let lon_e7 = i32::from_le_bytes(r[4..8].try_into().expect("4 bytes"));
            let s_off = u32::from_le_bytes(r[8..12].try_into().expect("4 bytes")) as usize;
            let s_len = u16::from_le_bytes(r[12..14].try_into().expect("2 bytes")) as usize;
            let l_off = u32::from_le_bytes(r[14..18].try_into().expect("4 bytes")) as usize;
            let l_len = u16::from_le_bytes(r[18..20].try_into().expect("2 bytes")) as usize;
            let h_off = u32::from_le_bytes(r[20..24].try_into().expect("4 bytes")) as usize;
            let h_len = u16::from_le_bytes(r[24..26].try_into().expect("2 bytes")) as usize;
            let p_off = u32::from_le_bytes(r[26..30].try_into().expect("4 bytes")) as usize;
            let p_len = u16::from_le_bytes(r[30..32].try_into().expect("2 bytes")) as usize;

            let street = read_string(strings, s_off, s_len)?;
            let locality = read_string(strings, l_off, l_len)?;
            let house = read_string(strings, h_off, h_len)?;
            let postcode = read_string(strings, p_off, p_len)?;

            records.push(ShardRecord {
                id: i as u32,
                lat: lat_e7 as f64 / 1e7,
                lon: lon_e7 as f64 / 1e7,
                street: Arc::from(street),
                locality: Arc::from(locality),
                housenumber: Arc::from(house),
                postcode: Arc::from(postcode),
            });
        }

        let index_bytes = &buf[index_off..index_end];
        let mut cursor = 0usize;
        let by_postcode = parse_index(index_bytes, &mut cursor)?;
        let by_locality = parse_index(index_bytes, &mut cursor)?;
        let by_street = parse_index(index_bytes, &mut cursor)?;
        let by_pc_street = parse_index(index_bytes, &mut cursor)?;

        let points: Vec<SpatialPoint> = records
            .iter()
            .map(|r| SpatialPoint {
                record_id: r.id,
                lat: r.lat,
                lon: r.lon,
            })
            .collect();
        let rtree = RTree::bulk_load(points);

        Ok(Self {
            records,
            by_postcode,
            by_locality,
            by_street,
            by_pc_street,
            rtree,
        })
    }

    #[inline]
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.records.len()
    }

    #[inline]
    #[must_use]
    pub fn record(&self, id: u32) -> Option<&ShardRecord> {
        self.records.get(id as usize)
    }

    #[must_use]
    pub fn postings_for_postcode(&self, pc: &str) -> &[u32] {
        self.by_postcode.get(pc).map_or(&[][..], Vec::as_slice)
    }

    #[must_use]
    pub fn postings_for_locality(&self, locality: &str) -> &[u32] {
        let key = normalize(locality);
        self.by_locality.get(&key).map_or(&[][..], Vec::as_slice)
    }

    #[must_use]
    pub fn postings_for_street(&self, street: &str) -> &[u32] {
        let key = normalize(street);
        self.by_street.get(&key).map_or(&[][..], Vec::as_slice)
    }

    #[must_use]
    pub fn postings_for_postcode_and_street(&self, pc: &str, street: &str) -> &[u32] {
        let key = format!("{}|{}", pc, normalize(street));
        self.by_pc_street.get(&key).map_or(&[][..], Vec::as_slice)
    }

    pub fn all_street_keys(&self) -> impl Iterator<Item = &str> {
        self.by_street.keys().map(String::as_str)
    }

    #[must_use]
    pub fn nearest(&self, lat: f64, lon: f64) -> Option<&ShardRecord> {
        let p = self.rtree.nearest_neighbor(&[lat, lon])?;
        self.records.get(p.record_id as usize)
    }

    #[must_use]
    pub fn nearest_within(
        &self,
        lat: f64,
        lon: f64,
        radius_m: f64,
        limit: usize,
    ) -> Vec<(&ShardRecord, f64)> {
        let deg_lat = radius_m / 111_000.0;
        let deg_lon = radius_m / (111_000.0 * lat.to_radians().cos().max(1e-6));
        let bbox = AABB::from_corners(
            [lat - deg_lat, lon - deg_lon],
            [lat + deg_lat, lon + deg_lon],
        );
        let mut hits: Vec<(&ShardRecord, f64)> = Vec::new();
        for p in self.rtree.locate_in_envelope(&bbox) {
            let d = haversine_m(lat, lon, p.lat, p.lon);
            if d <= radius_m
                && let Some(r) = self.records.get(p.record_id as usize)
            {
                hits.push((r, d));
            }
        }
        hits.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        hits.truncate(limit);
        hits
    }

    #[must_use]
    pub fn stats(&self) -> ShardStats {
        let avg = |idx: &HashMap<String, Vec<u32>>| -> f32 {
            if idx.is_empty() {
                0.0
            } else {
                let total: usize = idx.values().map(Vec::len).sum();
                total as f32 / idx.len() as f32
            }
        };
        ShardStats {
            avg_postcode_postings: avg(&self.by_postcode),
            avg_locality_postings: avg(&self.by_locality),
            avg_street_postings: avg(&self.by_street),
            total_addresses: self.records.len() as u32,
        }
    }
}

#[must_use]
pub fn haversine_m(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const R: f64 = 6_371_000.0;
    let phi1 = lat1.to_radians();
    let phi2 = lat2.to_radians();
    let dphi = (lat2 - lat1).to_radians();
    let dlam = (lon2 - lon1).to_radians();
    let a = (dphi / 2.0).sin().powi(2) + phi1.cos() * phi2.cos() * (dlam / 2.0).sin().powi(2);
    let c = 2.0 * a.sqrt().atan2((1.0 - a).sqrt());
    R * c
}

fn read_string(strings: &[u8], off: usize, len: usize) -> Result<String> {
    if len == 0 {
        return Ok(String::new());
    }
    let end = off
        .checked_add(len)
        .ok_or_else(|| anyhow!("string offset+len overflow"))?;
    if end > strings.len() {
        bail!("string out of bounds: {off}+{len} > {}", strings.len());
    }
    let s = std::str::from_utf8(&strings[off..end])
        .map_err(|e| anyhow!("invalid utf-8 in shard strings: {e}"))?;
    Ok(s.to_string())
}

fn parse_index(buf: &[u8], cursor: &mut usize) -> Result<HashMap<String, Vec<u32>>> {
    if buf.len() < *cursor + 4 {
        bail!("index truncated at count");
    }
    let count = u32::from_le_bytes(buf[*cursor..*cursor + 4].try_into().expect("4 bytes"));
    *cursor += 4;
    let mut out = HashMap::with_capacity(count as usize);
    for _ in 0..count {
        if buf.len() < *cursor + 2 {
            bail!("index truncated at key_len");
        }
        let key_len =
            u16::from_le_bytes(buf[*cursor..*cursor + 2].try_into().expect("2 bytes")) as usize;
        *cursor += 2;
        if buf.len() < *cursor + key_len {
            bail!("index truncated at key");
        }
        let key = std::str::from_utf8(&buf[*cursor..*cursor + key_len])
            .map_err(|e| anyhow!("invalid utf-8 in index key: {e}"))?
            .to_string();
        *cursor += key_len;
        if buf.len() < *cursor + 4 {
            bail!("index truncated at list_len");
        }
        let list_len =
            u32::from_le_bytes(buf[*cursor..*cursor + 4].try_into().expect("4 bytes")) as usize;
        *cursor += 4;
        if buf.len() < *cursor + list_len * 4 {
            bail!("index truncated at list");
        }
        let mut list = Vec::with_capacity(list_len);
        for j in 0..list_len {
            let id = u32::from_le_bytes(
                buf[*cursor + j * 4..*cursor + (j + 1) * 4]
                    .try_into()
                    .expect("4 bytes"),
            );
            list.push(id);
        }
        *cursor += list_len * 4;
        out.insert(key, list);
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn haversine_brussels_to_antwerp_is_about_43km() {
        let d = haversine_m(50.8467, 4.3525, 51.2194, 4.4025);
        assert!((d - 43_000.0).abs() < 5_000.0, "got {d}");
    }
}
