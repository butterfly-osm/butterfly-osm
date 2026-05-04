//! Shard reader. Memory-maps a `BFGS` v2 file and exposes lookups
//! that borrow directly from the mmap. No heap-resident copy of the
//! records section, no `HashMap<String, Vec<u32>>` clone of the
//! inverted indices.
//!
//! ## Memory model
//!
//! On open we:
//! - mmap the file ([`super::mmap::map_readonly`])
//! - validate header magic + version
//! - validate Pattern B CRCs (body_crc over body bytes, file_crc over
//!   header+body bytes; see `route/src/formats/turn_rules.rs`)
//! - parse the four CSR sub-indices into byte-slice views over the mmap
//! - intern unique strings into a small `Arc<str>` pool so per-record
//!   accessors do refcount-only clones
//! - build the in-memory R-tree (still heap; future ticket can pack
//!   it into the mmap)
//!
//! For Belgium (4M records) the heap residency drops from ~1.3 GB
//! (per-record `Arc<str>` allocations + HashMap inverted indices) to
//! under 200 MB (Arc<str> intern pool + R-tree only). The bulk of the
//! shard now lives in file-backed pages (`RssFile`) that the kernel
//! reclaims under memory pressure.

use std::collections::HashMap;
use std::fmt;
use std::path::Path;
use std::sync::Arc;

use anyhow::{Context, Result, anyhow, bail};
use crc::{CRC_64_XZ, Crc};
use memmap2::Mmap;
use rstar::{AABB, PointDistance, RTree, RTreeObject};

use super::mmap::map_readonly;
use super::{FOOTER_BYTES, HEADER_BYTES, MAGIC, RECORD_BYTES, VERSION};
use crate::geocoder::cost::ShardStats;
use crate::parser::normalize::normalize;

const CRC_ENGINE: Crc<u64> = Crc::<u64>::new(&CRC_64_XZ);

/// View over a single record. String fields are `Arc<str>` clones from
/// the shard's interned-string pool — building a `ShardRecord` does no
/// heap allocation (just refcount increments).
///
/// The interned pool is built once at [`Shard::open`] time by walking
/// the records and deduping by `(string_off, string_len)`. For the
/// Belgium 4M-record shard this is ~200 K unique strings (~10 MB
/// heap). Compared to the previous implementation (16 M individual
/// `Arc<str>` allocations) this is the dominant RSS reduction below
/// the mmap-vs-buffered file change.
#[derive(Clone)]
pub struct ShardRecord {
    pub id: u32,
    pub lat: f64,
    pub lon: f64,
    pub street: Arc<str>,
    pub locality: Arc<str>,
    pub housenumber: Arc<str>,
    pub postcode: Arc<str>,
}

impl fmt::Debug for ShardRecord {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ShardRecord")
            .field("id", &self.id)
            .field("lat", &self.lat)
            .field("lon", &self.lon)
            .field("street", &&*self.street)
            .field("locality", &&*self.locality)
            .field("housenumber", &&*self.housenumber)
            .field("postcode", &&*self.postcode)
            .finish()
    }
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

/// Parsed view over one of the 4 CSR sub-indices.
#[derive(Debug, Clone, Copy)]
struct SubIndex {
    keys_offsets_off: usize,
    num_keys: usize,
    keys_data_off: usize,
    keys_data_len: usize,
    postings_offsets_off: usize,
    postings_data_off: usize,
    total_postings: usize,
}

#[derive(Debug)]
pub struct Shard {
    mmap: Arc<Mmap>,
    record_count: usize,
    records_off: usize,
    by_postcode: SubIndex,
    by_locality: SubIndex,
    by_street: SubIndex,
    by_pc_street: SubIndex,
    rtree: RTree<SpatialPoint>,
    interned: HashMap<(u32, u16), Arc<str>>,
    empty_str: Arc<str>,
}

impl Shard {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let path = path.as_ref();
        let mmap = map_readonly(path)
            .with_context(|| format!("mapping shard at {}", path.display()))?;

        let buf: &[u8] = &mmap[..];
        if buf.len() < HEADER_BYTES + FOOTER_BYTES {
            bail!("shard file too short ({} bytes)", buf.len());
        }

        let body_end = buf.len() - FOOTER_BYTES;
        let body = &buf[HEADER_BYTES..body_end];
        let header_bytes = &buf[..HEADER_BYTES];

        // Pattern B CRC: body covers body bytes; file covers header+body.
        let body_crc_bytes: [u8; 8] = buf[body_end..body_end + 8]
            .try_into()
            .map_err(|_| anyhow!("malformed footer body crc"))?;
        let file_crc_bytes: [u8; 8] = buf[body_end + 8..body_end + 16]
            .try_into()
            .map_err(|_| anyhow!("malformed footer file crc"))?;
        let stored_body_crc = u64::from_le_bytes(body_crc_bytes);
        let stored_file_crc = u64::from_le_bytes(file_crc_bytes);

        let mut body_digest = CRC_ENGINE.digest();
        body_digest.update(body);
        let computed_body_crc = body_digest.finalize();

        let mut file_digest = CRC_ENGINE.digest();
        file_digest.update(header_bytes);
        file_digest.update(body);
        let computed_file_crc = file_digest.finalize();

        if stored_body_crc != computed_body_crc || stored_file_crc != computed_file_crc {
            bail!(
                "shard CRC mismatch (body 0x{:016X}/0x{:016X}, file 0x{:016X}/0x{:016X})",
                computed_body_crc,
                stored_body_crc,
                computed_file_crc,
                stored_file_crc
            );
        }

        let magic = u32::from_le_bytes(header_bytes[0..4].try_into().expect("4 bytes"));
        if magic != MAGIC {
            bail!("bad shard magic: {magic:#x} (expected {MAGIC:#x})");
        }
        let version = u16::from_le_bytes(header_bytes[4..6].try_into().expect("2 bytes"));
        if version != VERSION {
            bail!(
                "unsupported shard version: {version} (expected {VERSION}). Rebuild the shard."
            );
        }
        let record_count =
            u32::from_le_bytes(header_bytes[8..12].try_into().expect("4 bytes")) as usize;
        let strings_off =
            u64::from_le_bytes(header_bytes[16..24].try_into().expect("8 bytes")) as usize;
        let strings_len =
            u64::from_le_bytes(header_bytes[24..32].try_into().expect("8 bytes")) as usize;
        let records_off =
            u64::from_le_bytes(header_bytes[32..40].try_into().expect("8 bytes")) as usize;
        let records_len =
            u64::from_le_bytes(header_bytes[40..48].try_into().expect("8 bytes")) as usize;
        let index_off =
            u64::from_le_bytes(header_bytes[48..56].try_into().expect("8 bytes")) as usize;
        let index_len =
            u64::from_le_bytes(header_bytes[56..64].try_into().expect("8 bytes")) as usize;

        let strings_end = strings_off
            .checked_add(strings_len)
            .ok_or_else(|| anyhow!("strings overflow"))?;
        let records_end = records_off
            .checked_add(records_len)
            .ok_or_else(|| anyhow!("records overflow"))?;
        let index_end = index_off
            .checked_add(index_len)
            .ok_or_else(|| anyhow!("index overflow"))?;
        if strings_end > body_end || records_end > body_end || index_end > body_end {
            bail!("shard section out of bounds");
        }

        if records_len != record_count * RECORD_BYTES {
            bail!(
                "records length mismatch: {records_len} bytes for {record_count} records (expected {})",
                record_count * RECORD_BYTES
            );
        }

        if records_off % 4 != 0 {
            bail!("records section not 4-byte aligned: offset {records_off}");
        }
        if index_off % 4 != 0 {
            bail!("index section not 4-byte aligned: offset {index_off}");
        }

        // Parse the 4 sub-indices.
        let mut cursor = index_off;
        let by_postcode = parse_sub_index(buf, &mut cursor, index_end)?;
        let by_locality = parse_sub_index(buf, &mut cursor, index_end)?;
        let by_street = parse_sub_index(buf, &mut cursor, index_end)?;
        let by_pc_street = parse_sub_index(buf, &mut cursor, index_end)?;
        if cursor > index_end {
            bail!(
                "index section overflow: cursor={cursor}, index_end={index_end}"
            );
        }

        // R-tree + intern table: single pass over records.
        let mut points: Vec<SpatialPoint> = Vec::with_capacity(record_count);
        let mut interned: HashMap<(u32, u16), Arc<str>> =
            HashMap::with_capacity(record_count / 8 + 16);
        let strings = &buf[strings_off..strings_end];
        for i in 0..record_count {
            let base = records_off + i * RECORD_BYTES;
            let lat_e7 = i32::from_le_bytes(buf[base..base + 4].try_into().expect("4 bytes"));
            let lon_e7 = i32::from_le_bytes(buf[base + 4..base + 8].try_into().expect("4 bytes"));
            points.push(SpatialPoint {
                record_id: i as u32,
                lat: lat_e7 as f64 / 1e7,
                lon: lon_e7 as f64 / 1e7,
            });

            for (off_byte, len_byte) in [(8usize, 12usize), (14, 18), (20, 24), (26, 30)] {
                let off = u32::from_le_bytes(
                    buf[base + off_byte..base + off_byte + 4]
                        .try_into()
                        .expect("4 bytes"),
                );
                let len = u16::from_le_bytes(
                    buf[base + len_byte..base + len_byte + 2]
                        .try_into()
                        .expect("2 bytes"),
                );
                if len == 0 {
                    continue;
                }
                interned.entry((off, len)).or_insert_with(|| {
                    let s_start = off as usize;
                    let s_end = s_start + len as usize;
                    let bytes = strings.get(s_start..s_end).unwrap_or(&[]);
                    let s = std::str::from_utf8(bytes).unwrap_or("");
                    Arc::from(s)
                });
            }
        }
        let rtree = RTree::bulk_load(points);
        let empty_str: Arc<str> = Arc::from("");

        Ok(Self {
            mmap,
            record_count,
            records_off,
            by_postcode,
            by_locality,
            by_street,
            by_pc_street,
            rtree,
            interned,
            empty_str,
        })
    }

    fn intern(&self, off: u32, len: u16) -> Arc<str> {
        if len == 0 {
            return Arc::clone(&self.empty_str);
        }
        match self.interned.get(&(off, len)) {
            Some(a) => Arc::clone(a),
            None => Arc::clone(&self.empty_str),
        }
    }

    #[inline]
    #[must_use]
    pub fn record_count(&self) -> usize {
        self.record_count
    }

    #[inline]
    fn buf(&self) -> &[u8] {
        &self.mmap[..]
    }

    fn record_at(&self, idx: usize) -> Option<RawRecord> {
        if idx >= self.record_count {
            return None;
        }
        let base = self.records_off + idx * RECORD_BYTES;
        let buf = self.buf();
        let lat_e7 = i32::from_le_bytes(buf[base..base + 4].try_into().ok()?);
        let lon_e7 = i32::from_le_bytes(buf[base + 4..base + 8].try_into().ok()?);
        let s_off = u32::from_le_bytes(buf[base + 8..base + 12].try_into().ok()?);
        let s_len = u16::from_le_bytes(buf[base + 12..base + 14].try_into().ok()?);
        let l_off = u32::from_le_bytes(buf[base + 14..base + 18].try_into().ok()?);
        let l_len = u16::from_le_bytes(buf[base + 18..base + 20].try_into().ok()?);
        let h_off = u32::from_le_bytes(buf[base + 20..base + 24].try_into().ok()?);
        let h_len = u16::from_le_bytes(buf[base + 24..base + 26].try_into().ok()?);
        let p_off = u32::from_le_bytes(buf[base + 26..base + 30].try_into().ok()?);
        let p_len = u16::from_le_bytes(buf[base + 30..base + 32].try_into().ok()?);
        Some(RawRecord {
            lat_e7,
            lon_e7,
            s_off,
            s_len,
            l_off,
            l_len,
            h_off,
            h_len,
            p_off,
            p_len,
        })
    }

    /// View of record `id`. Returns `None` if `id` is out of range.
    /// The returned struct holds [`Arc<str>`] clones from the shard's
    /// interned-string pool — no heap allocation per call.
    #[must_use]
    pub fn record(&self, id: u32) -> Option<ShardRecord> {
        let raw = self.record_at(id as usize)?;
        Some(ShardRecord {
            id,
            lat: raw.lat_e7 as f64 / 1e7,
            lon: raw.lon_e7 as f64 / 1e7,
            street: self.intern(raw.s_off, raw.s_len),
            locality: self.intern(raw.l_off, raw.l_len),
            housenumber: self.intern(raw.h_off, raw.h_len),
            postcode: self.intern(raw.p_off, raw.p_len),
        })
    }

    fn lookup_postings(&self, sub: &SubIndex, key: &[u8]) -> &[u32] {
        let buf = self.buf();
        let keys_offsets = u32_slice(buf, sub.keys_offsets_off, sub.num_keys + 1);
        let keys_data = &buf[sub.keys_data_off..sub.keys_data_off + sub.keys_data_len];
        let mut lo = 0usize;
        let mut hi = sub.num_keys;
        while lo < hi {
            let mid = (lo + hi) / 2;
            let s = keys_offsets[mid] as usize;
            let e = keys_offsets[mid + 1] as usize;
            let k = &keys_data[s..e];
            match k.cmp(key) {
                std::cmp::Ordering::Less => lo = mid + 1,
                std::cmp::Ordering::Greater => hi = mid,
                std::cmp::Ordering::Equal => {
                    let postings_offsets =
                        u32_slice(buf, sub.postings_offsets_off, sub.num_keys + 1);
                    let p_start = postings_offsets[mid] as usize;
                    let p_end = postings_offsets[mid + 1] as usize;
                    return u32_slice_range(
                        buf,
                        sub.postings_data_off,
                        p_start,
                        p_end,
                        sub.total_postings,
                    );
                }
            }
        }
        &[]
    }

    #[must_use]
    pub fn postings_for_postcode(&self, pc: &str) -> &[u32] {
        self.lookup_postings(&self.by_postcode, pc.as_bytes())
    }

    #[must_use]
    pub fn postings_for_locality(&self, locality: &str) -> &[u32] {
        let key = normalize(locality);
        self.lookup_postings(&self.by_locality, key.as_bytes())
    }

    #[must_use]
    pub fn postings_for_street(&self, street: &str) -> &[u32] {
        let key = normalize(street);
        self.lookup_postings(&self.by_street, key.as_bytes())
    }

    #[must_use]
    pub fn postings_for_postcode_and_street(&self, pc: &str, street: &str) -> &[u32] {
        let key = format!("{}|{}", pc, normalize(street));
        self.lookup_postings(&self.by_pc_street, key.as_bytes())
    }

    /// Iterate all street keys (sorted lexicographic). Used by the
    /// fuzzy fallback — bounded by `ExecutionBudget::max_fuzzy_expansions`
    /// at the call site.
    pub fn all_street_keys(&self) -> StreetKeyIter<'_> {
        StreetKeyIter {
            shard: self,
            sub: self.by_street,
            i: 0,
        }
    }

    #[must_use]
    pub fn nearest(&self, lat: f64, lon: f64) -> Option<ShardRecord> {
        let p = self.rtree.nearest_neighbor(&[lat, lon])?;
        self.record(p.record_id)
    }

    #[must_use]
    pub fn nearest_within(
        &self,
        lat: f64,
        lon: f64,
        radius_m: f64,
        limit: usize,
    ) -> Vec<(ShardRecord, f64)> {
        let deg_lat = radius_m / 111_000.0;
        let deg_lon = radius_m / (111_000.0 * lat.to_radians().cos().max(1e-6));
        let bbox = AABB::from_corners(
            [lat - deg_lat, lon - deg_lon],
            [lat + deg_lat, lon + deg_lon],
        );
        let mut hits: Vec<(ShardRecord, f64)> = Vec::new();
        for p in self.rtree.locate_in_envelope(&bbox) {
            let d = haversine_m(lat, lon, p.lat, p.lon);
            if d <= radius_m
                && let Some(r) = self.record(p.record_id)
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
        let avg = |sub: &SubIndex| -> f32 {
            if sub.num_keys == 0 {
                0.0
            } else {
                sub.total_postings as f32 / sub.num_keys as f32
            }
        };
        ShardStats {
            avg_postcode_postings: avg(&self.by_postcode),
            avg_locality_postings: avg(&self.by_locality),
            avg_street_postings: avg(&self.by_street),
            total_addresses: self.record_count as u32,
        }
    }
}

#[derive(Debug, Clone, Copy)]
struct RawRecord {
    lat_e7: i32,
    lon_e7: i32,
    s_off: u32,
    s_len: u16,
    l_off: u32,
    l_len: u16,
    h_off: u32,
    h_len: u16,
    p_off: u32,
    p_len: u16,
}

/// Iterator over street keys. Holds a sub-index view; each `next()`
/// returns a `&str` borrowed from the mmap.
#[derive(Debug)]
pub struct StreetKeyIter<'a> {
    shard: &'a Shard,
    sub: SubIndex,
    i: usize,
}

impl<'a> Iterator for StreetKeyIter<'a> {
    type Item = &'a str;

    fn next(&mut self) -> Option<Self::Item> {
        if self.i >= self.sub.num_keys {
            return None;
        }
        let buf = self.shard.buf();
        let keys_offsets =
            u32_slice(buf, self.sub.keys_offsets_off, self.sub.num_keys + 1);
        let s = keys_offsets[self.i] as usize;
        let e = keys_offsets[self.i + 1] as usize;
        let key_bytes = &buf[self.sub.keys_data_off + s..self.sub.keys_data_off + e];
        self.i += 1;
        std::str::from_utf8(key_bytes).ok()
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

fn parse_sub_index(buf: &[u8], cursor: &mut usize, end: usize) -> Result<SubIndex> {
    if *cursor + 8 > end {
        bail!("sub-index header truncated at {cursor}");
    }
    let num_keys =
        u32::from_le_bytes(buf[*cursor..*cursor + 4].try_into().expect("4 bytes")) as usize;
    let keys_data_len =
        u32::from_le_bytes(buf[*cursor + 4..*cursor + 8].try_into().expect("4 bytes")) as usize;
    *cursor += 8;

    let keys_offsets_off = *cursor;
    if keys_offsets_off % 4 != 0 {
        bail!("keys_offsets not 4-aligned at {keys_offsets_off}");
    }
    let keys_offsets_bytes = (num_keys + 1)
        .checked_mul(4)
        .ok_or_else(|| anyhow!("u32 overflow"))?;
    if *cursor + keys_offsets_bytes > end {
        bail!("keys_offsets truncated");
    }
    *cursor += keys_offsets_bytes;

    let keys_data_off = *cursor;
    if *cursor + keys_data_len > end {
        bail!("keys_data truncated");
    }
    *cursor += keys_data_len;

    while *cursor % 4 != 0 {
        if *cursor >= end {
            bail!("keys_data padding overflows index region");
        }
        *cursor += 1;
    }

    let postings_offsets_off = *cursor;
    if postings_offsets_off % 4 != 0 {
        bail!("postings_offsets not 4-aligned at {postings_offsets_off}");
    }
    let postings_offsets_bytes = (num_keys + 1)
        .checked_mul(4)
        .ok_or_else(|| anyhow!("u32 overflow"))?;
    if *cursor + postings_offsets_bytes > end {
        bail!("postings_offsets truncated");
    }
    *cursor += postings_offsets_bytes;

    let total_postings_idx = postings_offsets_off + num_keys * 4;
    let total_postings = u32::from_le_bytes(
        buf[total_postings_idx..total_postings_idx + 4]
            .try_into()
            .expect("4 bytes"),
    ) as usize;
    let postings_data_bytes = total_postings
        .checked_mul(4)
        .ok_or_else(|| anyhow!("u32 overflow"))?;
    let postings_data_off = *cursor;
    if *cursor + postings_data_bytes > end {
        bail!("postings_data truncated");
    }
    *cursor += postings_data_bytes;

    Ok(SubIndex {
        keys_offsets_off,
        num_keys,
        keys_data_off,
        keys_data_len,
        postings_offsets_off,
        postings_data_off,
        total_postings,
    })
}

/// View `count` little-endian u32s starting at `off` as a `&[u32]`.
///
/// Builder writes u32s in little-endian. On every platform we ship to
/// (x86_64, aarch64) native byte order is little-endian, so a `&[u32]`
/// view over an aligned buffer is correct. The format guarantees 4-byte
/// alignment for these arrays via padding (validated in
/// `parse_sub_index`).
fn u32_slice(buf: &[u8], off: usize, count: usize) -> &[u32] {
    let bytes = &buf[off..off + count * 4];
    bytemuck::cast_slice::<u8, u32>(bytes)
}

fn u32_slice_range(
    buf: &[u8],
    base_off: usize,
    start: usize,
    end: usize,
    cap: usize,
) -> &[u32] {
    debug_assert!(end <= cap, "postings slice [{start}..{end}] exceeds cap {cap}");
    let _ = cap;
    let bytes = &buf[base_off + start * 4..base_off + end * 4];
    bytemuck::cast_slice::<u8, u32>(bytes)
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
