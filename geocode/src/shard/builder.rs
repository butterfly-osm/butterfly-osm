//! Shard builder. Takes a stream of [`AddressRecord`]s and writes a
//! `BFGS` v1 file.

use std::collections::BTreeMap;
use std::io::{BufWriter, Write};
use std::path::Path;

use crc::{CRC_64_XZ, Crc};

use crate::parser::normalize::normalize;

use super::{AddressRecord, FOOTER_BYTES, HEADER_BYTES, MAGIC, RECORD_BYTES, VERSION};

const CRC_ENGINE: Crc<u64> = Crc::<u64>::new(&CRC_64_XZ);

#[derive(Debug, Clone, Copy)]
pub struct BuildStats {
    pub record_count: u32,
    pub strings_bytes: u64,
    pub records_bytes: u64,
    pub index_bytes: u64,
    pub unique_postcodes: u32,
    pub unique_streets: u32,
}

pub fn build_shard<P: AsRef<Path>>(
    out_path: P,
    addresses: impl IntoIterator<Item = AddressRecord>,
) -> std::io::Result<BuildStats> {
    let mut addrs: Vec<AddressRecord> = addresses.into_iter().collect();
    addrs.sort_by(|a, b| {
        let an = normalize(&a.street);
        let bn = normalize(&b.street);
        a.postcode
            .cmp(&b.postcode)
            .then_with(|| an.cmp(&bn))
            .then_with(|| a.housenumber.cmp(&b.housenumber))
            .then_with(|| {
                a.lat
                    .partial_cmp(&b.lat)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| {
                a.lon
                    .partial_cmp(&b.lon)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
    });

    let mut strings: Vec<u8> = Vec::with_capacity(addrs.len() * 64);
    let mut interned: BTreeMap<String, (u32, u16)> = BTreeMap::new();

    let mut intern = |s: &str, strings: &mut Vec<u8>| -> (u32, u16) {
        if s.is_empty() {
            return (0, 0);
        }
        if let Some(&entry) = interned.get(s) {
            return entry;
        }
        let off = u32::try_from(strings.len()).expect("string table fits in u32");
        let truncated_bytes = if s.len() > u16::MAX as usize {
            &s.as_bytes()[..u16::MAX as usize]
        } else {
            s.as_bytes()
        };
        let len = u16::try_from(truncated_bytes.len()).unwrap_or(u16::MAX);
        strings.extend_from_slice(truncated_bytes);
        let entry = (off, len);
        interned.insert(s.to_string(), entry);
        entry
    };

    let mut records_bytes: Vec<u8> = Vec::with_capacity(addrs.len() * RECORD_BYTES);
    let mut by_postcode: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut by_locality: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut by_street: BTreeMap<String, Vec<u32>> = BTreeMap::new();
    let mut by_pc_street: BTreeMap<String, Vec<u32>> = BTreeMap::new();

    for (idx, a) in addrs.iter().enumerate() {
        let id = u32::try_from(idx).expect("record count fits in u32");

        let lat_e7 = (a.lat * 1e7).round() as i32;
        let lon_e7 = (a.lon * 1e7).round() as i32;

        let (street_off, street_len) = intern(&a.street, &mut strings);
        let (loc_off, loc_len) = intern(&a.locality, &mut strings);
        let (house_off, house_len) = intern(&a.housenumber, &mut strings);
        let (pc_off, pc_len) = intern(&a.postcode, &mut strings);

        records_bytes.extend_from_slice(&lat_e7.to_le_bytes());
        records_bytes.extend_from_slice(&lon_e7.to_le_bytes());
        records_bytes.extend_from_slice(&street_off.to_le_bytes());
        records_bytes.extend_from_slice(&street_len.to_le_bytes());
        records_bytes.extend_from_slice(&loc_off.to_le_bytes());
        records_bytes.extend_from_slice(&loc_len.to_le_bytes());
        records_bytes.extend_from_slice(&house_off.to_le_bytes());
        records_bytes.extend_from_slice(&house_len.to_le_bytes());
        records_bytes.extend_from_slice(&pc_off.to_le_bytes());
        records_bytes.extend_from_slice(&pc_len.to_le_bytes());
        records_bytes.extend_from_slice(&[0u8; 4]);

        if !a.postcode.is_empty() {
            by_postcode.entry(a.postcode.clone()).or_default().push(id);
        }
        let nl = normalize(&a.locality);
        if !nl.is_empty() {
            by_locality.entry(nl).or_default().push(id);
        }
        let ns = normalize(&a.street);
        if !ns.is_empty() {
            by_street.entry(ns.clone()).or_default().push(id);
            if !a.postcode.is_empty() {
                let key = format!("{}|{}", a.postcode, ns);
                by_pc_street.entry(key).or_default().push(id);
            }
        }
    }

    debug_assert_eq!(records_bytes.len(), RECORD_BYTES * addrs.len());

    let index_bytes = serialize_indices(&by_postcode, &by_locality, &by_street, &by_pc_street);

    let strings_off = HEADER_BYTES as u64;
    let strings_len = strings.len() as u64;
    let records_off = strings_off + strings_len;
    let records_len = records_bytes.len() as u64;
    let index_off = records_off + records_len;
    let index_len = index_bytes.len() as u64;

    let f = std::fs::File::create(&out_path)?;
    let mut w = BufWriter::new(f);

    let mut header = [0u8; HEADER_BYTES];
    header[0..4].copy_from_slice(&MAGIC.to_le_bytes());
    header[4..6].copy_from_slice(&VERSION.to_le_bytes());
    let count_u32: u32 = addrs.len().try_into().expect("record count fits in u32");
    header[8..12].copy_from_slice(&count_u32.to_le_bytes());
    header[16..24].copy_from_slice(&strings_off.to_le_bytes());
    header[24..32].copy_from_slice(&strings_len.to_le_bytes());
    header[32..40].copy_from_slice(&records_off.to_le_bytes());
    header[40..48].copy_from_slice(&records_len.to_le_bytes());
    header[48..56].copy_from_slice(&index_off.to_le_bytes());
    header[56..64].copy_from_slice(&index_len.to_le_bytes());

    w.write_all(&header)?;
    w.write_all(&strings)?;
    w.write_all(&records_bytes)?;
    w.write_all(&index_bytes)?;

    let mut body =
        Vec::with_capacity(HEADER_BYTES + strings.len() + records_bytes.len() + index_bytes.len());
    body.extend_from_slice(&header);
    body.extend_from_slice(&strings);
    body.extend_from_slice(&records_bytes);
    body.extend_from_slice(&index_bytes);

    let body_crc = CRC_ENGINE.checksum(&body);
    let file_crc = CRC_ENGINE.checksum(&body);

    w.write_all(&body_crc.to_le_bytes())?;
    w.write_all(&file_crc.to_le_bytes())?;
    let _ = FOOTER_BYTES;

    w.flush()?;

    Ok(BuildStats {
        record_count: addrs.len() as u32,
        strings_bytes: strings.len() as u64,
        records_bytes: records_bytes.len() as u64,
        index_bytes: index_bytes.len() as u64,
        unique_postcodes: by_postcode.len() as u32,
        unique_streets: by_street.len() as u32,
    })
}

fn serialize_indices(
    by_postcode: &BTreeMap<String, Vec<u32>>,
    by_locality: &BTreeMap<String, Vec<u32>>,
    by_street: &BTreeMap<String, Vec<u32>>,
    by_pc_street: &BTreeMap<String, Vec<u32>>,
) -> Vec<u8> {
    let mut buf = Vec::new();
    serialize_index(&mut buf, by_postcode);
    serialize_index(&mut buf, by_locality);
    serialize_index(&mut buf, by_street);
    serialize_index(&mut buf, by_pc_street);
    buf
}

fn serialize_index(buf: &mut Vec<u8>, idx: &BTreeMap<String, Vec<u32>>) {
    let count: u32 = idx.len().try_into().expect("index size fits in u32");
    buf.extend_from_slice(&count.to_le_bytes());
    for (key, list) in idx {
        let key_bytes = key.as_bytes();
        let actual = if key_bytes.len() > u16::MAX as usize {
            &key_bytes[..u16::MAX as usize]
        } else {
            key_bytes
        };
        let key_len: u16 = u16::try_from(actual.len()).unwrap_or(u16::MAX);
        buf.extend_from_slice(&key_len.to_le_bytes());
        buf.extend_from_slice(actual);
        let list_len: u32 = list.len().try_into().expect("posting list fits in u32");
        buf.extend_from_slice(&list_len.to_le_bytes());
        for &id in list {
            buf.extend_from_slice(&id.to_le_bytes());
        }
    }
}

#[cfg(test)]
mod tests {
    use super::super::reader::Shard;
    use super::*;

    fn rec(street: &str, num: &str, pc: &str, loc: &str, lat: f64, lon: f64) -> AddressRecord {
        AddressRecord {
            street: street.to_string(),
            housenumber: num.to_string(),
            postcode: pc.to_string(),
            locality: loc.to_string(),
            lat,
            lon,
        }
    }

    #[test]
    fn round_trip_small_shard() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("shard.bfgs");
        let addrs = vec![
            rec("Rue Wayez", "122", "1070", "Anderlecht", 50.834, 4.314),
            rec("Rue Wayez", "124", "1070", "Anderlecht", 50.834, 4.315),
            rec("Grote Markt", "1", "2000", "Antwerpen", 51.221, 4.401),
        ];
        let stats = build_shard(&path, addrs).unwrap();
        assert_eq!(stats.record_count, 3);
        assert!(stats.unique_postcodes >= 2);
        assert!(stats.unique_streets >= 2);

        let s = Shard::open(&path).unwrap();
        assert_eq!(s.record_count() as u32, 3);
        let pc_hits = s.postings_for_postcode("1070");
        assert_eq!(pc_hits.len(), 2);
    }
}
