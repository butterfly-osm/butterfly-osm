//! BOSA BeSt authoritative-source ingestion end-to-end tests.
//!
//! These cover the round trip from a real-shaped BOSA CSV row → shard
//! build → mmap reader → query → response. They DO NOT hit the
//! network — the test corpus is a 4-row real-shape sample lifted from
//! the live BOSA Brussels download (`openaddress-bebru.zip`,
//! verified 2026-05-04). Larger smoke tests live in
//! `belgium_e2e.rs` behind the `#[ignore]` gate that requires the
//! 6.7M-record real download.

use std::path::PathBuf;

use butterfly_geocode::CountryId;
use butterfly_geocode::shard::SourceTag;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use butterfly_geocode::sources::bosa::BosaCsvSource;
use butterfly_geocode::sources::{Source, collect_all};

/// Real-shape BOSA Brussels CSV: header + 4 rows. The header matches
/// the 2026-05-04 published format (20 columns); rows 1-3 are
/// `current`, row 4 is `retired` and must be filtered.
const REAL_SHAPE_CSV: &str = "EPSG:31370_x,EPSG:31370_y,EPSG:4326_lat,EPSG:4326_lon,address_id,box_number,house_number,municipality_id,municipality_name_de,municipality_name_fr,municipality_name_nl,postcode,postname_fr,postname_nl,street_id,streetname_de,streetname_fr,streetname_nl,region_code,status\n\
146321.07800,169504.60900,50.83595,4.31653,615867,RDC,475,21001,,Anderlecht,Anderlecht,1070,,,4568,,Chaussée de Mons,Bergense Steenweg,BE-BRU,current\n\
146059.99800,169206.03000,50.83327,4.31283,615868,2eET,603,21001,,Anderlecht,Anderlecht,1070,,,4568,,Chaussée de Mons,Bergense Steenweg,BE-BRU,current\n\
148000.00000,170000.00000,50.84671,4.35251,99999,,1,21004,,Bruxelles,Brussel,1000,,,1,,Grand-Place,Grote Markt,BE-BRU,current\n\
148000.00000,170001.00000,50.84671,4.35252,77777,,2,21004,,Bruxelles,Brussel,1000,,,1,,Grand-Place,Grote Markt,BE-BRU,retired\n";

fn write_sample(dir: &std::path::Path, csv: &str) -> PathBuf {
    let p = dir.join("bosa-sample.csv");
    std::fs::write(&p, csv).unwrap();
    p
}

#[test]
fn bosa_csv_to_shard_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let csv = write_sample(dir.path(), REAL_SHAPE_CSV);
    let shard_path = dir.path().join("bosa.bfgs");

    let loader = BosaCsvSource::new(&csv, CountryId::BE);
    let recs = collect_all(&loader, |_| {}).unwrap();
    assert!(!recs.is_empty(), "BOSA loader produced no records");
    // 3 active rows × {NL, FR} (DE empty in Brussels) = 6 records.
    assert_eq!(recs.len(), 6, "expected 6 records, got: {:#?}", recs);
    for r in &recs {
        assert_eq!(r.source, SourceTag::Bosa);
        assert!(r.source_id.is_some(), "address_id should propagate");
    }

    build_shard(&shard_path, CountryId::BE, recs).expect("build shard");

    // Reload the shard and verify the source byte survives.
    let shard = Shard::open(&shard_path).expect("open shard");
    assert_eq!(shard.country(), CountryId::BE);
    assert_eq!(shard.record_count(), 6);
    for i in 0..shard.record_count() as u32 {
        let r = shard.record(i).expect("record present");
        assert_eq!(
            r.source,
            SourceTag::Bosa,
            "source byte lost in round trip on record {i}: {r:#?}"
        );
    }
}

#[test]
fn bosa_query_round_trip_finds_real_address() {
    let dir = tempfile::tempdir().unwrap();
    let csv = write_sample(dir.path(), REAL_SHAPE_CSV);
    let shard_path = dir.path().join("bosa.bfgs");

    let loader = BosaCsvSource::new(&csv, CountryId::BE);
    let recs = collect_all(&loader, |_| {}).unwrap();
    build_shard(&shard_path, CountryId::BE, recs).unwrap();

    let shard = Shard::open(&shard_path).unwrap();

    // The 1070 postcode should have the 4 NL+FR records for the two
    // active Anderlecht rows.
    let p1070 = shard.postings_for_postcode("1070");
    assert_eq!(
        p1070.len(),
        4,
        "expected 4 records for postcode 1070 (2 rows × 2 langs), got {}",
        p1070.len()
    );

    // Reverse-geocode at the BOSA-published coordinate must hit
    // Bergense Steenweg / Chaussée de Mons.
    let nearest = shard.nearest(50.83595, 4.31653).expect("record nearby");
    let street = nearest.street.to_lowercase();
    assert!(
        street.contains("bergense") || street.contains("mons"),
        "expected Bergense/Mons street, got {}",
        nearest.street
    );
    assert_eq!(nearest.source, SourceTag::Bosa);
}

#[test]
fn bosa_dedup_via_merge() {
    use butterfly_geocode::shard::AddressRecord;
    use butterfly_geocode::sources::merge_records;

    // Two records at the same address: BOSA + OSM. Merge dedups the
    // OSM record because BOSA outranks it.
    let bosa = vec![AddressRecord {
        lat: 50.83595,
        lon: 4.31653,
        street: "Chaussée de Mons".to_string(),
        locality: "Anderlecht".to_string(),
        housenumber: "475".to_string(),
        postcode: "1070".to_string(),
        source: SourceTag::Bosa,
        source_id: Some("615867".to_string()),
    }];
    let osm = vec![AddressRecord {
        lat: 50.83594,
        lon: 4.31654,
        street: "chaussée de mons".to_string(),
        locality: "Anderlecht".to_string(),
        housenumber: "475".to_string(),
        postcode: "1070".to_string(),
        source: SourceTag::Osm,
        source_id: None,
    }];
    let merged = merge_records(vec![bosa, osm]);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].source, SourceTag::Bosa);
}

#[test]
fn build_shard_via_merge_preserves_per_record_source() {
    use butterfly_geocode::shard::AddressRecord;
    use butterfly_geocode::sources::merge_records;

    // Build two single-source shards, then materialise their records
    // and merge — same code path the CLI's `--merge` mode uses.
    let bosa_records = vec![AddressRecord {
        lat: 50.83595,
        lon: 4.31653,
        street: "Chaussée de Mons".to_string(),
        locality: "Anderlecht".to_string(),
        housenumber: "475".to_string(),
        postcode: "1070".to_string(),
        source: SourceTag::Bosa,
        source_id: None,
    }];
    let osm_records = vec![AddressRecord {
        lat: 51.221,
        lon: 4.401,
        street: "Grote Markt".to_string(),
        locality: "Antwerpen".to_string(),
        housenumber: "1".to_string(),
        postcode: "2000".to_string(),
        source: SourceTag::Osm,
        source_id: None,
    }];

    let merged = merge_records(vec![bosa_records, osm_records]);
    assert_eq!(merged.len(), 2, "two unique addresses survive");

    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("merged.bfgs");
    build_shard(&p, CountryId::BE, merged).unwrap();
    let shard = Shard::open(&p).unwrap();

    // Confirm BOTH source bytes survived the round trip.
    let mut tags: Vec<_> = (0..shard.record_count() as u32)
        .map(|i| shard.record(i).unwrap().source)
        .collect();
    tags.sort_by_key(|t| t.to_u8());
    assert_eq!(tags, vec![SourceTag::Osm, SourceTag::Bosa]);
}

/// Larger smoke test against the real Brussels BOSA download. This
/// requires the 17 MB ZIP to be present at the path below — fetch
/// with `butterfly-dl belgium --only addresses` (the URL is in
/// `dl/regions/belgium.toml`).
///
/// Marked `#[ignore]` because:
/// - it depends on a 17 MB external download
/// - it builds a 1.3M-record shard which takes ~13 s
///
/// Use `cargo test --release -p butterfly-geocode --test bosa_e2e -- --ignored bosa_real_brussels` to run it.
#[test]
#[ignore = "requires downloaded BOSA Brussels ZIP at data/belgium/addresses/bosa-bebru.zip"]
fn bosa_real_brussels_zip_round_trip() {
    let zip_path = std::path::Path::new("data/belgium/addresses/bosa-bebru.zip");
    if !zip_path.exists() {
        // Print a diagnostic instead of panicking so CI without the
        // download can still see the test is properly gated.
        eprintln!(
            "BOSA Brussels ZIP not found at {}; skipping real-data smoke",
            zip_path.display()
        );
        return;
    }

    let loader = BosaCsvSource::new(zip_path, CountryId::BE);
    let mut count = 0u64;
    let mut sample = Vec::new();
    let mut rows_seen = 0u64;
    loader
        .stream(
            &mut |evt| {
                if let butterfly_geocode::sources::SourceProgress::Records {
                    rows_seen: rs, ..
                } = evt
                {
                    rows_seen = rs;
                }
            },
            &mut |rec| {
                count += 1;
                if sample.len() < 3 {
                    sample.push(rec);
                }
            },
        )
        .expect("BOSA stream");

    assert!(
        count > 100_000,
        "expected ~1.3M Brussels records; got {count}"
    );
    assert!(
        rows_seen >= 800_000,
        "expected ~840 K input rows; got {rows_seen}"
    );
    for r in &sample {
        assert_eq!(r.source, SourceTag::Bosa);
        assert!(r.lat > 50.0 && r.lat < 51.5);
        assert!(r.lon > 4.0 && r.lon < 5.0);
        assert!(!r.postcode.is_empty());
        assert!(r.postcode.starts_with("1"));
    }
}
