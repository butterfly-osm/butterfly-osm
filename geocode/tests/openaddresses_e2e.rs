//! OpenAddresses authoritative-source ingestion end-to-end tests.
//!
//! These cover the round trip from a real-shape OpenAddresses
//! GeoJSON-seq feature → shard build → mmap reader → query →
//! response. They DO NOT hit the network — the test corpus is a
//! 4-feature real-shape sample lifted from the live OpenAddresses
//! Belgium / Brussels-FR job (`be/bru/bosa-region-brussels-fr`,
//! verified 2026-05-05). Larger smoke tests live in
//! `belgium_e2e.rs` behind the `#[ignore]` gate that requires the
//! 23 MB `.geojson.gz` download.

use std::io::Write;
use std::path::PathBuf;

use butterfly_geocode::CountryId;
use butterfly_geocode::shard::SourceTag;
use butterfly_geocode::shard::builder::build_shard;
use butterfly_geocode::shard::reader::Shard;
use butterfly_geocode::sources::openaddresses::OpenAddressesSource;
use butterfly_geocode::sources::{Source, collect_all};

/// Real-shape OpenAddresses GeoJSON-seq lifted from the live BE-BRU-FR
/// pack (job 824413, 2026-05-05). 4 features: 3 valid (rows 1-3),
/// 1 row with an empty `street` to exercise the skip path.
const REAL_SHAPE_GEOJSON: &str = concat!(
    r#"{"type":"Feature","properties":{"hash":"e9bfa4b3f42f842d","number":"475","street":"Chaussée de Mons","unit":"RDC","city":"Anderlecht","district":"","region":"","postcode":"1070","id":"BE-BRU:615867","accuracy":""},"geometry":{"type":"Point","coordinates":[4.31653,50.83595]}}"#,
    "\n",
    r#"{"type":"Feature","properties":{"hash":"85cad0bdc3514625","number":"603","street":"Chaussée de Mons","unit":"2eET","city":"Anderlecht","district":"","region":"","postcode":"1070","id":"BE-BRU:615868","accuracy":""},"geometry":{"type":"Point","coordinates":[4.31283,50.83327]}}"#,
    "\n",
    r#"{"type":"Feature","properties":{"hash":"d","number":"1","street":"Grand-Place","unit":"","city":"Bruxelles","district":"","region":"","postcode":"1000","id":"BE-BRU:99999","accuracy":""},"geometry":{"type":"Point","coordinates":[4.35251,50.84671]}}"#,
    "\n",
    r#"{"type":"Feature","properties":{"hash":"x","number":"","street":"","city":"","postcode":""},"geometry":{"type":"Point","coordinates":[4.0,50.0]}}"#,
    "\n",
);

fn write_sample(dir: &std::path::Path, ext: &str, contents: &str) -> PathBuf {
    let p = dir.join(format!("oa-sample.{ext}"));
    std::fs::write(&p, contents).unwrap();
    p
}

#[test]
fn oa_geojson_to_shard_round_trip() {
    let dir = tempfile::tempdir().unwrap();
    let geo = write_sample(dir.path(), "geojson", REAL_SHAPE_GEOJSON);
    let shard_path = dir.path().join("oa.bfgs");

    let loader = OpenAddressesSource::new(&geo, CountryId::BE);
    let recs = collect_all(&loader, |_| {}).unwrap();
    // 3 valid features, 1 skipped (empty street).
    assert_eq!(
        recs.len(),
        3,
        "expected 3 OA records, got {}: {:#?}",
        recs.len(),
        recs
    );
    for r in &recs {
        assert_eq!(r.source, SourceTag::OpenAddresses);
        assert!(r.source_id.is_some(), "OA id should propagate");
    }

    build_shard(&shard_path, CountryId::BE, recs).expect("build shard");

    // Reload + verify the source byte survives the v5 round trip.
    let shard = Shard::open(&shard_path).expect("open shard");
    assert_eq!(shard.country(), CountryId::BE);
    assert_eq!(shard.record_count(), 3);
    for i in 0..shard.record_count() as u32 {
        let r = shard.record(i).expect("record present");
        assert_eq!(
            r.source,
            SourceTag::OpenAddresses,
            "source byte lost in round trip on record {i}: {r:#?}"
        );
    }
}

#[test]
fn oa_query_round_trip_finds_real_address() {
    let dir = tempfile::tempdir().unwrap();
    let geo = write_sample(dir.path(), "geojson", REAL_SHAPE_GEOJSON);
    let shard_path = dir.path().join("oa.bfgs");

    let loader = OpenAddressesSource::new(&geo, CountryId::BE);
    let recs = collect_all(&loader, |_| {}).unwrap();
    build_shard(&shard_path, CountryId::BE, recs).unwrap();

    let shard = Shard::open(&shard_path).unwrap();

    // Postcode 1070 should hit the two Anderlecht records.
    let p1070 = shard.postings_for_postcode("1070");
    assert_eq!(
        p1070.len(),
        2,
        "expected 2 records for postcode 1070, got {}",
        p1070.len()
    );

    // Reverse-geocode at the OA-published coord → Chaussée de Mons.
    let nearest = shard.nearest(50.83595, 4.31653).expect("record nearby");
    let street = nearest.street.to_lowercase();
    assert!(
        street.contains("chaussée") || street.contains("mons"),
        "expected Chaussée de Mons, got {}",
        nearest.street
    );
    assert_eq!(nearest.source, SourceTag::OpenAddresses);
}

#[test]
fn oa_gzipped_geojson_round_trip() {
    use flate2::Compression;
    use flate2::write::GzEncoder;

    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("oa-sample.geojson.gz");
    let f = std::fs::File::create(&p).unwrap();
    let mut enc = GzEncoder::new(f, Compression::fast());
    enc.write_all(REAL_SHAPE_GEOJSON.as_bytes()).unwrap();
    enc.finish().unwrap();

    let loader = OpenAddressesSource::new(&p, CountryId::BE);
    let recs = collect_all(&loader, |_| {}).unwrap();
    assert_eq!(recs.len(), 3);

    let shard_path = dir.path().join("oa-gz.bfgs");
    build_shard(&shard_path, CountryId::BE, recs).unwrap();
    let s = Shard::open(&shard_path).unwrap();
    assert_eq!(s.record_count(), 3);
}

#[test]
fn oa_dedup_via_merge_against_osm() {
    use butterfly_geocode::shard::AddressRecord;
    use butterfly_geocode::sources::merge_records;

    // Two records at the same address: OA + OSM. Merge dedups OSM
    // because OA outranks it.
    let oa = vec![AddressRecord {
        lat: 50.83595,
        lon: 4.31653,
        street: "Chaussée de Mons".to_string(),
        locality: "Anderlecht".to_string(),
        housenumber: "475 RDC".to_string(),
        postcode: "1070".to_string(),
        source: SourceTag::OpenAddresses,
        source_id: Some("BE-BRU:615867".to_string()),
    }];
    let osm = vec![AddressRecord {
        lat: 50.83594,
        lon: 4.31654,
        street: "chaussée de mons".to_string(),
        locality: "Anderlecht".to_string(),
        housenumber: "475 RDC".to_string(),
        postcode: "1070".to_string(),
        source: SourceTag::Osm,
        source_id: None,
    }];
    let merged = merge_records(vec![oa, osm]);
    assert_eq!(merged.len(), 1);
    assert_eq!(merged[0].source, SourceTag::OpenAddresses);
}

#[test]
fn build_shard_via_merge_preserves_per_record_source() {
    use butterfly_geocode::shard::AddressRecord;
    use butterfly_geocode::sources::merge_records;

    // Build an OA record + an OSM record at different addresses so
    // both survive merge. Confirms the per-record source byte makes
    // it through the v5 round trip.
    let oa_records = vec![AddressRecord {
        lat: 50.83595,
        lon: 4.31653,
        street: "Chaussée de Mons".to_string(),
        locality: "Anderlecht".to_string(),
        housenumber: "475".to_string(),
        postcode: "1070".to_string(),
        source: SourceTag::OpenAddresses,
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

    let merged = merge_records(vec![oa_records, osm_records]);
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
    assert_eq!(tags, vec![SourceTag::Osm, SourceTag::OpenAddresses]);
}

#[test]
fn oa_cross_source_merge_drops_duplicates() {
    use butterfly_geocode::shard::AddressRecord;
    use butterfly_geocode::sources::merge_records;

    // Synthetic fixture: 5 records, 2 are exact dupes between OA and
    // OSM. Merge should produce 3 unique survivors (5 − 2 OSM dupes).
    let oa = vec![
        AddressRecord {
            lat: 50.834,
            lon: 4.314,
            street: "Rue Wayez".into(),
            locality: "Anderlecht".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            source: SourceTag::OpenAddresses,
            source_id: Some("oa1".into()),
        },
        AddressRecord {
            lat: 51.221,
            lon: 4.401,
            street: "Grote Markt".into(),
            locality: "Antwerpen".into(),
            housenumber: "1".into(),
            postcode: "2000".into(),
            source: SourceTag::OpenAddresses,
            source_id: Some("oa2".into()),
        },
    ];
    let osm = vec![
        // Duplicate of oa1 within ~30 m → dropped by merge.
        AddressRecord {
            lat: 50.8341,
            lon: 4.3141,
            street: "rue wayez".into(),
            locality: "Anderlecht".into(),
            housenumber: "122".into(),
            postcode: "1070".into(),
            source: SourceTag::Osm,
            source_id: None,
        },
        // Duplicate of oa2 within ~30 m → dropped.
        AddressRecord {
            lat: 51.2211,
            lon: 4.4011,
            street: "grote markt".into(),
            locality: "Antwerpen".into(),
            housenumber: "1".into(),
            postcode: "2000".into(),
            source: SourceTag::Osm,
            source_id: None,
        },
        // Unique OSM record (no OA equivalent) → survives.
        AddressRecord {
            lat: 50.5,
            lon: 5.5,
            street: "Place Saint-Lambert".into(),
            locality: "Liège".into(),
            housenumber: "1".into(),
            postcode: "4000".into(),
            source: SourceTag::Osm,
            source_id: None,
        },
    ];

    let merged = merge_records(vec![oa, osm]);
    assert_eq!(
        merged.len(),
        3,
        "expected 3 survivors (2 OA + 1 unique OSM), got {}: {:#?}",
        merged.len(),
        merged
    );
    // Tag distribution: 2 OA (won the dedup) + 1 OSM (unique).
    let n_oa = merged
        .iter()
        .filter(|r| r.source == SourceTag::OpenAddresses)
        .count();
    let n_osm = merged.iter().filter(|r| r.source == SourceTag::Osm).count();
    assert_eq!(n_oa, 2);
    assert_eq!(n_osm, 1);
}

/// Larger smoke test against the real Brussels-FR OpenAddresses job
/// (job 824413, `be/bru/bosa-region-brussels-fr`). Requires the
/// 23 MB `.geojson.gz` to be present at the path below — fetch via
/// `butterfly-dl belgium --only addresses` (URL is in
/// `dl/regions/belgium.toml`).
///
/// Marked `#[ignore]` because:
/// - it depends on a 23 MB external download
/// - it builds an 841k-record shard which takes ~6 s
///
/// Use `cargo test --release -p butterfly-geocode --test
/// openaddresses_e2e -- --ignored oa_real_brussels_round_trip` to run.
#[test]
#[ignore = "requires downloaded OA Brussels-FR .geojson.gz at data/belgium/addresses/oa-be-bru-fr.geojson.gz"]
fn oa_real_brussels_round_trip() {
    let path = std::path::Path::new("data/belgium/addresses/oa-be-bru-fr.geojson.gz");
    if !path.exists() {
        eprintln!(
            "OA Brussels-FR not found at {}; skipping real-data smoke",
            path.display()
        );
        return;
    }

    let loader = OpenAddressesSource::new(path, CountryId::BE);
    let mut count = 0u64;
    let mut sample = Vec::new();
    loader
        .stream(&mut |_| {}, &mut |rec| {
            count += 1;
            if sample.len() < 3 {
                sample.push(rec);
            }
        })
        .expect("OA stream");

    // Brussels-FR pack is ~840 K records — accept anything in that
    // ballpark in case of upstream re-runs.
    assert!(
        count > 700_000,
        "expected ~840 K Brussels-FR records; got {count}"
    );
    for r in &sample {
        assert_eq!(r.source, SourceTag::OpenAddresses);
        assert!(r.lat > 50.0 && r.lat < 51.5);
        assert!(r.lon > 4.0 && r.lon < 5.0);
        assert!(r.postcode.starts_with('1'), "expected BRU 1xxx postcode");
    }
}
