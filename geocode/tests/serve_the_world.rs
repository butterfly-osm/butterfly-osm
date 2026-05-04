//! Integration tests for the #96 "serve the world" pivot.
//!
//! Coverage:
//! - All 15 shipped country packs parse without panic.
//! - Classifier accuracy on a 60-input mixed-country test set.
//! - BFGS v3 → v4 migration: v3-magic shards are rejected with a
//!   precise error.
//! - The CountryId type accepts arbitrary 2-uppercase-letter codes
//!   (the architectural promise).

#![deny(unsafe_code)]

use butterfly_geocode::CountryId;
use butterfly_geocode::routing::{Classifier, PackRegistry};
use butterfly_geocode::shard::reader::Shard;

// `crc` crate is a transitive dev dep via butterfly-geocode itself, but
// only used inside test helpers. Adding `crc` as a dev-dep in Cargo.toml
// would be overkill since the lib already depends on it.

#[test]
fn shipped_packs_compile_with_15_countries() {
    let reg = PackRegistry::shipped().expect("shipped packs must compile");
    assert_eq!(reg.len(), 15);
    let want: Vec<CountryId> = vec![
        CountryId::AT,
        CountryId::AU,
        CountryId::BE,
        CountryId::BR,
        CountryId::CH,
        CountryId::DE,
        CountryId::ES,
        CountryId::FR,
        CountryId::GB,
        CountryId::IN,
        CountryId::IT,
        CountryId::JP,
        CountryId::LU,
        CountryId::NL,
        CountryId::US,
    ];
    let got = reg.countries();
    assert_eq!(got, want);
}

#[test]
fn arbitrary_iso2_works_without_pack() {
    // The architectural promise of #96: any 2-uppercase-letter input
    // is a valid CountryId. ZW (Zimbabwe), KE (Kenya), TH (Thailand)
    // — all valid even without a shipped pack. Adding a pack is a
    // TOML drop.
    for code in ["ZW", "KE", "TH", "MX", "NG", "ID", "TR"] {
        let c = CountryId::from_iso2(code).unwrap_or_else(|| panic!("{code} must parse"));
        assert_eq!(c.iso2(), code);
        // No pack loaded yet → registry returns None, gracefully.
        let reg = PackRegistry::shipped().unwrap();
        assert!(reg.get(c).is_none(), "no pack for {code} expected");
    }
}

/// Mixed-country accuracy test: 60 inputs across 15 countries (4 per).
/// Asserts top-1 accuracy ≥ 80% (architectural floor — the cheap
/// classifier is a routing prior, not a perfect predictor).
#[test]
fn classifier_accuracy_60_inputs() {
    let cases: &[(&str, CountryId)] = &[
        // ===== Belgium (4) =====
        ("Rue Wayez 122 1070 Anderlecht", CountryId::BE),
        ("Grote Markt 1 2000 Antwerpen", CountryId::BE),
        ("Boulevard Anspach 1 1000 Bruxelles", CountryId::BE),
        ("Korenmarkt 9000 Gent", CountryId::BE),
        // ===== France (4) =====
        ("10 rue de la Paix 75001 Paris", CountryId::FR),
        ("1 boulevard Saint-Germain 75005 Paris", CountryId::FR),
        ("Place Bellecour 69002 Lyon", CountryId::FR),
        ("Avenue de la République 13001 Marseille", CountryId::FR),
        // ===== Netherlands (4) =====
        ("Damrak 1 1012 LP Amsterdam", CountryId::NL),
        ("Coolsingel 100 3011 AD Rotterdam", CountryId::NL),
        ("Domplein 29 3512 JE Utrecht", CountryId::NL),
        ("Stationsplein 1 5611 AC Eindhoven", CountryId::NL),
        // ===== Luxembourg (4) =====
        ("12 rue de la Gare L-2453 Luxembourg", CountryId::LU),
        ("Place d'Armes Lëtzebuerg", CountryId::LU),
        ("Avenue de la Liberté L-1930 Luxembourg", CountryId::LU),
        ("Rue de l'Alzette Esch-sur-Alzette", CountryId::LU),
        // ===== Germany (4) =====
        ("Friedrichstraße 100 10117 Berlin", CountryId::DE),
        ("Marienplatz 1 80331 München", CountryId::DE),
        ("Reeperbahn 1 20359 Hamburg", CountryId::DE),
        ("Domplatz 50667 Köln", CountryId::DE),
        // ===== Austria (4) =====
        ("Stephansplatz 1 1010 Wien", CountryId::AT),
        ("Getreidegasse 9 5020 Salzburg", CountryId::AT),
        ("Hauptplatz 1 8010 Graz", CountryId::AT),
        ("Maria-Theresien-Straße 18 6020 Innsbruck", CountryId::AT),
        // ===== Switzerland (4) =====
        ("Bahnhofstrasse 1 8001 Zürich", CountryId::CH),
        ("Rue du Rhône 1 1204 Genève", CountryId::CH),
        ("Marktgasse 50 3011 Bern", CountryId::CH),
        ("Viale Cassarate Lugano", CountryId::CH),
        // ===== UK (4) =====
        ("10 Downing Street SW1A 2AA London", CountryId::GB),
        ("Princes Street EH2 2DG Edinburgh", CountryId::GB),
        ("Albert Square M2 5DB Manchester", CountryId::GB),
        ("New Street B2 4QA Birmingham", CountryId::GB),
        // ===== Spain (4) =====
        ("Calle Mayor 1 28013 Madrid", CountryId::ES),
        ("Passeig de Gràcia 1 08007 Barcelona", CountryId::ES),
        ("Avenida del Puerto 46021 Valencia", CountryId::ES),
        ("Calle Sierpes 41004 Sevilla", CountryId::ES),
        // ===== Italy (4) =====
        ("Via Roma 1 00184 Roma", CountryId::IT),
        ("Piazza Duomo 20121 Milano", CountryId::IT),
        ("Corso Umberto I 80138 Napoli", CountryId::IT),
        ("Via Garibaldi 10122 Torino", CountryId::IT),
        // ===== US (4) =====
        (
            "1600 Pennsylvania Ave NW Washington DC 20500",
            CountryId::US,
        ),
        ("350 Fifth Avenue New York NY 10118", CountryId::US),
        ("1 Infinite Loop Cupertino CA 95014", CountryId::US),
        ("700 Pennsylvania Ave Pittsburgh PA 15222", CountryId::US),
        // ===== Japan (4) =====
        ("東京都千代田区千代田1-1", CountryId::JP),
        ("〒100-0001 東京都千代田区千代田1丁目", CountryId::JP),
        ("大阪府大阪市中央区難波1-1", CountryId::JP),
        ("京都府京都市東山区祇園", CountryId::JP),
        // ===== Brazil (4) =====
        ("Avenida Paulista 1578 São Paulo 01310-200", CountryId::BR),
        ("Rua Oscar Freire 100 São Paulo", CountryId::BR),
        (
            "Avenida Atlântica 1702 Rio de Janeiro 22021-001",
            CountryId::BR,
        ),
        ("Rua das Pedras Curitiba", CountryId::BR),
        // ===== India (4) =====
        ("Rajpath New Delhi 110001", CountryId::IN),
        ("Marine Drive Mumbai 400020", CountryId::IN),
        ("MG Road Bangalore 560001", CountryId::IN),
        ("Anna Salai Chennai 600002", CountryId::IN),
        // ===== Australia (4) =====
        ("1 Macquarie Street Sydney NSW 2000", CountryId::AU),
        ("Federation Square Melbourne VIC 3000", CountryId::AU),
        ("Queen Street Brisbane QLD 4000", CountryId::AU),
        ("St Georges Terrace Perth WA 6000", CountryId::AU),
    ];

    let classifier = Classifier::shipped();
    let mut hits = 0usize;
    let mut misses: Vec<(String, CountryId, CountryId)> = Vec::new();
    for (text, expected) in cases {
        let posterior = classifier.classify(text);
        let top = posterior[0].0;
        if top == *expected {
            hits += 1;
        } else {
            misses.push(((*text).to_string(), *expected, top));
        }
    }
    let acc = hits as f64 / cases.len() as f64;
    eprintln!(
        "classifier accuracy = {}/{} = {:.1}%",
        hits,
        cases.len(),
        acc * 100.0
    );
    for (q, expected, got) in &misses {
        eprintln!("  miss: {q:?} expected {expected} got {got}");
    }
    assert!(
        acc >= 0.80,
        "classifier accuracy {:.1}% < 80% floor (misses: {})",
        acc * 100.0,
        misses.len()
    );
}

/// Helper: write a header-only shard with valid CRCs so the reader
/// passes Pattern B and reaches the version/iso2 checks.
fn write_minimal_shard(path: &std::path::Path, version: u16, country: [u8; 2]) {
    use crc::{CRC_64_XZ, Crc};
    let crc_engine = Crc::<u64>::new(&CRC_64_XZ);
    let mut header = [0u8; 64];
    header[0..4].copy_from_slice(b"BFGS");
    header[4..6].copy_from_slice(&version.to_le_bytes());
    header[6..8].copy_from_slice(&country);
    // record_count = 0, all sections zeroed → empty body. The body
    // CRC is over an empty slice; the file CRC is over the header.
    let body: &[u8] = &[];
    let mut body_digest = crc_engine.digest();
    body_digest.update(body);
    let body_crc = body_digest.finalize();
    let mut file_digest = crc_engine.digest();
    file_digest.update(&header);
    file_digest.update(body);
    let file_crc = file_digest.finalize();
    let mut buf = Vec::with_capacity(80);
    buf.extend_from_slice(&header);
    buf.extend_from_slice(body);
    buf.extend_from_slice(&body_crc.to_le_bytes());
    buf.extend_from_slice(&file_crc.to_le_bytes());
    std::fs::write(path, &buf).unwrap();
}

#[test]
fn rejects_v3_shards_with_helpful_error() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("v3.bfgs");
    // v3 header: country in byte 6 (single byte), byte 7 was _pad.
    write_minimal_shard(&path, 3, [1, 0]);
    let err = Shard::open(&path).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("BFGS v3"),
        "expected v3-rejection error, got: {msg}"
    );
    assert!(
        msg.contains("build-shard"),
        "expected upgrade hint, got: {msg}"
    );
}

#[test]
fn rejects_v4_shard_with_invalid_iso2() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("invalid.bfgs");
    // v4 header with lowercase letters where ISO2 belongs.
    write_minimal_shard(&path, 4, *b"be");
    let err = Shard::open(&path).unwrap_err();
    let msg = format!("{err:#}");
    assert!(
        msg.contains("ISO 3166-1") || msg.contains("uppercase"),
        "expected ISO2 validation error, got: {msg}"
    );
}
