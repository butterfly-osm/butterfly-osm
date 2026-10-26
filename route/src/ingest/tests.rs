//! step1-ingest on a PBF built by hand: a merged extract carries an object
//! once per version (#652) and the newest copy must win everywhere.
//!
//! The encoder below writes the OSM PBF wire format directly (protobuf
//! varints, uncompressed `raw` blobs, plain `Node`s rather than `DenseNodes`),
//! so the fixture is readable in the test itself and the test does not depend
//! on an external tool to produce it.

use super::*;
use crate::formats::{NodeSignalsFile, RelationsFile, WaysFile};
use std::io::Read;
use tempfile::TempDir;

// ---------------------------------------------------------------- protobuf --

fn varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let byte = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(byte);
            return;
        }
        out.push(byte | 0x80);
    }
}

fn zigzag(v: i64) -> u64 {
    ((v << 1) ^ (v >> 63)) as u64
}

fn field_varint(field: u32, v: u64, out: &mut Vec<u8>) {
    varint(u64::from(field << 3), out);
    varint(v, out);
}

fn field_bytes(field: u32, bytes: &[u8], out: &mut Vec<u8>) {
    varint(u64::from(field << 3 | 2), out);
    varint(bytes.len() as u64, out);
    out.extend_from_slice(bytes);
}

/// A packed repeated field of varints.
fn field_packed(field: u32, values: impl IntoIterator<Item = u64>, out: &mut Vec<u8>) {
    let mut body = Vec::new();
    for v in values {
        varint(v, &mut body);
    }
    field_bytes(field, &body, out);
}

// --------------------------------------------------------------- fixtures --

/// One OSM object of the fixture, in the (id, version) order `osmium merge`
/// writes them.
struct FNode {
    id: i64,
    version: i32,
    lat: f64,
    lon: f64,
    tags: Vec<(&'static str, &'static str)>,
}

struct FWay {
    id: i64,
    version: i32,
    refs: Vec<i64>,
    tags: Vec<(&'static str, &'static str)>,
}

struct FRel {
    id: i64,
    version: i32,
    /// (role, member kind 0 = node / 1 = way, ref id)
    members: Vec<(&'static str, u64, i64)>,
    tags: Vec<(&'static str, &'static str)>,
}

struct StringTable(Vec<Vec<u8>>);

impl StringTable {
    fn new() -> Self {
        StringTable(vec![Vec::new()]) // index 0 is reserved (the empty string)
    }
    fn index(&mut self, s: &str) -> u64 {
        if let Some(i) = self.0.iter().position(|e| e == s.as_bytes()) {
            return i as u64;
        }
        self.0.push(s.as_bytes().to_vec());
        (self.0.len() - 1) as u64
    }
    fn encode(&self) -> Vec<u8> {
        let mut out = Vec::new();
        for s in &self.0 {
            field_bytes(1, s, &mut out);
        }
        out
    }
}

fn info(version: i32) -> Vec<u8> {
    let mut out = Vec::new();
    field_varint(1, version as u64, &mut out);
    out
}

/// granularity 100 → one unit is 1e-7 degree.
const GRANULARITY: f64 = 100.0;

fn encode_primitive_block(nodes: &[FNode], ways: &[FWay], rels: &[FRel]) -> Vec<u8> {
    let mut st = StringTable::new();
    let mut group = Vec::new();

    for n in nodes {
        let mut node = Vec::new();
        field_varint(1, zigzag(n.id), &mut node);
        field_packed(2, n.tags.iter().map(|(k, _)| st.index(k)), &mut node);
        field_packed(3, n.tags.iter().map(|(_, v)| st.index(v)), &mut node);
        field_bytes(4, &info(n.version), &mut node);
        field_varint(
            8,
            zigzag((n.lat * 1e9 / GRANULARITY).round() as i64),
            &mut node,
        );
        field_varint(
            9,
            zigzag((n.lon * 1e9 / GRANULARITY).round() as i64),
            &mut node,
        );
        field_bytes(1, &node, &mut group);
    }
    for w in ways {
        let mut way = Vec::new();
        field_varint(1, w.id as u64, &mut way);
        field_packed(2, w.tags.iter().map(|(k, _)| st.index(k)), &mut way);
        field_packed(3, w.tags.iter().map(|(_, v)| st.index(v)), &mut way);
        field_bytes(4, &info(w.version), &mut way);
        let mut prev = 0i64;
        let refs: Vec<u64> = w
            .refs
            .iter()
            .map(|&r| {
                let d = r - prev;
                prev = r;
                zigzag(d)
            })
            .collect();
        field_packed(8, refs, &mut way);
        field_bytes(3, &way, &mut group);
    }
    for r in rels {
        let mut rel = Vec::new();
        field_varint(1, r.id as u64, &mut rel);
        field_packed(2, r.tags.iter().map(|(k, _)| st.index(k)), &mut rel);
        field_packed(3, r.tags.iter().map(|(_, v)| st.index(v)), &mut rel);
        field_bytes(4, &info(r.version), &mut rel);
        field_packed(
            8,
            r.members.iter().map(|(role, _, _)| st.index(role)),
            &mut rel,
        );
        let mut prev = 0i64;
        let memids: Vec<u64> = r
            .members
            .iter()
            .map(|&(_, _, id)| {
                let d = id - prev;
                prev = id;
                zigzag(d)
            })
            .collect();
        field_packed(9, memids, &mut rel);
        field_packed(10, r.members.iter().map(|&(_, kind, _)| kind), &mut rel);
        field_bytes(4, &rel, &mut group);
    }

    let mut block = Vec::new();
    field_bytes(1, &st.encode(), &mut block);
    field_bytes(2, &group, &mut block);
    field_varint(17, GRANULARITY as u64, &mut block);
    block
}

fn blob(kind: &str, payload: &[u8], out: &mut Vec<u8>) {
    let mut blob = Vec::new();
    field_bytes(1, payload, &mut blob);
    field_varint(2, payload.len() as u64, &mut blob);
    let mut header = Vec::new();
    field_bytes(1, kind.as_bytes(), &mut header);
    field_varint(3, blob.len() as u64, &mut header);
    out.extend_from_slice(&(header.len() as u32).to_be_bytes());
    out.extend_from_slice(&header);
    out.extend_from_slice(&blob);
}

fn encode_pbf(nodes: &[FNode], ways: &[FWay], rels: &[FRel]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut header_block = Vec::new();
    field_bytes(4, b"OsmSchema-V0.6", &mut header_block);
    blob("OSMHeader", &header_block, &mut out);
    blob(
        "OSMData",
        &encode_primitive_block(nodes, ways, rels),
        &mut out,
    );
    out
}

/// nodes.sa records: (id, lat, lon) at 1e-7 degree.
fn read_nodes_sa(path: &Path) -> Vec<(i64, i32, i32)> {
    let mut bytes = Vec::new();
    std::fs::File::open(path)
        .unwrap()
        .read_to_end(&mut bytes)
        .unwrap();
    let count = u64::from_le_bytes(bytes[8..16].try_into().unwrap()) as usize;
    (0..count)
        .map(|i| {
            let r = &bytes[128 + 16 * i..128 + 16 * (i + 1)];
            (
                i64::from_le_bytes(r[0..8].try_into().unwrap()),
                i32::from_le_bytes(r[8..12].try_into().unwrap()),
                i32::from_le_bytes(r[12..16].try_into().unwrap()),
            )
        })
        .collect()
}

fn ingest(pbf: &[u8]) -> (TempDir, IngestResult) {
    let dir = TempDir::new().unwrap();
    let input = dir.path().join("merged.osm.pbf");
    std::fs::write(&input, pbf).unwrap();
    let result = run_ingest(IngestConfig {
        input,
        outdir: dir.path().join("step1"),
    })
    .unwrap();
    (dir, result)
}

/// Two extracts of neighbouring regions taken one day apart, merged the way
/// `osmium merge` does it: every object once per (id, version), in that order.
///
/// Edited between the two days: node 1 moved and lost its signal (v1 → v2),
/// way 10 changed maxspeed 50 → 30 (v8 → v9), relation 20 changed its
/// restriction (v1 → v2). Node 3 and way 11 are identical in both extracts
/// (merge writes them once; node 3 is written twice here to show the engine
/// tolerates a merge that did not). Node 2 is only in the older extract, node
/// 4 only in the newer.
fn merged_fixture() -> Vec<u8> {
    let nodes = [
        FNode {
            id: 1,
            version: 1,
            lat: 50.0,
            lon: 4.0,
            tags: vec![("highway", "traffic_signals")],
        },
        FNode {
            id: 1,
            version: 2,
            lat: 50.0001,
            lon: 4.0,
            tags: vec![],
        },
        FNode {
            id: 2,
            version: 3,
            lat: 50.001,
            lon: 4.001,
            tags: vec![],
        },
        FNode {
            id: 3,
            version: 1,
            lat: 50.002,
            lon: 4.002,
            tags: vec![],
        },
        FNode {
            id: 3,
            version: 1,
            lat: 50.002,
            lon: 4.002,
            tags: vec![],
        },
        FNode {
            id: 4,
            version: 1,
            lat: 50.003,
            lon: 4.003,
            tags: vec![("highway", "traffic_signals")],
        },
    ];
    let ways = [
        FWay {
            id: 10,
            version: 8,
            refs: vec![1, 2],
            tags: vec![("highway", "tertiary"), ("maxspeed", "50")],
        },
        FWay {
            id: 10,
            version: 9,
            refs: vec![1, 2],
            tags: vec![("highway", "tertiary"), ("maxspeed", "30")],
        },
        FWay {
            id: 11,
            version: 1,
            refs: vec![2, 3, 4],
            tags: vec![("highway", "residential")],
        },
    ];
    let rels = [
        FRel {
            id: 20,
            version: 1,
            members: vec![("from", 1, 10), ("via", 0, 2), ("to", 1, 11)],
            tags: vec![("type", "restriction"), ("restriction", "no_left_turn")],
        },
        FRel {
            id: 20,
            version: 2,
            members: vec![("from", 1, 10), ("via", 0, 2), ("to", 1, 11)],
            tags: vec![("type", "restriction"), ("restriction", "no_right_turn")],
        },
    ];
    encode_pbf(&nodes, &ways, &rels)
}

#[test]
fn merged_extract_keeps_the_newest_version_of_every_object() {
    let (dir, result) = ingest(&merged_fixture());

    assert_eq!(result.nodes_count, 4, "one node per id");
    assert_eq!(result.ways_count, 2, "one way per id");
    assert_eq!(result.relations_count, 1, "one relation per id");

    // Nodes: ids unique and strictly increasing; node 1 at its v2 position.
    let nodes = read_nodes_sa(&result.nodes_sa_file);
    assert_eq!(
        nodes,
        vec![
            (1, 500_001_000, 40_000_000),
            (2, 500_010_000, 40_010_000),
            (3, 500_020_000, 40_020_000),
            (4, 500_030_000, 40_030_000),
        ]
    );

    // Signals follow the surviving version: node 1 lost its signal in v2.
    let signals = NodeSignalsFile::read(&result.node_signals_file).unwrap();
    assert!(
        !signals.has_signal(1),
        "v2 of node 1 has no traffic_signals tag"
    );
    assert!(signals.has_signal(4));
    assert_eq!(signals.len(), 1);

    // Ways: way 10 is v9 (maxspeed 30).
    let ways = WaysFile::read(&result.ways_file).unwrap();
    assert_eq!(ways.iter().map(|w| w.id).collect::<Vec<_>>(), vec![10, 11]);
    let way10 = &ways[0];
    assert!(
        way10.tags.iter().any(|(k, v)| k == "maxspeed" && v == "30"),
        "way 10 must be its v9 copy, got tags {:?}",
        way10.tags
    );
    assert_eq!(way10.nodes, vec![1, 2]);

    // Relations: relation 20 is v2 (no_right_turn).
    let rels = RelationsFile::read(&result.relations_file).unwrap();
    assert_eq!(rels.len(), 1);
    assert!(
        rels[0]
            .tags
            .iter()
            .any(|(k, v)| k == "restriction" && v == "no_right_turn"),
        "relation 20 must be its v2 copy, got {:?}",
        rels[0].tags
    );
    assert_eq!(rels[0].members.len(), 3);

    // And the step1 lock accepts the output.
    crate::validate::verify_lock_conditions(
        &result.nodes_sa_file,
        &result.nodes_si_file,
        &result.ways_file,
        &result.relations_file,
    )
    .unwrap();
    drop(dir);
}

#[test]
fn single_extract_is_untouched() {
    // Unique ids, no metadata (version 0 everywhere): nothing dropped, nothing
    // reordered, the same counts as before #652.
    let nodes = [
        FNode {
            id: 7,
            version: 0,
            lat: 50.5,
            lon: 4.5,
            tags: vec![],
        },
        FNode {
            id: 9,
            version: 0,
            lat: 50.6,
            lon: 4.6,
            tags: vec![("highway", "traffic_signals")],
        },
    ];
    let ways = [FWay {
        id: 30,
        version: 0,
        refs: vec![7, 9],
        tags: vec![("highway", "primary")],
    }];
    let (_dir, result) = ingest(&encode_pbf(&nodes, &ways, &[]));
    assert_eq!(result.nodes_count, 2);
    assert_eq!(result.ways_count, 1);
    assert_eq!(result.relations_count, 0);
    assert_eq!(result.signal_nodes_count, 1);
    assert_eq!(
        read_nodes_sa(&result.nodes_sa_file),
        vec![(7, 505_000_000, 45_000_000), (9, 506_000_000, 46_000_000)]
    );
}

#[test]
fn keep_newest_keeps_the_last_of_each_sorted_group() {
    // (id, version) already sorted; the survivor is the last of its group.
    let mut items = vec![(1, 1), (1, 2), (2, 1), (3, 1), (3, 1), (3, 4), (4, 1)];
    let dropped = keep_newest(&mut items, |&(id, _)| id);
    assert_eq!(dropped, 3);
    assert_eq!(items, vec![(1, 2), (2, 1), (3, 4), (4, 1)]);

    let mut empty: Vec<(i64, i32)> = Vec::new();
    assert_eq!(keep_newest(&mut empty, |&(id, _)| id), 0);
    assert!(empty.is_empty());
}

#[test]
fn step1_lock_refuses_a_duplicated_way_id() {
    // Write ways.raw with a duplicated id directly (bypassing the ingest's
    // dedupe) and check the lock condition catches it.
    let dir = TempDir::new().unwrap();
    let (_d, result) = ingest(&merged_fixture());
    let ways_path = dir.path().join("ways.raw");
    let ways = vec![
        Way {
            id: 10,
            nodes: vec![1, 2],
            tags: vec![("highway".into(), "tertiary".into())],
        },
        Way {
            id: 10,
            nodes: vec![1, 2],
            tags: vec![("highway".into(), "tertiary".into())],
        },
    ];
    WaysFile::write(&ways_path, &ways).unwrap();
    let err = crate::validate::verify_lock_conditions(
        &result.nodes_sa_file,
        &result.nodes_si_file,
        &ways_path,
        &result.relations_file,
    )
    .unwrap_err();
    assert!(
        err.to_string().contains("way ids not strictly increasing"),
        "{err}"
    );
}
