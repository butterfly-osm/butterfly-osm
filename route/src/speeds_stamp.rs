//! Graph-identity stamp check for the per-edge speeds table.
//!
//! `edge_speeds.parquet` is keyed by OSM node pairs, so it silently applies
//! to ANY graph: a table built against another extract still "loads" and
//! calibrates only the edges whose node pairs happen to survive (a table
//! keyed to another graph calibrated about a third of the directed edges
//! instead of the expected ~94 %, with nothing on `/health` to say so).
//!
//! The contract, generic and producer-independent:
//!
//! - The PRODUCER stamps the table's parquet key-value metadata with the
//!   identity of the graph it was built against, one `graph_<key>` entry per
//!   identity key (e.g. `graph_pbf_sha`), plus `gate=passed` once its own
//!   quality gate accepted the table.
//! - The OPERATOR stages, next to the container, a small text file naming the
//!   loaded graph's identity as whitespace-separated `key=value` tokens (any
//!   other token is ignored), and points `BUTTERFLY_GRAPH_IDENTITY_FILE` at
//!   it.
//! - At serve boot the engine compares, for every compared key the graph
//!   identity carries, the table's `graph_<key>`. A key the container does
//!   not carry is not compared: a table built on a superset extract (e.g.
//!   two countries) still keys every edge of a one-country container built
//!   from the same source.
//!
//! Anything but [`StampVerdict::Ok`] or [`StampVerdict::Unverified`] means
//! the table is NOT applied: the clean base car serves, the boot logs an
//! error naming both identities and `/health` reports the verdict so a
//! post-deploy gate can fail on it. `Unverified` (no identity configured)
//! applies a stamped, gated table but stays visible on `/health` — it is a
//! deployment that cannot prove what it serves, and the gate fails it.
//! `BUTTERFLY_SPEEDS_STAMP_CHECK=off` restores the unchecked behaviour (for
//! tables whose producer has no notion of the graph, e.g. a simulation's own
//! output).

use std::collections::BTreeMap;
use std::path::Path;

use anyhow::{Context, Result};

/// Prefix of the stamp keys in the table's parquet key-value metadata.
pub const STAMP_PREFIX: &str = "graph_";
/// Identity keys compared by default (`BUTTERFLY_SPEEDS_STAMP_KEYS` overrides,
/// comma-separated).
pub const DEFAULT_STAMP_KEYS: &[&str] = &["pbf_sha", "lu_pbf_sha"];
/// The value of the table's `gate` key once its producer's gate accepted it.
pub const GATE_PASSED: &str = "passed";

/// Path of the graph-identity file.
pub const ENV_IDENTITY_FILE: &str = "BUTTERFLY_GRAPH_IDENTITY_FILE";
/// `off` restores the unchecked behaviour.
pub const ENV_CHECK: &str = "BUTTERFLY_SPEEDS_STAMP_CHECK";
/// Comma-separated identity keys to compare.
pub const ENV_KEYS: &str = "BUTTERFLY_SPEEDS_STAMP_KEYS";

/// Outcome of comparing a speeds table's stamp with the loaded graph.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StampVerdict {
    /// Stamped, gated, and every compared key matches the loaded graph.
    Ok,
    /// A compared key differs (or the table lacks one the graph carries).
    Mismatched,
    /// The table carries no `graph_<key>` stamp for any compared key.
    Unstamped,
    /// Stamped, but `gate` is absent or not `passed`.
    Ungated,
    /// Stamped and gated, but the loaded graph's identity is unknown (no
    /// identity file configured, or it carries none of the compared keys).
    Unverified,
}

impl StampVerdict {
    /// The word `/health` reports.
    pub fn as_str(self) -> &'static str {
        match self {
            StampVerdict::Ok => "ok",
            StampVerdict::Mismatched => "mismatched",
            StampVerdict::Unstamped => "unstamped",
            StampVerdict::Ungated => "ungated",
            StampVerdict::Unverified => "unverified",
        }
    }

    /// Whether the table may be applied.
    pub fn applies(self) -> bool {
        matches!(self, StampVerdict::Ok | StampVerdict::Unverified)
    }
}

/// A verdict plus the human-readable identities behind it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StampCheck {
    pub verdict: StampVerdict,
    /// Both identities, e.g. `graph: pbf_sha=ab.. | table: graph_pbf_sha=cd.. gate=passed`.
    pub detail: String,
}

/// Parse a graph-identity text: whitespace-separated `key=value` tokens;
/// tokens without `=` (or with an empty key) are ignored.
pub fn parse_graph_identity(text: &str) -> BTreeMap<String, String> {
    text.split_whitespace()
        .filter_map(|t| t.split_once('='))
        .filter(|(k, _)| !k.is_empty())
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

/// Read and parse a graph-identity file.
pub fn read_graph_identity(path: &Path) -> Result<BTreeMap<String, String>> {
    let text = std::fs::read_to_string(path)
        .with_context(|| format!("reading graph identity {}", path.display()))?;
    Ok(parse_graph_identity(&text))
}

/// The table's parquet key-value metadata (footer only, no row read).
pub fn read_table_kv(path: &Path) -> Result<BTreeMap<String, String>> {
    use parquet::file::reader::FileReader;
    let f = std::fs::File::open(path)
        .with_context(|| format!("opening speeds table {}", path.display()))?;
    let r = parquet::file::serialized_reader::SerializedFileReader::new(f)
        .with_context(|| format!("reading parquet footer of {}", path.display()))?;
    let mut out = BTreeMap::new();
    if let Some(kvs) = r.metadata().file_metadata().key_value_metadata() {
        for kv in kvs {
            if let Some(v) = kv.value.as_ref() {
                out.insert(kv.key.clone(), v.clone());
            }
        }
    }
    Ok(out)
}

/// The compared keys: `BUTTERFLY_SPEEDS_STAMP_KEYS` or [`DEFAULT_STAMP_KEYS`].
pub fn stamp_keys_from_env() -> Vec<String> {
    match std::env::var(ENV_KEYS) {
        Ok(v) if !v.trim().is_empty() => v
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect(),
        _ => DEFAULT_STAMP_KEYS.iter().map(|s| s.to_string()).collect(),
    }
}

/// Whether the check is on (default) — `BUTTERFLY_SPEEDS_STAMP_CHECK=off|0|false` disables it.
pub fn check_enabled_from_env() -> bool {
    std::env::var(ENV_CHECK)
        .map(|v| !matches!(v.to_ascii_lowercase().as_str(), "off" | "0" | "false"))
        .unwrap_or(true)
}

fn fmt_pairs<'a>(pairs: impl Iterator<Item = (String, Option<&'a String>)>) -> String {
    let s: Vec<String> = pairs
        .map(|(k, v)| format!("{k}={}", v.map(String::as_str).unwrap_or("<absent>")))
        .collect();
    if s.is_empty() {
        "<none>".to_string()
    } else {
        s.join(" ")
    }
}

/// Compare a table's key-value metadata with the loaded graph's identity
/// (`None` = no identity configured). Pure: no I/O, no environment.
pub fn verify_stamp(
    identity: Option<&BTreeMap<String, String>>,
    table_kv: &BTreeMap<String, String>,
    keys: &[String],
) -> StampCheck {
    let stamp_key = |k: &str| format!("{STAMP_PREFIX}{k}");
    let table_side = fmt_pairs(
        keys.iter()
            .map(|k| (stamp_key(k), table_kv.get(&stamp_key(k))))
            .chain(std::iter::once(("gate".to_string(), table_kv.get("gate")))),
    );
    let graph_side = match identity {
        Some(id) => fmt_pairs(keys.iter().map(|k| (k.clone(), id.get(k)))),
        None => format!("<no identity: {ENV_IDENTITY_FILE} unset>"),
    };
    let detail = format!("graph: {graph_side} | table: {table_side}");
    let mk = |verdict| StampCheck {
        verdict,
        detail: detail.clone(),
    };

    if !keys.iter().any(|k| table_kv.contains_key(&stamp_key(k))) {
        return mk(StampVerdict::Unstamped);
    }
    if table_kv.get("gate").map(String::as_str) != Some(GATE_PASSED) {
        return mk(StampVerdict::Ungated);
    }
    let Some(identity) = identity else {
        return mk(StampVerdict::Unverified);
    };
    let mut compared = 0usize;
    for k in keys {
        let Some(want) = identity.get(k) else {
            continue; // the graph does not carry this key: not compared
        };
        compared += 1;
        if table_kv.get(&stamp_key(k)) != Some(want) {
            return mk(StampVerdict::Mismatched);
        }
    }
    if compared == 0 {
        return mk(StampVerdict::Unverified);
    }
    mk(StampVerdict::Ok)
}

/// Boot-time check of one table against the configured identity (env).
/// An unreadable table footer or a configured-but-unreadable identity file
/// is an `Err`: the caller refuses the table (a typo'd path must never pass
/// as "no identity configured").
pub fn check_table(table: &Path, keys: &[String]) -> Result<StampCheck> {
    let kv = read_table_kv(table)?;
    let identity = match std::env::var_os(ENV_IDENTITY_FILE).filter(|v| !v.is_empty()) {
        Some(p) => Some(read_graph_identity(Path::new(&p))?),
        None => None,
    };
    Ok(verify_stamp(identity.as_ref(), &kv, keys))
}

/// What serve boot did with one region's speeds table, for `/health`.
#[derive(Debug, Clone, PartialEq)]
pub struct SpeedsReport {
    pub region: String,
    /// `ok`, `mismatched`, `unstamped`, `ungated`, `unverified` (the stamp
    /// verdicts), `unchecked` (`BUTTERFLY_SPEEDS_STAMP_CHECK=off`), `error`
    /// (the table footer or the identity file could not be read), or
    /// `failed` (the recustomization itself failed), or `disabled`
    /// (`BUTTERFLY_BOOT_RECUSTOMIZE=off`: a staged table is deliberately
    /// ignored).
    pub status: &'static str,
    pub detail: String,
    /// Matched directed edges ÷ routable car edges, when the table was applied.
    pub matched_fraction: Option<f64>,
}

/// `/health`'s summary over every region's report: the first non-`ok`
/// report (the one an operator must act on), else the first; `none` when no
/// table was staged.
pub fn health_summary(reports: &[SpeedsReport]) -> (&'static str, Option<String>, Option<f64>) {
    match reports
        .iter()
        .find(|r| r.status != "ok")
        .or_else(|| reports.first())
    {
        None => ("none", None, None),
        Some(r) => (
            r.status,
            Some(format!("{}: {}", r.region, r.detail)),
            r.matched_fraction,
        ),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn health_summary_reports_the_first_problem() {
        let r = |region: &str, status: &'static str| SpeedsReport {
            region: region.into(),
            status,
            detail: "d".into(),
            matched_fraction: (status == "ok").then_some(0.9),
        };
        assert_eq!(health_summary(&[]).0, "none");
        assert_eq!(
            health_summary(&[r("A", "ok")]),
            ("ok", Some("A: d".into()), Some(0.9))
        );
        let s = health_summary(&[r("A", "ok"), r("B", "mismatched")]);
        assert_eq!((s.0, s.2), ("mismatched", None));
    }

    fn map(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
        pairs
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    fn keys() -> Vec<String> {
        DEFAULT_STAMP_KEYS.iter().map(|s| s.to_string()).collect()
    }

    const IDENTITY_TWO: &str = "sha=abc modes=car bike foot built=abc123 region=two pbf=2026-11-08 pbf_sha=aaaa lu_pbf_sha=bbbb\n";
    const IDENTITY_ONE: &str = "sha=abc modes=car bike built=abc123 region=one pbf_sha=aaaa\n";

    fn stamped(pbf: &str, lu: Option<&str>, gate: Option<&str>) -> BTreeMap<String, String> {
        let mut m = map(&[("time_scale", "1.1"), ("graph_pbf_sha", pbf)]);
        if let Some(lu) = lu {
            m.insert("graph_lu_pbf_sha".into(), lu.into());
        }
        if let Some(g) = gate {
            m.insert("gate".into(), g.into());
        }
        m
    }

    #[test]
    fn identity_parses_key_value_tokens_only() {
        let id = parse_graph_identity(IDENTITY_TWO);
        assert_eq!(id.get("pbf_sha").map(String::as_str), Some("aaaa"));
        assert_eq!(id.get("lu_pbf_sha").map(String::as_str), Some("bbbb"));
        assert_eq!(id.get("modes").map(String::as_str), Some("car"));
        assert!(!id.contains_key("bike"));
        assert!(parse_graph_identity("=x y= z").contains_key("y"));
        assert!(!parse_graph_identity("=x").contains_key(""));
    }

    #[test]
    fn ok_when_every_carried_key_matches() {
        let id = parse_graph_identity(IDENTITY_TWO);
        let c = verify_stamp(
            Some(&id),
            &stamped("aaaa", Some("bbbb"), Some("passed")),
            &keys(),
        );
        assert_eq!(c.verdict, StampVerdict::Ok, "{}", c.detail);
        assert!(c.verdict.applies());
    }

    #[test]
    fn superset_table_on_a_one_source_graph_is_ok() {
        // The graph carries no lu_pbf_sha: that key is not compared.
        let id = parse_graph_identity(IDENTITY_ONE);
        let c = verify_stamp(
            Some(&id),
            &stamped("aaaa", Some("bbbb"), Some("passed")),
            &keys(),
        );
        assert_eq!(c.verdict, StampVerdict::Ok, "{}", c.detail);
    }

    #[test]
    fn mismatch_on_a_different_source() {
        let id = parse_graph_identity(IDENTITY_TWO);
        let c = verify_stamp(
            Some(&id),
            &stamped("ffff", Some("bbbb"), Some("passed")),
            &keys(),
        );
        assert_eq!(c.verdict, StampVerdict::Mismatched);
        assert!(!c.verdict.applies());
        // Both identities are named.
        assert!(c.detail.contains("pbf_sha=aaaa"), "{}", c.detail);
        assert!(c.detail.contains("graph_pbf_sha=ffff"), "{}", c.detail);
    }

    #[test]
    fn mismatch_when_the_table_lacks_a_key_the_graph_carries() {
        // Two-source graph, one-source table: the second source is uncalibrated.
        let id = parse_graph_identity(IDENTITY_TWO);
        let c = verify_stamp(Some(&id), &stamped("aaaa", None, Some("passed")), &keys());
        assert_eq!(c.verdict, StampVerdict::Mismatched);
        assert!(
            c.detail.contains("graph_lu_pbf_sha=<absent>"),
            "{}",
            c.detail
        );
    }

    #[test]
    fn unstamped_table_is_refused() {
        let id = parse_graph_identity(IDENTITY_TWO);
        let kv = map(&[("time_scale", "1.1"), ("gate", "passed")]);
        let c = verify_stamp(Some(&id), &kv, &keys());
        assert_eq!(c.verdict, StampVerdict::Unstamped);
        assert!(!c.verdict.applies());
        // Unstamped wins over a missing identity: the table is the defect.
        assert_eq!(
            verify_stamp(None, &BTreeMap::new(), &keys()).verdict,
            StampVerdict::Unstamped
        );
    }

    #[test]
    fn no_gate_or_failed_gate_is_refused() {
        let id = parse_graph_identity(IDENTITY_TWO);
        for gate in [None, Some("failed"), Some("")] {
            let c = verify_stamp(Some(&id), &stamped("aaaa", Some("bbbb"), gate), &keys());
            assert_eq!(c.verdict, StampVerdict::Ungated, "gate {gate:?}");
            assert!(!c.verdict.applies());
        }
    }

    #[test]
    fn unknown_graph_identity_is_unverified() {
        let kv = stamped("aaaa", Some("bbbb"), Some("passed"));
        let c = verify_stamp(None, &kv, &keys());
        assert_eq!(c.verdict, StampVerdict::Unverified);
        assert!(c.detail.contains(ENV_IDENTITY_FILE), "{}", c.detail);
        // An identity that carries none of the compared keys proves nothing.
        let c = verify_stamp(Some(&map(&[("region", "x")])), &kv, &keys());
        assert_eq!(c.verdict, StampVerdict::Unverified);
    }

    #[test]
    fn custom_keys() {
        let id = map(&[("graph_hash", "h1")]);
        let kv = map(&[("graph_graph_hash", "h1"), ("gate", "passed")]);
        let k = vec!["graph_hash".to_string()];
        assert_eq!(verify_stamp(Some(&id), &kv, &k).verdict, StampVerdict::Ok);
    }

    #[test]
    fn reads_the_stamp_from_a_real_parquet_footer() {
        use arrow::array::{Float32Array, Int64Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use parquet::file::metadata::KeyValue;
        use parquet::file::properties::WriterProperties;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let table = dir.path().join("edge_speeds.parquet");
        let schema = Arc::new(Schema::new(vec![
            Field::new("osm_node_from", DataType::Int64, false),
            Field::new("osm_node_to", DataType::Int64, false),
            Field::new("speed_ratio", DataType::Float32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1i64])),
                Arc::new(Int64Array::from(vec![2i64])),
                Arc::new(Float32Array::from(vec![0.8f32])),
            ],
        )
        .unwrap();
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![
                KeyValue::new("graph_pbf_sha".into(), "aaaa".to_string()),
                KeyValue::new("graph_lu_pbf_sha".into(), "bbbb".to_string()),
                KeyValue::new("gate".into(), "passed".to_string()),
            ]))
            .build();
        let mut w =
            ArrowWriter::try_new(std::fs::File::create(&table).unwrap(), schema, Some(props))
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let kv = read_table_kv(&table).unwrap();
        let ident = dir.path().join("identity");
        std::fs::write(&ident, IDENTITY_TWO).unwrap();
        let id = read_graph_identity(&ident).unwrap();
        assert_eq!(
            verify_stamp(Some(&id), &kv, &keys()).verdict,
            StampVerdict::Ok
        );
        std::fs::write(&ident, IDENTITY_TWO.replace("aaaa", "cccc")).unwrap();
        let id = read_graph_identity(&ident).unwrap();
        assert_eq!(
            verify_stamp(Some(&id), &kv, &keys()).verdict,
            StampVerdict::Mismatched
        );
        assert!(read_graph_identity(&dir.path().join("missing")).is_err());
    }
}
