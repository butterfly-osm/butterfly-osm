//! The directed per-edge speeds contract (#450/#454) and its readers.
//!
//! The engine's ONE runtime calibration input is a generic
//! `edge_speeds.parquet`: rows keyed by the directed OSM node pair
//! `(osm_node_from, osm_node_to)` — the same keys `edges_batch` emits —
//! carrying either an absolute speed or a base-speed ratio, plus the
//! optional best/worst band columns and the optional `time_scale*` level
//! anchors in the file's parquet key-value metadata. Serve boot reads it and
//! recustomizes the car CCH weights in memory
//! (`ServerState::recustomize_car_from_edge_speeds`).
//!
//! The engine is source-independent: it never sees the producer, the bucket
//! or any deploy convention — any table with the two key columns plus one
//! value column works. Choosing, licensing and matching the observed-speed
//! dataset onto these keys happens entirely upstream.
//!
//! #582 retired the earlier per-way `(way_id, observed_avg_speed_kmh,
//! sample_count)` seam and the offline profile fit that consumed it: the
//! directed table supersedes both, and nothing produced the 3-column file
//! any more.

use std::path::Path;

use anyhow::{Context, Result, bail};

/// Locate a column by any of the accepted aliases (case-insensitive).
fn find_col<'a, I: Iterator<Item = &'a str>>(headers: I, aliases: &[&str]) -> Option<usize> {
    headers
        .enumerate()
        .find(|(_, h)| aliases.iter().any(|a| h.eq_ignore_ascii_case(a)))
        .map(|(i, _)| i)
}

fn arr_as_i64(col: &dyn arrow::array::Array, row: usize) -> Result<i64> {
    use arrow::array::*;
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(a.value(row) as i64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        let v = a.value(row);
        // `as i64` would wrap a > i64::MAX value to a negative way_id; fail loud.
        if v > i64::MAX as u64 {
            bail!("node id {v} exceeds i64::MAX — not a valid OSM node id");
        }
        return Ok(v as i64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt32Array>() {
        return Ok(a.value(row) as i64);
    }
    bail!(
        "unsupported arrow type {:?} for an integer field",
        col.data_type()
    );
}

fn arr_as_f32(col: &dyn arrow::array::Array, row: usize) -> Result<f32> {
    use arrow::array::*;
    if let Some(a) = col.as_any().downcast_ref::<Float64Array>() {
        return Ok(a.value(row) as f32);
    }
    if let Some(a) = col.as_any().downcast_ref::<Float32Array>() {
        return Ok(a.value(row));
    }
    // Tolerate integer-typed speed columns.
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.value(row) as f32);
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(a.value(row) as f32);
    }
    bail!(
        "unsupported arrow type {:?} for a float field",
        col.data_type()
    );
}

/// One directed junction-edge adjustment: the edge from OSM node `from` to
/// OSM node `to` (the same keys `edges_batch` emits), carrying EXACTLY ONE
/// of:
/// - `speed_kmh`: an absolute observed/derived mean speed, or
/// - `ratio`: a multiplicative factor on the edge's own base speed
///   (congested/free, in (0.05, 1.5]) — preferred when the producer knows
///   relative congestion but not the edge's legal/base speed (e.g. a
///   volume-delay function over assignment flows, #467).
///
/// The producer is irrelevant to the engine — any table with the two key
/// columns plus one value column works.
#[derive(Debug, Clone, Copy)]
pub struct EdgeSpeed {
    pub from: i64,
    pub to: i64,
    pub speed_kmh: Option<f32>,
    pub ratio: Option<f32>,
    /// #521 bands, named profiles since 2026-09-03 (SPEED domain): worst =
    /// weekday peaks, best = nights / free-flow. Optional columns.
    pub worst: Option<f32>,
    pub best: Option<f32>,
}

const FROM_ALIASES: &[&str] = &["osm_node_from", "node_from", "from", "u"];
const TO_ALIASES: &[&str] = &["osm_node_to", "node_to", "to", "v"];
const EDGE_SPEED_ALIASES: &[&str] = &[
    "speed_kmh",
    "speed",
    "avg_speed_kmh",
    "observed_avg_speed_kmh",
];
const EDGE_RATIO_ALIASES: &[&str] = &["speed_ratio", "ratio", "congestion_factor"];

/// Read the generic per-edge speeds parquet:
/// `(osm_node_from i64, osm_node_to i64, speed_kmh double)`.
/// Rows with nulls or non-positive/absurd speeds (outside (0, 200] km/h)
/// are skipped with a tally — a malformed table degrades, never aborts.
/// #524: optional global end-to-end time scale carried in the table's
/// parquet key-value metadata (`time_scale`). Applied by the recustomizer to
/// LINK weights AND turn penalties, so producer-measured level anchors
/// propagate exactly (scaling only ratios reaches ~55% per pass and warps
/// structure — the #481 transform leaves turns unscaled). Generic contract:
/// any producer may set it; absent -> 1.0.
pub fn read_time_scale(path: &Path) -> Result<Option<f64>> {
    read_time_scale_key(path, "time_scale")
}

/// Per-profile level (2026-09-03): the bands carry their own anchors —
/// `time_scale_best` / `time_scale_worst` KV keys, each measured against its
/// own time-stamped reference set. Absent key → `None` (callers fall back to
/// the typical `time_scale`).
pub fn read_time_scale_key(path: &Path, key: &str) -> Result<Option<f64>> {
    use parquet::file::reader::FileReader;
    let f = std::fs::File::open(path)?;
    let r = parquet::file::serialized_reader::SerializedFileReader::new(f)?;
    let md = r.metadata().file_metadata();
    if let Some(kvs) = md.key_value_metadata() {
        for kv in kvs {
            if kv.key == key
                && let Some(v) = kv.value.as_ref().and_then(|v| v.parse::<f64>().ok())
            {
                anyhow::ensure!(
                    (0.5..=2.0).contains(&v),
                    "{key} {v} outside sanity range [0.5, 2.0]"
                );
                return Ok(Some(v));
            }
        }
    }
    Ok(None)
}

/// #521: cheap schema-only probe — does the table carry the optional
/// band columns (`speed_ratio_best` + `speed_ratio_worst`, or the legacy q75/q25 pair)?
pub fn edge_table_has_bands(path: &Path) -> Result<bool> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    let file = std::fs::File::open(path)?;
    let b = ParquetRecordBatchReaderBuilder::try_new(file)?;
    let names: Vec<&str> = b
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().as_str())
        .collect();
    let has = |a: &str, b: &str| names.contains(&a) || names.contains(&b);
    Ok(has("speed_ratio_worst", "speed_ratio_q25") && has("speed_ratio_best", "speed_ratio_q75"))
}

pub fn read_edge_speeds(path: &Path) -> Result<Vec<EdgeSpeed>> {
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)
        .with_context(|| format!("opening parquet edge speeds {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading parquet metadata of {}", path.display()))?;
    let schema = builder.schema().clone();
    let names = || schema.fields().iter().map(|f| f.name().as_str());
    let from_col = find_col(names(), FROM_ALIASES).with_context(|| {
        format!(
            "{}: missing a from-node column (one of: {})",
            path.display(),
            FROM_ALIASES.join(", ")
        )
    })?;
    let to_col = find_col(names(), TO_ALIASES).with_context(|| {
        format!(
            "{}: missing a to-node column (one of: {})",
            path.display(),
            TO_ALIASES.join(", ")
        )
    })?;
    let speed_col = find_col(names(), EDGE_SPEED_ALIASES);
    let ratio_col = find_col(names(), EDGE_RATIO_ALIASES);
    // Named profiles (2026-09-03): worst = weekday peaks, best = nights /
    // free-flow. The pre-rename artefacts carried the same two profiles as
    // diurnal quantiles `speed_ratio_q25` (congested) / `speed_ratio_q75`
    // (fluid) — still accepted so a fleet can roll one side at a time.
    let worst_col = find_col(names(), &["speed_ratio_worst", "speed_ratio_q25"]);
    let best_col = find_col(names(), &["speed_ratio_best", "speed_ratio_q75"]);
    anyhow::ensure!(
        speed_col.is_some() != ratio_col.is_some(),
        "{}: need EXACTLY ONE of a speed column ({}) or a ratio column ({})",
        path.display(),
        EDGE_SPEED_ALIASES.join(", "),
        EDGE_RATIO_ALIASES.join(", ")
    );

    let reader = builder
        .build()
        .with_context(|| format!("building parquet reader for {}", path.display()))?;
    let mut out = Vec::new();
    let mut skipped = 0usize;
    for batch in reader {
        let batch =
            batch.with_context(|| format!("reading a parquet batch from {}", path.display()))?;
        let from_arr = batch.column(from_col).as_ref();
        let to_arr = batch.column(to_col).as_ref();
        let val_idx = speed_col.or(ratio_col).expect("ensured above");
        let val_arr = batch.column(val_idx).as_ref();
        let is_ratio = ratio_col.is_some();
        for row in 0..batch.num_rows() {
            if from_arr.is_null(row) || to_arr.is_null(row) || val_arr.is_null(row) {
                skipped += 1;
                continue;
            }
            let from = arr_as_i64(from_arr, row)?;
            let to = arr_as_i64(to_arr, row)?;
            let v = arr_as_f32(val_arr, row)?;
            let ok = if is_ratio {
                v.is_finite() && v > 0.05 && v <= 1.5
            } else {
                v.is_finite() && v > 0.0 && v <= 200.0
            };
            if !ok {
                skipped += 1;
                continue;
            }
            let band = |col: Option<usize>| -> Option<f32> {
                let i = col?;
                let a = batch.column(i).as_ref();
                if a.is_null(row) {
                    return None;
                }
                let v = arr_as_f32(a, row).ok()?;
                (v.is_finite() && v > 0.04 && v <= 1.5).then_some(v)
            };
            out.push(EdgeSpeed {
                from,
                to,
                speed_kmh: if is_ratio { None } else { Some(v) },
                ratio: if is_ratio { Some(v) } else { None },
                worst: band(worst_col),
                best: band(best_col),
            });
        }
    }
    if skipped > 0 {
        tracing::warn!(skipped, "edge speeds: skipped null/out-of-range rows");
    }
    Ok(out)
}
