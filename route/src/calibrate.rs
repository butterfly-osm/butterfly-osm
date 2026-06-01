//! Offline traffic-profile calibration (#388).
//!
//! Fits the per-[`DensityClass`] speed factors of a `traffic/*.traffic.json`
//! profile from an observed-drive-times dataset, replacing the hand-picked
//! multipliers of #386/#392 with learned numbers. The routing engine stays
//! deterministic: this tool runs **outside** the step1-8 pipeline, consumes
//! probe data plus the same `way_attrs.<mode>.bin` step 2 already produced,
//! and emits a profile that step 8's `--traffic` flag (#84) picks up
//! unchanged. Calibration is a swappable artifact, not a pipeline stage.
//!
//! ## What this tool deliberately does NOT do
//!
//! - **It does not choose or fetch the observed-speed dataset.** The data
//!   source (Sirius CDIS / TomTom Speed Profiles / INRIX / probe-vehicle
//!   traces / …) is a licensing + coverage decision that belongs to the
//!   operator — issue #388 open-question #1, "the real prerequisite". This
//!   engine is *source-independent*: it takes whatever `(way_id,
//!   observed_avg_speed_kmh, sample_count)` table the operator produces.
//! - **It does not resolve non-OSM segment identifiers.** The MVP adapter
//!   assumes `segment_identifier == OSM way_id`. OpenLR codes or
//!   polyline-matches need their own per-source resolver upstream of this
//!   tool (#388 "Resolve via a small adapter per source").
//!
//! ## Fitting (density-only variant of the #388 spec)
//!
//! The shipped [`TrafficProfile`] schema is five density factors with no
//! highway dimension (see #392 — Belgium ships one baked friction profile).
//! Each way already carries its base (legal-limit) speed in
//! `way_attrs.base_speed_mmps`, which encodes the highway class. So the right
//! per-density quantity is the **ratio** `observed_kmh / base_kmh`, aggregated
//! across every observed way in that density class — aggregating the *ratio*
//! rather than the raw speed normalises out the highway mix inside a class
//! (a class is a blend of highways with different base speeds; raw-speed
//! medians would be dominated by whichever highway is most sampled).
//!
//! Per class we take the sample-count-weighted median of the ratios, clamp it
//! to a sanity band (default `[0.30, 1.20]`, always within the schema's hard
//! `[0.1, 1.5]`), and fall back to the **global** weighted median for
//! under-sampled classes so all five keys are always emitted.

use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, Result, bail, ensure};

use crate::density::DensityClass;
use crate::formats::way_attrs;
use crate::traffic::{MAX_FACTOR, MIN_FACTOR, TrafficProfile};

/// One observed-speed record. `sample_count` is the per-segment observation
/// volume (probe count, trace count, …); sources without it should emit `1`.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Observation {
    pub way_id: i64,
    pub observed_kmh: f32,
    pub sample_count: u32,
}

/// Tunables for the fit.
#[derive(Debug, Clone)]
pub struct CalibrationParams {
    /// `name` written into the emitted profile JSON.
    pub name: String,
    /// `base_model` written into the emitted profile JSON.
    pub base_model: String,
    /// Minimum number of joined observation rows a density class needs before
    /// we trust its own median; below this it inherits the global median.
    pub min_samples: usize,
    /// Sanity clamp applied to every emitted factor. Must lie within the
    /// profile schema's hard bounds `[MIN_FACTOR, MAX_FACTOR]`.
    pub clamp_min: f32,
    pub clamp_max: f32,
}

impl Default for CalibrationParams {
    fn default() -> Self {
        Self {
            name: "calibrated".to_string(),
            base_model: "car".to_string(),
            min_samples: 100,
            clamp_min: 0.30,
            clamp_max: 1.20,
        }
    }
}

impl CalibrationParams {
    fn validate(&self) -> Result<()> {
        ensure!(
            !self.name.trim().is_empty(),
            "calibration: --name must be non-empty"
        );
        ensure!(
            !self.base_model.trim().is_empty(),
            "calibration: --base-model must be non-empty"
        );
        ensure!(
            self.clamp_min.is_finite() && self.clamp_max.is_finite(),
            "calibration: clamp bounds must be finite"
        );
        ensure!(
            self.clamp_min <= self.clamp_max,
            "calibration: --clamp-min ({}) must be <= --clamp-max ({})",
            self.clamp_min,
            self.clamp_max
        );
        ensure!(
            (MIN_FACTOR..=MAX_FACTOR).contains(&self.clamp_min)
                && (MIN_FACTOR..=MAX_FACTOR).contains(&self.clamp_max),
            "calibration: clamp band [{}, {}] must lie within the profile schema bounds [{}, {}]",
            self.clamp_min,
            self.clamp_max,
            MIN_FACTOR,
            MAX_FACTOR
        );
        Ok(())
    }
}

/// Per-class diagnostic, for the CLI summary and for tests.
#[derive(Debug, Clone)]
pub struct ClassFit {
    pub class: DensityClass,
    /// Joined observation rows landing in this class.
    pub n_obs: usize,
    /// Sum of `sample_count` over those rows.
    pub total_samples: u64,
    /// Weighted-median ratio from this class's own observations, before
    /// clamping; `None` when the class had no joined observations.
    pub raw_factor: Option<f32>,
    /// True when the class fell back to the global median (too few samples).
    pub used_fallback: bool,
    /// Final factor written to the profile (clamped).
    pub factor: f32,
}

/// Result of a fit: the ready-to-write profile plus diagnostics.
#[derive(Debug, Clone)]
pub struct CalibrationResult {
    pub profile: TrafficProfile,
    /// One entry per density class, in [`DensityClass::ALL`] order.
    pub per_class: Vec<ClassFit>,
    /// Observation rows joined to a way in the index.
    pub matched: usize,
    /// Observation rows whose `way_id` was absent from the index.
    pub unmatched: usize,
    /// Observation rows dropped because base speed or observed speed was
    /// non-positive / non-finite (cannot form a ratio).
    pub skipped_bad: usize,
    /// Global weighted-median ratio over all matched rows — the fallback
    /// value used for under-sampled classes.
    pub global_factor: f32,
}

/// Build the `way_id -> (base_speed_kmh, density_class_u8)` lookup from a
/// loaded `way_attrs.<mode>.bin`. Base speed is converted from mm/s to km/h
/// once here (`mm/s * 3600 / 1e6 = km/h`).
pub fn index_ways(attrs: &[way_attrs::WayAttr]) -> HashMap<i64, (f32, u8)> {
    let mut map: HashMap<i64, (f32, u8)> = HashMap::with_capacity(attrs.len());
    for wa in attrs {
        let base_kmh = wa.output.base_speed_mmps as f32 * 0.003_6;
        // way_attrs is sorted by way_id and ids are unique per mode, so the
        // last-writer-wins of insert is irrelevant; keep it simple.
        map.insert(wa.way_id, (base_kmh, wa.output.density_class));
    }
    map
}

/// Lower weighted median of `(value, weight)` samples. Returns `None` for an
/// empty slice. Mutates (sorts) the slice in place.
fn weighted_median(samples: &mut [(f32, u64)]) -> Option<f32> {
    if samples.is_empty() {
        return None;
    }
    samples.sort_by(|a, b| a.0.partial_cmp(&b.0).unwrap_or(std::cmp::Ordering::Equal));
    let total: u64 = samples.iter().map(|(_, w)| *w).sum();
    if total == 0 {
        // Degenerate (all-zero weights): fall back to the unweighted median.
        return Some(samples[samples.len() / 2].0);
    }
    let half = total as f64 / 2.0;
    let mut cum: u64 = 0;
    for (val, w) in samples.iter() {
        cum += *w;
        if cum as f64 >= half {
            return Some(*val);
        }
    }
    // Unreachable in practice (cum reaches total >= half), but be safe.
    samples.last().map(|(v, _)| *v)
}

/// Pure fitting core — no I/O. Buckets observations by the density class of
/// their way, computes a sample-count-weighted median of `observed/base`
/// ratios per class, clamps, and falls back to the global median for
/// under-sampled classes.
///
/// Errors only when *nothing* joins (no way in the index matched any
/// observation, or every matched row had a non-positive base/observed speed),
/// since then there is no signal to fit and no defensible profile to emit.
pub fn fit(
    observations: &[Observation],
    way_index: &HashMap<i64, (f32, u8)>,
    params: &CalibrationParams,
) -> Result<CalibrationResult> {
    params.validate()?;

    let mut buckets: [Vec<(f32, u64)>; 5] = Default::default();
    let mut global: Vec<(f32, u64)> = Vec::new();
    let mut matched = 0usize;
    let mut unmatched = 0usize;
    let mut skipped_bad = 0usize;

    for obs in observations {
        let Some((base_kmh, density_u8)) = way_index.get(&obs.way_id).copied() else {
            unmatched += 1;
            continue;
        };
        if !base_kmh.is_finite()
            || base_kmh <= 0.0
            || !obs.observed_kmh.is_finite()
            || obs.observed_kmh <= 0.0
        {
            skipped_bad += 1;
            continue;
        }
        let ratio = obs.observed_kmh / base_kmh;
        if !ratio.is_finite() {
            skipped_bad += 1;
            continue;
        }
        let class = DensityClass::from_u8(density_u8);
        let w = obs.sample_count.max(1) as u64;
        buckets[class.to_u8() as usize].push((ratio, w));
        global.push((ratio, w));
        matched += 1;
    }

    let global_factor = weighted_median(&mut global).with_context(|| {
        format!(
            "no observations could be joined to a way: {matched} matched, {unmatched} unmatched, \
             {skipped_bad} dropped (bad base/observed speed). Check that the segment identifiers \
             are OSM way_ids and that --way-attrs / --mode matches the dataset's transport mode."
        )
    })?;

    let mut per_class: Vec<ClassFit> = Vec::with_capacity(5);
    let mut factors = [0f32; 5];
    for class in DensityClass::ALL {
        let idx = class.to_u8() as usize;
        let bucket = &mut buckets[idx];
        let n_obs = bucket.len();
        let total_samples: u64 = bucket.iter().map(|(_, w)| *w).sum();
        let raw_factor = weighted_median(bucket);
        let (unclamped, used_fallback) = match raw_factor {
            Some(r) if n_obs >= params.min_samples => (r, false),
            _ => (global_factor, true),
        };
        let factor = unclamped.clamp(params.clamp_min, params.clamp_max);
        factors[idx] = factor;
        per_class.push(ClassFit {
            class,
            n_obs,
            total_samples,
            raw_factor,
            used_fallback,
            factor,
        });
    }

    let profile = TrafficProfile {
        name: params.name.clone(),
        base_model: params.base_model.clone(),
        factors,
    };

    // Defensive guarantee: the emitted profile must satisfy the exact schema
    // step 8 validates (all five keys, each in [0.1, 1.5]). Round-tripping it
    // here turns any future drift in the clamp logic into a loud failure at
    // calibration time rather than a silent reject at customization time.
    let json = profile.to_json_string()?;
    TrafficProfile::from_json(&json)
        .context("internal error: calibrated profile failed its own schema validation")?;

    Ok(CalibrationResult {
        profile,
        per_class,
        matched,
        unmatched,
        skipped_bad,
        global_factor,
    })
}

/// Read an observed-speed table, dispatching on file extension:
/// `.parquet`/`.pq` → Parquet; `.csv`/`.tsv`/`.txt` → delimited text.
pub fn read_observations(path: &Path) -> Result<Vec<Observation>> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("")
        .to_ascii_lowercase();
    match ext.as_str() {
        "parquet" | "pq" => read_observations_parquet(path),
        "tsv" => read_observations_delimited(path, b'\t'),
        "csv" | "txt" => read_observations_delimited(path, b','),
        other => bail!(
            "unsupported observations extension '.{other}' for {}: use .parquet, .csv, or .tsv",
            path.display()
        ),
    }
}

/// Locate a column by any of the accepted aliases (case-insensitive).
fn find_col<'a, I: Iterator<Item = &'a str>>(headers: I, aliases: &[&str]) -> Option<usize> {
    headers
        .enumerate()
        .find(|(_, h)| aliases.iter().any(|a| h.eq_ignore_ascii_case(a)))
        .map(|(i, _)| i)
}

const WAY_ALIASES: &[&str] = &["way_id", "segment_identifier", "osm_way_id", "id"];
const SPEED_ALIASES: &[&str] = &[
    "observed_avg_speed_kmh",
    "observed_kmh",
    "avg_speed_kmh",
    "speed_kmh",
];
const COUNT_ALIASES: &[&str] = &["sample_count", "samples", "count", "n"];

fn read_observations_delimited(path: &Path, delimiter: u8) -> Result<Vec<Observation>> {
    let mut rdr = csv::ReaderBuilder::new()
        .delimiter(delimiter)
        .has_headers(true)
        .trim(csv::Trim::All)
        .from_path(path)
        .with_context(|| format!("opening observations file {}", path.display()))?;

    let headers = rdr
        .headers()
        .with_context(|| format!("reading header row of {}", path.display()))?
        .clone();
    let way_idx = find_col(headers.iter(), WAY_ALIASES).with_context(|| {
        format!(
            "{}: missing a way-id column (one of: {})",
            path.display(),
            WAY_ALIASES.join(", ")
        )
    })?;
    let speed_idx = find_col(headers.iter(), SPEED_ALIASES).with_context(|| {
        format!(
            "{}: missing an observed-speed column (one of: {})",
            path.display(),
            SPEED_ALIASES.join(", ")
        )
    })?;
    let count_idx = find_col(headers.iter(), COUNT_ALIASES);

    let mut out = Vec::new();
    for (i, rec) in rdr.records().enumerate() {
        let row = i + 2; // 1-based, plus header line
        let rec = rec.with_context(|| format!("{}: parse error at row {row}", path.display()))?;
        let way_id: i64 = rec
            .get(way_idx)
            .unwrap_or("")
            .parse()
            .with_context(|| format!("{}: row {row}: bad way_id", path.display()))?;
        let observed_kmh: f32 = rec
            .get(speed_idx)
            .unwrap_or("")
            .parse()
            .with_context(|| format!("{}: row {row}: bad observed speed", path.display()))?;
        let sample_count: u32 = match count_idx {
            Some(ci) => rec.get(ci).unwrap_or("").parse().unwrap_or(1),
            None => 1,
        };
        out.push(Observation {
            way_id,
            observed_kmh,
            sample_count,
        });
    }
    Ok(out)
}

fn read_observations_parquet(path: &Path) -> Result<Vec<Observation>> {
    use arrow::array::Array;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    let file = std::fs::File::open(path)
        .with_context(|| format!("opening parquet observations {}", path.display()))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .with_context(|| format!("reading parquet metadata of {}", path.display()))?;

    let schema = builder.schema().clone();
    let way_col = find_col(
        schema.fields().iter().map(|f| f.name().as_str()),
        WAY_ALIASES,
    )
    .with_context(|| {
        format!(
            "{}: missing a way-id column (one of: {})",
            path.display(),
            WAY_ALIASES.join(", ")
        )
    })?;
    let speed_col = find_col(
        schema.fields().iter().map(|f| f.name().as_str()),
        SPEED_ALIASES,
    )
    .with_context(|| {
        format!(
            "{}: missing an observed-speed column (one of: {})",
            path.display(),
            SPEED_ALIASES.join(", ")
        )
    })?;
    let count_col = find_col(
        schema.fields().iter().map(|f| f.name().as_str()),
        COUNT_ALIASES,
    );

    let reader = builder
        .build()
        .with_context(|| format!("building parquet reader for {}", path.display()))?;

    let mut out = Vec::new();
    for batch in reader {
        let batch =
            batch.with_context(|| format!("reading a parquet batch from {}", path.display()))?;
        let n = batch.num_rows();
        let way_arr = batch.column(way_col).as_ref();
        let speed_arr = batch.column(speed_col).as_ref();
        let count_arr = count_col.map(|c| batch.column(c).clone());
        for row in 0..n {
            if way_arr.is_null(row) || speed_arr.is_null(row) {
                continue;
            }
            let way_id = arr_as_i64(way_arr, row)
                .with_context(|| format!("{}: way-id column", path.display()))?;
            let observed_kmh = arr_as_f32(speed_arr, row)
                .with_context(|| format!("{}: observed-speed column", path.display()))?;
            let sample_count = match &count_arr {
                Some(a) if !a.is_null(row) => arr_as_u32(a.as_ref(), row).unwrap_or(1),
                _ => 1,
            };
            out.push(Observation {
                way_id,
                observed_kmh,
                sample_count,
            });
        }
    }
    Ok(out)
}

// --- Arrow numeric extraction. DuckDB's `COPY ... TO '*.parquet'` writes
// BIGINT->Int64, INTEGER->Int32, DOUBLE->Float64, FLOAT->Float32, etc.; we
// accept the integer and float widths that any reasonable producer emits and
// reject anything else loudly.

fn arr_as_i64(col: &dyn arrow::array::Array, row: usize) -> Result<i64> {
    use arrow::array::*;
    if let Some(a) = col.as_any().downcast_ref::<Int64Array>() {
        return Ok(a.value(row));
    }
    if let Some(a) = col.as_any().downcast_ref::<Int32Array>() {
        return Ok(a.value(row) as i64);
    }
    if let Some(a) = col.as_any().downcast_ref::<UInt64Array>() {
        return Ok(a.value(row) as i64);
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

fn arr_as_u32(col: &dyn arrow::array::Array, row: usize) -> Result<u32> {
    let v = arr_as_i64(col, row)?;
    Ok(v.clamp(0, u32::MAX as i64) as u32)
}

/// File-driven convenience wrapper: load `way_attrs.<mode>.bin`, read the
/// observations table, and fit. Used by the `calibrate-traffic` CLI command.
pub fn run_calibration(
    observations_path: &Path,
    way_attrs_path: &Path,
    params: &CalibrationParams,
) -> Result<CalibrationResult> {
    let attrs = way_attrs::read_all(way_attrs_path)
        .with_context(|| format!("reading way attributes {}", way_attrs_path.display()))?;
    ensure!(
        !attrs.is_empty(),
        "way_attrs file {} is empty — wrong path or unbuilt step 2?",
        way_attrs_path.display()
    );
    let index = index_ways(&attrs);

    let observations = read_observations(observations_path)?;
    ensure!(
        !observations.is_empty(),
        "observations file {} produced no rows",
        observations_path.display()
    );

    fit(&observations, &index, params)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;

    /// Build a way index from `(way_id, base_kmh, class)` triples.
    fn idx(rows: &[(i64, f32, DensityClass)]) -> HashMap<i64, (f32, u8)> {
        rows.iter()
            .map(|(id, base, c)| (*id, (*base, c.to_u8())))
            .collect()
    }

    fn obs(way_id: i64, kmh: f32, n: u32) -> Observation {
        Observation {
            way_id,
            observed_kmh: kmh,
            sample_count: n,
        }
    }

    #[test]
    fn weighted_median_picks_the_weight_crossing_point() {
        let mut s = vec![(1.0f32, 1u64), (2.0, 1), (3.0, 1)];
        assert_eq!(weighted_median(&mut s), Some(2.0));
        // Heavy weight on the low value drags the median down.
        let mut s = vec![(0.5f32, 100u64), (5.0, 1)];
        assert_eq!(weighted_median(&mut s), Some(0.5));
        assert_eq!(weighted_median(&mut Vec::new()), None);
    }

    #[test]
    fn fit_recovers_known_per_class_ratios() {
        // Two density classes, each way base 100 km/h. urban_high observed at
        // 50 (ratio 0.5), rural observed at 95 (ratio 0.95).
        let index = idx(&[
            (1, 100.0, DensityClass::UrbanHigh),
            (2, 100.0, DensityClass::Rural),
        ]);
        let mut observations = Vec::new();
        for _ in 0..150 {
            observations.push(obs(1, 50.0, 1)); // urban_high → 0.50
            observations.push(obs(2, 95.0, 1)); // rural → 0.95
        }
        let res = fit(&observations, &index, &CalibrationParams::default()).unwrap();
        assert_eq!(res.matched, 300);
        assert_eq!(res.unmatched, 0);
        let f = &res.profile.factors;
        assert!(
            (f[DensityClass::UrbanHigh.to_u8() as usize] - 0.50).abs() < 1e-4,
            "{f:?}"
        );
        assert!(
            (f[DensityClass::Rural.to_u8() as usize] - 0.95).abs() < 1e-4,
            "{f:?}"
        );
        // The three classes with no observations fall back to the global
        // median (a blend of 0.5 and 0.95).
        for c in [
            DensityClass::UrbanMedium,
            DensityClass::UrbanLow,
            DensityClass::Suburban,
        ] {
            let fit = res.per_class.iter().find(|p| p.class == c).unwrap();
            assert!(fit.used_fallback, "{c:?} should use fallback");
            assert_eq!(fit.n_obs, 0);
        }
    }

    #[test]
    fn fit_undersampled_class_falls_back_to_global() {
        // urban_high well-sampled at 0.5; rural sampled only twice (< 100).
        let index = idx(&[
            (1, 100.0, DensityClass::UrbanHigh),
            (2, 100.0, DensityClass::Rural),
        ]);
        let mut observations = vec![obs(2, 95.0, 1), obs(2, 95.0, 1)];
        for _ in 0..200 {
            observations.push(obs(1, 50.0, 1));
        }
        let res = fit(&observations, &index, &CalibrationParams::default()).unwrap();
        let rural = res
            .per_class
            .iter()
            .find(|p| p.class == DensityClass::Rural)
            .unwrap();
        assert!(rural.used_fallback);
        // Global median is dominated by the 200 urban_high rows → ~0.5.
        assert!((rural.factor - 0.50).abs() < 1e-4, "{rural:?}");
    }

    #[test]
    fn fit_clamps_outliers_into_the_sanity_band() {
        let p = CalibrationParams {
            min_samples: 1,
            ..Default::default()
        };
        // base 100; observed 500 → ratio 5.0 (clamp to 1.20); observed 2 →
        // ratio 0.02 (clamp to 0.30).
        let index = idx(&[
            (1, 100.0, DensityClass::UrbanHigh),
            (2, 100.0, DensityClass::Rural),
        ]);
        let observations = vec![obs(1, 500.0, 10), obs(2, 2.0, 10)];
        let res = fit(&observations, &index, &p).unwrap();
        assert_eq!(
            res.profile.factors[DensityClass::UrbanHigh.to_u8() as usize],
            1.20
        );
        assert_eq!(
            res.profile.factors[DensityClass::Rural.to_u8() as usize],
            0.30
        );
    }

    #[test]
    fn fit_counts_unmatched_and_does_not_crash() {
        let index = idx(&[(1, 100.0, DensityClass::Suburban)]);
        let observations = vec![obs(1, 80.0, 5), obs(999, 80.0, 5), obs(998, 80.0, 5)];
        let p = CalibrationParams {
            min_samples: 1,
            ..Default::default()
        };
        let res = fit(&observations, &index, &p).unwrap();
        assert_eq!(res.matched, 1);
        assert_eq!(res.unmatched, 2);
    }

    #[test]
    fn fit_errors_when_nothing_joins() {
        let index = idx(&[(1, 100.0, DensityClass::Suburban)]);
        let observations = vec![obs(42, 80.0, 1)]; // no matching way
        assert!(fit(&observations, &index, &CalibrationParams::default()).is_err());
    }

    #[test]
    fn fit_drops_nonpositive_base_or_observed() {
        let index = idx(&[
            (1, 0.0, DensityClass::Suburban),   // zero base → skipped
            (2, 100.0, DensityClass::Suburban), // valid
        ]);
        let observations = vec![obs(1, 80.0, 1), obs(2, 80.0, 1), obs(2, -5.0, 1)];
        let p = CalibrationParams {
            min_samples: 1,
            ..Default::default()
        };
        let res = fit(&observations, &index, &p).unwrap();
        assert_eq!(res.matched, 1); // only way 2 @ 80
        assert_eq!(res.skipped_bad, 2); // zero-base row + negative-speed row
    }

    #[test]
    fn emitted_profile_passes_schema_validation() {
        let index = idx(&[(1, 100.0, DensityClass::UrbanHigh)]);
        let observations: Vec<_> = (0..200).map(|_| obs(1, 60.0, 1)).collect();
        let res = fit(&observations, &index, &CalibrationParams::default()).unwrap();
        let json = res.profile.to_json_string().unwrap();
        let reparsed = TrafficProfile::from_json(&json).unwrap();
        assert_eq!(reparsed, res.profile);
        // All five keys present in the serialized form.
        for c in DensityClass::ALL {
            assert!(
                json.contains(c.as_str()),
                "missing {} in {json}",
                c.as_str()
            );
        }
    }

    #[test]
    fn rejects_clamp_band_outside_schema_bounds() {
        let index = idx(&[(1, 100.0, DensityClass::UrbanHigh)]);
        let observations = vec![obs(1, 60.0, 1)];
        let bad = CalibrationParams {
            clamp_max: 2.0, // > MAX_FACTOR (1.5)
            min_samples: 1,
            ..Default::default()
        };
        assert!(fit(&observations, &index, &bad).is_err());
    }

    #[test]
    fn csv_reader_parses_aliases_and_defaults_count() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obs.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        // Use the segment_identifier alias and omit sample_count.
        writeln!(f, "segment_identifier,observed_avg_speed_kmh").unwrap();
        writeln!(f, "100,42.5").unwrap();
        writeln!(f, "200,30").unwrap();
        f.flush().unwrap();

        let obs = read_observations(&path).unwrap();
        assert_eq!(obs.len(), 2);
        assert_eq!(obs[0].way_id, 100);
        assert!((obs[0].observed_kmh - 42.5).abs() < 1e-6);
        assert_eq!(obs[0].sample_count, 1); // defaulted
        assert_eq!(obs[1].way_id, 200);
    }

    #[test]
    fn csv_reader_reads_sample_count_when_present() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obs.csv");
        let mut f = std::fs::File::create(&path).unwrap();
        writeln!(f, "way_id,observed_avg_speed_kmh,sample_count").unwrap();
        writeln!(f, "7,55.0,33").unwrap();
        f.flush().unwrap();

        let obs = read_observations(&path).unwrap();
        assert_eq!(obs.len(), 1);
        assert_eq!(obs[0].sample_count, 33);
    }

    #[test]
    fn parquet_round_trip_reads_observations() {
        use arrow::array::{Float64Array, Int64Array, UInt32Array};
        use arrow::datatypes::{DataType, Field, Schema};
        use arrow::record_batch::RecordBatch;
        use parquet::arrow::ArrowWriter;
        use std::sync::Arc;

        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("obs.parquet");

        let schema = Arc::new(Schema::new(vec![
            // Use the segment_identifier alias to exercise alias resolution
            // through the parquet path too.
            Field::new("segment_identifier", DataType::Int64, false),
            Field::new("observed_avg_speed_kmh", DataType::Float64, false),
            Field::new("sample_count", DataType::UInt32, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![100i64, 200, 300])),
                Arc::new(Float64Array::from(vec![42.5f64, 30.0, 88.0])),
                Arc::new(UInt32Array::from(vec![5u32, 7, 1])),
            ],
        )
        .unwrap();

        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let obs = read_observations(&path).unwrap();
        assert_eq!(obs.len(), 3);
        assert_eq!(obs[0].way_id, 100);
        assert!((obs[0].observed_kmh - 42.5).abs() < 1e-4);
        assert_eq!(obs[0].sample_count, 5);
        assert_eq!(obs[2].way_id, 300);
        assert_eq!(obs[2].sample_count, 1);
    }

    /// End-to-end through the production `way_attrs.<mode>.bin` writer +
    /// reader and the CSV adapter: proves `run_calibration` joins observed
    /// speeds to real on-disk way attributes and recovers the planted ratio.
    #[test]
    fn run_calibration_end_to_end_on_real_way_attrs_format() {
        use crate::formats::way_attrs::{self as wa, WayAttr};
        use crate::profile_abi::{Mode, WayOutput};

        let dir = tempfile::tempdir().unwrap();

        // 150 urban_high ways @ base 50 km/h (13888 mm/s) and 150 rural ways
        // @ base 120 km/h (33333 mm/s).
        let base_urban_mmps = (50.0_f32 / 0.003_6) as u32; // ~13888
        let base_rural_mmps = (120.0_f32 / 0.003_6) as u32; // ~33333
        let mut attrs: Vec<WayAttr> = Vec::new();
        for id in 0..150i64 {
            attrs.push(WayAttr {
                way_id: id,
                output: WayOutput {
                    base_speed_mmps: base_urban_mmps,
                    density_class: DensityClass::UrbanHigh.to_u8(),
                    ..Default::default()
                },
            });
        }
        for id in 1000..1150i64 {
            attrs.push(WayAttr {
                way_id: id,
                output: WayOutput {
                    base_speed_mmps: base_rural_mmps,
                    density_class: DensityClass::Rural.to_u8(),
                    ..Default::default()
                },
            });
        }
        let wa_path = dir.path().join("way_attrs.car.bin");
        wa::write(&wa_path, Mode(0), &attrs, &[0u8; 32], &[0u8; 32]).unwrap();

        // Observations: urban observed at half the base (ratio 0.5), rural at
        // base (ratio 1.0 → clamps under MAX 1.20, stays 1.0).
        let obs_path = dir.path().join("obs.csv");
        let mut f = std::fs::File::create(&obs_path).unwrap();
        writeln!(f, "way_id,observed_avg_speed_kmh,sample_count").unwrap();
        for id in 0..150i64 {
            writeln!(f, "{id},25.0,1").unwrap(); // 25 / 50 = 0.5
        }
        for id in 1000..1150i64 {
            writeln!(f, "{id},120.0,1").unwrap(); // 120 / 120 = 1.0
        }
        f.flush().unwrap();

        let res = run_calibration(&obs_path, &wa_path, &CalibrationParams::default()).unwrap();
        assert_eq!(res.matched, 300);
        assert_eq!(res.unmatched, 0);
        let f = &res.profile.factors;
        assert!(
            (f[DensityClass::UrbanHigh.to_u8() as usize] - 0.50).abs() < 1e-3,
            "{f:?}"
        );
        assert!(
            (f[DensityClass::Rural.to_u8() as usize] - 1.00).abs() < 1e-3,
            "{f:?}"
        );
        // Profile is schema-valid and writeable.
        TrafficProfile::from_json(&res.profile.to_json_string().unwrap()).unwrap();
    }
}
