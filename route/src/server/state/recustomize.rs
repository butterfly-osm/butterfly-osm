//! Serve-boot car recustomization (#433 per-way, #450/#454 per-edge, #521
//! uncertainty bands) and its on-disk weight cache (#444).
//!
//! Split out of `state.rs` by #552, which also made the three per-edge passes
//! (typical / best / worst) share ONE preparation of their inputs and ONE
//! cache file:
//!
//! - [`EdgeRecustomizePrep`] holds everything the three passes have in
//!   common — the parquet CRC that keys the cache, the per-column #524 level
//!   anchors, and (only if some pass actually misses the cache) the parsed
//!   rows, the directed lookup, the turn table, the expected outgoing turn
//!   charge and the filtered EBG. Each of those used to be re-read and
//!   rebuilt once per pass.
//! - The cache is one file with one key and three CRC-guarded sections, so a
//!   single section can be recomputed on its own without invalidating the
//!   other two.
//!
//! Cache-tag rule (#444, learned the hard way on #528): a stale cache HIT
//! serves the OLD derivation and is a prod-only regression — BUMP
//! [`RECUSTOMIZE_EDGE_ALGO_TAG`] on ANY change to the mapping, the
//! customization, or the weights derived from them.

use anyhow::Result;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use super::{ModeData, ModeSlot, ServerState, clone_mode_data, refresh_len_along_time};
use crate::formats::{CchWeights, WeightArray, WeightWidth};
use crate::matrix::bucket_ch::{DownAdjFlat, DownReverseAdjFlat, UpAdjFlat};
use crate::profile_abi::Mode;

// =====================================================================
// Cache algorithm tags (#444)
// =====================================================================

/// Per-way (#433) cache algo version — bump on ANY change to the
/// calibration or customization algorithm so stale caches self-invalidate.
/// v2: #552 (one-file section format + narrowed weight storage).
const RECUSTOMIZE_CACHE_VERSION: &[u8] = b"recustomize-car-v2";

/// Per-edge (#450/#454/#521) cache algo version. BUMP THIS TAG whenever the
/// served weights OR the len-along-time derivation changes: a stale cache
/// serves the OLD derivation on HIT (prod's v3 cache-hit served pre-#528
/// stale lat while the fresh miss-path was correct — that is exactly this
/// class of miss). v6: #552 (one file / one key / three sections, parquet
/// CRC instead of the raw bytes, narrowed weight storage).
const RECUSTOMIZE_EDGE_ALGO_TAG: &[u8] = b"recustomize-car-edge-v6";

/// Single cache file for the three per-edge passes.
const EDGE_CACHE_FILE: &str = "recustomize_cache.car.bin";
/// Cache file for the legacy per-way (#433) path.
const OBSERVED_CACHE_FILE: &str = "recustomize_cache.car.v1.bin";

/// Which value column of the edge_speeds table to recustomize from (#521):
/// the median (contract base) or one of the optional SPEED-domain band
/// named profiles (worst = weekday peaks, best = nights / free-flow).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum EdgeTableColumn {
    Median,
    /// weekday-peak profile (`speed_ratio_worst`, legacy `speed_ratio_q25`)
    Worst,
    /// night/free-flow profile (`speed_ratio_best`, legacy `speed_ratio_q75`)
    Best,
}

impl EdgeTableColumn {
    /// Lane index inside the shared lookup table and the cache section id.
    /// STABLE — it is part of the on-disk cache layout.
    #[inline]
    fn lane(self) -> usize {
        match self {
            Self::Median => 0,
            Self::Best => 1,
            Self::Worst => 2,
        }
    }

    #[inline]
    fn section_id(self) -> u8 {
        self.lane() as u8
    }
}

// =====================================================================
// #552: inputs shared by the three per-edge passes
// =====================================================================

/// Heavy inputs shared by the typical / best / worst passes: built at most
/// once per boot, and only when at least one pass misses the cache.
struct EdgeInputs {
    /// Number of rows the table yielded (the `rows=` field of the applied
    /// log line — identical for all three passes, as before #552).
    n_rows: usize,
    /// Directed `(osm_from, osm_to)` → per-column value. Lane order is
    /// [`EdgeTableColumn::lane`]; `f32::NAN` means "this row has no value
    /// for that column" (the reader only ever stores finite positives).
    /// One map with three lanes rather than three maps: same last-row-wins
    /// semantics per column, one hash per row, a third of the memory.
    lut: HashMap<(i64, i64), [f32; 3]>,
    /// Number of distinct directed edges carrying a value, per lane —
    /// only ever tested for `> 0` (the pre-#552 "empty column" guard).
    lane_counts: [usize; 3],
    /// The median column carries base-speed ratios (vs absolute km/h).
    /// Band columns are always ratios.
    median_is_ratio: bool,
    /// Per-EBG-node expected OUTGOING turn penalty (#481), seconds.
    expected_turn_s: Vec<u32>,
    /// The mode's turn table (read once — it used to be read twice per pass).
    turn_penalties: Vec<u32>,
    /// The mode's filtered EBG (read once).
    filtered_ebg: crate::formats::FilteredEbg,
}

/// Per-boot preparation shared by the per-edge recustomization passes.
///
/// Cheap to construct: every field is filled lazily, so a boot that hits the
/// cache for all three sections never parses the parquet body and never
/// touches the turn table.
pub struct EdgeRecustomizePrep {
    path: PathBuf,
    cache_path: PathBuf,
    /// crc64 of the parquet bytes — one streaming pass, shared by every key.
    file_crc: Option<Option<u64>>,
    /// Effective per-column #524 level anchors, lane-indexed. `Err` carries
    /// the message that lane's pass must fail with (see `time_scales`).
    time_scales: Option<[Result<f64, String>; 3]>,
    /// The one cache key covering all three sections.
    key: Option<Option<u64>>,
    inputs: Option<Arc<EdgeInputs>>,
}

impl EdgeRecustomizePrep {
    /// Point the preparation at an `edge_speeds.parquet`. Resolves the cache
    /// location (honouring `BUTTERFLY_RECUSTOMIZE_CACHE_DIR` for deployments
    /// whose data volume is mounted read-only) but reads nothing.
    pub fn new(edge_speeds_path: &Path) -> Self {
        let cache_path = match std::env::var_os("BUTTERFLY_RECUSTOMIZE_CACHE_DIR") {
            Some(dir) => PathBuf::from(dir).join(EDGE_CACHE_FILE),
            None => edge_speeds_path.with_file_name(EDGE_CACHE_FILE),
        };
        Self {
            path: edge_speeds_path.to_path_buf(),
            cache_path,
            file_crc: None,
            time_scales: None,
            key: None,
            inputs: None,
        }
    }

    /// Drop the shared heavy inputs. The LUT, the turn table and the
    /// expected-turn array are hundreds of MB that the flat rebuild does not
    /// need — the last pass releases them before it rebuilds its flats, so
    /// boot's peak RSS does not carry both at once.
    pub fn release(&mut self) {
        self.inputs = None;
    }

    /// crc64 over the parquet bytes, streamed in 1 MiB chunks (the pre-#552
    /// key read the whole file into memory, three times). `None` when the
    /// file is unreadable — that disables the cache for this boot.
    fn file_crc(&mut self) -> Option<u64> {
        if self.file_crc.is_none() {
            self.file_crc = Some(crc_of_file(&self.path));
        }
        self.file_crc.expect("just filled")
    }

    /// Effective per-column level anchors (#524), read once. A band uses its
    /// own anchor when the artefact carries one, else the typical anchor
    /// (bands are ratios of the typical speeds, so the typical scale is the
    /// right fallback); absent everywhere ⇒ 1.0.
    ///
    /// Failures stay PER LANE, exactly as when each pass read its own keys:
    /// a band anchor outside the reader's sanity range fails that band only
    /// (its `?mode=` is hidden and opt-in), while a bad typical anchor
    /// poisons every lane — the bands fall back to it.
    fn time_scales(&mut self) -> &[Result<f64, String>; 3] {
        self.time_scales.get_or_insert_with(|| {
            let typical = crate::calibrate::read_time_scale(&self.path).map_err(|e| e.to_string());
            let band = |key: &str| -> Result<f64, String> {
                let t = typical.clone()?;
                let v = crate::calibrate::read_time_scale_key(&self.path, key)
                    .map_err(|e| e.to_string())?;
                Ok(v.or(t).unwrap_or(1.0))
            };
            let mut lanes = [Ok(1.0), Ok(1.0), Ok(1.0)];
            lanes[EdgeTableColumn::Median.lane()] = typical.clone().map(|t| t.unwrap_or(1.0));
            lanes[EdgeTableColumn::Best.lane()] = band("time_scale_best");
            lanes[EdgeTableColumn::Worst.lane()] = band("time_scale_worst");
            lanes
        })
    }

    /// The effective level anchor for one column, or the error that column's
    /// own pass must fail with.
    fn time_scale(&mut self, column: EdgeTableColumn) -> Result<f64> {
        self.time_scales()[column.lane()]
            .clone()
            .map_err(|e| anyhow::anyhow!(e))
    }

    /// The one cache key for this parquet + this artifact: algo tag ⊕
    /// parquet CRC ⊕ the CRCs of every container section the derivation
    /// consumes ([`ServerState::derivation_sections_crc`]) ⊕ the three
    /// effective level anchors.
    fn cache_key(&mut self, sections_crc: u64) -> Option<u64> {
        if let Some(k) = self.key {
            return k;
        }
        let key = (|| {
            let file_crc = self.file_crc()?;
            let mut d = crate::formats::crc::Digest::new();
            d.update(RECUSTOMIZE_EDGE_ALGO_TAG);
            d.update(&file_crc.to_le_bytes());
            d.update(&sections_crc.to_le_bytes());
            // Every lane's anchor, so a section can never be served under a
            // level it was not customized at. An unreadable lane is hashed
            // as a distinct marker (its pass fails and stores nothing).
            for lane in self.time_scales() {
                match lane {
                    Ok(v) => {
                        d.update(&[1]);
                        d.update(&v.to_bits().to_le_bytes());
                    }
                    Err(_) => d.update(&[0]),
                }
            }
            Some(d.finalize())
        })();
        self.key = Some(key);
        key
    }
}

/// Stream a file through the CRC digest without loading it whole.
fn crc_of_file(path: &Path) -> Option<u64> {
    use std::io::Read;
    let f = std::fs::File::open(path).ok()?;
    let mut r = std::io::BufReader::new(f);
    let mut d = crate::formats::crc::Digest::new();
    let mut buf = vec![0u8; 1 << 20];
    loop {
        match r.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => d.update(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => continue,
            Err(_) => return None,
        }
    }
    Some(d.finalize())
}

/// Container sections the PER-EDGE derivation reads. Every one of them is
/// hashed into the cache key: changing any of them changes the weights a
/// pass would produce, and a stale HIT would serve the old derivation.
const EDGE_DERIVATION_SECTIONS: &[&str] = &[
    "mode/car/weights.time",
    "mode/car/node_weights.time",
    "mode/car/node_weights.turn",
    "mode/car/topo",
    "mode/car/filtered_ebg",
    "shared/ebg.nodes",
    "shared/ebg.csr",
    "shared/nbg.node_map",
    "shared/edge_osm_offsets",
    "shared/edge_osm_ids",
];

/// Same, for the PER-WAY (#433) derivation: no OSM chains, but the way
/// attributes and the NBG geometry the profile is applied through.
const OBSERVED_DERIVATION_SECTIONS: &[&str] = &[
    "mode/car/weights.time",
    "mode/car/node_weights.time",
    "mode/car/node_weights.turn",
    "mode/car/topo",
    "mode/car/filtered_ebg",
    "mode/car/way_attrs",
    "shared/ebg.nodes",
    "shared/nbg.geo",
];

impl ServerState {
    /// Provenance digest over the container sections a derivation consumes:
    /// crc64 of (section name, section CRC) pairs, in the listed order. The
    /// section CRCs are ENGINE-INTERNAL provenance (repo-boundary rule: no
    /// deploy-side file conventions in the open engine) — they change with
    /// every rebuilt artifact. A section that is absent hashes as its name
    /// plus a zero CRC, which is itself a stable, distinct state.
    fn derivation_sections_crc(&self, names: &[&str]) -> u64 {
        let mut d = crate::formats::crc::Digest::new();
        for name in names {
            d.update(name.as_bytes());
            let crc = self
                .lazy
                .as_ref()
                .and_then(|l| l.container().get(name).map(|e| e.crc))
                .unwrap_or(0);
            d.update(&crc.to_le_bytes());
        }
        d.finalize()
    }

    /// Read a `mode/car/<leaf>` container section as bytes (CRC-verified).
    fn car_section_bytes(&self, leaf: &str) -> Result<&[u8]> {
        let mmap = self
            ._mmap_arc
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("recustomize requires container-backed ServerState"))?;
        let lazy = self
            .lazy
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("recustomize requires LazyContainer"))?;
        let name = format!("mode/car/{}", leaf);
        let entry = lazy
            .container()
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("recustomize: missing section '{}'", name))?;
        let off = entry.offset as usize;
        let len = entry.len as usize;
        anyhow::ensure!(
            off + len <= mmap.len(),
            "recustomize: section '{}' bytes [{},{}) exceed mmap len {}",
            name,
            off,
            off + len,
            mmap.len()
        );
        lazy.verify_now(&name)?;
        Ok(&mmap[off..off + len])
    }

    /// Same as [`Self::car_section_bytes`] but returns the mmap handle +
    /// range, for readers that keep a zero-copy view of the section.
    fn car_section_mmap(&self, leaf: &str) -> Result<(Arc<memmap2::Mmap>, usize, usize)> {
        let mmap = self
            ._mmap_arc
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("recustomize requires container-backed ServerState"))?;
        let lazy = self
            .lazy
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("recustomize requires LazyContainer"))?;
        let name = format!("mode/car/{}", leaf);
        let entry = lazy
            .container()
            .get(&name)
            .ok_or_else(|| anyhow::anyhow!("recustomize: missing section '{}'", name))?;
        let off = entry.offset as usize;
        let len = entry.len as usize;
        anyhow::ensure!(
            off + len <= mmap.len(),
            "recustomize: section '{}' bytes [{},{}) exceed mmap len {}",
            name,
            off,
            off + len,
            mmap.len()
        );
        lazy.verify_now(&name)?;
        Ok((Arc::clone(mmap), off, len))
    }

    /// #433: serve-boot car traffic recustomization from a runtime
    /// `observed_speeds.parquet`.
    ///
    /// Re-reads the raw step4-7 car inputs that `pack` ships as container
    /// sections (`mode/car/{way_attrs,filtered_ebg,node_weights.turn}` plus the
    /// shared `ebg.nodes` / `nbg.geo` already resident in `ServerState` and the
    /// base car's `cch_topo` + `node_weights`), calibrates ONE car traffic
    /// profile from the observations, re-runs the TIME-only step8 customization
    /// in memory ([`crate::customization::customize_cch_time_in_memory`]),
    /// builds fresh TIME flats, and **hot-swaps** the car `ModeSlot` to the
    /// calibrated weights. In-flight queries finish on their previously-cloned
    /// `Arc<ModeData>`; new queries see the calibrated car. The slot is then
    /// pinned non-evictable so the #402 idle compactor can't drop the
    /// recustomized weights and silently lazy-reload the clean base car.
    ///
    /// Requires the container path (`_mmap_arc` + `lazy` populated). This keeps
    /// the engine provider-clean: `observed_path` is a generic
    /// `(way_id, observed_avg_speed_kmh, sample_count)` table — the provider
    /// never crosses into the artifact. Returns the fitted profile on success.
    /// EVERY failure mode (parquet absent/bad, zero observations, customization
    /// error) is the CALLER's to treat as non-fatal — the clean base car keeps
    /// serving unchanged because the swap only happens on the success path.
    pub fn recustomize_car_from_observed(
        &self,
        observed_path: &Path,
    ) -> Result<crate::traffic::TrafficProfile> {
        let t0 = std::time::Instant::now();

        let car_idx = *self
            .mode_lookup
            .get("car")
            .ok_or_else(|| anyhow::anyhow!("recustomize: no 'car' mode loaded"))?
            as usize;

        // Clone-base: the currently-resident car ModeData. At boot (well within
        // the compactor's grace window) car is resident; if it was somehow
        // evicted we bail (non-fatal — the next query lazy-reloads clean base).
        let base: Arc<ModeData> = self.modes[car_idx]
            .state
            .read()
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| anyhow::anyhow!("recustomize: car slot not resident at boot"))?;

        // #444: PVC-cached recustomization. The ~5-min calibrate+customize is
        // pure recompute when neither the observed table nor the artifact
        // changed — key the cached weights on crc64(algo version ⊕ parquet ⊕
        // the container's car TIME-weights section CRC) and skip straight to
        // the flats on a hit. Cache failures are non-fatal in both directions.
        // BUTTERFLY_RECUSTOMIZE_CACHE_DIR overrides the cache location for
        // deployments where the data volume is mounted read-only and a
        // separate writable directory is provided (generic env knob — no
        // deploy-specific conventions). Default: next to the parquet.
        let cache_path = match std::env::var_os("BUTTERFLY_RECUSTOMIZE_CACHE_DIR") {
            Some(dir) => PathBuf::from(dir).join(OBSERVED_CACHE_FILE),
            None => observed_path.with_file_name(OBSERVED_CACHE_FILE),
        };
        let cache_key = observed_cache_key(
            observed_path,
            self.derivation_sections_crc(OBSERVED_DERIVATION_SECTIONS),
        );
        let cached = cache_key.and_then(|k| cache_load_section(&cache_path, k, 0));

        let (profile, new_weights, adjusted_node_weights) = if let Some(hit) = cached {
            tracing::info!(
                path = %cache_path.display(),
                "recustomize: cache HIT — skipping calibration + customization (#444)"
            );
            let profile = crate::traffic::TrafficProfile::from_json(&hit.provenance)?;
            (profile, hit.weights, hit.node_weights)
        } else {
            // 1. Raw inputs from the container.
            let way_attrs_vec = crate::formats::way_attrs::read_all_from_bytes(
                self.car_section_bytes("way_attrs")?,
            )?;
            let turns = crate::formats::mod_turns::read_all_from_bytes(
                self.car_section_bytes("node_weights.turn")?,
            )?;
            let (fe_mmap, fe_off, fe_len) = self.car_section_mmap("filtered_ebg")?;
            let filtered_ebg = crate::formats::FilteredEbgFile::read_from_mmap_unverified(
                fe_mmap, fe_off, fe_len,
            )?;

            // 2. Calibrate ONE car profile from the observed speeds.
            let observations = crate::calibrate::read_observations(observed_path)?;
            anyhow::ensure!(
                !observations.is_empty(),
                "recustomize: 0 observations in {}",
                observed_path.display()
            );
            let way_index = crate::calibrate::index_ways(&way_attrs_vec);
            let params = crate::calibrate::CalibrationParams::default();
            let result = crate::calibrate::fit(&observations, &way_index, &params)?;
            let profile = result.profile;
            tracing::info!(
                profile = profile.name.as_str(),
                observations = observations.len(),
                "recustomize: calibrated car profile"
            );

            // 3. Re-run TIME-only customization in memory (triangle-relax always on).
            // Codex audit (#438): also take the traffic-ADJUSTED per-node time
            // weights — edges_batch derives per-edge durations from
            // ModeData.node_weights, so cloning the base (legal-limit) weights
            // here would emit wrong durations along calibrated paths.
            let (new_weights, adjusted_node_weights) =
                crate::customization::customize_cch_time_in_memory(
                    &base.cch_topo,
                    &filtered_ebg,
                    &base.node_weights,
                    &turns.penalties,
                    &self.ebg_nodes,
                    Some((&profile, &way_attrs_vec, &self.nbg_geo)),
                )?;

            if let Some(k) = cache_key {
                let pass = CachedPass {
                    provenance: profile.to_json_string()?,
                    matched: 0,
                    weights: &new_weights,
                    node_weights: &adjusted_node_weights,
                };
                if let Err(e) = cache_store_section(&cache_path, k, 0, &pass) {
                    tracing::warn!(error = %e, "recustomize: cache write failed (non-fatal)");
                } else {
                    tracing::info!(path = %cache_path.display(), "recustomize: cache written (#444)");
                }
            }
            (profile, new_weights, adjusted_node_weights)
        };

        // 4. Fresh TIME flats + #528 len-along-time refresh, then hot-swap.
        let new_car =
            rebuild_car_family_mode(&base, &self.ebg_nodes, new_weights, adjusted_node_weights);
        let slot = &self.modes[car_idx];
        {
            let mut w = slot.state.write();
            *w = Some(Arc::new(new_car));
        }
        slot.evictable
            .store(false, std::sync::atomic::Ordering::Relaxed);

        tracing::info!(
            elapsed_s = t0.elapsed().as_secs_f64(),
            "recustomize: hot-swapped + pinned calibrated car"
        );
        Ok(profile)
    }

    /// Build (or load from cache) the heavy per-edge inputs shared by the
    /// three passes: parsed rows → one directed lookup, the turn table, the
    /// expected outgoing turn charge, and the filtered EBG.
    fn edge_inputs(&self, prep: &mut EdgeRecustomizePrep) -> Result<Arc<EdgeInputs>> {
        if let Some(i) = &prep.inputs {
            return Ok(Arc::clone(i));
        }
        let t0 = std::time::Instant::now();

        // 1. Read the table + build the directed lookup, one lane per column.
        let rows = crate::calibrate::read_edge_speeds(&prep.path)?;
        anyhow::ensure!(!rows.is_empty(), "edge recustomize: empty table");
        // Values are either absolute km/h or base-speed ratios —
        // read_edge_speeds enforces exactly one column type per table.
        let median_is_ratio = rows[0].ratio.is_some();
        let mut lut: HashMap<(i64, i64), [f32; 3]> = HashMap::with_capacity(rows.len());
        let mut lane_counts = [0usize; 3];
        for r in &rows {
            let lanes = [
                Some(
                    r.ratio
                        .or(r.speed_kmh)
                        .expect("reader guarantees one value"),
                ),
                r.best,
                r.worst,
            ];
            let slot = lut.entry((r.from, r.to)).or_insert([f32::NAN; 3]);
            for (lane, v) in lanes.into_iter().enumerate() {
                if let Some(v) = v {
                    if slot[lane].is_nan() {
                        lane_counts[lane] += 1;
                    }
                    slot[lane] = v;
                }
            }
        }
        let n_rows = rows.len();
        drop(rows);

        // 2. #481: sensors measure DOOR-TO-DOOR speed — their slowdown
        // includes junction dwell that this edge-based engine ALREADY
        // charges as turn penalties on every transition. Scaling the
        // link weight by the raw ratio double-counts that dwell,
        // maximally on urban arterials (measured ~24% pessimism, ring
        // detours on 68/1000 reference benchmark trips) and not at all on
        // motorways. Correction (zero fitted parameters): subtract the
        // edge's own EXPECTED outgoing turn penalty from the observed
        // door-to-door time before setting the link weight, floored at
        // legal free-flow: w' = max(w, w/v − E[t_out]).
        let turns = crate::formats::mod_turns::read_all_from_bytes(
            self.car_section_bytes("node_weights.turn")?,
        )?;
        let n_nodes_csr = self.ebg_csr.n_nodes as usize;
        let csr_offsets = self.ebg_csr.offsets.as_slice();
        let mut expected_turn_s: Vec<u32> = vec![0; n_nodes_csr];
        for i in 0..n_nodes_csr {
            let (a, b) = (csr_offsets[i] as usize, csr_offsets[i + 1] as usize);
            let mut sum = 0u64;
            let mut n = 0u64;
            for &p in &turns.penalties[a..b] {
                if p != u32::MAX {
                    sum += p as u64;
                    n += 1;
                }
            }
            if let Some(mean) = sum.checked_div(n) {
                expected_turn_s[i] = mean as u32;
            }
        }

        // 3. The mode's filtered EBG (zero-copy view of the container).
        let (fe_mmap, fe_off, fe_len) = self.car_section_mmap("filtered_ebg")?;
        let filtered_ebg =
            crate::formats::FilteredEbgFile::read_from_mmap_unverified(fe_mmap, fe_off, fe_len)?;

        let inputs = Arc::new(EdgeInputs {
            n_rows,
            lut,
            lane_counts,
            median_is_ratio,
            expected_turn_s,
            turn_penalties: turns.penalties,
            filtered_ebg,
        });
        tracing::info!(
            rows = n_rows,
            elapsed_s = t0.elapsed().as_secs_f64(),
            "edge recustomize: inputs prepared once for all passes (#552)"
        );
        prep.inputs = Some(Arc::clone(&inputs));
        Ok(inputs)
    }

    /// #454/#467 core: read a per-edge speeds/ratios table, map it onto a
    /// BASE car-family mode's per-EBG-node time weights (chain-aware per-
    /// segment matching with junction fallback), and run in-memory CCH
    /// customization — with the #444 on-disk cache (one file, one key, one
    /// CRC-guarded section per column since #552). Returns `(matched,
    /// cch_weights, adjusted_node_weights)`; the caller decides what to do
    /// with them (hot-swap `car`, register a variant, ...).
    fn recustomized_weights_from_edge_table(
        &self,
        base: &ModeData,
        prep: &mut EdgeRecustomizePrep,
        column: EdgeTableColumn,
    ) -> Result<(usize, CchWeights, Vec<u32>)> {
        let cache_key = prep.cache_key(self.derivation_sections_crc(EDGE_DERIVATION_SECTIONS));
        let cache_path = prep.cache_path.clone();
        if let Some(hit) =
            cache_key.and_then(|k| cache_load_section(&cache_path, k, column.section_id()))
        {
            tracing::info!(
                path = %cache_path.display(),
                section = ?column,
                "edge recustomize: cache HIT — skipping mapping + customization (#444)"
            );
            return Ok((hit.matched as usize, hit.weights, hit.node_weights));
        }

        let time_scale = prep.time_scale(column)?;
        let inputs = self.edge_inputs(prep)?;
        let lane = column.lane();
        anyhow::ensure!(
            inputs.lane_counts[lane] > 0,
            "edge recustomize: no values for column {column:?}"
        );
        // Band columns (#521) are always ratios; the median column is
        // whichever single value type the reader validated.
        let table_is_ratio = match column {
            EdgeTableColumn::Median => inputs.median_is_ratio,
            _ => true,
        };
        let lookup = |a: i64, b: i64| -> Option<f32> {
            let v = inputs.lut.get(&(a, b))?[lane];
            (!v.is_nan()).then_some(v)
        };

        // 1. Map onto per-EBG-node times (free-flow fallback elsewhere).
        let mut weights: Vec<u32> = base.node_weights.to_vec();
        anyhow::ensure!(
            weights.len() == self.ebg_nodes.nodes.len(),
            "weights/EBG length mismatch"
        );
        let mut matched = 0usize;
        for (i, node) in self.ebg_nodes.nodes.iter().enumerate() {
            if weights[i] == 0 {
                continue; // inaccessible sentinel
            }
            let from = match self.nbg_node_to_osm.get(node.tail_nbg as usize) {
                Some(&v) => v,
                None => continue,
            };
            let to = match self.nbg_node_to_osm.get(node.head_nbg as usize) {
                Some(&v) => v,
                None => continue,
            };
            // Per-OSM-segment tables (e.g. VDF over assignment flows,
            // #467) key on intermediate nodes — resolve this directed
            // EBG edge to its segment pairs via the #460 id chains and
            // average the matched values; junction-pair lookup is the
            // fallback (single-segment edges / chains absent / older
            // junction-keyed tables).
            let mut val: Option<f32> = None;
            if let Some(segs) = self.edge_osm.directed_segments(node.geom_idx, from) {
                let mut sum = 0.0f64;
                let mut n = 0u32;
                for (a, b) in segs {
                    if let Some(v) = lookup(a, b) {
                        sum += v as f64;
                        n += 1;
                    }
                }
                if n > 0 {
                    val = Some((sum / n as f64) as f32);
                }
            }
            if val.is_none() {
                val = lookup(from, to);
            }
            if let Some(v) = val {
                if table_is_ratio {
                    // Door-to-door observed time minus the engine's own
                    // expected junction charge (see #481 note above),
                    // floored at legal free-flow.
                    let w = weights[i] as f64;
                    let door_to_door = w / (v as f64).clamp(0.05, 1.0);
                    let et = *inputs.expected_turn_s.get(i).unwrap_or(&0) as f64;
                    weights[i] = (door_to_door - et).round().max(w.round()).max(1.0) as u32;
                } else {
                    let secs = (node.length_m as f64 * 3.6 / v as f64).round();
                    weights[i] = (secs.max(1.0) as u32).max(1);
                }
                matched += 1;
            }
        }
        // #524: global end-to-end level anchor — scale link weights AND
        // turn penalties so producer-measured levels propagate exactly
        // (input-ratio scaling reaches ~55%/pass and erodes rank
        // correlation because turns stay fixed).
        let scaled = (time_scale - 1.0).abs() > 1e-6;
        if scaled {
            for w in weights.iter_mut() {
                if *w != 0 {
                    *w = ((*w as f64) * time_scale).round().max(1.0) as u32;
                }
            }
            tracing::info!(
                time_scale,
                "edge recustomize: applied global time scale (#524)"
            );
        }
        tracing::info!(
            rows = inputs.n_rows,
            matched,
            total_edges = weights.len(),
            "edge recustomize: applied directed per-edge speeds"
        );
        anyhow::ensure!(matched > 0, "edge recustomize: 0 rows matched the graph");

        // 2. Customize on the modified weights (no profile scaling).
        let scaled_turns: Vec<u32>;
        let turn_penalties: &[u32] = if scaled {
            scaled_turns = inputs
                .turn_penalties
                .iter()
                .map(|&p| {
                    if p == u32::MAX {
                        p // forbidden-turn sentinel stays forbidden
                    } else {
                        ((p as f64) * time_scale).round() as u32
                    }
                })
                .collect();
            &scaled_turns
        } else {
            &inputs.turn_penalties
        };
        let (new_weights, adjusted) = crate::customization::customize_cch_time_in_memory(
            &base.cch_topo,
            &inputs.filtered_ebg,
            &weights,
            turn_penalties,
            &self.ebg_nodes,
            None,
        )?;

        if let Some(k) = cache_key {
            let pass = CachedPass {
                provenance: String::new(),
                matched: matched as u64,
                weights: &new_weights,
                node_weights: &adjusted,
            };
            if let Err(e) = cache_store_section(&cache_path, k, column.section_id(), &pass) {
                tracing::warn!(error = %e, "edge recustomize: cache write failed (non-fatal)");
            }
        }
        Ok((matched, new_weights, adjusted))
    }

    /// #521 uncertainty bands: register TWO HIDDEN variant weight sets from
    /// the optional speed_ratio_worst/best columns of the SAME edge_speeds
    /// table — worst-speed (congested tail -> pessimistic TIME) and best-speed
    /// (fluid -> optimistic). Same clean base, same #481 turn correction,
    /// same #524 time_scale as the median. The slots are pushed into
    /// `modes` but NEVER into `mode_lookup`, so no `?mode=` can reach them:
    /// the ONLY public car profile stays the median; bands are an explicit
    /// opt-in (`uncertainty=bands`) because they cost real compute.
    pub fn register_car_bands_from_edge_speeds(
        &mut self,
        prep: &mut EdgeRecustomizePrep,
    ) -> Result<()> {
        let t0 = std::time::Instant::now();
        if !crate::calibrate::edge_table_has_bands(&prep.path)? {
            tracing::info!("no band columns in edge_speeds table — bands not registered");
            return Ok(());
        }
        anyhow::ensure!(self.band_worst_idx.is_none(), "bands already registered");
        let ff_idx = self
            .car_freeflow_idx
            .ok_or_else(|| anyhow::anyhow!("bands registration: no 'car_freeflow' base"))?;
        let car_idx = *self
            .mode_lookup
            .get("car")
            .ok_or_else(|| anyhow::anyhow!("bands registration: no 'car' mode"))?
            as usize;
        let base: Arc<ModeData> = self.modes[ff_idx]
            .state
            .read()
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| anyhow::anyhow!("bands registration: car_freeflow not resident"))?;

        let columns = [
            (EdgeTableColumn::Worst, "car#worst"),
            (EdgeTableColumn::Best, "car#best"),
        ];
        let last = columns.len() - 1;
        for (i, (column, slot_name)) in columns.into_iter().enumerate() {
            let (matched, new_weights, adjusted_node_weights) =
                self.recustomized_weights_from_edge_table(&base, prep, column)?;
            if i == last {
                // Nothing reads the shared inputs after the last pass; free
                // them before the flats double the working set.
                prep.release();
            }
            let band =
                rebuild_car_family_mode(&base, &self.ebg_nodes, new_weights, adjusted_node_weights);

            let new_index = self.modes.len();
            let slot = ModeSlot::new_loaded_variant(slot_name.to_string(), band);
            slot.evictable
                .store(false, std::sync::atomic::Ordering::Relaxed);
            self.modes.push(slot);
            // Deliberately NOT inserted into mode_lookup (hidden from ?mode=).
            self.mode_names.push(slot_name.to_string());
            if let Some(mask) = self.snap_index.masks.get(car_idx).cloned() {
                self.snap_index.masks.push(mask);
            } else {
                tracing::warn!("car snap mask missing — band snapping degraded");
            }
            match column {
                EdgeTableColumn::Worst => self.band_worst_idx = Some(new_index),
                _ => self.band_best_idx = Some(new_index),
            }
            tracing::info!(
                matched,
                slot = slot_name,
                "registered hidden uncertainty band (#521)"
            );
        }
        tracing::info!(
            elapsed_s = t0.elapsed().as_secs_f64(),
            "uncertainty bands ready (opt-in via uncertainty=bands)"
        );
        Ok(())
    }

    /// (worst, best) band Modes, if registered (#521).
    pub fn band_modes(&self) -> Option<(Mode, Mode)> {
        match (self.band_worst_idx, self.band_best_idx) {
            (Some(p), Some(o)) => Some((Mode(p as u8), Mode(o as u8))),
            _ => None,
        }
    }

    /// #450/#454: serve-boot car recustomization from a DIRECTED per-edge
    /// speeds table — the generic contract for flow-derived (or any
    /// per-edge-measured) speeds:
    /// `edge_speeds.parquet (osm_node_from i64, osm_node_to i64, speed_kmh)`.
    ///
    /// Each EBG node IS a directed junction-edge (tail→head NBG nodes); rows
    /// matching `(osm(tail), osm(head))` set that edge's time to
    /// `length_m / speed`; unmatched edges keep their clean legal-limit time
    /// (free-flow fallback). Then the standard tail: in-memory TIME
    /// customization → flats → hot-swap the `car` slot → pin (car_freeflow
    /// keeps serving the untouched base). Every failure is the CALLER's to
    /// treat as non-fatal.
    pub fn recustomize_car_from_edge_speeds(
        &self,
        prep: &mut EdgeRecustomizePrep,
    ) -> Result<usize> {
        let t0 = std::time::Instant::now();
        let car_idx = *self
            .mode_lookup
            .get("car")
            .ok_or_else(|| anyhow::anyhow!("edge recustomize: no 'car' mode loaded"))?
            as usize;
        let base: Arc<ModeData> = self.modes[car_idx]
            .state
            .read()
            .as_ref()
            .map(Arc::clone)
            .ok_or_else(|| anyhow::anyhow!("edge recustomize: car slot not resident"))?;
        let (matched, new_weights, adjusted_node_weights) =
            self.recustomized_weights_from_edge_table(&base, prep, EdgeTableColumn::Median)?;

        // Flats + #528 lat refresh + swap + pin (same as the per-way path).
        let new_car =
            rebuild_car_family_mode(&base, &self.ebg_nodes, new_weights, adjusted_node_weights);
        let slot = &self.modes[car_idx];
        {
            let mut w = slot.state.write();
            *w = Some(Arc::new(new_car));
        }
        slot.evictable
            .store(false, std::sync::atomic::Ordering::Relaxed);
        tracing::info!(
            matched,
            elapsed_s = t0.elapsed().as_secs_f64(),
            "edge recustomize: hot-swapped + pinned flow-derived car (#454)"
        );
        Ok(matched)
    }
}

/// Field-clone `base` and swap in a freshly recustomized TIME channel: new
/// flats, and the #528 len-along-time refresh from the NEW time middles
/// (length along the RECUSTOMIZED time-shortest path). Distance-shortest
/// (`cch_weights_dist`) IS physical/traffic-invariant and stays cloned.
fn rebuild_car_family_mode(
    base: &ModeData,
    ebg_nodes: &crate::formats::EbgNodes,
    new_weights: CchWeights,
    adjusted_node_weights: Vec<u32>,
) -> ModeData {
    let up_adj_flat = UpAdjFlat::build_with(&base.cch_topo, &new_weights, true);
    let down_rev_flat = DownReverseAdjFlat::build_with(&base.cch_topo, &new_weights, true);
    let down_adj_flat = DownAdjFlat::build(&base.cch_topo, &new_weights);
    let (lat_w, lat_up, lat_dn) =
        refresh_len_along_time(base, ebg_nodes, &new_weights, &adjusted_node_weights);
    let mut out = clone_mode_data(base);
    out.cch_weights = new_weights;
    out.node_weights = std::borrow::Cow::Owned(adjusted_node_weights);
    out.up_adj_flat = up_adj_flat;
    out.down_rev_flat = down_rev_flat;
    out.down_adj_flat = down_adj_flat;
    out.cch_weights_len_along_time = lat_w;
    out.up_adj_flat_len_along_time = lat_up;
    out.down_rev_flat_len_along_time = lat_dn;
    out.down_adj_flat_len_along_time_lazy = std::sync::OnceLock::new();
    out
}

// =====================================================================
// #444/#552: serve-boot recustomization cache (PVC-resident)
// =====================================================================
//
// One file, one key, N CRC-guarded sections:
//
//   header  : magic(4) "RCW2" | format version(4) | key(8) | crc64(8)
//   section*: id(1) pad(7) | payload_len(8) | payload | crc64(8)
//
// Sections are APPENDED as their pass completes (the typical pass runs
// before the bands are even known to exist), so a pass never rewrites the
// megabytes another pass already stored. The reader takes the first section
// whose id matches AND whose CRC verifies, so a section corrupted by a torn
// write is simply recomputed and appended again; a torn trailing section
// (power cut mid-append) just ends the scan. Any structural problem ⇒ the
// whole file is treated as absent — never fatal.

const CACHE_MAGIC: &[u8; 4] = b"RCW2";
const CACHE_FORMAT_VERSION: u32 = 1;
const CACHE_HEADER_LEN: usize = 24;
/// Sanity bound on a section payload (Belgium's is ~0.5 GB).
const MAX_SECTION_BYTES: u64 = 64 << 30;
/// Hard bound on how many section frames one cache file may hold. Normal
/// operation writes exactly three (typical/best/worst) under a given key; a
/// section that failed its CRC and got recomputed appends a fourth. Past
/// this bound the file is started over from its header, so a persistently
/// failing writer (torn appends, two boots sharing the volume) can never
/// grow the cache without limit — it just recomputes.
const MAX_SECTIONS: usize = 6;

/// Provenance key for the legacy per-way (#433) cache: crc64 over (algo
/// version ⊕ the observed parquet's CRC ⊕ the CRCs of every container
/// section the derivation consumes). `None` (unreadable parquet) disables
/// the cache for this boot.
fn observed_cache_key(observed_path: &Path, sections_crc: u64) -> Option<u64> {
    let mut d = crate::formats::crc::Digest::new();
    d.update(RECUSTOMIZE_CACHE_VERSION);
    d.update(&crc_of_file(observed_path)?.to_le_bytes());
    d.update(&sections_crc.to_le_bytes());
    Some(d.finalize())
}

/// One cached weight set, borrowed for writing.
struct CachedPass<'a> {
    /// Free-form provenance: the fitted profile JSON on the per-way path,
    /// empty on the per-edge path.
    provenance: String,
    /// Rows matched onto the graph (per-edge path; 0 elsewhere).
    matched: u64,
    weights: &'a CchWeights,
    node_weights: &'a [u32],
}

/// One cached weight set, owned, as returned by the reader.
struct LoadedPass {
    provenance: String,
    matched: u64,
    weights: CchWeights,
    node_weights: Vec<u32>,
}

fn width_code(w: WeightWidth) -> u8 {
    match w {
        WeightWidth::U32 => 0,
        WeightWidth::U16 => 1,
        WeightWidth::U24 => 2,
    }
}

fn width_from_code(c: u8) -> Option<WeightWidth> {
    match c {
        0 => Some(WeightWidth::U32),
        1 => Some(WeightWidth::U16),
        2 => Some(WeightWidth::U24),
        _ => None,
    }
}

/// Serialized size of one weight array: width(1) + n(8) + body.
fn weight_array_bytes(w: &WeightArray) -> u64 {
    9 + (w.len() * w.width().bytes_per_entry()) as u64
}

/// Serialized size of a length-prefixed u32 array.
fn u32_array_bytes(n: usize) -> u64 {
    8 + 4 * n as u64
}

fn payload_len(p: &CachedPass<'_>) -> u64 {
    4 + p.provenance.len() as u64
        + 8
        + weight_array_bytes(&p.weights.up)
        + weight_array_bytes(&p.weights.down)
        + u32_array_bytes(p.weights.up_middle.len())
        + u32_array_bytes(p.weights.down_middle.len())
        + u32_array_bytes(p.node_weights.len())
}

fn write_all(
    w: &mut impl std::io::Write,
    d: &mut crate::formats::crc::Digest,
    bytes: &[u8],
) -> Result<()> {
    d.update(bytes);
    w.write_all(bytes)?;
    Ok(())
}

fn write_u32s(
    w: &mut impl std::io::Write,
    d: &mut crate::formats::crc::Digest,
    v: &[u32],
) -> Result<()> {
    write_all(w, d, &(v.len() as u64).to_le_bytes())?;
    write_all(w, d, bytemuck::cast_slice(v))
}

/// The payload reader: a `Take` limited to the frame's declared payload
/// length. Every count read from the (not-yet-verified) payload is checked
/// against the bytes that are still declared to belong to this section
/// BEFORE anything is allocated — a flipped length byte can otherwise ask
/// for a multi-GB allocation and take the process down before the trailing
/// CRC ever gets a chance to reject it.
type PayloadReader<'a> = std::io::Take<&'a mut std::io::BufReader<std::fs::File>>;

/// Allocation guard: `n` items of `item_bytes` must fit in what is left of
/// the declared payload.
fn ensure_fits(r: &PayloadReader<'_>, n: u64, item_bytes: u64) -> Result<usize> {
    let need = n
        .checked_mul(item_bytes)
        .ok_or_else(|| anyhow::anyhow!("cache array size overflow: {n}×{item_bytes}"))?;
    anyhow::ensure!(
        need <= r.limit(),
        "cache array of {need} B exceeds the {} B left in the section",
        r.limit()
    );
    Ok(n as usize)
}

fn read_u32s(r: &mut PayloadReader<'_>, d: &mut crate::formats::crc::Digest) -> Result<Vec<u32>> {
    use std::io::Read;
    let mut lb = [0u8; 8];
    r.read_exact(&mut lb)?;
    d.update(&lb);
    let len = ensure_fits(r, u64::from_le_bytes(lb), 4)?;
    let mut v = vec![0u32; len];
    let bytes: &mut [u8] = bytemuck::cast_slice_mut(&mut v);
    r.read_exact(bytes)?;
    d.update(bytes);
    Ok(v)
}

/// Write a weight array at its OWN storage width — the narrowing the
/// in-memory customization already chose (#552) survives into the cache, so
/// a warm boot restores exactly the same bytes a cold boot produced.
fn write_weight_array(
    w: &mut impl std::io::Write,
    d: &mut crate::formats::crc::Digest,
    arr: &WeightArray,
) -> Result<()> {
    write_all(w, d, &[width_code(arr.width())])?;
    write_all(w, d, &(arr.len() as u64).to_le_bytes())?;
    match arr {
        WeightArray::U16(a) => write_all(w, d, bytemuck::cast_slice(a.as_slice())),
        WeightArray::U24(b) => write_all(w, d, b.as_slice()),
        WeightArray::U32(a) => write_all(w, d, bytemuck::cast_slice(a.as_slice())),
    }
}

fn read_weight_array(
    r: &mut PayloadReader<'_>,
    d: &mut crate::formats::crc::Digest,
) -> Result<WeightArray> {
    use std::io::Read;
    let mut wb = [0u8; 1];
    r.read_exact(&mut wb)?;
    d.update(&wb);
    let width = width_from_code(wb[0])
        .ok_or_else(|| anyhow::anyhow!("cache: bad weight width code {}", wb[0]))?;
    let mut nb = [0u8; 8];
    r.read_exact(&mut nb)?;
    d.update(&nb);
    let n = ensure_fits(r, u64::from_le_bytes(nb), width.bytes_per_entry() as u64)?;
    Ok(match width {
        WeightWidth::U16 => {
            let mut v = vec![0u16; n];
            let bytes: &mut [u8] = bytemuck::cast_slice_mut(&mut v);
            r.read_exact(bytes)?;
            d.update(bytes);
            WeightArray::from_vec_u16(v)
        }
        WeightWidth::U24 => {
            let mut bytes = vec![0u8; n * 3];
            r.read_exact(&mut bytes)?;
            d.update(&bytes);
            WeightArray::from_u24_bytes(bytes, n)
        }
        WeightWidth::U32 => {
            let mut v = vec![0u32; n];
            let bytes: &mut [u8] = bytemuck::cast_slice_mut(&mut v);
            r.read_exact(bytes)?;
            d.update(bytes);
            WeightArray::from_vec_u32(v)
        }
    })
}

fn cache_header_bytes(key: u64) -> [u8; CACHE_HEADER_LEN] {
    let mut hdr = [0u8; CACHE_HEADER_LEN];
    hdr[0..4].copy_from_slice(CACHE_MAGIC);
    hdr[4..8].copy_from_slice(&CACHE_FORMAT_VERSION.to_le_bytes());
    hdr[8..16].copy_from_slice(&key.to_le_bytes());
    let mut d = crate::formats::crc::Digest::new();
    d.update(&hdr[0..16]);
    hdr[16..24].copy_from_slice(&d.finalize().to_le_bytes());
    hdr
}

/// Does `path` already hold a valid header for `key`?
fn cache_header_matches(path: &Path, key: u64) -> bool {
    use std::io::Read;
    let mut hdr = [0u8; CACHE_HEADER_LEN];
    match std::fs::File::open(path).and_then(|mut f| f.read_exact(&mut hdr)) {
        Ok(()) => hdr == cache_header_bytes(key),
        Err(_) => false,
    }
}

/// Walk the section frames, reading frame headers only (no payload), and
/// return `(frame count, offset just past the last well-formed frame)`.
/// Any structural problem ends the walk: the bytes after `intact_end` are
/// a torn tail the writer cuts off before appending.
fn cache_scan_frames(path: &Path) -> (usize, u64) {
    use std::io::{Read, Seek, SeekFrom};
    let Ok(f) = std::fs::File::open(path) else {
        return (0, 0);
    };
    let file_len = f.metadata().map(|m| m.len()).unwrap_or(0);
    let mut r = std::io::BufReader::new(f);
    let mut hdr = [0u8; CACHE_HEADER_LEN];
    if r.read_exact(&mut hdr).is_err() {
        return (0, 0);
    }
    let mut n = 0usize;
    let mut end = CACHE_HEADER_LEN as u64;
    loop {
        let mut frame = [0u8; 16];
        if r.read_exact(&mut frame).is_err() {
            return (n, end);
        }
        let len = u64::from_le_bytes(match <[u8; 8]>::try_from(&frame[8..16]) {
            Ok(b) => b,
            Err(_) => return (n, end),
        });
        let Some(next) = end.checked_add(16 + len + 8) else {
            return (n, end);
        };
        if len > MAX_SECTION_BYTES || next > file_len || r.seek(SeekFrom::Start(next)).is_err() {
            return (n, end);
        }
        end = next;
        n += 1;
    }
}

/// Append one section, creating the file (header only, atomically) when it
/// does not yet exist or belongs to a different key.
fn cache_store_section(path: &Path, key: u64, id: u8, pass: &CachedPass<'_>) -> Result<()> {
    use std::io::Write;
    let (frames, intact_end) = cache_scan_frames(path);
    if !cache_header_matches(path, key) || frames >= MAX_SECTIONS {
        // A distinct temp name per process: two boots sharing the volume
        // must never write the same scratch file.
        let tmp = path.with_extension(format!("bin.tmp.{}", std::process::id()));
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&cache_header_bytes(key))?;
        f.sync_all()?;
        std::fs::rename(&tmp, path)?;
    } else if intact_end < std::fs::metadata(path)?.len() {
        // A torn trailing frame (power cut mid-append) would otherwise sit
        // between our append and the reader's scan. Cut back to the last
        // well-formed boundary first.
        std::fs::OpenOptions::new()
            .write(true)
            .open(path)?
            .set_len(intact_end)?;
    }

    let f = std::fs::OpenOptions::new().append(true).open(path)?;
    let mut w = std::io::BufWriter::new(f);
    let mut d = crate::formats::crc::Digest::new();
    // Domain-separate the section CRC with the file key (see the reader).
    d.update(&key.to_le_bytes());

    let mut frame = [0u8; 16];
    frame[0] = id;
    frame[8..16].copy_from_slice(&payload_len(pass).to_le_bytes());
    write_all(&mut w, &mut d, &frame)?;

    write_all(
        &mut w,
        &mut d,
        &(pass.provenance.len() as u32).to_le_bytes(),
    )?;
    write_all(&mut w, &mut d, pass.provenance.as_bytes())?;
    write_all(&mut w, &mut d, &pass.matched.to_le_bytes())?;
    write_weight_array(&mut w, &mut d, &pass.weights.up)?;
    write_weight_array(&mut w, &mut d, &pass.weights.down)?;
    write_u32s(&mut w, &mut d, pass.weights.up_middle.as_slice())?;
    write_u32s(&mut w, &mut d, pass.weights.down_middle.as_slice())?;
    write_u32s(&mut w, &mut d, pass.node_weights)?;

    w.write_all(&d.finalize().to_le_bytes())?;
    w.into_inner()?.sync_all()?;
    Ok(())
}

/// Load section `id` from the cache, or `None` (recompute path) on ANY
/// problem — key mismatch, magic, per-section CRC, truncation.
fn cache_load_section(path: &Path, key: u64, id: u8) -> Option<LoadedPass> {
    use std::io::{Read, Seek, SeekFrom};
    let inner = || -> Result<Option<LoadedPass>> {
        let f = std::fs::File::open(path)?;
        let mut r = std::io::BufReader::new(f);
        let mut hdr = [0u8; CACHE_HEADER_LEN];
        r.read_exact(&mut hdr)?;
        anyhow::ensure!(&hdr[0..4] == CACHE_MAGIC, "bad magic");
        anyhow::ensure!(hdr == cache_header_bytes(key), "key/version mismatch");

        loop {
            let mut frame = [0u8; 16];
            match r.read_exact(&mut frame) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
                Err(e) => return Err(e.into()),
            }
            let len = u64::from_le_bytes(frame[8..16].try_into()?);
            anyhow::ensure!(len <= MAX_SECTION_BYTES, "section len implausible: {len}");
            let payload_start = r.stream_position()?;
            if frame[0] != id {
                // Skip the payload AND its trailing CRC.
                r.seek(SeekFrom::Start(payload_start + len + 8))?;
                continue;
            }
            // The section CRC is seeded with the file key, so a section
            // written under another key (a concurrent boot that replaced the
            // header between our check and our append) can never verify here.
            let mut d = crate::formats::crc::Digest::new();
            d.update(&key.to_le_bytes());
            d.update(&frame);
            let section = {
                let mut payload = (&mut r).take(len);
                let parsed = read_section_payload(&mut payload, &mut d);
                // A short/long parse must not desynchronise the scan: realign
                // on the declared frame boundary whatever happened.
                let leftover = payload.limit();
                match parsed {
                    Ok(p) if leftover == 0 => Ok(p),
                    Ok(_) => Err(anyhow::anyhow!("section shorter than declared")),
                    Err(e) => Err(e),
                }
            };
            r.seek(SeekFrom::Start(payload_start + len))?;
            let mut fb = [0u8; 8];
            let tail = r.read_exact(&mut fb);
            let want_crc = d.finalize();
            match (section, tail) {
                (Ok(p), Ok(())) if u64::from_le_bytes(fb) == want_crc => return Ok(Some(p)),
                // Corrupt section: the stream is realigned on the next frame,
                // so a later append of the same id (the recompute) is still
                // found. Never fatal, never a wrong-derivation hit.
                (_, Ok(())) => {
                    tracing::info!(section = id, "recustomize cache: section rejected");
                    continue;
                }
                _ => return Ok(None),
            }
        }
    };
    match inner() {
        Ok(x) => x,
        Err(e) => {
            tracing::info!(error = %e, path = %path.display(), "recustomize cache unusable — recomputing");
            None
        }
    }
}

fn read_section_payload(
    r: &mut PayloadReader<'_>,
    d: &mut crate::formats::crc::Digest,
) -> Result<LoadedPass> {
    use std::io::Read;
    let mut pl = [0u8; 4];
    r.read_exact(&mut pl)?;
    d.update(&pl);
    let plen = ensure_fits(r, u32::from_le_bytes(pl) as u64, 1)?;
    let mut pj = vec![0u8; plen];
    r.read_exact(&mut pj)?;
    d.update(&pj);
    let provenance = String::from_utf8(pj)?;

    let mut mb = [0u8; 8];
    r.read_exact(&mut mb)?;
    d.update(&mb);
    let matched = u64::from_le_bytes(mb);

    let up = read_weight_array(r, d)?;
    let down = read_weight_array(r, d)?;
    let um = read_u32s(r, d)?;
    let dm = read_u32s(r, d)?;
    let nw = read_u32s(r, d)?;

    Ok(LoadedPass {
        provenance,
        matched,
        weights: CchWeights {
            up,
            down,
            up_middle: crate::formats::ArcCow::from_vec(um),
            down_middle: crate::formats::ArcCow::from_vec(dm),
        },
        node_weights: nw,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tiny_weights() -> CchWeights {
        CchWeights {
            up: WeightArray::from_vec_u32_narrowed(vec![1, 2, u32::MAX, 65_534]),
            down: WeightArray::from_vec_u32_narrowed(vec![100_000, u32::MAX, 7]),
            up_middle: crate::formats::ArcCow::from_vec(vec![0u32, 1, u32::MAX, 3]),
            down_middle: crate::formats::ArcCow::from_vec(vec![u32::MAX, 5, 6]),
        }
    }

    fn assert_same(a: &CchWeights, b: &CchWeights) {
        assert_eq!(a.up.to_vec_u32(), b.up.to_vec_u32());
        assert_eq!(a.down.to_vec_u32(), b.down.to_vec_u32());
        assert_eq!(a.up.width(), b.up.width(), "storage width must round-trip");
        assert_eq!(a.down.width(), b.down.width());
        assert_eq!(a.up_middle.as_slice(), b.up_middle.as_slice());
        assert_eq!(a.down_middle.as_slice(), b.down_middle.as_slice());
    }

    #[test]
    fn cache_sections_round_trip_independently() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("recustomize_cache.car.bin");
        let w = tiny_weights();
        let nw = vec![9u32, 8, 7];

        for id in [0u8, 2] {
            let pass = CachedPass {
                provenance: String::new(),
                matched: 40 + id as u64,
                weights: &w,
                node_weights: &nw,
            };
            cache_store_section(&path, 77, id, &pass).unwrap();
        }

        for id in [0u8, 2] {
            let got = cache_load_section(&path, 77, id).expect("section present");
            assert_eq!(got.matched, 40 + id as u64);
            assert_eq!(got.node_weights, nw);
            assert_same(&got.weights, &w);
        }
        // A section that was never written is simply absent...
        assert!(cache_load_section(&path, 77, 1).is_none());
        // ...and a different key invalidates the whole file.
        assert!(cache_load_section(&path, 78, 0).is_none());
    }

    #[test]
    fn appending_a_section_keeps_the_others() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.bin");
        let w = tiny_weights();
        let nw = vec![1u32];
        let pass = |m: u64| CachedPass {
            provenance: String::new(),
            matched: m,
            weights: &w,
            node_weights: &nw,
        };
        cache_store_section(&path, 1, 0, &pass(1)).unwrap();
        let after_first = std::fs::metadata(&path).unwrap().len();
        cache_store_section(&path, 1, 1, &pass(2)).unwrap();
        assert!(
            std::fs::metadata(&path).unwrap().len() > after_first,
            "second section must be appended, not rewrite the file"
        );
        assert_eq!(cache_load_section(&path, 1, 0).unwrap().matched, 1);
        assert_eq!(cache_load_section(&path, 1, 1).unwrap().matched, 2);
    }

    #[test]
    fn a_corrupt_section_does_not_poison_its_neighbours() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.bin");
        let w = tiny_weights();
        let nw = vec![3u32, 4];
        let pass = |m: u64| CachedPass {
            provenance: String::new(),
            matched: m,
            weights: &w,
            node_weights: &nw,
        };
        cache_store_section(&path, 5, 0, &pass(11)).unwrap();
        let end_of_first = std::fs::metadata(&path).unwrap().len() as usize;
        cache_store_section(&path, 5, 1, &pass(22)).unwrap();

        // Flip one byte inside the FIRST section's payload.
        let mut bytes = std::fs::read(&path).unwrap();
        bytes[CACHE_HEADER_LEN + 20] ^= 0xFF;
        assert!(end_of_first < bytes.len());
        std::fs::write(&path, &bytes).unwrap();

        assert!(
            cache_load_section(&path, 5, 0).is_none(),
            "the corrupted section must be recomputed"
        );
        assert_eq!(
            cache_load_section(&path, 5, 1).unwrap().matched,
            22,
            "the intact section must still load"
        );
    }

    #[test]
    fn a_truncated_trailing_section_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.bin");
        let w = tiny_weights();
        let nw = vec![3u32, 4];
        cache_store_section(
            &path,
            9,
            0,
            &CachedPass {
                provenance: "prof".into(),
                matched: 1,
                weights: &w,
                node_weights: &nw,
            },
        )
        .unwrap();
        let full = std::fs::read(&path).unwrap();
        std::fs::write(&path, &full[..full.len() - 9]).unwrap();
        assert!(cache_load_section(&path, 9, 0).is_none());
    }

    #[test]
    fn the_file_never_grows_past_the_section_bound() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.bin");
        let w = tiny_weights();
        let nw = vec![3u32];
        let pass = |m: u64| CachedPass {
            provenance: String::new(),
            matched: m,
            weights: &w,
            node_weights: &nw,
        };
        // Same key, far more appends than the three real passes ever make.
        for i in 0..(MAX_SECTIONS as u64 + 4) {
            cache_store_section(&path, 42, 0, &pass(i)).unwrap();
            assert!(cache_scan_frames(&path).0 <= MAX_SECTIONS);
        }
        // Compaction never leaves the file unreadable: a section still
        // loads, and every section under one key holds the same derivation
        // anyway (the key pins it), so which copy wins does not matter.
        assert!(cache_load_section(&path, 42, 0).is_some());
    }

    /// #552 hardening: a section body is CRC'd together with the file key,
    /// so a section written under key A can never be adopted under a header
    /// carrying key B (two boots racing on the same volume).
    #[test]
    fn a_section_cannot_be_adopted_under_another_key() {
        let dir = tempfile::tempdir().unwrap();
        let a = dir.path().join("a.bin");
        let b = dir.path().join("b.bin");
        let w = tiny_weights();
        let nw = vec![3u32, 4];
        let pass = CachedPass {
            provenance: String::new(),
            matched: 7,
            weights: &w,
            node_weights: &nw,
        };
        cache_store_section(&a, 111, 0, &pass).unwrap();
        cache_store_section(&b, 222, 0, &pass).unwrap();

        // Graft B's header (key 222) onto A's body — what a racing writer
        // would leave behind if the section CRC did not bind the key.
        let mut bytes = std::fs::read(&a).unwrap();
        let bhdr = std::fs::read(&b).unwrap();
        bytes[..CACHE_HEADER_LEN].copy_from_slice(&bhdr[..CACHE_HEADER_LEN]);
        std::fs::write(&a, &bytes).unwrap();

        assert!(
            cache_load_section(&a, 222, 0).is_none(),
            "a section derived under another key must never be served"
        );
    }

    #[test]
    fn a_fresh_key_truncates_the_previous_file() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.bin");
        let w = tiny_weights();
        let nw = vec![3u32];
        let pass = |m: u64| CachedPass {
            provenance: String::new(),
            matched: m,
            weights: &w,
            node_weights: &nw,
        };
        cache_store_section(&path, 1, 0, &pass(1)).unwrap();
        cache_store_section(&path, 2, 0, &pass(2)).unwrap();
        assert!(cache_load_section(&path, 1, 0).is_none());
        assert_eq!(cache_load_section(&path, 2, 0).unwrap().matched, 2);
    }

    #[test]
    fn provenance_round_trips_for_the_per_way_path() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("c.bin");
        let w = tiny_weights();
        let nw = vec![3u32];
        cache_store_section(
            &path,
            3,
            0,
            &CachedPass {
                provenance: "{\"name\":\"x\"}".into(),
                matched: 0,
                weights: &w,
                node_weights: &nw,
            },
        )
        .unwrap();
        assert_eq!(
            cache_load_section(&path, 3, 0).unwrap().provenance,
            "{\"name\":\"x\"}"
        );
    }
}
