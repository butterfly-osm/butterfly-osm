//! Boot phase 2: mode discovery, the per-mode bundles and the hidden
//! `car_freeflow` base (#578).
//!
//! Split out of the monolithic `load_from_container_with_options` along
//! its own phase banners. The `rss::checkpoint` calls travel with the
//! code they measure, so the boot memory report is unchanged.

use anyhow::Result;
use std::collections::HashMap;
use std::path::Path;

use super::shared::Sections;
use super::{ModeData, ModeSlot, clone_mode_data, load_mode_data_from_bundle};
use crate::formats::butterfly_dat::Container;
use crate::formats::snap_index::SnapMask;
use crate::model::types::Mode;

/// The per-mode tables the loader builds, kept together because every
/// registration site has to keep all three in lockstep: `data[i]` is
/// named `names[i]`, and `lookup` maps the publicly reachable subset of
/// those names back to `i`.
///
/// Hidden modes (`car_freeflow`, the uncertainty bands) are in `data` +
/// `names` but deliberately NOT in `lookup`, so no `?mode=` can reach
/// them.
///
/// #578: each entry carries its own `evictable` flag — the #402 idle
/// compactor may drop a base mode (it reloads from its container
/// bundle) but never a synthetic one (no bundle backs it). The flag is
/// pushed by the site that built the mode, not inferred from its name.
pub(super) struct ModeTables {
    pub data: Vec<(ModeData, bool)>,
    pub names: Vec<String>,
    pub lookup: HashMap<String, u8>,
}

impl ModeTables {
    pub fn with_capacity(n: usize) -> Self {
        Self {
            data: Vec::with_capacity(n),
            names: Vec::with_capacity(n),
            lookup: HashMap::with_capacity(n),
        }
    }

    /// Iterate the loaded modes in mode-index order.
    pub fn datas(&self) -> impl Iterator<Item = &ModeData> {
        self.data.iter().map(|(data, _)| data)
    }

    /// Borrow one mode's data by index.
    pub fn data_at(&self, index: usize) -> &ModeData {
        &self.data[index].0
    }

    /// Register a publicly reachable mode. Returns its mode index.
    pub fn push(&mut self, name: String, data: ModeData, evictable: bool) -> usize {
        let index = self.data.len();
        self.data.push((data, evictable));
        self.lookup.insert(name.clone(), index as u8);
        self.names.push(name);
        index
    }

    /// Register a mode that keeps a slot + a name but no `mode_lookup`
    /// entry — unreachable via `?mode=`.
    pub fn push_hidden(&mut self, name: String, data: ModeData, evictable: bool) -> usize {
        let index = self.data.len();
        self.data.push((data, evictable));
        self.names.push(name);
        index
    }

    /// Consume the tables into the three `ServerState` fields they
    /// become: the slots, the names, and the public-name lookup.
    ///
    /// #402: each ModeData is wrapped in a lazy/evictable slot, carrying
    /// the `evictable` flag its construction site pushed — synthetic
    /// modes (e.g. `car_freeflow`) are pinned because no container
    /// bundle can reload them.
    pub fn finish(self) -> (Vec<ModeSlot>, Vec<String>, HashMap<String, u8>) {
        let names = self.names;
        let slots: Vec<ModeSlot> = self
            .data
            .into_iter()
            .zip(names.iter().cloned())
            .map(|((data, evictable), name)| ModeSlot::new_loaded(name, data, evictable))
            .collect();
        (slots, names, self.lookup)
    }
}

/// The mode names a container carries, after applying `mode_filter`,
/// plus the GLOBAL mode-byte index (position in the unfiltered
/// alphabetical list — it must match step 4/5 indexing).
pub(super) fn discover(
    container: &Container,
    mode_filter: Option<&[String]>,
    container_path: &Path,
) -> Result<(Vec<String>, HashMap<String, u8>)> {
    let all_modes = container.list_modes();
    if all_modes.is_empty() {
        anyhow::bail!(
            "container {} has no `mode/<name>/...` bundles; cannot serve",
            container_path.display()
        );
    }
    let global_index: HashMap<String, u8> = all_modes
        .iter()
        .enumerate()
        .map(|(i, n)| (n.clone(), i as u8))
        .collect();
    let discovered_modes: Vec<String> = if let Some(filter) = mode_filter {
        all_modes
            .into_iter()
            .filter(|m| filter.iter().any(|f| f == m))
            .collect()
    } else {
        all_modes
    };
    if discovered_modes.is_empty() {
        anyhow::bail!("mode filter excluded every mode in the container");
    }
    tracing::info!(modes = ?discovered_modes, "discovered transport modes");
    Ok((discovered_modes, global_index))
}

/// Load one bundle per discovered mode.
pub(super) fn load_bundles(
    sec: &Sections<'_>,
    discovered_modes: &[String],
    global_index: &HashMap<String, u8>,
) -> Result<ModeTables> {
    let mut tables = ModeTables::with_capacity(discovered_modes.len());

    for (mode_index, mode_name) in discovered_modes.iter().enumerate() {
        let mode = Mode(global_index[mode_name]);
        let mode_data =
            load_mode_data_from_bundle(mode_name, mode, sec.container, sec.mmap, sec.lazy)?;
        tracing::info!(
            mode = mode_name.as_str(),
            index = mode_index,
            filtered_nodes = mode_data.n_filtered_nodes,
            up_edges = mode_data.cch_topo.up_targets.len(),
            "loaded mode bundle"
        );
        // Base modes are evictable: the #402 compactor can drop one and
        // `get_mode` reloads it from its container bundle.
        tables.push(mode_name.clone(), mode_data, true);
        crate::server::rss::checkpoint(&format!("load.mode.{}", mode_name));
    }

    Ok(tables)
}

/// Hint the kernel that the per-mode sections nothing reads after boot
/// are cold, so their pages leave RSS.
pub(super) fn evict_cold_mode_sections(sec: &Sections<'_>, discovered_modes: &[String]) {
    let container = sec.container;
    let mmap_for_bytes = sec.mmap;

    // #149: Now that every mode's flat adjacencies are built, hint
    // the kernel that the cch_weights.{time,dist} byte ranges are
    // cold. The routing hot path (CchQuery, isochrone PHAST,
    // matrix bucket M2M) reads weights through the flats; the only
    // remaining `cch_weights.up`/`.down` readers are
    //   - the transit fingerprint hash (one-time, at startup)
    //   - the per-call exclude/avoid recustomizers (cold)
    //   - validators / bench harness (off the production path)
    // so dropping these pages from RSS is a pure win. The Cow
    // slices into them remain valid; subsequent rare reads page
    // them back in at standard fault cost.
    for mode_name in discovered_modes {
        for leaf in ["weights.time", "weights.dist", "weights.lat"] {
            let section = format!("mode/{}/{}", mode_name, leaf);
            if let Some(entry) = container.get(&section) {
                let off = entry.offset as usize;
                let len = entry.len as usize;
                let range = &mmap_for_bytes[off..off + len];
                match crate::formats::mmap::madvise_dontneed(range) {
                    Ok(()) => tracing::info!(
                        section = %section,
                        bytes = len,
                        "madvise(DONTNEED) on cold weight section"
                    ),
                    Err(e) => tracing::warn!(
                        section = %section,
                        error = %e,
                        "madvise(DONTNEED) failed, ignoring"
                    ),
                }
            }
        }
    }

    // #279: evict TIME flats for non-default modes too.
    //
    // Each mode loads three time-metric flat sections
    // (up_adj_flat.time, down_reverse_adj_flat.time,
    // down_adj_flat.time). They are zero-copy Cow::Borrowed views
    // and the boot-time CRC walk forces them resident. For
    // workloads dominated by one mode (almost always car on
    // consumer routing, truck on delivery, etc.) the non-default
    // modes' flats sit resident for nothing.
    //
    // Pick the same "default" mode as the exclude-flag loader
    // (car if present, otherwise the first discovered mode). Evict
    // every other mode's time flats. madvise on a zero-copy view's
    // backing pages is safe — the view stays valid; kernel
    // demand-pages on first query of that mode.
    //
    // On Belgium with 4 modes (car/bike/foot/truck) this evicts
    // ~3 × 1.6 GiB ≈ 5 GiB of flats. First bike/foot/truck query
    // pays one cold page-in pass; the kernel keeps subsequent
    // queries hot via its page cache.
    let default_mode = if discovered_modes.iter().any(|m| m == "car") {
        "car"
    } else {
        discovered_modes[0].as_str()
    };
    for mode_name in discovered_modes {
        if mode_name == default_mode {
            continue;
        }
        for leaf in [
            "up_adj_flat.time",
            "down_reverse_adj_flat.time",
            "down_adj_flat.time",
        ] {
            let section = format!("mode/{}/{}", mode_name, leaf);
            if let Some(entry) = container.get(&section) {
                let off = entry.offset as usize;
                let len = entry.len as usize;
                // Use checked_add so corrupted container metadata
                // can't overflow usize and silently bypass the
                // bounds check.
                let end = match off.checked_add(len) {
                    Some(e) => e,
                    None => {
                        tracing::warn!(
                            section = %section,
                            offset = off,
                            len = len,
                            "container section offset+len overflows usize; skipping madvise"
                        );
                        continue;
                    }
                };
                if end > mmap_for_bytes.len() {
                    tracing::warn!(
                        section = %section,
                        offset = off,
                        len = len,
                        mmap_len = mmap_for_bytes.len(),
                        "container section out-of-bounds vs mmap; skipping madvise"
                    );
                    continue;
                }
                let range = &mmap_for_bytes[off..end];
                if let Err(e) = crate::formats::mmap::madvise_dontneed(range) {
                    tracing::warn!(
                        section = %section,
                        error = %e,
                        "madvise(DONTNEED) on non-default flat failed; ignoring"
                    );
                } else {
                    tracing::info!(
                        section = %section,
                        bytes = len,
                        "madvise(DONTNEED) on non-default mode time flat (#279)"
                    );
                }
            }
        }
    }
}

/// #450: register `car_freeflow` — an alias of the clean legal-limit
/// base car AS LOADED. When the #433 boot recustomization later swaps
/// the `car` slot to calibrated/flow-derived weights, this mode keeps
/// serving free-flow (maxspeed-honoring) — required by traffic
/// simulation consumers (congested speeds are circular for a sim).
/// Field-clone is cheap (mmap/Arc-backed sections); the slot is
/// non-evictable (no container section backs the synthetic name).
///
/// Returns the new mode index, or `None` when the container carries no
/// car mode.
pub(super) fn register_car_freeflow(
    tables: &mut ModeTables,
    snap_masks: &mut Vec<SnapMask>,
) -> Option<usize> {
    let &car_idx = tables.lookup.get("car")?;
    let freeflow = clone_mode_data(tables.data_at(car_idx as usize));
    // NOT inserted into mode_lookup — resident base only, hidden from
    // ?mode= and /health (single public car = median, #521). Kept in
    // mode_names so the slot has a name.
    // Pinned: no container section backs the synthetic name, so the
    // #402 compactor must never drop it.
    let new_index = tables.push_hidden("car_freeflow".to_string(), freeflow, false);
    // Snap masks are indexed by mode_idx — the synthetic mode shares
    // car's eligible-edges mask; without it every snap for this mode
    // returns None → no routes.
    if let Some(base_mask) = snap_masks.get(car_idx as usize).cloned() {
        snap_masks.push(base_mask);
    } else {
        tracing::warn!("car snap mask missing — car_freeflow snapping degraded");
    }
    tracing::info!("registered car_freeflow (clean legal-limit base) alongside car (#450)");
    Some(new_index)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::formats::mmap::ArcCow;
    use crate::formats::{BitsetField, CchTopo, CchWeights, WeightArray};
    use crate::matrix::bucket_ch::{DownAdjFlat, DownReverseAdjFlat, UpAdjFlat};
    use std::sync::atomic::Ordering;

    /// The smallest `ModeData` the slot plumbing accepts: one node, no
    /// arcs. Nothing here is routed over — the test is about which slot
    /// the #402 compactor is allowed to drop.
    fn tiny_mode_data() -> ModeData {
        let topo = CchTopo {
            n_nodes: 1,
            n_shortcuts: 0,
            n_original_arcs: 0,
            inputs_sha: [0u8; 32],
            up_offsets: ArcCow::from_vec(vec![0u64, 0]),
            up_targets: ArcCow::from_vec(Vec::new()),
            up_is_shortcut: BitsetField::from_bools(&[]),
            up_middle: WeightArray::from_vec_u32(Vec::new()),
            down_offsets: ArcCow::from_vec(vec![0u64, 0]),
            down_targets: ArcCow::from_vec(Vec::new()),
            down_is_shortcut: BitsetField::from_bools(&[]),
            down_middle: WeightArray::from_vec_u32(Vec::new()),
            rank_to_filtered: ArcCow::from_vec(vec![0u32]),
        };
        let weights = CchWeights {
            up: WeightArray::from_vec_u32(Vec::new()),
            down: WeightArray::from_vec_u32(Vec::new()),
            up_middle: ArcCow::from_vec(Vec::new()),
            down_middle: ArcCow::from_vec(Vec::new()),
        };
        ModeData {
            mode: Mode(0),
            cch_topo: topo.clone(),
            cch_weights: weights.clone(),
            cch_weights_dist: weights.clone(),
            cch_weights_len_along_time: None,
            orig_to_rank: ArcCow::from_vec(vec![0u32]),
            filtered_to_original: ArcCow::from_vec(vec![0u32]),
            n_filtered_nodes: 1,
            n_original_nodes: 1,
            node_weights: std::borrow::Cow::Owned(vec![0u32]),
            mask: vec![0u64],
            has_outbound: vec![0u64],
            has_inbound: vec![0u64],
            up_adj_flat: UpAdjFlat::build_with(&topo, &weights, true),
            down_rev_flat: DownReverseAdjFlat::build_with(&topo, &weights, true),
            down_adj_flat: DownAdjFlat::build(&topo, &weights),
            up_adj_flat_dist: UpAdjFlat::build_with(&topo, &weights, true),
            down_rev_flat_dist: DownReverseAdjFlat::build_with(&topo, &weights, true),
            up_adj_flat_len_along_time: None,
            down_rev_flat_len_along_time: None,
            down_adj_flat_len_along_time_lazy: std::sync::OnceLock::new(),
            exclude_cache: crate::server::exclude::ExcludeWeightCache::default(),
        }
    }

    /// #578: the pin must not be able to invert. `car_freeflow` has no
    /// container bundle behind it, so the #402 idle compactor must never
    /// evict it — evicting it would leave the next query to lazy-reload
    /// a mode that cannot be reloaded. Base modes must stay evictable,
    /// including one whose name happens to start with another loaded
    /// mode's name plus a suffix: the retired `is_variant_mode_name`
    /// prefix scan pinned `bike_cargo` merely because `bike` was loaded.
    #[test]
    fn car_freeflow_slot_is_pinned_and_base_modes_stay_evictable() {
        let mut tables = ModeTables::with_capacity(3);
        tables.push("bike".to_string(), tiny_mode_data(), true);
        tables.push("car".to_string(), tiny_mode_data(), true);
        tables.push("bike_cargo".to_string(), tiny_mode_data(), true);

        let mut snap_masks: Vec<SnapMask> = Vec::new();
        let freeflow_idx =
            register_car_freeflow(&mut tables, &mut snap_masks).expect("car mode is loaded");
        assert_eq!(freeflow_idx, 3);
        assert_eq!(tables.names[freeflow_idx], "car_freeflow");
        assert!(
            !tables.lookup.contains_key("car_freeflow"),
            "car_freeflow must stay hidden from ?mode="
        );

        let (slots, names, _lookup) = tables.finish();
        assert_eq!(names, ["bike", "car", "bike_cargo", "car_freeflow"]);
        assert!(
            slots[0].evictable.load(Ordering::Relaxed),
            "base mode 'bike' must stay evictable"
        );
        assert!(
            slots[1].evictable.load(Ordering::Relaxed),
            "base mode 'car' must stay evictable"
        );
        assert!(
            slots[2].evictable.load(Ordering::Relaxed),
            "'bike_cargo' is a base mode, not a variant of 'bike'"
        );
        assert!(
            !slots[freeflow_idx].evictable.load(Ordering::Relaxed),
            "car_freeflow has no bundle to reload from and must be pinned"
        );
    }
}
