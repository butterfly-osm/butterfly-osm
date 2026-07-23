//! Step 8: CCH Customization
//!
//! Applies per-mode weights to the CCH shortcuts using bottom-up customization
//! + parallel triangle relaxation.
//!
//! # Algorithm Overview
//!
//! CCH customization processes nodes in contraction order (lowest rank first).
//! For each edge in the up/down graphs:
//!
//! - **Original edges**: weight = edge_weight[target] + turn_penalty[arc]
//! - **Shortcuts u→w via m**: weight = weight(u→m) + weight(m→w)
//!
//! # Dependency Order (CRITICAL for bottom-up)
//!
//! For each node u processed at rank r:
//! 1. **DOWN edges must be processed FIRST**, in order of INCREASING target rank
//!    - Down shortcut u→v via m requires down_weights[u→m]
//!    - Since rank(m) < rank(v), processing by increasing rank ensures u→m before u→v
//! 2. **UP edges processed SECOND** (order doesn't matter within UP)
//!    - Up shortcut u→v via m requires down_weights[u→m] and up_weights[m→v]
//!    - down_weights[u→m] computed in phase 1
//!    - up_weights[m→v] computed when node m was processed (rank(m) < rank(u))
//!
//! # Triangle Relaxation (parallel)
//!
//! After bottom-up, triangle relaxation discovers cheaper paths through alternative
//! contracted nodes. Uses `AtomicU32::fetch_min` for lock-free parallel processing:
//! - Relaxation only *decreases* weights (monotone)
//! - Stale reads (Relaxed ordering) are safe: missed updates caught by next pass
//! - Convergence check (0 updates) guarantees correctness

use anyhow::Result;
use rayon::prelude::*;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::PathBuf;
use std::sync::atomic::{AtomicU32, AtomicU64, Ordering};

use crate::formats::{
    ArcCow, CchTopo, CchTopoFile, CchWeights, EbgNodes, EbgNodesFile, FilteredEbgFile,
    HybridStateFile, NbgGeoFile, WeightArray, mod_turns, mod_weights, way_attrs,
};
use crate::profile_abi::Mode;

/// Configuration for Step 8
pub struct Step8Config {
    pub cch_topo_path: PathBuf,
    pub filtered_ebg_path: PathBuf,
    pub weights_path: PathBuf, // w.*.u32
    pub turns_path: PathBuf,   // t.*.u32
    pub order_path: PathBuf,
    pub ebg_nodes_path: PathBuf, // ebg.nodes from step4
    pub mode: Mode,
    pub mode_name: String,
    pub outdir: PathBuf,
    /// Optional traffic recustomization. When `Some`, applies per-density-class
    /// speed factors to edge weights, writes outputs as
    /// `cch.w.<mode>_<variant>.u32` and skips the distance metric (distance is
    /// physical and unaffected by traffic).
    pub traffic: Option<TrafficCustomization>,
    /// When `true` AND `traffic` is set, write the traffic-customised
    /// weights to the BASE path `cch.w.<mode>.u32` instead of the
    /// suffixed variant path `cch.w.<mode>_<variant>.u32`. The sidecar
    /// `cch.w.<mode>.traffic.json` is still emitted for provenance so
    /// human-readable origin survives.
    ///
    /// Used to make a friction profile the implicit default — e.g.
    /// `step8-customize --traffic realistic --bake-as-base` makes
    /// `?mode=car` return realistic-friction durations instead of the
    /// legal-limit baseline, without introducing a separate variant
    /// mode name.
    pub bake_traffic_as_base: bool,
}

/// Inputs needed to apply a traffic profile during step 8.
pub struct TrafficCustomization {
    pub profile: crate::traffic::TrafficProfile,
    /// `way_attrs.<mode>.bin` — required for the per-way density class.
    pub way_attrs_path: PathBuf,
    /// `nbg.geo` from step 3 — required to map EBG node → first OSM way id.
    pub nbg_geo_path: PathBuf,
    /// DEVELOPMENT-ONLY: skip triangle relaxation. THIS PRODUCES INCORRECT
    /// (over-estimated) shortest-path durations because CCH search relies
    /// on shortcut weights equalling true shortest distances between their
    /// endpoints. Default false. Empirical Belgium check: skipping relax
    /// turned a 1947 s / 45 km Brussels–Antwerp route into a 5583 s / 77 km
    /// route — the algorithm picked a clearly suboptimal corridor because
    /// the shortcut weights were loose. Only flip this on for bench
    /// experiments, never for serving traffic to users.
    pub skip_triangle_relax: bool,
}

/// Result of Step 8 customization
#[derive(Debug)]
pub struct Step8Result {
    pub output_path: PathBuf,
    /// Empty PathBuf for traffic recustomization (distance is physical and
    /// not re-emitted — the freeflow `cch.d.<mode>.u32` covers all variants).
    pub distance_output_path: PathBuf,
    pub mode: Mode,
    pub mode_name: String,
    pub n_up_edges: u64,
    pub n_down_edges: u64,
    pub customize_time_ms: u64,
}

/// Sorted filtered EBG adjacency for fast arc index lookup
/// Uses filtered node IDs but stores original arc indices for turn penalty lookup
struct SortedFilteredEbgAdj {
    offsets: Vec<u64>,
    sorted_heads: Vec<u32>,        // Filtered node IDs (targets)
    sorted_orig_arc_idx: Vec<u32>, // Original arc indices for turn penalty lookup
}

impl SortedFilteredEbgAdj {
    /// Build sorted adjacency from FilteredEbg
    fn build(filtered_ebg: &crate::formats::FilteredEbg) -> Self {
        let n_nodes = filtered_ebg.n_filtered_nodes as usize;
        let n_arcs = filtered_ebg.n_filtered_arcs as usize;

        let sorted_per_node: Vec<Vec<(u32, u32)>> = (0..n_nodes)
            .into_par_iter()
            .map(|u| {
                let start = filtered_ebg.offsets[u] as usize;
                let end = filtered_ebg.offsets[u + 1] as usize;
                let mut edges: Vec<(u32, u32)> = (start..end)
                    .map(|i| (filtered_ebg.heads[i], filtered_ebg.original_arc_idx[i]))
                    .collect();
                edges.sort_unstable_by_key(|(head, _)| *head);
                edges
            })
            .collect();

        let mut offsets = Vec::with_capacity(n_nodes + 1);
        let mut sorted_heads = Vec::with_capacity(n_arcs);
        let mut sorted_orig_arc_idx = Vec::with_capacity(n_arcs);

        let mut offset = 0u64;
        for edges in sorted_per_node {
            offsets.push(offset);
            for (head, orig_arc_idx) in edges {
                sorted_heads.push(head);
                sorted_orig_arc_idx.push(orig_arc_idx);
            }
            offset = sorted_heads.len() as u64;
        }
        offsets.push(offset);

        Self {
            offsets,
            sorted_heads,
            sorted_orig_arc_idx,
        }
    }

    #[inline]
    fn find_original_arc_index(&self, u: usize, v: u32) -> Option<u32> {
        let start = self.offsets[u] as usize;
        let end = self.offsets[u + 1] as usize;
        if start >= end {
            return None;
        }
        match self.sorted_heads[start..end].binary_search(&v) {
            Ok(idx) => Some(self.sorted_orig_arc_idx[start + idx]),
            Err(_) => None,
        }
    }
}

// ===================================================================
// Main customization entry point
// ===================================================================

/// Customize CCH for a specific mode (time + distance weights, parallelized).
/// When `config.traffic` is `Some`, applies per-density-class speed factors
/// to time weights and writes outputs as `cch.w.<mode>_<variant>.u32`. The
/// distance metric is unaffected and not re-emitted.
pub fn customize_cch(config: Step8Config) -> Result<Step8Result> {
    let start_time = std::time::Instant::now();
    let mode_name = &config.mode_name;
    let traffic = config.traffic.as_ref();

    if let Some(t) = traffic {
        println!(
            "\n🚦 Step 8: Traffic recustomization for {} via profile '{}'...\n",
            mode_name, t.profile.name
        );
        for class in crate::density::DensityClass::ALL {
            println!(
                "  factor[{}] = {:.3}",
                class.as_str(),
                t.profile.factor_for(class)
            );
        }
        if t.profile.has_matrix() {
            println!(
                "  + (highway_class × density) matrix: {} highway rows (cells override the vector; missing cells fall back)",
                t.profile.matrix.len()
            );
            for (code, row) in &t.profile.matrix {
                let cells: Vec<String> = crate::density::DensityClass::ALL
                    .iter()
                    .filter_map(|c| {
                        row[c.to_u8() as usize].map(|f| format!("{}={:.3}", c.as_str(), f))
                    })
                    .collect();
                println!("    matrix[{}]: {}", code, cells.join(", "));
            }
        }
    } else {
        println!("\n🎨 Step 8: Customizing CCH for {}...\n", mode_name);
    }

    // Load all data
    println!("Loading CCH topology...");
    let topo = CchTopoFile::read(&config.cch_topo_path)?;
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();
    println!(
        "  ✓ {} nodes, {} up edges, {} down edges",
        n_nodes, n_up, n_down
    );

    println!("Loading filtered EBG...");
    let filtered_ebg = FilteredEbgFile::read(&config.filtered_ebg_path)?;
    println!(
        "  ✓ {} filtered nodes, {} arcs",
        filtered_ebg.n_filtered_nodes, filtered_ebg.n_filtered_arcs
    );

    println!("Loading weights ({})...", mode_name);
    let mut weights = mod_weights::read_all(&config.weights_path)?;
    println!("  ✓ {} node weights", weights.weights.len());

    println!("Loading turn penalties ({})...", mode_name);
    let turns = mod_turns::read_all(&config.turns_path)?;
    println!("  ✓ {} arc penalties", turns.penalties.len());

    println!("Loading EBG nodes...");
    let ebg_nodes = EbgNodesFile::read(&config.ebg_nodes_path)?;
    println!("  ✓ {} EBG nodes", ebg_nodes.n_nodes);

    // Apply traffic factors directly to the in-memory `weights.weights` array
    // (per-EBG-node travel-time in seconds, post-#297). The bottom-up customization
    // passes that follow then propagate the scaled originals through the
    // shortcut hierarchy.
    let traffic_skip_relax = if let Some(t) = traffic {
        let scale_start = std::time::Instant::now();
        // #294: weights.weights is Cow<[u32]>. Customization is a
        // build-time path that always owns the data; `to_mut()` is a
        // no-op on Owned and a copy-on-write on Borrowed.
        apply_traffic_to_node_weights(weights.weights.to_mut(), &ebg_nodes, t)?;
        println!(
            "  ✓ Applied '{}' speed factors to {} EBG node weights in {:.3}s",
            t.profile.name,
            weights.weights.len(),
            scale_start.elapsed().as_secs_f64()
        );
        t.skip_triangle_relax
    } else {
        false
    };

    // Build shared structures
    println!("\nBuilding sorted filtered EBG adjacency (parallel)...");
    let sorted_ebg = SortedFilteredEbgAdj::build(&filtered_ebg);
    println!("  ✓ Built sorted adjacency");

    let rank_to_filtered = &topo.rank_to_filtered;

    println!("Pre-sorting down edges by target rank (parallel)...");
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end {
                return Vec::new();
            }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();
    println!("  ✓ Pre-sorted down edges");

    println!("Building reverse DOWN adjacency...");
    let rev_down = build_reverse_down_adj_for_relax(&topo);
    println!("  ✓ {} entries", rev_down.sources.len());

    // ===================================================================
    // Bottom-up customization
    //
    // INVARIANT: Each bottom-up pass is internally sequential (rank order).
    // For traffic recustomization we only run TIME (distance is physical
    // and unchanged by traffic factors). For freeflow we run TIME + DIST
    // concurrently via rayon::join.
    // ===================================================================
    let bu_start = std::time::Instant::now();
    let (time_up, time_down, dist_pair_opt) = if traffic.is_some() {
        println!("\n⚡ Bottom-up customization (TIME only)...");
        let (tu, td) = bottom_up_customize(&topo, &sorted_down_indices, |u_rank, v_rank| {
            compute_original_weight_rank_aligned(
                u_rank,
                v_rank,
                &weights.weights,
                &turns.penalties,
                &sorted_ebg,
                &filtered_ebg.filtered_to_original,
                rank_to_filtered,
            )
        });
        (tu, td, None)
    } else {
        println!("\n⚡ Bottom-up customization (time + distance in parallel)...");
        let ((time_up, time_down), (dist_up, dist_down)) = rayon::join(
            || {
                bottom_up_customize(&topo, &sorted_down_indices, |u_rank, v_rank| {
                    compute_original_weight_rank_aligned(
                        u_rank,
                        v_rank,
                        &weights.weights,
                        &turns.penalties,
                        &sorted_ebg,
                        &filtered_ebg.filtered_to_original,
                        rank_to_filtered,
                    )
                })
            },
            || {
                bottom_up_customize(&topo, &sorted_down_indices, |_u_rank, v_rank| {
                    compute_distance_weight_rank_aligned(
                        v_rank,
                        &weights.weights,
                        &ebg_nodes.nodes,
                        &filtered_ebg.filtered_to_original,
                        rank_to_filtered,
                    )
                })
            },
        );
        (time_up, time_down, Some((dist_up, dist_down)))
    };
    println!("  ✓ Bottom-up in {:.2}s", bu_start.elapsed().as_secs_f64());

    // ===================================================================
    // Triangle relaxation (parallel internally via atomics)
    //
    // INVARIANT: relaxation only DECREASES weights (fetch_min).
    // For traffic recustomization with `skip_triangle_relax`, we keep the
    // original contraction middles — the resulting weights are valid upper
    // bounds (potentially loose by a few %), the trade-off being a ~30x
    // wall-time reduction for sub-second recustomization.
    // ===================================================================
    let (time_up, time_down, time_up_mid, time_down_mid) = if traffic_skip_relax {
        println!("\n🔺 Triangle relaxation for TIME: SKIPPED (traffic fast-path)");
        // Materialize the middles so they live as owned Vec<u32> matching
        // the relaxed branch's type.
        let up_mid: Vec<u32> = topo.up_middle.to_vec_u32();
        let down_mid: Vec<u32> = topo.down_middle.to_vec_u32();
        (time_up, time_down, up_mid, down_mid)
    } else if let Some((ref dist_up, ref dist_down)) = dist_pair_opt {
        // #529: non-traffic build — elect middles by (time, then
        // length-along-time). Seeds the length channel with the pre-relax
        // bottom-up DISTANCE weights (length along the contraction
        // decomposition), which are still owned by `dist_pair_opt` and
        // consumed unchanged by the DISTANCE relaxation below. TIME
        // weights are byte-identical to the time-only relaxation; only the
        // elected middles change (shortest length among equal-time apexes).
        println!("\n🔺 Triangle relaxation for TIME (parallel, #529 length tie-break)...");
        let tr_start = std::time::Instant::now();
        let (tu, td, tu_mid, td_mid, time_relax_count, time_relax_passes) =
            triangle_relax_lex_parallel(&topo, time_up, time_down, dist_up, dist_down, &rev_down);
        println!(
            "  ✓ {:.2}s, {} updates in {} passes",
            tr_start.elapsed().as_secs_f64(),
            time_relax_count,
            time_relax_passes
        );
        (tu, td, tu_mid, td_mid)
    } else {
        // Traffic recustomization (no DISTANCE channel available): keep the
        // time-only middle election. Traffic modes seldom tie on duration,
        // so the length tie-break is not needed here.
        println!("\n🔺 Triangle relaxation for TIME (parallel)...");
        let tr_start = std::time::Instant::now();
        let (tu, td, tu_mid, td_mid, time_relax_count, time_relax_passes) =
            triangle_relax_parallel(&topo, time_up, time_down, &rev_down);
        println!(
            "  ✓ {:.2}s, {} updates in {} passes",
            tr_start.elapsed().as_secs_f64(),
            time_relax_count,
            time_relax_passes
        );
        (tu, td, tu_mid, td_mid)
    };

    let dist_relaxed = match dist_pair_opt {
        Some((dist_up, dist_down)) => {
            println!("\n🔺 Triangle relaxation for DISTANCE (parallel)...");
            let tr_start = std::time::Instant::now();
            let (du, dd, _du_mid, _dd_mid, dist_relax_count, dist_relax_passes) =
                triangle_relax_parallel(&topo, dist_up, dist_down, &rev_down);
            println!(
                "  ✓ {:.2}s, {} updates in {} passes",
                tr_start.elapsed().as_secs_f64(),
                dist_relax_count,
                dist_relax_passes
            );
            Some((du, dd))
        }
        None => None,
    };

    // Length-along-time-shortest (#371/#372). For every CCH edge, the
    // sum of physical edge lengths along the time-optimal expansion
    // (using `time_*_mid` as the chosen middles). This is the
    // metric `/table`, `/trip`, and Flight matrix endpoints must
    // report as `distance` so the number belongs to the same path as
    // the duration — matching what `/route` already produces by
    // per-cell unpacking. The on-disk file `cch.lat.<mode>.u32` is
    // written alongside the existing `cch.d.<mode>.u32`; consumers
    // migrate in #372.
    let lat_pair = if dist_relaxed.is_some() {
        println!("\n📏 Length-along-time-shortest customization...");
        let lat_start = std::time::Instant::now();
        let (lat_up, lat_down) = bottom_up_with_external_middles(
            &topo,
            &sorted_down_indices,
            &time_up_mid,
            &time_down_mid,
            |_u_rank, v_rank| {
                compute_distance_weight_rank_aligned(
                    v_rank,
                    &weights.weights,
                    &ebg_nodes.nodes,
                    &filtered_ebg.filtered_to_original,
                    rank_to_filtered,
                )
            },
        );
        println!(
            "  ✓ {:.2}s — {} up entries, {} down entries",
            lat_start.elapsed().as_secs_f64(),
            lat_up.len(),
            lat_down.len()
        );
        Some((lat_up, lat_down))
    } else {
        None
    };

    // Sanity checks
    sanity_check_weights(&topo, &time_up, &time_down, "Time", 95.0)?;
    if let Some((ref du, ref dd)) = dist_relaxed {
        sanity_check_weights_simple(du, dd, "Distance", 95.0)?;
    }
    if let Some((ref lu, ref ld)) = lat_pair {
        sanity_check_weights_simple(lu, ld, "Length-along-time", 95.0)?;
    }

    // Write outputs
    std::fs::create_dir_all(&config.outdir)?;

    // Output filename — traffic variants get a `_<variant>` suffix
    // unless `--bake-as-base` was passed, in which case the variant
    // overwrites the base `cch.w.<mode>.u32`.
    let weight_suffix = match traffic {
        Some(t) if !config.bake_traffic_as_base => format!("{}_{}", mode_name, t.profile.name),
        Some(_) | None => mode_name.clone(),
    };
    let output_path = config.outdir.join(format!("cch.w.{}.u32", weight_suffix));
    println!("\nWriting time weights...");
    write_cch_weights(
        &output_path,
        &time_up,
        &time_down,
        &time_up_mid,
        &time_down_mid,
        config.mode,
    )?;
    println!("  ✓ Written {}", output_path.display());

    let distance_output_path = if let Some((dist_up, dist_down)) = dist_relaxed {
        let p = config.outdir.join(format!("cch.d.{}.u32", mode_name));
        println!("Writing distance weights...");
        let topo_up_mid: Vec<u32> = topo.up_middle.to_vec_u32();
        let topo_down_mid: Vec<u32> = topo.down_middle.to_vec_u32();
        write_cch_weights(
            &p,
            &dist_up,
            &dist_down,
            &topo_up_mid,
            &topo_down_mid,
            config.mode,
        )?;
        println!("  ✓ Written {}", p.display());
        p
    } else {
        // Traffic path: distance is unchanged, no new file written. We
        // surface the freeflow path so callers / lock files have a stable
        // reference, but it MUST already exist (server still reads it).
        config.outdir.join(format!("cch.d.{}.u32", mode_name))
    };

    // #371/#372: length-along-time-shortest weights. Same on-disk
    // shape as cch.d (same `write_cch_weights`); new file name so
    // both files coexist during migration. Reuses the time-optimal
    // middles since the metric is derived from the same path.
    if let Some((lat_up, lat_down)) = lat_pair {
        let p = config.outdir.join(format!("cch.lat.{}.u32", mode_name));
        println!("Writing length-along-time weights...");
        write_cch_weights(
            &p,
            &lat_up,
            &lat_down,
            &time_up_mid,
            &time_down_mid,
            config.mode,
        )?;
        println!("  ✓ Written {}", p.display());
    }

    // For traffic variants, also drop a sibling `.traffic.json` next to the
    // weight file for provenance — the server validates this on boot.
    if let Some(t) = traffic {
        let provenance_path = config
            .outdir
            .join(format!("cch.w.{}.traffic.json", weight_suffix));
        std::fs::write(&provenance_path, t.profile.to_json_string()?)?;
        println!("  ✓ Written {}", provenance_path.display());
    }

    let customize_time_ms = start_time.elapsed().as_millis() as u64;

    Ok(Step8Result {
        output_path,
        distance_output_path,
        mode: config.mode,
        mode_name: config.mode_name.clone(),
        n_up_edges: n_up as u64,
        n_down_edges: n_down as u64,
        customize_time_ms,
    })
}

/// Serve-boot TIME-only CCH recustomization, fully in memory.
///
/// Mirrors the TIME path of [`customize_cch`] exactly — same bottom-up +
/// triangle-relaxation leaf functions — but takes already-parsed inputs and
/// RETURNS the customized TIME weights as [`CchWeights`] instead of writing
/// `cch.w.<mode>.u32` to disk. Distance and length-along-time are physical and
/// unaffected by traffic, so the caller keeps the base mode's dist/lat weights
/// (the serve clones them from the base `ModeData`).
///
/// `traffic` = `(profile, way_attrs, nbg_geo)`: when `Some`, per-density-class
/// speed factors are applied to a PRIVATE clone of the node time-weights before
/// contraction (`node_weights_time` is borrowed read-only and left untouched).
/// Triangle relaxation is ALWAYS run — serving requires exact shortcut weights,
/// so the `skip_triangle_relax` dev fast-path is deliberately unavailable here.
///
/// Determinism: for identical inputs the returned `(up, down, up_middle,
/// down_middle)` values are element-for-element equal to what the CLI
/// `customize_cch` writes to `cch.w.<mode>.u32` (pinned by the
/// `customize_in_memory_matches_cli` round-trip test). The in-memory
/// `WeightArray`s use u32 storage rather than the on-disk narrowest width, but
/// that is a storage detail invisible to the value-level consumers (the
/// `*AdjFlat` builders and CCH search).
pub fn customize_cch_time_in_memory(
    topo: &CchTopo,
    filtered_ebg: &crate::formats::FilteredEbg,
    node_weights_time: &[u32],
    turn_penalties: &[u32],
    ebg_nodes: &EbgNodes,
    traffic: Option<(
        &crate::traffic::TrafficProfile,
        &[way_attrs::WayAttr],
        &crate::formats::NbgGeo,
    )>,
) -> Result<(CchWeights, Vec<u32>)> {
    let n_nodes = topo.n_nodes as usize;

    // Apply traffic to a private copy of the node time-weights — the caller's
    // slice (a container section) is borrowed read-only.
    let mut node_weights: Vec<u32> = node_weights_time.to_vec();
    if let Some((profile, way_attrs_slice, nbg_geo)) = traffic {
        apply_traffic_to_node_weights_in_memory(
            &mut node_weights,
            ebg_nodes,
            profile,
            way_attrs_slice,
            nbg_geo,
        )?;
    }

    // Shared structures — identical construction to the CLI TIME path.
    let sorted_ebg = SortedFilteredEbgAdj::build(filtered_ebg);
    let rank_to_filtered = &topo.rank_to_filtered;
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end {
                return Vec::new();
            }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();
    let rev_down = build_reverse_down_adj_for_relax(topo);

    // Bottom-up TIME customization.
    let (time_up, time_down) = bottom_up_customize(topo, &sorted_down_indices, |u_rank, v_rank| {
        compute_original_weight_rank_aligned(
            u_rank,
            v_rank,
            &node_weights,
            turn_penalties,
            &sorted_ebg,
            &filtered_ebg.filtered_to_original,
            rank_to_filtered,
        )
    });

    // Triangle relaxation — ALWAYS run (correctness-critical for serving).
    let (time_up, time_down, time_up_mid, time_down_mid, _relax_count, _relax_passes) =
        triangle_relax_parallel(topo, time_up, time_down, &rev_down);

    sanity_check_weights(topo, &time_up, &time_down, "Time", 95.0)?;

    Ok((
        CchWeights {
            up: WeightArray::from_vec_u32(time_up),
            down: WeightArray::from_vec_u32(time_down),
            up_middle: ArcCow::from_vec(time_up_mid),
            down_middle: ArcCow::from_vec(time_down_mid),
        },
        node_weights,
    ))
}

/// Apply per-density-class speed factors to the in-memory time-weight array.
///
/// For every accessible EBG node `n`:
///   - `way_id = nbg_geo.edges[ebg_nodes[n].geom_idx].first_osm_way_id`
///   - `(highway, class) = way_attrs[way_id].{highway_class, density_class}`
///   - `factor = profile.factor_for_cell(highway, class)` — the
///     `(highway_class × density)` matrix cell when the profile has one
///     (#428), else the per-density vector factor
///   - `weights[n] = round(weights[n] / factor)` saturating at u32::MAX,
///     preserving the `0 = inaccessible` sentinel.
///
/// Inaccessible nodes (weight 0) stay zero. Wall-time bound: O(n_ebg).
fn apply_traffic_to_node_weights(
    weights: &mut [u32],
    ebg_nodes: &EbgNodes,
    traffic: &TrafficCustomization,
) -> Result<()> {
    println!("\nLoading traffic profile inputs...");
    let way_attrs_vec = way_attrs::read_all(&traffic.way_attrs_path)?;
    println!(
        "  ✓ {} way attrs from {}",
        way_attrs_vec.len(),
        traffic.way_attrs_path.display()
    );
    let nbg_geo = NbgGeoFile::read(&traffic.nbg_geo_path)?;
    println!(
        "  ✓ {} nbg edges from {}",
        nbg_geo.edges.len(),
        traffic.nbg_geo_path.display()
    );

    apply_traffic_to_node_weights_in_memory(
        weights,
        ebg_nodes,
        &traffic.profile,
        &way_attrs_vec,
        &nbg_geo,
    )
}

/// In-memory core of [`apply_traffic_to_node_weights`]: scale the per-EBG-node
/// time-weights by the profile's per-density-class factors given the
/// already-parsed way attributes + nbg geometry (no file reads, no profile
/// path resolution).
///
/// Used by the build-time CLI path (via the file-reading shell above) AND by
/// the serve-boot recustomization, which feeds in structs decoded from the
/// `.butterfly` container sections. The loop is byte-for-byte identical to the
/// pre-split CLI logic, so the built artifact is unchanged.
pub(crate) fn apply_traffic_to_node_weights_in_memory(
    weights: &mut [u32],
    ebg_nodes: &EbgNodes,
    profile: &crate::traffic::TrafficProfile,
    way_attrs_slice: &[way_attrs::WayAttr],
    nbg_geo: &crate::formats::NbgGeo,
) -> Result<()> {
    use crate::density::DensityClass;
    use std::collections::HashMap;

    // way_id -> (highway_class, density_class) lookup. The highway class is
    // the model-defined u16 code step 2 stored in way_attrs (#428).
    let way_cell: HashMap<i64, (u16, u8)> = way_attrs_slice
        .iter()
        .map(|w| (w.way_id, (w.output.highway_class, w.output.density_class)))
        .collect();

    // Pre-compute the inverse factors as fixed-point rationals to avoid f32
    // rounding drift across runs. Since factor f ∈ [0.1, 1.5], 1/f ∈
    // [0.667, 10.0] which fits comfortably in f64.
    let inv_factors: [f64; 5] = std::array::from_fn(|i| {
        let class = DensityClass::from_u8(i as u8);
        1.0 / profile.factor_for(class) as f64
    });

    // (highway_class × density) matrix rows resolved to inverse factors, with
    // unspecified cells pre-filled from the vector (#428). Empty when the
    // profile has no matrix — the per-node lookup then always falls through
    // to `inv_factors`, reproducing the pre-#428 behavior bit-for-bit. Each
    // row carries a hit counter so rows whose highway code never occurs in
    // this graph can be flagged after the pass.
    let mut inv_matrix: HashMap<u16, ([f64; 5], u64)> = profile
        .matrix
        .iter()
        .map(|(code, row)| {
            let mut inv = inv_factors;
            for (i, cell) in row.iter().enumerate() {
                if let Some(f) = cell {
                    inv[i] = 1.0 / *f as f64;
                }
            }
            (*code, (inv, 0u64))
        })
        .collect();

    let mut adjusted = 0usize;
    let mut missing_way = 0usize;

    anyhow::ensure!(
        weights.len() == ebg_nodes.nodes.len(),
        "weights len {} mismatches EBG node count {}",
        weights.len(),
        ebg_nodes.nodes.len()
    );

    for (i, node) in ebg_nodes.nodes.iter().enumerate() {
        if weights[i] == 0 {
            // Inaccessible — preserve sentinel.
            continue;
        }
        let geom_idx = node.geom_idx as usize;
        if geom_idx >= nbg_geo.edges.len() {
            missing_way += 1;
            continue;
        }
        let way_id = nbg_geo.edges[geom_idx].first_osm_way_id;
        let inv = match way_cell.get(&way_id) {
            Some((highway, class)) => {
                let class_idx = (*class as usize).min(4);
                match inv_matrix.get_mut(highway) {
                    Some((row, hits)) => {
                        *hits += 1;
                        row[class_idx]
                    }
                    None => inv_factors[class_idx],
                }
            }
            None => {
                // Treat unknown ways as Suburban (neutral) on the vector —
                // their highway class is unknown, so no matrix row applies.
                missing_way += 1;
                inv_factors[3]
            }
        };
        // weight / factor = weight * (1 / factor). Keep ≥ 1 to preserve the
        // accessibility invariant (only 0 means inaccessible).
        let scaled = (weights[i] as f64 * inv).round();
        let scaled_u = if scaled >= u32::MAX as f64 {
            u32::MAX
        } else if scaled < 1.0 {
            1
        } else {
            scaled as u32
        };
        if scaled_u != weights[i] {
            adjusted += 1;
        }
        weights[i] = scaled_u;
    }

    println!(
        "  Adjusted {} weights ({:.1}%); missing way lookup: {}",
        adjusted,
        100.0 * adjusted as f64 / weights.len() as f64,
        missing_way
    );

    // Flag matrix rows whose highway code matched zero ways: the row is dead
    // weight, and a profile full of dead rows usually means it was calibrated
    // against a different model's highway_class table. Iterate the profile's
    // BTreeMap so the output order is deterministic.
    for code in profile.matrix.keys() {
        let hits = inv_matrix.get(code).map_or(0, |(_, h)| *h);
        if hits == 0 {
            eprintln!(
                "  ⚠️  traffic matrix row {code} matched zero ways — highway_class \
                 code {code} does not occur in this graph's way_attrs (profile \
                 calibrated against a different model, or stale?)"
            );
        }
    }

    Ok(())
}

// ===================================================================
// Reusable customization building blocks
// ===================================================================

/// Reverse DOWN adjacency for triangle relaxation.
/// For each node m, stores all incoming DOWN edges x→m.
struct ReverseDownAdj {
    offsets: Vec<u64>,
    sources: Vec<u32>,
    edge_idx: Vec<usize>,
}

fn build_reverse_down_adj_for_relax(topo: &CchTopo) -> ReverseDownAdj {
    let n_nodes = topo.n_nodes as usize;

    let mut counts = vec![0u64; n_nodes];
    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            counts[topo.down_targets[i] as usize] += 1;
        }
    }

    let mut offsets = vec![0u64; n_nodes + 1];
    for m in 0..n_nodes {
        offsets[m + 1] = offsets[m] + counts[m];
    }

    let total = offsets[n_nodes] as usize;
    let mut sources = vec![0u32; total];
    let mut edge_idx = vec![0usize; total];
    let mut insert = vec![0u64; n_nodes];

    for u in 0..n_nodes {
        let start = topo.down_offsets[u] as usize;
        let end = topo.down_offsets[u + 1] as usize;
        for i in start..end {
            let m = topo.down_targets[i] as usize;
            let pos = (offsets[m] + insert[m]) as usize;
            sources[pos] = u as u32;
            edge_idx[pos] = i;
            insert[m] += 1;
        }
    }

    ReverseDownAdj {
        offsets,
        sources,
        edge_idx,
    }
}

/// Generic bottom-up CCH customization.
///
/// INVARIANT: Processes ranks in ascending order (sequential, NOT parallel).
/// For each rank u:
///   1. DOWN edges sorted by target rank (ensures u→m done before u→v when rank(m) < rank(v))
///   2. UP edges after DOWN (UP shortcuts need down_weights[u→m])
///
/// `orig_weight_fn(u_rank, v_rank) -> u32` provides original edge weight.
/// Shortcuts always use: weight(u→m) + weight(m→v) via stored middle node.
/// Bottom-up customize using EXTERNAL middles (e.g. the post-triangle-
/// relax time-optimal middles), to compute "length along the time-
/// shortest path" per shortcut for #371/#372.
///
/// For non-shortcut edges, `orig_weight_fn(u, v)` returns the physical
/// edge length (mode-independent). For shortcut edges, the value is
/// recursively `w[u→m] + w[m→v]` where `m` is the supplied external
/// middle for that shortcut (the time-optimal apex, NOT
/// `topo.{up,down}_middle` which holds the contraction-time middle
/// pre-relax).
///
/// Iteration order (rank ascending, DOWN then UP within each rank)
/// matches `bottom_up_customize`, so the recursive dependency holds:
/// when we visit a shortcut at rank u, all sub-edges `u→m` (down within
/// rank u, processed first by target-rank sort) and `m→v` (up from m,
/// `m < u` so processed in an earlier outer iteration) already have
/// their length-along-time computed.
/// #528: recompute the length-along-time-shortest weights for a set of TIME
/// middles produced by an in-memory (re)customization. The base build-time
/// `cch.lat.<mode>.u32` describes the CLEAN time-shortest paths; after boot
/// recustomization (#454/#524) the time middles change, so the length that
/// belongs to the new time-optimal expansion must be recomputed — otherwise
/// 2-channel `/table` / Flight `matrix` report distances from the OLD paths.
/// Physical edge lengths are traffic-invariant; only which path is optimal
/// changes, which is exactly what the (up/down) middles encode.
pub fn recompute_len_along_time_from_middles(
    topo: &CchTopo,
    filtered_to_original: &[u32],
    ebg_nodes: &EbgNodes,
    node_weights_time: &[u32],
    up_middle: &[u32],
    down_middle: &[u32],
) -> CchWeights {
    let n_nodes = topo.n_nodes as usize;
    let rank_to_filtered = &topo.rank_to_filtered;
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end {
                return Vec::new();
            }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();
    let (lat_up, lat_down) = bottom_up_with_external_middles(
        topo,
        &sorted_down_indices,
        up_middle,
        down_middle,
        |_u_rank, v_rank| {
            compute_distance_weight_rank_aligned(
                v_rank,
                node_weights_time,
                &ebg_nodes.nodes,
                filtered_to_original,
                rank_to_filtered,
            )
        },
    );
    CchWeights {
        up: WeightArray::from_vec_u32(lat_up),
        down: WeightArray::from_vec_u32(lat_down),
        up_middle: ArcCow::from_vec(up_middle.to_vec()),
        down_middle: ArcCow::from_vec(down_middle.to_vec()),
    }
}

pub fn bottom_up_with_external_middles(
    topo: &CchTopo,
    sorted_down_indices: &[Vec<usize>],
    external_up_mid: &[u32],
    external_down_mid: &[u32],
    orig_weight_fn: impl Fn(usize, usize) -> u32,
) -> (Vec<u32>, Vec<u32>) {
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();

    assert_eq!(external_up_mid.len(), n_up);
    assert_eq!(external_down_mid.len(), n_down);

    let mut up_weights = vec![u32::MAX; n_up];
    let mut down_weights = vec![u32::MAX; n_down];

    for rank in 0..n_nodes {
        let u = rank;

        for &i in &sorted_down_indices[u] {
            let v = topo.down_targets[i] as usize;
            if !topo.down_is_shortcut.bit(i) {
                down_weights[i] = orig_weight_fn(u, v);
            } else {
                let m = external_down_mid[i] as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                down_weights[i] = w_um.saturating_add(w_mv);
            }
        }

        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i] as usize;
            if !topo.up_is_shortcut.bit(i) {
                up_weights[i] = orig_weight_fn(u, v);
            } else {
                let m = external_up_mid[i] as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                up_weights[i] = w_um.saturating_add(w_mv);
            }
        }
    }

    (up_weights, down_weights)
}

fn bottom_up_customize(
    topo: &CchTopo,
    sorted_down_indices: &[Vec<usize>],
    orig_weight_fn: impl Fn(usize, usize) -> u32,
) -> (Vec<u32>, Vec<u32>) {
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();

    let mut up_weights = vec![u32::MAX; n_up];
    let mut down_weights = vec![u32::MAX; n_down];

    for rank in 0..n_nodes {
        let u = rank;

        // PHASE 1: DOWN edges (sorted by target rank for correct dependency order)
        for &i in &sorted_down_indices[u] {
            let v = topo.down_targets[i] as usize;
            if !topo.down_is_shortcut.bit(i) {
                down_weights[i] = orig_weight_fn(u, v);
            } else {
                let m = topo.down_middle.get(i) as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                down_weights[i] = w_um.saturating_add(w_mv);
            }
        }

        // PHASE 2: UP edges (all down_weights[u→*] are now computed)
        let up_start = topo.up_offsets[u] as usize;
        let up_end = topo.up_offsets[u + 1] as usize;
        for i in up_start..up_end {
            let v = topo.up_targets[i] as usize;
            if !topo.up_is_shortcut.bit(i) {
                up_weights[i] = orig_weight_fn(u, v);
            } else {
                let m = topo.up_middle.get(i) as usize;
                let w_um =
                    find_edge_weight(u, m, &topo.down_offsets, &topo.down_targets, &down_weights);
                let w_mv = find_edge_weight(m, v, &topo.up_offsets, &topo.up_targets, &up_weights);
                up_weights[i] = w_um.saturating_add(w_mv);
            }
        }
    }

    (up_weights, down_weights)
}

/// Pack (weight, middle_rank) into a single u64 for atomic fetch_min.
/// Weight in high 32 bits so fetch_min minimizes by weight first.
#[inline]
fn pack_wm(weight: u32, middle: u32) -> u64 {
    ((weight as u64) << 32) | (middle as u64)
}

#[inline]
fn unpack_weight(packed: u64) -> u32 {
    (packed >> 32) as u32
}

#[inline]
fn unpack_middle(packed: u64) -> u32 {
    packed as u32
}

/// Parallel triangle relaxation using atomic fetch_min on packed (weight, middle).
///
/// For each apex m (processed in parallel), relaxes edges x→y where:
///   - x→m is a DOWN edge from x (rank[x] > rank[m])
///   - m→y is an UP edge from m (rank[y] > rank[m])
///   - w(x,y) = min(w(x,y), w(x,m) + w(m,y))
///
/// CRITICAL: When a better weight is found through apex m, the middle node is
/// updated atomically alongside the weight. This ensures path unpacking follows
/// the OPTIMAL middle, not the original contraction middle.
///
/// Returns (up_weights, down_weights, up_middles, down_middles, total_relaxations, passes).
fn triangle_relax_parallel(
    topo: &CchTopo,
    up_weights: Vec<u32>,
    down_weights: Vec<u32>,
    rev_down: &ReverseDownAdj,
) -> (Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>, u64, u32) {
    let n_nodes = topo.n_nodes as usize;

    // Pack (weight, middle) into AtomicU64 for lock-free update of both.
    // `topo.{up,down}_middle` are now [`WeightArray`] whose iterator
    // yields `u32` by value (not `&u32`), so the lambda binds `m` plain.
    let atomic_up: Vec<AtomicU64> = up_weights
        .iter()
        .zip(topo.up_middle.iter())
        .map(|(&w, m)| AtomicU64::new(pack_wm(w, m)))
        .collect();
    let atomic_down: Vec<AtomicU64> = down_weights
        .iter()
        .zip(topo.down_middle.iter())
        .map(|(&w, m)| AtomicU64::new(pack_wm(w, m)))
        .collect();

    let mut total_relaxations = 0u64;
    let mut pass = 0u32;

    loop {
        pass += 1;
        let pass_updates = AtomicU64::new(0);

        // Process all apexes in parallel
        (0..n_nodes).into_par_iter().for_each(|m| {
            let rev_start = rev_down.offsets[m] as usize;
            let rev_end = rev_down.offsets[m + 1] as usize;

            for i_rev in rev_start..rev_end {
                let x = rev_down.sources[i_rev] as usize;
                let edge_idx_xm = rev_down.edge_idx[i_rev];
                let w_xm = unpack_weight(atomic_down[edge_idx_xm].load(Ordering::Relaxed));

                if w_xm == u32::MAX {
                    continue;
                }

                let up_start = topo.up_offsets[m] as usize;
                let up_end = topo.up_offsets[m + 1] as usize;

                for i_my in up_start..up_end {
                    let y = topo.up_targets[i_my] as usize;
                    if y == x {
                        continue;
                    }

                    let w_my = unpack_weight(atomic_up[i_my].load(Ordering::Relaxed));
                    if w_my == u32::MAX {
                        continue;
                    }

                    let new_weight = w_xm.saturating_add(w_my);
                    let new_packed = pack_wm(new_weight, m as u32);

                    if y > x {
                        // UP edge from x
                        if let Some(idx) = find_edge_index(x, y, &topo.up_offsets, &topo.up_targets)
                        {
                            let old = atomic_up[idx].fetch_min(new_packed, Ordering::Relaxed);
                            if new_packed < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        // DOWN edge from x
                        if let Some(idx) =
                            find_edge_index(x, y, &topo.down_offsets, &topo.down_targets)
                        {
                            let old = atomic_down[idx].fetch_min(new_packed, Ordering::Relaxed);
                            if new_packed < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    }
                }
            }
        });

        let pu = pass_updates.into_inner();
        println!("  Pass {}: {} updates", pass, pu);
        total_relaxations += pu;

        if pu == 0 {
            break;
        }
        if pass >= 100 {
            panic!(
                "CCH customization did not converge after 100 passes ({} updates in last pass). This indicates a bug in the contraction hierarchy.",
                pu
            );
        }
    }

    let up: Vec<u32> = atomic_up
        .iter()
        .map(|a| unpack_weight(a.load(Ordering::Relaxed)))
        .collect();
    let down: Vec<u32> = atomic_down
        .iter()
        .map(|a| unpack_weight(a.load(Ordering::Relaxed)))
        .collect();
    let up_mid: Vec<u32> = atomic_up
        .iter()
        .map(|a| unpack_middle(a.load(Ordering::Relaxed)))
        .collect();
    let down_mid: Vec<u32> = atomic_down
        .iter()
        .map(|a| unpack_middle(a.load(Ordering::Relaxed)))
        .collect();

    (up, down, up_mid, down_mid, total_relaxations, pass)
}

/// Pack a (time, length) pair into a u64 for lexicographic `fetch_min`
/// (time in the high 32 bits, length in the low 32). #529.
#[inline]
fn pack_tl(time: u32, len: u32) -> u64 {
    ((time as u64) << 32) | (len as u64)
}

#[inline]
fn tl_time(packed: u64) -> u32 {
    (packed >> 32) as u32
}

#[inline]
fn tl_len(packed: u64) -> u32 {
    packed as u32
}

/// #529: TIME triangle relaxation with a (time, then length-along-time)
/// lexicographic middle election.
///
/// The returned `up`/`down` TIME weights are identical to what
/// [`triangle_relax_parallel`] produces (time is the PRIMARY key, so the
/// length tie-break can never change a duration). Only the elected
/// middles differ: among equal-time apexes we keep the one whose
/// expansion has the SHORTEST length-along-time, with the smallest apex
/// index as the final deterministic tie-break. This makes `/route`
/// (which unpacks via these middles) and the 2-channel `/table`/matrix
/// (which reads the length-along-time weights derived from these middles)
/// agree on distance even when one-way-agnostic modes tie on duration.
///
/// Mechanism: pack (time, length) into one `AtomicU64` and `fetch_min`.
/// Components are summed SEPARATELY (each `saturating_add`) and only
/// packed for the comparison, so a length sum can never carry into the
/// time field. `fetch_min` on the packed value is then exactly the
/// lexicographic min of (time, length); (min-lex, +) is a valid
/// shortest-path semiring because lexicographic order is preserved under
/// componentwise addition, so the fixpoint converges to the lex-min
/// (time, length) of every edge.
///
/// The middle is deliberately NOT tracked during relaxation — writing it
/// alongside the packed `fetch_min` would race (a stale apex could be
/// paired with the winning weight). Instead, after convergence a
/// deterministic recovery pass elects, per shortcut edge, the SMALLEST
/// apex whose two halves' packed values sum to the edge's converged
/// packed value. That pass reads only converged (immutable) data, so it
/// is race-free and reproducible.
///
/// `len_up`/`len_down` seed the length channel and MUST be the bottom-up
/// length-along-contraction-middle weights (the pre-distance-relax
/// values), so each seed `(time, length)` pair describes ONE real path
/// (the contraction decomposition) and is therefore a consistent witness.
fn triangle_relax_lex_parallel(
    topo: &CchTopo,
    time_up: Vec<u32>,
    time_down: Vec<u32>,
    len_up: &[u32],
    len_down: &[u32],
    rev_down: &ReverseDownAdj,
) -> (Vec<u32>, Vec<u32>, Vec<u32>, Vec<u32>, u64, u32) {
    let n_nodes = topo.n_nodes as usize;

    let atomic_up: Vec<AtomicU64> = time_up
        .iter()
        .zip(len_up.iter())
        .map(|(&t, &l)| AtomicU64::new(pack_tl(t, l)))
        .collect();
    let atomic_down: Vec<AtomicU64> = time_down
        .iter()
        .zip(len_down.iter())
        .map(|(&t, &l)| AtomicU64::new(pack_tl(t, l)))
        .collect();

    let mut total_relaxations = 0u64;
    let mut pass = 0u32;

    loop {
        pass += 1;
        let pass_updates = AtomicU64::new(0);

        (0..n_nodes).into_par_iter().for_each(|m| {
            let rev_start = rev_down.offsets[m] as usize;
            let rev_end = rev_down.offsets[m + 1] as usize;

            for i_rev in rev_start..rev_end {
                let x = rev_down.sources[i_rev] as usize;
                let edge_idx_xm = rev_down.edge_idx[i_rev];
                let p_xm = atomic_down[edge_idx_xm].load(Ordering::Relaxed);
                let t_xm = tl_time(p_xm);
                if t_xm == u32::MAX {
                    continue;
                }
                let l_xm = tl_len(p_xm);

                let up_start = topo.up_offsets[m] as usize;
                let up_end = topo.up_offsets[m + 1] as usize;

                for i_my in up_start..up_end {
                    let y = topo.up_targets[i_my] as usize;
                    if y == x {
                        continue;
                    }
                    let p_my = atomic_up[i_my].load(Ordering::Relaxed);
                    let t_my = tl_time(p_my);
                    if t_my == u32::MAX {
                        continue;
                    }

                    // Components summed separately (no cross-field carry),
                    // then packed only for the lexicographic comparison.
                    let new_time = t_xm.saturating_add(t_my);
                    let new_len = l_xm.saturating_add(tl_len(p_my));
                    let new_packed = pack_tl(new_time, new_len);

                    if y > x {
                        if let Some(idx) = find_edge_index(x, y, &topo.up_offsets, &topo.up_targets)
                        {
                            let old = atomic_up[idx].fetch_min(new_packed, Ordering::Relaxed);
                            if new_packed < old {
                                pass_updates.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else if let Some(idx) =
                        find_edge_index(x, y, &topo.down_offsets, &topo.down_targets)
                    {
                        let old = atomic_down[idx].fetch_min(new_packed, Ordering::Relaxed);
                        if new_packed < old {
                            pass_updates.fetch_add(1, Ordering::Relaxed);
                        }
                    }
                }
            }
        });

        let pu = pass_updates.into_inner();
        println!("  Pass {}: {} updates", pass, pu);
        total_relaxations += pu;

        if pu == 0 {
            break;
        }
    }

    // ---- Deterministic middle recovery (race-free) -------------------
    // For every shortcut edge, elect the SMALLEST apex whose two halves'
    // converged packed values sum EXACTLY to the edge's converged packed
    // value. Reads only immutable converged atomics, so `fetch_min` on the
    // apex index is fully deterministic (smallest apex wins).
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();
    let mid_up: Vec<AtomicU32> = (0..n_up).map(|_| AtomicU32::new(u32::MAX)).collect();
    let mid_down: Vec<AtomicU32> = (0..n_down).map(|_| AtomicU32::new(u32::MAX)).collect();

    (0..n_nodes).into_par_iter().for_each(|m| {
        let rev_start = rev_down.offsets[m] as usize;
        let rev_end = rev_down.offsets[m + 1] as usize;
        for i_rev in rev_start..rev_end {
            let x = rev_down.sources[i_rev] as usize;
            let edge_idx_xm = rev_down.edge_idx[i_rev];
            let p_xm = atomic_down[edge_idx_xm].load(Ordering::Relaxed);
            let t_xm = tl_time(p_xm);
            if t_xm == u32::MAX {
                continue;
            }
            let l_xm = tl_len(p_xm);

            let up_start = topo.up_offsets[m] as usize;
            let up_end = topo.up_offsets[m + 1] as usize;
            for i_my in up_start..up_end {
                let y = topo.up_targets[i_my] as usize;
                if y == x {
                    continue;
                }
                let p_my = atomic_up[i_my].load(Ordering::Relaxed);
                let t_my = tl_time(p_my);
                if t_my == u32::MAX {
                    continue;
                }
                let cand = pack_tl(t_xm.saturating_add(t_my), l_xm.saturating_add(tl_len(p_my)));

                if y > x {
                    if let Some(idx) = find_edge_index(x, y, &topo.up_offsets, &topo.up_targets)
                        && topo.up_is_shortcut.bit(idx)
                        && cand == atomic_up[idx].load(Ordering::Relaxed)
                    {
                        mid_up[idx].fetch_min(m as u32, Ordering::Relaxed);
                    }
                } else if let Some(idx) =
                    find_edge_index(x, y, &topo.down_offsets, &topo.down_targets)
                    && topo.down_is_shortcut.bit(idx)
                    && cand == atomic_down[idx].load(Ordering::Relaxed)
                {
                    mid_down[idx].fetch_min(m as u32, Ordering::Relaxed);
                }
            }
        }
    });

    let up: Vec<u32> = atomic_up
        .iter()
        .map(|a| tl_time(a.load(Ordering::Relaxed)))
        .collect();
    let down: Vec<u32> = atomic_down
        .iter()
        .map(|a| tl_time(a.load(Ordering::Relaxed)))
        .collect();
    // Shortcut edges take the recovered lex-min-length apex; non-shortcut
    // edges keep the contraction middle (unpack ignores it — it gates on
    // is_shortcut first — but we preserve the array shape). A shortcut with
    // no recovered apex (must not happen) falls back to the contraction
    // middle rather than an invalid u32::MAX.
    let up_mid: Vec<u32> = (0..n_up)
        .map(|i| {
            let m = mid_up[i].load(Ordering::Relaxed);
            if m != u32::MAX {
                m
            } else {
                topo.up_middle.get(i)
            }
        })
        .collect();
    let down_mid: Vec<u32> = (0..n_down)
        .map(|i| {
            let m = mid_down[i].load(Ordering::Relaxed);
            if m != u32::MAX {
                m
            } else {
                topo.down_middle.get(i)
            }
        })
        .collect();

    (up, down, up_mid, down_mid, total_relaxations, pass)
}

// ===================================================================
// Original edge weight functions
// ===================================================================

/// Compute time weight for an original edge in rank-aligned CCH.
/// Converts rank → filtered_id → original_id for weight + turn penalty lookup.
#[inline]
fn compute_original_weight_rank_aligned(
    u_rank: usize,
    v_rank: usize,
    node_weights: &[u32],
    turn_penalties: &[u32],
    sorted_ebg: &SortedFilteredEbgAdj,
    filtered_to_original: &[u32],
    rank_to_filtered: &[u32],
) -> u32 {
    let u_filtered = rank_to_filtered[u_rank] as usize;
    let v_filtered = rank_to_filtered[v_rank] as usize;
    let original_v = filtered_to_original[v_filtered] as usize;
    let w_v = node_weights[original_v];

    if w_v == 0 {
        return u32::MAX;
    }

    match sorted_ebg.find_original_arc_index(u_filtered, v_filtered as u32) {
        Some(orig_arc_idx) => w_v.saturating_add(turn_penalties[orig_arc_idx as usize]),
        None => u32::MAX,
    }
}

/// Compute distance weight for an original edge in rank-aligned CCH.
/// Distance = length_m (physical distance, mode-independent).
/// Accessibility uses same check as time: node_weights[v] == 0 → inaccessible.
/// No turn penalties for distance.
#[inline]
fn compute_distance_weight_rank_aligned(
    v_rank: usize,
    node_weights: &[u32], // Time weights, for accessibility check only
    ebg_nodes: &[crate::formats::ebg_nodes::EbgNode],
    filtered_to_original: &[u32],
    rank_to_filtered: &[u32],
) -> u32 {
    let v_filtered = rank_to_filtered[v_rank] as usize;
    let original_v = filtered_to_original[v_filtered] as usize;

    if node_weights[original_v] == 0 {
        return u32::MAX;
    }

    ebg_nodes[original_v].length_m
}

// ===================================================================
// CCH CSR lookup helpers
// ===================================================================

#[inline]
fn find_edge_weight(u: usize, v: usize, offsets: &[u64], targets: &[u32], weights: &[u32]) -> u32 {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    if start >= end {
        return u32::MAX;
    }
    match targets[start..end].binary_search(&(v as u32)) {
        Ok(idx) => weights[start + idx],
        Err(_) => u32::MAX,
    }
}

#[inline]
fn find_edge_index(u: usize, v: usize, offsets: &[u64], targets: &[u32]) -> Option<usize> {
    let start = offsets[u] as usize;
    let end = offsets[u + 1] as usize;
    if start >= end {
        return None;
    }
    match targets[start..end].binary_search(&(v as u32)) {
        Ok(idx) => Some(start + idx),
        Err(_) => None,
    }
}

// ===================================================================
// Sanity checks
// ===================================================================

fn sanity_check_weights(
    topo: &CchTopo,
    up_weights: &[u32],
    down_weights: &[u32],
    label: &str,
    fail_threshold: f64,
) -> Result<()> {
    let n_up = up_weights.len();
    let n_down = down_weights.len();

    let mut up_orig_max = 0usize;
    let mut up_short_max = 0usize;
    let mut up_orig_total = 0usize;
    let mut up_short_total = 0usize;
    let mut down_orig_max = 0usize;
    let mut down_short_max = 0usize;
    let mut down_orig_total = 0usize;
    let mut down_short_total = 0usize;

    for (i, &w) in up_weights.iter().enumerate() {
        if topo.up_is_shortcut.bit(i) {
            up_short_total += 1;
            if w == u32::MAX {
                up_short_max += 1;
            }
        } else {
            up_orig_total += 1;
            if w == u32::MAX {
                up_orig_max += 1;
            }
        }
    }
    for (i, &w) in down_weights.iter().enumerate() {
        if topo.down_is_shortcut.bit(i) {
            down_short_total += 1;
            if w == u32::MAX {
                down_short_max += 1;
            }
        } else {
            down_orig_total += 1;
            if w == u32::MAX {
                down_orig_max += 1;
            }
        }
    }

    let total_max = up_orig_max + up_short_max + down_orig_max + down_short_max;
    let total_edges = n_up + n_down;
    let max_pct = (total_max as f64 / total_edges as f64) * 100.0;

    println!("\n📊 {} sanity check:", label);
    println!(
        "  Unreachable: {} / {} ({:.2}%)",
        total_max, total_edges, max_pct
    );
    println!(
        "    Up original:  {} / {} ({:.2}%)",
        up_orig_max,
        up_orig_total,
        if up_orig_total > 0 {
            up_orig_max as f64 / up_orig_total as f64 * 100.0
        } else {
            0.0
        }
    );
    println!(
        "    Up shortcuts: {} / {} ({:.2}%)",
        up_short_max,
        up_short_total,
        if up_short_total > 0 {
            up_short_max as f64 / up_short_total as f64 * 100.0
        } else {
            0.0
        }
    );
    println!(
        "    Down original:  {} / {} ({:.2}%)",
        down_orig_max,
        down_orig_total,
        if down_orig_total > 0 {
            down_orig_max as f64 / down_orig_total as f64 * 100.0
        } else {
            0.0
        }
    );
    println!(
        "    Down shortcuts: {} / {} ({:.2}%)",
        down_short_max,
        down_short_total,
        if down_short_total > 0 {
            down_short_max as f64 / down_short_total as f64 * 100.0
        } else {
            0.0
        }
    );

    if max_pct > fail_threshold {
        anyhow::bail!("CRITICAL: {}% of {} edges are unreachable!", max_pct, label);
    }
    Ok(())
}

fn sanity_check_weights_simple(
    up_weights: &[u32],
    down_weights: &[u32],
    label: &str,
    fail_threshold: f64,
) -> Result<()> {
    let max_count = up_weights.iter().filter(|&&w| w == u32::MAX).count()
        + down_weights.iter().filter(|&&w| w == u32::MAX).count();
    let total = up_weights.len() + down_weights.len();
    let pct = (max_count as f64 / total as f64) * 100.0;
    println!("\n📊 {} sanity check:", label);
    println!("  Unreachable: {} / {} ({:.2}%)", max_count, total, pct);
    if pct > fail_threshold {
        anyhow::bail!("CRITICAL: {}% of {} edges are unreachable!", pct, label);
    }
    Ok(())
}

// ===================================================================
// File I/O
// ===================================================================

fn write_cch_weights(
    path: &std::path::Path,
    up_weights: &[u32],
    down_weights: &[u32],
    up_middle: &[u32],
    down_middle: &[u32],
    mode: Mode,
) -> Result<()> {
    use crate::formats::WeightWidth;
    use crate::formats::crc::Digest;

    const MAGIC: u32 = 0x43434857; // "CCHW"
    // v4 (#306 PR 3): per-direction 2-bit width code in header byte 7.
    //   00 = u32, 01 = u16, 10 = u24, 11 = reserved
    // Reader requires exactly v4; older files must be regenerated.
    const VERSION: u16 = 4;

    // Decide per-direction width based on max value (excluding the
    // u32::MAX "no edge" sentinel — that's encoded as u16::MAX on
    // the u16 path).
    let up_width = WeightWidth::choose(up_weights);
    let down_width = WeightWidth::choose(down_weights);

    // Per-direction 2-bit width code in header byte 7 (#306 PR 3):
    //   00 = u32
    //   01 = u16
    //   10 = u24
    let width_code = |w: WeightWidth| -> u8 {
        match w {
            WeightWidth::U32 => 0,
            WeightWidth::U16 => 1,
            WeightWidth::U24 => 2,
        }
    };
    let width_flags = width_code(up_width) | (width_code(down_width) << 2);

    let mut writer = BufWriter::new(File::create(path)?);
    let mut crc_digest = Digest::new();

    // Header (32 bytes): magic(4) | version(2) | mode(1) | flags(1)
    //                  | n_up(8)  | n_down(8) | reserved(8)
    let magic_bytes = MAGIC.to_le_bytes();
    let version_bytes = VERSION.to_le_bytes();
    let mode_byte = mode.0;
    let n_up = (up_weights.len() as u64).to_le_bytes();
    let n_down = (down_weights.len() as u64).to_le_bytes();
    let padding = [0u8; 8];

    writer.write_all(&magic_bytes)?;
    writer.write_all(&version_bytes)?;
    writer.write_all(&[mode_byte, width_flags])?;
    writer.write_all(&n_up)?;
    writer.write_all(&n_down)?;
    writer.write_all(&padding)?;

    crc_digest.update(&magic_bytes);
    crc_digest.update(&version_bytes);
    crc_digest.update(&[mode_byte, width_flags]);
    crc_digest.update(&n_up);
    crc_digest.update(&n_down);
    crc_digest.update(&padding);

    // Write body at chosen width per direction. `u32::MAX` (no edge)
    // collapses to `u16::MAX` in the compact path so the read-side
    // sentinel mapping reconstructs `u32::MAX` losslessly.
    // Each u16 body is padded to a 4-byte boundary so the following
    // arrays (the other direction's body + u32 middles) stay aligned.
    write_weights_body(&mut writer, &mut crc_digest, up_weights, up_width)?;
    write_padding(&mut writer, &mut crc_digest, up_width, up_weights.len())?;
    write_weights_body(&mut writer, &mut crc_digest, down_weights, down_width)?;
    write_padding(&mut writer, &mut crc_digest, down_width, down_weights.len())?;

    // Write relaxed middle arrays — stay at u32 (middle node ids
    // address `n_filtered_nodes` which planet-scale exceeds 65 535).
    for &m in up_middle {
        let bytes = m.to_le_bytes();
        writer.write_all(&bytes)?;
        crc_digest.update(&bytes);
    }
    for &m in down_middle {
        let bytes = m.to_le_bytes();
        writer.write_all(&bytes)?;
        crc_digest.update(&bytes);
    }

    let crc = crc_digest.finalize();
    writer.write_all(&crc.to_le_bytes())?;
    writer.write_all(&crc.to_le_bytes())?;

    writer.flush()?;
    Ok(())
}

/// Emit a weight body at the chosen width, updating the CRC.
fn write_weights_body<W: std::io::Write>(
    writer: &mut W,
    crc_digest: &mut crate::formats::crc::Digest,
    weights: &[u32],
    width: crate::formats::WeightWidth,
) -> Result<()> {
    use crate::formats::{U24_SENTINEL, WeightWidth};
    match width {
        WeightWidth::U32 => {
            for &w in weights {
                let bytes = w.to_le_bytes();
                writer.write_all(&bytes)?;
                crc_digest.update(&bytes);
            }
        }
        WeightWidth::U16 => {
            for &w in weights {
                let v16: u16 = if w == u32::MAX { u16::MAX } else { w as u16 };
                let bytes = v16.to_le_bytes();
                writer.write_all(&bytes)?;
                crc_digest.update(&bytes);
            }
        }
        WeightWidth::U24 => {
            for &w in weights {
                // u32::MAX → U24_SENTINEL (0x00FF_FFFF) so the read
                // path's `U24_SENTINEL → u32::MAX` mapping round-trips
                // the "no edge" marker.
                let v24: u32 = if w == u32::MAX { U24_SENTINEL } else { w };
                let bytes = v24.to_le_bytes();
                writer.write_all(&bytes[..3])?;
                crc_digest.update(&bytes[..3]);
            }
        }
    }
    Ok(())
}

/// Emit 0-3 zero bytes so the next array begins on a 4-byte boundary.
/// u32 bodies are already 4-aligned; u16 needs exactly 2 bytes of pad
/// when `n` is odd (else 0); u24 needs 0, 1, 2, or 3 bytes of pad to
/// round `n * 3` up to the next multiple of 4. CRC covers the padding.
fn write_padding<W: std::io::Write>(
    writer: &mut W,
    crc_digest: &mut crate::formats::crc::Digest,
    width: crate::formats::WeightWidth,
    n: usize,
) -> Result<()> {
    let pad = width.padded_body_bytes(n) - width.bytes_per_entry() * n;
    if pad > 0 {
        let zeros = [0u8; 4];
        writer.write_all(&zeros[..pad])?;
        crc_digest.update(&zeros[..pad]);
    }
    Ok(())
}

// ==========================================================================
// Hybrid State Graph CCH Customization
// ==========================================================================

/// Configuration for Step 8 with hybrid state graph
pub struct Step8HybridConfig {
    pub cch_topo_path: PathBuf,
    pub hybrid_state_path: PathBuf,
    pub mode: Mode,
    pub mode_name: String,
    pub outdir: PathBuf,
}

/// Sorted hybrid state graph adjacency for fast arc index lookup
struct SortedHybridAdj {
    offsets: Vec<u64>,
    sorted_targets: Vec<u32>,
    sorted_weights: Vec<u32>,
}

impl SortedHybridAdj {
    fn build(hybrid: &crate::formats::HybridState) -> Self {
        let n_states = hybrid.n_states as usize;
        let n_arcs = hybrid.n_arcs as usize;

        let sorted_per_state: Vec<Vec<(u32, u32)>> = (0..n_states)
            .into_par_iter()
            .map(|u| {
                let start = hybrid.offsets[u] as usize;
                let end = hybrid.offsets[u + 1] as usize;
                let mut edges: Vec<(u32, u32)> = (start..end)
                    .map(|i| (hybrid.targets[i], hybrid.weights[i]))
                    .collect();
                edges.sort_unstable_by_key(|(target, _)| *target);
                edges
            })
            .collect();

        let mut offsets = Vec::with_capacity(n_states + 1);
        let mut sorted_targets = Vec::with_capacity(n_arcs);
        let mut sorted_weights = Vec::with_capacity(n_arcs);

        let mut offset = 0u64;
        for edges in sorted_per_state {
            offsets.push(offset);
            for (target, weight) in edges {
                sorted_targets.push(target);
                sorted_weights.push(weight);
            }
            offset = sorted_targets.len() as u64;
        }
        offsets.push(offset);

        Self {
            offsets,
            sorted_targets,
            sorted_weights,
        }
    }

    #[inline]
    fn find_weight(&self, u: usize, v: u32) -> Option<u32> {
        let start = self.offsets[u] as usize;
        let end = self.offsets[u + 1] as usize;
        if start >= end {
            return None;
        }
        match self.sorted_targets[start..end].binary_search(&v) {
            Ok(idx) => Some(self.sorted_weights[start + idx]),
            Err(_) => None,
        }
    }
}

/// Customize CCH for hybrid state graph (uses parallel triangle relaxation)
pub fn customize_cch_hybrid(config: Step8HybridConfig) -> Result<Step8Result> {
    let start_time = std::time::Instant::now();
    let mode_name = &config.mode_name;
    println!(
        "\n🎨 Step 8: Customizing CCH for {} (HYBRID)...\n",
        mode_name
    );

    println!("Loading CCH topology (hybrid)...");
    let topo = CchTopoFile::read(&config.cch_topo_path)?;
    let n_nodes = topo.n_nodes as usize;
    let n_up = topo.up_targets.len();
    let n_down = topo.down_targets.len();
    println!(
        "  ✓ {} nodes, {} up edges, {} down edges",
        n_nodes, n_up, n_down
    );

    println!("Loading hybrid state graph...");
    let hybrid = HybridStateFile::read(&config.hybrid_state_path)?;
    println!("  ✓ {} states, {} arcs", hybrid.n_states, hybrid.n_arcs);

    if hybrid.n_states != topo.n_nodes {
        anyhow::bail!(
            "State count mismatch: hybrid has {} states, CCH topo has {} nodes",
            hybrid.n_states,
            topo.n_nodes
        );
    }

    println!("\nBuilding sorted hybrid adjacency (parallel)...");
    let sorted_hybrid = SortedHybridAdj::build(&hybrid);
    println!("  ✓ Built sorted adjacency");

    let rank_to_state = &topo.rank_to_filtered;

    println!("Pre-sorting down edges by target rank (parallel)...");
    let sorted_down_indices: Vec<Vec<usize>> = (0..n_nodes)
        .into_par_iter()
        .map(|u| {
            let start = topo.down_offsets[u] as usize;
            let end = topo.down_offsets[u + 1] as usize;
            if start >= end {
                return Vec::new();
            }
            let mut indices: Vec<usize> = (start..end).collect();
            indices.sort_unstable_by_key(|&i| topo.down_targets[i]);
            indices
        })
        .collect();
    println!("  ✓ Pre-sorted down edges");

    // Bottom-up customization (sequential, single metric for hybrid)
    println!("\nCustomizing weights (bottom-up)...");
    let (up_weights, down_weights) =
        bottom_up_customize(&topo, &sorted_down_indices, |u_rank, v_rank| {
            compute_hybrid_original_weight(u_rank, v_rank, &sorted_hybrid, rank_to_state)
        });
    println!("  ✓ Initial customization complete");

    // Parallel triangle relaxation
    println!("\nBuilding reverse DOWN adjacency...");
    let rev_down = build_reverse_down_adj_for_relax(&topo);
    println!("  ✓ {} entries", rev_down.sources.len());

    println!("\n🔺 Triangle relaxation (parallel)...");
    let tr_start = std::time::Instant::now();
    let (up_weights, down_weights, _up_middles, _down_middles, relax_count, relax_passes) =
        triangle_relax_parallel(&topo, up_weights, down_weights, &rev_down);
    println!(
        "  ✓ {:.2}s, {} updates in {} passes",
        tr_start.elapsed().as_secs_f64(),
        relax_count,
        relax_passes
    );

    sanity_check_weights(&topo, &up_weights, &down_weights, "Hybrid", 95.0)?;

    std::fs::create_dir_all(&config.outdir)?;
    let output_path = config
        .outdir
        .join(format!("cch.w.hybrid.{}.u32", mode_name));

    println!("\nWriting output...");
    let topo_up_mid: Vec<u32> = topo.up_middle.to_vec_u32();
    let topo_down_mid: Vec<u32> = topo.down_middle.to_vec_u32();
    write_cch_weights(
        &output_path,
        &up_weights,
        &down_weights,
        &topo_up_mid,
        &topo_down_mid,
        config.mode,
    )?;
    println!("  ✓ Written {}", output_path.display());

    let customize_time_ms = start_time.elapsed().as_millis() as u64;

    // Hybrid mode doesn't produce distance weights (no EBG nodes available)
    let distance_output_path = config
        .outdir
        .join(format!("cch.d.hybrid.{}.u32", mode_name));

    Ok(Step8Result {
        output_path,
        distance_output_path,
        mode: config.mode,
        mode_name: config.mode_name.clone(),
        n_up_edges: n_up as u64,
        n_down_edges: n_down as u64,
        customize_time_ms,
    })
}

#[inline]
fn compute_hybrid_original_weight(
    u_rank: usize,
    v_rank: usize,
    sorted_hybrid: &SortedHybridAdj,
    rank_to_state: &[u32],
) -> u32 {
    let u_state = rank_to_state[u_rank] as usize;
    let v_state = rank_to_state[v_rank];
    sorted_hybrid
        .find_weight(u_state, v_state)
        .unwrap_or(u32::MAX)
}

#[cfg(test)]
mod determinism_tests {
    use super::*;
    use crate::formats::CchWeightsFile;
    use std::path::PathBuf;

    /// #433: prove the in-memory serve-boot recustomization
    /// ([`customize_cch_time_in_memory`], traffic = `None`) reproduces the CLI
    /// [`customize_cch`] legal-limit `cch.w.car.u32` at the value level —
    /// up/down weights and up/down middles element-for-element. This is the
    /// contract the serve-boot hot-swap relies on: feeding the same raw inputs
    /// the build used must yield the same weights the build baked.
    ///
    /// Skipped unless all six fixture paths are provided via env, because the
    /// Belgium step4-7/step8 outputs are large and not committed. To run
    /// against a real Belgium build:
    /// ```text
    /// BT_TOPO=data/belgium/step7/cch.car.topo \
    /// BT_FILTERED_EBG=data/belgium/step5/filtered.car.ebg \
    /// BT_W_CAR=data/belgium/step5/w.car.u32 \
    /// BT_T_CAR=data/belgium/step5/t.car.u32 \
    /// BT_EBG_NODES=data/belgium/step4/ebg.nodes \
    /// BT_CCH_W_CAR=data/belgium/step8/cch.w.car.u32 \
    ///   cargo test -p butterfly-route customize_in_memory_matches_cli -- --nocapture
    /// ```
    /// `BT_CCH_W_CAR` MUST be a legal-limit (no-traffic-bake) step8 output.
    #[test]
    fn customize_in_memory_matches_cli() {
        const KEYS: [&str; 6] = [
            "BT_TOPO",
            "BT_FILTERED_EBG",
            "BT_W_CAR",
            "BT_T_CAR",
            "BT_EBG_NODES",
            "BT_CCH_W_CAR",
        ];
        let Some(paths) = KEYS
            .iter()
            .map(|k| std::env::var(k).ok().map(PathBuf::from))
            .collect::<Option<Vec<_>>>()
        else {
            eprintln!(
                "skipping customize_in_memory_matches_cli: set {} to a legal-limit Belgium build",
                KEYS.join(", ")
            );
            return;
        };

        let topo = CchTopoFile::read(&paths[0]).unwrap();
        let filtered_ebg = FilteredEbgFile::read(&paths[1]).unwrap();
        let weights = mod_weights::read_all(&paths[2]).unwrap();
        let turns = mod_turns::read_all(&paths[3]).unwrap();
        let ebg_nodes = EbgNodesFile::read(&paths[4]).unwrap();

        let (got, _adjusted_node_weights) = customize_cch_time_in_memory(
            &topo,
            &filtered_ebg,
            &weights.weights,
            &turns.penalties,
            &ebg_nodes,
            None,
        )
        .unwrap();

        let want = CchWeightsFile::read(&paths[5]).unwrap();

        assert_eq!(
            got.up.to_vec_u32(),
            want.up.to_vec_u32(),
            "up weights diverged from CLI customize_cch"
        );
        assert_eq!(
            got.down.to_vec_u32(),
            want.down.to_vec_u32(),
            "down weights diverged from CLI customize_cch"
        );
        let got_um: Vec<u32> = got.up_middle.iter().copied().collect();
        let want_um: Vec<u32> = want.up_middle.iter().copied().collect();
        assert_eq!(
            got_um, want_um,
            "up middles diverged from CLI customize_cch"
        );
        let got_dm: Vec<u32> = got.down_middle.iter().copied().collect();
        let want_dm: Vec<u32> = want.down_middle.iter().copied().collect();
        assert_eq!(
            got_dm, want_dm,
            "down middles diverged from CLI customize_cch"
        );
    }
}

#[cfg(test)]
mod traffic_apply_tests {
    use super::*;
    use crate::density::DensityClass;
    use crate::formats::{EbgNode, NbgEdge, NbgGeo};
    use crate::profile_abi::WayOutput;
    use crate::traffic::TrafficProfile;
    use std::collections::BTreeMap;

    /// Synthetic 4-node fixture: one EBG node per way, ways spanning two
    /// highway classes × two density classes, all weights 1000.
    ///
    /// | node | way | highway_class | density    |
    /// |------|-----|---------------|------------|
    /// | 0    | 10  | 1 (motorway)  | Rural      |
    /// | 1    | 11  | 12 (resid.)   | UrbanHigh  |
    /// | 2    | 12  | 12 (resid.)   | Rural      |
    /// | 3    | 13  | 7 (secondary) | Suburban   |
    fn fixture() -> (EbgNodes, NbgGeo, Vec<way_attrs::WayAttr>) {
        let mk_node = |geom_idx: u32| EbgNode {
            tail_nbg: 0,
            head_nbg: 1,
            geom_idx,
            length_m: 10,
            class_bits: 0,
            primary_way: 0,
        };
        let ebg_nodes = EbgNodes {
            n_nodes: 4,
            created_unix: 0,
            inputs_sha: [0; 32],
            nodes: ArcCow::from_vec(vec![mk_node(0), mk_node(1), mk_node(2), mk_node(3)]),
        };
        let mk_edge = |way_id: i64| NbgEdge {
            u_node: 0,
            v_node: 1,
            length_mm: 10_000,
            bearing_deci_deg: 0,
            n_poly_pts: 0,
            poly_off: 0,
            first_osm_way_id: way_id,
            flags: 0,
        };
        let nbg_geo = NbgGeo {
            n_edges_und: 4,
            edges: vec![mk_edge(10), mk_edge(11), mk_edge(12), mk_edge(13)],
            polylines: vec![],
        };
        let mk_attr = |way_id: i64, highway: u16, density: DensityClass| way_attrs::WayAttr {
            way_id,
            output: WayOutput {
                access_fwd: true,
                base_speed_mmps: 10_000,
                highway_class: highway,
                density_class: density.to_u8(),
                ..Default::default()
            },
        };
        let attrs = vec![
            mk_attr(10, 1, DensityClass::Rural),
            mk_attr(11, 12, DensityClass::UrbanHigh),
            mk_attr(12, 12, DensityClass::Rural),
            mk_attr(13, 7, DensityClass::Suburban),
        ];
        (ebg_nodes, nbg_geo, attrs)
    }

    fn vector_profile() -> TrafficProfile {
        TrafficProfile {
            name: "vec".to_string(),
            base_model: "car".to_string(),
            factors: [0.5, 0.6, 0.7, 0.8, 0.9],
            matrix: BTreeMap::new(),
        }
    }

    fn apply(profile: &TrafficProfile) -> Vec<u32> {
        let (ebg_nodes, nbg_geo, attrs) = fixture();
        let mut weights = vec![1000u32; 4];
        apply_traffic_to_node_weights_in_memory(
            &mut weights,
            &ebg_nodes,
            profile,
            &attrs,
            &nbg_geo,
        )
        .unwrap();
        weights
    }

    #[test]
    fn vector_profile_scales_by_density_class() {
        let w = apply(&vector_profile());
        // weight / factor, rounded: rural 1000/0.9, urban_high 1000/0.5,
        // rural 1000/0.9, suburban 1000/0.8.
        assert_eq!(w, vec![1111, 2000, 1111, 1250]);
    }

    #[test]
    fn matrix_replicating_the_vector_is_bit_identical_to_vector_only() {
        let base = vector_profile();
        let mut replicated = base.clone();
        // Full rows for every highway code in the fixture, each cell copying
        // the vector value for its density.
        for code in [1u16, 7, 12] {
            let row: crate::traffic::MatrixRow = std::array::from_fn(|i| Some(base.factors[i]));
            replicated.matrix.insert(code, row);
        }
        assert!(replicated.has_matrix());
        assert_eq!(
            apply(&base),
            apply(&replicated),
            "a matrix that replicates the vector must produce identical weights"
        );
    }

    #[test]
    fn matrix_cell_overrides_only_its_highway_density_cell() {
        let mut p = vector_profile();
        // residential (12) × Rural slowed to 0.4; every other cell falls back.
        let mut row: crate::traffic::MatrixRow = [None; 5];
        row[DensityClass::Rural.to_u8() as usize] = Some(0.4);
        p.matrix.insert(12, row);

        let w = apply(&p);
        // node 0: motorway×Rural → vector 0.9 → 1111 (no row for code 1)
        // node 1: residential×UrbanHigh → vector 0.5 → 2000 (cell absent in row)
        // node 2: residential×Rural → matrix 0.4 → 2500 (overridden)
        // node 3: secondary×Suburban → vector 0.8 → 1250
        assert_eq!(w, vec![1111, 2000, 2500, 1250]);
    }

    #[test]
    fn inaccessible_sentinel_preserved_with_matrix() {
        let (ebg_nodes, nbg_geo, attrs) = fixture();
        let mut p = vector_profile();
        p.matrix.insert(12, std::array::from_fn(|_| Some(0.4)));
        let mut weights = vec![1000u32, 0, 1000, 0];
        apply_traffic_to_node_weights_in_memory(&mut weights, &ebg_nodes, &p, &attrs, &nbg_geo)
            .unwrap();
        assert_eq!(weights[1], 0, "0 = inaccessible must survive");
        assert_eq!(weights[3], 0, "0 = inaccessible must survive");
        assert_eq!(weights[2], 2500, "accessible node scaled by matrix cell");
    }
}

#[cfg(test)]
mod len_along_time_middle_tests {
    //! #528 regression: the length-along-time-shortest weight of a shortcut
    //! is a FUNCTION OF ITS TIME-OPTIMAL MIDDLE, not a traffic-invariant
    //! physical constant. This is the exact fact that the stale-clone bug
    //! (both the boot recustomization sites, fixed via
    //! `refresh_len_along_time`, and the container-baked traffic-variant
    //! loader in `server/state.rs`) violated: they kept the base car's
    //! len-along-time bytes while feeding a DIFFERENT set of time weights,
    //! so the shortcut expansion followed the wrong (clean-car) apex and the
    //! distance channel of `/table` / Flight `matrix` diverged from `/route`
    //! by up to 15% on recustomized car.
    //!
    //! These are pure-function tests on `bottom_up_with_external_middles`
    //! (the core of `recompute_len_along_time_from_middles`) with a
    //! hand-built 4-node CCH — no server, no Belgium container. If anyone
    //! "optimizes" by treating len-along-time as middle-independent, both
    //! assertions below fail.
    use super::*;
    use crate::formats::BitsetField;

    /// 4-node CCH. Ranks 0,1 are two candidate low-rank apexes; ranks 2,3
    /// are the high-rank endpoints of a single UP shortcut 2→3 that can be
    /// expanded through either apex:
    ///
    /// ```text
    ///   UP edges  : 0→3 (orig, len 10), 1→3 (orig, len 100), 2→3 (SHORTCUT)
    ///   DOWN edges: 2→0 (orig, len 3),  2→1 (orig, len 5)
    /// ```
    ///
    /// via apex 0 : len(2→0)+len(0→3) = 3 + 10  = 13
    /// via apex 1 : len(2→1)+len(1→3) = 5 + 100 = 105
    fn topo_4node() -> CchTopo {
        CchTopo {
            n_nodes: 4,
            n_shortcuts: 1,
            n_original_arcs: 4,
            inputs_sha: [0u8; 32],
            // node0:[3] node1:[3] node2:[3] node3:[]
            up_offsets: ArcCow::from_vec(vec![0u64, 1, 2, 3, 3]),
            up_targets: ArcCow::from_vec(vec![3u32, 3, 3]),
            up_is_shortcut: BitsetField::from_bools(&[false, false, true]),
            // unused by bottom_up_with_external_middles (external middles win)
            up_middle: WeightArray::from_vec_u32(vec![u32::MAX, u32::MAX, 0]),
            // node0:[] node1:[] node2:[0,1] node3:[]  (targets sorted for binary_search)
            down_offsets: ArcCow::from_vec(vec![0u64, 0, 0, 2, 2]),
            down_targets: ArcCow::from_vec(vec![0u32, 1]),
            down_is_shortcut: BitsetField::from_bools(&[false, false]),
            down_middle: WeightArray::from_vec_u32(vec![u32::MAX, u32::MAX]),
            rank_to_filtered: ArcCow::from_vec(vec![0u32, 1, 2, 3]),
        }
    }

    fn leaf_len(u: usize, v: usize) -> u32 {
        match (u, v) {
            (0, 3) => 10,
            (1, 3) => 100,
            (2, 0) => 3,
            (2, 1) => 5,
            _ => panic!("unexpected original edge ({u},{v})"),
        }
    }

    #[test]
    fn shortcut_len_along_time_follows_the_time_optimal_middle() {
        let topo = topo_4node();
        // Mirror recompute_len_along_time_from_middles' sorted_down_indices.
        let sorted_down_indices: Vec<Vec<usize>> =
            vec![Vec::new(), Vec::new(), vec![0usize, 1], Vec::new()];
        let down_mid = [u32::MAX, u32::MAX]; // no down shortcuts

        // Expand the 2→3 shortcut through apex 0.
        let (up_via0, dn_via0) = bottom_up_with_external_middles(
            &topo,
            &sorted_down_indices,
            &[u32::MAX, u32::MAX, 0], // up shortcut idx2 -> apex 0
            &down_mid,
            |u, v| leaf_len(u, v),
        );
        // Expand the SAME shortcut through apex 1 (as a different set of time
        // weights would elect).
        let (up_via1, dn_via1) = bottom_up_with_external_middles(
            &topo,
            &sorted_down_indices,
            &[u32::MAX, u32::MAX, 1], // up shortcut idx2 -> apex 1
            &down_mid,
            |u, v| leaf_len(u, v),
        );

        // Leaf edges are middle-independent: identical across both runs.
        assert_eq!(
            dn_via0, dn_via1,
            "original DOWN edges must not depend on middle"
        );
        assert_eq!(
            up_via0[0], up_via1[0],
            "orig up 0->3 must not depend on middle"
        );
        assert_eq!(
            up_via0[1], up_via1[1],
            "orig up 1->3 must not depend on middle"
        );

        // The shortcut (index 2) length IS a function of the elected apex.
        assert_eq!(up_via0[2], 13, "via apex 0: 3 + 10");
        assert_eq!(up_via1[2], 105, "via apex 1: 5 + 100");
        assert_ne!(
            up_via0[2], up_via1[2],
            "#528: len-along-time of a shortcut MUST change with its time-optimal \
             middle — it is NOT traffic-invariant, so it must be recomputed (never \
             cloned from base) whenever time weights are recustomized"
        );
    }

    #[test]
    fn pack_wm_is_a_bijection_and_orders_by_weight_then_middle() {
        // pack_wm must (1) round-trip both fields exactly and (2) put the
        // weight in the high 32 bits so a `<` / fetch_min on the packed u64
        // minimises weight first, breaking ties on the middle. Both facts are
        // load-bearing: triangle relaxation and the exclude recustomization
        // pick the lex-smallest (weight, middle) via this single comparison.
        for (w, m) in [(0u32, 0u32), (1, 7), (72, 3), (u32::MAX, 0), (5, u32::MAX)] {
            let p = pack_wm(w, m);
            assert_eq!(unpack_weight(p), w, "weight must round-trip");
            assert_eq!(unpack_middle(p), m, "middle must round-trip");
        }
        // Smaller weight wins regardless of middle.
        assert!(pack_wm(10, u32::MAX) < pack_wm(11, 0));
        // Equal weight -> smaller middle wins (deterministic tie-break).
        assert!(pack_wm(10, 3) < pack_wm(10, 4));
    }

    #[test]
    fn unchanged_middles_reproduce_the_same_len_bit_for_bit() {
        // The other half of the contract: feeding the SAME middles twice is
        // deterministic and byte-identical (so refresh with unchanged time
        // middles reproduces the base lat exactly — no spurious drift).
        let topo = topo_4node();
        let sorted_down_indices: Vec<Vec<usize>> =
            vec![Vec::new(), Vec::new(), vec![0usize, 1], Vec::new()];
        let up_mid = [u32::MAX, u32::MAX, 0];
        let down_mid = [u32::MAX, u32::MAX];
        let a = bottom_up_with_external_middles(
            &topo,
            &sorted_down_indices,
            &up_mid,
            &down_mid,
            leaf_len,
        );
        let b = bottom_up_with_external_middles(
            &topo,
            &sorted_down_indices,
            &up_mid,
            &down_mid,
            leaf_len,
        );
        assert_eq!(
            a, b,
            "recompute with identical middles must be bit-for-bit stable"
        );
    }

    // ---- #529: (time, then length) lexicographic middle election --------
    //
    // The reused `topo_4node` has a single UP shortcut 2→3 expandable
    // through apex 0 or apex 1. `triangle_relax_lex_parallel` is seeded
    // with per-edge (time, length) and must, among EQUAL-TIME apexes,
    // elect the SHORTER-length one — and do so deterministically (smallest
    // apex index on a length tie). These are the invariants that make
    // `/route` and the 2-channel `/table` agree on one-way-agnostic ties.

    /// Run the lex relaxation on `topo_4node` with the given seeds and
    /// return `(time_up, up_middles)`.
    fn run_lex(
        time_up: Vec<u32>,
        time_down: Vec<u32>,
        len_up: Vec<u32>,
        len_down: Vec<u32>,
    ) -> (Vec<u32>, Vec<u32>) {
        let topo = topo_4node();
        let rev_down = build_reverse_down_adj_for_relax(&topo);
        let (up, _down, up_mid, _down_mid, _c, _p) =
            triangle_relax_lex_parallel(&topo, time_up, time_down, &len_up, &len_down, &rev_down);
        (up, up_mid)
    }

    #[test]
    fn lex_election_prefers_shorter_length_among_equal_time() {
        // apex 0: time 3+7=10, length 3+10=13
        // apex 1: time 5+5=10, length 5+100=105   (EQUAL time, longer)
        // Shortcut seeded via the LONGER apex 1 (time 10, len 105); the
        // relaxation must switch it to apex 0 (same time, shorter length).
        let (up, up_mid) = run_lex(
            vec![7, 5, 10], // up:  0→3, 1→3, shortcut 2→3 (seed = apex 1)
            vec![3, 5],     // down: 2→0, 2→1
            vec![10, 100, 105],
            vec![3, 5],
        );
        assert_eq!(up[2], 10, "duration must NOT change (time is primary)");
        assert_eq!(
            up_mid[2], 0,
            "must elect apex 0 — the shorter equal-time expansion"
        );
    }

    #[test]
    fn lex_election_is_not_hardwired_to_apex_zero() {
        // Mirror image: equal times (both 10), but apex 1 is now the shorter
        // expansion, so the election must pick apex 1 (not a hardcoded 0).
        // apex 0: time 5+5=10, length 5+100=105
        // apex 1: time 5+5=10, length 3+10=13   (shorter)
        let (up, up_mid) = run_lex(
            vec![5, 5, 10],     // up 0→3 t=5, 1→3 t=5, shortcut seed t=10
            vec![5, 5],         // down 2→0 t=5, 2→1 t=5  → both apexes time 10
            vec![100, 10, 999], // up lens: 0→3=100, 1→3=10, shortcut seed worse
            vec![5, 3],         // down lens: 2→0=5, 2→1=3
        );
        assert_eq!(up[2], 10, "duration unchanged");
        assert_eq!(
            up_mid[2], 1,
            "must elect apex 1 — the shorter equal-time expansion"
        );
    }

    #[test]
    fn lex_election_length_tie_breaks_to_smallest_apex_and_is_deterministic() {
        // Both apexes give the IDENTICAL (time, length) = (10, 10). The final
        // tie-break must be the smallest apex index, and reproducible.
        let seeds = || {
            (
                vec![7u32, 5, 999], // times: 0→3=7, 1→3=5, shortcut seed worse
                vec![3u32, 5],      // times: 2→0=3, 2→1=5 → apex0=10, apex1=10
                vec![6u32, 6, 999], // lens:  0→3=6, 1→3=6
                vec![4u32, 4],      // lens:  2→0=4, 2→1=4 → apex0=10, apex1=10
            )
        };
        let (a_up, a_mid) = {
            let (t_u, t_d, l_u, l_d) = seeds();
            run_lex(t_u, t_d, l_u, l_d)
        };
        let (b_up, b_mid) = {
            let (t_u, t_d, l_u, l_d) = seeds();
            run_lex(t_u, t_d, l_u, l_d)
        };
        assert_eq!(a_up[2], 10, "duration unchanged");
        assert_eq!(a_mid[2], 0, "equal (time,length): smallest apex index wins");
        assert_eq!(a_up, b_up, "time weights must be reproducible");
        assert_eq!(a_mid, b_mid, "elected middles must be reproducible");
    }
}
