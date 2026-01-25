///! CLI commands for butterfly-route

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::ingest::{run_ingest, IngestConfig};
use crate::validate::{verify_lock_conditions, validate_step4, validate_step5, validate_step6, validate_step7, Counts, LockFile};
use crate::profile::{run_profiling, ProfileConfig};
use crate::nbg::{build_nbg, NbgConfig};
use crate::ebg::{build_ebg, EbgConfig};
use crate::step5;
use crate::step6;
use crate::step7;
use crate::step8;
use crate::step9;
use crate::profile_abi::Mode;

#[derive(Parser)]
#[command(name = "butterfly-route")]
#[command(about = "High-performance OSM routing engine", long_about = None)]
pub struct Cli {
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Step 1: Ingest OSM PBF into immutable artifacts
    Step1Ingest {
        /// Input OSM PBF file
        #[arg(short, long)]
        input: PathBuf,

        /// Output directory for artifacts
        #[arg(short, long)]
        outdir: PathBuf,

        /// Number of threads (currently unused, kept for future)
        #[arg(short, long, default_value = "8")]
        threads: usize,

        /// Verify only (don't write, just check CRCs)
        #[arg(long)]
        verify_only: bool,
    },

    /// Step 2: Generate per-mode attributes via routing profiles
    Step2Profile {
        /// Path to ways.raw from Step 1
        #[arg(long)]
        ways: PathBuf,

        /// Path to relations.raw from Step 1
        #[arg(long)]
        relations: PathBuf,

        /// Output directory for way_attrs.*.bin and turn_rules.*.bin
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 3: Build node-based graph (NBG) from Step 1 and Step 2
    Step3Nbg {
        /// Path to nodes.sa from Step 1
        #[arg(long)]
        nodes: PathBuf,

        /// Path to ways.raw from Step 1
        #[arg(long)]
        ways: PathBuf,

        /// Path to way_attrs.car.bin from Step 2
        #[arg(long)]
        car: PathBuf,

        /// Path to way_attrs.bike.bin from Step 2
        #[arg(long)]
        bike: PathBuf,

        /// Path to way_attrs.foot.bin from Step 2
        #[arg(long)]
        foot: PathBuf,

        /// Output directory for nbg.csr, nbg.geo, nbg.node_map
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 4: Build edge-based graph (EBG) with turn expansion
    Step4Ebg {
        /// Path to nbg.csr from Step 3
        #[arg(long)]
        nbg_csr: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to nbg.node_map from Step 3
        #[arg(long)]
        nbg_node_map: PathBuf,

        /// Path to way_attrs.car.bin from Step 2
        #[arg(long)]
        way_attrs_car: PathBuf,

        /// Path to way_attrs.bike.bin from Step 2
        #[arg(long)]
        way_attrs_bike: PathBuf,

        /// Path to way_attrs.foot.bin from Step 2
        #[arg(long)]
        way_attrs_foot: PathBuf,

        /// Path to turn_rules.car.bin from Step 2
        #[arg(long)]
        turn_rules_car: PathBuf,

        /// Path to turn_rules.bike.bin from Step 2
        #[arg(long)]
        turn_rules_bike: PathBuf,

        /// Path to turn_rules.foot.bin from Step 2
        #[arg(long)]
        turn_rules_foot: PathBuf,

        /// Output directory for ebg.nodes, ebg.csr, ebg.turn_table
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 5: Generate per-mode weights & masks
    Step5Weights {
        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to ebg.csr from Step 4
        #[arg(long)]
        ebg_csr: PathBuf,

        /// Path to ebg.turn_table from Step 4
        #[arg(long)]
        turn_table: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to way_attrs.car.bin from Step 2
        #[arg(long)]
        way_attrs_car: PathBuf,

        /// Path to way_attrs.bike.bin from Step 2
        #[arg(long)]
        way_attrs_bike: PathBuf,

        /// Path to way_attrs.foot.bin from Step 2
        #[arg(long)]
        way_attrs_foot: PathBuf,

        /// Output directory for w.*.u32, t.*.u32, mask.*.bitset
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 6: Generate per-mode CCH ordering on filtered EBG via nested dissection
    Step6Order {
        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to nbg.geo from Step 3 (for coordinates)
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Mode to generate ordering for (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output directory for order.*.ebg
        #[arg(short, long)]
        outdir: PathBuf,

        /// Leaf threshold for recursion (default: 8192)
        #[arg(long, default_value = "8192")]
        leaf_threshold: usize,

        /// Balance epsilon (default: 0.05)
        #[arg(long, default_value = "0.05")]
        balance_eps: f32,
    },

    /// Step 7: Build per-mode CCH topology via contraction on filtered EBG
    Step7Contract {
        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to w.*.u32 weights file from Step 5 (for metric-aware witness search)
        #[arg(long)]
        weights: PathBuf,

        /// Path to t.*.u32 turn penalties file from Step 5 (for metric-aware witness search)
        #[arg(long)]
        turns: PathBuf,

        /// Mode to build CCH for (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.*.topo
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 8: Customize per-mode CCH with weights
    Step8Customize {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to w.*.u32 weights file from Step 5
        #[arg(long)]
        weights: PathBuf,

        /// Path to t.*.u32 turn penalties file from Step 5
        #[arg(long)]
        turns: PathBuf,

        /// Mode to customize for (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.w.*.u32
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 9: Start query server
    Serve {
        /// Directory containing all step outputs (step3/, step4/, etc.)
        #[arg(short, long)]
        data_dir: PathBuf,

        /// Port to listen on (default: find free port starting from 8080)
        #[arg(short, long)]
        port: Option<u16>,
    },

    /// Validate CCH correctness by comparing bidirectional CCH vs CCH-Dijkstra
    ValidateCch {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Mode to validate (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Number of random query pairs (default: 50000)
        #[arg(long, default_value = "50000")]
        n_pairs: usize,

        /// Random seed (default: 42424242)
        #[arg(long, default_value = "42424242")]
        seed: u64,

        /// Output file for failures (optional)
        #[arg(long)]
        failures_file: Option<PathBuf>,
    },

    /// Run targeted regression tests for CCH edge cases
    RegressionCch {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Mode to test (car, bike, foot)
        #[arg(long)]
        mode: String,
    },

    /// Validate graph/weight invariants for CCH correctness
    ValidateInvariants {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Mode to validate (car, bike, foot)
        #[arg(long)]
        mode: String,
    },

    /// Bounded Dijkstra for isochrone (range query)
    RangeCch {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,
    },

    /// Validate range query properties (monotonicity, equivalence, P2P consistency)
    ValidateRange {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,
    },

    /// PHAST-based range query (fast one-to-many)
    PhastRange {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,
    },

    /// Validate PHAST correctness against naive Dijkstra
    ValidatePhast {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Origin node ID (EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,
    },

    /// Validate block-gated PHAST against active-set PHAST
    ValidateBlockGated {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Comma-separated origin node IDs (EBG nodes)
        #[arg(long, default_value = "0,1000,10000,100000")]
        origins: String,

        /// Comma-separated time thresholds in milliseconds
        #[arg(long, default_value = "60000,300000,600000")]
        thresholds: String,
    },

    /// Extract frontier on base graph (real road segments, not CCH shortcuts)
    ExtractFrontier {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to w.*.u32 (base edge weights) from Step 5
        #[arg(long)]
        base_weights: PathBuf,

        /// Origin node ID (filtered EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Optional: export frontier to GeoJSON file
        #[arg(long)]
        geojson_out: Option<PathBuf>,
    },

    /// Generate isochrone polygon (full pipeline: PHAST → frontier → contour)
    Isochrone {
        /// Path to cch.*.topo from Step 7
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to cch.w.*.u32 from Step 8
        #[arg(long)]
        cch_weights: PathBuf,

        /// Path to order.*.ebg from Step 6
        #[arg(long)]
        order: PathBuf,

        /// Path to filtered.*.ebg from Step 5
        #[arg(long)]
        filtered_ebg: PathBuf,

        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to nbg.geo from Step 3
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Path to w.*.u32 (base edge weights) from Step 5
        #[arg(long)]
        base_weights: PathBuf,

        /// Origin node ID (filtered EBG node, not OSM ID)
        #[arg(long)]
        origin_node: u32,

        /// Time threshold in milliseconds
        #[arg(long)]
        threshold_ms: u32,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output GeoJSON file
        #[arg(long)]
        output: PathBuf,

        /// Grid cell size in meters (default: mode-dependent)
        #[arg(long)]
        grid_size: Option<f64>,
    },

    /// Analyze hybrid state graph (step towards beating OSRM)
    HybridAnalysis {
        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to ebg.csr from Step 4
        #[arg(long)]
        ebg_csr: PathBuf,

        /// Path to nbg.node_map from Step 3
        #[arg(long)]
        nbg_node_map: PathBuf,

        /// Path to turn_rules.car.bin from Step 2
        #[arg(long)]
        turn_rules_car: PathBuf,

        /// Path to w.car.u32 from Step 5
        #[arg(long)]
        weights: PathBuf,

        /// Path to t.car.u32 from Step 5
        #[arg(long)]
        turns: PathBuf,
    },

    /// Analyze equivalence classes to determine if equivalence-class hybrid is worth building
    EquivalenceAnalysis {
        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to ebg.csr from Step 4
        #[arg(long)]
        ebg_csr: PathBuf,

        /// Path to t.<mode>.u32 from Step 5
        #[arg(long)]
        turns: PathBuf,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,
    },

    /// Step 5.5: Build hybrid state graph for specified mode
    Step5Hybrid {
        /// Path to ebg.nodes from Step 4
        #[arg(long)]
        ebg_nodes: PathBuf,

        /// Path to ebg.csr from Step 4
        #[arg(long)]
        ebg_csr: PathBuf,

        /// Path to nbg.node_map from Step 3
        #[arg(long)]
        nbg_node_map: PathBuf,

        /// Path to turn_rules.<mode>.bin from Step 2
        #[arg(long)]
        turn_rules: PathBuf,

        /// Path to w.<mode>.u32 from Step 5
        #[arg(long)]
        weights: PathBuf,

        /// Path to t.<mode>.u32 from Step 5
        #[arg(long)]
        turns: PathBuf,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output directory for hybrid.<mode>.state
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 6 (Hybrid): Generate CCH ordering on hybrid state graph
    Step6Hybrid {
        /// Path to hybrid.<mode>.state from Step 5.5
        #[arg(long)]
        hybrid_state: PathBuf,

        /// Path to nbg.geo from Step 3 (for coordinates)
        #[arg(long)]
        nbg_geo: PathBuf,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output directory for order.hybrid.<mode>.ebg
        #[arg(short, long)]
        outdir: PathBuf,

        /// Leaf threshold for recursion (default: 8192)
        #[arg(long, default_value = "8192")]
        leaf_threshold: usize,

        /// Balance epsilon (default: 0.05)
        #[arg(long, default_value = "0.05")]
        balance_eps: f32,
    },

    /// Step 7 (Hybrid): Build CCH topology via contraction on hybrid state graph
    Step7Hybrid {
        /// Path to hybrid.<mode>.state from Step 5.5
        #[arg(long)]
        hybrid_state: PathBuf,

        /// Path to order.hybrid.<mode>.ebg from Step 6 Hybrid
        #[arg(long)]
        order: PathBuf,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.hybrid.<mode>.topo
        #[arg(short, long)]
        outdir: PathBuf,
    },

    /// Step 8 (Hybrid): Customize CCH with weights from hybrid state graph
    Step8Hybrid {
        /// Path to cch.hybrid.<mode>.topo from Step 7 Hybrid
        #[arg(long)]
        cch_topo: PathBuf,

        /// Path to hybrid.<mode>.state from Step 5.5
        #[arg(long)]
        hybrid_state: PathBuf,

        /// Mode (car, bike, foot)
        #[arg(long)]
        mode: String,

        /// Output directory for cch.w.hybrid.<mode>.u32
        #[arg(short, long)]
        outdir: PathBuf,
    },
}

impl Cli {
    pub fn run(self) -> Result<()> {
        match self.command {
            Commands::Step1Ingest {
                input,
                outdir,
                threads,
                verify_only,
            } => {
                if verify_only {
                    // Verify mode: check existing files
                    let nodes_sa_path = outdir.join("nodes.sa");
                    let nodes_si_path = outdir.join("nodes.si");
                    let ways_path = outdir.join("ways.raw");
                    let relations_path = outdir.join("relations.raw");

                    verify_lock_conditions(&nodes_sa_path, &nodes_si_path, &ways_path, &relations_path)?;
                } else {
                    // Ingest mode: run the pipeline
                    let config = IngestConfig {
                        input: input.clone(),
                        outdir: outdir.clone(),
                        threads,
                    };

                    let result = run_ingest(config)?;

                    // Verify the output
                    println!();
                    verify_lock_conditions(
                        &result.nodes_sa_file,
                        &result.nodes_si_file,
                        &result.ways_file,
                        &result.relations_file,
                    )?;

                    // Generate lock file
                    println!();
                    let lock = LockFile::create(
                        &input,
                        &result.nodes_sa_file,
                        &result.nodes_si_file,
                        &result.ways_file,
                        &result.relations_file,
                        Counts {
                            nodes: result.nodes_count,
                            ways: result.ways_count,
                            relations: result.relations_count,
                        },
                    )?;

                    let lock_path = outdir.join("step1.lock.json");
                    lock.write(&lock_path)?;

                    println!();
                    println!("🎉 Success! All lock conditions passed.");
                    println!("📋 Lock file: {}", lock_path.display());
                }

                Ok(())
            }
            Commands::Step2Profile {
                ways,
                relations,
                outdir,
            } => {
                let config = ProfileConfig {
                    ways_path: ways,
                    relations_path: relations,
                    outdir,
                };

                run_profiling(config)?;
                Ok(())
            }
            Commands::Step3Nbg {
                nodes,
                ways,
                car,
                bike,
                foot,
                outdir,
            } => {
                let config = NbgConfig {
                    nodes_sa_path: nodes,
                    ways_path: ways,
                    way_attrs_car_path: car,
                    way_attrs_bike_path: bike,
                    way_attrs_foot_path: foot,
                    outdir: outdir.clone(),
                };

                let result = build_nbg(config)?;

                // Verify lock conditions
                println!();
                crate::validate::verify_step3_lock_conditions(
                    &result.csr_path,
                    &result.geo_path,
                    &result.node_map_path,
                )?;

                // Generate lock file
                println!();
                println!("🔒 Generating Step 3 lock file...");

                let components = crate::validate::step3::compute_component_stats(&result.csr_path)?;

                let lock = crate::validate::Step3LockFile::create(
                    &result.csr_path,
                    &result.geo_path,
                    &result.node_map_path,
                    result.n_nodes,
                    result.n_edges_und,
                    components,
                    0, // RSS tracking would require build-time instrumentation
                )?;

                let lock_path = outdir.join("step3.lock.json");
                lock.write(&lock_path)?;
                println!("  ✓ Wrote {}", lock_path.display());

                println!();
                println!("🎉 Success! All lock conditions passed.");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step4Ebg {
                nbg_csr,
                nbg_geo,
                nbg_node_map,
                way_attrs_car,
                way_attrs_bike,
                way_attrs_foot,
                turn_rules_car,
                turn_rules_bike,
                turn_rules_foot,
                outdir,
            } => {
                let config = EbgConfig {
                    nbg_csr_path: nbg_csr.clone(),
                    nbg_geo_path: nbg_geo.clone(),
                    nbg_node_map_path: nbg_node_map.clone(),
                    way_attrs_car_path: way_attrs_car.clone(),
                    way_attrs_bike_path: way_attrs_bike.clone(),
                    way_attrs_foot_path: way_attrs_foot.clone(),
                    turn_rules_car_path: turn_rules_car.clone(),
                    turn_rules_bike_path: turn_rules_bike.clone(),
                    turn_rules_foot_path: turn_rules_foot.clone(),
                    outdir: outdir.clone(),
                };

                let result = build_ebg(config)?;

                // Run validation and generate lock file
                println!();
                let lock_file = validate_step4(
                    &result.nodes_path,
                    &result.csr_path,
                    &result.turn_table_path,
                    &nbg_csr,
                    &nbg_geo,
                    &nbg_node_map,
                    &way_attrs_car,
                    &way_attrs_bike,
                    &way_attrs_foot,
                    &turn_rules_car,
                    &turn_rules_bike,
                    &turn_rules_foot,
                    result.build_time_ms,
                )?;

                let lock_path = outdir.join("step4.lock.json");
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ EBG validation complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step5Weights {
                ebg_nodes,
                ebg_csr,
                turn_table,
                nbg_geo,
                way_attrs_car,
                way_attrs_bike,
                way_attrs_foot,
                outdir,
            } => {
                let result = step5::generate_weights(
                    &ebg_nodes,
                    &ebg_csr,
                    &turn_table,
                    &nbg_geo,
                    &way_attrs_car,
                    &way_attrs_bike,
                    &way_attrs_foot,
                    &outdir,
                )?;

                // Run validation and generate lock file
                println!();
                let lock_file = validate_step5(
                    &result,
                    &ebg_nodes,
                    &ebg_csr,
                    &turn_table,
                    &nbg_geo,
                    &way_attrs_car,
                    &way_attrs_bike,
                    &way_attrs_foot,
                )?;

                let lock_path = outdir.join("step5.lock.json");
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 5 weights validation complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step6Order {
                filtered_ebg,
                ebg_nodes,
                nbg_geo,
                mode,
                outdir,
                leaf_threshold,
                balance_eps,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let config = step6::Step6Config {
                    filtered_ebg_path: filtered_ebg.clone(),
                    ebg_nodes_path: ebg_nodes,
                    nbg_geo_path: nbg_geo,
                    mode,
                    outdir: outdir.clone(),
                    leaf_threshold,
                    balance_eps,
                };

                let result = step6::generate_ordering(config)?;

                // Run validation and generate lock file
                println!();
                let lock_file = validate_step6(&result, &filtered_ebg)?;

                let mode_name = match result.mode {
                    Mode::Car => "car",
                    Mode::Bike => "bike",
                    Mode::Foot => "foot",
                };
                let lock_path = outdir.join(format!("step6.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 6 ordering complete for {} mode!", mode_name);
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step7Contract {
                filtered_ebg,
                order,
                weights,
                turns,
                mode,
                outdir,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let config = step7::Step7Config {
                    filtered_ebg_path: filtered_ebg.clone(),
                    order_path: order.clone(),
                    weights_path: weights,
                    turns_path: turns,
                    mode,
                    outdir: outdir.clone(),
                };

                let result = step7::build_cch_topology(config)?;

                // Run validation and generate lock file
                println!();
                let lock_file = validate_step7(&result, &filtered_ebg, &order)?;

                let mode_name = match result.mode {
                    Mode::Car => "car",
                    Mode::Bike => "bike",
                    Mode::Foot => "foot",
                };
                let lock_path = outdir.join(format!("step7.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 7 CCH contraction complete for {} mode!", mode_name);
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step8Customize {
                cch_topo,
                filtered_ebg,
                order,
                weights,
                turns,
                mode,
                outdir,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let config = step8::Step8Config {
                    cch_topo_path: cch_topo,
                    filtered_ebg_path: filtered_ebg,
                    order_path: order,
                    weights_path: weights,
                    turns_path: turns,
                    mode,
                    outdir: outdir.clone(),
                };

                let result = step8::customize_cch(config)?;

                // Generate lock file
                let mode_name = match result.mode {
                    Mode::Car => "car",
                    Mode::Bike => "bike",
                    Mode::Foot => "foot",
                };

                let lock = serde_json::json!({
                    "mode": mode_name,
                    "output_path": result.output_path.display().to_string(),
                    "n_up_edges": result.n_up_edges,
                    "n_down_edges": result.n_down_edges,
                    "customize_time_ms": result.customize_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(format!("step8.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 8 CCH customization complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Serve { data_dir, port } => {
                // Create tokio runtime
                let rt = tokio::runtime::Runtime::new()?;
                rt.block_on(step9::serve(&data_dir, port))?;
                Ok(())
            }
            Commands::ValidateCch {
                cch_topo,
                cch_weights,
                order,
                mode,
                n_pairs,
                seed,
                failures_file,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let (result, failures) = crate::validate::validate_cch_correctness(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    n_pairs,
                    seed,
                    mode,
                )?;

                // Write failures to file if requested
                if let Some(path) = failures_file {
                    use std::io::Write;
                    let mut f = std::fs::File::create(&path)?;
                    writeln!(f, "src,dst,bidi_cost,baseline_cost,diff")?;
                    for failure in &failures {
                        let diff = (failure.bidi_cost as i64) - (failure.baseline_cost as i64);
                        writeln!(f, "{},{},{},{},{}",
                                 failure.src, failure.dst, failure.bidi_cost, failure.baseline_cost, diff)?;
                    }
                    println!("\nFailures written to: {}", path.display());
                }

                if result.mismatches > 0 {
                    anyhow::bail!("Validation failed with {} mismatches", result.mismatches);
                }

                Ok(())
            }
            Commands::RegressionCch {
                cch_topo,
                cch_weights,
                order,
                mode,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let results = crate::validate::run_regression_tests(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    mode,
                )?;

                let failed_count = results.iter().filter(|r| !r.passed).count();
                if failed_count > 0 {
                    anyhow::bail!("Regression tests failed: {} failures", failed_count);
                }

                Ok(())
            }
            Commands::ValidateInvariants {
                cch_topo,
                cch_weights,
                order,
                mode,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let result = crate::validate::validate_invariants(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    mode,
                )?;

                if !result.passed {
                    anyhow::bail!("Invariant validation failed with {} errors", result.errors.len());
                }

                Ok(())
            }
            Commands::RangeCch {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                threshold_ms,
                mode,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let result = crate::range::run_range_query(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    threshold_ms,
                    mode,
                )?;

                // Success if no errors in verification
                let engine = crate::range::RangeEngine::load(&cch_topo, &cch_weights, &order, mode)?;
                let errors = engine.verify(&result, origin_node, threshold_ms);
                if !errors.is_empty() {
                    anyhow::bail!("Range query verification failed with {} errors", errors.len());
                }

                Ok(())
            }
            Commands::ValidateRange {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                mode,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                crate::range::run_range_validation(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    mode,
                )?;

                Ok(())
            }
            Commands::PhastRange {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                threshold_ms,
                mode,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                crate::range::run_phast_query(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    threshold_ms,
                    mode,
                )?;

                Ok(())
            }
            Commands::ValidatePhast {
                cch_topo,
                cch_weights,
                order,
                origin_node,
                threshold_ms,
                mode,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                crate::range::validate_phast(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    origin_node,
                    threshold_ms,
                    mode,
                )?;

                Ok(())
            }
            Commands::ValidateBlockGated {
                cch_topo,
                cch_weights,
                order,
                origins,
                thresholds,
            } => {
                // Parse origins
                let origins: Vec<u32> = origins
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();

                // Parse thresholds
                let thresholds: Vec<u32> = thresholds
                    .split(',')
                    .filter_map(|s| s.trim().parse().ok())
                    .collect();

                crate::range::validate_block_gated_phast(
                    &cch_topo,
                    &cch_weights,
                    &order,
                    &origins,
                    &thresholds,
                )?;

                Ok(())
            }
            Commands::ExtractFrontier {
                cch_topo,
                cch_weights,
                order,
                filtered_ebg,
                ebg_nodes,
                nbg_geo,
                base_weights,
                origin_node,
                threshold_ms,
                mode,
                geojson_out,
            } => {
                // Parse mode
                let mode = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                // First run PHAST to get distances
                println!("Running PHAST to compute distances...");
                let phast_engine = crate::range::PhastEngine::load(&cch_topo, &cch_weights, &order)?;
                let phast_result = phast_engine.query_bounded(origin_node, threshold_ms);
                println!("  ✓ PHAST complete: {} reachable nodes in {} ms",
                         phast_result.n_reachable, phast_result.stats.total_time_ms);

                // Then extract frontier on base graph
                let cut_points = crate::range::run_frontier_extraction(
                    &filtered_ebg,
                    &ebg_nodes,
                    &nbg_geo,
                    &base_weights,
                    &phast_result.dist,
                    threshold_ms,
                    mode,
                )?;

                println!("\n✅ Frontier extraction complete: {} cut points", cut_points.len());

                // Export to GeoJSON if requested
                if let Some(geojson_path) = geojson_out {
                    crate::range::frontier::export_geojson(&cut_points, &geojson_path)?;
                    println!("  Exported to: {}", geojson_path.display());
                }

                Ok(())
            }
            Commands::Isochrone {
                cch_topo,
                cch_weights,
                order,
                filtered_ebg,
                ebg_nodes,
                nbg_geo,
                base_weights,
                origin_node,
                threshold_ms,
                mode,
                output,
                grid_size,
            } => {
                // Parse mode
                let mode_enum = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let mode_name = mode.to_lowercase();

                println!("\n🗺️  Isochrone Generation ({} mode)", mode_name);
                println!("  Origin: node {}", origin_node);
                println!("  Threshold: {} ms ({:.1} min)", threshold_ms, threshold_ms as f64 / 60_000.0);

                // Step 1: PHAST distances
                println!("\n[1/4] Running PHAST...");
                let phast_engine = crate::range::PhastEngine::load(&cch_topo, &cch_weights, &order)?;
                let phast_result = phast_engine.query_bounded(origin_node, threshold_ms);
                println!("  ✓ {} reachable nodes in {} ms",
                         phast_result.n_reachable, phast_result.stats.total_time_ms);

                // Step 2: Extract reachable road segments
                println!("\n[2/4] Extracting reachable road segments...");
                let extractor = crate::range::FrontierExtractor::load(
                    &filtered_ebg,
                    &ebg_nodes,
                    &nbg_geo,
                    &base_weights,
                )?;
                let segments = extractor.extract_reachable_segments(&phast_result.dist, threshold_ms);
                println!("  ✓ {} reachable road segments", segments.len());

                // Step 3: Generate contour (grid fill + marching squares)
                println!("\n[3/4] Generating contour...");
                let config = if let Some(size) = grid_size {
                    crate::range::GridConfig {
                        cell_size_m: size,
                        simplify_tolerance_m: size,
                        closing_iterations: 1,
                    }
                } else {
                    match mode_enum {
                        Mode::Car => crate::range::GridConfig::for_car(),
                        Mode::Bike => crate::range::GridConfig::for_bike(),
                        Mode::Foot => crate::range::GridConfig::for_foot(),
                    }
                };

                println!("  Grid: {}m cells, {}m simplify, {} closing iterations",
                         config.cell_size_m, config.simplify_tolerance_m, config.closing_iterations);

                let contour = crate::range::generate_contour(&segments, &config)?;

                println!("  ✓ {}x{} grid, {} filled cells → {} vertices (before simplify: {})",
                         contour.stats.grid_cols, contour.stats.grid_rows,
                         contour.stats.filled_cells,
                         contour.stats.contour_vertices_after_simplify,
                         contour.stats.contour_vertices_before_simplify);

                // Step 4: Export
                println!("\n[4/4] Exporting GeoJSON...");
                crate::range::export_contour_geojson(&contour, &output)?;

                let file_size = std::fs::metadata(&output)?.len();
                println!("  ✓ Saved to: {} ({:.1} KB)", output.display(), file_size as f64 / 1024.0);

                println!("\n=== ISOCHRONE COMPLETE ===");
                println!("  Total vertices: {}", contour.stats.contour_vertices_after_simplify);
                println!("  Processing time: {} ms", contour.stats.elapsed_ms);

                Ok(())
            }
            Commands::HybridAnalysis {
                ebg_nodes,
                ebg_csr,
                nbg_node_map,
                turn_rules_car,
                weights,
                turns,
            } => {
                use crate::formats::{EbgNodesFile, EbgCsrFile, NbgNodeMapFile, turn_rules, mod_weights, mod_turns};
                use crate::hybrid::HybridGraphBuilder;

                println!("\n=== HYBRID STATE GRAPH ANALYSIS ===\n");

                // Load EBG nodes
                println!("[1/6] Loading EBG nodes...");
                let ebg_nodes_data = EbgNodesFile::read(&ebg_nodes)?;
                println!("  ✓ {} EBG nodes", ebg_nodes_data.nodes.len());

                // Load EBG CSR
                println!("[2/6] Loading EBG CSR...");
                let ebg_csr_data = EbgCsrFile::read(&ebg_csr)?;
                println!("  ✓ {} arcs", ebg_csr_data.heads.len());

                // Load NBG node map (OSM ID → compact ID)
                println!("[3/6] Loading NBG node map...");
                let osm_to_nbg = NbgNodeMapFile::read(&nbg_node_map)?;
                println!("  ✓ {} OSM→NBG mappings", osm_to_nbg.len());

                // Compute actual n_nbg_nodes from EBG node data (max NBG ID + 1)
                let n_nbg_nodes = ebg_nodes_data.nodes.iter()
                    .flat_map(|n| [n.tail_nbg, n.head_nbg])
                    .max()
                    .map(|m| m as usize + 1)
                    .unwrap_or(0);
                println!("  ✓ {} NBG nodes (from EBG)", n_nbg_nodes);

                // Load turn rules
                println!("[4/6] Loading turn rules (car mode)...");
                let turn_rules_data = turn_rules::read_all(&turn_rules_car)?;
                println!("  ✓ {} turn rules", turn_rules_data.len());

                // Load weights
                println!("[5/6] Loading weights...");
                let weights_data = mod_weights::read_all(&weights)?;
                println!("  ✓ {} weights", weights_data.weights.len());

                // Load turn costs
                let turns_data = mod_turns::read_all(&turns)?;
                println!("  ✓ {} turn costs", turns_data.penalties.len());

                // Build hybrid state graph
                println!("\n[6/6] Building hybrid state graph...");
                let mut builder = HybridGraphBuilder::new();
                builder.classify_nodes(&turn_rules_data, &osm_to_nbg);

                let hybrid_graph = builder.build(
                    &ebg_nodes_data,
                    &ebg_csr_data,
                    &weights_data.weights,
                    &turns_data.penalties,
                    n_nbg_nodes,
                );

                println!();
                hybrid_graph.print_stats();

                println!("\n=== HYBRID ANALYSIS COMPLETE ===");
                println!();
                println!("Expected performance impact:");
                println!("  State reduction: {:.2}x → proportional speedup in searches", hybrid_graph.stats.state_reduction_ratio);
                println!("  Arc reduction: {:.2}x → proportional speedup in relaxations", hybrid_graph.stats.arc_reduction_ratio);
                println!();
                println!("Next step: Build CCH on hybrid state graph for actual benchmark.");

                Ok(())
            }
            Commands::EquivalenceAnalysis {
                ebg_nodes,
                ebg_csr,
                turns,
                mode,
            } => {
                use crate::formats::{EbgNodesFile, EbgCsrFile, mod_turns};
                use crate::hybrid::analyze_equivalence_classes;

                let mode_name = mode.to_lowercase();
                println!("\n=== EQUIVALENCE CLASS ANALYSIS ({}) ===\n", mode_name);

                // Load EBG nodes
                println!("[1/3] Loading EBG nodes...");
                let ebg_nodes_data = EbgNodesFile::read(&ebg_nodes)?;
                let ebg_nodes_vec: Vec<(u32, u32)> = ebg_nodes_data.nodes.iter()
                    .map(|n| (n.tail_nbg, n.head_nbg))
                    .collect();
                println!("  ✓ {} EBG nodes", ebg_nodes_vec.len());

                // Load EBG CSR
                println!("[2/3] Loading EBG CSR...");
                let ebg_csr_data = EbgCsrFile::read(&ebg_csr)?;
                println!("  ✓ {} arcs", ebg_csr_data.heads.len());

                // Load turn costs
                println!("[3/3] Loading turn costs...");
                let turns_data = mod_turns::read_all(&turns)?;
                println!("  ✓ {} turn costs", turns_data.penalties.len());

                // Run equivalence analysis
                println!("\nAnalyzing equivalence classes...");
                let analysis = analyze_equivalence_classes(
                    &ebg_nodes_vec,
                    &ebg_csr_data.offsets,
                    &ebg_csr_data.heads,
                    &turns_data.penalties,
                );

                analysis.print();

                Ok(())
            }
            Commands::Step5Hybrid {
                ebg_nodes,
                ebg_csr,
                nbg_node_map,
                turn_rules,
                weights,
                turns,
                mode,
                outdir,
            } => {
                use crate::formats::{EbgNodesFile, EbgCsrFile, NbgNodeMapFile, turn_rules as tr, mod_weights, mod_turns, HybridStateFile};
                use crate::hybrid::HybridGraphBuilder;
                use sha2::{Sha256, Digest as Sha2Digest};

                // Parse mode
                let mode_enum = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };
                let mode_name = mode.to_lowercase();

                println!("\n=== STEP 5.5: HYBRID STATE GRAPH ({}) ===\n", mode_name);

                // Create output directory
                std::fs::create_dir_all(&outdir)?;

                // Load EBG nodes
                println!("[1/7] Loading EBG nodes...");
                let ebg_nodes_data = EbgNodesFile::read(&ebg_nodes)?;
                println!("  ✓ {} EBG nodes", ebg_nodes_data.nodes.len());

                // Build ebg_head_nbg mapping
                let ebg_head_nbg: Vec<u32> = ebg_nodes_data.nodes.iter()
                    .map(|n| n.head_nbg)
                    .collect();

                // Load EBG CSR
                println!("[2/7] Loading EBG CSR...");
                let ebg_csr_data = EbgCsrFile::read(&ebg_csr)?;
                println!("  ✓ {} arcs", ebg_csr_data.heads.len());

                // Load NBG node map (OSM ID → compact ID)
                println!("[3/7] Loading NBG node map...");
                let osm_to_nbg = NbgNodeMapFile::read(&nbg_node_map)?;
                println!("  ✓ {} OSM→NBG mappings", osm_to_nbg.len());

                // Compute actual n_nbg_nodes from EBG node data (max NBG ID + 1)
                let n_nbg_nodes = ebg_nodes_data.nodes.iter()
                    .flat_map(|n| [n.tail_nbg, n.head_nbg])
                    .max()
                    .map(|m| m as usize + 1)
                    .unwrap_or(0);
                println!("  ✓ {} NBG nodes (from EBG)", n_nbg_nodes);

                // Load turn rules
                println!("[4/7] Loading turn rules ({} mode)...", mode_name);
                let turn_rules_data = tr::read_all(&turn_rules)?;
                println!("  ✓ {} turn rules", turn_rules_data.len());

                // Load weights
                println!("[5/7] Loading weights...");
                let weights_data = mod_weights::read_all(&weights)?;
                println!("  ✓ {} weights", weights_data.weights.len());

                // Load turn costs
                let turns_data = mod_turns::read_all(&turns)?;
                println!("  ✓ {} turn costs", turns_data.penalties.len());

                // Build hybrid state graph
                println!("\n[6/7] Building hybrid state graph...");
                let mut builder = HybridGraphBuilder::new();
                builder.classify_nodes(&turn_rules_data, &osm_to_nbg);

                let hybrid_graph = builder.build(
                    &ebg_nodes_data,
                    &ebg_csr_data,
                    &weights_data.weights,
                    &turns_data.penalties,
                    n_nbg_nodes,
                );

                println!();
                hybrid_graph.print_stats();

                // Compute inputs SHA for reproducibility
                let mut hasher = Sha256::new();
                hasher.update(ebg_nodes.to_string_lossy().as_bytes());
                hasher.update(ebg_csr.to_string_lossy().as_bytes());
                hasher.update(nbg_node_map.to_string_lossy().as_bytes());
                hasher.update(turn_rules.to_string_lossy().as_bytes());
                hasher.update(weights.to_string_lossy().as_bytes());
                hasher.update(turns.to_string_lossy().as_bytes());
                let hash = hasher.finalize();
                let mut inputs_sha = [0u8; 32];
                inputs_sha.copy_from_slice(&hash);

                // Convert to format and serialize
                println!("\n[7/7] Serializing hybrid state graph...");
                let format_data = hybrid_graph.to_format(mode_enum, ebg_head_nbg, inputs_sha);

                let output_path = outdir.join(format!("hybrid.{}.state", mode_name));
                HybridStateFile::write(&output_path, &format_data)?;

                let file_size = std::fs::metadata(&output_path)?.len();
                println!("  ✓ Wrote {} ({:.1} MB)", output_path.display(), file_size as f64 / 1_000_000.0);

                println!("\n=== STEP 5.5 COMPLETE ===");
                println!();
                println!("Output: {}", output_path.display());
                println!("State reduction: {:.2}x", hybrid_graph.stats.state_reduction_ratio);
                println!("Arc reduction: {:.2}x", hybrid_graph.stats.arc_reduction_ratio);
                println!();
                println!("Next: Run Step 6 ordering on hybrid.{}.state", mode_name);

                Ok(())
            }
            Commands::Step6Hybrid {
                hybrid_state,
                nbg_geo,
                mode,
                outdir,
                leaf_threshold,
                balance_eps,
            } => {
                // Parse mode
                let mode_enum = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let config = step6::Step6HybridConfig {
                    hybrid_state_path: hybrid_state.clone(),
                    nbg_geo_path: nbg_geo,
                    mode: mode_enum,
                    outdir: outdir.clone(),
                    leaf_threshold,
                    balance_eps,
                };

                let result = step6::generate_ordering_hybrid(config)?;

                // Generate lock file
                let mode_name = mode.to_lowercase();
                let lock = serde_json::json!({
                    "mode": mode_name,
                    "graph_type": "hybrid",
                    "order_path": result.order_path.display().to_string(),
                    "n_nodes": result.n_nodes,
                    "n_components": result.n_components,
                    "tree_depth": result.tree_depth,
                    "build_time_ms": result.build_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(format!("step6.hybrid.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 6 (Hybrid) ordering complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step7Hybrid {
                hybrid_state,
                order,
                mode,
                outdir,
            } => {
                // Parse mode
                let mode_enum = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let config = step7::Step7HybridConfig {
                    hybrid_state_path: hybrid_state.clone(),
                    order_path: order.clone(),
                    mode: mode_enum,
                    outdir: outdir.clone(),
                };

                let result = step7::build_cch_topology_hybrid(config)?;

                // Generate lock file
                let mode_name = mode.to_lowercase();
                let lock = serde_json::json!({
                    "mode": mode_name,
                    "graph_type": "hybrid",
                    "topo_path": result.topo_path.display().to_string(),
                    "n_nodes": result.n_nodes,
                    "n_original_arcs": result.n_original_arcs,
                    "n_shortcuts": result.n_shortcuts,
                    "n_up_edges": result.n_up_edges,
                    "n_down_edges": result.n_down_edges,
                    "build_time_ms": result.build_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(format!("step7.hybrid.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 7 (Hybrid) CCH contraction complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
            Commands::Step8Hybrid {
                cch_topo,
                hybrid_state,
                mode,
                outdir,
            } => {
                // Parse mode
                let mode_enum = match mode.to_lowercase().as_str() {
                    "car" => Mode::Car,
                    "bike" => Mode::Bike,
                    "foot" => Mode::Foot,
                    _ => anyhow::bail!("Invalid mode: {}. Use car, bike, or foot.", mode),
                };

                let config = step8::Step8HybridConfig {
                    cch_topo_path: cch_topo,
                    hybrid_state_path: hybrid_state,
                    mode: mode_enum,
                    outdir: outdir.clone(),
                };

                let result = step8::customize_cch_hybrid(config)?;

                // Generate lock file
                let mode_name = mode.to_lowercase();
                let lock = serde_json::json!({
                    "mode": mode_name,
                    "graph_type": "hybrid",
                    "output_path": result.output_path.display().to_string(),
                    "n_up_edges": result.n_up_edges,
                    "n_down_edges": result.n_down_edges,
                    "customize_time_ms": result.customize_time_ms,
                    "created_at_utc": chrono::Utc::now().to_rfc3339(),
                });

                let lock_path = outdir.join(format!("step8.hybrid.{}.lock.json", mode_name));
                let lock_json = serde_json::to_string_pretty(&lock)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ Step 8 (Hybrid) CCH customization complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
        }
    }
}
