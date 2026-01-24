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
        }
    }
}
