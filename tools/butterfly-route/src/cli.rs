///! CLI commands for butterfly-route

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::ingest::{run_ingest, IngestConfig};
use crate::validate::{verify_lock_conditions, verify_step3_lock_conditions, validate_step4, Counts, LockFile};
use crate::profile::{run_profiling, ProfileConfig};
use crate::nbg::{build_nbg, NbgConfig};
use crate::ebg::{build_ebg, EbgConfig};

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
                    &turn_rules_car,
                    &turn_rules_bike,
                    &turn_rules_foot,
                )?;

                let lock_path = outdir.join("step4.lock.json");
                let lock_json = serde_json::to_string_pretty(&lock_file)?;
                std::fs::write(&lock_path, lock_json)?;

                println!();
                println!("✅ EBG validation complete!");
                println!("📋 Lock file: {}", lock_path.display());

                Ok(())
            }
        }
    }
}
