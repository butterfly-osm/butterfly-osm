///! CLI commands for butterfly-route

use anyhow::Result;
use clap::{Parser, Subcommand};
use std::path::PathBuf;

use crate::ingest::{run_ingest, IngestConfig};
use crate::validate::{verify_lock_conditions, Counts, LockFile};

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
        }
    }
}
