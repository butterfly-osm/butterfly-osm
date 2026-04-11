use anyhow::Result;
use clap::Parser;

use butterfly_route::cli::{Cli, Commands};

fn main() -> Result<()> {
    let cli = Cli::parse();

    // Use env_logger for pipeline steps (1-8), tracing is initialized
    // inside the Serve handler via init_tracing()
    if !matches!(cli.command, Commands::Serve { .. }) {
        env_logger::init();
    }

    cli.run()?;

    Ok(())
}
