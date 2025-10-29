use anyhow::Result;
use clap::Parser;

use butterfly_route::cli::Cli;

fn main() -> Result<()> {
    env_logger::init();

    let cli = Cli::parse();
    cli.run()?;

    Ok(())
}
