use clap::Parser;
use std::path::PathBuf;

#[derive(Parser)]
#[command(author, version, about = "A tool to shrink OpenStreetMap data", long_about = None)]
struct Cli {
    /// Input PBF file
    #[arg(value_name = "INPUT_FILE")]
    input: PathBuf,

    /// Output PBF file
    #[arg(value_name = "OUTPUT_FILE")]
    output: PathBuf,
}

fn main() -> butterfly_common::Result<()> {
    let cli = Cli::parse();

    // For now, just echo the input to output
    butterfly_shrink::echo_pbf(&cli.input, &cli.output)?;

    Ok(())
}
