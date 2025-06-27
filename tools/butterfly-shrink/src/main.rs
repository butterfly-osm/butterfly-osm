use clap::Parser;

#[derive(Parser)]
#[command(author, version, about, long_about = None)]
struct Cli {
    #[arg(short, long)]
    name: Option<String>,
}

fn main() {
    let cli = Cli::parse();

    if let Some(name) = cli.name.as_deref() {
        println!("Hello, {}!", name);
    } else {
        println!("Hello, world!");
    }
}
