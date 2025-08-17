//! Binary entry point for butterfly-plan CLI

use butterfly_plan::PlanCli;
use std::env;
use std::process;

fn main() {
    // Initialize logger
    env_logger::init();
    
    let args: Vec<String> = env::args().collect();
    
    // Check for special commands first
    if args.len() > 1 {
        match args[1].as_str() {
            "--validate-plan" => {
                let cli = create_cli_from_args(&args[2..]);
                match cli.validate_plan() {
                    Ok(_) => process::exit(0),
                    Err(e) => {
                        eprintln!("Validation failed: {}", e);
                        process::exit(1);
                    }
                }
            }
            "--debug-plan" => {
                let cli = create_cli_from_args(&args[2..]);
                cli.debug_plan();
                process::exit(0);
            }
            "--help" | "-h" => {
                print_help();
                process::exit(0);
            }
            _ => {
                eprintln!("Unknown command: {}", args[1]);
                eprintln!("Use --help for usage information.");
                process::exit(1);
            }
        }
    } else {
        print_help();
        process::exit(0);
    }
}

fn create_cli_from_args(args: &[String]) -> PlanCli {
    // Create CLI with remaining args
    let mut full_args = vec!["butterfly-plan".to_string()];
    full_args.extend_from_slice(args);
    
    let mut cli = match PlanCli::from_args(full_args) {
        Ok(cli) => cli,
        Err(e) => {
            eprintln!("Error parsing arguments: {}", e);
            process::exit(1);
        }
    };
    
    // Load from environment and config files
    cli.load_env();
    
    // Try to load from default config file
    if let Ok(home) = env::var("HOME") {
        let config_path = format!("{}/.config/butterfly/plan.toml", home);
        let _ = cli.load_toml(&config_path); // Ignore errors for optional config
    }
    
    cli
}

fn print_help() {
    println!("butterfly-plan - Autopilot memory planning and validation");
    println!();
    println!("USAGE:");
    println!("    butterfly-plan [COMMAND] [OPTIONS]");
    println!();
    println!("COMMANDS:");
    println!("    --validate-plan    Validate memory budget with detailed output");
    println!("    --debug-plan       Show debug information about the current plan");
    println!("    --help             Show this help message");
    println!();
    println!("OPTIONS:");
    println!("    --max-ram <MB>     Maximum RAM usage in MB (default: {})", butterfly_plan::BFLY_MAX_RAM_MB);
    println!("    --workers <N>      Number of worker threads (default: auto-detect)");
    println!("    --deterministic    Enable deterministic mode (fixed parameters)");
    println!("    --debug-plan       Enable debug output during validation");
    println!();
    println!("ENVIRONMENT VARIABLES:");
    println!("    BFLY_MAX_RAM_MB     Override maximum RAM");
    println!("    BFLY_WORKERS        Override worker count");
    println!("    BFLY_DETERMINISTIC  Enable deterministic mode");
    println!("    BFLY_DEBUG          Enable debug output");
    println!();
    println!("CONFIG FILE:");
    println!("    ~/.config/butterfly/plan.toml    Optional TOML configuration");
    println!();
    println!("EXAMPLES:");
    println!("    butterfly-plan --validate-plan");
    println!("    butterfly-plan --debug-plan --max-ram 8192");
    println!("    butterfly-plan --validate-plan --workers 4 --deterministic");
}