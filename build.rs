use std::env;
use std::fs;
use std::path::Path;

fn main() {
    // Read version from VERSION file
    let version = fs::read_to_string("VERSION")
        .expect("Failed to read VERSION file")
        .trim()
        .to_string();
    
    // Set version as environment variable for use in code
    println!("cargo:rustc-env=BUTTERFLY_VERSION={}", version);
    
    // Generate version.rs file
    let out_dir = env::var("OUT_DIR").unwrap();
    let dest_path = Path::new(&out_dir).join("version.rs");
    fs::write(
        dest_path,
        format!("pub const VERSION: &str = \"{}\";", version)
    ).expect("Failed to write version.rs");
    
    // Set linking information for C libraries
    if cfg!(feature = "c-bindings") {
        // Ensure we link against the C runtime
        println!("cargo:rustc-link-lib=c");
        
        // Set library search paths for pkg-config
        if let Ok(pkg_config_path) = env::var("PKG_CONFIG_PATH") {
            println!("cargo:rustc-env=PKG_CONFIG_PATH={}", pkg_config_path);
        }
    }

    // Print feature information
    println!("cargo:rerun-if-changed=src/");
    println!("cargo:rerun-if-changed=Cargo.toml");
    println!("cargo:rerun-if-changed=VERSION");
}