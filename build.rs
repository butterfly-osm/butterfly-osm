use std::env;

fn main() {
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
}