# New Tool Template

This document provides a template for adding new tools to the butterfly-osm workspace.

## Creating a New Tool

### 1. Directory Structure
```
tools/
└── butterfly-{tool-name}/
    ├── Cargo.toml
    ├── src/
    │   ├── main.rs
    │   ├── lib.rs
    │   └── core/
    ├── tests/
    └── README.md
```

### 2. Cargo.toml Template
```toml
[package]
name = "butterfly-{tool-name}"
version.workspace = true
authors.workspace = true
license.workspace = true
repository.workspace = true
edition.workspace = true
description = "Brief description of the tool"
keywords = ["osm", "openstreetmap", "{tool-name}"]
categories = ["command-line-utilities", "api-bindings"]
homepage = "https://github.com/butterfly-osm/butterfly-osm"
documentation = "https://docs.rs/butterfly-{tool-name}"
readme = "../../README.md"

[lib]
name = "butterfly_{tool_name}"
path = "src/lib.rs"
crate-type = ["cdylib", "staticlib", "rlib"]

[[bin]]
name = "butterfly-{tool-name}"
path = "src/main.rs"

[features]
default = ["cli"]
cli = ["clap", "tokio"]
c-bindings = []

[dependencies]
butterfly-common = { path = "../../butterfly-common", version = "2.0", features = ["http"] }
tokio.workspace = true
clap.workspace = true
thiserror.workspace = true
# Add tool-specific dependencies here

[dev-dependencies]
tempfile.workspace = true
ctor.workspace = true
```

### 3. Update Workspace
Add the new tool to the root `Cargo.toml`:
```toml
[workspace]
members = [
    "butterfly-common",
    "tools/butterfly-dl",
    "tools/butterfly-{tool-name}",  # Add this line
]
```

### 4. Library Template (src/lib.rs)
```rust
//! # Butterfly-{tool-name} Library
//!
//! Brief description of what this tool does for OSM data.

// Re-export common types
pub use butterfly_common::{Error, Result};

// Tool-specific exports
// pub use crate::core::SomeStruct;

mod core;

#[cfg(feature = "c-bindings")]
pub mod ffi;

/// Main function for {tool-name} operations
pub async fn process(input: &str, output: Option<&str>) -> Result<()> {
    // Implementation here
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic_functionality() {
        // Add tests here
    }
}
```

### 5. CLI Template (src/main.rs)
```rust
//! Butterfly-{tool-name} - Command line interface

use clap::{Arg, Command};
use std::process;

#[tokio::main]
async fn main() {
    let matches = Command::new("butterfly-{tool-name}")
        .version(env!("CARGO_PKG_VERSION"))
        .author("Pierre <pierre@warnier.net>")
        .about("Brief description of the tool")
        .arg(
            Arg::new("input")
                .help("Input specification")
                .required(true)
                .index(1),
        )
        .arg(
            Arg::new("output")
                .help("Output file path")
                .short('o')
                .long("output")
                .value_name("FILE"),
        )
        .get_matches();

    let input = matches.get_one::<String>("input").unwrap();
    let output = matches.get_one::<String>("output").map(|s| s.as_str());

    if let Err(e) = butterfly_{tool_name}::process(input, output).await {
        eprintln!("Error: {}", e);
        process::exit(1);
    }
}
```

### 6. Core Module Template (src/core/mod.rs)
```rust
//! Core functionality for butterfly-{tool-name}

use butterfly_common::{Error, Result};

// Add your core functionality here
```

## Building and Testing

### Build the new tool:
```bash
cargo build -p butterfly-{tool-name}
```

### Test the new tool:
```bash
cargo test -p butterfly-{tool-name}
```

### Run the CLI:
```bash
cargo run -p butterfly-{tool-name} -- --help
```

## Publishing

Each tool can be published independently to crates.io:
```bash
cargo publish -p butterfly-{tool-name}
```

Make sure to:
1. Update version numbers appropriately
2. Test thoroughly before publishing
3. Update documentation
4. Follow semantic versioning