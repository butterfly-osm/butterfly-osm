# Butterfly-OSM Workspace Migration Implementation Plan

## Executive Summary

This document provides a concrete implementation plan for migrating butterfly-dl to a Rust workspace structure, based on the architectural decision documented in PLAN.md. The plan focuses on the mechanical aspects of the migration while maintaining flexibility for future tool development.

## Prerequisites

- Git history preservation is critical
- Backward compatibility for existing butterfly-dl users
- Ability to publish individual crates to crates.io
- Minimal disruption to current development

## Implementation Phases

### Phase 1: Prepare for Migration (Day 1-2)

**Goal:** Set up safety measures and understand current state.

1. **Create Migration Branch:**
   ```bash
   git checkout -b workspace-migration
   ```

2. **Document Current State:**
   - Record current version (1.4.12)
   - List all public API exports
   - Document FFI interface
   - Save current Cargo.toml configuration

3. **Backup Strategy:**
   - Tag current state: `git tag pre-workspace-v1.4.12`
   - Create a maintenance branch: `git checkout -b maintenance/1.x`

4. **Test Baseline:**
   - Run full test suite and save results
   - Benchmark current performance
   - Document current build artifacts

### Phase 2: Create Workspace Structure (Day 3-4)

**Goal:** Establish workspace without breaking existing code.

1. **Create Workspace Layout:**
   ```bash
   # From repository root
   mkdir butterfly-common
   mkdir tools
   
   # Move butterfly-dl to tools subdirectory
   git mv src tools/butterfly-dl/src
   git mv Cargo.toml tools/butterfly-dl/Cargo.toml
   git mv build.rs tools/butterfly-dl/build.rs
   # ... move other butterfly-dl specific files
   ```

2. **Create Root Cargo.toml:**
   ```toml
   [workspace]
   resolver = "2"
   members = [
       "butterfly-common",
       "tools/butterfly-dl",
   ]
   
   [workspace.package]
   version = "2.0.0"
   authors = ["Pierre <pierre@warnier.net>"]
   license = "MIT"
   repository = "https://github.com/butterfly-osm/butterfly-osm"
   edition = "2021"
   
   [workspace.dependencies]
   tokio = { version = "1.45", features = ["full"] }
   reqwest = { version = "0.12", features = ["stream"] }
   clap = { version = "4.7", features = ["derive"] }
   thiserror = "2.0"
   ```

3. **Initialize butterfly-common:**
   ```bash
   cd butterfly-common
   cargo init --lib
   ```

4. **Update butterfly-dl Cargo.toml:**
   - Add workspace inheritance
   - Update relative paths
   - Maintain all current dependencies

### Phase 3: Extract Common Components (Day 5-7)

**Goal:** Identify and extract truly shared code.

1. **Analyze Code for Sharing Potential:**
   - Start with most obvious candidates:
     - Error types
     - Result type aliases
     - Common traits
   - Leave domain-specific code in butterfly-dl for now

2. **Create Minimal butterfly-common:**
   ```rust
   // butterfly-common/src/lib.rs
   pub mod error;
   pub mod result;
   
   // Start small, grow as needed
   ```

3. **Gradual Migration:**
   - Move one module at a time
   - Run tests after each move
   - Update imports incrementally

4. **Maintain Compatibility Layer:**
   ```rust
   // butterfly-dl/src/lib.rs
   // Re-export for backward compatibility
   pub use butterfly_common::error::{Error, Result};
   ```

### Phase 4: Verify and Stabilize (Day 8-9)

**Goal:** Ensure nothing is broken before adding new tools.

1. **Comprehensive Testing:**
   ```bash
   # Test workspace
   cargo test --workspace
   
   # Test individual packages
   cargo test -p butterfly-dl
   cargo test -p butterfly-common
   
   # Test release builds
   cargo build --release --workspace
   ```

2. **FFI Verification:**
   - Build C libraries
   - Verify header files still generated correctly
   - Test with a simple C program

3. **Benchmark Comparison:**
   - Run performance benchmarks
   - Compare with baseline
   - Investigate any regressions

4. **Documentation Updates:**
   - Update README for workspace structure
   - Document how to build specific tools
   - Update contribution guidelines

### Phase 5: Prepare for New Tools (Day 10)

**Goal:** Set up structure for adding new tools without disrupting butterfly-dl.

1. **Create Tool Template:**
   ```toml
   # tools/new-tool/Cargo.toml template
   [package]
   name = "butterfly-{name}"
   version.workspace = true
   authors.workspace = true
   license.workspace = true
   edition.workspace = true
   
   [dependencies]
   butterfly-common = { path = "../../butterfly-common", version = "2.0" }
   
   [lib]
   name = "butterfly_{name}"
   crate-type = ["cdylib", "staticlib", "rlib"]
   
   [[bin]]
   name = "butterfly-{name}"
   required-features = ["cli"]
   
   [features]
   default = ["cli"]
   cli = ["clap", "tokio"]
   ```

2. **Update CI/CD:**
   - Modify workflows for workspace builds
   - Add matrix strategy for multiple tools
   - Ensure each tool can be built independently

3. **Publishing Strategy:**
   ```toml
   # Each tool maintains its own version
   [package]
   name = "butterfly-dl"
   version = "2.0.0"  # Can be different from other tools
   ```

### Phase 6: Migration Execution (Day 11-12)

**Goal:** Execute the migration with minimal disruption.

1. **Pre-Migration Checklist:**
   - [ ] All tests passing
   - [ ] Benchmarks acceptable
   - [ ] Documentation updated
   - [ ] Rollback plan ready

2. **Migration Steps:**
   - Merge workspace-migration branch
   - Tag new version: `v2.0.0`
   - Update crates.io with deprecation notice for old package structure

3. **Post-Migration:**
   - Monitor issues
   - Be ready to provide support
   - Maintain 1.x branch for critical fixes

## Rollback Strategy

If issues arise:

1. **Immediate Rollback:**
   ```bash
   git checkout main
   git reset --hard pre-workspace-v1.4.12
   ```

2. **Partial Rollback:**
   - Keep workspace structure
   - Move butterfly-dl back to root
   - Maintain as hybrid temporarily

## Testing Strategy

### Unit Tests
- Each crate runs its own tests
- Shared tests in butterfly-common
- Tool-specific tests remain with tools

### Integration Tests
```rust
// tests/workspace_integration.rs
#[test]
fn all_crates_build() {
    // Verify workspace integrity
}
```

### Publishing Tests
```bash
# Dry run publishing
cargo publish --dry-run -p butterfly-common
cargo publish --dry-run -p butterfly-dl
```

## Success Metrics

1. **No Breaking Changes:**
   - Existing code using butterfly-dl 1.x continues to work
   - FFI interface unchanged
   - CLI behavior identical

2. **Performance:**
   - Build time within 10% of original
   - Runtime performance unchanged
   - Binary size comparable

3. **Developer Experience:**
   - Clear documentation
   - Simple build commands
   - Easy to add new tools

## Long-term Considerations

1. **Version Management:**
   - Tools can version independently
   - butterfly-common versioning strategy
   - Coordination for breaking changes

2. **Maintenance:**
   - Security updates easier to apply
   - Shared dependencies updated once
   - Clear ownership model

3. **Growth:**
   - New tools follow established pattern
   - Consistent quality standards
   - Shared CI/CD infrastructure

## Conclusion

This implementation plan provides a low-risk path to migrate butterfly-dl to a workspace structure. By focusing on preserving existing functionality while preparing for future growth, we enable the development of additional tools without disrupting current users. The phased approach allows for validation at each step and provides clear rollback options if needed.