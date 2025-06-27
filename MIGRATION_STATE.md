# Migration State Documentation

## Current State Before Workspace Migration

### Version Information
- Current version: 1.4.12
- Tag created: pre-workspace-v1.4.12
- Maintenance branch: maintenance/1.x

### Public API Exports
Current public API from src/lib.rs:
- `get(source, output_file)` - Main download function
- `get_stream(source)` - Stream download function  
- `get_with_progress(source, output_file, progress_callback)` - Download with progress
- `get_with_options(source, output_file, options)` - Download with custom options
- `Downloader` struct - Core downloader
- `SourceConfig` struct - Configuration for sources
- `Error` enum - Error types
- `Result<T>` type alias

### FFI Interface
- C library exports: libbutterfly_dl.so/.dylib/.dll
- Header file: include/butterfly.h
- Functions exposed to C:
  - butterfly_get()
  - butterfly_get_stream()
  - butterfly_has_s3_support() (removed in recent versions)

### Current Dependencies (from Cargo.toml)
Production dependencies:
- reqwest = "0.12.20"
- tokio = { version = "1.45", features = ["full"] }
- clap = { version = "4.7", features = ["derive"] }
- thiserror = "2.0"
- env_logger = "0.11.8"
- indicatif = "0.17"
- strsim = "0.11"

Dev dependencies:
- wiremock = "0.6.4"
- ctor = "0.4.2"
- tempfile = "3.3"

### Build Configuration
- Library types: ["cdylib", "staticlib", "rlib"]
- Binary: butterfly-dl
- Build script: build.rs (for C header generation)

### Test Suite Baseline
✅ All tests passing:
- Unit tests: 22 passed
- Bin tests: 5 passed  
- Integration tests: 7 passed
- Doc tests: 9 passed
- Total: 43 tests passed, 0 failed

✅ Release build successful
- Build time: ~11.5 seconds
- No compilation errors or warnings