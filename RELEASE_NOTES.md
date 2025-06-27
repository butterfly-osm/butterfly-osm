# 🚀 butterfly-dl v2.0.0 - Workspace Architecture Migration

## 🌟 Major Milestone: Ecosystem Foundation

This release represents a fundamental architectural evolution, transforming butterfly-dl from a single-tool repository into the foundation of the **butterfly-osm toolkit ecosystem**.

## 🏗️ What's New

### Workspace Architecture
- **🔧 Rust Workspace**: Organized as multi-tool workspace with shared components
- **📚 butterfly-common**: New shared library for error handling and utilities  
- **🎯 Tool Foundation**: Ready for butterfly-shrink, butterfly-extract, butterfly-serve
- **📦 Independent Publishing**: Each tool can be published separately to crates.io

### Enhanced Error Handling
- **🧠 Advanced Fuzzy Matching**: Improved geographic source suggestions
- **🌍 Semantic Understanding**: Better context-aware error correction
- **🔄 Shared Components**: Common error types across all future tools

### Developer Experience
- **📋 Tool Template**: Standardized structure for new tools
- **🤝 API Compatibility**: 100% backward compatible with v1.x
- **⚡ Performance**: Improved build times and maintained runtime performance
- **🧪 Better Testing**: Isolated and shared test suites

## 📦 Installation

### Pre-built Binaries
- [Linux x86_64](https://github.com/butterfly-osm/butterfly-osm/releases/download/v2.0.0/butterfly-dl-v2.0.0-x86_64-linux.tar.gz)
- [macOS x86_64](https://github.com/butterfly-osm/butterfly-osm/releases/download/v2.0.0/butterfly-dl-v2.0.0-x86_64-macos.tar.gz)
- [Windows x86_64](https://github.com/butterfly-osm/butterfly-osm/releases/download/v2.0.0/butterfly-dl-v2.0.0-x86_64-windows.zip)

### Package Managers
```bash
# Cargo (Rust)
cargo install butterfly-dl

# From source
git clone https://github.com/butterfly-osm/butterfly-osm
cd butterfly-osm
cargo build --release -p butterfly-dl
```

## 🔄 Migration Guide

### For End Users
**No action required!** All existing workflows continue to work:
```bash
butterfly-dl planet                 # Same as v1.x
butterfly-dl europe/belgium         # Same as v1.x
```

### For Developers
**API remains 100% compatible:**
```rust
// v1.x code continues to work unchanged
use butterfly_dl::{get, Error, Result};

#[tokio::main]
async fn main() -> Result<()> {
    butterfly_dl::get("europe/belgium", None).await
}
```

### For Library Users
**FFI interface unchanged:**
```c
// C bindings work identically
#include "butterfly.h"
int result = butterfly_get("planet", "planet.pbf");
```

## 📊 Technical Details

### Architecture Changes
- **From**: Single tool repository
- **To**: Multi-tool workspace with shared components
- **Preserved**: Git history, API compatibility, performance
- **Added**: Shared utilities, tool templates, ecosystem foundation

### Verification
- ✅ **28 tests passing** (14 butterfly-dl + 9 butterfly-common + 5 CLI)
- ✅ **Performance maintained**: Same runtime performance, improved build times
- ✅ **Memory efficiency**: <1GB RAM for any file size (including 81GB planet)
- ✅ **FFI libraries**: libbutterfly_dl.so/.a/.dylib generated correctly

## 🚀 What's Next

### Upcoming Tools
- **butterfly-shrink**: Polygon-based area extraction
- **butterfly-extract**: Advanced filtering and transformation  
- **butterfly-serve**: HTTP tile server

### Ecosystem Benefits
- **Shared Components**: Common error handling, utilities, and patterns
- **Coordinated Development**: Unified testing, documentation, and releases
- **Independent Evolution**: Each tool can evolve at its own pace

## 🤝 Backward Compatibility

**Zero breaking changes for users:**
- All v1.x command-line usage works identically
- All v1.x library APIs preserved
- All v1.x C FFI functions unchanged
- Migration is seamless and automatic

## 📋 Changelog

### Added
- butterfly-common shared library with error handling
- Workspace architecture supporting multiple tools
- Advanced fuzzy matching with semantic understanding
- Tool template for future development
- Comprehensive migration documentation

### Changed  
- Repository structure organized as Rust workspace
- Version bumped to 2.0.0 reflecting architectural change
- Build system optimized for multi-tool development

### Maintained
- 100% API compatibility with v1.x
- All existing functionality and performance
- FFI library generation and C bindings
- Command-line interface and behavior

---

**Full Changelog**: [v1.4.12...v2.0.0](https://github.com/butterfly-osm/butterfly-osm/compare/v1.4.12...v2.0.0)