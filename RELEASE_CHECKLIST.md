# Release v2.0.0 Preparation Checklist

## Pre-Release Verification ✅

### Code Quality
- [x] All tests passing (28 library tests)
- [x] Workspace builds successfully
- [x] FFI libraries generate correctly  
- [x] Documentation updated
- [x] Version numbers consistent across workspace

### Repository State
- [x] All changes committed to workspace-migration branch
- [x] Tagged as v2.0.0
- [x] Migration documentation complete
- [x] README updated for workspace structure

## Release Assets Preparation

### 1. Build Release Binaries
```bash
# Ensure clean build
cargo clean

# Build optimized release binaries
cargo build --release --workspace

# Verify binary works
./target/release/butterfly-dl --version
# Should output: butterfly-dl 2.0.0
```

### 2. Create Release Archives
Create this script as `build-release-assets.sh`:

```bash
#!/bin/bash
set -e

VERSION="2.0.0"
PROJECT="butterfly-dl"

echo "🏗️ Building release assets for v$VERSION..."

# Clean and build
cargo clean
cargo build --release --workspace

# Create release directory
mkdir -p releases

# Function to create archive
create_archive() {
    local platform=$1
    local binary_name=$2
    local archive_type=$3
    
    echo "📦 Creating $platform archive..."
    
    local dir_name="${PROJECT}-v${VERSION}-${platform}"
    mkdir -p "releases/$dir_name"
    
    # Copy binary
    cp "target/release/$binary_name" "releases/$dir_name/"
    
    # Copy documentation
    cp README.md "releases/$dir_name/"
    cp LICENSE "releases/$dir_name/"
    cp MIGRATION_SUMMARY.md "releases/$dir_name/"
    
    # Copy FFI libraries (if they exist)
    if [ -f "target/release/libbutterfly_dl.so" ]; then
        cp target/release/libbutterfly_dl.* "releases/$dir_name/" 2>/dev/null || true
    fi
    
    # Create archive
    cd releases
    if [ "$archive_type" = "zip" ]; then
        zip -r "${dir_name}.zip" "$dir_name"
    else
        tar -czf "${dir_name}.tar.gz" "$dir_name"
    fi
    cd ..
    
    # Cleanup
    rm -rf "releases/$dir_name"
    
    echo "✅ Created releases/${dir_name}.$archive_type"
}

# Determine platform and create appropriate archive
case "$(uname -s)" in
    Linux*)
        create_archive "x86_64-linux" "butterfly-dl" "tar.gz"
        ;;
    Darwin*)
        create_archive "x86_64-macos" "butterfly-dl" "tar.gz"
        ;;
    MINGW*|CYGWIN*|MSYS*)
        create_archive "x86_64-windows" "butterfly-dl.exe" "zip"
        ;;
    *)
        echo "Unknown platform: $(uname -s)"
        exit 1
        ;;
esac

echo "🎉 Release assets ready in releases/ directory"
ls -la releases/
```

### 3. Prepare Debian Package (Linux only)
```bash
# Install cargo-deb if not available
cargo install cargo-deb

# Build debian package
cargo deb -p butterfly-dl

# Package will be in target/debian/
```

## GitHub Release Creation

### 1. Release Notes Template
```markdown
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
```

### 2. GitHub Release Commands
```bash
# Using GitHub CLI (recommended)
gh release create v2.0.0 \
  --title "🚀 butterfly-dl v2.0.0 - Workspace Architecture Migration" \
  --notes-file RELEASE_NOTES.md \
  --draft \
  releases/*.tar.gz releases/*.zip

# Or manually through GitHub web interface:
# 1. Go to https://github.com/butterfly-osm/butterfly-osm/releases/new
# 2. Tag: v2.0.0
# 3. Title: "🚀 butterfly-dl v2.0.0 - Workspace Architecture Migration"
# 4. Description: [Paste release notes]
# 5. Upload assets: All files from releases/ directory
```

## Post-Release Tasks

### 1. Verification
- [ ] Release appears on GitHub
- [ ] All assets download correctly
- [ ] Installation instructions work
- [ ] Version matches: `butterfly-dl --version`

### 2. Announcements
- [ ] Update repository description
- [ ] Create announcement issue
- [ ] Notify in relevant communities
- [ ] Update external documentation

### 3. Monitoring
- [ ] Watch for user feedback
- [ ] Monitor download metrics
- [ ] Track any reported issues
- [ ] Update documentation based on feedback

## Rollback Plan

If critical issues arise:
```bash
# Emergency rollback to v1.4.12
git checkout main
git reset --hard pre-workspace-v1.4.12
git tag v1.4.13 -m "Emergency rollback from v2.0.0"
```

However, this is highly unlikely given the comprehensive testing and backward compatibility preservation.