#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

VERSION="2.0.0"
PROJECT="butterfly-dl"

echo -e "${BLUE}🏗️ Building release assets for v$VERSION...${NC}"
echo ""

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ] || ! grep -q "workspace" Cargo.toml; then
    echo -e "${RED}❌ Error: Not in workspace root directory${NC}"
    echo "Please run this script from the butterfly-osm workspace root"
    exit 1
fi

# Clean and build
echo -e "${GREEN}🧹 Cleaning previous builds...${NC}"
cargo clean

echo -e "${GREEN}⚙️ Building optimized release binaries...${NC}"
cargo build --release --workspace

# Verify binary works
echo -e "${GREEN}🔍 Verifying binary...${NC}"
BINARY_VERSION=$(./target/release/butterfly-dl --version 2>/dev/null | head -n1 || echo "")
if [[ "$BINARY_VERSION" == *"$VERSION"* ]]; then
    echo -e "${GREEN}✅ Binary verification successful: $BINARY_VERSION${NC}"
else
    echo -e "${RED}❌ Binary verification failed. Expected version $VERSION${NC}"
    echo "Got: $BINARY_VERSION"
    exit 1
fi

# Create release directory
mkdir -p releases
rm -rf releases/* 2>/dev/null || true

# Function to create archive
create_archive() {
    local platform=$1
    local binary_name=$2
    local archive_type=$3
    
    echo -e "${GREEN}📦 Creating $platform archive...${NC}"
    
    local dir_name="${PROJECT}-v${VERSION}-${platform}"
    mkdir -p "releases/$dir_name"
    
    # Copy binary
    if [ -f "target/release/$binary_name" ]; then
        cp "target/release/$binary_name" "releases/$dir_name/"
        echo "  ✅ Added binary: $binary_name"
    else
        echo -e "${RED}  ❌ Binary not found: target/release/$binary_name${NC}"
        return 1
    fi
    
    # Copy documentation
    cp README.md "releases/$dir_name/" && echo "  ✅ Added README.md"
    cp LICENSE "releases/$dir_name/" && echo "  ✅ Added LICENSE"
    
    # Copy migration docs
    if [ -f "MIGRATION_SUMMARY.md" ]; then
        cp MIGRATION_SUMMARY.md "releases/$dir_name/" && echo "  ✅ Added MIGRATION_SUMMARY.md"
    fi
    
    # Copy FFI libraries (if they exist)
    local ffi_copied=false
    for lib in target/release/libbutterfly_dl.*; do
        if [ -f "$lib" ]; then
            cp "$lib" "releases/$dir_name/"
            echo "  ✅ Added FFI library: $(basename "$lib")"
            ffi_copied=true
        fi
    done
    
    if [ "$ffi_copied" = false ]; then
        echo "  ℹ️ No FFI libraries found (this is normal for some platforms)"
    fi
    
    # Create archive
    cd releases
    if [ "$archive_type" = "zip" ]; then
        zip -r -q "${dir_name}.zip" "$dir_name"
        echo "  ✅ Created ZIP archive"
    else
        tar -czf "${dir_name}.tar.gz" "$dir_name"
        echo "  ✅ Created TAR.GZ archive"
    fi
    cd ..
    
    # Calculate and display size
    local archive_file="releases/${dir_name}.$archive_type"
    local size=$(du -h "$archive_file" | cut -f1)
    echo -e "${GREEN}  📏 Archive size: $size${NC}"
    
    # Cleanup temp directory
    rm -rf "releases/$dir_name"
    
    echo -e "${GREEN}✅ Created releases/${dir_name}.$archive_type${NC}"
    echo ""
}

# Determine platform and create appropriate archive
echo -e "${BLUE}🔍 Detecting platform...${NC}"

case "$(uname -s)" in
    Linux*)
        echo "Platform: Linux"
        create_archive "x86_64-linux" "butterfly-dl" "tar.gz"
        ;;
    Darwin*)
        echo "Platform: macOS"
        create_archive "x86_64-macos" "butterfly-dl" "tar.gz"
        ;;
    MINGW*|CYGWIN*|MSYS*)
        echo "Platform: Windows"
        create_archive "x86_64-windows" "butterfly-dl.exe" "zip"
        ;;
    *)
        echo -e "${RED}❌ Unknown platform: $(uname -s)${NC}"
        echo "Supported platforms: Linux, macOS, Windows"
        exit 1
        ;;
esac

# Try to build Debian package (Linux only)
if [[ "$(uname -s)" == "Linux" ]]; then
    echo -e "${BLUE}📦 Attempting to build Debian package...${NC}"
    
    if command -v cargo-deb >/dev/null 2>&1; then
        echo "Building debian package..."
        if cargo deb -p butterfly-dl --no-build; then
            # Copy the .deb file to releases directory
            DEB_FILE=$(find target/debian -name "*.deb" | head -n1)
            if [ -f "$DEB_FILE" ]; then
                cp "$DEB_FILE" releases/
                echo -e "${GREEN}✅ Created Debian package: $(basename "$DEB_FILE")${NC}"
            fi
        else
            echo -e "${YELLOW}⚠️ Debian package build failed (non-critical)${NC}"
        fi
    else
        echo -e "${YELLOW}ℹ️ cargo-deb not installed. To build .deb packages:${NC}"
        echo "  cargo install cargo-deb"
        echo "  Then run this script again"
    fi
    echo ""
fi

# Show results
echo "=================================="
echo -e "${GREEN}🎉 Release assets ready!${NC}"
echo "=================================="
echo ""

echo -e "${BLUE}📁 Created files:${NC}"
ls -la releases/
echo ""

echo -e "${BLUE}📊 Archive details:${NC}"
for file in releases/*; do
    if [ -f "$file" ]; then
        size=$(du -h "$file" | cut -f1)
        echo "  $(basename "$file"): $size"
    fi
done
echo ""

echo -e "${BLUE}🚀 Next steps:${NC}"
echo "1. Create GitHub release v$VERSION"
echo "2. Upload all files from releases/ directory"
echo "3. Use the release notes from RELEASE_CHECKLIST.md"
echo ""

echo -e "${BLUE}📋 Quick GitHub release command:${NC}"
echo "gh release create v$VERSION \\"
echo "  --title \"🚀 butterfly-dl v$VERSION - Workspace Architecture Migration\" \\"
echo "  --notes-file RELEASE_NOTES.md \\"
echo "  --draft \\"
echo "  releases/*"
echo ""

echo -e "${GREEN}✅ Build complete! All release assets are ready for upload.${NC}"