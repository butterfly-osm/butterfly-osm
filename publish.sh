#!/bin/bash
set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Emojis for better UX
ROCKET="🚀"
PACKAGE="📦"
CHECK="✅"
CROSS="❌"
CLOCK="⏳"
SEARCH="🔍"
CELEBRATE="🎉"

echo -e "${BLUE}${ROCKET} Publishing butterfly-osm crates to crates.io...${NC}"
echo ""

# Function to print status
print_status() {
    echo -e "${GREEN}${1}${NC}"
}

print_warning() {
    echo -e "${YELLOW}${1}${NC}"
}

print_error() {
    echo -e "${RED}${1}${NC}"
}

# Check if we're in the right directory
if [ ! -f "Cargo.toml" ] || ! grep -q "workspace" Cargo.toml; then
    print_error "${CROSS} Error: Not in workspace root directory"
    echo "Please run this script from the butterfly-osm workspace root"
    exit 1
fi

# Check if git is clean
if [ -n "$(git status --porcelain)" ]; then
    print_error "${CROSS} Error: Git working directory is not clean"
    echo "Please commit or stash your changes before publishing"
    git status --short
    exit 1
fi

# Check login status
print_status "${SEARCH} Checking crates.io login status..."
if ! cargo search --limit 1 >/dev/null 2>&1; then
    print_error "${CROSS} Error: Not logged into crates.io"
    echo "Please login first:"
    echo "  cargo login"
    echo "Then run this script again."
    exit 1
fi

print_status "${CHECK} Login verified"
echo ""

# Function to wait for crate indexing
wait_for_indexing() {
    local crate_name=$1
    local max_attempts=20
    local attempt=1
    
    print_status "${CLOCK} Waiting for $crate_name to be indexed on crates.io..."
    
    while [ $attempt -le $max_attempts ]; do
        if cargo search "$crate_name" 2>/dev/null | grep -q "^$crate_name"; then
            print_status "${CHECK} $crate_name indexed successfully"
            return 0
        fi
        
        echo "  ${CLOCK} Attempt $attempt/$max_attempts - waiting for indexing..."
        sleep 15
        attempt=$((attempt + 1))
    done
    
    print_error "${CROSS} Timeout waiting for $crate_name to be indexed"
    return 1
}

# Function to verify package before publishing
verify_package() {
    local package=$1
    print_status "${SEARCH} Verifying $package package..."
    
    if ! cargo publish --dry-run -p "$package" >/dev/null 2>&1; then
        print_error "${CROSS} Error: $package package verification failed"
        echo "Running cargo publish --dry-run -p $package for details:"
        cargo publish --dry-run -p "$package"
        return 1
    fi
    
    print_status "${CHECK} $package package verified"
    return 0
}

# Function to publish a package
publish_package() {
    local package=$1
    print_status "${PACKAGE} Publishing $package..."
    
    if cargo publish -p "$package"; then
        print_status "${CHECK} $package published successfully"
        return 0
    else
        print_error "${CROSS} Failed to publish $package"
        return 1
    fi
}

# Step 1: Verify both packages
echo "=================================="
echo "Step 1: Package Verification"
echo "=================================="

if ! verify_package "butterfly-common"; then
    exit 1
fi

if ! verify_package "butterfly-dl"; then
    exit 1
fi

echo ""

# Step 2: Publish butterfly-common
echo "=================================="
echo "Step 2: Publishing butterfly-common"
echo "=================================="

if ! publish_package "butterfly-common"; then
    exit 1
fi

echo ""

# Step 3: Wait for indexing
echo "=================================="
echo "Step 3: Waiting for Indexing"
echo "=================================="

if ! wait_for_indexing "butterfly-common"; then
    print_error "${CROSS} butterfly-common indexing failed"
    print_warning "You may need to wait longer and publish butterfly-dl manually:"
    print_warning "  cargo publish -p butterfly-dl"
    exit 1
fi

echo ""

# Step 4: Publish butterfly-dl
echo "=================================="
echo "Step 4: Publishing butterfly-dl"
echo "=================================="

if ! publish_package "butterfly-dl"; then
    exit 1
fi

echo ""

# Step 5: Final verification
echo "=================================="
echo "Step 5: Final Verification"
echo "=================================="

print_status "${SEARCH} Verifying both crates are available..."

# Wait a moment for final indexing
sleep 10

if cargo search butterfly-common 2>/dev/null | grep -q "^butterfly-common"; then
    print_status "${CHECK} butterfly-common available on crates.io"
else
    print_warning "${CLOCK} butterfly-common still indexing..."
fi

if cargo search butterfly-dl 2>/dev/null | grep -q "^butterfly-dl"; then
    print_status "${CHECK} butterfly-dl available on crates.io"
else
    print_warning "${CLOCK} butterfly-dl still indexing..."
fi

echo ""

# Success message
echo "=================================="
print_status "${CELEBRATE} Successfully published both crates!"
echo "=================================="
echo ""

print_status "📋 Verification commands:"
echo "  cargo search butterfly-common"
echo "  cargo search butterfly-dl"
echo "  cargo info butterfly-common"
echo "  cargo info butterfly-dl"
echo ""

print_status "🌐 Crate URLs:"
echo "  https://crates.io/crates/butterfly-common"
echo "  https://crates.io/crates/butterfly-dl"
echo ""

print_status "🧪 Test installation:"
echo "  cargo install butterfly-dl --version 2.0.0"
echo "  butterfly-dl --version"
echo ""

print_status "${ROCKET} Next steps:"
echo "  1. Create GitHub release v2.0.0"
echo "  2. Upload pre-built binaries"
echo "  3. Announce the migration"
echo ""

print_status "${CELEBRATE} Publishing complete! The butterfly-osm ecosystem is now live on crates.io!"