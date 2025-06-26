#!/bin/bash
set -e

# Build packages locally for testing
# Usage: ./scripts/build-packages.sh [version]

VERSION=${1:-$(cat VERSION 2>/dev/null || echo "1.4.1")}
echo "Building packages for version: $VERSION"

# Ensure cargo-deb is installed
if ! command -v cargo-deb &> /dev/null; then
    echo "Installing cargo-deb..."
    cargo install cargo-deb
fi

# Build for x86_64 Linux
echo "Building x86_64 Linux binary..."
cargo build --release --target x86_64-unknown-linux-gnu

# Create Debian package for x86_64
echo "Creating Debian package for x86_64..."
cargo deb --target x86_64-unknown-linux-gnu --no-build

# Find and rename the package
DEB_FILE=$(find target/x86_64-unknown-linux-gnu/debian -name "*.deb" -type f)
if [ -n "$DEB_FILE" ]; then
    NEW_NAME="butterfly-dl_${VERSION}_amd64.deb"
    cp "$DEB_FILE" "$NEW_NAME"
    echo "Created: $NEW_NAME"
    
    # Generate checksum
    sha256sum "$NEW_NAME" > "$NEW_NAME.sha256"
    echo "Created: $NEW_NAME.sha256"
else
    echo "Error: Debian package not found"
    exit 1
fi

# Create binary archive
echo "Creating binary archive..."
ARCHIVE_NAME="butterfly-dl-v${VERSION}-x86_64-linux"
mkdir -p "$ARCHIVE_NAME"
cp target/x86_64-unknown-linux-gnu/release/butterfly-dl "$ARCHIVE_NAME/"
cp README.md LICENSE "$ARCHIVE_NAME/"
tar czf "$ARCHIVE_NAME.tar.gz" "$ARCHIVE_NAME"
rm -rf "$ARCHIVE_NAME"
echo "Created: $ARCHIVE_NAME.tar.gz"

# Generate archive checksum
sha256sum "$ARCHIVE_NAME.tar.gz" > "$ARCHIVE_NAME.tar.gz.sha256"
echo "Created: $ARCHIVE_NAME.tar.gz.sha256"

echo ""
echo "Package build complete!"
echo "Files created:"
echo "  - butterfly-dl_${VERSION}_amd64.deb"
echo "  - butterfly-dl_${VERSION}_amd64.deb.sha256"
echo "  - $ARCHIVE_NAME.tar.gz"
echo "  - $ARCHIVE_NAME.tar.gz.sha256"
echo ""
echo "To test the Debian package:"
echo "  sudo dpkg -i butterfly-dl_${VERSION}_amd64.deb"
echo ""
echo "To test the binary archive:"
echo "  tar -xzf $ARCHIVE_NAME.tar.gz"
echo "  ./$ARCHIVE_NAME/butterfly-dl --help"