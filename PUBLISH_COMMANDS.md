# Crates.io Publishing Commands

## Prerequisites
Make sure you're logged into crates.io:
```bash
cargo login
# Enter your API token when prompted
```

## Publishing Order
**IMPORTANT**: Must publish in this exact order due to dependencies.

### Step 1: Publish butterfly-common
```bash
# Verify package is ready
cargo publish --dry-run -p butterfly-common

# Publish to crates.io
cargo publish -p butterfly-common

# Verify publication (wait ~1 minute for indexing)
cargo search butterfly-common
```

### Step 2: Publish butterfly-dl
```bash
# Wait for butterfly-common to be indexed (~1-2 minutes)
# Verify butterfly-common is available
cargo search butterfly-common

# Test packaging
cargo publish --dry-run -p butterfly-dl

# Publish to crates.io
cargo publish -p butterfly-dl

# Verify publication
cargo search butterfly-dl
```

## Alternative: Automated Publishing Script
Save this as `publish.sh`:

```bash
#!/bin/bash
set -e

echo "🚀 Publishing butterfly-osm crates to crates.io..."

# Check login status
if ! cargo search butterfly-common >/dev/null 2>&1; then
    echo "❌ Please login to crates.io first: cargo login"
    exit 1
fi

echo "📦 Publishing butterfly-common v2.0.0..."
cargo publish -p butterfly-common

echo "⏳ Waiting for butterfly-common to be indexed..."
sleep 60

echo "🔍 Verifying butterfly-common is available..."
MAX_ATTEMPTS=10
ATTEMPT=1
while [ $ATTEMPT -le $MAX_ATTEMPTS ]; do
    if cargo search butterfly-common | grep -q "butterfly-common"; then
        echo "✅ butterfly-common indexed successfully"
        break
    fi
    echo "⏳ Attempt $ATTEMPT/$MAX_ATTEMPTS - waiting for indexing..."
    sleep 30
    ATTEMPT=$((ATTEMPT + 1))
done

if [ $ATTEMPT -gt $MAX_ATTEMPTS ]; then
    echo "❌ butterfly-common indexing timeout. Please try publishing butterfly-dl manually."
    exit 1
fi

echo "📦 Publishing butterfly-dl v2.0.0..."
cargo publish -p butterfly-dl

echo "🎉 Successfully published both crates!"
echo ""
echo "📋 Verification commands:"
echo "  cargo search butterfly-common"
echo "  cargo search butterfly-dl"
echo "  cargo info butterfly-common"
echo "  cargo info butterfly-dl"
```

Make executable: `chmod +x publish.sh`

## Post-Publishing Verification

### Test Installation
```bash
# Create test directory
mkdir -p /tmp/butterfly-test
cd /tmp/butterfly-test

# Test installing from crates.io
cargo init --name test-install
echo 'butterfly-dl = "2.0"' >> Cargo.toml
echo 'butterfly-common = "2.0"' >> Cargo.toml

# Test compilation
cargo check

# Test CLI installation
cargo install butterfly-dl --version 2.0.0
butterfly-dl --version
```

### Update Package Information
```bash
# Update crate metadata if needed
cargo update
```

## Expected Output
- **butterfly-common v2.0.0**: Base library with error handling and fuzzy matching
- **butterfly-dl v2.0.0**: OpenStreetMap downloader using butterfly-common

## Troubleshooting

### Common Issues:
1. **Login required**: Run `cargo login` with your crates.io API token
2. **Indexing delay**: Wait 1-2 minutes between publishing dependencies
3. **Version conflicts**: Ensure no existing v2.0.0 versions exist
4. **Network issues**: Retry after a few minutes

### Verification URLs:
- https://crates.io/crates/butterfly-common
- https://crates.io/crates/butterfly-dl

## Timeline:
- **butterfly-common**: ~2-3 minutes to publish and index
- **butterfly-dl**: ~2-3 minutes to publish after butterfly-common is indexed
- **Total time**: ~5-10 minutes for complete publishing process