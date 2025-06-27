# 🤖 Automated Release Checklist

## ✅ Pre-Release Verification

### Code Quality
- [ ] All tests passing: `cargo test --workspace`
- [ ] Workspace builds successfully: `cargo build --release --workspace`
- [ ] Documentation updated
- [ ] Version numbers consistent across workspace

### Release Preparation
- [ ] `RELEASE_NOTES.md` created with detailed notes
- [ ] All changes committed to release branch
- [ ] Crates.io publishing complete: `./publish.sh`

## 🚀 Automated Release Process

### Step 1: Publish to Crates.io
```bash
# Run automated publishing script
./publish.sh

# Verify both crates are published
cargo search butterfly-common
cargo search butterfly-dl
```

### Step 2: Prepare Release Notes
```bash
# Create detailed release notes (example)
cat > RELEASE_NOTES.md << 'EOF'
# 🚀 butterfly-dl v2.0.1 - Release Title

## 🌟 What's New
- Feature descriptions
- Bug fixes
- Performance improvements

## 📦 Installation
[Standard installation instructions]

## 🔄 Migration Guide
[Any breaking changes or migration steps]

---
**Full Changelog**: [v2.0.0...v2.0.1](https://github.com/butterfly-osm/butterfly-osm/compare/v2.0.0...v2.0.1)
EOF
```

### Step 3: Trigger Automated Release
```bash
# Commit release notes
git add RELEASE_NOTES.md
git commit -m "release: prepare v2.0.1 with detailed notes"

# Create and push tag (triggers automation)
git tag v2.0.1
git push origin v2.0.1

# GitHub Actions automatically:
# ✅ Creates release with RELEASE_NOTES.md content  
# ✅ Builds binaries for 5 platforms
# ✅ Generates checksums for all assets
# ✅ Uploads everything to GitHub release
# ✅ Publishes release immediately
```

### Step 4: Verification
```bash
# Check release was created
gh release view v2.0.1

# Verify downloads work
wget https://github.com/butterfly-osm/butterfly-osm/releases/download/v2.0.1/butterfly-dl-v2.0.1-x86_64-unknown-linux-gnu.tar.gz

# Test installation
cargo install butterfly-dl --version 2.0.1
butterfly-dl --version
```

## 📊 What Gets Built Automatically

### Platforms
- ✅ **Linux x86_64**: `butterfly-dl-vX.X.X-x86_64-unknown-linux-gnu.tar.gz`
- ✅ **Linux ARM64**: `butterfly-dl-vX.X.X-aarch64-unknown-linux-gnu.tar.gz`
- ✅ **macOS Intel**: `butterfly-dl-vX.X.X-x86_64-apple-darwin.tar.gz`
- ✅ **macOS Apple Silicon**: `butterfly-dl-vX.X.X-aarch64-apple-darwin.tar.gz`
- ✅ **Windows x86_64**: `butterfly-dl-vX.X.X-x86_64-pc-windows-msvc.zip`

### Assets Per Platform
- ✅ **Binary**: `butterfly-dl` (or `.exe` for Windows)
- ✅ **Documentation**: `README.md`, `LICENSE`, `MIGRATION_SUMMARY.md`
- ✅ **FFI Libraries**: `libbutterfly_dl.*` (native builds only)
- ✅ **Checksums**: Individual `.sha256` files + combined `checksums.txt`

## 🚨 Troubleshooting

### If Automation Fails
```bash
# Check GitHub Actions logs
gh run list --repo butterfly-osm/butterfly-osm

# View specific failed run
gh run view <run-id>

# Re-trigger by deleting and re-creating tag
git tag -d v2.0.1
git push origin :refs/tags/v2.0.1
git tag v2.0.1
git push origin v2.0.1
```

### Manual Override (Emergency)
```bash
# If automation completely fails, manual release:
gh release create v2.0.1 \
  --title "🚀 butterfly-dl v2.0.1" \
  --notes-file RELEASE_NOTES.md \
  manually-built-assets/*
```

## 🎯 Benefits of Automated Process

- **⚡ Speed**: Complete release in ~5 minutes (vs 30+ manual)
- **🌍 Coverage**: 5 platforms instead of 1  
- **🔒 Security**: Automatic checksums and verification
- **📝 Consistency**: Standardized release notes and assets
- **🛡️ Reliability**: No manual steps to forget or mess up

---

**See `AUTOMATED_RELEASE_PROCESS.md` for detailed workflow documentation.**