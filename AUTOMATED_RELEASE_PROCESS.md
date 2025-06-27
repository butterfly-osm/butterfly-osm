# 🤖 Automated Release Process

## 🎯 Overview

The butterfly-osm ecosystem uses **fully automated releases** via GitHub Actions. Manual release creation has been replaced with a streamlined automated workflow.

## 🚀 How It Works

### 1. Automated Triggers
```bash
# Push a tag → Automatic release
git tag v2.0.1
git push origin v2.0.1
# → GitHub Actions automatically:
#   ✅ Creates release with proper notes
#   ✅ Builds binaries for all platforms 
#   ✅ Uploads assets with checksums
#   ✅ Publishes immediately
```

### 2. Supported Platforms (Automatic)
- **Linux x86_64**: `butterfly-dl-v2.0.0-x86_64-unknown-linux-gnu.tar.gz`
- **Linux ARM64**: `butterfly-dl-v2.0.0-aarch64-unknown-linux-gnu.tar.gz`
- **macOS Intel**: `butterfly-dl-v2.0.0-x86_64-apple-darwin.tar.gz`
- **macOS Apple Silicon**: `butterfly-dl-v2.0.0-aarch64-apple-darwin.tar.gz`
- **Windows x86_64**: `butterfly-dl-v2.0.0-x86_64-pc-windows-msvc.zip`

### 3. Release Notes System
- **Custom notes**: Place detailed notes in `RELEASE_NOTES.md` → Used automatically
- **Auto-generated**: No `RELEASE_NOTES.md` → Git changelog used as fallback
- **Rich formatting**: Full Markdown support in release notes

## 📋 Release Workflow

### Step 1: Prepare Release
```bash
# 1. Update versions
# Already done: Cargo.toml versions managed by workspace

# 2. Create detailed release notes (optional but recommended)
cat > RELEASE_NOTES.md << 'EOF'
# 🚀 butterfly-dl v2.0.1 - Bug Fixes

## What's Fixed
- Fixed workspace build issue in CI/CD
- Updated documentation for automated releases

## Installation
[Installation instructions...]
EOF

# 3. Commit everything
git add .
git commit -m "feat: prepare v2.0.1 release"
```

### Step 2: Publish to Crates.io (Manual)
```bash
# Still manual (dependency order matters)
./publish.sh
```

### Step 3: Create Release (Automatic)
```bash
# Tag and push → Everything else is automatic
git tag v2.0.1
git push origin v2.0.1

# GitHub Actions automatically:
# ✅ Creates release with RELEASE_NOTES.md content
# ✅ Builds 5 platform binaries in parallel
# ✅ Generates checksums for all assets  
# ✅ Uploads everything to GitHub release
# ✅ Publishes release immediately
```

### Step 4: Verify (Manual)
```bash
# Check release page
open https://github.com/butterfly-osm/butterfly-osm/releases/tag/v2.0.1

# Test installation
cargo install butterfly-dl --version 2.0.1
butterfly-dl --version
```

## 🔧 Advanced Configuration

### Workspace-Aware Building
The CI automatically detects workspace structure:
```yaml
# Builds specific package from workspace
cargo build --release -p butterfly-dl --target ${{ matrix.target }}
```

### Asset Bundling
Each platform archive includes:
- ✅ **Binary**: `butterfly-dl` or `butterfly-dl.exe`
- ✅ **Documentation**: `README.md`, `LICENSE`
- ✅ **Migration docs**: `MIGRATION_SUMMARY.md` (if exists)
- ✅ **FFI libraries**: `libbutterfly_dl.*` (native builds only)

### Checksums & Security
- ✅ **Individual checksums**: Each archive gets `.sha256` file
- ✅ **Combined checksums**: `checksums.txt` with verification instructions
- ✅ **Integrity verification**: `sha256sum -c checksums.txt`

## 🚨 Emergency Procedures

### Rollback Release
```bash
# Delete problematic release
gh release delete v2.0.1 --yes

# Reset tag
git tag -d v2.0.1
git push origin :refs/tags/v2.0.1

# Fix issues and re-release
git tag v2.0.1
git push origin v2.0.1
```

### Manual Override
```bash
# If automation fails, manual release with same naming:
gh release create v2.0.1 \
  --title "🚀 butterfly-dl v2.0.1" \
  --notes-file RELEASE_NOTES.md \
  manually-built-assets/*
```

## 📊 Comparison: Before vs After

| Aspect | Manual Process | Automated Process |
|--------|---------------|-------------------|
| **Platforms** | 1 (local only) | 5 (all major platforms) |
| **Time** | 30+ minutes | 5 minutes hands-off |
| **Consistency** | Manual errors possible | Reproducible builds |
| **Documentation** | Easy to forget | Automatic inclusion |
| **Checksums** | Manual generation | Automatic + verification guide |
| **Release Notes** | Manual formatting | Markdown with fallback |

## 🎉 Benefits

### For Maintainers
- **⚡ Fast**: Tag push → Complete release in ~5 minutes
- **🛡️ Reliable**: No manual steps, consistent process
- **🌍 Cross-platform**: Automatic multi-platform builds
- **📝 Documented**: All steps are in Git history

### For Users  
- **📦 More platforms**: ARM64 support on Linux and macOS
- **🔒 Secure**: Checksums for integrity verification
- **📚 Better docs**: Consistent documentation in all packages
- **⬇️ Faster downloads**: Optimized binaries for each platform

---

**The new automated process eliminates manual errors while providing better coverage and faster releases for the butterfly-osm ecosystem.**