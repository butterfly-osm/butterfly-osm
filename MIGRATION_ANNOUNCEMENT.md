# 🚀 Important: butterfly-dl v2.0.0 Migration Announcement

## TL;DR for Busy Users
✅ **Your existing code and commands work unchanged**  
✅ **Performance and features remain the same**  
✅ **New features: Better error messages and ecosystem foundation**  
🔄 **Repository moves to butterfly-osm organization for multi-tool ecosystem**

---

## 🌟 What's Happening

The **butterfly-dl** project is evolving into the **butterfly-osm toolkit ecosystem**! We've successfully migrated from a single-tool repository to a comprehensive workspace that will house multiple OSM tools.

### 📍 Repository Migration
- **Old**: `github.com/butterfly-osm/butterfly-dl`
- **New**: `github.com/butterfly-osm/butterfly-osm`
- **Redirects**: GitHub automatically redirects old URLs

## 🔄 What This Means for You

### ✅ Nothing Changes for Current Users

Your existing workflows continue to work **exactly the same**:

```bash
# All these commands work identically
butterfly-dl planet
butterfly-dl europe/belgium  
butterfly-dl asia/japan - | osmium fileinfo -

# API usage unchanged
cargo install butterfly-dl
use butterfly_dl::{get, Error, Result};
```

### 🆕 What You Get

#### Better Error Messages
```bash
# Before: "Source 'austrailia' not found"
# After: "Source 'austrailia' not found. Did you mean 'australia-oceania'?"

butterfly-dl austrailia
# → Suggests: australia-oceania

butterfly-dl antartica/belgium  
# → Suggests: europe/belgium (geographic correction!)
```

#### Ecosystem Foundation
- **Future tools** coming soon: butterfly-shrink, butterfly-extract, butterfly-serve
- **Shared components** for better consistency across tools
- **Coordinated development** and releases

## 📦 Installation & Upgrade

### Existing Users
```bash
# Upgrade to v2.0.0 (recommended)
cargo install butterfly-dl --force

# Verify version
butterfly-dl --version  # Should show: butterfly-dl 2.0.0
```

### New Users
```bash
# Same installation as always
cargo install butterfly-dl

# Or download pre-built binaries
wget https://github.com/butterfly-osm/butterfly-osm/releases/latest/download/butterfly-dl-v2.0.0-x86_64-linux.tar.gz
```

## 🏗️ For Developers

### Library API (100% Compatible)
```rust
// v1.x code works unchanged in v2.0.0
use butterfly_dl::{get, get_stream, Error, Result, Downloader};

#[tokio::main]
async fn main() -> Result<()> {
    // Same API, enhanced error handling
    butterfly_dl::get("europe/belgium", None).await?;
    Ok(())
}
```

### New Workspace Structure
```bash
# Build specific tool
cargo build -p butterfly-dl

# Test specific tool  
cargo test -p butterfly-dl

# Build entire ecosystem (when more tools arrive)
cargo build --workspace
```

### FFI/C Bindings (Unchanged)
```c
// C code continues to work identically
#include "butterfly.h"
int result = butterfly_get("planet", "planet.pbf");
```

## 🚧 Migration Timeline

### ✅ Phase 1: Technical Migration (Complete)
- Workspace architecture implemented
- All tests passing, performance maintained
- Documentation updated

### 🔄 Phase 2: Repository Transition (In Progress)
- Publishing v2.0.0 to crates.io
- Creating GitHub release with binaries
- Repository rename/organization

### 🚀 Phase 3: Ecosystem Expansion (Coming Soon)
- butterfly-shrink: Polygon-based area extraction
- butterfly-extract: Advanced filtering and transformation
- butterfly-serve: HTTP tile server for OSM data

## 🤔 FAQ

### Q: Do I need to change my code?
**A: No!** All existing code, scripts, and workflows continue to work unchanged.

### Q: Will performance be affected?
**A: No!** Runtime performance is identical. Build times are actually improved.

### Q: What about the old repository?
**A: GitHub automatically redirects** old URLs. Your bookmarks and links continue to work.

### Q: Should I upgrade immediately?
**A: Recommended but not urgent.** v2.0.0 adds better error messages and prepares for ecosystem tools, but v1.x functionality is identical.

### Q: What if I find issues?
**A: Report them!** We've done extensive testing, but community feedback helps ensure quality.

### Q: Will there be breaking changes in future versions?
**A: We follow semantic versioning.** Major version changes (3.0.0) would indicate breaking changes, but our goal is long-term API stability.

## 📞 Support & Feedback

### Getting Help
- **Issues**: [GitHub Issues](https://github.com/butterfly-osm/butterfly-osm/issues)
- **Discussions**: [GitHub Discussions](https://github.com/butterfly-osm/butterfly-osm/discussions)
- **Documentation**: [README](https://github.com/butterfly-osm/butterfly-osm#readme)

### Reporting Problems
If you encounter any issues:
1. Check you're using v2.0.0: `butterfly-dl --version`
2. Try the same operation with v1.4.12 to confirm it's migration-related
3. [Create an issue](https://github.com/butterfly-osm/butterfly-osm/issues/new) with:
   - Your butterfly-dl version
   - Command that failed
   - Expected vs actual behavior
   - Platform (Linux/macOS/Windows)

## 🎉 Thank You!

This migration represents months of careful planning and implementation to ensure zero disruption while building the foundation for exciting new OSM tools. 

**Your continued usage and feedback** help make butterfly-osm the best OpenStreetMap toolkit available.

---

### Quick Links
- 📦 [Download v2.0.0](https://github.com/butterfly-osm/butterfly-osm/releases/tag/v2.0.0)
- 📚 [Documentation](https://github.com/butterfly-osm/butterfly-osm#readme)  
- 🐛 [Report Issues](https://github.com/butterfly-osm/butterfly-osm/issues)
- 💬 [Community Discussions](https://github.com/butterfly-osm/butterfly-osm/discussions)
- 📊 [Migration Details](https://github.com/butterfly-osm/butterfly-osm/blob/main/MIGRATION_SUMMARY.md)

**Happy mapping! 🗺️**

---
*This announcement covers the migration from butterfly-dl v1.x to the butterfly-osm v2.0.0 ecosystem. For technical details, see [MIGRATION_SUMMARY.md](MIGRATION_SUMMARY.md).*