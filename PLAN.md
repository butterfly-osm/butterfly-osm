# Butterfly-OSM Architecture Reorganization Plan

## Executive Summary

This document explores architectural approaches for expanding the butterfly-osm project from a single tool (butterfly-dl) to multiple related tools. We compare two primary approaches: a Rust workspace (monorepo) versus separate crates (multi-repo), analyzing trade-offs to inform the architectural decision.

## Current State

- **Single tool**: butterfly-dl
- **Single repository**: butterfly-osm/butterfly-dl
- **Multiple artifacts**: CLI binary, Rust library (rlib), C libraries (cdylib, staticlib)
- **Established user base**: Published on crates.io

## Architectural Approaches

### Approach 1: Rust Workspace (Monorepo)

#### Structure
```
butterfly-osm/                    # Single repository
├── Cargo.toml                   # Workspace root
├── butterfly-common/            # Shared code
│   ├── Cargo.toml
│   └── src/
├── butterfly-dl/                # Existing tool
│   ├── Cargo.toml
│   └── src/
├── butterfly-{tool2}/           # New tool
│   ├── Cargo.toml
│   └── src/
└── butterfly-{tool3}/           # Another tool
    ├── Cargo.toml
    └── src/
```

#### Characteristics
- **Single repository** containing all tools
- **Shared version management** through workspace
- **Internal dependencies** via path references
- **Unified CI/CD** pipeline
- **Atomic commits** across tools

#### Benefits
1. **Code Sharing**
   - Easy refactoring across tools
   - Shared utilities without versioning overhead
   - Immediate visibility of breaking changes

2. **Development Efficiency**
   - Single clone for all development
   - Unified tooling and configuration
   - Simplified cross-tool testing

3. **Maintenance**
   - Coordinated releases possible
   - Single issue tracker
   - Consolidated documentation

4. **Quality Control**
   - Uniform code standards
   - Shared CI/CD pipeline
   - Consistent testing approach

#### Drawbacks
1. **Coupling Risk**
   - Tools may become inadvertently coupled
   - Shared dependencies affect all tools
   - Large repository size

2. **Release Complexity**
   - Independent versioning more difficult
   - Must coordinate breaking changes
   - All tools rebuilt on CI

3. **Contributor Friction**
   - Larger codebase to understand
   - More complex build process
   - Longer CI times

### Approach 2: Separate Crates (Multi-repo)

#### Structure
```
butterfly-osm/                    # GitHub organization
├── butterfly-dl/                # Repository 1
│   ├── Cargo.toml
│   └── src/
├── butterfly-{tool2}/           # Repository 2
│   ├── Cargo.toml
│   └── src/
├── butterfly-{tool3}/           # Repository 3
│   ├── Cargo.toml
│   └── src/
└── butterfly-common/            # Shared library repo
    ├── Cargo.toml
    └── src/
```

#### Characteristics
- **Multiple repositories** under organization
- **Independent versioning** per tool
- **External dependencies** via crates.io
- **Separate CI/CD** per repository
- **Independent commit history**

#### Benefits
1. **True Independence**
   - Tools evolve separately
   - Independent release cycles
   - Clear boundaries

2. **Focused Development**
   - Smaller, focused repositories
   - Faster CI/CD per tool
   - Easier for new contributors

3. **Flexible Deployment**
   - Users clone only what they need
   - Independent versioning
   - Separate issue tracking

4. **Clear Ownership**
   - Per-tool maintainers
   - Focused documentation
   - Independent decision-making

#### Drawbacks
1. **Code Sharing Complexity**
   - Must publish shared code to crates.io
   - Version management overhead
   - Delayed propagation of fixes

2. **Development Overhead**
   - Multiple repositories to manage
   - Cross-tool changes require coordination
   - More complex local development

3. **Discovery Challenge**
   - Users may not find all tools
   - Fragmented documentation
   - Multiple places to watch

### Approach 3: Hybrid Solutions

#### 3.1 Workspace with Published Crates
- Keep workspace structure
- Publish each tool independently to crates.io
- Best of both worlds for development and deployment

#### 3.2 Meta Repository
- Separate repositories for each tool
- Meta repository with git submodules
- Provides unified view when needed

#### 3.3 Core + Plugins
- Core butterfly-osm crate
- Tools as optional features or plugins
- Single binary with subcommands

## Code Sharing Strategies

### For Workspace Approach
```toml
# butterfly-dl/Cargo.toml
[dependencies]
butterfly-common = { path = "../butterfly-common", version = "1.0" }
```

- Direct path dependencies
- Immediate access to changes
- Version field for crates.io compatibility

### For Separate Crates Approach
```toml
# butterfly-dl/Cargo.toml
[dependencies]
butterfly-common = "1.0"

# For development
[patch.crates-io]
butterfly-common = { path = "../butterfly-common" }
```

- Published version dependencies
- Local development via patch
- Clear version boundaries

## Comparison Matrix

| Aspect | Workspace | Separate Crates | Hybrid |
|--------|-----------|-----------------|--------|
| Code Sharing | ⭐⭐⭐ Easy | ⭐ Complex | ⭐⭐ Moderate |
| Independence | ⭐ Coupled | ⭐⭐⭐ Independent | ⭐⭐ Balanced |
| Development | ⭐⭐⭐ Simple | ⭐ Complex | ⭐⭐ Moderate |
| CI/CD Speed | ⭐ Slow | ⭐⭐⭐ Fast | ⭐⭐ Varies |
| Maintenance | ⭐⭐⭐ Unified | ⭐ Distributed | ⭐⭐ Mixed |
| Versioning | ⭐ Complex | ⭐⭐⭐ Simple | ⭐⭐ Flexible |
| Discovery | ⭐⭐⭐ Easy | ⭐ Hard | ⭐⭐ Moderate |
| Contributions | ⭐ Complex | ⭐⭐⭐ Focused | ⭐⭐ Varies |

## Decision Framework

### Choose Workspace If:
1. **Tight Integration** - Tools share significant code
2. **Coordinated Development** - Tools evolve together
3. **Small Team** - Limited maintenance resources
4. **Unified Experience** - Consistent UX across tools
5. **Rapid Iteration** - Frequent cross-tool changes

### Choose Separate Crates If:
1. **Independent Evolution** - Tools have different lifecycles
2. **Large Community** - Many contributors
3. **Clear Boundaries** - Little shared code
4. **Different Maintainers** - Per-tool ownership
5. **Diverse User Base** - Users need specific tools

### Choose Hybrid If:
1. **Mixed Requirements** - Some tools coupled, others independent
2. **Migration Path** - Starting unified, planning to split
3. **Flexible Deployment** - Development vs production differs
4. **Gradual Growth** - Uncertain about future structure

## Migration Considerations

### From Single to Workspace
1. Create workspace Cargo.toml
2. Move existing code to subdirectory
3. Update import paths
4. Extract common code iteratively
5. Maintain backward compatibility

### From Single to Separate Crates
1. Extract common code first
2. Publish common crate
3. Create new repositories
4. Update dependencies
5. Set up redirects/links

### Between Approaches
- Workspace to Separate: Extract and publish
- Separate to Workspace: Consolidate repositories
- Both directions possible but require planning

## Build and Distribution

### Workspace Approach
```bash
# Build everything
cargo build --workspace

# Build specific tool
cargo build -p butterfly-dl

# Test everything
cargo test --workspace

# Publish (requires version management)
cargo publish -p butterfly-common
cargo publish -p butterfly-dl
```

### Separate Crates Approach
```bash
# Each repository
cargo build
cargo test
cargo publish

# Local development setup
mkdir butterfly-dev
cd butterfly-dev
git clone all repositories
# Use cargo patches for local development
```

## Maintenance Implications

### Workspace
- **Updates**: Single PR can update all tools
- **Security**: One place to audit
- **Dependencies**: Shared Cargo.lock
- **Breaking Changes**: Immediately visible

### Separate Crates
- **Updates**: Multiple PRs required
- **Security**: Multiple audits needed
- **Dependencies**: Independent resolution
- **Breaking Changes**: Version boundaries

## Recommendation Process

1. **Analyze Relationships**
   - How much code will be shared?
   - How coupled are the tools?
   - Will tools have different release cycles?

2. **Consider Team Structure**
   - Single maintainer or multiple?
   - Centralized or distributed development?
   - Community contribution model?

3. **Evaluate User Needs**
   - Install all tools or selective?
   - Unified interface important?
   - Performance considerations?

4. **Plan for Growth**
   - How many tools eventually?
   - Possibility of external tools?
   - Long-term maintenance capacity?

## Conclusion

The choice between workspace and separate crates is not purely technical—it reflects the project's philosophy, team structure, and user needs. Workspace favors integration and simplicity, while separate crates favor independence and modularity. Hybrid approaches offer flexibility at the cost of complexity.

Consider starting with the approach that best matches current needs while keeping migration paths open for future evolution.