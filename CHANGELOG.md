# Butterfly-OSM Ecosystem Changelog

All notable changes to the butterfly-osm ecosystem will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

For detailed tool-specific changes, see individual tool changelogs:
- [butterfly-dl](./tools/butterfly-dl/CHANGELOG.md) - OSM data downloader

## [Unreleased] — 2026-05-23

### Added

- **butterfly-route**: Incremental `avoid_polygons` customization
  ([#240](https://github.com/butterfly-osm/butterfly-osm/issues/240),
  [#249](https://github.com/butterfly-osm/butterfly-osm/pull/249)). The
  recustomization pass now walks an explicit BFS frontier seeded from
  the edges that intersect the avoid polygons, instead of re-running a
  whole-graph triangle relaxation. A 1 km rural polygon on Belgium went
  from 37 s to ~780 ms end-to-end (47× speedup); the larger E19
  motorway-corridor polygon settles at 1.16 s. Cold `/route` requests
  that previously dominated the response now spend the bulk of their
  time in I/O and snap, not in customization.
- **butterfly-route**: LRU avoid-polygon cache with operational
  visibility ([#242](https://github.com/butterfly-osm/butterfly-osm/issues/242),
  [#243](https://github.com/butterfly-osm/butterfly-osm/issues/243),
  [#246](https://github.com/butterfly-osm/butterfly-osm/pull/246),
  [#247](https://github.com/butterfly-osm/butterfly-osm/pull/247)).
  Cache hit rate, entry count, and eviction counters are now surfaced
  on `GET /health` and exported as four Prometheus gauges on
  `GET /metrics`. Polygon inputs are canonicalized before hashing so
  semantically equivalent JSON inputs (rotation, whitespace, ring
  closure) collide on the same cache entry. Booth's algorithm
  ([#250](https://github.com/butterfly-osm/butterfly-osm/pull/250))
  replaces the quadratic rotation search used in the first cut of
  canonicalization.
- **belgium-latest container** ([#236](https://github.com/butterfly-osm/butterfly-osm/issues/236)):
  refreshed Belgium build deployed with 5.13M EBG nodes, 14.98M edges,
  769K named roads, and 4 modes (bike, car, foot, truck). Used as the
  reference dataset for every benchmark in this release.

### Changed

- **butterfly-route**: Avoid cache now returns `Arc<AvoidEntry>` rather
  than cloning the customized weight set per request
  ([#241](https://github.com/butterfly-osm/butterfly-osm/issues/241),
  [#245](https://github.com/butterfly-osm/butterfly-osm/pull/245)).
  `/table` warm-hit latency dropped from 366 ms to 22 ms, matching the
  baseline `/table` cost on un-avoided queries.
- **butterfly-route**: `POST /table/stream` now borrows the flat
  adjacency arrays from the cached `AvoidEntry` instead of cloning
  them ([#248](https://github.com/butterfly-osm/butterfly-osm/pull/248)).
  Eliminates a 100–200 MB per-request clone on Belgium-sized inputs;
  visible as a flat memory profile under sustained streaming load.

### Fixed

- **butterfly-route**: Matrix gap closed
  ([#197](https://github.com/butterfly-osm/butterfly-osm/issues/197),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  K-best snap and SCC-aware role masks are now applied at every snap
  site — `/route`, `/nearest`, `/table`, `/matrix`, `/isochrone`,
  `/trip`, and the Flight gRPC equivalents — instead of only `/route`.
  A 200-pair Belgium `/route` ↔ `/table` correlation sweep now reports
  100% agreement, up from a ~9% gap where `/table` would return
  unreachable for pairs `/route` resolved successfully.
- **butterfly-route**: Small-N matrix dispatch fast-path
  ([#191](https://github.com/butterfly-osm/butterfly-osm/issues/191),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  10×10 and 25×25 matrices no longer fall through to the bulk
  scheduler — rayon thread-dispatch overhead at those sizes outweighed
  the parallelism win.
- **butterfly-route**: Sparse triangle correctness for avoid polygons
  ([#235](https://github.com/butterfly-osm/butterfly-osm/issues/235),
  [#232](https://github.com/butterfly-osm/butterfly-osm/pull/232)).
  `/route` and `/table` durations now match exactly on avoided
  queries; the previous implementation had an 8% disagreement caused
  by the sparse pass touching a different node set than the dense
  baseline.
- **butterfly-route**: Stale unpacked geometry in serve-time triangle
  relaxation ([#239](https://github.com/butterfly-osm/butterfly-osm/issues/239),
  [#244](https://github.com/butterfly-osm/butterfly-osm/pull/244)).
  When the relax loop replaced a shortcut's middle node, the unpacking
  arrays still pointed at the original topology middle, producing
  polylines that crossed the avoid polygon even though the duration
  number was correct. `up_middle` and `down_middle` are now updated in
  lockstep with the weight.
- **butterfly-route**: Additional correctness and review fixes for
  the incremental avoid path
  ([#233](https://github.com/butterfly-osm/butterfly-osm/issues/233),
  [#234](https://github.com/butterfly-osm/butterfly-osm/issues/234),
  [#248](https://github.com/butterfly-osm/butterfly-osm/pull/248),
  [#251](https://github.com/butterfly-osm/butterfly-osm/pull/251),
  [#252](https://github.com/butterfly-osm/butterfly-osm/pull/252)).

### Removed

- **butterfly-geocode**: Crate shelved
  ([#253](https://github.com/butterfly-osm/butterfly-osm/issues/253),
  [#254](https://github.com/butterfly-osm/butterfly-osm/pull/254)).
  The full geocoder work tree is preserved under the git tag
  `geocode-shelved-2026-05-23` and can be restored at any time; it is
  removed from the workspace to keep CI and release artifacts focused
  on the routing engine.

### Documentation

- New top-level `docs/` directory with a quickstart guide, REST + gRPC
  API reference, deployment guide, architecture overview, and
  troubleshooting notes.
- README rewritten to reflect the current state of the workspace
  (route engine production-ready, geocoder shelved, downloader stable).
- Stale "sparse triangle" comments across `route/src/server/exclude.rs`
  and adjacent modules updated to "incremental BFS"
  ([#251](https://github.com/butterfly-osm/butterfly-osm/pull/251),
  [#252](https://github.com/butterfly-osm/butterfly-osm/pull/252)) so
  the code matches the algorithm that actually runs.

### Performance reference (Belgium, 2026-05-23)

- 10k×10k distance matrix: **18.3 s**, 1.8× faster than OSRM CH on the
  same dataset.
- 50k×50k Flight gRPC matrix: **9.61 min**, at parity with the
  historical `/table/stream` baseline and well outside what OSRM can
  serve at all (URL-length limits, no streaming).
- `/route` with `avoid_polygons`, warm cache hit: **11 ms**.
- `/route` with `avoid_polygons`, cold miss: **~780 ms** for a 1 km
  rural polygon (was 37 s); **1.16 s** for the E19 motorway corridor.
- `/table` with `avoid_polygons`, warm cache hit: **22 ms** (was
  366 ms before the `Arc<AvoidEntry>` return).

## [Unreleased] — 2026-04-14

### Changed

- **License**: relicensed from MIT to AGPL-3.0-or-later. See
  [#99](https://github.com/butterfly-osm/butterfly-osm/issues/99) for the
  full rationale. Every workspace crate (`butterfly-common`,
  `butterfly-dl`, `butterfly-route`) now ships under
  AGPL-3.0-or-later. Network-deployed forks must publish source per the
  AGPL §13 requirement. The `LICENSE` file now carries the canonical FSF
  AGPL-3.0 text byte-for-byte. `CONTRIBUTING.md` documents the
  submission-implies-agreement contributor grant.

### Removed
- **butterfly-route**: Experimental PHAST routing implementation and related routing tools
- **benchmarks/**: Deprecated benchmark infrastructure
- **scripts/**: Deprecated utility scripts
- **Planned tool scaffolds**: Removed placeholder directories for butterfly-shrink, butterfly-extract, and butterfly-serve to focus on core functionality first

### Changed
- **Workspace structure**: Simplified to focus on production-ready butterfly-dl and butterfly-common foundation
- **Development focus**: Concentrating on core data acquisition tools before expanding to additional planned tools

## [2.0.0] - 2025-06-27

### 🌟 Major Milestone: Ecosystem Foundation

**Transformation from single-tool to ecosystem workspace**

### Added
- **🏗️ Workspace Architecture**: Multi-tool Rust workspace with shared components
- **📚 butterfly-common**: Shared library for error handling, geographic algorithms, and utilities
- **🤖 Automated Release Process**: Modern GitHub Actions with multi-platform builds (5 platforms)
- **🔒 Security**: Automatic checksums and integrity verification for all releases
- **📋 Tool Templates**: Standardized structure for future butterfly tools
- **🌍 Enhanced Geographic Intelligence**: Advanced fuzzy matching with semantic understanding
- **🎯 Project Roadmap**: Comprehensive development plan for ecosystem expansion
- **📊 CI Badge**: Added build status badge to README for transparency

### Changed
- **Repository Structure**: Organized as multi-tool workspace
- **Release Process**: Fully automated from tag push to published release (~4 minutes)
- **Performance**: Improved build times while maintaining runtime performance
- **Documentation**: Ecosystem-focused with tool-specific documentation

### Maintained
- **100% Backward Compatibility**: All v1.x APIs and CLI usage preserved
- **Performance**: Same runtime characteristics and memory efficiency
- **Features**: All existing functionality retained

### Performance
- **Build Efficiency**: Shared dependencies across tools
- **Release Speed**: 4-minute automated releases vs 30+ minute manual process
- **Platform Coverage**: 5 platforms (Linux x86_64/ARM64, macOS Intel/Apple Silicon, Windows x86_64)

---

## butterfly-dl Evolution (1.0.0 → 2.0.0)

*For detailed version history, see [butterfly-dl CHANGELOG](./tools/butterfly-dl/CHANGELOG.md)*

### Key Milestones

#### 🚀 **Performance Era** (1.4.x)
- Hurricane-fast downloads: **79% faster** than aria2, **3x faster** than curl
- Memory efficiency: **<1GB RAM** for any file size (including 81GB planet)
- Network resilience with intelligent retry and resume
- Beautiful progress displays with tqdm-style formatting

#### 🧠 **Intelligence Era** (1.2.x - 1.3.x)  
- Geographic-aware fuzzy matching: knows Belgium is in Europe, not Antarctica
- Dynamic source discovery from Geofabrik API
- Semantic error correction: "austrailia" → "australia-oceania" (not "austria")
- Real-time source updates, no hardcoded lists

#### 🏗️ **Architecture Era** (1.0.x - 1.1.x)
- Library + CLI architecture with C FFI bindings
- HTTP-only design for security and simplicity
- Smart connection scaling based on file size
- Comprehensive benchmarking against industry standards

#### 🛠️ **Foundation Era** (0.1.x)
- Multi-connection parallel downloads
- Docker-first development
- Convention over configuration approach
- Production-ready Geofabrik downloader

### Performance Achievements

| Metric | Achievement | Comparison |
|--------|-------------|------------|
| **Speed** | 14.07 MB/s | 79% faster than aria2 |
| **Memory** | <1GB fixed | 4-16x less than alternatives |
| **Reliability** | Smart resume | Network resilience with retry |
| **Intelligence** | Geographic fuzzy matching | Semantic understanding |

---

## Upcoming Tools

### 🔄 **Development Roadmap**

#### **Phase 2: Geometric Operations** 
- **butterfly-shrink**: Polygon-based extraction with GEOS integration
- Target: **10x faster** than osmium extract
- Memory limit: **<2GB** for planet-scale operations

#### **Phase 3: Data Transformation**
- **butterfly-extract**: Advanced filtering and transformation engine  
- Target: **5-10x faster** than osmosis
- Memory limit: **<1GB** for streaming operations

#### **Phase 4: High-Performance Serving**
- **butterfly-serve**: HTTP tile server with intelligent caching
- Target: **10-50x faster** QPS than existing solutions
- Memory limit: **<500MB** baseline + configurable caching

### 🎯 **Ecosystem Goals**

- **10x Performance**: Across all operations vs state-of-the-art
- **Minimal Memory**: Fixed memory usage regardless of data size  
- **Modern Architecture**: Rust's safety + async performance
- **Composable Design**: Unix philosophy applied to OSM processing

---

## Contributing

See [CONTRIBUTING.md](./CONTRIBUTING.md) for ecosystem development guidelines.

### Performance Standards
- All performance claims must be benchmarked
- Memory usage must be predictable and bounded
- Tools must compose via standard streams and formats

### Tool Development
- Each tool has a single, well-defined responsibility
- Shared functionality goes in butterfly-common
- Comprehensive test coverage including performance tests

---

**butterfly-osm** - Hurricane-fast OSM processing for the modern era.