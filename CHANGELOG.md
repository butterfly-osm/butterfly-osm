# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.1.0] - 2025-06-24

### Added
- **Geofabrik PBF Downloader Component** - Initial working implementation
- **Multi-connection parallel downloads** with 8 connections and 100MB chunks for optimal performance
- **File freshness checking** - Skip downloads if files are newer than `RENEW_PBF_PERIOD` (default: 7 days)
- **Complete CLI interface** with country, continent, and batch download support
- **List command** with filtering (countries/continents/all)
- **Dry-run mode** for previewing downloads without downloading
- **FROM scratch production Docker image** (13.4MB) for minimal attack surface
- **Development Docker image** with full Alpine + Rust toolchain for debugging
- **Convention over configuration** approach with hardcoded optimal defaults
- **Comprehensive test coverage** (16 tests) including integration tests
- **Makefile with production/development workflows**
- **Environment variable configuration** for logging and renewal period

### Performance
- **8 parallel connections** hardcoded for maximum speed
- **100MB chunks** optimized for large file downloads
- **Automatic range request detection** with fallback to single connection
- **Progress tracking** with real-time connection count display
- **Speed improvements**: Up to 15-40 MB/s vs 2-5 MB/s single connection

### Infrastructure
- **Docker-first development** with both production and development images
- **XP pair programming** workflow with human + AI collaboration
- **Comprehensive documentation** with clear usage examples
- **MIT license** and proper project metadata

### Notes
- This is the first component of the larger **butterfly** project
- Implements a complete, production-ready Geofabrik downloader
- Foundation established for future components and integrations