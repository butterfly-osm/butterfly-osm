# Geofabrik PBF Downloader - TODO

## Project Overview
Rust library + CLI tool for downloading OpenStreetMap PBF files from Geofabrik, containerized with Docker Compose.

## Phase 1: Core Setup ✅ COMPLETED
- [x] Initialize Rust project with Cargo.toml
- [x] Create Docker setup (Dockerfile, docker-compose.yml) 
- [x] Setup .env configuration for docker-compose
- [x] Create ./data/pbf/ volume mount structure
- [x] Create Makefile for Docker operations
- [x] Test Docker build and run functionality
- [ ] Add dependencies: `reqwest`, `clap`, `serde`, `tokio`, `anyhow`, `indicatif`

## Phase 2: Library Development
- [ ] Parse Geofabrik JSON API (https://download.geofabrik.de/index-v1.json)
- [ ] Create data structures for continents and countries (no sub-regions)
- [ ] Implement HTTP client for downloading PBF files
- [ ] Add progress bars for downloads
- [ ] Handle file organization: `./data/pbf/{continent}/{country}.pbf`

## Phase 3: CLI Interface
- [ ] Support individual downloads: `--country monaco`
- [ ] Support continent downloads: `--continent europe`  
- [ ] Support lists: `--countries monaco,andorra` or `--continents europe,africa`
- [ ] Add dry-run mode to preview downloads
- [ ] Implement proper error handling and logging

## Phase 4: Testing & Validation
- [ ] Test with Monaco (smallest country)
- [ ] Test continent download (start with smallest continent)
- [ ] Test list functionality with multiple countries
- [ ] Verify file structure and naming conventions
- [ ] Add unit tests for core library functions

## Phase 5: Docker Integration
- [x] Containerize CLI application
- [x] Configure docker-compose with environment variables
- [x] Test volume mounting for ./data directory
- [x] Create Makefile for container operations
- [ ] Test actual PBF downloads within container
- [ ] Document Docker usage patterns

## Phase 6: Documentation & Release
- [ ] Write comprehensive README.md
- [ ] Document CLI usage and examples
- [ ] Add library documentation with rustdoc
- [ ] Create CHANGELOG.md
- [ ] Prepare for community release

## Development Notes
- Start with Monaco for testing (fastest download)
- Use JSON API to discover available regions
- No checksum verification (not in JSON API)  
- Follow XP principles: test-first, simple design, atomic commits

## Docker Commands (Working)
```bash
make build          # Build Docker image
make run ARGS="..."  # Run with arguments
make test           # Run tests in container
make clean          # Clean up resources
```

## Current Status
✅ **Infrastructure Ready**: Docker, Compose, Makefile all working
🔄 **Next**: Add Rust dependencies and start library development
🎯 **Goal**: Download Monaco PBF file as first working test