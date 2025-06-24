# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added
- Initial project structure with Rust and Docker setup
- Docker Compose configuration with environment variable support
- Makefile for Docker operations (`build`, `run`, `test`, `clean`)
- Multi-stage Dockerfile for optimized Alpine builds
- Volume mounting for `./data/pbf/` directory structure
- Project documentation (README.md, TODO.md)
- MIT license and proper Cargo.toml metadata

### Infrastructure
- Docker-first development approach
- XP pair programming workflow with human + AI
- Continuous integration ready structure

## [0.1.0] - 2025-06-24

### Added
- Initial commit with basic project scaffolding
- Docker containerization working end-to-end
- Foundation for Geofabrik PBF downloader development

### Development Notes
- Tested with latest Rust in Alpine container
- Volume mounting verified for data persistence
- Ready for HTTP client and API integration phase