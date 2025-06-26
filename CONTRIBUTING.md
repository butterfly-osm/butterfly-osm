# Contributing to Geofabrik PBF Downloader

Welcome! This project follows **XP (Extreme Programming) pair programming** with human + AI collaboration.

## Development Philosophy

### Core Principles
- **Docker-first development** - All work happens in containers
- **Test-driven development** - Write failing tests, then implement
- **Simplest design** - Choose minimal abstractions that work
- **Continuous refactoring** - Improve code fearlessly under test coverage
- **Collective ownership** - Anyone can modify any part of the codebase

### When in Doubt
**Ask questions, never assume, Keep It Simple, Stupid (KISS) at all times.**

## Getting Started

### Prerequisites
- Docker and Docker Compose
- Make (for convenience commands)
- Git

### Setup
```bash
git clone <repository-url>
cd butterfly-dl
make build
make test
```

## Development Workflow

### 1. Explore → Plan → Code → Test → Refactor → Document

```bash
# Start with exploration
make run ARGS="list --filter countries"

# Run tests frequently
make test

# Build and verify
make build
```

### 2. Branch Strategy
```bash
# Create feature branch
git checkout -b feature/short-description

# Work incrementally
git add .
git commit -m "feat(module): add specific feature"

# Push and create PR
git push -u origin feature/short-description
```

### 3. Testing
```bash
# Unit tests
cargo test

# Integration tests  
cargo test --test integration

# Docker tests
make test
```

## Code Standards

### Naming Conventions
- **Functions**: `snake_case` with clear, descriptive names
- **Types**: `PascalCase` following Rust conventions  
- **Variables**: `snake_case`, meaningful in context
- **Files**: `snake_case.rs`

### Documentation
- All public functions need rustdoc comments
- Include examples in documentation
- Update README for user-facing changes
- Keep CHANGELOG.md current

### Error Handling
- Use custom error types (`GeofabrikError`)
- Provide helpful error messages
- Log errors with appropriate levels

### Testing Requirements
- Unit tests for all core logic
- Integration tests for CLI workflows
- Test both success and error cases
- Use descriptive test names

## Architecture

### Core Components
```
src/
├── main.rs              # CLI interface and main logic
├── lib.rs               # Library interface (future)
└── ...                  # Additional modules as needed
```

### Key Features
- **CLI Commands**: country, continent, countries, continents, list
- **Configuration**: YAML files + environment variables
- **Error Handling**: Custom error types with context
- **Logging**: Structured logging with levels
- **Testing**: Comprehensive unit and integration tests

## Submitting Changes

### Pull Request Process
1. **Branch naming**: `feature/description` or `fix/description`
2. **Commit messages**: Use [Conventional Commits](https://conventionalcommits.org/)
   - `feat(cli): add list command`
   - `fix(download): handle network timeouts`
   - `docs(readme): update examples`
3. **PR description**: 
   - Link to related issues
   - Describe what changed and why
   - Include test results
4. **Review**: Address feedback iteratively

### Commit Message Format
```
type(scope): description

[optional body]

[optional footer]
```

Types: `feat`, `fix`, `docs`, `test`, `refactor`, `chore`

## Local Development Tips

### Quick Commands
```bash
# Format code
cargo fmt

# Check linting
cargo clippy

# Run specific test
cargo test test_name

# Debug logging
RUST_LOG=debug make run ARGS="country monaco"

# Dry run testing
make run ARGS="--dry-run list"
```

### Configuration Testing
```bash
# Test with custom config
echo "data_dir: /tmp/test" > geofabrik.yaml
make run ARGS="country monaco"

# Test environment variables
GEOFABRIK_DATA_DIR=/tmp make run ARGS="list"
```

## Issue Reporting

### Bug Reports
Include:
- Steps to reproduce
- Expected vs actual behavior
- Environment details (OS, Docker version)
- Log output with `RUST_LOG=debug`

### Feature Requests
Include:
- Use case description
- Proposed API/interface
- Implementation suggestions (optional)

## Communication

- **Issues**: For bugs, features, and questions
- **Pull Requests**: For code changes
- **Discussions**: For architectural decisions

## Release Process

### Automated Releases
This project uses automated GitHub Actions workflows for releases:

#### Creating a Release
1. **Update version numbers**:
   ```bash
   # Update VERSION file
   echo "1.4.2" > VERSION
   
   # Update Cargo.toml (manual due to Cargo limitations)
   # Edit version = "1.4.2" in Cargo.toml
   ```

2. **Commit and tag**:
   ```bash
   git add VERSION Cargo.toml
   git commit -m "chore: bump version to v1.4.2"
   git tag -a v1.4.2 -m "Release v1.4.2"
   git push origin main --tags
   ```

3. **Automatic workflow triggers**:
   - Cross-platform builds (Linux x86_64/ARM64, macOS Intel/Apple Silicon, Windows)
   - Binary packaging with checksums
   - GitHub release creation with changelog
   - Asset uploads to release

#### Release Artifacts
The automated workflow generates:
- `butterfly-dl-v1.4.2-x86_64-linux.tar.gz`
- `butterfly-dl-v1.4.2-aarch64-linux.tar.gz`
- `butterfly-dl-v1.4.2-x86_64-macos.tar.gz`
- `butterfly-dl-v1.4.2-aarch64-macos.tar.gz`
- `butterfly-dl-v1.4.2-x86_64-windows.zip`
- Individual SHA256 checksum files
- Combined `checksums.txt`

#### CI/CD Workflows
- **CI**: Runs on all PRs and main branch pushes
  - Tests across Linux, macOS, Windows
  - Clippy linting and formatting checks
  - Cross-compilation testing
  - Security audits
  - Documentation generation
  - Performance benchmarks
- **Release**: Triggered by version tags
- **Dependabot**: Automatic dependency updates with auto-merge for minor/patch versions

### Manual Release Steps (if needed)
```bash
# Emergency manual release process
cargo build --release --target x86_64-unknown-linux-gnu
tar czf butterfly-dl-manual.tar.gz target/x86_64-unknown-linux-gnu/release/butterfly-dl README.md LICENSE
```

## Getting Help

### Useful Resources
- [Rust Documentation](https://doc.rust-lang.org/)
- [Clap CLI Framework](https://docs.rs/clap/)
- [Docker Best Practices](https://docs.docker.com/develop/dev-best-practices/)
- [Geofabrik API](https://download.geofabrik.de/technical.html)
- [GitHub Actions Documentation](https://docs.github.com/en/actions)

### Project Maintainer
Pierre <pierre@warnier.net>

---

**Remember**: This is a collaborative project. Every contribution, no matter how small, is valuable. When in doubt, open an issue and ask!