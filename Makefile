.PHONY: build build-prod build-dev run run-prod run-dev test clean help list download dev-shell config-help

# Build both production and development images
build:
	docker compose build

# Build production image only
build-prod:
	docker compose build geofabrik-downloader

# Build development image only  
build-dev:
	docker compose build geofabrik-dev

# Run with production image (default - minimal scratch-based)
run:
	docker compose run --rm geofabrik-downloader $(ARGS)

# Run with production image (explicit)
run-prod:
	docker compose run --rm geofabrik-downloader $(ARGS)

# Run with development image (full Alpine with tools)
run-dev:
	docker compose run --rm geofabrik-dev $(ARGS)

# Run tests in container
test:
	docker compose run --rm --entrypoint="" geofabrik-dev cargo test

# Clean up Docker resources
clean:
	docker compose down --rmi all --volumes --remove-orphans

# List available regions (production)
list:
	docker compose run --rm geofabrik-downloader list

# List available regions (development)
list-dev:
	docker compose run --rm geofabrik-dev list

# Quick download shortcuts (production)
download:
	@echo "Usage: make download REGION=country_or_continent"
	@echo "Example: make download REGION=monaco"
	@echo "Example: make download REGION=europe"
	@if [ -z "$(REGION)" ]; then \
		echo "Error: REGION parameter is required"; \
		exit 1; \
	fi
	docker compose run --rm geofabrik-downloader country $(REGION) || \
	docker compose run --rm geofabrik-downloader continent $(REGION)

# Quick download shortcuts (development)
download-dev:
	@echo "Usage: make download-dev REGION=country_or_continent"
	@echo "Example: make download-dev REGION=monaco"
	@echo "Example: make download-dev REGION=europe"
	@if [ -z "$(REGION)" ]; then \
		echo "Error: REGION parameter is required"; \
		exit 1; \
	fi
	docker compose run --rm geofabrik-dev country $(REGION) || \
	docker compose run --rm geofabrik-dev continent $(REGION)

# Open development shell
dev-shell:
	docker compose run --rm geofabrik-dev sh

# Show configuration help
config-help:
	@echo "Geofabrik Downloader Configuration:"
	@echo ""
	@echo "Convention over Configuration:"
	@echo "  ✅ 8 parallel connections (hardcoded optimal default)"
	@echo "  ✅ 100MB chunks (hardcoded optimal default)"
	@echo "  ✅ Multi-connection enabled (hardcoded optimal default)"
	@echo "  ✅ Automatic range request detection and fallback"
	@echo ""
	@echo "Environment Variables:"
	@echo "  RUST_LOG          - Logging level (debug, info, warn, error)"
	@echo "  RENEW_PBF_PERIOD  - Days before re-downloading files (default: 7)"
	@echo ""
	@echo "Examples:"
	@echo "  RUST_LOG=debug make run ARGS='country monaco'"
	@echo "  RUST_LOG=warn make run-prod ARGS='continent europe'"
	@echo "  RENEW_PBF_PERIOD=14 make run ARGS='country belgium'"
	@echo ""
	@echo "Note: Performance settings are hardcoded for optimal speed."
	@echo "Files are only re-downloaded if older than RENEW_PBF_PERIOD days."

# Show help
help:
	@echo "Geofabrik PBF Downloader - Docker Commands"
	@echo ""
	@echo "Build Commands:"
	@echo "  build         - Build both production and development images"
	@echo "  build-prod    - Build production image only (13.4MB scratch-based)"
	@echo "  build-dev     - Build development image only (Alpine with tools)"
	@echo ""
	@echo "Run Commands:"
	@echo "  run ARGS=...  - Run with production image (default, minimal)"
	@echo "  run-prod ARGS=... - Run with production image (explicit)"
	@echo "  run-dev ARGS=...  - Run with development image (debugging)"
	@echo "  test          - Run tests in development container"
	@echo "  clean         - Clean up Docker resources"
	@echo ""
	@echo "Convenience Commands:"
	@echo "  list          - List available regions (production)"
	@echo "  list-dev      - List available regions (development)"
	@echo "  download REGION=... - Quick download with production image"
	@echo "  download-dev REGION=... - Quick download with development image"
	@echo "  dev-shell     - Open development shell"
	@echo ""
	@echo "Production Examples (13.4MB scratch image):"
	@echo "  make run ARGS='list'"
	@echo "  make run ARGS='list countries'"
	@echo "  make run ARGS='country monaco'"
	@echo "  make run ARGS='continent europe'"
	@echo "  make run ARGS='countries monaco,belgium'"
	@echo "  make run ARGS='--dry-run country monaco'"
	@echo "  make download REGION=monaco"
	@echo ""
	@echo "Development Examples (Alpine with debugging tools):"
	@echo "  make run-dev ARGS='country monaco'"
	@echo "  make download-dev REGION=europe"
	@echo "  make dev-shell  # Interactive shell for debugging"
	@echo ""
	@echo "Image Sizes:"
	@echo "  Production:  ~13.4MB (FROM scratch, binary only)"
	@echo "  Development: ~500MB+ (Alpine + Rust + tools)"