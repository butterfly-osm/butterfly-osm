.PHONY: build run test clean help

# Build the Docker image
build:
	docker compose build

# Run the downloader (example: make run ARGS="--country monaco")
run:
	docker compose run --rm geofabrik-downloader $(ARGS)

# Run tests in container
test:
	docker compose run --rm geofabrik-downloader cargo test

# Clean up Docker resources
clean:
	docker compose down --rmi all --volumes --remove-orphans

# Show help
help:
	@echo "Available commands:"
	@echo "  build  - Build the Docker image"
	@echo "  run    - Run the downloader (use ARGS='--help' for options)"
	@echo "  test   - Run tests in container"
	@echo "  clean  - Clean up Docker resources"
	@echo ""
	@echo "Examples:"
	@echo "  make run ARGS='--help'"
	@echo "  make run ARGS='--country monaco'"