# Geofabrik PBF Downloader

A Rust library and CLI tool for downloading OpenStreetMap PBF files from Geofabrik with Docker support.

## What

Downloads OSM data in PBF format from [Geofabrik](https://download.geofabrik.de/) with support for:
- Individual countries (`--country monaco`)
- Entire continents (`--continent europe`) 
- Lists of regions (`--countries monaco,andorra`)
- Dockerized execution with volume mounting

## Why

Simplifies OSM data acquisition for mapping applications, routing engines, and geospatial analysis by providing a reliable, containerized download tool.

## How

### Docker (Recommended)

```bash
# Build
make build

# Download Monaco
make run ARGS="--country monaco"

# Download multiple countries  
make run ARGS="--countries monaco,andorra,malta"

# Download entire continent
make run ARGS="--continent europe"
```

### Native

```bash
cargo build --release
./target/release/geofabrik-downloader --country monaco
```

## File Structure

Downloaded files are organized as:
```
./data/pbf/
├── europe/
│   ├── monaco.pbf
│   └── andorra.pbf
└── africa/
    └── ...
```

## Development

Docker-first development with XP practices:

```bash
make build    # Build container
make test     # Run tests  
make clean    # Clean up
```

## Status

🔄 **Early Development** - Basic Docker infrastructure completed
🎯 **Next**: Add HTTP client and JSON API parsing  
📋 **Roadmap**: See [TODO.md](TODO.md)

## Contributing

This project follows XP pair programming with human + AI collaboration. See [CLAUDE.md](CLAUDE.md) for development guidelines.

## Who

Built by Pierre <pierre@warnier.net> for the broader OpenStreetMap community.
