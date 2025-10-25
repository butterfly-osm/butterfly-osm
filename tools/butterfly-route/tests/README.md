# Butterfly-Route Test Suite

## Comparison Testing Against Production Routing Engines

### compare_osrm.sh

Comprehensive test script that compares butterfly-route performance and route quality against production routing engines:
- **OSRM** (Open Source Routing Machine)
- **Valhalla** (Mapbox/Linux Foundation)
- **GraphHopper**

#### Features

- Tests multiple routes across Belgium (urban, intercity, long routes)
- Runs multiple iterations per route for statistical accuracy
- Calculates performance statistics (mean, median, stddev, min, max)
- Compares query latency, distance, and duration across all engines
- Optional JSON output for CI/CD integration
- Automatic binary detection
- Graceful handling of unavailable routing engines

#### Usage

```bash
cd /home/snape/projects/butterfly-osm

# Basic usage (tests all available engines)
./tools/butterfly-route/tests/compare_osrm.sh

# Custom number of runs
./tools/butterfly-route/tests/compare_osrm.sh --runs 5

# Specify graph file
./tools/butterfly-route/tests/compare_osrm.sh --butterfly-graph belgium-restrictions.graph

# Save results to JSON
./tools/butterfly-route/tests/compare_osrm.sh --output results.json

# Verbose mode for debugging
./tools/butterfly-route/tests/compare_osrm.sh --verbose

# Custom server URLs
./tools/butterfly-route/tests/compare_osrm.sh \
  --osrm-url http://localhost:6666 \
  --valhalla-url http://localhost:6667 \
  --graphhopper-url http://localhost:6668
```

#### Options

- `--butterfly-graph PATH`: Path to butterfly-route graph file (default: belgium-restrictions.graph)
- `--butterfly-bin PATH`: Path to butterfly-route binary (default: auto-detect)
- `--osrm-url URL`: OSRM server URL (default: http://localhost:6666)
- `--valhalla-url URL`: Valhalla server URL (default: http://localhost:6667)
- `--graphhopper-url URL`: GraphHopper server URL (default: http://localhost:6668)
- `--runs N`: Number of test runs per route (default: 5)
- `--output FILE`: Save results to JSON file
- `--verbose`: Enable verbose output
- `--help`: Show help message

#### Prerequisites

- **butterfly-route**: Built binary (release mode recommended for accurate benchmarking)
- **Routing Engines** (at least one required):
  - OSRM server with Belgium dataset (port 6666)
  - Valhalla server with Belgium dataset (port 6667)
  - GraphHopper server with Belgium dataset (port 6668)
- **jq**: JSON processor (`apt install jq` or `brew install jq`)
- **bc**: Basic calculator (`apt install bc` or `brew install bc`)
- **curl**: HTTP client (usually pre-installed)

#### Setting up Routing Engines

##### OSRM

See the main PLAN.md for OSRM setup instructions. Quick start:

```bash
# Pull OSRM Docker image
docker pull ghcr.io/project-osrm/osrm-backend

# Process Belgium PBF
docker run -t -v $(pwd):/data ghcr.io/project-osrm/osrm-backend osrm-extract -p /opt/car.lua /data/belgium.pbf
docker run -t -v $(pwd):/data ghcr.io/project-osrm/osrm-backend osrm-partition /data/belgium.osrm
docker run -t -v $(pwd):/data ghcr.io/project-osrm/osrm-backend osrm-customize /data/belgium.osrm

# Start OSRM server
docker run -t -i -p 6666:5000 -v $(pwd):/data ghcr.io/project-osrm/osrm-backend osrm-routed --algorithm mld /data/belgium.osrm
```

##### Valhalla

```bash
# Pull Valhalla Docker image
docker pull ghcr.io/valhalla/valhalla:latest

# Process Belgium PBF
mkdir -p valhalla_tiles
docker run -v $(pwd):/data ghcr.io/valhalla/valhalla:latest \
  valhalla_build_config \
  --mjolnir-tile-dir /data/valhalla_tiles \
  --mjolnir-tile-extract /data/valhalla_tiles.tar \
  --mjolnir-timezone /data/valhalla_tiles/timezones.sqlite \
  --mjolnir-admin /data/valhalla_tiles/admins.sqlite \
  > valhalla.json

docker run -v $(pwd):/data ghcr.io/valhalla/valhalla:latest \
  valhalla_build_tiles -c /data/valhalla.json /data/belgium.pbf

# Start Valhalla server
docker run -p 6667:8002 -v $(pwd):/data ghcr.io/valhalla/valhalla:latest \
  valhalla_service /data/valhalla.json 1
```

##### GraphHopper

```bash
# Pull GraphHopper Docker image
docker pull graphhopper/graphhopper:latest

# Start GraphHopper server (processes PBF on startup)
docker run -p 6668:8989 -v $(pwd):/data \
  -e "JAVA_OPTS=-Xmx4g -Xms1g" \
  graphhopper/graphhopper:latest \
  --input /data/belgium.pbf \
  --host 0.0.0.0
```

**Note**: All engines need to process the Belgium PBF file. This can take several minutes to hours depending on your hardware. OSRM is typically the fastest to set up.

#### Test Routes

The script includes 10 predefined routes covering:

1. **Short urban routes**: Brussels Center, Antwerp Center
2. **Medium intercity routes**: Brussels-Antwerp, Brussels-Ghent, Brussels-Namur, Antwerp-Ghent
3. **Long routes**: Brussels-Liège, Ghent-Liège
4. **Edge cases**: Coastal route, Ardennes route

Note: Some routes may fail if there's no valid path in the graph (e.g., isolated road networks or data gaps in the OSM extract).

#### Output Format

The script generates a formatted table comparing butterfly-route against all available engines:

```
Route                   | Butterfly     | OSRM        | Valhalla    | GraphHopper | Distance
-----------------------------------------------------------------------------------------------
Brussels_Center         |     0.751s    |   0.006s    |   0.008s    |   0.010s    |    0.9km
Brussels_Antwerp        |     0.825s    |   0.007s    |   0.009s    |   0.012s    |   45.9km
Brussels_Ghent          |     0.689s    |   0.008s    |   0.010s    |   0.011s    |   55.9km
...

Overall Statistics:
  Successful tests: 8/10

  Engine                Avg Time
  --------------------------------
  butterfly-route         0.755s
  OSRM                    0.007s  (107.9x faster)
  Valhalla                0.009s  (83.9x faster)
  GraphHopper             0.011s  (68.6x faster)
```

#### JSON Output

When using `--output FILE`, results are saved in JSON format for programmatic analysis:

```json
{
  "timestamp": "2025-10-25T17:00:00Z",
  "configuration": {
    "butterfly_bin": "/path/to/butterfly-route",
    "butterfly_graph": "belgium-restrictions.graph",
    "osrm_url": "http://localhost:6666",
    "num_runs": 5
  },
  "results": {
    "Brussels_Antwerp": {
      "description": "Brussels to Antwerp (major cities)",
      "coordinates": {
        "from": {"lat": 50.8503, "lon": 4.3517},
        "to": {"lat": 51.2194, "lon": 4.4025}
      },
      "butterfly": {
        "status": "SUCCESS",
        "time_mean": 0.751,
        "time_median": 0.748,
        "time_stddev": 0.012,
        "distance": 29223,
        "duration": 32.5,
        "nodes_visited": 28441
      },
      "osrm": {
        "status": "SUCCESS",
        "time_mean": 0.006,
        "time_median": 0.006,
        "time_stddev": 0.001,
        "distance": 45935,
        "duration": 36.2
      }
    }
  }
}
```

#### Interpreting Results

**Performance**: Production routing engines (OSRM, Valhalla, GraphHopper) are typically 50-150x faster due to advanced preprocessing techniques:
- OSRM uses Contraction Hierarchies (CH) or Multi-Level Dijkstra (MLD)
- Valhalla uses tiled graph architecture with bidirectional A*
- GraphHopper supports CH and Landmarks (ALT)

Butterfly-route prioritizes simplicity, clarity, and educational value over raw speed.

**Route Quality**: Differences in distance/duration across engines may indicate:
- Different routing strategies and speed models
- OSM data interpretation differences (e.g., access restrictions, road classifications)
- Turn restriction enforcement variations
- Different cost functions and heuristics

**Expected Performance**: For Belgium dataset:
- butterfly-route: 0.5-1.5s per query
- OSRM: 0.005-0.015s per query
- Valhalla: 0.008-0.020s per query
- GraphHopper: 0.010-0.025s per query

#### Troubleshooting

**"Error: Graph file not found"**: Specify correct path with `--butterfly-graph`

**"Engine server not accessible"**: Ensure the routing engine is running on the specified port. Script will continue testing other available engines.

**"No route found between points"**: Some test coordinates may not have valid paths. This is expected for isolated areas or data gaps in the OSM extract.

**Script hangs**: Check if butterfly-route queries are completing. Try `--verbose` mode to see detailed progress.

## CI/CD Integration

Example GitHub Actions workflow:

```yaml
- name: Run comparison tests
  run: |
    ./tools/butterfly-route/tests/compare_osrm.sh \
      --runs 3 \
      --butterfly-graph belgium-restrictions.graph \
      --output results.json

    # Parse results and fail if performance degrades significantly
    jq '.results | to_entries | map(select(.value.butterfly.status == "SUCCESS")) |
        if length < 5 then error("Too many failed routes") else . end' results.json
```

## Development

To add new test routes, edit the `TEST_ROUTES` array in `compare_osrm.sh`:

```bash
declare -a TEST_ROUTES=(
    "Route_Name|from_lat|from_lon|to_lat|to_lon|Description"
)
```

Coordinates should be in decimal degrees (lat,lon format).
