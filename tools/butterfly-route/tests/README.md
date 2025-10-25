# Butterfly-Route Test Suite

## Comparison Testing Against OSRM

### compare_osrm.sh

Comprehensive test script that compares butterfly-route performance and route quality against OSRM.

#### Features

- Tests multiple routes across Belgium (urban, intercity, long routes)
- Runs multiple iterations per route for statistical accuracy
- Calculates performance statistics (mean, median, stddev, min, max)
- Compares query latency, distance, and duration
- Optional JSON output for CI/CD integration
- Automatic binary detection
- Graceful handling of OSRM availability

#### Usage

```bash
cd /home/snape/projects/butterfly-osm

# Basic usage (requires OSRM running on localhost:6666)
./tools/butterfly-route/tests/compare_osrm.sh

# Custom number of runs
./tools/butterfly-route/tests/compare_osrm.sh --runs 5

# Specify graph file
./tools/butterfly-route/tests/compare_osrm.sh --butterfly-graph belgium-restrictions.graph

# Save results to JSON
./tools/butterfly-route/tests/compare_osrm.sh --output results.json

# Verbose mode for debugging
./tools/butterfly-route/tests/compare_osrm.sh --verbose

# Custom OSRM server
./tools/butterfly-route/tests/compare_osrm.sh --osrm-url http://osrm-server:5000
```

#### Options

- `--butterfly-graph PATH`: Path to butterfly-route graph file (default: belgium-restrictions.graph)
- `--butterfly-bin PATH`: Path to butterfly-route binary (default: auto-detect)
- `--osrm-url URL`: OSRM server URL (default: http://localhost:6666)
- `--runs N`: Number of test runs per route (default: 5)
- `--output FILE`: Save results to JSON file
- `--verbose`: Enable verbose output
- `--help`: Show help message

#### Prerequisites

- **butterfly-route**: Built binary (release mode recommended for accurate benchmarking)
- **OSRM**: Running OSRM server with Belgium dataset
- **jq**: JSON processor (`apt install jq` or `brew install jq`)
- **bc**: Basic calculator (`apt install bc` or `brew install bc`)
- **curl**: HTTP client (usually pre-installed)

#### Setting up OSRM

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

#### Test Routes

The script includes 10 predefined routes covering:

1. **Short urban routes**: Brussels Center, Antwerp Center
2. **Medium intercity routes**: Brussels-Antwerp, Brussels-Ghent, Brussels-Namur, Antwerp-Ghent
3. **Long routes**: Brussels-Liège, Ghent-Liège
4. **Edge cases**: Coastal route, Ardennes route

Note: Some routes may fail if there's no valid path in the graph (e.g., isolated road networks or data gaps in the OSM extract).

#### Output Format

The script generates a formatted table comparing butterfly-route vs OSRM:

```
Route                     | Butterfly Time  | OSRM Time       | Speedup   | Butterfly Dist | OSRM Dist
--------------------------------------------------------------------------------------------------------------------------
Brussels_Antwerp          |        0.751s   |        0.006s   |    125.2x |       29223m   |       45935m (+57%)
Brussels_Ghent            |        0.689s   |        0.007s   |     98.4x |       34650m   |       55863m (+61%)
...

Overall Statistics:
  Average butterfly-route query time: 0.720s
  Average OSRM query time: 0.0065s
  Average speedup: 110.8x (OSRM is faster)
  Successful tests: 8/10
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

**Performance**: OSRM is typically 100-150x faster due to Contraction Hierarchies preprocessing. Butterfly-route prioritizes simplicity and educational value over raw speed.

**Route Quality**: Differences in distance/duration may indicate:
- Different routing strategies (OSRM uses real road speeds, butterfly uses simplified speed model)
- OSM data interpretation differences
- Turn restriction enforcement (butterfly-route includes restrictions)

**Expected Performance**: For Belgium dataset:
- butterfly-route: 0.5-1.5s per query
- OSRM: 0.005-0.015s per query

#### Troubleshooting

**"Error: Graph file not found"**: Specify correct path with `--butterfly-graph`

**"OSRM server not accessible"**: Ensure OSRM is running on the specified port. Script will continue with butterfly-only tests.

**"No route found between points"**: Some test coordinates may not have valid paths. This is expected for isolated areas or data gaps.

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
