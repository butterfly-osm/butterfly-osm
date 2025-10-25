#!/bin/bash
#
# Comprehensive comparison test suite: butterfly-route vs OSRM
#
# This script tests multiple routes across Belgium to compare:
# 1. Query performance (latency)
# 2. Route quality (distance, duration)
# 3. Consistency and reliability
#
# Usage:
#   ./compare_osrm.sh [options]
#
# Options:
#   --butterfly-graph PATH    Path to butterfly-route graph file (default: belgium-restrictions.graph)
#   --butterfly-bin PATH      Path to butterfly-route binary (default: auto-detect)
#   --osrm-url URL           OSRM server URL (default: http://localhost:6666)
#   --runs N                 Number of test runs per route (default: 5)
#   --output FILE            Save results to JSON file
#   --verbose                Enable verbose output
#   --help                   Show this help
#

set -euo pipefail

# Default configuration
BUTTERFLY_GRAPH="${BUTTERFLY_GRAPH:-belgium-restrictions.graph}"
BUTTERFLY_BIN=""
OSRM_URL="${OSRM_URL:-http://localhost:6666}"
NUM_RUNS=5
OUTPUT_FILE=""
VERBOSE=0
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../../.." && pwd)"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
BOLD='\033[1m'
NC='\033[0m' # No Color

# Test routes: name, from_lat, from_lon, to_lat, to_lon, description
declare -a TEST_ROUTES=(
    # Short urban routes
    "Brussels_Center|50.8503|4.3517|50.8485|4.3621|Short urban route in Brussels center"
    "Antwerp_Center|51.2194|4.4025|51.2150|4.4100|Short urban route in Antwerp center"

    # Medium intercity routes
    "Brussels_Antwerp|50.8503|4.3517|51.2194|4.4025|Brussels to Antwerp (major cities)"
    "Brussels_Ghent|50.8467|4.3517|51.0543|3.7174|Brussels to Ghent (highway route)"
    "Brussels_Namur|50.8503|4.3517|50.4669|4.8624|Brussels to Namur (southern route)"
    "Antwerp_Ghent|51.2194|4.4025|51.0543|3.7174|Antwerp to Ghent (east-west)"

    # Long routes
    "Brussels_Liege|50.8503|4.3517|50.6326|5.5797|Brussels to Liège (long eastern route)"
    "Ghent_Liege|51.0543|3.7174|50.6326|5.5797|Ghent to Liège (cross-country)"

    # Edge cases
    "Coastal_Route|51.2200|2.9300|51.3300|3.2500|Coastal route (Ostend to Knokke)"
    "Ardennes_Route|50.4100|5.8000|50.0100|5.4500|Ardennes region (rural, hilly)"
)

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --butterfly-graph)
            BUTTERFLY_GRAPH="$2"
            shift 2
            ;;
        --butterfly-bin)
            BUTTERFLY_BIN="$2"
            shift 2
            ;;
        --osrm-url)
            OSRM_URL="$2"
            shift 2
            ;;
        --runs)
            NUM_RUNS="$2"
            shift 2
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        --verbose)
            VERBOSE=1
            shift
            ;;
        --help)
            grep '^#' "$0" | grep -v '#!/bin/bash' | sed 's/^# \?//'
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Auto-detect butterfly-route binary if not specified
if [[ -z "$BUTTERFLY_BIN" ]]; then
    if [[ -f "$PROJECT_ROOT/target/release/butterfly-route" ]]; then
        BUTTERFLY_BIN="$PROJECT_ROOT/target/release/butterfly-route"
    elif command -v butterfly-route &> /dev/null; then
        BUTTERFLY_BIN="butterfly-route"
    else
        echo -e "${RED}Error: Cannot find butterfly-route binary${NC}"
        echo "Please specify with --butterfly-bin or build with 'cargo build --release'"
        exit 1
    fi
fi

# Validate dependencies
if ! command -v jq &> /dev/null; then
    echo -e "${RED}Error: jq is required but not installed${NC}"
    exit 1
fi

if ! command -v bc &> /dev/null; then
    echo -e "${RED}Error: bc is required but not installed${NC}"
    exit 1
fi

if ! command -v curl &> /dev/null; then
    echo -e "${RED}Error: curl is required but not installed${NC}"
    exit 1
fi

# Check if butterfly-route graph exists
if [[ ! -f "$BUTTERFLY_GRAPH" ]]; then
    echo -e "${RED}Error: Graph file not found: $BUTTERFLY_GRAPH${NC}"
    exit 1
fi

# Check if OSRM is accessible
if ! curl -s --max-time 2 "$OSRM_URL/health" &> /dev/null; then
    echo -e "${YELLOW}Warning: OSRM server at $OSRM_URL is not accessible${NC}"
    echo "Starting with butterfly-route only tests..."
    OSRM_AVAILABLE=0
else
    OSRM_AVAILABLE=1
fi

# Helper functions
log_info() {
    echo -e "${BLUE}[INFO]${NC} $*"
}

log_success() {
    echo -e "${GREEN}[SUCCESS]${NC} $*"
}

log_warning() {
    echo -e "${YELLOW}[WARNING]${NC} $*"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $*"
}

log_verbose() {
    if [[ $VERBOSE -eq 1 ]]; then
        echo -e "${BLUE}[VERBOSE]${NC} $*"
    fi
}

# Calculate statistics
calc_stats() {
    local values=("$@")
    local sum=0
    local count=${#values[@]}

    # Calculate mean
    for val in "${values[@]}"; do
        sum=$(echo "$sum + $val" | bc -l)
    done
    local mean=$(echo "scale=6; $sum / $count" | bc -l)

    # Calculate median
    IFS=$'\n' sorted=($(sort -n <<<"${values[*]}"))
    unset IFS
    local median
    if (( count % 2 == 1 )); then
        median=${sorted[$((count/2))]}
    else
        local mid1=${sorted[$((count/2-1))]}
        local mid2=${sorted[$((count/2))]}
        median=$(echo "scale=6; ($mid1 + $mid2) / 2" | bc -l)
    fi

    # Calculate standard deviation
    local sum_sq_diff=0
    for val in "${values[@]}"; do
        local diff=$(echo "$val - $mean" | bc -l)
        local sq=$(echo "$diff * $diff" | bc -l)
        sum_sq_diff=$(echo "$sum_sq_diff + $sq" | bc -l)
    done
    local variance=$(echo "scale=6; $sum_sq_diff / $count" | bc -l)
    local stddev=$(echo "scale=6; sqrt($variance)" | bc -l)

    # Min and max
    local min=${sorted[0]}
    local max=${sorted[$((count-1))]}

    echo "$mean $median $stddev $min $max"
}

# Test butterfly-route
test_butterfly() {
    local name=$1
    local from_lat=$2
    local from_lon=$3
    local to_lat=$4
    local to_lon=$5

    local times=()
    local distances=()
    local durations=()
    local nodes_visited=()
    local success_count=0

    for ((i=1; i<=$NUM_RUNS; i++)); do
        log_verbose "  Run $i/$NUM_RUNS..."

        local start=$(date +%s%N)
        local result
        if result=$("$BUTTERFLY_BIN" route "$BUTTERFLY_GRAPH" \
            --from "$from_lat,$from_lon" \
            --to "$to_lat,$to_lon" 2>&1); then
            local end=$(date +%s%N)
            local elapsed=$(echo "scale=6; ($end - $start) / 1000000000" | bc -l)

            # Parse results
            local distance=$(echo "$result" | grep "Distance:" | awk '{print $2}' | tr -d 'm')
            local duration=$(echo "$result" | grep "Time:" | awk '{print $2}' | tr -d 'minutes')
            local nodes=$(echo "$result" | grep "Nodes visited:" | awk '{print $3}')

            if [[ -n "$distance" && -n "$duration" && -n "$nodes" ]]; then
                times+=("$elapsed")
                distances+=("$distance")
                durations+=("$duration")
                nodes_visited+=("$nodes")
                ((success_count++))
            else
                log_warning "Failed to parse butterfly-route output for $name run $i"
            fi
        else
            log_warning "butterfly-route query failed for $name run $i"
        fi
    done

    if [[ $success_count -eq 0 ]]; then
        echo "FAILED 0 0 0 0 0 0"
        return 1
    fi

    # Calculate statistics
    local time_stats=($(calc_stats "${times[@]}"))
    local dist_stats=($(calc_stats "${distances[@]}"))
    local dur_stats=($(calc_stats "${durations[@]}"))
    local nodes_stats=($(calc_stats "${nodes_visited[@]}"))

    echo "SUCCESS ${time_stats[0]} ${time_stats[1]} ${time_stats[2]} ${dist_stats[0]} ${dur_stats[0]} ${nodes_stats[0]}"
}

# Test OSRM
test_osrm() {
    local name=$1
    local from_lat=$2
    local from_lon=$3
    local to_lat=$4
    local to_lon=$5

    if [[ $OSRM_AVAILABLE -eq 0 ]]; then
        echo "SKIPPED 0 0 0 0 0"
        return 0
    fi

    local times=()
    local distances=()
    local durations=()
    local success_count=0

    # OSRM uses lon,lat order (not lat,lon!)
    local url="$OSRM_URL/route/v1/driving/$from_lon,$from_lat;$to_lon,$to_lat?overview=false"

    for ((i=1; i<=$NUM_RUNS; i++)); do
        log_verbose "  Run $i/$NUM_RUNS..."

        local start=$(date +%s%N)
        local result
        if result=$(curl -s --max-time 10 "$url"); then
            local end=$(date +%s%N)
            local elapsed=$(echo "scale=6; ($end - $start) / 1000000000" | bc -l)

            # Parse OSRM response
            local code=$(echo "$result" | jq -r '.code')
            if [[ "$code" == "Ok" ]]; then
                local distance=$(echo "$result" | jq -r '.routes[0].distance')
                local duration=$(echo "$result" | jq -r '.routes[0].duration')

                times+=("$elapsed")
                distances+=("$distance")
                durations+=("$duration")
                ((success_count++))
            else
                log_warning "OSRM returned error for $name run $i: $code"
            fi
        else
            log_warning "OSRM query failed for $name run $i"
        fi
    done

    if [[ $success_count -eq 0 ]]; then
        echo "FAILED 0 0 0 0 0"
        return 1
    fi

    # Calculate statistics
    local time_stats=($(calc_stats "${times[@]}"))
    local dist_stats=($(calc_stats "${distances[@]}"))
    local dur_stats=($(calc_stats "${durations[@]}"))

    # Convert duration from seconds to minutes for comparison
    local dur_minutes=$(echo "scale=2; ${dur_stats[0]} / 60" | bc -l)

    echo "SUCCESS ${time_stats[0]} ${time_stats[1]} ${time_stats[2]} ${dist_stats[0]} $dur_minutes"
}

# Main test execution
echo -e "${BOLD}========================================${NC}"
echo -e "${BOLD}  butterfly-route vs OSRM Comparison${NC}"
echo -e "${BOLD}========================================${NC}"
echo ""
echo "Configuration:"
echo "  butterfly-route: $BUTTERFLY_BIN"
echo "  Graph file: $BUTTERFLY_GRAPH"
echo "  OSRM URL: $OSRM_URL"
echo "  Test runs per route: $NUM_RUNS"
echo "  OSRM available: $([ $OSRM_AVAILABLE -eq 1 ] && echo 'Yes' || echo 'No')"
echo ""

# Results storage
declare -A results

# Run tests
for route_def in "${TEST_ROUTES[@]}"; do
    IFS='|' read -r name from_lat from_lon to_lat to_lon description <<< "$route_def"

    echo -e "${BOLD}Testing: $name${NC}"
    echo "  Description: $description"
    echo "  Route: ($from_lat, $from_lon) → ($to_lat, $to_lon)"
    echo ""

    # Test butterfly-route
    echo -n "  butterfly-route: "
    butterfly_result=$(test_butterfly "$name" "$from_lat" "$from_lon" "$to_lat" "$to_lon")
    echo "$butterfly_result" | awk '{print $1}'
    results["${name}_butterfly"]="$butterfly_result"

    # Test OSRM
    echo -n "  OSRM: "
    osrm_result=$(test_osrm "$name" "$from_lat" "$from_lon" "$to_lat" "$to_lon")
    echo "$osrm_result" | awk '{print $1}'
    results["${name}_osrm"]="$osrm_result"

    echo ""
done

# Generate comparison report
echo -e "${BOLD}========================================${NC}"
echo -e "${BOLD}  Comparison Summary${NC}"
echo -e "${BOLD}========================================${NC}"
echo ""

printf "%-25s | %15s | %15s | %10s | %15s | %15s\n" \
    "Route" "Butterfly Time" "OSRM Time" "Speedup" "Butterfly Dist" "OSRM Dist"
printf "%.s-" {1..120}
echo ""

total_butterfly_time=0
total_osrm_time=0
test_count=0

for route_def in "${TEST_ROUTES[@]}"; do
    IFS='|' read -r name _ _ _ _ _ <<< "$route_def"

    butterfly_result="${results[${name}_butterfly]}"
    osrm_result="${results[${name}_osrm]}"

    IFS=' ' read -r b_status b_time_mean b_time_med b_time_std b_dist b_dur b_nodes <<< "$butterfly_result"
    IFS=' ' read -r o_status o_time_mean o_time_med o_time_std o_dist o_dur <<< "$osrm_result"

    if [[ "$b_status" == "SUCCESS" && "$o_status" == "SUCCESS" ]]; then
        speedup=$(echo "scale=1; $b_time_mean / $o_time_mean" | bc -l)
        dist_diff=$(echo "scale=1; ($o_dist - $b_dist) / $b_dist * 100" | bc -l)

        printf "%-25s | %12.3fs | %12.3fs | %8.1fx | %12.0fm | %12.0fm (+%.0f%%)\n" \
            "$name" "$b_time_mean" "$o_time_mean" "$speedup" "$b_dist" "$o_dist" "$dist_diff"

        total_butterfly_time=$(echo "$total_butterfly_time + $b_time_mean" | bc -l)
        total_osrm_time=$(echo "$total_osrm_time + $o_time_mean" | bc -l)
        ((test_count++))
    elif [[ "$b_status" == "SUCCESS" ]]; then
        printf "%-25s | %12.3fs | %15s | %10s | %12.0fm | %15s\n" \
            "$name" "$b_time_mean" "N/A" "N/A" "$b_dist" "N/A"
    else
        printf "%-25s | %15s | %15s | %10s | %15s | %15s\n" \
            "$name" "FAILED" "FAILED" "N/A" "N/A" "N/A"
    fi
done

if [[ $test_count -gt 0 ]]; then
    echo ""
    avg_butterfly=$(echo "scale=3; $total_butterfly_time / $test_count" | bc -l)
    avg_osrm=$(echo "scale=3; $total_osrm_time / $test_count" | bc -l)
    avg_speedup=$(echo "scale=1; $avg_butterfly / $avg_osrm" | bc -l)

    echo -e "${BOLD}Overall Statistics:${NC}"
    echo "  Average butterfly-route query time: ${avg_butterfly}s"
    echo "  Average OSRM query time: ${avg_osrm}s"
    echo "  Average speedup: ${avg_speedup}x (OSRM is faster)"
    echo "  Successful tests: $test_count/${#TEST_ROUTES[@]}"
fi

# Save to JSON if requested
if [[ -n "$OUTPUT_FILE" ]]; then
    log_info "Saving results to $OUTPUT_FILE..."

    echo "{" > "$OUTPUT_FILE"
    echo "  \"timestamp\": \"$(date -u +%Y-%m-%dT%H:%M:%SZ)\"," >> "$OUTPUT_FILE"
    echo "  \"configuration\": {" >> "$OUTPUT_FILE"
    echo "    \"butterfly_bin\": \"$BUTTERFLY_BIN\"," >> "$OUTPUT_FILE"
    echo "    \"butterfly_graph\": \"$BUTTERFLY_GRAPH\"," >> "$OUTPUT_FILE"
    echo "    \"osrm_url\": \"$OSRM_URL\"," >> "$OUTPUT_FILE"
    echo "    \"num_runs\": $NUM_RUNS" >> "$OUTPUT_FILE"
    echo "  }," >> "$OUTPUT_FILE"
    echo "  \"results\": {" >> "$OUTPUT_FILE"

    first=1
    for route_def in "${TEST_ROUTES[@]}"; do
        IFS='|' read -r name from_lat from_lon to_lat to_lon description <<< "$route_def"

        [[ $first -eq 0 ]] && echo "," >> "$OUTPUT_FILE"
        first=0

        butterfly_result="${results[${name}_butterfly]}"
        osrm_result="${results[${name}_osrm]}"

        IFS=' ' read -r b_status b_time_mean b_time_med b_time_std b_dist b_dur b_nodes <<< "$butterfly_result"
        IFS=' ' read -r o_status o_time_mean o_time_med o_time_std o_dist o_dur <<< "$osrm_result"

        echo "    \"$name\": {" >> "$OUTPUT_FILE"
        echo "      \"description\": \"$description\"," >> "$OUTPUT_FILE"
        echo "      \"coordinates\": {" >> "$OUTPUT_FILE"
        echo "        \"from\": {\"lat\": $from_lat, \"lon\": $from_lon}," >> "$OUTPUT_FILE"
        echo "        \"to\": {\"lat\": $to_lat, \"lon\": $to_lon}" >> "$OUTPUT_FILE"
        echo "      }," >> "$OUTPUT_FILE"
        echo "      \"butterfly\": {" >> "$OUTPUT_FILE"
        echo "        \"status\": \"$b_status\"," >> "$OUTPUT_FILE"
        echo "        \"time_mean\": $b_time_mean," >> "$OUTPUT_FILE"
        echo "        \"time_median\": $b_time_med," >> "$OUTPUT_FILE"
        echo "        \"time_stddev\": $b_time_std," >> "$OUTPUT_FILE"
        echo "        \"distance\": $b_dist," >> "$OUTPUT_FILE"
        echo "        \"duration\": $b_dur," >> "$OUTPUT_FILE"
        echo "        \"nodes_visited\": $b_nodes" >> "$OUTPUT_FILE"
        echo "      }," >> "$OUTPUT_FILE"
        echo "      \"osrm\": {" >> "$OUTPUT_FILE"
        echo "        \"status\": \"$o_status\"," >> "$OUTPUT_FILE"
        echo "        \"time_mean\": $o_time_mean," >> "$OUTPUT_FILE"
        echo "        \"time_median\": $o_time_med," >> "$OUTPUT_FILE"
        echo "        \"time_stddev\": $o_time_std," >> "$OUTPUT_FILE"
        echo "        \"distance\": $o_dist," >> "$OUTPUT_FILE"
        echo "        \"duration\": $o_dur" >> "$OUTPUT_FILE"
        echo "      }" >> "$OUTPUT_FILE"
        echo -n "    }" >> "$OUTPUT_FILE"
    done

    echo "" >> "$OUTPUT_FILE"
    echo "  }" >> "$OUTPUT_FILE"
    echo "}" >> "$OUTPUT_FILE"

    log_success "Results saved to $OUTPUT_FILE"
fi

echo ""
log_success "Test suite completed!"
