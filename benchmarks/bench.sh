#!/bin/bash
# Comprehensive benchmark: butterfly-dl vs curl vs aria2
# Usage: ./bench.sh <country>
# Example: ./bench.sh monaco

set -e
export LC_NUMERIC=C

if [[ $# -ne 1 ]]; then
    echo "Usage: $0 <country>"
    echo "Examples:"
    echo "  $0 monaco"
    echo "  $0 europe/belgium"
    echo "  $0 europe/france"
    exit 1
fi

COUNTRY="$1"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"
DOWNLOAD_DIR="$SCRIPT_DIR"

# Check tool availability
echo "=== Tool Availability Check ==="
AVAILABLE_TOOLS=()

# Check curl
if command -v curl &> /dev/null; then
    echo "✅ curl: $(curl --version | head -n1 | awk '{print $2}')"
    AVAILABLE_TOOLS+=("curl")
else
    echo "❌ curl: not found"
fi

# Check aria2
if command -v aria2c &> /dev/null; then
    echo "✅ aria2: $(aria2c --version | head -n1 | awk '{print $3}')"
    AVAILABLE_TOOLS+=("aria2")
else
    echo "❌ aria2: not found"
fi

# Build butterfly-dl
echo "🔨 butterfly-dl: building..."
cd "$PROJECT_ROOT"
if cargo build --release --quiet; then
    echo "✅ butterfly-dl: built successfully"
    AVAILABLE_TOOLS+=("butterfly")
else
    echo "❌ butterfly-dl: build failed"
    exit 1
fi

echo ""
echo "=== Benchmarking: $COUNTRY ==="
echo "Tools to test: ${AVAILABLE_TOOLS[*]}"
echo ""

# Clean up any existing files
SAFE_NAME=$(echo "$COUNTRY" | tr '/' '_')
for tool in "${AVAILABLE_TOOLS[@]}"; do
    rm -f "$DOWNLOAD_DIR/${SAFE_NAME}_${tool}.pbf"
done

# Function to calculate optimal connections (same logic as butterfly-dl)
calculate_connections() {
    local file_size=$1
    local max_connections=${2:-16}
    
    if (( file_size <= 1024 * 1024 )); then
        echo 1    # <= 1MB: single connection
    elif (( file_size <= 10 * 1024 * 1024 )); then
        echo 2    # <= 10MB: 2 connections  
    elif (( file_size <= 100 * 1024 * 1024 )); then
        echo 4    # <= 100MB: 4 connections
    elif (( file_size <= 512 * 1024 * 1024 )); then
        echo 8    # <= 512MB: 8 connections
    elif (( file_size <= 1024 * 1024 * 1024 )); then
        echo 12   # <= 1GB: 12 connections
    else
        echo 16   # > 1GB: 16 connections
    fi
}

# Function to test a single tool
test_tool() {
    local tool="$1"
    local output_file="$DOWNLOAD_DIR/${SAFE_NAME}_${tool}.pbf"
    
    echo "Testing $tool..."
    
    # Remove any existing file
    rm -f "$output_file"
    
    # Time the download
    local start_time=$(date +%s.%N)
    
    case $tool in
        "butterfly")
            "$PROJECT_ROOT/target/release/butterfly-dl" "$COUNTRY" "$output_file"
            ;;
        "curl")
            local url="https://download.geofabrik.de/${COUNTRY}-latest.osm.pbf"
            curl --progress-bar -L -o "$output_file" "$url"
            ;;
        "aria2")
            local url="https://download.geofabrik.de/${COUNTRY}-latest.osm.pbf"
            # Get file size to calculate optimal connections
            local file_size=$(curl -sI "$url" | grep -i content-length | awk '{print $2}' | tr -d '\r')
            local connections=1
            if [[ -n "$file_size" && "$file_size" -gt 0 ]]; then
                connections=$(calculate_connections "$file_size")
            fi
            aria2c --summary-interval=0 --download-result=hide --split="$connections" --max-connection-per-server="$connections" -o "$(basename "$output_file")" -d "$(dirname "$output_file")" "$url"
            ;;
    esac
    
    local end_time=$(date +%s.%N)
    local duration=$(echo "$end_time - $start_time" | bc -l)
    
    if [[ -f "$output_file" ]]; then
        local file_size=$(stat -c%s "$output_file")
        local file_size_mb=$(echo "scale=2; $file_size / 1024 / 1024" | bc -l)
        local speed=$(echo "scale=2; $file_size_mb / $duration" | bc -l)
        
        echo "  Duration: ${duration}s"
        echo "  Size: ${file_size_mb} MB"
        echo "  Speed: ${speed} MB/s"
        
        # Store results for comparison
        eval "${tool}_duration=$duration"
        eval "${tool}_speed=$speed" 
        eval "${tool}_size=$file_size_mb"
        echo "  ✅ Success"
    else
        echo "  ❌ FAILED"
        eval "${tool}_duration=0"
        eval "${tool}_speed=0"
        eval "${tool}_size=0"
    fi
    echo ""
}

# Test all available tools
for tool in "${AVAILABLE_TOOLS[@]}"; do
    test_tool "$tool"
done

echo ""
echo "=== COMPARISON ==="

# Find the fastest tool (minimum duration)
fastest_tool=""
fastest_duration=999999
for tool in "${AVAILABLE_TOOLS[@]}"; do
    duration_var="${tool}_duration"
    duration=${!duration_var}
    if [[ "$duration" != "0" ]] && (( $(echo "$duration < $fastest_duration" | bc -l) )); then
        fastest_duration=$duration
        fastest_tool=$tool
    fi
done

if [[ -n "$fastest_tool" ]]; then
    echo "🏆 Fastest tool: $fastest_tool (${fastest_duration}s)"
    echo ""
    
    # Compare all tools against the fastest
    for tool in "${AVAILABLE_TOOLS[@]}"; do
        if [[ "$tool" == "$fastest_tool" ]]; then
            continue
        fi
        
        duration_var="${tool}_duration"
        speed_var="${tool}_speed"
        duration=${!duration_var}
        speed=${!speed_var}
        
        if [[ "$duration" != "0" ]]; then
            if (( $(echo "$duration > $fastest_duration" | bc -l) )); then
                slower=$(echo "scale=1; ($duration - $fastest_duration) / $fastest_duration * 100" | bc -l)
                echo "🐌 $tool is ${slower}% slower than $fastest_tool"
            else
                faster=$(echo "scale=1; ($fastest_duration - $duration) / $fastest_duration * 100" | bc -l)
                echo "⚡ $tool is ${faster}% faster than $fastest_tool"
            fi
            
            ratio=$(echo "scale=2; $duration / $fastest_duration" | bc -l)
            echo "   Time ratio ($tool/$fastest_tool): ${ratio}x"
        else
            echo "❌ $tool failed"
        fi
        echo ""
    done
else
    echo "❌ No tools completed successfully"
fi

# Performance summary table
echo "=== PERFORMANCE SUMMARY ==="
printf "%-12s %-12s %-12s %-10s\n" "Tool" "Duration(s)" "Speed(MB/s)" "Status"
echo "--------------------------------------------------------"
for tool in "${AVAILABLE_TOOLS[@]}"; do
    duration_var="${tool}_duration"
    speed_var="${tool}_speed"
    duration=${!duration_var}
    speed=${!speed_var}
    
    if [[ "$duration" != "0" ]]; then
        printf "%-12s %-12.3f %-12.2f %-10s\n" "$tool" "$duration" "$speed" "✅ Success"
    else
        printf "%-12s %-12s %-12s %-10s\n" "$tool" "FAILED" "-" "❌ Failed"
    fi
done

echo ""

# Validate file integrity with MD5 checksums
echo "=== FILE INTEGRITY VALIDATION ==="
declare -A file_md5s
all_files_valid=true

for tool in "${AVAILABLE_TOOLS[@]}"; do
    output_file="$DOWNLOAD_DIR/${SAFE_NAME}_${tool}.pbf"
    if [[ -f "$output_file" ]]; then
        md5=$(md5sum "$output_file" | awk '{print $1}')
        file_md5s["$tool"]="$md5"
        echo "📋 $tool: $md5"
    else
        echo "❌ $tool: file not found"
        all_files_valid=false
    fi
done

# Check if all MD5s are the same
if [[ ${#file_md5s[@]} -gt 1 ]]; then
    first_md5=""
    all_match=true
    
    for tool in "${!file_md5s[@]}"; do
        if [[ -z "$first_md5" ]]; then
            first_md5="${file_md5s[$tool]}"
        elif [[ "${file_md5s[$tool]}" != "$first_md5" ]]; then
            all_match=false
            break
        fi
    done
    
    if [[ "$all_match" == true ]]; then
        echo "✅ All files have matching MD5 checksums"
    else
        echo "❌ MD5 checksums do not match - file corruption detected!"
        all_files_valid=false
        for tool in "${!file_md5s[@]}"; do
            echo "   $tool: ${file_md5s[$tool]}"
        done
    fi
elif [[ ${#file_md5s[@]} -eq 1 ]]; then
    echo "ℹ️  Only one file to validate"
else
    echo "❌ No files found for validation"
    all_files_valid=false
fi

echo ""

# Clean up test files
echo "Cleaning up test files..."
for tool in "${AVAILABLE_TOOLS[@]}"; do
    rm -f "$DOWNLOAD_DIR/${SAFE_NAME}_${tool}.pbf"
done
echo "✅ Test files removed"

if [[ "$all_files_valid" == false ]]; then
    echo ""
    echo "⚠️  WARNING: File integrity validation failed!"
    exit 1
fi