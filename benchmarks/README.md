# Butterfly-dl Benchmarks

Comprehensive performance benchmarks comparing butterfly-dl against curl and aria2.

## Usage

```bash
# Benchmark any supported region
./benchmarks/bench.sh <country>

# Examples
./benchmarks/bench.sh europe/monaco       # Small file (~1 MB)
./benchmarks/bench.sh europe/belgium      # Medium file (~43 MB)  
./benchmarks/bench.sh europe/france       # Large file (~3.5 GB)
```

## Tools Compared

- **butterfly-dl** - Our optimized downloader with smart connection strategy
- **curl** - Standard HTTP client (if available)
- **aria2** - Multi-connection download accelerator (if available)

The script automatically detects which tools are available and tests only those.

## What it measures

- **Tool availability** check
- **Download duration** (seconds)
- **Download speed** (MB/s)
- **File size verification** (MB)
- **Performance comparison** against fastest tool
- **MD5 checksum validation** for file integrity
- **Summary table** with all results

## Sample Output

```bash
$ ./benchmarks/bench.sh europe/monaco

=== Tool Availability Check ===
✅ curl: 7.81.0
✅ aria2: 1.36.0
🔨 butterfly-dl: building...
✅ butterfly-dl: built successfully

=== Benchmarking: europe/monaco ===
Tools to test: curl aria2 butterfly

Testing curl...
######################################################################## 100.0%
  Duration: 0.337s
  Size: 0.61 MB
  Speed: 1.80 MB/s
  ✅ Success

Testing aria2...
  Duration: 0.291s
  Size: 0.61 MB
  Speed: 2.09 MB/s
  ✅ Success

Testing butterfly...
📁 Saving to: ./benchmarks/europe_monaco_butterfly.pbf
🌐 Downloading europe/monaco
  Duration: 0.421s
  Size: 0.61 MB
  Speed: 1.44 MB/s
  ✅ Success

=== COMPARISON ===
🏆 Fastest tool: aria2 (0.291s)

🐌 curl is 15.8% slower than aria2
   Time ratio (curl/aria2): 1.16x

🐌 butterfly is 44.7% slower than aria2
   Time ratio (butterfly/aria2): 1.45x

=== PERFORMANCE SUMMARY ===
Tool         Duration(s)  Speed(MB/s)  Status    
--------------------------------------------------------
curl         0.337        1.80         ✅ Success
aria2        0.291        2.09         ✅ Success
butterfly    0.421        1.44         ✅ Success

=== FILE INTEGRITY VALIDATION ===
📋 curl: a1b2c3d4e5f6789012345678901234567890abcd
📋 aria2: a1b2c3d4e5f6789012345678901234567890abcd
📋 butterfly: a1b2c3d4e5f6789012345678901234567890abcd
✅ All files have matching MD5 checksums

Cleaning up test files...
✅ Test files removed
```

## Expected Results

- **Small files** (Monaco): aria2 typically fastest, curl competitive, butterfly-dl improving
- **Medium files** (Belgium): butterfly-dl should be competitive with parallel connections
- **Large files** (France): butterfly-dl should excel with optimized parallel strategy

## Notes

- **Automatic tool detection** - only tests available tools
- **Fair comparison** - all tools download the same file
- **Clean benchmarking** - temporary files automatically removed
- **Progress indication** - each tool shows appropriate progress feedback
- **Network dependent** - results vary with connection and server load
- **Multiple runs recommended** - network conditions can vary between tests