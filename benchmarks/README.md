# Butterfly-dl Benchmarks

Simple performance benchmarks comparing butterfly-dl against curl.

## Usage

```bash
# Benchmark any supported region
./benchmarks/bench.sh <country>

# Examples
./benchmarks/bench.sh monaco              # Small file (~1 MB)
./benchmarks/bench.sh europe/belgium      # Medium file (~43 MB)  
./benchmarks/bench.sh europe/france       # Large file (~3.5 GB)
```

## What it measures

- **Download duration** (seconds)
- **Download speed** (MB/s)
- **File size** (MB)
- **Performance comparison** (faster/slower %)

## Sample Output

```bash
$ ./benchmarks/bench.sh europe/monaco

Building butterfly-dl...

=== Benchmarking: europe/monaco ===

Testing butterfly-dl...
📁 Saving to: ./benchmarks/europe_monaco_butterfly.pbf
🌐 Downloading europe/monaco
  Duration: 0.421s
  Size: 0.61 MB
  Speed: 1.44 MB/s

Testing curl...
######################################################################## 100.0%
  Duration: 0.337s
  Size: 0.61 MB
  Speed: 1.80 MB/s

=== COMPARISON ===
🐌 Butterfly-dl is 20.0% slower
📉 Butterfly-dl has 20.0% lower throughput
📊 Time ratio (curl/butterfly): 0.80x

Cleaning up test files...
✅ Test files removed
```

## Expected Results

- **Small files** (Monaco): curl may be faster due to lower overhead
- **Medium files** (Belgium): competitive performance
- **Large files** (France): butterfly-dl should excel with parallel connections

## Notes

- Downloads are temporarily saved to `./benchmarks/` directory
- Both tools download the same file for fair comparison
- Network conditions affect results
- Test files are automatically cleaned up after benchmarking
- curl shows progress bar during download