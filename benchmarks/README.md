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
$ ./benchmarks/bench.sh monaco

Building butterfly-dl...

=== Benchmarking: monaco ===

Testing butterfly-dl...
📁 Saving to: ./benchmarks/monaco_butterfly.pbf
🌐 Downloading monaco
  Duration: 0.234s
  Size: 1.23 MB
  Speed: 5.26 MB/s

Testing curl...
  Duration: 0.198s
  Size: 1.23 MB
  Speed: 6.21 MB/s

=== COMPARISON ===
🐌 Butterfly-dl is 18.2% slower
📉 Butterfly-dl has 15.3% lower throughput
📊 Time ratio (curl/butterfly): 0.85x

Files saved in: ./benchmarks
  monaco_butterfly.pbf
  monaco_curl.pbf
```

## Expected Results

- **Small files** (Monaco): curl may be faster due to lower overhead
- **Medium files** (Belgium): competitive performance
- **Large files** (France): butterfly-dl should excel with parallel connections

## Notes

- Downloads are saved to `./benchmarks/` directory
- Both tools download the same file for fair comparison
- Network conditions affect results
- Files are kept for verification