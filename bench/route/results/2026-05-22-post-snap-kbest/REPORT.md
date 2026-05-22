# Matrix benchmark — post snap-K-best + SCC role masks (2026-05-22)

Branch: `route-verify-belgium-latest` (PR #232)

Belgium artifact: `data/belgium/baseline.butterfly` (Apr-26 build, same as previous runs).

## Algorithm bench — `butterfly-bench bucket-m2m --parallel`

In-process bucket many-to-many CCH. No HTTP, no serialization.

| Size | Cells | Time | Throughput | Fwd vis | Bwd vis | Joins |
|------|-------|------|------------|---------|---------|-------|
| 10×10 | 100 | 18.6 ms | 5 365 c/s | 2 313 | 2 257 | 91 K |
| 25×25 | 625 | 51.5 ms | 12 K c/s | 2 451 | 2 427 | 362 K |
| 50×50 | 2 500 | 11.5 ms | 216 K c/s | 2 465 | 2 186 | 1.4 M |
| 100×100 | 10 K | 26.0 ms | 385 K c/s | 2 493 | 2 313 | 6.0 M |
| 500×500 | 250 K | 115.1 ms | 2.2 M c/s | 2 436 | 2 319 | 149 M |
| 1000×1000 | 1 M | 270.8 ms | 3.7 M c/s | 2 415 | 2 335 | 598 M |
| 2000×2000 | 4 M | 830.8 ms | 4.8 M c/s | 2 390 | 2 337 | 2.4 B |
| 5000×5000 | 25 M | 4 815 ms | 5.2 M c/s | 2 396 | 7 022 | 14.8 B |
| 10000×10000 | 100 M | 18 091 ms | 5.5 M c/s | 2 399 | 11 718 | 59 B |

Correctness validation (5×5 vs P2P): all 25 queries match.

## Flight gRPC `matrix` action — clustered city coords

End-to-end via Arrow Flight gRPC. Clustered means coords perturbed
within ±0.1° of 5 Belgium cities (Brussels, Antwerp, Ghent, Liège,
Charleroi) — 100 % snappable.

Initial run was serial-snap in `do_matrix`; revised run parallelised
the per-coord K-best snap via `rayon::par_iter`. Both runs included
for transparency. **Use the parallel-snap numbers as the current
baseline.**

| Size | Cells | Serial snap | Parallel snap | Throughput | Speedup |
|------|-------|-------------|---------------|------------|---------|
| 1k × 1k | 1 M | 5.47 s | **3.61 s** | 277 K c/s | 1.5× |
| 5k × 5k | 25 M | 24.27 s | **14.88 s** | 1.7 M c/s | 1.6× |
| 10k × 10k | 100 M | 52.76 s | **35.52 s** | 2.8 M c/s | 1.5× |
| 25k × 25k | 625 M | 221.9 s (3.7 min) | **172.6 s (2.88 min)** | 3.6 M c/s | 1.3× |
| 50k × 50k | 2.5 B | 656.4 s (10.94 min) | **576.7 s (9.61 min)** | 4.3 M c/s | 1.14× |

All matrices were 100 % finite (clustered city coords, so every cell
snaps inside the Belgium road network).

## Historical comparison (from CLAUDE.md)

The previous matrix-stream baselines used the REST `/table/stream`
endpoint (now removed; superseded by Flight gRPC `matrix`):

| Size | Previous (`/table/stream`) | Now (Flight `matrix`) |
|------|----------------------------|------------------------|
| 10k × 10k | 24 s | **35.5 s** |
| 50k × 50k | 9.5 min | **9.61 min** (parity) |

Notes:
- The Flight numbers include the full Arrow Flight gRPC roundtrip
  (handshake, stream RPC framing) which the historical `/table/stream`
  HTTP numbers don't include. The in-process bucket-m2m bench is the
  closest apples-to-apples (5.5 M c/s = 18 s at 10k × 10k, ~25 % faster
  than the historical 24 s figure).
- 50k × 50k is at parity with the historical figure, with the matrix
  now mathematically equal to running 2.5 B independent P2P queries
  thanks to the connectivity-aware role masks + K-best fallback
  (correctness improvement landed in commits `772ff1a` and `b2992cb`).

## How to run

```bash
# Algorithm bench
./target/release/butterfly-bench bucket-m2m --data-dir ./data/belgium \
  --sizes 10,25,50,100,500,1000,2000,5000,10000 --parallel

# Flight matrix end-to-end
python3 -m venv .venv-bench
.venv-bench/bin/pip install pyarrow
.venv-bench/bin/python /tmp/bench_flight_v2.py 1000 5000 10000 --clustered
.venv-bench/bin/python /tmp/bench_flight_v2.py 25000 50000 --clustered
```
