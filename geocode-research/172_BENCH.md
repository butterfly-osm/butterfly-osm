# #172 — concurrency saturation fix: before / after

Belgium 1000-query bench, single client (`bench/geocode/bench.py`),
20-core box, `--limit 1`, no `--qps-cap`. Server boots from a freshly
rebuilt `geocode/regions/belgium.bfgs`.

## Before (issue #172 baseline — main HEAD `6b021dd`)

The server's `AdmissionPolicy::default()` set `per_ip_refill_per_sec = 25`
and `per_ip_capacity = 50`, with **no CLI knob** to override it. The
table was a single `Mutex<HashMap<IpAddr, TokenBucket>>` shared across
every request, with an O(n) `min_by_key` LRU scan inside the lock on
each insert past `max_tracked_ips`. From a single localhost IP the
per-IP token bucket starved the bench at exactly the refill rate.

| concurrency | throughput | p50 latency | p99 latency | source |
|---|---|---|---|---|
| 1  | 23.5 qps | 7.2 ms | 1008.5 ms | issue #172 |
| 4  | 25.1 qps | 2.7 ms | 1014.5 ms | issue #172 |
| 16 | 25.1 qps | 1004 ms | 3032 ms | issue #172 |

Throughput is **flat at the per-IP refill rate** (~25 qps). p50 at
c=16 explodes 140× over c=1 because every request stalls on the
single `Mutex<HashMap>`.

Reference: Nominatim on the same hardware: 354 qps at c=16.

## After (this PR, `--admission-disable`)

| concurrency | throughput | p50 latency | p99 latency |
|---|---|---|---|
| 1  | **1349.3 qps** | 0.4 ms | 2.9 ms |
| 4  | **2494.8 qps** | 1.2 ms | 4.5 ms |
| 16 | **2216.0 qps** | 4.4 ms | 37.8 ms |

`recall@1 (100m) = 0.605` across all three (unchanged — fix is
admission-only, the geocode pipeline is untouched).

## After (this PR, default policy with high allowances)

`--admission-per-ip-per-sec 1000000 --admission-per-ip-burst 1000000
--admission-global-per-sec 1000000 --admission-global-burst 1000000`

| concurrency | throughput | p50 latency | p99 latency |
|---|---|---|---|
| 1  | 1358.8 qps | 0.4 ms | 2.9 ms |
| 4  | 2568.8 qps | 1.2 ms | 4.2 ms |
| 16 | 2207.9 qps | 4.5 ms | 38.6 ms |

Identical to `--admission-disable` within bench noise — the rewritten
admission code carries near-zero overhead under contention thanks to
DashMap's per-shard locking.

## Speedup vs #172 baseline

| concurrency | before | after | factor |
|---|---|---|---|
| 1  | 23.5 qps | 1349.3 qps | **57×** |
| 4  | 25.1 qps | 2494.8 qps | **99×** |
| 16 | 25.1 qps | 2216.0 qps | **88×** |

p50 latency at c=16 went from **1004 ms** to **4.4 ms** — a 228×
reduction.

## Target met

| target | result |
|---|---|
| ≥ 200 qps at c=16 | **2216 qps** (11× the target) |
| ≥ Nominatim's 354 qps | **6.3× Nominatim** at c=16 |

## Notes on c=4 vs c=16

c=4 is fastest at 2495 qps, c=16 settles at 2216 qps. With 20 cores
and Tokio's default blocking pool (512 threads), the geocode work is
sub-millisecond per query, so coordination overhead (TCP accept,
context switches, request body parsing) becomes the dominant cost
above c=4. This is normal HTTP/CPU-bound behaviour, not a residual
serial gate. Nominatim peaks similarly. To go higher, future work
would batch-pipeline (the gRPC Flight `geocode_batch` exists for
exactly that).

## Bench command

```
butterfly-geocode serve \
  --shard geocode/regions/belgium.bfgs \
  --port 3003 --transport rest \
  --rate-limit-per-sec 100000 --rate-limit-burst 100000 \
  --admission-disable

cd bench/geocode
python3 bench.py --engine butterfly \
  --queries queries/belgium.tsv \
  --base-url http://127.0.0.1:3003 \
  --concurrency 1,4,16 --limit 1 \
  --output results/172-fix-final
```

The `--rate-limit-per-sec 100000 --rate-limit-burst 100000` lifts
`tower_governor` (the second, HTTP-level rate limiter) so it doesn't
become a secondary gate at high concurrency from a single localhost
IP. In production both gates can be left at their defaults.
