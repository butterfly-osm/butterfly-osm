# nominatim bench results

Queries: 1000

## concurrency=1
- throughput: 56.4 qps
- recall@1 (100 m): 0.864
- latency p50 / p95 / p99: 17.1 / 27.9 / 54.5 ms
- distance p50 / p95: 0.0 / 50.636661270628736 m

## concurrency=4
- throughput: 246.2 qps
- recall@1 (100 m): 0.864
- latency p50 / p95 / p99: 14.8 / 27.0 / 43.6 ms
- distance p50 / p95: 0.0 / 50.636661270628736 m

## concurrency=16
- throughput: 354.1 qps
- recall@1 (100 m): 0.864
- latency p50 / p95 / p99: 31.2 / 118.4 / 165.6 ms
- distance p50 / p95: 0.0 / 50.636661270628736 m
