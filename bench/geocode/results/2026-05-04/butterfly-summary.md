# butterfly bench results

Queries: 1000

## concurrency=1
- throughput: 23.5 qps
- recall@1 (100 m): 0.470
- latency p50 / p95 / p99: 7.2 / 127.3 / 1008.5 ms
- distance p50 / p95: 66.90412825170186 / 4439.454309297506 m

## concurrency=4
- throughput: 25.1 qps
- recall@1 (100 m): 0.470
- latency p50 / p95 / p99: 2.7 / 1009.3 / 1014.5 ms
- distance p50 / p95: 66.90412825170186 / 4439.454309297506 m

## concurrency=16
- throughput: 25.1 qps
- recall@1 (100 m): 0.470
- latency p50 / p95 / p99: 1004.0 / 2025.7 / 3032.0 ms
- distance p50 / p95: 66.90412825170186 / 4439.454309297506 m
