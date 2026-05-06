# Multi-country geocode bench

- Label: 2026-05-06-be-merged-shard
- Concurrency: 4, limit: 5, radius: 100.0m
- Butterfly: http://127.0.0.1:31100
- Nominatim: http://localhost:8080

## Per-country recall and latency

| ISO | n  | BF top1 | Nom top1 | BF top5 | Nom top5 | BF p50 (ms) | Nom p50 (ms) |
|:----|:---|:--------|:---------|:--------|:---------|:------------|:-------------|
| BE | 1000 | 0.388 | 0.830 | 0.453 | 0.848 | 1.5 | 29.2 |

**Mean BF top1 across countries: 0.388**
**Mean BF top5 across countries: 0.453**
**Mean Nom top1 across countries: 0.830**
**Mean Nom top5 across countries: 0.848**
