# Build & run

(Part of the [engineering guide](../ENGINEERING.md).)

```bash
cargo build --workspace [--release]                       # all three crates
cargo test  --workspace                                   # unit tests, no data needed
cargo clippy --workspace --all-targets --all-features     # warnings are errors
cargo fmt --all
cargo build --release --features bench --bin butterfly-bench  # benchmark harness (bench feature)
./target/release/butterfly-bench bucket-m2m --data-dir ./data/belgium --sizes 10,25,50,100

docker build -t butterfly-route .                   # serving image (default target)
docker build --target tools -t butterfly-tools .    # pipeline / fetch image
docker run -d --name butterfly -p 3001:8080 -p 3002:8081 \
  -v "${PWD}/data/belgium:/data" butterfly-route && curl localhost:3001/health
```

There is no protobuf build dependency: the GTFS-Realtime bindings are generated
ahead of time and committed (`route/src/transit/gtfs_realtime.rs`, #574).
`protoc` is needed only by `scripts/gen-gtfs-rt.sh`, run by hand when the
(frozen) spec moves. The toolchain is pinned by `rust-version` in the root
`Cargo.toml` (edition 2024).

One multi-target image manifest (#573): `Dockerfile` holds a shared `builder`
stage plus a `tools` and a `runtime` stage, `runtime` last so a bare
`docker build .` still produces the serving image. `Dockerfile.tools` is a
deprecated generated shim (`scripts/gen-dockerfile-tools.sh`, checked by CI)
kept for one release. Release binaries are stripped
(`[profile.release] strip = "symbols"`); build `--profile release-debug` when
you need named backtrace frames. Multi-stage image
(`rust:1.95-trixie` → `debian:trixie-slim`, non-root); default
`CMD` is `serve --data-dir /data --port 8080 --log-format json` — REST on 8080,
Arrow Flight on 8081. `serve` flags that matter:

| Flag | Meaning |
|---|---|
| `--data <f.butterfly>` / `--data-dir <dir>` | one packed container (mmap) vs a directory of containers / step outputs |
| `--port`, `--grpc-port`, `--transport rest\|grpc\|both` | listeners; gRPC defaults to REST port + 1 |
| `--modes car,bike,foot,truck`, `--transit on\|off` | load a subset (each mode costs RAM); skip all feed and transfer-graph work |
| `--regions BE,LU`, `--eager-regions` | multi-region selection; loading is lazy by default |
| `--rss-budget-gb`, `--idle-compact-secs` | LRU region eviction, per-worker scratch compaction |
| `--eager-verify` / `--warmup-on-boot` | section CRC verification at boot vs in background |
| `--overlay <file>`, `--log-format text\|json` | inter-region P2P overlay; structured logging |

Env overrides worth knowing: `RUST_LOG`, `BUTTERFLY_TRANSIT`,
`BUTTERFLY_RSS_BUDGET_GB`, `BUTTERFLY_IDLE_COMPACT_SECS`, `BUTTERFLY_REFS_DIR`,
`BUTTERFLY_MODELS_DIR`, `BUTTERFLY_MATRIX_ALGO=bucket|phast` (read ONCE at the
first matrix call and then frozen for the life of the process — set it before
`serve` starts, it cannot be flipped on a live server; #546, #591),
`BUTTERFLY_BOOT_RECUSTOMIZE=off` (uncalibrated free-flow car).
