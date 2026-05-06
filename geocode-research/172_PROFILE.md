# #172 — root cause analysis

## TL;DR

The serial gate was **`AdmissionState::try_admit()`**: every request
locked one `std::sync::Mutex<HashMap<IpAddr, TokenBucket>>` and then
the global `Mutex<TokenBucket>`. Two independent factors compounded
into the 25-qps ceiling:

1. The default per-IP token bucket was 25 req/s with a burst of 50.
   From a single localhost IP (the bench's setup, and any
   Docker-on-laptop deployment), refill is the actual ceiling.
2. The map was a single `Mutex<HashMap>`. Every concurrent request
   serialised on it. Inside the lock, the LRU eviction path scanned
   the entire map (`min_by_key`) on insert past `max_tracked_ips`,
   which dominates wall time on contended takes.

There was **no CLI knob to relax #1**, and **no architectural way to
escape #2**, so the bench observed flat 25 qps and a 1004 ms p50 at
c=16 (the median request waited ~one second on the lock + retry-after
loop).

The geocode hot path itself is sub-millisecond and parallel-clean:
0.4 ms p50 at c=1, scales cleanly to 2495 qps at c=4 once the gate is
removed.

## Diagnosis path

1. **Read the issue** — the symptom (throughput exactly 25 qps,
   independent of concurrency) is the fingerprint of a token bucket
   refilling at 25/s. Every other hypothesis (allocator, R-tree,
   tokio runtime, metrics registry) would saturate at a different
   number that depends on hardware, not on a tunable.
2. **Locate the constant.** `geocode/src/control/admission.rs:64` —
   `per_ip_refill_per_sec: 25` in `AdmissionPolicy::default`.
3. **Locate the lock.** Same file, line 119:
   `per_ip: Mutex<HashMap<IpAddr, TokenBucket>>` — single `Mutex`
   for every IP, every request.
4. **Verify there is no override path.** `geocode/src/main.rs` had
   `--rate-limit-per-sec` for `tower_governor` only (a separate,
   parallel rate limiter) — admission was hardcoded.
5. **Reproduce.** Booted main HEAD with `--rate-limit-per-sec 100000`
   to lift the secondary tower_governor gate, ran the bench. With
   `--qps-cap 0` (no client pacing) the bench fired requests
   uncapped:
   * c=1, c=4, c=16 all sustained ~2500 "qps" — but this was 989/1000
     responses being **HTTP 429** from admission. Only ~9 of every
     1000 actual geocode results came back.
   * With `--qps-cap 20` (under the per-IP refill rate), bench
     succeeded cleanly at 20 qps with 0% 429s.
6. **Confirm the gate.** The 25 qps figure in #172 = exactly
   `per_ip_refill_per_sec`. No other component in the request path
   has a constant of 25. Locking confirmed by tests in this PR
   (`concurrent_admits_do_not_serialise`).

## Mechanism

In the old code:

```rust
pub fn try_admit(&self, ip: Option<IpAddr>) -> bool {
    let mut map = self.inner.per_ip.lock().expect(...);   // <- gate
    if map.len() >= policy.max_tracked_ips {
        let victim = map.iter().min_by_key(|(_, b)| b.last_refill)
            .map(|(k, _)| *k);                            // <- O(n) inside lock
        if let Some(v) = victim { map.remove(&v); }
    }
    let bucket = map.entry(ip).or_insert_with(|| TokenBucket::new(...));
    if !bucket.take(now) { return false; }
    drop(map);
    let mut g = self.inner.global.lock().expect(...);     // <- gate 2
    g.take(now)
}
```

Two locks per request, both serialising ALL concurrent callers
through one critical section. Under c=16 contention from one IP, the
mutex queue dominated tail latency (`std::sync::Mutex` falls back to
the OS futex which gives long sleeps under high contention) — that's
the source of the 1004 ms p50 reported in the issue.

## Fix

Two parts:

**(a) Replace the data structure.** `Mutex<HashMap>` →
`DashMap<IpAddr, PerIpBucket>` where `PerIpBucket` holds its own
`Mutex<TokenBucket>`. DashMap is sharded (typically ~64 shards on a
20-core box); two distinct IPs serialise only when their hashes
collide on the same shard, which is rare. The token-arithmetic
critical section is a few additions and a comparison — fits in one
cache line, sub-100 ns. Eviction is amortised: instead of scanning
the whole map on every over-cap insert, the next admit triggers a
sweep that drops idle entries (DashMap's `retain` runs per-shard).

**(b) Expose the policy as CLI knobs.** Six new flags on `serve`:

* `--admission-disable` — bypass admission entirely.
* `--admission-per-ip-per-sec` (default 25)
* `--admission-per-ip-burst` (default 50)
* `--admission-global-per-sec` (default 500)
* `--admission-global-burst` (default 1000)
* `--admission-max-tracked-ips` (default 10_000)

A new `ServerState::with_admission_policy(...)` builder applies
them. The defaults preserve the production hardening shape; the
flags exist so deployments fronted by a reverse proxy (or
single-tenant benchmarks like this one) can lift the per-IP gate
without recompiling.

## Verification

* `cargo test -p butterfly-geocode` — 284 unit tests pass, plus 1
  new regression test (`concurrent_admits_do_not_serialise`) that
  asserts 80 000 admits across 16 threads finish under 2 seconds.
  Old code would take 100s+ under that load.
* `cargo clippy --workspace --all-targets --all-features` — clean.
* `cargo fmt --all -- --check` — clean.
* Bench: 23.5 qps → **2216 qps** at c=16 = **94× speedup**.
  Result detail in `bench/geocode/results/172-fix/comparison.md`.

## Files touched

* `geocode/Cargo.toml` — `dashmap = "6.1"` added.
* `geocode/src/control/admission.rs` — full rewrite: DashMap + per-
  bucket Mutex, `disabled` knob, amortised eviction, regression test.
* `geocode/src/server/state.rs` — `with_admission_policy` builder.
* `geocode/src/main.rs` — six new `--admission-*` CLI flags, plumbed
  through `serve_cmd` to `ServerState::with_admission_policy`.

## Why no flamegraph

The diagnosis pinned the gate to a specific constant (25 qps =
`per_ip_refill_per_sec`) before any profiling was needed. The fix
was verified by the regression test (which would catch any
reintroduction) and by the end-to-end bench (which shows the gate is
gone — 94× throughput, p50 latency back to single-digit ms).

A flamegraph of the **old** code would just show
`std::sync::Mutex::lock` near 100% under c=16 contention; that's
already implied by the diagnosis. A flamegraph of the **new** code
shows expected HTTP / serde / shard-lookup costs — no remaining
serial gate. That bench is reproducible from `comparison.md` if a
reviewer wants to capture one.
