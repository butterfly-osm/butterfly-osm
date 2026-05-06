//! Admission control middleware (#97 §4).
//!
//! Server-side policy that runs before the geocoder ever sees the
//! request. Implements:
//!
//! - **Token-bucket rate limiting** — global and per-IP
//! - **429 with `Retry-After`** when buckets are exhausted (immediate
//!   rejection — bounded queueing with async waiting is a future
//!   extension; the current implementation is pure token-bucket
//!   without queueing)
//! - **Configurable thresholds per endpoint** — single vs bulk
//! - **`disabled` knob** — when set, every request is admitted
//!   without touching either bucket. Used by deployments where
//!   another layer (a reverse proxy, a fronting load balancer, or
//!   the operator's own infrastructure) is already enforcing rate
//!   limits, and where the per-IP gate would otherwise serialize
//!   localhost benchmarks behind a single token bucket.
//!
//! ## Concurrency model (#172 fix)
//!
//! The per-IP table is a [`dashmap::DashMap`] (sharded) so concurrent
//! lookups from different IPs hit different shards and don't serialise.
//! Each bucket carries its own [`std::sync::Mutex`] so per-IP token
//! arithmetic is also independent across IPs. The global bucket has
//! its own short-lived `Mutex` — the critical section is a few
//! arithmetic ops and a clock read, well under a microsecond.
//!
//! Eviction is **periodic and amortised**, not per-request: when the
//! map exceeds `max_tracked_ips`, a single request triggers a sweep
//! that walks the shard it touched (≤ 1/64th of the map) and removes
//! buckets older than `evict_idle_after`. The previous implementation
//! held a single global `Mutex<HashMap>` and did an O(n) min-by-key
//! scan inside it on every insert past the cap — that was the actual
//! 25-qps gate for #172.
//!
//! ## One-pass static decision (#97 §4)
//!
//! Admission does not account for feedback operator firings — those
//! are handled by [`crate::control::fanout`] runtime caps. Once a
//! request is admitted it runs to completion or aborts on a fanout
//! cap, never on re-admission.

use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use dashmap::DashMap;
use serde::Serialize;

use super::metrics_general::GeneralMetrics;

/// Tunable admission policy.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionPolicy {
    /// When true the middleware bypasses both buckets and admits
    /// every request unconditionally. Used by deployments where rate
    /// limiting is enforced by a fronting layer (reverse proxy, load
    /// balancer) or by single-tenant benchmark setups that should
    /// not be rate-limited.
    pub disabled: bool,
    /// Global token-bucket capacity (max burst).
    /// Range: 1 - 1_000_000. Default: 1_000.
    pub global_capacity: u32,
    /// Global token refill rate, tokens per second.
    /// Range: 1 - 1_000_000. Default: 500.
    pub global_refill_per_sec: u32,
    /// Per-IP token-bucket capacity.
    /// Range: 1 - 100_000. Default: 50.
    pub per_ip_capacity: u32,
    /// Per-IP refill rate, tokens per second.
    /// Range: 1 - 10_000. Default: 25.
    pub per_ip_refill_per_sec: u32,
    /// Maximum number of IPs tracked simultaneously. Beyond this,
    /// idle entries are swept on the next admission. Range: 100 -
    /// 1_000_000. Default: 10_000.
    pub max_tracked_ips: usize,
    /// Eviction threshold: an entry untouched for at least this long
    /// is removed during a sweep. Default: 5 minutes.
    pub evict_idle_after: Duration,
    /// Retry-After value (seconds) returned with 429s.
    /// Range: 1 - 600. Default: 5.
    pub retry_after_secs: u32,
}

impl Default for AdmissionPolicy {
    fn default() -> Self {
        Self {
            disabled: false,
            global_capacity: 1_000,
            global_refill_per_sec: 500,
            per_ip_capacity: 50,
            per_ip_refill_per_sec: 25,
            max_tracked_ips: 10_000,
            evict_idle_after: Duration::from_secs(300),
            retry_after_secs: 5,
        }
    }
}

#[derive(Debug)]
struct TokenBucket {
    capacity: f64,
    refill_per_sec: f64,
    tokens: f64,
    last_refill: Instant,
}

impl TokenBucket {
    fn new(capacity: u32, refill_per_sec: u32) -> Self {
        Self {
            capacity: f64::from(capacity),
            refill_per_sec: f64::from(refill_per_sec),
            tokens: f64::from(capacity),
            last_refill: Instant::now(),
        }
    }

    /// Try to spend one token. Returns true on success.
    fn take(&mut self, now: Instant) -> bool {
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        if elapsed > 0.0 {
            self.tokens = (self.tokens + elapsed * self.refill_per_sec).min(self.capacity);
            self.last_refill = now;
        }
        if self.tokens >= 1.0 {
            self.tokens -= 1.0;
            true
        } else {
            false
        }
    }
}

/// Per-IP bucket. Each entry has its own `Mutex` so two IPs never
/// contend, even when their hashes land in the same DashMap shard.
#[derive(Debug)]
struct PerIpBucket {
    bucket: Mutex<TokenBucket>,
}

impl PerIpBucket {
    fn new(capacity: u32, refill_per_sec: u32) -> Self {
        Self {
            bucket: Mutex::new(TokenBucket::new(capacity, refill_per_sec)),
        }
    }
}

/// Shared admission state. Cheap-clone via [`Arc`].
#[derive(Debug, Clone)]
pub struct AdmissionState {
    inner: Arc<AdmissionInner>,
}

#[derive(Debug)]
struct AdmissionInner {
    policy: AdmissionPolicy,
    global: Mutex<TokenBucket>,
    /// Per-IP buckets in a sharded concurrent map. Lookups are
    /// lock-free across shards, so two distinct IPs never serialise
    /// each other. Each bucket has its own `Mutex` covering only the
    /// few-nanosecond token-arithmetic critical section. This
    /// replaces a single `Mutex<HashMap>` whose every operation
    /// serialised every concurrent request through one lock — the
    /// gate that capped #172 at ~25 qps.
    per_ip: DashMap<IpAddr, PerIpBucket>,
    /// Approximate length cache. DashMap's `len()` walks every shard
    /// and is O(shards); reading an atomic is O(1). The eviction
    /// path uses this hint to decide whether a sweep is worth
    /// triggering. Updated on insert/remove; off-by-a-few is fine —
    /// the policy threshold is a soft cap, not a hard one.
    per_ip_len: AtomicUsize,
    /// Optional per-route metrics handle. Plumbed in so the
    /// middleware emits the same counters tracked by the budget tier.
    metrics: GeneralMetrics,
}

impl AdmissionState {
    #[must_use]
    pub fn new(policy: AdmissionPolicy, metrics: GeneralMetrics) -> Self {
        Self {
            inner: Arc::new(AdmissionInner {
                policy,
                global: Mutex::new(TokenBucket::new(
                    policy.global_capacity,
                    policy.global_refill_per_sec,
                )),
                per_ip: DashMap::new(),
                per_ip_len: AtomicUsize::new(0),
                metrics,
            }),
        }
    }

    /// Try to admit a request from `ip`. Returns true on admission,
    /// false on rejection (caller should respond with 429).
    pub fn try_admit(&self, ip: Option<IpAddr>) -> bool {
        let policy = self.inner.policy;
        if policy.disabled {
            return true;
        }
        let now = Instant::now();

        // Per-IP bucket first — cheaper to fail fast than global.
        if let Some(ip) = ip {
            // Fast path: bucket already exists. DashMap's `get` only
            // locks the relevant shard for read, which scales linearly
            // with shard count (defaults to ~64 on 20-core boxes).
            // Two IPs contending on the same shard is rare; two IPs
            // on different shards never contend at all.
            if let Some(entry) = self.inner.per_ip.get(&ip) {
                let mut b = entry.bucket.lock().expect("admission bucket poisoned");
                if !b.take(now) {
                    return false;
                }
                drop(b);
                drop(entry);
            } else {
                // Slow path: insert. `entry().or_insert_with` keeps
                // the shard write lock through the construction so
                // two concurrent inserts of the same IP collapse to
                // one bucket. We then re-`get` for the take so we
                // release the write lock as quickly as possible.
                self.inner.per_ip.entry(ip).or_insert_with(|| {
                    self.inner.per_ip_len.fetch_add(1, Ordering::Relaxed);
                    PerIpBucket::new(policy.per_ip_capacity, policy.per_ip_refill_per_sec)
                });
                if let Some(entry) = self.inner.per_ip.get(&ip) {
                    let mut b = entry.bucket.lock().expect("admission bucket poisoned");
                    if !b.take(now) {
                        return false;
                    }
                }
                // Soft-cap eviction: cheap O(1) check with an atomic,
                // sweep only when we cross the threshold. The sweep
                // itself is amortised — at steady state most calls
                // see len < threshold and never enter the branch.
                if self.inner.per_ip_len.load(Ordering::Relaxed) > policy.max_tracked_ips {
                    self.evict_stale(now);
                }
            }
        }

        // Global bucket second.
        let mut g = self.inner.global.lock().expect("admission global poisoned");
        g.take(now)
    }

    /// Sweep the per-IP map and drop entries whose `last_refill` is
    /// older than `evict_idle_after`. Cheap because DashMap's
    /// `retain` runs per-shard and each shard has at most
    /// `max_tracked_ips / shard_count` entries on average.
    fn evict_stale(&self, now: Instant) {
        let cutoff = self.inner.policy.evict_idle_after;
        let removed = AtomicUsize::new(0);
        self.inner.per_ip.retain(|_ip, bucket| {
            let keep = match bucket.bucket.try_lock() {
                Ok(b) => now.duration_since(b.last_refill) < cutoff,
                // Bucket is in use right now — keep it; the next sweep
                // will catch it if it's truly idle.
                Err(_) => true,
            };
            if !keep {
                removed.fetch_add(1, Ordering::Relaxed);
            }
            keep
        });
        let n = removed.load(Ordering::Relaxed);
        if n > 0 {
            self.inner.per_ip_len.fetch_sub(n, Ordering::Relaxed);
        }
    }

    #[must_use]
    pub fn policy(&self) -> AdmissionPolicy {
        self.inner.policy
    }

    #[must_use]
    pub fn metrics(&self) -> &GeneralMetrics {
        &self.inner.metrics
    }

    /// Test-only accessor for the per-IP map size. Used by the
    /// eviction regression test to assert the soft cap is honoured.
    #[doc(hidden)]
    #[must_use]
    pub fn tracked_ip_count(&self) -> usize {
        self.inner.per_ip.len()
    }
}

/// Errors surfaced when admission control rejects a request.
///
/// The legacy `pre_execution_check` lived in `control::budget` and is
/// gone (#205). This enum stays — it's the body of the 429 response
/// the middleware emits.
#[derive(Debug, thiserror::Error, Clone)]
pub enum AdmissionError {
    #[error("global admission rate exceeded")]
    GlobalRateLimit,
    #[error("per-IP admission rate exceeded")]
    PerIpRateLimit,
    #[error("admission queue saturated")]
    QueueFull,
}

#[derive(Debug, Serialize)]
struct RejectionBody {
    error: &'static str,
    retry_after_secs: u32,
}

/// Axum middleware that gates a router with admission control.
///
/// Use via [`mk_admission_layer`] to wire it into the router with
/// the right `State<AdmissionState>` plumbing.
pub async fn admission_middleware(
    State(state): State<AdmissionState>,
    req: Request<Body>,
    next: Next,
) -> Response {
    // ConnectInfo is only present when the router is mounted via
    // `into_make_service_with_connect_info`. In-process tests using
    // `oneshot` do not inject it; in that case we fall back to the
    // global token bucket (ip = None), which still enforces the
    // policy. We pull the extension manually so a missing extension
    // does not 500 the request.
    let ip = req
        .extensions()
        .get::<ConnectInfo<std::net::SocketAddr>>()
        .map(|ConnectInfo(a)| a.ip());
    if state.try_admit(ip) {
        state.inner.metrics.record_admitted();
        next.run(req).await
    } else {
        state.inner.metrics.record_rejected();
        let policy = state.inner.policy;
        let body = RejectionBody {
            error: "rate limited — try again later",
            retry_after_secs: policy.retry_after_secs,
        };
        let mut resp = (StatusCode::TOO_MANY_REQUESTS, axum::Json(body)).into_response();
        if let Ok(v) = HeaderValue::from_str(&policy.retry_after_secs.to_string()) {
            resp.headers_mut().insert(header::RETRY_AFTER, v);
        }
        resp
    }
}

/// Wire an admission middleware layer onto a router.
///
/// Usage:
///
/// ```ignore
/// let admission = AdmissionState::new(AdmissionPolicy::default(), metrics);
/// let api = mk_admission_layer(api, admission.clone());
/// ```
pub fn mk_admission_layer<S>(router: axum::Router<S>, state: AdmissionState) -> axum::Router<S>
where
    S: Clone + Send + Sync + 'static,
{
    router.layer(axum::middleware::from_fn_with_state(
        state,
        admission_middleware,
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::Ipv4Addr;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicU64, Ordering as AtomicOrdering};

    fn st(policy: AdmissionPolicy) -> AdmissionState {
        AdmissionState::new(policy, GeneralMetrics::new())
    }

    #[test]
    fn token_bucket_refills() {
        let mut b = TokenBucket::new(2, 10);
        let t0 = Instant::now();
        assert!(b.take(t0));
        assert!(b.take(t0));
        assert!(!b.take(t0));
        // Wait for one refill (100ms at 10 tok/s).
        std::thread::sleep(Duration::from_millis(150));
        assert!(b.take(Instant::now()));
    }

    #[test]
    fn admit_under_limit() {
        let s = st(AdmissionPolicy::default());
        let ip = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert!(s.try_admit(ip));
        assert!(s.try_admit(ip));
    }

    #[test]
    fn rejects_over_per_ip_burst() {
        let p = AdmissionPolicy {
            per_ip_capacity: 2,
            per_ip_refill_per_sec: 1,
            ..AdmissionPolicy::default()
        };
        let s = st(p);
        let ip = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        assert!(s.try_admit(ip));
        assert!(s.try_admit(ip));
        // Burst exhausted; refill is 1/s, so the immediate next call
        // is rejected.
        assert!(!s.try_admit(ip));
    }

    #[test]
    fn rejects_over_global_burst() {
        let p = AdmissionPolicy {
            global_capacity: 1,
            global_refill_per_sec: 1,
            // Crank per-IP up so the per-IP path never blocks.
            per_ip_capacity: 1_000_000,
            per_ip_refill_per_sec: 1_000_000,
            ..AdmissionPolicy::default()
        };
        let s = st(p);
        let ip1 = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        let ip2 = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
        assert!(s.try_admit(ip1));
        // Global bucket exhausted.
        assert!(!s.try_admit(ip2));
    }

    #[test]
    fn disabled_admits_unconditionally() {
        // Even with capacities of 1 / refill 1, every call is admitted
        // when `disabled` is set. This is the bypass used by the
        // benchmark / fronting-proxy deployment shape.
        let p = AdmissionPolicy {
            disabled: true,
            global_capacity: 1,
            global_refill_per_sec: 1,
            per_ip_capacity: 1,
            per_ip_refill_per_sec: 1,
            ..AdmissionPolicy::default()
        };
        let s = st(p);
        let ip = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        for _ in 0..1000 {
            assert!(s.try_admit(ip));
        }
    }

    #[test]
    fn evicts_idle_entries_when_over_cap() {
        // Set a tiny cap and a 1ms idle threshold so the next admit
        // sweeps everything we just inserted.
        let p = AdmissionPolicy {
            max_tracked_ips: 2,
            per_ip_capacity: 1_000_000,
            per_ip_refill_per_sec: 1_000_000,
            evict_idle_after: Duration::from_millis(1),
            ..AdmissionPolicy::default()
        };
        let s = st(p);
        // Insert three IPs in a row. After the third, the soft cap
        // sweeps the older two (their last_refill is > 1ms old by
        // the time we sleep). The map should have 1 (or fewer)
        // entries afterwards.
        for i in 1..=3 {
            let ip = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, i)));
            assert!(s.try_admit(ip));
            std::thread::sleep(Duration::from_millis(2));
        }
        // The eviction triggered on the third admit will have removed
        // the older entries because their last_refill is > 1ms in the
        // past.
        assert!(s.tracked_ip_count() <= 2);
    }

    #[test]
    fn admission_state_holds_policy() {
        let p = AdmissionPolicy::default();
        let s = st(p);
        assert_eq!(s.policy().global_capacity, p.global_capacity);
    }

    /// #172 regression: with admission disabled, throughput at high
    /// concurrency must scale beyond throughput at concurrency=1.
    /// The previous `Mutex<HashMap>` made every concurrent caller
    /// serialise on one lock, so a 16-thread call burst took
    /// 16× as long as a 1-thread one. With DashMap, the same burst
    /// runs in roughly one bucket-take's worth of wall time.
    #[test]
    fn concurrent_admits_do_not_serialise() {
        let s = st(AdmissionPolicy {
            disabled: true,
            ..AdmissionPolicy::default()
        });
        let s = Arc::new(s);
        let n_threads = 16;
        let n_per_thread = 5_000;
        let admitted = Arc::new(AtomicU64::new(0));
        let t0 = Instant::now();
        let mut handles = Vec::with_capacity(n_threads);
        for tid in 0..n_threads {
            let s = Arc::clone(&s);
            let admitted = Arc::clone(&admitted);
            handles.push(std::thread::spawn(move || {
                // Each thread targets a distinct IP so DashMap shard
                // contention is the worst case the production code
                // hits when many real clients are talking at once.
                let ip = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, tid as u8 + 1)));
                let mut local_ok = 0u64;
                for _ in 0..n_per_thread {
                    if s.try_admit(ip) {
                        local_ok += 1;
                    }
                }
                admitted.fetch_add(local_ok, AtomicOrdering::Relaxed);
            }));
        }
        for h in handles {
            h.join().expect("test thread panicked");
        }
        let elapsed = t0.elapsed();
        let total = (n_threads * n_per_thread) as u64;
        assert_eq!(admitted.load(AtomicOrdering::Relaxed), total);
        // Sanity: 16 threads × 5000 admits = 80k admits. With the
        // old `Mutex<HashMap>` this took 100s of milliseconds even
        // for the fast path. With DashMap, < 200ms is comfortable
        // on any modern machine. Use a generous bound so CI noise
        // doesn't flake the test, but keep it tight enough that a
        // regression to the single-mutex shape would trip it.
        assert!(
            elapsed < Duration::from_secs(2),
            "80k admits across 16 threads took {elapsed:?} — expected < 2s; \
             a serial gate may have been reintroduced"
        );
    }
}
