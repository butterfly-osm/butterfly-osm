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
//!
//! ## One-pass static decision (#97 §4)
//!
//! Admission does not account for feedback operator firings — those
//! are handled by [`crate::control::fanout`] runtime caps. Once a
//! request is admitted it runs to completion or aborts on a fanout
//! cap, never on re-admission.

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::Mutex;
use std::time::Instant;

use axum::body::Body;
use axum::extract::{ConnectInfo, State};
use axum::http::{HeaderValue, Request, StatusCode, header};
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use serde::Serialize;

use super::metrics_general::GeneralMetrics;

/// Tunable admission policy.
#[derive(Debug, Clone, Copy)]
pub struct AdmissionPolicy {
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
    /// the LRU IP is evicted. Range: 100 - 1_000_000. Default: 10_000.
    pub max_tracked_ips: usize,
    /// Retry-After value (seconds) returned with 429s.
    /// Range: 1 - 600. Default: 5.
    pub retry_after_secs: u32,
}

impl Default for AdmissionPolicy {
    fn default() -> Self {
        Self {
            global_capacity: 1_000,
            global_refill_per_sec: 500,
            per_ip_capacity: 50,
            per_ip_refill_per_sec: 25,
            max_tracked_ips: 10_000,
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

/// Shared admission state. Cheap-clone via [`Arc`].
#[derive(Debug, Clone)]
pub struct AdmissionState {
    inner: Arc<AdmissionInner>,
}

#[derive(Debug)]
struct AdmissionInner {
    policy: AdmissionPolicy,
    global: Mutex<TokenBucket>,
    /// Per-IP buckets in an LRU-ish HashMap. The mutex covers both the
    /// map (insert/evict) and per-bucket take. Contention is bounded
    /// by request rate; for ≥10K rps a sharded lock would help, but
    /// the MVP target is far below that.
    per_ip: Mutex<HashMap<IpAddr, TokenBucket>>,
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
                per_ip: Mutex::new(HashMap::new()),
                metrics,
            }),
        }
    }

    /// Try to admit a request from `ip`. Returns true on admission,
    /// false on rejection (caller should respond with 429).
    pub fn try_admit(&self, ip: Option<IpAddr>) -> bool {
        let now = Instant::now();
        let policy = self.inner.policy;

        // Per-IP bucket first — cheaper to fail fast than global.
        if let Some(ip) = ip {
            let mut map = self
                .inner
                .per_ip
                .lock()
                .expect("admission per-ip mutex poisoned");
            // LRU eviction: if at capacity, drop the oldest by
            // `last_refill`. Cheap because the map is small (10k by
            // default) and eviction is amortised.
            if map.len() >= policy.max_tracked_ips
                && let Some(victim) = map
                    .iter()
                    .min_by_key(|(_, b)| b.last_refill)
                    .map(|(k, _)| *k)
            {
                map.remove(&victim);
            }
            let bucket = map.entry(ip).or_insert_with(|| {
                TokenBucket::new(policy.per_ip_capacity, policy.per_ip_refill_per_sec)
            });
            if !bucket.take(now) {
                return false;
            }
        }

        // Global bucket second.
        let mut g = self
            .inner
            .global
            .lock()
            .expect("admission global mutex poisoned");
        g.take(now)
    }

    #[must_use]
    pub fn policy(&self) -> AdmissionPolicy {
        self.inner.policy
    }

    #[must_use]
    pub fn metrics(&self) -> &GeneralMetrics {
        &self.inner.metrics
    }
}

/// Errors surfaced via [`pre_execution_check`].
///
/// Re-exported from [`crate::control::budget`] for convenience.
pub use super::budget::AdmissionError;

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
    use std::time::Duration;

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
    fn lru_evicts_oldest_ip() {
        let p = AdmissionPolicy {
            max_tracked_ips: 2,
            per_ip_capacity: 1,
            per_ip_refill_per_sec: 1_000_000, // generous refill, isolate map size
            ..AdmissionPolicy::default()
        };
        let s = st(p);
        let ip1 = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)));
        let ip2 = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)));
        let ip3 = Some(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 3)));
        assert!(s.try_admit(ip1));
        assert!(s.try_admit(ip2));
        // Map full; ip3 forces eviction.
        assert!(s.try_admit(ip3));
        let map = s.inner.per_ip.lock().unwrap();
        assert!(map.len() <= 2);
    }

    #[test]
    fn admission_state_holds_policy() {
        let p = AdmissionPolicy::default();
        let s = st(p);
        assert_eq!(s.policy().global_capacity, p.global_capacity);
    }
}
