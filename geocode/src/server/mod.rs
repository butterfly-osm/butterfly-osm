//! HTTP server for forward + reverse geocoding.
//!
//! Endpoints (REST, JSON default, GeoJSON via `Accept` header):
//!
//! - `GET /geocode?q=...&country=BE&limit=N` — forward
//! - `GET /geocode/reverse?lat=...&lon=...&radius_m=...&limit=N` — reverse
//! - `GET /health` — uptime + record count + version
//! - `GET /metrics` — Prometheus
//!
//! Content negotiation per the project's standing API design preference
//! (CLAUDE.md memory: "User strongly prefers content negotiation
//! via Accept header over separate endpoints"). No `/format` variants.
//!
//! ## Middleware order
//!
//! Two concentric rings of layers, applied in reverse order of mention
//! (outer-first is what the request hits first):
//!
//! ### Outer ring — runs for ALL routes including `/metrics`:
//!
//! 1. CORS — handle preflight, inject permissive headers (production
//!    deployments should narrow `Access-Control-Allow-Origin` via a
//!    reverse proxy).
//! 2. TraceLayer — span every request with a `tracing` `INFO` log.
//! 3. Prometheus — collect HTTP-level histograms / counters. Note
//!    that `/metrics` itself is one of the routes wrapped by this
//!    layer, so Prometheus tracks scrapes too.
//! 4. CatchPanicLayer — convert panics into 500 instead of dropping
//!    the connection.
//!
//! ### Inner ring — runs ONLY for `/geocode*` and `/health`, NOT for
//! `/metrics`:
//!
//! 5. Compression — gzip/brotli on responses based on `Accept-Encoding`.
//! 6. Timeout — `cfg.request_timeout` server-side cap, 408 on expiry.
//! 7. RequestBodyLimit — `cfg.max_request_body_bytes` cap on request
//!    bodies (POST endpoints will use this once they land; GET ignores
//!    it but having the layer up means a future POST endpoint can't
//!    accidentally accept unbounded uploads).
//!
//! ### Innermost — runs ONLY for `/geocode*`:
//!
//! 8. Governor (per-IP HTTP-level rate limit) — runs *before*
//!    admission so abusive clients are dropped at the cheapest layer.
//! 9. Admission (token-bucket cost-based gate) — only on `/geocode*`,
//!    not on `/health` or `/metrics` so monitors are never throttled.
//!
//! `/metrics` intentionally bypasses every layer except the outer
//! ring: scrapers must always be able to read it, so timeouts /
//! body limits / compression / rate limiting do not apply.

// tonic::Status is 176 bytes — the canonical gRPC error type.
// Every gRPC handler returns Result<_, Status>; boxing adds indirection
// with no benefit. Suppression is module-scoped (mirrors butterfly-route).
#[allow(clippy::result_large_err)]
pub mod flight;
pub mod handlers;
pub mod state;

use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::Duration;

use axum::Router;
use axum::extract::ConnectInfo;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use axum_prometheus::PrometheusMetricLayer;
use axum_prometheus::metrics_exporter_prometheus::PrometheusHandle;
use tower_governor::GovernorLayer;
use tower_governor::errors::GovernorError;
use tower_governor::governor::GovernorConfigBuilder;
use tower_governor::key_extractor::KeyExtractor;
use tower_http::catch_panic::CatchPanicLayer;
use tower_http::compression::CompressionLayer;
use tower_http::cors::{Any, CorsLayer};
use tower_http::limit::RequestBodyLimitLayer;
use tower_http::timeout::TimeoutLayer;
use tower_http::trace::TraceLayer;

use crate::control::admission::mk_admission_layer;

pub use state::ServerState;

/// Tunable HTTP-server configuration. Defaults match the production
/// hardening targets — operators that need to lift them reach for
/// [`build_router_with_config`].
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Per-IP requests-per-second steady state. Default: 100.
    /// Range: 1 - 100_000. Values above 100_000 are clamped because
    /// `governor` uses a `NonZeroU32` quota internally.
    pub rate_limit_per_sec: u32,
    /// Per-IP burst capacity (max tokens in the bucket).
    /// Default: 200. Range: 1 - 100_000.
    pub rate_limit_burst: u32,
    /// Whole-request server-side timeout. Default: 30 s. Beyond this
    /// the layer returns 408 to free the worker even if the handler
    /// is still running on `spawn_blocking`.
    pub request_timeout: Duration,
    /// POST/PUT body cap. Default: 4 KB. GET requests are unaffected
    /// (the body limit only fires when there's a body to limit).
    pub max_request_body_bytes: usize,
    /// CIDR allowlist of trusted reverse proxies. When non-empty AND
    /// the connecting peer's IP is contained in any of these CIDRs,
    /// the [`PeerIpKey`] extractor pulls the client IP from
    /// `X-Forwarded-For` (rightmost non-trusted entry per RFC 7239)
    /// instead of the connection peer. Without this, all requests
    /// behind a reverse proxy share the proxy's IP and rate-limiting
    /// becomes global rather than per-client.
    pub trusted_proxies: Vec<TrustedCidr>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_sec: 100,
            rate_limit_burst: 200,
            request_timeout: Duration::from_secs(30),
            max_request_body_bytes: 4 * 1024,
            trusted_proxies: Vec::new(),
        }
    }
}

impl ServerConfig {
    /// Construct with the documented defaults. Convenience alias for
    /// [`Self::default`] kept for callers that prefer the explicit
    /// builder shape.
    pub fn with_defaults() -> Self {
        Self::default()
    }
}

/// Compact CIDR range used by the trusted-proxy allowlist. Supports
/// both v4 and v6. Pure data — comparison is `O(prefix_len)`.
#[derive(Debug, Clone)]
pub struct TrustedCidr {
    network: IpAddr,
    prefix: u8,
}

impl TrustedCidr {
    /// Parse a CIDR string like `10.0.0.0/8` or `2001:db8::/32`.
    pub fn parse(s: &str) -> Result<Self, String> {
        let (addr, prefix) = match s.split_once('/') {
            Some((a, p)) => (a, p),
            None => {
                // Bare IP → /32 for v4, /128 for v6.
                let ip: IpAddr = s.parse().map_err(|e| format!("invalid IP '{s}': {e}"))?;
                let prefix = match ip {
                    IpAddr::V4(_) => 32,
                    IpAddr::V6(_) => 128,
                };
                return Ok(Self {
                    network: ip,
                    prefix,
                });
            }
        };
        let network: IpAddr = addr
            .parse()
            .map_err(|e| format!("invalid CIDR network '{addr}': {e}"))?;
        let prefix: u8 = prefix
            .parse()
            .map_err(|e| format!("invalid CIDR prefix '{prefix}': {e}"))?;
        let max = match network {
            IpAddr::V4(_) => 32,
            IpAddr::V6(_) => 128,
        };
        if prefix > max {
            return Err(format!("CIDR prefix /{prefix} exceeds /{max} for {addr}"));
        }
        Ok(Self { network, prefix })
    }

    /// Test whether `ip` falls inside this CIDR.
    pub fn contains(&self, ip: IpAddr) -> bool {
        match (self.network, ip) {
            (IpAddr::V4(net), IpAddr::V4(client)) => {
                let net_bits = u32::from_be_bytes(net.octets());
                let client_bits = u32::from_be_bytes(client.octets());
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u32::MAX << (32 - self.prefix)
                };
                (net_bits & mask) == (client_bits & mask)
            }
            (IpAddr::V6(net), IpAddr::V6(client)) => {
                let net_bits = u128::from_be_bytes(net.octets());
                let client_bits = u128::from_be_bytes(client.octets());
                let mask = if self.prefix == 0 {
                    0
                } else {
                    u128::MAX << (128 - self.prefix)
                };
                (net_bits & mask) == (client_bits & mask)
            }
            _ => false,
        }
    }
}

/// Parse a CSV `--trusted-proxies` flag into a vector of
/// [`TrustedCidr`]. Empty / `None` input yields an empty allowlist
/// (the default — no proxy trust).
pub fn parse_trusted_proxies(s: Option<&str>) -> Result<Vec<TrustedCidr>, String> {
    let Some(s) = s else {
        return Ok(Vec::new());
    };
    s.split(',')
        .map(str::trim)
        .filter(|t| !t.is_empty())
        .map(TrustedCidr::parse)
        .collect()
}

/// Per-IP key extractor for `tower_governor`. Pulls the client IP
/// from `ConnectInfo<SocketAddr>` (injected by
/// `into_make_service_with_connect_info`). Falls back to
/// `127.0.0.1` when `ConnectInfo` is absent — happens in in-process
/// `oneshot` tests; in that case every test request shares the same
/// key, which still proves the layer is wired.
///
/// Using a fallback (rather than returning `UnableToExtractKey`)
/// keeps tests green without requiring every test to thread a fake
/// SocketAddr into the request extensions. Production deployments
/// always go through `into_make_service_with_connect_info` so the
/// fallback is never hit.
///
/// ## Trusted-proxy support (#170)
///
/// When [`ServerConfig::trusted_proxies`] is non-empty AND the
/// connecting peer is inside one of the listed CIDRs, the extractor
/// walks `X-Forwarded-For` (or the standardised `Forwarded` header
/// per RFC 7239) and returns the rightmost entry whose IP is NOT
/// itself in a trusted CIDR. This is the standard "trust hop chain"
/// pattern: the proxy is trusted to add a header but not to lie
/// about clients further upstream. Without this configuration, every
/// request behind a reverse proxy shares the proxy's IP and the rate
/// limiter becomes global.
#[derive(Debug, Clone, Default)]
pub struct PeerIpKey {
    trusted: Arc<[TrustedCidr]>,
}

impl PeerIpKey {
    /// Construct a key extractor with the given trusted-proxy
    /// allowlist. An empty allowlist disables the XFF/Forwarded fast
    /// path; the extractor returns the connection peer in every case.
    pub fn new(trusted: Vec<TrustedCidr>) -> Self {
        Self {
            trusted: trusted.into(),
        }
    }

    fn is_trusted(&self, ip: IpAddr) -> bool {
        self.trusted.iter().any(|c| c.contains(ip))
    }

    /// Walk the `X-Forwarded-For` (or `Forwarded`) header values and
    /// return the rightmost entry whose IP is NOT in a trusted CIDR.
    /// The semantics: every IP between the rightmost-non-trusted and
    /// the actual peer is a trusted proxy that injected the header,
    /// so that rightmost-non-trusted IP is the true client. Returns
    /// `None` when no XFF header is present or every entry is itself
    /// a trusted proxy.
    fn untrusted_xff_ip<T>(&self, req: &Request<T>) -> Option<IpAddr> {
        let xff = req
            .headers()
            .get("x-forwarded-for")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        if let Some(xff) = xff {
            // RFC 7239 §5.2: comma-separated, leftmost = original.
            // Walk right-to-left looking for the first untrusted IP.
            for entry in xff.split(',').rev() {
                let candidate = entry.trim();
                if let Ok(ip) = candidate.parse::<IpAddr>()
                    && !self.is_trusted(ip)
                {
                    return Some(ip);
                }
            }
        }
        // Fallback: RFC 7239 `Forwarded` with `for=` parameter. Same
        // semantics, different syntax. Each entry looks like
        // `for=192.0.2.43;proto=http;host=...`.
        let fwd = req
            .headers()
            .get("forwarded")
            .and_then(|v| v.to_str().ok())
            .map(|s| s.to_string());
        if let Some(fwd) = fwd {
            for entry in fwd.split(',').rev() {
                for kv in entry.split(';') {
                    let kv = kv.trim();
                    let Some(value) = kv.strip_prefix("for=").or_else(|| kv.strip_prefix("For="))
                    else {
                        continue;
                    };
                    let value = value.trim_matches('"');
                    // RFC 7239 also allows `for="[2001:db8::1]:8080"`.
                    // Strip any port.
                    let host = value.rsplit_once(':').map(|(h, _)| h).unwrap_or(value);
                    let host = host.trim_matches('[').trim_matches(']');
                    if let Ok(ip) = host.parse::<IpAddr>()
                        && !self.is_trusted(ip)
                    {
                        return Some(ip);
                    }
                }
            }
        }
        None
    }
}

impl KeyExtractor for PeerIpKey {
    type Key = IpAddr;

    fn extract<T>(&self, req: &Request<T>) -> Result<Self::Key, GovernorError> {
        let peer = req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(a)| a.ip())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        // If the peer is itself a trusted proxy, the real client IP
        // lives in XFF / Forwarded. Otherwise we trust the peer as
        // the client.
        if self.is_trusted(peer)
            && let Some(real) = self.untrusted_xff_ip(req)
        {
            return Ok(real);
        }
        Ok(peer)
    }
}

/// Build the prometheus layer + handle exactly once per process.
/// `PrometheusMetricLayer::pair()` calls into the `metrics` crate's
/// global recorder, which can only be set once. The first call wins;
/// subsequent calls (e.g. from tests that build multiple routers in
/// the same process) reuse the cached pair.
fn prometheus_pair() -> &'static (PrometheusMetricLayer<'static>, PrometheusHandle) {
    static PAIR: OnceLock<(PrometheusMetricLayer<'static>, PrometheusHandle)> = OnceLock::new();
    PAIR.get_or_init(PrometheusMetricLayer::pair)
}

/// Construct the full HTTP router with default [`ServerConfig`].
///
/// Tests and most call sites use this. Operators that need to override
/// the rate-limit knobs reach for [`build_router_with_config`].
pub fn build_router(state: Arc<ServerState>) -> Router {
    let (router, _gc) = build_router_with_config(state, ServerConfig::with_defaults());
    router
}

/// Handle returned alongside the router so callers can opt-in to
/// driving the per-IP map garbage-collector. Returned by
/// [`build_router_with_config`] separately from the router so router
/// construction stays a pure synchronous fn — moving the
/// `tokio::spawn` out of the build means `build_router_with_config`
/// no longer panics when called outside a Tokio runtime, and tests
/// that build multiple routers don't leak background tasks.
///
/// `serve_cmd` (the only production caller) drives this with
/// [`spawn_governor_gc`]; tests typically drop the handle and never
/// run GC.
pub struct GovernorGcHandle {
    /// Closures that prune (`retain_recent`) the limiter and report
    /// its current map size. Boxed so the handle's type isn't
    /// parameterised over the governor's generic key/state types.
    retain: Box<dyn Fn() + Send + Sync>,
    len: Box<dyn Fn() -> usize + Send + Sync>,
}

impl std::fmt::Debug for GovernorGcHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GovernorGcHandle")
            .field("limiter_size", &(self.len)())
            .finish()
    }
}

/// Spawn a long-running task that periodically GCs the per-IP rate
/// limiter map. Idempotent across `build_router_with_config` calls in
/// the same process: only the first invocation per process actually
/// spawns; subsequent calls drop the new handle. Pulled out of
/// `build_router_with_config` so router construction stays
/// runtime-agnostic.
pub fn spawn_governor_gc(handle: GovernorGcHandle) {
    static SPAWNED: OnceLock<()> = OnceLock::new();
    if SPAWNED.set(()).is_err() {
        return;
    }
    tokio::spawn(async move {
        let interval = Duration::from_secs(60);
        loop {
            tokio::time::sleep(interval).await;
            tracing::trace!(
                rate_limit_storage_size = (handle.len)(),
                "governor retain_recent"
            );
            (handle.retain)();
        }
    });
}

/// Construct the full HTTP router with an explicit [`ServerConfig`].
///
/// Returns `(router, gc_handle)`. The handle is intentionally
/// detached from the router so the caller can spawn the per-IP map
/// garbage-collector at the right point in its runtime lifecycle
/// (typically `serve_cmd`). Tests that don't need GC drop the
/// handle.
pub fn build_router_with_config(
    state: Arc<ServerState>,
    cfg: ServerConfig,
) -> (Router, GovernorGcHandle) {
    let (prometheus_layer, metric_handle) = prometheus_pair().clone();

    // Permissive CORS by default — the geocoder is read-only, queries
    // are GET-only today, and operators that want to lock origins
    // down can wrap the binary behind a reverse proxy that re-injects
    // the policy. Documented in `geocode/README.md`.
    let cors = CorsLayer::new()
        .allow_origin(Any)
        .allow_methods(Any)
        .allow_headers(Any);

    // tower_governor is a leaky-bucket rate limiter keyed by client
    // IP via [`PeerIpKeyExtractor`]. It reads
    // `ConnectInfo<SocketAddr>` from request extensions, which is
    // injected when the service is mounted via
    // `into_make_service_with_connect_info::<SocketAddr>` (see
    // `main.rs::serve_cmd`). It runs *before* admission so abusive
    // clients are dropped at the cheapest layer. Both layers are
    // needed: governor gives raw HTTP-level fairness, admission gives
    // cost-based backpressure (#97 §4).
    //
    // `per_second` is the steady-state rate; `burst_size` is the
    // bucket capacity (governor refills at 1/per_second tokens per
    // second). Defaults of 100 req/s / burst=200 absorb bursty
    // browsers and small spikes, throttle sustained floods.
    //
    // `governor` uses `NonZeroU32` internally; clamp to >=1 so a
    // misconfigured 0 falls back to 1 instead of panicking on build.
    // We also clamp the upper bound at 100_000 — a runaway number
    // (e.g. u32::MAX) would defeat the purpose of the layer and waste
    // bookkeeping memory.
    let per_second = cfg.rate_limit_per_sec.clamp(1, 100_000) as u64;
    let burst_size = cfg.rate_limit_burst.clamp(1, 100_000);
    let key_extractor = PeerIpKey::new(cfg.trusted_proxies.clone());
    let governor_cfg = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(per_second)
            .burst_size(burst_size)
            .key_extractor(key_extractor)
            .finish()
            .expect("governor config valid (per_second>=1, burst_size>=1)"),
    );

    // Hand the limiter back to the caller wrapped in two closures so
    // the GC can be spawned at the right runtime stage. See
    // [`spawn_governor_gc`]. Two clones share the same Arc'd limiter
    // so they observe / mutate the same per-IP table.
    let limiter_for_retain = governor_cfg.limiter().clone();
    let limiter_for_len = governor_cfg.limiter().clone();
    let gc_handle = GovernorGcHandle {
        retain: Box::new(move || limiter_for_retain.retain_recent()),
        len: Box::new(move || limiter_for_len.len()),
    };

    // Admission control wraps the geocode endpoints only — health
    // and metrics are intentionally excluded so monitors and probes
    // are not rate-limited (#97 §4 standard practice). Same applies
    // to the governor: probes hit at fixed intervals from a finite
    // set of IPs, no need to gate them.
    let geocode_routes = Router::new()
        .route("/geocode", get(handlers::forward))
        .route("/geocode/reverse", get(handlers::reverse))
        .with_state(state.clone());
    let geocode_routes = mk_admission_layer(geocode_routes, state.admission.clone());
    let geocode_routes = geocode_routes.layer(GovernorLayer::new(governor_cfg));

    let unauth = Router::new()
        .route("/health", get(handlers::health))
        .with_state(state.clone());

    let api = geocode_routes
        .merge(unauth)
        .layer(RequestBodyLimitLayer::new(cfg.max_request_body_bytes))
        .layer(CompressionLayer::new())
        .layer(TimeoutLayer::with_status_code(
            StatusCode::REQUEST_TIMEOUT,
            cfg.request_timeout,
        ));

    let router = Router::new()
        .merge(api)
        .route(
            "/metrics",
            get(move || {
                let h = metric_handle.clone();
                async move { h.render() }
            }),
        )
        .layer(CatchPanicLayer::new())
        .layer(prometheus_layer)
        .layer(TraceLayer::new_for_http())
        .layer(cors);
    (router, gc_handle)
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::Request;

    #[test]
    fn cidr_v4_parses_and_contains() {
        let cidr = TrustedCidr::parse("10.0.0.0/8").unwrap();
        assert!(cidr.contains("10.1.2.3".parse().unwrap()));
        assert!(cidr.contains("10.255.255.255".parse().unwrap()));
        assert!(!cidr.contains("11.0.0.0".parse().unwrap()));
        assert!(!cidr.contains("9.255.255.255".parse().unwrap()));
    }

    #[test]
    fn cidr_v4_bare_ip_is_slash_32() {
        let cidr = TrustedCidr::parse("192.168.1.1").unwrap();
        assert!(cidr.contains("192.168.1.1".parse().unwrap()));
        assert!(!cidr.contains("192.168.1.2".parse().unwrap()));
    }

    #[test]
    fn cidr_v6_parses_and_contains() {
        let cidr = TrustedCidr::parse("2001:db8::/32").unwrap();
        assert!(cidr.contains("2001:db8::1".parse().unwrap()));
        assert!(cidr.contains("2001:db8:abcd:ef::1".parse().unwrap()));
        assert!(!cidr.contains("2001:db9::1".parse().unwrap()));
    }

    #[test]
    fn cidr_invalid_prefix_is_rejected() {
        assert!(TrustedCidr::parse("10.0.0.0/33").is_err());
        assert!(TrustedCidr::parse("not-an-ip/8").is_err());
        assert!(TrustedCidr::parse("10.0.0.0/abc").is_err());
    }

    #[test]
    fn parse_trusted_proxies_handles_csv_and_empty() {
        assert!(parse_trusted_proxies(None).unwrap().is_empty());
        assert!(parse_trusted_proxies(Some("")).unwrap().is_empty());
        let v = parse_trusted_proxies(Some("10.0.0.0/8, 192.168.0.0/16, 2001:db8::/32")).unwrap();
        assert_eq!(v.len(), 3);
    }

    fn make_req(peer: &str, xff: Option<&str>) -> Request<()> {
        let mut builder = Request::builder().method("GET").uri("/geocode");
        if let Some(xff) = xff {
            builder = builder.header("x-forwarded-for", xff);
        }
        let mut req = builder.body(()).unwrap();
        let peer_addr: SocketAddr = format!("{peer}:1234").parse().unwrap();
        req.extensions_mut().insert(ConnectInfo(peer_addr));
        req
    }

    #[test]
    fn peer_ip_key_returns_direct_peer_when_no_proxies_configured() {
        let key = PeerIpKey::default();
        let req = make_req("203.0.113.1", Some("198.51.100.1"));
        let extracted = key.extract(&req).unwrap();
        // No trusted proxies → peer IP wins, XFF is ignored.
        assert_eq!(extracted, "203.0.113.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn peer_ip_key_uses_xff_when_peer_is_trusted_proxy() {
        let key = PeerIpKey::new(vec![TrustedCidr::parse("10.0.0.0/8").unwrap()]);
        let req = make_req("10.1.2.3", Some("198.51.100.1, 10.5.5.5"));
        let extracted = key.extract(&req).unwrap();
        // Peer is trusted (10.x), XFF rightmost-non-trusted is the
        // first non-trusted IP walking from the right: 10.5.5.5 is
        // trusted (10.x), so we walk one more left and get the real
        // client at 198.51.100.1.
        assert_eq!(extracted, "198.51.100.1".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn peer_ip_key_keeps_peer_when_peer_is_not_trusted() {
        let key = PeerIpKey::new(vec![TrustedCidr::parse("10.0.0.0/8").unwrap()]);
        let req = make_req("203.0.113.7", Some("198.51.100.1"));
        let extracted = key.extract(&req).unwrap();
        // Peer is NOT a trusted proxy — XFF is ignored even when
        // present (a malicious client can't promote itself).
        assert_eq!(extracted, "203.0.113.7".parse::<IpAddr>().unwrap());
    }

    #[test]
    fn peer_ip_key_falls_back_to_peer_when_xff_only_lists_trusted_proxies() {
        let key = PeerIpKey::new(vec![TrustedCidr::parse("10.0.0.0/8").unwrap()]);
        // All XFF entries are trusted proxies. Means the peer is the
        // closest-to-client we know about.
        let req = make_req("10.1.2.3", Some("10.5.5.5, 10.6.6.6"));
        let extracted = key.extract(&req).unwrap();
        assert_eq!(extracted, "10.1.2.3".parse::<IpAddr>().unwrap());
    }
}

// =============================================================================
// Transport boot — REST / gRPC / both (#145)
// =============================================================================

/// Transport selection for the geocoder server.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Transport {
    /// REST/JSON only (Axum HTTP)
    Rest,
    /// gRPC Arrow Flight only (tonic)
    Grpc,
    /// Both REST and gRPC (default)
    Both,
}

impl Transport {
    pub fn parse(s: &str) -> anyhow::Result<Self> {
        match s.to_lowercase().as_str() {
            "rest" => Ok(Transport::Rest),
            "grpc" => Ok(Transport::Grpc),
            "both" => Ok(Transport::Both),
            other => anyhow::bail!("Invalid transport '{}'. Use: rest, grpc, both", other),
        }
    }
}

/// Default REST port (matches the legacy single-transport default).
pub const DEFAULT_REST_PORT: u16 = 3003;
/// Default gRPC Flight port. Mirrors butterfly-route's "REST + 1" rule.
pub const DEFAULT_GRPC_PORT: u16 = 3004;

/// Start the gRPC Arrow Flight server on `host:port` with a graceful
/// shutdown trigger.
pub async fn start_grpc_server(
    state: Arc<ServerState>,
    host: &str,
    port: u16,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> anyhow::Result<()> {
    let addr: SocketAddr = format!("{host}:{port}").parse()?;
    tracing::info!(addr = %addr, "gRPC Flight server listening");
    let svc = flight::build_flight_server(state);
    tonic::transport::Server::builder()
        .add_service(svc)
        .serve_with_shutdown(addr, shutdown)
        .await?;
    Ok(())
}
