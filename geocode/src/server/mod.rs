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
//! Outermost first (the order shown is the order requests are
//! processed):
//!
//! 1. CORS — handle preflight, inject permissive headers (production
//!    deployments should narrow `Access-Control-Allow-Origin` via a
//!    reverse proxy).
//! 2. TraceLayer — span every request with a `tracing` `INFO` log.
//! 3. Prometheus — collect HTTP-level histograms / counters.
//! 4. CatchPanicLayer — convert panics into 500 instead of dropping
//!    the connection.
//! 5. Compression — gzip/brotli on responses based on `Accept-Encoding`.
//! 6. Timeout — `cfg.request_timeout` server-side cap, 408 on expiry.
//! 7. RequestBodyLimit — `cfg.max_request_body_bytes` cap on request
//!    bodies (POST endpoints will use this once they land; GET ignores
//!    it but having the layer up means a future POST endpoint can't
//!    accidentally accept unbounded uploads).
//! 8. Governor (per-IP HTTP-level rate limit) — runs *before*
//!    admission so abusive clients are dropped at the cheapest layer.
//! 9. Admission (token-bucket cost-based gate) — only on `/geocode*`,
//!    not on `/health` or `/metrics` so monitors are never throttled.

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
#[derive(Debug, Clone, Copy)]
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
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            rate_limit_per_sec: 100,
            rate_limit_burst: 200,
            request_timeout: Duration::from_secs(30),
            max_request_body_bytes: 4 * 1024,
        }
    }
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
#[derive(Debug, Clone, Copy, Default)]
pub struct PeerIpKey;

impl KeyExtractor for PeerIpKey {
    type Key = IpAddr;

    fn extract<T>(&self, req: &Request<T>) -> Result<Self::Key, GovernorError> {
        let ip = req
            .extensions()
            .get::<ConnectInfo<SocketAddr>>()
            .map(|ConnectInfo(a)| a.ip())
            .unwrap_or(IpAddr::V4(Ipv4Addr::LOCALHOST));
        Ok(ip)
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
    build_router_with_config(state, ServerConfig::default())
}

/// Construct the full HTTP router with an explicit [`ServerConfig`].
pub fn build_router_with_config(state: Arc<ServerState>, cfg: ServerConfig) -> Router {
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
    let governor_cfg = Arc::new(
        GovernorConfigBuilder::default()
            .per_second(per_second)
            .burst_size(burst_size)
            .key_extractor(PeerIpKey)
            .finish()
            .expect("governor config valid (per_second>=1, burst_size>=1)"),
    );

    // Background task to garbage-collect the per-IP map so long-running
    // deployments don't leak memory on rotating client IPs. The
    // `governor` README recommends pruning every 60 s.
    let governor_limiter = governor_cfg.limiter().clone();
    tokio::spawn(async move {
        let interval = Duration::from_secs(60);
        loop {
            tokio::time::sleep(interval).await;
            tracing::trace!(
                rate_limit_storage_size = governor_limiter.len(),
                "governor retain_recent"
            );
            governor_limiter.retain_recent();
        }
    });

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

    Router::new()
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
        .layer(cors)
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
