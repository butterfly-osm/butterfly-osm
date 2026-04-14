//! Transit feed scraper.
//!
//! Operational model: transit feeds are downloaded *at rebuild time*, not
//! continuously by the running server. This is the same model used for the
//! OSM PBF — the operator runs the scraper on a cron (or as part of a
//! rebuild pipeline) to refresh everything, then restarts the server.
//!
//! This module exposes:
//!
//! * [`download_if_changed`] — primitive: GET a URL, write to `target` if
//!   the content's sha256 differs from `previous_sha`. Used by the CLI.
//! * [`fetch_all`] — entry point for the `transit-fetch` CLI subcommand.
//!   Downloads every static feed listed in the config (and optionally
//!   their one-shot GTFS-RT blobs), writing sha256 sidecars so the next
//!   run is a no-op for unchanged feeds.
//! * [`hash_file_if_exists`] — rehash an on-disk blob (used to seed
//!   `previous_sha` and by the unit tests).
//!
//! There is deliberately **no background task**. The server loads the
//! on-disk snapshot at startup and never downloads. Fresh data means
//! "re-run the scraper and restart the server".

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result};
use sha2::{Digest, Sha256};

use super::config::{FeedConfig, TransitConfig};

/// HTTP client used by the scraper. Bounded timeout, explicit user agent.
fn build_http_client() -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .user_agent("butterfly-route/2.0 transit-fetch")
        .timeout(Duration::from_secs(300))
        .build()
        .context("building reqwest client for transit-fetch")
}

/// Verification constraints applied at download time. Rejects bogus
/// responses (HTML squat pages, empty bodies, truncated fetches) before
/// anything touches `target`. Follow-up #100 moves this into butterfly-dl
/// as a generic library primitive — this is the scoped fix for the
/// current transit scraper.
#[derive(Debug, Clone, Default)]
pub struct ContentChecks {
    /// Minimum body size in bytes. If the body is smaller, the download
    /// fails before the file is written.
    pub min_bytes: Option<usize>,
    /// Required magic prefix at the very start of the body (e.g.
    /// `b"PK\x03\x04"` for a zip archive). Catches "200 OK with HTML
    /// masquerading as application/zip", which is the exact failure mode
    /// observed on STIB's iRail mirror (Huwise domain-squat page).
    pub magic_prefix: Option<&'static [u8]>,
}

impl ContentChecks {
    /// Checks appropriate for a GTFS static zip feed: PKZIP magic + at
    /// least 10 KB of content (real feeds are megabytes; 10 KB rejects
    /// squat pages and empty stubs without false-positives on a tiny
    /// but legitimate feed).
    pub fn gtfs_zip() -> Self {
        Self {
            min_bytes: Some(10 * 1024),
            magic_prefix: Some(b"PK\x03\x04"),
        }
    }
}

/// Download a URL to a target file, verifying SHA-256 and any
/// [`ContentChecks`] constraints.
///
/// Returns the new SHA-256 if the content changed (or if the file was
/// absent), or `None` if the content matches `previous_sha`.
///
/// If a content check fails, this returns an error, the on-disk `target`
/// is **not** modified, and the `.tmp` staging file is cleaned up.
/// Callers should treat the error as "this feed is currently unavailable"
/// and move on — one dead mirror must not block the rest of the fetch.
pub async fn download_if_changed(
    client: &reqwest::Client,
    url: &str,
    target: &Path,
    previous_sha: Option<[u8; 32]>,
    checks: &ContentChecks,
) -> Result<Option<[u8; 32]>> {
    let resp = client
        .get(url)
        .send()
        .await
        .with_context(|| format!("GET {url}"))?;
    if !resp.status().is_success() {
        anyhow::bail!("GET {url} returned {}", resp.status());
    }
    let body = resp
        .bytes()
        .await
        .with_context(|| format!("reading body of {url}"))?;

    // Content sanity checks — applied before anything hits disk so that
    // a bad response cannot pollute the target or leave a sidecar.
    if let Some(min) = checks.min_bytes
        && body.len() < min
    {
        anyhow::bail!(
            "GET {url}: body too small ({} bytes < min {}). The upstream \
             mirror may be returning a stub or squat page. Target left untouched.",
            body.len(),
            min
        );
    }
    if let Some(prefix) = checks.magic_prefix {
        if body.len() < prefix.len() || &body[..prefix.len()] != prefix {
            let preview_len = body.len().min(prefix.len());
            anyhow::bail!(
                "GET {url}: magic prefix mismatch. Expected {:02x?}, got \
                 {:02x?}. The upstream mirror is not serving a valid \
                 archive — likely an HTML squat page or a 200-OK error \
                 response. Target left untouched.",
                prefix,
                &body[..preview_len]
            );
        }
    }

    let mut hasher = Sha256::new();
    hasher.update(&body);
    let mut sha = [0u8; 32];
    sha.copy_from_slice(hasher.finalize().as_slice());

    if let Some(prev) = previous_sha
        && prev == sha
    {
        return Ok(None);
    }

    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    // Atomic: write to a .tmp, then rename. If the rename fails or
    // anything panics in between, the .tmp is cleaned up on the way out.
    let tmp = target.with_extension("tmp");
    tokio::fs::write(&tmp, &body)
        .await
        .with_context(|| format!("writing {}", tmp.display()))?;
    if let Err(e) = tokio::fs::rename(&tmp, target).await {
        // Best-effort cleanup of the staged file.
        let _ = tokio::fs::remove_file(&tmp).await;
        return Err(anyhow::anyhow!(e).context(format!(
            "renaming {} -> {}",
            tmp.display(),
            target.display()
        )));
    }

    Ok(Some(sha))
}

/// Hash an existing local file, if present.
pub fn hash_file_if_exists(path: &Path) -> Option<[u8; 32]> {
    let bytes = std::fs::read(path).ok()?;
    let mut h = Sha256::new();
    h.update(&bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(h.finalize().as_slice());
    Some(out)
}

fn sidecar_path(target: &Path) -> PathBuf {
    let mut s = target.as_os_str().to_os_string();
    s.push(".sha256");
    PathBuf::from(s)
}

/// Read a previously-written sha256 sidecar (hex-encoded).
pub fn read_sidecar(target: &Path) -> Option<[u8; 32]> {
    let text = std::fs::read_to_string(sidecar_path(target)).ok()?;
    let trimmed = text.trim();
    if trimmed.len() != 64 {
        return None;
    }
    let mut out = [0u8; 32];
    for (i, chunk) in trimmed.as_bytes().chunks(2).enumerate() {
        let s = std::str::from_utf8(chunk).ok()?;
        out[i] = u8::from_str_radix(s, 16).ok()?;
    }
    Some(out)
}

/// Write a sha256 sidecar next to `target`.
pub fn write_sidecar(target: &Path, sha: [u8; 32]) -> Result<()> {
    let path = sidecar_path(target);
    std::fs::write(&path, hex::encode(sha))
        .with_context(|| format!("writing sidecar {}", path.display()))?;
    Ok(())
}

/// Result of a single feed fetch attempt.
#[derive(Debug, Clone)]
pub enum FeedFetchOutcome {
    /// First download — no previous sha.
    Downloaded { sha: [u8; 32], bytes: usize },
    /// Content matches previous sha — no rewrite.
    Unchanged,
    /// Content differs from previous sha — rewritten.
    Updated { sha: [u8; 32], bytes: usize },
    /// Fetch failed — error is logged, never fatal for the whole run.
    Failed { error: String },
}

/// Report for one feed after the scraper runs.
#[derive(Debug, Clone)]
pub struct FeedFetchReport {
    pub feed_id: String,
    pub static_outcome: FeedFetchOutcome,
    pub rt_outcome: Option<FeedFetchOutcome>,
}

/// Download every feed listed in `config` into the transit directory.
///
/// This is the entry point for the `transit-fetch` CLI subcommand. One
/// fetch per static feed; optionally one fetch per RT URL. Failures on
/// individual feeds are recorded in the report and do **not** abort the
/// overall run — the scraper is expected to be resilient to a single
/// dead mirror.
///
/// A sha256 sidecar is written next to every successfully-downloaded
/// zip so the next invocation can skip unchanged feeds cheaply.
pub async fn fetch_all(
    config: &TransitConfig,
    include_realtime: bool,
) -> Result<Vec<FeedFetchReport>> {
    let client = build_http_client()?;
    let mut reports = Vec::with_capacity(config.feeds.len());

    for feed in &config.feeds {
        let static_outcome = fetch_one_static(&client, config, feed).await;
        let rt_outcome = if include_realtime && feed.rt_url.is_some() {
            Some(fetch_one_rt(&client, config, feed).await)
        } else {
            None
        };
        reports.push(FeedFetchReport {
            feed_id: feed.id.clone(),
            static_outcome,
            rt_outcome,
        });
    }

    Ok(reports)
}

async fn fetch_one_static(
    client: &reqwest::Client,
    config: &TransitConfig,
    feed: &FeedConfig,
) -> FeedFetchOutcome {
    let target = config.feed_zip_path(feed);
    // Prefer the sidecar if present; otherwise rehash whatever is on disk.
    let previous_sha = read_sidecar(&target).or_else(|| hash_file_if_exists(&target));
    let first_download = previous_sha.is_none();
    let checks = ContentChecks::gtfs_zip();
    match download_if_changed(client, &feed.url, &target, previous_sha, &checks).await {
        Ok(Some(sha)) => {
            let bytes = std::fs::metadata(&target)
                .map(|m| m.len() as usize)
                .unwrap_or(0);
            if let Err(e) = write_sidecar(&target, sha) {
                return FeedFetchOutcome::Failed {
                    error: format!("downloaded OK but failed to write sidecar: {e:#}"),
                };
            }
            if first_download {
                FeedFetchOutcome::Downloaded { sha, bytes }
            } else {
                FeedFetchOutcome::Updated { sha, bytes }
            }
        }
        Ok(None) => FeedFetchOutcome::Unchanged,
        Err(e) => FeedFetchOutcome::Failed {
            error: format!("{e:#}"),
        },
    }
}

async fn fetch_one_rt(
    client: &reqwest::Client,
    config: &TransitConfig,
    feed: &FeedConfig,
) -> FeedFetchOutcome {
    let Some(rt_url) = feed.rt_url.as_deref() else {
        return FeedFetchOutcome::Unchanged;
    };
    let target = config.feed_rt_path(feed);
    let previous_sha = read_sidecar(&target).or_else(|| hash_file_if_exists(&target));
    let first_download = previous_sha.is_none();
    // GTFS-RT is protobuf; no magic prefix check available, but require
    // at least 8 bytes so an empty-body 200 OK is still rejected.
    let checks = ContentChecks {
        min_bytes: Some(8),
        magic_prefix: None,
    };
    match download_if_changed(client, rt_url, &target, previous_sha, &checks).await {
        Ok(Some(sha)) => {
            let bytes = std::fs::metadata(&target)
                .map(|m| m.len() as usize)
                .unwrap_or(0);
            if let Err(e) = write_sidecar(&target, sha) {
                return FeedFetchOutcome::Failed {
                    error: format!("RT downloaded OK but failed to write sidecar: {e:#}"),
                };
            }
            if first_download {
                FeedFetchOutcome::Downloaded { sha, bytes }
            } else {
                FeedFetchOutcome::Updated { sha, bytes }
            }
        }
        Ok(None) => FeedFetchOutcome::Unchanged,
        Err(e) => FeedFetchOutcome::Failed {
            error: format!("{e:#}"),
        },
    }
}

/// Format a concise one-line human summary for a report. Used by the CLI
/// so a single `transit-fetch` run prints an at-a-glance status per feed.
pub fn format_report(report: &FeedFetchReport) -> String {
    let static_line = match &report.static_outcome {
        FeedFetchOutcome::Downloaded { bytes, .. } => format!("downloaded ({} bytes)", bytes),
        FeedFetchOutcome::Updated { bytes, .. } => format!("updated ({} bytes)", bytes),
        FeedFetchOutcome::Unchanged => "unchanged".to_string(),
        FeedFetchOutcome::Failed { error } => format!("FAILED: {error}"),
    };
    let rt_line = match &report.rt_outcome {
        None => String::new(),
        Some(FeedFetchOutcome::Downloaded { bytes, .. }) => {
            format!(" (rt downloaded, {} bytes)", bytes)
        }
        Some(FeedFetchOutcome::Updated { bytes, .. }) => format!(" (rt updated, {} bytes)", bytes),
        Some(FeedFetchOutcome::Unchanged) => " (rt unchanged)".to_string(),
        Some(FeedFetchOutcome::Failed { error }) => format!(" (rt FAILED: {error})"),
    };
    format!("{}: {}{}", report.feed_id, static_line, rt_line)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[tokio::test]
    async fn hash_file_works() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("blob.bin");
        std::fs::write(&p, b"hello world").unwrap();
        let h = hash_file_if_exists(&p).unwrap();
        // SHA-256("hello world") = b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9
        assert_eq!(
            hex::encode(h),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    #[tokio::test]
    async fn sidecar_roundtrip() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("feed.zip");
        std::fs::write(&p, b"payload").unwrap();
        let sha = hash_file_if_exists(&p).unwrap();
        write_sidecar(&p, sha).unwrap();
        assert_eq!(read_sidecar(&p), Some(sha));
    }

    #[tokio::test]
    async fn download_mocked_via_wiremock() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(b"payload-v1".to_vec()))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.bin");
        let client = build_http_client().unwrap();
        let checks = ContentChecks::default(); // no magic, no min — raw fetch
        let sha = download_if_changed(&client, &server.uri(), &target, None, &checks)
            .await
            .unwrap()
            .expect("first download should return Some");
        assert_eq!(std::fs::read(&target).unwrap(), b"payload-v1".to_vec());

        // Second call with same hash → Ok(None).
        let second = download_if_changed(&client, &server.uri(), &target, Some(sha), &checks)
            .await
            .unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn magic_prefix_rejects_html_masquerading_as_zip() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        let html = b"<!DOCTYPE html><html><head><title>This domain could not be found</title></head></html>\n"
            .repeat(200); // ~17 KB, above the min_bytes threshold
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/zip")
                    .set_body_bytes(html.clone()),
            )
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.zip");
        let sidecar = sidecar_path(&target);
        let tmp_sibling = target.with_extension("tmp");

        let client = build_http_client().unwrap();
        let checks = ContentChecks::gtfs_zip();
        let err = download_if_changed(&client, &server.uri(), &target, None, &checks)
            .await
            .expect_err("HTML-as-zip must error out before writing");

        let msg = format!("{err:#}");
        assert!(
            msg.contains("magic prefix mismatch"),
            "expected magic-prefix error, got: {msg}"
        );
        // Target must not exist, sidecar must not exist, tmp must be gone.
        assert!(
            !target.exists(),
            "target file must not exist after rejected download"
        );
        assert!(
            !sidecar.exists(),
            "sidecar must not exist after rejected download"
        );
        assert!(!tmp_sibling.exists(), ".tmp staging file must not linger");
    }

    #[tokio::test]
    async fn min_bytes_rejects_tiny_stub_zip() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        // Valid PKZIP magic but only 100 bytes — well below the 10 KB
        // threshold for a real GTFS feed.
        let mut body = b"PK\x03\x04".to_vec();
        body.extend_from_slice(&[0u8; 96]);
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.zip");
        let client = build_http_client().unwrap();
        let checks = ContentChecks::gtfs_zip();
        let err = download_if_changed(&client, &server.uri(), &target, None, &checks)
            .await
            .expect_err("tiny-stub must be rejected");
        let msg = format!("{err:#}");
        assert!(msg.contains("body too small"), "got: {msg}");
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn valid_zip_passes_content_checks() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        // Real PKZIP prefix + 20 KB of body data (crosses min_bytes).
        let mut body = b"PK\x03\x04".to_vec();
        body.extend(std::iter::repeat_n(0xAB, 20 * 1024));
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.zip");
        let client = build_http_client().unwrap();
        let checks = ContentChecks::gtfs_zip();
        let sha = download_if_changed(&client, &server.uri(), &target, None, &checks)
            .await
            .unwrap()
            .expect("should return Some on first download");
        assert_eq!(std::fs::read(&target).unwrap(), body);
        // Second call: unchanged.
        let second = download_if_changed(&client, &server.uri(), &target, Some(sha), &checks)
            .await
            .unwrap();
        assert!(second.is_none());
    }

    #[tokio::test]
    async fn fetch_all_downloads_missing_and_skips_cached() {
        use wiremock::matchers::method;
        use wiremock::{Mock, MockServer, ResponseTemplate};
        // Body must satisfy ContentChecks::gtfs_zip() (PKZIP magic + ≥10 KB).
        let mut body = b"PK\x03\x04".to_vec();
        body.extend(std::iter::repeat_n(0u8, 16 * 1024));
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let cfg = TransitConfig {
            data_dir: dir.path().to_path_buf(),
            feeds: vec![FeedConfig {
                id: "test".to_string(),
                url: server.uri(),
                rt_url: None,
                format: Default::default(),
            }],
            ..TransitConfig::default()
        };

        let report1 = fetch_all(&cfg, false).await.unwrap();
        assert_eq!(report1.len(), 1);
        assert!(matches!(
            report1[0].static_outcome,
            FeedFetchOutcome::Downloaded { .. }
        ));
        // Sidecar should now exist.
        let zip = cfg.feed_zip_path(&cfg.feeds[0]);
        assert!(zip.exists());
        assert!(sidecar_path(&zip).exists());

        // Second run: same content → Unchanged.
        let report2 = fetch_all(&cfg, false).await.unwrap();
        assert!(matches!(
            report2[0].static_outcome,
            FeedFetchOutcome::Unchanged
        ));
    }
}
