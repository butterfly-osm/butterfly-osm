//! Transit feed scraper — thin orchestrator over butterfly-dl.
//!
//! **All HTTP download logic lives in `butterfly-dl`** (issue #100).
//! This module iterates `TransitConfig.feeds`, computes the target
//! path for each feed (GTFS zip, NeTEx EPIP XML, optional GTFS-RT
//! blob), and fans out to [`butterfly_dl::verified::download_verified`]
//! **in parallel** via `futures::future::join_all`.
//!
//! Operational model: transit feeds are downloaded *at rebuild time*,
//! not continuously by the running server. This is the same model
//! used for OSM PBFs — the operator runs the scraper on a cron (or
//! the `transit-fetch` CLI subcommand, or the one-shot
//! `butterfly-dl belgium` region-indexed command) to refresh
//! everything, then restarts the server.
//!
//! Every download:
//!
//! - runs through `butterfly_dl::verified::download_verified` with
//!   extension-derived defaults (magic prefix + min-bytes + sha256
//!   sidecar + atomic .tmp → rename);
//! - shares butterfly-dl's process-wide `GLOBAL_CLIENT` (one
//!   connection pool, one TLS config, one set of tuned timeouts);
//! - runs **concurrently** with every other feed in the same
//!   config — bandwidth saturates per-origin instead of serialising.
//!
//! Failures on individual feeds are recorded in the report and do
//! NOT abort the overall run — the scraper is expected to be
//! resilient to a single dead mirror.

use std::path::PathBuf;

use anyhow::Result;
use butterfly_dl::verified::{Outcome, VerifiedOptions, download_verified};
use futures::future::join_all;

use super::config::TransitConfig;

/// Result of a single feed fetch attempt. Translated from
/// `butterfly_dl::verified::Outcome` plus the error string.
#[derive(Debug, Clone)]
pub enum FeedFetchOutcome {
    /// First download — no previous sidecar.
    Downloaded { sha: [u8; 32], bytes: usize },
    /// Content matches previous sidecar — nothing rewritten.
    Unchanged,
    /// Content differs from previous sidecar — rewritten.
    Updated { sha: [u8; 32], bytes: usize },
    /// Fetch failed — error is logged, never fatal for the whole run.
    Failed { error: String },
}

impl FeedFetchOutcome {
    fn from_verified(outcome: Outcome, had_previous: bool) -> Self {
        match outcome {
            Outcome::Downloaded { bytes, sha256 } => {
                if had_previous {
                    Self::Updated {
                        sha: sha256,
                        bytes: bytes as usize,
                    }
                } else {
                    Self::Downloaded {
                        sha: sha256,
                        bytes: bytes as usize,
                    }
                }
            }
            Outcome::Updated { bytes, sha256 } => Self::Updated {
                sha: sha256,
                bytes: bytes as usize,
            },
            Outcome::Unchanged => Self::Unchanged,
        }
    }
}

/// Report for one feed after the scraper runs.
#[derive(Debug, Clone)]
pub struct FeedFetchReport {
    pub feed_id: String,
    pub static_outcome: FeedFetchOutcome,
    pub rt_outcome: Option<FeedFetchOutcome>,
}

/// Download every feed listed in `config` into the transit directory
/// **in parallel**.
///
/// Each feed runs through `butterfly_dl::verified::download_verified`
/// with its target path's extension preset (zip / xml / pb …). Every
/// feed is dispatched as an independent tokio task via
/// `futures::future::join_all`, so mirrors on different origins
/// saturate their own bandwidth concurrently.
///
/// `include_realtime=true` additionally fetches each feed's
/// `rt_url` (GTFS-RT protobuf blob) when present. RT blobs use
/// unknown-extension defaults (no magic, no min-bytes, no sidecar)
/// because they're not archives and change on every poll.
pub async fn fetch_all(
    config: &TransitConfig,
    include_realtime: bool,
) -> Result<Vec<FeedFetchReport>> {
    // Build a per-feed work item holding the static target path,
    // the optional RT target path, and ownership of everything we
    // need to call `download_verified` without borrowing `config`.
    struct Work {
        feed_id: String,
        static_url: String,
        static_target: PathBuf,
        rt_url: Option<String>,
        rt_target: Option<PathBuf>,
    }
    let work: Vec<Work> = config
        .feeds
        .iter()
        .map(|feed| Work {
            feed_id: feed.id.clone(),
            static_url: feed.url.clone(),
            static_target: config.feed_zip_path(feed),
            rt_url: if include_realtime {
                feed.rt_url.clone()
            } else {
                None
            },
            rt_target: if include_realtime && feed.rt_url.is_some() {
                Some(config.feed_rt_path(feed))
            } else {
                None
            },
        })
        .collect();

    // One async task per feed, all fanned out via join_all.
    let tasks = work.into_iter().map(|w| async move {
        let static_outcome = fetch_one(&w.static_url, &w.static_target, false).await;
        let rt_outcome = match (w.rt_url.as_deref(), w.rt_target.as_ref()) {
            (Some(url), Some(target)) => Some(fetch_one(url, target, true).await),
            _ => None,
        };
        FeedFetchReport {
            feed_id: w.feed_id,
            static_outcome,
            rt_outcome,
        }
    });
    let reports = join_all(tasks).await;
    Ok(reports)
}

/// Fetch one URL into `target` via butterfly-dl. Extension-derived
/// defaults give GTFS zips + NeTEx XML their magic prefixes + min
/// bytes + sidecar handling automatically; GTFS-RT protobuf blobs
/// are intentionally fetched with unknown-extension defaults (no
/// magic, no min-bytes, no sidecar) because the `.pb` extension
/// isn't in the preset table and RT content changes on every poll.
async fn fetch_one(url: &str, target: &std::path::Path, is_realtime: bool) -> FeedFetchOutcome {
    let mut opts = VerifiedOptions::for_extension(target);
    if is_realtime {
        // Force a tiny-but-non-zero min so an empty 200 OK is still
        // rejected, but don't keep a sidecar (content is expected
        // to change every poll and the sidecar optimisation would
        // waste a disk round-trip).
        opts.min_bytes = Some(8);
        opts.sha256_sidecar = false;
        opts.skip_if_matches_sidecar = false;
    }
    // We don't know a priori whether there was a previous sidecar
    // without reading it; `download_verified` handles that
    // internally and returns `Updated`/`Downloaded` correctly only
    // when `skip_if_matches_sidecar` is set. Static feeds always
    // have it set (via `for_extension`), so the translation below
    // picks up the correct variant. For RT we disabled it, so we
    // treat every successful fetch as `Downloaded`.
    let had_previous =
        !is_realtime && butterfly_dl::verified::read_sidecar(target).is_some();
    match download_verified(url, target, &opts).await {
        Ok(outcome) => FeedFetchOutcome::from_verified(outcome, had_previous),
        Err(e) => FeedFetchOutcome::Failed {
            error: format!("{e:#}"),
        },
    }
}

/// Format a concise one-line human summary for a report. Used by the
/// CLI so a single `transit-fetch` run prints an at-a-glance status
/// per feed.
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

/// Compat helper kept so existing callers (`config::compute_provenance`
/// etc.) can still hash a local file when the transit pipeline wants
/// to decide whether a feed has rotated. Delegates to butterfly-dl's
/// identical primitive.
pub fn hash_file_if_exists(path: &std::path::Path) -> Option<[u8; 32]> {
    butterfly_dl::verified::hash_file_if_exists(path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;

    #[test]
    fn hash_file_works() {
        let dir = tempdir().unwrap();
        let p = dir.path().join("blob.bin");
        std::fs::write(&p, b"hello world").unwrap();
        let h = hash_file_if_exists(&p).unwrap();
        // SHA-256("hello world")
        assert_eq!(
            hex::encode(h),
            "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
        );
    }

    // The wiremock-backed download tests have moved to
    // `dl::verified::tests` where the verified-download primitive
    // actually lives. This file is now pure orchestration.
}
