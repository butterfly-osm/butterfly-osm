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
//! resilient to a single dead mirror — but the RUN is not: see
//! [`fetch_outcome`], which turns any failed feed into a failed
//! command so a rotted URL cannot sit unnoticed.
//!
//! The one way to proceed without an operator is to DECLARE it
//! (`[[excluded_feeds]]` in `transit.toml`, #603): a declared feed is
//! never requested, is named by [`format_exclusions`] on every run, and
//! is reported missing by the timetable it is absent from. Nothing else
//! is skipped, so an undeclared 404 still fails the run.

use std::path::{Path, PathBuf};

use anyhow::Result;
use butterfly_dl::verified::{Outcome, VerifiedOptions, download_verified};
use futures::future::join_all;

use super::config::{ExcludedFeed, TransitConfig};

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
    /// Fetch failed. Reported per feed, and fatal for the run — see
    /// [`fetch_outcome`].
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
    /// The URL that was requested. Carried on the report so a failure can
    /// name it — a 404 whose message omits the URL tells the operator
    /// nothing about which of several config sources is wrong.
    pub url: String,
    pub static_outcome: FeedFetchOutcome,
    pub rt_outcome: Option<FeedFetchOutcome>,
}

/// Turn a completed run into a process outcome, returning how many feeds
/// downloaded.
///
/// A feed that 404s used to be printed, counted, and then ignored: the
/// command exited 0 as long as one other feed had worked. So a default
/// URL could rot for months while every rebuild quietly produced a
/// timetable one operator short, and nothing downstream said so — the
/// server logs a warning for a feed that is not on disk and serves the
/// rest. A failed fetch is now a failed run, naming every feed that
/// failed, the URL it asked for, and the file that decides that URL.
///
/// Real-time snapshots are deliberately not fatal: `rt_url` is an
/// optional one-shot blob the loader is documented to skip when it is
/// missing or unreadable, and the schedule does not depend on it. Its
/// failures are still printed per feed.
pub fn fetch_outcome(reports: &[FeedFetchReport], config_path: &Path) -> anyhow::Result<usize> {
    let failed: Vec<&FeedFetchReport> = reports
        .iter()
        .filter(|r| matches!(r.static_outcome, FeedFetchOutcome::Failed { .. }))
        .collect();
    if failed.is_empty() {
        return Ok(reports.len());
    }

    let mut msg = format!(
        "{} of {} transit feed(s) failed to download:",
        failed.len(),
        reports.len()
    );
    for r in &failed {
        let FeedFetchOutcome::Failed { error } = &r.static_outcome else {
            unreachable!("filtered to failures");
        };
        msg.push_str(&format!("\n  {} {} — {}", r.feed_id, r.url, error));
    }
    msg.push_str(&format!(
        "\nFix or remove the feed in {}{}",
        config_path.display(),
        if config_path.is_file() {
            "."
        } else {
            " (create it to override the shipped region feed list)."
        }
    ));
    anyhow::bail!(msg)
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
    // #603: a declared exclusion is honoured HERE, so it has one meaning —
    // the operator is not downloaded (and `load_from_disk` will not merge
    // it either). Undeclared feeds are all still attempted, and any failure
    // among them still fails the run via `fetch_outcome`.
    let work: Vec<Work> = config
        .active_feeds()
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
            url: w.static_url,
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
        // #418: RT content changes every poll — never skip the transfer.
        opts.conditional_get = false;
    }
    // We don't know a priori whether there was a previous sidecar
    // without reading it; `download_verified` handles that
    // internally and returns `Updated`/`Downloaded` correctly only
    // when `skip_if_matches_sidecar` is set. Static feeds always
    // have it set (via `for_extension`), so the translation below
    // picks up the correct variant. For RT we disabled it, so we
    // treat every successful fetch as `Downloaded`.
    let had_previous = !is_realtime && butterfly_dl::verified::read_sidecar(target).is_some();
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

/// One line per knowingly-excluded operator (#603), for the run's output.
///
/// Printed on EVERY run that honours a declaration. A declaration nobody
/// ever sees is a silent skip with extra steps — which is how a rotted URL
/// survived for months — so the exclusion is as loud as the failure it
/// replaces, just not fatal.
pub fn format_exclusions(excluded: &[ExcludedFeed]) -> Vec<String> {
    excluded
        .iter()
        .map(|e| {
            format!(
                "KNOWINGLY EXCLUDED: {} is not fetched and will be missing from the timetable — {}",
                e.id, e.reason
            )
        })
        .collect()
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

    /// #537: a feed that 404s must be REPORTED, not swallowed. The run
    /// used to exit 0 whenever at least one other feed worked, which is
    /// how two dead default URLs survived unnoticed until a calendar ran
    /// out. The failure has to carry what the operator needs to act:
    /// which feed, which URL, and which file decides that URL.
    #[test]
    fn a_failed_feed_fails_the_run_and_says_where_to_fix_it() {
        use std::path::Path;

        let report = |id: &str, url: &str, outcome: FeedFetchOutcome| FeedFetchReport {
            feed_id: id.to_string(),
            url: url.to_string(),
            static_outcome: outcome,
            rt_outcome: None,
        };
        let ok = |id: &str| {
            report(
                id,
                "https://example.org/ok.zip",
                FeedFetchOutcome::Unchanged,
            )
        };
        let dead = |id: &str, url: &str| {
            report(
                id,
                url,
                FeedFetchOutcome::Failed {
                    error: "GET returned HTTP 404 Not Found".to_string(),
                },
            )
        };
        let toml = Path::new("/data/transit/transit.toml");

        assert_eq!(
            fetch_outcome(&[ok("sncb"), ok("tec")], toml).expect("all fetched"),
            2
        );

        // One dead feed among three that worked is still a failed run.
        let err = fetch_outcome(
            &[
                ok("sncb"),
                dead("delijn", "https://example.org/gone/delijn.zip"),
                ok("tec"),
            ],
            toml,
        )
        .expect_err("a dead feed must not pass as success");
        let msg = format!("{err:#}");
        for needle in [
            "delijn",
            "https://example.org/gone/delijn.zip",
            "404",
            "transit.toml",
        ] {
            assert!(msg.contains(needle), "{msg} must mention {needle}");
        }
        assert!(
            !msg.contains("sncb") && !msg.contains("tec"),
            "only the failures belong in the message: {msg}"
        );

        // Every feed dead: all of them are named, not just the first.
        let err = fetch_outcome(
            &[
                dead("delijn", "https://example.org/a.zip"),
                dead("stib", "https://example.org/b.xml"),
            ],
            toml,
        )
        .expect_err("every feed dead is a failed run");
        let msg = format!("{err:#}");
        assert!(msg.contains("delijn") && msg.contains("stib"), "{msg}");

        // A real-time snapshot is optional by contract; its failure is
        // printed but does not fail the run.
        let mut rt_only = ok("sncb");
        rt_only.rt_outcome = Some(FeedFetchOutcome::Failed {
            error: "GET returned HTTP 503".to_string(),
        });
        assert_eq!(
            fetch_outcome(&[rt_only], toml).expect("an rt blob is not the schedule"),
            1
        );
    }

    /// #603: the ONLY way a broken operator stops failing the run is an
    /// explicit declaration. Undeclared: still fatal. Declared: never even
    /// requested, so there is no failure to swallow — and the run says so.
    #[test]
    fn an_undeclared_failure_still_fails_and_a_declared_one_does_not() {
        use super::super::config::{ExcludedFeed, FeedConfig, TransitConfig};
        use std::path::Path;

        let feed = |id: &str| FeedConfig {
            id: id.to_string(),
            url: format!("https://example.org/{id}.zip"),
            rt_url: None,
            format: Default::default(),
        };
        // Whatever the fetcher would attempt, one of these two answers 404.
        let attempt = |cfg: &TransitConfig| -> Vec<FeedFetchReport> {
            cfg.active_feeds()
                .map(|f| FeedFetchReport {
                    feed_id: f.id.clone(),
                    url: f.url.clone(),
                    static_outcome: if f.id == "regional_b" {
                        FeedFetchOutcome::Failed {
                            error: "GET returned HTTP 404 Not Found".to_string(),
                        }
                    } else {
                        FeedFetchOutcome::Unchanged
                    },
                    rt_outcome: None,
                })
                .collect()
        };
        let toml = Path::new("/data/transit/transit.toml");

        // Undeclared: the broken operator is attempted and fails the run.
        let plain = TransitConfig {
            feeds: vec![feed("national"), feed("regional_b")],
            ..TransitConfig::default()
        };
        let reports = attempt(&plain);
        assert_eq!(reports.len(), 2, "nothing is skipped without a declaration");
        let err = fetch_outcome(&reports, toml).expect_err("an undeclared 404 must fail the run");
        assert!(format!("{err:#}").contains("regional_b"), "{err:#}");

        // Declared: never requested, the run succeeds without it, and the
        // remaining feeds are still judged exactly as before.
        let declared = TransitConfig {
            excluded_feeds: vec![ExcludedFeed {
                id: "regional_b".to_string(),
                reason: "published address 404s at the source; tracked upstream".to_string(),
            }],
            ..plain.clone()
        };
        let reports = attempt(&declared);
        assert_eq!(reports.len(), 1);
        assert_eq!(reports[0].feed_id, "national");
        assert_eq!(
            fetch_outcome(&reports, toml).expect("a declared exclusion is not a failure"),
            1
        );

        // ... and the declaration is loud: named, with its reason, on every run.
        let lines = format_exclusions(&declared.excluded_feeds);
        assert_eq!(lines.len(), 1);
        assert!(lines[0].contains("regional_b"), "{}", lines[0]);
        assert!(lines[0].contains("404s at the source"), "{}", lines[0]);
        assert!(
            format_exclusions(&plain.excluded_feeds).is_empty(),
            "nothing to say when nothing is declared"
        );

        // Declaring the WRONG operator does not rescue the broken one.
        let wrong = TransitConfig {
            excluded_feeds: vec![ExcludedFeed {
                id: "national".to_string(),
                reason: "unrelated".to_string(),
            }],
            ..plain
        };
        fetch_outcome(&attempt(&wrong), toml).expect_err("only the declared operator is exempt");
    }

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
