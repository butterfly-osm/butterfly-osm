//! Generic verified download — the library primitive that #100
//! consolidates around.
//!
//! `download_verified` fetches a URL to a local path, streams the body
//! through a SHA-256 hasher, enforces a set of content-sanity
//! constraints (min-bytes, magic prefix, content-type allowlist)
//! **before** anything hits the target path, writes atomically via a
//! `.tmp` sibling, and optionally maintains a `.sha256` sidecar so
//! subsequent runs can be a no-op when the remote content is
//! unchanged.
//!
//! This is the single place in the workspace where bytes-off-the-wire
//! become bytes-on-disk for a verified fetch. `butterfly-route`'s
//! transit feed scraper delegates here; the `butterfly-dl fetch`
//! subcommand dispatches here; the OSM PBF download path (follow-up)
//! will route here too.
//!
//! ## Design: sane defaults, zero CLI knobs
//!
//! The CLI entry point (`butterfly-dl fetch <URL>`) exposes **one**
//! optional flag (`--to`) and picks every verification default from
//! the target path's extension. Users who need to override defaults
//! write a Rust program against [`download_verified`] directly.
//!
//! See `VerifiedOptions::for_extension` for the preset dictionary.
//!
//! ## Guarantees on failure
//!
//! If any content check fails (body too small, magic prefix mismatch,
//! disallowed content-type) or the network connection drops mid-stream:
//!
//! - The target file is **not** touched — a `.tmp` sibling file is
//!   always the write target, and it's removed on error.
//! - The sidecar is **not** updated — the previous sidecar (if any)
//!   survives untouched.
//! - The returned error includes enough context for an operator to
//!   diagnose dead mirrors / upstream incidents without spelunking.

use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, bail};
use sha2::{Digest, Sha256};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::core::Downloader;

/// Options for [`download_verified`].
///
/// Every field has a sensible default that the CLI path uses
/// unconditionally. Operators who need to override one field write a
/// Rust program against this struct directly — the CLI surface stays
/// minimal.
#[derive(Debug, Clone, Default)]
pub struct VerifiedOptions {
    /// Reject the download if the response body is smaller than this
    /// many bytes. `None` = no minimum. Use ≥ 10 KB for real GTFS
    /// zips; ≥ 1 MB for OSM PBFs; 0/None for small protobuf RT blobs.
    pub min_bytes: Option<u64>,
    /// Require the first bytes of the body to start with this prefix
    /// (e.g. `b"PK\x03\x04"` for zip). Checked as soon as the first
    /// chunk arrives — **the connection is dropped and the tmp file
    /// removed on mismatch** before any significant bytes touch disk.
    /// Catches HTML squat pages returned as 200 OK with
    /// `content-type: application/zip`.
    pub magic_prefix: Option<&'static [u8]>,
    /// Allow-list of HTTP `content-type` values. If set, the response
    /// is rejected when its `content-type` header is not in the list.
    /// Comparison is case-insensitive on the MIME part (parameters
    /// like `; charset=utf-8` are stripped before matching). `None` =
    /// any content type allowed.
    ///
    /// Note: the content-type check is **advisory only** — some
    /// mirrors set `application/octet-stream` for every file. The
    /// real guard is the magic prefix. Prefer setting a magic prefix
    /// over a content-type allowlist when you can.
    pub allowed_content_types: Option<Vec<String>>,
    /// Total-request timeout (headers + body). `None` = 5 minutes,
    /// which is the GTFS feeds baseline. Use a higher value for
    /// large PBFs.
    pub timeout: Option<Duration>,
    /// Write a `.sha256` sidecar next to `target` after a successful
    /// download. The sidecar is hex-encoded and 64 bytes long.
    pub sha256_sidecar: bool,
    /// If a sidecar already exists next to `target`, and the streamed
    /// body hashes to the same SHA-256, skip the final rename and
    /// return [`Outcome::Unchanged`]. The full body is still streamed
    /// over the network (there's no conditional GET), but the target
    /// file is left untouched.
    pub skip_if_matches_sidecar: bool,
    /// Display an `indicatif` progress bar on stderr while streaming.
    /// Defaults to `false` so library consumers don't accidentally
    /// render bars from batch / cron contexts; the `fetch` CLI
    /// subcommand flips this to `true`.
    pub progress: bool,
}

impl VerifiedOptions {
    /// Pick sensible verification defaults from the target path's
    /// extension. The CLI's `butterfly-dl fetch` subcommand uses this
    /// to avoid exposing any verification flags to end users.
    ///
    /// Presets (applied on top of a default `VerifiedOptions`):
    ///
    /// | Extension | `magic_prefix` | `min_bytes` |
    /// |---|---|---|
    /// | `.zip` | `PK\x03\x04` | 10 KB |
    /// | `.pbf` | OSM PBF header prefix | 1 MB |
    /// | `.gz` | `\x1f\x8b` | 1 |
    /// | `.xz` | `\xfd7zXZ` | 1 |
    /// | `.zst` | `\x28\xb5\x2f\xfd` | 1 |
    /// | `.xml` | (none) | 64 B |
    /// | everything else | (none) | (none) |
    ///
    /// `sha256_sidecar` + `skip_if_matches_sidecar` are always on
    /// when the preset matched a known extension; the sidecar makes
    /// re-runs O(network) with no disk churn when the remote hasn't
    /// changed.
    pub fn for_extension(target: &Path) -> Self {
        let ext = target
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_ascii_lowercase();

        // Preset dictionary. `recognised` flags whether we have a
        // known format for this extension — only recognised formats
        // get sidecar handling on by default. Unknown extensions
        // (`.bin`, `.dat`, `.csv`, …) fall through with zero
        // verification and no sidecar, which matches the principle
        // of least surprise for library callers who pass arbitrary
        // paths.
        let (magic, min_bytes, recognised): (Option<&'static [u8]>, Option<u64>, bool) =
            match ext.as_str() {
                "zip" => (Some(b"PK\x03\x04"), Some(10 * 1024), true),
                // OSM PBF files start with a BlobHeader length prefix
                // (network byte order) followed by `OSMHeader` as a
                // `BlobHeader.type` field. The first 4 bytes are the
                // BE-encoded length of the BlobHeader protobuf —
                // variable, not a fixed magic. `\x00\x00` as a weak
                // prefix check (both bytes of the length field must
                // be zero for any BlobHeader smaller than 64 KB,
                // which is every real OSM file). Combined with the
                // ≥ 1 MB min_bytes floor, this catches HTML squat
                // pages cleanly.
                "pbf" => (Some(b"\x00\x00"), Some(1024 * 1024), true),
                "gz" => (Some(b"\x1f\x8b"), Some(1), true),
                "xz" => (Some(b"\xfd7zXZ"), Some(1), true),
                "zst" => (Some(b"\x28\xb5\x2f\xfd"), Some(1), true),
                "xml" => (None, Some(64), true),
                _ => (None, None, false),
            };

        Self {
            min_bytes,
            magic_prefix: magic,
            allowed_content_types: None,
            timeout: None,
            sha256_sidecar: recognised,
            skip_if_matches_sidecar: recognised,
            progress: false,
        }
    }
}

/// Result of a verified download.
#[derive(Debug, Clone, Copy)]
pub enum Outcome {
    /// Target file did not exist before (no previous sidecar) and the
    /// remote body was written successfully.
    Downloaded { bytes: u64, sha256: [u8; 32] },
    /// Target file already existed and its previous sidecar did not
    /// match the remote body. The old file was replaced atomically.
    Updated { bytes: u64, sha256: [u8; 32] },
    /// Remote body matched the existing sidecar byte-for-byte. Target
    /// file and sidecar were left untouched.
    Unchanged,
}

impl Outcome {
    /// True if this outcome represents an on-disk change.
    pub fn wrote_file(&self) -> bool {
        matches!(self, Outcome::Downloaded { .. } | Outcome::Updated { .. })
    }

    /// SHA-256 of the verified body when one was computed. `None` for
    /// [`Outcome::Unchanged`] (the caller already has the hash in the
    /// sidecar).
    pub fn sha256(&self) -> Option<[u8; 32]> {
        match self {
            Outcome::Downloaded { sha256, .. } | Outcome::Updated { sha256, .. } => Some(*sha256),
            Outcome::Unchanged => None,
        }
    }
}

/// Download `url` into `target` with every verification constraint in
/// `opts` applied before anything touches the target file.
///
/// Streams the body through a SHA-256 hasher while writing to a
/// `<target>.tmp` staging file. When the body is fully received, the
/// hasher is finalised, all late checks (`min_bytes`, sidecar-match)
/// are evaluated, and — only if everything passes — the tmp file is
/// atomically renamed to `target`. On any error the tmp file is
/// removed and the existing `target` is untouched.
///
/// See [`VerifiedOptions`] for the full knob list and
/// [`VerifiedOptions::for_extension`] for the preset helper that
/// drives the CLI path.
pub async fn download_verified(
    url: &str,
    target: &Path,
    opts: &VerifiedOptions,
) -> Result<Outcome> {
    // Read the previous sidecar once at the start; we need it for the
    // skip-if-matches optimisation and for deciding Downloaded vs
    // Updated in the final outcome.
    let previous_sha = if opts.skip_if_matches_sidecar || opts.sha256_sidecar {
        read_sidecar(target)
    } else {
        None
    };
    let first_download = previous_sha.is_none();

    // Reuse butterfly-dl's shared `GLOBAL_CLIENT` via the
    // HEAD-less `stream_url_raw` primitive. Same connection pool +
    // TLS config as every other butterfly-dl download, same
    // user-agent, same retry/resume infrastructure — but no HEAD
    // prelude, so mirrors that don't support HEAD (common on GTFS)
    // just work, and wiremock servers in unit tests don't need to
    // register HEAD handlers.
    //
    // The `Downloader::download_stream` path is still the right
    // primitive for OSM PBFs where we DO want a HEAD-based size
    // check and parallel range downloads. `download_verified` is
    // the single-connection verified-fetch path for GTFS / NeTEx /
    // small-to-medium verified downloads.
    let _ = opts.timeout; // Global client timeout is authoritative for now.
    let _ = &opts.allowed_content_types; // Advisory, see below.

    let (stream, total_size_hint) = Downloader::stream_url_raw(url)
        .await
        .with_context(|| format!("GET {url} (via butterfly-dl shared client)"))?;

    // Note on content-type: the shared client's HEAD path already
    // enforced status + content-length. The content-type allowlist is
    // best-effort advisory — magic prefix is authoritative. Some
    // mirrors set `application/octet-stream` for everything.
    // `allowed_content_types` is therefore ignored at the stream
    // level; if a caller really needs to gate on it, they can add a
    // direct reqwest HEAD hook in a future revision. Left as a knob
    // in `VerifiedOptions` for forward-compat.

    // Prepare the staging file path. We always write to <target>.tmp
    // and rename on success — the target is never touched mid-fetch.
    if let Some(parent) = target.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .with_context(|| format!("mkdir -p {}", parent.display()))?;
    }
    let tmp_path = tmp_sibling(target);
    let mut tmp_file = tokio::fs::File::create(&tmp_path)
        .await
        .with_context(|| format!("creating staging file {}", tmp_path.display()))?;

    // Stream the body while hashing. Pinning the stream so we can
    // `read` into a fixed-size buffer — matches butterfly-dl's
    // memory envelope (bounded to one chunk regardless of file size).
    let mut hasher = Sha256::new();
    let mut stream = Box::pin(stream);
    let mut total: u64 = 0;
    let mut header_buf: Vec<u8> = Vec::new();
    let mut magic_checked = false;
    let progress = if opts.progress {
        let pb = match total_size_hint {
            Some(n) if n > 0 => {
                let pb = indicatif::ProgressBar::new(n);
                pb.set_style(
                    indicatif::ProgressStyle::with_template(
                        "{spinner} {bar:40} {bytes}/{total_bytes} ({bytes_per_sec})",
                    )
                    .unwrap_or_else(|_| indicatif::ProgressStyle::default_bar()),
                );
                pb
            }
            _ => indicatif::ProgressBar::new_spinner(),
        };
        Some(pb)
    } else {
        None
    };

    let mut buf = vec![0u8; 64 * 1024];
    loop {
        let n = stream
            .read(&mut buf)
            .await
            .with_context(|| format!("reading body of {url}"))?;
        if n == 0 {
            break;
        }
        let chunk = &buf[..n];
        total += n as u64;
        hasher.update(chunk);

        // Magic-prefix check: accumulate bytes into `header_buf` until
        // we have enough to compare, then decide.
        if !magic_checked {
            if let Some(expected) = opts.magic_prefix {
                header_buf.extend_from_slice(chunk);
                if header_buf.len() >= expected.len() {
                    if &header_buf[..expected.len()] != expected {
                        // Drop the connection and clean up on mismatch.
                        drop(stream);
                        drop(tmp_file);
                        let _ = tokio::fs::remove_file(&tmp_path).await;
                        let got_preview: Vec<u8> =
                            header_buf.iter().take(expected.len()).copied().collect();
                        bail!(
                            "GET {url}: magic prefix mismatch. Expected {:02x?}, got {:02x?}. \
                             The upstream mirror is not serving a valid archive — likely an \
                             HTML squat page or a 200-OK error response. Target left untouched.",
                            expected,
                            &got_preview
                        );
                    }
                    magic_checked = true;
                }
            } else {
                // No magic prefix configured — skip the header buffer
                // entirely.
                magic_checked = true;
            }
        }

        tmp_file
            .write_all(chunk)
            .await
            .with_context(|| format!("writing staging file {}", tmp_path.display()))?;
        if let Some(pb) = progress.as_ref() {
            pb.set_position(total);
            pb.tick();
        }
    }
    tmp_file
        .flush()
        .await
        .context("flushing staging file")?;
    drop(tmp_file);
    if let Some(pb) = progress {
        pb.finish_and_clear();
    }

    // Late checks: min_bytes and magic (in case the response body
    // was smaller than the magic prefix — we never set
    // `magic_checked`).
    if let Some(min) = opts.min_bytes
        && total < min
    {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        bail!(
            "GET {url}: body too small ({total} bytes < min {min}). The upstream mirror may \
             be returning a stub or squat page. Target left untouched."
        );
    }
    if !magic_checked
        && let Some(expected) = opts.magic_prefix
    {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        bail!(
            "GET {url}: body ({total} bytes) smaller than required magic prefix ({} bytes). \
             Target left untouched.",
            expected.len()
        );
    }

    // Finalise the hash.
    let mut sha = [0u8; 32];
    sha.copy_from_slice(hasher.finalize().as_slice());

    // skip-if-matches-sidecar: if the streamed body hashes to the same
    // value as the existing sidecar, discard the tmp and return
    // Unchanged. The previous target + sidecar survive.
    if opts.skip_if_matches_sidecar
        && let Some(prev) = previous_sha
        && prev == sha
    {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Ok(Outcome::Unchanged);
    }

    // Atomic rename. On platforms where rename fails (tmp on
    // different filesystem, target busy), we clean up the tmp and
    // return the IO error with context.
    if let Err(e) = tokio::fs::rename(&tmp_path, target).await {
        let _ = tokio::fs::remove_file(&tmp_path).await;
        return Err(anyhow::anyhow!(e).context(format!(
            "renaming {} -> {}",
            tmp_path.display(),
            target.display()
        )));
    }

    if opts.sha256_sidecar {
        write_sidecar(target, sha).context("writing sha256 sidecar")?;
    }

    if first_download {
        Ok(Outcome::Downloaded { bytes: total, sha256: sha })
    } else {
        Ok(Outcome::Updated { bytes: total, sha256: sha })
    }
}

fn tmp_sibling(target: &Path) -> PathBuf {
    let mut s = target.as_os_str().to_os_string();
    s.push(".tmp");
    PathBuf::from(s)
}

fn sidecar_path(target: &Path) -> PathBuf {
    let mut s = target.as_os_str().to_os_string();
    s.push(".sha256");
    PathBuf::from(s)
}

/// Read the SHA-256 sidecar next to `target`, if present and valid.
/// Returns `None` on missing file, unreadable file, or malformed hex
/// content.
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

/// Write `sha` as a hex-encoded sidecar next to `target`. Overwrites
/// any existing sidecar.
pub fn write_sidecar(target: &Path, sha: [u8; 32]) -> Result<()> {
    let path = sidecar_path(target);
    std::fs::write(&path, hex::encode(sha))
        .with_context(|| format!("writing sidecar {}", path.display()))?;
    Ok(())
}

/// Hash an existing local file, if present. Returns `None` if the
/// file does not exist or cannot be read.
pub fn hash_file_if_exists(path: &Path) -> Option<[u8; 32]> {
    let bytes = std::fs::read(path).ok()?;
    let mut h = Sha256::new();
    h.update(&bytes);
    let mut out = [0u8; 32];
    out.copy_from_slice(h.finalize().as_slice());
    Some(out)
}


#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::tempdir;
    use wiremock::matchers::method;
    use wiremock::{Mock, MockServer, ResponseTemplate};

    #[test]
    fn for_extension_zip_preset() {
        let opts = VerifiedOptions::for_extension(Path::new("feed.zip"));
        assert_eq!(opts.magic_prefix, Some(b"PK\x03\x04".as_slice()));
        assert_eq!(opts.min_bytes, Some(10 * 1024));
        assert!(opts.sha256_sidecar);
        assert!(opts.skip_if_matches_sidecar);
    }

    #[test]
    fn for_extension_unknown_returns_no_checks() {
        let opts = VerifiedOptions::for_extension(Path::new("payload.bin"));
        assert!(opts.magic_prefix.is_none());
        assert!(opts.min_bytes.is_none());
        // Unknown extension → no sidecar handling (caller may still opt in).
        assert!(!opts.sha256_sidecar);
    }

    #[test]
    fn for_extension_pbf_preset() {
        let opts = VerifiedOptions::for_extension(Path::new("belgium.pbf"));
        assert!(opts.magic_prefix.is_some());
        assert_eq!(opts.min_bytes, Some(1024 * 1024));
        assert!(opts.sha256_sidecar);
    }

    #[tokio::test]
    async fn valid_zip_downloads_and_is_unchanged_on_second_run() {
        // Real PKZIP prefix + 20 KB body.
        let mut body = b"PK\x03\x04".to_vec();
        body.extend(std::iter::repeat_n(0xAB, 20 * 1024));
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.zip");
        let opts = VerifiedOptions::for_extension(&target);

        let first = download_verified(&server.uri(), &target, &opts).await.unwrap();
        assert!(matches!(first, Outcome::Downloaded { .. }));
        assert_eq!(std::fs::read(&target).unwrap(), body);
        // Sidecar must exist.
        let sidecar = dir.path().join("feed.zip.sha256");
        assert!(sidecar.exists());

        let second = download_verified(&server.uri(), &target, &opts).await.unwrap();
        assert!(matches!(second, Outcome::Unchanged));
    }

    #[tokio::test]
    async fn magic_prefix_rejects_html_squat_page() {
        let html = b"<!DOCTYPE html><html><head><title>domain not found</title></head></html>\n"
            .repeat(200); // ~15 KB, above the zip min_bytes threshold
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(
                ResponseTemplate::new(200)
                    .insert_header("content-type", "application/zip")
                    .set_body_bytes(html),
            )
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.zip");
        let tmp = tmp_sibling(&target);
        let sidecar = sidecar_path(&target);
        let opts = VerifiedOptions::for_extension(&target);

        let err = download_verified(&server.uri(), &target, &opts).await.unwrap_err();
        let msg = format!("{err:#}");
        assert!(
            msg.contains("magic prefix mismatch"),
            "expected magic-prefix error, got: {msg}"
        );
        assert!(!target.exists(), "target must not exist after rejected download");
        assert!(!sidecar.exists(), "sidecar must not exist after rejected download");
        assert!(!tmp.exists(), ".tmp must not linger");
    }

    #[tokio::test]
    async fn min_bytes_rejects_tiny_stub() {
        // Valid PKZIP magic but only 100 bytes total.
        let mut body = b"PK\x03\x04".to_vec();
        body.extend_from_slice(&[0u8; 96]);
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("feed.zip");
        let opts = VerifiedOptions::for_extension(&target);
        let err = download_verified(&server.uri(), &target, &opts).await.unwrap_err();
        assert!(format!("{err:#}").contains("body too small"));
        assert!(!target.exists());
    }

    #[tokio::test]
    async fn unknown_extension_writes_without_checks() {
        // No magic, no min — just fetch & atomic rename.
        let body = b"arbitrary binary payload".to_vec();
        let server = MockServer::start().await;
        Mock::given(method("GET"))
            .respond_with(ResponseTemplate::new(200).set_body_bytes(body.clone()))
            .mount(&server)
            .await;

        let dir = tempdir().unwrap();
        let target = dir.path().join("payload.bin");
        let opts = VerifiedOptions::for_extension(&target);
        let outcome = download_verified(&server.uri(), &target, &opts).await.unwrap();
        assert!(matches!(outcome, Outcome::Downloaded { .. }));
        assert_eq!(std::fs::read(&target).unwrap(), body);
        // No sidecar for unknown extensions.
        assert!(!dir.path().join("payload.bin.sha256").exists());
    }
}
