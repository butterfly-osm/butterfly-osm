//! Container liveness probe, in-binary (#573).
//!
//! The runtime image used to ship `curl` for a single purpose: the
//! Dockerfile `HEALTHCHECK`. That dragged libcurl and ~10 transitive
//! libraries — and their CVE stream — into an image that otherwise holds
//! one static binary. Kubernetes ignores `HEALTHCHECK` anyway (it runs its
//! own `httpGet` probe), but plain `docker run`, Compose and Swarm do not,
//! so deleting the healthcheck would have *removed* a capability.
//!
//! Instead the binary probes itself: `butterfly-route healthcheck` issues a
//! plain HTTP/1.1 `GET` over a `TcpStream` and exits 0 on a 2xx status, 1
//! otherwise. No new dependency — the server it probes is always on
//! loopback, so there is no TLS to speak.

use anyhow::{Context, Result, anyhow, bail};
use std::io::{Read, Write};
use std::net::{TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Default endpoint: the port the container `CMD` serves on.
pub const DEFAULT_URL: &str = "http://127.0.0.1:8080/health";

/// The pieces of an `http://` URL this probe needs.
#[derive(Debug, PartialEq, Eq)]
struct Target {
    host: String,
    port: u16,
    path: String,
}

/// Parse an `http://host[:port][/path]` URL.
///
/// Deliberately minimal: `https` is rejected rather than silently probed in
/// cleartext, because a healthcheck that quietly talks to the wrong thing is
/// worse than one that fails loudly.
fn parse_url(url: &str) -> Result<Target> {
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| anyhow!("healthcheck URL must start with http:// (got {url:?})"))?;
    if rest.is_empty() {
        bail!("healthcheck URL has no host: {url:?}");
    }
    let (authority, path) = match rest.find('/') {
        Some(i) => (&rest[..i], &rest[i..]),
        None => (rest, "/"),
    };
    let (host, port) = match authority.rsplit_once(':') {
        Some((h, p)) => (
            h,
            p.parse::<u16>()
                .with_context(|| format!("bad port in healthcheck URL {url:?}"))?,
        ),
        None => (authority, 80u16),
    };
    if host.is_empty() {
        bail!("healthcheck URL has no host: {url:?}");
    }
    Ok(Target {
        host: host.to_string(),
        port,
        path: path.to_string(),
    })
}

/// Extract the status code from an HTTP status line (`HTTP/1.1 200 OK`).
fn status_code(status_line: &str) -> Result<u16> {
    let mut parts = status_line.split_whitespace();
    let version = parts
        .next()
        .ok_or_else(|| anyhow!("empty HTTP status line"))?;
    if !version.starts_with("HTTP/") {
        bail!("not an HTTP response: {status_line:?}");
    }
    let code = parts
        .next()
        .ok_or_else(|| anyhow!("HTTP status line has no code: {status_line:?}"))?;
    code.parse::<u16>()
        .with_context(|| format!("unparseable HTTP status code in {status_line:?}"))
}

/// `GET` the URL and return its status code.
fn probe(url: &str, timeout: Duration) -> Result<u16> {
    let target = parse_url(url)?;
    let addr = (target.host.as_str(), target.port)
        .to_socket_addrs()
        .with_context(|| format!("resolving {}:{}", target.host, target.port))?
        .next()
        .ok_or_else(|| anyhow!("no address for {}:{}", target.host, target.port))?;

    let mut stream = TcpStream::connect_timeout(&addr, timeout)
        .with_context(|| format!("connecting to {addr}"))?;
    stream.set_read_timeout(Some(timeout))?;
    stream.set_write_timeout(Some(timeout))?;

    let request = format!(
        "GET {} HTTP/1.1\r\nHost: {}:{}\r\nUser-Agent: butterfly-route-healthcheck\r\nConnection: close\r\nAccept: */*\r\n\r\n",
        target.path, target.host, target.port
    );
    stream
        .write_all(request.as_bytes())
        .context("sending healthcheck request")?;
    stream.flush().context("flushing healthcheck request")?;

    // The status line is all we need; cap the read so a chatty or hung
    // endpoint can never turn the probe into a memory or time sink.
    let mut buf = [0u8; 256];
    let n = stream
        .read(&mut buf)
        .context("reading healthcheck response")?;
    if n == 0 {
        bail!("healthcheck endpoint closed the connection without responding");
    }
    let head = String::from_utf8_lossy(&buf[..n]);
    let line = head.lines().next().unwrap_or_default();
    status_code(line)
}

/// Run the probe, mapping the outcome onto a process exit status.
///
/// Returns `Ok(())` on a 2xx so `main` exits 0; every other outcome —
/// non-2xx, connection refused, timeout, malformed response — is an error,
/// which `main` reports and turns into exit 1. That is exactly the contract
/// Docker's `HEALTHCHECK` expects.
pub fn run(url: &str, timeout_secs: u64) -> Result<()> {
    let code = probe(url, Duration::from_secs(timeout_secs))?;
    if (200..300).contains(&code) {
        Ok(())
    } else {
        bail!("healthcheck {url} returned HTTP {code}")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::BufRead;
    use std::net::TcpListener;

    #[test]
    fn parses_urls() {
        assert_eq!(
            parse_url("http://127.0.0.1:8080/health").unwrap(),
            Target {
                host: "127.0.0.1".into(),
                port: 8080,
                path: "/health".into()
            }
        );
        // No port → HTTP default.
        assert_eq!(
            parse_url("http://localhost/health").unwrap(),
            Target {
                host: "localhost".into(),
                port: 80,
                path: "/health".into()
            }
        );
        // No path → root.
        assert_eq!(
            parse_url("http://localhost:9").unwrap(),
            Target {
                host: "localhost".into(),
                port: 9,
                path: "/".into()
            }
        );
    }

    #[test]
    fn rejects_non_http_urls() {
        // https would need TLS; probing it in cleartext would "succeed"
        // against nothing useful, so it must fail loudly.
        assert!(parse_url("https://127.0.0.1:8080/health").is_err());
        assert!(parse_url("127.0.0.1:8080/health").is_err());
        assert!(parse_url("http://").is_err());
        assert!(parse_url("http://:8080/health").is_err());
        assert!(parse_url("http://host:notaport/health").is_err());
    }

    #[test]
    fn parses_status_lines() {
        assert_eq!(status_code("HTTP/1.1 200 OK").unwrap(), 200);
        assert_eq!(
            status_code("HTTP/1.1 503 Service Unavailable").unwrap(),
            503
        );
        assert!(status_code("").is_err());
        assert!(status_code("GARBAGE").is_err());
        assert!(status_code("HTTP/1.1").is_err());
        assert!(status_code("HTTP/1.1 abc").is_err());
    }

    /// Serve one canned response on an ephemeral loopback port and check
    /// the probe's verdict end-to-end.
    fn serve_once(response: &'static str) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        std::thread::spawn(move || {
            if let Ok((mut sock, _)) = listener.accept() {
                // Drain the request headers so the client's write completes.
                let mut reader = std::io::BufReader::new(sock.try_clone().unwrap());
                let mut line = String::new();
                while reader.read_line(&mut line).unwrap_or(0) > 0 {
                    if line == "\r\n" || line == "\n" {
                        break;
                    }
                    line.clear();
                }
                let _ = sock.write_all(response.as_bytes());
                let _ = sock.flush();
            }
        });
        port
    }

    #[test]
    fn healthy_endpoint_succeeds() {
        let port = serve_once("HTTP/1.1 200 OK\r\nContent-Length: 2\r\n\r\nok");
        run(&format!("http://127.0.0.1:{port}/health"), 5).unwrap();
    }

    #[test]
    fn unhealthy_endpoint_fails() {
        let port = serve_once("HTTP/1.1 503 Service Unavailable\r\nContent-Length: 0\r\n\r\n");
        let err = run(&format!("http://127.0.0.1:{port}/health"), 5).unwrap_err();
        assert!(err.to_string().contains("503"), "got: {err}");
    }

    #[test]
    fn unreachable_endpoint_fails() {
        // Bind, read the port, drop the listener: nothing is listening now.
        let port = {
            let l = TcpListener::bind("127.0.0.1:0").unwrap();
            l.local_addr().unwrap().port()
        };
        assert!(run(&format!("http://127.0.0.1:{port}/health"), 2).is_err());
    }
}
