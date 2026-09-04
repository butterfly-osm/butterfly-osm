//! #579: a lean container boots and serves — identically.
//!
//! `PackOptions::lean` drops sections and trims `nbg.geo`'s polyline
//! blob on the claim that no server ever reads them. Unit tests in
//! `pack.rs` pin WHAT is dropped; this file pins the consequence: a
//! container with those bytes removed still boots and answers `/route`,
//! `/table` and `/isochrone` — byte-for-byte the same responses as the
//! container that still has them.
//!
//! The lean container is built here by rewriting a real artifact
//! section by section under the packer's own policy
//! ([`LEAN_OMITTED_SECTIONS`] + [`NbgGeoFile::encode_edges_only`]), so
//! the test needs no step tree — only the `.butterfly` the deploy
//! already ships.
//!
//! **Self-skips (#587)** when no artifact is present (CI ships none):
//!
//! ```bash
//! BUTTERFLY_TEST_DATA_DIR=/path/to/data cargo test -p butterfly-route \
//!     --release --test lean_container_boot
//! ```
//!
//! It is deliberately heavy when it does run: it writes a full copy of
//! the artifact (Belgium: ~25 GiB, alongside the source) and boots two
//! servers. It skips rather than fails if the copy runs out of space.

use std::io::{Read, Write};
use std::net::TcpStream;
use std::path::Path;
use std::sync::Arc;

use butterfly_route::formats::NbgGeoFile;
use butterfly_route::formats::butterfly_dat::{Container, ContainerWriter};
use butterfly_route::pack::LEAN_OMITTED_SECTIONS;
use butterfly_route::server::regions::RegionsState;
use butterfly_route::testutil;

const SCOPE: &str = "lean_container_boot";

/// Public landmarks, no client data: Brussels, Antwerp, Bruges.
const BRUSSELS: (f64, f64) = (4.3517, 50.8503);
const ANTWERP: (f64, f64) = (4.4025, 51.2194);
const BRUGES: (f64, f64) = (3.2247, 51.2093);

/// Rewrite `src` into `dst` applying the packer's lean policy: drop
/// every section in `LEAN_OMITTED_SECTIONS`, and re-encode
/// `shared/nbg.geo` edges-only when the flat geometry sections are
/// present. Everything else is copied verbatim.
///
/// Returns what it did, so the caller can tell an artifact that was
/// still fat from one a current packer already trimmed.
struct LeanCopy {
    dropped: Vec<String>,
    saved: u64,
    geo_trimmed: bool,
}

fn lean_copy(src: &Path, dst: &Path) -> anyhow::Result<LeanCopy> {
    let c = Container::open(src)?;
    let has_flat_geom =
        c.get("shared/edge_geom_offsets").is_some() && c.get("shared/edge_geom_points").is_some();

    let mut w = ContainerWriter::create(dst)?;
    let mut out = LeanCopy {
        dropped: Vec::new(),
        saved: 0,
        geo_trimmed: false,
    };
    for sec in &c.sections {
        if LEAN_OMITTED_SECTIONS.contains(&sec.name.as_str()) {
            out.dropped.push(sec.name.clone());
            out.saved += sec.len;
            continue;
        }
        let bytes = c.read_section_verified(src, sec)?;
        if sec.name == "shared/nbg.geo" && has_flat_geom {
            match NbgGeoFile::encode_edges_only(std::io::Cursor::new(&bytes)) {
                Ok(lean) => {
                    out.saved += bytes.len() as u64 - lean.len() as u64;
                    out.geo_trimmed = true;
                    w.append_bytes(sec.kind, &sec.name, &lean)?;
                }
                // A source packed by a current packer already carries
                // the edges-only image; nothing left to trim.
                Err(e) if e.to_string().contains("already an edges-only image") => {
                    w.append_bytes(sec.kind, &sec.name, &bytes)?;
                }
                Err(e) => return Err(e),
            }
        } else {
            w.append_bytes(sec.kind, &sec.name, &bytes)?;
        }
    }
    w.finalize()?;
    Ok(out)
}

/// Serve `dir` on an ephemeral loopback port; returns the port.
///
/// The runtime must outlive the returned port, so the caller keeps it.
///
/// The router is assembled here from the production handlers rather
/// than via `server::api::build_router`: that one installs the global
/// Prometheus recorder, which can only happen once per process, and
/// this test needs two servers side by side. The middleware it adds
/// (CORS, compression, timeouts) is orthogonal to what a container
/// answers.
fn serve(rt: &tokio::runtime::Runtime, dir: &Path) -> u16 {
    use axum::Router;
    use axum::routing::{get, post};
    use butterfly_route::server::{health_handler, isochrone_handler, route, table};

    // Eager (`lazy = false`): the container is loaded HERE, so "does it
    // boot?" is answered by this call, with the failure attributable to
    // this container, rather than inside the first request.
    let regions = RegionsState::load_from_dir_with_opts(dir, None, None, false)
        .unwrap_or_else(|e| panic!("load_from_dir({}): {e:#}", dir.display()));
    let app = Router::new()
        .route("/route", get(route::route_handler))
        .route("/table", post(table::table_post_handler))
        .route("/isochrone", get(isochrone_handler::isochrone_handler))
        .route(
            "/nearest",
            get(butterfly_route::server::nearest::nearest_handler),
        )
        .route("/health", get(health_handler::health_handler))
        .with_state(Arc::new(regions));
    rt.block_on(async move {
        // Port 0 = kernel-assigned. Never a fixed port: this test must
        // not collide with anything an operator is running.
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        tokio::spawn(async move {
            axum::serve(listener, app).await.unwrap();
        });
        port
    })
}

/// Minimal blocking HTTP/1.1 client: returns `(status, body)`.
///
/// `Connection: close` makes the server hang up at the end of the
/// response, so "read to EOF" is the whole message.
fn http(port: u16, request_line: &str, body: Option<&str>) -> (u16, String) {
    let mut req = format!("{request_line} HTTP/1.1\r\nHost: 127.0.0.1\r\nConnection: close\r\n");
    if let Some(b) = body {
        req.push_str("Content-Type: application/json\r\n");
        req.push_str(&format!("Content-Length: {}\r\n", b.len()));
    }
    req.push_str("\r\n");
    if let Some(b) = body {
        req.push_str(b);
    }

    let mut sock = TcpStream::connect(("127.0.0.1", port)).expect("connect");
    sock.write_all(req.as_bytes()).expect("write request");
    sock.flush().expect("flush");
    let mut raw = Vec::new();
    sock.read_to_end(&mut raw).expect("read response");
    let text = String::from_utf8_lossy(&raw).into_owned();

    let status: u16 = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| panic!("no status line in response: {text:.200}"));
    let body = match text.find("\r\n\r\n") {
        Some(i) => text[i + 4..].to_string(),
        None => panic!("no header/body split in response: {text:.200}"),
    };
    (status, body)
}

fn get(port: u16, path: &str) -> (u16, String) {
    http(port, &format!("GET {path}"), None)
}

fn post(port: u16, path: &str, json: &str) -> (u16, String) {
    http(port, &format!("POST {path}"), Some(json))
}

/// The query surfaces, as a client sees them. `/route` asks for steps
/// and per-edge annotations on purpose: road names, geometry and the
/// per-edge OSM chains are exactly the things that come out of the
/// sections this change touches.
fn probes() -> Vec<(String, Option<String>)> {
    let (b_lon, b_lat) = BRUSSELS;
    let (a_lon, a_lat) = ANTWERP;
    let (g_lon, g_lat) = BRUGES;
    vec![
        (
            format!(
                "/route?origin_lon={b_lon}&origin_lat={b_lat}\
                 &destination_lon={a_lon}&destination_lat={a_lat}&mode=car\
                 &steps=true&annotations=duration,distance,speed,nodes"
            ),
            None,
        ),
        (format!("/nearest?lon={b_lon}&lat={b_lat}&mode=car"), None),
        (
            "/table".to_string(),
            Some(format!(
                r#"{{"origins":[[{b_lon},{b_lat}],[{g_lon},{g_lat}]],
                    "destinations":[[{a_lon},{a_lat}],[{g_lon},{g_lat}]],
                    "mode":"car","annotations":"duration,distance"}}"#
            )),
        ),
        (
            format!("/isochrone?lon={b_lon}&lat={b_lat}&time_s=600&mode=car&geometries=geojson"),
            None,
        ),
    ]
}

#[test]
fn a_lean_container_boots_and_serves_identical_answers() {
    let Some(src) = testutil::require_container(SCOPE) else {
        return;
    };
    if !src.is_file() {
        // The probe found a step{N}/ tree, not a packed container.
        let _: Option<()> = testutil::skip(SCOPE, "a packed *.butterfly container");
        return;
    }

    // Stage the source next to a lean copy of it, both alone in their
    // own directory so `load_from_dir` sees exactly one region each.
    // The copy lands on the artifact's own filesystem — the one that
    // already holds a container this size.
    let parent = src.parent().expect("container has a parent dir");
    let scratch = match tempfile::TempDir::new_in(parent) {
        Ok(d) => d,
        Err(e) => {
            eprintln!("SKIP {SCOPE}: cannot create a scratch dir next to the artifact: {e}");
            return;
        }
    };
    let full_dir = scratch.path().join("full");
    let lean_dir = scratch.path().join("lean");
    std::fs::create_dir_all(&full_dir).unwrap();
    std::fs::create_dir_all(&lean_dir).unwrap();
    std::os::unix::fs::symlink(&src, full_dir.join("source.butterfly")).unwrap();

    let lean_path = lean_dir.join("lean.butterfly");
    let copy = match lean_copy(&src, &lean_path) {
        Ok(v) => v,
        Err(e) => {
            if e.downcast_ref::<std::io::Error>()
                .and_then(|io| io.raw_os_error())
                == Some(28)
            {
                eprintln!("SKIP {SCOPE}: no room next to the artifact for a lean copy");
                return;
            }
            panic!("building the lean copy failed: {e:#}");
        }
    };

    let before = std::fs::metadata(&src).unwrap().len();
    let after = std::fs::metadata(&lean_path).unwrap().len();
    eprintln!(
        "{SCOPE}: {} -> {} bytes ({} MiB saved; geo trimmed: {}; dropped {:?})",
        before,
        after,
        copy.saved / (1024 * 1024),
        copy.geo_trimmed,
        copy.dropped
    );
    if copy.saved > 0 {
        assert!(
            after < before,
            "the lean copy must be smaller than the artifact it came from"
        );
    } else {
        // The source was packed by a current packer: there is nothing
        // left to strip. Booting and serving it is still the point.
        eprintln!("{SCOPE}: source artifact is already lean");
    }

    // Whatever the source carried of the omitted set must be gone.
    let lean = Container::open(&lean_path).unwrap();
    for name in LEAN_OMITTED_SECTIONS {
        assert!(lean.get(name).is_none(), "lean copy still carries {name}");
    }
    // And a trimmed nbg.geo must be the edges-only image: readable as
    // edges, refused by the full reader (which would otherwise report a
    // network with no geometry at all).
    if copy.geo_trimmed {
        let sec = lean.get("shared/nbg.geo").expect("nbg.geo section");
        let bytes = lean.read_section_verified(&lean_path, sec).unwrap();
        assert!(
            NbgGeoFile::read_edges_only_from_bytes(&bytes).is_ok(),
            "trimmed nbg.geo must still read edges-only"
        );
        assert!(
            NbgGeoFile::read_from_bytes(&bytes).is_err(),
            "trimmed nbg.geo must refuse a full read"
        );
    }

    let rt = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    // Boot BOTH — the lean copy first, so a boot failure fails here
    // rather than after a second 50 s load.
    let lean_port = serve(&rt, &lean_dir);
    let full_port = serve(&rt, &full_dir);

    let (status, body) = get(lean_port, "/health");
    assert_eq!(status, 200, "lean container /health: {body}");

    for (path, json) in probes() {
        let (lean_status, lean_body) = match &json {
            Some(b) => post(lean_port, &path, b),
            None => get(lean_port, &path),
        };
        let (full_status, full_body) = match &json {
            Some(b) => post(full_port, &path, b),
            None => get(full_port, &path),
        };
        assert_eq!(
            lean_status, 200,
            "lean container failed to serve {path}: {lean_body}"
        );
        assert_eq!(full_status, 200, "{path}: {full_body}");
        assert_eq!(
            lean_body, full_body,
            "{path} answered differently from a lean container"
        );
        // A 200 with an empty answer would satisfy the equality above.
        assert!(
            lean_body.len() > 64,
            "{path} answered suspiciously little: {lean_body}"
        );
    }
}
