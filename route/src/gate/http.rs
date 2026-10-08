//! HTTP — the blocking client the REST checks share. An HTTP error status is
//! an error of its own kind (`GateErr::Http`), so a 400/404 "no route" can be
//! told from a transport failure, exactly like the Python gate's
//! `urllib.error.HTTPError` split.

use std::fmt;
use std::time::Duration;

use serde_json::Value;

#[derive(Debug, Clone)]
pub enum GateErr {
    /// The server answered with a status ≥ 400.
    Http { status: u16, body: String },
    /// Transport failure, timeout, malformed body, Flight error.
    Other(String),
}

impl fmt::Display for GateErr {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            GateErr::Http { status, body } => write!(f, "HTTP Error {status}: {body}"),
            GateErr::Other(s) => write!(f, "{s}"),
        }
    }
}

impl std::error::Error for GateErr {}

impl From<reqwest::Error> for GateErr {
    fn from(e: reqwest::Error) -> Self {
        GateErr::Other(format!("{e}"))
    }
}

impl From<serde_json::Error> for GateErr {
    fn from(e: serde_json::Error) -> Self {
        GateErr::Other(format!("JSON: {e}"))
    }
}

impl From<anyhow::Error> for GateErr {
    fn from(e: anyhow::Error) -> Self {
        GateErr::Other(format!("{e:#}"))
    }
}

/// A 400/404 is the server SAYING "no route / off network".
pub fn is_no_route(e: &GateErr) -> bool {
    matches!(e, GateErr::Http { status, .. } if *status == 400 || *status == 404)
}

pub type GResult<T> = Result<T, GateErr>;

pub struct Http {
    client: reqwest::blocking::Client,
}

impl Default for Http {
    fn default() -> Self {
        Self::new()
    }
}

impl Http {
    pub fn new() -> Self {
        let client = reqwest::blocking::Client::builder()
            .pool_max_idle_per_host(32)
            .build()
            .expect("reqwest client");
        Http { client }
    }

    fn send(
        &self,
        req: reqwest::blocking::RequestBuilder,
        timeout: u64,
    ) -> GResult<(u16, String, Vec<u8>)> {
        let resp = req.timeout(Duration::from_secs(timeout)).send()?;
        let status = resp.status().as_u16();
        let ctype = resp
            .headers()
            .get(reqwest::header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("")
            .to_string();
        let body = resp.bytes()?.to_vec();
        Ok((status, ctype, body))
    }

    fn raise_status(status: u16, body: &[u8]) -> GResult<()> {
        if status >= 400 {
            return Err(GateErr::Http {
                status,
                body: String::from_utf8_lossy(body).into_owned(),
            });
        }
        Ok(())
    }

    /// GET → bytes (raises on ≥ 400).
    pub fn bytes(&self, url: &str, timeout: u64, accept: Option<&str>) -> GResult<Vec<u8>> {
        let mut req = self.client.get(url);
        if let Some(a) = accept {
            req = req.header(reqwest::header::ACCEPT, a);
        }
        let (status, _ct, body) = self.send(req, timeout)?;
        Self::raise_status(status, &body)?;
        Ok(body)
    }

    /// GET → JSON (raises on ≥ 400).
    pub fn json(&self, url: &str, timeout: u64) -> GResult<Value> {
        let body = self.bytes(url, timeout, None)?;
        Ok(serde_json::from_slice(&body)?)
    }

    /// POST JSON → JSON (raises on ≥ 400).
    pub fn post_json(&self, url: &str, payload: &Value, timeout: u64) -> GResult<Value> {
        let (v, _headers) = self.post_json_with_headers(url, payload, timeout)?;
        Ok(v)
    }

    /// POST JSON → (JSON, response headers) — the matrix plan header lives there.
    pub fn post_json_with_headers(
        &self,
        url: &str,
        payload: &Value,
        timeout: u64,
    ) -> GResult<(Value, reqwest::header::HeaderMap)> {
        let resp = self
            .client
            .post(url)
            .header(reqwest::header::CONTENT_TYPE, "application/json")
            .body(serde_json::to_vec(payload)?)
            .timeout(Duration::from_secs(timeout))
            .send()?;
        let status = resp.status().as_u16();
        let headers = resp.headers().clone();
        let body = resp.bytes()?.to_vec();
        Self::raise_status(status, &body)?;
        Ok((serde_json::from_slice(&body)?, headers))
    }

    /// (status, content type, bytes): an error STATUS is returned, not raised.
    pub fn status(
        &self,
        url: &str,
        method: &str,
        body: Option<&Value>,
        timeout: u64,
    ) -> GResult<(u16, String, Vec<u8>)> {
        let req = match (method, body) {
            ("POST", Some(b)) => self
                .client
                .post(url)
                .header(reqwest::header::CONTENT_TYPE, "application/json")
                .body(serde_json::to_vec(b)?),
            ("POST", None) => self.client.post(url),
            _ => self.client.get(url),
        };
        self.send(req, timeout)
    }
}

/// `urllib.parse.urlencode` for the query shapes the gate builds: numbers
/// are rendered like Python's `str()` (shortest round-trip, `x.0` for
/// integral floats), strings percent-encoded.
pub fn urlencode(params: &[(&str, String)]) -> String {
    params
        .iter()
        .map(|(k, v)| format!("{k}={}", pct_encode(v)))
        .collect::<Vec<_>>()
        .join("&")
}

pub fn pct_encode(s: &str) -> String {
    let mut out = String::with_capacity(s.len());
    for b in s.bytes() {
        match b {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char)
            }
            b' ' => out.push('+'),
            _ => out.push_str(&format!("%{b:02X}")),
        }
    }
    out
}

/// Python `str(float)`: shortest round-trip repr, `.0` on integral values.
pub fn pyf(x: f64) -> String {
    if x.is_finite() && x.fract() == 0.0 && x.abs() < 1e16 {
        format!("{x:.1}")
    } else {
        format!("{x}")
    }
}
