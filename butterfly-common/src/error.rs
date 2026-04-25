//! Error types for the butterfly-osm toolkit.
//!
//! Fuzzy correction logic for misspelled source identifiers lives in
//! the sibling [`crate::fuzzy`] module — this file is the error type
//! and its `From` impls only.

use std::fmt;

/// Main error type for butterfly-osm operations.
#[derive(Debug)]
pub enum Error {
    /// Source identifier not recognized or supported.
    SourceNotFound(String),

    /// Network or HTTP-related download failure (no further classification
    /// available; consumers should treat as non-retriable unless paired
    /// with `NetworkError` from the same call site).
    DownloadFailed(String),

    /// HTTP-specific error (4xx / 5xx response). Generally non-retriable.
    HttpError(String),

    /// File I/O error.
    IoError(std::io::Error),

    /// Invalid configuration or parameters supplied by the caller.
    InvalidInput(String),

    /// Network connectivity issues (timeout, connect failure). Generally
    /// retriable.
    NetworkError(String),
}

impl Error {
    /// `true` if this error is likely to succeed on retry (network blip,
    /// timeout, transient connection failure). Callers can use this to
    /// drive a retry loop without parsing the message string. (#135)
    pub fn is_transient(&self) -> bool {
        matches!(self, Error::NetworkError(_))
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SourceNotFound(source) => {
                write!(f, "Source '{source}' not found or not supported")
            }
            Error::DownloadFailed(msg) => write!(f, "Download failed: {msg}"),
            Error::HttpError(msg) => write!(f, "HTTP error: {msg}"),
            Error::IoError(err) => write!(f, "I/O error: {err}"),
            Error::InvalidInput(msg) => write!(f, "Invalid input: {msg}"),
            Error::NetworkError(msg) => write!(f, "Network error: {msg}"),
        }
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Error::IoError(err) => Some(err),
            _ => None,
        }
    }
}

impl From<std::io::Error> for Error {
    fn from(err: std::io::Error) -> Self {
        Error::IoError(err)
    }
}

#[cfg(feature = "http")]
impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        if err.is_connect() || err.is_timeout() {
            Error::NetworkError(err.to_string())
        } else {
            Error::HttpError(err.to_string())
        }
    }
}

/// Convenience result type for butterfly-osm operations.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn is_transient_classifies_correctly() {
        assert!(Error::NetworkError("conn refused".into()).is_transient());
        assert!(!Error::HttpError("500".into()).is_transient());
        assert!(!Error::SourceNotFound("zz".into()).is_transient());
        assert!(!Error::InvalidInput("bad".into()).is_transient());
    }
}
