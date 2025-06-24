//! Error types for butterfly-dl library
//!
//! Provides comprehensive error handling for download operations.

use std::fmt;

/// Main error type for butterfly-dl operations
#[derive(Debug)]
pub enum Error {
    /// Source identifier not recognized or supported
    SourceNotFound(String),
    
    /// Network or HTTP-related download failure
    DownloadFailed(String),
    
    /// S3-specific error (only when S3 feature is enabled)
    #[cfg(feature = "s3")]
    S3Error(String),
    
    /// HTTP-specific error
    HttpError(String),
    
    /// File I/O error
    IoError(std::io::Error),
    
    /// Invalid configuration or parameters
    InvalidInput(String),
    
    /// Network connectivity issues
    NetworkError(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::SourceNotFound(source) => {
                write!(f, "Source '{}' not found or not supported", source)
            }
            Error::DownloadFailed(msg) => {
                write!(f, "Download failed: {}", msg)
            }
            #[cfg(feature = "s3")]
            Error::S3Error(msg) => {
                write!(f, "S3 error: {}", msg)
            }
            Error::HttpError(msg) => {
                write!(f, "HTTP error: {}", msg)
            }
            Error::IoError(err) => {
                write!(f, "I/O error: {}", err)
            }
            Error::InvalidInput(msg) => {
                write!(f, "Invalid input: {}", msg)
            }
            Error::NetworkError(msg) => {
                write!(f, "Network error: {}", msg)
            }
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

impl From<reqwest::Error> for Error {
    fn from(err: reqwest::Error) -> Self {
        if err.is_connect() || err.is_timeout() {
            Error::NetworkError(err.to_string())
        } else {
            Error::HttpError(err.to_string())
        }
    }
}

#[cfg(feature = "s3")]
impl From<aws_sdk_s3::Error> for Error {
    fn from(err: aws_sdk_s3::Error) -> Self {
        Error::S3Error(err.to_string())
    }
}

/// Convenience result type for butterfly-dl operations
pub type Result<T> = std::result::Result<T, Error>;