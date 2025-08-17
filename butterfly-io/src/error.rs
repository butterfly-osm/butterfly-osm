use thiserror::Error;

#[derive(Error, Debug)]
pub enum IoError {
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    #[error("Compression error: {0}")]
    Compression(String),

    #[error("Checksum mismatch: expected {expected:x}, got {actual:x}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("Invalid format: {0}")]
    InvalidFormat(String),
}

pub type IoResult<T> = Result<T, IoError>;
