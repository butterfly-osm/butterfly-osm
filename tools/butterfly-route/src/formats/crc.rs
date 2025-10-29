///! CRC-64-ISO checksum utilities

use crc::{Crc, CRC_64_GO_ISO};

/// CRC-64-ISO algorithm
pub const CRC64: Crc<u64> = Crc::<u64>::new(&CRC_64_GO_ISO);

/// Compute CRC-64 checksum for a byte slice
pub fn checksum(data: &[u8]) -> u64 {
    CRC64.checksum(data)
}

/// Incremental CRC-64 digest
pub struct Digest {
    digest: crc::Digest<'static, u64>,
}

impl Digest {
    pub fn new() -> Self {
        Self {
            digest: CRC64.digest(),
        }
    }

    pub fn update(&mut self, data: &[u8]) {
        self.digest.update(data);
    }

    pub fn finalize(self) -> u64 {
        self.digest.finalize()
    }
}

impl Default for Digest {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_crc64_basic() {
        let data = b"hello world";
        let csum = checksum(data);
        assert_ne!(csum, 0);
    }

    #[test]
    fn test_crc64_incremental() {
        let data = b"hello world";
        let mut digest = Digest::new();
        digest.update(&data[..5]);
        digest.update(&data[5..]);
        let csum = digest.finalize();

        assert_eq!(csum, checksum(data));
    }
}
