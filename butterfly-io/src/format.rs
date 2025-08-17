//! BFLY binary format definitions

/// BFLY magic bytes: "BFLY"
pub const BFLY_MAGIC: [u8; 4] = *b"BFLY";

/// Standard 32-byte BFLY header
#[repr(C)]
#[derive(Debug, Clone, Copy)]
pub struct BflyHeader {
    /// Magic bytes: "BFLY"
    pub magic: [u8; 4],
    /// Format version
    pub version: u32,
    /// Header CRC32
    pub header_crc: u32,
    /// Payload size in bytes
    pub payload_size: u64,
    /// Payload XXH3 hash
    pub payload_hash: u64,
    /// Reserved for future use
    pub reserved: [u8; 8],
}

impl BflyHeader {
    pub const SIZE: usize = 32;

    pub fn new(payload_size: u64, payload_hash: u64) -> Self {
        let mut header = Self {
            magic: BFLY_MAGIC,
            version: crate::BFLY_VERSION,
            header_crc: 0,
            payload_size,
            payload_hash,
            reserved: [0; 8],
        };
        header.header_crc = header.calculate_header_crc();
        header
    }

    fn calculate_header_crc(&self) -> u32 {
        // Calculate CRC32 of header excluding the crc field itself
        // This is a stub implementation
        0
    }
}
