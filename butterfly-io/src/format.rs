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
    pub reserved: [u8; 4],
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
            reserved: [0; 4],
        };
        header.header_crc = header.calculate_header_crc();
        header
    }

    fn calculate_header_crc(&self) -> u32 {
        use crc32fast::Hasher;
        
        let mut hasher = Hasher::new();
        hasher.update(&self.magic);
        hasher.update(&self.version.to_le_bytes());
        // Skip header_crc field itself
        hasher.update(&self.payload_size.to_le_bytes());
        hasher.update(&self.payload_hash.to_le_bytes());
        hasher.update(&self.reserved);
        hasher.finalize()
    }
    
    /// Validate header integrity
    pub fn validate(&self) -> crate::IoResult<()> {
        if self.magic != BFLY_MAGIC {
            return Err(crate::IoError::InvalidFormat(
                format!("Invalid magic bytes: expected {:?}, got {:?}", BFLY_MAGIC, self.magic)
            ));
        }
        
        if self.version != crate::BFLY_VERSION {
            return Err(crate::IoError::InvalidFormat(
                format!("Unsupported version: {}", self.version)
            ));
        }
        
        let expected_crc = self.calculate_header_crc();
        if self.header_crc != expected_crc {
            return Err(crate::IoError::ChecksumMismatch {
                expected: expected_crc,
                actual: self.header_crc,
            });
        }
        
        Ok(())
    }
    
    /// Serialize header to bytes
    pub fn to_bytes(&self) -> [u8; 32] {
        let mut bytes = [0u8; 32];
        bytes[0..4].copy_from_slice(&self.magic);
        bytes[4..8].copy_from_slice(&self.version.to_le_bytes());
        bytes[8..12].copy_from_slice(&self.header_crc.to_le_bytes());
        bytes[12..20].copy_from_slice(&self.payload_size.to_le_bytes());
        bytes[20..28].copy_from_slice(&self.payload_hash.to_le_bytes());
        bytes[28..32].copy_from_slice(&self.reserved);
        bytes
    }
    
    /// Deserialize header from bytes
    pub fn from_bytes(bytes: &[u8]) -> crate::IoResult<Self> {
        if bytes.len() < 32 {
            return Err(crate::IoError::InvalidFormat(
                "Header too short".to_string()
            ));
        }
        
        let mut magic = [0u8; 4];
        magic.copy_from_slice(&bytes[0..4]);
        
        let version = u32::from_le_bytes([bytes[4], bytes[5], bytes[6], bytes[7]]);
        let header_crc = u32::from_le_bytes([bytes[8], bytes[9], bytes[10], bytes[11]]);
        let payload_size = u64::from_le_bytes([
            bytes[12], bytes[13], bytes[14], bytes[15],
            bytes[16], bytes[17], bytes[18], bytes[19]
        ]);
        let payload_hash = u64::from_le_bytes([
            bytes[20], bytes[21], bytes[22], bytes[23],
            bytes[24], bytes[25], bytes[26], bytes[27]
        ]);
        
        let mut reserved = [0u8; 4];
        reserved.copy_from_slice(&bytes[28..32]);
        
        let header = Self {
            magic,
            version,
            header_crc,
            payload_size,
            payload_hash,
            reserved,
        };
        
        header.validate()?;
        Ok(header)
    }
}
