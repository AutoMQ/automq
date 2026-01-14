//! Binary protocol parser for KLAIS wire format.
//!
//! Wire format:
//! ```text
//! ┌────────┬──────┬───────────────┬────────────┬───────────────────┐
//! │ Offset │ Size │ Field         │ Type       │ Description       │
//! ├────────┼──────┼───────────────┼────────────┼───────────────────┤
//! │ 0      │ 2    │ magic         │ u16 BE     │ 0xAA55            │
//! │ 2      │ 16   │ device_id     │ [u8; 16]   │ UTF-8, null-pad   │
//! │ 18     │ 4    │ sequence      │ u32 BE     │ Monotonic counter │
//! │ 22     │ var  │ payload       │ JSON       │ Application data  │
//! └────────┴──────┴───────────────┴────────────┴───────────────────┘
//! ```

use bytes::Bytes;
use serde::{Deserialize, Serialize};
use std::fmt;
use thiserror::Error;

/// Magic header bytes: 0xAA 0x55
pub const MAGIC_HEADER: [u8; 2] = [0xAA, 0x55];

/// Minimum packet size (magic + device_id + sequence)
pub const MIN_PACKET_SIZE: usize = 2 + 16 + 4;

/// Device ID size in bytes
pub const DEVICE_ID_SIZE: usize = 16;

/// Parse errors for KLAIS protocol
#[derive(Error, Debug, Clone, PartialEq, Eq)]
pub enum ParseError {
    #[error("packet too short: expected at least {MIN_PACKET_SIZE} bytes, got {0}")]
    TooShort(usize),

    #[error("invalid magic header: expected 0xAA55, got 0x{0:02X}{1:02X}")]
    InvalidMagic(u8, u8),

    #[error("invalid device ID: not valid UTF-8")]
    InvalidDeviceId,

    #[error("invalid JSON payload: {0}")]
    InvalidPayload(String),
}

/// Parsed KLAIS packet
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct KlaisPacket {
    /// Device identifier (16 bytes, null-trimmed)
    pub device_id: String,
    
    /// Sequence number (monotonic per device)
    pub sequence: u32,
    
    /// JSON payload as raw bytes
    #[serde(skip)]
    pub payload_raw: Bytes,
    
    /// Parsed JSON payload (lazy)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub payload: Option<serde_json::Value>,
    
    /// Receive timestamp (nanoseconds since epoch)
    pub received_at: u64,
}

impl KlaisPacket {
    /// Create a new packet with current timestamp
    pub fn new(device_id: String, sequence: u32, payload_raw: Bytes) -> Self {
        let received_at = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos() as u64;

        Self {
            device_id,
            sequence,
            payload_raw,
            payload: None,
            received_at,
        }
    }

    /// Parse the JSON payload lazily
    pub fn parse_payload(&mut self) -> Result<&serde_json::Value, ParseError> {
        if self.payload.is_none() {
            let value: serde_json::Value = serde_json::from_slice(&self.payload_raw)
                .map_err(|e| ParseError::InvalidPayload(e.to_string()))?;
            self.payload = Some(value);
        }
        Ok(self.payload.as_ref().unwrap())
    }

    /// Convert to Kafka-ready JSON format
    /// Format: {"id": device_id, "seq": sequence, "data": payload, "ts": received_at}
    pub fn to_kafka_json(&self) -> Result<Vec<u8>, serde_json::Error> {
        #[derive(Serialize)]
        struct KafkaRecord<'a> {
            id: &'a str,
            seq: u32,
            data: &'a serde_json::value::RawValue,
            ts: u64,
        }

        // Parse payload as raw JSON to avoid re-serialization
        let raw_json = serde_json::value::RawValue::from_string(
            String::from_utf8_lossy(&self.payload_raw).into_owned()
        )?;

        let record = KafkaRecord {
            id: &self.device_id,
            seq: self.sequence,
            data: &raw_json,
            ts: self.received_at,
        };

        serde_json::to_vec(&record)
    }

    /// Get device ID as bytes for Kafka key
    pub fn device_id_bytes(&self) -> &[u8] {
        self.device_id.as_bytes()
    }
}

impl fmt::Display for KlaisPacket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "KlaisPacket {{ device: {}, seq: {}, payload_len: {} }}",
            self.device_id,
            self.sequence,
            self.payload_raw.len()
        )
    }
}

/// Parse a raw UDP datagram into a KlaisPacket.
///
/// # Arguments
/// * `data` - Raw bytes from UDP socket
///
/// # Returns
/// * `Ok(KlaisPacket)` on successful parse
/// * `Err(ParseError)` if the packet is malformed
///
/// # Example
/// ```
/// use klais::protocol::parse_packet;
/// use bytes::Bytes;
///
/// let mut packet = vec![0xAA, 0x55];  // Magic
/// packet.extend_from_slice(b"device-001\0\0\0\0\0\0");  // Device ID (16 bytes)
/// packet.extend_from_slice(&42u32.to_be_bytes());  // Sequence
/// packet.extend_from_slice(br#"{"temp": 25.5}"#);  // Payload
///
/// let parsed = parse_packet(&packet).unwrap();
/// assert_eq!(parsed.device_id, "device-001");
/// assert_eq!(parsed.sequence, 42);
/// ```
pub fn parse_packet(data: &[u8]) -> Result<KlaisPacket, ParseError> {
    // 1. Check minimum length
    if data.len() < MIN_PACKET_SIZE {
        return Err(ParseError::TooShort(data.len()));
    }

    // 2. Validate magic header
    if data[0] != MAGIC_HEADER[0] || data[1] != MAGIC_HEADER[1] {
        return Err(ParseError::InvalidMagic(data[0], data[1]));
    }

    // 3. Extract device ID (bytes 2-17), trim null padding
    let device_id_bytes = &data[2..2 + DEVICE_ID_SIZE];
    let device_id = extract_device_id(device_id_bytes)?;

    // 4. Extract sequence number (bytes 18-21, big-endian)
    let sequence = u32::from_be_bytes([data[18], data[19], data[20], data[21]]);

    // 5. Extract payload (remaining bytes)
    let payload_raw = if data.len() > MIN_PACKET_SIZE {
        Bytes::copy_from_slice(&data[MIN_PACKET_SIZE..])
    } else {
        Bytes::new()
    };

    Ok(KlaisPacket::new(device_id, sequence, payload_raw))
}

/// Extract device ID from raw bytes, trimming null padding
fn extract_device_id(bytes: &[u8]) -> Result<String, ParseError> {
    // Find first null byte or use full length
    let end = bytes.iter().position(|&b| b == 0).unwrap_or(bytes.len());
    
    std::str::from_utf8(&bytes[..end])
        .map(|s| s.to_string())
        .map_err(|_| ParseError::InvalidDeviceId)
}

/// FNV-1a hash for device ID (used for consistent hashing)
pub fn fnv1a_hash(data: &[u8]) -> u32 {
    const FNV_OFFSET: u32 = 2166136261;
    const FNV_PRIME: u32 = 16777619;

    let mut hash = FNV_OFFSET;
    for byte in data {
        hash ^= *byte as u32;
        hash = hash.wrapping_mul(FNV_PRIME);
    }
    hash
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_packet(device_id: &str, seq: u32, payload: &str) -> Vec<u8> {
        let mut packet = vec![0xAA, 0x55];
        
        // Device ID (16 bytes, null-padded)
        let mut device_bytes = [0u8; 16];
        let id_bytes = device_id.as_bytes();
        device_bytes[..id_bytes.len().min(16)].copy_from_slice(&id_bytes[..id_bytes.len().min(16)]);
        packet.extend_from_slice(&device_bytes);
        
        // Sequence number (big-endian)
        packet.extend_from_slice(&seq.to_be_bytes());
        
        // Payload
        packet.extend_from_slice(payload.as_bytes());
        
        packet
    }

    #[test]
    fn test_parse_valid_packet() {
        let data = make_test_packet("device-001", 42, r#"{"temp": 25.5}"#);
        let packet = parse_packet(&data).unwrap();
        
        assert_eq!(packet.device_id, "device-001");
        assert_eq!(packet.sequence, 42);
        assert_eq!(&packet.payload_raw[..], br#"{"temp": 25.5}"#);
    }

    #[test]
    fn test_parse_too_short() {
        let data = vec![0xAA, 0x55, 0x00];
        assert!(matches!(parse_packet(&data), Err(ParseError::TooShort(3))));
    }

    #[test]
    fn test_parse_invalid_magic() {
        let mut data = make_test_packet("device", 1, "{}");
        data[0] = 0xFF;
        assert!(matches!(parse_packet(&data), Err(ParseError::InvalidMagic(0xFF, 0x55))));
    }

    #[test]
    fn test_parse_empty_payload() {
        let data = make_test_packet("device", 1, "");
        let packet = parse_packet(&data).unwrap();
        assert!(packet.payload_raw.is_empty());
    }

    #[test]
    fn test_fnv1a_hash_consistency() {
        let hash1 = fnv1a_hash(b"device-001");
        let hash2 = fnv1a_hash(b"device-001");
        assert_eq!(hash1, hash2);
        
        let hash3 = fnv1a_hash(b"device-002");
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_to_kafka_json() {
        let data = make_test_packet("dev-1", 100, r#"{"val":42}"#);
        let packet = parse_packet(&data).unwrap();
        
        let json = packet.to_kafka_json().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&json).unwrap();
        
        assert_eq!(parsed["id"], "dev-1");
        assert_eq!(parsed["seq"], 100);
        assert_eq!(parsed["data"]["val"], 42);
    }
}
