//! Binary protocol parser for IoT UDP datagrams.
//!
//! Wire Format:
//! | Field        | Size    | Type              |
//! |--------------|---------|-------------------|
//! | Magic Header | 2 bytes | 0xAA 0x55         |
//! | Device ID    | 16 bytes| UTF-8 null-padded |
//! | Sequence Num | 4 bytes | u32 Big-Endian    |
//! | Payload      | Variable| JSON              |

use serde::{Deserialize, Serialize};
use thiserror::Error;

/// Magic header bytes for protocol validation
const MAGIC_HEADER: [u8; 2] = [0xAA, 0x55];

/// Fixed size fields before variable payload
const HEADER_SIZE: usize = 2;  // Magic
const DEVICE_ID_SIZE: usize = 16;
const SEQUENCE_SIZE: usize = 4;
const MIN_PACKET_SIZE: usize = HEADER_SIZE + DEVICE_ID_SIZE + SEQUENCE_SIZE;

/// Parsed IoT packet ready for Kafka ingestion
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IotPacket {
    /// Device identifier (trimmed of null padding)
    pub device_id: String,
    /// Sequence number for ordering
    pub sequence: u32,
    /// Raw JSON payload as Value for flexibility
    pub payload: serde_json::Value,
}

impl IotPacket {
    /// Serialize to Kafka message value format
    #[allow(dead_code)]
    pub fn to_kafka_value(&self) -> Result<Vec<u8>, serde_json::Error> {
        #[derive(Serialize)]
        struct KafkaMessage<'a> {
            id: &'a str,
            seq: u32,
            data: &'a serde_json::Value,
        }

        let msg = KafkaMessage {
            id: &self.device_id,
            seq: self.sequence,
            data: &self.payload,
        };
        serde_json::to_vec(&msg)
    }

    /// Get the Kafka message key (device_id)
    #[allow(dead_code)]
    pub fn kafka_key(&self) -> &str {
        &self.device_id
    }
}

/// Protocol parsing errors
#[derive(Debug, Error)]
pub enum ParseError {
    #[error("packet too short: {0} bytes (minimum: {MIN_PACKET_SIZE})")]
    TooShort(usize),

    #[error("invalid magic header: expected 0xAA55, got 0x{0:02X}{1:02X}")]
    InvalidMagic(u8, u8),

    #[error("invalid device ID: not valid UTF-8")]
    InvalidDeviceId(#[from] std::str::Utf8Error),

    #[error("invalid JSON payload: {0}")]
    InvalidJson(#[from] serde_json::Error),
}

/// Parse a raw UDP datagram into an IoT packet.
///
/// # Arguments
/// * `buf` - Raw bytes received from UDP socket
///
/// # Returns
/// * `Ok(IotPacket)` - Successfully parsed packet
/// * `Err(ParseError)` - Parsing failed
pub fn parse_packet(buf: &[u8]) -> Result<IotPacket, ParseError> {
    // Check minimum size
    if buf.len() < MIN_PACKET_SIZE {
        return Err(ParseError::TooShort(buf.len()));
    }

    // Validate magic header
    if buf[0] != MAGIC_HEADER[0] || buf[1] != MAGIC_HEADER[1] {
        return Err(ParseError::InvalidMagic(buf[0], buf[1]));
    }

    // Extract device ID (bytes 2-17), trim null padding
    let device_id_bytes = &buf[HEADER_SIZE..HEADER_SIZE + DEVICE_ID_SIZE];
    let device_id = std::str::from_utf8(device_id_bytes)?
        .trim_end_matches('\0')
        .to_string();

    // Extract sequence number (bytes 18-21) as big-endian u32
    let seq_offset = HEADER_SIZE + DEVICE_ID_SIZE;
    let sequence = u32::from_be_bytes([
        buf[seq_offset],
        buf[seq_offset + 1],
        buf[seq_offset + 2],
        buf[seq_offset + 3],
    ]);

    // Remaining bytes are JSON payload
    let payload_offset = seq_offset + SEQUENCE_SIZE;
    let payload: serde_json::Value = if payload_offset < buf.len() {
        serde_json::from_slice(&buf[payload_offset..])?
    } else {
        serde_json::Value::Null
    };

    Ok(IotPacket {
        device_id,
        sequence,
        payload,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_test_packet(device_id: &str, seq: u32, json: &str) -> Vec<u8> {
        let mut buf = vec![0xAA, 0x55]; // Magic

        // Device ID (16 bytes, null-padded)
        let mut id_bytes = [0u8; 16];
        let id_slice = device_id.as_bytes();
        id_bytes[..id_slice.len().min(16)].copy_from_slice(&id_slice[..id_slice.len().min(16)]);
        buf.extend_from_slice(&id_bytes);

        // Sequence (big-endian u32)
        buf.extend_from_slice(&seq.to_be_bytes());

        // JSON payload
        buf.extend_from_slice(json.as_bytes());

        buf
    }

    #[test]
    fn test_valid_packet() {
        let packet = make_test_packet("sensor-001", 42, r#"{"temp": 25.5}"#);
        let result = parse_packet(&packet).unwrap();

        assert_eq!(result.device_id, "sensor-001");
        assert_eq!(result.sequence, 42);
        assert_eq!(result.payload["temp"], 25.5);
    }

    #[test]
    fn test_packet_too_short() {
        let short = vec![0xAA, 0x55, 0x00];
        assert!(matches!(parse_packet(&short), Err(ParseError::TooShort(_))));
    }

    #[test]
    fn test_invalid_magic() {
        let mut packet = make_test_packet("sensor-001", 1, "{}");
        packet[0] = 0xFF;
        assert!(matches!(parse_packet(&packet), Err(ParseError::InvalidMagic(0xFF, 0x55))));
    }

    #[test]
    fn test_device_id_null_trimming() {
        let packet = make_test_packet("dev", 1, "{}");
        let result = parse_packet(&packet).unwrap();
        assert_eq!(result.device_id, "dev");
        assert_eq!(result.device_id.len(), 3); // Not 16
    }

    #[test]
    fn test_kafka_serialization() {
        let packet = IotPacket {
            device_id: "test-device".to_string(),
            sequence: 100,
            payload: serde_json::json!({"value": 42}),
        };

        let value = packet.to_kafka_value().unwrap();
        let parsed: serde_json::Value = serde_json::from_slice(&value).unwrap();

        assert_eq!(parsed["id"], "test-device");
        assert_eq!(parsed["seq"], 100);
        assert_eq!(parsed["data"]["value"], 42);
    }

    #[test]
    fn test_empty_payload() {
        // Packet with no JSON payload (just header + device_id + seq)
        let mut buf = vec![0xAA, 0x55];
        buf.extend_from_slice(&[0u8; 16]); // Empty device ID
        buf.extend_from_slice(&0u32.to_be_bytes());
        // No payload bytes

        let result = parse_packet(&buf).unwrap();
        assert_eq!(result.payload, serde_json::Value::Null);
    }
}
