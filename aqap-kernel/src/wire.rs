// aqap-kernel/src/wire.rs
use pyo3::prelude::*;
use pyo3::types::PyBytes;
use uuid::Uuid;
use crate::types::*;
use crate::error::{KernelError, ErrorCode};

/// Fixed wire header — 64 bytes
#[derive(Debug, Clone)]
#[pyclass]
pub struct WireHeader {
    pub message_id: Uuid,       // 16 bytes
    pub topic_len: u16,         // variable topic length
    pub header_count: u16,      // number of headers
    pub total_len: u32,         // total wire message length
    pub timestamp_ms: i64,      // unix milliseconds
    pub ttl_ms: u32,            // time-to-live ms
    pub max_body_size: u32,     // max allowed body size
    pub flags: Flags,
    pub priority: Priority,
    pub key_id: u8,             // 2 bits used
}

/// Encode a WireHeader into a fixed-size [u8; 64] byte array.
/// Zero-copy: caller gets ownership of the stack-allocated array.
pub fn encode_header(header: &WireHeader) -> [u8; HEADER_SIZE] {
    let mut buf = [0u8; HEADER_SIZE];

    // Magic (0-3)
    buf[0..4].copy_from_slice(&MAGIC.to_be_bytes());

    // Version (4-9)
    buf[4..6].copy_from_slice(&VERSION_MAJOR.to_be_bytes());
    buf[6..8].copy_from_slice(&VERSION_MINOR.to_be_bytes());
    buf[8..10].copy_from_slice(&VERSION_PATCH.to_be_bytes());

    // Flags (10)
    buf[10] = header.flags.as_byte();

    // Priority (11)
    buf[11] = header.priority as u8;

    // Reserved (12-13)
    buf[12..14].copy_from_slice(&[0u8; 2]);

    // key_id + ttl_hi (14-15)
    // ttl_ms is u32, so ttl_hi is always 0 in this version.
    // key_id uses top 2 bits of the 16-bit field.
    let key_ttl = ((header.key_id & 0x03) as u16) << 14;
    buf[14..16].copy_from_slice(&key_ttl.to_be_bytes());

    // message_id (16-31) — UUID v7, 128-bit
    buf[16..32].copy_from_slice(header.message_id.as_bytes());

    // timestamp_ms (32-39) — i64 BE
    buf[32..40].copy_from_slice(&header.timestamp_ms.to_be_bytes());

    // ttl_ms_lo (40-43) — u32 BE
    buf[40..44].copy_from_slice(&(header.ttl_ms as u32).to_be_bytes());

    // max_body_size (44-47) — u32 BE
    buf[44..48].copy_from_slice(&header.max_body_size.to_be_bytes());

    // topic_len (48-49) — u16 BE
    buf[48..50].copy_from_slice(&header.topic_len.to_be_bytes());

    // header_count (50-51) — u16 BE
    buf[50..52].copy_from_slice(&header.header_count.to_be_bytes());

    // total_len (52-55) — u32 BE
    buf[52..56].copy_from_slice(&header.total_len.to_be_bytes());

    // checksum (56-63) — placeholder, filled after message body
    // Initially zero; caller must compute xxHash64 over header[0..56] + topic + message
    buf[56..64].copy_from_slice(&0u64.to_be_bytes());

    buf
}

/// Decode a WireHeader from a byte slice.
/// Validates magic bytes, major version, and priority field.
pub fn decode_header(buf: &[u8]) -> Result<WireHeader, KernelError> {
    if buf.len() < HEADER_SIZE {
        return Err(KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("buffer too short: {} < {}", buf.len(), HEADER_SIZE),
        });
    }

    // Magic check
    let magic = u32::from_be_bytes([buf[0], buf[1], buf[2], buf[3]]);
    if magic != MAGIC {
        return Err(KernelError::Wire {
            code: ErrorCode::MagicMismatch,
            detail: format!("expected 0x{:08X}, got 0x{:08X}", MAGIC, magic),
        });
    }

    let version_major = u16::from_be_bytes([buf[4], buf[5]]);
    if version_major != VERSION_MAJOR {
        return Err(KernelError::Wire {
            code: ErrorCode::VersionMismatch,
            detail: format!("expected major={}, got {}", VERSION_MAJOR, version_major),
        });
    }

    let flags = Flags::from_byte(buf[10]);
    let priority = match buf[11] {
        0 => Priority::Low,
        1 => Priority::Normal,
        2 => Priority::High,
        3 => Priority::Critical,
        v => return Err(KernelError::Wire {
            code: ErrorCode::InvalidPriority,
            detail: format!("invalid priority value: {}", v),
        }),
    };

    let key_ttl = u16::from_be_bytes([buf[14], buf[15]]);
    let key_id = ((key_ttl >> 14) & 0x03) as u8;
    // ttl_hi is reserved in this version (ttl_ms is u32), always 0
    let _ttl_hi = (key_ttl & 0x3FFF) as u32;

    let message_id = Uuid::from_slice(&buf[16..32]).map_err(|e| KernelError::Wire {
        code: ErrorCode::MalformedWire,
        detail: format!("invalid UUID: {}", e),
    })?;

    let timestamp_ms = i64::from_be_bytes([
        buf[32], buf[33], buf[34], buf[35], buf[36], buf[37], buf[38], buf[39],
    ]);

    let ttl_ms_lo = u32::from_be_bytes([buf[40], buf[41], buf[42], buf[43]]);
    let ttl_ms = ttl_ms_lo;

    let max_body_size = u32::from_be_bytes([buf[44], buf[45], buf[46], buf[47]]);
    let topic_len = u16::from_be_bytes([buf[48], buf[49]]);
    let header_count = u16::from_be_bytes([buf[50], buf[51]]);
    let total_len = u32::from_be_bytes([buf[52], buf[53], buf[54], buf[55]]);

    Ok(WireHeader {
        message_id,
        topic_len,
        header_count,
        total_len,
        timestamp_ms,
        ttl_ms,
        max_body_size,
        flags,
        priority,
        key_id,
    })
}

// ── PyO3 bindings ──

#[pymethods]
impl WireHeader {
    #[new]
    #[pyo3(signature = (message_id, topic_len, header_count, total_len,
                        timestamp_ms, ttl_ms, max_body_size,
                        priority="normal", flags=0, key_id=0))]
    fn py_new(
        message_id: Bound<'_, PyAny>,
        topic_len: u16,
        header_count: u16,
        total_len: u32,
        timestamp_ms: i64,
        ttl_ms: u32,
        max_body_size: u32,
        priority: &str,
        flags: u8,
        key_id: u8,
    ) -> PyResult<Self> {
        // Accept both uuid.UUID objects (via .hex) and strings
        let uuid_str: String = if let Ok(hex_attr) = message_id.getattr("hex") {
            hex_attr.extract()?
        } else {
            message_id.extract::<String>()?
        };

        let parsed = Uuid::try_parse(&uuid_str)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("invalid UUID: {}", e)
            ))?;

        let prio = match priority {
            "low" => Priority::Low,
            "normal" => Priority::Normal,
            "high" => Priority::High,
            "critical" => Priority::Critical,
            _ => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("invalid priority: {}", priority)
            )),
        };

        Ok(WireHeader {
            message_id: parsed,
            topic_len,
            header_count,
            total_len,
            timestamp_ms,
            ttl_ms,
            max_body_size,
            flags: Flags::from_byte(flags),
            priority: prio,
            key_id,
        })
    }

    #[getter]
    fn message_id(&self) -> String { self.message_id.to_string() }

    #[getter]
    fn topic_len(&self) -> u16 { self.topic_len }

    #[getter]
    fn header_count(&self) -> u16 { self.header_count }

    #[getter]
    fn total_len(&self) -> u32 { self.total_len }

    #[getter]
    fn timestamp_ms(&self) -> i64 { self.timestamp_ms }

    #[getter]
    fn ttl_ms(&self) -> u32 { self.ttl_ms }

    #[getter]
    fn max_body_size(&self) -> u32 { self.max_body_size }

    #[getter]
    fn flags(&self) -> u8 { self.flags.as_byte() }

    #[getter]
    fn priority(&self) -> &str {
        match self.priority {
            Priority::Low => "low",
            Priority::Normal => "normal",
            Priority::High => "high",
            Priority::Critical => "critical",
        }
    }

    #[getter]
    fn key_id(&self) -> u8 { self.key_id }
}

/// Python function: encode a WireHeader into bytes.
#[pyfunction]
pub fn wire_header_encode(py: Python<'_>, header: &WireHeader) -> Py<PyBytes> {
    let encoded = encode_header(header);
    PyBytes::new(py, &encoded).into()
}

/// Python function: decode bytes into a WireHeader.
#[pyfunction]
pub fn wire_header_decode(_py: Python<'_>, data: &[u8]) -> PyResult<WireHeader> {
    decode_header(data).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
        format!("{}", e)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_round_trip() {
        let id = Uuid::now_v7();
        let header = WireHeader {
            message_id: id,
            topic_len: 24,
            header_count: 0,
            total_len: 128,
            timestamp_ms: 1719820800000,
            ttl_ms: 30000,
            max_body_size: 10485760,
            flags: Flags::from_byte(0b00_00_01_00),
            priority: Priority::Normal,
            key_id: 0,
        };
        let encoded = encode_header(&header);
        assert_eq!(encoded.len(), 64);
        let decoded = decode_header(&encoded).unwrap();
        assert_eq!(decoded.message_id, id);
        assert_eq!(decoded.topic_len, 24);
        assert_eq!(decoded.total_len, 128);
        assert_eq!(decoded.timestamp_ms, 1719820800000);
        assert_eq!(decoded.ttl_ms, 30000);
        assert_eq!(decoded.priority, Priority::Normal);
        assert_eq!(decoded.flags.as_byte(), 0b00_00_01_00);
    }

    #[test]
    fn test_magic_bytes() {
        let header = WireHeader {
            message_id: Uuid::now_v7(),
            topic_len: 0,
            header_count: 0,
            total_len: 64,
            timestamp_ms: 0,
            ttl_ms: 0,
            max_body_size: 0,
            flags: Flags::from_byte(0),
            priority: Priority::Normal,
            key_id: 0,
        };
        let encoded = encode_header(&header);
        assert_eq!(&encoded[0..4], b"AQAP");
    }

    #[test]
    fn test_decode_invalid_magic() {
        let bad = [0u8; 64];
        let result = decode_header(&bad);
        assert!(result.is_err());
        match result.unwrap_err() {
            KernelError::Wire { code, .. } => assert_eq!(code, ErrorCode::MagicMismatch),
            _ => panic!("expected Wire error"),
        }
    }

    #[test]
    fn test_decode_buffer_too_short() {
        let short = [0u8; 32];
        let result = decode_header(&short);
        assert!(result.is_err());
    }

    #[test]
    fn test_decode_invalid_priority() {
        let mut buf = [0u8; 64];
        buf[0..4].copy_from_slice(&MAGIC.to_be_bytes());
        buf[4..6].copy_from_slice(&VERSION_MAJOR.to_be_bytes());
        buf[11] = 99; // invalid priority
        let result = decode_header(&buf);
        assert!(result.is_err());
    }

    #[test]
    fn test_version_fields() {
        let header = WireHeader {
            message_id: Uuid::now_v7(),
            topic_len: 0,
            header_count: 0,
            total_len: 64,
            timestamp_ms: 0,
            ttl_ms: 0,
            max_body_size: 0,
            flags: Flags::from_byte(0),
            priority: Priority::Normal,
            key_id: 0,
        };
        let encoded = encode_header(&header);
        assert_eq!(u16::from_be_bytes([encoded[4], encoded[5]]), 3);
        assert_eq!(u16::from_be_bytes([encoded[6], encoded[7]]), 0);
        assert_eq!(u16::from_be_bytes([encoded[8], encoded[9]]), 0);
    }

    #[test]
    fn test_key_id_ttl_packing() {
        // key_id uses 2 bits (0-3) in the top of bytes 14-15
        let header = WireHeader {
            message_id: Uuid::now_v7(),
            topic_len: 0,
            header_count: 0,
            total_len: 64,
            timestamp_ms: 0,
            ttl_ms: 30000,
            max_body_size: 0,
            flags: Flags::from_byte(0),
            priority: Priority::Normal,
            key_id: 2,
        };
        let encoded = encode_header(&header);
        let decoded = decode_header(&encoded).unwrap();
        assert_eq!(decoded.key_id, 2);
        assert_eq!(decoded.ttl_ms, 30000);
    }
}
