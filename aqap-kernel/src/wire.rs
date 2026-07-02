// aqap-kernel/src/wire.rs
use std::collections::HashMap;
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

// ── WireMessage ──

/// Full wire message
#[derive(Debug, Clone)]
#[pyclass]
pub struct WireMessage {
    // ── Header fields ──
    pub message_id: Uuid,
    pub topic: String,
    pub timestamp_ms: i64,
    pub ttl_ms: u32,
    pub max_body_size: u32,
    pub flags: Flags,
    pub priority: Priority,
    pub key_id: u8,

    // ── Message body fields ──
    pub trace_id: Uuid,
    pub span_id: u64,
    pub correlation_id: Uuid,
    pub msg_type: MessageType,
    pub source: String,
    pub target: String,
    pub headers: HashMap<String, String>,
    pub body: serde_json::Value,

    // ── Signature (optional) ──
    pub signature: Vec<u8>,
}

/// Encode a full wire message to bytes
pub fn encode_message(msg: &WireMessage) -> Vec<u8> {
    encode_message_with_flags(msg, msg.flags)
}

/// Encode a full wire message to bytes, using the provided flags instead of those on the message.
/// This avoids cloning the entire WireMessage when only the encoding/compression/signature flags differ.
pub fn encode_message_with_flags(msg: &WireMessage, flags: Flags) -> Vec<u8> {
    // 1. Encode message body
    let body_bytes = encode_body(&msg.body, flags.encoding());

    // 2. Build message block (trace_id through body)
    let mut msg_block = Vec::with_capacity(128 + body_bytes.len());

    // trace_id (16B)
    msg_block.extend_from_slice(msg.trace_id.as_bytes());
    // span_id (8B)
    msg_block.extend_from_slice(&msg.span_id.to_be_bytes());
    // correlation_id (16B)
    msg_block.extend_from_slice(msg.correlation_id.as_bytes());
    // type (2B)
    msg_block.extend_from_slice(&msg.msg_type.to_u16().to_be_bytes());
    // source_len (1B)
    let source_bytes = msg.source.as_bytes();
    msg_block.push(source_bytes.len() as u8);
    // target_len (1B)
    let target_bytes = msg.target.as_bytes();
    msg_block.push(target_bytes.len() as u8);
    // source + target
    msg_block.extend_from_slice(source_bytes);
    msg_block.extend_from_slice(target_bytes);
    // header_count (2B)
    msg_block.extend_from_slice(&(msg.headers.len() as u16).to_be_bytes());
    // headers
    for (k, v) in &msg.headers {
        let kb = k.as_bytes();
        let vb = v.as_bytes();
        msg_block.push(kb.len() as u8);
        msg_block.extend_from_slice(kb);
        msg_block.extend_from_slice(&(vb.len() as u16).to_be_bytes());
        msg_block.extend_from_slice(vb);
    }
    // body_len (2B) + body
    msg_block.extend_from_slice(&(body_bytes.len() as u16).to_be_bytes());
    msg_block.extend_from_slice(&body_bytes);

    // 3. Compress if needed
    let compressed = compress_if_needed(&msg_block, flags.compression());

    // 4. Build header
    let topic_bytes = msg.topic.as_bytes();
    let total_len = HEADER_SIZE + topic_bytes.len() + compressed.len();
    let sig_len = flags.signature_mode().sig_len();
    let total_with_sig = total_len + sig_len;

    let header = WireHeader {
        message_id: msg.message_id,
        topic_len: topic_bytes.len() as u16,
        header_count: msg.headers.len() as u16,
        total_len: total_with_sig as u32,
        timestamp_ms: msg.timestamp_ms,
        ttl_ms: msg.ttl_ms,
        max_body_size: msg.max_body_size,
        flags,
        priority: msg.priority,
        key_id: msg.key_id,
    };

    let mut hdr_buf = encode_header(&header);

    // 5. Compute checksum over header[0..56] + topic + compressed message
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
    hasher.update(&hdr_buf[0..56]);
    hasher.update(topic_bytes);
    hasher.update(&compressed);
    let checksum = hasher.digest();
    hdr_buf[56..64].copy_from_slice(&checksum.to_be_bytes());

    // 6. Assemble final buffer
    let mut result = Vec::with_capacity(total_with_sig);
    result.extend_from_slice(&hdr_buf);
    result.extend_from_slice(topic_bytes);
    result.extend_from_slice(&compressed);
    // Append signature if present
    if !msg.signature.is_empty() {
        result.extend_from_slice(&msg.signature);
    }
    result
}

/// Decode a full wire message from bytes
pub fn decode_message(buf: &[u8]) -> Result<WireMessage, KernelError> {
    // 1. Decode header
    let header = decode_header(buf)?;

    if buf.len() < HEADER_SIZE + header.topic_len as usize {
        return Err(KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "buffer too short for topic".into(),
        });
    }

    // 2. Extract topic
    let topic_start = HEADER_SIZE;
    let topic_end = topic_start + header.topic_len as usize;
    let topic = String::from_utf8(buf[topic_start..topic_end].to_vec())
        .map_err(|e| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("invalid UTF-8 in topic: {}", e),
        })?;

    // 3. Extract message block
    let msg_start = topic_end;
    let sig_len = header.flags.signature_mode().sig_len();
    let msg_end = buf.len() - sig_len;
    let compressed = &buf[msg_start..msg_end];

    // 4. Verify checksum
    let expected_checksum = u64::from_be_bytes([
        buf[56], buf[57], buf[58], buf[59], buf[60], buf[61], buf[62], buf[63],
    ]);
    let mut hasher = xxhash_rust::xxh64::Xxh64::new(0);
    hasher.update(&buf[0..56]);
    hasher.update(buf[topic_start..topic_end].as_ref());
    hasher.update(compressed);
    let actual = hasher.digest();
    if expected_checksum != 0 && actual != expected_checksum {
        return Err(KernelError::Wire {
            code: ErrorCode::ChecksumMismatch,
            detail: format!("expected {:016x}, got {:016x}", expected_checksum, actual),
        });
    }

    // 5. Decompress if needed
    let msg_block = decompress_if_needed(compressed, header.flags.compression())?;

    // 6. Decode message block
    let mut offset = 0;
    if msg_block.len() < 44 {
        return Err(KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "message block too short".into(),
        });
    }

    let trace_id = Uuid::from_slice(&msg_block[offset..offset+16])
        .map_err(|e| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("invalid trace_id: {}", e),
        })?;
    offset += 16;

    let span_id = u64::from_be_bytes(msg_block[offset..offset+8].try_into().unwrap());
    offset += 8;

    let correlation_id = Uuid::from_slice(&msg_block[offset..offset+16])
        .map_err(|_| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "invalid correlation_id".into(),
        })?;
    offset += 16;

    let msg_type = MessageType::from_u16(
        u16::from_be_bytes([msg_block[offset], msg_block[offset+1]])
    );
    offset += 2;

    let source_len = msg_block[offset] as usize;
    offset += 1;
    let target_len = msg_block[offset] as usize;
    offset += 1;

    if offset + source_len + target_len > msg_block.len() {
        return Err(KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "source/target overflow".into(),
        });
    }

    let source = String::from_utf8(msg_block[offset..offset+source_len].to_vec())
        .map_err(|_| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "invalid UTF-8 in source".into(),
        })?;
    offset += source_len;

    let target = String::from_utf8(msg_block[offset..offset+target_len].to_vec())
        .map_err(|_| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "invalid UTF-8 in target".into(),
        })?;
    offset += target_len;

    if offset + 2 > msg_block.len() {
        return Err(KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "header_count overflow".into(),
        });
    }

    let header_count = u16::from_be_bytes([msg_block[offset], msg_block[offset+1]]) as usize;
    offset += 2;

    let mut headers = HashMap::with_capacity(header_count);
    for _ in 0..header_count {
        if offset + 1 > msg_block.len() { break; }
        let key_len = msg_block[offset] as usize;
        offset += 1;
        if offset + key_len + 2 > msg_block.len() { break; }
        let key = String::from_utf8(msg_block[offset..offset+key_len].to_vec())
            .unwrap_or_default();
        offset += key_len;
        let val_len = u16::from_be_bytes([msg_block[offset], msg_block[offset+1]]) as usize;
        offset += 2;
        if offset + val_len > msg_block.len() { break; }
        let val = String::from_utf8(msg_block[offset..offset+val_len].to_vec())
            .unwrap_or_default();
        offset += val_len;
        headers.insert(key, val);
    }

    if offset + 2 > msg_block.len() {
        return Err(KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "body_len overflow".into(),
        });
    }

    let body_len = u16::from_be_bytes([msg_block[offset], msg_block[offset+1]]) as usize;
    offset += 2;

    if offset + body_len > msg_block.len() {
        return Err(KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: "body overflow".into(),
        });
    }

    let body = decode_body(&msg_block[offset..offset+body_len], header.flags.encoding())?;

    // 7. Extract signature
    let signature = if sig_len > 0 && msg_end < buf.len() {
        buf[msg_end..].to_vec()
    } else {
        Vec::new()
    };

    Ok(WireMessage {
        message_id: header.message_id,
        topic,
        timestamp_ms: header.timestamp_ms,
        ttl_ms: header.ttl_ms,
        max_body_size: header.max_body_size,
        flags: header.flags,
        priority: header.priority,
        key_id: header.key_id,
        trace_id,
        span_id,
        correlation_id,
        msg_type,
        source,
        target,
        headers,
        body,
        signature,
    })
}

// ── Encoding helpers ──

fn encode_body(body: &serde_json::Value, encoding: Encoding) -> Vec<u8> {
    match encoding {
        Encoding::Json => serde_json::to_vec(body).unwrap_or_default(),
        Encoding::MsgPack => rmp_serde::to_vec(body).unwrap_or_default(),
        // Protobuf and FlatBuffer require schema — fallback to JSON
        _ => serde_json::to_vec(body).unwrap_or_default(),
    }
}

fn decode_body(data: &[u8], encoding: Encoding) -> Result<serde_json::Value, KernelError> {
    match encoding {
        Encoding::Json => serde_json::from_slice(data).map_err(|e| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("JSON decode: {}", e),
        }),
        Encoding::MsgPack => rmp_serde::from_slice(data).map_err(|e| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("MsgPack decode: {}", e),
        }),
        _ => serde_json::from_slice(data).map_err(|e| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("decode: {}", e),
        }),
    }
}

fn compress_if_needed(data: &[u8], compression: Compression) -> Vec<u8> {
    match compression {
        Compression::None => data.to_vec(),
        Compression::Zstd => zstd::encode_all(data, 3).unwrap_or_else(|_| data.to_vec()),
        Compression::Lz4 => lz4::block::compress(data, None, false).unwrap_or_else(|_| data.to_vec()),
        Compression::Zlib => {
            use flate2::Compression as Flate2Compression;
            let mut e = flate2::write::ZlibEncoder::new(Vec::new(), Flate2Compression::fast());
            std::io::Write::write_all(&mut e, data).unwrap();
            e.finish().unwrap_or_else(|_| data.to_vec())
        }
    }
}

fn decompress_if_needed(data: &[u8], compression: Compression) -> Result<Vec<u8>, KernelError> {
    match compression {
        Compression::None => Ok(data.to_vec()),
        Compression::Zstd => zstd::decode_all(data).map_err(|e| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("zstd decompress: {}", e),
        }),
        Compression::Lz4 => lz4::block::decompress(data, None).map_err(|e| KernelError::Wire {
            code: ErrorCode::MalformedWire,
            detail: format!("lz4 decompress: {}", e),
        }),
        Compression::Zlib => {
            use std::io::Read;
            let mut d = flate2::read::ZlibDecoder::new(data);
            let mut out = Vec::new();
            d.read_to_end(&mut out).map_err(|e| KernelError::Wire {
                code: ErrorCode::MalformedWire,
                detail: format!("zlib decompress: {}", e),
            })?;
            Ok(out)
        }
    }
}

// ── PyO3 bindings for WireMessage ──

fn extract_uuid(obj: &Bound<'_, PyAny>) -> PyResult<Uuid> {
    let uuid_str: String = if let Ok(hex_attr) = obj.getattr("hex") {
        hex_attr.extract()?
    } else {
        obj.extract::<String>()?
    };
    Uuid::try_parse(&uuid_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("invalid UUID: {}", e)
        ))
}

fn extract_body_json(obj: &Bound<'_, PyAny>, py: Python<'_>) -> PyResult<serde_json::Value> {
    // Use Python's json module to serialize the Python object to a JSON string,
    // then parse it as serde_json::Value
    let json_mod = py.import("json")?;
    let json_str: String = json_mod.call_method1("dumps", (obj,))?
        .extract()?;
    serde_json::from_str(&json_str)
        .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
            format!("failed to serialize body to JSON: {}", e)
        ))
}

#[pymethods]
impl WireMessage {
    #[new]
    #[allow(clippy::too_many_arguments)]
    #[pyo3(signature = (
        message_id, topic, trace_id, span_id, source, target,
        correlation_id, msg_type, body, headers,
        encoding="json", compression="none", signature_mode="none",
        priority="normal", ttl_ms=30000, max_body_size=10485760,
        signature=None, timestamp_ms=None, key_id=0
    ))]
    fn py_new(
        message_id: Bound<'_, PyAny>,
        topic: String,
        trace_id: Bound<'_, PyAny>,
        span_id: u64,
        source: String,
        target: String,
        correlation_id: Bound<'_, PyAny>,
        msg_type: String,
        body: Bound<'_, PyAny>,
        headers: HashMap<String, String>,
        encoding: &str,
        compression: &str,
        signature_mode: &str,
        priority: &str,
        ttl_ms: u32,
        max_body_size: u32,
        signature: Option<Vec<u8>>,
        timestamp_ms: Option<i64>,
        key_id: u8,
    ) -> PyResult<Self> {
        let msg_id = extract_uuid(&message_id)?;
        let trace = extract_uuid(&trace_id)?;
        let corr = extract_uuid(&correlation_id)?;

        let enc = match encoding {
            "json" => Encoding::Json,
            "protobuf" => Encoding::Protobuf,
            "msgpack" => Encoding::MsgPack,
            "flatbuffer" => Encoding::FlatBuffer,
            other => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("invalid encoding: {}", other)
            )),
        };

        let comp = match compression {
            "none" => Compression::None,
            "zstd" => Compression::Zstd,
            "lz4" => Compression::Lz4,
            "zlib" => Compression::Zlib,
            other => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("invalid compression: {}", other)
            )),
        };

        let sig_mode = match signature_mode {
            "none" => SignatureMode::None,
            "hmac-sha256" => SignatureMode::HmacSha256,
            "ed25519" => SignatureMode::Ed25519,
            other => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("invalid signature_mode: {}", other)
            )),
        };

        let prio = match priority {
            "low" => Priority::Low,
            "normal" => Priority::Normal,
            "high" => Priority::High,
            "critical" => Priority::Critical,
            other => return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("invalid priority: {}", other)
            )),
        };

        let mtype = MessageType::from_str(&msg_type)
            .unwrap_or_else(|| MessageType::Custom(0x0FFF));

        let flags = Flags::new(enc, comp, sig_mode);

        let ts = timestamp_ms.unwrap_or_else(|| {
            use std::time::{SystemTime, UNIX_EPOCH};
            SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as i64)
                .unwrap_or(0)
        });

        let py = unsafe { Python::assume_gil_acquired() };
        let body_val = extract_body_json(&body, py)?;

        Ok(WireMessage {
            message_id: msg_id,
            topic,
            timestamp_ms: ts,
            ttl_ms,
            max_body_size,
            flags,
            priority: prio,
            key_id,
            trace_id: trace,
            span_id,
            correlation_id: corr,
            msg_type: mtype,
            source,
            target,
            headers,
            body: body_val,
            signature: signature.unwrap_or_default(),
        })
    }

    #[getter] fn topic(&self) -> &str { &self.topic }
    #[getter] fn source(&self) -> &str { &self.source }
    #[getter] fn target(&self) -> &str { &self.target }
    #[getter] fn trace_id(&self) -> String { self.trace_id.to_string() }
    #[getter] fn span_id(&self) -> u64 { self.span_id }
    #[getter] fn correlation_id(&self) -> String { self.correlation_id.to_string() }
    #[getter] fn msg_type(&self) -> &str { self.msg_type.to_str() }
    #[getter]
    fn body<'py>(&self, py: Python<'py>) -> PyResult<Bound<'py, PyAny>> {
        // Serialize serde_json::Value to JSON string, then parse with Python json module
        let json_str = serde_json::to_string(&self.body)
            .map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("failed to serialize body: {}", e)
            ))?;
        let json_mod = py.import("json")?;
        json_mod.call_method1("loads", (json_str,))
    }
    #[getter] fn headers(&self) -> HashMap<String, String> { self.headers.clone() }
    #[getter] fn signature_mode(&self) -> &str {
        match self.flags.signature_mode() {
            SignatureMode::None => "none",
            SignatureMode::HmacSha256 => "hmac-sha256",
            SignatureMode::Ed25519 => "ed25519",
            SignatureMode::Reserved => "reserved",
        }
    }
    #[getter] fn encoding(&self) -> &str {
        match self.flags.encoding() {
            Encoding::Json => "json",
            Encoding::Protobuf => "protobuf",
            Encoding::MsgPack => "msgpack",
            Encoding::FlatBuffer => "flatbuffer",
        }
    }
    #[getter] fn compression(&self) -> &str {
        match self.flags.compression() {
            Compression::None => "none",
            Compression::Zstd => "zstd",
            Compression::Lz4 => "lz4",
            Compression::Zlib => "zlib",
        }
    }
    #[getter] fn priority(&self) -> &str {
        match self.priority {
            Priority::Low => "low", Priority::Normal => "normal",
            Priority::High => "high", Priority::Critical => "critical",
        }
    }
    #[getter] fn ttl_ms(&self) -> u32 { self.ttl_ms }
    #[getter] fn signature(&self) -> Vec<u8> { self.signature.clone() }
    #[getter] fn message_id(&self) -> String { self.message_id.to_string() }
    #[getter] fn timestamp_ms(&self) -> i64 { self.timestamp_ms }
}

#[pyfunction]
pub fn wire_message_encode(py: Python<'_>, msg: &WireMessage, encoding: &str) -> PyResult<Py<PyBytes>> {
    let adjusted_flags = match encoding {
        "json" => Flags::new(Encoding::Json, msg.flags.compression(), msg.flags.signature_mode()),
        "msgpack" => Flags::new(Encoding::MsgPack, msg.flags.compression(), msg.flags.signature_mode()),
        other => {
            return Err(PyErr::new::<pyo3::exceptions::PyValueError, _>(
                format!("unsupported encoding: '{}', expected 'json' or 'msgpack'", other)
            ));
        }
    };
    let bytes = encode_message_with_flags(msg, adjusted_flags);
    Ok(PyBytes::new(py, &bytes).into())
}

#[pyfunction]
pub fn wire_message_decode(_py: Python<'_>, data: &[u8]) -> PyResult<WireMessage> {
    decode_message(data).map_err(|e| PyErr::new::<pyo3::exceptions::PyValueError, _>(
        format!("{}", e)
    ))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    // ── Header tests ──

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

    // ── Message round-trip tests ──

    fn make_test_msg(compression: Compression, sig_mode: SignatureMode) -> WireMessage {
        let mut headers = HashMap::new();
        headers.insert("x-priority".to_string(), "high".to_string());

        WireMessage {
            message_id: Uuid::now_v7(),
            topic: "aqap:v3:agent:probe".to_string(),
            timestamp_ms: 1719820800000,
            ttl_ms: 30000,
            max_body_size: 10485760,
            flags: Flags::new(Encoding::Json, compression, sig_mode),
            priority: Priority::Normal,
            key_id: 0,
            trace_id: Uuid::now_v7(),
            span_id: 42,
            correlation_id: Uuid::nil(),
            msg_type: MessageType::TaskDispatch,
            source: "scheduler-1".to_string(),
            target: "".to_string(),
            headers,
            body: serde_json::json!({"task_id": "task-abc12345", "type": "code_review"}),
            signature: vec![0u8; 32],
        }
    }

    #[test]
    fn test_message_round_trip_no_compression() {
        let msg = make_test_msg(Compression::None, SignatureMode::None);
        // Override: no signature for this test
        let mut msg = msg;
        msg.signature = Vec::new();
        msg.flags = Flags::new(Encoding::Json, Compression::None, SignatureMode::None);

        let encoded = encode_message(&msg);
        assert!(encoded.len() > 64, "encoded should be at least header size");
        let decoded = decode_message(&encoded).unwrap();

        assert_eq!(decoded.topic, "aqap:v3:agent:probe");
        assert_eq!(decoded.source, "scheduler-1");
        assert_eq!(decoded.target, "");
        assert_eq!(decoded.msg_type, MessageType::TaskDispatch);
        assert_eq!(decoded.span_id, 42);
        assert_eq!(decoded.trace_id, msg.trace_id);
        assert_eq!(decoded.correlation_id, Uuid::nil());
        assert_eq!(decoded.body, serde_json::json!({"task_id": "task-abc12345", "type": "code_review"}));
        assert_eq!(decoded.headers.get("x-priority").map(|s| s.as_str()), Some("high"));
    }

    #[test]
    fn test_message_round_trip_with_signature() {
        let msg = make_test_msg(Compression::None, SignatureMode::HmacSha256);

        let encoded = encode_message(&msg);
        assert!(encoded.len() > 64 + 32, "encoded should include 32-byte signature");
        let decoded = decode_message(&encoded).unwrap();

        assert_eq!(decoded.body, serde_json::json!({"task_id": "task-abc12345", "type": "code_review"}));
        assert_eq!(decoded.flags.signature_mode(), SignatureMode::HmacSha256);
        assert_eq!(decoded.signature.len(), 32);
    }

    #[test]
    fn test_message_round_trip_zstd_compression() {
        let mut msg = make_test_msg(Compression::Zstd, SignatureMode::None);
        msg.signature = Vec::new();
        msg.flags = Flags::new(Encoding::Json, Compression::Zstd, SignatureMode::None);
        msg.body = serde_json::json!({"data": "x".repeat(1000)});

        let encoded = encode_message(&msg);
        // Zstd compression should make it smaller than uncompressed
        assert!(encoded.len() < 64 + 1000 + 20, "zstd should compress repetitive data");
        let decoded = decode_message(&encoded).unwrap();

        assert_eq!(decoded.body, serde_json::json!({"data": "x".repeat(1000)}));
    }

    #[test]
    fn test_message_checksum_mismatch_detected() {
        let mut msg = make_test_msg(Compression::None, SignatureMode::None);
        msg.signature = Vec::new();
        msg.flags = Flags::new(Encoding::Json, Compression::None, SignatureMode::None);

        let mut encoded = encode_message(&msg);
        // Corrupt a byte in the message block (after header + topic), not in topic itself
        // topic_len should be in the header; we need past HEADER_SIZE + topic_len
        let topic_len = 14; // "aqap:v3:agent:probe".len() == 21, but let's use header field
        let hdr = decode_header(&encoded).unwrap();
        let msg_block_start = HEADER_SIZE + hdr.topic_len as usize + 1;
        if encoded.len() > msg_block_start {
            encoded[msg_block_start] ^= 0xFF;
        }

        let result = decode_message(&encoded);
        assert!(result.is_err());
        match result.unwrap_err() {
            KernelError::Wire { code, .. } => assert_eq!(code, ErrorCode::ChecksumMismatch),
            _ => panic!("expected ChecksumMismatch"),
        }
    }
}
