// aqap-kernel/src/types.rs
use serde::{Deserialize, Serialize};

/// AQAP v3 MessageType — u16 wire encoding
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u16)]
pub enum MessageType {
    // ── Task flow ──
    TaskDispatch   = 0x0000,
    TaskResult     = 0x0001,
    TaskCancel     = 0x0002,

    // ── Judge flow ──
    JudgeRequest   = 0x0010,
    JudgeVerdict   = 0x0011,

    // ── Report flow ──
    ReportRequest  = 0x0020,
    ReportDeliver  = 0x0021,

    // ── System ──
    Heartbeat      = 0x0100,
    Register       = 0x0101,
    Shutdown       = 0x0102,
    Error          = 0x0103,
    KeyRotate      = 0x0104,
    GatewayRoute   = 0x0105,
    DlqMessage     = 0x0106,

    // ── Agent roles ──
    Probe          = 0x0200,
    Judge          = 0x0201,
    Report         = 0x0202,
    Result         = 0x0203,

    // ── Plugin ──
    PluginEvent    = 0x0300,

    // ── Custom range ──
    Custom(u16)    = 0x0FFF,
}

impl MessageType {
    /// Convert from u16 wire value
    pub fn from_u16(v: u16) -> Self {
        match v {
            0x0000 => Self::TaskDispatch,
            0x0001 => Self::TaskResult,
            0x0002 => Self::TaskCancel,
            0x0010 => Self::JudgeRequest,
            0x0011 => Self::JudgeVerdict,
            0x0020 => Self::ReportRequest,
            0x0021 => Self::ReportDeliver,
            0x0100 => Self::Heartbeat,
            0x0101 => Self::Register,
            0x0102 => Self::Shutdown,
            0x0103 => Self::Error,
            0x0104 => Self::KeyRotate,
            0x0105 => Self::GatewayRoute,
            0x0106 => Self::DlqMessage,
            0x0200 => Self::Probe,
            0x0201 => Self::Judge,
            0x0202 => Self::Report,
            0x0203 => Self::Result,
            0x0300 => Self::PluginEvent,
            _ => Self::Custom(v),
        }
    }

    pub fn to_u16(self) -> u16 {
        match self {
            Self::TaskDispatch   => 0x0000,
            Self::TaskResult     => 0x0001,
            Self::TaskCancel     => 0x0002,
            Self::JudgeRequest   => 0x0010,
            Self::JudgeVerdict   => 0x0011,
            Self::ReportRequest  => 0x0020,
            Self::ReportDeliver  => 0x0021,
            Self::Heartbeat      => 0x0100,
            Self::Register       => 0x0101,
            Self::Shutdown       => 0x0102,
            Self::Error          => 0x0103,
            Self::KeyRotate      => 0x0104,
            Self::GatewayRoute   => 0x0105,
            Self::DlqMessage     => 0x0106,
            Self::Probe          => 0x0200,
            Self::Judge          => 0x0201,
            Self::Report         => 0x0202,
            Self::Result         => 0x0203,
            Self::PluginEvent    => 0x0300,
            Self::Custom(v)      => v,
        }
    }

    /// Human-readable string for JSON compatibility mode
    pub fn to_str(self) -> &'static str {
        match self {
            Self::TaskDispatch  => "task:dispatch",
            Self::TaskResult    => "task:result",
            Self::TaskCancel    => "task:cancel",
            Self::JudgeRequest  => "judge:request",
            Self::JudgeVerdict  => "judge:verdict",
            Self::ReportRequest => "report:request",
            Self::ReportDeliver => "report:deliver",
            Self::Heartbeat     => "system:heartbeat",
            Self::Register      => "system:register",
            Self::Shutdown      => "system:shutdown",
            Self::Error         => "system:error",
            Self::KeyRotate     => "system:key-rotate",
            Self::GatewayRoute  => "system:gateway-route",
            Self::DlqMessage    => "system:dlq",
            Self::Probe         => "agent:probe",
            Self::Judge         => "agent:judge",
            Self::Report        => "agent:report",
            Self::Result        => "agent:result",
            Self::PluginEvent   => "plugin:event",
            Self::Custom(_)     => "custom",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "task:dispatch"     => Some(Self::TaskDispatch),
            "task:result"       => Some(Self::TaskResult),
            "task:cancel"       => Some(Self::TaskCancel),
            "judge:request"     => Some(Self::JudgeRequest),
            "judge:verdict"     => Some(Self::JudgeVerdict),
            "report:request"    => Some(Self::ReportRequest),
            "report:deliver"    => Some(Self::ReportDeliver),
            "system:heartbeat"  => Some(Self::Heartbeat),
            "system:register"   => Some(Self::Register),
            "system:shutdown"   => Some(Self::Shutdown),
            "system:error"      => Some(Self::Error),
            "system:key-rotate" => Some(Self::KeyRotate),
            "system:gateway-route" => Some(Self::GatewayRoute),
            "system:dlq"        => Some(Self::DlqMessage),
            "agent:probe"       => Some(Self::Probe),
            "agent:judge"       => Some(Self::Judge),
            "agent:report"      => Some(Self::Report),
            "agent:result"      => Some(Self::Result),
            "plugin:event"      => Some(Self::PluginEvent),
            _ => None,
        }
    }
}

/// Encoding format (flags bits 7-6)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Encoding {
    Json      = 0b00,
    Protobuf  = 0b01,
    MsgPack   = 0b10,
    FlatBuffer = 0b11,
}

/// Compression algorithm (flags bits 5-4)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Compression {
    None = 0b00,
    Zstd = 0b01,
    Lz4  = 0b10,
    Zlib = 0b11,
}

/// Signature mode (flags bits 3-2)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum SignatureMode {
    None        = 0b00,
    HmacSha256  = 0b01,  // 32 bytes
    Ed25519     = 0b10,  // 64 bytes
    Reserved    = 0b11,
}

impl SignatureMode {
    pub fn sig_len(self) -> usize {
        match self {
            Self::None => 0,
            Self::HmacSha256 => 32,
            Self::Ed25519 => 64,
            Self::Reserved => 0,
        }
    }
}

/// Priority (offset 11)
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
#[repr(u8)]
pub enum Priority {
    Low      = 0,
    Normal   = 1,
    High     = 2,
    Critical = 3,
}

/// Compact flags byte (offset 10)
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Flags(u8);

impl Flags {
    pub fn new(encoding: Encoding, compression: Compression, signature: SignatureMode) -> Self {
        let e = (encoding as u8) << 6;
        let c = (compression as u8) << 4;
        let s = (signature as u8) << 2;
        Self(e | c | s)
    }

    pub fn encoding(self) -> Encoding {
        match (self.0 >> 6) & 0b11 {
            0b00 => Encoding::Json,
            0b01 => Encoding::Protobuf,
            0b10 => Encoding::MsgPack,
            _    => Encoding::FlatBuffer,
        }
    }

    pub fn compression(self) -> Compression {
        match (self.0 >> 4) & 0b11 {
            0b00 => Compression::None,
            0b01 => Compression::Zstd,
            0b10 => Compression::Lz4,
            _    => Compression::Zlib,
        }
    }

    pub fn signature_mode(self) -> SignatureMode {
        match (self.0 >> 2) & 0b11 {
            0b00 => SignatureMode::None,
            0b01 => SignatureMode::HmacSha256,
            0b10 => SignatureMode::Ed25519,
            _    => SignatureMode::Reserved,
        }
    }

    pub fn as_byte(self) -> u8 { self.0 }
    pub fn from_byte(b: u8) -> Self { Self(b & 0b1111_1100) }
}

/// Wire message header: magic + version + timestamp
pub const MAGIC: u32 = 0x4151_4150; // "AQAP"
pub const VERSION_MAJOR: u16 = 3;
pub const VERSION_MINOR: u16 = 0;
pub const VERSION_PATCH: u16 = 0;
pub const HEADER_SIZE: usize = 64;

#[cfg(test)]
mod tests {
    use super::*;

    // ── MessageType tests ──

    #[test]
    fn test_message_type_u16_roundtrip_all_variants() {
        let variants: &[(MessageType, u16)] = &[
            (MessageType::TaskDispatch, 0x0000),
            (MessageType::TaskResult, 0x0001),
            (MessageType::TaskCancel, 0x0002),
            (MessageType::JudgeRequest, 0x0010),
            (MessageType::JudgeVerdict, 0x0011),
            (MessageType::ReportRequest, 0x0020),
            (MessageType::ReportDeliver, 0x0021),
            (MessageType::Heartbeat, 0x0100),
            (MessageType::Register, 0x0101),
            (MessageType::Shutdown, 0x0102),
            (MessageType::Error, 0x0103),
            (MessageType::KeyRotate, 0x0104),
            (MessageType::GatewayRoute, 0x0105),
            (MessageType::DlqMessage, 0x0106),
            (MessageType::Probe, 0x0200),
            (MessageType::Judge, 0x0201),
            (MessageType::Report, 0x0202),
            (MessageType::Result, 0x0203),
            (MessageType::PluginEvent, 0x0300),
        ];
        for (variant, expected_code) in variants {
            assert_eq!(variant.to_u16(), *expected_code, "to_u16 failed for {:?}", variant);
            assert_eq!(MessageType::from_u16(*expected_code), *variant, "from_u16 failed for 0x{:04x}", expected_code);
        }
    }

    #[test]
    fn test_message_type_custom_range() {
        // Values >= 0x0FFF map to Custom(v)
        assert_eq!(MessageType::from_u16(0x0FFF), MessageType::Custom(0x0FFF));
        assert_eq!(MessageType::from_u16(0x1000), MessageType::Custom(0x1000));
        assert_eq!(MessageType::from_u16(0xFFFF), MessageType::Custom(0xFFFF));
        // Unknown values below 0x0FFF also map to Custom(v)
        assert_eq!(MessageType::from_u16(0x0999), MessageType::Custom(0x0999));
    }

    #[test]
    fn test_message_type_custom_roundtrip() {
        let custom = MessageType::Custom(0x1234);
        assert_eq!(custom.to_u16(), 0x1234);
        assert_eq!(MessageType::from_u16(0x1234), custom);
    }

    #[test]
    fn test_message_type_str_roundtrip_all_variants() {
        let pairs = [
            (MessageType::TaskDispatch, "task:dispatch"),
            (MessageType::TaskResult, "task:result"),
            (MessageType::TaskCancel, "task:cancel"),
            (MessageType::JudgeRequest, "judge:request"),
            (MessageType::JudgeVerdict, "judge:verdict"),
            (MessageType::ReportRequest, "report:request"),
            (MessageType::ReportDeliver, "report:deliver"),
            (MessageType::Heartbeat, "system:heartbeat"),
            (MessageType::Register, "system:register"),
            (MessageType::Shutdown, "system:shutdown"),
            (MessageType::Error, "system:error"),
            (MessageType::KeyRotate, "system:key-rotate"),
            (MessageType::GatewayRoute, "system:gateway-route"),
            (MessageType::DlqMessage, "system:dlq"),
            (MessageType::Probe, "agent:probe"),
            (MessageType::Judge, "agent:judge"),
            (MessageType::Report, "agent:report"),
            (MessageType::Result, "agent:result"),
            (MessageType::PluginEvent, "plugin:event"),
        ];
        for (variant, expected_str) in pairs {
            assert_eq!(variant.to_str(), expected_str, "to_str failed for {:?}", variant);
            assert_eq!(MessageType::from_str(expected_str), Some(variant), "from_str failed for {}", expected_str);
        }
    }

    #[test]
    fn test_message_type_from_str_unknown_returns_none() {
        assert_eq!(MessageType::from_str("bogus:value"), None);
        assert_eq!(MessageType::from_str(""), None);
        assert_eq!(MessageType::from_str("unknown"), None);
    }

    #[test]
    fn test_message_type_custom_to_str() {
        assert_eq!(MessageType::Custom(0x1234).to_str(), "custom");
        assert_eq!(MessageType::Custom(0xFFFF).to_str(), "custom");
    }

    #[test]
    fn test_message_type_custom_from_str_not_supported() {
        // "custom" strings do NOT round-trip via from_str
        assert_eq!(MessageType::from_str("custom"), None);
    }

    // ── Flags tests ──

    #[test]
    fn test_flags_encode_decode_roundtrip() {
        let flag = Flags::new(Encoding::MsgPack, Compression::Zstd, SignatureMode::Ed25519);
        assert_eq!(flag.encoding(), Encoding::MsgPack);
        assert_eq!(flag.compression(), Compression::Zstd);
        assert_eq!(flag.signature_mode(), SignatureMode::Ed25519);
    }

    #[test]
    fn test_flags_json_none_no_signature() {
        let flag = Flags::new(Encoding::Json, Compression::None, SignatureMode::None);
        assert_eq!(flag.as_byte(), 0b00_00_00_00);
        assert_eq!(flag.encoding(), Encoding::Json);
        assert_eq!(flag.compression(), Compression::None);
        assert_eq!(flag.signature_mode(), SignatureMode::None);
    }

    #[test]
    fn test_flags_all_combinations() {
        // Test all encoding/compression combinations with HmacSha256
        for enc in [Encoding::Json, Encoding::Protobuf, Encoding::MsgPack, Encoding::FlatBuffer] {
            for comp in [Compression::None, Compression::Zstd, Compression::Lz4, Compression::Zlib] {
                for sig in [SignatureMode::None, SignatureMode::HmacSha256, SignatureMode::Ed25519, SignatureMode::Reserved] {
                    let flag = Flags::new(enc, comp, sig);
                    assert_eq!(flag.encoding(), enc, "encoding mismatch: {:?} {:?} {:?}", enc, comp, sig);
                    assert_eq!(flag.compression(), comp, "compression mismatch: {:?} {:?} {:?}", enc, comp, sig);
                    assert_eq!(flag.signature_mode(), sig, "signature mismatch: {:?} {:?} {:?}", enc, comp, sig);
                }
            }
        }
    }

    #[test]
    fn test_flags_from_byte_masks_high_bits() {
        // from_byte masks to bits 7-2 (0b1111_1100)
        let flag = Flags::from_byte(0xFF); // all 8 bits set
        assert_eq!(flag.as_byte(), 0b1111_1100); // bottom 2 bits zeroed
        assert_eq!(flag.encoding(), Encoding::FlatBuffer);
        assert_eq!(flag.compression(), Compression::Zlib);
        assert_eq!(flag.signature_mode(), SignatureMode::Reserved);
    }

    #[test]
    fn test_flags_as_byte_from_byte_roundtrip() {
        let flag = Flags::new(Encoding::Json, Compression::Zstd, SignatureMode::HmacSha256);
        let byte = flag.as_byte();
        let flag2 = Flags::from_byte(byte);
        assert_eq!(flag, flag2);
    }

    // ── SignatureMode tests ──

    #[test]
    fn test_signature_mode_sig_len() {
        assert_eq!(SignatureMode::None.sig_len(), 0);
        assert_eq!(SignatureMode::HmacSha256.sig_len(), 32);
        assert_eq!(SignatureMode::Ed25519.sig_len(), 64);
        assert_eq!(SignatureMode::Reserved.sig_len(), 0);
    }

    // ── Priority tests ──

    #[test]
    fn test_priority_ordering() {
        assert!(Priority::Critical > Priority::High);
        assert!(Priority::High > Priority::Normal);
        assert!(Priority::Normal > Priority::Low);
        // repr values
        assert_eq!(Priority::Low as u8, 0);
        assert_eq!(Priority::Normal as u8, 1);
        assert_eq!(Priority::High as u8, 2);
        assert_eq!(Priority::Critical as u8, 3);
    }

    // ── Constants tests ──

    #[test]
    fn test_magic_is_aqap() {
        assert_eq!(MAGIC, 0x4151_4150);
        // Verify ASCII: "A"=0x41, "Q"=0x51, "A"=0x41, "P"=0x50
        let bytes = MAGIC.to_be_bytes();
        assert_eq!(bytes, [0x41, 0x51, 0x41, 0x50]);
    }

    #[test]
    fn test_header_size() {
        assert_eq!(HEADER_SIZE, 64);
    }

    #[test]
    fn test_version_constants() {
        assert_eq!(VERSION_MAJOR, 3);
        assert_eq!(VERSION_MINOR, 0);
        assert_eq!(VERSION_PATCH, 0);
    }

    // ── Serde roundtrip tests ──

    #[test]
    fn test_message_type_serde_json() {
        let mt = MessageType::TaskDispatch;
        let json = serde_json::to_string(&mt).unwrap();
        let back: MessageType = serde_json::from_str(&json).unwrap();
        assert_eq!(mt, back);
    }

    #[test]
    fn test_message_type_serde_all_variants() {
        let variants = [
            MessageType::TaskDispatch,
            MessageType::TaskResult,
            MessageType::TaskCancel,
            MessageType::JudgeRequest,
            MessageType::JudgeVerdict,
            MessageType::ReportRequest,
            MessageType::ReportDeliver,
            MessageType::Heartbeat,
            MessageType::Register,
            MessageType::Shutdown,
            MessageType::Error,
            MessageType::KeyRotate,
            MessageType::GatewayRoute,
            MessageType::DlqMessage,
            MessageType::Probe,
            MessageType::Judge,
            MessageType::Report,
            MessageType::Result,
            MessageType::PluginEvent,
        ];
        for variant in variants {
            let json = serde_json::to_string(&variant).unwrap();
            let back: MessageType = serde_json::from_str(&json).unwrap();
            assert_eq!(variant, back, "serde roundtrip failed for {:?}", variant);
        }
    }
}

