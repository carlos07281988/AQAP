// aqap-kernel/src/error.rs
use std::fmt;
use thiserror::Error;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[repr(u16)]
pub enum ErrorCode {
    // ── Protocol (0x00xx) ──
    UnknownMessageType  = 0x0001,
    VersionMismatch     = 0x0002,
    MalformedWire       = 0x0003,
    MagicMismatch       = 0x0004,
    ChecksumMismatch    = 0x0005,
    PayloadTooLarge     = 0x0006,
    TtlExpired          = 0x0007,
    InvalidPriority     = 0x0008,
    ReservedFlags       = 0x0009,

    // ── Routing (0x01xx) ──
    TopicNotFound       = 0x0101,
    HandlerNotFound     = 0x0102,
    TargetUnreachable   = 0x0103,
    RoutingLoop         = 0x0104,
    ConsumerGroupFull   = 0x0105,

    // ── Schema (0x02xx) ──
    SchemaNotFound       = 0x0201,
    SchemaVersionMissing = 0x0202,
    ValidationFailed     = 0x0203,
    TypeMismatch         = 0x0204,
    RequiredFieldMissing = 0x0205,
    PatternMismatch      = 0x0206,
    EnumValueInvalid     = 0x0207,
    MinimumViolation     = 0x0208,
    MaximumViolation     = 0x0209,

    // ── Security (0x03xx) ──
    AuthFailed          = 0x0301,
    Forbidden           = 0x0302,
    SignatureInvalid    = 0x0303,
    DecryptFailed       = 0x0304,
    KeyExpired          = 0x0305,
    KeyNotFound         = 0x0306,

    // ── Runtime (0x04xx) ──
    HandlerPanic        = 0x0401,
    Timeout             = 0x0402,
    RateLimitExceeded   = 0x0403,
    CircuitBreakerOpen  = 0x0404,
    OutOfMemory         = 0x0405,
    TransportError      = 0x0406,
    ShutdownInProgress  = 0x0407,

    // ── DLQ (0x05xx) ──
    MaxRetriesExceeded  = 0x0501,
    DlqFull             = 0x0502,
    ReplayFailed        = 0x0503,
    DlqMessageExpired   = 0x0504,
}

impl ErrorCode {
    pub fn from_u16(v: u16) -> Self {
        match v {
            0x0001 => Self::UnknownMessageType,
            0x0002 => Self::VersionMismatch,
            0x0003 => Self::MalformedWire,
            0x0004 => Self::MagicMismatch,
            0x0005 => Self::ChecksumMismatch,
            0x0006 => Self::PayloadTooLarge,
            0x0007 => Self::TtlExpired,
            0x0008 => Self::InvalidPriority,
            0x0009 => Self::ReservedFlags,
            0x0101 => Self::TopicNotFound,
            0x0102 => Self::HandlerNotFound,
            0x0103 => Self::TargetUnreachable,
            0x0104 => Self::RoutingLoop,
            0x0105 => Self::ConsumerGroupFull,
            0x0201 => Self::SchemaNotFound,
            0x0202 => Self::SchemaVersionMissing,
            0x0203 => Self::ValidationFailed,
            0x0204 => Self::TypeMismatch,
            0x0205 => Self::RequiredFieldMissing,
            0x0206 => Self::PatternMismatch,
            0x0207 => Self::EnumValueInvalid,
            0x0208 => Self::MinimumViolation,
            0x0209 => Self::MaximumViolation,
            0x0301 => Self::AuthFailed,
            0x0302 => Self::Forbidden,
            0x0303 => Self::SignatureInvalid,
            0x0304 => Self::DecryptFailed,
            0x0305 => Self::KeyExpired,
            0x0306 => Self::KeyNotFound,
            0x0401 => Self::HandlerPanic,
            0x0402 => Self::Timeout,
            0x0403 => Self::RateLimitExceeded,
            0x0404 => Self::CircuitBreakerOpen,
            0x0405 => Self::OutOfMemory,
            0x0406 => Self::TransportError,
            0x0407 => Self::ShutdownInProgress,
            0x0501 => Self::MaxRetriesExceeded,
            0x0502 => Self::DlqFull,
            0x0503 => Self::ReplayFailed,
            0x0504 => Self::DlqMessageExpired,
            _ => Self::UnknownMessageType, // fallback
        }
    }

    pub fn to_u16(self) -> u16 { self as u16 }

    /// Human-readable string for error messages
    pub fn to_str(self) -> &'static str {
        match self {
            Self::UnknownMessageType  => "UNKNOWN_MESSAGE_TYPE",
            Self::VersionMismatch     => "VERSION_MISMATCH",
            Self::MalformedWire       => "MALFORMED_WIRE",
            Self::MagicMismatch       => "MAGIC_MISMATCH",
            Self::ChecksumMismatch    => "CHECKSUM_MISMATCH",
            Self::PayloadTooLarge     => "PAYLOAD_TOO_LARGE",
            Self::TtlExpired          => "TTL_EXPIRED",
            Self::InvalidPriority     => "INVALID_PRIORITY",
            Self::ReservedFlags       => "RESERVED_FLAGS",
            Self::TopicNotFound       => "TOPIC_NOT_FOUND",
            Self::HandlerNotFound     => "HANDLER_NOT_FOUND",
            Self::TargetUnreachable   => "TARGET_UNREACHABLE",
            Self::RoutingLoop         => "ROUTING_LOOP",
            Self::ConsumerGroupFull   => "CONSUMER_GROUP_FULL",
            Self::SchemaNotFound       => "SCHEMA_NOT_FOUND",
            Self::SchemaVersionMissing => "SCHEMA_VERSION_MISSING",
            Self::ValidationFailed     => "VALIDATION_FAILED",
            Self::TypeMismatch         => "TYPE_MISMATCH",
            Self::RequiredFieldMissing => "REQUIRED_FIELD_MISSING",
            Self::PatternMismatch      => "PATTERN_MISMATCH",
            Self::EnumValueInvalid     => "ENUM_VALUE_INVALID",
            Self::MinimumViolation     => "MINIMUM_VIOLATION",
            Self::MaximumViolation     => "MAXIMUM_VIOLATION",
            Self::AuthFailed          => "AUTH_FAILED",
            Self::Forbidden           => "FORBIDDEN",
            Self::SignatureInvalid    => "SIGNATURE_INVALID",
            Self::DecryptFailed       => "DECRYPT_FAILED",
            Self::KeyExpired          => "KEY_EXPIRED",
            Self::KeyNotFound         => "KEY_NOT_FOUND",
            Self::HandlerPanic        => "HANDLER_PANIC",
            Self::Timeout             => "TIMEOUT",
            Self::RateLimitExceeded   => "RATE_LIMIT_EXCEEDED",
            Self::CircuitBreakerOpen  => "CIRCUIT_BREAKER_OPEN",
            Self::OutOfMemory         => "OUT_OF_MEMORY",
            Self::TransportError      => "TRANSPORT_ERROR",
            Self::ShutdownInProgress  => "SHUTDOWN_IN_PROGRESS",
            Self::MaxRetriesExceeded  => "MAX_RETRIES_EXCEEDED",
            Self::DlqFull             => "DLQ_FULL",
            Self::ReplayFailed        => "REPLAY_FAILED",
            Self::DlqMessageExpired   => "DLQ_MESSAGE_EXPIRED",
        }
    }
}

impl fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.to_str())
    }
}

#[derive(Debug, Error)]
pub enum KernelError {
    #[error("Wire format error: {code} — {detail}")]
    Wire { code: ErrorCode, detail: String },

    #[error("Schema validation error: {errors:?}")]
    SchemaValidation { errors: Vec<String>, schema_id: String },

    #[error("Security error: {code} — {detail}")]
    Security { code: ErrorCode, detail: String },

    #[error("Routing error: {code} — {detail}")]
    Routing { code: ErrorCode, detail: String },

    #[error("Python error: {0}")]
    Python(String),
}

impl KernelError {
    pub fn error_code(&self) -> ErrorCode {
        match self {
            Self::Wire { code, .. }       => *code,
            Self::SchemaValidation { .. } => ErrorCode::ValidationFailed,
            Self::Security { code, .. }   => *code,
            Self::Routing { code, .. }    => *code,
            Self::Python(_)               => ErrorCode::HandlerPanic,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ── ErrorCode tests ──

    #[test]
    fn test_error_code_u16_roundtrip_all_codes() {
        // Test every single error code
        let codes: &[(ErrorCode, u16)] = &[
            (ErrorCode::UnknownMessageType, 0x0001),
            (ErrorCode::VersionMismatch, 0x0002),
            (ErrorCode::MalformedWire, 0x0003),
            (ErrorCode::MagicMismatch, 0x0004),
            (ErrorCode::ChecksumMismatch, 0x0005),
            (ErrorCode::PayloadTooLarge, 0x0006),
            (ErrorCode::TtlExpired, 0x0007),
            (ErrorCode::InvalidPriority, 0x0008),
            (ErrorCode::ReservedFlags, 0x0009),
            (ErrorCode::TopicNotFound, 0x0101),
            (ErrorCode::HandlerNotFound, 0x0102),
            (ErrorCode::TargetUnreachable, 0x0103),
            (ErrorCode::RoutingLoop, 0x0104),
            (ErrorCode::ConsumerGroupFull, 0x0105),
            (ErrorCode::SchemaNotFound, 0x0201),
            (ErrorCode::SchemaVersionMissing, 0x0202),
            (ErrorCode::ValidationFailed, 0x0203),
            (ErrorCode::TypeMismatch, 0x0204),
            (ErrorCode::RequiredFieldMissing, 0x0205),
            (ErrorCode::PatternMismatch, 0x0206),
            (ErrorCode::EnumValueInvalid, 0x0207),
            (ErrorCode::MinimumViolation, 0x0208),
            (ErrorCode::MaximumViolation, 0x0209),
            (ErrorCode::AuthFailed, 0x0301),
            (ErrorCode::Forbidden, 0x0302),
            (ErrorCode::SignatureInvalid, 0x0303),
            (ErrorCode::DecryptFailed, 0x0304),
            (ErrorCode::KeyExpired, 0x0305),
            (ErrorCode::KeyNotFound, 0x0306),
            (ErrorCode::HandlerPanic, 0x0401),
            (ErrorCode::Timeout, 0x0402),
            (ErrorCode::RateLimitExceeded, 0x0403),
            (ErrorCode::CircuitBreakerOpen, 0x0404),
            (ErrorCode::OutOfMemory, 0x0405),
            (ErrorCode::TransportError, 0x0406),
            (ErrorCode::ShutdownInProgress, 0x0407),
            (ErrorCode::MaxRetriesExceeded, 0x0501),
            (ErrorCode::DlqFull, 0x0502),
            (ErrorCode::ReplayFailed, 0x0503),
            (ErrorCode::DlqMessageExpired, 0x0504),
        ];
        assert_eq!(codes.len(), 40, "expected 40 error codes (36 brief claims + verified)");
        for (code, expected_val) in codes {
            assert_eq!(code.to_u16(), *expected_val, "to_u16 failed for {:?}", code);
            assert_eq!(ErrorCode::from_u16(*expected_val), *code, "from_u16 failed for 0x{:04x}", expected_val);
        }
    }

    #[test]
    fn test_error_code_from_u16_unknown_fallback() {
        assert_eq!(ErrorCode::from_u16(0x0000), ErrorCode::UnknownMessageType);
        assert_eq!(ErrorCode::from_u16(0x9999), ErrorCode::UnknownMessageType);
        assert_eq!(ErrorCode::from_u16(0xFFFF), ErrorCode::UnknownMessageType);
    }

    #[test]
    fn test_error_code_to_str_is_screaming_snake() {
        for code in [
            ErrorCode::UnknownMessageType,
            ErrorCode::ValidationFailed,
            ErrorCode::AuthFailed,
            ErrorCode::MaxRetriesExceeded,
            ErrorCode::DlqFull,
        ] {
            let s = code.to_str();
            assert!(!s.contains(' '), "to_str contains spaces: {}", s);
            assert!(s.chars().all(|c| c.is_uppercase() || c == '_'), "not SCREAMING_SNAKE: {}", s);
        }
    }

    #[test]
    fn test_error_code_to_str_coverage_all() {
        // Spot-check that to_str returns different strings for different codes
        assert_eq!(ErrorCode::UnknownMessageType.to_str(), "UNKNOWN_MESSAGE_TYPE");
        assert_eq!(ErrorCode::VersionMismatch.to_str(), "VERSION_MISMATCH");
        assert_eq!(ErrorCode::ValidationFailed.to_str(), "VALIDATION_FAILED");
        assert_eq!(ErrorCode::AuthFailed.to_str(), "AUTH_FAILED");
        assert_eq!(ErrorCode::HandlerPanic.to_str(), "HANDLER_PANIC");
        assert_eq!(ErrorCode::MaxRetriesExceeded.to_str(), "MAX_RETRIES_EXCEEDED");
        assert_eq!(ErrorCode::DlqFull.to_str(), "DLQ_FULL");
        assert_eq!(ErrorCode::DlqMessageExpired.to_str(), "DLQ_MESSAGE_EXPIRED");
    }

    #[test]
    fn test_error_code_display() {
        assert_eq!(format!("{}", ErrorCode::Timeout), "TIMEOUT");
        assert_eq!(format!("{}", ErrorCode::CircuitBreakerOpen), "CIRCUIT_BREAKER_OPEN");
    }

    // ── KernelError tests ──

    #[test]
    fn test_kernel_error_wire_code() {
        let err = KernelError::Wire {
            code: ErrorCode::MagicMismatch,
            detail: "bad magic".into(),
        };
        assert_eq!(err.error_code(), ErrorCode::MagicMismatch);
        let msg = format!("{}", err);
        assert!(msg.contains("MagicMismatch") || msg.contains("MAGIC_MISMATCH"));
        assert!(msg.contains("bad magic"));
    }

    #[test]
    fn test_kernel_error_schema_validation_code() {
        let err = KernelError::SchemaValidation {
            errors: vec!["field x required".into()],
            schema_id: "task_dispatch_v1".into(),
        };
        assert_eq!(err.error_code(), ErrorCode::ValidationFailed);
        let msg = format!("{}", err);
        assert!(msg.contains("field x required"));
        // schema_id is stored but not in Display format (this is per the design)
    }

    #[test]
    fn test_kernel_error_security_code() {
        let err = KernelError::Security {
            code: ErrorCode::SignatureInvalid,
            detail: "signature mismatch".into(),
        };
        assert_eq!(err.error_code(), ErrorCode::SignatureInvalid);
    }

    #[test]
    fn test_kernel_error_routing_code() {
        let err = KernelError::Routing {
            code: ErrorCode::TopicNotFound,
            detail: "topic foo/bar".into(),
        };
        assert_eq!(err.error_code(), ErrorCode::TopicNotFound);
    }

    #[test]
    fn test_kernel_error_python_code() {
        let err = KernelError::Python("something broke".into());
        assert_eq!(err.error_code(), ErrorCode::HandlerPanic);
        let msg = format!("{}", err);
        assert!(msg.contains("something broke"));
    }

    // ── Category coverage ──

    #[test]
    fn test_error_code_categories() {
        // Verify 6 categories exist with at least one code each
        // Protocol 0x00xx
        assert_eq!(ErrorCode::from_u16(0x0001), ErrorCode::UnknownMessageType);
        // Routing 0x01xx
        assert_eq!(ErrorCode::from_u16(0x0101), ErrorCode::TopicNotFound);
        // Schema 0x02xx
        assert_eq!(ErrorCode::from_u16(0x0201), ErrorCode::SchemaNotFound);
        // Security 0x03xx
        assert_eq!(ErrorCode::from_u16(0x0301), ErrorCode::AuthFailed);
        // Runtime 0x04xx
        assert_eq!(ErrorCode::from_u16(0x0401), ErrorCode::HandlerPanic);
        // DLQ 0x05xx
        assert_eq!(ErrorCode::from_u16(0x0501), ErrorCode::MaxRetriesExceeded);
    }
}
