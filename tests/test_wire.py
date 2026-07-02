# tests/test_wire.py
import pytest
from aqap.kernel import (
    WireHeader,
    wire_header_encode,
    wire_header_decode,
    WireMessage,
    wire_message_encode,
    wire_message_decode,
)


class TestWireHeader:
    def test_round_trip_basic(self):
        """Header encode -> decode should be identity."""
        import uuid
        msg_id = uuid.uuid4()
        header = WireHeader(
            message_id=msg_id,
            topic_len=24,
            header_count=0,
            total_len=128,
            timestamp_ms=1719820800000,
            ttl_ms=30000,
            max_body_size=10485760,
            priority="normal",
            flags=0b00_00_01_00,  # JSON, no compression, HMAC-SHA256
            key_id=0,
        )
        encoded = wire_header_encode(header)
        assert len(encoded) == 64
        decoded = wire_header_decode(encoded)
        assert uuid.UUID(decoded.message_id) == msg_id
        assert decoded.topic_len == 24
        assert decoded.total_len == 128
        assert decoded.timestamp_ms == 1719820800000
        assert decoded.ttl_ms == 30000
        assert decoded.priority == "normal"
        assert decoded.flags == 0b00_00_01_00

    def test_magic_bytes(self):
        """First 4 bytes must be AQAP magic."""
        import uuid
        header = WireHeader(
            message_id=uuid.uuid4(),
            topic_len=0,
            header_count=0,
            total_len=64,
            timestamp_ms=0,
            ttl_ms=0,
            max_body_size=0,
            priority="normal",
            flags=0,
            key_id=0,
        )
        encoded = wire_header_encode(header)
        assert encoded[:4] == b"AQAP"

    def test_decode_invalid_magic(self):
        """Decoding non-AQAP bytes must fail."""
        bad = b"\x00" * 64
        with pytest.raises(ValueError, match="(?i)magic"):
            wire_header_decode(bad)

    def test_version_fields(self):
        """Version major/minor/patch at correct offsets."""
        import uuid
        header = WireHeader(
            message_id=uuid.uuid4(),
            topic_len=0, header_count=0, total_len=64,
            timestamp_ms=0, ttl_ms=0, max_body_size=0,
            priority="normal", flags=0, key_id=0,
        )
        encoded = wire_header_encode(header)
        # offset 4: version_major (u16 BE)
        assert int.from_bytes(encoded[4:6], "big") == 3
        # offset 6: version_minor (u16 BE)
        assert int.from_bytes(encoded[6:8], "big") == 0
        # offset 8: version_patch (u16 BE)
        assert int.from_bytes(encoded[8:10], "big") == 0


class TestWireMessage:
    def test_message_round_trip(self):
        """Full message encode -> decode should be identity."""
        import uuid
        msg = WireMessage(
            message_id=uuid.uuid4(),
            topic="aqap:v3:agent:probe",
            trace_id=uuid.uuid4(),
            span_id=42,
            source="scheduler-1",
            target="",
            correlation_id=uuid.UUID(int=0),
            msg_type="task:dispatch",
            body={"task_id": "task-abc12345", "type": "code_review"},
            headers={"x-priority": "high"},
            encoding="json",
            compression="none",
            signature_mode="none",
            priority="normal",
            ttl_ms=30000,
        )
        encoded = wire_message_encode(msg, encoding="json")
        assert len(encoded) > 64  # at least header
        decoded = wire_message_decode(encoded)
        assert decoded.topic == "aqap:v3:agent:probe"
        assert decoded.source == "scheduler-1"
        assert decoded.target == ""
        assert decoded.msg_type == "task:dispatch"
        assert decoded.body == {"task_id": "task-abc12345", "type": "code_review"}
        assert decoded.headers == {"x-priority": "high"}

    def test_message_with_signature(self):
        """HMAC-signed message must survive round-trip."""
        import uuid
        msg = WireMessage(
            message_id=uuid.uuid4(),
            topic="aqap:v3:system:heartbeat",
            trace_id=uuid.uuid4(),
            span_id=1,
            source="probe-1",
            target="",
            correlation_id=uuid.UUID(int=0),
            msg_type="system:heartbeat",
            body={"alive": True},
            headers={},
            encoding="json",
            compression="none",
            signature_mode="hmac-sha256",
            priority="normal",
            ttl_ms=10000,
            signature=b"\x00" * 32,  # placeholder
        )
        encoded = wire_message_encode(msg, encoding="json")
        decoded = wire_message_decode(encoded)
        assert decoded.signature_mode == "hmac-sha256"
        assert decoded.body == {"alive": True}

    def test_compression_zstd(self):
        """Zstd-compressed message should decompress transparently."""
        import uuid
        msg = WireMessage(
            message_id=uuid.uuid4(),
            topic="aqap:v3:agent:probe",
            trace_id=uuid.uuid4(),
            span_id=0,
            source="test", target="",
            correlation_id=uuid.UUID(int=0),
            msg_type="task:dispatch",
            body={"data": "x" * 1000},
            headers={},
            encoding="json",
            compression="zstd",
            signature_mode="none",
            priority="normal",
            ttl_ms=0,
        )
        encoded = wire_message_encode(msg, encoding="json")
        decoded = wire_message_decode(encoded)
        assert decoded.body == {"data": "x" * 1000}
        # Compressed should be smaller than uncompressed
        assert len(encoded) < 64 + 1000 + 20
