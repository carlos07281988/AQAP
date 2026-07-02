# tests/test_wire.py
import pytest
from aqap.kernel import wire_header_encode, wire_header_decode, WireHeader


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
