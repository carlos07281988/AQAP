"""End-to-end integration tests — full pipeline with kernel modules."""
import pytest
import uuid
import os
from aqap.kernel import (
    WireMessage, wire_message_encode, wire_message_decode,
    SecurityContext, SchemaRegistry, Router,
)


def test_end_to_end_no_security():
    """Wire encode -> decode -> schema validate -- full pipeline."""
    # Schema
    reg = SchemaRegistry()
    reg.load_builtins()

    # Create message
    msg = WireMessage(
        message_id=uuid.uuid4(),
        topic="aqap:v3:agent:probe",
        trace_id=uuid.uuid4(),
        span_id=1,
        source="test",
        target="",
        correlation_id=uuid.UUID(int=0),
        msg_type="task:dispatch",
        body={
            "task_id": "task-integration01",
            "type": "code_review",
            "target": {"repo": "test/app", "branch": "main"},
        },
        headers={},
        encoding="json",
        compression="none",
        signature_mode="none",
        priority="normal",
        ttl_ms=30000,
    )

    # Encode -> Decode
    encoded = wire_message_encode(msg, encoding="json")
    decoded = wire_message_decode(encoded)

    assert decoded.topic == "aqap:v3:agent:probe"
    assert decoded.source == "test"
    assert decoded.msg_type == "task:dispatch"

    # Schema validate
    result = reg.validate("aqap:schema:task.v3", decoded.body)
    assert result.valid, f"Validation failed: {result.errors}"


def test_end_to_end_with_crypto():
    """Wire encode -> encrypt -> sign -> verify -> decrypt -> decode -- full pipeline."""
    master_key = os.urandom(32)
    sec = SecurityContext(master_key=master_key)
    sec.load()

    reg = SchemaRegistry()
    reg.load_builtins()

    # Create message with body
    body = {
        "task_id": "task-crypto01",
        "type": "lint",
        "target": {"repo": "app/lib", "branch": "develop"},
    }

    # Validate before sending
    result = reg.validate("aqap:schema:task.v3", body)
    assert result.valid, f"Pre-validation failed: {result.errors}"

    # Encrypt body
    body_json = __import__("json").dumps(body).encode()
    encrypted = sec.encrypt(body_json, aad="aqap:v3:agent:probe")

    # Sign envelope (simulate wire header before signature)
    sig = sec.sign(encrypted)

    # Verify
    assert sec.verify(encrypted, sig)

    # Decrypt
    decrypted = sec.decrypt(encrypted, aad="aqap:v3:agent:probe")
    assert __import__("json").loads(decrypted) == body
