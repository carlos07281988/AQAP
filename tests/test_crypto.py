# tests/test_crypto.py
import os

import pytest

from aqap.kernel import (
    SecurityContext,
    decrypt_payload,
    encrypt_payload,
    hkdf_derive,
    sign_envelope,
    verify_envelope,
)


class TestHKDF:
    def test_derive_deterministic(self):
        """Same inputs -> same derived key."""
        master = os.urandom(32)
        k1 = hkdf_derive(master, b"aqap-v3-payload", b"encrypt", 32)
        k2 = hkdf_derive(master, b"aqap-v3-payload", b"encrypt", 32)
        assert k1 == k2

    def test_derive_different_salt(self):
        """Different salt -> different key."""
        master = os.urandom(32)
        k1 = hkdf_derive(master, b"salt-a", b"info", 32)
        k2 = hkdf_derive(master, b"salt-b", b"info", 32)
        assert k1 != k2

    def test_derive_different_info(self):
        """Different info -> different key."""
        master = os.urandom(32)
        k1 = hkdf_derive(master, b"salt", b"encrypt", 32)
        k2 = hkdf_derive(master, b"salt", b"sign", 32)
        assert k1 != k2


class TestEncryptDecrypt:
    def test_round_trip(self):
        """encrypt -> decrypt should return original."""
        key = os.urandom(32)
        plaintext = b"hello world"
        aad = b"aqap:v3:agent:probe"
        ciphertext = encrypt_payload(key, plaintext, aad)
        assert len(ciphertext) >= 12  # nonce present
        decrypted = decrypt_payload(key, ciphertext, aad)
        assert decrypted == plaintext

    def test_tampered_ciphertext_fails(self):
        """Modified ciphertext must fail decryption."""
        key = os.urandom(32)
        ciphertext = encrypt_payload(key, b"secret", b"aad")
        # Flip a byte in the ciphertext (after nonce)
        tampered = bytearray(ciphertext)
        tampered[15] ^= 0x01
        with pytest.raises(ValueError, match="decrypt"):
            decrypt_payload(key, bytes(tampered), b"aad")

    def test_wrong_key_fails(self):
        """Wrong key must fail decryption."""
        k1 = os.urandom(32)
        k2 = os.urandom(32)
        ciphertext = encrypt_payload(k1, b"secret", b"aad")
        with pytest.raises(ValueError, match="decrypt"):
            decrypt_payload(k2, ciphertext, b"aad")

    def test_wrong_aad_fails(self):
        """Wrong AAD must fail decryption."""
        key = os.urandom(32)
        ciphertext = encrypt_payload(key, b"secret", b"aad-correct")
        with pytest.raises(ValueError, match="decrypt"):
            decrypt_payload(key, ciphertext, b"aad-wrong")


class TestSignVerify:
    def test_round_trip(self):
        """sign -> verify should return True."""
        key = os.urandom(32)
        data = b"hello world"
        sig = sign_envelope(key, data)
        assert len(sig) == 32  # HMAC-SHA256
        assert verify_envelope(key, data, sig)

    def test_wrong_key_fails(self):
        """Wrong key must fail verification."""
        k1 = os.urandom(32)
        k2 = os.urandom(32)
        sig = sign_envelope(k1, b"data")
        assert not verify_envelope(k2, b"data", sig)

    def test_tampered_data_fails(self):
        """Modified data must fail verification."""
        key = os.urandom(32)
        sig = sign_envelope(key, b"original")
        assert not verify_envelope(key, b"modified", sig)

    def test_tampered_signature_fails(self):
        """Modified signature must fail verification."""
        key = os.urandom(32)
        sig = bytearray(sign_envelope(key, b"data"))
        sig[0] ^= 0x01
        assert not verify_envelope(key, b"data", bytes(sig))


class TestSecurityContext:
    def test_full_workflow(self):
        """SecurityContext: load -> derive keys -> encrypt -> decrypt."""
        master = os.urandom(32)
        ctx = SecurityContext(master_key=master)
        ctx.load()

        # Encrypt
        plaintext = b'{"task_id": "task-abc12345"}'
        topic = "aqap:v3:agent:probe"
        ciphertext = ctx.encrypt(plaintext, aad=topic)
        assert ciphertext != plaintext

        # Decrypt
        decrypted = ctx.decrypt(ciphertext, aad=topic)
        assert decrypted == plaintext

        # Sign
        sig = ctx.sign(plaintext)
        assert len(sig) == 32
        assert ctx.verify(plaintext, sig)

    def test_different_contexts_different_keys(self):
        """Different master keys -> different derived keys."""
        ctx1 = SecurityContext(master_key=os.urandom(32))
        ctx1.load()
        ctx2 = SecurityContext(master_key=os.urandom(32))
        ctx2.load()

        ciphertext = ctx1.encrypt(b"data", aad="test")
        with pytest.raises(ValueError):
            ctx2.decrypt(ciphertext, aad="test")

    def test_is_loaded(self):
        """is_loaded getter reflects load state."""
        ctx = SecurityContext(master_key=os.urandom(32))
        assert not ctx.is_loaded
        ctx.load()
        assert ctx.is_loaded

    def test_sign_route_verify(self):
        """sign_route -> verify_route round-trip."""
        ctx = SecurityContext(master_key=os.urandom(32))
        ctx.load()
        topic = "aqap:v3:agent:probe"
        sig = ctx.sign_route(topic)
        assert len(sig) == 32
        assert ctx.verify_route(topic, sig)

    def test_sign_route_tampered_topic_fails(self):
        """verify_route must fail for a different topic."""
        ctx = SecurityContext(master_key=os.urandom(32))
        ctx.load()
        sig = ctx.sign_route("aqap:v3:agent:probe")
        assert not ctx.verify_route("aqap:v3:agent:planner", sig)
