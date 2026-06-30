"""
AQAP 安全层 — payload 端到端加密

可选特性: 当 config.yaml 中 security.enabled=true 时,
Agent 在 publish 前加密 payload, subscribe 后解密 payload。

加密方式: AES-256-GCM (认证加密，防篡改)，符合 PROTOCOL.md §7.2。
"""
from __future__ import annotations

import base64
import json
import logging
import os
from typing import Any

logger = logging.getLogger("aqap.core.security")

CRYPTO_AVAILABLE = False
HKDF_AVAILABLE = False
try:
    from cryptography.hazmat.primitives.ciphers.aead import AESGCM
    from cryptography.hazmat.primitives.kdf.hkdf import HKDF
    from cryptography.hazmat.primitives import hashes

    HKDF_AVAILABLE = True
    CRYPTO_AVAILABLE = True
except ImportError:
    pass


def _derive_key_bytes(secret: str) -> bytes:
    """从 secret 派生 32-byte AES-256 密钥 (HKDF with SHA-256)

    生产环境使用 HKDF (基于 HMAC 的密钥派生函数)，
    比原始 SHA-256 更安全，防止长度扩展攻击。
    回退: SHA-256 (仅当 cryptography 版本过旧时)。
    """
    raw = secret.encode("utf-8")
    if HKDF_AVAILABLE:
        hkdf = HKDF(
            algorithm=hashes.SHA256(), length=32, salt=None, info=b"aqap-payload-key"
        )
        return hkdf.derive(raw)
    # 回退: SHA-256 (向后兼容)
    import hashlib
    return hashlib.sha256(raw).digest()


class PayloadCipher:
    """payload 加解密器

    使用 AES-256-GCM 认证加密。
    密钥从配置中的 security.secret 派生。
    加密后格式: {"_encrypted": true, "_ciphertext": "<base64>", "_nonce": "<base64>"}
    符合 PROTOCOL.md §7.2。
    """

    def __init__(self, secret: str | None = None):
        self._enabled = False
        self._key: bytes | None = None
        if secret and CRYPTO_AVAILABLE:
            self._key = _derive_key_bytes(secret)
            self._enabled = True
        elif secret and not CRYPTO_AVAILABLE:
            logger.warning("cryptography 未安装, 无法启用加密")

    @property
    def enabled(self) -> bool:
        return self._enabled

    def encrypt_payload(self, payload: dict) -> dict:
        """加密 payload — AES-256-GCM

        返回: {"_encrypted": True, "_ciphertext": "base64...", "_nonce": "base64..."}
        """
        if not self._enabled:
            return payload
        plaintext = json.dumps(payload, ensure_ascii=False).encode()
        nonce = os.urandom(12)  # GCM 推荐 12 字节
        aesgcm = AESGCM(self._key)
        ciphertext = aesgcm.encrypt(nonce, plaintext, None)
        return {
            "_encrypted": True,
            "_ciphertext": base64.b64encode(ciphertext).decode(),
            "_nonce": base64.b64encode(nonce).decode(),
        }

    def decrypt_payload(self, payload: dict) -> dict:
        """解密 payload — AES-256-GCM"""
        if not self._enabled:
            return payload
        if not payload.get("_encrypted"):
            return payload
        ciphertext = base64.b64decode(payload["_ciphertext"])
        nonce = base64.b64decode(payload["_nonce"])
        aesgcm = AESGCM(self._key)
        plaintext = aesgcm.decrypt(nonce, ciphertext, None)
        return json.loads(plaintext.decode())


def generate_secret() -> str:
    """生成随机密钥 (用于初始化配置)"""
    return base64.urlsafe_b64encode(os.urandom(32)).decode()
