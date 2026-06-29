"""
AQAP 消息验证 — core 与 SDK 共享

提取 validate_message 到此单一模块，
core/message.py 和 sdk/aqap_sdk/message.py 均从此处导入。
"""
from __future__ import annotations

from typing import Any

# ── 已知消息类型 (同步于 PROTOCOL.md §2) ──

KNOWN_MESSAGE_TYPES = {
    "UNKNOWN",
    "HEARTBEAT", "REGISTER", "SHUTDOWN",
    "TASK_DISPATCH", "TASK_RESULT",
    "JUDGE_REQUEST", "JUDGE_VERDICT",
    "REPORT_REQUEST", "REPORT_DELIVER",
    "ERROR", "LOG", "PLUGIN_EVENT",
}


def validate_message(data: dict[str, Any]) -> list[str]:
    """
    验证消息信封是否符合 PROTOCOL.md §1 规范。

    返回错误列表，空列表表示消息合法。
    core 和 SDK 共享此唯一实现。
    """
    errors: list[str] = []

    # 必填字段
    required = ["type", "source", "payload", "version"]
    for field in required:
        if field not in data:
            errors.append(f"缺少必填字段: {field}")

    # source 不能为空
    source = data.get("source", "")
    if not isinstance(source, str) or not source.strip():
        errors.append("source 不能为空")

    # type 检查
    raw_type = data.get("type")
    if isinstance(raw_type, str):
        if raw_type not in KNOWN_MESSAGE_TYPES and raw_type.upper() not in KNOWN_MESSAGE_TYPES:
            errors.append(f"未知消息类型: {raw_type}")
    elif raw_type is not None:
        errors.append(f"type 必须是字符串, 收到 {type(raw_type).__name__}")

    # version 检查
    version = data.get("version", "")
    if version:
        major = version.split(".")[0] if "." in version else version
        if not major.isdigit() or int(major) != 1:
            errors.append(f"协议版本不匹配: 期望 1.x, 收到 {version}")

    # target / correlation_id 类型
    for field in ("target", "correlation_id"):
        val = data.get(field)
        if val is not None and not isinstance(val, str):
            errors.append(f"{field} 必须是字符串, 收到 {type(val).__name__}")

    # payload 类型
    payload = data.get("payload")
    if payload is not None and not isinstance(payload, dict):
        errors.append("payload 必须是 JSON Object")

    return errors
