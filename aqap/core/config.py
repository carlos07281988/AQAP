"""AQAP 配置加载器 — YAML + 环境变量合并"""
from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Any

logger = logging.getLogger("aqap.core.config")


class AQAPConfig:
    """AQAP 系统配置 — 支持 YAML 文件 + 环境变量覆盖"""

    DEFAULTS = {
        "app": {
            "name": "Agent Quality Assurance",
            "version": "1.0.0",
            "debug": False,
        },
        # ── 日志 ──
        "logging": {
            "level": "DEBUG",
            "format": "detailed",
            "loggers": {},
        },
        # ── 后端 ──
        "transport": {
            "backend": "redis-streams",
            "redis_url": "redis://127.0.0.1:6379",
            "kafka_servers": "127.0.0.1:9092",
        },
        # ── 安全 (可选) ──
        "security": {
            "enabled": False,
            "secret": "",
        },
        # ── Agent ──
        "agents": {},
        # ── 插件 ──
        "plugins": {},
        # ── Supervisor ──
        "supervisor": {
            "heartbeat_timeout": 90,
        },
    }

    def __init__(self, config_path: Path, env_prefix: str = "AQAP_"):
        self._data: dict[str, Any] = {}
        self._config_path = config_path
        self._env_prefix = env_prefix
        self._load()

    def _load(self) -> None:
        """加载 YAML → 环境变量覆盖 → 合并默认值"""
        import yaml

        self._data = dict(self.DEFAULTS)

        if self._config_path.exists():
            user_cfg = yaml.safe_load(self._config_path.read_text(encoding="utf-8"))
            if user_cfg:
                self._merge(self._data, user_cfg)

        self._env_override()

    def _merge(self, base: dict, overlay: dict) -> None:
        """递归合并 dict, overlay 优先"""
        for key, value in overlay.items():
            if key in base and isinstance(base[key], dict) and isinstance(value, dict):
                self._merge(base[key], value)
            else:
                base[key] = value

    def _env_override(self) -> None:
        """环境变量覆盖 (例: AQAP_DEBUG=true, AQAP_TRANSPORT_BACKEND=kafka)"""
        prefix = self._env_prefix
        for key, val in os.environ.items():
            if key.startswith(prefix):
                parts = key[len(prefix) :].lower().split("_")
                target = self._data
                for part in parts[:-1]:
                    if part not in target:
                        target[part] = {}
                    target = target[part]
                # 类型转换
                key_part = parts[-1]
                if val.lower() in ("true", "false"):
                    target[key_part] = val.lower() == "true"
                elif val.isdigit():
                    target[key_part] = int(val)
                else:
                    target[key_part] = val

    def get(self, *keys: str, default: Any = None) -> Any:
        """安全取值, 支持嵌套 key 链"""
        target = self._data
        for k in keys:
            if isinstance(target, dict):
                target = target.get(k)
            else:
                return default
        return target if target is not None else default

    def __getitem__(self, key: str) -> Any:
        return self._data[key]

    def __repr__(self) -> str:
        return f"<AQAPConfig path={self._config_path}>"
