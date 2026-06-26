"""AQAP 日志配置 — 统一日志初始化

用法:
    from aqap.core.log_config import setup_logging
    setup_logging()

所有模块使用标准 logging.getLogger() 获取 logger:
    import logging
    logger = logging.getLogger("aqap.agent.base")
"""
from __future__ import annotations

import logging
import logging.config
import sys
from typing import Any

# 默认日志配置 (与 config.yaml 的 logging 段合并)
DEFAULT_LOGGING_CONFIG: dict[str, Any] = {
    "version": 1,
    "disable_existing_loggers": False,
    "formatters": {
        "standard": {
            "format": "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
        "detailed": {
            "format": (
                "%(asctime)s.%(msecs)03d [%(levelname)-5s] "
                "%(name)s (%(filename)s:%(lineno)d): %(message)s"
            ),
            "datefmt": "%Y-%m-%d %H:%M:%S",
        },
    },
    "handlers": {
        "console": {
            "class": "logging.StreamHandler",
            "level": "DEBUG",
            "formatter": "detailed",
            "stream": "ext://sys.stderr",
        },
    },
    "root": {
        "level": "INFO",
        "handlers": ["console"],
    },
    "loggers": {
        "aqap": {
            "level": "DEBUG",
            "handlers": ["console"],
            "propagate": False,
        },
    },
}


def setup_logging(debug: bool = False, config: dict | None = None) -> None:
    """初始化日志配置

    Args:
        debug: 是否开启详细日志 (如果为 True, aqa 日志级别设为 DEBUG)
        config: 外部配置覆盖 (来自 config.yaml 的 logging 段)
    """
    cfg = DEFAULT_LOGGING_CONFIG.copy()

    # 外部配置合并
    if config:
        _deep_merge(cfg, config)

    # debug 模式: 降低 aqa 日志级别到 DEBUG
    if debug:
        if "loggers" in cfg and "aqap" in cfg["loggers"]:
            cfg["loggers"]["aqap"]["level"] = "DEBUG"
        if "root" in cfg:
            cfg["root"]["level"] = "DEBUG"

    logging.config.dictConfig(cfg)
    logging.getLogger("aqap").debug("日志系统已初始化 (debug=%s)", debug)


def _deep_merge(base: dict, override: dict) -> None:
    """递归合并字典 (override 的值覆盖 base)"""
    for key, value in override.items():
        if key in base and isinstance(base[key], dict) and isinstance(value, dict):
            _deep_merge(base[key], value)
        else:
            base[key] = value


def get_logger(name: str) -> logging.Logger:
    """获取日志器 (快捷方式)"""
    return logging.getLogger(name)
