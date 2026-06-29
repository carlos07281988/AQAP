"""TraceCollector — 记录消息处理耗时与链路

记录每条消息的处理耗时、来源、类型，用于可观测性。
支持内存索引查询 + JSON 文件导出。
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import datetime, timezone
from typing import Any

from aqap.plugin.base import Plugin

logger = logging.getLogger("aqap.trace")


class TraceCollector(Plugin):
    """链路追踪收集器

    记录每条消息的处理耗时、来源、类型，用于可观测性。
    支持 query_trace() / query_recent() / 文件导出。
    """

    @property
    def name(self) -> str:
        return "trace-collector"

    @property
    def version(self) -> str:
        return "1.1.0"

    async def initialize(self, config: dict) -> None:
        self._report_interval = config.get("report_interval", 60)
        self._buffer: list[dict] = []
        self._trace_index: dict[str, list[dict]] = {}
        self._max_events_per_trace = config.get("max_events_per_trace", 1000)
        self._max_traces = config.get("max_traces", 10000)
        self._export_dir = config.get("export_dir", "")
        self._export_enabled = bool(config.get("export_enabled", False))

    async def execute(self, context: dict) -> dict:
        context_start = context.get("_aqap_start_time", None)
        if not context_start:
            return {}

        elapsed = time.time() - context_start
        trace_id = context.get("_aqap_trace_id", "unknown")
        msg_type = context.get("_aqap_message_type", "unknown")
        source = context.get("_aqap_source", "unknown")

        record = {
            "trace_id": trace_id,
            "type": msg_type,
            "source": source,
            "elapsed_ms": round(elapsed * 1000, 2),
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

        self._buffer.append(record)

        # 索引
        if trace_id not in self._trace_index:
            self._trace_index[trace_id] = []
        events = self._trace_index[trace_id]
        if len(events) < self._max_events_per_trace:
            events.append(record)

        # 限制索引大小
        if len(self._trace_index) > self._max_traces:
            keys = sorted(self._trace_index.keys())
            for k in keys[:len(keys) // 4]:
                del self._trace_index[k]

        logger.info(
            "[trace] %s | %s | %s | %.2fms",
            trace_id[:12] if len(trace_id) >= 12 else trace_id,
            msg_type,
            source,
            elapsed * 1000,
        )

        if len(self._buffer) >= self._report_interval:
            self._flush()

        return {"traced": True, "elapsed_ms": round(elapsed * 1000, 2)}

    async def query_trace(self, trace_id: str) -> list[dict]:
        """按 trace_id 查询链路事件"""
        return self._trace_index.get(trace_id, [])

    async def query_recent(self, limit: int = 100) -> list[dict]:
        """查询最近追踪事件"""
        return list(reversed(self._buffer[-limit:]))

    async def stats(self) -> dict:
        """追踪统计"""
        return {
            "buffer_size": len(self._buffer),
            "traces_indexed": len(self._trace_index),
            "export_enabled": self._export_enabled,
            "export_dir": self._export_dir,
        }

    def _flush(self):
        """批量导出到文件"""
        if not self._buffer:
            return

        if self._export_enabled and self._export_dir:
            self._export_to_file()

        logger.debug("[trace] flush %d records", len(self._buffer))
        self._buffer.clear()

    def _export_to_file(self):
        """导出为 JSON Lines 文件"""
        if not self._export_dir:
            return
        try:
            os.makedirs(self._export_dir, exist_ok=True)
            date_str = datetime.now(timezone.utc).strftime("%Y%m%d-%H%M%S")
            filename = os.path.join(self._export_dir, f"trace-{date_str}.jsonl")
            with open(filename, "a") as f:
                for record in self._buffer:
                    f.write(json.dumps(record, ensure_ascii=False) + "\n")
            logger.debug("[trace] 已导出 %d 条到 %s", len(self._buffer), filename)
        except OSError as e:
            logger.warning("[trace] 导出失败: %s", e)

    async def cleanup(self) -> None:
        self._flush()
