"""DLQ Consumer Agent — 死信队列消费与处理

消费 aqap:dlq topic, 对死信消息执行:
  - 日志记录 (结构化)
  - 统计计数
  - 支持人工重放 (将原始消息重新发布到原始 topic)
  - 定期清理过期死信
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from aqap.core.message import (
    Message,
    MessageType,
    Topic,
    task_dispatch,
)
from aqap.agent.base import Agent

logger = logging.getLogger("aqap.agent.dlq_consumer")

# 死信默认保留时间 (秒)
DEFAULT_DLQ_TTL = 86400 * 7  # 7 天


class DLQConsumerAgent(Agent):
    """死信队列消费者

    职责:
    1. 消费 aqap:dlq topic
    2. 记录死信到结构化日志
    3. 维护死信索引供查询和重放
    4. 支持手动重放死信消息
    5. 定期清理过期死信
    """

    def __init__(
        self,
        agent_id: str = "dlq-consumer",
        transport=None,
        dlq_ttl: int = DEFAULT_DLQ_TTL,
        **kwargs,
    ):
        super().__init__(agent_id, transport, **kwargs)
        self._dlq_ttl = dlq_ttl
        self._dead_letters: list[dict[str, Any]] = []  # 死信索引
        self._stats: dict[str, int] = {
            "total_dead_letters": 0,
            "replayed": 0,
            "expired": 0,
        }
        self._cleanup_task: asyncio.Task | None = None

    @property
    def agent_type(self) -> str:
        return "dlq-consumer"

    async def on_start(self) -> None:
        """启动定期清理任务"""
        self._cleanup_task = asyncio.create_task(self._cleanup_loop())
        logger.info("[dlq-consumer] 已启动, TTL=%ds", self._dlq_ttl)

    async def on_stop(self) -> None:
        if self._cleanup_task:
            self._cleanup_task.cancel()
            try:
                await self._cleanup_task
            except asyncio.CancelledError:
                pass

    async def handle_message(self, message: Message) -> list[Message] | None:
        """处理死信消息"""
        payload = message.payload.copy()

        # 提取死信元数据
        original = payload.get("original_message", payload.get("original", {}))
        error_info = payload.get("error", "unknown")
        retry_count = payload.get("retry_count", 0)
        max_retries = payload.get("max_retries", 0)
        failed_by = payload.get("failed_by", "unknown")
        failed_at = payload.get(
            "failed_at",
            datetime.now(timezone.utc).isoformat(),
        )

        # 结构化日志
        logger.error(
            "[dlq-consumer] 死信收到 | trace=%s | failed_by=%s | "
            "retries=%d/%d | error=%s | original_type=%s",
            message.trace_id,
            failed_by,
            retry_count,
            max_retries,
            error_info,
            original.get("type", "?"),
        )

        # 记录到索引
        self._dead_letters.append({
            "message_id": message.message_id,
            "trace_id": message.trace_id,
            "original_message": original,
            "error": error_info,
            "retry_count": retry_count,
            "failed_by": failed_by,
            "failed_at": failed_at,
            "received_at": time.time(),
        })
        self._stats["total_dead_letters"] += 1

        # 限制索引大小
        if len(self._dead_letters) > 5000:
            self._dead_letters = self._dead_letters[-5000:]

        return None  # DLQ 消息不需要回复

    async def replay(self, message_id: str | None = None, trace_id: str | None = None) -> int:
        """重放死信消息到原始 topic

        Args:
            message_id: 按 message_id 查找并重放
            trace_id: 按 trace_id 查找并重放所有匹配死信

        Returns:
            成功重放的消息数
        """
        to_replay = []
        remaining = []

        for dl in self._dead_letters:
            matched = False
            if message_id and dl["message_id"] == message_id:
                matched = True
            elif trace_id and dl["trace_id"] == trace_id:
                matched = True

            if matched:
                to_replay.append(dl)
            else:
                remaining.append(dl)

        for dl in to_replay:
            original = dl["original_message"]
            if not original:
                continue

            # 重新构造原始消息
            msg = Message(
                type=MessageType(original.get("type", "TASK_DISPATCH")),
                source="dlq-consumer",
                payload=original.get("payload", {}),
                trace_id=dl.get("trace_id", ""),
                correlation_id=original.get("correlation_id", ""),
                topic=original.get("topic", Topic.AGENT_PROBE),
                target=original.get("target", ""),
            )

            # 发布到原始 topic
            original_topic = original.get("topic", Topic.AGENT_PROBE)
            await self._transport.publish(original_topic, msg)
            logger.info(
                "[dlq-consumer] 死信重放: msg=%s → topic=%s",
                dl.get("message_id"),
                original_topic,
            )

        self._dead_letters = remaining
        count = len(to_replay)
        self._stats["replayed"] += count
        return count

    async def get_dead_letters(self, limit: int = 50) -> list[dict[str, Any]]:
        """返回死信列表 (最近优先)"""
        return list(reversed(self._dead_letters[-limit:]))

    @property
    def stats(self) -> dict[str, Any]:
        return {
            **self._stats,
            "pending_dead_letters": len(self._dead_letters),
        }

    async def _cleanup_loop(self) -> None:
        """定期清理过期死信"""
        while self._running:
            await asyncio.sleep(self._dlq_ttl // 10)  # 每 TTL/10 秒清理一次
            if not self._dead_letters:
                continue
            now = time.time()
            cutoff = now - self._dlq_ttl
            expired_count = sum(
                1 for dl in self._dead_letters
                if dl.get("received_at", 0) < cutoff
            )
            if expired_count:
                self._dead_letters = [
                    dl for dl in self._dead_letters
                    if dl.get("received_at", 0) >= cutoff
                ]
                self._stats["expired"] += expired_count
                logger.info(
                    "[dlq-consumer] 清理 %d 条过期死信 (剩余 %d)",
                    expired_count,
                    len(self._dead_letters),
                )
