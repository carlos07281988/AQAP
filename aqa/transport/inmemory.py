"""
AQA InMemory Transport — 纯内存消息队列

用于测试和演示, 不需要 Redis / Kafka 即可验证架构。
"""
from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator

from aqa.core.message import Message, Topic
from aqa.transport.base import Transport

logger = logging.getLogger("aqa.transport.inmemory")


class InMemoryTransport(Transport):
    """内存 Transport — 使用 asyncio.Queue 模拟消息队列"""

    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}
        self._subscribers: dict[str, list[asyncio.Queue]] = {}
        self._running = True

    @property
    def name(self) -> str:
        return "in-memory"

    async def connect(self):
        logger.info("[inmemory] 已就绪")

    async def disconnect(self):
        self._running = False
        logger.info("[inmemory] 已关闭")

    async def create_group(self, topic: str | Topic, group: str):
        pass

    async def publish(self, topic: str | Topic, message: Message):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        if topic_str not in self._subscribers:
            logger.debug("[inmemory] publish %s → 无订阅者, 丢弃", topic_str)
            return
        for q in self._subscribers[topic_str]:
            await q.put(message)
        logger.debug(
            "[inmemory] publish %s → %d 订阅者 (msg=%s)",
            topic_str,
            len(self._subscribers[topic_str]),
            message.message_id[:8] if message.message_id else "?",
        )

    async def subscribe(
        self,
        topic: str | Topic,
        group: str = "aqa-default",
        consumer: str = "",
    ) -> AsyncGenerator[Message, None]:
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(topic_str, []).append(q)
        logger.info("[inmemory] 订阅 %s (consumer=%s)", topic_str, consumer)

        try:
            while self._running:
                try:
                    message = await asyncio.wait_for(q.get(), timeout=1.0)
                    logger.debug("[inmemory] 收到 %s → %s", topic_str, message.message_id[:8] if message.message_id else "?")
                    yield message
                except asyncio.TimeoutError:
                    continue
        finally:
            subs = self._subscribers.get(topic_str, [])
            if q in subs:
                subs.remove(q)
            logger.info("[inmemory] 取消订阅 %s (consumer=%s)", topic_str, consumer)

    async def ack(self, topic: str | Topic, message_id: str | None = None, group: str = "aqa-default"):
        pass  # InMemory 无需 ACK
