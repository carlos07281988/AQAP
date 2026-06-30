"""
AQAP InMemory Transport — 纯内存消息队列

用于测试和演示, 不需要 Redis / Kafka 即可验证架构。

v2 改进:
  - 消费者组隔离: 同 group 的消费者共享队列 (负载均衡)
  - 不同 group 的消费者各自独立消费 (fanout)
"""
from __future__ import annotations

import asyncio
import logging
from typing import AsyncGenerator

from aqap.core.message import Message, Topic
from aqap.transport.base import Transport

logger = logging.getLogger("aqap.transport.inmemory")


class _GroupQueue:
    """消费者组队列 — 同组消费者轮转分配消息"""

    def __init__(self):
        self._queues: list[asyncio.Queue] = []

    def add_consumer(self, q: asyncio.Queue):
        self._queues.append(q)

    def remove_consumer(self, q: asyncio.Queue):
        if q in self._queues:
            self._queues.remove(q)

    async def publish(self, message: Message):
        if not self._queues:
            return
        # 轮转: 发给最空闲的消费者
        q = self._queues[0]
        await q.put(message)
        # 简单轮转
        self._queues.append(self._queues.pop(0))

    @property
    def consumer_count(self) -> int:
        return len(self._queues)


class InMemoryTransport(Transport):
    """内存 Transport — 支持消费者组隔离"""

    def __init__(self):
        # topic -> group_name -> _GroupQueue
        self._groups: dict[str, dict[str, _GroupQueue]] = {}
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
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        self._groups.setdefault(topic_str, {})
        if group not in self._groups[topic_str]:
            self._groups[topic_str][group] = _GroupQueue()
            logger.debug("[inmemory] 创建组 %s/%s", topic_str, group)

    async def publish(self, topic: str | Topic, message: Message):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        topic_groups = self._groups.get(topic_str)
        if not topic_groups:
            logger.debug("[inmemory] publish %s → 无消费者, 丢弃", topic_str)
            return
        for group_name, gq in topic_groups.items():
            await gq.publish(message)
        logger.debug(
            "[inmemory] publish %s → %d 组 (msg=%s)",
            topic_str,
            len(topic_groups),
            message.message_id[:8] if message.message_id else "?",
        )

    async def subscribe(
        self,
        topic: str | Topic,
        group: str = "aqap-default",
        consumer: str = "",
    ) -> AsyncGenerator[Message, None]:
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        await self.create_group(topic_str, group)

        q: asyncio.Queue = asyncio.Queue()
        self._groups[topic_str][group].add_consumer(q)
        logger.info(
            "[inmemory] 订阅 %s (group=%s, consumer=%s)",
            topic_str, group, consumer,
        )

        try:
            while self._running:
                try:
                    message = await asyncio.wait_for(q.get(), timeout=1.0)
                    yield message
                except asyncio.TimeoutError:
                    continue
        finally:
            gq = self._groups.get(topic_str, {}).get(group)
            if gq:
                gq.remove_consumer(q)
            logger.info(
                "[inmemory] 取消订阅 %s (group=%s, consumer=%s)",
                topic_str, group, consumer,
            )

    async def ack(self, topic: str | Topic, message_id: str | None = None, group: str = "aqap-default"):
        pass  # InMemory 无需 ACK
