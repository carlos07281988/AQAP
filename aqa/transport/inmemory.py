"""
AQA InMemory Transport — 纯内存消息队列

用于测试和演示, 不需要 Redis / Kafka 即可验证架构。
"""
from __future__ import annotations

import asyncio
from typing import AsyncGenerator

from aqa.core.message import Message, Topic
from aqa.transport.base import Transport


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
        print("[transport] InMemory 已就绪")

    async def disconnect(self):
        self._running = False
        print("[transport] InMemory 已关闭")

    async def create_group(self, topic: str | Topic, group: str):
        pass

    async def publish(self, topic: str | Topic, message: Message):
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        if topic_str not in self._subscribers:
            return
        for q in self._subscribers[topic_str]:
            await q.put(message)

    async def subscribe(
        self,
        topic: str | Topic,
        group: str = "aqa-default",
        consumer: str = "",
    ) -> AsyncGenerator[Message, None]:
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        q: asyncio.Queue = asyncio.Queue()
        self._subscribers.setdefault(topic_str, []).append(q)

        try:
            while self._running:
                try:
                    message = await asyncio.wait_for(q.get(), timeout=1.0)
                    yield message
                except asyncio.TimeoutError:
                    continue
        finally:
            subs = self._subscribers.get(topic_str, [])
            if q in subs:
                subs.remove(q)

    async def ack(self, topic: str | Topic, message_id: str | None = None, group: str = "aqa-default"):
        pass  # InMemory 无需 ACK
