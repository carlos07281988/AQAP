"""Redis Streams Transport 实现"""
from __future__ import annotations

import asyncio

import json
import logging
from typing import Any, AsyncGenerator

from aqap.core.message import Message, Topic
from aqap.transport.base import Transport

logger = logging.getLogger("aqap.transport.redis_streams")


class RedisStreamsTransport(Transport):
    """
    基于 Redis Streams 的消息队列传输层。

    使用 Redis 原生的 Stream 数据结构实现可靠的消息传递，
    支持消费组、待处理消息列表 (pending list) 和死信机制。

    v2 改进:
      - 去掉空轮询的 sleep(0.1), 完全依赖 BLOCK 参数实现长轮询
      - 添加连接异常自动重连
      - 改进 bytes/str key 兼容性
    """

    def __init__(self, stream_url: str = "redis://127.0.0.1:6379", **kwargs):
        import redis.asyncio as aioredis

        self._redis: aioredis.Redis = aioredis.from_url(stream_url, **kwargs)
        self._running = False
        self._stream_url = stream_url

    @property
    def name(self) -> str:
        return "redis-streams"

    async def connect(self) -> None:
        await self._redis.ping()
        self._running = True
        logger.info("[redis] 已连接")

    async def disconnect(self) -> None:
        self._running = False
        await self._redis.close()
        logger.info("[redis] 已断开")

    async def create_group(self, topic: str | Topic, group: str) -> None:
        """创建消费组（幂等）"""
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        try:
            await self._redis.xgroup_create(topic_str, group, id="0", mkstream=True)
        except Exception:
            pass  # 组已存在

    async def publish(self, topic: str | Topic, message: Message) -> None:
        """发布消息到 Redis Stream"""
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        payload = message.to_json()
        await self._redis.xadd(
            topic_str, {"json": payload}, maxlen=10000, approximate=True
        )
        logger.debug("[redis] 已发布到 %s: %s", topic_str, message.message_id[:8])

    async def subscribe(
        self,
        topic: str | Topic,
        group: str = "aqap-default",
        consumer: str = "",
    ) -> AsyncGenerator[Message, None]:
        """订阅 topic 并持续消费消息（异步生成器）

        长轮询:
          - 使用 XREADGROUP BLOCK 5000 实现 (无需额外 sleep)
          - 异常时自动重连 Redis 连接
        """
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        await self.create_group(topic_str, group)

        while self._running:
            try:
                results = await self._redis.xreadgroup(
                    group, consumer, {topic_str: ">"},
                    count=10, block=5000,
                )
                if not results:
                    continue

                for _stream_name, messages in results:
                    for msg_id, data in messages:
                        try:
                            # 兼容 bytes/str key
                            raw = None
                            for k in (b"json", "json"):
                                val = data.get(k)
                                if val is not None:
                                    raw = val.decode() if isinstance(val, bytes) else val
                                    break
                            if not raw:
                                continue

                            message = Message.from_json(raw)
                            message._transport_msg_id = msg_id
                            yield message
                        except Exception as e:
                            logger.warning("[redis] 消息解析失败 %s: %s", msg_id, e)
            except Exception as e:
                if not self._running:
                    break
                logger.error("[redis] 订阅循环异常, 5s 后重连: %s", e)
                await asyncio.sleep(5)
                try:
                    await self._redis.ping()
                except Exception:
                    logger.info("[redis] 尝试重新连接...")
                    await self.connect()

    async def ack(self, topic: str | Topic, message_id: str | None = None, group: str = "") -> None:
        """确认消息已处理"""
        if message_id is None:
            return
        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        group_str = group or "aqap-default"
        await self._redis.xack(topic_str, group_str, message_id)

    async def pending(
        self, topic: str, group: str, count: int = 100
    ) -> list[dict[str, Any]]:
        """查看待处理消息"""
        pending_info = await self._redis.xpending_range(
            topic, group, min="-", max="+", count=count
        )
        return [
            {
                "msg_id": p["message_id"],
                "consumer": p["consumer"],
                "delivered": p["times_delivered"],
                "last_delivered": p["last_delivered"],
            }
            for p in pending_info
        ]

    async def health(self) -> dict[str, Any]:
        try:
            await self._redis.ping()
            return {"status": "ok", "backend": "redis-streams"}
        except Exception as e:
            return {"status": "error", "backend": "redis-streams", "error": str(e)}
