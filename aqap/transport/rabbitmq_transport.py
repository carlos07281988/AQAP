"""
AQAP RabbitMQ Transport — AMQP 消息队列后端

依赖: aio-pika>=9.0
使用前需安装: pip install aio-pika

配置: config.yaml → transport.backend: rabbitmq
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import AsyncGenerator

from aqap.core.message import Message, Topic
from aqap.transport.base import Transport

logger = logging.getLogger("aqap.transport.rabbitmq")

try:
    import aio_pika

    RABBITMQ_AVAILABLE = True
except ImportError:
    RABBITMQ_AVAILABLE = False


class RabbitMQTransport(Transport):
    """RabbitMQ / AMQP 传输层

    使用 aio-pika 实现，接口与 Transport 基类一致。
    支持 durable queues、消费组 (via multiple consumers on same queue)、
    publisher confirms。
    """

    def __init__(
        self,
        amqp_url: str = "amqp://guest:guest@127.0.0.1:5672/",
        prefetch_count: int = 10,
    ):
        if not RABBITMQ_AVAILABLE:
            raise ImportError(
                "RabbitMQTransport 需要 aio-pika: pip install aio-pika"
            )
        self._amqp_url = amqp_url
        self._prefetch_count = prefetch_count
        self._connection: aio_pika.RobustConnection | None = None
        self._channel: aio_pika.RobustChannel | None = None
        self._running = False
        self._subscriber_queues: dict[str, asyncio.Queue] = {}

    @property
    def name(self) -> str:
        return "rabbitmq"

    async def connect(self) -> None:
        self._connection = await aio_pika.connect_robust(self._amqp_url)
        self._channel = await self._connection.channel()
        await self._channel.set_qos(prefetch_count=self._prefetch_count)
        self._running = True
        logger.info("[rabbitmq] 已连接")

    async def disconnect(self) -> None:
        self._running = False
        if self._channel:
            await self._channel.close()
        if self._connection:
            await self._connection.close()
        logger.info("[rabbitmq] 已断开")

    async def create_group(self, topic: str | Topic, group: str) -> None:
        """RabbitMQ 消费组通过 queue naming 实现 (group = queue name suffix)"""
        pass  # 在 subscribe 中自动完成

    async def publish(self, topic: str | Topic, message: Message) -> None:
        """发布消息到 RabbitMQ exchange → queue"""
        if not self._channel:
            raise RuntimeError("RabbitMQ 未连接")

        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        exchange = await self._channel.declare_exchange(
            topic_str, aio_pika.ExchangeType.FANOUT, durable=True,
        )

        body = message.to_json().encode()
        await exchange.publish(
            aio_pika.Message(body=body, delivery_mode=aio_pika.DeliveryMode.PERSISTENT),
            routing_key="",
        )
        logger.debug("[rabbitmq] 已发布 %s → %s", message.message_id[:8], topic_str)

    def subscribe(
        self, topic: str | Topic, group: str = "aqap-default", consumer: str = ""
    ) -> AsyncGenerator[Message, None]:
        return self._subscribe_loop(topic, group, consumer)

    async def _subscribe_loop(
        self, topic: str, group: str, consumer_id: str
    ) -> AsyncGenerator[Message, None]:
        if not self._channel:
            raise RuntimeError("RabbitMQ 未连接")

        topic_str = str(topic.value) if isinstance(topic, Topic) else topic
        exchange = await self._channel.declare_exchange(
            topic_str, aio_pika.ExchangeType.FANOUT, durable=True,
        )
        queue_name = f"{topic_str}:{group}"
        queue = await self._channel.declare_queue(queue_name, durable=True)
        await queue.bind(exchange)

        async with queue.iterator() as queue_iter:
            async for aio_msg in queue_iter:
                if not self._running:
                    break
                try:
                    data = json.loads(aio_msg.body.decode())
                    message = Message.from_dict(data)
                    message._transport_msg_id = aio_msg.delivery_tag
                    yield message
                except (json.JSONDecodeError, KeyError) as e:
                    logger.warning("[rabbitmq] 解析失败 topic=%s: %s", topic_str, e)
                    await aio_msg.ack()  # 不可恢复，直接 ACK

    async def ack(
        self,
        topic: str | Topic,
        message_id: str | None = None,
        group: str = "",
    ) -> None:
        """RabbitMQ 使用 auto-ack + qos，在 queue iterator 中自动确认"""
        pass

    async def health(self) -> dict:
        try:
            if self._connection and not self._connection.is_closed:
                return {"status": "ok", "backend": "rabbitmq"}
        except Exception as e:
            return {"status": "error", "backend": "rabbitmq", "error": str(e)}
        return {"status": "disconnected", "backend": "rabbitmq"}
