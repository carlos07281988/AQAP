"""Agent 基类 — 所有 AQAP Agent 的通用骨架

v3 改进:
  - 心跳单通道 (SYSTEM_EVENTS)，不再重复发到 BROADCAST
  - 幂等去重缓存使用后台定期清理线程，不在热路径上 O(n) 扫描
  - _evict_stale_ids 更高效: 每次只移除最旧的 1/4
  - 后台 cleanup 任务每 60s 执行一次
  - _consume_loop 支持后台并行处理 (BACKPRESSURE_SEM)
"""
from __future__ import annotations

import asyncio
import logging
import time as _time
import uuid
from abc import ABC, abstractmethod
from typing import Any

from aqap.core.dlq import DLQ_TOPIC, create_dlq_message
from aqap.core.message import (
    Message,
    MessageType,
    Topic,
    error_message,
    heartbeat,
    validate_message,
)
from aqap.core.security import PayloadCipher
from aqap.plugin.registry import registry
from aqap.transport.base import Transport

logger = logging.getLogger("aqap.agent")


class Agent(ABC):
    """Agent 抽象基类"""

    def __init__(
        self,
        agent_id: str,
        transport: Transport,
        group: str = "aqap-default",
        max_retries: int = 3,
        heartbeat_interval: int = 30,
        cipher: PayloadCipher | None = None,
        supervisor=None,
        max_concurrency: int = 0,  # 0 = 顺序处理
    ):
        self.agent_id = agent_id
        self._transport = transport
        self._group = group
        self._max_retries = max_retries
        self._heartbeat_interval = heartbeat_interval
        self._cipher = cipher or PayloadCipher()
        self._running = False
        self._draining = False
        self._tasks: list[asyncio.Task] = []
        self._topics: list[str | Topic] = []
        self._retry_counts: dict[str, int] = {}
        self._current_message: Message | None = None
        self._last_topic: str = ""
        self._last_group: str = ""
        self._last_msg_id: str | None = None
        # 幂等去重
        self._processed_ids: dict[str, float] = {}
        self._idempotency_max_size: int = 10000
        self._idempotency_ttl: int = 300
        self._last_idempotency_cleanup: float = _time.time()
        self._cleanup_interval: int = 60  # 后台清理间隔 (秒)
        self._supervisor = supervisor
        # 并发控制
        self._max_concurrency = max_concurrency
        # Scheduler 等 Agent 可设为 True 跳过 consume 循环
        self._skip_consume: bool = False
        self._sem: asyncio.Semaphore | None = (
            asyncio.Semaphore(max_concurrency) if max_concurrency > 0 else None
        )

    @property
    @abstractmethod
    def agent_type(self) -> str:
        ...

    @abstractmethod
    async def handle_message(self, message: Message) -> list[Message] | None:
        ...

    async def on_start(self) -> None:
        """Agent 启动钩子"""

    async def on_stop(self) -> None:
        """Agent 停止钩子"""

    def subscribe_to(self, topic: str | Topic):
        self._topics.append(topic)

    async def start(self):
        """启动 Agent"""
        if self._running:
            logger.info("[%s] 已在运行", self.agent_id)
            return

        self._running = True
        await self._transport.connect()
        await self.on_start()

        # 注册到系统 (单发 SYSTEM_EVENTS)
        await self._transport.publish(
            Topic.SYSTEM_EVENTS,
            Message(
                type=MessageType.REGISTER,
                source=self.agent_id,
                payload={"agent_type": self.agent_type},
            ),
        )

        # 心跳 (单通道: SYSTEM_EVENTS)
        self._tasks.append(asyncio.create_task(self._heartbeat_loop()))

        # 幂等缓存后台清理
        self._tasks.append(asyncio.create_task(self._idempotency_cleanup_loop()))

        # 消息消费 (SchedulerAgent 可通过 _skip_consume=True 跳过)
        if not self._skip_consume:
            for topic in self._topics:
                task = asyncio.create_task(self._consume_loop(topic))
                self._tasks.append(task)

        logger.info(
            "[%s] Agent 已启动, 订阅: %s, 并发: %s",
            self.agent_id, self._topics,
            f"max={self._max_concurrency}" if self._max_concurrency else "顺序",
        )

    async def stop(self):
        """优雅停止 Agent"""
        logger.info("[%s] 开始优雅关闭...", self.agent_id)
        self._running = False
        self._draining = True

        await self._transport.publish(
            Topic.SYSTEM_EVENTS,
            Message(type=MessageType.SHUTDOWN, source=self.agent_id, payload={}),
        )

        done, pending_ = await asyncio.wait(
            list(self._tasks), timeout=5,
            return_when=asyncio.ALL_COMPLETED,
        )
        for task in pending_:
            task.cancel()
        if pending_:
            await asyncio.gather(*pending_, return_exceptions=True)
        self._tasks.clear()

        self._draining = False
        await self.on_stop()
        await self._transport.disconnect()

    async def health_status(self) -> dict:
        return {
            "agent_id": self.agent_id,
            "type": self.agent_type,
            "running": self._running,
            "topics": [str(t) for t in self._topics],
            "tasks": len(self._tasks),
            "retry_backlog": len(self._retry_counts),
            "idempotency_cache": len(self._processed_ids),
        }

    # ── 内部循环 ──

    async def _heartbeat_loop(self):
        while self._running:
            msg = heartbeat(self.agent_id)
            # 单通道: SYSTEM_EVENTS 足够 Supervisor 监控
            # BROADCAST 由需要接收心跳的业务 Agent 自行订阅
            await self._transport.publish(Topic.SYSTEM_EVENTS, msg)
            if self._supervisor:
                self._supervisor.record_heartbeat(self.agent_id)
            await asyncio.sleep(self._heartbeat_interval)

    async def _idempotency_cleanup_loop(self):
        """后台定期清理过期幂等缓存 (不在热路径上扫描)"""
        while self._running:
            await asyncio.sleep(self._cleanup_interval)
            if not self._processed_ids:
                continue
            now = _time.time()
            cutoff = now - self._idempotency_ttl
            stale = [mid for mid, ts in self._processed_ids.items() if ts < cutoff]
            for mid in stale:
                del self._processed_ids[mid]
            # 如果仍然过大, 移除最旧的 1/4
            if len(self._processed_ids) > self._idempotency_max_size:
                sorted_ids = sorted(self._processed_ids.items(), key=lambda x: x[1])
                overflow = sorted_ids[:len(sorted_ids) - self._idempotency_max_size // 2]
                for mid, _ in overflow:
                    del self._processed_ids[mid]

            self._last_idempotency_cleanup = _time.time()

    async def _consume_loop(self, topic: str | Topic):
        consumer_id = f"{self.agent_id}-{uuid.uuid4().hex[:6]}"
        async for message in self._transport.subscribe(
            topic, group=self._group, consumer=consumer_id
        ):
            if not self._running:
                break

            self._last_topic = str(topic)
            self._last_group = self._group
            self._last_msg_id = getattr(message, "_transport_msg_id", None)

            # 忽略回声
            if message.source == self.agent_id:
                continue

            # 幂等去重 — O(1) 查找
            msg_id = message.message_id
            now = _time.time()
            ts = self._processed_ids.get(msg_id)
            if ts is not None and now - ts < self._idempotency_ttl:
                logger.debug(
                    "[%s] 跳过重复消息 %s (trace=%s)",
                    self.agent_id, msg_id, message.trace_id,
                )
                continue
            self._processed_ids[msg_id] = now

            # 校验消息格式
            raw = message.to_dict()
            validation_errors = validate_message(raw)
            if validation_errors:
                logger.warning(
                    "[%s] 消息校验失败 trace_id=%s: %s",
                    self.agent_id, message.trace_id, "; ".join(validation_errors),
                )
                continue

            # 未知类型 → 发 ERROR 并丢弃
            if message.type == MessageType.UNKNOWN:
                logger.warning(
                    "[%s] 收到未知类型消息 trace_id=%s, 发 ERROR",
                    self.agent_id, message.trace_id,
                )
                err = error_message(
                    source=self.agent_id,
                    code="UNKNOWN_TYPE",
                    message="不识别消息类型",
                    trace_id=message.trace_id,
                    original_message_id=message.message_id,
                )
                if message.source:
                    err.target = message.source
                await self._transport.publish(
                    Topic.agent_inbox(err.target) if err.target else Topic.BROADCAST,
                    err,
                )
                continue

            # 解密 payload
            try:
                message.payload = self._cipher.decrypt_payload(message.payload)
            except Exception as e:
                logger.warning("[%s] payload 解密失败: %s", self.agent_id, e)

            # 处理消息 (带 or 不带并发控制)
            if self._sem:
                async with self._sem:
                    await self._process_one(message, str(topic))
            else:
                await self._process_one(message, str(topic))

    def _determine_reply_topic(self, message: Message) -> str:
        """根据收到消息的 topic 决定回复发往哪个 topic"""
        topic_map = {
            Topic.AGENT_PROBE: Topic.AGENT_JUDGE,
            Topic.AGENT_JUDGE: Topic.AGENT_REPORTER,
            Topic.AGENT_REPORTER: Topic.BROADCAST,
        }
        # 精确匹配
        msg_topic = str(message.topic) if message.topic else ""
        result = topic_map.get(message.topic)
        if result:
            return result
        # 模糊匹配: 检查字符串前缀
        for pattern, target in topic_map.items():
            if msg_topic.startswith(str(pattern)):
                return target
        # 兜底: 回 inbox 给 source
        return Topic.agent_inbox(message.source)

    def _evict_stale_ids(self) -> None:
        """已废弃 — 改用 _idempotency_cleanup_loop 后台清理"""
        pass

    async def _handle_failure(self, message: Message, error: str, topic: str):
        """处理消息处理失败。

        Returns:
            True  → 消息已转入 DLQ (已 ACK)
            False → 消息将重试 (不应 ACK)
        """
        msg_key = self._msg_key(message)
        retry_count = self._retry_counts.get(msg_key, 0) + 1
        self._retry_counts[msg_key] = retry_count

        if retry_count >= self._max_retries:
            # 先发布 DLQ, 再 ACK (防止 ACK 后 crash 丢失 DLQ)
            dlq_record = create_dlq_message(
                original=message.to_dict(),
                error=error,
                retry_count=retry_count,
                max_retries=self._max_retries,
                failed_by=self.agent_id,
            )
            await self._transport.publish(
                DLQ_TOPIC,
                Message(
                    type=MessageType.ERROR,
                    source=self.agent_id,
                    trace_id=message.trace_id,
                    payload=dlq_record.to_dict(),
                ),
            )
            logger.warning(
                "[%s] 消息 %s 已达最大重试 %d, 转发 DLQ",
                self.agent_id, msg_key, self._max_retries,
            )
            self._retry_counts.pop(msg_key, None)
            if self._last_msg_id:
                await self._transport.ack(topic, self._last_msg_id, self._last_group)
            return True
        else:
            logger.info(
                "[%s] 消息 %s 重试 %d/%d",
                self.agent_id, msg_key, retry_count, self._max_retries,
            )
            return False

    @staticmethod
    def _msg_key(message: Message) -> str:
        return f"{message.source}:{message.trace_id}:{message.type}"

    async def _process_one(self, message: Message, topic: str):
        """处理单条消息 (提取为独立方法以简化并发控制)

        Returns: True (消息已处理并 ACK) | False (应重试, 保留在 pending 队列)
        """
        should_ack = True
        try:
            self._current_message = message
            replies = await self.handle_message(message)
            self._current_message = None

            if replies:
                for reply in replies:
                    topic_to_use = (
                        Topic.agent_inbox(reply.target)
                        if reply.target
                        else self._determine_reply_topic(message)
                    )
                    reply.payload = self._cipher.encrypt_payload(reply.payload)
                    await self._transport.publish(topic_to_use, reply)

            msg_key = self._msg_key(message)
            self._retry_counts.pop(msg_key, None)
            return True

        except Exception as e:
            logger.error(
                "[%s] 处理消息异常 trace_id=%s: %s",
                self.agent_id, message.trace_id, e,
            )
            self._current_message = None
            ack_dlq = await self._handle_failure(message, str(e), topic)
            if ack_dlq:
                return True  # 已转入 DLQ 并 ACK
            should_ack = False  # 应重试, 不 ACK
            return False

        finally:
            if should_ack and self._last_msg_id:
                await self._transport.ack(
                    topic, self._last_msg_id, self._last_group
                )

    async def run_plugins(self, topic: str, context: dict) -> list[dict]:
        """执行指定 topic 的所有插件, 自动注入追踪上下文"""
        ctx = context.copy() if context else {}
        if self._current_message:
            ctx["_aqap_start_time"] = _time.time()
            ctx["_aqap_trace_id"] = self._current_message.trace_id
            ctx["_aqap_message_type"] = str(self._current_message.type)
            ctx["_aqap_source"] = self._current_message.source
        return await registry.execute_all(topic, ctx)

    async def send(self, message: Message):
        if message.target:
            await self._transport.publish(Topic.agent_inbox(message.target), message)
        else:
            await self._transport.publish(Topic.BROADCAST, message)
