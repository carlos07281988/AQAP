"""
AQAP 扩展测试 — DLQ + Scheduler + Hot Reload + RabbitMQ Transport
"""
from __future__ import annotations

import asyncio
import json
import pytest

from aqap.core.message import (
    Message,
    MessageType,
    Topic,
    task_dispatch,
    ErrorCode,
)
from aqap.core.dlq import DLQ_TOPIC, create_dlq_message, DeadLetterRecord
from aqap.plugin.registry import registry
from aqap.transport.base import Transport
from typing import AsyncGenerator


# ── 测试用 Transport ──

class _TestTransport(Transport):
    def __init__(self):
        self._queues: dict[str, asyncio.Queue] = {}
        self._subs: dict[str, list[asyncio.Queue]] = {}
        self._running = True
        self.published: list[tuple[str, Message]] = []
        self.acked: list[tuple[str, str]] = []

    @property
    def name(self):
        return "test"

    async def connect(self): pass
    async def disconnect(self):
        self._running = False

    async def create_group(self, topic, group): pass

    async def publish(self, topic, message):
        t = str(topic.value) if isinstance(topic, Topic) else topic
        self.published.append((t, message))
        for q in self._subs.get(t, []):
            await q.put(message)

    async def subscribe(self, topic, group="", consumer="") -> AsyncGenerator[Message, None]:
        t = str(topic.value) if isinstance(topic, Topic) else topic
        q = asyncio.Queue()
        self._subs.setdefault(t, []).append(q)
        try:
            while self._running:
                msg = await asyncio.wait_for(q.get(), timeout=0.5)
                yield msg
        except asyncio.TimeoutError:
            pass
        finally:
            if q in self._subs.get(t, []):
                self._subs[t].remove(q)

    async def ack(self, topic, msg_id=None, group=""):
        if msg_id:
            self.acked.append((str(topic), msg_id))


# ── DLQ Consumer 测试 ──

class TestDLQConsumer:
    """DLQConsumerAgent 测试"""

    @pytest.mark.asyncio
    async def test_dlq_consumer_receives_dead_letters(self):
        from aqap.agent.dlq_consumer import DLQConsumerAgent

        transport = _TestTransport()
        dlq = DLQConsumerAgent("dlq-test", transport, heartbeat_interval=999)
        dlq.subscribe_to(DLQ_TOPIC)
        await dlq.start()
        await asyncio.sleep(0.05)

        # 发送一条模拟死信
        dlq_msg = create_dlq_message(
            original={"type": "TASK_DISPATCH", "source": "test-1", "payload": {}},
            error="ZeroDivisionError",
            retry_count=3,
            max_retries=3,
            failed_by="probe-1",
        )
        msg = Message(
            type=MessageType.ERROR,
            source="test-system",
            topic=DLQ_TOPIC,
            payload=dlq_msg.to_dict(),
        )
        await transport.publish(DLQ_TOPIC, msg)
        await asyncio.sleep(0.5)
        await dlq.stop()

        assert dlq.stats["total_dead_letters"] >= 1

    @pytest.mark.asyncio
    async def test_dlq_replay(self):
        from aqap.agent.dlq_consumer import DLQConsumerAgent

        transport = _TestTransport()
        dlq = DLQConsumerAgent("dlq-test", transport, heartbeat_interval=999)
        dlq.subscribe_to(DLQ_TOPIC)
        await dlq.start()
        await asyncio.sleep(0.05)

        # 添加一条死信到索引
        dlq._dead_letters.append({
            "message_id": "abc123",
            "trace_id": "trace-xyz",
            "original_message": {"type": "TASK_DISPATCH", "topic": "aqap:agent:probe", "payload": {"task_id": "replay-1"}},
            "error": "test-error",
            "retry_count": 3,
            "failed_by": "probe-1",
            "failed_at": "2026-01-01T00:00:00+00:00",
            "received_at": asyncio.get_event_loop().time(),
        })

        count = await dlq.replay(message_id="abc123")
        await asyncio.sleep(0.1)
        await dlq.stop()

        assert count == 1
        assert dlq.stats["replayed"] == 1


# ── Scheduler 测试 ──

class TestScheduler:
    """SchedulerAgent 测试"""

    @pytest.mark.asyncio
    async def test_scheduler_dispatches(self):
        from aqap.agent.scheduler import SchedulerAgent

        transport = _TestTransport()
        sched = SchedulerAgent(
            "sched-test", transport,
            schedule=[{"cron": "*/1s", "topic": Topic.AGENT_PROBE, "payload": {"task_id": "sched-001"}}],
            heartbeat_interval=999,
        )
        await sched.start()
        await asyncio.sleep(2.0)
        await sched.stop()

        # 应该至少发了一条 TASK_DISPATCH
        dispatches = [
            m for t, m in transport.published
            if m.type == MessageType.TASK_DISPATCH and m.source == "sched-test"
        ]
        assert len(dispatches) >= 1
        assert dispatches[0].payload["task_id"] == "sched-001"

    @pytest.mark.asyncio
    async def test_scheduler_multiple_jobs(self):
        from aqap.agent.scheduler import SchedulerAgent

        transport = _TestTransport()
        sched = SchedulerAgent(
            "sched-multi", transport,
            schedule=[
                {"cron": "*/1s", "topic": Topic.AGENT_PROBE, "payload": {"task_id": "job-a"}},
                {"cron": "*/2s", "topic": Topic.AGENT_JUDGE, "payload": {"task_id": "job-b"}},
            ],
            heartbeat_interval=999,
        )
        await sched.start()
        await asyncio.sleep(2.5)
        await sched.stop()

        dispatches = [
            (t, m) for t, m in transport.published
            if m.type == MessageType.TASK_DISPATCH and m.source == "sched-multi"
        ]
        assert len(dispatches) >= 2


# ── CLI 测试 ──

class TestCLI:
    """CLI 工具测试"""

    def test_secret_generation(self):
        from aqap.core.security import generate_secret

        s1 = generate_secret()
        s2 = generate_secret()
        assert len(s1) == 44  # base64(32 bytes)
        assert s1 != s2

    def test_error_code_enum(self):
        assert ErrorCode.AUTH_FAILURE == "AUTH_FAILURE"
        assert ErrorCode.FORBIDDEN == "FORBIDDEN"
        assert str(ErrorCode.UNKNOWN_TYPE) == "UNKNOWN_TYPE"


# ── 配置热加载测试 ──

class TestHotReload:
    """ConfigWatcher 测试"""

    @pytest.mark.asyncio
    async def test_watcher_detects_change(self, tmp_path):
        from aqap.config_hotreload import ConfigWatcher

        config_file = tmp_path / "test-config.yaml"
        config_file.write_text("app:\n  version: \"1.0.0\"\n")

        changes = []

        async def callback(new_config):
            changes.append(new_config)

        watcher = ConfigWatcher(config_file, callback, poll_interval=0.3)
        await watcher.start()

        # 修改文件
        await asyncio.sleep(0.5)
        config_file.write_text("app:\n  version: \"2.0.0\"\n")

        await asyncio.sleep(1.0)
        await watcher.stop()

        assert len(changes) >= 1
        assert changes[0]["app"]["version"] == "2.0.0"


# ── CronSchedule 解析测试 ──

class TestCronSchedule:
    """CronSchedule 解析"""

    def test_parse_seconds(self):
        from aqap.agent.scheduler import CronSchedule

        assert CronSchedule("*/30s").next_seconds() == 30
        assert CronSchedule("*/5s").next_seconds() == 5

    def test_parse_minutes(self):
        from aqap.agent.scheduler import CronSchedule

        assert CronSchedule("*/5m").next_seconds() == 300
        assert CronSchedule("*/1m").next_seconds() == 60

    def test_parse_hours(self):
        from aqap.agent.scheduler import CronSchedule

        assert CronSchedule("*/1h").next_seconds() == 3600

    def test_parse_hourly_daily(self):
        from aqap.agent.scheduler import CronSchedule

        assert CronSchedule("@hourly").next_seconds() == 3600
        assert CronSchedule("@daily").next_seconds() == 86400

    def test_default(self):
        from aqap.agent.scheduler import CronSchedule

        assert CronSchedule("invalid-expr").next_seconds() == 60
