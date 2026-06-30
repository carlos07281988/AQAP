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


class TestInMemoryConsumerGroups:
    """InMemoryTransport 消费者组隔离测试 (v2)"""

    @pytest.mark.asyncio
    async def test_same_group_load_balance(self):
        """同组消费者负载均衡: 每条消息只被一个消费者消费"""
        from aqap.transport.inmemory import InMemoryTransport

        transport = InMemoryTransport()
        received_a = []
        received_b = []

        async def consume(transport, group, consumer_id, results):
            async for msg in transport.subscribe(
                "aqap:agent:probe", group=group, consumer=consumer_id
            ):
                results.append(msg)
                if len(results) >= 2:
                    break

        task_a = asyncio.create_task(
            consume(transport, "group-1", "worker-a", received_a)
        )
        task_b = asyncio.create_task(
            consume(transport, "group-1", "worker-b", received_b)
        )

        await asyncio.sleep(0.1)

        await transport.publish(
            "aqap:agent:probe",
            Message(MessageType.TASK_DISPATCH, "tester", {"seq": 1}),
        )
        await transport.publish(
            "aqap:agent:probe",
            Message(MessageType.TASK_DISPATCH, "tester", {"seq": 2}),
        )

        await asyncio.sleep(0.3)
        task_a.cancel()
        task_b.cancel()
        for t in (task_a, task_b):
            try:
                await t
            except (asyncio.CancelledError, StopAsyncIteration):
                pass
        await transport.disconnect()

        all_received = received_a + received_b
        assert len(all_received) == 2, f"Expected 2 total, got {len(all_received)}"
        seqs = sorted(m.payload["seq"] for m in all_received)
        assert seqs == [1, 2]

    @pytest.mark.asyncio
    async def test_different_group_fanout(self):
        """不同组消费者扇出: 每条消息被所有组消费"""
        from aqap.transport.inmemory import InMemoryTransport

        transport = InMemoryTransport()
        received_g1 = []
        received_g2 = []

        async def consume(transport, group, results):
            async for msg in transport.subscribe("aqap:agent:probe", group=group, consumer="w"):
                results.append(msg)
                if len(results) >= 1:
                    break

        task1 = asyncio.create_task(consume(transport, "group-x", received_g1))
        task2 = asyncio.create_task(consume(transport, "group-y", received_g2))
        await asyncio.sleep(0.1)

        await transport.publish(
            "aqap:agent:probe",
            Message(MessageType.TASK_DISPATCH, "tester", {"seq": 1}),
        )
        await asyncio.sleep(0.3)

        task1.cancel()
        task2.cancel()
        for t in (task1, task2):
            try:
                await t
            except (asyncio.CancelledError, StopAsyncIteration):
                pass
        await transport.disconnect()

        assert len(received_g1) == 1, f"group-x got {len(received_g1)}"
        assert len(received_g2) == 1, f"group-y got {len(received_g2)}"


class TestValidationEdgeCases:
    """消息验证边界测试"""

    def test_validate_empty_payload(self):
        errors = validate_message({
            "type": "HEARTBEAT", "source": "agent-1", "payload": {}, "version": "1.0",
        })
        assert errors == []

    def test_validate_non_dict_payload(self):
        errors = validate_message({
            "type": "HEARTBEAT", "source": "agent-1", "payload": "not-a-dict", "version": "1.0",
        })
        assert any("JSON Object" in e for e in errors)

    def test_validate_long_type_name(self):
        errors = validate_message({
            "type": "A" * 33, "source": "tester", "payload": {}, "version": "1.0",
        })
        assert any("未知" in e for e in errors)

    def test_validate_version_minor_bump(self):
        errors = validate_message({
            "type": "TASK_DISPATCH", "source": "tester", "payload": {}, "version": "1.5",
        })
        assert errors == []

    def test_validate_version_major_bump(self):
        errors = validate_message({
            "type": "TASK_DISPATCH", "source": "tester", "payload": {}, "version": "2.0",
        })
        assert any("版本" in e for e in errors)

    def test_validate_missing_fields(self):
        errors = validate_message({"type": "TASK_DISPATCH"})
        missing = [e for e in errors if "缺少" in e]
        assert len(missing) >= 2


class TestAgentEdgeCases:
    """Agent 边界场景测试"""

    @pytest.mark.asyncio
    async def test_inbox_message_routing(self):
        """带 target 的消息应路由到 inbox"""
        from test_aqa import _TestTransport
        from aqap.agent.probe import ProbeAgent

        transport = _TestTransport()
        probe = ProbeAgent("probe-routing", transport, heartbeat_interval=999)
        probe.subscribe_to("aqap:broadcast")
        await probe.start()
        await asyncio.sleep(0.05)

        msg = Message(
            type=MessageType.TASK_DISPATCH,
            source="cli",
            target="",
            payload={"task_id": "routing-test"},
        )
        await transport.publish("aqap:broadcast", msg)
        await asyncio.sleep(0.3)
        await probe.stop()

        dispatched = [m for t, m in transport.published if m.type == MessageType.TASK_RESULT]
        assert len(dispatched) >= 1

    @pytest.mark.asyncio
    async def test_echo_suppression(self):
        """Agent 应忽略自己发出的消息 (回声抑制)"""
        from test_aqa import _TestTransport
        from aqap.agent.base import Agent

        class _EchoAgent(Agent):
            @property
            def agent_type(self):
                return "echo"

            async def handle_message(self, message):
                return [message.reply(MessageType.TASK_RESULT, {"echo": True})]

        transport = _TestTransport()
        agent = _EchoAgent("echo-test", transport, heartbeat_interval=999)
        agent.subscribe_to("aqap:broadcast")
        await agent.start()
        await asyncio.sleep(0.05)

        msg = Message(type=MessageType.TASK_DISPATCH, source="echo-test", payload={"x": 1})
        await transport.publish("aqap:broadcast", msg)
        await asyncio.sleep(0.3)
        await agent.stop()

        # 回声抑制后不应有 TASK_RESULT
        results = [m for t, m in transport.published if m.type == MessageType.TASK_RESULT]
        assert len(results) == 0, f"Echo was not suppressed, got {len(results)} results"
