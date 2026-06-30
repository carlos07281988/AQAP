#!/usr/bin/env python3
"""
AQAP 设计全景 Demo — 协议 · 生命周期 · 传输 · 安全 · 插件 · 错误处理 · 对接

运行: python examples/design_demo.py
"""
from __future__ import annotations

import asyncio
import json
import logging

from aqap.core.message import Message, MessageType, Topic
from aqap.transport.inmemory import InMemoryTransport
from aqap.agent.base import Agent
from aqap.agent.probe import ProbeAgent
from aqap.agent.judge import JudgeAgent
from aqap.agent.reporter import ReporterAgent
from aqap.core.security import PayloadCipher, generate_secret
from aqap.plugin.base import Plugin
from aqap.plugin.registry import registry
from aqap.core.dlq import DLQ_TOPIC

logging.basicConfig(level=logging.WARNING, format="%(levelname)s %(message)s")


# ═══════════════════════════════════════════════════════════════
#  5. 插件 — 可插拔行为扩展
# ═══════════════════════════════════════════════════════════════

class AuditPlugin(Plugin):
    @property
    def name(self) -> str: return "audit"
    @property
    def version(self) -> str: return "1.0.0"
    async def initialize(self, config: dict): pass
    async def execute(self, ctx: dict) -> dict:
        trace = ctx.get("_aqap_trace_id", "?")[:10]
        print(f"       [plugin:audit] trace={trace}")
        return {"audited": True}
    async def cleanup(self): pass


class ScorerPlugin(Plugin):
    """评分插件 — 给 Judge 提供评分依据"""
    @property
    def name(self) -> str: return "scorer"
    @property
    def version(self) -> str: return "1.0.0"
    async def initialize(self, config: dict): pass
    async def execute(self, ctx: dict) -> dict:
        score = ctx.get("score", 0.0) or 0.85
        return {"score": score, "passed": score >= 0.7}
    async def cleanup(self): pass


# ═══════════════════════════════════════════════════════════════
#  6. 错误处理 — 用 CrashAgent 触发 DLQ
# ═══════════════════════════════════════════════════════════════

class CrashAgent(Agent):
    """收到任何消息就崩溃 → trigger 重试 → DLQ"""
    @property
    def agent_type(self) -> str: return "crash"

    async def handle_message(self, message: Message) -> list[Message] | None:
        raise RuntimeError(f"CRASH: task_id={message.payload.get('task_id', '?')}")


# ═══════════════════════════════════════════════════════════════
#  快速关闭 (跳过 5s drain)
# ═══════════════════════════════════════════════════════════════

async def fast_stop(*agents: Agent):
    for a in agents:
        a._running = False
        for t in a._tasks:
            t.cancel()
        if a._tasks:
            await asyncio.gather(*a._tasks, return_exceptions=True)
        a._tasks.clear()


async def main():
    transport = InMemoryTransport()
    cipher = PayloadCipher(generate_secret())

    # 注册插件
    registry.register(AuditPlugin(), topics=["probe", "reporter"])
    registry.register(ScorerPlugin(), topics=["judge"])
    await registry.initialize_all({})

    # ── 标准链路 ──
    probe = ProbeAgent("probe-1", transport, cipher=cipher)
    judge = JudgeAgent("judge-1", transport, cipher=cipher)
    reporter = ReporterAgent("reporter-1", transport, cipher=cipher)

    probe.subscribe_to(Topic.AGENT_PROBE)
    judge.subscribe_to(Topic.AGENT_JUDGE)
    judge.subscribe_to(Topic.agent_inbox("judge-1"))
    reporter.subscribe_to(Topic.AGENT_REPORTER)
    reporter.subscribe_to(Topic.agent_inbox("reporter-1"))

    for a in [probe, judge, reporter]:
        a._running = True
        await transport.connect()
        for t in a._topics:
            a._tasks.append(asyncio.create_task(a._consume_loop(t)))
        await a.on_start()
    await asyncio.sleep(0.1)

    print("╔" + "═" * 58 + "╗")
    print("║  AQAP 设计全景                                          ║")
    print("║  协议 · 生命周期 · 传输 · 安全 · 插件 · 错误处理 · 对接 ║")
    print("╚" + "═" * 58 + "╝")

    # 外部监听广播
    bc_results = []
    async def listen_bc():
        async for msg in transport.subscribe("aqap:broadcast", consumer="cli"):
            if msg.type == MessageType.REPORT_DELIVER:
                bc_results.append(msg)
    bc_listener = asyncio.create_task(listen_bc())
    await asyncio.sleep(0.02)

    # ═══════════════════════════════════════════════════════════════
    #  Phase 1: 协议设计 — 完整状态机
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'─' * 60}")
    print("  [1] 协议设计 — 完整状态机 (DISPATCH→JUDGE→REPORT)")
    print(f"{'─' * 60}")

    await transport.publish(Topic.AGENT_PROBE, Message(
        MessageType.TASK_DISPATCH, "cli", {
            "task_id": "d-001", "name": "API 检测",
            "target": "user-service",
        },
    ))
    await asyncio.sleep(1.0)

    # ═══════════════════════════════════════════════════════════════
    #  Phase 2: 安全
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'─' * 60}")
    print("  [2] 安全设计 — Payload AES-256-GCM 加密")
    print(f"{'─' * 60}")

    payload = {"task_id": "sec-001", "api_key": "sk-abc123"}
    enc = cipher.encrypt_payload(payload)
    dec = cipher.decrypt_payload(enc)
    print(f"\n  ▶ 明文:   {payload}")
    print(f"  ▶ 密文:   {json.dumps(enc)}")
    print(f"  ▶ 解密:   {dec}")
    print(f"\n  说明: Agent 基类自动加解密, 线上传输密文, 开发者零感知")

    # ═══════════════════════════════════════════════════════════════
    #  Phase 3: 错误 + DLQ
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'─' * 60}")
    print("  [3] 错误处理 — 重试 + DLQ 死信路由")
    print(f"{'─' * 60}")

    crash = CrashAgent("crash-1", transport, max_retries=1)
    crash.subscribe_to("test:dlq")
    crash._running = True
    await transport.connect()
    crash._tasks.append(asyncio.create_task(crash._consume_loop("test:dlq")))
    await crash.on_start()
    await asyncio.sleep(0.05)

    dlq_msgs = []
    async def listen_dlq():
        async for msg in transport.subscribe(DLQ_TOPIC, consumer="monitor"):
            dlq_msgs.append(msg)
    dlq_listener = asyncio.create_task(listen_dlq())
    await asyncio.sleep(0.02)

    # 发送 → CrashAgent 崩溃 → max_retries=1 → 直达 DLQ
    await transport.publish("test:dlq", Message(
        MessageType.TASK_DISPATCH, "cli", {"task_id": "fail-001"},
    ))
    await asyncio.sleep(0.5)

    dlq_listener.cancel()
    try: await dlq_listener
    except asyncio.CancelledError: pass

    if dlq_msgs:
        print(f"\n  ▶ DLQ 收到 {len(dlq_msgs)} 条死信:")
        for d in dlq_msgs:
            err = d.payload.get("error", "")
            orig = d.payload.get("original_message", {})
            print(f"     error='{err}'")
            print(f"     original_task_id={orig.get('payload',{}).get('task_id','?')}")
            print(f"     retry_count={d.payload.get('retry_count','?')}")
    else:
        print(f"\n  ▶ 未触发 DLQ")
    print(f"\n  说明: handle_message 抛异常 → 不 ACK → 重试达上限 → DLQ")
    print(f"        InMemory 不自动重投; Redis/Kafka pending 列表会重试")

    await fast_stop(crash)

    # ═══════════════════════════════════════════════════════════════
    #  Phase 4-6: 传输层 / 对接 / 生命周期
    # ═══════════════════════════════════════════════════════════════
    print(f"\n{'─' * 60}")
    print("  [4] 传输层设计 — Transport 可插拔抽象")
    print(f"{'─' * 60}")
    print(f"  当前: {transport.name}")
    print(f"  可选: RedisStreamsTransport | KafkaTransport | RabbitMQTransport")
    print(f"  原则: Agent 只认 Transport 抽象接口, 换后端不改 Agent 代码")

    print(f"\n{'─' * 60}")
    print("  [5] 对接设计 — 任意语言一条 JSON 接入")
    print(f"{'─' * 60}")
    print(f"  发送: 拼 AQA 信封 JSON → publish 到 topic")
    print(f"  接收: subscribe aqap:broadcast → 收 REPORT_DELIVER")
    print(f"  示例: {json.dumps({'type':'TASK_DISPATCH','source':'curl','topic':'aqap:agent:probe','payload':{'task_id':'x','cmd':'ping'}}, ensure_ascii=False)}")
    print(f"  无 SDK, 只要能连消息队列就能接入")

    print(f"\n{'─' * 60}")
    print("  [6] 生命周期 — 状态机: created→started→running→draining→stopped")
    print(f"{'─' * 60}")
    print(f"  Agent: probe-1 / judge-1 / reporter-1")
    print(f"  由 Supervisor 统一管理: register → start → heartbeat → stop")
    print(f"  心跳超时自动重启, SIGTERM 优雅关闭")

    # 结果汇总
    bc_listener.cancel()
    try: await bc_listener
    except asyncio.CancelledError: pass

    if bc_results:
        print(f"\n{'─' * 60}")
        print("  [广播结果] aqap:broadcast 共 %d 条" % len(bc_results))
        print(f"{'─' * 60}")
        for r in bc_results:
            p = r.payload
            print(f"     {p.get('task_id','?')}  score={p.get('score','?')}  "
                  f"passed={p.get('passed','?')}  {p.get('summary','')[:30]}")

    await fast_stop(probe, judge, reporter)
    await transport.disconnect()
    await registry.cleanup_all()

    print(f"\n{'═' * 60}")
    print("  ✅ 设计全景展示完成 — 7 维度已覆盖")
    print(f"{'═' * 60}")


if __name__ == "__main__":
    asyncio.run(main())
