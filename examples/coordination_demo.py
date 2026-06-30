#!/usr/bin/env python3
"""
AQAP 极致传输·极致对接 — payload 承载任意 JSON
"""
from __future__ import annotations

import asyncio
import random

from aqap.core.message import Message, MessageType, Topic
from aqap.transport.inmemory import InMemoryTransport
from aqap.agent.base import Agent


class Worker(Agent):
    @property
    def agent_type(self) -> str:
        return "worker"

    async def handle_message(self, message: Message) -> list[Message] | None:
        if message.type != MessageType.TASK_DISPATCH:
            return None
        # 模拟处理: 在原数据上加个 result
        processed = dict(message.payload)
        processed["result"] = {
            "status": "ok",
            "score": round(random.uniform(0.8, 1.0), 3),
        }
        return [message.reply(MessageType.TASK_RESULT, processed)]


class Coordinator(Agent):
    @property
    def agent_type(self) -> str:
        return "coordinator"

    async def handle_message(self, message: Message) -> list[Message] | None:
        if message.type == MessageType.TASK_DISPATCH:
            return [Message(MessageType.TASK_DISPATCH, self.agent_id,
                            message.payload, target="worker-1",
                            trace_id=message.trace_id)]
        if message.type == MessageType.TASK_RESULT:
            await self.send(Message(MessageType.REPORT_DELIVER, self.agent_id,
                                    {"result": message.payload},
                                    trace_id=message.trace_id))
        return None


async def main():
    transport = InMemoryTransport()
    coord = Coordinator("coord-1", transport)
    worker = Worker("worker-1", transport)
    coord.subscribe_to("cmd:job")
    coord.subscribe_to(Topic.agent_inbox("coord-1"))
    worker.subscribe_to(Topic.agent_inbox("worker-1"))

    await asyncio.gather(coord.start(), worker.start())
    await asyncio.sleep(0.05)

    print("═" * 60)
    print("  AQAP  ·  payload 承载任意 JSON")
    print("═" * 60)

    results = []

    async def listen_broadcast():
        async for msg in transport.subscribe("aqap:broadcast", consumer="cli"):
            if msg.type == MessageType.REPORT_DELIVER:
                results.append(msg)

    listener = asyncio.create_task(listen_broadcast())
    await asyncio.sleep(0.02)

    # payload 里放真正的业务数据
    task = {
        "task_id": "t1",
        "cmd": "audit",
        "content": {
            "user": "alice",
            "action": "deploy",
            "target": "prod-us-east",
            "version": "v2.1.3",
            "checksum": "a1b2c3d4",
            "files": ["app.py", "config.yaml", "schema.sql"],
            "metadata": {
                "env": "production",
                "region": "us-east-1",
                "deployed_by": "ci-bot",
                "timestamp": "2026-06-30T10:00:00Z",
            },
        },
    }

    print("\n  ▶ 外部系统发送 (payload 含嵌套 content):")
    msg = Message(MessageType.TASK_DISPATCH, "external-client", task, topic="cmd:job")
    await transport.publish("cmd:job", msg)
    import json
    print(json.dumps(msg.payload, indent=2, ensure_ascii=False))

    await asyncio.sleep(0.3)
    listener.cancel()
    try: await listener
    except asyncio.CancelledError: pass

    print("\n  ▶ 外部系统收到 (payload 保留原数据 + Worker 追加 result):")
    for r in results:
        print(json.dumps(r.payload["result"], indent=2, ensure_ascii=False))

    for a in [coord, worker]:
        a._running = False
        for t in a._tasks:
            t.cancel()
        if a._tasks:
            await asyncio.gather(*a._tasks, return_exceptions=True)
        a._tasks.clear()
    await transport.disconnect()

    print(f"\n  ✅ payload 任意 JSON · 透传不丢字段\n")


if __name__ == "__main__":
    asyncio.run(main())
