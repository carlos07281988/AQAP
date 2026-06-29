"""CronScheduler Agent — 定时任务调度器

按 cron 表达式或固定间隔向指定 topic 发布 TASK_DISPATCH 消息。
支持 config.yaml 中配置定时任务列表。
"""
from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone
from typing import Any

from aqap.core.message import (
    Message,
    MessageType,
    Topic,
    task_dispatch,
)
from aqap.agent.base import Agent

logger = logging.getLogger("aqap.agent.scheduler")


class CronSchedule:
    """简易 cron 解析 (分钟级精度)

    支持:
      - `"*/5"` — 每 N 分钟
      - `"@hourly"` — 每小时
      - `"@daily"` — 每天
      - `"*/1h"` — 每 N 小时
      - `"*/30s"` — 每 N 秒 (用于测试)
      - ISO-8601 时间戳 (一次性)
    """

    def __init__(self, expr: str):
        self.expr = expr.strip()

    def next_seconds(self) -> float:
        """返回距离下次触发的秒数"""
        if self.expr.startswith("*/") and self.expr.endswith("s"):
            return int(self.expr[2:-1])
        if self.expr.startswith("*/") and self.expr.endswith("m"):
            return int(self.expr[2:-1]) * 60
        if self.expr.startswith("*/") and self.expr.endswith("h"):
            return int(self.expr[2:-1]) * 3600
        if self.expr.startswith("*/"):
            return int(self.expr[2:]) * 60  # 默认分钟
        if self.expr == "@hourly":
            return 3600
        if self.expr == "@daily":
            return 86400
        if self.expr.isdigit():
            return int(self.expr)
        return 60  # 默认每分钟


class SchedulerAgent(Agent):
    """定时调度 Agent

    配置示例 (config.yaml):
      agents:
        scheduler-1:
          type: scheduler
          schedule:
            - cron: "*/5m"
              topic: "aqap:agent:probe"
              payload:
                task_id: "scheduled-check"
                name: "定时质量检测"
                required_fields: ["task_id", "passed"]
    """

    def __init__(
        self,
        agent_id: str = "scheduler-1",
        transport=None,
        schedule: list[dict] | None = None,
        **kwargs,
    ):
        super().__init__(agent_id, transport, **kwargs)
        self._schedules: list[CronSchedule] = []
        self._topics: list[str] = []
        self._payloads: list[dict] = []
        self._last_runs: list[float] = []

        if schedule:
            for entry in schedule:
                self._schedules.append(CronSchedule(entry.get("cron", "*/5m")))
                self._topics.append(entry.get("topic", Topic.AGENT_PROBE))
                self._payloads.append(entry.get("payload", {}))

        if not self._schedules:
            self._schedules.append(CronSchedule("*/5m"))
            self._topics.append(Topic.AGENT_PROBE)
            self._payloads.append({"task_id": "scheduled-001", "name": "定时检测"})

    @property
    def agent_type(self) -> str:
        return "scheduler"

    async def on_start(self) -> None:
        self._last_runs = [0.0] * len(self._schedules)
        logger.info("[scheduler] 已启动, %d 个定时任务", len(self._schedules))

    async def handle_message(self, message: Message) -> list[Message] | None:
        """调度器不处理入站消息"""
        return None

    async def start(self):
        """启动调度器 + 调度循环"""
        await super().start()
        self._tasks.append(asyncio.create_task(self._schedule_loop()))

    async def _schedule_loop(self):
        """调度循环"""
        await asyncio.sleep(0.5)  # 等所有 Agent 就绪
        while self._running:
            now = time.time()
            for i, schedule in enumerate(self._schedules):
                interval = schedule.next_seconds()
                if now - self._last_runs[i] >= interval:
                    payload = dict(self._payloads[i])
                    if "task_id" not in payload:
                        payload["task_id"] = f"sched-{i}-{int(now)}"
                    payload["scheduled_at"] = datetime.now(timezone.utc).isoformat()

                    msg = task_dispatch(self.agent_id, payload)
                    msg.trace_id = f"sched-{self.agent_id}-{i}-{int(now)}"
                    await self._transport.publish(self._topics[i], msg)

                    logger.info(
                        "[scheduler] 触发定时任务: %s → %s (cron=%s)",
                        payload.get("task_id"),
                        self._topics[i],
                        self._schedules[i].expr,
                    )
                    self._last_runs[i] = now
            await asyncio.sleep(1)
