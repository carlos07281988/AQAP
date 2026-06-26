"""Probe Agent — 检测执行器"""
from __future__ import annotations

import logging
from typing import Any

from aqap.core.message import (
    Message,
    MessageType,
    Topic,
    task_result,
)
from aqap.agent.base import Agent

logger = logging.getLogger("aqap.agent.probe")


class ProbeAgent(Agent):
    """
    检测 Agent

    职责:
    1. 接收 TASK_DISPATCH 消息
    2. 调用绑定的插件执行检测
    3. 返回检测结果 (TASK_RESULT)
    4. 请求评判 (JUDGE_REQUEST)
    """

    def __init__(
        self,
        agent_id: str,
        transport,
        judge_target: str = "judge-1",
        **kwargs,
    ):
        super().__init__(agent_id, transport, **kwargs)
        self._judge_target = judge_target

    @property
    def agent_type(self) -> str:
        return "probe"

    async def handle_message(self, message: Message) -> list[Message] | None:
        replies = []

        if message.type == MessageType.TASK_DISPATCH:
            logger.info("[%s] 收到检测任务 trace_id=%s", self.agent_id, message.trace_id)
            # 执行检测
            result = await self._probe(message.payload)

            # 返回检测结果
            replies.append(task_result(self.agent_id, result))

            # 请求评判
            judge_msg = message.reply(
                MessageType.JUDGE_REQUEST,
                payload={"task": message.payload, "result": result},
            )
            judge_msg.target = self._judge_target
            replies.append(judge_msg)

        return replies or None

    async def _probe(self, task: dict[str, Any]) -> dict[str, Any]:
        """执行检测逻辑 — 调用插件"""
        # 运行绑定到 "probe" topic 的插件
        plugin_results = await self.run_plugins("probe", task)
        task_id = task.get("task_id", "unknown")
        passed = all(
            r.get("result", {}).get("passed", True)
            for r in plugin_results
            if r["error"] is None
        )
        logger.info("[%s] 检测完成 task_id=%s passed=%s", self.agent_id, task_id, passed)

        return {
            "task_id": task_id,
            "agent": self.agent_id,
            "plugin_results": plugin_results,
            "passed": passed,
        }
