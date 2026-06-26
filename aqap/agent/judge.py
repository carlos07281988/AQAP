"""Judge Agent — 评判裁决器"""
from __future__ import annotations

import logging
from typing import Any

from aqap.core.message import Message, MessageType, judge_verdict
from aqap.agent.base import Agent

logger = logging.getLogger("aqap.agent.judge")


class JudgeAgent(Agent):
    """
    评判 Agent

    职责:
    1. 接收 JUDGE_REQUEST
    2. 调用评分/验证插件
    3. 给出裁决 (JUDGE_VERDICT)
    4. 触发报告生成 (REPORT_REQUEST)
    """

    def __init__(
        self,
        agent_id: str,
        transport,
        reporter_target: str = "reporter-1",
        **kwargs,
    ):
        super().__init__(agent_id, transport, **kwargs)
        self._reporter_target = reporter_target

    @property
    def agent_type(self) -> str:
        return "judge"

    async def handle_message(self, message: Message) -> list[Message] | None:
        replies = []

        if message.type == MessageType.JUDGE_REQUEST:
            logger.info("[%s] 收到评判请求 trace_id=%s", self.agent_id, message.trace_id)
            evidence = message.payload
            verdict = await self._judge(evidence)

            verdict_msg = judge_verdict(self.agent_id, verdict)
            verdict_msg.trace_id = message.trace_id
            verdict_msg.correlation_id = message.correlation_id

            # 发送裁决结果
            replies.append(verdict_msg)

            # 触发报告生成
            report_msg = message.reply(
                MessageType.REPORT_REQUEST,
                payload={
                    "task": evidence.get("task", {}),
                    "result": evidence.get("result", {}),
                    "verdict": verdict,
                },
            )
            report_msg.target = self._reporter_target
            replies.append(report_msg)

        return replies or None

    async def _judge(self, evidence: dict[str, Any]) -> dict[str, Any]:
        """评判逻辑 — 调用评分/验证插件"""
        plugin_results = await self.run_plugins("judge", evidence)

        scores = []
        passed = True
        for pr in plugin_results:
            if pr["error"] is None:
                r = pr["result"]
                score = r.get("score", 0.5)
                scores.append(score)
                if not r.get("passed", True):
                    passed = False
            else:
                scores.append(0.0)
                passed = False

        avg_score = sum(scores) / len(scores) if scores else 0.0
        task_id = evidence.get("task", {}).get("task_id", "unknown")
        logger.info("[%s] 评判完成 task_id=%s score=%s passed=%s", self.agent_id, task_id, round(avg_score, 4), passed)

        return {
            "task_id": task_id,
            "agent": self.agent_id,
            "score": round(avg_score, 4),
            "passed": passed,
            "details": plugin_results,
        }
