"""Admin API — FastAPI 运维接口

提供:
  GET  /health          — 系统健康检查
  GET  /agents          — 所有 Agent 状态
  GET  /agents/{id}     — 单个 Agent 状态
  POST /agents/{id}/restart — 重启 Agent
  GET  /dlq/stats       — 死信统计
  GET  /dlq/dead-letters   — 死信列表
  POST /dlq/replay      — 重放死信
  GET  /traces/{id}     — 按 trace_id 查询消息链路
  GET  /topics          — 所有活跃 topic
  GET  /config          — 当前配置摘要
"""
from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

logger = logging.getLogger("aqap.admin")

try:
    from fastapi import FastAPI, HTTPException, Query
    from fastapi.responses import JSONResponse
    import uvicorn

    FASTAPI_AVAILABLE = True
except ImportError:
    FASTAPI_AVAILABLE = False

app = FastAPI(
    title="AQAP Admin API",
    version="1.0.0",
    description="Agent Queue Agent Communication Protocol — 运维管理接口",
)

# Engine 引用 (由 serve_admin 注入)
_engine: Any = None


def set_engine(engine):
    """注入 AQAPEngine 实例"""
    global _engine
    _engine = engine


# ── 健康检查 ──


@app.get("/health")
async def health():
    """系统健康概览"""
    if not _engine:
        return {"status": "not_initialized"}
    try:
        h = await _engine.health()
        return {"status": "ok", **h}
    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={"status": "error", "message": str(e)},
        )


# ── Agent 管理 ──


@app.get("/agents")
async def list_agents():
    """所有 Agent 及其健康状态"""
    if not _engine:
        return {"agents": {}, "total": 0}
    h = await _engine.health()
    return h.get("agents", h)


@app.get("/agents/{agent_id}")
async def get_agent(agent_id: str):
    """单个 Agent 状态"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    h = await _engine.health()
    agents = h.get("agents", {})
    if agent_id not in agents:
        raise HTTPException(404, f"Agent '{agent_id}' 不存在")
    return agents[agent_id]


@app.post("/agents/{agent_id}/restart")
async def restart_agent(agent_id: str):
    """重启指定 Agent"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    if agent_id not in _engine.supervisor._agents:
        raise HTTPException(404, f"Agent '{agent_id}' 不存在")
    await _engine.supervisor.restart_agent(agent_id)
    return {"status": "restarted", "agent_id": agent_id}


# ── 死信管理 ──


@app.get("/dlq/stats")
async def dlq_stats():
    """死信队列统计"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    dlq_consumer = _engine.supervisor._agents.get("dlq-consumer")
    if not dlq_consumer:
        return {"error": "DLQ Consumer 未注册"}
    return dlq_consumer.stats


@app.get("/dlq/dead-letters")
async def dlq_list(limit: int = Query(50, ge=1, le=500)):
    """死信列表 (最近优先)"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    dlq_consumer = _engine.supervisor._agents.get("dlq-consumer")
    if not dlq_consumer:
        return {"error": "DLQ Consumer 未注册", "dead_letters": []}
    return {"dead_letters": await dlq_consumer.get_dead_letters(limit)}


@app.post("/dlq/replay")
async def dlq_replay(
    message_id: str | None = None,
    trace_id: str | None = None,
):
    """重放死信消息"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    if not message_id and not trace_id:
        raise HTTPException(400, "至少需要 message_id 或 trace_id")
    dlq_consumer = _engine.supervisor._agents.get("dlq-consumer")
    if not dlq_consumer:
        raise HTTPException(503, "DLQ Consumer 未注册")
    count = await dlq_consumer.replay(
        message_id=message_id,
        trace_id=trace_id,
    )
    return {"status": "replayed", "count": count}


# ── 链路追踪 ──


@app.get("/traces/{trace_id}")
async def get_trace(trace_id: str):
    """按 trace_id 查询消息链路"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    # 从 TraceCollector 查询
    from aqap.plugin.registry import registry

    tc = registry.get("trace-collector")
    if not tc:
        return {"trace_id": trace_id, "events": [], "note": "TraceCollector 未注册"}
    events = await tc.query_trace(trace_id)
    return {"trace_id": trace_id, "events": events}


# ── 运维信息 ──


@app.get("/topics")
async def list_topics():
    """所有活跃 topic"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    transport = _engine.transport
    topics = set()
    for agent in _engine.supervisor._agents.values():
        for t in agent._topics:
            topics.add(str(t))
    return {
        "topics": sorted(topics),
        "transport_backend": transport.name if transport else "unknown",
    }


@app.get("/config")
async def config_summary():
    """当前配置摘要 (不含密钥)"""
    if not _engine:
        raise HTTPException(503, "Engine 未初始化")
    cfg = _engine.config
    return {
        "app": cfg.get("app", default={}),
        "transport": {"backend": cfg.get("transport", "backend")},
        "security_enabled": cfg.get("security", "enabled", default=False),
        "agent_count": len(cfg.get("agents", default={})),
        "plugin_count": len(cfg.get("plugins", default={})),
    }


def serve_admin(engine, host: str = "0.0.0.0", port: int = 8080):
    """启动 Admin API 服务器 (阻塞调用)"""
    if not FASTAPI_AVAILABLE:
        raise ImportError("Admin API 需要 fastapi + uvicorn: pip install fastapi uvicorn")
    set_engine(engine)
    logger.info("[admin] Admin API 启动于 http://%s:%d", host, port)
    uvicorn.run(app, host=host, port=port, log_level="info")
