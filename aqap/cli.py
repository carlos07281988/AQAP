#!/usr/bin/env python3
"""
AQAP CLI — 运维管理工具

用法:
  python -m aqap.cli dlq list                    # 列出死信
  python -m aqap.cli dlq replay --trace-id TRACE  # 重放死信
  python -m aqap.cli dlq replay --message-id MSG  # 按消息 ID 重放
  python -m aqap.cli dlq stats                    # 死信统计
  python -m aqap.cli trace show TRACE_ID          # 查看链路
  python -m aqap.cli trace recent [-n 20]         # 最近追踪事件
  python -m aqap.cli agents                       # 列出 Agent 状态
  python -m aqap.cli health                       # 系统健康检查
  python -m aqap.cli secret                       # 生成加密密钥
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path

logger = logging.getLogger("aqap.cli")


def _get_engine():
    """懒加载 Engine"""
    from aqap.core.engine import AQAPEngine

    engine = AQAPEngine()
    return engine


async def cmd_dlq_list(args):
    engine = _get_engine()
    await engine.start()
    dlq = engine.supervisor._agents.get("dlq-consumer")
    if not dlq:
        print("错误: DLQ Consumer 未注册")
        return
    letters = await dlq.get_dead_letters(args.limit)
    if not letters:
        print("死信队列为空")
        return
    for i, dl in enumerate(letters, 1):
        print(f"  [{i}] msg={dl['message_id'][:12]} trace={dl['trace_id'][:12]} "
              f"error={dl['error'][:40]} retries={dl['retry_count']}")
    print(f"\n共 {len(letters)} 条死信")
    await engine.stop()


async def cmd_dlq_replay(args):
    engine = _get_engine()
    await engine.start()
    dlq = engine.supervisor._agents.get("dlq-consumer")
    if not dlq:
        print("错误: DLQ Consumer 未注册")
        return
    count = await dlq.replay(
        message_id=args.message_id,
        trace_id=args.trace_id,
    )
    print(f"已重放 {count} 条死信")
    await engine.stop()


async def cmd_dlq_stats(args):
    engine = _get_engine()
    await engine.start()
    dlq = engine.supervisor._agents.get("dlq-consumer")
    if not dlq:
        print("错误: DLQ Consumer 未注册")
        return
    stats = dlq.stats
    print(json.dumps(stats, indent=2, ensure_ascii=False))
    await engine.stop()


async def cmd_trace_show(args):
    engine = _get_engine()
    await engine.start()
    from aqap.plugin.registry import registry

    tc = registry.get("trace-collector")
    if not tc:
        print("错误: TraceCollector 未注册")
        return
    events = await tc.query_trace(args.trace_id)
    if not events:
        print(f"无 trace_id={args.trace_id} 的记录")
        return
    for e in events:
        print(f"  {e['timestamp']} | {e['type']} | {e['source']} | {e['elapsed_ms']}ms")
    await engine.stop()


async def cmd_trace_recent(args):
    engine = _get_engine()
    await engine.start()
    from aqap.plugin.registry import registry

    tc = registry.get("trace-collector")
    if not tc:
        print("错误: TraceCollector 未注册")
        return
    events = await tc.query_recent(args.n)
    if not events:
        print("无追踪记录")
        return
    for e in events:
        print(f"  {e['timestamp']} | {e['type']} | {e['source']} | {e['elapsed_ms']}ms | {e['trace_id'][:12]}")
    await engine.stop()


async def cmd_agents(args):
    engine = _get_engine()
    await engine.start()
    h = await engine.health()
    agents = h.get("agents", {})
    if not agents:
        print("无 Agent 注册")
        return
    for aid, s in agents.items():
        status = s.get("status", "?")
        icon = "✅" if status == "healthy" else "❌" if status == "stale" else "❓"
        elapsed = s.get("elapsed_seconds", "?")
        print(f"  {icon} {aid}: {status} (last_heartbeat: {elapsed}s ago)")
    await engine.stop()


async def cmd_health(args):
    engine = _get_engine()
    await engine.start()
    h = await engine.health()
    print(json.dumps(h, indent=2, ensure_ascii=False, default=str))
    await engine.stop()


def cmd_secret(args):
    from aqap.core.security import generate_secret

    print(generate_secret())


def main():
    parser = argparse.ArgumentParser(
        description="AQAP CLI — 运维管理工具",
        prog="aqap",
    )
    sub = parser.add_subparsers(dest="command")

    # dlq
    dlq_parser = sub.add_parser("dlq", help="死信队列管理")
    dlq_sub = dlq_parser.add_subparsers(dest="dlq_action")
    dlq_list = dlq_sub.add_parser("list", help="列出死信")
    dlq_list.add_argument("-n", "--limit", type=int, default=50)
    dlq_replay = dlq_sub.add_parser("replay", help="重放死信")
    dlq_replay.add_argument("--trace-id", type=str)
    dlq_replay.add_argument("--message-id", type=str)
    dlq_sub.add_parser("stats", help="死信统计")

    # trace
    trace_parser = sub.add_parser("trace", help="链路追踪")
    trace_sub = trace_parser.add_subparsers(dest="trace_action")
    trace_show = trace_sub.add_parser("show", help="按 trace_id 查看")
    trace_show.add_argument("trace_id", type=str)
    trace_recent = trace_sub.add_parser("recent", help="最近追踪")
    trace_recent.add_argument("-n", type=int, default=20)

    # agents
    sub.add_parser("agents", help="Agent 状态")

    # health
    sub.add_parser("health", help="系统健康检查")

    # secret
    sub.add_parser("secret", help="生成加密密钥")

    args = parser.parse_args()

    if not args.command:
        parser.print_help()
        sys.exit(1)

    # 分发
    disp = {
        ("dlq", "list"): cmd_dlq_list,
        ("dlq", "replay"): cmd_dlq_replay,
        ("dlq", "stats"): cmd_dlq_stats,
        ("trace", "show"): cmd_trace_show,
        ("trace", "recent"): cmd_trace_recent,
        "agents": cmd_agents,
        "health": cmd_health,
        "secret": cmd_secret,
    }

    cmd = args.command
    if cmd in ("dlq", "trace"):
        cmd = (cmd, getattr(args, f"{cmd}_action", "list"))
    fn = disp.get(cmd)
    if fn:
        if asyncio.iscoroutinefunction(fn):
            asyncio.run(fn(args))
        else:
            fn(args)
    else:
        parser.print_help()


if __name__ == "__main__":
    main()
