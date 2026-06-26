# AQA 架构总览

## 一、整体架构图

```
┌──────────────────────────────────────────────────────────────────────┐
│                         配置层 (config.yaml)                          │
│  ┌──────────┐  ┌───────────┐  ┌────────┐  ┌──────────┐  ┌────────┐ │
│  │ transport│  │  security │  │ agents │  │ plugins  │  │supervis.│ │
│  └─────┬────┘  └─────┬─────┘  └───┬────┘  └────┬─────┘  └────┬───┘ │
└────────┼──────────────┼────────────┼────────────┼──────────────┼─────┘
         │              │            │            │              │
┌────────▼──────────────▼────────────▼────────────▼──────────────▼─────┐
│                         AQA Engine                                    │
│                                                                       │
│  1. _discover_transports()  — 加载 Transport 实现                     │
│  2. _init_security()        — 初始化 Payload 加密                     │
│  3. _init_transport()       — 创建 Transport (Redis/Kafka/内存)       │
│  4. _init_plugins()         — 注册插件                                │
│  5. _init_agents()          — 创建 Agent 实例                         │
│  6. Supervisor.start_all()  — 启动所有 Agent                          │
│  7. wait_until_shutdown()   — 安装信号处理器, 等待退出                │
└───────┬───────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│                         Transport 层                                  │
│  ┌─────────────────┐  ┌──────────────────┐  ┌──────────────────────┐ │
│  │ InMemoryTransport│  │ RedisStreamsTrans.│  │ KafkaTransport      │ │
│  │ (测试/演示)      │  │ (生产)            │  │ (高吞吐)             │ │
│  └────────┬────────┘  └───────┬──────────┘  └──────────┬───────────┘ │
│           │                  │                        │              │
│           └──────────────────┴────────────────────────┘              │
│                          publish / subscribe / ack                    │
└───────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│                        Agent 层                                       │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────┐        │
│  │                    AgentSupervisor                        │        │
│  │  ┌──────────┐    ┌──────────┐    ┌────────────┐         │        │
│  │  │ Probe-1  │    │ Judge-1  │    │ Reporter-1 │         │        │
│  │  │(检测执行) │    │(评判裁决) │    │(报告生成)  │         │        │
│  │  └─────┬────┘    └────┬─────┘    └─────┬──────┘         │        │
│  │        │               │                │                │        │
│  │  ┌─────┴────┐   ┌─────┴─────┐   ┌─────┴──────┐         │        │
│  │  │ 插件链  │   │  插件链   │   │  插件链    │         │        │
│  │  │ validator │   │  validator │   │ trace_col. │         │        │
│  │  │ scorer    │   │  scorer    │   │            │         │        │
│  │  │ trace_col.│   │  trace_col.│   │            │         │        │
│  │  └──────────┘   └────────────┘   └────────────┘         │        │
│  └──────────────────────────────────────────────────────────┘        │
└───────────────────────────────────────────────────────────────────────┘
        │
        ▼
┌───────────────────────────────────────────────────────────────────────┐
│                       插件层                                          │
│  ┌────────────────┐  ┌──────────────┐  ┌─────────────────────────┐  │
│  │ Plugin 基类    │  │ PluginRegistry │  │ 内置插件               │  │
│  │ name, version  │  │ register()    │  │  - ValidatorPlugin     │  │
│  │ initialize()   │  │ unregister()  │  │  - ScorerPlugin        │  │
│  │ execute()      │  │ execute_all() │  │  - TraceCollector      │  │
│  │ cleanup()      │  │ list()        │  │  - DemoCheckPlugin     │  │
│  └────────────────┘  └──────────────┘  └─────────────────────────┘  │
└───────────────────────────────────────────────────────────────────────┘
```

## 二、消息流转路径

### 标准检测流程

```
Scheduler/CLI                Probe Agent              Judge Agent            Reporter Agent
     │                           │                        │                      │
     │  TASK_DISPATCH             │                        │                      │
     │──────────────────────────► │                        │                      │
     │                           │                        │                      │
     │                     ┌─────┴─────┐                  │                      │
     │                     │ 执行插件   │                  │                      │
     │                     │ validator  │                  │                      │
     │                     │ trace_col. │                  │                      │
     │                     └─────┬─────┘                  │                      │
     │                           │                        │                      │
     │                     TASK_RESULT                    │                      │
     │                     JUDGE_REQUEST                  │                      │
     │                           │───────────────────────►│                      │
     │                           │                        │                      │
     │                           │                  ┌─────┴─────┐                │
     │                           │                  │ 执行插件   │                │
     │                           │                  │ scorer     │                │
     │                           │                  │ validator  │                │
     │                           │                  │ trace_col. │                │
     │                           │                  └─────┬─────┘                │
     │                           │                        │                      │
     │                           │                  JUDGE_VERDICT               │
     │                           │                  REPORT_REQUEST              │
     │                           │                        │────────────────────►│
     │                           │                        │                      │
     │                           │                        │                ┌────┴────┐
     │                           │                        │                │reporter │
     │                           │                        │                │ plugins │
     │                           │                        │                └────┬────┘
     │                           │                        │                      │
     │                           │                  REPORT_DELIVER              │
     │◄────────────────────────────────────────────────────────────────────────│
```

### Topic 路由映射

| 出发 Agent | 消息类型 | 目标 Topic |
|---|---|---|
| CLI/Scheduler | TASK_DISPATCH | `aqa:agent:probe` |
| Probe Agent | TASK_RESULT | `aqa:agent:judge` |
| Probe Agent | JUDGE_REQUEST | `aqa:inbox:judge-1` (定向) |
| Judge Agent | JUDGE_VERDICT | `aqa:agent:reporter` |
| Judge Agent | REPORT_REQUEST | `aqa:inbox:reporter-1` (定向) |
| Reporter Agent | REPORT_DELIVER | `aqa:inbox:{source}` (定向回传) + `aqa:broadcast` |

## 三、模块依赖关系

```
aqa/
├── core/                      ← 内核层 (零依赖)
│   ├── message.py             → 无 (仅标准库)
│   ├── config.py              → 无 (仅标准库)
│   ├── engine.py              → agent/*, transport/base, plugin/registry, core/*
│   ├── dlq.py                 → core/message
│   └── security.py            → cryptography (可选)
│
├── transport/                 ← 传输层 (依赖 core/message)
│   ├── base.py                → core/message
│   ├── inmemory.py            → core/message, transport/base
│   ├── redis_streams.py       → core/message, transport/base, redis (可选)
│   └── kafka_transport.py     → core/message, transport/base, aiokafka (可选)
│
├── agent/                     ← Agent 层 (依赖 transport + core + plugin)
│   ├── base.py                → core/*, transport/base, plugin/registry
│   ├── probe.py               → agent/base, core/message
│   ├── judge.py               → agent/base, core/message
│   ├── reporter.py            → agent/base, core/message
│   └── supervisor.py          → agent/base
│
├── plugin/                    ← 插件层 (依赖 core/message)
│   ├── base.py                → 无 (仅标准库)
│   └── registry.py            → plugin/base
│
└── plugins/                   ← 内置插件 (依赖 plugin/base)
    ├── validator.py           → plugin/base
    ├── scorer.py              → plugin/base
    └── trace_collector.py     → plugin/base
```

### 依赖方向规则

1. **core → 无** — 内核层不能依赖任何其他 AQA 模块
2. **transport → core** — 传输层可以依赖 core/message
3. **agent → transport + core + plugin** — Agent 层聚合所有下层
4. **plugin → 无** — 插件基类不依赖任何 AQA 模块
5. **内置插件 → plugin/base** — 实现类只依赖 Plugin ABC

## 四、配置体系

`config.yaml` 是唯一入口，共有 6 个配置段：

| 配置段 | 职责 | 示例 |
|---|---|---|
| `app` | 应用标识 | `name`, `version`, `debug` |
| `transport` | 队列后端 | `backend: redis-streams | kafka | in-memory` |
| `security` | Payload 加密 | `enabled: bool`, `secret: string` |
| `agents` | Agent 定义 | `{agent_id}: {type, group, topics, targets}` |
| `plugins` | 插件注册 | `{name}: {class, enabled, topic_bind, config}` |
| `supervisor` | 生命周期管理 | `heartbeat_timeout: int` |

详见 [docs/CONFIG_REFERENCE.md](CONFIG_REFERENCE.md)（配置参考）或直接阅读 `config.yaml`。

## 五、全链路追踪

每一条任务链共享同一个 `trace_id`：

```
TASK_DISPATCH  →  trace_id = "trace_x001"  ← 入口生成
TASK_RESULT    →  trace_id = "trace_x001"  ← 透传
JUDGE_VERDICT  →  trace_id = "trace_x001"  ← 透传
REPORT_DELIVER →  trace_id = "trace_x001"  ← 透传
```

`correlation_id` 关联消息级回复：

```
消息 A (message_id="msg_a1")
  → 消息 B (correlation_id="msg_a1")  ← B 是 A 的回复
```

## 六、代码行数统计

| 模块 | 文件数 | 行数 | 占比 |
|---|---|---|---|
| `aqa/core/` | 5 | 827 | 22% |
| `aqa/agent/` | 5 | 692 | 18% |
| `aqa/transport/` | 4 | 391 | 10% |
| `aqa/plugin/` | 2 | 142 | 4% |
| `aqa/plugins/` | 3 | 204 | 5% |
| `sdk/` | 5 | 899 | 24% |
| `tests/` | 2 | 613 | 16% |
| 其他 | 3 | 130 | 3% |
| **总计** | **29** | **~3776** | **100%** |
