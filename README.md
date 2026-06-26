# AQAP — Agent Queue Agent Communication Protocol

基于消息队列的 Agent 间通信协议与质量保障系统。

**队列即协议** — Agent 之间没有直接引用、没有函数调用、没有 HTTP 端点。
通信的唯一原语是 **往某个 topic 发一条消息**，别的 Agent 从 topic 上消费。

---

## 一、设计哲学

```
┌────────────────────────────────────────────────────────────────────┐
│                         设计原则                                    │
│                                                                    │
│  1. 队列即协议 — MQ 就是通信契约, 不是 RPC 的传输层                │
│  2. 信封标准化 — 所有消息共享同一个 JSON 信封结构                  │
│  3. 版本化演进 — 协议版本字段保证多版本 Agent 共存                │
│  4. 全链路追踪 — trace_id 贯穿所有消息, 无死角                    │
│  5. 可插拔后端 — Transport 层屏蔽队列实现 (Redis/Kafka/内存)      │
│  6. 外部友好  — 任何语言只要拼 JSON 就能接入                      │
└────────────────────────────────────────────────────────────────────┘
```

### 为什么不用 HTTP/RPC 做 Agent 间通信

| 对比项 | HTTP/RPC | 消息队列 (本协议) |
|---|---|---|
| 耦合度 | 调用方依赖被调方的地址+接口定义 | 零依赖, 只认 topic |
| 背压 | 调用方需要自己处理超时/重试 | 队列自带 backpressure |
| 故障隔离 | 级联失败 (A 挂了→B 重试→C 雪崩) | 消费者故障不影响生产者 |
| 多语言 | 每个语言写一套 client stub | 拼 JSON 即可 |
| 热升级 | 需要服务发现 + 负载均衡 | 停消费者→升级→启动, 消息不丢 |
| 可观测 | 需要额外埋点 | 消息本身带 trace_id |

---

## 二、消息信封 (Message Envelope)

这是系统中**所有**消息的固定 JSON 结构。无论是内部 Python Agent 还是外部 Go/JS 程序, 发送的消息都必须是这个格式。

```json
{
  "type":           "TASK_DISPATCH",
  "message_id":     "a1b2c3d4e5f6g7h8",
  "source":         "cli-orchestrator",
  "target":         "",
  "topic":          "aqa:agent:probe",
  "trace_id":       "trace_dcf9a2b1",
  "correlation_id": "",
  "version":        "1.0",
  "payload":        { "task_id": "t-001", "target_svc": "svc-a" },
  "timestamp":      "2026-06-25T10:30:00+00:00"
}
```

协议规范见 **[PROTOCOL.md](PROTOCOL.md)**（JSON Schema、状态机、路由规则），以该文件为准。

---

## 三、消息类型 (Message Types)

| 类型 | 语义 | 方向 | 典型 payload |
|---|---|---|---|
| `HEARTBEAT` | 心跳 | Agent → 广播 | `{"cpu": 0.3, "mem": 512, "status": "healthy"}` |
| `TASK_DISPATCH` | 下发检测任务 | 调度器 → Probe | `{"task_id": "t-001", "target": "svc-a", "required_fields": [...]}` |
| `TASK_RESULT` | 检测结果 | Probe → Judge | `{"task_id": "t-001", "passed": true, "score": 0.95}` |
| `JUDGE_REQUEST` | 请求评判 | Probe → Judge | `{"task_id": "t-001", "task": {...}, "result": {...}}` |
| `JUDGE_VERDICT` | 评判裁决 | Judge → Reporter | `{"task_id": "t-001", "score": 92, "passed": true, "details": [...]}` |
| `REPORT_REQUEST` | 请求报告 | Judge → Reporter | `{"task_id": "t-001", "task": {...}, "result": {...}, "verdict": {...}}` |
| `REPORT_DELIVER` | 报告投递 | Reporter → CLI/broadcast | `{"task_id": "t-001", "title": "...", "score": 0.92, "summary": "..."}` |
| `ERROR` | 系统错误 | 任意 Agent | `{"code": "TIMEOUT", "message": "probe-1 超时", "trace_id": "..."}` |
| `REGISTER` | Agent 注册 | Agent → 系统事件 | `{"agent_type": "probe"}` |
| `SHUTDOWN` | Agent 下线 | Agent → 系统事件 | `{}` |

### 类型扩展规则

1. 类型名使用 `SCREAMING_SNAKE_CASE`, 长度不超过 32 字符
2. 新增类型必须在本 README 中登记
3. 自定义类型前缀建议加模块名, 如 `PLUGIN_CUSTOM_CHECK`
4. 收到不识别的类型, Agent **必须**通过 `ERROR` 消息报告, 不能静默丢弃

---

## 四、Topic 系统

Topic 是消息路由的唯一依据。Agent 不关心"发给谁", 只关心"投到哪个 topic"和"订阅哪个 topic"。

### 内置 Topic

| Topic | 用途 | 消费者 |
|---|---|---|
| `aqa:broadcast` | 全局广播通道 (心跳 / 系统通知) | 所有 Agent |
| `aqa:agent:probe` | 检测任务分发 | Probe Agent |
| `aqa:agent:judge` | 评判裁决 | Judge Agent |
| `aqa:agent:reporter` | 报告生成 | Reporter Agent |
| `aqa:inbox:{agent_id}` | Agent 私有收件箱 (定向消息) | 指定 Agent |
| `aqa:system:events` | 系统事件 (注册/下线) | 监控组件 |

### Topic 命名规范

```
aqa:{scope}:{name}
  │      │       └─ 具体名称, 小写字母 + 连字符
  │      └───────── 作用域 (agent / inbox / broadcast / system)
  └──────────────── 系统保留前缀
```

---

## 五、全链路追踪 (Trace & Correlation)

### trace_id — 任务链标识

```
初始消息                     trace_id = "trace_a1b2"
  TASK_DISPATCH ────────► aqa:agent:probe
                               │
                          trace_id 保持不变 ──── 透传
                               │
  TASK_RESULT    ◄──────── aqa:agent:probe
    trace_id = "trace_a1b2"
                               │
  JUDGE_VERDICT  ◄──────── aqa:agent:judge
    trace_id = "trace_a1b2"
```

**规则**: 一条任务链上所有的消息共享同一个 `trace_id`。每个 Agent 在处理消息后继续往下游发消息时,**必须透传原始的 `trace_id`**, 不能重新生成。

### correlation_id — 消息级回覆

```
Agent A 发送 message_id="msg_001"
                    │
                    ▼
Agent B 处理完回复 message_id="msg_002", correlation_id="msg_001"
                                              │
                                              └── Agent A 通过 correlation_id
                                                  知道这是 msg_001 的回复
```

**规则**: 当消息是对另一条消息的回复时, `correlation_id` 填原始消息的 `message_id`。非回复消息的 `correlation_id` 为空字符串。

---

## 六、数据流

```
Scheduler/CLI                Probe Agent              Judge Agent            Reporter Agent
     │                           │                        │                      │
     │  TASK_DISPATCH             │                        │                      │
     │──────────────────────────► │                        │                      │
     │                           │                        │                      │
     │                     ┌─────┴─────┐                  │                      │
     │                     │ 执行插件   │                  │                      │
     │                     └─────┬─────┘                  │                      │
     │                           │                        │                      │
     │                     TASK_RESULT                    │                      │
     │                     JUDGE_REQUEST                  │                      │
     │                           │───────────────────────►│                      │
     │                           │                        │                      │
     │                           │                  ┌─────┴─────┐                │
     │                           │                  │ 执行评分   │                │
     │                           │                  └─────┬─────┘                │
     │                           │                        │                      │
     │                           │                  JUDGE_VERDICT               │
     │                           │                  REPORT_REQUEST              │
     │                           │                        │────────────────────►│
     │                           │                        │                      │
     │                           │                  REPORT_DELIVER              │
     │◄────────────────────────────────────────────────────────────────────────│
```

---

## 七、外部 Agent 接入

外部 Agent (Go / JS / Java / Rust / 任意语言) 接入的核心原则:

> **不需要 AQA SDK, 不需要 AQA 内核, 不需要 Python 环境。**
> 只需要能连接队列, 会拼 JSON。

SDK 实现见 `sdk/` 目录:
- `aqa_sdk/message.py` — Python 端消息构造/解析
- `aqa_sdk/consumer.py` — 自动 ACK 的消费循环
- `aqa_sdk/bridge/` — HTTPBridge 实现
- `examples/go_agent.go` — Go 语言外部 Agent 完整示例
- `examples/js_agent.mjs` — JS/Node.js 外部 Agent 完整示例

---

## 八、快速开始

```bash
# 1. 安装
cd AQA
python3 -m venv venv
./venv/bin/pip install -r requirements.txt

# 2. 运行测试 (37 项, 无需外部依赖)
PYTHONPATH=. ./venv/bin/python -m pytest tests/ sdk/tests/ -v

# 3. 运行 InMemory Demo (无需 Redis)
PYTHONPATH=. ./venv/bin/python examples/demo.py
# 日志输出格式: [2026-06-27 10:30:00] [aqa.agent.probe] [INFO] ...

# 4. 调整日志级别 (修改 config.yaml)
# logging:
#   level: "DEBUG"    # 开发调试; "INFO"=生产

# 5. 使用 Engine 启动全部 Agent (配置驱动)
PYTHONPATH=. ./venv/bin/python -m aqa.run

# 6. 使用 Docker Compose (Redis + AQA)
docker compose up -d
docker compose logs -f
```

---

## 九、项目结构

```
AQA/
├── PROTOCOL.md              # ★ 通信协议规范 (JSON Schema / 状态机 / 路由规则)
├── CHANGELOG.md             # 历次修改记录
├── README.md                # 本文件
├── docs/                    # 设计文档目录
│   ├── ARCHITECTURE.md      # 架构总览
│   ├── AGENT_SYSTEM.md      # Agent 子系统详解
│   ├── TRANSPORT.md         # Transport 层详解
│   ├── PLUGIN_SYSTEM.md     # 插件系统详解
│   ├── TESTING.md           # 测试策略
│   └── CONFIG_REFERENCE.md  # 配置参考
│
├── aqa/                     # AQA 内核
│   ├── core/
│   │   ├── message.py       # 消息协议实现 (遵循 PROTOCOL.md)
│   │   ├── engine.py        # 配置驱动运行时引擎
│   │   ├── dlq.py           # 死信队列
│   │   ├── log_config.py    # 日志初始化 (从 config.yaml 读取级别)
│   │   ├── config.py        # 配置加载
│   │   └── security.py      # Payload AES 加密
│   ├── transport/
│   │   ├── base.py          # Transport ABC
│   │   ├── inmemory.py      # InMemory 实现 (测试/演示)
│   │   ├── redis_streams.py # Redis Streams 实现 (生产)
│   │   └── kafka_transport.py   # Kafka 实现 (高吞吐)
│   ├── plugin/
│   │   ├── base.py          # Plugin ABC
│   │   └── registry.py      # 插件注册中心
│   ├── agent/
│   │   ├── base.py          # Agent 基类 (心跳/重试/DLQ/幂等去重/优雅关闭)
│   │   ├── supervisor.py    # Agent 生命周期总管
│   │   ├── probe.py         # 检测 Agent
│   │   ├── judge.py         # 评判 Agent
│   │   └── reporter.py      # 报告 Agent
│   └── plugins/
│       ├── validator.py     # 字段校验插件
│       ├── scorer.py        # 加权评分插件
│       └── trace_collector.py # 链路追踪插件
│
├── sdk/                     # 外部 Agent SDK
│   ├── aqa_sdk/             #   Python SDK (独立包)
│   ├── README.md            #   协议文档 + 多语言示例
│   └── examples/
│       ├── go_agent.go      #   Go 外部 Agent 示例
│       └── js_agent.mjs     #   JS 外部 Agent 示例
│
├── tests/                   # 核心测试
│   └── test_aqa.py          # 30 项测试
├── examples/
│   └── demo.py              # InMemory Demo
├── Dockerfile
├── docker-compose.yml
└── config.yaml              # 主配置 (完整版)
```

---

## 十、文档索引

| 文档 | 内容 | 受众 |
|---|---|---|
| **[PROTOCOL.md](PROTOCOL.md)** | 消息协议 JSON Schema、状态机、路由规则（唯一权威线格式规范） | 所有 Agent 开发者 |
| **[CHANGELOG.md](CHANGELOG.md)** | 历次代码修改记录，按时间倒序 | 维护者 |
| **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** | 系统架构总览：模块依赖、数据流、配置体系 | 新开发者入门 |
| **[docs/AGENT_SYSTEM.md](docs/AGENT_SYSTEM.md)** | Agent 基类、Probe/Judge/Reporter、Supervisor 详解 | Agent 开发者 |
| **[docs/TRANSPORT.md](docs/TRANSPORT.md)** | Transport 抽象、InMemory/Redis/Kafka 实现细节 | Transport 实现者 |
| **[docs/PLUGIN_SYSTEM.md](docs/PLUGIN_SYSTEM.md)** | Plugin 基类、Registry、内置插件、自定义指南 | 插件开发者 |
| **[docs/TESTING.md](docs/TESTING.md)** | 测试策略、运行方式、添加测试指南 | 测试者 |
| **[docs/CONFIG_REFERENCE.md](docs/CONFIG_REFERENCE.md)** | config.yaml 配置参考 | 运维/部署 |

---

## 十一、版本与兼容性

```
消息版本: version: "1.0"                       ← 协议版本
系统版本: app.version: "1.0.0" in config.yaml   ← 部署版本
```

- `version` 字段只在大版本不兼容时升级 (比如字段重命名、类型定义变更)
- 新增字段或新增消息类型不算不兼容变更, `version` 可保持不变
- 系统支持多个版本的 `version` 同时存在, Agent 按 `version` 字段路由到对应的处理逻辑

### 降级策略

Agent 处理消息时:
1. 检查 `version` — 如果不支持, 发布 `ERROR` 消息
2. 检查 `type` — 如果不识别, 发布 `ERROR` 消息
3. 检查 `trace_id` — 如果缺失, 主动生成一个并记日志警告

---

## 十二、协议维护

协议以 **[PROTOCOL.md](PROTOCOL.md)** 为准，`README.md` 仅作概述。

修改协议时需要同步：
1. `PROTOCOL.md` — 协议规范
2. `aqa/core/message.py` — 消息实现
3. `sdk/aqa_sdk/message.py` — SDK 消息实现
4. `tests/test_aqa.py` 和 `sdk/tests/test_sdk.py` — 更新测试
