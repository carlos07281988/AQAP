# AQAP v3 — Agent Queue Agent Communication Protocol

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
│  2. 三层分离   — Transport(路由) / Protocol(追踪) / Schema(契约)  │
│  3. 设计时分离, 运行时零拷贝 — 概念清晰 + 性能极致                │
│  4. 全链路追踪 — trace_id 贯穿所有消息, span_id 标记每步处理      │
│  5. 可插拔后端 — Transport 层屏蔽队列实现 (Redis/Kafka/RabbitMQ)  │
│  6. 极致简单   — 声明式 1 行注解, 函数式 5 行跑, curl 都能接入   │
└────────────────────────────────────────────────────────────────────┘
```

### 为什么不用 HTTP/RPC 做 Agent 间通信

| 对比项 | HTTP/RPC | 消息队列 (本协议) |
|---|---|---|
| 耦合度 | 调用方依赖被调方的地址+接口定义 | 零依赖, 只认 topic |
| 背压 | 调用方需要自己处理超时/重试 | 队列自带 backpressure |
| 故障隔离 | 级联失败 (A 挂了→B 重试→C 雪崩) | 消费者故障不影响生产者 |
| 多语言 | 每个语言写一套 client stub | 拼 JSON 即可, 或装 SDK |
| 热升级 | 需要服务发现 + 负载均衡 | 停消费者→升级→启动, 消息不丢 |
| 可观测 | 需要额外埋点 | 消息本身带 trace_id + span_id |

---

## 二、协议层次

AQAP v3 采用三层分离架构：

```
设计时（概念清晰）                      运行时（零拷贝）
─────────────────                      ────────────────

┌──────────────────────┐                ┌──────────────────────────┐
│ Transport Envelope    │               │ WireMessage (单次 alloc)  │
│ ──────────────────── │               │                          │
│ message_id: UUID v7   │  ◄── 路由 ──► │ Header (64B fixed)       │
│ topic: str            │  ◄── 安全 ──► │ Topic (variable)         │
│ signature: bytes      │               │ Message (variable)       │
│ payload_encoding: u8  │               │   ├─ trace_id, span_id   │
└──────────┬───────────┘               │   ├─ source, target      │
           │                            │   ├─ type, headers       │
┌──────────▼───────────┐               │   └─ body                │
│ Protocol Message      │               │ Signature (optional)     │
│ ──────────────────── │               └──────────────────────────┘
│ source: str           │  ◄── 追踪 ──
│ target: str           │
│ trace_id: UUID v7     │
│ span_id: u64          │
│ correlation_id: UUID  │
│ type: u16             │
│ headers: dict         │
│ body: bytes           │
└──────────┬───────────┘
           │
┌──────────▼───────────┐
│ Business Schema       │
│ ──────────────────── │
│ schema_id: str        │  ◄── 契约 ──
│ data: dict            │
└───────────────────────┘
```

| 层 | 数据结构 | 关心什么 | 不关心什么 |
|----|----------|----------|------------|
| Transport | `Envelope` | 路由 (topic)、可靠性 (message_id)、安全 (signature) | 消息内容 |
| Protocol | `Message` | 追踪 (trace/span)、路由 (source/target)、类型 (type) | 业务数据含义 |
| Business | `SchemaEnvelope` | 契约校验 (schema_id)、数据正确性 | 传输细节 |

---

## 三、协议内核 (Rust — PyO3)

AQAP v3 内核用 Rust 编写，编译为 Python 原生扩展 (`.so`)。所有热路径操作（序列化、签名、加密、Schema 校验）在 Rust 层完成。

```
┌──────────────────────────────────────────────────────────────┐
│                   AQAP Kernel (Rust)                          │
│  ┌──────────┐ ┌──────────────┐ ┌────────────┐ ┌───────────┐ │
│  │ wire.rs  │ │ crypto.rs    │ │ schema.rs  │ │router.rs  │ │
│  │ 64B header│ │ AES-256-GCM  │ │ JSON Schema│ │ Topic→Hdlr│ │
│  │ msgpack   │ │ HMAC-SHA256  │ │ Registry   │ │ Map       │ │
│  │ zstd/lz4  │ │ HKDF derive  │ │ 7 builtins │ │           │ │
│  │ xxHash64  │ │ Ed25519(wire)│ │            │ │           │ │
│  └──────────┘ └──────────────┘ └────────────┘ └───────────┘ │
└──────────────────────────────────────────────────────────────┘
```

```python
from aqap.kernel import (
    WireHeader, WireMessage,          # 线格式
    SecurityContext, encrypt_payload,  # 安全
    SchemaRegistry, ValidationResult,  # Schema
    Router,                            # 路由
)
```

### 线格式 (Wire Format)

64 字节固定头 + 变长 topic + 变长消息体 + 可选签名：

| 偏移 | 长度 | 字段 |
|------|------|------|
| 0 | 4 | magic `0x41514150` ("AQAP") |
| 4 | 2 | version_major (3) |
| 6 | 2 | version_minor (0) |
| 8 | 2 | version_patch (0) |
| 10 | 1 | flags (encoding 2b + compression 2b + signature 2b) |
| 11 | 1 | priority (0=low, 1=normal, 2=high, 3=critical) |
| 16 | 16 | message_id (UUID v7) |
| 32 | 8 | timestamp_ms (Unix 毫秒) |
| 48 | 2 | topic_len |
| 52 | 4 | total_len |
| 56 | 8 | checksum (xxHash64) |

支持的编码: **JSON** / **MsgPack** / Protobuf / FlatBuffer  
支持的压缩: none / **zstd** / **lz4** / zlib  
支持的签名: none / **HMAC-SHA256** (32B) / Ed25519 (64B)

### 安全层

```
Master Key (32 bytes)
  ├── HKDF(info="encrypt") → AES-256-GCM 加密 body
  ├── HKDF(info="sign")    → HMAC-SHA256 签名 envelope
  └── HKDF(info="route")   → HMAC-SHA256 签名 topic (防篡改)
```

支持密钥轮换：最多 3 个密钥同时有效（当前 + 2 个旧密钥仅解密）。

### Schema 系统

7 个内置 Schema (JSON Schema Draft 7):
- `aqap:schema:task.v3` — 检测任务
- `aqap:schema:result.v3` — 检测结果
- `aqap:schema:verdict.v3` — 评判裁决
- `aqap:schema:report.v3` — 报告
- `aqap:schema:heartbeat.v3` — 心跳
- `aqap:schema:error.v3` — 错误
- `aqap:schema:dlq.v3` — 死信

---

## 四、Python SDK — 两个入口

### 函数式 API（5 行跑起来）

```python
from aqap import AQAP

app = AQAP(transport="redis://localhost:6379")

@app.on("aqap:v3:agent:probe")
async def handle_task(msg):
    score = await check(msg["data"]["target"]["repo"])
    return {"passed": True, "score": score}

app.run()  # 一行启动：连接、注册、心跳、消费、reply、追踪、重试
```

### 声明式 API（全功能）

```python
from aqap import agent, Agent

@agent(
    agent_id="my-probe",
    transport="redis://localhost:6379",
    subscribe=["aqap:v3:agent:probe"],
    schema_in=TaskSchema,
    schema_out=ResultSchema,
    concurrency=4,
)
class MyProbe(Agent):
    async def handle(self, task: TaskSchema) -> ResultSchema:
        findings = await self.model.analyze(task.target)
        return ResultSchema(
            task_id=task.task_id,
            passed=all(f.severity != "critical" for f in findings),
            score=avg(f.score for f in findings),
        )
```

### REST Gateway（curl 接入）

```bash
curl -X POST http://localhost:8080/v3/request/aqap:v3:agent:probe \
  -H "Content-Type: application/json" \
  -d '{"task_id":"task-abc","type":"code_review","target":{"repo":"my/repo","branch":"main"}}'
```

---

## 五、多语言 SDK

| 语言 | 安装 | 状态 |
|------|------|:---:|
| Python | `pip install aqap-sdk` + `aqap-kernel` | Phase 1 完成 |
| TypeScript | `npm install @aqap/sdk` | Phase 4 计划 |
| Go | `go get github.com/aqap/sdk-go/v3` | Phase 4 计划 |

---

## 六、快速开始

```bash
# 1. 安装内核 (需要 Rust 工具链)
cd aqap-kernel && cargo build --release
cp target/release/libaqap_kernel.dylib ../aqap/kernel/_aqap_kernel.so

# 2. 运行 Rust 测试 (58 项)
cargo test

# 3. 运行 Python 测试 (35 项内核测试)
cd .. && python3 -m pytest tests/test_wire.py tests/test_crypto.py \
  tests/test_schema.py tests/test_kernel_integration.py -v

# 4. 运行 v1 兼容测试 (30 项)
python3 -m pytest tests/test_aqa.py -v
```

---

## 七、项目结构

```
AQAP/
├── PROTOCOL.md                # v1 协议规范 (向后兼容)
├── PROTOCOL_v2.md             # v2 协议草案
├── README.md                  # 本文件
│
├── docs/
│   ├── superpowers/specs/     # 设计规格
│   │   └── 2026-07-01-aqap-v3-protocol-design.md
│   ├── superpowers/plans/     # 实现计划
│   │   └── 2026-07-01-aqap-v3-phase1-protocol-kernel.md
│   ├── ARCHITECTURE.md        # 架构总览
│   ├── AGENT_SYSTEM.md        # Agent 子系统详解
│   ├── TRANSPORT.md           # Transport 层详解
│   ├── PLUGIN_SYSTEM.md       # 插件系统详解
│   ├── TESTING.md             # 测试策略
│   └── CONFIG_REFERENCE.md    # 配置参考
│
├── aqap-kernel/               # ★ Rust 协议内核 (Phase 1)
│   ├── Cargo.toml
│   └── src/
│       ├── lib.rs             # PyO3 入口
│       ├── types.rs           # MessageType, ErrorCode, Flags
│       ├── wire.rs            # 线格式 encode/decode
│       ├── crypto.rs          # AES-GCM, HMAC, HKDF
│       ├── schema.rs          # JSON Schema 校验引擎
│       ├── router.rs          # Topic→Handler 路由
│       ├── error.rs           # 错误码体系
│       └── schemas/           # 7 个内置 JSON Schema
│
├── aqap/                      # Python SDK
│   ├── kernel/                #   Rust 内核 Python 绑定
│   │   ├── __init__.py
│   │   └── _aqap_kernel.so
│   ├── core/                  #   v1 内核 (兼容)
│   │   ├── message.py         #     消息协议
│   │   ├── engine.py          #     运行时引擎
│   │   ├── dlq.py             #     死信队列
│   │   ├── config.py          #     配置加载
│   │   └── security.py        #     Payload 加密
│   ├── transport/             #   传输层
│   │   ├── base.py            #     Transport ABC
│   │   ├── inmemory.py        #     InMemory 实现
│   │   ├── redis_streams.py   #     Redis Streams 实现
│   │   ├── kafka_transport.py #     Kafka 实现
│   │   └── rabbitmq_transport.py # RabbitMQ 实现
│   ├── agent/                 #   Agent 层
│   │   ├── base.py            #     Agent 基类
│   │   ├── supervisor.py      #     Supervisor
│   │   ├── probe.py           #     Probe Agent
│   │   ├── judge.py           #     Judge Agent
│   │   └── reporter.py        #     Reporter Agent
│   ├── v2/                    #   v2 协议实现
│   └── cli/                   #   CLI 工具 (Phase 5 计划)
│
├── sdk/                       # 外部 Agent SDK
│   ├── aqap_sdk/               #   Python SDK
│   ├── README.md              #   多语言接入文档
│   └── examples/              #   Go / JS 外部 Agent 示例
│
├── tests/
│   ├── test_wire.py           # 线格式测试 (7)
│   ├── test_crypto.py         # 安全层测试 (15)
│   ├── test_schema.py         # Schema 测试 (13)
│   ├── test_kernel_integration.py # 集成测试 (2)
│   ├── test_aqa.py            # v1 兼容测试 (30)
│   └── test_extended.py       # 扩展测试
│
├── config.yaml
├── pyproject.toml
└── requirements.txt
```

---

## 八、消息类型

| 类型 | 语义 | u16 编码 |
|------|------|:------:|
| `task:dispatch` | 下发检测任务 | 0x0000 |
| `task:result` | 检测结果 | 0x0001 |
| `task:cancel` | 任务取消 | 0x0002 |
| `judge:request` | 请求评判 | 0x0010 |
| `judge:verdict` | 评判裁决 | 0x0011 |
| `report:request` | 请求报告 | 0x0020 |
| `report:deliver` | 报告投递 | 0x0021 |
| `system:heartbeat` | 心跳 | 0x0100 |
| `system:register` | Agent 注册 | 0x0101 |
| `system:shutdown` | Agent 下线 | 0x0102 |
| `system:error` | 错误通知 | 0x0103 |

完整错误码体系见设计规格：`docs/superpowers/specs/2026-07-01-aqap-v3-protocol-design.md`

---

## 九、Topic 系统

| Topic | 用途 | v3 路径 |
|-------|------|---------|
| Probe 任务分发 | 检测任务路由 | `aqap:v3:agent:probe` |
| Judge 评判 | 评判请求路由 | `aqap:v3:agent:judge` |
| Reporter 报告 | 报告生成路由 | `aqap:v3:agent:reporter` |
| 系统事件 | 注册/下线/轮换 | `aqap:v3:system:events` |
| 心跳 | Agent 心跳 | `aqap:v3:system:heartbeat` |
| 死信队列 | 失败消息 | `aqap:v3:error:dlq` |
| Agent 收件箱 | 定向消息 | `aqap:v3:inbox:{agent_id}` |
| 广播 | 全局广播 | `aqap:v3:broadcast` |

---

## 十、全链路追踪

```
TASK_DISPATCH  →  trace_id = "0192abcd..."  ← 入口生成, span_id = aaaa
TASK_RESULT    →  trace_id = "0192abcd..."  ← 透传, span_id = bbbb
JUDGE_VERDICT  →  trace_id = "0192abcd..."  ← 透传, span_id = cccc
REPORT_DELIVER →  trace_id = "0192abcd..."  ← 透传, span_id = dddd

correlation_id: 回复时填原始消息的 message_id
```

---

## 十一、文档索引

| 文档 | 内容 | 受众 |
|------|------|------|
| **[设计规格](docs/superpowers/specs/2026-07-01-aqap-v3-protocol-design.md)** | v3 完整协议设计 (线格式/SDK/安全/Gateway) | 所有开发者 |
| **[Phase 1 计划](docs/superpowers/plans/2026-07-01-aqap-v3-phase1-protocol-kernel.md)** | 内核实现计划 (6 tasks) | 实现者 |
| **[PROTOCOL.md](PROTOCOL.md)** | v1 协议规范 (向后兼容) | 外部 Agent 开发者 |
| **[PROTOCOL_v2.md](PROTOCOL_v2.md)** | v2 协议草案 | 参考 |
| **[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)** | 系统架构总览 | 新开发者 |
| **[docs/AGENT_SYSTEM.md](docs/AGENT_SYSTEM.md)** | Agent 详解 | Agent 开发者 |
| **[docs/TRANSPORT.md](docs/TRANSPORT.md)** | Transport 实现细节 | Transport 实现者 |
| **[sdk/README.md](sdk/README.md)** | 外部 Agent 多语言接入 | 外部开发者 |

---

## 十二、版本

| 版本 | 协议 | 内核 | 状态 |
|------|------|------|:---:|
| v1 | `PROTOCOL.md` | `aqap/core/` | 稳定, 兼容 |
| v2 | `PROTOCOL_v2.md` | `aqap/v2/` | 草案 |
| **v3** | 设计规格 | `aqap-kernel/` | **Phase 1 完成** |

- `message_id` 和 `trace_id` 使用 **UUID v7** (128-bit, 时间有序)
- 线格式版本号 `3.0.0` (semver)
- major 不兼容需桥接 Agent, minor/patch 向后兼容

---

## 十三、协议维护

修改协议时需要同步：

1. `docs/superpowers/specs/` — 设计规格
2. `aqap-kernel/src/` — Rust 内核实现
3. `aqap/` — Python SDK
4. `tests/` — 测试
