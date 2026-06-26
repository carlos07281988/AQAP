# AQA 通信协议规范 v1.0

> 这是 AQA Agent 之间通信的**唯一权威协议规范**。
> 所有语言实现（Python / Go / JS / Java / Rust...）必须遵循此文档定义的线格式。
> 代码是实现，不是规范。

---

## 目录

1. [线格式（Wire Format）](#1-线格式wire-format)
2. [消息类型（Message Types）](#2-消息类型message-types)
3. [Topic 系统](#3-topic-系统)
4. [全链路追踪](#4-全链路追踪)
5. [错误处理](#5-错误处理)
6. [版本与兼容性](#6-版本与兼容性)
7. [安全](#7-安全)
8. [实现检查清单](#8-实现检查清单)

---

## 1. 线格式（Wire Format）

### 1.1 JSON 信封

所有消息使用统一的 JSON 对象在线路上传输：

```json
{
  "type":           "TASK_DISPATCH",
  "message_id":     "a1b2c3d4e5f6g7h8",
  "source":         "orchestrator-1",
  "target":         "",
  "topic":          "aqap:agent:probe",
  "trace_id":       "trace_dcf9a2b1",
  "correlation_id": "",
  "version":        "1.0",
  "payload":        { "task_id": "t-001" },
  "timestamp":      "2026-06-25T10:30:00+00:00"
}
```

### 1.2 字段定义

| 字段 | 类型 | 必须 | 默认值 | 说明 |
|---|---|---|---|---|
| `type` | `String(SCREAMING_SNAKE)` | ✅ | — | 消息语义类型，见第 2 节 |
| `message_id` | `String(hex, max 64)` | ✅ | 自动生成 | 消息唯一标识，建议 uuid4.hex[:16] |
| `source` | `String(non-empty)` | ✅ | — | 发送者标识，格式 `{agent_id}` 或 `{lang}-{name}` |
| `target` | `String` | ✅ | `""` | 目标 Agent ID。**空字符串 = 广播**，该 topic 下所有消费者均可处理 |
| `topic` | `String` | ✅ | — | 路由目标 topic，见第 3 节 |
| `trace_id` | `String(hex, max 64)` | ✅ | 自动生成 | 全链路追踪 ID。**同一任务链上所有消息共享同一个 trace_id** |
| `correlation_id` | `String` | ✅ | `""` | 回复关联的消息 ID。**非回复消息为 `""`** |
| `version` | `String(semver)` | ✅ | `"1.0"` | 协议版本，大版本不兼容时变更 |
| `payload` | `Object` | ✅ | `{}` | 业务负载。任何合法的 JSON Object |
| `timestamp` | `String(ISO-8601)` | ✅ | 当前时间 | 消息生成时间，UTC |

### 1.3 序列化规则

1. **字段顺序不重要** — 实现可以按任意顺序输出 JSON key
2. **未知字段必须保留并透传** — 实现不能丢弃不认识的自定义字段
3. **字符串必须 UTF-8 编码**
4. `target` 和 `correlation_id` **必须**是字符串，不要用 `null`。空字符串表示「无目标」/「非回复」
5. `message_id` 在整个 AQA 部署范围内没有严格的全局唯一要求，但建议使用足够的熵 (128bit) 以大概率避免碰撞
6. 时间戳格式：`YYYY-MM-DDTHH:mm:ss±HH:mm`，必须包含时区

### 1.4 字段约束

```
type ∈ {TASK_DISPATCH, TASK_RESULT, JUDGE_REQUEST, JUDGE_VERDICT,
        REPORT_REQUEST, REPORT_DELIVER, HEARTBEAT, ERROR, REGISTER, SHUTDOWN,
        <自定义>}

source.len ≥ 1
source.len ≤ 64

target.len ≤ 64
target == "" → 广播语义

version ~ /^\d+\.\d+$/
message_id.len ≤ 64
trace_id.len ≤ 64
correlation_id.len ≤ 64
```

---

## 2. 消息类型（Message Types）

### 2.1 类型目录

| 类型 | 方向 | 语义 | payload 必填字段 |
|---|---|---|---|
| `REGISTER` | Agent → SYSTEM | Agent 上线注册 | `{"agent_id": "...", "capabilities": [...]}` |
| `SHUTDOWN` | Agent → SYSTEM | Agent 优雅下线 | `{"reason": "..."}` |
| `HEARTBEAT` | Agent → BROADCAST | 心跳保活 | `{"status": {"alive": true}}` |
| `TASK_DISPATCH` | Scheduler → Probe | 下发检测任务 | `{"task_id": "...", "target": "..."}` |
| `TASK_RESULT` | Probe → Judge | 检测结果 | `{"task_id": "...", "passed": bool, "score": float}` |
| `JUDGE_REQUEST` | Probe → Judge | 请求评判 | `{"task_id": "...", "evidences": [...]}` |
| `JUDGE_VERDICT` | Judge → Reporter | 评判裁决 | `{"task_id": "...", "verdict": "PASS"/"FAIL"/"WARN", "score": float}` |
| `REPORT_REQUEST` | Judge → Reporter | 请求报告 | `{"task_id": "...", "format": "html"/"json"/"md"}` |
| `REPORT_DELIVER` | Reporter → 下游 | 报告送达 | `{"task_id": "...", "report": {...}}` |
| `ERROR` | 任意 Agent | 协议级错误 | `{"code": "...", "message": "...", "trace_id": "..."}` |

### 2.2 标准流程状态机

```
                REGISTER
                    │
                    ▼
           ┌────────────────┐
           │   TASK_DISPATCH │
           └───────┬────────┘
                   │
          ┌────────┴────────┐
          ▼                 ▼
    TASK_RESULT      JUDGE_REQUEST
                   (含证据)
          │                 │
          └────────┬────────┘
                   ▼
           JUDGE_VERDICT
                   │
                   ▼
           REPORT_REQUEST
                   │
                   ▼
            REPORT_DELIVER
```

### 2.3 错误码体系

| 错误码 | 语义 | 触发条件 |
|---|---|---|
| `UNKNOWN_TYPE` | 不识别的消息类型 | `type` 字段不在已注册集合中 |
| `VERSION_MISMATCH` | 协议版本不支持 | `version` 字段无法处理 |
| `MALFORMED` | 消息格式错误 | JSON 解析失败，或必填字段缺失 |
| `ROUTING_FAILURE` | 路由失败 | target 指定的 Agent 不存在 |
| `PROCESSING_ERROR` | 处理时异常 | Agent 执行过程中抛出未预期异常 |
| `TIMEOUT` | 任务超时 | 任务超过配置的超时时间 |
| `AUTH_FAILURE` | 认证失败 | 发送者身份验证失败 |
| `FORBIDDEN` | 权限不足 | 发送者无权向目标 topic 发消息 |

ERROR 消息的 payload 格式:

```json
{
  "code": "PROCESSING_ERROR",
  "message": "probe-1: 处理任务 t-001 时发生 ZeroDivisionError",
  "trace_id": "trace_dcf9a2b1",
  "original_message_id": "a1b2c3d4e5f6g7h8"
}
```

---

## 3. Topic 系统

### 3.1 命名规范

```
{auth}[:{scope}[:{sub}]]      # 全部小写字母 + 连字符
```

| 前缀 | 保留用途 | 示例 |
|---|---|---|
| `aqap:agent:*` | 内置 Agent 通道 | `aqap:agent:probe` |
| `aqap:system:*` | 系统级通道 | `aqap:system:events` |
| `aqap:broadcast` | 全局广播 | — |
| `aqap:inbox:*` | Agent 私有收件箱 | `aqap:inbox:probe-1` |
| `aqap:plugin:*` | 插件事件 | `aqap:plugin:events` |
| `aqap:dlq` | 死信队列 | — |
| `<自定义>` | 外部 Agent 自定义 | `external:my-service:alerts` |

### 3.2 Topic 作用域

| Topic | 创建者 | 消费者 | 保留 |
|---|---|---|---|
| `aqap:broadcast` | 任何 Agent | 所有 Agent | ✅ |
| `aqap:system:events` | 系统 | 所有 Agent | ✅ |
| `aqap:agent:probe` | Scheduler | Probe Agent | ✅ |
| `aqap:agent:judge` | Probe Agent | Judge Agent | ✅ |
| `aqap:agent:reporter` | Judge Agent | Reporter Agent | ✅ |
| `aqap:inbox:{agent_id}` | 任何 Agent | 只有 `{agent_id}` 消费 | ✅ |
| `aqap:plugin:events` | Plugin | 订阅的 Agent | ✅ |
| `aqap:dlq` | 系统 (自动) | DLQ Consumer | ✅ |
| 自定义 | 外部 Agent | 外部 Agent | ❌ |

### 3.3 路由规则

```
publish(topic, msg) → 所有订阅 topic 的消费者组收到消息
                       │
                 ┌─────┴─────┐
                 │            │
            msg.target     msg.target
               = ""          ≠ ""
                 │            │
           广播给整个      inbox 路由:
           group 内所有    topic 不变,
           消费者          target={id} →
                          aqap:inbox:{id}
```

---

## 4. 全链路追踪

### 4.1 trace_id — 任务链标识

**规则**: 同一条任务链上所有消息共享同一个 `trace_id`。

```
入口消息生成 trace_id          trace_id = "trace_a1b2"
    TASK_DISPATCH ──────────► aqap:agent:probe
                                  │
                            透传 trace_id ←─── 不准重新生成
                                  │
    TASK_RESULT     ◄──────── aqap:agent:probe
    trace_id = "trace_a1b2"
                                  │
    JUDGE_VERDICT   ◄──────── aqap:agent:judge
    trace_id = "trace_a1b2"
```

### 4.2 correlation_id — 消息级关联

**规则**: 回复消息时，`correlation_id` 填原始消息的 `message_id`。

```
Agent A 发送  message_id="msg_001", correlation_id=""
                     │
                     ▼
Agent B 回复  message_id="msg_002", correlation_id="msg_001"
                                               │
                                               └── A 通过 correlation_id
                                                   知道这是 msg_001 的回复
```

### 4.3 追踪示例

```json
// 消息 1: 入口
{ "message_id": "msg_a1", "type": "TASK_DISPATCH",
  "trace_id": "trace_x001", "correlation_id": "", ... }

// 消息 2: 回复 (透传 trace_id)
{ "message_id": "msg_b2", "type": "TASK_RESULT",
  "trace_id": "trace_x001", "correlation_id": "msg_a1", ... }

// 消息 3: 继续回复 (透传 trace_id)
{ "message_id": "msg_c3", "type": "JUDGE_VERDICT",
  "trace_id": "trace_x001", "correlation_id": "msg_b2", ... }
```

---

## 5. 错误处理

### 5.1 协议层错误处理

当收到不合法或不支持的消息时，Agent **必须**：

1. 确认消息已收到（ACK 或等效操作），**不能**让消息停留在队列中重试
2. 向原始消息的 source 发一条 `ERROR` 类型消息
3. `ERROR` 消息的 payload 必须包含 `code`, `message`, `trace_id`, `original_message_id`

例外：如果消息本身无法解析（JSON 有语法错误），ACK 后丢弃，无需发 ERROR。

### 5.2 业务层错误处理

处理 payload 时发生业务异常：

1. **不 ACK** — 消息重返 pending 队列
2. 重试计数超过 `max_retries` 后，ACK 并转发到 `aqap:dlq` topic

### 5.3 DLQ 消息格式

DLQ 消息是标准的 AQA 信封：

```json
{
  "type":           "ERROR",
  "message_id":     "dlq_uuid",
  "source":         "system-dlq",
  "target":         "",
  "topic":          "aqap:dlq",
  "trace_id":       "原消息 trace_id",
  "correlation_id": "原消息 message_id",
  "version":        "1.0",
  "payload": {
    "code": "PROCESSING_ERROR",
    "message": "重试 3 次后仍失败",
    "original": { ... },          ← 原始消息完整 JSON
    "error": "ZeroDivisionError",
    "retry_count": 3,
    "failed_at": "2026-06-25T10:31:00+00:00"
  },
  "timestamp": "2026-06-25T10:31:00+00:00"
}
```

---

## 6. 版本与兼容性

### 6.1 版本策略

```
version = "MAJOR.MINOR"
```

| 变更类型 | MAJOR | MINOR | 示例 |
|---|---|---|---|
| 新增字段 | 不变 | 不变 | 接收方必须保留未知字段 |
| 新增消息类型 | 不变 | 不变 | 新增 enum 值 |
| 新增可选字段 | 不变 | +1 | `1.0` → `1.1` |
| 删除必填字段 | +1 | 0 | `1.0` → `2.0` |
| 重命名字段 | +1 | 0 | `1.0` → `2.0` |
| 修改语义 | +1 | 0 | `1.0` → `2.0` |

### 6.2 降级策略

- 收到 `version > 支持版本`：尝试解析，如果遇到不识别的必填字段则发 `VERSION_MISMATCH` ERROR
- 收到 `version < 支持版本`：正常处理（向后兼容）
- 收到不识别的 `type`：发 `UNKNOWN_TYPE` ERROR
- 收到 `trace_id` 为空：主动生成一个并记日志告警

---

## 7. 安全

### 7.1 传输层安全

- Redis：TLS + ACL 认证
- Kafka：SASL/SCRAM 或 TLS 双向认证

### 7.2 Payload 加密（可选）

启用时，Agent 在 `publish()` 前加密 payload，在 `subscribe()` 后解密 payload。

加密方式：**AES-256-GCM**（认证加密，防篡改）

加密后的 payload 格式：

```json
{
  "_encrypted": true,
  "_ciphertext": "base64-encoded",
  "_nonce": "base64-encoded",
  "_tag": "base64-encoded"
}
```

### 7.3 Topic 级访问控制（规划中）

- 禁止外部 Agent 向 `aqap:*` 系统 topic 发布消息
- Agent `source` 和白名单 topic 绑定

---

## 8. 实现检查清单

每个语言实现必须满足以下条件才能声称「兼容 AQA 协议」：

### 8.1 线格式

- [ ] 能构造、序列化、反序列化符合第 1 节 JSON 信封的消息
- [ ] 反序列化时能自动填充 `message_id`、`trace_id`、`timestamp`、`version`（source 必须由调用方提供）
- [ ] 反序列化时保留未知字段
- [ ] `target` 和 `correlation_id` 为空字符串时语义正确

### 8.2 消息类型

- [ ] 能发送和接收所有标准 MessageType
- [ ] 不识别的类型收到后发 `UNKNOWN_TYPE` ERROR，不静默丢弃
- [ ] `version` 不匹配时发 `VERSION_MISMATCH` ERROR

### 8.3 追踪

- [ ] `trace_id` 在消息链中透传（不准重新生成）
- [ ] 回复消息时 `correlation_id` 设置为原消息的 `message_id`
- [ ] 无 `trace_id` 时自动生成并告警

### 8.4 错误处理

- [ ] 解析失败的消息 ACK 后丢弃
- [ ] 处理失败的消息重试或发 DLQ
- [ ] ERROR 消息包含 `code`、`message`、`trace_id`、`original_message_id`
