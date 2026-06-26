# Agent 系统

AQA 共有三种 Agent 类型 + 一个 Supervisor，全部实现在 `aqap/agent/` 目录下。

```
aqap/agent/
├── base.py        # Agent 抽象基类 (311 行)
├── probe.py       # 检测 Agent (74 行)
├── judge.py       # 评判 Agent (88 行)
├── reporter.py    # 报告 Agent (74 行)
└── supervisor.py  # 生命周期总管 (145 行)
```

---

## 一、Agent 基类 (`base.py`)

所有 Agent 继承自 `Agent` 抽象基类。基类提供了完整的消息生命周期管理。

### 构造函数参数

```python
Agent(
    agent_id: str,                    # Agent 唯一标识
    transport: Transport,             # 消息队列后端
    group: str = "aqap-default",       # 消费者组名
    max_retries: int = 3,            # 消息重试上限
    heartbeat_interval: int = 30,    # 心跳间隔(秒)
    cipher: PayloadCipher | None = None,  # payload 加密
)
```

### 生命周期

```
  start()
    │
    ├─ connect() — 建立 Transport 连接
    ├─ on_start() — 子类钩子
    ├─ publish(REGISTER) — 注册到系统事件通道
    ├─ create_task(_heartbeat_loop) — 启动心跳
    └─ create_task(_consume_loop) × N — 为每个订阅 topic 启动消费循环
    │
  running...
    │
  stop()
    │
    ├─ publish(SHUTDOWN) — 广播下线
    ├─ drain — 等待进行中任务完成 (最多 5s)
    ├─ cancel — 取消剩余任务
    ├─ on_stop() — 子类钩子
    └─ disconnect() — 关闭 Transport
```

### 消息处理循环 (`_consume_loop`)

```
收到消息
    │
    ├─ 回声检测 ── source == self.agent_id? ── YES → 跳过
    │
    ├─ 幂等去重 ── message_id 已存在? ── YES → 跳过
    │
    ├─ 格式校验 ── validate_message() 失败? ── YES → ack 丢弃
    │
    ├─ payload 解密 ── cipher.decrypt_payload()
    │
    ├─ handle_message() ── 子类实现
    │      │
    │      └─ 返回 replies → 逐条发布
    │             │
    │             ├─ reply.target 非空 → Topic.agent_inbox(reply.target)
    │             └─ reply.target 为空 → _determine_reply_topic(message)
    │
    ├─ 成功 → 清除重试计数
    │
    ├─ 异常 → _handle_failure() → 重试或转发 DLQ
    │
    └─ ack() — 确认消息已处理
```

### 回复路由 (`_determine_reply_topic`)

根据收到消息的 `topic` 决定回复发往哪个 topic：

| 收到消息的 topic | 回复发往 |
|---|---|
| `aqap:agent:probe` | `aqap:agent:judge` |
| `aqap:agent:judge` | `aqap:agent:reporter` |
| `aqap:agent:reporter` | `aqap:broadcast` |
| 其他 | `aqap:inbox:{message.source}` |

如果 reply 的 `target` 非空，则强制路由到 `aqap:inbox:{reply.target}`。

### 重试与死信 (`_handle_failure`)

```
handle_message 抛出异常
    │
    ├─ 重试计数 +1
    │
    ├─ 重试 < max_retries? ── YES → 不 ACK, 下次重新消费
    │
    └─ 重试 >= max_retries? ── YES → 转发 DLQ
           │
           ├─ create_dlq_message() — 构造死信消息
           ├─ publish(DLQ_TOPIC, ERROR)
           └─ ack() — 从 pending 队列移除
```

### 心跳 (`_heartbeat_loop`)

- 每 `heartbeat_interval` 秒发一次 `HEARTBEAT`
- 双通道：`aqap:broadcast` + `aqap:system:events`
- Supervisor 通过心跳时间戳检测失联

### 插件执行 (`run_plugins`)

```python
async def run_plugins(self, topic: str, context: dict) -> list[dict]:
```

- 自动注入追踪上下文：`_aqap_start_time`, `_aqap_trace_id`, `_aqap_message_type`, `_aqap_source`
- 调用 `registry.execute_all(topic, ctx)` 执行绑定到该 topic 的所有插件

### 发送消息 (`send`)

```python
async def send(self, message: Message):
    if message.target:
        await self._transport.publish(Topic.agent_inbox(message.target), message)
    else:
        await self._transport.publish(Topic.BROADCAST, message)
```

- 有 target → inbox 路由
- 无 target → broadcast

### 幂等去重

- `_processed_ids: set[str]` 缓存已处理消息 ID
- 上限 `_idempotency_max_size = 10000`
- 超过上限时裁剪至 5000 条
- 解决 at-least-once 投递语义下的重复处理问题

---

## 二、Probe Agent (`probe.py`)

检测执行器，接收 `TASK_DISPATCH`，返回 `TASK_RESULT` + `JUDGE_REQUEST`。

### 构造函数

```python
ProbeAgent(
    judge_target: str = "judge-1",   # JUDGE_REQUEST 的 target
)
```

### 处理逻辑

```
TASK_DISPATCH
    │
    ├─ _probe(task)
    │     │
    │     └─ run_plugins("probe", task)  →  调用绑定的验证/追踪插件
    │
    ├─ task_result() → TASK_RESULT (发往 aqap:agent:judge)
    │
    └─ message.reply(JUDGE_REQUEST) → JUDGE_REQUEST
          │
          └─ judge_msg.target = self._judge_target → aqap:inbox:{judge_target}
```

---

## 三、Judge Agent (`judge.py`)

评判裁决器，接收 `JUDGE_REQUEST`，返回 `JUDGE_VERDICT` + `REPORT_REQUEST`。

### 构造函数

```python
JudgeAgent(
    reporter_target: str = "reporter-1",  # REPORT_REQUEST 的 target
)
```

### 处理逻辑

```
JUDGE_REQUEST
    │
    ├─ _judge(evidence)
    │     │
    │     └─ run_plugins("judge", evidence)  →  调用评分/验证/追踪插件
    │           │
    │           └─ 综合所有插件结果 → avg_score, passed
    │
    ├─ judge_verdict() → JUDGE_VERDICT (无 target → 按 topic 路由)
    │
    └─ message.reply(REPORT_REQUEST) → REPORT_REQUEST
          │
          └─ report_msg.target = self._reporter_target → aqap:inbox:{reporter_target}
```

---

## 四、Reporter Agent (`reporter.py`)

报告生成器，接收 `REPORT_REQUEST`，返回 `REPORT_DELIVER`。

### 处理逻辑

```
REPORT_REQUEST
    │
    ├─ _generate_report(data)
    │     │
    │     └─ run_plugins("reporter", data)
    │
    ├─ REPORT_DELIVER (定向回传 → target = message.source)
    │
    └─ self.send(broadcast) → REPORT_DELIVER (广播到 aqap:broadcast)
```

双向投递：定向回传给请求者 + 广播到全局通道。

---

## 五、Supervisor (`supervisor.py`)

Agent 生命周期总管。

### 职责

| 方法 | 职责 |
|---|---|
| `register(agent)` | 注册 Agent |
| `start_all()` | 启动所有 Agent + 心跳监控循环 |
| `stop_all()` | 优雅停止所有 Agent |
| `restart_agent(id)` | 重启指定 Agent |
| `health_check()` | 返回所有 Agent 健康状态 |
| `record_heartbeat(id)` | 记录心跳时间戳 |
| `install_signal_handlers()` | 安装 SIGTERM/SIGINT → 优雅关闭 |

### 心跳监控循环

```
每 heartbeat_timeout/3 秒
    │
    └─ health_check() → 检查所有 Agent 心跳时间戳
         │
         └─ last_heartbeat > heartbeat_timeout? → 自动 restart_agent
```

### 信号处理

- 收到 `SIGTERM` 或 `SIGINT` → 启动 stop_all() → 等待所有 Agent 优雅关闭
- 非 Unix 平台（Windows）回退处理
