# 配置参考

AQA 使用 `config.yaml` 作为唯一配置入口。引擎启动时自动加载。

---

## 一、完整 `config.yaml` 结构

```yaml
# 应用基础信息
app:
  name: "Agent Queue Agent Communication Protocol"
  version: "1.0.0"
  debug: false

# 传输层配置
transport:
  backend: "in-memory"           # redis-streams | kafka | in-memory
  redis_url: "redis://127.0.0.1:6379/0"
  kafka_servers: "localhost:9092"

# 安全配置
security:
  enabled: true                   # 是否启用 payload AES 加密
  secret: ""                      # 加密密钥 (留空则禁用)

# 插件注册
plugins:
  validator:                      # 插件名称 (任意)
    class: "aqap.plugins.validator.ValidatorPlugin"  # 全限定类名
    enabled: true                 # 是否启用
    topic_bind:                   # 绑定到哪些阶段名
      - "probe"
      - "judge"
    config:                       # 插件自定义配置
      required_fields:
        - "task_id"
        - "passed"
        - "plugin_results"

  scorer:
    class: "aqap.plugins.scorer.ScorerPlugin"
    enabled: true
    topic_bind:
      - "judge"
    config:
      weights:
        pass_rate: 0.5
        field_completeness: 0.3
        plugin_health: 0.2

  trace_collector:
    class: "aqap.plugins.trace_collector.TraceCollector"
    enabled: true
    topic_bind:
      - "probe"
      - "judge"
      - "reporter"
    config:
      report_interval: 60

# Agent 定义
agents:
  probe-1:
    type: "probe"                 # Agent 类型 (probe | judge | reporter)
    topics:                       # 订阅的消息 Topic
      - "aqap:broadcast"
      - "aqap:agent:probe"
    max_retries: 3
    heartbeat_interval: 30
    group: "aqap-probes"
    targets:                      # 透传给 Agent 构造函数的关键字参数
      judge_target: "judge-1"

  judge-1:
    type: "judge"
    topics:
      - "aqap:agent:judge"
      - "aqap:inbox:judge-1"       # 必须有 inbox 订阅 (定向消息)
    max_retries: 3
    heartbeat_interval: 30
    group: "aqap-judges"
    targets:
      reporter_target: "reporter-1"

  reporter-1:
    type: "reporter"
    topics:
      - "aqap:agent:reporter"
      - "aqap:inbox:reporter-1"   # 必须有 inbox 订阅 (定向消息)
    max_retries: 3
    heartbeat_interval: 30
    group: "aqap-reporters"

# 监控配置
supervisor:
  heartbeat_timeout: 90           # Agent 心跳超时阈值 (秒)
```

---

## 二、各配置段详解

### 2.1 `app` — 应用标识

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `name` | string | `"Agent Queue Agent Communication Protocol"` | 应用名 |
| `version` | string | `"1.0.0"` | 语义化版本号 |
| `debug` | bool | `false` | 调试日志开关 |

### 2.2 `transport` — 传输层

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `backend` | string | `"redis-streams"` | 后端类型：`redis-streams`、`kafka`、`in-memory` |
| `redis_url` | string | `"redis://127.0.0.1:6379/0"` | Redis 连接 URL，可通过 `REDIS_URL` 环境变量覆盖 |
| `kafka_servers` | string | `"localhost:9092"` | Kafka 地址 |

**环境变量覆盖**：`transport.redis_url` → `REDIS_URL`

### 2.3 `security` — Payload 加密

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `enabled` | bool | `true` | 启用 AES-256-GCM payload 加密 |
| `secret` | string | `""` | AES 密钥。必填（或配置文件中留空则运行中跳过） |

**注意**：`enabled=true` 但 `secret` 为空时，引擎会打印警告但不阻断启动。

### 2.4 `plugins` — 插件注册

每一项插件配置的字段：

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `class` | string | 必填 | 插件类的全限定名（如 `"aqap.plugins.validator.ValidatorPlugin"`） |
| `enabled` | bool | `true` | 是否启用 |
| `topic_bind` | list[string] | `[]` | 绑定到哪些阶段名（`probe`、`judge`、`reporter`） |
| `config` | dict | `{}` | 插件自定义配置，通过 `initialize(config)` 传给插件 |

### 2.5 `agents` — Agent 定义

每一项 Agent 配置的字段：

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `type` | string | 必填 | `probe` / `judge` / `reporter` |
| `topics` | list[string] | `[]` | 要订阅的消息 Topic |
| `max_retries` | int | `3` | 消息处理重试上限 |
| `heartbeat_interval` | int | `30` | 心跳间隔（秒） |
| `group` | string | `"aqap-{type}s"` | 消费者组名 |
| `targets` | dict | `{}` | 透传给 Agent 构造函数的 `**kwargs` |

**关键约束**：`judge` 和 `reporter` 必须订阅自己的 `aqap:inbox:{agent_id}` 以接收定向消息。

### 2.6 `supervisor` — 生命周期

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `heartbeat_timeout` | int | `90` | Agent 心跳超时阈值（秒），超时则自动重启 |

---

## 三、引擎初始化流程

```
AQAEngine(config.yaml)
    │
    ├─ 1. AQAConfig.__init__() — 加载 YAML + 环境变量覆盖
    │
    ├─ 2. _init_security()     — 检查 security.enabled + secret
    │
    ├─ 3. _init_transport()    — 根据 transport.backend 创建 Transport 实例
    │      │
    │      └─ TRANSPORT_MAP:
    │           "redis-streams" → RedisStreamsTransport(redis_url)
    │           "kafka"         → KafkaTransport(servers)
    │           "in-memory"     → InMemoryTransport()
    │
    ├─ 4. _init_plugins()      — 遍历 plugins.{name}, import class, register
    │
    ├─ 5. _init_agents()       — 遍历 agents.{id}, 创建 Agent (type + targets), subscribe
    │
    └─ 6. Supervisor.start_all() — 启动所有 Agent + 心跳监控
```

---

## 四、AQAConfig API

```python
class AQAConfig:
    transport_backend: str       # transport.backend
    redis_url: str               # transport.redis_url (支持环境变量)
    kafka_servers: str           # transport.kafka_servers
    debug: bool                  # app.debug
    agent_configs: dict          # agents 段
    plugin_configs: dict         # plugins 段

    def get(*keys, default=None)  # 深层取值: config.get("security", "enabled")
    def raw() -> dict             # 返回完整配置字典
```
