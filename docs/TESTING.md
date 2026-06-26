# 测试策略

## 一、测试概览

AQA 共 **37 项测试**，分布在两个目录：

```
tests/
├── conftest.py          # pytest-asyncio 配置
└── test_aqa.py          # 核心测试 (450 行, ~30 项)
sdk/tests/
├── conftest.py          # SDK 测试配置
└── test_sdk.py          # SDK 测试 (163 行, ~7 项)
```

全部测试无需外部依赖（Redis / Kafka），使用 `InMemoryTransport` 运行。

## 二、运行测试

```bash
# 全部测试
PYTHONPATH=. ./venv/bin/python -m pytest tests/ sdk/tests/ -v

# 仅核心测试
PYTHONPATH=. ./venv/bin/python -m pytest tests/ -v

# 仅 SDK 测试
PYTHONPATH=. ./venv/bin/python -m pytest sdk/tests/ -v

# 带覆盖率
PYTHONPATH=. ./venv/bin/python -m pytest tests/ sdk/tests/ -v --cov=aqa

# 指定测试方法
PYTHONPATH=. ./venv/bin/python -m pytest tests/test_aqa.py::TestAgentLifecycle -v
```

## 三、测试分类

### 3.1 消息协议 (`TestMessageProtocol`)

| 测试方法 | 验证内容 |
|---|---|
| `test_message_creation` | Message 构造、默认值填充 |
| `test_message_to_json` | 序列化为 JSON 的正确性 |
| `test_message_from_json` | 反序列化的正确性 |
| `test_envelope_structure` | JSON 信封所有字段的存在性 |
| `test_topic_enum` | Topic 枚举值与预期字符串一致 |
| `test_message_type_enum` | MessageType 枚举值与预期字符串一致 |

### 3.2 Agent 生命周期 (`TestAgentLifecycle`)

| 测试方法 | 验证内容 |
|---|---|
| `test_agent_start_stop` | Agent start/stop 基本流程 |
| `test_agent_subscribe_topics` | 订阅多个 topic 后的启动/停止 |
| `test_agent_heartbeat` | 心跳消息发送到 broadcast + system_events |

### 3.3 Agent 通信 (`TestAgentCommunication`)

| 测试方法 | 验证内容 |
|---|---|
| `test_agent_send_receive` | Probe 收到 TASK_DISPATCH 后发布 TASK_RESULT + JUDGE_REQUEST |
| `test_full_flow_in_memory` | 完整流程：TASK_DISPATCH → TASK_RESULT → JUDGE_VERDICT → REPORT_DELIVER |

### 3.4 错误处理 (`TestErrorHandling`)

| 测试方法 | 验证内容 |
|---|---|
| `test_unknown_message_type` | 收到不识别的 type 后发 ERROR |
| `test_version_mismatch` | 收到不兼容版本后发 ERROR |
| `test_malformed_message` | 格式错误消息跳过处理 |

### 3.5 DLQ (`TestDLQ`)

| 测试方法 | 验证内容 |
|---|---|
| `test_dlq_message_creation` | DLQ 消息构造 |
| `test_dlq_topic_constant` | DLQ_TOPIC 常量 |

### 3.6 配置 (`TestConfig`)

| 测试方法 | 验证内容 |
|---|---|
| `test_config_load_basic` | 配置加载 |
| `test_config_get` | 深层取值 |
| `test_default_values` | 默认值 |

### 3.7 安全 (`TestSecurity`)

| 测试方法 | 验证内容 |
|---|---|
| `test_encrypt_decrypt` | AES-256-GCM 加密解密 |
| `test_validate_message_sync` | Core 和 SDK 的 validate_message 输出一致 |

### 3.8 跨层一致性 (`TestCrossLayerConsistency`)

| 测试方法 | 验证内容 |
|---|---|
| `test_validate_message_sync` | 同步模式下 core 和 SDK 校验一致 |
| `test_message_type_enum_consistent` | 消息类型枚举值跨层一致 |

### 3.9 插件 (`TestPlugin`)

| 测试方法 | 验证内容 |
|---|---|
| `test_plugin_register` | 插件注册与列出 |
| `test_plugin_execute` | 插件执行与结果格式 |
| `test_plugin_initialize_cleanup` | 插件初始化与清理 |

### 3.10 SDK 测试 (`sdk/tests/test_sdk.py`)

| 测试方法 | 验证内容 |
|---|---|
| `test_sdk_message_create` | SDK Message 构造 |
| `test_sdk_message_to_json` | SDK 序列化 |
| `test_sdk_message_from_json` | SDK 反序列化 |
| `test_sdk_validate_message` | SDK 校验函数 |
| `test_sdk_consumer_connect` | SDK 消费者连接 |

## 四、测试基础设施

### InMemoryTransport

所有测试使用 `InMemoryTransport`（`aqa/transport/inmemory.py`），纯异步队列模拟。

```python
transport = InMemoryTransport()
transport.published  # [(topic, message), ...] — 记录所有发布
```

### 测试用 Agent

测试中直接使用 `ProbeAgent`、`JudgeAgent`、`ReporterAgent` 的真实实例，不 mock。

```python
probe = ProbeAgent("probe-1", transport)
probe.subscribe_to(Topic.AGENT_PROBE)
await probe.start()
# ... 发消息 ...
await probe.stop()
```

### 竞态条件处理

异步测试中，在 publish 前添加 `await asyncio.sleep(0.05)` 确保 consume loop 已启动。

## 五、添加测试指南

1. **测试文件**：核心测试在 `tests/test_aqa.py`，SDK 测试在 `sdk/tests/test_sdk.py`
2. **测试类**：按功能分组（`TestMessageProtocol`、`TestAgentCommunication` 等）
3. **测试方法**：以 `test_` 开头，描述性命名
4. **pytest-asyncio**：所有异步测试自动通过 conftest 的 `event_loop_policy` 配置支持
5. **不用 mock**：优先使用真实组件 + InMemoryTransport，避免 mock 隐藏真实行为
6. **断言**：每个测试至少一个断言，不要留 `assert True`
