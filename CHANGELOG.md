# AQA Changelog

本文件记录 AQA 项目的所有代码修改。按时间倒序排列。

---

## 2026-06-27 — 测试修复 + 全流程验证

### 修复
- **`tests/test_aqa.py`** — 为 Judge 和 Reporter 添加 Inbox 订阅，修复全流程路由
  - `judge.subscribe_to(Topic.agent_inbox("judge-1"))`
  - `reporter.subscribe_to(Topic.agent_inbox("reporter-1"))`
  - 原因：`_consume_loop` 将带 target 的 reply 路由到 `aqa:inbox:{target}`，但 Agent 只订阅了角色 topic

### 验证
- 全部 37 项测试通过（E2E 全链路 + 单元测试）
- 全流程验证：TASK_DISPATCH → TASK_RESULT → JUDGE_VERDICT → REPORT_DELIVER

---

## 2026-06-27 — Review #15: 幂等去重

### 新增: `aqa/agent/base.py`
- `Agent.__init__()` 添加 `_processed_ids: set[str]` 缓存已处理消息 ID
- `Agent.__init__()` 添加 `_idempotency_max_size: int = 10000` 缓存上限
- `_consume_loop()` 在 `message.source == self.agent_id` 回声检测之后增加幂等检查
- 超过缓存上限时裁剪到 5000 条

### 原因
- 队列投递语义为 at-least-once，相同 message_id 可能被投递多次
- 幂等去重避免重复处理导致状态错乱

---

## 2026-06-27 — Review #14: SDK validate_message 同步化

### 新增: `tests/test_aqa.py`
- `TestCrossLayerConsistency.test_validate_message_sync` — 验证 core 和 SDK 的 validate_message 在同一输入上输出一致
- 测试 5 个用例：合法消息、缺失必填字段、非法类型、版本不匹配、空 payload

### 修复: `sdk/aqa_sdk/message.py`
- `validate_message()` 从异步同步化，移除 `async def` 标签
- 使 SDK 和 Core 的校验函数签名一致

---

## 2026-06-27 — Review #12: 测试假断言修复

### 修复: `tests/test_aqa.py`
- `test_agent_send_receive`: 替换 `assert True` → 检查 `transport.published` 包含 `TASK_RESULT` 和 `JUDGE_REQUEST`
- `test_full_flow_in_memory`: 替换 `assert True` → 检查完整消息链 `TASK_RESULT` → `JUDGE_VERDICT` → `REPORT_DELIVER`
- 修复竞态条件：在 publish 前添加 `await asyncio.sleep(0.05)` 确保 consume loop 启动

### 辅助修复: `tests/test_aqa.py`
- `_TestTransport.publish()` 增加记录 `self.published.append((t, message))` 追踪发布

---

## 2026-06-27 — Review #9: Redis Streams ack 消费者组参数化

### 修改: `aqa/transport/redis_streams.py`
- `ack()` 方法签名从 `ack(self, topic, message_id)` 改为 `ack(self, topic, message_id=None, group="aqa-default")`
- 添加 `group` 参数，允许指定消费者组

### 修改: `aqa/transport/base.py`
- `Transport.ack()` 抽象方法签名同步更新

### 修改: `aqa/transport/kafka_transport.py`
- `ack()` 签名同步更新

### 修改: `aqa/transport/inmemory.py`
- `ack()` 签名同步更新，保持 no-op

### 修改: `aqa/agent/base.py`
- `_consume_loop()` 在 ack 调用中传递 `self._last_group`

### 修复
- 修复 `_consume_loop` 第 173 行引用未定义变量 `group` 的 bug，改为 `self._group`

---

## 2026-06-27 — Review #8: ReporterAgent 改用 self.send()

### 修改: `aqa/agent/reporter.py`
- 广播发布从 `await self._transport.publish(Topic.BROADCAST, broadcast)` 改为 `await self.send(broadcast)`
- `send()` 方法根据 target 字段自动路由到 inbox 或 broadcast

### 原因
- 统一 Agent 发布语义，让 Reporter 和其他 Agent 使用相同的发布路径
- 不需要手动指定 topic

---

## 2026-06-27 — Review #5: InMemoryTransport 提取

### 新增: `aqa/transport/inmemory.py`
- 从 `examples/demo.py` 提取 `InMemoryTransport` 类到独立模块（68 行）
- 完整实现 Transport ABC 的 5 个方法

### 修改: `examples/demo.py`
- 移除内联 `InMemoryTransport` 类
- 导入改为 `from aqa.transport.inmemory import InMemoryTransport`

### 修改: `aqa/core/engine.py`
- `TRANSPORT_MAP` 注册 `"in-memory"` → `InMemoryTransport`

### 修改: `tests/test_aqa.py`
- 使用 `InMemoryTransport` 替代原有的 _TestTransport（简化版 InMemory）

---

## 2026-06-27 — Review #4: Agent 目标可配置

### 修改: `aqa/agent/probe.py`
- `ProbeAgent.__init__()` 添加 `judge_target: str = "judge-1"` 参数
- 用于 JUDGE_REQUEST 消息的 target 字段

### 修改: `aqa/agent/judge.py`
- `JudgeAgent.__init__()` 添加 `reporter_target: str = "reporter-1"` 参数
- 用于 REPORT_REQUEST 消息的 target 字段

### 修改: `aqa/core/engine.py`
- `_init_agents()` 从 config 读取 `targets` 段并透传给 Agent 构造函数
- 使用 `**cfg.get("targets", {})` 传递目标参数

---

## 2026-06-27 — 架构重构（初始阶段）

### 新增
- `aqa/agent/base.py` — Agent 基类 v2（心跳双通道、DLQ、重试、插件追踪、优雅关闭、幂等去重）
- `aqa/agent/supervisor.py` — Agent 生命周期总管
- `aqa/transport/kafka_transport.py` — 完整 Kafka 实现
- `aqa/core/dlq.py` — 死信队列
- `aqa/core/security.py` — Payload AES-256-GCM 加密
- `aqa/plugins/trace_collector.py` — 链路追踪插件
- `aqa/plugins/validator.py` — 验证插件
- `aqa/plugins/scorer.py` — 评分插件

### 修改
- `aqa/core/message.py` — 重构 Message 类，添加工厂函数和枚举
- `aqa/core/engine.py` — 重构 Engine，加载配置驱动 Agent 创建
- `aqa/plugin/registry.py` — 实现插件注册/卸载
- `aqa/plugin/base.py` — Plugin ABC

### 修复
- `examples/demo.py` — 适配新 API（Agent 构造函数签名、subscribe_to、start/stop 生命周期）

---

## 2026-06-26 — 初始搭建

### 新增
- 项目目录结构、setup.py、requirements.txt
- `aqa/core/message.py` — Message 类和枚举
- `aqa/core/engine.py` — 初始 AQAEngine
- `aqa/core/config.py` — YAML 配置加载器
- `aqa/transport/base.py` — Transport ABC
- `aqa/transport/redis_streams.py` — Redis Streams Transport
- `aqa/agent/base.py` — Agent 基类 v1
- `aqa/agent/probe.py` — Probe Agent
- `aqa/agent/judge.py` — Judge Agent
- `aqa/agent/reporter.py` — Reporter Agent
- `aqa/plugin/base.py` — Plugin ABC
- `aqa/plugin/registry.py` — 插件注册中心
- `aqa/sdk/` — 外部 Agent SDK（Python）
- `examples/demo.py` — 演示脚本
- `config.yaml` — 完整配置
- `tests/test_aqa.py` — 初始测试
- `PROTOCOL.md` — 通信协议规范
- `README.md` — 项目文档
- Dockerfile, docker-compose.yml
