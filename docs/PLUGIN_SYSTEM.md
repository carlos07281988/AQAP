# 插件系统

插件是 AQA Agent 处理 `payload` 时的扩展点。插件不参与消息路由，只负责业务逻辑。

```
收到消息 → 反序列化 → 路由到 Agent → 执行插件链 → 构建回复 → 发布
                                       │
                                ┌──────┴──────┐
                                │ Plugin.      │
                                │ execute(ctx) │
                                └─────────────┘
```

---

## 一、Plugin 基类

定义在 `aqa/plugin/base.py` (43 行)：

```python
class Plugin(ABC):
    @property
    @abstractmethod
    def name(self) -> str: ...
    # 插件唯一标识名

    @property
    @abstractmethod
    def version(self) -> str: ...
    # 插件版本 (语义化)

    @property
    def description(self) -> str: ...
    # 插件描述 (可选)

    @abstractmethod
    async def initialize(self, config: dict) -> None: ...
    # 插件初始化 (启动时调用)

    @abstractmethod
    async def execute(self, context: dict) -> dict: ...
    # 插件执行 — 核心逻辑

    async def cleanup(self) -> None: ...
    # 插件清理 (关闭时调用, 可选)
```

### 插件契约

1. `initialize(config)` 在 Agent 启动时调用，config 来自 `config.yaml`
2. `execute(context)` 接收一个 `dict` 上下文，返回一个 `dict` 结果
3. 执行失败时抛出异常，由 PluginRegistry 捕获并记录到结果中
4. `cleanup()` 在 Agent 关闭时调用，用于释放资源

### 返回值格式

`execute()` 返回的结果会被 PluginRegistry 包裹为：

```python
{
    "plugin": plugin.name,
    "version": plugin.version,
    "result": <execute() 的返回值>,
    "error": None,  # 或异常信息
}
```

---

## 二、PluginRegistry

定义在 `aqa/plugin/registry.py` (99 行)：

### 全局单例

```python
from aqa.plugin.registry import registry

# registry 是模块级全局单例
registry = PluginRegistry()
```

### 方法一览

| 方法 | 参数 | 说明 |
|---|---|---|
| `register(plugin, topics)` | plugin 实例 + topic 列表 | 注册插件到指定 topic |
| `unregister(name)` | 插件名 | 卸载插件 |
| `execute_all(topic, context)` | topic + 上下文 | 执行 topic 下所有插件 |
| `get(name)` | 插件名 | 获取插件实例 |
| `list()` | — | 列出所有插件 `{name: version}` |
| `initialize_all(config)` | 配置 | 初始化所有插件 |
| `cleanup_all()` | — | 清理所有插件 |
| `clear()` | — | 清空注册表 |
| `count` | — | 插件总数 (属性) |

### execute_all 执行流程

```
execute_all(topic, context)
    │
    ├─ 找到 topic 下绑定的所有插件
    │
    ├─ 依次执行每个插件:
    │     │
    │     ├─ plugin.initialize() 未调用? → 先初始化
    │     ├─ plugin.execute(context)
    │     ├─ 成功 → {"plugin": name, "version": v, "result": r, "error": None}
    │     └─ 异常 → {"plugin": name, "version": v, "result": None, "error": str(e)}
    │
    └─ 返回 list[dict] — 所有插件的结果
```

插件按注册顺序执行，一个插件的失败不影响后续插件。

### 注册示例

```python
# 代码注册
from aqa.plugin.registry import registry
from aqa.plugins.validator import ValidatorPlugin

registry.register(ValidatorPlugin(), topics=["probe", "judge"])

# 或通过 config.yaml
# plugins:
#   validator:
#     class: "aqa.plugins.validator.ValidatorPlugin"
#     enabled: true
#     topic_bind: ["probe", "judge"]
```

### topic_bind 机制

插件可以绑定到一个或多个 `topic`（注意：这里 topic 指的是 Agent 处理逻辑的阶段名，如 `"probe"`、`"judge"`、`"reporter"`，与消息队列的 topic 不同）。

Agent 调用 `run_plugins("probe", context)` 时，Registry 只执行绑定到 `"probe"` 的插件。

---

## 三、内置插件

### 1. ValidatorPlugin (`aqa/plugins/validator.py`)

验证检测结果中必填字段是否存在。

```python
class ValidatorPlugin(Plugin):
    name = "validator"
    version = "1.0.0"

    # config:
    #   required_fields: ["task_id", "passed", "plugin_results"]
```

**执行逻辑**：
- 检查 context 的每一项是否包含所有 `required_fields`
- 返回缺失字段列表
- `passed` = 无缺失字段

### 2. ScorerPlugin (`aqa/plugins/scorer.py`)

加权评分插件。

```python
class ScorerPlugin(Plugin):
    name = "scorer"
    version = "1.0.0"

    # config:
    #   weights:
    #     pass_rate: 0.5
    #     field_completeness: 0.3
    #     plugin_health: 0.2
```

**执行逻辑**：
- 从 context 提取 `pass_rate`、`field_completeness`、`plugin_health`
- 加权求和得到 `total_score`
- 默认权重：pass_rate=0.5, field_completeness=0.3, plugin_health=0.2

### 3. TraceCollector (`aqa/plugins/trace_collector.py`)

链路追踪数据收集器。

```python
class TraceCollector(Plugin):
    name = "trace_collector"
    version = "1.0.0"

    # config:
    #   report_interval: 60
```

**执行逻辑**：
- 从 context 自动注入的 `_aqa_start_time`、`_aqa_trace_id` 等字段收集追踪数据
- 统计处理耗时、消息类型、来源 Agent
- 达到 `report_interval` 条后输出统计报告

### 4. DemoCheckPlugin (`examples/demo.py`)

演示用检测插件——总是返回通过。

```python
class DemoCheckPlugin(Plugin):
    name = "demo-check"
    version = "1.0.0"
```

---

## 四、编写自定义插件

### 最小示例

```python
from aqa.plugin.base import Plugin

class MyPlugin(Plugin):
    @property
    def name(self) -> str:
        return "my-plugin"

    @property
    def version(self) -> str:
        return "0.1.0"

    async def initialize(self, config: dict) -> None:
        self.timeout = config.get("timeout", 30)

    async def execute(self, context: dict) -> dict:
        # context 包含当前消息的 payload
        # 返回业务结果
        return {
            "checked": True,
            "issues": [],
        }

    async def cleanup(self) -> None:
        pass
```

### 注册方式

**方式一：代码注册**

```python
from aqa.plugin.registry import registry

registry.register(MyPlugin(), topics=["probe"])
```

**方式二：config.yaml 注册**

```yaml
plugins:
  my-plugin:
    class: "my_module.MyPlugin"
    enabled: true
    topic_bind:
      - "probe"
    config:
      timeout: 30
```

### 自动注入的上下文

Agent 调用 `run_plugins()` 时自动注入以下字段：

| 字段 | 类型 | 说明 |
|---|---|---|
| `_aqa_start_time` | float | 消息处理开始时间戳 |
| `_aqa_trace_id` | str | 当前消息的 trace_id |
| `_aqa_message_type` | str | 当前消息的类型 |
| `_aqa_source` | str | 当前消息的 source |
