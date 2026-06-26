# Transport 层

Transport 是 AQA 的消息队列抽象层。所有队列后端实现统一接口，通过 `config.yaml` 切换。

```
Transport (ABC)
    │
    ├─ InMemoryTransport       — 纯内存 (测试/演示)
    ├─ RedisStreamsTransport   — Redis Streams (生产)
    └─ KafkaTransport          — Kafka (高吞吐)
```

---

## 一、Transport 接口

定义在 `aqa/transport/base.py`，共 5 个方法 + 1 个属性：

```python
class Transport(ABC):
    async def connect(self) -> None
    async def disconnect(self) -> None
    async def publish(self, topic: str | Topic, message: Message) -> None
    async def subscribe(self, topic: str | Topic, group: str, consumer: str) -> AsyncGenerator[Message, None]
    async def ack(self, topic: str | Topic, message_id: str | None, group: str) -> None
    async def create_group(self, topic: str | Topic, group: str) -> None
    @property
    def name(self) -> str
```

### 方法详解

| 方法 | 输入 | 输出 | 说明 |
|---|---|---|---|
| `connect()` | — | — | 建立与消息队列的连接 |
| `disconnect()` | — | — | 关闭连接，释放资源 |
| `publish(topic, message)` | topic 名 + Message 对象 | — | 发布消息。内部序列化为 JSON |
| `subscribe(topic, group, consumer)` | topic/group/consumer | `AsyncGenerator[Message]` | 持续消费消息的异步生成器 |
| `ack(topic, message_id, group)` | topic/message_id/group | — | 确认消息已处理 |
| `create_group(topic, group)` | topic + 组名 | — | 创建消费者组，幂等 |
| `name` | — | str | Transport 标识名 |

### 调用约定

1. 调用顺序：`connect()` → `publish / subscribe` → `ack` → `disconnect()`
2. `subscribe()` 返回的 generator 必须在关闭时退出生效
3. `ack()` 是在收到消息并成功处理后调用的
4. `create_group()` 必须幂等（group 已存在时静默跳过）

---

## 二、InMemoryTransport

路径：`aqa/transport/inmemory.py` (67 行)

**用途**：测试和演示，无需外部依赖。

### 实现原理

- 使用 `asyncio.Queue` 模拟消息队列
- `_subscribers: dict[str, list[asyncio.Queue]]` 管理订阅者
- `publish()` 遍历所有订阅该 topic 的队列，逐一 `put(message)`
- `subscribe()` 创建新队列加入订阅列表，yield 消息直到 `_running = False`
- `ack()` 为 no-op（内存消息无需确认）
- `create_group()` 为 no-op（内存无需消费组）

### 实现细节

```python
class InMemoryTransport(Transport):
    _queues: dict[str, asyncio.Queue]         # 消息队列
    _subscribers: dict[str, list[asyncio.Queue]]  # 订阅者列表
    _running: bool                             # 运行标志

    # publish: 将消息投递给所有该 topic 的订阅者
    # subscribe: 新建 asyncio.Queue 加入订阅, 1 秒超时轮询
    # cleanup: subscribe generator exit 时自动从订阅列表移除
```

### 使用场景

- 单元测试（`tests/test_aqa.py`, `sdk/tests/`）
- 演示脚本（`examples/demo.py`）
- 本地开发调试

---

## 三、RedisStreamsTransport

路径：`aqa/transport/redis_streams.py` (135 行)

**用途**：生产环境，高可用消息队列。

### 实现原理

- 使用 Redis Streams 数据结构
- 每个 topic 对应一个 Redis Stream key
- 消费者组模式（`XGROUP`）支持负载均衡和消息重试
- PEL（Pending Entries List）实现 at-least-once 投递

```python
class RedisStreamsTransport(Transport):
    _redis_url: str             # Redis 连接地址
    _redis: Redis               # Redis 连接实例
    _groups: dict[str, str]     # 已创建的消费者组缓存

    # publish: redis.xadd(stream_key, {"message": json.dumps(msg.to_dict())})
    # subscribe: redis.xreadgroup(GROUP group consumer BLOCK 1000 STREAMS stream_key >)
    # ack: redis.xack(stream_key, group, message_id)
    # create_group: redis.xgroup_create(stream_key, group, mkstream=True)
```

### 配置

在 `config.yaml` 中：

```yaml
transport:
  backend: redis-streams
  redis_url: "redis://127.0.0.1:6379"
```

环境变量 `REDIS_URL` 可覆盖配置中的 `redis_url`。

### 依赖

```bash
pip install redis>=5.0
```

### 注意事项

- `create_group()` 使用 `mkstream=True` 确保 stream 不存在时自动创建
- 消息 ID 的格式为 `{timestamp}-{seq}`，在 ACK 时传递给 Redis
- XREADGROUP 的 BLOCK 超时设为 1000ms，支持 graceful shutdown

---

## 四、KafkaTransport

路径：`aqa/transport/kafka_transport.py` (125 行)

**用途**：高吞吐场景，跨数据中心消息分发。

### 实现原理

- 使用 aiokafka 异步客户端
- 每个 topic 对应一个 Kafka topic
- 消费者组自动负载均衡
- offset 自动提交

```python
class KafkaTransport(Transport):
    _servers: str              # Kafka broker 地址
    _producer: AIOKafkaProducer
    _consumer: AIOKafkaConsumer | None  # 当前消费者的引用
    _groups: dict[str, str]

    # publish: producer.send(topic, value=json.dumps(msg.to_dict()).encode())
    # subscribe: AIOKafkaConsumer(topic, group_id=group, bootstrap_servers=...)
    # ack: consumer.commit()
    # create_group: Kafka 消费者组自动管理
```

### 配置

```yaml
transport:
  backend: kafka
  kafka_servers: "127.0.0.1:9092"
```

### 依赖

```bash
pip install aiokafka>=0.10.0
```

### 与 Redis Streams 的差异

| 特性 | Redis Streams | Kafka |
|---|---|---|
| 消息持久化 | RDB/AOF 文件 | 磁盘日志 |
| 消费组模型 | XREADGROUP | 消费者组协议 |
| ACK 机制 | XACK | commit() (offset 提交) |
| 消息顺序 | 单分区保序 | 单分区保序 |
| 典型场景 | 中小规模, 低延时 | 大规模, 高吞吐 |

---

## 五、Transport 注册与发现

Engine 启动时自动发现所有 Transport 实现：

```python
# aqa/core/engine.py
TRANSPORT_MAP = {}

def _discover_transports():
    TRANSPORT_MAP["redis-streams"] = RedisStreamsTransport
    TRANSPORT_MAP["in-memory"] = InMemoryTransport
    try:
        TRANSPORT_MAP["kafka"] = KafkaTransport
    except ImportError:
        pass  # Kafka 依赖可选
```

Engine 根据 `config.transport_backend` 从 `TRANSPORT_MAP` 中查找并实例化。

---

## 六、新增 Transport 实现

实现一个新的后端只需三步：

1. **继承 `Transport`** 并实现全部 5 个方法 + 1 个属性
2. **在 `TRANSPORT_MAP` 中注册**
3. **在 `config.yaml` 中配置 `transport.backend`**

示例（RabbitMQ）：

```python
class RabbitMQTransport(Transport):
    @property
    def name(self) -> str:
        return "rabbitmq"

    async def connect(self):
        # ... 连接 RabbitMQ

    async def publish(self, topic, message):
        # ... channel.basic_publish

    async def subscribe(self, topic, group, consumer):
        # ... channel.basic_consume

    async def ack(self, topic, message_id, group):
        # ... channel.basic_ack

    async def create_group(self, topic, group):
        # ... queue_declare
```
