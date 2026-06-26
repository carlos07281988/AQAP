1|# Transport 层
2|
3|Transport 是 AQAP 的消息队列抽象层。所有队列后端实现统一接口，通过 `config.yaml` 切换。
4|
5|```
6|Transport (ABC)
7|    │
8|    ├─ InMemoryTransport       — 纯内存 (测试/演示)
9|    ├─ RedisStreamsTransport   — Redis Streams (生产)
10|    └─ KafkaTransport          — Kafka (高吞吐)
11|```
12|
13|---
14|
15|## 一、Transport 接口
16|
17|定义在 `aqap/transport/base.py`，共 5 个方法 + 1 个属性：
18|
19|```python
20|class Transport(ABC):
21|    async def connect(self) -> None
22|    async def disconnect(self) -> None
23|    async def publish(self, topic: str | Topic, message: Message) -> None
24|    async def subscribe(self, topic: str | Topic, group: str, consumer: str) -> AsyncGenerator[Message, None]
25|    async def ack(self, topic: str | Topic, message_id: str | None, group: str) -> None
26|    async def create_group(self, topic: str | Topic, group: str) -> None
27|    @property
28|    def name(self) -> str
29|```
30|
31|### 方法详解
32|
33|| 方法 | 输入 | 输出 | 说明 |
34||---|---|---|---|
35|| `connect()` | — | — | 建立与消息队列的连接 |
36|| `disconnect()` | — | — | 关闭连接，释放资源 |
37|| `publish(topic, message)` | topic 名 + Message 对象 | — | 发布消息。内部序列化为 JSON |
38|| `subscribe(topic, group, consumer)` | topic/group/consumer | `AsyncGenerator[Message]` | 持续消费消息的异步生成器 |
39|| `ack(topic, message_id, group)` | topic/message_id/group | — | 确认消息已处理 |
40|| `create_group(topic, group)` | topic + 组名 | — | 创建消费者组，幂等 |
41|| `name` | — | str | Transport 标识名 |
42|
43|### 调用约定
44|
45|1. 调用顺序：`connect()` → `publish / subscribe` → `ack` → `disconnect()`
46|2. `subscribe()` 返回的 generator 必须在关闭时退出生效
47|3. `ack()` 是在收到消息并成功处理后调用的
48|4. `create_group()` 必须幂等（group 已存在时静默跳过）
49|
50|---
51|
52|## 二、InMemoryTransport
53|
54|路径：`aqap/transport/inmemory.py` (67 行)
55|
56|**用途**：测试和演示，无需外部依赖。
57|
58|### 实现原理
59|
60|- 使用 `asyncio.Queue` 模拟消息队列
61|- `_subscribers: dict[str, list[asyncio.Queue]]` 管理订阅者
62|- `publish()` 遍历所有订阅该 topic 的队列，逐一 `put(message)`
63|- `subscribe()` 创建新队列加入订阅列表，yield 消息直到 `_running = False`
64|- `ack()` 为 no-op（内存消息无需确认）
65|- `create_group()` 为 no-op（内存无需消费组）
66|
67|### 实现细节
68|
69|```python
70|class InMemoryTransport(Transport):
71|    _queues: dict[str, asyncio.Queue]         # 消息队列
72|    _subscribers: dict[str, list[asyncio.Queue]]  # 订阅者列表
73|    _running: bool                             # 运行标志
74|
75|    # publish: 将消息投递给所有该 topic 的订阅者
76|    # subscribe: 新建 asyncio.Queue 加入订阅, 1 秒超时轮询
77|    # cleanup: subscribe generator exit 时自动从订阅列表移除
78|```
79|
80|### 使用场景
81|
82|- 单元测试（`tests/test_aqap.py`, `sdk/tests/`）
83|- 演示脚本（`examples/demo.py`）
84|- 本地开发调试
85|
86|---
87|
88|## 三、RedisStreamsTransport
89|
90|路径：`aqap/transport/redis_streams.py` (135 行)
91|
92|**用途**：生产环境，高可用消息队列。
93|
94|### 实现原理
95|
96|- 使用 Redis Streams 数据结构
97|- 每个 topic 对应一个 Redis Stream key
98|- 消费者组模式（`XGROUP`）支持负载均衡和消息重试
99|- PEL（Pending Entries List）实现 at-least-once 投递
100|
101|```python
102|class RedisStreamsTransport(Transport):
103|    _redis_url: str             # Redis 连接地址
104|    _redis: Redis               # Redis 连接实例
105|    _groups: dict[str, str]     # 已创建的消费者组缓存
106|
107|    # publish: redis.xadd(stream_key, {"message": json.dumps(msg.to_dict())})
108|    # subscribe: redis.xreadgroup(GROUP group consumer BLOCK 1000 STREAMS stream_key >)
109|    # ack: redis.xack(stream_key, group, message_id)
110|    # create_group: redis.xgroup_create(stream_key, group, mkstream=True)
111|```
112|
113|### 配置
114|
115|在 `config.yaml` 中：
116|
117|```yaml
118|transport:
119|  backend: redis-streams
120|  redis_url: "redis://127.0.0.1:***@property
225|    def name(self) -> str:
226|        return "rabbitmq"
227|
228|    async def connect(self):
229|        # ... 连接 RabbitMQ
230|
231|    async def publish(self, topic, message):
232|        # ... channel.basic_publish
233|
234|    async def subscribe(self, topic, group, consumer):
235|        # ... channel.basic_consume
236|
237|    async def ack(self, topic, message_id, group):
238|        # ... channel.basic_ack
239|
240|    async def create_group(self, topic, group):
241|        # ... queue_declare
242|```
243|