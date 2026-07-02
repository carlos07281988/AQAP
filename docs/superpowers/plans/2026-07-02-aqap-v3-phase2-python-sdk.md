# AQAP v3 Python SDK — Phase 2 Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the Python SDK surface layer — `AQAP` app class (functional API), `@agent` decorator (declarative API), `Context` object, `Middleware` system, `Config`, and kernel-integrated Transport — all composing the Phase 1 Rust kernel.

**Architecture:** Pure Python layer wrapping the Rust kernel via `aqap.kernel`. The `AQAP` class is the central orchestrator: it holds a Transport, SchemaRegistry, Router, and SecurityContext, and exposes `on()` for handler registration and `run()` for lifecycle management. The `@agent` decorator compiles a class into the same internal representation. Middleware is a chain of `before`/`after` hooks. Transport backends use the kernel for wire encode/decode on publish/subscribe.

**Tech Stack:** Python 3.10+, asyncio, aqap.kernel (Rust PyO3), pytest-asyncio

## Global Constraints

- Python 3.10+ (match statement support)
- All public functions must have type annotations (strict mode)
- asyncio throughout — no synchronous blocking I/O
- Kernel wire functions (`wire_message_encode`, `wire_message_decode`) used for all serialization
- Kernel schema functions (`SchemaRegistry.validate`) used for all validation
- v1 code in `aqap/core/` and `aqap/agent/` is NOT modified — Phase 2 creates new files alongside
- TDD: failing test first, then implementation

---

## File Structure

```
Creating:
  aqap/v3/__init__.py          # v3 SDK public API
  aqap/v3/app.py               # AQAP class (functional entry point)
  aqap/v3/agent.py             # @agent decorator + Agent base class
  aqap/v3/config.py            # Config dataclass
  aqap/v3/context.py           # Context object (trace_id, log, metrics, storage)
  aqap/v3/middleware.py         # Middleware base + built-in middleware
  aqap/v3/transport.py          # Transport integration with kernel
  aqap/v3/serializer.py         # Kernel-bridged serializer

Modifying:
  aqap/__init__.py              # Add v3 re-exports
  README.md                     # Update SDK section with Phase 2 status

Testing:
  tests/test_v3_app.py          # AQAP class tests
  tests/test_v3_agent.py        # @agent decorator tests
  tests/test_v3_context.py      # Context object tests
  tests/test_v3_middleware.py   # Middleware tests
  tests/test_v3_integration.py  # End-to-end: app + kernel + transport
```

---

### Task 1: Config — Configuration Dataclass

**Files:**
- Create: `aqap/v3/__init__.py`
- Create: `aqap/v3/config.py`
- Test: `tests/test_v3_config.py`

**Interfaces:**
- Produces: `Config` dataclass, `SecurityConfig` dataclass, `Config.from_yaml(path) -> Config`, `Config.from_env() -> Config`

- [ ] **Step 1: Write failing test**

```python
# tests/test_v3_config.py
import pytest
from aqap.v3.config import Config, SecurityConfig


class TestConfig:
    def test_defaults(self):
        """Config should have sensible defaults."""
        c = Config()
        assert c.transport == "redis://localhost:6379"
        assert c.protocol_version == "3.0"
        assert c.agent_id == ""
        assert c.group == "aqap-default"
        assert c.max_retries == 3
        assert c.heartbeat_interval == 30
        assert c.concurrency == 0
        assert c.max_body_size == 10 * 1024 * 1024
        assert c.default_timeout_ms == 30_000

    def test_security_default(self):
        """SecurityConfig should default to disabled."""
        c = Config()
        assert c.security is not None
        assert c.security.enabled is False
        assert c.security.encrypt_payload is False
        assert c.security.sign_envelope is False

    def test_from_url_string(self):
        """Config should accept a URL string shorthand."""
        c = Config(transport="kafka://broker:9092")
        assert c.transport == "kafka://broker:9092"

    def test_security_enabled(self):
        """When security is enabled, key_source defaults to env."""
        c = Config(security=SecurityConfig(enabled=True))
        assert c.security.enabled is True
        assert c.security.key_source == "env"

    def test_transport_url_parsed(self):
        """Config should parse transport URL into scheme and host."""
        c = Config(transport="redis://user:pass@host:6379/0")
        assert c.transport_scheme == "redis"
        assert c.transport_host == "host"
        assert c.transport_port == 6379
```

Run: `python3 -m pytest tests/test_v3_config.py -v`
Expected: FAIL — `ModuleNotFoundError: No module named 'aqap.v3.config'`

- [ ] **Step 2: Implement Config**

```python
# aqap/v3/config.py
from __future__ import annotations

from dataclasses import dataclass, field
from urllib.parse import urlparse
from pathlib import Path


@dataclass
class SecurityConfig:
    """Security configuration."""
    enabled: bool = False
    key_source: str = "env"       # env | file | vault | aws-secrets
    key_path: str = ""
    encrypt_payload: bool = False
    sign_envelope: bool = False
    algorithm: str = "AES-256-GCM"
    key_rotation_interval: int = 86400  # seconds


@dataclass
class Config:
    """AQAP v3 configuration.

    Can be constructed from:
      - Direct parameters: Config(transport="redis://...")
      - YAML file: Config.from_yaml("aqap.yaml")
      - Environment: Config.from_env()
    """
    transport: str = "redis://localhost:6379"
    protocol_version: str = "3.0"
    agent_id: str = ""
    group: str = "aqap-default"
    max_retries: int = 3
    heartbeat_interval: int = 30
    concurrency: int = 0            # 0 = sequential
    max_body_size: int = 10 * 1024 * 1024
    default_timeout_ms: int = 30_000
    security: SecurityConfig = field(default_factory=SecurityConfig)

    @property
    def transport_scheme(self) -> str:
        """e.g. 'redis', 'kafka', 'memory'"""
        return urlparse(self.transport).scheme or "redis"

    @property
    def transport_host(self) -> str:
        return urlparse(self.transport).hostname or "localhost"

    @property
    def transport_port(self) -> int:
        return urlparse(self.transport).port or 6379

    @classmethod
    def from_yaml(cls, path: str | Path) -> "Config":
        """Load configuration from YAML file."""
        import yaml
        data = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
        return cls._from_dict(data.get("aqap", data))

    @classmethod
    def from_env(cls, prefix: str = "AQAP_") -> "Config":
        """Load configuration from environment variables."""
        import os
        kwargs: dict = {}
        for key, val in os.environ.items():
            if key.startswith(prefix):
                config_key = key[len(prefix):].lower()
                # Type coercion
                if val.lower() in ("true", "false"):
                    kwargs[config_key] = val.lower() == "true"
                elif val.isdigit():
                    kwargs[config_key] = int(val)
                else:
                    kwargs[config_key] = val
        return cls(**kwargs)

    @classmethod
    def _from_dict(cls, data: dict) -> "Config":
        sec = data.get("security", {})
        return cls(
            transport=data.get("transport", "redis://localhost:6379"),
            protocol_version=data.get("protocol_version", "3.0"),
            agent_id=data.get("agent_id", ""),
            group=data.get("group", "aqap-default"),
            max_retries=data.get("max_retries", 3),
            heartbeat_interval=data.get("heartbeat_interval", 30),
            concurrency=data.get("concurrency", 0),
            max_body_size=data.get("max_body_size", 10 * 1024 * 1024),
            default_timeout_ms=data.get("default_timeout_ms", 30_000),
            security=SecurityConfig(
                enabled=sec.get("enabled", False),
                key_source=sec.get("key_source", "env"),
                key_path=sec.get("key_path", ""),
                encrypt_payload=sec.get("encrypt_payload", False),
                sign_envelope=sec.get("sign_envelope", False),
            ),
        )
```

- [ ] **Step 3: Create __init__.py**

```python
# aqap/v3/__init__.py
"""AQAP v3 SDK — Python public API surface."""
from aqap.v3.config import Config, SecurityConfig

__all__ = ["Config", "SecurityConfig"]
```

- [ ] **Step 4: Run test**

Run: `python3 -m pytest tests/test_v3_config.py -v`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add aqap/v3/__init__.py aqap/v3/config.py tests/test_v3_config.py
git commit -m "feat(v3): Config dataclass with YAML, env, and URL parsing

- Config: all v3 settings with sensible defaults
- SecurityConfig: key_source, encrypt/sign toggles, rotation interval
- URL parsing: transport_scheme, transport_host, transport_port
- from_yaml(), from_env() class methods

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 2: Context — Per-Message Context Object

**Files:**
- Create: `aqap/v3/context.py`
- Modify: `aqap/v3/__init__.py` — export Context
- Test: `tests/test_v3_context.py`

**Interfaces:**
- Produces: `Context` dataclass with fields: trace_id, span_id, correlation_id, source, message_type, headers, log (Logger), metrics (Metrics), storage (KVStore); methods: reply(body) -> Awaitable[str], forward(topic) -> Awaitable[str], ack() -> Awaitable[None], nack(requeue=True) -> Awaitable[None]

- [ ] **Step 1: Write failing test**

```python
# tests/test_v3_context.py
import pytest
import logging
from uuid import uuid4
from aqap.v3.context import Context


class TestContext:
    def test_context_creation(self):
        """Context should hold all tracing fields."""
        trace_id = uuid4()
        ctx = Context(
            trace_id=trace_id,
            span_id=42,
            source="test-agent",
            message_type="task:dispatch",
        )
        assert ctx.trace_id == trace_id
        assert ctx.span_id == 42
        assert ctx.source == "test-agent"
        assert ctx.message_type == "task:dispatch"
        assert ctx.correlation_id is None

    def test_context_logger_has_trace_id(self):
        """Logger should automatically include trace_id in records."""
        ctx = Context(
            trace_id=uuid4(),
            span_id=1,
            source="test",
            message_type="task:dispatch",
        )
        assert isinstance(ctx.log, logging.Logger)
        assert ctx.log.name.startswith("aqap")

    def test_context_headers_readonly(self):
        """Headers should default to empty dict."""
        ctx = Context(
            trace_id=uuid4(),
            span_id=1,
            source="test",
            message_type="task:dispatch",
        )
        assert ctx.headers == {}

    def test_context_with_headers(self):
        """Headers should be storable."""
        ctx = Context(
            trace_id=uuid4(),
            span_id=1,
            source="test",
            message_type="task:dispatch",
            headers={"x-custom": "value"},
        )
        assert ctx.headers == {"x-custom": "value"}


class TestMetrics:
    def test_context_has_metrics(self):
        """Context should provide a Metrics object."""
        from aqap.v3.context import Metrics
        m = Metrics()
        assert m.get("messages.total") == 0
        m.inc("messages.total")
        assert m.get("messages.total") == 1
        m.inc("messages.total", 5)
        assert m.get("messages.total") == 6
```

Run: `python3 -m pytest tests/test_v3_context.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 2: Implement Context**

```python
# aqap/v3/context.py
from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Callable
from uuid import UUID


@dataclass
class Metrics:
    """Simple in-memory metrics collector.

    Expose to Prometheus via aqap admin metrics in Phase 5.
    """
    _counters: dict[str, int] = field(default_factory=dict)
    _gauges: dict[str, float] = field(default_factory=dict)
    _histograms: dict[str, list[float]] = field(default_factory=dict)
    _timers: dict[str, float] = field(default_factory=dict)

    def inc(self, name: str, delta: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) + delta

    def dec(self, name: str, delta: int = 1) -> None:
        self._counters[name] = self._counters.get(name, 0) - delta

    def set(self, name: str, value: float) -> None:
        self._gauges[name] = value

    def observe(self, name: str, value: float) -> None:
        self._histograms.setdefault(name, []).append(value)

    def get(self, name: str) -> int | float | None:
        if name in self._counters:
            return self._counters[name]
        if name in self._gauges:
            return self._gauges[name]
        return None

    def timer(self, name: str) -> "_Timer":
        return _Timer(name, self)

    def snapshot(self) -> dict[str, Any]:
        return {
            "counters": dict(self._counters),
            "gauges": dict(self._gauges),
            "histograms": {k: len(v) for k, v in self._histograms.items()},
        }


class _Timer:
    def __init__(self, name: str, metrics: Metrics):
        self.name = name
        self.metrics = metrics
        self._start: float = 0.0

    def __enter__(self):
        self._start = time.monotonic()
        return self

    def __exit__(self, *args):
        elapsed = time.monotonic() - self._start
        self.metrics.observe(self.name + ".seconds", elapsed)


class KVStore:
    """Local key-value store scoped to the agent process.

    Supports optional TTL per key. Not persistent.
    """
    def __init__(self):
        self._data: dict[str, tuple[Any, float | None]] = {}

    def get(self, key: str, default: Any = None) -> Any:
        entry = self._data.get(key)
        if entry is None:
            return default
        value, expires_at = entry
        if expires_at is not None and time.monotonic() > expires_at:
            del self._data[key]
            return default
        return value

    def set(self, key: str, value: Any, ttl_seconds: float | None = None) -> None:
        expires_at = time.monotonic() + ttl_seconds if ttl_seconds else None
        self._data[key] = (value, expires_at)

    def delete(self, key: str) -> None:
        self._data.pop(key, None)

    def clear(self) -> None:
        self._data.clear()


@dataclass
class Context:
    """Per-message context passed to every handler.

    Provides:
      - Tracing: trace_id, span_id, correlation_id
      - Routing: source, message_type
      - Metadata: headers (read-only)
      - Observability: log (auto-tagged), metrics
      - Actions: reply, forward, ack, nack
      - State: storage (local KV)
    """
    trace_id: UUID
    span_id: int
    source: str
    message_type: str
    correlation_id: UUID | None = None
    target: str = ""
    topic: str = ""
    headers: dict[str, str] = field(default_factory=dict)
    _on_reply: Callable | None = field(default=None, repr=False)
    _on_forward: Callable | None = field(default=None, repr=False)
    _on_ack: Callable | None = field(default=None, repr=False)
    _on_nack: Callable | None = field(default=None, repr=False)

    # ── Observability (created lazily) ──
    _log: logging.Logger | None = field(default=None, repr=False)
    _metrics: Metrics | None = field(default=None, repr=False)
    _storage: KVStore | None = field(default=None, repr=False)

    @property
    def log(self) -> logging.Logger:
        if self._log is None:
            self._log = logging.getLogger(f"aqap.ctx.{self.trace_id}")
        return self._log

    @property
    def metrics(self) -> Metrics:
        if self._metrics is None:
            self._metrics = Metrics()
        return self._metrics

    @property
    def storage(self) -> KVStore:
        if self._storage is None:
            self._storage = KVStore()
        return self._storage

    async def reply(self, body: Any) -> str:
        """Reply to the source agent. Returns message_id."""
        if self._on_reply is None:
            raise RuntimeError("reply() called outside handler context")
        return await self._on_reply(body)

    async def forward(self, topic: str) -> str:
        """Forward the current message to another topic."""
        if self._on_forward is None:
            raise RuntimeError("forward() called outside handler context")
        return await self._on_forward(topic)

    async def ack(self) -> None:
        """Acknowledge message processing (for at-least-once transports)."""
        if self._on_ack:
            await self._on_ack()

    async def nack(self, requeue: bool = True) -> None:
        """Negative-acknowledge (reject/requeue)."""
        if self._on_nack:
            await self._on_nack(requeue)
```

- [ ] **Step 3: Export Context in __init__.py**

```python
# aqap/v3/__init__.py — add:
from aqap.v3.context import Context, Metrics, KVStore
__all__ += ["Context", "Metrics", "KVStore"]
```

- [ ] **Step 4: Run test**

Run: `python3 -m pytest tests/test_v3_context.py -v`
Expected: 6 passed

- [ ] **Step 5: Commit**

```bash
git add aqap/v3/context.py aqap/v3/__init__.py tests/test_v3_context.py
git commit -m "feat(v3): Context — per-message tracing, logging, metrics, storage

- Context: trace_id, span_id, correlation_id, source, message_type
- Logger auto-tagged with trace_id
- Metrics: counter, gauge, histogram, timer
- KVStore: local key-value with optional TTL
- Reply, forward, ack, nack actions (callback-injected by framework)

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 3: Serializer — Kernel-Bridged Serialization

**Files:**
- Create: `aqap/v3/serializer.py`
- Modify: `aqap/v3/__init__.py` — export serializer functions
- Test: `tests/test_v3_serializer.py`

**Interfaces:**
- Consumes: `aqap.kernel.wire_message_encode`, `aqap.kernel.wire_message_decode`, `aqap.kernel.WireMessage`, `aqap.kernel.SchemaRegistry`
- Produces: `serialize_message(kernel_msg: WireMessage, encoding: str = "json") -> bytes`, `deserialize_message(data: bytes) -> WireMessage`, `body_to_dict(wire_msg) -> dict`, `dict_to_body(data: dict, schema_id: str, registry) -> bytes`

- [ ] **Step 1: Write failing test**

```python
# tests/test_v3_serializer.py
import pytest
import uuid
from aqap.v3.serializer import serialize_message, deserialize_message
from aqap.kernel import WireMessage


class TestSerializer:
    def test_round_trip_json(self):
        """Serialize → deserialize should be identity."""
        msg_id = uuid.uuid4()
        trace_id = uuid.uuid4()
        msg = WireMessage(
            message_id=msg_id,
            topic="aqap:v3:agent:probe",
            trace_id=trace_id,
            span_id=1,
            source="test",
            target="",
            correlation_id=uuid.UUID(int=0),
            msg_type="task:dispatch",
            body={"task_id": "task-test"},
            headers={},
            encoding="json",
            compression="none",
            signature_mode="none",
            priority="normal",
            ttl_ms=30000,
        )
        encoded = serialize_message(msg)
        decoded = deserialize_message(encoded)
        assert decoded.topic == "aqap:v3:agent:probe"
        assert decoded.source == "test"
        assert decoded.body == {"task_id": "task-test"}

    def test_encoding_override(self):
        """serialize_message should allow encoding override."""
        msg = WireMessage(
            message_id=uuid.uuid4(),
            topic="aqap:v3:test",
            trace_id=uuid.uuid4(),
            span_id=0,
            source="test",
            target="",
            correlation_id=uuid.UUID(int=0),
            msg_type="task:dispatch",
            body={"k": "v"},
            headers={},
            encoding="json",
            compression="none",
            signature_mode="none",
            priority="normal",
            ttl_ms=0,
        )
        encoded = serialize_message(msg, encoding="msgpack")
        decoded = deserialize_message(encoded)
        assert decoded.body == {"k": "v"}
```

Run: `python3 -m pytest tests/test_v3_serializer.py -v`
Expected: FAIL

- [ ] **Step 2: Implement serializer**

```python
# aqap/v3/serializer.py
"""Kernel-bridged serialization layer.

Thin wrappers around aqap.kernel wire functions.
Handles WireMessage construction from Python dicts and vice versa.
"""
from __future__ import annotations

import uuid as _uuid
from typing import Any

from aqap.kernel import (
    WireMessage,
    wire_message_encode,
    wire_message_decode,
    SchemaRegistry,
    ValidationResult,
)


def serialize_message(
    wire_msg: WireMessage,
    encoding: str = "json",
) -> bytes:
    """Serialize a WireMessage to wire-format bytes using the kernel."""
    return bytes(wire_message_encode(wire_msg, encoding=encoding))


def deserialize_message(data: bytes) -> WireMessage:
    """Deserialize wire-format bytes to a WireMessage using the kernel."""
    return wire_message_decode(data)


def body_to_dict(wire_msg: WireMessage) -> dict[str, Any]:
    """Extract the body from a WireMessage as a Python dict.

    The kernel stores body as a JSON Value; this returns it as a plain dict.
    """
    import json
    body = wire_msg.body
    if isinstance(body, dict):
        return body
    if isinstance(body, str):
        return json.loads(body)
    # Fallback: use repr round-trip (for non-JSON encodings)
    return json.loads(json.dumps(body))


def dict_to_body(
    data: dict[str, Any],
    schema_id: str,
    registry: SchemaRegistry | None = None,
) -> bytes:
    """Convert a dict to body bytes, optionally validating against a schema.

    Returns JSON-encoded bytes suitable for WireMessage.body.
    """
    import json

    if registry:
        result: ValidationResult = registry.validate(schema_id, data)
        if not result.valid:
            raise ValueError(
                f"Schema validation failed for {schema_id}: "
                f"{'; '.join(result.errors)}"
            )

    return json.dumps(data, ensure_ascii=False).encode("utf-8")


def make_wire_message(
    topic: str,
    body: dict[str, Any],
    source: str,
    msg_type: str = "task:dispatch",
    target: str = "",
    trace_id: _uuid.UUID | None = None,
    span_id: int = 0,
    correlation_id: _uuid.UUID | None = None,
    headers: dict[str, str] | None = None,
    encoding: str = "json",
    compression: str = "none",
    signature_mode: str = "none",
    priority: str = "normal",
    ttl_ms: int = 30_000,
) -> WireMessage:
    """Construct a WireMessage with sensible defaults.

    This is the primary factory function for creating outbound messages
    from the Python SDK layer.
    """
    return WireMessage(
        message_id=_uuid.uuid4(),
        topic=topic,
        trace_id=trace_id or _uuid.uuid4(),
        span_id=span_id,
        source=source,
        target=target,
        correlation_id=correlation_id or _uuid.UUID(int=0),
        msg_type=msg_type,
        body=body,
        headers=headers or {},
        encoding=encoding,
        compression=compression,
        signature_mode=signature_mode,
        priority=priority,
        ttl_ms=ttl_ms,
    )
```

- [ ] **Step 3: Export in __init__.py**

```python
# aqap/v3/__init__.py — add:
from aqap.v3.serializer import (
    serialize_message,
    deserialize_message,
    body_to_dict,
    dict_to_body,
    make_wire_message,
)
__all__ += ["serialize_message", "deserialize_message", "body_to_dict", "dict_to_body", "make_wire_message"]
```

- [ ] **Step 4: Run test**

Run: `python3 -m pytest tests/test_v3_serializer.py -v`
Expected: 2 passed

- [ ] **Step 5: Commit**

```bash
git add aqap/v3/serializer.py aqap/v3/__init__.py tests/test_v3_serializer.py
git commit -m "feat(v3): serializer — kernel-bridged serialize/deserialize

- serialize_message / deserialize_message: thin kernel wrappers
- body_to_dict / dict_to_body: dict<->bytes conversion with schema validation
- make_wire_message: factory function for outbound WireMessage construction

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 4: Middleware — Hook Chain

**Files:**
- Create: `aqap/v3/middleware.py`
- Modify: `aqap/v3/__init__.py` — export middleware
- Test: `tests/test_v3_middleware.py`

**Interfaces:**
- Produces: `Middleware` ABC with `async before(ctx, msg) -> (ctx, msg)` and `async after(ctx, result) -> (ctx, result)`, `MiddlewareChain` class, built-in: `RateLimiter`, `Logger`, `Timeout`, `Retry`

- [ ] **Step 1: Write failing test**

```python
# tests/test_v3_middleware.py
import pytest
import asyncio
from unittest.mock import AsyncMock
from aqap.v3.middleware import Middleware, MiddlewareChain, Logger, RateLimiter, Timeout


class TestMiddlewareChain:
    async def test_empty_chain_passthrough(self):
        """An empty chain should pass context and result through unchanged."""
        chain = MiddlewareChain([])
        ctx = {"trace_id": "test"}
        msg = {"body": "hello"}
        result_ctx, result_msg = await chain.before(ctx, msg)
        assert result_ctx == ctx
        assert result_msg == msg

    async def test_single_middleware_before(self):
        """A middleware should be able to modify context before the handler."""
        class AddHeader(Middleware):
            async def before(self, ctx, msg):
                msg["headers"] = msg.get("headers", {})
                msg["headers"]["x-added"] = "true"
                return ctx, msg

        chain = MiddlewareChain([AddHeader()])
        ctx = {}
        msg = {}
        result_ctx, result_msg = await chain.before(ctx, msg)
        assert result_msg["headers"] == {"x-added": "true"}

    async def test_middleware_chain_order(self):
        """Middleware should execute in registration order."""
        calls = []

        class First(Middleware):
            async def before(self, ctx, msg):
                calls.append("first")
                return ctx, msg

        class Second(Middleware):
            async def before(self, ctx, msg):
                calls.append("second")
                return ctx, msg

        chain = MiddlewareChain([First(), Second()])
        await chain.before({}, {})
        assert calls == ["first", "second"]

    async def test_middleware_after(self):
        """After-hooks should fire in reverse order."""
        calls = []

        class A(Middleware):
            async def before(self, ctx, msg): return ctx, msg
            async def after(self, ctx, result):
                calls.append("after-A")
                return ctx, result

        class B(Middleware):
            async def before(self, ctx, msg): return ctx, msg
            async def after(self, ctx, result):
                calls.append("after-B")
                return ctx, result

        chain = MiddlewareChain([A(), B()])
        await chain.after({}, {"status": "ok"})
        assert calls == ["after-B", "after-A"]  # reverse order


class TestBuiltinMiddleware:
    async def test_logger_creates_log(self):
        """Logger middleware should add timing info."""
        log = Logger()
        ctx = {}
        msg = {"body": "test"}
        result_ctx, result_msg = await log.before(ctx, msg)
        assert "_start_time" in result_ctx

    async def test_timeout_passes_through_fast(self):
        """Timeout should pass through if handler is fast."""
        tm = Timeout(seconds=5.0)
        ctx = {}
        msg = {}
        result_ctx, result_msg = await tm.before(ctx, msg)
        assert result_ctx is ctx  # no change for fast path

    async def test_rate_limiter_allows_within_limit(self):
        """RateLimiter should allow requests within QPS."""
        rl = RateLimiter(qps=100)
        ctx = {}
        msg = {}
        for _ in range(10):
            result_ctx, result_msg = await rl.before(ctx, msg)
        assert result_ctx is ctx  # should not block
```

Run: `python3 -m pytest tests/test_v3_middleware.py -v`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 2: Implement middleware**

```python
# aqap/v3/middleware.py
from __future__ import annotations

import asyncio
import time
from abc import ABC, abstractmethod
from typing import Any


class Middleware(ABC):
    """Base middleware — hook into the message processing pipeline.

    before() runs before the handler.
    after() runs after the handler (in reverse registration order).

    Order: before(A) → before(B) → handler → after(B) → after(A)
    """

    @abstractmethod
    async def before(self, ctx: dict, msg: dict) -> tuple[dict, dict]:
        """Transform context and/or message before the handler runs.

        Returns (ctx, msg) — may be modified copies.
        """
        return ctx, msg

    @abstractmethod
    async def after(self, ctx: dict, result: Any) -> tuple[dict, Any]:
        """Transform context and/or result after the handler runs.

        Returns (ctx, result) — may be modified copies.
        """
        return ctx, result


class MiddlewareChain:
    """Ordered chain of middleware."""

    def __init__(self, middlewares: list[Middleware] | None = None):
        self._middlewares = middlewares or []

    def add(self, mw: Middleware) -> None:
        self._middlewares.append(mw)

    async def before(self, ctx: dict, msg: dict) -> tuple[dict, dict]:
        for mw in self._middlewares:
            ctx, msg = await mw.before(ctx, msg)
        return ctx, msg

    async def after(self, ctx: dict, result: Any) -> tuple[dict, Any]:
        for mw in reversed(self._middlewares):
            ctx, result = await mw.after(ctx, result)
        return ctx, result

    def wrap(self, handler):
        """Wrap an async handler with this middleware chain.

        Returns a new async function that runs before → handler → after.
        """
        chain = self

        async def wrapped(ctx: dict, msg: dict) -> Any:
            ctx, msg = await chain.before(ctx, msg)
            result = await handler(ctx, msg)
            ctx, result = await chain.after(ctx, result)
            return result

        return wrapped

    def __len__(self) -> int:
        return len(self._middlewares)

    def __bool__(self) -> bool:
        return len(self._middlewares) > 0


# ── Built-in middleware ──

class Logger(Middleware):
    """Log timing for every message processed."""

    async def before(self, ctx: dict, msg: dict) -> tuple[dict, dict]:
        ctx["_start_time"] = time.monotonic()
        return ctx, msg

    async def after(self, ctx: dict, result: Any) -> tuple[dict, Any]:
        elapsed = time.monotonic() - ctx.pop("_start_time", time.monotonic())
        trace_id = ctx.get("trace_id", "unknown")
        import logging
        log = logging.getLogger("aqap.middleware.logger")
        log.info("trace=%s handled in %.3fs", trace_id, elapsed)
        return ctx, result


class Timeout(Middleware):
    """Cancel handler if it exceeds a time limit."""

    def __init__(self, seconds: float):
        self.seconds = seconds

    async def before(self, ctx: dict, msg: dict) -> tuple[dict, dict]:
        ctx["_deadline"] = time.monotonic() + self.seconds
        return ctx, msg

    async def after(self, ctx: dict, result: Any) -> tuple[dict, Any]:
        deadline = ctx.pop("_deadline", None)
        if deadline and time.monotonic() > deadline:
            raise asyncio.TimeoutError(
                f"Handler exceeded timeout of {self.seconds}s"
            )
        return ctx, result


class RateLimiter(Middleware):
    """Token bucket rate limiter."""

    def __init__(self, qps: int):
        self.qps = qps
        self._tokens = float(qps)
        self._last_refill = time.monotonic()
        self._lock = asyncio.Lock()

    async def before(self, ctx: dict, msg: dict) -> tuple[dict, dict]:
        async with self._lock:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(float(self.qps), self._tokens + elapsed * self.qps)
            self._last_refill = now
            if self._tokens < 1.0:
                wait = (1.0 - self._tokens) / self.qps
                await asyncio.sleep(wait)
                self._tokens = 0.0
            else:
                self._tokens -= 1.0
        return ctx, msg

    async def after(self, ctx: dict, result: Any) -> tuple[dict, Any]:
        return ctx, result


class Retry(Middleware):
    """Retry handler on exception with configurable backoff."""

    def __init__(self, max_retries: int = 3, backoff_seconds: float = 1.0):
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds

    async def before(self, ctx: dict, msg: dict) -> tuple[dict, dict]:
        return ctx, msg

    async def after(self, ctx: dict, result: Any) -> tuple[dict, Any]:
        return ctx, result  # Retry is handled at the app level
```

- [ ] **Step 3: Export in __init__.py**

```python
# aqap/v3/__init__.py — add:
from aqap.v3.middleware import Middleware, MiddlewareChain, Logger, Timeout, RateLimiter, Retry
__all__ += ["Middleware", "MiddlewareChain", "Logger", "Timeout", "RateLimiter", "Retry"]
```

- [ ] **Step 4: Run test**

Run: `python3 -m pytest tests/test_v3_middleware.py -v`
Expected: 7 passed

- [ ] **Step 5: Commit**

```bash
git add aqap/v3/middleware.py aqap/v3/__init__.py tests/test_v3_middleware.py
git commit -m "feat(v3): Middleware chain with built-in Logger, Timeout, RateLimiter, Retry

- Middleware ABC: before/after hooks
- MiddlewareChain: ordered execution, reverse after-hooks, wrap() helper
- Logger: auto-timing per message
- Timeout: deadline-based cancellation
- RateLimiter: token bucket
- Retry: max_retries + backoff config

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 5: AQAP App Class — Functional API Core

**Files:**
- Create: `aqap/v3/app.py`
- Modify: `aqap/v3/__init__.py` — export AQAP
- Test: `tests/test_v3_app.py`

**Interfaces:**
- Consumes: `Config`, `Context`, `MiddlewareChain`, `make_wire_message`, `serialize_message`, `deserialize_message`, `body_to_dict`, kernel's `SchemaRegistry`, kernel's `Router`
- Produces: `AQAP` class with `on(topic, handler, **opts)`, `dispatch(topic, body, **opts) -> str`, `request(topic, body, timeout) -> Any`, `run()`, `start()/stop()`, `health()`

- [ ] **Step 1: Write failing test**

```python
# tests/test_v3_app.py
import pytest
import asyncio
from aqap.v3.app import AQAP
from aqap.v3.config import Config


class TestAQAP:
    def test_create_app_with_url(self):
        """AQAP should accept a URL string."""
        app = AQAP(transport="memory://")
        assert app.config.transport == "memory://"

    def test_create_app_with_config(self):
        """AQAP should accept a Config object."""
        cfg = Config(transport="memory://", agent_id="test-agent")
        app = AQAP(cfg)
        assert app.config.agent_id == "test-agent"

    def test_app_has_schema_registry(self):
        """AQAP should auto-create a SchemaRegistry."""
        app = AQAP(transport="memory://")
        assert app.schema_registry is not None

    def test_app_has_router(self):
        """AQAP should auto-create a Router."""
        app = AQAP(transport="memory://")
        assert app.router is not None

    def test_on_registers_handler(self):
        """on() should register a handler in the router."""
        app = AQAP(transport="memory://")

        @app.on("aqap:v3:agent:probe")
        async def my_handler(msg):
            return {"ok": True}

        assert app.router.has_topic("aqap:v3:agent:probe")
        assert app.router.handler_count("aqap:v3:agent:probe") == 1

    async def test_dispatch_fire_and_forget(self):
        """dispatch() should send a message to a topic."""
        app = AQAP(transport="memory://")
        await app.start()
        try:
            msg_id = await app.dispatch(
                "aqap:v3:agent:probe",
                {"task_id": "test-001", "type": "code_review"},
                source="test-scheduler",
            )
            assert msg_id  # Should return a message ID string
        finally:
            await app.stop()

    async def test_request_wait_for_reply(self):
        """request() should wait for a reply."""
        app = AQAP(transport="memory://", agent_id="scheduler")

        @app.on("aqap:v3:agent:probe")
        async def handler(msg):
            return {"passed": True, "score": 95}

        await app.start()
        try:
            result = await app.request(
                "aqap:v3:agent:probe",
                {"task_id": "test-req-001"},
                source="scheduler",
                timeout=5.0,
            )
            assert result["passed"] is True
            assert result["score"] == 95
        finally:
            await app.stop()

    def test_health_status(self):
        """health() should return status dict."""
        app = AQAP(transport="memory://")
        status = app.health()
        assert status["config"]["transport"] == "memory://"
        assert status["running"] is False
```

Run: `python3 -m pytest tests/test_v3_app.py -v --asyncio-mode=auto`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 2: Implement AQAP app class**

```python
# aqap/v3/app.py
"""AQAP v3 — main application class.

The AQAP class is the central orchestrator: it holds the config, transport,
schema registry, router, security context, and middleware chain, and exposes
the functional API surface (on, dispatch, request, run).
"""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Callable, Awaitable

from aqap.kernel import SchemaRegistry, Router
from aqap.v3.config import Config
from aqap.v3.context import Context
from aqap.v3.middleware import MiddlewareChain
from aqap.v3.serializer import (
    serialize_message,
    deserialize_message,
    body_to_dict,
    make_wire_message,
)

logger = logging.getLogger("aqap.v3.app")


class AQAP:
    """AQAP v3 application — functional API entry point.

    Usage:
        app = AQAP(transport="redis://localhost:6379")

        @app.on("aqap:v3:agent:probe")
        async def handle(msg):
            return {"passed": True}

        app.run()
    """

    def __init__(
        self,
        transport: str | Config = "memory://",
        *,
        agent_id: str = "",
        group: str = "aqap-default",
        **kwargs,
    ):
        # Config
        if isinstance(transport, Config):
            self.config = transport
        elif isinstance(transport, str):
            self.config = Config(transport=transport, agent_id=agent_id, group=group, **kwargs)
        else:
            self.config = Config(transport=str(transport), agent_id=agent_id, group=group, **kwargs)

        # Kernel components
        self.schema_registry = SchemaRegistry()
        self.schema_registry.load_builtins()
        self.router = Router()

        # Middleware chain
        self._middleware = MiddlewareChain()

        # Runtime state
        self._running = False
        self._transport = None
        self._tasks: list[asyncio.Task] = []
        self._pending_requests: dict[str, asyncio.Future] = {}
        self._security = None

        # Handler storage
        self._handlers: dict[str, Callable[..., Awaitable[Any]]] = {}
        self._handler_schemas: dict[str, str] = {}  # topic -> schema_id

    # ── Configuration ──

    def use(self, middleware) -> "AQAP":
        """Add middleware to the chain. Returns self for chaining."""
        self._middleware.add(middleware)
        return self

    # ── Handler registration ──

    def on(
        self,
        topic: str,
        *,
        schema_in: str | None = None,
        schema_out: str | None = None,
        concurrency: int = 0,
    ) -> Callable:
        """Register a handler for a topic.

        Usage as decorator:
            @app.on("aqap:v3:agent:probe")
            async def my_handler(msg):
                return {"ok": True}
        """
        def decorator(handler: Callable) -> Callable:
            handler_id = f"{topic}:{handler.__name__}"
            self.router.add_topic(topic, handler_id)
            self._handlers[handler_id] = handler
            if schema_in:
                self._handler_schemas[handler_id] = schema_in
            return handler

        return decorator

    # ── Message operations ──

    async def dispatch(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        source: str = "",
        msg_type: str = "task:dispatch",
        target: str = "",
        headers: dict[str, str] | None = None,
        **kwargs,
    ) -> str:
        """Fire-and-forget: send a message to a topic. Returns message_id."""
        msg = make_wire_message(
            topic=topic,
            body=body,
            source=source or self.config.agent_id or "aqap",
            msg_type=msg_type,
            target=target,
            headers=headers,
            **kwargs,
        )
        encoded = serialize_message(msg)
        await self._transport_publish(topic, encoded)
        return msg.message_id

    async def request(
        self,
        topic: str,
        body: dict[str, Any],
        *,
        source: str = "",
        timeout: float = 30.0,
        **kwargs,
    ) -> Any:
        """Request-reply: send a message and wait for a response."""
        correlation_id = uuid.uuid4()
        msg = make_wire_message(
            topic=topic,
            body=body,
            source=source or self.config.agent_id or "aqap",
            correlation_id=correlation_id,
            **kwargs,
        )
        encoded = serialize_message(msg)

        # Register a future for the reply
        future: asyncio.Future = asyncio.get_event_loop().create_future()
        self._pending_requests[str(correlation_id)] = future

        try:
            await self._transport_publish(topic, encoded)
            result = await asyncio.wait_for(future, timeout=timeout)
            return result
        finally:
            self._pending_requests.pop(str(correlation_id), None)

    # ── Lifecycle ──

    def run(self) -> None:
        """Blocking entry point — runs until interrupted."""
        asyncio.run(self._run())

    async def _run(self) -> None:
        await self.start()
        try:
            # Wait forever (until signal)
            while self._running:
                await asyncio.sleep(3600)
        except (KeyboardInterrupt, asyncio.CancelledError):
            pass
        finally:
            await self.stop()

    async def start(self) -> None:
        """Start the AQAP app: connect transport, start consumers."""
        if self._running:
            return

        self._running = True

        # Connect transport
        await self._init_transport()

        # Start consumer loops for all registered topics
        for topic in self.router.list_topics():
            task = asyncio.create_task(self._consume_loop(topic))
            self._tasks.append(task)

        logger.info("AQAP v3 started — %d topics registered", len(self.router.list_topics()))

    async def stop(self) -> None:
        """Graceful shutdown."""
        if not self._running:
            return

        self._running = False

        # Cancel consumer tasks
        for task in self._tasks:
            task.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)
        self._tasks.clear()

        # Disconnect transport
        if self._transport and hasattr(self._transport, 'disconnect'):
            await self._transport.disconnect()

        logger.info("AQAP v3 stopped")

    def health(self) -> dict[str, Any]:
        """Return health status."""
        return {
            "running": self._running,
            "config": {
                "transport": self.config.transport,
                "protocol_version": self.config.protocol_version,
                "agent_id": self.config.agent_id,
            },
            "topics": self.router.list_topics(),
            "pending_requests": len(self._pending_requests),
            "middleware_count": len(self._middleware),
        }

    # ── Internal ──

    async def _init_transport(self) -> None:
        """Initialize transport based on config."""
        scheme = self.config.transport_scheme

        if scheme == "memory" or scheme == "in-memory":
            from aqap.transport.inmemory import InMemoryTransport
            self._transport = InMemoryTransport()
        elif scheme == "redis":
            from aqap.transport.redis_streams import RedisStreamsTransport
            self._transport = RedisStreamsTransport(stream_url=self.config.transport)
        elif scheme == "kafka":
            from aqap.transport.kafka_transport import KafkaTransport
            self._transport = KafkaTransport(servers=f"{self.config.transport_host}:{self.config.transport_port}")
        else:
            # Fallback to in-memory
            logger.warning("Unknown transport scheme '%s', falling back to in-memory", scheme)
            from aqap.transport.inmemory import InMemoryTransport
            self._transport = InMemoryTransport()

        await self._transport.connect()
        logger.info("Transport connected: %s (scheme=%s)", self._transport.name, scheme)

    async def _transport_publish(self, topic: str, data: bytes) -> None:
        """Publish raw bytes to transport."""
        if self._transport is None:
            raise RuntimeError("Transport not initialized")
        # Store data as a dict-compatible message for existing transports
        msg = deserialize_message(data)
        from aqap.core.message import Message as V1Message, MessageType, Topic as V1Topic
        v1_msg = V1Message(
            type=MessageType.TASK_DISPATCH,
            source=msg.source,
            target=msg.target,
            payload=body_to_dict(msg),
            trace_id=str(msg.trace_id),
            topic=topic,
        )
        await self._transport.publish(topic, v1_msg)

    async def _consume_loop(self, topic: str) -> None:
        """Consume messages from a topic and dispatch to handlers."""
        consumer_id = f"{self.config.agent_id or 'aqap'}-{uuid.uuid4().hex[:6]}"

        async for v1_msg in self._transport.subscribe(
            topic, group=self.config.group, consumer=consumer_id
        ):
            if not self._running:
                break

            # Convert v1 Message to v3 Context + body dict
            body = v1_msg.payload if isinstance(v1_msg.payload, dict) else {}
            ctx = Context(
                trace_id=uuid.UUID(v1_msg.trace_id) if v1_msg.trace_id else uuid.uuid4(),
                span_id=0,
                source=v1_msg.source,
                message_type=str(v1_msg.type),
                correlation_id=uuid.UUID(v1_msg.correlation_id) if v1_msg.correlation_id else None,
                target=v1_msg.target,
                topic=topic,
            )

            # Resolve handlers
            handler_ids = self.router.resolve(topic)
            for hid in handler_ids:
                handler = self._handlers.get(hid)
                if handler is None:
                    continue

                # Check for pending request (correlation_id match)
                if ctx.correlation_id and str(ctx.correlation_id) in self._pending_requests:
                    future = self._pending_requests.pop(str(ctx.correlation_id))
                    if not future.done():
                        future.set_result(body)
                    return

                # Run middleware chain + handler
                wrapped = self._middleware.wrap(handler)
                ctx_dict = {"trace_id": str(ctx.trace_id), "span_id": ctx.span_id}
                try:
                    result = await wrapped(ctx_dict, body)
                    # If handler returns a dict, auto-reply to source
                    if isinstance(result, dict) and ctx.source:
                        await self.dispatch(
                            f"aqap:v3:inbox:{ctx.source}",
                            result,
                            source=self.config.agent_id or "aqap",
                            correlation_id=ctx.correlation_id,
                        )
                except Exception as e:
                    logger.error("Handler error for %s: %s", topic, e)
```

- [ ] **Step 3: Export in __init__.py**

```python
# aqap/v3/__init__.py — add:
from aqap.v3.app import AQAP
__all__ += ["AQAP"]
```

- [ ] **Step 4: Run test**

Run: `python3 -m pytest tests/test_v3_app.py -v --asyncio-mode=auto`
Expected: 8 passed

- [ ] **Step 5: Commit**

```bash
git add aqap/v3/app.py aqap/v3/__init__.py tests/test_v3_app.py
git commit -m "feat(v3): AQAP app class — functional API core

- AQAP class: on(), dispatch(), request(), run(), start(), stop(), health()
- Auto-creates SchemaRegistry (with builtins), Router
- Transport auto-detected from config URL scheme (memory/redis/kafka)
- Consume loop dispatches to registered handlers via Router
- request() returns reply via correlation_id future matching
- MiddlewareChain.wrap() applied to all handlers

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 6: @agent Decorator — Declarative API

**Files:**
- Create: `aqap/v3/agent.py`
- Modify: `aqap/v3/__init__.py` — export agent, Agent
- Test: `tests/test_v3_agent.py`

**Interfaces:**
- Produces: `Agent` base class with `handle(msg) -> Any`, `on_start()`, `on_stop()`, `health()`; `agent(**opts)` decorator that wraps a class and registers it with an AQAP instance

- [ ] **Step 1: Write failing test**

```python
# tests/test_v3_agent.py
import pytest
from aqap.v3.agent import agent, Agent


class TestAgentDecorator:
    def test_agent_creates_class(self):
        """@agent should return the original class (unchanged type)."""

        @agent(agent_id="test-probe", transport="memory://")
        class MyProbe(Agent):
            async def handle(self, msg):
                return {"ok": True}

        assert issubclass(MyProbe, Agent)
        instance = MyProbe()
        assert instance.agent_id == "test-probe"
        assert instance.agent_type == "MyProbe"

    def test_agent_registers_with_app(self):
        """@agent should store an _aqap_config dict on the class."""

        @agent(
            agent_id="probe-1",
            transport="redis://broker:6379",
            subscribe=["aqap:v3:agent:probe"],
            concurrency=4,
            max_retries=5,
        )
        class ProbeAgent(Agent):
            async def handle(self, msg):
                return {"passed": True}

        cfg = ProbeAgent._aqap_config
        assert cfg["agent_id"] == "probe-1"
        assert cfg["transport"] == "redis://broker:6379"
        assert cfg["subscribe"] == ["aqap:v3:agent:probe"]
        assert cfg["concurrency"] == 4
        assert cfg["max_retries"] == 5

    async def test_agent_lifecycle_hooks(self):
        """Agent should call on_start/on_stop hooks."""
        calls = []

        @agent(agent_id="lifecycle-test", transport="memory://")
        class LifecycleAgent(Agent):
            async def on_start(self):
                calls.append("start")

            async def handle(self, msg):
                return {"ok": True}

            async def on_stop(self):
                calls.append("stop")

        instance = LifecycleAgent()
        await instance.on_start()
        assert "start" in calls
        await instance.on_stop()
        assert "stop" in calls

    async def test_agent_handle(self):
        """Agent.handle() should process a message and return a result."""

        @agent(agent_id="handler-test", transport="memory://")
        class HandlerAgent(Agent):
            async def handle(self, msg):
                return {"status": "processed", "input": msg}

        instance = HandlerAgent()
        result = await instance.handle({"task_id": "test"})
        assert result == {"status": "processed", "input": {"task_id": "test"}}


class TestAgentBase:
    async def test_agent_default_handle_raises(self):
        """Agent base class handle() should raise NotImplementedError."""

        @agent(agent_id="base-test", transport="memory://")
        class BaseAgent(Agent):
            pass  # no handle()

        instance = BaseAgent()
        with pytest.raises(NotImplementedError):
            await instance.handle({"msg": "test"})
```

Run: `python3 -m pytest tests/test_v3_agent.py -v --asyncio-mode=auto`
Expected: FAIL — `ModuleNotFoundError`

- [ ] **Step 2: Implement agent module**

```python
# aqap/v3/agent.py
"""AQAP v3 declarative API — @agent decorator + Agent base class."""
from __future__ import annotations

import asyncio
import logging
import uuid
from typing import Any, Callable

logger = logging.getLogger("aqap.v3.agent")


class Agent:
    """Base class for declarative agents.

    Subclass and decorate with @agent(). Override handle().

    Example:
        @agent(agent_id="my-probe", transport="redis://...")
        class MyProbe(Agent):
            async def handle(self, msg):
                return {"passed": True}
    """

    agent_id: str
    agent_type: str
    _aqap_config: dict[str, Any] = {}

    def __init_subclass__(cls, **kwargs):
        super().__init_subclass__(**kwargs)
        # agent_type defaults to class name
        if "agent_type" not in cls.__dict__:
            cls.agent_type = cls.__name__

    def __init__(self, **kwargs):
        self._running = False
        self._tasks: list[asyncio.Task] = []
        # Override config values with constructor kwargs
        for key, val in kwargs.items():
            setattr(self, key, val)

    async def handle(self, msg: Any) -> Any:
        """Override this method with your business logic.

        Receives a message dict (schema-validated if schema_in is set).
        Returns a result dict (schema-validated if schema_out is set).
        """
        raise NotImplementedError("Subclass must implement handle()")

    async def on_start(self) -> None:
        """Called when the agent starts. Override for initialization."""

    async def on_stop(self) -> None:
        """Called when the agent stops. Override for cleanup."""

    async def health(self) -> dict[str, Any]:
        """Return agent health status."""
        return {
            "agent_id": self.agent_id,
            "agent_type": self.agent_type,
            "running": self._running,
        }


def agent(
    *,
    agent_id: str,
    transport: str = "redis://localhost:6379",
    subscribe: list[str] | None = None,
    schema_in: type | str | None = None,
    schema_out: type | str | None = None,
    group: str = "aqap-default",
    max_retries: int = 3,
    heartbeat_interval: int = 30,
    concurrency: int = 0,
    middleware: list[str] | None = None,
    **kwargs,
) -> Callable:
    """Decorator that configures an Agent subclass for AQAP v3.

    Usage:
        @agent(agent_id="my-probe", transport="redis://...", subscribe=["aqap:v3:agent:probe"])
        class MyProbe(Agent):
            async def handle(self, msg):
                return {"passed": True}
    """
    def decorator(cls):
        # Validate it's an Agent subclass
        if not issubclass(cls, Agent):
            raise TypeError(
                f"@agent decorator can only be applied to Agent subclasses, "
                f"got {cls.__name__}"
            )

        # Store config on the class
        cls._aqap_config = {
            "agent_id": agent_id,
            "transport": transport,
            "subscribe": subscribe or [],
            "schema_in": _resolve_schema(schema_in),
            "schema_out": _resolve_schema(schema_out),
            "group": group,
            "max_retries": max_retries,
            "heartbeat_interval": heartbeat_interval,
            "concurrency": concurrency,
            "middleware": middleware or [],
            **kwargs,
        }

        # Ensure agent_id is set on instances
        original_init = cls.__init__

        def __init__(self, **kw):
            self.agent_id = agent_id
            self.agent_type = cls.__name__
            if original_init is not object.__init__:
                original_init(self, **kw)

        cls.__init__ = __init__
        return cls

    return decorator


def _resolve_schema(schema: type | str | None) -> str:
    """Resolve a schema to its schema_id string.

    - None → ""
    - str → the string itself (assumed to be schema_id)
    - Pydantic model → model_config["schema_id"]
    """
    if schema is None:
        return ""
    if isinstance(schema, str):
        return schema
    if hasattr(schema, "model_config"):
        return schema.model_config.get("schema_id", schema.__name__)
    return getattr(schema, "__name__", str(schema))
```

- [ ] **Step 3: Export in __init__.py**

```python
# aqap/v3/__init__.py — add:
from aqap.v3.agent import agent, Agent
__all__ += ["agent", "Agent"]
```

- [ ] **Step 4: Run test**

Run: `python3 -m pytest tests/test_v3_agent.py -v --asyncio-mode=auto`
Expected: 5 passed

- [ ] **Step 5: Commit**

```bash
git add aqap/v3/agent.py aqap/v3/__init__.py tests/test_v3_agent.py
git commit -m "feat(v3): @agent decorator — declarative Agent base class

- Agent base class: handle(), on_start(), on_stop(), health()
- @agent() decorator: stores _aqap_config on class with all settings
- Schema resolution: Pydantic model_config or plain str
- Constructor auto-sets agent_id and agent_type

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

### Task 7: Top-Level aqap Re-exports + Integration Test

**Files:**
- Modify: `aqap/__init__.py` — add v3 re-exports
- Test: `tests/test_v3_integration.py`

**Interfaces:**
- Produces: `from aqap import AQAP, agent, Agent, Config, Context` works as top-level imports

- [ ] **Step 1: Write integration test**

```python
# tests/test_v3_integration.py
"""End-to-end integration tests: app + kernel + transport + handler."""
import pytest
import asyncio
from aqap import AQAP


class TestEndToEnd:
    async def test_full_flow_two_agents(self):
        """Probe agent handles task, Judge agent evaluates result."""
        probe = AQAP(transport="memory://", agent_id="probe-1")

        @probe.on("aqap:v3:agent:probe")
        async def probe_handler(msg):
            return {
                "task_id": msg.get("task_id"),
                "passed": True,
                "score": 92.5,
                "summary": "All checks passed",
            }

        judge = AQAP(transport="memory://", agent_id="judge-1")

        verdicts = []

        @judge.on("aqap:v3:agent:probe")
        async def judge_handler(msg):
            if msg.get("score", 0) >= 90:
                verdicts.append({"verdict": "PASS", "task_id": msg["task_id"]})
            else:
                verdicts.append({"verdict": "FAIL", "task_id": msg["task_id"]})
            return verdicts[-1]

        await probe.start()
        await judge.start()

        try:
            result = await probe.request(
                "aqap:v3:agent:probe",
                {
                    "task_id": "task-e2e-001",
                    "type": "code_review",
                    "target": {"repo": "test/repo", "branch": "main"},
                },
                source="e2e-test",
                timeout=5.0,
            )
            assert result["passed"] is True
            assert result["score"] == 92.5
        finally:
            await judge.stop()
            await probe.stop()

    async def test_agent_decorator_integration(self):
        """@agent-decorated class should work with AQAP app."""
        from aqap import agent, Agent

        @agent(
            agent_id="integrated-probe",
            transport="memory://",
            subscribe=["aqap:v3:agent:probe"],
        )
        class IntegratedProbe(Agent):
            async def handle(self, msg):
                return {"status": "ok", "echo": msg}

        app = AQAP(transport="memory://", agent_id="orchestrator")
        instance = IntegratedProbe()

        # Register the instance's handler
        @app.on("aqap:v3:agent:probe")
        async def wrapper(msg):
            return await instance.handle(msg)

        await app.start()
        try:
            result = await app.request(
                "aqap:v3:agent:probe",
                {"hello": "world"},
                source="orchestrator",
                timeout=5.0,
            )
            assert result == {"status": "ok", "echo": {"hello": "world"}}
        finally:
            await app.stop()
```

Run: `python3 -m pytest tests/test_v3_integration.py -v --asyncio-mode=auto`
Expected: FAIL — if re-exports not set up yet

- [ ] **Step 2: Update aqap/__init__.py**

```python
# aqap/__init__.py — update:
"""AQAP — Agent Queue Agent Communication Protocol."""
__version__ = "3.0.0"

# v3 SDK (primary)
from aqap.v3.app import AQAP
from aqap.v3.agent import agent, Agent
from aqap.v3.config import Config, SecurityConfig
from aqap.v3.context import Context
from aqap.v3.middleware import Middleware, MiddlewareChain

# v2 protocol (backward compatible)
from aqap.v2 import (
    Envelope,
    Message,
    SchemaEnvelope,
    SchemaRegistry,
    ValidationResult,
)

__all__ = [
    "AQAP",
    "agent",
    "Agent",
    "Config",
    "SecurityConfig",
    "Context",
    "Middleware",
    "MiddlewareChain",
    # v2 backward compat
    "Envelope",
    "Message",
    "SchemaEnvelope",
    "SchemaRegistry",
    "ValidationResult",
]
```

- [ ] **Step 3: Run all v3 tests**

Run: `python3 -m pytest tests/test_v3_*.py -v --asyncio-mode=auto`
Expected: ALL PASS (~28 tests across 6 test files)

- [ ] **Step 4: Commit**

```bash
git add aqap/__init__.py aqap/v3/ tests/test_v3_*.py
git commit -m "feat(v3): top-level re-exports + end-to-end integration tests

- aqap/__init__.py: AQAP, agent, Agent, Config, Context, Middleware at top level
- from aqap import AQAP — works as single import
- v2 backward compatibility preserved
- Integration tests: full probe→judge flow, @agent decorator with AQAP app

Co-Authored-By: Claude Opus 4.8 (1M context) <noreply@anthropic.com>"
```

---

## Phase 2 Summary

| Task | Deliverable | Tests |
|------|------------|-------|
| 1 | Config + SecurityConfig dataclasses | 5 |
| 2 | Context, Metrics, KVStore | 6 |
| 3 | Serializer (kernel bridge) | 2 |
| 4 | Middleware chain + built-ins | 7 |
| 5 | AQAP app class (functional API) | 8 |
| 6 | @agent decorator (declarative API) | 5 |
| 7 | Top-level re-exports + integration | 2 |
| **Total** | **7 modules, ~800 lines** | **~35** |

**Phase 2 outcome**: `from aqap import AQAP` — 5-line functional API and `@agent` declarative API both functional. Transport auto-detected. Kernel used for all serialization. Middleware chain active. Ready for Phase 3 (Transport hardening) and Phase 4 (Gateway + multi-lang SDKs).
