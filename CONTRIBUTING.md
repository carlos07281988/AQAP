# Contributing to AQAP

## Development Setup

```bash
git clone <repo-url>
cd AQAP
python3 -m venv venv
source venv/bin/activate
pip install -e ".[dev]"
```

## Project Structure

```
aqap/           # Core library
  core/         # Message protocol, engine, config, DLQ, security
  agent/        # Probe, Judge, Reporter, Supervisor, DLQConsumer
  transport/    # Redis Streams, Kafka, InMemory transports
  plugin/       # Plugin base class + registry
  plugins/      # Built-in: validator, scorer, trace_collector
sdk/            # External Agent SDK (Python + examples in Go/JS)
tests/          # Core tests
docs/           # Architecture & design docs
```

## Running Tests

```bash
# All tests (no external deps)
PYTHONPATH=. pytest tests/ sdk/tests/ -v

# Core only
PYTHONPATH=. pytest tests/ -v

# SDK only
PYTHONPATH=sdk:$PYTHONPATH pytest sdk/tests/ -v

# With coverage
PYTHONPATH=. pytest tests/ sdk/tests/ -v --cov=aqap --cov=aqap_sdk
```

## Code Style

- Python 3.9+ with type annotations
- Follow existing patterns in the codebase
- Use `ruff` for linting: `ruff check aqap/`
- All public methods should have docstrings
- Match surrounding code style (naming, comments, structure)

## Protocol Changes

When modifying the protocol (PROTOCOL.md), update all of:
1. `PROTOCOL.md` — the authoritative spec
2. `aqap/core/message.py` — core message implementation
3. `sdk/aqap_sdk/message.py` — SDK message implementation
4. `tests/test_aqa.py` and `sdk/tests/test_sdk.py` — tests

## Adding a Transport

1. Implement `aqap/transport/base.py:Transport` ABC
2. Register in `aqap/core/engine.py:_discover_transports()`
3. Add config example to `config.yaml`
4. Document in `docs/TRANSPORT.md`

## Adding a Plugin

1. Implement `aqap/plugin/base.py:Plugin` ABC
2. Register via `config.yaml` or `registry.register()`
3. Document in `docs/PLUGIN_SYSTEM.md`

## PR Process

1. Branch from `main`
2. Write/update tests
3. Ensure all tests pass: `PYTHONPATH=. pytest tests/ sdk/tests/ -v`
4. Update CHANGELOG.md
5. Open PR with description of changes
