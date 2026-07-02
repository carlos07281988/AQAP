"""Tests for SchemaRegistry and Router — Task 5."""
import pytest
from aqap.kernel import SchemaRegistry, ValidationResult, Router


TASK_SCHEMA = {
    "$id": "aqap:schema:task.v3",
    "title": "Quality Task",
    "type": "object",
    "properties": {
        "task_id": {"type": "string", "pattern": r"^task-[a-z0-9]{8,}$"},
        "type": {"type": "string", "enum": ["code_review", "unit_test", "integration_test", "lint"]},
        "target": {
            "type": "object",
            "properties": {
                "repo": {"type": "string", "pattern": r"^[\w.-]+/[\w.-]+$"},
                "branch": {"type": "string"},
                "commit": {"type": "string", "pattern": r"^[a-f0-9]{40}$"},
            },
            "required": ["repo", "branch"],
        },
    },
    "required": ["task_id", "type", "target"],
}


class TestSchemaRegistry:
    def test_register_and_validate_valid(self):
        reg = SchemaRegistry()
        reg.register("aqap:schema:task.v3", TASK_SCHEMA, "3.0.0")
        result = reg.validate("aqap:schema:task.v3", {
            "task_id": "task-abc12345",
            "type": "code_review",
            "target": {"repo": "myorg/myrepo", "branch": "main"},
        })
        assert result.valid
        assert len(result.errors) == 0

    def test_validate_missing_required(self):
        reg = SchemaRegistry()
        reg.register("aqap:schema:task.v3", TASK_SCHEMA, "3.0.0")
        result = reg.validate("aqap:schema:task.v3", {
            "type": "code_review",
        })
        assert not result.valid
        assert any("task_id" in e for e in result.errors)

    def test_validate_wrong_type(self):
        reg = SchemaRegistry()
        reg.register("aqap:schema:task.v3", TASK_SCHEMA, "3.0.0")
        result = reg.validate("aqap:schema:task.v3", {
            "task_id": 12345,
            "type": "code_review",
            "target": {"repo": "myorg/myrepo", "branch": "main"},
        })
        assert not result.valid

    def test_validate_pattern_mismatch(self):
        reg = SchemaRegistry()
        reg.register("aqap:schema:task.v3", TASK_SCHEMA, "3.0.0")
        result = reg.validate("aqap:schema:task.v3", {
            "task_id": "bad-id",
            "type": "code_review",
            "target": {"repo": "myorg/myrepo", "branch": "main"},
        })
        assert not result.valid
        assert any("match" in e.lower() for e in result.errors)

    def test_validate_enum_invalid(self):
        reg = SchemaRegistry()
        reg.register("aqap:schema:task.v3", TASK_SCHEMA, "3.0.0")
        result = reg.validate("aqap:schema:task.v3", {
            "task_id": "task-abc12345",
            "type": "deploy",
            "target": {"repo": "myorg/myrepo", "branch": "main"},
        })
        assert not result.valid

    def test_schema_not_found(self):
        reg = SchemaRegistry()
        result = reg.validate("aqap:schema:nonexistent.v1", {})
        assert not result.valid
        assert any("not found" in e.lower() for e in result.errors)

    def test_version_resolution(self):
        """Get latest version when version not specified."""
        reg = SchemaRegistry()
        reg.register("test:schema:v1", TASK_SCHEMA, "1.0.0")
        reg.register("test:schema:v1", TASK_SCHEMA, "2.0.0")
        schema = reg.get("test:schema:v1")  # no version = latest
        assert schema is not None

    def test_validate_and_wrap(self):
        """validate_and_wrap returns SchemaEnvelope or None."""
        reg = SchemaRegistry()
        reg.register("aqap:schema:task.v3", TASK_SCHEMA, "3.0.0")
        wrapped = reg.validate_and_wrap("aqap:schema:task.v3", {
            "task_id": "task-abc12345",
            "type": "code_review",
            "target": {"repo": "myorg/myrepo", "branch": "main"},
        })
        assert wrapped is not None
        assert wrapped.schema_id == "aqap:schema:task.v3"
        assert wrapped.data["task_id"] == "task-abc12345"

    def test_load_builtins(self):
        """Built-in schemas should be available after load_builtins()."""
        reg = SchemaRegistry()
        reg.load_builtins()
        assert reg.has("aqap:schema:task.v3")
        assert reg.has("aqap:schema:result.v3")
        assert reg.has("aqap:schema:verdict.v3")
        assert reg.has("aqap:schema:heartbeat.v3")
        assert reg.has("aqap:schema:error.v3")


class TestRouter:
    def test_add_and_resolve_topic(self):
        router = Router()
        router.add_topic("aqap:v3:agent:probe", "handler-1")
        router.add_topic("aqap:v3:agent:probe", "handler-2")
        handlers = router.resolve("aqap:v3:agent:probe")
        assert handlers == ["handler-1", "handler-2"]
        assert router.has_topic("aqap:v3:agent:probe")

    def test_resolve_unknown_topic(self):
        router = Router()
        assert router.resolve("nonexistent") == []
        assert not router.has_topic("nonexistent")

    def test_list_topics(self):
        router = Router()
        router.add_topic("z:topic", "h1")
        router.add_topic("a:topic", "h2")
        router.add_topic("m:topic", "h3")
        topics = router.list_topics()
        assert topics == ["a:topic", "m:topic", "z:topic"]

    def test_handler_count(self):
        router = Router()
        router.add_topic("test", "h1")
        router.add_topic("test", "h2")
        router.add_topic("test", "h3")
        assert router.handler_count("test") == 3
        assert router.handler_count("nonexistent") == 0
