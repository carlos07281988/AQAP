// aqap-kernel/src/schema.rs — JSON Schema validation (Task 5)
use pyo3::prelude::*;
use std::collections::HashMap;
use jsonschema::{Draft, JSONSchema};
use serde_json::Value;

// ── Helpers: Python value <-> serde_json::Value ──

fn python_value_to_json(val: &Bound<'_, PyAny>) -> PyResult<Value> {
    let json_str: String = Python::with_gil(|py| {
        py.import("json")
            .and_then(|json_mod| json_mod.call_method1("dumps", (val,)))
            .and_then(|obj| obj.extract::<String>())
    })?;
    serde_json::from_str(&json_str).map_err(|e| {
        PyErr::new::<pyo3::exceptions::PyValueError, _>(format!("JSON parse: {}", e))
    })
}

fn json_to_python(py: Python<'_>, val: &Value) -> Option<PyObject> {
    let json_str = serde_json::to_string(val).ok()?;
    py.import("json")
        .ok()?
        .call_method1("loads", (json_str,))
        .ok()
        .map(|o| o.into())
}

// ── ValidationResult ──

#[pyclass]
pub struct ValidationResult {
    #[pyo3(get)]
    pub valid: bool,
    #[pyo3(get)]
    pub errors: Vec<String>,
    #[pyo3(get)]
    pub schema_id: String,
    pub data: Option<Py<PyAny>>,
}

#[pymethods]
impl ValidationResult {
    #[getter]
    fn data(&self, py: Python<'_>) -> Option<PyObject> {
        self.data.as_ref().map(|d| d.clone_ref(py))
    }

    fn __bool__(&self) -> bool {
        self.valid
    }

    fn __repr__(&self) -> String {
        format!(
            "ValidationResult(valid={}, errors={:?}, schema_id={})",
            self.valid, self.errors, self.schema_id
        )
    }
}

// ── SchemaEnvelope ──

#[pyclass]
pub struct SchemaEnvelope {
    #[pyo3(get)]
    pub schema_id: String,
    #[pyo3(get)]
    pub schema_version: String,
    pub data: Option<Py<PyAny>>,
}

#[pymethods]
impl SchemaEnvelope {
    #[getter]
    fn data(&self, py: Python<'_>) -> Option<PyObject> {
        self.data.as_ref().map(|d| d.clone_ref(py))
    }

    fn __repr__(&self) -> String {
        format!(
            "SchemaEnvelope(schema_id={}, schema_version={})",
            self.schema_id, self.schema_version
        )
    }
}

// ── SchemaMeta ──

#[pyclass]
#[derive(Clone)]
pub struct SchemaMeta {
    #[pyo3(get)]
    pub schema_id: String,
    #[pyo3(get)]
    pub version: String,
    #[pyo3(get)]
    pub title: String,
}

#[pymethods]
impl SchemaMeta {
    fn __repr__(&self) -> String {
        format!(
            "SchemaMeta(schema_id={}, version={}, title={})",
            self.schema_id, self.version, self.title
        )
    }
}

// ── SchemaRegistry ──

#[pyclass]
pub struct SchemaRegistry {
    schemas: HashMap<String, HashMap<String, Value>>,
    builtins_loaded: bool,
}

#[pymethods]
impl SchemaRegistry {
    #[new]
    fn new() -> Self {
        SchemaRegistry {
            schemas: HashMap::new(),
            builtins_loaded: false,
        }
    }

    /// Register a JSON Schema under a schema_id with a version string.
    fn register(
        &mut self,
        schema_id: String,
        schema: Bound<'_, PyAny>,
        version: String,
    ) -> PyResult<()> {
        let schema_json: Value = python_value_to_json(&schema)?;
        self.schemas
            .entry(schema_id)
            .or_default()
            .insert(version, schema_json);
        Ok(())
    }

    /// Get a schema by id (and optionally version). Returns None if not found.
    #[pyo3(signature = (schema_id, version=None))]
    fn get(&self, schema_id: &str, version: Option<&str>) -> Option<PyObject> {
        let versions = self.schemas.get(schema_id)?;
        let schema = if let Some(v) = version {
            versions.get(v)?
        } else {
            // Return latest version (lexicographic sort — last key is "latest")
            let mut sorted: Vec<_> = versions.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            sorted.last()?.1
        };
        Python::with_gil(|py| json_to_python(py, schema))
    }

    /// Check if a schema is registered. Optionally filter by version.
    #[pyo3(signature = (schema_id, version=None))]
    fn has(&self, schema_id: &str, version: Option<&str>) -> bool {
        match self.schemas.get(schema_id) {
            Some(versions) => match version {
                Some(v) => versions.contains_key(v),
                None => !versions.is_empty(),
            },
            None => false,
        }
    }

    /// Validate a Python value against a registered schema.
    #[pyo3(signature = (schema_id, data, version=None))]
    fn validate(
        &self,
        schema_id: &str,
        data: Bound<'_, PyAny>,
        version: Option<&str>,
    ) -> PyResult<ValidationResult> {
        let versions = match self.schemas.get(schema_id) {
            Some(v) => v,
            None => {
                return Ok(ValidationResult {
                    valid: false,
                    errors: vec![format!("Schema not found: {}", schema_id)],
                    schema_id: schema_id.to_string(),
                    data: None,
                });
            }
        };

        let schema_value = if let Some(v) = version {
            match versions.get(v) {
                Some(s) => s.clone(),
                None => {
                    return Ok(ValidationResult {
                        valid: false,
                        errors: vec![format!(
                            "Schema version not found: {} v{}",
                            schema_id, v
                        )],
                        schema_id: schema_id.to_string(),
                        data: None,
                    });
                }
            }
        } else {
            // Latest version
            let mut sorted: Vec<_> = versions.iter().collect();
            sorted.sort_by_key(|(k, _)| *k);
            sorted.last().unwrap().1.clone()
        };

        let data_value: Value = python_value_to_json(&data)?;

        let compiled = match JSONSchema::options()
            .with_draft(Draft::Draft7)
            .compile(&schema_value)
        {
            Ok(v) => v,
            Err(e) => {
                return Ok(ValidationResult {
                    valid: false,
                    errors: vec![format!("Schema compilation error: {}", e)],
                    schema_id: schema_id.to_string(),
                    data: None,
                });
            }
        };

        let validation_errors: Vec<String> = match compiled.validate(&data_value) {
            Ok(_) => Vec::new(),
            Err(errors) => errors
                .map(|e| format!("{}: {}", e.instance_path, e))
                .collect(),
        };

        let valid = validation_errors.is_empty();

        Python::with_gil(|py| {
            Ok(ValidationResult {
                valid,
                errors: validation_errors,
                schema_id: schema_id.to_string(),
                data: Some(
                    json_to_python(py, &data_value).unwrap_or_else(|| py.None()),
                ),
            })
        })
    }

    /// Validate and return a SchemaEnvelope if valid, None otherwise.
    #[pyo3(signature = (schema_id, data, version=None))]
    fn validate_and_wrap(
        &self,
        schema_id: &str,
        data: Bound<'_, PyAny>,
        version: Option<&str>,
    ) -> PyResult<Option<SchemaEnvelope>> {
        let mut result = self.validate(schema_id, data, version)?;
        if result.valid {
            Ok(Some(SchemaEnvelope {
                schema_id: std::mem::take(&mut result.schema_id),
                schema_version: version.unwrap_or("latest").to_string(),
                data: result.data.take(),
            }))
        } else {
            Ok(None)
        }
    }

    /// List all registered schemas as SchemaMeta entries.
    fn list(&self) -> Vec<SchemaMeta> {
        let mut metas: Vec<SchemaMeta> = self
            .schemas
            .iter()
            .flat_map(|(id, versions)| {
                versions.iter().map(|(ver, schema)| SchemaMeta {
                    schema_id: id.clone(),
                    version: ver.clone(),
                    title: schema
                        .get("title")
                        .and_then(|t| t.as_str())
                        .unwrap_or(id)
                        .to_string(),
                })
            })
            .collect();
        metas.sort_by(|a, b| {
            a.schema_id
                .cmp(&b.schema_id)
                .then_with(|| a.version.cmp(&b.version))
        });
        metas
    }

    /// Load built-in schemas: task, result, verdict, report, heartbeat, error, dlq.
    fn load_builtins(&mut self) -> PyResult<()> {
        if self.builtins_loaded {
            return Ok(());
        }

        let builtins: &[(&str, &str)] = &[
            ("aqap:schema:task.v3", include_str!("schemas/task_v3.json")),
            (
                "aqap:schema:result.v3",
                include_str!("schemas/result_v3.json"),
            ),
            (
                "aqap:schema:verdict.v3",
                include_str!("schemas/verdict_v3.json"),
            ),
            (
                "aqap:schema:report.v3",
                include_str!("schemas/report_v3.json"),
            ),
            (
                "aqap:schema:heartbeat.v3",
                include_str!("schemas/heartbeat_v3.json"),
            ),
            ("aqap:schema:error.v3", include_str!("schemas/error_v3.json")),
            ("aqap:schema:dlq.v3", include_str!("schemas/dlq_v3.json")),
        ];

        for (schema_id, schema_text) in builtins {
            let schema_value: Value =
                serde_json::from_str(schema_text).map_err(|e| {
                    PyErr::new::<pyo3::exceptions::PyValueError, _>(format!(
                        "Failed to parse built-in schema {}: {}",
                        schema_id, e
                    ))
                })?;
            self.schemas
                .entry(schema_id.to_string())
                .or_default()
                .insert("3.0.0".to_string(), schema_value);
        }

        self.builtins_loaded = true;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn empty_registry() -> SchemaRegistry {
        SchemaRegistry {
            schemas: HashMap::new(),
            builtins_loaded: false,
        }
    }

    #[test]
    fn test_schema_registry_register_and_get() {
        let mut reg = empty_registry();
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        });

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register("test:schema".into(), py_schema.bind(py).clone(), "1.0.0".into())
                .unwrap();
        });

        assert!(reg.has("test:schema", None));
        assert!(reg.has("test:schema", Some("1.0.0")));
        assert!(!reg.has("test:schema", Some("2.0.0")));
        assert!(!reg.has("nonexistent", None));

        let retrieved = reg.get("test:schema", Some("1.0.0"));
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_schema_version_resolution_latest() {
        let mut reg = empty_registry();
        let schema = json!({"type": "object"});

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register(
                "test:schema".into(),
                py_schema.bind(py).clone(),
                "1.0.0".into(),
            )
            .unwrap();
            reg.register(
                "test:schema".into(),
                py_schema.bind(py).clone(),
                "2.0.0".into(),
            )
            .unwrap();
        });

        // get without version should return latest (2.0.0)
        let retrieved = reg.get("test:schema", None);
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_schema_validate_valid() {
        let mut reg = empty_registry();
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string", "minLength": 1}
            },
            "required": ["name"]
        });

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register(
                "test:schema".into(),
                py_schema.bind(py).clone(),
                "1.0.0".into(),
            )
            .unwrap();

            let data = json!({"name": "hello"});
            let py_data = json_to_python(py, &data).unwrap();
            let result = reg
                .validate("test:schema", py_data.bind(py).clone(), None)
                .unwrap();
            assert!(result.valid);
            assert!(result.errors.is_empty());
        });
    }

    #[test]
    fn test_schema_validate_invalid_missing_field() {
        let mut reg = empty_registry();
        let schema = json!({
            "type": "object",
            "properties": {
                "name": {"type": "string"}
            },
            "required": ["name"]
        });

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register(
                "test:schema".into(),
                py_schema.bind(py).clone(),
                "1.0.0".into(),
            )
            .unwrap();

            let data = json!({});
            let py_data = json_to_python(py, &data).unwrap();
            let result = reg
                .validate("test:schema", py_data.bind(py).clone(), None)
                .unwrap();
            assert!(!result.valid);
            assert!(!result.errors.is_empty());
        });
    }

    #[test]
    fn test_schema_validate_type_mismatch() {
        let mut reg = empty_registry();
        let schema = json!({
            "type": "object",
            "properties": {
                "age": {"type": "integer"}
            },
            "required": ["age"]
        });

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register(
                "test:schema".into(),
                py_schema.bind(py).clone(),
                "1.0.0".into(),
            )
            .unwrap();

            let data = json!({"age": "not-a-number"});
            let py_data = json_to_python(py, &data).unwrap();
            let result = reg
                .validate("test:schema", py_data.bind(py).clone(), None)
                .unwrap();
            assert!(!result.valid);
        });
    }

    #[test]
    fn test_schema_validate_schema_not_found() {
        let reg = empty_registry();

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let data = json!({"x": 1});
            let py_data = json_to_python(py, &data).unwrap();
            let result = reg
                .validate(
                    "aqap:schema:nonexistent.v1",
                    py_data.bind(py).clone(),
                    None,
                )
                .unwrap();
            assert!(!result.valid);
            assert!(result
                .errors
                .iter()
                .any(|e| e.to_lowercase().contains("not found")));
        });
    }

    #[test]
    fn test_schema_validate_pattern_mismatch() {
        let mut reg = empty_registry();
        let schema = json!({
            "type": "object",
            "properties": {
                "id": {"type": "string", "pattern": "^[a-z0-9]{8,}$"}
            },
            "required": ["id"]
        });

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register(
                "test:schema".into(),
                py_schema.bind(py).clone(),
                "1.0.0".into(),
            )
            .unwrap();

            let data = json!({"id": "BAD-UPPER"});
            let py_data = json_to_python(py, &data).unwrap();
            let result = reg
                .validate("test:schema", py_data.bind(py).clone(), None)
                .unwrap();
            assert!(!result.valid);
            assert!(result
                .errors
                .iter()
                .any(|e| e.to_lowercase().contains("match")));
        });
    }

    #[test]
    fn test_schema_enum_validation() {
        let mut reg = empty_registry();
        let schema = json!({
            "type": "object",
            "properties": {
                "color": {"type": "string", "enum": ["red", "green", "blue"]}
            },
            "required": ["color"]
        });

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register(
                "test:schema".into(),
                py_schema.bind(py).clone(),
                "1.0.0".into(),
            )
            .unwrap();

            // Valid
            let data = json!({"color": "red"});
            let py_data = json_to_python(py, &data).unwrap();
            let result = reg
                .validate("test:schema", py_data.bind(py).clone(), None)
                .unwrap();
            assert!(result.valid);

            // Invalid
            let data = json!({"color": "yellow"});
            let py_data = json_to_python(py, &data).unwrap();
            let result = reg
                .validate("test:schema", py_data.bind(py).clone(), None)
                .unwrap();
            assert!(!result.valid);
        });
    }

    #[test]
    fn test_load_builtins() {
        let mut reg = empty_registry();
        reg.load_builtins().unwrap();

        assert!(reg.has("aqap:schema:task.v3", None));
        assert!(reg.has("aqap:schema:result.v3", None));
        assert!(reg.has("aqap:schema:verdict.v3", None));
        assert!(reg.has("aqap:schema:report.v3", None));
        assert!(reg.has("aqap:schema:heartbeat.v3", None));
        assert!(reg.has("aqap:schema:error.v3", None));
        assert!(reg.has("aqap:schema:dlq.v3", None));

        // list should return all 7
        let metas = reg.list();
        assert_eq!(metas.len(), 7);
        assert!(metas.iter().all(|m| m.version == "3.0.0"));
    }

    #[test]
    fn test_validate_and_wrap() {
        let mut reg = empty_registry();
        let schema = json!({
            "type": "object",
            "properties": {
                "task_id": {"type": "string"}
            },
            "required": ["task_id"]
        });

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let py_schema = json_to_python(py, &schema).unwrap();
            reg.register(
                "aqap:schema:task.v3".into(),
                py_schema.bind(py).clone(),
                "3.0.0".into(),
            )
            .unwrap();

            // Valid
            let data = json!({"task_id": "task-abc12345"});
            let py_data = json_to_python(py, &data).unwrap();
            let envelope = reg
                .validate_and_wrap(
                    "aqap:schema:task.v3",
                    py_data.bind(py).clone(),
                    None,
                )
                .unwrap();
            assert!(envelope.is_some());
            let env = envelope.unwrap();
            assert_eq!(env.schema_id, "aqap:schema:task.v3");
            assert_eq!(env.schema_version, "latest");

            // Invalid
            let data = json!({});
            let py_data = json_to_python(py, &data).unwrap();
            let envelope = reg
                .validate_and_wrap(
                    "aqap:schema:task.v3",
                    py_data.bind(py).clone(),
                    None,
                )
                .unwrap();
            assert!(envelope.is_none());
        });
    }

    #[test]
    fn test_list_sorts_by_id_then_version() {
        let mut reg = empty_registry();

        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let s1 = json_to_python(py, &json!({"title": "A", "type": "object"})).unwrap();
            let s2 = json_to_python(py, &json!({"title": "B", "type": "object"})).unwrap();
            reg.register("z:schema".into(), s1.bind(py).clone(), "1.0.0".into())
                .unwrap();
            reg.register("a:schema".into(), s2.bind(py).clone(), "1.0.0".into())
                .unwrap();
        });

        let metas = reg.list();
        assert_eq!(metas.len(), 2);
        assert_eq!(metas[0].schema_id, "a:schema");
        assert_eq!(metas[1].schema_id, "z:schema");
    }

    #[test]
    fn test_validation_result_bool() {
        let vr = ValidationResult {
            valid: true,
            errors: vec![],
            schema_id: "test".into(),
            data: None,
        };
        assert!(vr.__bool__());

        let vr = ValidationResult {
            valid: false,
            errors: vec!["bad".into()],
            schema_id: "test".into(),
            data: None,
        };
        assert!(!vr.__bool__());
    }
}
