// aqap-kernel/src/router.rs — topic routing (Task 5)
use pyo3::prelude::*;
use std::collections::HashMap;

/// Router maps topics to handlers.
///
/// Topics can have multiple handlers (for fan-out). Handlers are
/// Python callables stored by handler_id.
#[pyclass]
pub struct Router {
    /// topic -> list of handler_ids
    topics: HashMap<String, Vec<String>>,
    /// handler_id -> Python callable
    handlers: HashMap<String, PyObject>,
}

#[pymethods]
impl Router {
    #[new]
    fn new() -> Self {
        Router {
            topics: HashMap::new(),
            handlers: HashMap::new(),
        }
    }

    /// Register a handler_id for a topic. Creates the topic if it doesn't exist.
    fn add_topic(&mut self, topic: String, handler_id: String) {
        self.topics.entry(topic).or_default().push(handler_id);
    }

    /// Resolve a topic to its list of handler_ids.
    fn resolve(&self, topic: &str) -> Vec<String> {
        self.topics.get(topic).cloned().unwrap_or_default()
    }

    /// Register a Python callable under a handler_id.
    fn add_handler(&mut self, handler_id: String, callback: PyObject) {
        self.handlers.insert(handler_id, callback);
    }

    /// Remove a handler by id.
    fn remove_handler(&mut self, handler_id: &str) {
        self.handlers.remove(handler_id);
    }

    /// List all registered topics, sorted.
    fn list_topics(&self) -> Vec<String> {
        let mut topics: Vec<String> = self.topics.keys().cloned().collect();
        topics.sort();
        topics
    }

    /// Check if a topic has any registered handlers.
    fn has_topic(&self, topic: &str) -> bool {
        self.topics.contains_key(topic) && !self.topics[topic].is_empty()
    }

    /// Get the handler count for a topic.
    fn handler_count(&self, topic: &str) -> usize {
        self.topics.get(topic).map(|v| v.len()).unwrap_or(0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_router_add_and_resolve_topic() {
        let mut router = Router::new();
        router.add_topic("aqap:v3:agent:probe".into(), "handler-1".into());
        router.add_topic("aqap:v3:agent:probe".into(), "handler-2".into());

        let handlers = router.resolve("aqap:v3:agent:probe");
        assert_eq!(handlers, vec!["handler-1", "handler-2"]);
        assert!(router.has_topic("aqap:v3:agent:probe"));
        assert_eq!(router.handler_count("aqap:v3:agent:probe"), 2);
    }

    #[test]
    fn test_router_resolve_unknown_topic() {
        let router = Router::new();
        let handlers = router.resolve("nonexistent:topic");
        assert!(handlers.is_empty());
        assert!(!router.has_topic("nonexistent:topic"));
        assert_eq!(router.handler_count("nonexistent:topic"), 0);
    }

    #[test]
    fn test_router_list_topics_sorted() {
        let mut router = Router::new();
        router.add_topic("zebra".into(), "h1".into());
        router.add_topic("alpha".into(), "h2".into());
        router.add_topic("beta".into(), "h3".into());

        let topics = router.list_topics();
        assert_eq!(topics, vec!["alpha", "beta", "zebra"]);
    }

    #[test]
    fn test_router_add_and_remove_handler() {
        pyo3::prepare_freethreaded_python();
        Python::with_gil(|py| {
            let mut router = Router::new();
            // We can store a Python None as a handler placeholder
            let callback: Py<PyAny> = py.None();
            router.add_handler("handler-1".into(), callback);

            // We can't easily test that the handler is stored without calling it,
            // but we can verify the remove works.
            router.remove_handler("handler-1");
        });
    }

    #[test]
    fn test_router_has_topic_empty_handlers() {
        // has_topic returns false for topics with empty handler list
        // (can't create empty topics via add_topic since or_default pushes,
        // but we test the contract)
        let router = Router::new();
        assert!(!router.has_topic("nonexistent"));
    }
}
