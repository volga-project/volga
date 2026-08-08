//! Worker-scoped shared backend engine handle (not store logic).

use anyhow::Result;

use crate::api::spec::state::OperatorStateBackendConfig;

/// Shared handle for one state engine on a worker.
///
/// `None` at the registry means no remote/engine session (pure in-memory stores).
#[derive(Debug, Clone)]
pub enum StateSessionHandle {
    // Future: Scylla(Arc<ScyllaStateSession>), SlateDb(...), Rocks(...),
}

impl StateSessionHandle {
    /// Single init site for the worker: build a session from backend config.
    /// In-memory backends do not need a session (`Ok(None)`).
    pub fn connect(backend: &OperatorStateBackendConfig) -> Result<Option<Self>> {
        match backend {
            OperatorStateBackendConfig::InMemory => Ok(None),
        }
    }
}
