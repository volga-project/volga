//! Worker-scoped shared backend engine handle (not store logic).

use std::sync::Arc;

/// Shared handle for one state engine on a worker.
#[derive(Debug, Clone, Default)]
pub enum StateSessionHandle {
    #[default]
    InMem,
    // Future: Scylla(Arc<ScyllaStateSession>), SlateDb(...), Rocks(...),
}

impl StateSessionHandle {
    pub fn in_mem() -> Self {
        Self::InMem
    }

    pub fn is_in_mem(&self) -> bool {
        matches!(self, Self::InMem)
    }
}

/// Marker for future typed session wrappers.
#[derive(Debug)]
pub struct InMemStateSession;

impl InMemStateSession {
    pub fn shared() -> Arc<Self> {
        Arc::new(Self)
    }
}
