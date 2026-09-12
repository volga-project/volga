//! Worker-scoped shared backend engine handle (not store logic).

use std::sync::Arc;

use anyhow::Result;
use scylla::client::session::Session;
use scylla::client::session_builder::SessionBuilder;

use crate::api::spec::state::{OperatorStateBackendConfig, ScyllaConfig};

const KEYSPACE_CQL: &str = r#"
CREATE KEYSPACE IF NOT EXISTS {keyspace}
WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
"#;

/// Shared handle for one state engine on a worker.
///
/// `None` at the registry means no remote/engine session (pure in-memory stores).
/// Operator stores keep their own prepared statements and table DDL; this is the
/// driver connection (pool, token metadata) shared across `OperatorKind`s.
#[derive(Clone)]
pub enum StateSessionHandle {
    Scylla(Arc<Session>),
}

impl std::fmt::Debug for StateSessionHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Scylla(_) => f.write_str("Scylla"),
        }
    }
}

impl StateSessionHandle {
    /// Single init site for the worker: reach the engine from backend config.
    /// In-memory backends do not need a session (`Ok(None)`). Scylla fails here
    /// if the cluster is down — not on first store op.
    pub async fn connect(backend: &OperatorStateBackendConfig) -> Result<Option<Self>> {
        match backend {
            OperatorStateBackendConfig::InMemory => Ok(None),
            OperatorStateBackendConfig::Scylla(cfg) => {
                Ok(Some(Self::Scylla(connect_scylla(cfg).await?)))
            }
        }
    }

    pub fn scylla(&self) -> &Arc<Session> {
        match self {
            Self::Scylla(session) => session,
        }
    }
}

async fn connect_scylla(config: &ScyllaConfig) -> Result<Arc<Session>> {
    let mut builder = SessionBuilder::new();
    for node in &config.contact_points {
        builder = builder.known_node(node);
    }
    if let Some(dc) = &config.datacenter {
        builder = builder.known_node(dc);
    }
    let session = builder.build().await?;
    let ks = KEYSPACE_CQL.replace("{keyspace}", &config.keyspace);
    session.query_unpaged(ks.as_str(), &[]).await?;
    session.use_keyspace(&config.keyspace, false).await?;
    Ok(Arc::new(session))
}
