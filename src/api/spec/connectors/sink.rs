use serde::{Deserialize, Serialize};

use crate::runtime::functions::sink::ParquetSinkSpec;
use crate::runtime::operators::sink::sink_operator::SinkConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SinkSpec {
    InMemoryStorageGrpc {
        server_addr: String,
        /// When non-empty, explode rows and upsert into the keyed map by these columns.
        #[serde(default)]
        upsert_key_columns: Vec<String>,
    },
    Request,
    Parquet(ParquetSinkSpec),
    /// Drop-payload sink: count records, do not retain batches.
    Count,
}

impl SinkSpec {
    pub fn in_memory_grpc(server_addr: impl Into<String>) -> Self {
        Self::InMemoryStorageGrpc {
            server_addr: server_addr.into(),
            upsert_key_columns: Vec::new(),
        }
    }

    /// Upsert keys only; local/docker harness installs a concrete `server_addr`.
    pub fn in_memory_upsert(key_columns: Vec<String>) -> Self {
        Self::InMemoryStorageGrpc {
            server_addr: String::new(),
            upsert_key_columns: key_columns,
        }
    }

    pub fn with_upsert_key_columns(mut self, columns: Vec<String>) -> Self {
        if let Self::InMemoryStorageGrpc {
            upsert_key_columns, ..
        } = &mut self
        {
            *upsert_key_columns = columns;
        }
        self
    }

    pub fn with_server_addr(mut self, server_addr: impl Into<String>) -> Self {
        if let Self::InMemoryStorageGrpc {
            server_addr: addr, ..
        } = &mut self
        {
            *addr = server_addr.into();
        }
        self
    }

    pub fn to_sink_config(&self) -> SinkConfig {
        match self {
            SinkSpec::InMemoryStorageGrpc {
                server_addr,
                upsert_key_columns,
            } => {
                if server_addr.trim().is_empty() {
                    panic!("InMemoryStorageGrpc server_addr must be set before to_sink_config");
                }
                SinkConfig::in_memory_grpc(server_addr.clone())
                    .with_upsert_key_columns(upsert_key_columns.clone())
            }
            SinkSpec::Request => SinkConfig::RequestSinkConfig,
            SinkSpec::Parquet(spec) => SinkConfig::ParquetSinkConfig(spec.to_config()),
            SinkSpec::Count => SinkConfig::CountSinkConfig,
        }
    }

    /// True when this sink needs the in-memory gRPC store process/pod.
    pub fn needs_in_memory_store(&self) -> bool {
        matches!(self, Self::InMemoryStorageGrpc { .. })
    }
}
