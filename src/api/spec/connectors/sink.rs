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
}

impl SinkSpec {
    pub fn in_memory_grpc(server_addr: impl Into<String>) -> Self {
        Self::InMemoryStorageGrpc {
            server_addr: server_addr.into(),
            upsert_key_columns: Vec::new(),
        }
    }

    /// In-memory sink with upsert keys; address is filled in by the harness/deployer.
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
            } => SinkConfig::in_memory_grpc(server_addr.clone())
                .with_upsert_key_columns(upsert_key_columns.clone()),
            SinkSpec::Request => SinkConfig::RequestSinkConfig,
            SinkSpec::Parquet(spec) => SinkConfig::ParquetSinkConfig(spec.to_config()),
        }
    }
}
