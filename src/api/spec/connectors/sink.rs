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
    pub fn to_sink_config(&self) -> SinkConfig {
        match self {
            SinkSpec::InMemoryStorageGrpc {
                server_addr,
                upsert_key_columns,
            } => SinkConfig::InMemoryStorageGrpcSinkConfig {
                server_addr: server_addr.clone(),
                upsert_key_columns: upsert_key_columns.clone(),
            },
            SinkSpec::Request => SinkConfig::RequestSinkConfig,
            SinkSpec::Parquet(spec) => SinkConfig::ParquetSinkConfig(spec.to_config()),
        }
    }
}
