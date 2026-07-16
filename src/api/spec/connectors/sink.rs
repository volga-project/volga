use serde::{Deserialize, Serialize};

use crate::runtime::functions::sink::ParquetSinkSpec;
use crate::runtime::operators::sink::sink_operator::SinkConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SinkSpec {
    InMemoryStorageGrpc {
        server_addr: String,
        /// When true, sink upserts rows into a dedup map keyed by `(key|event_ts)`.
        #[serde(default)]
        dedup: bool,
    },
    Request,
    Parquet(ParquetSinkSpec),
}

impl SinkSpec {
    pub fn to_sink_config(&self) -> SinkConfig {
        match self {
            SinkSpec::InMemoryStorageGrpc {
                server_addr,
                dedup,
            } => SinkConfig::InMemoryStorageGrpcSinkConfig {
                server_addr: server_addr.clone(),
                dedup: *dedup,
            },
            SinkSpec::Request => SinkConfig::RequestSinkConfig,
            SinkSpec::Parquet(spec) => SinkConfig::ParquetSinkConfig(spec.to_config()),
        }
    }
}

