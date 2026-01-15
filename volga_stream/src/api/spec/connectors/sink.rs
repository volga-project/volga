use serde::{Deserialize, Serialize};

use crate::runtime::operators::sink::sink_operator::SinkConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum SinkSpec {
    InMemoryStorageGrpc { server_addr: String },
    Request,
}

impl SinkSpec {
    pub fn to_sink_config(&self) -> SinkConfig {
        match self {
            SinkSpec::InMemoryStorageGrpc { server_addr } => {
                SinkConfig::InMemoryStorageGrpcSinkConfig(server_addr.clone())
            }
            SinkSpec::Request => SinkConfig::RequestSinkConfig,
        }
    }
}

