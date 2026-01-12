use serde::{Deserialize, Serialize};

use crate::api::spec::storage::StorageSpec;
use crate::transport::transport_spec::TransportSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRuntimeSpec {
    pub transport: TransportSpec,
    pub storage: StorageSpec,
}

impl Default for WorkerRuntimeSpec {
    fn default() -> Self {
        Self {
            transport: TransportSpec::default(),
            storage: StorageSpec::default(),
        }
    }
}

