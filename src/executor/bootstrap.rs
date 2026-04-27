use serde::{Deserialize, Serialize};

use crate::control_plane::types::ExecutionIds;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerBootstrapPayload {
    pub worker_id: String,
    pub execution_ids: ExecutionIds,
    pub payload_bytes: Vec<u8>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MasterBootstrapPayload {
    pub execution_ids: ExecutionIds,
    pub worker_payloads: Vec<WorkerBootstrapPayload>,
}
