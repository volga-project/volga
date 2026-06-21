use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineSpecId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineId(pub Uuid);

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineState {
    Creating,
    Starting,
    Running,
    Stopping,
    Stopped,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub pipeline_spec_id: PipelineSpecId,
    pub created_at: DateTime<Utc>,
    pub spec_bytes: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatus {
    pub pipeline_id: PipelineId,
    pub state: PipelineState,
    pub updated_at: DateTime<Utc>,
    pub worker_count: usize,
    pub task_count: usize,
    pub last_checkpoint_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    pub pipeline_id: PipelineId,
    pub checkpoint_id: u64,
    pub committed_at: DateTime<Utc>,
    pub manifest_uri: String,
}

