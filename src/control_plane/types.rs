use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineSpecId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct PipelineId(pub Uuid);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct AttemptId(pub u32);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct WorkerId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OperatorId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct TaskId(pub String);

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct PipelineExecutionContext {
    pub pipeline_spec_id: PipelineSpecId,
    pub pipeline_id: PipelineId,
    pub attempt_id: AttemptId,
}

impl PipelineExecutionContext {
    pub fn new(pipeline_spec_id: PipelineSpecId, pipeline_id: PipelineId, attempt_id: AttemptId) -> Self {
        Self {
            pipeline_spec_id,
            pipeline_id,
            attempt_id,
        }
    }

    pub fn fresh(attempt_id: AttemptId) -> Self {
        Self::new(
            PipelineSpecId(Uuid::new_v4()),
            PipelineId(Uuid::new_v4()),
            attempt_id,
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineDesiredState {
    Running,
    Stopped,
    Paused,
    Draining,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum PipelineLifecycleState {
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
pub struct PipelineRun {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub created_at: DateTime<Utc>,
    pub desired_state: PipelineDesiredState,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineStatus {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub state: PipelineLifecycleState,
    pub updated_at: DateTime<Utc>,
    pub worker_count: usize,
    pub task_count: usize,
    pub last_checkpoint_id: Option<u64>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum PipelineEventKind {
    DesiredStateChanged { desired: PipelineDesiredState },
    StateChanged { state: PipelineLifecycleState },
    WorkerSeen { worker_id: WorkerId },
    WorkerLost { worker_id: WorkerId, reason: String },
    CheckpointStarted { checkpoint_id: u64 },
    CheckpointCompleted { checkpoint_id: u64 },
    CheckpointFailed { checkpoint_id: u64, error: String },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PipelineEvent {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub at: DateTime<Utc>,
    pub kind: PipelineEventKind,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CheckpointMetadata {
    pub pipeline_execution_context: PipelineExecutionContext,
    pub checkpoint_id: u64,
    pub committed_at: DateTime<Utc>,
    pub manifest_uri: String,
}

