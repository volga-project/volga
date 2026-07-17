//! Master checkpoint domain: protocol + blob storage behind a single façade.

mod blobs;
mod protocol;

use std::collections::HashSet;
use std::time::Duration;

use blobs::CheckpointBlobs;
use protocol::CheckpointProtocol;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TaskKey {
    pub vertex_id: String,
    pub task_index: i32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointStartError {
    AlreadyInFlight { checkpoint_id: u64 },
    NoCheckpointableTasks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointAckOutcome {
    /// Ack recorded; checkpoint still waiting for more tasks.
    Pending,
    /// Exact expected task set acknowledged; checkpoint newly completed.
    Completed,
    /// Ignored / rejected without completing.
    Rejected(CheckpointAckReject),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointAckReject {
    NotInFlight,
    UnexpectedTask,
    DuplicateTask,
    AlreadyComplete,
}

/// Checkpoint protocol + blob store. No lifecycle journaling or attempt fencing.
#[derive(Debug, Default)]
pub struct Checkpoints {
    expected_tasks: HashSet<TaskKey>,
    protocol: CheckpointProtocol,
    blobs: CheckpointBlobs,
}

impl Checkpoints {
    pub fn configure(&mut self, expected_tasks: HashSet<TaskKey>) {
        self.expected_tasks = expected_tasks;
    }

    pub fn start(&mut self) -> Result<u64, CheckpointStartError> {
        self.protocol.start(&self.expected_tasks)
    }

    pub fn abort_in_flight(&mut self) -> Option<u64> {
        self.protocol.abort_in_flight()
    }

    pub fn in_flight_id(&self) -> Option<u64> {
        self.protocol.in_flight_id()
    }

    pub fn in_flight_timed_out(&self, timeout: Duration) -> Option<u64> {
        self.protocol.in_flight_timed_out(timeout)
    }

    pub fn latest_complete(&self) -> Option<u64> {
        self.protocol.latest_complete()
    }

    pub fn get(&self, checkpoint_id: u64, task: &TaskKey) -> Vec<(String, Vec<u8>)> {
        self.blobs.get(checkpoint_id, task)
    }

    /// Store blobs and record an ack. Completes when the expected task set is exact.
    pub fn report(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        blobs: Vec<(String, Vec<u8>)>,
    ) -> CheckpointAckOutcome {
        if !self.expected_tasks.contains(&task) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::UnexpectedTask);
        }
        self.blobs.put(checkpoint_id, task.clone(), blobs);
        self.protocol
            .ack(checkpoint_id, task, &self.expected_tasks)
    }
}
