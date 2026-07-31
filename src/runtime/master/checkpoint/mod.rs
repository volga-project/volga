//! Master checkpoint domain: protocol + task checkpoint storage.

mod protocol;
mod restore;
mod store;

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::Duration;

use crate::common::types::PipelineId;
use crate::runtime::checkpoint::{CompletedCheckpoint, SerializedCheckpoint};

pub use crate::runtime::checkpoint::TaskKey;
use protocol::CheckpointProtocol;
pub use restore::RestorePlanner;
pub use store::{create_checkpoint_store, CheckpointStore, InMemoryCheckpointStore};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointStartError {
    AlreadyInFlight { checkpoint_id: u64 },
    NoCheckpointableTasks,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum CheckpointAckOutcome {
    /// Progress recorded; still waiting for state acks and/or barrier aligns.
    Pending,
    /// Expected state acks and barrier aligns both satisfied; newly completed.
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

/// Checkpoint protocol + completed-checkpoint store.
#[derive(Debug)]
pub struct Checkpoints {
    /// Tasks that must report operator checkpoint data.
    expected_acks: HashSet<TaskKey>,
    /// All tasks that must report barrier progress (Injected or Aligned).
    expected_aligns: HashSet<TaskKey>,
    /// Max completed checkpoints to keep. Always ≥ 1.
    retention: usize,
    pipeline_id: Option<PipelineId>,
    protocol: CheckpointProtocol,
    store: Option<Arc<dyn CheckpointStore>>,
    pending: HashMap<u64, HashMap<TaskKey, SerializedCheckpoint>>,
}

impl Default for Checkpoints {
    fn default() -> Self {
        Self {
            expected_acks: HashSet::new(),
            expected_aligns: HashSet::new(),
            retention: 1,
            pipeline_id: None,
            protocol: CheckpointProtocol::default(),
            store: None,
            pending: HashMap::new(),
        }
    }
}

impl Checkpoints {
    pub fn configure(
        &mut self,
        pipeline_id: PipelineId,
        expected_acks: HashSet<TaskKey>,
        expected_aligns: HashSet<TaskKey>,
        retention: usize,
        store: Arc<dyn CheckpointStore>,
    ) {
        self.pipeline_id = Some(pipeline_id);
        self.expected_acks = expected_acks;
        self.expected_aligns = expected_aligns;
        self.retention = retention.max(1);
        self.store = Some(store);
        self.protocol = CheckpointProtocol::default();
        self.pending.clear();
    }

    pub fn start(&mut self) -> Result<u64, CheckpointStartError> {
        self.protocol
            .start(&self.expected_acks, &self.expected_aligns)
    }

    pub async fn abort_in_flight(&mut self) -> anyhow::Result<Option<u64>> {
        let Some(checkpoint_id) = self.protocol.abort_in_flight() else {
            return Ok(None);
        };
        self.pending.remove(&checkpoint_id);
        Ok(Some(checkpoint_id))
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

    pub fn is_completed(&self, checkpoint_id: u64) -> bool {
        self.protocol.is_completed(checkpoint_id)
    }

    pub async fn load(&self, checkpoint_id: u64) -> anyhow::Result<CompletedCheckpoint> {
        self.store()?
            .load(self.pipeline_id()?, checkpoint_id)
            .await?
            .ok_or_else(|| anyhow::anyhow!("completed checkpoint {checkpoint_id} not found"))
    }

    /// Record operator state and its ack. May complete if aligns are already done.
    pub async fn report(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        checkpoint: SerializedCheckpoint,
    ) -> anyhow::Result<CheckpointAckOutcome> {
        if !self.expected_acks.contains(&task) {
            return Ok(CheckpointAckOutcome::Rejected(
                CheckpointAckReject::UnexpectedTask,
            ));
        }
        let outcome = self.protocol.ack(
            checkpoint_id,
            task.clone(),
            &self.expected_acks,
            &self.expected_aligns,
        );
        if !matches!(outcome, CheckpointAckOutcome::Rejected(_)) {
            self.pending
                .entry(checkpoint_id)
                .or_default()
                .insert(task, checkpoint);
        }
        self.finish_if_completed(checkpoint_id, outcome).await
    }

    /// Record barrier Injected/Aligned for a task. May complete if acks are already done.
    pub async fn note_barrier_progress(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
    ) -> anyhow::Result<CheckpointAckOutcome> {
        let outcome = self.protocol.align(
            checkpoint_id,
            task,
            &self.expected_acks,
            &self.expected_aligns,
        );
        self.finish_if_completed(checkpoint_id, outcome).await
    }

    async fn finish_if_completed(
        &mut self,
        checkpoint_id: u64,
        outcome: CheckpointAckOutcome,
    ) -> anyhow::Result<CheckpointAckOutcome> {
        if matches!(outcome, CheckpointAckOutcome::Completed) {
            let tasks = self.pending.remove(&checkpoint_id).unwrap_or_default();
            anyhow::ensure!(
                tasks.len() == self.expected_acks.len(),
                "checkpoint {checkpoint_id} completed without all task state"
            );
            self.store()?
                .save(
                    self.pipeline_id()?,
                    CompletedCheckpoint {
                        checkpoint_id,
                        tasks,
                    },
                )
                .await?;
            for pruned_id in self.protocol.retain_completed(self.retention) {
                self.store()?
                    .remove(self.pipeline_id()?, pruned_id)
                    .await?;
            }
        }
        Ok(outcome)
    }

    fn pipeline_id(&self) -> anyhow::Result<&PipelineId> {
        self.pipeline_id
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("checkpoints are not configured"))
    }

    fn store(&self) -> anyhow::Result<&Arc<dyn CheckpointStore>> {
        self.store
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("checkpoint store is not configured"))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn task(vertex: &str, index: i32) -> TaskKey {
        TaskKey {
            vertex_id: vertex.to_string(),
            task_index: index,
        }
    }

    fn pipeline() -> PipelineId {
        PipelineId("test-pipeline".to_string())
    }

    fn store() -> Arc<dyn CheckpointStore> {
        Arc::new(InMemoryCheckpointStore::default())
    }

    fn checkpoint(value: u8) -> SerializedCheckpoint {
        SerializedCheckpoint::new(vec![value])
    }

    async fn complete(cps: &mut Checkpoints, id_hint: u64) -> u64 {
        let id = cps.start().unwrap();
        assert_eq!(id, id_hint);
        assert_eq!(
            cps.report(id, task("a", 0), checkpoint(id as u8),)
                .await
                .unwrap(),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            cps.note_barrier_progress(id, task("a", 0)).await.unwrap(),
            CheckpointAckOutcome::Completed
        );
        id
    }

    #[tokio::test]
    async fn retention_keeps_latest_n() {
        let mut cps = Checkpoints::default();
        let acks: HashSet<_> = [task("a", 0)].into_iter().collect();
        let aligns = acks.clone();
        cps.configure(pipeline(), acks, aligns, 2, store());

        let id1 = complete(&mut cps, 1).await;
        let id2 = complete(&mut cps, 2).await;
        assert_eq!(cps.load(id1).await.unwrap().tasks.len(), 1);
        assert_eq!(cps.load(id2).await.unwrap().tasks.len(), 1);

        let id3 = complete(&mut cps, 3).await;
        assert!(cps.load(id1).await.is_err());
        assert_eq!(cps.load(id2).await.unwrap().tasks.len(), 1);
        assert_eq!(cps.load(id3).await.unwrap().tasks.len(), 1);
        assert_eq!(cps.latest_complete(), Some(id3));
        assert!(!cps.is_completed(id1));
        assert!(cps.is_completed(id2));
        assert!(cps.is_completed(id3));
    }

    #[tokio::test]
    async fn abort_drops_partial_state() {
        let mut cps = Checkpoints::default();
        let acks: HashSet<_> = [task("a", 0), task("a", 1)].into_iter().collect();
        let aligns = acks.clone();
        cps.configure(pipeline(), acks, aligns, 1, store());
        let id = cps.start().unwrap();
        assert_eq!(
            cps.report(id, task("a", 0), checkpoint(1),).await.unwrap(),
            CheckpointAckOutcome::Pending
        );
        assert!(cps.load(id).await.is_err());
        assert_eq!(cps.abort_in_flight().await.unwrap(), Some(id));
        assert!(cps.load(id).await.is_err());
    }

    #[tokio::test]
    async fn completes_after_downstream_align() {
        let mut cps = Checkpoints::default();
        let acks: HashSet<_> = [task("src", 0)].into_iter().collect();
        let aligns: HashSet<_> = [task("src", 0), task("sink", 0)].into_iter().collect();
        cps.configure(pipeline(), acks, aligns, 1, store());
        let id = cps.start().unwrap();
        assert_eq!(
            cps.note_barrier_progress(id, task("src", 0)).await.unwrap(),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            cps.report(id, task("src", 0), checkpoint(1),)
                .await
                .unwrap(),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            cps.note_barrier_progress(id, task("sink", 0))
                .await
                .unwrap(),
            CheckpointAckOutcome::Completed
        );
    }
}
