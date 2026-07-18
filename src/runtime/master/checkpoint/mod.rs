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

/// Checkpoint protocol + blob store. No lifecycle journaling or attempt fencing.
#[derive(Debug)]
pub struct Checkpoints {
    /// Tasks that must report state blobs (checkpointable ops).
    expected_acks: HashSet<TaskKey>,
    /// All tasks that must report barrier progress (Injected or Aligned).
    expected_aligns: HashSet<TaskKey>,
    /// Max completed checkpoints to keep (ids + blobs). Always ≥ 1.
    retention: usize,
    protocol: CheckpointProtocol,
    blobs: CheckpointBlobs,
}

impl Default for Checkpoints {
    fn default() -> Self {
        Self {
            expected_acks: HashSet::new(),
            expected_aligns: HashSet::new(),
            retention: 1,
            protocol: CheckpointProtocol::default(),
            blobs: CheckpointBlobs::default(),
        }
    }
}

impl Checkpoints {
    pub fn configure(
        &mut self,
        expected_acks: HashSet<TaskKey>,
        expected_aligns: HashSet<TaskKey>,
        retention: usize,
    ) {
        self.expected_acks = expected_acks;
        self.expected_aligns = expected_aligns;
        self.retention = retention.max(1);
    }

    pub fn start(&mut self) -> Result<u64, CheckpointStartError> {
        self.protocol
            .start(&self.expected_acks, &self.expected_aligns)
    }

    pub fn abort_in_flight(&mut self) -> Option<u64> {
        let checkpoint_id = self.protocol.abort_in_flight()?;
        self.blobs.remove_checkpoint(checkpoint_id);
        Some(checkpoint_id)
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

    pub fn get(&self, checkpoint_id: u64, task: &TaskKey) -> Vec<(String, Vec<u8>)> {
        self.blobs.get(checkpoint_id, task)
    }

    /// Store blobs and record a state ack. May complete if aligns are already done.
    pub fn report(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        blobs: Vec<(String, Vec<u8>)>,
    ) -> CheckpointAckOutcome {
        if !self.expected_acks.contains(&task) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::UnexpectedTask);
        }
        self.blobs.put(checkpoint_id, task.clone(), blobs);
        let outcome = self.protocol.ack(
            checkpoint_id,
            task,
            &self.expected_acks,
            &self.expected_aligns,
        );
        self.prune_if_completed(outcome)
    }

    /// Record barrier Injected/Aligned for a task. May complete if acks are already done.
    pub fn note_barrier_progress(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
    ) -> CheckpointAckOutcome {
        let outcome = self.protocol.align(
            checkpoint_id,
            task,
            &self.expected_acks,
            &self.expected_aligns,
        );
        self.prune_if_completed(outcome)
    }

    fn prune_if_completed(&mut self, outcome: CheckpointAckOutcome) -> CheckpointAckOutcome {
        if matches!(outcome, CheckpointAckOutcome::Completed) {
            for pruned_id in self.protocol.retain_completed(self.retention) {
                self.blobs.remove_checkpoint(pruned_id);
            }
        }
        outcome
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

    fn complete(cps: &mut Checkpoints, id_hint: u64) -> u64 {
        let id = cps.start().unwrap();
        assert_eq!(id, id_hint);
        assert_eq!(
            cps.report(id, task("a", 0), vec![("s".into(), vec![id as u8])]),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            cps.note_barrier_progress(id, task("a", 0)),
            CheckpointAckOutcome::Completed
        );
        id
    }

    #[test]
    fn retention_keeps_latest_n_and_drops_blobs() {
        let mut cps = Checkpoints::default();
        let acks: HashSet<_> = [task("a", 0)].into_iter().collect();
        let aligns = acks.clone();
        cps.configure(acks, aligns, 2);

        let id1 = complete(&mut cps, 1);
        let id2 = complete(&mut cps, 2);
        assert!(!cps.get(id1, &task("a", 0)).is_empty());
        assert!(!cps.get(id2, &task("a", 0)).is_empty());

        let id3 = complete(&mut cps, 3);
        assert!(cps.get(id1, &task("a", 0)).is_empty());
        assert!(!cps.get(id2, &task("a", 0)).is_empty());
        assert!(!cps.get(id3, &task("a", 0)).is_empty());
        assert_eq!(cps.latest_complete(), Some(id3));
        assert!(!cps.is_completed(id1));
        assert!(cps.is_completed(id2));
        assert!(cps.is_completed(id3));
    }

    #[test]
    fn abort_drops_partial_blobs() {
        let mut cps = Checkpoints::default();
        let acks: HashSet<_> = [task("a", 0), task("a", 1)].into_iter().collect();
        let aligns = acks.clone();
        cps.configure(acks, aligns, 1);
        let id = cps.start().unwrap();
        assert_eq!(
            cps.report(id, task("a", 0), vec![("s".into(), vec![1])]),
            CheckpointAckOutcome::Pending
        );
        assert!(!cps.get(id, &task("a", 0)).is_empty());
        assert_eq!(cps.abort_in_flight(), Some(id));
        assert!(cps.get(id, &task("a", 0)).is_empty());
    }

    #[test]
    fn completes_after_downstream_align() {
        let mut cps = Checkpoints::default();
        let acks: HashSet<_> = [task("src", 0)].into_iter().collect();
        let aligns: HashSet<_> = [task("src", 0), task("sink", 0)].into_iter().collect();
        cps.configure(acks, aligns, 1);
        let id = cps.start().unwrap();
        assert_eq!(
            cps.note_barrier_progress(id, task("src", 0)),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            cps.report(id, task("src", 0), vec![("s".into(), vec![1])]),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            cps.note_barrier_progress(id, task("sink", 0)),
            CheckpointAckOutcome::Completed
        );
    }
}
