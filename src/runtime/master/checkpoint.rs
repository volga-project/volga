use std::collections::{BTreeSet, HashMap, HashSet};
use std::time::Instant;

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

#[derive(Debug)]
struct InFlightCheckpoint {
    checkpoint_id: u64,
    started_at: Instant,
    acks: HashSet<TaskKey>,
}

#[derive(Debug, Default)]
pub struct MasterCheckpointCoordinator {
    next_id: u64,
    in_flight: Option<InFlightCheckpoint>,
    pub completed: BTreeSet<u64>,
    /// Accumulated acks for debugging / late inspection (not used for completion).
    pub acks: HashMap<u64, HashSet<TaskKey>>,
}

impl MasterCheckpointCoordinator {
    pub fn in_flight_id(&self) -> Option<u64> {
        self.in_flight.as_ref().map(|c| c.checkpoint_id)
    }

    pub fn in_flight_started_at(&self) -> Option<Instant> {
        self.in_flight.as_ref().map(|c| c.started_at)
    }

    pub fn start(
        &mut self,
        expected_tasks: &HashSet<TaskKey>,
    ) -> Result<u64, CheckpointStartError> {
        if expected_tasks.is_empty() {
            return Err(CheckpointStartError::NoCheckpointableTasks);
        }
        if let Some(in_flight) = &self.in_flight {
            return Err(CheckpointStartError::AlreadyInFlight {
                checkpoint_id: in_flight.checkpoint_id,
            });
        }
        self.next_id += 1;
        let checkpoint_id = self.next_id;
        self.in_flight = Some(InFlightCheckpoint {
            checkpoint_id,
            started_at: Instant::now(),
            acks: HashSet::new(),
        });
        self.acks.insert(checkpoint_id, HashSet::new());
        Ok(checkpoint_id)
    }

    pub fn abort_in_flight(&mut self) -> Option<u64> {
        self.in_flight.take().map(|c| c.checkpoint_id)
    }

    /// Record an ack for the in-flight checkpoint. Completes only when acks equal
    /// `expected_tasks` exactly (same set).
    pub fn ack(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        expected_tasks: &HashSet<TaskKey>,
    ) -> CheckpointAckOutcome {
        if self.completed.contains(&checkpoint_id) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::AlreadyComplete);
        }
        let Some(in_flight) = self.in_flight.as_mut() else {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::NotInFlight);
        };
        if in_flight.checkpoint_id != checkpoint_id {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::NotInFlight);
        }
        if !expected_tasks.contains(&task) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::UnexpectedTask);
        }
        if !in_flight.acks.insert(task.clone()) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::DuplicateTask);
        }
        self.acks
            .entry(checkpoint_id)
            .or_default()
            .insert(task);

        if in_flight.acks == *expected_tasks {
            self.in_flight = None;
            self.completed.insert(checkpoint_id);
            CheckpointAckOutcome::Completed
        } else {
            CheckpointAckOutcome::Pending
        }
    }

    pub fn latest_complete(&self) -> Option<u64> {
        self.completed.iter().next_back().copied()
    }
}

#[derive(Debug, Default)]
pub struct MasterCheckpointStore {
    pub checkpoint_snapshots: HashMap<(u64, TaskKey), Vec<(String, Vec<u8>)>>,
}

impl MasterCheckpointStore {
    pub fn put(&mut self, checkpoint_id: u64, task: TaskKey, blobs: Vec<(String, Vec<u8>)>) {
        self.checkpoint_snapshots
            .insert((checkpoint_id, task), blobs);
    }

    pub fn get(&self, checkpoint_id: u64, task: &TaskKey) -> Option<&Vec<(String, Vec<u8>)>> {
        self.checkpoint_snapshots.get(&(checkpoint_id, task.clone()))
    }
}

#[derive(Debug, Default)]
pub struct MasterCheckpointRegistry {
    pub coordinator: MasterCheckpointCoordinator,
    pub store: MasterCheckpointStore,
    pub expected_tasks: HashSet<TaskKey>,
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

    #[test]
    fn completes_only_on_exact_expected_set() {
        let mut coord = MasterCheckpointCoordinator::default();
        let expected: HashSet<_> = [task("a", 0), task("a", 1)].into_iter().collect();
        let id = coord.start(&expected).unwrap();
        assert_eq!(
            coord.ack(id, task("a", 0), &expected),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            coord.ack(id, task("a", 1), &expected),
            CheckpointAckOutcome::Completed
        );
        assert_eq!(coord.latest_complete(), Some(id));
        assert!(coord.in_flight_id().is_none());
    }

    #[test]
    fn rejects_unexpected_and_duplicate() {
        let mut coord = MasterCheckpointCoordinator::default();
        let expected: HashSet<_> = [task("a", 0)].into_iter().collect();
        let id = coord.start(&expected).unwrap();
        assert!(matches!(
            coord.ack(id, task("b", 0), &expected),
            CheckpointAckOutcome::Rejected(CheckpointAckReject::UnexpectedTask)
        ));
        assert_eq!(
            coord.ack(id, task("a", 0), &expected),
            CheckpointAckOutcome::Completed
        );
        assert!(matches!(
            coord.ack(id, task("a", 0), &expected),
            CheckpointAckOutcome::Rejected(CheckpointAckReject::AlreadyComplete)
        ));
    }

    #[test]
    fn single_in_flight() {
        let mut coord = MasterCheckpointCoordinator::default();
        let expected: HashSet<_> = [task("a", 0)].into_iter().collect();
        let id = coord.start(&expected).unwrap();
        assert_eq!(
            coord.start(&expected),
            Err(CheckpointStartError::AlreadyInFlight {
                checkpoint_id: id
            })
        );
        coord.abort_in_flight();
        assert!(coord.start(&expected).is_ok());
    }
}
