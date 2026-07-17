//! Checkpoint protocol: single in-flight start / ack / abort / complete.

use std::collections::{BTreeSet, HashSet};
use std::time::{Duration, Instant};

use super::{
    CheckpointAckOutcome, CheckpointAckReject, CheckpointStartError, TaskKey,
};

#[derive(Debug)]
struct InFlightCheckpoint {
    checkpoint_id: u64,
    started_at: Instant,
    acks: HashSet<TaskKey>,
}

#[derive(Debug, Default)]
pub(super) struct CheckpointProtocol {
    next_id: u64,
    in_flight: Option<InFlightCheckpoint>,
    completed: BTreeSet<u64>,
}

impl CheckpointProtocol {
    pub(super) fn in_flight_id(&self) -> Option<u64> {
        self.in_flight.as_ref().map(|c| c.checkpoint_id)
    }

    pub(super) fn in_flight_timed_out(&self, timeout: Duration) -> Option<u64> {
        let in_flight = self.in_flight.as_ref()?;
        if in_flight.started_at.elapsed() >= timeout {
            Some(in_flight.checkpoint_id)
        } else {
            None
        }
    }

    pub(super) fn start(
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
        Ok(checkpoint_id)
    }

    pub(super) fn abort_in_flight(&mut self) -> Option<u64> {
        self.in_flight.take().map(|c| c.checkpoint_id)
    }

    /// Record an ack for the in-flight checkpoint. Completes only when acks equal
    /// `expected_tasks` exactly (same set).
    pub(super) fn ack(
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
        if !in_flight.acks.insert(task) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::DuplicateTask);
        }

        if in_flight.acks == *expected_tasks {
            self.in_flight = None;
            self.completed.insert(checkpoint_id);
            CheckpointAckOutcome::Completed
        } else {
            CheckpointAckOutcome::Pending
        }
    }

    pub(super) fn latest_complete(&self) -> Option<u64> {
        self.completed.iter().next_back().copied()
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

    #[test]
    fn completes_only_on_exact_expected_set() {
        let mut protocol = CheckpointProtocol::default();
        let expected: HashSet<_> = [task("a", 0), task("a", 1)].into_iter().collect();
        let id = protocol.start(&expected).unwrap();
        assert_eq!(
            protocol.ack(id, task("a", 0), &expected),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            protocol.ack(id, task("a", 1), &expected),
            CheckpointAckOutcome::Completed
        );
        assert_eq!(protocol.latest_complete(), Some(id));
        assert!(protocol.in_flight_id().is_none());
    }

    #[test]
    fn rejects_unexpected_and_duplicate() {
        let mut protocol = CheckpointProtocol::default();
        let expected: HashSet<_> = [task("a", 0)].into_iter().collect();
        let id = protocol.start(&expected).unwrap();
        assert!(matches!(
            protocol.ack(id, task("b", 0), &expected),
            CheckpointAckOutcome::Rejected(CheckpointAckReject::UnexpectedTask)
        ));
        assert_eq!(
            protocol.ack(id, task("a", 0), &expected),
            CheckpointAckOutcome::Completed
        );
        assert!(matches!(
            protocol.ack(id, task("a", 0), &expected),
            CheckpointAckOutcome::Rejected(CheckpointAckReject::AlreadyComplete)
        ));
    }

    #[test]
    fn single_in_flight() {
        let mut protocol = CheckpointProtocol::default();
        let expected: HashSet<_> = [task("a", 0)].into_iter().collect();
        let id = protocol.start(&expected).unwrap();
        assert_eq!(
            protocol.start(&expected),
            Err(CheckpointStartError::AlreadyInFlight {
                checkpoint_id: id
            })
        );
        protocol.abort_in_flight();
        assert!(protocol.start(&expected).is_ok());
    }
}
