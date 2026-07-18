//! Checkpoint protocol: single in-flight start / state-ack / barrier-align / complete.

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
    aligns: HashSet<TaskKey>,
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
        expected_acks: &HashSet<TaskKey>,
        expected_aligns: &HashSet<TaskKey>,
    ) -> Result<u64, CheckpointStartError> {
        if expected_acks.is_empty() {
            return Err(CheckpointStartError::NoCheckpointableTasks);
        }
        if expected_aligns.is_empty() {
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
            aligns: HashSet::new(),
        });
        Ok(checkpoint_id)
    }

    pub(super) fn abort_in_flight(&mut self) -> Option<u64> {
        self.in_flight.take().map(|c| c.checkpoint_id)
    }

    /// Record a state ack. Completes only when acks and aligns both match expected sets.
    pub(super) fn ack(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        expected_acks: &HashSet<TaskKey>,
        expected_aligns: &HashSet<TaskKey>,
    ) -> CheckpointAckOutcome {
        self.record_member(
            checkpoint_id,
            task,
            expected_acks,
            |in_flight| &mut in_flight.acks,
            expected_acks,
            expected_aligns,
        )
    }

    /// Record barrier progress (Injected or Aligned). Completes when both sets match.
    pub(super) fn align(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        expected_acks: &HashSet<TaskKey>,
        expected_aligns: &HashSet<TaskKey>,
    ) -> CheckpointAckOutcome {
        self.record_member(
            checkpoint_id,
            task,
            expected_aligns,
            |in_flight| &mut in_flight.aligns,
            expected_acks,
            expected_aligns,
        )
    }

    fn record_member(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        expected_for_kind: &HashSet<TaskKey>,
        bucket: impl FnOnce(&mut InFlightCheckpoint) -> &mut HashSet<TaskKey>,
        expected_acks: &HashSet<TaskKey>,
        expected_aligns: &HashSet<TaskKey>,
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
        if !expected_for_kind.contains(&task) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::UnexpectedTask);
        }
        if !bucket(in_flight).insert(task) {
            return CheckpointAckOutcome::Rejected(CheckpointAckReject::DuplicateTask);
        }

        if in_flight.acks == *expected_acks && in_flight.aligns == *expected_aligns {
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

    pub(super) fn is_completed(&self, checkpoint_id: u64) -> bool {
        self.completed.contains(&checkpoint_id)
    }

    /// Drop oldest completed ids until at most `retention` remain. Returns pruned ids.
    pub(super) fn retain_completed(&mut self, retention: usize) -> Vec<u64> {
        let retention = retention.max(1);
        let mut pruned = Vec::new();
        while self.completed.len() > retention {
            let Some(id) = self.completed.iter().next().copied() else {
                break;
            };
            self.completed.remove(&id);
            pruned.push(id);
        }
        pruned
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

    fn complete_one(
        protocol: &mut CheckpointProtocol,
        acks: &HashSet<TaskKey>,
        aligns: &HashSet<TaskKey>,
        t: TaskKey,
    ) -> u64 {
        let id = protocol.start(acks, aligns).unwrap();
        assert_eq!(protocol.ack(id, t.clone(), acks, aligns), CheckpointAckOutcome::Pending);
        assert_eq!(
            protocol.align(id, t, acks, aligns),
            CheckpointAckOutcome::Completed
        );
        id
    }

    #[test]
    fn completes_only_when_acks_and_aligns_match() {
        let mut protocol = CheckpointProtocol::default();
        let acks: HashSet<_> = [task("src", 0)].into_iter().collect();
        let aligns: HashSet<_> = [task("src", 0), task("sink", 0)].into_iter().collect();
        let id = protocol.start(&acks, &aligns).unwrap();

        assert_eq!(
            protocol.ack(id, task("src", 0), &acks, &aligns),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            protocol.align(id, task("src", 0), &acks, &aligns),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            protocol.align(id, task("sink", 0), &acks, &aligns),
            CheckpointAckOutcome::Completed
        );
        assert_eq!(protocol.latest_complete(), Some(id));
        assert!(protocol.in_flight_id().is_none());
    }

    #[test]
    fn rejects_unexpected_and_duplicate_ack() {
        let mut protocol = CheckpointProtocol::default();
        let acks: HashSet<_> = [task("a", 0)].into_iter().collect();
        let aligns: HashSet<_> = [task("a", 0)].into_iter().collect();
        let id = protocol.start(&acks, &aligns).unwrap();
        assert!(matches!(
            protocol.ack(id, task("b", 0), &acks, &aligns),
            CheckpointAckOutcome::Rejected(CheckpointAckReject::UnexpectedTask)
        ));
        assert_eq!(
            protocol.ack(id, task("a", 0), &acks, &aligns),
            CheckpointAckOutcome::Pending
        );
        assert_eq!(
            protocol.align(id, task("a", 0), &acks, &aligns),
            CheckpointAckOutcome::Completed
        );
        assert!(matches!(
            protocol.ack(id, task("a", 0), &acks, &aligns),
            CheckpointAckOutcome::Rejected(CheckpointAckReject::AlreadyComplete)
        ));
    }

    #[test]
    fn single_in_flight() {
        let mut protocol = CheckpointProtocol::default();
        let acks: HashSet<_> = [task("a", 0)].into_iter().collect();
        let aligns: HashSet<_> = [task("a", 0)].into_iter().collect();
        let id = protocol.start(&acks, &aligns).unwrap();
        assert_eq!(
            protocol.start(&acks, &aligns),
            Err(CheckpointStartError::AlreadyInFlight {
                checkpoint_id: id
            })
        );
        protocol.abort_in_flight();
        assert!(protocol.start(&acks, &aligns).is_ok());
    }

    #[test]
    fn retain_completed_prunes_oldest() {
        let mut protocol = CheckpointProtocol::default();
        let acks: HashSet<_> = [task("a", 0)].into_iter().collect();
        let aligns: HashSet<_> = [task("a", 0)].into_iter().collect();
        for _ in 0..3 {
            complete_one(&mut protocol, &acks, &aligns, task("a", 0));
        }
        assert_eq!(protocol.retain_completed(2), vec![1]);
        assert!(!protocol.is_completed(1));
        assert!(protocol.is_completed(2));
        assert!(protocol.is_completed(3));
        assert_eq!(protocol.latest_complete(), Some(3));
    }
}
