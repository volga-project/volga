use std::collections::VecDeque;

use serde::{Deserialize, Serialize};

const MAX_EVENTS: usize = 512;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LifecycleEventRecord {
    pub sequence: u64,
    pub event: LifecycleEvent,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum LifecycleEvent {
    WorkerRegistered {
        worker_id: String,
    },
    AttemptStarted {
        attempt_id: u64,
        restore_checkpoint_id: Option<u64>,
    },
    AttemptScheduled {
        attempt_id: u64,
        worker_ids: Vec<String>,
    },
    AttemptRunning {
        attempt_id: u64,
        worker_ids: Vec<String>,
    },
    WorkerFailure {
        attempt_id: u64,
        worker_id: String,
        kind: String,
        detail: String,
    },
    RecoveryStarted {
        attempt_id: u64,
        replacement_worker_ids: Vec<String>,
    },
    ReplacementRequested {
        worker_ids: Vec<String>,
    },
    CheckpointCompleted {
        checkpoint_id: u64,
    },
    PipelineFinished,
    PipelineFailed {
        detail: String,
    },
}

#[derive(Default)]
pub(super) struct LifecycleJournal {
    next_sequence: u64,
    events: VecDeque<LifecycleEventRecord>,
}

impl LifecycleJournal {
    pub(super) fn record(&mut self, event: LifecycleEvent) -> LifecycleEventRecord {
        self.next_sequence += 1;
        let record = LifecycleEventRecord {
            sequence: self.next_sequence,
            event,
        };
        self.events.push_back(record.clone());
        if self.events.len() > MAX_EVENTS {
            self.events.pop_front();
        }
        record
    }

    pub(super) fn since(&self, sequence: u64) -> Vec<LifecycleEventRecord> {
        self.events
            .iter()
            .filter(|record| record.sequence > sequence)
            .cloned()
            .collect()
    }
}
