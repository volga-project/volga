use std::collections::{BTreeSet, HashMap, HashSet};

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct TaskKey {
    pub vertex_id: String,
    pub task_index: i32,
}

#[derive(Debug, Default)]
pub struct MasterCheckpointCoordinator {
    pub acks: HashMap<u64, HashSet<TaskKey>>,
    pub completed: BTreeSet<u64>,
}

impl MasterCheckpointCoordinator {
    pub fn ack(&mut self, checkpoint_id: u64, task: TaskKey, expected_task_count: usize) {
        self.acks.entry(checkpoint_id).or_default().insert(task);
        if expected_task_count != 0 {
            if let Some(acked) = self.acks.get(&checkpoint_id) {
                if acked.len() == expected_task_count {
                    self.completed.insert(checkpoint_id);
                }
            }
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
}

#[derive(Debug, Default)]
pub struct MasterCheckpointRegistry {
    pub coordinator: MasterCheckpointCoordinator,
    pub store: MasterCheckpointStore,
    pub expected_tasks: HashSet<TaskKey>,
}
