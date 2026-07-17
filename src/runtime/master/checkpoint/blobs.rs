//! Per-task checkpoint blob storage.

use std::collections::HashMap;

use super::TaskKey;

#[derive(Debug, Default)]
pub(super) struct CheckpointBlobs {
    snapshots: HashMap<(u64, TaskKey), Vec<(String, Vec<u8>)>>,
}

impl CheckpointBlobs {
    pub(super) fn put(
        &mut self,
        checkpoint_id: u64,
        task: TaskKey,
        blobs: Vec<(String, Vec<u8>)>,
    ) {
        self.snapshots.insert((checkpoint_id, task), blobs);
    }

    pub(super) fn get(
        &self,
        checkpoint_id: u64,
        task: &TaskKey,
    ) -> Vec<(String, Vec<u8>)> {
        self.snapshots
            .get(&(checkpoint_id, task.clone()))
            .cloned()
            .unwrap_or_default()
    }
}
