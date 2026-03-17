use std::sync::Arc;

use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::batch::Timestamp;
use crate::storage::index::BucketIndex;

pub trait BucketIndexState: Send + Sync {
    fn bucket_index(&self) -> &BucketIndex;
    fn bucket_index_mut(&mut self) -> &mut BucketIndex;
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DumpMode {
    KeepHot,
    EvictHot,
}

#[derive(Clone)]
pub struct DumpPlan<T: BucketIndexState> {
    pub task_id: TaskId,
    pub key: Key,
    pub arc: Arc<RwLock<T>>,
    pub bucket_ts: Timestamp,
    pub expected_version: u64,
    pub key_hash: u64,
}
