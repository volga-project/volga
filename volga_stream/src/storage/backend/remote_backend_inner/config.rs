use serde::{Deserialize, Serialize};

use crate::storage::index::TimeGranularity;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteBackendInnerConfig {
    pub db_path: String,
    pub checkpoint_lifetime_secs: u64,

    pub bucket_granularity: TimeGranularity,
    pub max_batch_size: usize,

    pub foyer_memory_bytes: usize,
    pub foyer_disk_bytes: usize,
    pub foyer_disk_path: String,
}

