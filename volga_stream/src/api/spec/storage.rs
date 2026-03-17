use serde::{Deserialize, Serialize};

use crate::storage::{StorageBackendConfig, TimeGranularity};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageSpec {
    pub budgets: StorageBackendConfig,
    pub inmem_store_lock_pool_size: usize,
    pub inmem_store_bucket_granularity: TimeGranularity,
    pub inmem_store_max_batch_size: usize,
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            budgets: StorageBackendConfig::default(),
            inmem_store_lock_pool_size: 4096,
            inmem_store_bucket_granularity: TimeGranularity::Seconds(1),
            inmem_store_max_batch_size: 1024,
        }
    }
}

