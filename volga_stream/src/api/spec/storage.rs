use serde::{Deserialize, Serialize};

use crate::runtime::operators::window::TimeGranularity;
use crate::storage::StorageBudgetConfig;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorageSpec {
    pub budgets: StorageBudgetConfig,
    pub inmem_store_lock_pool_size: usize,
    pub inmem_store_bucket_granularity: TimeGranularity,
    pub inmem_store_max_batch_size: usize,
}

impl Default for StorageSpec {
    fn default() -> Self {
        Self {
            budgets: StorageBudgetConfig::default(),
            inmem_store_lock_pool_size: 4096,
            inmem_store_bucket_granularity: TimeGranularity::Seconds(1),
            inmem_store_max_batch_size: 1024,
        }
    }
}

