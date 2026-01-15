use crate::api::StorageSpec;
use crate::runtime::operators::window::TimeGranularity;
use crate::storage::StorageBudgetConfig;

pub fn default_window_storage_spec() -> StorageSpec {
    StorageSpec {
        budgets: StorageBudgetConfig::default(),
        inmem_store_lock_pool_size: 4096,
        inmem_store_bucket_granularity: TimeGranularity::Seconds(1),
        inmem_store_max_batch_size: 1024,
    }
}

