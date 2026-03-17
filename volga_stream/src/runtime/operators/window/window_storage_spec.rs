use crate::api::StorageSpec;
use crate::storage::{StorageBackendConfig, TimeGranularity};

pub fn default_window_storage_spec() -> StorageSpec {
    StorageSpec {
        budgets: StorageBackendConfig::default(),
        inmem_store_lock_pool_size: 4096,
        inmem_store_bucket_granularity: TimeGranularity::Seconds(1),
        inmem_store_max_batch_size: 1024,
    }
}

