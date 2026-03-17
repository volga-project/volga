use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::storage::batch_pins::BatchPins;
use crate::storage::batch_retirement::BatchRetirementQueue;
use crate::storage::batch_store::BatchStore;
use crate::storage::budget::StorageBudgetConfig;
use crate::storage::compactor::Compactor;
use crate::storage::in_mem_batch_cache::InMemBatchCache;
use crate::storage::stats::StorageStats;
use crate::storage::work_budget::WorkBudget;

/// Worker-level shared storage utilities and budgets.
///
/// Today this is owned by the window operator state; the intent is to move it into a worker-level
/// context and share across all operator tasks on the same worker.
#[derive(Debug)]
pub struct WorkerStorageContext {
    pub batch_store: Arc<dyn BatchStore>,
    pub batch_pins: Arc<BatchPins>,
    pub batch_retirement: Arc<BatchRetirementQueue>,
    pub in_mem: Arc<InMemBatchCache>,
    pub compactor: Compactor,
    pub stats: Arc<StorageStats>,

    /// Limits concurrent key processing (work memory admission control).
    pub inflight_keys: Arc<Semaphore>,
    /// Limits concurrent store IO fanout inside one read/hydration.
    pub load_io_parallelism: usize,
    /// Work-memory byte budget (hydration / read working sets).
    pub work_budget: Arc<WorkBudget>,

    pub budgets: StorageBudgetConfig,
}

impl WorkerStorageContext {
    pub fn new(batch_store: Arc<dyn BatchStore>, budgets: StorageBudgetConfig) -> anyhow::Result<Arc<Self>> {
        budgets.validate()?;

        let stats = StorageStats::global();
        let in_mem = Arc::new(InMemBatchCache::new_with_stats(stats.clone()));
        in_mem.set_limit_bytes(budgets.in_mem_limit_bytes);

        let batch_pins = Arc::new(BatchPins::new());
        let batch_retirement = Arc::new(BatchRetirementQueue::new());
        let compactor = Compactor::new(
            batch_store.clone(),
            batch_pins.clone(),
            batch_retirement.clone(),
            in_mem.clone(),
        );

        Ok(Arc::new(Self {
            batch_store,
            batch_pins,
            batch_retirement,
            in_mem,
            compactor,
            stats,
            inflight_keys: Arc::new(Semaphore::new(budgets.max_inflight_keys)),
            load_io_parallelism: budgets.load_io_parallelism,
            work_budget: WorkBudget::new(budgets.work_limit_bytes),
            budgets,
        }))
    }
}




