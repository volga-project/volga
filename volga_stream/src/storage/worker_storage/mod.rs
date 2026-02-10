use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use tokio::sync::Semaphore;

use crate::storage::backend::{DynStorageBackend, StorageCheckpointToken};
use crate::storage::delete::batch_pins::BatchPins;
use crate::storage::delete::batch_retirement::BatchRetirementQueue;
use crate::storage::budget::StorageBackendConfig;
use crate::storage::read::base_runs::BaseRuns;
use crate::storage::write::write_buffer::WriteBuffer;
use crate::storage::flow_control::BacklogBudget;
use crate::storage::maintenance::MaintenanceSupervisor;
use crate::storage::memory_pool::{ScratchLimiter, WorkerMemoryPool};
use crate::storage::stats::StorageStats;
use crate::runtime::TaskId;
use crate::storage::write::state_serde::StateSerde;


/// Worker storage runtime (operator-facing).
///
/// This is the composition root for RAM governance (tenants), caches, admission control,
/// and persistence orchestration.
#[derive(Debug)]
pub struct WorkerStorageRuntime {
    pub backend: DynStorageBackend,

    pub stats: Arc<StorageStats>,
    pub mem_pool: Arc<WorkerMemoryPool>,
    pub scratch: ScratchLimiter,
    pub write_buffer: Arc<WriteBuffer>,
    pub base_runs: Arc<BaseRuns>,
    pub batch_pins: Arc<BatchPins>,
    pub batch_retirement: Arc<BatchRetirementQueue>,
    pub maintenance: Arc<MaintenanceSupervisor>,
    pub backlog: Arc<BacklogBudget>,

    pub inflight_keys: Arc<Semaphore>,
    pub load_io_parallelism: usize,

    pub config: StorageBackendConfig,
}

impl WorkerStorageRuntime {
    pub fn reader(self: &Arc<Self>) -> crate::storage::read::StorageReader {
        crate::storage::read::StorageReader::new(self)
    }

    pub fn writer(self: &Arc<Self>) -> crate::storage::write::StorageWriter {
        crate::storage::write::StorageWriter::new(self)
    }

    pub fn state_handle<V>(
        self: &Arc<Self>,
        task_id: TaskId,
        namespace: &str,
        serde: StateSerde<V>,
    ) -> crate::storage::write::StateHandle<V> {
        crate::storage::write::StateHandle::new(self, task_id, namespace, serde)
    }

    fn from_parts(
        backend: DynStorageBackend,
        config: StorageBackendConfig,
        stats: Arc<StorageStats>,
        mem_pool: Arc<WorkerMemoryPool>,
        scratch: ScratchLimiter,
    ) -> Arc<Self> {
        let write_buffer = Arc::new(WriteBuffer::new_with_stats(stats.clone(), mem_pool.clone()));
        let base_runs = Arc::new(BaseRuns::new_with_stats(stats.clone(), mem_pool.clone()));
        let batch_pins = Arc::new(BatchPins::new());
        let batch_retirement = Arc::new(BatchRetirementQueue::new());

        let maintenance = Arc::new(MaintenanceSupervisor::new_with_stats(
            backend.clone(),
            batch_pins.clone(),
            batch_retirement.clone(),
            write_buffer.clone(),
            stats.clone(),
        ));
        let backlog = BacklogBudget::new(config.backlog_limit_bytes);

        Arc::new(Self {
            backend,
            stats,
            mem_pool: mem_pool.clone(),
            scratch: scratch.clone(),
            write_buffer,
            base_runs,
            batch_pins,
            batch_retirement,
            maintenance,
            backlog,
            inflight_keys: Arc::new(Semaphore::new(config.concurrency.max_inflight_keys)),
            load_io_parallelism: config.concurrency.load_io_parallelism,
            config,
        })
    }

    pub fn new_with_backend(
        config: StorageBackendConfig,
        build_backend: impl FnOnce() -> DynStorageBackend,
    ) -> anyhow::Result<Arc<Self>> {
        config.validate()?;

        let stats = StorageStats::new();
        let mem_pool = WorkerMemoryPool::new(config.memory);
        let scratch = ScratchLimiter::new(config.concurrency.max_inflight_keys);
        let backend = build_backend();
        Ok(Self::from_parts(backend, config, stats, mem_pool, scratch))
    }

    pub async fn new_with_backend_async(
        config: StorageBackendConfig,
        build_backend: impl FnOnce() -> Pin<Box<dyn Future<Output = anyhow::Result<DynStorageBackend>> + Send>>,
    ) -> anyhow::Result<Arc<Self>> {
        config.validate()?;

        let stats = StorageStats::new();
        let mem_pool = WorkerMemoryPool::new(config.memory);
        let scratch = ScratchLimiter::new(config.concurrency.max_inflight_keys);
        let backend = build_backend().await?;
        Ok(Self::from_parts(backend, config, stats, mem_pool, scratch))
    }

    pub async fn checkpoint(&self, task_id: TaskId) -> anyhow::Result<StorageCheckpointToken> {
        self.backend.checkpoint(task_id).await
    }

    pub async fn apply_checkpoint(
        &self,
        task_id: TaskId,
        token: StorageCheckpointToken,
    ) -> anyhow::Result<()> {
        self.backend.apply_checkpoint(task_id, token).await
    }
}

