mod compact;
mod dump;
mod orchestrator;
mod pressure;
mod rehydrate;
mod supervisor;
mod types;

pub use compact::{compact_bucket, periodic_compact_in_mem};
pub use dump::{
    checkpoint_flush_to_store, dump_bucket, dump_plans_concurrent, periodic_dump_to_store,
};
pub use orchestrator::spawn_background_tasks;
pub use pressure::relieve_in_mem_pressure;
pub use rehydrate::rehydrate_bucket;
pub use supervisor::{MaintenanceSupervisor, TaskHandles};
pub use types::{BucketIndexState, DumpMode, DumpPlan};

use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::RwLock;

use crate::common::Key;
use crate::runtime::TaskId;

pub fn start_task_background_tasks<T: BucketIndexState + 'static>(
    supervisor: Arc<MaintenanceSupervisor>,
    window_states: Arc<DashMap<Key, Arc<RwLock<T>>>>,
    task_id: TaskId,
    ts_column_index: usize,
    hot_bucket_count: usize,
    dump_parallelism: usize,
    low_watermark_per_mille: u32,
    compaction_interval: Duration,
    dump_interval: Duration,
    delete_budget_per_tick: usize,
) {
    let deleter = supervisor.retired.clone().spawn_deleter(
        supervisor.backend.clone(),
        supervisor.batch_pins.clone(),
        task_id.clone(),
        dump_interval,
        delete_budget_per_tick,
    );
    let (compaction, dump) = spawn_background_tasks(
        supervisor.clone(),
        window_states,
        task_id.clone(),
        ts_column_index,
        hot_bucket_count,
        dump_parallelism,
        low_watermark_per_mille,
        compaction_interval,
        dump_interval,
    );
    let handles = TaskHandles {
        deleter: Some(deleter),
        compaction: Some(compaction),
        dump: Some(dump),
    };
    if let Some(handles) = supervisor.register_task_handles(task_id.clone(), handles) {
        if let Some(h) = handles.deleter {
            h.abort();
        }
        if let Some(h) = handles.compaction {
            h.abort();
        }
        if let Some(h) = handles.dump {
            h.abort();
        }
    }
}

pub async fn stop_task_background_tasks(supervisor: &MaintenanceSupervisor, task_id: &TaskId) {
    supervisor.stop_task_handles(task_id).await;
}
