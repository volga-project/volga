use std::sync::Arc;
use std::time::Duration;

use dashmap::DashMap;
use tokio::sync::RwLock;
use tokio::task::JoinHandle;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::maintenance::MaintenanceSupervisor;

use super::{periodic_compact_in_mem, periodic_dump_to_store, relieve_in_mem_pressure};
use crate::storage::maintenance::types::BucketIndexState;

pub fn spawn_background_tasks<T: BucketIndexState + 'static>(
    supervisor: Arc<MaintenanceSupervisor>,
    window_states: Arc<DashMap<Key, Arc<RwLock<T>>>>,
    task_id: TaskId,
    ts_column_index: usize,
    hot_bucket_count: usize,
    dump_parallelism: usize,
    low_watermark_per_mille: u32,
    compaction_interval: Duration,
    dump_interval: Duration,
) -> (JoinHandle<()>, JoinHandle<()>) {
    let mgr_for_compact = supervisor.clone();
    let ws_for_compact = window_states.clone();
    let task_id_for_compact = task_id.clone();
    let compact_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(compaction_interval);
        loop {
            tick.tick().await;
            let _ = periodic_compact_in_mem(
                &mgr_for_compact,
                &ws_for_compact,
                task_id_for_compact.clone(),
                ts_column_index,
                hot_bucket_count,
            )
            .await;

            if mgr_for_compact
                .write_buffer
                .is_over_limit(&task_id_for_compact)
            {
                let _ = relieve_in_mem_pressure(
                    &mgr_for_compact,
                    &ws_for_compact,
                    task_id_for_compact.clone(),
                    ts_column_index,
                    hot_bucket_count,
                    low_watermark_per_mille,
                    dump_parallelism,
                )
                .await;
            }
        }
    });

    let mgr_for_dump = supervisor.clone();
    let ws_for_dump = window_states.clone();
    let task_id_for_dump = task_id.clone();
    let dump_handle = tokio::spawn(async move {
        let mut tick = tokio::time::interval(dump_interval);
        loop {
            tick.tick().await;
            let _ = periodic_dump_to_store(
                &mgr_for_dump,
                &ws_for_dump,
                task_id_for_dump.clone(),
                ts_column_index,
                hot_bucket_count,
                dump_parallelism,
            )
            .await;
        }
    });

    (compact_handle, dump_handle)
}
