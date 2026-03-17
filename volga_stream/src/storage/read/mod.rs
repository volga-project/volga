pub mod base_runs;
pub mod exec;
pub mod plan;

use std::sync::Arc;

use crate::common::Key;
use crate::runtime::TaskId;
use crate::storage::index::SortedRangeView;
use crate::storage::read::exec::{LoaderContext, execute_planned_load};
use crate::storage::read::plan::{PlannedRangesLoad, RangesLoadPlan};
use crate::storage::WorkerStorageRuntime;

/// Operator-facing read API that hides pins, admission, and backend IO.
#[derive(Clone)]
pub struct StorageReader {
    ctx: LoaderContext,
}

impl StorageReader {
    pub fn new(storage: &Arc<WorkerStorageRuntime>) -> Self {
        Self {
            ctx: LoaderContext {
                backend: storage.backend.clone(),
                pins: storage.batch_pins.clone(),
                scratch: storage.scratch.clone(),
                write_buffer: storage.write_buffer.clone(),
                load_io_parallelism: storage.load_io_parallelism,
            },
        }
    }

    pub async fn execute_planned_load(
        &self,
        planned: PlannedRangesLoad,
        task_id: TaskId,
        key: &Key,
        ts_column_index: usize,
        plans: &[RangesLoadPlan],
    ) -> Vec<Vec<SortedRangeView>> {
        execute_planned_load(planned, &self.ctx, task_id, key, ts_column_index, plans).await
    }
}
