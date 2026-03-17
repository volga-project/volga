use super::*;
use crate::storage::maintenance;

impl WindowOperatorState {
    pub fn start_background_tasks(&self, compaction_interval: Duration, dump_interval: Duration) {
        maintenance::start_task_background_tasks(
            self.storage.maintenance.clone(),
            self.state_handle.values().clone(),
            self.task_id.clone(),
            self.ts_column_index,
            self.dump_hot_bucket_count,
            self.in_mem_dump_parallelism,
            self.in_mem_low_watermark_per_mille,
            compaction_interval,
            dump_interval,
            1024,
        );
    }

    pub async fn stop_background_tasks(&self) {
        maintenance::stop_task_background_tasks(&self.storage.maintenance, &self.task_id).await;
    }
}
