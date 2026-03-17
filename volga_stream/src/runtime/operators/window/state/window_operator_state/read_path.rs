use super::*;

impl WindowOperatorState {
    pub async fn load_sorted_ranges_views(
        &self,
        key: &Key,
        plans: &[RangesLoadPlan],
    ) -> Vec<Vec<SortedRangeView>> {
        if plans.is_empty() {
            return Vec::new();
        }
        if plans.iter().all(|p| p.requests.is_empty()) {
            return vec![Vec::new(); plans.len()];
        }

        let Some(windows_state_guard) = self.get_windows_state(key).await else {
            return vec![Vec::new(); plans.len()];
        };
        let planned = plan_load_from_index(&windows_state_guard.value().bucket_index, plans);
        drop(windows_state_guard);
        self.storage
            .reader()
            .execute_planned_load(planned, self.task_id(), key, self.ts_column_index(), plans)
            .await
    }
}
