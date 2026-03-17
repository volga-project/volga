use super::*;

impl WindowOperatorState {
    pub async fn prune_if_needed(&self, key: &Key) {
        let Some(lateness) = self.lateness else {
            return;
        };
        use crate::storage::index::get_window_length_ms;
        use crate::storage::index::get_window_size_rows;
        use datafusion::logical_expr::WindowFrameUnits;

        let Some(arc_rwlock) = self.state_handle.cache().get(key) else {
            return;
        };
        let mut windows_state = arc_rwlock.write().await;
        let mut min_cutoff_timestamp = i64::MAX;

        // For each window state, calculate its specific cutoff and prune tiles
        let window_ids: Vec<WindowId> = windows_state.window_states.keys().cloned().collect();
        for window_id in window_ids {
            let window_config = self
                .window_configs
                .get(&window_id)
                .expect("Window config should exist");
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state
                .window_states
                .get(&window_id)
                .expect("Window state should exist");

            let window_end_ts = window_state
                .processed_pos
                .map(|p| p.ts)
                .unwrap_or(i64::MIN);
            let window_cutoff = match window_frame.units {
                WindowFrameUnits::Rows => {
                    // ROWS windows are count-based (last N rows), so pruning must also be count-based.
                    // We keep enough buckets to cover the last `window_size` rows ending at roughly
                    // `processed_pos.ts - lateness`.
                    let window_size = get_window_size_rows(window_frame);
                    if window_size == 0 || window_end_ts == i64::MIN {
                        0
                    } else if windows_state.bucket_index.is_empty() {
                        0
                    } else {
                        let search_ts = window_end_ts.saturating_sub(lateness);
                        let end_bucket_ts = windows_state
                            .bucket_index
                            .bucket_granularity()
                            .start(search_ts);
                        let all_ts = windows_state.bucket_index.bucket_timestamps();
                        let first_ts = all_ts.first().copied().unwrap_or(0);
                        let last_ts = all_ts.last().copied().unwrap_or(first_ts);
                        let within = crate::storage::index::BucketRange::new(
                            first_ts,
                            end_bucket_ts.min(last_ts),
                        );
                        windows_state
                            .bucket_index
                            .plan_rows_tail(search_ts, window_size, within)
                            .unwrap_or(within)
                            .start
                    }
                }
                _ => {
                    // RANGE windows: cutoff = processed_pos.ts - window_length - lateness
                    let window_length = get_window_length_ms(window_frame);
                    window_end_ts - window_length - lateness
                }
            };

            min_cutoff_timestamp = min_cutoff_timestamp.min(window_cutoff);

            if window_cutoff > 0 {
                let window_state = windows_state
                    .window_states
                    .get_mut(&window_id)
                    .expect("Window state should exist");
                if let Some(ref mut tiles) = window_state.tiles {
                    tiles.prune(window_cutoff);
                }
            }
        }

        // Use minimal cutoff to prune batch_index and storage
        if min_cutoff_timestamp != i64::MAX && min_cutoff_timestamp > 0 {
            let pruned = windows_state.bucket_index.prune(min_cutoff_timestamp);

            if !pruned.stored.is_empty() {
                self.storage
                    .writer()
                    .retire_stored(self.task_id.clone(), key, &pruned.stored);
            }
            if !pruned.inmem.is_empty() {
                self.storage.writer().drop_inmem(&pruned.inmem);
            }
            self.storage.writer().backlog_release(pruned.bytes_estimate);
        }

        self.state_handle.cache().mark_dirty(key);
    }
}
