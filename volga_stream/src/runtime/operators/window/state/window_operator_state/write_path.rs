use super::*;

impl WindowOperatorState {
    pub async fn insert_batch(
        &self,
        key: &Key,
        watermark_ts: Option<Timestamp>,
        batch: RecordBatch,
    ) -> usize {
        self.storage
            .writer()
            .record_key_access(self.task_id.clone(), key);
        if batch.num_rows() == 0 {
            return 0;
        }
        // Get or create windows_state
        let windows_state = create_empty_windows_state(
            &self.window_ids,
            &self.tiling_configs,
            self.bucket_granularity,
        );
        let arc_rwlock = self
            .state_handle
            .cache()
            .get_or_insert(key, || windows_state);

        let (record_batch, dropped_rows) = {
            // Acquire write lock for mutable access
            let mut windows_state = arc_rwlock.write().await;

            // Assign per-row seq_no (tie-breaker for same timestamps).
            let start_seq = windows_state.next_seq_no;
            let record_batch_with_seq = append_seq_no_column(&batch, start_seq);
            windows_state.next_seq_no = windows_state
                .next_seq_no
                .saturating_add(record_batch_with_seq.num_rows() as u64);

            // Drop rows according to watermark-based policy:
            // - If watermark is known, drop anything with ts <= watermark (already finalized).
            //   (We currently don't support late-event recomputation.)
            // - Otherwise: keep all rows (can't classify lateness yet).
            let (record_batch, dropped_rows) = if let Some(wm) = watermark_ts {
                drop_too_late_entries(&record_batch_with_seq, self.ts_column_index, 0, wm)
            } else {
                (record_batch_with_seq.clone(), 0)
            };

            if record_batch.num_rows() == 0 {
                return dropped_rows;
            }

            (record_batch, dropped_rows)
        };

        let appended = self
            .storage
            .writer()
            .append(
                self.task_id.clone(),
                key,
                self.ts_column_index,
                record_batch,
            )
            .await;

        let over_limit = {
            let mut windows_state = arc_rwlock.write().await;

            // Calculate pre-aggregated tiles if needed (use already-sorted batches).
            for (window_id, window_state) in windows_state.window_states.iter_mut() {
                if let Some(ref mut tiles) = window_state.tiles {
                    let window_expr = &self
                        .window_configs
                        .get(&window_id)
                        .expect("Window config should exist")
                        .window_expr;
                    for entry in appended.iter() {
                        tiles.add_sorted_batch(&entry.batch, window_expr, self.ts_column_index);
                    }
                }
            }

            for entry in appended {
                windows_state.bucket_index.insert_batch_ref(
                    entry.bucket_ts,
                    BatchRef::InMem(entry.in_mem_id),
                    entry.min_pos,
                    entry.max_pos,
                    entry.row_count,
                    entry.bytes_estimate,
                );
            }

            self.storage.writer().is_over_limit(&self.task_id)
        };

        if over_limit {
            let _ = crate::storage::maintenance::relieve_in_mem_pressure(
                &self.storage.maintenance,
                self.state_handle.cache().values().as_ref(),
                self.task_id.clone(),
                self.ts_column_index,
                self.dump_hot_bucket_count,
                self.in_mem_low_watermark_per_mille,
                self.in_mem_dump_parallelism,
            )
            .await;
        }

        self.state_handle.cache().mark_dirty(key);

        // TODO(metrics): report dropped_rows and per-key drops.
        dropped_rows
    }
}
