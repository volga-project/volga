use std::collections::HashMap;
use arrow::array::RecordBatch;
use crossbeam_skiplist::SkipMap;

use crate::common::Key;
use crate::storage::storage::{BatchId, RowIdx, Timestamp, extract_timestamp};

pub type TimeIdx = (Timestamp, BatchId, RowIdx); // timestamp, row index within same timestamp

#[derive(Debug)]
pub struct TimeIndex {
    time_index: HashMap<Key, SkipMap<Timestamp, Vec<TimeIdx>>>,
}

impl TimeIndex {
    pub fn new() -> Self {
        Self {
            time_index: HashMap::new(),
        }
    }

    pub fn update_time_index(
        &mut self,
        partition_key: &Key,
        batch_id: BatchId,
        batch: &RecordBatch,
        ts_column_index: usize,
    ) -> Vec<TimeIdx> {
        let mut timestamps = Vec::new();
        for row_idx in 0..batch.num_rows() {
            let timestamp = extract_timestamp(batch.column(ts_column_index), row_idx);
            timestamps.push((timestamp, batch_id, row_idx));
        }

        // Get or create time entries for this partition
        if !self.time_index.contains_key(partition_key) {
            self.time_index.insert(partition_key.clone(), SkipMap::new());
        }

        let time_entries = self.time_index.get(partition_key).expect("Time entries should exist");

        // Insert all entries
        for (timestamp, batch_id, row_idx) in &timestamps {
            let idxs = time_entries.get(timestamp);
            if idxs.is_none() {
                time_entries.insert(*timestamp, vec![(*timestamp, *batch_id, *row_idx)]);
            } else {
                let mut new_idxs = idxs.unwrap().value().clone();
                new_idxs.push((*timestamp, *batch_id, *row_idx));
                time_entries.insert(*timestamp, new_idxs);
            }
        }

        timestamps
    }

    pub fn get_time_index(&self, partition_key: &Key) -> Option<&SkipMap<Timestamp, Vec<TimeIdx>>> {
        self.time_index.get(partition_key)
    }
}