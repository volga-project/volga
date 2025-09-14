use std::sync::Arc;
use std::cmp::Ordering;
use arrow::array::RecordBatch;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion::scalar::ScalarValue;
use uuid::Uuid;

use crate::common::Key;
use crate::runtime::operators::window::state::state::WindowState;
use crate::storage::storage::{BatchId, RowIdx, Timestamp, extract_timestamp};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TimeIdx {
    pub timestamp: Timestamp,
    pub pos_idx: usize, // position within same timestamp
    pub batch_id: BatchId,
    pub row_idx: RowIdx,
}

impl PartialOrd for TimeIdx {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for TimeIdx {
    fn cmp(&self, other: &Self) -> Ordering {
        // First compare by timestamp
        match self.timestamp.cmp(&other.timestamp) {
            Ordering::Equal => {
                // If timestamps are equal, compare by position index
                self.pos_idx.cmp(&other.pos_idx)
            }
            other => other,
        }
    }
}

#[derive(Debug)]
pub struct TimeIndex {
    time_index: DashMap<Key, Arc<SkipSet<TimeIdx>>>,
}

impl TimeIndex {
    pub fn new() -> Self {
        Self {
            time_index: DashMap::new(),
        }
    }

    pub fn update_time_index(
        &self,
        partition_key: &Key,
        batch_id: BatchId,
        batch: &RecordBatch,
        ts_column_index: usize,
    ) -> Vec<TimeIdx> {
        let mut time_idxs = Vec::new();
        
        // Get or create time entries for this partition
        if !self.time_index.contains_key(partition_key) {
            self.time_index.insert(partition_key.clone(), Arc::new(SkipSet::new()));
        }
        
        let time_entries = self.get_time_index(partition_key).expect("Time entries should exist");
        
        for row_idx in 0..batch.num_rows() {
            let timestamp = extract_timestamp(batch.column(ts_column_index), row_idx);
            
            // Search for lower bound with (timestamp, usize::MAX)
            let search_key = TimeIdx {
                timestamp,
                pos_idx: usize::MAX,
                batch_id: Uuid::nil(), // dummy values for search
                row_idx: 0,
            };
            
            // Find the position index to use
            let pos_idx = if let Some(existing_entry) = time_entries.lower_bound(std::ops::Bound::Included(&search_key)) {
                if existing_entry.timestamp == timestamp {
                    // Same timestamp exists, increment position
                    existing_entry.pos_idx + 1
                } else {
                    // Different (smaller) timestamp, start at 0
                    0
                }
            } else {
                // No existing entries or all are smaller, start at 0
                0
            };
            
            let time_idx = TimeIdx {
                timestamp,
                pos_idx,
                batch_id,
                row_idx,
            };
            
            time_entries.insert(time_idx);
            time_idxs.push(time_idx);
        }
        
        time_idxs
    }

    pub fn get_time_index(&self, partition_key: &Key) -> Option<Arc<SkipSet<TimeIdx>>> {
        self.time_index.get(partition_key).map(|entry| entry.value().clone())
    }
}

fn find_retracts(
    window_frame: &Arc<WindowFrame>,
    new_window_end: TimeIdx,
    time_entries: &SkipSet<TimeIdx>,
    window_state: &WindowState,
) -> (Vec<TimeIdx>, Option<TimeIdx>) {

    if window_frame.units == WindowFrameUnits::Rows {
        // Handle ROWS
        match window_frame.start_bound {
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(delta))) => {
                // Jump backwards delta elements from new_window_end
                let new_window_start_idx = time_entries.range(..=new_window_end)
                    .rev()
                    .nth(delta as usize)
                    .map(|entry| *entry);

                if new_window_start_idx.is_none() {
                    // window does not have enough rows to retract
                    return (Vec::new(), None);
                }
                
                // Collect all TimeIdxs between previous window start and new_window_start for retraction
                let new_start_idx = new_window_start_idx.unwrap();
                let retract_idxs: Vec<TimeIdx> = time_entries
                    .range(window_state.start_idx..new_start_idx)
                    .map(|entry| *entry)
                    .collect();

                return (retract_idxs, Some(new_start_idx));
            }
            _ => panic!("Only Preceding start bound is supported for ROWS WindowFrameUnits"),
        }
    } else if window_frame.units == WindowFrameUnits::Range {
        // Handle RANGE
        let window_end_timestamp = new_window_end.timestamp;
        let window_start_timestamp = match &window_frame.start_bound {
            WindowFrameBound::Preceding(delta) => {
                if window_frame.start_bound.is_unbounded() || delta.is_null() {
                    panic!("Can not retract UNBOUNDED PRECEDING");
                }
                
                let delta_i64 = i64::try_from(delta.clone())
                    .expect("Should be able to convert delta to i64");
                    
                window_end_timestamp.saturating_sub(delta_i64)
            },
            _ => panic!("Only Preceding start bound is supported"),
        };
        
        // Get previous window start from window state
        let previous_window_start = window_state.start_idx;
        let search_key = TimeIdx {
            timestamp: window_start_timestamp,
            pos_idx: 0,
            batch_id: Uuid::nil(), // dummy values for search
            row_idx: 0,
        };

        // Include all rows with this timestamp
        let window_start_idx = time_entries.upper_bound(std::ops::Bound::Included(&search_key))
            .map(|entry| *entry);

        if window_start_idx.is_none() {
            // window does not have enough rows to retract
            return (Vec::new(), None);
        }   

        let retract_idxs: Vec<TimeIdx> = time_entries
            // Exclude window_start_idx
            .range(previous_window_start..window_start_idx.unwrap())
            .map(|entry| *entry)
            .collect();
        
        return (retract_idxs, window_start_idx)
    } else {
        panic!("Unsupported WindowFrame type: only ROW and RANGE are supported");
    }
}

pub fn advance_window_position(window_frame: &Arc<WindowFrame>, window_state: &mut WindowState, time_entries: &Arc<SkipSet<TimeIdx>>) -> Vec<(TimeIdx, Vec<TimeIdx>)> {
    let latest_entry = time_entries.back().expect("Time entries should exist");
    let latest_idx = *latest_entry;
    let previous_window_end = window_state.end_idx;

    let mut updates_and_retracts = Vec::new();

    for entry in time_entries.range(previous_window_end..=latest_idx) {
        let time_idx = *entry;
        
        let (retracts, new_start_idx) = find_retracts(window_frame, time_idx, time_entries, &window_state);
        updates_and_retracts.push((time_idx, retracts));
        if let Some(new_start_idx) = new_start_idx {
            window_state.start_idx = new_start_idx;
        }
        window_state.end_idx = time_idx;
    }
    
    updates_and_retracts
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use arrow::record_batch::RecordBatch;
    use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
    use uuid::Uuid;

    fn create_test_batch(timestamps: Vec<i64>, values: Vec<i64>) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Int64, false),
        ]));

        let timestamp_array = TimestampMillisecondArray::from(timestamps);
        let value_array = Int64Array::from(values);

        RecordBatch::try_new(schema, vec![
            Arc::new(timestamp_array),
            Arc::new(value_array),
        ]).expect("Failed to create test batch")
    }

    fn create_rows_window_frame(rows: u64) -> Arc<WindowFrame> {
        let mut frame = WindowFrame::new(Some(false));
        frame.units = WindowFrameUnits::Rows;
        frame.start_bound = WindowFrameBound::Preceding(datafusion::scalar::ScalarValue::UInt64(Some(rows)));
        frame.end_bound = WindowFrameBound::CurrentRow;
        Arc::new(frame)
    }

    fn create_range_window_frame(range_ms: u64) -> Arc<WindowFrame> {
        let mut frame = WindowFrame::new(Some(false));
        frame.units = WindowFrameUnits::Range;
        frame.start_bound = WindowFrameBound::Preceding(datafusion::scalar::ScalarValue::UInt64(Some(range_ms)));
        frame.end_bound = WindowFrameBound::CurrentRow;
        Arc::new(frame)
    }

    fn create_test_key(partition_name: &str) -> crate::common::Key {
        let schema = Arc::new(Schema::new(vec![
            Field::new("partition", DataType::Utf8, false),
        ]));
        
        let partition_array = arrow::array::StringArray::from(vec![partition_name]);
        let key_batch = RecordBatch::try_new(schema, vec![Arc::new(partition_array)])
            .expect("Failed to create key batch");
        
        crate::common::Key::new(key_batch).expect("Failed to create key")
    }

    #[test]
    fn test_time_index_insertion() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // Test 1: Insert batch in random order [3000, 1000, 2000]
        let batch1 = create_test_batch(vec![3000, 1000, 2000], vec![30, 10, 20]);
        let batch_id1 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id1, &batch1, 0);
        
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        assert_eq!(time_entries.len(), 3, "Should have 3 timestamp entries");
        
        // Verify entries are sorted by timestamp
        let entries_vec: Vec<_> = time_entries.iter().map(|entry| *entry).collect();
        assert_eq!(entries_vec[0].timestamp, 1000, "First entry should be timestamp 1000");
        assert_eq!(entries_vec[1].timestamp, 2000, "Second entry should be timestamp 2000");
        assert_eq!(entries_vec[2].timestamp, 3000, "Third entry should be timestamp 3000");
        
        // Verify front and back
        let front = time_entries.front().expect("Should have front entry");
        let back = time_entries.back().expect("Should have back entry");
        assert_eq!(front.timestamp, 1000, "Front should be earliest timestamp");
        assert_eq!(back.timestamp, 3000, "Back should be latest timestamp");
        
        // Test 2: Insert batch with same timestamps [1000, 1000, 2000, 2000]
        let batch2 = create_test_batch(vec![1000, 1000, 2000, 2000], vec![11, 12, 21, 22]);
        let batch_id2 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id2, &batch2, 0);
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        let entries_vec: Vec<_> = time_entries.iter().map(|entry| *entry).collect();
        assert_eq!(entries_vec.len(), 6, "Should have 6 total entries (3 from each batch)");
        
        // Verify multiple entries for same timestamp
        let entries_1000: Vec<_> = entries_vec.iter().filter(|e| e.timestamp == 1000).collect();
        let entries_2000: Vec<_> = entries_vec.iter().filter(|e| e.timestamp == 2000).collect();
        
        // Should have 3 entries for timestamp 1000 (1 from batch1, 2 from batch2)
        assert_eq!(entries_1000.len(), 3, "Should have 3 entries for timestamp 1000");
        // Should have 3 entries for timestamp 2000 (1 from batch1, 2 from batch2)
        assert_eq!(entries_2000.len(), 3, "Should have 3 entries for timestamp 2000");
        
        // Verify batch IDs and row indices are correct
        let mut batch1_count = 0;
        let mut batch2_count = 0;
        
        for time_idx in &entries_1000 {
            if time_idx.batch_id == batch_id1 {
                batch1_count += 1;
                assert_eq!(time_idx.row_idx, 1); // Row index from first batch
            } else if time_idx.batch_id == batch_id2 {
                batch2_count += 1;
                assert!(time_idx.row_idx == 0 || time_idx.row_idx == 1); // Row indices 0,1 from second batch
            }
        }
        
        assert_eq!(batch1_count, 1, "Should have 1 entry from batch1 for timestamp 1000");
        assert_eq!(batch2_count, 2, "Should have 2 entries from batch2 for timestamp 1000");
    }

    #[test]
    fn test_advance_window_position_first_time() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // Create batch with timestamps [1000, 2000, 3000, 4000, 5000]
        let batch = create_test_batch(
            vec![1000, 2000, 3000, 4000, 5000], 
            vec![10, 20, 30, 40, 50]
        );
        let batch_id = Uuid::new_v4();
        
        time_index.update_time_index(&key, batch_id, &batch, 0);
        
        // Create initial window state (empty - first time)
        let mut window_state = WindowState {
            accumulator_state: None,
            start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
        };
        
        // Create window frame: ROWS 2 PRECEDING
        let window_frame = create_rows_window_frame(2);
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        
        // Advance window position
        let updates_and_retracts = advance_window_position(&window_frame, &mut window_state, &time_entries);
        
        // Should process all 5 events since it's the first time
        assert_eq!(updates_and_retracts.len(), 5, "Should have 5 updates");
        
        // Verify first update (timestamp 1000)
        let (update_idx, retracts) = &updates_and_retracts[0];
        assert_eq!(update_idx.timestamp, 1000);
        assert_eq!(update_idx.batch_id, batch_id);
        assert_eq!(update_idx.row_idx, 0);
        assert!(retracts.is_empty(), "First event should have no retracts");
        
        // Verify third update (timestamp 3000) - should have retracts
        let (update_idx, retracts) = &updates_and_retracts[2];
        assert_eq!(update_idx.timestamp, 3000);
        assert_eq!(update_idx.row_idx, 2);
        assert_eq!(retracts.len(), 1, "Third event should retract first event (window size = 3)");
        assert_eq!(retracts[0].timestamp, 1000); // Should retract timestamp 1000
        
        // Verify final window state
        assert_eq!(window_state.end_idx.timestamp, 5000, "Window end should be at timestamp 5000");
        assert_eq!(window_state.start_idx.timestamp, 3000, "Window start should be at timestamp 3000");
    }

    #[test]
    fn test_advance_window_position_incremental() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // First batch: timestamps [1000, 2000, 3000]
        let batch1 = create_test_batch(vec![1000, 2000, 3000], vec![10, 20, 30]);
        let batch_id1 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id1, &batch1, 0);
        
        // Initial window state after first processing
        let mut window_state = WindowState {
            accumulator_state: None,
            start_idx: TimeIdx { timestamp: 1000, pos_idx: 0, batch_id: batch_id1, row_idx: 0 },
            end_idx: TimeIdx { timestamp: 3000, pos_idx: 0, batch_id: batch_id1, row_idx: 2 },
        };
        
        // Add second batch: timestamps [4000, 5000]
        let batch2 = create_test_batch(vec![4000, 5000], vec![40, 50]);
        let batch_id2 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id2, &batch2, 0);
        
        // Create window frame: ROWS 2 PRECEDING (window size = 3)
        let window_frame = create_rows_window_frame(2);
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        
        // Advance window position incrementally
        let updates_and_retracts = advance_window_position(&window_frame, &mut window_state, &time_entries);
        
        // Should only process new events (4000, 5000)
        assert_eq!(updates_and_retracts.len(), 2, "Should have 2 incremental updates");
        
        // Verify first incremental update (timestamp 4000)
        let (update_idx, retracts) = &updates_and_retracts[0];
        assert_eq!(update_idx.timestamp, 4000);
        assert_eq!(update_idx.batch_id, batch_id2);
        assert_eq!(update_idx.row_idx, 0);
        assert_eq!(retracts.len(), 1, "Should retract one old event");
        assert_eq!(retracts[0].timestamp, 1000); // Should retract timestamp 1000
        assert_eq!(retracts[0].batch_id, batch_id1); // From first batch
        
        // Verify second incremental update (timestamp 5000)
        let (update_idx, retracts) = &updates_and_retracts[1];
        assert_eq!(update_idx.timestamp, 5000);
        assert_eq!(update_idx.batch_id, batch_id2);
        assert_eq!(update_idx.row_idx, 1);
        assert_eq!(retracts.len(), 1, "Should retract one old event");
        assert_eq!(retracts[0].timestamp, 2000); // Should retract timestamp 2000
        assert_eq!(retracts[0].batch_id, batch_id1); // From first batch
        
        // Verify final window state
        assert_eq!(window_state.end_idx.timestamp, 5000, "Window end should be at timestamp 5000");
        assert_eq!(window_state.start_idx.timestamp, 3000, "Window start should be at timestamp 3000");
    }

    #[test]
    fn test_advance_window_position_range_based() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // Create batch with timestamps [1000, 1500, 2000, 2500, 3000]
        let batch = create_test_batch(
            vec![1000, 1500, 2000, 2500, 3000], 
            vec![10, 15, 20, 25, 30]
        );
        let batch_id = Uuid::new_v4();
        
        time_index.update_time_index(&key, batch_id, &batch, 0);
        
        // Create initial window state (empty - first time)
        let mut window_state = WindowState {
            accumulator_state: None,
            start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
        };
        
        // Create range window frame: RANGE 1000ms PRECEDING (1 second window)
        let window_frame = create_range_window_frame(1000);
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        
        // Advance window position
        let updates_and_retracts = advance_window_position(&window_frame, &mut window_state, &time_entries);
        
        // Should process all 5 events since it's the first time
        assert_eq!(updates_and_retracts.len(), 5, "Should have 5 updates");
        
        // Verify first update (timestamp 1000)
        let (update_idx, retracts) = &updates_and_retracts[0];
        assert_eq!(update_idx.timestamp, 1000);
        assert_eq!(update_idx.batch_id, batch_id);
        assert_eq!(update_idx.row_idx, 0);
        assert!(retracts.is_empty(), "First event should have no retracts");
        
        // Verify third update (timestamp 2000) - should have retracts
        let (update_idx, retracts) = &updates_and_retracts[2];
        assert_eq!(update_idx.timestamp, 2000);
        assert_eq!(update_idx.row_idx, 2);
        assert_eq!(retracts.len(), 1, "Event at 2000 should retract event at 1000 (outside 1000ms range)");
        assert_eq!(retracts[0].timestamp, 1000); // Should retract timestamp 1000
        
        // Verify final window state
        assert_eq!(window_state.end_idx.timestamp, 3000, "Window end should be at timestamp 3000");
        assert_eq!(window_state.start_idx.timestamp, 2000, "Window start should be at timestamp 2000 (3000-1000)");
    }

    #[test]
    fn test_advance_window_position_range_incremental() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // First batch: timestamps [1000, 2000, 3000]
        let batch1 = create_test_batch(vec![1000, 2000, 3000], vec![10, 20, 30]);
        let batch_id1 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id1, &batch1, 0);
        
        // Initial window state after first processing (range window: 1500ms)
        let mut window_state = WindowState {
            accumulator_state: None,
            start_idx: TimeIdx { timestamp: 1500, pos_idx: 0, batch_id: batch_id1, row_idx: 0 },
            end_idx: TimeIdx { timestamp: 3000, pos_idx: 0, batch_id: batch_id1, row_idx: 2 },
        };
        
        // Add second batch: timestamps [4000, 5000]
        let batch2 = create_test_batch(vec![4000, 5000], vec![40, 50]);
        let batch_id2 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id2, &batch2, 0);
        
        // Create range window frame: RANGE 1500ms PRECEDING
        let window_frame = create_range_window_frame(1500);
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        
        // Advance window position incrementally
        let updates_and_retracts = advance_window_position(&window_frame, &mut window_state, &time_entries);
        
        // Should only process new events (4000, 5000)
        assert_eq!(updates_and_retracts.len(), 2, "Should have 2 incremental updates");
        
        // Verify first incremental update (timestamp 4000)
        let (update_idx, retracts) = &updates_and_retracts[0];
        assert_eq!(update_idx.timestamp, 4000);
        assert_eq!(update_idx.batch_id, batch_id2);
        assert_eq!(update_idx.row_idx, 0);
        // Range window: 4000-1500=2500, so should retract events before 2500
        assert!(retracts.len() >= 1, "Should retract old events outside range");
        
        // Verify second incremental update (timestamp 5000)
        let (update_idx, _retracts) = &updates_and_retracts[1];
        assert_eq!(update_idx.timestamp, 5000);
        assert_eq!(update_idx.batch_id, batch_id2);
        assert_eq!(update_idx.row_idx, 1);
        // Range window: 5000-1500=3500, so should include events from 3500 onwards
        
        // Verify final window state
        assert_eq!(window_state.end_idx.timestamp, 5000, "Window end should be at timestamp 5000");
        assert_eq!(window_state.start_idx.timestamp, 3500, "Window start should be at timestamp 3500 (5000-1500)");
    }

    #[test]
    fn test_rows_vs_range_window_comparison() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // Create batch with uneven time intervals [1000, 1100, 2000, 2100, 3000]
        let batch = create_test_batch(
            vec![1000, 1100, 2000, 2100, 3000], 
            vec![10, 11, 20, 21, 30]
        );
        let batch_id = Uuid::new_v4();
        
        time_index.update_time_index(&key, batch_id, &batch, 0);
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        
        // Test 1: ROWS 2 PRECEDING (fixed number of rows)
        {
            let mut window_state_rows = WindowState {
                accumulator_state: None,
                start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
                end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            };
            
            let rows_frame = create_rows_window_frame(2);
            let updates_and_retracts_rows = advance_window_position(&rows_frame, &mut window_state_rows, &time_entries);
            
            // ROWS window: always includes exactly 3 rows (current + 2 preceding)
            // At timestamp 3000, should include: 2000, 2100, 3000 (3 rows)
            assert_eq!(window_state_rows.end_idx.timestamp, 3000);
            
            // Check third update (timestamp 2000) - should retract first two events
            let (_, retracts) = &updates_and_retracts_rows[2];
            assert_eq!(retracts.len(), 1, "ROWS window should retract exactly 1 old row");
            assert_eq!(retracts[0].timestamp, 1000); // Should retract timestamp 1000
        }
        
        // Test 2: RANGE 1000ms PRECEDING (time-based window)
        {
            let mut window_state_range = WindowState {
                accumulator_state: None,
                start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
                end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            };
            
            let range_frame = create_range_window_frame(1000);
            let updates_and_retracts_range = advance_window_position(&range_frame, &mut window_state_range, &time_entries);
            
            // RANGE window: includes all events within 1000ms time range
            // At timestamp 3000, should include: 2000, 2100, 3000 (within 1000ms)
            assert_eq!(window_state_range.end_idx.timestamp, 3000);
            
            // Check the retractions for range window
            let (_, retracts) = &updates_and_retracts_range[2]; // timestamp 2000
            assert_eq!(retracts.len(), 2, "RANGE window should retract events outside 1000ms range");
            // Should retract both 1000 and 1100 (outside 2000-1000=1000ms range)
        }
    }

    #[test]
    fn test_multiple_rows_same_timestamp_with_windows() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // Create batch with duplicate timestamps [1000, 1000, 2000, 2000, 3000]
        let batch = create_test_batch(
            vec![1000, 1000, 2000, 2000, 3000], 
            vec![10, 11, 20, 21, 30]
        );
        let batch_id = Uuid::new_v4();
        
        time_index.update_time_index(&key, batch_id, &batch, 0);
        let time_entries = time_index.get_time_index(&key).expect("Time entries should exist");
        
        // Test with ROWS 3 PRECEDING window
        let mut window_state = WindowState {
            accumulator_state: None,
            start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
        };
        
        let window_frame = create_rows_window_frame(3);
        let updates_and_retracts = advance_window_position(&window_frame, &mut window_state, &time_entries);
        
        // Should process all 5 events (including duplicates)
        assert_eq!(updates_and_retracts.len(), 5, "Should process all events including duplicates");
        
        // Verify that duplicate timestamps are handled correctly
        let mut timestamp_1000_count = 0;
        let mut timestamp_2000_count = 0;
        
        for (update_idx, _) in &updates_and_retracts {
            match update_idx.timestamp {
                1000 => timestamp_1000_count += 1,
                2000 => timestamp_2000_count += 1,
                _ => {}
            }
        }
        
        assert_eq!(timestamp_1000_count, 2, "Should process both events at timestamp 1000");
        assert_eq!(timestamp_2000_count, 2, "Should process both events at timestamp 2000");
        
        // Verify final window state
        assert_eq!(window_state.end_idx.timestamp, 3000, "Window end should be at timestamp 3000");
        // With ROWS 3 PRECEDING, window should include last 4 rows (current + 3 preceding)
        // Starting from the row before the last 4 rows
    }
}