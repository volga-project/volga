use std::collections::BTreeSet;
use std::sync::Arc;
use std::cmp::Ordering;
use arrow::array::RecordBatch;
use crossbeam_skiplist::SkipSet;
use dashmap::DashMap;
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
use datafusion::scalar::ScalarValue;
use uuid::Uuid;

use crate::common::Key;
use crate::runtime::operators::window::state::WindowState;
use crate::runtime::operators::window::tiles::Tile;
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

    pub async fn update_time_index(
        &self,
        partition_key: &Key,
        batch_id: BatchId,
        batch: &RecordBatch,
        ts_column_index: usize,
    ) -> Vec<TimeIdx> {
        let mut time_idxs = Vec::new();
        let time_entries = self.get_or_create_time_index(partition_key).await;
        
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
            let pos_idx = if let Some(existing_entry) = time_entries.upper_bound(std::ops::Bound::Included(&search_key)) {
                if existing_entry.timestamp == timestamp {
                    // Same timestamp exists, increment position
                    existing_entry.pos_idx + 1
                } else {
                    // Different timestamp
                    0
                }
            } else {
                // No existing entries
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

    pub async fn get_or_create_time_index(&self, partition_key: &Key) -> Arc<SkipSet<TimeIdx>> {
        if !self.time_index.contains_key(partition_key) {
            self.time_index.insert(partition_key.clone(), Arc::new(SkipSet::new()));
        }
        self.time_index.get(partition_key).expect("Time entries should exist").value().clone()
    }

    pub async fn insert_time_index(&self, partition_key: &Key, time_entries: Arc<SkipSet<TimeIdx>>) {
        self.time_index.insert(partition_key.clone(), time_entries);
    }
}

pub fn get_window_entries(
    window_frame: &Arc<WindowFrame>,
    window_end_idx: TimeIdx,
    time_entries: Arc<SkipSet<TimeIdx>>,
) -> Vec<TimeIdx> {
    let window_start_idx = get_window_start_idx(window_frame, window_end_idx, &time_entries);
    if window_start_idx.is_none() {
        return time_entries.range(..=window_end_idx).map(|entry| *entry).collect();
    } else {
        return time_entries.range(window_start_idx.unwrap()..=window_end_idx).map(|entry| *entry).collect();
    }
}

pub fn get_window_start_idx(
    window_frame: &Arc<WindowFrame>,
    window_end_idx: TimeIdx,
    time_entries: &SkipSet<TimeIdx>,
) -> Option<TimeIdx> {
    if window_frame.units == WindowFrameUnits::Rows {
        // Handle ROWS
        match window_frame.start_bound {
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(delta))) => {
                // Jump backwards delta elements from new_window_end
                let window_start_idx = time_entries.range(..=window_end_idx)
                    .rev()
                    .nth(delta as usize)
                    .map(|entry| *entry);

                return window_start_idx;
            }
            _ => panic!("Only Preceding start bound is supported for ROWS WindowFrameUnits"),
        }
    } else if window_frame.units == WindowFrameUnits::Range {
        // Handle RANGE
        let window_end_timestamp = window_end_idx.timestamp;
        let window_start_timestamp = match &window_frame.start_bound {
            WindowFrameBound::Preceding(delta) => {
                if window_frame.start_bound.is_unbounded() || delta.is_null() {
                    panic!("Can not retract UNBOUNDED PRECEDING");
                }
                
                let delta_i64 = convert_interval_to_milliseconds(delta);
                    
                window_end_timestamp.saturating_sub(delta_i64)
            },
            _ => panic!("Only Preceding start bound is supported"),
        };
        
        // Get new window start from time index
        let search_key = TimeIdx {
            timestamp: window_start_timestamp,
            pos_idx: 0,
            batch_id: Uuid::nil(), // dummy values for search
            row_idx: 0,
        };
        // Include all rows with this timestamp
        let window_start_idx = time_entries.lower_bound(std::ops::Bound::Included(&search_key))
            .map(|entry| *entry);

        return window_start_idx;
    } else {
        panic!("Unsupported WindowFrame type: only ROW and RANGE are supported");
    }
}

fn find_retracts(
    window_frame: &Arc<WindowFrame>,
    new_window_end: TimeIdx,
    time_entries: &SkipSet<TimeIdx>,
    window_state: &WindowState,
) -> (Vec<TimeIdx>, Option<TimeIdx>) {

    let new_window_start_idx = get_window_start_idx(window_frame, new_window_end, time_entries);

    if window_frame.units == WindowFrameUnits::Rows {
        // Handle ROWS
        if new_window_start_idx.is_none() {
            // window does not have enough rows to retract
            return (Vec::new(), None);
        }
        
        // Collect all TimeIdxs between previous window start and new_window_start for retraction
        let new_start_idx = new_window_start_idx.expect("Window start idx should exist");
        let retract_idxs: Vec<TimeIdx> = time_entries
            .range(window_state.start_idx..new_start_idx)
            .map(|entry| *entry)
            .collect();

        return (retract_idxs, Some(new_start_idx));
    } else if window_frame.units == WindowFrameUnits::Range {
        // Handle RANGE

        let previous_window_start_idx = window_state.start_idx;
        let retract_idxs: Vec<TimeIdx> = time_entries
            // Exclude window_start_idx
            .range(previous_window_start_idx..new_window_start_idx.expect("Window start idx should exist"))
            .map(|entry| *entry)
            .collect();
        
        return (retract_idxs, new_window_start_idx)
    } else {
        panic!("Unsupported WindowFrame type: only ROW and RANGE are supported");
    }
}


pub fn slide_window_position(window_frame: &Arc<WindowFrame>, window_state: &mut WindowState, time_entries: &Arc<SkipSet<TimeIdx>>, with_retracts: bool) -> (Vec<TimeIdx>, Vec<Vec<TimeIdx>>) {
    let mut updates = Vec::new();
    let mut retracts = Vec::new();
    
    let latest_idx = *time_entries.back().expect("Time entries should exist");
    let previous_window_end = window_state.end_idx;

    if previous_window_end == latest_idx {
        // nothing changed
        return (updates, retracts);
    }

    let range_start_idx = if previous_window_end.batch_id == Uuid::nil() {
        // first time
        *time_entries.front().expect("Time entries should exist")
    } else {
        // start from closest timestamp to previous window end
        time_entries.lower_bound(std::ops::Bound::Excluded(&previous_window_end))
            .map(|entry| *entry).expect(&format!("Range start idx should exist for {:?}, {:?}", previous_window_end, time_entries.iter().collect::<Vec<_>>()))
    };

    for entry in time_entries.range(range_start_idx..=latest_idx) {
        let time_idx = *entry;
        
        let (rets, new_start_idx) = find_retracts(window_frame, time_idx, time_entries, &window_state);
        updates.push(time_idx);
        if with_retracts {
            retracts.push(rets);
        }
        if let Some(new_start_idx) = new_start_idx {
            window_state.start_idx = new_start_idx;
        }
        window_state.end_idx = time_idx;
    }
    
    (updates, retracts)
}

// returns a mix of raw events and tiles for range
pub fn get_tiled_range(
    window_state: Option<&WindowState>,
    time_entries: &Arc<SkipSet<TimeIdx>>,
    start_idx: TimeIdx,
    end_idx: TimeIdx
) -> (Vec<TimeIdx>, Vec<Tile>, Vec<TimeIdx>) {
    let tiles_in_range = if let Some(window_state) = window_state {
        if let Some(ref tiles) = window_state.tiles {
            tiles.get_tiles_for_range(start_idx.timestamp, end_idx.timestamp)
        } else {
            Vec::new()
        }
    } else {
        Vec::new()
    };
    
    // we assume that tiles range has no gaps that can be filled by raw events 
    // i.e. if a tile does not exist (gap) then events do not exist (due to write consistency)
    // hence check for raw events only between range start and first tile start and between last tile end and range end
    if tiles_in_range.is_empty() {
        // no tiles, use all entries in range
        (time_entries.range(start_idx..=end_idx).map(|entry| *entry).collect(), Vec::new(), Vec::new())
    } else {
        // get front padding
        let front_boundry = TimeIdx {
            timestamp: tiles_in_range[0].tile_start,
            pos_idx: 0,
            batch_id: Uuid::nil(), // dummy values for search
            row_idx: 0,
        };
        let front_padding: Vec<TimeIdx> = time_entries.range(start_idx..front_boundry).map(|entry| *entry).collect();
        // get back padding
        let back_boundry = TimeIdx {
            timestamp: tiles_in_range[tiles_in_range.len() - 1].tile_end,
            pos_idx: 0,
            batch_id: Uuid::nil(), // dummy values for search
            row_idx: 0,
        };
        let back_padding: Vec<TimeIdx> = time_entries.range(back_boundry..=end_idx).map(|entry| *entry).collect();
        // front_padding.extend(back_padding);
        // front_padding

        (front_padding, tiles_in_range, back_padding)
    }
}

// return batch_id's needed to produce window aggregates for given entries, 
// excluding ranges covered by tiles
pub fn get_batches_for_entries(
    entries: &Vec<TimeIdx>,
    time_entries: &Arc<SkipSet<TimeIdx>>,
    window_frame: &Arc<WindowFrame>,
    window_state: Option<&WindowState>,
) -> BTreeSet<BatchId> {
    let mut res = BTreeSet::new();
    for entry in entries {
        res.insert(entry.batch_id);
        let window_start = get_window_start_idx(window_frame, *entry, &time_entries).unwrap_or(time_entries.front().expect("Time entries should exist").value().clone());
        let tiled_range = get_tiled_range(window_state, &time_entries, window_start, *entry);
        // skip tiles, only front and back padding
        for entry in tiled_range.0 {
            res.insert(entry.batch_id);
        }
        for entry in tiled_range.2 {
            res.insert(entry.batch_id);
        }
    }
    res
}

/// Convert various interval types to milliseconds for timestamp arithmetic
fn convert_interval_to_milliseconds(delta: &ScalarValue) -> i64 {
    match delta {
        ScalarValue::IntervalMonthDayNano(Some(interval)) => {
            // Based on DataFusion's interval_mdn_to_duration_ns logic
            if interval.months == 0 && interval.days == 0 {
                // Convert nanoseconds to milliseconds
                interval.nanoseconds / 1_000_000
            } else {
                panic!("Cannot convert interval with non-zero months or days to milliseconds");
            }
        },
        ScalarValue::IntervalDayTime(Some(interval)) => {
            // Based on DataFusion's interval_dt_to_duration_ms logic  
            if interval.days == 0 {
                interval.milliseconds as i64
            } else {
                panic!("Cannot convert interval with non-zero days to milliseconds");
            }
        },
        _ => i64::try_from(delta.clone())
            .expect("Should be able to convert delta to i64")
    }
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

    fn create_range_window_frame(range_ms: i64) -> Arc<WindowFrame> {
        let mut frame = WindowFrame::new(Some(false));
        frame.units = WindowFrameUnits::Range;
        frame.start_bound = WindowFrameBound::Preceding(datafusion::scalar::ScalarValue::TimestampMillisecond(Some(range_ms), None));
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

    #[tokio::test]
    async fn test_time_index_insertion() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // Test 1: Insert batch in random order [3000, 1000, 2000]
        let batch1 = create_test_batch(vec![3000, 1000, 2000], vec![30, 10, 20]);
        let batch_id1 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id1, &batch1, 0).await;
        
        let time_entries = time_index.get_or_create_time_index(&key).await;
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
        time_index.update_time_index(&key, batch_id2, &batch2, 0).await;
        
        let time_entries = time_index.get_or_create_time_index(&key).await;
        let entries_vec: Vec<_> = time_entries.iter().map(|entry| *entry).collect();
        assert_eq!(entries_vec.len(), 7, "Should have 7 total entries");
        
        // Verify multiple entries for same timestamp
        let entries_1000: Vec<_> = entries_vec.iter().filter(|e| e.timestamp == 1000).collect();
        let entries_2000: Vec<_> = entries_vec.iter().filter(|e| e.timestamp == 2000).collect();
        
        // Should have 3 entries for timestamp 1000 (1 from batch1, 2 from batch2)
        assert_eq!(entries_1000.len(), 3, "Should have 3 entries for timestamp 1000");
        // Should have 3 entries for timestamp 2000 (1 from batch1, 2 from batch2)
        assert_eq!(entries_2000.len(), 3, "Should have 3 entries for timestamp 2000");
    }

    #[tokio::test]
    async fn test_advance_row_window_position() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // First batch: timestamps [1000, 2000, 3000]
        let batch1 = create_test_batch(vec![1000, 2000, 3000], vec![10, 20, 30]);
        let batch_id1 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id1, &batch1, 0).await;
        
        // Create window frame: ROWS 2 PRECEDING (window size = 3)
        let window_frame = create_rows_window_frame(2);
        let time_entries = time_index.get_or_create_time_index(&key).await;
        
        // Initial empty window state
        let mut window_state = WindowState {
            tiles: None,
            accumulator_state: None,
            start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
        };
        
        // First advance window position after first batch
        let (first_updates, first_retracts) = slide_window_position(&window_frame, &mut window_state, &time_entries, true);
        
        // Verify first batch processing (similar to test_advance_window_position_first_time)
        assert_eq!(first_updates.len(), 3, "Should have 3 updates for first batch");
        
        let expected_first_updates: [(i64, Uuid, usize, Vec<(i64, Uuid)>); 3] = [
            (1000, batch_id1, 0, vec![]), // 1st: no retracts (window: [1000])
            (2000, batch_id1, 1, vec![]), // 2nd: no retracts (window: [1000, 2000])
            (3000, batch_id1, 2, vec![]), // 3rd: no retracts (window: [1000, 2000, 3000] - window full)
        ];
        
        for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_first_updates.iter().enumerate() {
            let update_idx = &first_updates[i];
            let retracts = &first_retracts[i];

            // Verify update
            assert_eq!(update_idx.timestamp, *expected_ts, "First batch update {} should have timestamp {}", i + 1, expected_ts);
            assert_eq!(update_idx.batch_id, *expected_batch_id, "First batch update {} should have correct batch_id", i + 1);
            assert_eq!(update_idx.row_idx, *expected_row_idx, "First batch update {} should have row_idx {}", i + 1, expected_row_idx);
            
            // Verify retracts
            assert_eq!(retracts.len(), expected_retracts.len(), "First batch update {} should have {} retracts", i + 1, expected_retracts.len());
        }
        
        // Verify window state after first batch
        assert_eq!(window_state.end_idx.timestamp, 3000, "Window end should be at timestamp 3000 after first batch");
        assert_eq!(window_state.start_idx.timestamp, 1000, "Window start should be at timestamp 1000 after first batch");
        
        // Add second batch: timestamps [4000, 5000]
        let batch2 = create_test_batch(vec![4000, 5000], vec![40, 50]);
        let batch_id2 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id2, &batch2, 0).await;
        
        // Second advance window position (incremental processing)
        let (updates, retracts) = slide_window_position(&window_frame, &mut window_state, &time_entries, true);
        
        // Should only process new events (4000, 5000)
        assert_eq!(updates.len(), 2, "Should have 2 incremental updates");
        
        // Expected behavior for incremental updates with ROWS 2 PRECEDING (window size = 3)
        // Window state before: [1000, 2000, 3000] (from first batch)
        let expected_updates = [
            (4000, batch_id2, 0, vec![(1000, batch_id1)]), // Add 4000, retract 1000 (window: [2000, 3000, 4000])
            (5000, batch_id2, 1, vec![(2000, batch_id1)]), // Add 5000, retract 2000 (window: [3000, 4000, 5000])
        ];
        
        for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_updates.iter().enumerate() {
            let update_idx = &updates[i];
            let retracts = &retracts[i];
            
            // Verify update
            assert_eq!(update_idx.timestamp, *expected_ts, "Incremental update {} should have timestamp {}", i + 1, expected_ts);
            assert_eq!(update_idx.batch_id, *expected_batch_id, "Incremental update {} should have correct batch_id", i + 1);
            assert_eq!(update_idx.row_idx, *expected_row_idx, "Incremental update {} should have row_idx {}", i + 1, expected_row_idx);
            
            // Verify retracts
            assert_eq!(retracts.len(), expected_retracts.len(), "Incremental update {} should have {} retracts", i + 1, expected_retracts.len());
            for (j, (expected_retract_ts, expected_retract_batch_id)) in expected_retracts.iter().enumerate() {
                assert_eq!(retracts[j].timestamp, *expected_retract_ts, "Incremental update {} retract {} should have timestamp {}", i + 1, j, expected_retract_ts);
                assert_eq!(retracts[j].batch_id, *expected_retract_batch_id, "Incremental update {} retract {} should have correct batch_id", i + 1, j);
            }
        }
        
        // Verify final window state
        assert_eq!(window_state.end_idx.timestamp, 5000, "Window end should be at timestamp 5000");
        assert_eq!(window_state.start_idx.timestamp, 3000, "Window start should be at timestamp 3000");
    }

    #[tokio::test]
    async fn test_advance_range_window_position() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // First batch: timestamps [1000, 1200, 1400, 2000, 2200] - multiple events within range
        let batch1 = create_test_batch(vec![1000, 1200, 1400, 2000, 2200], vec![10, 12, 14, 20, 22]);
        let batch_id1 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id1, &batch1, 0).await;
        
        // Create range window frame: RANGE 1000ms PRECEDING (1 second window)
        let window_frame = create_range_window_frame(1000);
        let time_entries = time_index.get_or_create_time_index(&key).await;
        
        // Initial empty window state
        let mut window_state = WindowState {
            tiles: None,
            accumulator_state: None,
            start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
        };
        
        // First advance window position after first batch
        let (first_updates, first_retracts) = slide_window_position(&window_frame, &mut window_state, &time_entries, true);
        
        // Verify first batch processing
        assert_eq!(first_updates.len(), 5, "Should have 5 updates for first batch");
        
        let expected_first_updates: [(i64, Uuid, usize, Vec<(i64, Uuid)>); 5] = [
            (1000, batch_id1, 0, vec![]), // 1st: no retracts (window: [0, 1000])
            (1200, batch_id1, 1, vec![]), // 2nd: no retracts (window: [200, 1200] - but no events before 1000)
            (1400, batch_id1, 2, vec![]), // 3rd: no retracts (window: [400, 1400] - but no events before 1000)
            (2000, batch_id1, 3, vec![]), // 4th: no retracts (window: [1000, 2000] - 1000 is included)
            (2200, batch_id1, 4, vec![(1000, batch_id1)]), // 5th: retract 1000 (window: [1200, 2200] - 1000 is outside)
        ];
        
        for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_first_updates.iter().enumerate() {
            let update_idx = &first_updates[i];
            let retracts = &first_retracts[i];
            
            // Verify update
            assert_eq!(update_idx.timestamp, *expected_ts, "First batch update {} should have timestamp {}", i + 1, expected_ts);
            assert_eq!(update_idx.batch_id, *expected_batch_id, "First batch update {} should have correct batch_id", i + 1);
            assert_eq!(update_idx.row_idx, *expected_row_idx, "First batch update {} should have row_idx {}", i + 1, expected_row_idx);
            
            // Verify retracts
            assert_eq!(retracts.len(), expected_retracts.len(), "First batch update {} should have {} retracts", i + 1, expected_retracts.len());
            for (j, (expected_retract_ts, expected_retract_batch_id)) in expected_retracts.iter().enumerate() {
                assert_eq!(retracts[j].timestamp, *expected_retract_ts, "First batch update {} retract {} should have timestamp {}", i + 1, j, expected_retract_ts);
                assert_eq!(retracts[j].batch_id, *expected_retract_batch_id, "First batch update {} retract {} should have correct batch_id", i + 1, j);
            }
        }
        
        // Verify window state after first batch
        assert_eq!(window_state.end_idx.timestamp, 2200, "Window end should be at timestamp 2200 after first batch");
        assert_eq!(window_state.start_idx.timestamp, 1200, "Window start should be at timestamp 1200 after first batch (1000 was retracted)");
        
        // Add second batch: timestamps [3500, 4000] - will cause multiple retracts
        let batch2 = create_test_batch(vec![3500, 4000], vec![35, 40]);
        let batch_id2 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id2, &batch2, 0).await;
        
        // Second advance window position (incremental processing)
        let (updates, retracts) = slide_window_position(&window_frame, &mut window_state, &time_entries, true);
        
        // Should only process new events (3500, 4000)
        assert_eq!(updates.len(), 2, "Should have 2 incremental updates");
        
        // Expected behavior for incremental updates with RANGE 1000ms PRECEDING (inclusive)
        // Window state before: [1200, 1400, 2000, 2200] (from first batch, 1000 was already retracted)
        let expected_updates = [
            (3500, batch_id2, 0, vec![(1200, batch_id1), (1400, batch_id1), (2000, batch_id1), (2200, batch_id1)]), // Add 3500, retract all old events (outside [2500, 3500] range)
            (4000, batch_id2, 1, vec![]), // Add 4000, no retracts (3500 is within [3000, 4000] range)
        ];
        
        for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_updates.iter().enumerate() {
            let update_idx = &updates[i];
            let retracts = &retracts[i];
            
            // Verify update
            assert_eq!(update_idx.timestamp, *expected_ts, "Incremental update {} should have timestamp {}", i + 1, expected_ts);
            assert_eq!(update_idx.batch_id, *expected_batch_id, "Incremental update {} should have correct batch_id", i + 1);
            assert_eq!(update_idx.row_idx, *expected_row_idx, "Incremental update {} should have row_idx {}", i + 1, expected_row_idx);
            
            // Verify retracts
            assert_eq!(retracts.len(), expected_retracts.len(), "Incremental update {} should have {} retracts", i + 1, expected_retracts.len());
            for (j, (expected_retract_ts, expected_retract_batch_id)) in expected_retracts.iter().enumerate() {
                assert_eq!(retracts[j].timestamp, *expected_retract_ts, "Incremental update {} retract {} should have timestamp {}", i + 1, j, expected_retract_ts);
                assert_eq!(retracts[j].batch_id, *expected_retract_batch_id, "Incremental update {} retract {} should have correct batch_id", i + 1, j);
            }
        }
        
        // Verify final window state
        assert_eq!(window_state.end_idx.timestamp, 4000, "Window end should be at timestamp 4000");
        assert_eq!(window_state.start_idx.timestamp, 3500, "Window start should be at timestamp 3500 (4000-1000 range includes 3500)");
    }

    #[tokio::test]
    async fn test_same_timestamp() {
        let time_index = TimeIndex::new();
        let key = create_test_key("test_partition");
        
        // First batch: duplicate timestamps [1000, 1000, 1500, 2000, 2000]
        let batch1 = create_test_batch(
            vec![1000, 1000, 1500, 2000, 2000], 
            vec![10, 11, 15, 20, 21]
        );
        let batch_id1 = Uuid::new_v4();
        time_index.update_time_index(&key, batch_id1, &batch1, 0).await;
        
        // Test 1: ROWS 2 PRECEDING window (window size = 3)
        {
            let mut window_state_rows = WindowState {
                tiles: None,
                accumulator_state: None,
                start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
                end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            };
            
            let rows_frame = create_rows_window_frame(2);
            let time_entries = time_index.get_or_create_time_index(&key).await;
            
            // First advance - process first batch
            let (first_updates, first_retracts) = slide_window_position(&rows_frame, &mut window_state_rows, &time_entries, true);
            assert_eq!(first_updates.len(), 5, "Should process all 5 events with duplicates");
            
            // Verify duplicate timestamps are processed correctly
            let expected_first_updates = [
                (1000, batch_id1, 0, vec![]), // 1st duplicate at 1000
                (1000, batch_id1, 1, vec![]), // 2nd duplicate at 1000  
                (1500, batch_id1, 2, vec![]), // 3rd event
                (2000, batch_id1, 3, vec![(1000, batch_id1)]), // 4th: retract first 1000
                (2000, batch_id1, 4, vec![(1000, batch_id1)]), // 5th: retract second 1000
            ];
            
            for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_first_updates.iter().enumerate() {
                let update_idx = &first_updates[i];
                let retracts = &first_retracts[i];
                assert_eq!(update_idx.timestamp, *expected_ts, "ROWS update {} should have timestamp {}", i + 1, expected_ts);
                assert_eq!(update_idx.batch_id, *expected_batch_id, "ROWS update {} should have correct batch_id", i + 1);
                assert_eq!(update_idx.row_idx, *expected_row_idx, "ROWS update {} should have row_idx {}", i + 1, expected_row_idx);
                assert_eq!(retracts.len(), expected_retracts.len(), "ROWS update {} should have {} retracts", i + 1, expected_retracts.len());
            }
            
            // Add second batch with more duplicates
            let batch2 = create_test_batch(vec![3000, 3000], vec![30, 31]);
            let batch_id2 = Uuid::new_v4();
            time_index.update_time_index(&key, batch_id2, &batch2, 0).await;
            
            // Second advance - incremental processing
            let (second_updates, second_retracts) = slide_window_position(&rows_frame, &mut window_state_rows, &time_entries, true);
            assert_eq!(second_updates.len(), 2, "Should process 2 new duplicate events");
            
            let expected_second_updates = [
                (3000, batch_id2, 0, vec![(1500, batch_id1)]), // Add first 3000, retract 1500
                (3000, batch_id2, 1, vec![(2000, batch_id1)]), // Add second 3000, retract first 2000
            ];
            
            for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_second_updates.iter().enumerate() {
                let update_idx = &second_updates[i];
                let retracts = &second_retracts[i];
                assert_eq!(update_idx.timestamp, *expected_ts, "ROWS incremental update {} should have timestamp {}", i + 1, expected_ts);
                assert_eq!(update_idx.batch_id, *expected_batch_id, "ROWS incremental update {} should have correct batch_id", i + 1);
                assert_eq!(update_idx.row_idx, *expected_row_idx, "ROWS incremental update {} should have row_idx {}", i + 1, expected_row_idx);
                assert_eq!(retracts.len(), expected_retracts.len(), "ROWS incremental update {} should have {} retracts", i + 1, expected_retracts.len());
            }
        }
        
        // Test 2: RANGE 800ms PRECEDING window
        {
            let mut window_state_range = WindowState {
                tiles: None,
                accumulator_state: None,
                start_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
                end_idx: TimeIdx { timestamp: 0, pos_idx: 0, batch_id: Uuid::nil(), row_idx: 0 },
            };
            
            let range_frame = create_range_window_frame(800);
            let time_entries = time_index.get_or_create_time_index(&key).await;
            
            // First advance - process first batch
            let (first_updates, first_retracts) = slide_window_position(&range_frame, &mut window_state_range, &time_entries, true);
            assert_eq!(first_updates.len(), 7, "Should process all 7 events (5 from batch1 + 2 from batch2)");
            
            // Verify range window behavior with duplicates
            // At timestamp 2000: window = [1200, 2000], so 1000 events should be retracted
            let update_4_idx = &first_updates[3]; // First 2000 event
            let update_4_retracts = &first_retracts[3];
            assert_eq!(update_4_idx.timestamp, 2000);
            assert_eq!(update_4_retracts.len(), 2, "First 2000 event should retract both 1000 events");
            
            let update_5_idx = &first_updates[4]; // Second 2000 event  
            let update_5_retracts = &first_retracts[4];
            assert_eq!(update_5_idx.timestamp, 2000);
            assert_eq!(update_5_retracts.len(), 0, "Second 2000 event should have no retracts");
        }
    }
}