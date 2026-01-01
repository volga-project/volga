// use std::sync::Arc;
// use std::cmp::Ordering;
// use arrow::array::RecordBatch;
// use crossbeam_skiplist::{SkipSet, SkipMap};
// use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};
// use datafusion::scalar::ScalarValue;
// use indexmap::IndexSet;
// use serde::{Serialize, Deserialize};

// use crate::runtime::operators::window::window_operator_state::WindowState;
// use crate::runtime::operators::window::tiles::Tile;
// use crate::runtime::operators::window::Tiles;
// use crate::storage::batch_store::{BatchId, RowIdx, Timestamp};

// #[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
// pub struct TimeIdx {
//     pub timestamp: Timestamp,
//     pub pos_idx: usize, // position within same timestamp
//     pub batch_id: BatchId,
//     pub row_idx: RowIdx,
// }

// impl PartialOrd for TimeIdx {
//     fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
//         Some(self.cmp(other))
//     }
// }

// impl Ord for TimeIdx {
//     fn cmp(&self, other: &Self) -> Ordering {
//         // First compare by timestamp
//         match self.timestamp.cmp(&other.timestamp) {
//             Ordering::Equal => {
//                 // If timestamps are equal, compare by position index
//                 self.pos_idx.cmp(&other.pos_idx)
//             }
//             other => other,
//         }
//     }
// }

// #[derive(Debug)]
// pub struct TimeEntries {
//     pub entries: SkipSet<TimeIdx>,
//     pub batch_ids: SkipMap<Timestamp, Arc<Vec<BatchId>>>,
// }

// impl Serialize for TimeEntries {
//     fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
//     where
//         S: serde::Serializer,
//     {
//         #[derive(Serialize)]
//         struct TimeEntriesSerde {
//             entries: Vec<TimeIdx>,
//             batch_ids: Vec<(Timestamp, Vec<BatchId>)>,
//         }

//         let entries = self.entries.iter().map(|e| *e).collect::<Vec<_>>();
//         let batch_ids = self
//             .batch_ids
//             .iter()
//             .map(|e| (*e.key(), (*e.value()).as_ref().clone()))
//             .collect::<Vec<_>>();

//         TimeEntriesSerde { entries, batch_ids }.serialize(serializer)
//     }
// }

// impl<'de> Deserialize<'de> for TimeEntries {
//     fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
//     where
//         D: serde::Deserializer<'de>,
//     {
//         #[derive(Deserialize)]
//         struct TimeEntriesSerde {
//             entries: Vec<TimeIdx>,
//             batch_ids: Vec<(Timestamp, Vec<BatchId>)>,
//         }

//         let decoded = TimeEntriesSerde::deserialize(deserializer)?;
//         let time_entries = TimeEntries::new();
//         for entry in decoded.entries {
//             time_entries.entries.insert(entry);
//         }
//         for (ts, batch_ids) in decoded.batch_ids {
//             time_entries.batch_ids.insert(ts, Arc::new(batch_ids));
//         }
//         Ok(time_entries)
//     }
// }

// impl TimeEntries {
//     pub fn new() -> Self {
//         Self {
//             entries: SkipSet::new(),
//             batch_ids: SkipMap::new(),
//         }
//     }

//     pub fn latest_idx(&self) -> TimeIdx {
//         self.entries.back().map(|entry| *entry).expect("Time entries should exist")
//     }

//     pub fn earliest_idx(&self) -> TimeIdx {
//         self.entries.front().map(|entry| *entry).expect("Time entries should exist")
//     }

//     pub fn insert_batch(
//         &self,
//         batch_id: BatchId,
//         batch: &RecordBatch,
//         ts_column_index: usize
//     ) -> Vec<TimeIdx> {
//         use arrow::array::{TimestampMillisecondArray, Array};
//         use arrow::compute::kernels::aggregate::max;
        
//         let num_rows = batch.num_rows();
//         if num_rows == 0 {
//             return Vec::new();
//         }
        
//         let ts_column = batch.column(ts_column_index);
//         let ts_array = ts_column.as_any().downcast_ref::<TimestampMillisecondArray>()
//             .expect("Timestamp column should be TimestampMillisecondArray");
        
//         // Extract all timestamps at once (vectorized)
//         let timestamps: Vec<i64> = (0..num_rows)
//             .map(|i| {
//                 if ts_array.is_null(i) {
//                     i64::MIN // Handle nulls - will be filtered or handled separately if needed
//                 } else {
//                     ts_array.value(i)
//                 }
//             })
//             .collect();
        
//         // Find max timestamp using Arrow kernel
//         let max_timestamp = max(ts_array)
//             .and_then(|sv| i64::try_from(sv).ok())
//             .unwrap_or(i64::MIN);
        
//         // Track the last pos_idx used for each timestamp to optimize lookups
//         // This allows us to avoid SkipSet lookup for consecutive rows with same timestamp
//         use std::collections::HashMap;
//         let mut timestamp_to_last_pos: HashMap<i64, usize> = HashMap::new();
        
//         let mut time_idxs = Vec::with_capacity(num_rows);
        
//         // Process rows in order to maintain correct pos_idx assignment
//         for row_idx in 0..num_rows {
//             let timestamp = timestamps[row_idx];
            
//             // Get or compute the pos_idx for this timestamp
//             let pos_idx = if let Some(&last_pos) = timestamp_to_last_pos.get(&timestamp) {
//                 // We've seen this timestamp in this batch, increment
//                 let next_pos = last_pos + 1;
//                 timestamp_to_last_pos.insert(timestamp, next_pos);
//                 next_pos
//             } else {
//                 // First time seeing this timestamp in this batch, find max in SkipSet
//                 let search_key = TimeIdx {
//                     timestamp,
//                     pos_idx: usize::MAX,
//                     batch_id: BatchId::nil(),
//                     row_idx: 0,
//                 };
                
//                 let base_pos_idx = if let Some(existing_entry) = self.entries.upper_bound(std::ops::Bound::Included(&search_key)) {
//                     if existing_entry.timestamp == timestamp {
//                         existing_entry.pos_idx + 1
//                     } else {
//                         0
//                     }
//                 } else {
//                     0
//                 };
                
//                 timestamp_to_last_pos.insert(timestamp, base_pos_idx);
//                 base_pos_idx
//             };
            
//             let time_idx = TimeIdx {
//                 timestamp,
//                 pos_idx,
//                 batch_id,
//                 row_idx,
//             };
            
//             self.entries.insert(time_idx);
//             time_idxs.push(time_idx);
//         }
        
//         // Insert batch mapping once with max timestamp
//         // Use Arc to avoid cloning the Vec - only clone the Arc reference
//         if let Some(existing_batches) = self.batch_ids.get(&max_timestamp) {
//             let mut batches: Vec<BatchId> = existing_batches.value().as_ref().clone();
//             batches.push(batch_id);
//             self.batch_ids.insert(max_timestamp, Arc::new(batches));
//         } else {
//             self.batch_ids.insert(max_timestamp, Arc::new(vec![batch_id]));
//         }
        
//         time_idxs
//     }
    
//     pub fn get_window_start(
//         &self,
//         window_frame: &Arc<WindowFrame>,
//         window_end: TimeIdx,
//         use_front_as_start: bool,
//     ) -> Option<TimeIdx> {
//         let res = 
//             if window_frame.units == WindowFrameUnits::Rows {
//                 // Handle ROWS
//                 match window_frame.start_bound {
//                     WindowFrameBound::Preceding(ScalarValue::UInt64(Some(delta))) => {
//                         // For ROWS windows, we need to find the position of window_end in the entries
//                         // and then jump backwards delta elements from that position
                        
//                         // Find the position where window_end would be inserted (or exists)
//                         let window_start = if let Some(exact_entry) = self.entries.get(&window_end) {
//                             // Exact match found, use all entries up to and including this one
//                             self.entries.range(..=*exact_entry)
//                                 .rev()
//                                 .nth(delta as usize)
//                                 .map(|entry| *entry)
//                         } else {
//                             // No exact match, find the position where it would be inserted
//                             // Use lower_bound to find the first entry greater than window_end
//                             if let Some(next_entry) = self.entries.lower_bound(std::ops::Bound::Excluded(&window_end)) {
//                                 self.entries.range(..=*next_entry)
//                                     .rev()
//                                     .nth(delta as usize)
//                                     .map(|entry| *entry)
//                             } else {
//                                 // window_end is beyond all entries, use all entries up to and including the latest entry
//                                 let latest = self.latest_idx();
//                                 // subtract 1 from delta to make sure we include the end which is not inside the range
//                                 self.entries.range(..=latest)
//                                     .rev()
//                                     .nth((delta as usize).saturating_sub(1))
//                                     .map(|entry| *entry)
//                             }
//                         };

//                         window_start
//                     }
//                     _ => panic!("Only Preceding start bound is supported for ROWS WindowFrameUnits"),
//                 }
//             } else if window_frame.units == WindowFrameUnits::Range {
//                 // Handle RANGE
//                 let window_end_timestamp = window_end.timestamp;
//                 if window_end_timestamp < self.earliest_idx().timestamp {
//                     panic!("Window end timestamp is before earliest timestamp");
//                 }

//                 let window_start_timestamp = match &window_frame.start_bound {
//                     WindowFrameBound::Preceding(delta) => {
//                         if window_frame.start_bound.is_unbounded() || delta.is_null() {
//                             panic!("Can not retract UNBOUNDED PRECEDING");
//                         }
                        
//                         let delta_i64 = convert_interval_to_milliseconds(delta);
                            
//                         window_end_timestamp.saturating_sub(delta_i64)
//                     },
//                     _ => panic!("Only Preceding start bound is supported"),
//                 };
                
//                 // Get new window start from time index
//                 let search_key = TimeIdx {
//                     timestamp: window_start_timestamp,
//                     pos_idx: 0,
//                     batch_id: BatchId::nil(), // dummy values for search
//                     row_idx: 0,
//                 };
//                 // Include all rows with this timestamp
//                 let window_start = self.entries.lower_bound(std::ops::Bound::Included(&search_key))
//                     .map(|entry| *entry);

//                 window_start
//             } else {
//                 panic!("Unsupported WindowFrame type: only ROW and RANGE are supported");
//             };

//         if res.is_none() && use_front_as_start {
//             return self.entries.front().map(|entry| *entry)
//         }
//         res
//     }
    
//     pub fn find_retracts(
//         &self,
//         window_frame: &Arc<WindowFrame>,
//         previous_window_start: TimeIdx,
//         new_window_end: TimeIdx,
//     ) -> (Vec<TimeIdx>, Option<TimeIdx>) {
//         let new_window_start = self.get_window_start(window_frame, new_window_end, false);
    
//         if window_frame.units == WindowFrameUnits::Rows {
//             // Handle ROWS
//             if new_window_start.is_none() {
//                 // window does not have enough rows to retract
//                 return (Vec::new(), None);
//             }
            
//             // Collect all TimeIdxs between previous window start and new_window_start for retraction
//             let new_start_idx = new_window_start.expect("Window start idx should exist");
//             let retract_idxs: Vec<TimeIdx> = self.entries
//                 .range(previous_window_start..new_start_idx)
//                 .map(|entry: crossbeam_skiplist::set::Entry<'_, TimeIdx>| *entry)
//                 .collect();
    
//             return (retract_idxs, Some(new_start_idx));
//         } else if window_frame.units == WindowFrameUnits::Range {
//             // Handle RANGE
//             if new_window_start.is_none() {
//                 // window moved beyong range, retract all
//                 let retract_idxs: Vec<TimeIdx> = self.entries
//                     .range(previous_window_start..)
//                     .map(|entry| *entry)
//                     .collect();
//                 return (retract_idxs, None);
//             } else {
//                 let retract_idxs: Vec<TimeIdx> = self.entries
//                     // Exclude new_window_start
//                     .range(previous_window_start..new_window_start.expect("Window start idx should exist"))
//                     .map(|entry| *entry)
//                     .collect();
//                 return (retract_idxs, new_window_start);
//             }
//         } else {
//             panic!("Unsupported WindowFrame type: only ROW and RANGE are supported");
//         }
//     }
    
//     pub fn slide_window_position(
//         &self,
//         window_frame: &Arc<WindowFrame>, 
//         previous_window_start: TimeIdx,
//         previous_window_end: TimeIdx,
//         new_window_end: TimeIdx,
//         with_retracts: bool
//     ) -> (Vec<TimeIdx>, Vec<Vec<TimeIdx>>, TimeIdx, TimeIdx) {
//         let mut updates = Vec::new();
//         let mut retracts = Vec::new();
    
//         if previous_window_end == new_window_end {
//             // nothing changed
//             return (updates, retracts, previous_window_start, previous_window_end);
//         }
    
//         let diff_range_start = if previous_window_end.batch_id == BatchId::nil() {
//             // first time
//             *self.entries.front().expect("Time entries should exist")
//         } else {
//             // start from closest timestamp to previous window end
//             self.entries.lower_bound(std::ops::Bound::Excluded(&previous_window_end))
//                 .map(|entry| *entry).expect(&format!("Range start idx should exist for {:?}", previous_window_end))
//         };
    
//         let mut start = previous_window_start;
//         let mut end = previous_window_end;
//         for entry in self.entries.range(diff_range_start..=new_window_end) {
//             let time_idx = *entry;
            
//             let (rets, new_start) = self.find_retracts(window_frame, start, time_idx);
//             updates.push(time_idx);
//             if with_retracts {
//                 retracts.push(rets);
//             }
//             if let Some(new_start) = new_start {
//                 start = new_start;
//             }
//             end = time_idx;
//         }
        
//         (updates, retracts, start, end)
//     }
    
//     // returns a mix of raw events and tiles for range
//     pub fn get_entries_in_range(
//         &self,
//         tiles: Option<&Tiles>,
//         start_idx: TimeIdx,
//         end_idx: TimeIdx
//     ) -> (Vec<TimeIdx>, Vec<Tile>, Vec<TimeIdx>) {
//         let tiles_in_range = if let Some(tiles) = tiles {
//             tiles.get_tiles_for_range(start_idx.timestamp, end_idx.timestamp)
//         } else {
//             Vec::new()
//         };
        
//         // we assume that tiles range has no gaps that can be filled by raw events 
//         // i.e. if a tile does not exist (gap) then events do not exist (due to write consistency)
//         // hence check for raw events only between range start and first tile start and between last tile end and range end
//         if tiles_in_range.is_empty() {
//             // no tiles, use all entries in range
//             (self.entries.range(start_idx..=end_idx).map(|entry| *entry).collect(), Vec::new(), Vec::new())
//         } else {
//             // get front padding
//             let front_boundry = TimeIdx {
//                 timestamp: tiles_in_range[0].tile_start,
//                 pos_idx: 0,
//                 batch_id: BatchId::nil(), // dummy values for search
//                 row_idx: 0,
//             };
//             let front_padding: Vec<TimeIdx> = self.entries.range(start_idx..front_boundry).map(|entry| *entry).collect();
//             // get back padding
//             let back_boundry = TimeIdx {
//                 timestamp: tiles_in_range[tiles_in_range.len() - 1].tile_end,
//                 pos_idx: 0,
//                 batch_id: BatchId::nil(), // dummy values for search
//                 row_idx: 0,
//             };
//             let back_padding: Vec<TimeIdx> = self.entries.range(back_boundry..=end_idx).map(|entry| *entry).collect();
            
//             (front_padding, tiles_in_range, back_padding)
//         }
//     }

//     pub fn get_cutoff_timestamp(
//         &self,
//         window_frame: &Arc<WindowFrame>,
//         window_state: &WindowState,
//         lateness: i64
//     ) -> Timestamp {
//         match window_frame.units {
//             WindowFrameUnits::Range => {
//                 // For range windows: simple logic
//                 // cutoff = end_timestamp - (lateness + window_length_ms)
//                 if let WindowFrameBound::Preceding(window_length) = &window_frame.start_bound {
//                     let window_length_ms = convert_interval_to_milliseconds(window_length);
//                     // let cutoff = window_state.end_idx.timestamp - (lateness + window_length_ms);
//                     let cutoff = 0;
//                     if cutoff > 0 {
//                         cutoff
//                     } else {
//                         0
//                     }
//                 } else {
//                     panic!("Expected Preceding bound for range windows");
//                 }
//             },
//             WindowFrameUnits::Rows => {
//                 // For row windows: use lookup key and get_window_start;
//                 let search_key = TimeIdx {
//                     // timestamp: window_state.end_idx.timestamp - lateness,
//                     timestamp: 0,
//                     pos_idx: usize::MAX,
//                     batch_id: BatchId::nil(),
//                     row_idx: 0,
//                 };
//                 if let Some(last_valid_entry) = self.entries.upper_bound(std::ops::Bound::Included(&search_key)) {
//                     if let Some(window_start) = self.get_window_start(&window_frame, *last_valid_entry, false) {
//                         window_start.timestamp
//                     } else {
//                         // If we can't find window start, no cutoff
//                         0
//                     }
//                 } else {
//                     // No entries found, no cutoff
//                     0
//                 }
//             },
//             _ => {
//                 panic!("Unsupported WindowFrameUnits: only ROWS and RANGE are supported");
//             }
//         }
//     }

//     pub fn prune(
//         &self, 
//         cutoff_timestamp: Timestamp
//     ) -> Vec<BatchId> {
//         let mut batches_to_delete = IndexSet::new();
        
//         // Collect batches to delete and prune timestamp_to_batch mapping
//         let timestamps_to_remove: Vec<Timestamp> = self.batch_ids
//             .range(..cutoff_timestamp)
//             .map(|entry| {
//                 // Add all batch IDs for this timestamp
//                 for batch_id in entry.value().iter() {
//                     batches_to_delete.insert(*batch_id);
//                 }
//                 *entry.key()
//             })
//             .collect();
        
//         // Remove timestamps from mapping
//         for timestamp in timestamps_to_remove {
//             self.batch_ids.remove(&timestamp);
//         }
        
//         // Prune time entries
//         let cutoff_key = TimeIdx {
//             timestamp: cutoff_timestamp,
//             pos_idx: 0,
//             batch_id: BatchId::nil(),
//             row_idx: 0,
//         };
        
//         // Collect entries to remove
//         let entries_to_remove: Vec<TimeIdx> = self.entries
//             .range(..cutoff_key)
//             .map(|entry| *entry)
//             .collect();
        
//         // Remove the entries
//         for entry in entries_to_remove {
//             self.entries.remove(&entry);
//         }
        
//         batches_to_delete.into_iter().collect()
//     }
// }


// /// Convert various interval types to milliseconds for timestamp arithmetic
// pub fn convert_interval_to_milliseconds(delta: &ScalarValue) -> i64 {
//     match delta {
//         ScalarValue::IntervalMonthDayNano(Some(interval)) => {
//             // Based on DataFusion's interval_mdn_to_duration_ns logic
//             if interval.months == 0 && interval.days == 0 {
//                 // Convert nanoseconds to milliseconds
//                 interval.nanoseconds / 1_000_000
//             } else {
//                 panic!("Cannot convert interval with non-zero months or days to milliseconds");
//             }
//         },
//         ScalarValue::IntervalDayTime(Some(interval)) => {
//             // Based on DataFusion's interval_dt_to_duration_ms logic  
//             if interval.days == 0 {
//                 interval.milliseconds as i64
//             } else {
//                 panic!("Cannot convert interval with non-zero days to milliseconds");
//             }
//         },
//         _ => i64::try_from(delta.clone())
//             .expect("Should be able to convert delta to i64")
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use std::sync::Arc;
//     use arrow::array::{Int64Array, TimestampMillisecondArray};
//     use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
//     use arrow::record_batch::RecordBatch;
//     use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

//     fn create_test_batch(timestamps: Vec<i64>, values: Vec<i64>) -> RecordBatch {
//         let schema = Arc::new(Schema::new(vec![
//             Field::new("ts", DataType::Timestamp(TimeUnit::Millisecond, None), false),
//             Field::new("value", DataType::Int64, false),
//         ]));

//         let timestamp_array = TimestampMillisecondArray::from(timestamps);
//         let value_array = Int64Array::from(values);

//         RecordBatch::try_new(schema, vec![
//             Arc::new(timestamp_array),
//             Arc::new(value_array),
//         ]).expect("Failed to create test batch")
//     }

//     fn create_rows_window_frame(rows: u64) -> Arc<WindowFrame> {
//         let mut frame = WindowFrame::new(Some(false));
//         frame.units = WindowFrameUnits::Rows;
//         frame.start_bound = WindowFrameBound::Preceding(datafusion::scalar::ScalarValue::UInt64(Some(rows)));
//         frame.end_bound = WindowFrameBound::CurrentRow;
//         Arc::new(frame)
//     }

//     fn create_range_window_frame(range_ms: i64) -> Arc<WindowFrame> {
//         let mut frame = WindowFrame::new(Some(false));
//         frame.units = WindowFrameUnits::Range;
//         frame.start_bound = WindowFrameBound::Preceding(datafusion::scalar::ScalarValue::TimestampMillisecond(Some(range_ms), None));
//         frame.end_bound = WindowFrameBound::CurrentRow;
//         Arc::new(frame)
//     }

//     #[tokio::test]
//     async fn test_time_index_insertion() {
//         let time_entries = TimeEntries::new();
        
//         // Test 1: Insert batch in random order [3000, 1000, 2000]
//         let batch1 = create_test_batch(vec![3000, 1000, 2000], vec![30, 10, 20]);
//         let batch_id1 = BatchId::random();
//         time_entries.insert_batch(batch_id1, &batch1, 0);
        
//         // let time_entries = time_index.get_or_create_time_index(&key).await;
//         assert_eq!(time_entries.entries.len(), 3, "Should have 3 timestamp entries");
        
//         // Verify entries are sorted by timestamp
//         let entries_vec: Vec<_> = time_entries.entries.iter().map(|entry| *entry).collect();
//         assert_eq!(entries_vec[0].timestamp, 1000, "First entry should be timestamp 1000");
//         assert_eq!(entries_vec[1].timestamp, 2000, "Second entry should be timestamp 2000");
//         assert_eq!(entries_vec[2].timestamp, 3000, "Third entry should be timestamp 3000");
        
//         // Verify front and back
//         let front = time_entries.entries.front().expect("Should have front entry");
//         let back = time_entries.entries.back().expect("Should have back entry");
//         assert_eq!(front.timestamp, 1000, "Front should be earliest timestamp");
//         assert_eq!(back.timestamp, 3000, "Back should be latest timestamp");
        
//         // Test 2: Insert batch with same timestamps [1000, 1000, 2000, 2000]
//         let batch2 = create_test_batch(vec![1000, 1000, 2000, 2000], vec![11, 12, 21, 22]);
//         let batch_id2 = BatchId::random();
//         time_entries.insert_batch(batch_id2, &batch2, 0);
        
//         // let time_entries = time_index.get_or_create_time_index(&key).await;
//         let entries_vec: Vec<_> = time_entries.entries.iter().map(|entry| *entry).collect();
//         assert_eq!(entries_vec.len(), 7, "Should have 7 total entries");
        
//         // Verify multiple entries for same timestamp
//         let entries_1000: Vec<_> = entries_vec.iter().filter(|e| e.timestamp == 1000).collect();
//         let entries_2000: Vec<_> = entries_vec.iter().filter(|e| e.timestamp == 2000).collect();
        
//         // Should have 3 entries for timestamp 1000 (1 from batch1, 2 from batch2)
//         assert_eq!(entries_1000.len(), 3, "Should have 3 entries for timestamp 1000");
//         // Should have 3 entries for timestamp 2000 (1 from batch1, 2 from batch2)
//         assert_eq!(entries_2000.len(), 3, "Should have 3 entries for timestamp 2000");
//     }

//     #[tokio::test]
//     async fn test_slide_row_window_position() {
//         let time_entries = TimeEntries::new();
        
//         // First batch: timestamps [1000, 2000, 3000]
//         let batch1 = create_test_batch(vec![1000, 2000, 3000], vec![10, 20, 30]);
//         let batch_id1 = BatchId::random();
//         time_entries.insert_batch(batch_id1, &batch1, 0);
        
//         // Create window frame: ROWS 2 PRECEDING (window size = 3)
//         let window_frame = create_rows_window_frame(2);
        
//         // Initial empty window bounds
//         let start_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };
//         let end_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };
        
//         // First slide window position after first batch
//         let (first_updates, first_retracts, new_window_start, new_window_end) = time_entries.slide_window_position(
//             &window_frame, start_idx, end_idx, time_entries.latest_idx(), true
//         );
        
//         // Verify first batch processing
//         assert_eq!(first_updates.len(), 3, "Should have 3 updates for first batch");
        
//         let expected_first_updates: [(i64, BatchId, usize, Vec<(i64, BatchId)>); 3] = [
//             (1000, batch_id1, 0, vec![]), // 1st: no retracts (window: [1000])
//             (2000, batch_id1, 1, vec![]), // 2nd: no retracts (window: [1000, 2000])
//             (3000, batch_id1, 2, vec![]), // 3rd: no retracts (window: [1000, 2000, 3000] - window full)
//         ];
        
//         for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_first_updates.iter().enumerate() {
//             let update_idx = &first_updates[i];
//             let retracts = &first_retracts[i];

//             // Verify update
//             assert_eq!(update_idx.timestamp, *expected_ts, "First batch update {} should have timestamp {}", i + 1, expected_ts);
//             assert_eq!(update_idx.batch_id, *expected_batch_id, "First batch update {} should have correct batch_id", i + 1);
//             assert_eq!(update_idx.row_idx, *expected_row_idx, "First batch update {} should have row_idx {}", i + 1, expected_row_idx);
            
//             // Verify retracts
//             assert_eq!(retracts.len(), expected_retracts.len(), "First batch update {} should have {} retracts", i + 1, expected_retracts.len());
//         }
        
//         // Verify window state after first batch
//         assert_eq!(new_window_end.timestamp, 3000, "Window end should be at timestamp 3000 after first batch");
//         assert_eq!(new_window_start.timestamp, 1000, "Window start should be at timestamp 1000 after first batch");
        
//         // Add second batch: timestamps [4000, 5000]
//         let batch2 = create_test_batch(vec![4000, 5000], vec![40, 50]);
//         let batch_id2 = BatchId::random();
//         time_entries.insert_batch(batch_id2, &batch2, 0);
        
//         // Second slide window position (incremental processing)
//         let (updates, retracts, new_window_start, new_window_end) = time_entries.slide_window_position(&window_frame, new_window_start, new_window_end, time_entries.latest_idx(), true);
        
//         // Should only process new events (4000, 5000)
//         assert_eq!(updates.len(), 2, "Should have 2 incremental updates");
        
//         // Expected behavior for incremental updates with ROWS 2 PRECEDING (window size = 3)
//         // Window state before: [1000, 2000, 3000] (from first batch)
//         let expected_updates = [
//             (4000, batch_id2, 0, vec![(1000, batch_id1)]), // Add 4000, retract 1000 (window: [2000, 3000, 4000])
//             (5000, batch_id2, 1, vec![(2000, batch_id1)]), // Add 5000, retract 2000 (window: [3000, 4000, 5000])
//         ];
        
//         for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_updates.iter().enumerate() {
//             let update_idx = &updates[i];
//             let retracts = &retracts[i];
            
//             // Verify update
//             assert_eq!(update_idx.timestamp, *expected_ts, "Incremental update {} should have timestamp {}", i + 1, expected_ts);
//             assert_eq!(update_idx.batch_id, *expected_batch_id, "Incremental update {} should have correct batch_id", i + 1);
//             assert_eq!(update_idx.row_idx, *expected_row_idx, "Incremental update {} should have row_idx {}", i + 1, expected_row_idx);
            
//             // Verify retracts
//             assert_eq!(retracts.len(), expected_retracts.len(), "Incremental update {} should have {} retracts", i + 1, expected_retracts.len());
//             for (j, (expected_retract_ts, expected_retract_batch_id)) in expected_retracts.iter().enumerate() {
//                 assert_eq!(retracts[j].timestamp, *expected_retract_ts, "Incremental update {} retract {} should have timestamp {}", i + 1, j, expected_retract_ts);
//                 assert_eq!(retracts[j].batch_id, *expected_retract_batch_id, "Incremental update {} retract {} should have correct batch_id", i + 1, j);
//             }
//         }
        
//         // Verify final window state
//         assert_eq!(new_window_end.timestamp, 5000, "Window end should be at timestamp 5000");
//         assert_eq!(new_window_start.timestamp, 3000, "Window start should be at timestamp 3000");
//     }

//     #[tokio::test]
//     async fn test_slide_range_window_position() {
//         let time_entries = TimeEntries::new();
        
//         // First batch: timestamps [1000, 1200, 1400, 2000, 2200] - multiple events within range
//         let batch1 = create_test_batch(vec![1000, 1200, 1400, 2000, 2200], vec![10, 12, 14, 20, 22]);
//         let batch_id1 = BatchId::random();
//         time_entries.insert_batch(batch_id1, &batch1, 0);
        
//         // Create range window frame: RANGE 1000ms PRECEDING (1 second window)
//         let window_frame = create_range_window_frame(1000);
        
//         // Initial empty window bounds
//         let start_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };
//         let end_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };
        
//         // First slide window position after first batch
//         let (first_updates, first_retracts, new_window_start, new_window_end) = time_entries.slide_window_position(
//             &window_frame, start_idx, end_idx, time_entries.latest_idx(), true
//         );
        
//         // Verify first batch processing
//         assert_eq!(first_updates.len(), 5, "Should have 5 updates for first batch");
        
//         let expected_first_updates: [(i64, BatchId, usize, Vec<(i64, BatchId)>); 5] = [
//             (1000, batch_id1, 0, vec![]), // 1st: no retracts (window: [0, 1000])
//             (1200, batch_id1, 1, vec![]), // 2nd: no retracts (window: [200, 1200] - but no events before 1000)
//             (1400, batch_id1, 2, vec![]), // 3rd: no retracts (window: [400, 1400] - but no events before 1000)
//             (2000, batch_id1, 3, vec![]), // 4th: no retracts (window: [1000, 2000] - 1000 is included)
//             (2200, batch_id1, 4, vec![(1000, batch_id1)]), // 5th: retract 1000 (window: [1200, 2200] - 1000 is outside)
//         ];
        
//         for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_first_updates.iter().enumerate() {
//             let update_idx = &first_updates[i];
//             let retracts = &first_retracts[i];
            
//             // Verify update
//             assert_eq!(update_idx.timestamp, *expected_ts, "First batch update {} should have timestamp {}", i + 1, expected_ts);
//             assert_eq!(update_idx.batch_id, *expected_batch_id, "First batch update {} should have correct batch_id", i + 1);
//             assert_eq!(update_idx.row_idx, *expected_row_idx, "First batch update {} should have row_idx {}", i + 1, expected_row_idx);
            
//             // Verify retracts
//             assert_eq!(retracts.len(), expected_retracts.len(), "First batch update {} should have {} retracts", i + 1, expected_retracts.len());
//             for (j, (expected_retract_ts, expected_retract_batch_id)) in expected_retracts.iter().enumerate() {
//                 assert_eq!(retracts[j].timestamp, *expected_retract_ts, "First batch update {} retract {} should have timestamp {}", i + 1, j, expected_retract_ts);
//                 assert_eq!(retracts[j].batch_id, *expected_retract_batch_id, "First batch update {} retract {} should have correct batch_id", i + 1, j);
//             }
//         }
        
//         // Verify window state after first batch
//         assert_eq!(new_window_end.timestamp, 2200, "Window end should be at timestamp 2200 after first batch");
//         assert_eq!(new_window_start.timestamp, 1200, "Window start should be at timestamp 1200 after first batch (1000 was retracted)");
        
//         // Add second batch: timestamps [3500, 4000] - will cause multiple retracts
//         let batch2 = create_test_batch(vec![3500, 4000], vec![35, 40]);
//         let batch_id2 = BatchId::random();
//         time_entries.insert_batch(batch_id2, &batch2, 0);
        
//         // Second slide window position (incremental processing)
//         let (updates, retracts, new_window_start, new_window_end) = time_entries.slide_window_position(
//             &window_frame, new_window_start, new_window_end, time_entries.latest_idx(), true
//         );
        
//         // Should only process new events (3500, 4000)
//         assert_eq!(updates.len(), 2, "Should have 2 incremental updates");
        
//         // Expected behavior for incremental updates with RANGE 1000ms PRECEDING (inclusive)
//         // Window state before: [1200, 1400, 2000, 2200] (from first batch, 1000 was already retracted)
//         let expected_updates = [
//             (3500, batch_id2, 0, vec![(1200, batch_id1), (1400, batch_id1), (2000, batch_id1), (2200, batch_id1)]), // Add 3500, retract all old events (outside [2500, 3500] range)
//             (4000, batch_id2, 1, vec![]), // Add 4000, no retracts (3500 is within [3000, 4000] range)
//         ];
        
//         for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_updates.iter().enumerate() {
//             let update_idx = &updates[i];
//             let retracts = &retracts[i];
            
//             // Verify update
//             assert_eq!(update_idx.timestamp, *expected_ts, "Incremental update {} should have timestamp {}", i + 1, expected_ts);
//             assert_eq!(update_idx.batch_id, *expected_batch_id, "Incremental update {} should have correct batch_id", i + 1);
//             assert_eq!(update_idx.row_idx, *expected_row_idx, "Incremental update {} should have row_idx {}", i + 1, expected_row_idx);
            
//             // Verify retracts
//             assert_eq!(retracts.len(), expected_retracts.len(), "Incremental update {} should have {} retracts", i + 1, expected_retracts.len());
//             for (j, (expected_retract_ts, expected_retract_batch_id)) in expected_retracts.iter().enumerate() {
//                 assert_eq!(retracts[j].timestamp, *expected_retract_ts, "Incremental update {} retract {} should have timestamp {}", i + 1, j, expected_retract_ts);
//                 assert_eq!(retracts[j].batch_id, *expected_retract_batch_id, "Incremental update {} retract {} should have correct batch_id", i + 1, j);
//             }
//         }
        
//         // Verify final window state
//         assert_eq!(new_window_end.timestamp, 4000, "Window end should be at timestamp 4000");
//         assert_eq!(new_window_start.timestamp, 3500, "Window start should be at timestamp 3500 (4000-1000 range includes 3500)");
//     }

//     #[tokio::test]
//     async fn test_slide_same_timestamp() {
//         let time_entries = TimeEntries::new();
        
//         // First batch: duplicate timestamps [1000, 1000, 1500, 2000, 2000]
//         let batch1 = create_test_batch(
//             vec![1000, 1000, 1500, 2000, 2000], 
//             vec![10, 11, 15, 20, 21]
//         );
//         let batch_id1 = BatchId::random();
//         time_entries.insert_batch(batch_id1, &batch1, 0);
        
//         // Test 1: ROWS 2 PRECEDING window (window size = 3)
//         {
//             let start_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };
//             let end_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };
            
//             let rows_frame = create_rows_window_frame(2);
            
//             // First slide - process first batch
//             let (first_updates, first_retracts, new_window_start, new_window_end) = time_entries.slide_window_position(
//                 &rows_frame, start_idx, end_idx, time_entries.latest_idx(), true
//             );
//             assert_eq!(first_updates.len(), 5, "Should process all 5 events with duplicates");
            
//             // Verify duplicate timestamps are processed correctly
//             let expected_first_updates = [
//                 (1000, batch_id1, 0, vec![]), // 1st duplicate at 1000
//                 (1000, batch_id1, 1, vec![]), // 2nd duplicate at 1000  
//                 (1500, batch_id1, 2, vec![]), // 3rd event
//                 (2000, batch_id1, 3, vec![(1000, batch_id1)]), // 4th: retract first 1000
//                 (2000, batch_id1, 4, vec![(1000, batch_id1)]), // 5th: retract second 1000
//             ];
            
//             for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_first_updates.iter().enumerate() {
//                 let update_idx = &first_updates[i];
//                 let retracts = &first_retracts[i];
//                 assert_eq!(update_idx.timestamp, *expected_ts, "ROWS update {} should have timestamp {}", i + 1, expected_ts);
//                 assert_eq!(update_idx.batch_id, *expected_batch_id, "ROWS update {} should have correct batch_id", i + 1);
//                 assert_eq!(update_idx.row_idx, *expected_row_idx, "ROWS update {} should have row_idx {}", i + 1, expected_row_idx);
//                 assert_eq!(retracts.len(), expected_retracts.len(), "ROWS update {} should have {} retracts", i + 1, expected_retracts.len());
//             }
            
//             // Add second batch with more duplicates
//             let batch2 = create_test_batch(vec![3000, 3000], vec![30, 31]);
//             let batch_id2 = BatchId::random();
//             time_entries.insert_batch(batch_id2, &batch2, 0);
            
//             // Second slide - incremental processing
//             let (second_updates, second_retracts, _, _) = time_entries.slide_window_position(
//                 &rows_frame, new_window_start, new_window_end, time_entries.latest_idx(), true
//             );
//             assert_eq!(second_updates.len(), 2, "Should process 2 new duplicate events");
            
//             let expected_second_updates = [
//                 (3000, batch_id2, 0, vec![(1500, batch_id1)]), // Add first 3000, retract 1500
//                 (3000, batch_id2, 1, vec![(2000, batch_id1)]), // Add second 3000, retract first 2000
//             ];
            
//             for (i, (expected_ts, expected_batch_id, expected_row_idx, expected_retracts)) in expected_second_updates.iter().enumerate() {
//                 let update_idx = &second_updates[i];
//                 let retracts = &second_retracts[i];
//                 assert_eq!(update_idx.timestamp, *expected_ts, "ROWS incremental update {} should have timestamp {}", i + 1, expected_ts);
//                 assert_eq!(update_idx.batch_id, *expected_batch_id, "ROWS incremental update {} should have correct batch_id", i + 1);
//                 assert_eq!(update_idx.row_idx, *expected_row_idx, "ROWS incremental update {} should have row_idx {}", i + 1, expected_row_idx);
//                 assert_eq!(retracts.len(), expected_retracts.len(), "ROWS incremental update {} should have {} retracts", i + 1, expected_retracts.len());
//             }
//         }
        
//         // Test 2: RANGE 800ms PRECEDING window
//         {
//             let start_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };
//             let end_idx = TimeIdx { timestamp: 0, pos_idx: 0, batch_id: BatchId::nil(), row_idx: 0 };

//             let range_frame = create_range_window_frame(800);
            
//             // First slide - process first batch
//             let (first_updates, first_retracts, _, _) = time_entries.slide_window_position(
//                 &range_frame, start_idx, end_idx, time_entries.latest_idx(), true
//             );
//             assert_eq!(first_updates.len(), 7, "Should process all 7 events (5 from batch1 + 2 from batch2)");
            
//             // Verify range window behavior with duplicates
//             // At timestamp 2000: window = [1200, 2000], so 1000 events should be retracted
//             let update_4_idx = &first_updates[3]; // First 2000 event
//             let update_4_retracts = &first_retracts[3];
//             assert_eq!(update_4_idx.timestamp, 2000);
//             assert_eq!(update_4_retracts.len(), 2, "First 2000 event should retract both 1000 events");
            
//             let update_5_idx = &first_updates[4]; // Second 2000 event  
//             let update_5_retracts = &first_retracts[4];
//             assert_eq!(update_5_idx.timestamp, 2000);
//             assert_eq!(update_5_retracts.len(), 0, "Second 2000 event should have no retracts");
//         }
//     }

//     #[tokio::test]
//     async fn test_get_window_start_rows() {
//         let time_entries = TimeEntries::new();
        
//         // Setup test data: timestamps [500, 1000, 2000, 3000, 4000, 5000]
//         let batch = create_test_batch(vec![500, 1000, 2000, 3000, 4000, 5000], vec![5, 10, 20, 30, 40, 50]);
//         let batch_id = BatchId::random();
//         let time_idxs = time_entries.insert_batch(batch_id, &batch, 0);
        
//         // Create ROWS 2 PRECEDING window frame (window size = 3)
//         let rows_frame = create_rows_window_frame(2);
        
//         // Test 1: window_end is within time_range and exactly matches an entry
//         let window_end_exact = time_idxs[3]; // timestamp 3000, should find window start at 1000
//         let window_start = time_entries.get_window_start(&rows_frame, window_end_exact, false);
//         assert!(window_start.is_some(), "Should find window start for exact match");
//         assert_eq!(window_start.unwrap().timestamp, 1000, "Window start should be at timestamp 1000 (3 entries back from 3000)");
//         assert_eq!(window_start.unwrap().batch_id, batch_id, "Window start should have correct batch_id");
//         assert_eq!(window_start.unwrap().row_idx, 1, "Window start should be at row 1");
        
//         // Test 2: window_end is within time_range but doesn't exactly match any entry
//         let window_end_between = TimeIdx {
//             timestamp: 3500, // Between 3000 and 4000
//             pos_idx: 0,
//             batch_id: BatchId::random(),
//             row_idx: 0,
//         };
//         let window_start = time_entries.get_window_start(&rows_frame, window_end_between, false);
//         assert!(window_start.is_some(), "Should find window start even for non-exact match");
//         assert_eq!(window_start.unwrap().timestamp, 2000, "Window start should be at timestamp 2000 (3 entries back from position near 3500)");
        
//         // Test 3: window_end is outside (larger than latest), but window start is inside the range
//         let window_end_outside = TimeIdx {
//             timestamp: 7000, // Beyond latest timestamp (5000)
//             pos_idx: 0,
//             batch_id: BatchId::random(),
//             row_idx: 0,
//         };
//         let window_start = time_entries.get_window_start(&rows_frame, window_end_outside, false);
//         assert!(window_start.is_some(), "Should find window start even when window_end is outside range");
//         assert_eq!(window_start.unwrap().timestamp, 4000, "Window start should be at timestamp 4000 (2 entries back from end of available data since we also count end itself)");
        
//     }

//     #[tokio::test]
//     async fn test_get_window_start_range() {
//         let time_entries = TimeEntries::new();
        
//         // Setup test data: timestamps [1000, 2000, 3000, 4000, 5000]
//         let batch = create_test_batch(vec![1000, 2000, 3000, 4000, 5000], vec![10, 20, 30, 40, 50]);
//         let batch_id = BatchId::random();
//         let time_idxs = time_entries.insert_batch(batch_id, &batch, 0);
        
//         // Create RANGE 1500ms PRECEDING window frame
//         let range_frame = create_range_window_frame(1500);
        
//         // Test 1: window_end is within time_range and exactly matches an entry
//         let window_end_exact = time_idxs[3]; // timestamp 4000
//         let window_start = time_entries.get_window_start(&range_frame, window_end_exact, false);
//         assert!(window_start.is_some(), "Should find window start for exact match");
//         // Window range: [4000-1500, 4000] = [2500, 4000], so start should be at 3000 (first entry >= 2500)
//         assert_eq!(window_start.unwrap().timestamp, 3000, "Window start should be at timestamp 3000");
//         assert_eq!(window_start.unwrap().batch_id, batch_id, "Window start should have correct batch_id");
//         assert_eq!(window_start.unwrap().row_idx, 2, "Window start should be at row 2");
        
//         // Test 2: window_end is within time_range but doesn't exactly match any entry
//         let window_end_between = TimeIdx {
//             timestamp: 3500, // Between 3000 and 4000
//             pos_idx: 0,
//             batch_id: BatchId::random(),
//             row_idx: 0,
//         };
//         let window_start = time_entries.get_window_start(&range_frame, window_end_between, false);
//         assert!(window_start.is_some(), "Should find window start for non-exact match");
//         // Window range: [3500-1500, 3500] = [2000, 3500], so start should be at 2000
//         assert_eq!(window_start.unwrap().timestamp, 2000, "Window start should be at timestamp 2000");
        
//         // Test 3: window_end is outside (larger than latest), but window start is inside the range
//         let window_end_outside = TimeIdx {
//             timestamp: 6000, // Beyond latest timestamp (5000)
//             pos_idx: 0,
//             batch_id: BatchId::random(),
//             row_idx: 0,
//         };
//         let window_start = time_entries.get_window_start(&range_frame, window_end_outside, false);
//         assert!(window_start.is_some(), "Should find window start when window_end is outside but start is inside");
//         // Window range: [6000-1500, 6000] = [4500, 6000], so start should be at 5000 (first entry >= 4500)
//         assert_eq!(window_start.unwrap().timestamp, 5000, "Window start should be at timestamp 5000");
        
//         // Test 4: Both window_end and corresponding start are outside (should return None for RANGE)
//         let window_end_far_outside = TimeIdx {
//             timestamp: 10000, // Beyond latest timestamp (5000)
//             pos_idx: 0,
//             batch_id: BatchId::random(),
//             row_idx: 0,
//         };
//         let window_start = time_entries.get_window_start(&range_frame, window_end_far_outside, false);
//         assert!(window_start.is_none(), "Should return None when both window_end and start are outside range");
        
//         // Test edge case: window_end exactly at boundary
//         let boundary_window_end = TimeIdx {
//             timestamp: 2500, // Exactly 1500ms after first entry (1000)
//             pos_idx: 0,
//             batch_id: BatchId::random(),
//             row_idx: 0,
//         };
//         let window_start = time_entries.get_window_start(&range_frame, boundary_window_end, false);
//         assert!(window_start.is_some(), "Should find window start at boundary");
//         // Window range: [2500-1500, 2500] = [1000, 2500], so start should be at 1000
//         assert_eq!(window_start.unwrap().timestamp, 1000, "Window start should be at timestamp 1000 for boundary case");
//     }
// }