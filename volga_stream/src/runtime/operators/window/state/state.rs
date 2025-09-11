use std::collections::HashMap;
use std::sync::Arc;
use arrow::record_batch::RecordBatch;
use datafusion::physical_plan::{WindowExpr, expressions::Column};
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{WindowFrameBound, WindowFrameUnits};
use crossbeam_skiplist::SkipMap;

use crate::storage::storage::{BatchId, RowIdx, Timestamp};
use crate::common::Key;

type TimeIdx = (Timestamp, BatchId, RowIdx); // timestamp, row index within same timestamp
type WindowId = usize;


#[derive(Debug, Clone)]
pub struct WindowState {
    pub expr: Arc<dyn WindowExpr>,
    pub start_idx: TimeIdx,
    pub end_idx: TimeIdx,
}

#[derive(Debug)]
pub struct State {
    time_index: HashMap<Key, SkipMap<Timestamp, Vec<TimeIdx>>>,
    window_exprs: Vec<Arc<dyn WindowExpr>>,
    keyed_window_states: HashMap<Key, Vec<WindowState>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            time_index: HashMap::new(),
            window_exprs: Vec::new(),
            keyed_window_states: HashMap::new(),
        }
    }
    
    pub fn init_windows(&mut self, window_exprs: Vec<Arc<dyn WindowExpr>>) {
        self.window_exprs = window_exprs.clone();
    }

    pub fn update_time_index(
        &mut self,
        partition_key: &Key,
        batch_id: BatchId,
        batch: &RecordBatch,
    ) -> Vec<TimeIdx> {
        assert!(!self.window_exprs.is_empty(), "No window expressions available");

        // Use the first window expression by default
        let window_expr = &self.window_exprs[0];
        
        // Extract timestamps for all rows first
        let timestamps: Vec<TimeIdx> = (0..batch.num_rows())
            .map(|row_idx| (self.get_timestamp_from_row(window_expr, batch, row_idx), batch_id, row_idx))
            .collect();

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
    
    // TODO this assumes timestamps are sorted and increasing since window's last end
    pub async fn advance_window(&mut self, window_id: WindowId, timestamps: Vec<TimeIdx>, partition_key: &Key) -> Vec<(Vec<TimeIdx>, Vec<TimeIdx>)> {
        let mut updates_and_retracts = Vec::new();

        if !self.keyed_window_states.contains_key(partition_key) {
            let window_states = self.window_exprs.iter().map(|expr| {
                WindowState {
                    expr: expr.clone(),
                    start_idx: (0, BatchId::nil(), 0),
                    end_idx: (0, BatchId::nil(), 0),
                }
            }).collect();
            self.keyed_window_states.insert(partition_key.clone(), window_states);
        }
        
        for time_idx in timestamps {
            // 1) Find retract row idxs
            let window_expr = &self.window_exprs[window_id];
            let (retract_time_idxs, current_start_idx) = self.find_retract_range(window_expr, time_idx.clone(), &partition_key, window_id);
            
            // 2) Update window state with new boundaries
            {
                let window_states = self.keyed_window_states.get_mut(partition_key).expect("Window states should exist");
                let window_state = &mut window_states[window_id];
                window_state.start_idx = current_start_idx;
                window_state.end_idx = time_idx.clone();
            }
            
            updates_and_retracts.push((vec![time_idx.clone()], retract_time_idxs));
        }
        
        updates_and_retracts
    }

    fn find_retract_range(
        &self,
        window_expr: &Arc<dyn WindowExpr>,
        current_window_end: TimeIdx,
        partition_key: &Key,
        window_id: WindowId,
    ) -> (Vec<TimeIdx>, TimeIdx) {
        let window_frame = window_expr.get_window_frame();

        let time_entries = self.time_index.get(partition_key).expect("Time entries should exist");

        if window_frame.units == WindowFrameUnits::Rows {
            // Handle ROWS
            match window_frame.start_bound {
                WindowFrameBound::Preceding(ScalarValue::UInt64(Some(delta))) => {
                    let (window_end_timestamp, window_end_batch_id, window_end_row_idx) = current_window_end;
                    
                    // 1) Find current_window_start TimeIdx by counting backwards from current_window_end
                    let mut current_window_start_idx: Option<TimeIdx> = None;
                    let mut count = 0;
                    let mut found_current_end = false;
                    
                    // Iterate backwards through timestamps
                    for entry in time_entries.range(..=window_end_timestamp).rev() {
                        
                        // Find entries for this timestamp in reverse order
                        for time_idx in entry.value().iter().rev() {
                            let (ts, batch_id, row_idx) = time_idx;
                            
                            // Start counting from the exact position of current_window_end
                            if !found_current_end {
                                if *ts == window_end_timestamp && *batch_id == window_end_batch_id && *row_idx == window_end_row_idx {
                                    found_current_end = true;
                                    count = 1; // Include current row
                                }
                                continue;
                            }
                            
                            count += 1;
                            if count > delta as usize {
                                current_window_start_idx = Some(*time_idx);
                                break;
                            }
                        }
                        
                        if current_window_start_idx.is_some() {
                            break;
                        }
                    }

                    if !found_current_end {
                        panic!("Current window end not found");
                    }
                    
                    // 2) Get previous window start from state
                    let previous_window_start_idx = self.keyed_window_states.get(partition_key)
                        .expect("Window states should exist")
                        .get(window_id)
                        .expect("Window state should exist")
                        .start_idx;
                    
                    // 3) Collect all TimeIdxs between previous_window_start and current_window_start for retraction
                    let mut retract_idxs = Vec::new();
                    
                    let current_start_idx = current_window_start_idx.expect("Current window start should exist");
                    let mut collecting = false;
                    let mut found_current_start = false;
                    
                    // Iterate forward through timestamps to collect retraction range
                    for entry in time_entries.range(previous_window_start_idx.0..=current_start_idx.0) {
                        for time_idx in entry.value() {
                            let (ts, batch_id, row_idx) = *time_idx;
                            
                            // Start collecting from previous window start
                            if ts == previous_window_start_idx.0 && batch_id == previous_window_start_idx.1 && row_idx == previous_window_start_idx.2 {
                                collecting = true;
                            }
                            
                            // Stop collecting when we reach current window start
                            if ts == current_start_idx.0 && batch_id == current_start_idx.1 && row_idx == current_start_idx.2 {
                                found_current_start = true;
                                break;
                            }
                            
                            if collecting {
                                retract_idxs.push(*time_idx);
                            }
                        }
                        
                        if found_current_start {
                            break;
                        }
                    }
                    
                    return (retract_idxs, current_start_idx);
                }
                _ => panic!("Only Preceding start bound is supported for ROWS WindowFrameUnits"),
            }
        } else if window_frame.units == WindowFrameUnits::Range {
            // Handle RANGE
            let (window_end_timestamp, _, _) = current_window_end;
            let window_start_timestamp = match &window_frame.start_bound {
                WindowFrameBound::Preceding(delta) => {
                    if window_frame.start_bound.is_unbounded() || delta.is_null() {
                        panic!("Can not retract UNBOUNDED PRECEDING");
                    }
                    
                    let delta_u64 = u64::try_from(delta.clone())
                        .expect("Should be able to convert delta to u64");
                        
                    window_end_timestamp.saturating_sub(delta_u64)
                },
                _ => panic!("Only Preceding start bound is supported"),
            };
            
            let mut retract_idxs = Vec::new();
            
            // Get previous window start from window state
            let previous_window_start_timestamp = self.keyed_window_states.get(partition_key)
                .expect("Window states should exist")
                .get(window_id)
                .expect("Window State should exist")
                .start_idx.0;

            for entry in time_entries.range(previous_window_start_timestamp..window_start_timestamp) {
                for (_, batch_id, row_idx) in entry.value() {
                    retract_idxs.push((*entry.key(), batch_id.clone(), *row_idx));
                }
            }

            // construct window start idx from raw timestamp
            let window_start_idx = time_entries.get(&window_start_timestamp).expect("Entry should exist").value()[0];
            
            return (retract_idxs, window_start_idx)
        } else {
            panic!("Unsupported WindowFrame type: only ROW and RANGE are supported");
        }
        
    }

    fn get_timestamp_from_row(
        &self,
        window_expr: &Arc<dyn WindowExpr>,
        batch: &RecordBatch,
        row_idx: usize,
    ) -> Timestamp {
        let order_by = window_expr.order_by();
        
        assert_eq!(
            order_by.len(), 
            1, 
            "Expected exactly one ORDER BY expression, found {}", 
            order_by.len()
        );
        
        let sort_expr = &order_by[0];
        let column = sort_expr.expr.as_any().downcast_ref::<Column>()
            .expect("Expected Column expression in ORDER BY");
            
        let column_array = batch.column(column.index());
        let scalar_value = ScalarValue::try_from_array(column_array, row_idx)
            .expect("Should be able to extract scalar value from array");
        
        assert!(
            !scalar_value.is_null(),
            "NULL value found in ORDER BY column '{}' at row {}. \
             Streaming windows require non-null ORDER BY values for correct \
             temporal semantics (watermarks, window assignment).",
            column.name(), row_idx
        );
        
        u64::try_from(scalar_value)
            .expect("Should be able to convert scalar value to u64")
    }
}