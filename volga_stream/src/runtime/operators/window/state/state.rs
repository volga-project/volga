use std::collections::HashMap;
use std::sync::Arc;
use crossbeam_skiplist::SkipMap;
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::logical_expr::{WindowFrame, WindowFrameBound, WindowFrameUnits};

use crate::storage::storage::{Timestamp};
use crate::common::Key;
use crate::runtime::operators::window::time_index::{TimeIdx};

pub type WindowId = usize;

#[derive(Debug, Clone)]
pub struct WindowState {
    pub accumulator_state: Option<Vec<ScalarValue>>,
    pub start_idx: TimeIdx,
    pub end_idx: TimeIdx,
}

#[derive(Debug)]
pub struct State {
    window_states: DashMap<Key, HashMap<WindowId, WindowState>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            window_states: DashMap::new(),
        }
    }
    
    // move window from its last position to the end of time index
    // pub async fn advance_window(
    //     &mut self, 
    //     window_frame: &Arc<WindowFrame>, 
    //     time_entries: &SkipMap<Timestamp, Vec<TimeIdx>>,
    //     window_state: &WindowState
    // ) -> (Vec<TimeIdx>, Vec<TimeIdx>, TimeIdx, TimeIdx) {
    //     // let window_state = self.get_or_create_window_state(partition_key, window_id).clone();
    //     // let time_entries = time_index.get_time_index(partition_key).expect("Time entries should exist");
        
    //     // the last entry will be new window end
    //     let latest_entry = time_entries.back().expect("Time entries should exist");
    //     let latest_idx = *latest_entry.value().last().expect("Time entries should exist");
    //     let updates = self.find_updates(latest_idx, time_entries, &window_state);

    //     let (retracts,  new_start_idx) = self.find_retracts(window_frame, latest_idx, time_entries, &window_state);
        
    //     // self.update_window_state(partition_key, window_id, new_start_idx, latest_idx);
        
    //     (updates, retracts, new_start_idx, latest_idx)
    // }

    // fn find_updates(
    //     &self,
    //     new_window_end: TimeIdx,
    //     time_entries: &SkipMap<Timestamp, Vec<TimeIdx>>,
    //     window_state: &WindowState
    // ) -> Vec<TimeIdx> {
    //     let mut update_idxs = Vec::new();
    //     let previous_window_end = window_state.end_idx;
        
    //     let mut collecting = false;
    //     let mut found_new_end = false;
        
    //     for entry in time_entries.range(previous_window_end.0..=new_window_end.0) {
    //         for time_idx in entry.value() {
    //             let (ts, batch_id, row_idx) = *time_idx;
                
    //             // Start collecting from previous window end
    //             if ts == previous_window_end.0 && batch_id == previous_window_end.1 && row_idx == previous_window_end.2 {
    //                 collecting = true;
    //             }
                
    //             // Stop collecting when we reach new window end
    //             if ts == new_window_end.0 && batch_id == new_window_end.1 && row_idx == new_window_end.2 {
    //                 found_new_end = true;
    //                 break;
    //             }
                
    //             if collecting {
    //                 update_idxs.push(*time_idx);
    //             }
    //         }
            
    //         if found_new_end {
    //             break;
    //         }
    //     }

    //     update_idxs
    // }

    pub async fn get_window_state(&self, key: &Key, window_id: WindowId) -> Option<WindowState> {
        self.window_states.get(key)
            .and_then(|window_states| window_states.get(&window_id).cloned())
    }

    pub async fn insert_window_state(&self, key: &Key, window_id: WindowId, window_state: WindowState) {
        self.window_states.entry(key.clone()).or_insert(HashMap::new()).insert(window_id, window_state);
    }
}

fn find_retracts(
    window_frame: &Arc<WindowFrame>,
    new_window_end: TimeIdx,
    time_entries: &SkipMap<Timestamp, Vec<TimeIdx>>,
    window_state: &WindowState,
) -> (Vec<TimeIdx>, TimeIdx) {

    if window_frame.units == WindowFrameUnits::Rows {
        // Handle ROWS
        match window_frame.start_bound {
            WindowFrameBound::Preceding(ScalarValue::UInt64(Some(delta))) => {
                let (window_end_timestamp, window_end_batch_id, window_end_row_idx) = new_window_end;
                
                // 1) Find new_window_start TimeIdx by counting backwards from new_window_end
                let mut new_window_start_idx: Option<TimeIdx> = None;
                let mut count = 0;
                let mut found_new_end = false;
                
                // Iterate backwards through timestamps
                for entry in time_entries.range(..=window_end_timestamp).rev() {
                    
                    // Find entries for this timestamp in reverse order
                    for time_idx in entry.value().iter().rev() {
                        let (ts, batch_id, row_idx) = time_idx;
                        
                        // Start counting from the exact position of new_window_end
                        if !found_new_end {
                            if *ts == window_end_timestamp && *batch_id == window_end_batch_id && *row_idx == window_end_row_idx {
                                found_new_end = true;
                                count = 1; // Include current row
                            }
                            continue;
                        }
                        
                        count += 1;
                        if count > delta as usize {
                            new_window_start_idx = Some(*time_idx);
                            break;
                        }
                    }
                    
                    if new_window_start_idx.is_some() {
                        break;
                    }
                }

                if !found_new_end {
                    panic!("new window end not found");
                }
                
                // 2) Get previous window start from state
                let previous_window_start_idx = window_state.start_idx;
                
                // 3) Collect all TimeIdxs between previous_window_start and new_window_start for retraction
                let mut retract_idxs = Vec::new();
                
                let new_start_idx = new_window_start_idx.expect("New window start should exist");
                let mut collecting = false;
                let mut found_new_start = false;
                
                // Iterate forward through timestamps to collect retraction range
                for entry in time_entries.range(previous_window_start_idx.0..=new_start_idx.0) {
                    for time_idx in entry.value() {
                        let (ts, batch_id, row_idx) = *time_idx;
                        
                        // Start collecting from previous window start
                        if ts == previous_window_start_idx.0 && batch_id == previous_window_start_idx.1 && row_idx == previous_window_start_idx.2 {
                            collecting = true;
                        }
                        
                        // Stop collecting when we reach new window start
                        if ts == new_start_idx.0 && batch_id == new_start_idx.1 && row_idx == new_start_idx.2 {
                            found_new_start = true;
                            break;
                        }
                        
                        if collecting {
                            retract_idxs.push(*time_idx);
                        }
                    }
                    
                    if found_new_start {
                        break;
                    }
                }
                
                return (retract_idxs, new_start_idx);
            }
            _ => panic!("Only Preceding start bound is supported for ROWS WindowFrameUnits"),
        }
    } else if window_frame.units == WindowFrameUnits::Range {
        // Handle RANGE
        let (window_end_timestamp, _, _) = new_window_end;
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
        let previous_window_start_timestamp = window_state.start_idx.0;

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


pub fn advance_window_position(window_frame: &Arc<WindowFrame>, window_state: &mut WindowState, time_entries: &SkipMap<Timestamp, Vec<TimeIdx>>) -> Vec<(TimeIdx, Vec<TimeIdx>)> {
    // let mut window_state = match self.windows_state.get_window_state(key, window_id).await {
    //     Some(state) => state,
    //     None => WindowState {
    //         accumulator_state: None,
    //         start_idx: (0, BatchId::nil(), 0),
    //         end_idx: (0, BatchId::nil(), 0),
    //     }
    // };

    // let window_expr = self.windows[&window_id].clone();
    // let window_frame = window_expr.get_window_frame();
    // let time_entries = self.time_index.get_time_index(key).expect("Time entries should exist");

    let latest_entry = time_entries.back().expect("Time entries should exist");
    let latest_idx = *latest_entry.value().last().expect("Time entries should exist");
    let previous_window_end = window_state.end_idx;

    let mut processing = false;
    let mut updates_and_retracts = Vec::new();

    for entry in time_entries.range(previous_window_end.0..=latest_idx.0) {
        for time_idx in entry.value() {
            let (ts, batch_id, row_idx) = *time_idx;
            
            if ts == previous_window_end.0 && batch_id == previous_window_end.1 && row_idx == previous_window_end.2 {
                // found exact window end, start processing new events
                processing = true;
            }

            if processing {
                let (retracts, new_start_idx) = find_retracts(window_frame, *time_idx, time_entries, &window_state);
                updates_and_retracts.push((*time_idx, retracts));
                window_state.start_idx = new_start_idx;
                window_state.end_idx = *time_idx;
            }
        }
    }
    
    updates_and_retracts
}

