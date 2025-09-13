use std::collections::HashMap;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{ArrayRef, UInt64Array};
use async_trait::async_trait;

use crossbeam_skiplist::SkipMap;
use datafusion::logical_expr::{Accumulator, WindowFrame};
use datafusion::physical_expr::window::SlidingAggregateWindowExpr;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::WindowExpr;
use datafusion::scalar::ScalarValue;
use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType};
use crate::runtime::operators::window::state::state::{advance_window_position, State, WindowId, WindowState};
use crate::runtime::operators::window::time_index::{TimeIdx, TimeIndex};
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::storage::{BatchId, Storage, Timestamp};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExecutionMode {
    EventBased, // all events are processed immediately, late events are handled with best effort
    WatermarkBased, // events are buffered until watermark is reached, late events are dropped
}

#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub execution_mode: ExecutionMode,
}

impl WindowConfig {
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {    
        Self {
            window_exec,
            execution_mode: ExecutionMode::EventBased,
        }
    }
}

#[derive(Debug)]
pub struct WindowOperator {
    base: OperatorBase,
    windows: HashMap<WindowId, Arc<dyn WindowExpr>>,
    windows_state: State,
    time_index: TimeIndex,
    ts_column_index: usize,
    keys_to_process: Vec<Key>,
    execution_mode: ExecutionMode,
}

impl WindowOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let window_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        let ts_column_index = window_config.window_exec.window_expr()[0].order_by()[0].expr.as_any().downcast_ref::<Column>().expect("Expected Column expression in ORDER BY").index();

        // let window_ids = window_config.window_exec.window_expr().iter().enumerate().map(|(i, _)| i).collect::<Vec<_>>();
        let mut windows = HashMap::new();
        for (window_id, window_expr) in window_config.window_exec.window_expr().iter().enumerate() {
            windows.insert(window_id, window_expr.clone());
        }
        Self {
            base: OperatorBase::new(config, storage),
            windows,
            windows_state: State::new(),
            time_index: TimeIndex::new(),
            ts_column_index,
            keys_to_process: Vec::new(),
            execution_mode: window_config.execution_mode,
        }
    }

    async fn advance_window(&self, key: &Key, window_id: WindowId) -> Vec<ScalarValue> {
        let mut window_state = match self.windows_state.get_window_state(key, window_id).await {
            Some(state) => state,
            None => WindowState {
                accumulator_state: None,
                start_idx: (0, BatchId::nil(), 0),
                end_idx: (0, BatchId::nil(), 0),
            }
        };
    
        let window_expr = self.windows[&window_id].clone();
        let window_frame = window_expr.get_window_frame();
        let time_entries = self.time_index.get_time_index(key).expect("Time entries should exist");
        
        let updates_and_retracts = advance_window_position(window_frame, &mut window_state, time_entries);

        let mut batches_to_load = std::collections::BTreeSet::new();
        for (update_idx, retracts) in &updates_and_retracts {
            batches_to_load.insert(update_idx.1.clone());
            for retract_idx in retracts {
                batches_to_load.insert(retract_idx.1.clone());
            }
        }

        // TODO we should make an iterator here with fixed loaded memory and pre-loading
        let batches = self.base.storage.load_batches(batches_to_load.into_iter().collect(), key).await;

        let mut accumulator = create_sliding_accumulator(&window_expr);
        if let Some(accumulator_state) = window_state.accumulator_state {
            let state_arrays: Vec<ArrayRef> = accumulator_state
                .iter()
                .map(|sv| sv.to_array_of_size(1))
                .collect::<Result<Vec<_>, _>>()
                .expect("Should be able to convert scalar values to arrays");
            
            accumulator.merge_batch(&state_arrays).expect("Should be able to merge accumulator state");
        }

        let mut results = Vec::new();

        for (update_idx, retract_idxs) in &updates_and_retracts {
            let update_batch = batches.get(&update_idx.1).expect("Update batch should exist");
            // Extract single row from update batch using the row index
            let update_row_batch = {
                let indices = UInt64Array::from(vec![update_idx.2 as u64]);
                arrow::compute::take_record_batch(update_batch, &indices)
                    .expect("Should be able to take row from batch")
            };

            let update_args = window_expr.evaluate_args(&update_row_batch)
                .expect("Should be able to evaluate window args");

            accumulator.update_batch(&update_args).expect("Should be able to update accumulator");

            // Handle retractions
            if !retract_idxs.is_empty() {
                // Extract retract rows in original order
                let mut retract_batches = Vec::new();
                for retract_idx in retract_idxs {
                    let batch = batches.get(&retract_idx.1).expect("Retract batch should exist");
                    let indices_array = UInt64Array::from(vec![retract_idx.2 as u64]);
                    let retract_batch = arrow::compute::take_record_batch(batch, &indices_array)
                        .expect("Should be able to take row from batch");
                    retract_batches.push(retract_batch);
                }

                // Concatenate retract batches if multiple, otherwise use single batch
                let final_retract_batch = if retract_batches.len() == 1 {
                    retract_batches.into_iter().next().unwrap()
                } else {
                    let schema = retract_batches[0].schema();
                    arrow::compute::concat_batches(&schema, &retract_batches)
                        .expect("Should be able to concat retract batches")
                };

                let retract_args = window_expr.evaluate_args(&final_retract_batch)
                    .expect("Should be able to evaluate retract args");

                accumulator.retract_batch(&retract_args).expect("Should be able to retract from accumulator");
            }

            let result = accumulator.evaluate().expect("Should be able to evaluate accumulator");
            results.push(result);
        }

        // update final accumulator state
        window_state.accumulator_state = Some(accumulator.state().expect("Should be able to get accumulator state"));

        // update window state
        self.windows_state.insert_window_state(key, window_id, window_state).await;
        
        results
    }

    async fn advance_windows(&self, key: &Key) -> Vec<Vec<ScalarValue>> {
        let mut results = Vec::new();
        // TODO parallelize this
        for window_id in self.windows.keys() {
            results.push(self.advance_window(key, *window_id).await);
        }
        results
    }
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    async fn process_message(&mut self, message: Message) -> Option<Vec<Message>> {
        let storage = self.base.storage.clone();
        let partition_key = message.key().expect("Window Operator expects KeyedMessage");

        // TODO based one execution mode (event vs watermark based) we may need to drop late events

        // TODO calculate pre-aggregates
        
        let batches = storage.append_records(message.record_batch().clone(), partition_key, self.ts_column_index).await;
        for (batch_id, batch) in batches {
            self.time_index.update_time_index(partition_key, batch_id, &batch, self.ts_column_index);
        }
        
        if self.execution_mode == ExecutionMode::WatermarkBased {
            // buffer for processing on watermark
            self.keys_to_process.push(partition_key.clone());
        } else {
            // immidiate processing for event based mode
        }

        None
    }

    async fn process_watermark(&mut self, _watermark: u64) -> Option<Vec<Message>> {
        if self.execution_mode == ExecutionMode::EventBased {
            panic!("EventBased execution mode does not support watermark processing");
        }
        None
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}

fn create_sliding_accumulator(window_expr: &Arc<dyn WindowExpr>) -> Box<dyn Accumulator> {
    let aggregate_expr = window_expr.as_any()
        .downcast_ref::<SlidingAggregateWindowExpr>()
        .expect("Only SlidingAggregateWindowExpr is supported");
    
    let accumulator = aggregate_expr.get_aggregate_expr().create_sliding_accumulator()
        .expect("Should be able to create accumulator");

    if !accumulator.supports_retract_batch() {
        panic!("Accumulator {:?} does not support retract batch", accumulator);
    }

    accumulator
}