use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::ops::Bound;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{new_null_array, RecordBatch};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::StreamExt;
use indexmap::IndexMap;

use datafusion::scalar::ScalarValue;

use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType};
use crate::runtime::operators::window::aggregates::{split_entries_for_parallelism, Aggregation};
use crate::runtime::operators::window::state::{State, WindowId, WindowsState};
use crate::runtime::operators::window::time_entries::{TimeEntries, TimeIdx};
use crate::runtime::operators::window::AggregatorType;
use crate::runtime::operators::window::window_operator::{
    init, is_entry_late, is_ts_too_late, load_batches, produce_aggregates, stack_concat_results, WindowConfig
};
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::storage::{extract_timestamp, BatchId, Storage};
use tokio_rayon::rayon::ThreadPool;

pub struct WindowRequestOperator {
    base: OperatorBase,
    windows: BTreeMap<WindowId, WindowConfig>,
    state: State,
    ts_column_index: usize,
    parallelize: bool,
    thread_pool: Option<ThreadPool>,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    lateness: Option<i64>,
}

impl fmt::Debug for WindowRequestOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowRequestOperator")
            .field("base", &self.base)
            .field("windows", &self.windows)
            .field("state", &self.state)

            .field("parallelize", &self.parallelize)
            .field("thread_pool", &self.thread_pool)
            .field("output_schema", &self.output_schema)
            .field("input_schema", &self.input_schema)
            .finish()
    }
}

impl WindowRequestOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let window_operator_config = match config.clone() {
            OperatorConfig::WindowConfig(config) => config,
            _ => panic!("Expected WindowRequestConfig, got {:?}", config),
        };

        let (ts_column_index, windows, input_schema, output_schema, thread_pool) = init(&window_operator_config);

        Self {
            base: OperatorBase::new(config, storage),
            windows,
            // TODO pass state as param - should be shared with WindowOperator
            state: State::new(),
            ts_column_index,
            parallelize: window_operator_config.parallelize,
            thread_pool,
            output_schema,
            input_schema,
            lateness: window_operator_config.lateness
        }
    }

    pub fn get_state(&self) -> &State {
        &self.state
    }

    fn create_virtual_entries(
        &self,
        record_batch: &RecordBatch,
        time_entries: &TimeEntries,
    ) -> (Vec<Option<TimeIdx>>, BatchId) {
        let mut virtual_entries = Vec::with_capacity(record_batch.num_rows());
        let last_entry = time_entries.entries.back().map(|entry| *entry);

        let batch_id = BatchId::random(); // virtual batch id

        for row_idx in 0..record_batch.num_rows() {
            let timestamp = extract_timestamp(record_batch.column(self.ts_column_index), row_idx);

            let search_key = TimeIdx {
                timestamp,
                pos_idx: usize::MAX,
                batch_id,
                row_idx: 0,
            };

            if let (Some(lateness_ms), Some(last_entry)) = (self.lateness, last_entry) {
                if is_ts_too_late(timestamp, last_entry, lateness_ms) {
                    virtual_entries.push(None);
                    continue;
                }
            }

            let pos_idx = time_entries
                .entries
                .upper_bound(Bound::Included(&search_key))
                .map(|existing_entry| {
                    if existing_entry.timestamp == timestamp {
                        existing_entry.pos_idx + 1
                    } else {
                        0
                    }
                })
                .unwrap_or(0);

            virtual_entries.push(Some(TimeIdx {
                timestamp,
                pos_idx,
                batch_id,
                row_idx,
            }));
        }

        (virtual_entries, batch_id)
    }

    async fn process_key(&self, key: &Key, windows_state: &WindowsState, record_batch: &RecordBatch) -> RecordBatch {
        let time_entries = &windows_state.time_entries;
        let (virtual_entries, temp_batch_id) = self.create_virtual_entries(record_batch, time_entries);
        
        let last_entry = time_entries.latest_idx();

        let mut orig_positions = HashMap::new();
        let mut aggregations = IndexMap::new();
        
        for (window_id, window_config) in &self.windows {
            let window_frame = window_config.window_expr.get_window_frame();
            let window_state = windows_state.window_states.get(window_id).expect("Window state should exist");
            let aggregator_type = self.windows[window_id].aggregator_type;
            let tiles = window_state.tiles.as_ref();
            let accumulator_state = window_state.accumulator_state.as_ref();
            
            let mut aggs = Vec::new();
            let mut agg_idx = 0;

            let mut plain_agg_entries = Vec::new();
            let mut plain_agg_orig_positions = Vec::new();
            let mut eval_agg_entries = Vec::new();
            let mut eval_agg_orig_positions = Vec::new();

            for i in 0..virtual_entries.len() {
                if let Some(entry) = virtual_entries[i] {
                    let is_late = is_entry_late(entry, last_entry);
                    let aggregator_type = if is_late {
                        // lates are handled as plain
                        AggregatorType::PlainAccumulator
                    } else {
                        aggregator_type
                    };

                    match aggregator_type {
                        AggregatorType::RetractableAccumulator => {
                            let (retracts, _) = time_entries.find_retracts(window_frame, window_state.start_idx, entry);
                            let aggregation = Aggregation::new(
                                vec![entry],
                                aggregator_type,
                                window_config.window_expr.clone(),
                                Some(vec![retracts]),
                                accumulator_state,
                                tiles,
                            );
                            aggs.push(aggregation);
                            orig_positions.insert((*window_id, agg_idx), vec![i]);
                            agg_idx += 1;
                        }
                        AggregatorType::PlainAccumulator => {
                            plain_agg_entries.push(entry);
                            plain_agg_orig_positions.push(i);
                        }
                        AggregatorType::Evaluator => {
                            eval_agg_entries.push(entry);
                            eval_agg_orig_positions.push(i);
                        }
                    }
                }
            }

            let mut pos_idx = 0;
            for entries in split_entries_for_parallelism(&plain_agg_entries) {
                let entries_len = entries.len();
                aggs.push(Aggregation::new(
                    entries,
                    AggregatorType::PlainAccumulator,
                    window_config.window_expr.clone(),
                    None,
                    accumulator_state,
                    tiles,
                ));

                let orig_pos = plain_agg_orig_positions[pos_idx..pos_idx + entries_len].to_vec();
                orig_positions.insert((*window_id, agg_idx), orig_pos);
                pos_idx += entries_len;
                agg_idx += 1;
            }

            pos_idx = 0;
            for entries in split_entries_for_parallelism(&eval_agg_entries) {
                let entries_len = entries.len();
                aggs.push(Aggregation::new(
                    entries,
                    AggregatorType::Evaluator,
                    window_config.window_expr.clone(),
                    None,
                    accumulator_state,
                    tiles,
                ));

                let orig_pos = eval_agg_orig_positions[pos_idx..pos_idx + entries_len].to_vec();
                orig_positions.insert((*window_id, agg_idx), orig_pos);
                pos_idx += entries_len;
                agg_idx += 1;
            }

            aggregations.insert(*window_id, aggs);
        }

        // Load batches
        let mut batches = load_batches(&self.base.storage, key, &aggregations, time_entries).await;
        
        // Add current batch
        batches.insert(temp_batch_id, record_batch.clone());

        // Run aggregation
        let aggregation_results = produce_aggregates(&aggregations, &batches, time_entries, self.thread_pool.as_ref()).await;
        
        let mut aggregated_values = Vec::new();
        
        // Insert aggregated values into original positions
        for (window_id, aggs) in aggregation_results {
            let null_scalar = ScalarValue::Null;
            let mut values: Vec<ScalarValue> = vec![null_scalar.clone(); virtual_entries.len()];
            
            for (agg_idx, (agg_values, _)) in aggs.iter().enumerate() {
                let orig_pos = orig_positions.get(&(window_id, agg_idx)).expect("orig position should exist");
                if agg_values.len() != orig_pos.len() {
                    panic!("Mismatch between aggregate values count ({}) and original positions count ({})", agg_values.len(), orig_pos.len());
                }
                for (i, scalar) in orig_pos.iter().zip(agg_values.iter()) {
                    values[*i] = scalar.clone();
                }
            }
            
            aggregated_values.push(values);
        }
        
        let input_values = get_input_values(&record_batch, &self.input_schema);

        // Stack input value rows and result rows producing single result batch
        stack_concat_results(input_values, aggregated_values, &self.output_schema, &self.input_schema)
    }
}


// Extract input values (values which were in original argument batch, but were not aggregated, e.g keys) 
// for each update row.
// We assume window operator schema is fixed: input columns first, then window columns
fn get_input_values(
    batch: &RecordBatch, 
    input_schema: &SchemaRef
) -> Vec<Vec<ScalarValue>> {
    let mut input_values = Vec::new();
        
    let input_column_count = input_schema.fields().len();
    
    for row_idx in 0..batch.num_rows() {
        let mut row_input_values = Vec::new();
        for col_idx in 0..input_column_count {
            let array = batch.column(col_idx);
            let scalar_value = ScalarValue::try_from_array(array, row_idx)
                .expect("Should be able to extract scalar value");
            row_input_values.push(scalar_value);
        }
        input_values.push(row_input_values);
    }
    input_values
}

#[async_trait]
impl OperatorTrait for WindowRequestOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    fn set_input(&mut self, input: Option<MessageStream>) {
        self.base.set_input(input);
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        let input_stream = self.base.input.as_mut().expect("input stream not set");
        
        match input_stream.next().await {
            Some(message) => {
                let ingest_ts = message.ingest_timestamp();
                let extras = message.get_extras();
                
                match message {
                    Message::Keyed(keyed_message) => {
                        let key = keyed_message.key();
                        
                        // TODO we should get state (copy-read), not take - window request does not mutate state 
                        let windows_state = self.state.take_windows_state(key).await
                            .expect("Window state should exist for request operator");

                        let record_batch = keyed_message.base.record_batch.clone();

                        let result = self.process_key(&key, &windows_state, &record_batch).await;

                        OperatorPollResult::Ready(Message::new(None, result, ingest_ts, extras))
                    }
                    _ => {
                        panic!("Window request operator expects keyed messages only");
                    }
                }
            }
            None => OperatorPollResult::None,
        }
    }
}
