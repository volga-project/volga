use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::scalar::ScalarValue;

use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::operators::operator::{
    MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType,
};
use crate::runtime::operators::window::aggregates::VirtualPoint;
use crate::runtime::operators::window::evaluate::evaluate_range_points;
use crate::runtime::operators::window::frame_utils::require_range_frame;
use crate::runtime::operators::window::config::{BuiltWindows, WindowConfig};
use crate::runtime::operators::window::evaluate::assemble_window_batch;
use crate::runtime::operators::window::store::{StateNamespace, WindowStateStore};
use crate::runtime::operators::window::window_operator::WindowOperatorConfig;
use crate::runtime::operators::window::window_operator_state::WindowId;
use crate::runtime::operators::window::window_tuning::WindowOperatorSpec;
use crate::runtime::operators::window::TileConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::{InMemSortedKV, SortedKV};

#[derive(Debug, Clone)]
pub struct WindowRequestOperatorConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub tiling_configs: Vec<Option<TileConfig>>,
    pub spec: WindowOperatorSpec,
    /// `EXCLUDE CURRENT ROW`: lookup time only (no request-row args). SQL wiring still TODO.
    pub exclude_current_row: bool,
}

impl WindowRequestOperatorConfig {
    pub fn from_window_operator_config(window_operator_config: WindowOperatorConfig) -> Self {
        Self {
            window_exec: window_operator_config.window_exec,
            tiling_configs: window_operator_config.tiling_configs,
            spec: window_operator_config.spec,
            exclude_current_row: false,
        }
    }
}

pub struct WindowRequestOperator {
    base: OperatorBase,
    window_configs: BTreeMap<WindowId, WindowConfig>,
    store: Option<WindowStateStore>,
    ts_column_index: usize,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
}

impl fmt::Debug for WindowRequestOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowRequestOperator")
            .field("base", &self.base)
            .field("windows", &self.window_configs)
            .finish()
    }
}

impl WindowRequestOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let window_request_operator_config = match config.clone() {
            OperatorConfig::WindowRequestConfig(config) => config,
            _ => panic!("Expected WindowRequestConfig, got {:?}", config),
        };

        let built = BuiltWindows::for_wro(
            &window_request_operator_config.window_exec,
            &window_request_operator_config.tiling_configs,
            &window_request_operator_config.spec,
            window_request_operator_config.exclude_current_row,
        );

        for w in built.windows.values() {
            require_range_frame(w.window_expr.get_window_frame());
        }

        Self {
            base: OperatorBase::new(config),
            window_configs: built.windows,
            store: None,
            ts_column_index: built.ts_column_index,
            output_schema: built.output_schema,
            input_schema: built.input_schema,
        }
    }

    async fn process_key(&self, key: &Key, record_batch: &RecordBatch) -> RecordBatch {
        let store = self.store.as_ref().expect("store");

        // No lateness filter: retention is WO prune only. Answer any request point
        // still covered by retained state (empty/partial if already GC'd).
        if record_batch.num_rows() == 0 {
            return RecordBatch::new_empty(self.output_schema.clone());
        }

        let ts_array = record_batch
            .column(self.ts_column_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Timestamp column");

        let kept_rows: Vec<usize> = (0..record_batch.num_rows()).collect();
        // VirtualPoint is global per key lookup; args from first window expr (assumes
        // compatible inputs across windows — see VirtualPoint docs).
        let window_cfg = self.window_configs.values().next().expect("window");
        let exclude = window_cfg.exclude_current_row.unwrap_or(false);
        let mut points = Vec::with_capacity(kept_rows.len());
        for &row in &kept_rows {
            let ts = ts_array.value(row);
            let args = if exclude {
                None
            } else {
                let one = record_batch.slice(row, 1);
                let evaluated = window_cfg
                    .window_expr
                    .evaluate_args(&one)
                    .expect("evaluate_args");
                Some(Arc::new(evaluated))
            };
            points.push(VirtualPoint { ts, args });
        }

        let aggregated = evaluate_range_points(
            store,
            key,
            &self.window_configs,
            &points,
            exclude,
        )
        .await
        .expect("evaluate");

        let input_values = get_input_values_for_rows(record_batch, &self.input_schema, &kept_rows);
        assemble_window_batch(
            input_values,
            aggregated,
            &self.output_schema,
            &self.input_schema,
        )
    }
}

fn get_input_values_for_rows(
    batch: &RecordBatch,
    input_schema: &SchemaRef,
    rows: &[usize],
) -> Vec<Vec<ScalarValue>> {
    let mut input_values = Vec::new();
    let input_column_count = input_schema.fields().len();
    for &row_idx in rows {
        let mut row_input_values = Vec::new();
        for col_idx in 0..input_column_count {
            let array = batch.column(col_idx);
            let scalar_value = ScalarValue::try_from_array(array, row_idx)
                .expect("extract scalar");
            row_input_values.push(scalar_value);
        }
        input_values.push(row_input_values);
    }
    input_values
}

#[async_trait]
impl OperatorTrait for WindowRequestOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;
        let kv: Arc<dyn SortedKV> = context
            .sorted_kv()
            .unwrap_or_else(|| Arc::new(InMemSortedKV::new()) as Arc<dyn SortedKV>);
        let ns = context
            .window_state_namespace()
            .unwrap_or_else(|| StateNamespace::new(b"window_state"));
        // Own client handle (clone shares backend) — no peer WO local state.
        self.store = Some(WindowStateStore::new(kv, ns));
        Ok(())
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

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        if let Some(msg) = self.base.pop_pending_output() {
            return OperatorPollResult::Ready(msg);
        }

        match self.base.next_input().await {
            Some(message) => match message {
                Message::Keyed(keyed) => {
                    let key = keyed.key();
                    let out = self
                        .process_key(key, &keyed.base.record_batch)
                        .await;
                    OperatorPollResult::Ready(Message::new(None, out, None, None))
                }
                Message::Watermark(w) => OperatorPollResult::Ready(Message::Watermark(w)),
                other => panic!("WindowRequestOperator unexpected message: {:?}", other),
            },
            None => OperatorPollResult::None,
        }
    }
}
