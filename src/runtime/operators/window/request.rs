use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::{RecordBatch, TimestampMillisecondArray};
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::PhysicalExpr;
use datafusion::scalar::ScalarValue;
use futures::{stream, StreamExt};

use crate::common::message::Message;
use crate::common::Key;
use crate::runtime::functions::key_by::pack::split_by_key_exprs;
use crate::runtime::operators::operator::{
    OperatorBase, OperatorConfig, OperatorTrait, OperatorType, Output, StreamOperator,
};
use crate::runtime::operators::window::config::{BuiltWindows, WindowConfig};
use crate::runtime::operators::window::eval::{assemble_window_batch, evaluate_points};
use crate::runtime::operators::window::frame_utils::require_range_frame;
use crate::runtime::operators::window::model::WindowId;
use crate::runtime::operators::window::operator::WindowOperatorConfig;
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::store::{
    open_window_request_store, PartitionKey, StateNamespace, WindowRequestStore,
};
use crate::runtime::operators::window::TileConfig;
use crate::runtime::operators::window::PARTITION_IO_CONCURRENCY;
use crate::runtime::runtime_context::RuntimeContext;

#[derive(Debug, Clone)]
pub struct WindowRequestOperatorConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub tiling_configs: Vec<Option<TileConfig>>,
    pub spec: WindowSpec,
    /// `EXCLUDE CURRENT ROW`: lookup time only (no request-row args). SQL wiring still TODO.
    pub exclude_current_row: bool,
    pub state_owner_operator_id: Option<String>,
}

impl WindowRequestOperatorConfig {
    pub fn from_window_operator_config(window_operator_config: WindowOperatorConfig) -> Self {
        Self {
            window_exec: window_operator_config.window_exec,
            tiling_configs: window_operator_config.tiling_configs,
            spec: window_operator_config.spec,
            exclude_current_row: false,
            state_owner_operator_id: None,
        }
    }
}

pub struct WindowRequestOperator {
    base: OperatorBase,
    window_configs: BTreeMap<WindowId, WindowConfig>,
    store: Option<Arc<dyn WindowRequestStore>>,
    namespace: Option<StateNamespace>,
    state_owner_operator_id: Option<String>,
    ts_column_index: usize,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
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
            namespace: None,
            state_owner_operator_id: window_request_operator_config.state_owner_operator_id,
            ts_column_index: built.ts_column_index,
            partition_by: window_request_operator_config.window_exec.window_expr()[0]
                .partition_by()
                .to_vec(),
            output_schema: built.output_schema,
            input_schema: built.input_schema,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_state_with_store_and_ns(
        &mut self,
        store: Arc<dyn WindowRequestStore>,
        namespace: StateNamespace,
    ) {
        assert!(
            self.store.is_none() && self.namespace.is_none(),
            "window request state is already configured"
        );
        self.store = Some(store);
        self.namespace = Some(namespace);
    }

    async fn process_key(&self, key: &Key, record_batch: &RecordBatch) -> RecordBatch {
        let store = self.store.as_ref().expect("store");
        let partition = PartitionKey::new(self.namespace.as_ref().expect("namespace"), key);

        // No lateness filter. Answer from whatever state the backend still retains.
        if record_batch.num_rows() == 0 {
            return RecordBatch::new_empty(self.output_schema.clone());
        }

        let ts_array = record_batch
            .column(self.ts_column_index)
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .expect("Timestamp column");

        let n = record_batch.num_rows();
        let mut point_timestamps = Vec::with_capacity(n);
        for row in 0..n {
            point_timestamps.push(ts_array.value(row));
        }

        let aggregated = evaluate_points(
            store.as_ref(),
            &partition,
            &self.window_configs,
            &point_timestamps,
            record_batch,
            self.ts_column_index,
        )
        .await
        .expect("evaluate");

        let input_values = get_input_values(record_batch, &self.input_schema);
        assemble_window_batch(
            input_values,
            aggregated,
            &self.output_schema,
            &self.input_schema,
        )
    }

    async fn process_groups(&self, groups: Vec<(Key, RecordBatch)>) -> RecordBatch {
        let mut batches: Vec<(usize, RecordBatch)> = stream::iter(groups.into_iter().enumerate())
            .map(|(i, (key, payload))| async move {
                (i, self.process_key(&key, &payload).await)
            })
            .buffer_unordered(PARTITION_IO_CONCURRENCY)
            .collect()
            .await;
        batches.sort_by_key(|(i, _)| *i);
        let batches: Vec<RecordBatch> = batches.into_iter().map(|(_, batch)| batch).collect();
        if batches.len() == 1 {
            batches.into_iter().next().unwrap()
        } else {
            arrow::compute::concat_batches(&self.output_schema, &batches)
                .expect("concat WRO batches")
        }
    }
}

fn get_input_values(batch: &RecordBatch, input_schema: &SchemaRef) -> Vec<Vec<ScalarValue>> {
    let mut input_values = Vec::with_capacity(batch.num_rows());
    let input_column_count = input_schema.fields().len();
    for row_idx in 0..batch.num_rows() {
        let mut row_input_values = Vec::new();
        for col_idx in 0..input_column_count {
            let array = batch.column(col_idx);
            let scalar_value = ScalarValue::try_from_array(array, row_idx).expect("extract scalar");
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
        if self.store.is_none() {
            let request_store = context
                .request_store()
                .expect("request store must be configured for WindowRequestOperator");
            self.store = Some(open_window_request_store(request_store).await?);
        }
        if self.namespace.is_none() {
            let owner_operator_id = self
                .state_owner_operator_id
                .as_deref()
                .expect("state owner operator id must be configured for WindowRequestOperator");
            self.namespace = Some(StateNamespace::for_operator_task(
                context
                    .pipeline_id()
                    .expect("pipeline id must be configured for WindowRequestOperator"),
                owner_operator_id,
                context.task_index(),
            ));
        }
        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        self.base.close().await
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }

    fn operator_config(&self) -> &OperatorConfig {
        self.base.operator_config()
    }
}

#[async_trait]
impl StreamOperator for WindowRequestOperator {
    async fn process_data(&mut self, data: Vec<Message>, out: &mut dyn Output) -> Result<()> {
        for message in data {
            let Message::Regular(base) = message else {
                panic!("window request ingest expects data messages, got {message:?}");
            };
            let groups = split_by_key_exprs(&base.record_batch, &self.partition_by);
            if groups.is_empty() {
                out.emit(Message::new(
                    None,
                    RecordBatch::new_empty(self.output_schema.clone()),
                    None,
                    None,
                ))
                .await?;
                continue;
            }
            let batch = self.process_groups(groups).await;
            out.emit(Message::new(None, batch, None, None)).await?;
        }
        Ok(())
    }
}
