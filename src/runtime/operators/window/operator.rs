use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::compute::concat_batches;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::{stream, StreamExt, TryStreamExt};

use crate::common::key::Key;
use crate::common::message::Message;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::functions::key_by::pack::split_by_key_exprs;
use crate::runtime::checkpoint::{SerializedCheckpoint, SerializedRestore};
use crate::runtime::operators::operator::{
    MessageStream, NextInputs, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait,
    OperatorType,
};
use crate::runtime::operators::window::config::{BuiltWindows, WindowConfig};
use crate::runtime::operators::window::eval::advance_key;
use crate::runtime::operators::window::frame_utils::{get_window_length_ms, require_range_frame};
use crate::runtime::metrics::MetricsLabels;
use crate::runtime::operators::window::metrics;
use crate::runtime::operators::window::model::{Cursor, WindowId};
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::state::{WindowOperatorState, WindowStateSnapshot};
use crate::runtime::operators::window::store::{open_window_operator_store, StateNamespace};
use crate::runtime::operators::window::TileConfig;
use crate::runtime::operators::window::PARTITION_IO_CONCURRENCY;
use crate::runtime::observability::TaskMetadata;
use crate::runtime::runtime_context::RuntimeContext;
use crate::runtime::state::{OperatorTaskState, StateRegistry};
use crate::runtime::VertexId;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use datafusion::physical_plan::PhysicalExpr;

#[cfg(test)]
use crate::runtime::operators::window::store::WindowOperatorStore;

pub const TASK_METADATA_ROWS_ACCEPTED: &str = "window_rows_accepted";
pub const TASK_METADATA_ROWS_DROPPED_LATE: &str = "window_rows_dropped_late";

/// One ingest step: take ready data inputs until this many records.
const INGEST_MAX_RECORDS: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowOutputMode {
    Emit,
    StateOnly,
}

#[derive(Debug, Clone)]
pub struct WindowOperatorConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub output_mode: WindowOutputMode,
    /// Per-window tiling overrides (index-aligned with window exprs).
    /// Gaps filled from `spec.tiling` at operator construction.
    pub tiling_configs: Vec<Option<TileConfig>>,
    pub spec: WindowSpec,
}

impl WindowOperatorConfig {
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {
        Self {
            window_exec,
            output_mode: WindowOutputMode::Emit,
            tiling_configs: Vec::new(),
            spec: WindowSpec::default(),
        }
    }

    pub fn set_spec(&mut self, spec: WindowSpec) -> &mut Self {
        self.spec = spec;
        self
    }
}

pub struct WindowOperator {
    base: OperatorBase,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    state: Option<Arc<WindowOperatorState>>,
    output_mode: WindowOutputMode,
    /// Retention padding behind `max_window_length_ms` (see `WindowSpec.lateness`).
    lateness_ms: i64,
    max_window_length_ms: i64,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    ts_column_index: usize,
    partition_by: Vec<Arc<dyn PhysicalExpr>>,
    task_metadata: TaskMetadata,
    state_registry: Option<Arc<StateRegistry>>,
    vertex_id: Option<VertexId>,
    /// Worker-scoped labels for hot-path metrics (task id is `vertex_id`).
    metrics_labels: Option<MetricsLabels>,
}

impl fmt::Debug for WindowOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowOperator")
            .field("base", &self.base)
            .field("windows", &self.window_configs)
            .field("output_mode", &self.output_mode)
            .finish()
    }
}

impl WindowOperator {
    pub fn new(config: OperatorConfig) -> Self {
        let window_operator_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        let built = BuiltWindows::for_wo(
            &window_operator_config.window_exec,
            &window_operator_config.tiling_configs,
            &window_operator_config.spec,
        );

        for w in built.windows.values() {
            require_range_frame(w.window_expr.get_window_frame());
        }

        let windows = Arc::new(built.windows);
        let max_window_length_ms = windows
            .values()
            .map(|window| get_window_length_ms(window.window_expr.get_window_frame()))
            .max()
            .unwrap_or(0);
        Self {
            base: OperatorBase::new(config),
            window_configs: windows,
            state: None,
            output_mode: window_operator_config.output_mode,
            lateness_ms: window_operator_config.spec.lateness,
            max_window_length_ms,
            output_schema: built.output_schema,
            input_schema: built.input_schema,
            ts_column_index: built.ts_column_index,
            partition_by: window_operator_config.window_exec.window_expr()[0]
                .partition_by()
                .to_vec(),
            task_metadata: TaskMetadata::default(),
            state_registry: None,
            vertex_id: None,
            metrics_labels: None,
        }
    }

    fn metrics_target(&self) -> Option<(&str, &MetricsLabels)> {
        match (&self.vertex_id, &self.metrics_labels) {
            (Some(task_id), Some(labels)) => Some((task_id.as_ref(), labels)),
            _ => None,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_state_with_store_and_ns(
        &mut self,
        store: Arc<dyn WindowOperatorStore>,
        namespace: StateNamespace,
    ) {
        assert!(self.state.is_none(), "window state is already configured");
        self.state = Some(Arc::new(WindowOperatorState::new(
            store,
            namespace,
            Arc::from("test-task"),
            self.ts_column_index,
            self.window_configs.clone(),
            self.lateness_ms,
            self.max_window_length_ms,
        )));
    }

    #[cfg(test)]
    pub(crate) fn task_state(&self) -> Arc<WindowOperatorState> {
        self.state_ref().clone()
    }

    fn state_ref(&self) -> &Arc<WindowOperatorState> {
        self.state
            .as_ref()
            .expect("WindowOperator must be opened first")
    }

    async fn process_due(&self, through: Cursor) -> RecordBatch {
        const PROCESS_DUE_CONCURRENCY: usize = 8;
        let state = self.state_ref();
        let after = state
            .watermark_frontier()
            .map(|timestamp| Cursor::new(timestamp, u64::MAX));
        let mut pages = state
            .store()
            .stream_due(state.namespace(), after, through);
        let mut batches = Vec::new();
        while let Some(work) = pages
            .try_next()
            .await
            .expect("stream due window triggers")
        {
            let page = stream::iter(work)
                .map(|work| {
                    advance_key(
                        state.store(),
                        work,
                        self.window_configs.as_ref(),
                        self.ts_column_index,
                        &self.output_schema,
                        &self.input_schema,
                    )
                })
                .buffered(PROCESS_DUE_CONCURRENCY)
                .try_collect::<Vec<_>>()
                .await
                .expect("advance due window triggers");
            batches.extend(page);
        }
        if batches.is_empty() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }
        arrow::compute::concat_batches(&self.output_schema, &batches).expect("concat")
    }

    async fn ingest_data(&mut self, messages: Vec<Message>) -> OperatorPollResult {
        let ingest_started = Instant::now();
        let emit = self.output_mode == WindowOutputMode::Emit;
        let mut by_key: HashMap<Key, Vec<RecordBatch>> = HashMap::new();
        let mut input_rows = 0usize;
        for message in messages {
            let Message::Regular(base) = message else {
                panic!("window ingest expects data messages, got {message:?}");
            };
            input_rows += base.record_batch.num_rows();
            for (key, payload) in split_by_key_exprs(&base.record_batch, &self.partition_by) {
                by_key.entry(key).or_default().push(payload);
            }
        }
        let groups = by_key.into_iter().map(|(key, batches)| {
            let batch = if batches.len() == 1 {
                batches.into_iter().next().unwrap()
            } else {
                concat_batches(&batches[0].schema(), &batches).expect("concat same-key rows")
            };
            (key, batch)
        });
        let state = self.state_ref().clone();
        let dropped: usize = stream::iter(groups)
            .map(|(key, payload)| {
                let state = state.clone();
                async move { state.insert_batch(&key, payload, emit).await }
            })
            .buffer_unordered(PARTITION_IO_CONCURRENCY)
            .fold(0usize, |acc, n| async move { acc + n })
            .await;
        debug_assert!(dropped <= input_rows);
        self.task_metadata.increment_u64(
            TASK_METADATA_ROWS_ACCEPTED,
            (input_rows - dropped) as u64,
        );
        self.task_metadata
            .increment_u64(TASK_METADATA_ROWS_DROPPED_LATE, dropped as u64);
        if let Some((task_id, labels)) = self.metrics_target() {
            metrics::record_ingest_ms(
                task_id,
                labels,
                ingest_started.elapsed().as_secs_f64() * 1000.0,
            );
            metrics::add_late_dropped(task_id, labels, dropped as u64);
        }
        OperatorPollResult::Continue
    }

    async fn handle_control(&mut self, message: Message) -> OperatorPollResult {
        match message {
            Message::Watermark(watermark) => {
                let wm_ts = if watermark.watermark_value == MAX_WATERMARK_VALUE {
                    i64::MAX
                } else {
                    watermark.watermark_value as i64
                };
                let advance_to = Cursor::new(wm_ts, u64::MAX);
                let state = self.state_ref();

                let advances_frontier = state
                    .watermark_frontier()
                    .map_or(true, |frontier| wm_ts > frontier);
                let result = if advances_frontier
                    && self.output_mode == WindowOutputMode::Emit
                {
                    let started = Instant::now();
                    let batch = self.process_due(advance_to).await;
                    if let Some((task_id, labels)) = self.metrics_target() {
                        metrics::record_wm_process_ms(
                            task_id,
                            labels,
                            started.elapsed().as_secs_f64() * 1000.0,
                        );
                    }
                    batch
                } else {
                    RecordBatch::new_empty(self.output_schema.clone())
                };
                if advances_frontier {
                    state
                        .watermark_frontier
                        .store(wm_ts, Ordering::Release);
                }

                self.base
                    .pending_messages
                    .push(Message::Watermark(watermark));

                if self.output_mode == WindowOutputMode::StateOnly {
                    OperatorPollResult::Continue
                } else {
                    OperatorPollResult::Ready(Message::new(None, result, None, None))
                }
            }
            Message::CheckpointBarrier(barrier) => {
                OperatorPollResult::Ready(Message::CheckpointBarrier(barrier))
            }
            other => panic!("Window operator expects data messages or watermarks, got {other:?}"),
        }
    }
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;
        self.task_metadata = context.task_metadata();
        self.task_metadata.set(TASK_METADATA_ROWS_ACCEPTED, 0);
        self.task_metadata
            .set(TASK_METADATA_ROWS_DROPPED_LATE, 0);

        if self.state.is_none() {
            let backend = context
                .operator_state_backend()
                .expect("state backend must be configured for WindowOperator");
            let registry = context
                .state_registry()
                .expect("state registry must be configured for WindowOperator");
            let store = open_window_operator_store(registry, backend)?;
            let ns = StateNamespace::for_operator_task(
                context
                    .pipeline_id()
                    .expect("pipeline id must be configured for WindowOperator"),
                context
                    .operator_id()
                    .expect("operator id must be configured for WindowOperator"),
                context.task_index(),
            );
            let task_id = context.vertex_id_arc();
            let state = Arc::new(WindowOperatorState::new(
                store,
                ns,
                task_id.clone(),
                self.ts_column_index,
                self.window_configs.clone(),
                self.lateness_ms,
                self.max_window_length_ms,
            ));
            registry.insert_task_state(
                task_id.clone(),
                state.clone() as Arc<dyn OperatorTaskState>,
            );
            self.state_registry = Some(registry.clone());
            self.vertex_id = Some(task_id);
            self.metrics_labels = context.metrics_labels();
            self.state = Some(state);
        }

        Ok(())
    }

    async fn close(&mut self) -> Result<()> {
        if let (Some(registry), Some(vertex_id)) = (&self.state_registry, &self.vertex_id) {
            registry.remove_task_state(vertex_id.as_ref());
        }
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

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<SerializedCheckpoint> {
        let snapshot = self.state_ref().checkpoint().await?;
        Ok(SerializedCheckpoint::new(bincode::serialize(&snapshot)?))
    }

    async fn restore(&mut self, restore: SerializedRestore) -> Result<()> {
        let bytes = restore.into_bytes();
        let snapshot: WindowStateSnapshot = bincode::deserialize(&bytes)?;
        self.state_ref().restore(snapshot).await?;
        Ok(())
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        if let Some(msg) = self.base.pop_pending_output() {
            return OperatorPollResult::Ready(msg);
        }

        match self.base.next_inputs(INGEST_MAX_RECORDS).await {
            NextInputs::Exhausted => OperatorPollResult::None,
            NextInputs::Control(message) => self.handle_control(message).await,
            NextInputs::Data(messages) => self.ingest_data(messages).await,
        }
    }
}
