use std::collections::BTreeMap;
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::future;

use crate::common::message::Message;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::checkpoint::{SerializedCheckpoint, SerializedRestore};
use crate::runtime::operators::operator::{
    MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType,
};
use crate::runtime::operators::window::config::{BuiltWindows, WindowConfig};
use crate::runtime::operators::window::eval::advance_due_work;
use crate::runtime::operators::window::frame_utils::require_range_frame;
use crate::runtime::operators::window::model::{Cursor, WindowId};
use crate::runtime::operators::window::spec::WindowSpec;
use crate::runtime::operators::window::state::{WindowOperatorState, WindowStateSnapshot};
use crate::runtime::operators::window::store::{open_window_operator_store, StateNamespace};
use crate::runtime::operators::window::TileConfig;
use crate::runtime::runtime_context::RuntimeContext;
use datafusion::physical_plan::windows::BoundedWindowAggExec;

#[cfg(test)]
use crate::runtime::operators::window::store::WindowOperatorStore;

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
    watermark_frontier: Option<i64>,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    ts_column_index: usize,
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
        Self {
            base: OperatorBase::new(config),
            window_configs: windows,
            state: None,
            output_mode: window_operator_config.output_mode,
            watermark_frontier: None,
            output_schema: built.output_schema,
            input_schema: built.input_schema,
            ts_column_index: built.ts_column_index,
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
            self.ts_column_index,
            self.window_configs.clone(),
        )));
    }

    fn state_ref(&self) -> &Arc<WindowOperatorState> {
        self.state
            .as_ref()
            .expect("WindowOperator must be opened first")
    }

    async fn process_due(&self, through: Cursor) -> RecordBatch {
        let Some(state) = self.state.as_ref() else {
            return RecordBatch::new_empty(self.output_schema.clone());
        };
        let after = self
            .watermark_frontier
            .map(|timestamp| Cursor::new(timestamp, u64::MAX));
        let mut continuation = None;
        let mut batches = Vec::new();
        loop {
            let page = state
                .store()
                .load_due_page(state.namespace(), after, through, continuation)
                .await
                .expect("load due window triggers");
            let futures = page.work.into_iter().map(|work| {
                advance_due_work(
                    state.store(),
                    work,
                    self.window_configs.as_ref(),
                    true,
                    self.ts_column_index,
                    &self.output_schema,
                    &self.input_schema,
                )
            });
            batches.extend(
                future::try_join_all(futures)
                    .await
                    .expect("advance due window triggers"),
            );
            let Some(next) = page.continuation else {
                break;
            };
            continuation = Some(next);
        }
        if batches.is_empty() {
            return RecordBatch::new_empty(self.output_schema.clone());
        }
        arrow::compute::concat_batches(&self.output_schema, &batches).expect("concat")
    }
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;

        if self.state.is_none() {
            let backend = context
                .operator_state_backend()
                .expect("state backend must be configured for WindowOperator");
            let store = open_window_operator_store(backend).await?;
            let ns = StateNamespace::for_operator_task(
                context
                    .pipeline_id()
                    .expect("pipeline id must be configured for WindowOperator"),
                context
                    .operator_id()
                    .expect("operator id must be configured for WindowOperator"),
                context.task_index(),
            );
            self.state = Some(Arc::new(WindowOperatorState::new(
                store,
                ns,
                self.ts_column_index,
                self.window_configs.clone(),
            )));
        }

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

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<SerializedCheckpoint> {
        let snapshot = self
            .state_ref()
            .checkpoint(self.watermark_frontier)
            .await?;
        Ok(SerializedCheckpoint::new(bincode::serialize(&snapshot)?))
    }

    async fn restore(&mut self, restore: SerializedRestore) -> Result<()> {
        let bytes = restore.into_bytes();
        let snapshot: WindowStateSnapshot = bincode::deserialize(&bytes)?;
        self.watermark_frontier = self.state_ref().restore(snapshot).await?;
        Ok(())
    }

    async fn poll_next(&mut self) -> OperatorPollResult {
        if let Some(msg) = self.base.pop_pending_output() {
            return OperatorPollResult::Ready(msg);
        }

        match self.base.next_input().await {
            Some(message) => match message {
                Message::Keyed(keyed_message) => {
                    let key = keyed_message.key();
                    let input_rows = keyed_message.base.record_batch.num_rows();
                    let dropped = self
                        .state_ref()
                        .insert_batch(
                            key,
                            keyed_message.base.record_batch.clone(),
                            self.watermark_frontier,
                            self.output_mode == WindowOutputMode::Emit,
                        )
                        .await;
                    debug_assert!(dropped <= input_rows);
                    OperatorPollResult::Continue
                }
                Message::Watermark(watermark) => {
                    let wm_ts = if watermark.watermark_value == MAX_WATERMARK_VALUE {
                        i64::MAX
                    } else {
                        watermark.watermark_value as i64
                    };
                    let advance_to = Cursor::new(wm_ts, u64::MAX);

                    let advances_frontier = self
                        .watermark_frontier
                        .map_or(true, |frontier| wm_ts > frontier);
                    let result = if advances_frontier
                        && self.output_mode == WindowOutputMode::Emit
                    {
                        self.process_due(advance_to).await
                    } else {
                        RecordBatch::new_empty(self.output_schema.clone())
                    };
                    if advances_frontier {
                        self.watermark_frontier = Some(wm_ts);
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
                _ => panic!("Window operator expects keyed messages or watermarks"),
            },
            None => OperatorPollResult::None,
        }
    }
}
