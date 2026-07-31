use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::future;

use crate::common::message::Message;
use crate::common::Key;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::checkpoint::{SerializedCheckpoint, SerializedRestore};
use crate::runtime::operators::operator::{
    MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType,
};
use crate::runtime::operators::window::config::{BuiltWindows, WindowConfig};
use crate::runtime::operators::window::eval::advance_key;
use crate::runtime::operators::window::frame_utils::require_range_frame;
use crate::runtime::operators::window::model::{Cursor, WindowId};
use crate::runtime::operators::window::spec::{WindowAdvancePolicy, WindowSpec};
use crate::runtime::operators::window::state::{WindowOperatorState, WindowStateSnapshot};
use crate::runtime::operators::window::store::{
    create_window_operator_store, StateNamespace, WindowOperatorStore,
};
use crate::runtime::operators::window::TileConfig;
use crate::runtime::runtime_context::RuntimeContext;
use datafusion::physical_plan::windows::BoundedWindowAggExec;

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
    buffered_keys: HashSet<Key>,
    output_mode: WindowOutputMode,
    advance_policy: WindowAdvancePolicy,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    ts_column_index: usize,
    lateness: Option<i64>,
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

        if matches!(
            window_operator_config.spec.advance_policy,
            WindowAdvancePolicy::OnIngest
        ) && window_operator_config.output_mode != WindowOutputMode::StateOnly
        {
            panic!("OnIngest advance is only valid for state-only WO");
        }

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
            buffered_keys: HashSet::new(),
            output_mode: window_operator_config.output_mode,
            advance_policy: window_operator_config.spec.advance_policy,
            output_schema: built.output_schema,
            input_schema: built.input_schema,
            ts_column_index: built.ts_column_index,
            lateness: window_operator_config.spec.lateness,
        }
    }

    #[cfg(test)]
    pub(crate) fn set_store_and_namespace(
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

    async fn process_key(&self, key: &Key, advance_to: Cursor) -> (RecordBatch, bool) {
        let emit = self.output_mode == WindowOutputMode::Emit;
        let partition = self.state_ref().partition(key);
        let (batch, pending) = advance_key(
            self.state_ref().store(),
            &partition,
            self.window_configs.as_ref(),
            advance_to,
            emit,
            self.ts_column_index,
            &self.output_schema,
            &self.input_schema,
            self.lateness,
        )
        .await
        .expect("advance_key");
        (batch, pending)
    }

    async fn process_buffered(
        &self,
        keys: Vec<Key>,
        advance_to: Cursor,
    ) -> (RecordBatch, HashSet<Key>) {
        if keys.is_empty() {
            return (
                RecordBatch::new_empty(self.output_schema.clone()),
                HashSet::new(),
            );
        }

        let futures: Vec<_> = keys
            .iter()
            .map(|k| async move { (k.clone(), self.process_key(k, advance_to).await) })
            .collect();
        let results = future::join_all(futures).await;

        let mut batches = Vec::with_capacity(results.len());
        let mut pending_keys = HashSet::new();
        for (k, (batch, pending)) in results {
            batches.push(batch);
            if pending {
                pending_keys.insert(k);
            }
        }

        let out = arrow::compute::concat_batches(&self.output_schema, &batches).expect("concat");
        (out, pending_keys)
    }
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;

        if self.state.is_none() {
            let backend = context
                .state_backend()
                .expect("state backend must be configured for WindowOperator");
            let store = create_window_operator_store(backend);
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
        // TODO: Drain buffered keys to a defined checkpoint frontier before flushing.
        let snapshot = self.state_ref().checkpoint().await?;
        Ok(SerializedCheckpoint::new(bincode::serialize(&snapshot)?))
    }

    async fn restore(&mut self, restore: SerializedRestore) -> Result<()> {
        let bytes = restore.into_bytes();
        let snapshot: WindowStateSnapshot = bincode::deserialize(&bytes)?;
        self.state_ref().restore(snapshot).await
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
                    let (dropped, max_seen) = self
                        .state_ref()
                        .insert_batch(key, keyed_message.base.record_batch.clone())
                        .await;
                    if dropped < input_rows {
                        if self.output_mode == WindowOutputMode::StateOnly
                            && matches!(self.advance_policy, WindowAdvancePolicy::OnIngest)
                        {
                            let max = max_seen.expect("accepted ingest must update max_seen");
                            let _ = self.process_key(key, max).await;
                        } else {
                            self.buffered_keys.insert(key.clone());
                        }
                    }
                    OperatorPollResult::Continue
                }
                Message::Watermark(watermark) => {
                    let wm_ts = if watermark.watermark_value == MAX_WATERMARK_VALUE {
                        i64::MAX
                    } else {
                        watermark.watermark_value as i64
                    };
                    let advance_to = Cursor::new(wm_ts, u64::MAX);

                    let keys: Vec<Key> = self.buffered_keys.iter().cloned().collect();
                    let (result, pending_keys) = self.process_buffered(keys, advance_to).await;
                    if watermark.watermark_value == MAX_WATERMARK_VALUE {
                        self.buffered_keys.clear();
                    } else {
                        self.buffered_keys = pending_keys;
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
