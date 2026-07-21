use std::collections::{BTreeMap, HashSet};
use std::fmt;
use std::sync::Arc;

use anyhow::Result;
use arrow::array::RecordBatch;
use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use futures::future;

use datafusion::physical_plan::windows::BoundedWindowAggExec;
use crate::common::message::Message;
use crate::common::Key;
use crate::common::MAX_WATERMARK_VALUE;
use crate::runtime::operators::operator::{
    MessageStream, OperatorBase, OperatorConfig, OperatorPollResult, OperatorTrait, OperatorType,
};
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::evaluate::advance_key;
use crate::runtime::operators::window::frame_utils::require_range_frame;
use crate::runtime::operators::window::shared::{
    build_window_operator_parts, resolve_tiling_configs, WindowConfig,
};
use crate::runtime::operators::window::store::{StateNamespace, WindowStateStore};
use crate::runtime::operators::window::window_operator_state::{WindowOperatorState, WindowId};
use crate::runtime::operators::window::window_tuning::WindowOperatorSpec;
use crate::runtime::operators::window::TileConfig;
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::{InMemSortedKV, SortedKV};

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WindowEmitMode {
    Regular, // operator produces messages
    Request, // operator only updates state, produces no messages
}

#[derive(Debug, Clone, Copy, serde::Serialize, serde::Deserialize)]
pub enum RequestAdvancePolicy {
    /// Advance windows only when watermark is received (default, supports complex DAGs).
    OnWatermark,
    /// Advance windows on every keyed message (for simple ingest+request topologies; max freshness).
    OnIngest,
}

#[derive(Debug, Clone)]
pub struct WindowOperatorConfig {
    pub window_exec: Arc<BoundedWindowAggExec>,
    pub execution_mode: WindowEmitMode,
    /// Per-window tiling overrides (index-aligned with window exprs).
    /// Gaps filled from `spec.tiling` at operator construction.
    pub tiling_configs: Vec<Option<TileConfig>>,
    pub spec: WindowOperatorSpec,
}

impl WindowOperatorConfig {
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {
        Self {
            window_exec,
            execution_mode: WindowEmitMode::Regular,
            tiling_configs: Vec::new(),
            spec: WindowOperatorSpec::default(),
        }
    }

    pub fn set_spec(&mut self, spec: WindowOperatorSpec) -> &mut Self {
        self.spec = spec;
        self
    }
}

pub struct WindowOperator {
    base: OperatorBase,
    window_configs: Arc<BTreeMap<WindowId, WindowConfig>>,
    state: Option<Arc<WindowOperatorState>>,
    buffered_keys: HashSet<Key>,
    execution_mode: WindowEmitMode,
    request_advance_policy: RequestAdvancePolicy,
    output_schema: SchemaRef,
    input_schema: SchemaRef,
    current_watermark: Option<u64>,
    ts_column_index: usize,
    lateness: Option<i64>,
}

impl fmt::Debug for WindowOperator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WindowOperator")
            .field("base", &self.base)
            .field("windows", &self.window_configs)
            .field("execution_mode", &self.execution_mode)
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
            window_operator_config.spec.request_advance_policy,
            RequestAdvancePolicy::OnIngest
        ) && window_operator_config.execution_mode != WindowEmitMode::Request
        {
            panic!("RequestAdvancePolicy::OnIngest is only valid for WindowEmitMode::Request");
        }

        let tiling_configs = resolve_tiling_configs(
            window_operator_config.window_exec.window_expr().len(),
            &window_operator_config.tiling_configs,
            &window_operator_config.spec,
        );
        let (ts_column_index, windows, input_schema, output_schema) =
            build_window_operator_parts(
                false,
                &window_operator_config.window_exec,
                &tiling_configs,
            );

        for w in windows.values() {
            require_range_frame(w.window_expr.get_window_frame());
        }

        Self {
            base: OperatorBase::new(config),
            window_configs: Arc::new(windows),
            state: None,
            buffered_keys: HashSet::new(),
            execution_mode: window_operator_config.execution_mode,
            request_advance_policy: window_operator_config.spec.request_advance_policy,
            output_schema,
            input_schema,
            current_watermark: None,
            ts_column_index,
            lateness: window_operator_config.spec.lateness,
        }
    }

    fn state_ref(&self) -> &Arc<WindowOperatorState> {
        self.state.as_ref().expect("WindowOperator must be opened first")
    }

    async fn process_key(&self, key: &Key, advance_to: Cursor) -> (RecordBatch, bool) {
        let emit = self.execution_mode == WindowEmitMode::Regular;
        let (batch, pending) = advance_key(
            self.state_ref().store(),
            key,
            self.window_configs.as_ref(),
            advance_to,
            emit,
            &self.output_schema,
            &self.input_schema,
        )
        .await
        .expect("advance_key");
        self.state_ref().prune_if_needed(key).await;
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

        let out = arrow::compute::concat_batches(&self.output_schema, &batches)
            .expect("concat");
        (out, pending_keys)
    }
}

#[async_trait]
impl OperatorTrait for WindowOperator {
    async fn open(&mut self, context: &RuntimeContext) -> Result<()> {
        self.base.open(context).await?;

        if self.state.is_none() {
            let kv: Arc<dyn SortedKV> = context
                .sorted_kv()
                .unwrap_or_else(|| Arc::new(InMemSortedKV::new()) as Arc<dyn SortedKV>);
            let ns = context
                .window_state_namespace()
                .unwrap_or_else(|| StateNamespace::new(b"window_state"));
            let store = WindowStateStore::new(kv, ns);
            self.state = Some(Arc::new(WindowOperatorState::new(
                store,
                self.ts_column_index,
                self.window_configs.clone(),
                self.lateness,
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
                        if self.execution_mode == WindowEmitMode::Request
                            && matches!(
                                self.request_advance_policy,
                                RequestAdvancePolicy::OnIngest
                            )
                        {
                            let max = max_seen.unwrap_or(Cursor::new(i64::MAX, u64::MAX));
                            let _ = self.process_key(key, max).await;
                        } else {
                            self.buffered_keys.insert(key.clone());
                        }
                    }
                    OperatorPollResult::Continue
                }
                Message::Watermark(watermark) => {
                    self.current_watermark = Some(watermark.watermark_value);
                    let wm_ts = if watermark.watermark_value == MAX_WATERMARK_VALUE {
                        i64::MAX
                    } else {
                        watermark.watermark_value as i64
                    };
                    let advance_to = Cursor::new(wm_ts, u64::MAX);

                    let keys: Vec<Key> = self.buffered_keys.iter().cloned().collect();
                    let (result, pending_keys) =
                        self.process_buffered(keys, advance_to).await;
                    if watermark.watermark_value == MAX_WATERMARK_VALUE {
                        self.buffered_keys.clear();
                    } else {
                        self.buffered_keys = pending_keys;
                    }

                    self.base.pending_messages.push(Message::Watermark(watermark));

                    if self.execution_mode == WindowEmitMode::Request {
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

    async fn checkpoint(&mut self, _checkpoint_id: u64) -> Result<Vec<(String, Vec<u8>)>> {
        self.state_ref().flush().await?;
        let state_cp = self.state_ref().to_checkpoint();
        Ok(vec![(
            "window_operator_state".to_string(),
            bincode::serialize(&state_cp)?,
        )])
    }
}
