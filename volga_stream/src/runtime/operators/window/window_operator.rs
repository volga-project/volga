use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::windows::BoundedWindowAggExec;
use crate::common::message::Message;
use crate::runtime::operators::operator::{OperatorBase, OperatorConfig, OperatorTrait, OperatorType};
use crate::runtime::operators::window::state::state::State;
use crate::runtime::runtime_context::RuntimeContext;
use crate::storage::storage::{BatchId, Storage};

#[derive(Debug, Clone)]
pub struct WindowConfig {
    pub window_exec: Arc<BoundedWindowAggExec> 
}

impl WindowConfig {
    pub fn new(window_exec: Arc<BoundedWindowAggExec>) -> Self {    
        Self {
            window_exec
        }
    }
}

#[derive(Debug)]
pub struct WindowOperator {
    base: OperatorBase,
    state: State,
    ts_column_index: usize,
    to_process: Vec<BatchId>,
}

impl WindowOperator {
    pub fn new(config: OperatorConfig, storage: Arc<Storage>) -> Self {
        let window_config = match config.clone() {
            OperatorConfig::WindowConfig(window_config) => window_config,
            _ => panic!("Expected WindowConfig, got {:?}", config),
        };

        let mut state = State::new();
        state.init_windows(window_config.window_exec.window_expr().to_vec());

        let ts_column_index = window_config.window_exec.window_expr()[0].order_by()[0].expr.as_any().downcast_ref::<Column>().expect("Expected Column expression in ORDER BY").index();

        Self {
            base: OperatorBase::new(config, storage),
            state,
            ts_column_index,
            to_process: Vec::new(),
        }
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
        let partition_key = message.key().expect("msg");

        // TODO based one execution mode (low-latency vs watermark based)
        // we may need to filter and drop late events

        // TODO calculate pre-aggregates
        
        // TODO this is not sorted
        let batch_ids = storage.append_records(message.record_batch().clone(), partition_key, self.ts_column_index).await;
        self.to_process.extend(batch_ids);
        None
    }

    async fn process_watermark(&mut self, _watermark: u64) -> Option<Vec<Message>> {
        // TODO: Implementation
        None
    }

    fn operator_type(&self) -> OperatorType {
        self.base.operator_type()
    }
}