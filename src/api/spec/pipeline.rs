use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::api::spec::connectors::{RequestSourceSinkSpec, SinkSpec, SourceSpec};
use crate::api::spec::operators::{OperatorOverride, OperatorOverrides};
use crate::api::spec::storage::StorageSpec;
use crate::api::spec::worker_runtime::WorkerRuntimeSpec;
use crate::orchestrator::task_assignment::TaskWorkerAssignmentStrategyType;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::runtime::operators::window::TimeGranularity;
use crate::storage::StorageBudgetConfig;
use crate::transport::transport_spec::OperatorTransportSpec;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub enum ExecutionMode {
    Request,
    Streaming,
    Batch,
}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
pub enum ExecutionProfile {
    SingleWorker { num_threads_per_task: usize },
    MasterWorker { num_threads_per_task: usize },
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub execution_profile: Option<ExecutionProfile>,
    pub execution_mode: ExecutionMode,
    pub parallelism: usize,
    #[serde(default)]
    pub worker_runtime: WorkerRuntimeSpec,
    /// Per-operator-type storage overrides (shared across all instances of that operator type in a worker).
    /// Key is a stable operator-type string (e.g. "window").
    #[serde(default)]
    pub operator_type_storage: HashMap<String, StorageSpec>,
    #[serde(default)]
    pub operator_overrides: OperatorOverrides,
    pub sources: Vec<SourceSpec>,
    pub request_source_sink: Option<RequestSourceSinkSpec>,
    pub sink: Option<SinkSpec>,
    pub sql: Option<String>,
    pub task_assignment_strategy: Option<TaskWorkerAssignmentStrategyType>,
}

#[derive(Clone, Debug, Default)]
pub struct ConnectorConfigs {
    pub sources: HashMap<String, (SourceConfig, Arc<Schema>)>,
    pub request_source: Option<SourceConfig>,
    pub request_sink: Option<SinkConfig>,
    pub sink: Option<SinkConfig>,
}

#[derive(Clone, Debug)]
pub struct PipelineSpecBuilder {
    spec: PipelineSpec,
}

fn operator_override_transport_queue_records(o: &OperatorOverride) -> Option<u32> {
    o.transport.as_ref().and_then(|t| t.queue_records)
}

impl PipelineSpecBuilder {
    pub fn new() -> Self {
        Self {
            spec: PipelineSpec {
                execution_profile: None,
                execution_mode: ExecutionMode::Streaming,
                parallelism: 1,
                worker_runtime: WorkerRuntimeSpec::default(),
                operator_type_storage: HashMap::new(),
                operator_overrides: OperatorOverrides::default(),
                sources: Vec::new(),
                request_source_sink: None,
                sink: None,
                sql: None,
                task_assignment_strategy: None,
            },
        }
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.spec.parallelism = parallelism;
        self
    }

    pub fn with_execution_mode(mut self, execution_mode: ExecutionMode) -> Self {
        self.spec.execution_mode = execution_mode;
        self
    }

    pub fn with_execution_profile(mut self, profile: ExecutionProfile) -> Self {
        self.spec.execution_profile = Some(profile);
        self
    }

    pub fn with_transport_default_queue_records(mut self, queue_records: u32) -> Self {
        self.spec.worker_runtime.transport.default_queue_records = queue_records.max(1);
        self
    }

    pub fn with_operator_transport_queue_records(
        mut self,
        operator_id: &str,
        queue_records: u32,
    ) -> Self {
        self.spec
            .operator_overrides
            .per_operator
            .entry(operator_id.to_string())
            .or_insert_with(OperatorOverride::default)
            .transport = Some(OperatorTransportSpec {
            queue_records: Some(queue_records.max(1)),
        });
        self
    }

    pub fn with_storage_budgets(mut self, budgets: StorageBudgetConfig) -> Self {
        self.spec.worker_runtime.storage.budgets = budgets;
        self
    }

    pub fn with_snapshot_history_retention_window_ms(mut self, ms: u64) -> Self {
        self.spec.worker_runtime.history_retention_window_ms = Some(ms.max(1));
        self
    }

    pub fn with_inmem_store_lock_pool_size(mut self, size: usize) -> Self {
        self.spec.worker_runtime.storage.inmem_store_lock_pool_size = size;
        self
    }

    pub fn with_inmem_store_bucket_granularity(mut self, g: TimeGranularity) -> Self {
        self.spec
            .worker_runtime
            .storage
            .inmem_store_bucket_granularity = g;
        self
    }

    pub fn with_inmem_store_max_batch_size(mut self, size: usize) -> Self {
        self.spec.worker_runtime.storage.inmem_store_max_batch_size = size;
        self
    }

    pub fn with_operator_type_storage_spec(
        mut self,
        operator_type: &str,
        spec: StorageSpec,
    ) -> Self {
        self.spec
            .operator_type_storage
            .insert(operator_type.to_string(), spec);
        self
    }

    pub fn with_window_storage_spec(self, spec: StorageSpec) -> Self {
        self.with_operator_type_storage_spec("window", spec)
    }

    pub fn with_operator_overrides_defaults(mut self, defaults: OperatorOverride) -> Self {
        self.spec.operator_overrides.defaults = defaults;
        self
    }

    pub fn with_operator_override(
        mut self,
        operator_id: &str,
        override_spec: OperatorOverride,
    ) -> Self {
        self.spec
            .operator_overrides
            .per_operator
            .insert(operator_id.to_string(), override_spec);
        self
    }

    pub fn sql(mut self, sql: &str) -> Self {
        self.spec.sql = Some(sql.to_string());
        self
    }

    pub fn with_task_assignment_strategy(
        mut self,
        strategy: TaskWorkerAssignmentStrategyType,
    ) -> Self {
        self.spec.task_assignment_strategy = Some(strategy);
        self
    }

    pub fn with_source(mut self, src: SourceSpec) -> Self {
        if self
            .spec
            .sources
            .iter()
            .any(|s| s.table_name == src.table_name)
        {
            panic!(
                "Duplicate source table_name in PipelineSpecBuilder::with_source: {}",
                src.table_name
            );
        }
        self.spec.sources.push(src);
        self
    }

    pub fn with_request_source_sink(mut self, cfg: RequestSourceSinkSpec) -> Self {
        self.spec.request_source_sink = Some(cfg);
        self
    }

    pub fn with_sink(mut self, sink: SinkSpec) -> Self {
        self.spec.sink = Some(sink);
        self
    }

    pub fn build(self) -> PipelineSpec {
        // assert that the execution profile is set
        if self.spec.execution_profile.is_none() {
            panic!("Execution profile must be set");
        }
        self.spec
    }
}

impl PipelineSpec {
    pub fn transport_overrides_queue_records(&self) -> HashMap<String, u32> {
        let mut out = HashMap::new();
        for (op_id, ov) in &self.operator_overrides.per_operator {
            if let Some(v) = operator_override_transport_queue_records(ov) {
                out.insert(op_id.clone(), v.max(1));
            }
        }
        out
    }

    pub fn operator_type_storage_overrides(&self) -> HashMap<String, StorageSpec> {
        self.operator_type_storage.clone()
    }
}
