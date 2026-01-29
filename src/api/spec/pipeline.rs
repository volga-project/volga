use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use serde::{Deserialize, Serialize};

use crate::api::LogicalGraph;
use crate::api::spec::connectors::{RequestSourceSinkSpec, SinkSpec, SourceBindingSpec};
use crate::api::spec::operators::{OperatorOverride, OperatorOverrides};
use crate::api::spec::runtime_adapter::RuntimeAdapterSpec;
use crate::executor::placement::TaskPlacementStrategyName;
use crate::api::spec::resources::{ResourceProfiles, ResourceStrategy};
use crate::api::spec::worker_runtime::WorkerRuntimeSpec;
use crate::api::spec::storage::StorageSpec;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;
use crate::transport::transport_spec::OperatorTransportSpec;
use crate::storage::StorageBudgetConfig;
use crate::runtime::operators::window::TimeGranularity;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ExecutionMode {
    Request,
    Streaming,
    Batch,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ExecutionProfile {
    InProcess,
    Local {
        task_placement_strategy: TaskPlacementStrategyName,
        resource_strategy: ResourceStrategy,
    },
    K8s {
        task_placement_strategy: TaskPlacementStrategyName,
        resource_strategy: ResourceStrategy,
    },
    Custom {
        runtime_adapter: RuntimeAdapterSpec,
        task_placement_strategy: TaskPlacementStrategyName,
        resource_strategy: ResourceStrategy,
    },
}

impl ExecutionProfile {
    pub fn local_default() -> Self {
        Self::Local {
            task_placement_strategy: TaskPlacementStrategyName::SingleWorker,
            resource_strategy: ResourceStrategy::PerWorker,
        }
    }

    pub fn k8s_default() -> Self {
        Self::K8s {
            task_placement_strategy: TaskPlacementStrategyName::SingleWorker,
            resource_strategy: ResourceStrategy::PerWorker,
        }
    }

    pub fn runtime_adapter_spec(&self) -> Option<RuntimeAdapterSpec> {
        match self {
            ExecutionProfile::InProcess => None,
            ExecutionProfile::Local { .. } => Some(RuntimeAdapterSpec::Local),
            ExecutionProfile::K8s { .. } => Some(RuntimeAdapterSpec::K8s),
            ExecutionProfile::Custom { runtime_adapter, .. } => Some(runtime_adapter.clone()),
        }
    }

    pub fn task_placement_strategy(&self) -> Option<&TaskPlacementStrategyName> {
        match self {
            ExecutionProfile::InProcess => None,
            ExecutionProfile::Local { task_placement_strategy, .. } => Some(task_placement_strategy),
            ExecutionProfile::K8s { task_placement_strategy, .. } => Some(task_placement_strategy),
            ExecutionProfile::Custom { task_placement_strategy, .. } => Some(task_placement_strategy),
        }
    }

    pub fn resource_strategy(&self) -> Option<&ResourceStrategy> {
        match self {
            ExecutionProfile::InProcess => None,
            ExecutionProfile::Local { resource_strategy, .. } => Some(resource_strategy),
            ExecutionProfile::K8s { resource_strategy, .. } => Some(resource_strategy),
            ExecutionProfile::Custom { resource_strategy, .. } => Some(resource_strategy),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PipelineSpec {
    pub execution_profile: ExecutionProfile,
    pub execution_mode: ExecutionMode,
    pub parallelism: usize,
    pub resource_profiles: ResourceProfiles,
    pub worker_runtime: WorkerRuntimeSpec,
    /// Per-operator-type storage overrides (shared across all instances of that operator type in a worker).
    /// Key is a stable operator-type string (e.g. "window").
    pub operator_type_storage: HashMap<String, StorageSpec>,
    pub operator_overrides: OperatorOverrides,
    pub sources: Vec<SourceBindingSpec>,
    pub request_source_sink: Option<RequestSourceSinkSpec>,
    pub sink: Option<SinkSpec>,
    pub sql: Option<String>,
    #[serde(skip)]
    pub logical_graph: Option<LogicalGraph>,
    #[serde(skip)]
    pub inline_sources: HashMap<String, (SourceConfig, Arc<Schema>)>,
    #[serde(skip)]
    pub inline_request_source: Option<SourceConfig>,
    #[serde(skip)]
    pub inline_request_sink: Option<SinkConfig>,
    #[serde(skip)]
    pub inline_sink: Option<SinkConfig>,
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
                execution_profile: ExecutionProfile::InProcess,
                execution_mode: ExecutionMode::Streaming,
                parallelism: 1,
                resource_profiles: ResourceProfiles::default(),
                worker_runtime: WorkerRuntimeSpec::default(),
                operator_type_storage: HashMap::new(),
                operator_overrides: OperatorOverrides::default(),
                sources: Vec::new(),
                request_source_sink: None,
                sink: None,
                sql: None,
                logical_graph: None,
                inline_sources: HashMap::new(),
                inline_request_source: None,
                inline_request_sink: None,
                inline_sink: None,
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
        self.spec.execution_profile = profile;
        self
    }

    pub fn with_worker_runtime_spec(mut self, worker_runtime: WorkerRuntimeSpec) -> Self {
        self.spec.worker_runtime = worker_runtime;
        self
    }

    pub fn with_resource_profiles(mut self, profiles: ResourceProfiles) -> Self {
        self.spec.resource_profiles = profiles;
        self
    }

    pub fn with_transport_default_queue_records(mut self, queue_records: u32) -> Self {
        self.spec.worker_runtime.transport.default_queue_records = queue_records.max(1);
        self
    }

    pub fn with_operator_transport_queue_records(mut self, operator_id: &str, queue_records: u32) -> Self {
        self.spec.operator_overrides.per_operator
            .entry(operator_id.to_string())
            .or_insert_with(OperatorOverride::default)
            .transport = Some(OperatorTransportSpec { queue_records: Some(queue_records.max(1)) });
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
        self.spec.worker_runtime.storage.inmem_store_bucket_granularity = g;
        self
    }

    pub fn with_inmem_store_max_batch_size(mut self, size: usize) -> Self {
        self.spec.worker_runtime.storage.inmem_store_max_batch_size = size;
        self
    }

    pub fn with_operator_type_storage_spec(mut self, operator_type: &str, spec: StorageSpec) -> Self {
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

    pub fn with_operator_override(mut self, operator_id: &str, override_spec: OperatorOverride) -> Self {
        self.spec
            .operator_overrides
            .per_operator
            .insert(operator_id.to_string(), override_spec);
        self
    }

    pub fn sql(mut self, sql: &str) -> Self {
        self.spec.sql = Some(sql.to_string());
        self.spec.logical_graph = None;
        self
    }

    pub fn with_logical_graph(mut self, logical_graph: LogicalGraph) -> Self {
        self.spec.logical_graph = Some(logical_graph);
        self.spec.sql = None;
        self
    }

    pub fn add_source_binding(mut self, src: SourceBindingSpec) -> Self {
        self.spec.sources.push(src);
        self
    }

    pub fn with_source(mut self, table_name: String, source_config: SourceConfig, schema: Arc<Schema>) -> Self {
        self.spec.inline_sources.insert(table_name, (source_config, schema));
        self
    }

    pub fn with_request_source_sink_inline(mut self, source_config: SourceConfig, sink_config: Option<SinkConfig>) -> Self {
        self.spec.inline_request_source = Some(source_config);
        self.spec.inline_request_sink = sink_config;
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

    pub fn with_sink_inline(mut self, sink: SinkConfig) -> Self {
        self.spec.inline_sink = Some(sink);
        self
    }

    pub fn build(self) -> PipelineSpec {
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

