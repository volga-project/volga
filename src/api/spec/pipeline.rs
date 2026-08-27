use std::collections::HashMap;
use std::sync::Arc;

use arrow::datatypes::Schema;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::api::spec::connectors::{RequestSourceSinkSpec, SinkSpec, SourceSpec};
use crate::api::spec::event_time::EventTimeSpec;
use crate::api::spec::operators::{OperatorOverride, OperatorOverrides};
use crate::api::spec::state::StateSpec;
use crate::api::spec::state::{
    CheckpointStoreConfig, OperatorStateBackendConfig, RequestStoreConfig,
};
use crate::api::spec::worker_runtime::WorkerRuntimeSpec;
use crate::orchestrator::task_assignment::TaskWorkerAssignmentStrategyType;
use crate::runtime::operators::sink::sink_operator::SinkConfig;
use crate::runtime::operators::source::source_operator::SourceConfig;
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
    /// Upper bound on parallelism for key-group assignment. Defaults to [`Self::parallelism`].
    #[serde(default)]
    pub max_parallelism: Option<usize>,
    #[serde(default)]
    pub worker_runtime: WorkerRuntimeSpec,
    #[serde(default)]
    pub state: StateSpec,
    #[serde(default)]
    pub operator_overrides: OperatorOverrides,
    /// Pipeline-wide watermark / window lateness defaults (not via operator_overrides).
    #[serde(default)]
    pub event_time: EventTimeSpec,
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
                max_parallelism: None,
                worker_runtime: WorkerRuntimeSpec::default(),
                state: StateSpec::default(),
                operator_overrides: OperatorOverrides::default(),
                event_time: EventTimeSpec::default(),
                sources: Vec::new(),
                request_source_sink: None,
                sink: None,
                sql: None,
                task_assignment_strategy: None,
            },
        }
    }

    pub fn with_event_time(mut self, event_time: EventTimeSpec) -> Self {
        self.spec.event_time = event_time;
        self
    }

    pub fn with_out_of_orderness_ms(mut self, out_of_orderness_ms: u64) -> Self {
        self.spec.event_time.watermark.out_of_orderness_ms = out_of_orderness_ms;
        self
    }

    pub fn with_watermark_idle_timeout_ms(mut self, idle_timeout_ms: Option<u64>) -> Self {
        self.spec.event_time.watermark.idle_timeout_ms = idle_timeout_ms;
        self
    }

    pub fn with_watermark_emit_interval_ms(mut self, emit_interval_ms: Option<u64>) -> Self {
        self.spec.event_time.watermark.emit_interval_ms = emit_interval_ms;
        self
    }

    pub fn with_window_allowed_lateness_ms(mut self, allowed_lateness_ms: i64) -> Self {
        self.spec.event_time.window.allowed_lateness_ms = allowed_lateness_ms;
        self
    }

    /// Sets watermark out-of-orderness and window retention (`allowed_lateness_ms`) to the same value.
    pub fn with_event_time_skew_ms(mut self, skew_ms: u64) -> Self {
        self.spec.event_time.watermark.out_of_orderness_ms = skew_ms;
        self.spec.event_time.window.allowed_lateness_ms = skew_ms as i64;
        self
    }

    pub fn with_parallelism(mut self, parallelism: usize) -> Self {
        self.spec.parallelism = parallelism;
        self
    }

    pub fn with_max_parallelism(mut self, max_parallelism: usize) -> Self {
        self.spec.max_parallelism = Some(max_parallelism.max(1));
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

    pub fn with_snapshot_history_retention_window_ms(mut self, ms: u64) -> Self {
        self.spec.worker_runtime.history_retention_window_ms = Some(ms.max(1));
        self
    }

    pub fn with_checkpoint_store(mut self, checkpoint_store: CheckpointStoreConfig) -> Self {
        self.spec.state.checkpoint_store = checkpoint_store;
        self
    }

    pub fn with_operator_state_backend(
        mut self,
        operator_backend: OperatorStateBackendConfig,
    ) -> Self {
        self.spec.state.operator_backend = operator_backend;
        self
    }

    pub fn with_request_store(mut self, request_store: RequestStoreConfig) -> Self {
        self.spec.state.request_store = Some(request_store);
        self
    }

    pub fn with_state_maintenance(mut self, enabled: bool, interval_ms: u64) -> Self {
        self.spec.state.maintenance_enabled = enabled;
        self.spec.state.maintenance_interval_ms = interval_ms.max(1);
        self
    }

    pub fn with_checkpoint(
        mut self,
        interval_ms: Option<u64>,
        timeout_ms: Option<u64>,
        retention: Option<u64>,
    ) -> Self {
        self.spec.state.checkpoint.interval_ms = interval_ms;
        self.spec.state.checkpoint.timeout_ms = timeout_ms;
        self.spec.state.checkpoint.retention = retention;
        self
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
        self.spec
            .validate()
            .unwrap_or_else(|error| panic!("invalid pipeline spec: {error}"));
        self.spec
    }
}

impl PipelineSpec {
    pub fn validate(&self) -> Result<(), String> {
        if self.execution_profile.is_none() {
            return Err("execution profile must be set".to_string());
        }
        if let Some(max_p) = self.max_parallelism {
            if max_p == 0 {
                return Err("max_parallelism must be >= 1".to_string());
            }
            if max_p < self.parallelism {
                return Err(format!(
                    "max_parallelism ({max_p}) must be >= parallelism ({})",
                    self.parallelism
                ));
            }
        }
        if self.execution_mode == ExecutionMode::Request && self.state.request_store.is_none() {
            return Err("request mode requires a request store".to_string());
        }
        if self.state.checkpoint.interval_ms.is_none() {
            return Err(
                "state.checkpoint.interval_ms is required (0 disables interval checkpoints)"
                    .to_string(),
            );
        }
        if self.state.checkpoint.timeout_ms.is_none()
            || matches!(self.state.checkpoint.timeout_ms, Some(0))
        {
            return Err("state.checkpoint.timeout_ms is required and must be > 0".to_string());
        }
        if self.state.checkpoint.retention.is_none()
            || matches!(self.state.checkpoint.retention, Some(0))
        {
            return Err("state.checkpoint.retention is required and must be >= 1".to_string());
        }
        Ok(())
    }

    /// Key-group space for this job. Defaults to [`Self::parallelism`].
    pub fn resolved_max_parallelism(&self) -> usize {
        self.max_parallelism.unwrap_or(self.parallelism.max(1)).max(1)
    }

    pub fn transport_overrides_queue_records(&self) -> HashMap<String, u32> {
        let mut out = HashMap::new();
        for (op_id, ov) in &self.operator_overrides.per_operator {
            if let Some(v) = operator_override_transport_queue_records(ov) {
                out.insert(op_id.clone(), v.max(1));
            }
        }
        out
    }
}
