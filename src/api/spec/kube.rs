use anyhow::Context;
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::api::spec::connectors::{RequestSourceSinkSpec, SinkSpec, SourceSpec};
use crate::api::spec::event_time::EventTimeSpec;
use crate::api::spec::operators::OperatorOverrides;
use crate::api::spec::pipeline::{ExecutionMode, ExecutionProfile, PipelineSpec};
use crate::api::spec::worker_runtime::WorkerRuntimeSpec;
use crate::orchestrator::task_assignment::TaskWorkerAssignmentStrategyType;

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct KubePipelineSpec {
    #[serde(default)]
    pub execution_profile: Option<ExecutionProfile>,
    #[serde(default = "default_execution_mode")]
    pub execution_mode: ExecutionMode,
    #[serde(default = "default_parallelism")]
    pub parallelism: usize,
    #[serde(default = "default_worker_runtime_json")]
    pub worker_runtime: Value,
    /// Deprecated / ignored (kept so older YAML still parses under deny_unknown_fields).
    #[serde(default = "default_operator_type_storage_json")]
    pub operator_type_storage: Value,
    #[serde(default = "default_operator_overrides_json")]
    pub operator_overrides: Value,
    #[serde(default)]
    pub event_time: EventTimeSpec,
    #[serde(default = "default_sources_json")]
    pub sources: Value,
    #[serde(default)]
    pub request_source_sink: Option<Value>,
    #[serde(default)]
    pub sink: Option<Value>,
    #[serde(default)]
    pub sql: Option<String>,
    #[serde(default)]
    #[serde(alias = "node_assignment_strategy")]
    pub task_assignment_strategy: Option<TaskWorkerAssignmentStrategyType>,
}

fn default_execution_mode() -> ExecutionMode {
    ExecutionMode::Streaming
}

fn default_parallelism() -> usize {
    1
}

fn default_worker_runtime_json() -> Value {
    serde_json::to_value(WorkerRuntimeSpec::default()).expect("serialize default worker runtime")
}

fn default_operator_type_storage_json() -> Value {
    Value::Object(serde_json::Map::new())
}

fn default_operator_overrides_json() -> Value {
    serde_json::to_value(OperatorOverrides::default())
        .expect("serialize default operator overrides")
}

fn default_sources_json() -> Value {
    Value::Array(Vec::new())
}

fn normalize_json_strings(value: Value) -> Value {
    match value {
        Value::String(s) => {
            // Allow embedding raw JSON objects/arrays in YAML string blocks.
            if let Ok(parsed) = serde_json::from_str::<Value>(&s) {
                if parsed.is_object() || parsed.is_array() {
                    return normalize_json_strings(parsed);
                }
            }
            Value::String(s)
        }
        Value::Array(arr) => Value::Array(arr.into_iter().map(normalize_json_strings).collect()),
        Value::Object(obj) => Value::Object(
            obj.into_iter()
                .map(|(k, v)| (k, normalize_json_strings(v)))
                .collect(),
        ),
        other => other,
    }
}

impl TryFrom<KubePipelineSpec> for PipelineSpec {
    type Error = anyhow::Error;

    fn try_from(spec: KubePipelineSpec) -> Result<Self, Self::Error> {
        let worker_runtime: WorkerRuntimeSpec =
            serde_json::from_value(normalize_json_strings(spec.worker_runtime))
                .context("invalid worker_runtime")?;
        let _ = spec.operator_type_storage; // ignored
        let operator_overrides: OperatorOverrides =
            serde_json::from_value(normalize_json_strings(spec.operator_overrides))
                .context("invalid operator_overrides")?;
        let sources: Vec<SourceSpec> =
            serde_json::from_value(normalize_json_strings(spec.sources)).context("invalid sources")?;
        let request_source_sink: Option<RequestSourceSinkSpec> = match spec.request_source_sink {
            Some(v) => Some(
                serde_json::from_value(normalize_json_strings(v))
                    .context("invalid request_source_sink")?,
            ),
            None => None,
        };
        let sink: Option<SinkSpec> = match spec.sink {
            Some(v) => Some(
                serde_json::from_value(normalize_json_strings(v)).context("invalid sink")?,
            ),
            None => None,
        };

        Ok(PipelineSpec {
            execution_profile: spec.execution_profile,
            execution_mode: spec.execution_mode,
            parallelism: spec.parallelism,
            worker_runtime,
            operator_overrides,
            event_time: spec.event_time,
            sources,
            request_source_sink,
            sink,
            sql: spec.sql,
            task_assignment_strategy: spec.task_assignment_strategy,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs;
    use std::path::PathBuf;

    #[test]
    fn kube_pipeline_spec_parses_embedded_json_strings() {
        let path = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("src/api/spec/testdata/kube_embedded_json.yaml");
        let yaml = fs::read_to_string(&path).expect("read test fixture yaml");
        let kube_spec: KubePipelineSpec =
            serde_yaml::from_str(&yaml).expect("parse kube embedded json yaml fixture");

        let spec = PipelineSpec::try_from(kube_spec).expect("kube spec should parse");
        assert_eq!(spec.sources.len(), 1);
        assert_eq!(spec.sources[0].table_name, "test_table");
        assert_eq!(
            spec.sources[0].schema_json,
            serde_json::json!({
                "fields": [{
                    "name": "value",
                    "nullable": false,
                    "type": { "name": "utf8" },
                    "children": []
                }]
            })
        );
        match &spec.sink {
            Some(SinkSpec::InMemoryStorageGrpc { server_addr, .. }) => {
                assert_eq!(
                    server_addr,
                    "http://volga-test-storage.default.svc.cluster.local:50071"
                );
            }
            other => panic!("expected InMemoryStorageGrpc sink, got {:?}", other),
        }
    }
}
