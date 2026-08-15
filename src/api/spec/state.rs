use std::time::Duration;

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum CheckpointStoreConfig {
    #[default]
    InMemory,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum OperatorStateBackendConfig {
    #[default]
    InMemory,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequestStoreConfig {}

#[derive(Clone, Debug, Serialize, Deserialize, JsonSchema)]
#[serde(default)]
#[schemars(default)]
pub struct StateSpec {
    pub checkpoint_store: CheckpointStoreConfig,
    pub operator_backend: OperatorStateBackendConfig,
    pub request_store: Option<RequestStoreConfig>,
    /// Per-job checkpoint interval / timeout / retention. Omitted fields use runtime consts.
    #[serde(default)]
    pub checkpoint: CheckpointSpec,
    /// When true, workers run periodic `OperatorStore::maintain`.
    pub maintenance_enabled: bool,
    /// Cleaner tick interval in milliseconds.
    pub maintenance_interval_ms: u64,
}

/// Job checkpoint knobs. Master uses each `Some` value; `None` falls back to
/// `master.checkpoint_{interval,timeout,retention}` consts. Heartbeats / RPC /
/// kube health stay on consts.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<u64>,
}

impl CheckpointSpec {
    pub fn interval(&self) -> Option<Duration> {
        self.interval_ms.filter(|ms| *ms > 0).map(Duration::from_millis)
    }

    pub fn timeout(&self) -> Option<Duration> {
        self.timeout_ms.filter(|ms| *ms > 0).map(Duration::from_millis)
    }

    pub fn interval_or(&self, fallback: Duration) -> Duration {
        self.interval().unwrap_or(fallback)
    }

    pub fn timeout_or(&self, fallback: Duration) -> Duration {
        self.timeout().unwrap_or(fallback)
    }

    pub fn retention_or(&self, fallback: u64) -> u64 {
        self.retention.filter(|n| *n > 0).unwrap_or(fallback)
    }
}

impl Default for StateSpec {
    fn default() -> Self {
        Self {
            checkpoint_store: CheckpointStoreConfig::default(),
            operator_backend: OperatorStateBackendConfig::default(),
            request_store: None,
            checkpoint: CheckpointSpec::default(),
            maintenance_enabled: true,
            maintenance_interval_ms: 1_000,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn omitted_checkpoint_uses_consts_fallback() {
        let spec: StateSpec = serde_json::from_value(serde_json::json!({
            "checkpoint_store": "in_memory",
            "operator_backend": "in_memory",
            "maintenance_enabled": true,
            "maintenance_interval_ms": 1000
        }))
        .unwrap();
        assert_eq!(spec.checkpoint, CheckpointSpec::default());
        assert_eq!(spec.checkpoint.interval_or(Duration::from_secs(30)), Duration::from_secs(30));
        assert_eq!(spec.checkpoint.retention_or(1), 1);
    }

    #[test]
    fn checkpoint_fields_override_fallback() {
        let spec: StateSpec = serde_json::from_value(serde_json::json!({
            "checkpoint": {
                "interval_ms": 5000,
                "timeout_ms": 15000,
                "retention": 3
            }
        }))
        .unwrap();
        assert_eq!(spec.checkpoint.interval(), Some(Duration::from_secs(5)));
        assert_eq!(spec.checkpoint.timeout(), Some(Duration::from_secs(15)));
        assert_eq!(spec.checkpoint.retention_or(1), 3);
    }

    #[test]
    fn zero_checkpoint_knobs_are_invalid() {
        use crate::api::{ExecutionMode, ExecutionProfile, PipelineSpec};

        let mut spec = PipelineSpec {
            execution_profile: Some(ExecutionProfile::SingleWorker {
                num_threads_per_task: 1,
            }),
            execution_mode: ExecutionMode::Streaming,
            parallelism: 1,
            worker_runtime: Default::default(),
            state: StateSpec::default(),
            operator_overrides: Default::default(),
            event_time: Default::default(),
            sources: Vec::new(),
            request_source_sink: None,
            sink: None,
            sql: None,
            task_assignment_strategy: None,
        };
        spec.state.checkpoint.interval_ms = Some(0);
        assert!(spec.validate().is_err());
        spec.state.checkpoint.interval_ms = Some(1_000);
        spec.state.checkpoint.retention = Some(0);
        assert!(spec.validate().is_err());
        spec.state.checkpoint.retention = Some(1);
        assert!(spec.validate().is_ok());
    }
}
