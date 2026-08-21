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
    /// Per-job checkpoint interval / timeout / retention. Spec is the only source.
    /// `interval_ms: 0` disables interval checkpoints.
    #[serde(default)]
    pub checkpoint: CheckpointSpec,
    /// When true, workers run periodic `OperatorStore::maintain`.
    pub maintenance_enabled: bool,
    /// Cleaner tick interval in milliseconds.
    pub maintenance_interval_ms: u64,
}

/// Job checkpoint knobs. Spec is the only source.
/// `interval_ms: 0` disables interval checkpoints. Missing fields are off / no timeout
/// at runtime (no panic); [`crate::api::PipelineSpec::validate`] requires all three.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
pub struct CheckpointSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub interval_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention: Option<u64>,
}

impl Default for CheckpointSpec {
    fn default() -> Self {
        Self {
            interval_ms: Some(0),
            timeout_ms: Some(60_000),
            retention: Some(1),
        }
    }
}

impl CheckpointSpec {
    pub fn interval(&self) -> Option<Duration> {
        match self.interval_ms {
            Some(0) | None => None,
            Some(ms) => Some(Duration::from_millis(ms)),
        }
    }

    pub fn timeout(&self) -> Option<Duration> {
        match self.timeout_ms {
            Some(0) | None => None,
            Some(ms) => Some(Duration::from_millis(ms)),
        }
    }

    pub fn retention(&self) -> usize {
        self.retention.filter(|n| *n > 0).unwrap_or(0) as usize
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
