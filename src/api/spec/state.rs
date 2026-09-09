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
    Scylla(ScyllaConfig),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum ServingPublish {
    /// Wait for the serving LWT after each ingest (once catch-up allows).
    #[default]
    OnCommit,
    /// Ingest does not wait. Promote this key when `interval_ms` has elapsed
    /// since its last promote. Checkpoint also flushes touched keys.
    Interval { interval_ms: u64 },
    /// Ingest does not wait. Promote touched keys on checkpoint only.
    Checkpoint,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub struct ScyllaConfig {
    pub contact_points: Vec<String>,
    pub keyspace: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datacenter: Option<String>,
    /// How often this writer publishes `window_head.serving_*`. Default
    /// `on_commit`. Not a fence — owner CAS is. WRO always pins serving.
    #[serde(default, skip_serializing_if = "is_on_commit_publish")]
    pub serving_publish: ServingPublish,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_parallelism: Option<usize>,
}

fn is_on_commit_publish(policy: &ServingPublish) -> bool {
    matches!(policy, ServingPublish::OnCommit)
}

impl ServingPublish {
    /// Whether ingest should wait on `promote_serving` (after catch-up).
    /// `last` is this key's last successful promote time (`None` = never).
    pub fn promote_on_ingest(&self, last: Option<std::time::Instant>) -> bool {
        match self {
            Self::OnCommit => true,
            Self::Checkpoint => false,
            Self::Interval { interval_ms } => match last {
                None => true,
                Some(at) => at.elapsed() >= Duration::from_millis(*interval_ms),
            },
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum RequestStoreConfig {
    Scylla(ScyllaConfig),
}

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
