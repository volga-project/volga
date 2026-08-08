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
    /// When true, workers run periodic `OperatorStore::maintain`.
    pub maintenance_enabled: bool,
    /// Cleaner tick interval in milliseconds.
    pub maintenance_interval_ms: u64,
}

impl Default for StateSpec {
    fn default() -> Self {
        Self {
            checkpoint_store: CheckpointStoreConfig::default(),
            operator_backend: OperatorStateBackendConfig::default(),
            request_store: None,
            maintenance_enabled: true,
            maintenance_interval_ms: 1_000,
        }
    }
}
