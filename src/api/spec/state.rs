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

#[derive(Clone, Debug, Default, Serialize, Deserialize, JsonSchema)]
pub struct StateSpec {
    #[serde(default)]
    pub checkpoint_store: CheckpointStoreConfig,
    #[serde(default)]
    pub operator_backend: OperatorStateBackendConfig,
    #[serde(default)]
    pub request_store: Option<RequestStoreConfig>,
}
