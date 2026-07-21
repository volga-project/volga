use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::transport::transport_spec::TransportSpec;

#[derive(Clone, Debug, Serialize)]
pub struct WorkerRuntimeSpec {
    pub transport: TransportSpec,
    /// SortedKV key namespace shared by WO and WRO clients.
    pub window_state_namespace: String,
    /// Snapshot history retention window, in milliseconds.
    /// When omitted, defaults to 10 minutes.
    pub history_retention_window_ms: Option<u64>,
}

fn default_window_state_namespace() -> String {
    "window_state".to_string()
}

impl Default for WorkerRuntimeSpec {
    fn default() -> Self {
        Self {
            transport: TransportSpec::default(),
            window_state_namespace: default_window_state_namespace(),
            history_retention_window_ms: None,
        }
    }
}

impl WorkerRuntimeSpec {
    pub fn history_retention_window(&self) -> Duration {
        let ms = self
            .history_retention_window_ms
            .unwrap_or(10 * 60 * 1000)
            .max(1);
        Duration::from_millis(ms)
    }
}

#[derive(Deserialize)]
struct WorkerRuntimeSpecDe {
    #[serde(default)]
    transport: TransportSpec,
    #[serde(default)]
    window_state_namespace: Option<String>,
    /// Legacy nested form: `storage.state_namespace`.
    #[serde(default)]
    storage: Option<LegacyStorageDe>,
    history_retention_window_ms: Option<u64>,
}

#[derive(Deserialize)]
struct LegacyStorageDe {
    #[serde(default)]
    state_namespace: Option<String>,
}

impl<'de> Deserialize<'de> for WorkerRuntimeSpec {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let raw = WorkerRuntimeSpecDe::deserialize(deserializer)?;
        let window_state_namespace = raw
            .window_state_namespace
            .or_else(|| raw.storage.and_then(|s| s.state_namespace))
            .unwrap_or_else(default_window_state_namespace);
        Ok(Self {
            transport: raw.transport,
            window_state_namespace,
            history_retention_window_ms: raw.history_retention_window_ms,
        })
    }
}
