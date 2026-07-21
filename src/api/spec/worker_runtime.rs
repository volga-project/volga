use std::time::Duration;

use serde::{Deserialize, Serialize};

use crate::transport::transport_spec::TransportSpec;

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct WorkerRuntimeSpec {
    #[serde(default)]
    pub transport: TransportSpec,
    /// SortedKV key namespace shared by WO and WRO clients.
    #[serde(default = "default_window_state_namespace")]
    pub window_state_namespace: String,
    /// Snapshot history retention window, in milliseconds.
    /// When omitted, defaults to 10 minutes.
    #[serde(default)]
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
