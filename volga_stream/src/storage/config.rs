use std::collections::HashMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

/// Remote backend settings for a SlateDB+Foyer+ObjectStore `StorageBackend`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteBackendConfig {
    /// SlateDB DB path/prefix (inside the object store prefix).
    pub db_path: String,
    /// Object store prefix root (local filesystem for now).
    pub object_store_root: String,
    /// Local disk path for Foyer hybrid cache.
    pub foyer_disk_path: String,
    pub foyer_memory_bytes: usize,
    pub foyer_disk_bytes: usize,
    pub checkpoint_lifetime_secs: u64,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum StorageBackendSelection {
    InMem,
    Remote(RemoteBackendConfig),
}

pub fn parse_storage_backend_from_job_config(
    job_config: &HashMap<String, Value>,
) -> Option<StorageBackendSelection> {
    // Backward-compatible key name (legacy config block).
    let v = job_config.get("remote_batch_store")?;
    let obj = v.as_object()?;
    if obj.get("enabled").and_then(|v| v.as_bool()) != Some(true) {
        return None;
    }

    Some(StorageBackendSelection::Remote(RemoteBackendConfig {
        db_path: obj.get("db_path")?.as_str()?.to_string(),
        object_store_root: obj.get("object_store_root")?.as_str()?.to_string(),
        foyer_disk_path: obj.get("foyer_disk_path")?.as_str()?.to_string(),
        foyer_memory_bytes: obj.get("foyer_memory_bytes")?.as_u64()? as usize,
        foyer_disk_bytes: obj.get("foyer_disk_bytes")?.as_u64()? as usize,
        checkpoint_lifetime_secs: obj.get("checkpoint_lifetime_secs")?.as_u64()?,
    }))
}

pub fn storage_backend_to_job_config_value(sel: &StorageBackendSelection) -> Value {
    match sel {
        StorageBackendSelection::InMem => {
            let mut m = serde_json::Map::new();
            m.insert("enabled".to_string(), Value::Bool(false));
            Value::Object(m)
        }
        StorageBackendSelection::Remote(cfg) => {
            let mut m = serde_json::Map::new();
            m.insert("enabled".to_string(), Value::Bool(true));
            m.insert("db_path".to_string(), Value::String(cfg.db_path.clone()));
            m.insert(
                "object_store_root".to_string(),
                Value::String(cfg.object_store_root.clone()),
            );
            m.insert(
                "foyer_disk_path".to_string(),
                Value::String(cfg.foyer_disk_path.clone()),
            );
            m.insert(
                "foyer_memory_bytes".to_string(),
                Value::from(cfg.foyer_memory_bytes as u64),
            );
            m.insert(
                "foyer_disk_bytes".to_string(),
                Value::from(cfg.foyer_disk_bytes as u64),
            );
            m.insert(
                "checkpoint_lifetime_secs".to_string(),
                Value::from(cfg.checkpoint_lifetime_secs),
            );
            Value::Object(m)
        }
    }
}

