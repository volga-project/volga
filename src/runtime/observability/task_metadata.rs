use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use serde::{Deserialize, Serialize};

/// Shared per-task key/value bag published into task and worker snapshots.
#[derive(Clone, Debug, Default)]
pub struct TaskMetadata {
    values: Arc<Mutex<HashMap<String, String>>>,
}

impl TaskMetadata {
    pub fn set(&self, key: &str, value: impl ToString) {
        self.values
            .lock()
            .expect("task metadata lock poisoned")
            .insert(key.to_string(), value.to_string());
    }

    pub fn increment_u64(&self, key: &str, delta: u64) {
        let mut values = self.values.lock().expect("task metadata lock poisoned");
        let next = values
            .get(key)
            .map(|value| value.parse::<u64>().expect("task metadata must be u64"))
            .unwrap_or_default()
            + delta;
        values.insert(key.to_string(), next.to_string());
    }

    pub fn get(&self, key: &str) -> Option<String> {
        self.values
            .lock()
            .expect("task metadata lock poisoned")
            .get(key)
            .cloned()
    }

    pub fn extend(&self, other: &Self) {
        let other = other
            .values
            .lock()
            .expect("task metadata lock poisoned")
            .clone();
        self.values
            .lock()
            .expect("task metadata lock poisoned")
            .extend(other);
    }

    pub fn is_empty(&self) -> bool {
        self.values
            .lock()
            .expect("task metadata lock poisoned")
            .is_empty()
    }
}

impl Serialize for TaskMetadata {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        self.values
            .lock()
            .expect("task metadata lock poisoned")
            .serialize(serializer)
    }
}

impl<'de> Deserialize for TaskMetadata {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let values = HashMap::<String, String>::deserialize(deserializer)?;
        Ok(Self {
            values: Arc::new(Mutex::new(values)),
        })
    }
}
