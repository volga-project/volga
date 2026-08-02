use std::collections::HashMap;
use std::sync::{Arc, Mutex};

/// Task-local operator data published into task snapshots for test and debugging use.
#[derive(Clone, Debug, Default)]
pub struct TaskMetadataReporter {
    values: Arc<Mutex<HashMap<String, String>>>,
}

impl TaskMetadataReporter {
    pub fn set(&self, key: &str, value: impl ToString) {
        self.values
            .lock()
            .expect("task metadata lock poisoned")
            .insert(key.to_string(), value.to_string());
    }

    pub fn increment_u64(&self, key: &str, delta: u64) {
        let mut values = self.values.lock().expect("task metadata lock poisoned");
        let value = values
            .get(key)
            .map(|value| value.parse::<u64>().expect("task metadata must be u64"))
            .unwrap_or_default()
            + delta;
        values.insert(key.to_string(), value.to_string());
    }

    pub fn snapshot(&self) -> HashMap<String, String> {
        self.values
            .lock()
            .expect("task metadata lock poisoned")
            .clone()
    }
}
