use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Hash, PartialEq, Eq, Serialize, Deserialize)]
pub struct TaskKey {
    pub vertex_id: String,
    pub task_index: i32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedCheckpoint(Vec<u8>);

impl SerializedCheckpoint {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SerializedRestore(Vec<u8>);

impl SerializedRestore {
    pub fn new(bytes: Vec<u8>) -> Self {
        Self(bytes)
    }

    pub fn into_bytes(self) -> Vec<u8> {
        self.0
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompletedCheckpoint {
    pub checkpoint_id: u64,
    pub tasks: HashMap<TaskKey, SerializedCheckpoint>,
}

#[derive(Debug, Clone)]
pub struct RestorePlan {
    pub tasks: HashMap<TaskKey, SerializedRestore>,
}
