use std::collections::HashMap;
use dashmap::DashMap;
use datafusion::common::ScalarValue;

use crate::common::Key;
use crate::runtime::operators::window::time_index::TimeIdx;

pub type WindowId = usize;

#[derive(Debug, Clone)]
pub struct WindowState {
    pub accumulator_state: Option<Vec<ScalarValue>>,
    pub start_idx: TimeIdx,
    pub end_idx: TimeIdx,
}

#[derive(Debug)]
pub struct State {
    window_states: DashMap<Key, HashMap<WindowId, WindowState>>,
}

impl State {
    pub fn new() -> Self {
        Self {
            window_states: DashMap::new(),
        }
    }

    pub async fn get_window_state(&self, key: &Key, window_id: WindowId) -> Option<WindowState> {
        self.window_states.get(key)
            .and_then(|window_states| window_states.get(&window_id).cloned())
    }

    pub async fn insert_window_state(&self, key: &Key, window_id: WindowId, window_state: WindowState) {
        self.window_states.entry(key.clone()).or_insert(HashMap::new()).insert(window_id, window_state);
    }
}