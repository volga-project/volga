use std::collections::HashMap;
use dashmap::DashMap;
use datafusion::common::ScalarValue;

use crate::common::Key;
use crate::runtime::operators::window::time_index::TimeIdx;

pub type WindowId = usize;

pub type AccumulatorState = Vec<ScalarValue>;

#[derive(Debug, Clone)]
pub struct WindowState {
    pub accumulator_state: Option<AccumulatorState>,
    pub start_idx: TimeIdx,
    pub end_idx: TimeIdx,
}

pub type WindowsState = HashMap<WindowId, WindowState>;

#[derive(Debug)]
pub struct State {
    window_states: DashMap<Key, WindowsState>,
}

impl State {
    pub fn new() -> Self {
        Self {
            window_states: DashMap::new(),
        }
    }

    pub async fn get_windows_state(&self, key: &Key) -> Option<WindowsState> {
        self.window_states.get(key)
            .map(|window_states| window_states.clone())
    }

    pub async fn insert_windows_state(&self, key: &Key, windows_state: WindowsState) {
        self.window_states.insert(key.clone(), windows_state);
    }
}