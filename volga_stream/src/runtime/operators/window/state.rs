use std::collections::HashMap;
use std::sync::Arc;
use dashmap::DashMap;
use datafusion::common::ScalarValue;
use datafusion::physical_plan::WindowExpr;

use crate::common::Key;
use crate::runtime::operators::window::time_index::TimeIdx;
use crate::runtime::operators::window::{TileConfig, Tiles};

pub type WindowId = usize;

pub type AccumulatorState = Vec<ScalarValue>;

#[derive(Debug, Clone)]
pub struct WindowState {
    pub tiles: Option<Tiles>,
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

    pub async fn get_or_create_windows_state(&self, key: &Key, window_ids: &[WindowId], tiling: &[Option<TileConfig>], window_exprs: &[Arc<dyn WindowExpr>]) -> WindowsState {
        self.window_states.get(key).map(|windows_state| windows_state.clone()).unwrap_or_else(|| {
            window_ids.iter().map(|&window_id| {
                (window_id, WindowState {
                    tiles: tiling.get(window_id).and_then(|tile_config| tile_config.as_ref().map(|config| Tiles::new(config.clone(), window_exprs[window_id].clone()))),
                    accumulator_state: None,
                    start_idx: TimeIdx { 
                        batch_id: uuid::Uuid::new_v4(),
                        timestamp: 0,
                        pos_idx: 0,
                        row_idx: 0,
                    },
                    end_idx: TimeIdx { 
                        batch_id: uuid::Uuid::new_v4(),
                        timestamp: 0,
                        pos_idx: 0,
                        row_idx: 0,
                    },
                })
            }).collect()
        })
    }

    pub async fn insert_windows_state(&self, key: &Key, windows_state: WindowsState) {
        self.window_states.insert(key.clone(), windows_state);
    }
}