use std::any::Any;
use std::sync::Arc;
use dashmap::DashMap;

pub trait OperatorState: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Debug)]
pub struct OperatorStates {
    states: DashMap<String, Arc<dyn OperatorState>>,
}

impl OperatorStates {
    pub fn new() -> Self {
        Self {
            states: DashMap::new(),
        }
    }

    pub fn insert_operator_state(&self, vertex_id: String, state: Arc<dyn OperatorState>) {
        self.states.insert(vertex_id, state);
    }

    pub fn get_operator_state(&self, vertex_id: &str) -> Option<Arc<dyn OperatorState>> {
        self.states.get(vertex_id).map(|entry| entry.value().clone())
    }

    pub fn get_or_insert_operator_state<F>(&self, vertex_id: &str, factory: F) -> Arc<dyn OperatorState>
    where
        F: FnOnce() -> Arc<dyn OperatorState>,
    {
        if let Some(state) = self.states.get(vertex_id) {
            state.value().clone()
        } else {
            let new_state = factory();
            self.states.insert(vertex_id.to_string(), new_state.clone());
            new_state
        }
    }
}

impl Default for OperatorStates {
    fn default() -> Self {
        Self::new()
    }
}

