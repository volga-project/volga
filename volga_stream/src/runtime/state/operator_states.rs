use std::any::Any;
use std::sync::Arc;
use dashmap::DashMap;
use tokio::sync::Notify;

use crate::runtime::VertexId;

pub trait OperatorState: Send + Sync + std::fmt::Debug {
    fn as_any(&self) -> &dyn Any;
    fn as_any_mut(&mut self) -> &mut dyn Any;
}

#[derive(Debug)]
pub struct OperatorStates {
    states: DashMap<VertexId, Arc<dyn OperatorState>>,
    notifies: DashMap<VertexId, Arc<Notify>>,
}

impl OperatorStates {
    pub fn new() -> Self {
        Self {
            states: DashMap::new(),
            notifies: DashMap::new(),
        }
    }

    pub fn insert_operator_state(&self, vertex_id: VertexId, state: Arc<dyn OperatorState>) {
        let id = vertex_id.clone();
        self.states.insert(vertex_id.clone(), state);
        if let Some(n) = self.notifies.get(&id) {
            n.notify_waiters();
        }
    }

    pub fn get_operator_state(&self, vertex_id: &str) -> Option<Arc<dyn OperatorState>> {
        self.states.get(vertex_id).map(|entry| entry.value().clone())
    }

    pub async fn wait_for_operator_state(&self, vertex_id: &str) -> Arc<dyn OperatorState> {
        loop {
            if let Some(state) = self.get_operator_state(vertex_id) {
                return state;
            }
            let notify = self
                .notifies
                .entry(Arc::<str>::from(vertex_id))
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone();
            // Re-check to avoid missed notifications between the first check and registration.
            if let Some(state) = self.get_operator_state(vertex_id) {
                return state;
            }
            notify.notified().await;
        }
    }

    pub fn get_or_insert_operator_state<F>(&self, vertex_id: &str, factory: F) -> Arc<dyn OperatorState>
    where
        F: FnOnce() -> Arc<dyn OperatorState>,
    {
        if let Some(state) = self.states.get(vertex_id) {
            state.value().clone()
        } else {
            let new_state = factory();
            let id: VertexId = Arc::<str>::from(vertex_id);
            self.states.insert(id.clone(), new_state.clone());
            if let Some(n) = self.notifies.get(vertex_id) {
                n.notify_waiters();
            }
            new_state
        }
    }
}

impl Default for OperatorStates {
    fn default() -> Self {
        Self::new()
    }
}

