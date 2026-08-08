//! Worker-scoped registry: shared [`OperatorStore`]s + per-task [`OperatorTaskState`]s.

use std::any::Any;
use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use dashmap::DashMap;
use tokio::sync::Notify;

use crate::runtime::operators::OperatorKind;
use crate::runtime::VertexId;

use super::resource_tracker::StateResourceTracker;
use super::session::StateSessionHandle;
use super::store::OperatorStore;
use super::task_state::OperatorTaskState;

/// Worker-scoped state runtime: one session, stores keyed by kind, task states by vertex.
pub struct StateRegistry {
    session: StateSessionHandle,
    tracker: Arc<StateResourceTracker>,
    stores: Mutex<HashMap<OperatorKind, Arc<dyn Any + Send + Sync>>>,
    /// Stores that implement maintenance (same Arcs as `stores` when registered).
    operator_stores: Mutex<HashMap<OperatorKind, Arc<dyn OperatorStore>>>,
    task_states: DashMap<VertexId, Arc<dyn OperatorTaskState>>,
    notifies: DashMap<VertexId, Arc<Notify>>,
    maintenance_enabled: AtomicBool,
}

impl std::fmt::Debug for StateRegistry {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StateRegistry")
            .field("session", &self.session)
            .field("maintenance_enabled", &self.maintenance_enabled.load(Ordering::Relaxed))
            .field("store_count", &self.stores.lock().map(|m| m.len()).unwrap_or(0))
            .field("task_state_count", &self.task_states.len())
            .finish()
    }
}

impl Default for StateRegistry {
    fn default() -> Self {
        Self::new(
            StateSessionHandle::InMem,
            Arc::new(StateResourceTracker::new()),
        )
    }
}

impl StateRegistry {
    pub fn new(session: StateSessionHandle, tracker: Arc<StateResourceTracker>) -> Self {
        Self {
            session,
            tracker,
            stores: Mutex::new(HashMap::new()),
            operator_stores: Mutex::new(HashMap::new()),
            task_states: DashMap::new(),
            notifies: DashMap::new(),
            maintenance_enabled: AtomicBool::new(false),
        }
    }

    pub fn session(&self) -> &StateSessionHandle {
        &self.session
    }

    pub fn tracker(&self) -> &Arc<StateResourceTracker> {
        &self.tracker
    }

    pub fn set_maintenance_enabled(&self, enabled: bool) {
        self.maintenance_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn maintenance_enabled(&self) -> bool {
        self.maintenance_enabled.load(Ordering::Relaxed)
    }

    /// Get or create a shared store for `kind` (one backend per worker).
    pub fn get_or_insert_store<T, F>(&self, kind: OperatorKind, factory: F) -> Arc<T>
    where
        T: OperatorStore + Send + Sync + 'static,
        F: FnOnce() -> Arc<T>,
    {
        let mut stores = self.stores.lock().expect("state registry stores");
        if let Some(existing) = stores.get(&kind) {
            return existing
                .clone()
                .downcast::<T>()
                .expect("operator store kind type mismatch");
        }
        let created = factory();
        {
            let mut operator_stores = self.operator_stores.lock().expect("operator stores");
            operator_stores.insert(kind, created.clone() as Arc<dyn OperatorStore>);
        }
        stores.insert(kind, created.clone() as Arc<dyn Any + Send + Sync>);
        created
    }

    pub fn insert_task_state(&self, vertex_id: VertexId, state: Arc<dyn OperatorTaskState>) {
        let id = vertex_id.clone();
        self.task_states.insert(vertex_id, state);
        if let Some(n) = self.notifies.get(&id) {
            n.notify_waiters();
        }
    }

    pub fn remove_task_state(&self, vertex_id: &str) {
        self.task_states.remove(vertex_id);
    }

    pub fn get_task_state(&self, vertex_id: &str) -> Option<Arc<dyn OperatorTaskState>> {
        self.task_states
            .get(vertex_id)
            .map(|entry| entry.value().clone())
    }

    pub async fn wait_for_task_state(&self, vertex_id: &str) -> Arc<dyn OperatorTaskState> {
        loop {
            if let Some(state) = self.get_task_state(vertex_id) {
                return state;
            }
            let notify = self
                .notifies
                .entry(Arc::<str>::from(vertex_id))
                .or_insert_with(|| Arc::new(Notify::new()))
                .clone();
            if let Some(state) = self.get_task_state(vertex_id) {
                return state;
            }
            notify.notified().await;
        }
    }

    pub fn task_states(&self, kind: OperatorKind) -> Vec<Arc<dyn OperatorTaskState>> {
        self.task_states
            .iter()
            .filter(|entry| entry.value().kind() == kind)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// One cleaner tick: per registered store, maintain each matching task state.
    pub async fn run_maintenance_once(&self) -> Result<()> {
        let stores: Vec<(OperatorKind, Arc<dyn OperatorStore>)> = self
            .operator_stores
            .lock()
            .expect("operator stores")
            .iter()
            .map(|(k, s)| (*k, s.clone()))
            .collect();
        for (kind, store) in stores {
            let states = self.task_states(kind);
            let futs = states.iter().map(|state| {
                let store = store.clone();
                let state = state.clone();
                async move { store.maintain(state.as_ref()).await }
            });
            futures::future::try_join_all(futs).await?;
        }
        Ok(())
    }
}
