//! Worker-scoped registry: shared [`OperatorStore`]s + per-task [`OperatorTaskState`]s.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use anyhow::Result;
use dashmap::DashMap;

use crate::runtime::metrics::MetricsLabels;
use crate::runtime::operators::OperatorKind;
use crate::runtime::VertexId;

use super::resource_tracker::StateResourceTracker;
use super::session::StateSessionHandle;
use super::store::OperatorStore;
use super::task_state::OperatorTaskState;

/// Worker-scoped state runtime: optional session, stores keyed by kind, task states by vertex.
pub struct StateRegistry {
    session: Option<StateSessionHandle>,
    tracker: Arc<StateResourceTracker>,
    stores: Mutex<HashMap<OperatorKind, Arc<dyn OperatorStore>>>,
    task_states: DashMap<VertexId, Arc<dyn OperatorTaskState>>,
    maintenance_enabled: AtomicBool,
    /// Worker-scoped Prom/registry labels; copied onto stores at create.
    metrics_labels: Option<MetricsLabels>,
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
        Self::new(None, Arc::new(StateResourceTracker::new()), None)
    }
}

impl StateRegistry {
    pub fn new(
        session: Option<StateSessionHandle>,
        tracker: Arc<StateResourceTracker>,
        metrics_labels: Option<MetricsLabels>,
    ) -> Self {
        Self {
            session,
            tracker,
            stores: Mutex::new(HashMap::new()),
            task_states: DashMap::new(),
            maintenance_enabled: AtomicBool::new(true),
            metrics_labels,
        }
    }

    pub fn session(&self) -> Option<&StateSessionHandle> {
        self.session.as_ref()
    }

    pub fn tracker(&self) -> &Arc<StateResourceTracker> {
        &self.tracker
    }

    pub fn metrics_labels(&self) -> Option<&MetricsLabels> {
        self.metrics_labels.as_ref()
    }

    pub fn set_maintenance_enabled(&self, enabled: bool) {
        self.maintenance_enabled.store(enabled, Ordering::Relaxed);
    }

    pub fn maintenance_enabled(&self) -> bool {
        self.maintenance_enabled.load(Ordering::Relaxed)
    }

    /// Get or create a shared store for `kind` (one backend per worker).
    pub fn get_or_insert_store<F>(&self, kind: OperatorKind, factory: F) -> Arc<dyn OperatorStore>
    where
        F: FnOnce(Option<&StateSessionHandle>) -> Arc<dyn OperatorStore>,
    {
        let mut stores = self.stores.lock().expect("state registry stores");
        if let Some(existing) = stores.get(&kind) {
            return existing.clone();
        }
        let created = factory(self.session.as_ref());
        stores.insert(kind, created.clone());
        created
    }

    pub fn insert_task_state(&self, vertex_id: VertexId, state: Arc<dyn OperatorTaskState>) {
        self.task_states.insert(vertex_id, state);
    }

    pub fn remove_task_state(&self, vertex_id: &str) {
        self.task_states.remove(vertex_id);
    }

    pub fn get_task_state(&self, vertex_id: &str) -> Option<Arc<dyn OperatorTaskState>> {
        self.task_states
            .get(vertex_id)
            .map(|entry| entry.value().clone())
    }

    pub fn task_states(&self, kind: OperatorKind) -> Vec<Arc<dyn OperatorTaskState>> {
        self.task_states
            .iter()
            .filter(|entry| entry.value().kind() == kind)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// One cleaner tick: one parallel loop per op kind; within a kind, all task states join.
    pub async fn run_maintenance_once(&self) -> Result<()> {
        let work: Vec<(OperatorKind, Arc<dyn OperatorStore>)> = self
            .stores
            .lock()
            .expect("operator stores")
            .iter()
            .map(|(k, s)| (*k, s.clone()))
            .collect();
        let kind_futs = work.into_iter().map(|(kind, store)| {
            let states = self.task_states(kind);
            async move {
                let futs = states
                    .iter()
                    .map(|state| store.maintain(state.state_namespace(), state.as_ref()));
                futures::future::try_join_all(futs).await
            }
        });
        futures::future::try_join_all(kind_futs).await?;
        Ok(())
    }
}
