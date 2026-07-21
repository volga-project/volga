use std::sync::Arc;

use crate::storage::SortedKV;

use super::event_store::EventStore;
use super::keys::StateNamespace;
use super::meta_store::MetaStore;
use super::tile_store::TileStore;

/// Per-operator handle to remote window state (one SortedKV client).
#[derive(Debug, Clone)]
pub struct WindowStateStore {
    pub kv: Arc<dyn SortedKV>,
    pub ns: StateNamespace,
    pub events: EventStore,
    pub tiles: TileStore,
    pub meta: MetaStore,
}

impl WindowStateStore {
    pub fn new(kv: Arc<dyn SortedKV>, ns: StateNamespace) -> Self {
        let events = EventStore::new(kv.clone(), ns.clone());
        let tiles = TileStore::new(kv.clone(), ns.clone());
        let meta = MetaStore::new(kv.clone(), ns.clone());
        Self {
            kv,
            ns,
            events,
            tiles,
            meta,
        }
    }

    pub async fn flush(&self) -> anyhow::Result<()> {
        self.kv.flush().await
    }
}
