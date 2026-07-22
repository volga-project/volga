use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowId};
use crate::runtime::utils;
use crate::storage::{SortedKV, WriteBatch};

use super::keys::{key_state_key, StateNamespace};

/// Single meta blob per stream key.
///
/// - `max_seen`: last ingested cursor; next seq = max_seen.seq_no + 1 (or 0).
/// - `processed_pos`: WO advance frontier (shared across windows on this key).
/// - `first_ingested`: first accepted ingest cursor — cold coverage lower bound only
///   (before `processed_pos` exists). Not maintained on prune/retract.
/// - `accumulators`: per-window retractable state.
#[serde_as]
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct KeyState {
    pub max_seen: Option<Cursor>,
    pub processed_pos: Option<Cursor>,
    #[serde_as(as = "BTreeMap<_, Vec<utils::ScalarValueAsBytes>>")]
    pub accumulators: BTreeMap<WindowId, AccumulatorState>,
    /// Cold-plan origin; see struct docs. Not updated on prune.
    #[serde(default)]
    pub first_ingested: Option<Cursor>,
}

impl KeyState {
    pub fn next_seq(&self) -> u64 {
        self.max_seen
            .map(|c| c.seq_no.saturating_add(1))
            .unwrap_or(0)
    }
}

#[derive(Debug, Clone)]
pub struct MetaStore {
    kv: Arc<dyn SortedKV>,
    ns: StateNamespace,
}

impl MetaStore {
    pub fn new(kv: Arc<dyn SortedKV>, ns: StateNamespace) -> Self {
        Self { kv, ns }
    }

    pub async fn get_key(&self, key: &Key) -> Result<KeyState> {
        let k = key_state_key(&self.ns, key);
        match self.kv.get(&k).await? {
            Some(bytes) => Ok(bincode::deserialize(&bytes)?),
            None => Ok(KeyState::default()),
        }
    }

    pub async fn put_key(&self, key: &Key, state: &KeyState) -> Result<()> {
        let mut wb = WriteBatch::with_capacity(1);
        self.append_put_key(&mut wb, key, state)?;
        self.kv.write(wb).await
    }

    /// Append meta put into `wb` (does not commit).
    pub fn append_put_key(&self, wb: &mut WriteBatch, key: &Key, state: &KeyState) -> Result<()> {
        let k = key_state_key(&self.ns, key);
        let bytes = bincode::serialize(state)?;
        wb.put(k, bytes);
        Ok(())
    }
}
