use anyhow::Result;
use serde::{Deserialize, Serialize};
use serde_with::serde_as;
use std::collections::BTreeMap;
use std::sync::Arc;

use crate::common::Key;
use crate::runtime::operators::window::cursor::Cursor;
use crate::runtime::operators::window::window_operator_state::{AccumulatorState, WindowId};
use crate::runtime::utils;
use crate::storage::{KvSnapshot, SortedKV, WriteBatch};

use super::keys::{key_state_key, StateNamespace};

/// Single meta blob per stream key.
///
/// - `max_seen`: max ingested [`Cursor`] `(ts, seq)` (event-time frontier).
/// - `next_seq`: next row `seq_no` to assign (monotonic ingest allocator; **not**
///   derived from `max_seen.seq_no`, which is ts-first).
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
    /// Next `__seq_no` to assign on ingest. Independent of `max_seen`.
    #[serde(default)]
    pub next_seq: u64,
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

    /// Meta get on an open partition snapshot (same epoch as data scans).
    pub async fn get_key_on(&self, snap: &dyn KvSnapshot, key: &Key) -> Result<KeyState> {
        let k = key_state_key(&self.ns, key);
        match snap.get(&k).await? {
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
