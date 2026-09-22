use std::any::Any;
use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::{Arc, Mutex};

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use scylla::client::session::Session;
use tokio::sync::OnceCell;

use crate::api::spec::state::ScyllaConfig;
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, TileMap, TileRun, WindowTrigger,
};
use crate::runtime::operators::window::store::backend::{
    Attempt, CutHistory, Version, WindowBackendSnapshot, WindowOperatorStore, WindowStoreTaskScope,
};
use crate::runtime::state::{OperatorStore, OperatorTaskState, StateSessionHandle};

use super::cql::{
    prepare_stmts, PreparedDml, INSERT_KEY_STATES, INSERT_KG_BUCKETS, INSERT_RAW, INSERT_TILES,
    INSERT_TRIGGERS, SELECT_KEY_STATE, SELECT_RAW, SELECT_TILES,
    SELECT_TRIGGERS,
};
use super::schema::TABLES;
use super::{read, triggers, write};

#[derive(Default)]
struct GroupClock {
    next_epoch: u64,
    in_flight: BTreeSet<u64>,
}

#[derive(Clone)]
pub struct ScyllaWindowStore {
    pub(super) config: ScyllaConfig,
    session: Arc<Session>,
    prepared: Arc<OnceCell<PreparedDml>>,
}

impl std::fmt::Debug for ScyllaWindowStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScyllaWindowStore")
            .field("keyspace", &self.config.keyspace)
            .finish()
    }
}

impl ScyllaWindowStore {
    pub fn new(config: ScyllaConfig, session: Arc<Session>) -> Self {
        Self {
            config,
            session,
            prepared: Arc::new(OnceCell::new()),
        }
    }

    /// Test/tooling: connect a cluster and open a window store on it.
    pub async fn connect(config: ScyllaConfig) -> Result<Self> {
        let Some(handle) = StateSessionHandle::connect(
            &crate::api::spec::state::OperatorStateBackendConfig::Scylla(config.clone()),
        )
        .await?
        else {
            anyhow::bail!("Scylla backend produced no session");
        };
        Ok(Self::new(config, Arc::clone(handle.scylla())))
    }

    pub(super) fn session(&self) -> Arc<Session> {
        Arc::clone(&self.session)
    }

    pub(super) async fn prepared(&self) -> Result<&PreparedDml> {
        self.prepared
            .get_or_try_init(|| async {
                let session = self.session();
                futures::future::try_join_all(
                    TABLES.iter().map(|table| session.query_unpaged(*table, &[])),
                )
                .await?;
                let [
                    insert_raw,
                    insert_kg_buckets,
                    insert_tiles,
                    insert_key_states,
                    insert_triggers,
                    select_key_state,
                    select_raw,
                    select_tiles,
                    select_triggers,
                ] = prepare_stmts(
                    session.as_ref(),
                    [
                        INSERT_RAW,
                        INSERT_KG_BUCKETS,
                        INSERT_TILES,
                        INSERT_KEY_STATES,
                        INSERT_TRIGGERS,
                        SELECT_KEY_STATE,
                        SELECT_RAW,
                        SELECT_TILES,
                        SELECT_TRIGGERS,
                    ],
                )
                .await?;
                Ok::<_, anyhow::Error>(PreparedDml {
                    insert_raw,
                    insert_kg_buckets,
                    insert_tiles,
                    insert_key_states,
                    insert_triggers,
                    select_key_state,
                    select_raw,
                    select_tiles,
                    select_triggers,
                })
            })
            .await
            .map_err(|e| anyhow!("{e}"))
    }

    pub fn client(&self, scope: WindowStoreTaskScope) -> ScyllaWindowStoreClient {
        ScyllaWindowStoreClient {
            inner: Arc::new(self.clone()),
            scope,
            groups: Arc::new(Mutex::new(HashMap::new())),
            in_flight_keys: Arc::new(Mutex::new(HashSet::new())),
            cp_cut: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Clone)]
pub struct ScyllaWindowStoreClient {
    pub(super) inner: Arc<ScyllaWindowStore>,
    pub(super) scope: WindowStoreTaskScope,
    groups: Arc<Mutex<HashMap<i32, GroupClock>>>,
    in_flight_keys: Arc<Mutex<HashSet<Vec<u8>>>>,
    /// Restored overlay; empty until #288 restore.
    cp_cut: Arc<Mutex<HashMap<i32, CutHistory>>>,
}

impl std::fmt::Debug for ScyllaWindowStoreClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ScyllaWindowStoreClient")
            .field("keyspace", &self.inner.config.keyspace)
            .finish()
    }
}

impl ScyllaWindowStoreClient {
    pub(super) fn key_group(&self, partition: &PartitionKey) -> Result<i32> {
        anyhow::ensure!(
            partition.namespace.as_slice() == self.scope.namespace.bytes.as_slice(),
            "partition namespace is outside this client scope"
        );
        let kg = partition.key_group(self.scope.max_parallelism);
        anyhow::ensure!(
            self.scope.key_group_range.contains(kg),
            "partition key_group is outside this task's bound range"
        );
        Ok(kg as i32)
    }

    pub(super) fn my_attempt(&self) -> Attempt {
        self.scope.attempt
    }

    /// Allocate the next epoch for `kg` and mark it in-flight.
    pub(super) fn alloc_epoch(&self, kg: i32) -> u64 {
        let mut groups = self.groups.lock().expect("groups");
        let clock = groups.entry(kg).or_default();
        let epoch = clock.next_epoch;
        clock.next_epoch += 1;
        clock.in_flight.insert(epoch);
        epoch
    }

    pub(super) fn ack_epoch(&self, kg: i32, epoch: u64) {
        let mut groups = self.groups.lock().expect("groups");
        if let Some(clock) = groups.get_mut(&kg) {
            clock.in_flight.remove(&epoch);
        }
    }

    #[allow(dead_code)]
    pub(super) fn cut_top(&self, kg: i32) -> Option<u64> {
        let groups = self.groups.lock().expect("groups");
        let clock = groups.get(&kg)?;
        let first = clock.in_flight.iter().next().copied();
        match first {
            Some(0) => None,
            Some(h) => Some(h - 1),
            None if clock.next_epoch == 0 => None,
            None => Some(clock.next_epoch - 1),
        }
    }

    pub(super) fn begin_key(&self, key: &[u8]) -> Result<()> {
        let mut keys = self.in_flight_keys.lock().expect("in_flight_keys");
        anyhow::ensure!(
            keys.insert(key.to_vec()),
            "commit already in flight for this key"
        );
        Ok(())
    }

    pub(super) fn end_key(&self, key: &[u8]) {
        self.in_flight_keys
            .lock()
            .expect("in_flight_keys")
            .remove(key);
    }

    #[allow(dead_code)]
    pub(super) fn set_cp_cut(&self, kg: i32, cut: CutHistory) {
        self.cp_cut.lock().expect("cp_cut").insert(kg, cut);
    }

    pub(super) fn overlay_visible(&self, kg: i32, attempt: i64, epoch: i64) -> bool {
        let v = Version {
            attempt: attempt as Attempt,
            epoch: epoch as u64,
        };
        if v.attempt == self.scope.attempt {
            return true;
        }
        let cuts = self.cp_cut.lock().expect("cp_cut");
        cuts.get(&kg).is_some_and(|cut| cut.allows(v))
    }
}

#[async_trait]
impl WindowOperatorStore for ScyllaWindowStoreClient {
    async fn load_key_state(&self, partition: &PartitionKey) -> Result<KeyState> {
        read::load_key_state(self, partition).await
    }

    async fn load_raw(
        &self,
        partition: &PartitionKey,
        runs: &[RawRun],
    ) -> Result<Vec<RecordBatch>> {
        read::load_raw(self, partition, runs).await
    }

    async fn load_tiles(&self, partition: &PartitionKey, runs: &[TileRun]) -> Result<TileMap> {
        read::load_tiles(self, partition, runs).await
    }

    async fn commit_events(
        &self,
        partition: &PartitionKey,
        ts_column_index: usize,
        events: &RecordBatch,
        tiles: &TileMap,
        meta: &KeyState,
        triggers: &[WindowTrigger],
    ) -> Result<()> {
        write::commit_events(self, partition, ts_column_index, events, tiles, meta, triggers)
            .await
    }

    async fn load_triggers(
        &self,
        after: Option<Cursor>,
        through: Cursor,
        resume: Option<&crate::runtime::operators::window::store::TriggerResume>,
        limit: usize,
    ) -> Result<(
        Vec<WindowTrigger>,
        Option<crate::runtime::operators::window::store::TriggerResume>,
    )> {
        triggers::load_triggers(self, after, through, resume, limit).await
    }

    async fn store_key_state(&self, partition: &PartitionKey, state: &KeyState) -> Result<()> {
        write::store_key_state(self, partition, state).await
    }

    async fn checkpoint(&self) -> Result<WindowBackendSnapshot> {
        anyhow::bail!("Scylla checkpoint lands in feat/scylla-wo-checkpoint")
    }

    async fn restore(&self, _snapshot: &WindowBackendSnapshot) -> Result<()> {
        anyhow::bail!("Scylla restore lands in feat/scylla-wo-checkpoint")
    }
}

#[async_trait]
impl OperatorStore for ScyllaWindowStoreClient {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn maintain(
        &self,
        _ns: &crate::runtime::operators::window::model::StateNamespace,
        _state: &dyn OperatorTaskState,
    ) -> Result<()> {
        anyhow::bail!("Scylla maintain lands in feat/scylla-wo-maintain")
    }
}

#[async_trait]
impl OperatorStore for ScyllaWindowStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn maintain(
        &self,
        _ns: &crate::runtime::operators::window::model::StateNamespace,
        _state: &dyn OperatorTaskState,
    ) -> Result<()> {
        anyhow::bail!("Scylla maintain lands in feat/scylla-wo-maintain")
    }
}
