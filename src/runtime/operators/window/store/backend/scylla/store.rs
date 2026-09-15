use std::any::Any;
use std::collections::{HashMap, HashSet};
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Instant;

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use scylla::client::session::Session;
use tokio::sync::OnceCell;

use crate::api::spec::state::{ScyllaConfig, ServingPublish};
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, TileMap, TileRun, WindowTrigger,
};
use crate::runtime::operators::window::state::WATERMARK_UNSET;
use crate::runtime::state::{OperatorStore, OperatorTaskState, StateSessionHandle};

use super::cql::{
    prepare_stmts, PreparedDml, INSERT_KEY_STATES, INSERT_KG_BUCKETS, INSERT_LEASE_IF_NOT_EXISTS,
    INSERT_RAW, INSERT_TILES, INSERT_TRIGGERS, PUBLISH_LEASE, SELECT_KEY_STATE, SELECT_LEASE,
    SELECT_RAW, SELECT_TILES, SELECT_TRIGGERS, STEAL_LEASE,
};
use super::schema::TABLES;
use super::vis::overlay_visible;
use super::{checkpoint, read, triggers, write};
use crate::runtime::operators::window::store::backend::{
    StateVersion, WindowBackendSnapshot, WindowOperatorStore, WindowStoreTaskScope,
};

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
                    select_lease,
                    insert_lease_if_not_exists,
                    steal_lease,
                    publish_lease,
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
                        SELECT_LEASE,
                        INSERT_LEASE_IF_NOT_EXISTS,
                        STEAL_LEASE,
                        PUBLISH_LEASE,
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
                    select_lease,
                    insert_lease_if_not_exists,
                    steal_lease,
                    publish_lease,
                })
            })
            .await
            .map_err(|e| anyhow!("{e}"))
    }

    pub fn client(&self, scope: WindowStoreTaskScope) -> ScyllaWindowStoreClient {
        ScyllaWindowStoreClient {
            inner: Arc::new(self.clone()),
            scope,
            last_epoch: Arc::new(AtomicI64::new(0)),
            current_wm: Arc::new(AtomicI64::new(WATERMARK_UNSET)),
            restore_base: Arc::new(Mutex::new(None)),
            stolen_groups: Arc::new(Mutex::new(HashSet::new())),
            last_publish: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[derive(Clone)]
pub struct ScyllaWindowStoreClient {
    pub(super) inner: Arc<ScyllaWindowStore>,
    pub(super) scope: WindowStoreTaskScope,
    pub(super) last_epoch: Arc<AtomicI64>,
    current_wm: Arc<AtomicI64>,
    restore_base: Arc<Mutex<Option<StateVersion>>>,
    stolen_groups: Arc<Mutex<HashSet<i32>>>,
    last_publish: Arc<Mutex<HashMap<i32, Instant>>>,
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

    pub(super) fn inc_epoch(&self) -> i64 {
        self.last_epoch.fetch_add(1, Ordering::AcqRel) + 1
    }

    pub(super) fn writer_epoch(&self) -> i64 {
        self.last_epoch.load(Ordering::Acquire)
    }

    pub fn observe_watermark(&self, wm: Option<i64>) {
        self.current_wm
            .store(wm.unwrap_or(WATERMARK_UNSET), Ordering::Release);
    }

    pub(super) fn current_wm(&self) -> i64 {
        self.current_wm.load(Ordering::Acquire)
    }

    pub(super) fn serving_publish(&self) -> Option<ServingPublish> {
        self.inner.config.serving_publish.clone()
    }

    pub(super) fn set_restore_base(&self, version: StateVersion) {
        *self.restore_base.lock().expect("restore_base") = Some(version);
    }

    pub(super) fn overlay_visible(&self, attempt: &[u8], epoch: i64) -> bool {
        let cp = self.restore_base.lock().expect("restore_base");
        overlay_visible(&self.scope.attempt, cp.as_ref(), attempt, epoch)
    }

    pub(super) fn mark_stolen(&self, kg: i32) -> bool {
        self.stolen_groups.lock().expect("stolen_groups").insert(kg)
    }

    pub(super) fn stolen_groups(&self) -> Vec<i32> {
        self.stolen_groups
            .lock()
            .expect("stolen_groups")
            .iter()
            .copied()
            .collect()
    }

    pub(super) fn clear_stolen(&self) {
        self.stolen_groups.lock().expect("stolen_groups").clear();
        self.last_publish.lock().expect("last_publish").clear();
    }

    pub(super) fn last_published_at(&self, kg: i32) -> Option<Instant> {
        self.last_publish.lock().expect("last_publish").get(&kg).copied()
    }

    pub(super) fn note_published(&self, kg: i32, at: Instant) {
        self.last_publish.lock().expect("last_publish").insert(kg, at);
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
        checkpoint::checkpoint(self).await
    }

    async fn restore(&self, snapshot: &WindowBackendSnapshot) -> Result<()> {
        checkpoint::restore(self, snapshot).await
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
