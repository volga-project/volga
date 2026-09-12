use std::any::Any;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{anyhow, Result};
use arrow::array::RecordBatch;
use async_trait::async_trait;
use dashmap::DashMap;
use scylla::client::session::Session;
use tokio::sync::OnceCell;
use tokio::sync::Mutex as AsyncMutex;

use crate::api::spec::state::ScyllaConfig;
use crate::runtime::operators::window::model::{
    Cursor, KeyState, PartitionKey, RawRun, TileMap, TileRun, WindowTrigger,
};
use crate::runtime::state::{OperatorStore, OperatorTaskState, StateSessionHandle};

use super::cql::{
    encode_owner_writer, prepare_stmts, HeadClaim, PreparedDml, DELETE_KG_BUCKETS,
    DELETE_KEY_STATE_VERSION, DELETE_RAW, DELETE_RAW_VERSION, DELETE_TILES, DELETE_TILE_VERSION,
    INSERT_HEAD_IF_NOT_EXISTS, INSERT_KEY_STATES, INSERT_KG_BUCKETS, INSERT_RAW,
    INSERT_RECOVERY_BASES, INSERT_TILES, INSERT_TRIGGERS, SELECT_HEAD, SELECT_HEAD_VERSIONS,
    SELECT_KEY_STATE, SELECT_KEY_STATE_VERSIONS, SELECT_KG_BUCKETS, SELECT_RAW, SELECT_RAW_VERSIONS,
    SELECT_RECOVERY_BASES, SELECT_TILES, SELECT_TILE_VERSIONS, SELECT_TRIGGERS, SELECT_TRIGGERS_AFTER,
    UPDATE_HEAD_PROMOTE_SERVING, UPDATE_HEAD_STEAL_OWNER,
};
use super::schema::TABLES;
use super::{checkpoint, maintain, read, triggers, write};
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
                    steal_head_if_owner,
                    promote_head_if_owner,
                    insert_head_if_not_exists,
                    insert_recovery_bases,
                    select_key_state,
                    select_raw,
                    select_tiles,
                    select_triggers,
                    select_triggers_after,
                    select_kg_buckets,
                    select_head_versions,
                    select_recovery_bases,
                    select_key_state_versions,
                    select_raw_versions,
                    select_tile_versions,
                    delete_raw,
                    delete_tiles,
                    delete_kg_buckets,
                    delete_key_state_version,
                    delete_raw_version,
                    delete_tile_version,
                    select_head,
                ] = prepare_stmts(
                    session.as_ref(),
                    [
                        INSERT_RAW,
                        INSERT_KG_BUCKETS,
                        INSERT_TILES,
                        INSERT_KEY_STATES,
                        INSERT_TRIGGERS,
                        UPDATE_HEAD_STEAL_OWNER,
                        UPDATE_HEAD_PROMOTE_SERVING,
                        INSERT_HEAD_IF_NOT_EXISTS,
                        INSERT_RECOVERY_BASES,
                        SELECT_KEY_STATE,
                        SELECT_RAW,
                        SELECT_TILES,
                        SELECT_TRIGGERS,
                        SELECT_TRIGGERS_AFTER,
                        SELECT_KG_BUCKETS,
                        SELECT_HEAD_VERSIONS,
                        SELECT_RECOVERY_BASES,
                        SELECT_KEY_STATE_VERSIONS,
                        SELECT_RAW_VERSIONS,
                        SELECT_TILE_VERSIONS,
                        DELETE_RAW,
                        DELETE_TILES,
                        DELETE_KG_BUCKETS,
                        DELETE_KEY_STATE_VERSION,
                        DELETE_RAW_VERSION,
                        DELETE_TILE_VERSION,
                        SELECT_HEAD,
                    ],
                )
                .await?;
                let mut prepared = PreparedDml {
                    insert_raw,
                    insert_kg_buckets,
                    insert_tiles,
                    insert_key_states,
                    insert_triggers,
                    steal_head_if_owner,
                    promote_head_if_owner,
                    insert_head_if_not_exists,
                    insert_recovery_bases,
                    select_key_state,
                    select_raw,
                    select_tiles,
                    select_triggers,
                    select_triggers_after,
                    select_kg_buckets,
                    select_head_versions,
                    select_recovery_bases,
                    select_key_state_versions,
                    select_raw_versions,
                    select_tile_versions,
                    delete_raw,
                    delete_tiles,
                    delete_kg_buckets,
                    delete_key_state_version,
                    delete_raw_version,
                    delete_tile_version,
                    select_head,
                };
                prepared.mark_idempotent_dml();
                Ok::<_, anyhow::Error>(prepared)
            })
            .await
            .map_err(|e| anyhow!("{e}"))
    }

    pub fn client(&self, scope: WindowStoreTaskScope) -> ScyllaWindowStoreClient {
        ScyllaWindowStoreClient {
            inner: Arc::new(self.clone()),
            scope,
            last_epoch: Arc::new(AtomicI64::new(0)),
            restore_base: Arc::new(AsyncMutex::new(None)),
            head_claims: Arc::new(DashMap::new()),
            last_promoted: Arc::new(DashMap::new()),
        }
    }
}

#[derive(Clone)]
pub struct ScyllaWindowStoreClient {
    pub(super) inner: Arc<ScyllaWindowStore>,
    pub(super) scope: WindowStoreTaskScope,
    pub(super) last_epoch: Arc<AtomicI64>,
    pub(super) restore_base: Arc<AsyncMutex<Option<StateVersion>>>,
    pub(super) head_claims: Arc<DashMap<Vec<u8>, HeadClaim>>,
    pub(super) last_promoted: Arc<DashMap<Vec<u8>, (i64, Instant)>>,
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

    pub(super) fn owner_writer(&self) -> Vec<u8> {
        encode_owner_writer(&self.scope.attempt, &self.scope.writer_id.0)
    }

    pub(super) async fn head_claim(&self, key: &[u8]) -> HeadClaim {
        if let Some(claim) = self.head_claims.get(key) {
            return *claim;
        }
        if self.restore_base.lock().await.is_some() {
            HeadClaim::Steal
        } else {
            HeadClaim::Empty
        }
    }

    pub(super) fn overlay_ok(&self, attempt: &[u8], epoch: i64, writer_epoch: Option<i64>) -> bool {
        if attempt == self.scope.attempt.as_slice() {
            return writer_epoch.map_or(true, |cut| epoch <= cut);
        }
        false
    }

    pub(super) async fn overlay_visible(&self, attempt: &[u8], epoch: i64) -> bool {
        if self.overlay_ok(attempt, epoch, None) {
            return true;
        }
        let base = self.restore_base.lock().await;
        match base.as_ref() {
            Some(base) => attempt == base.attempt.as_slice() && epoch <= base.epoch as i64,
            None => false,
        }
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
        ns: &crate::runtime::operators::window::model::StateNamespace,
        state: &dyn OperatorTaskState,
    ) -> Result<()> {
        self.inner.maintain(ns, state).await
    }
}

#[async_trait]
impl OperatorStore for ScyllaWindowStore {
    fn as_any(&self) -> &dyn Any {
        self
    }

    async fn maintain(
        &self,
        ns: &crate::runtime::operators::window::model::StateNamespace,
        state: &dyn OperatorTaskState,
    ) -> Result<()> {
        maintain::maintain(self, ns, state).await
    }
}
