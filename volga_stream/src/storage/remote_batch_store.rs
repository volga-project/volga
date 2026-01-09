use std::sync::Arc;
use std::time::Duration;

use arrow::record_batch::RecordBatch;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};

use crate::common::Key;
use crate::runtime::operators::window::TimeGranularity;
use crate::runtime::TaskId;
use crate::storage::batch_store::{
    BatchId, BatchStore, BatchStoreCheckpoint, BoxFut, InMemBatchStore, RemoteCheckpointToken, Timestamp,
};

use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, CheckpointScope, Settings, WriteOptions};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::CachedEntry;
use slatedb::object_store::path::Path;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

use foyer::{DirectFsDeviceOptions, Engine, HybridCacheBuilder};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RemoteBatchStoreConfig {
    pub db_path: String,
    pub checkpoint_lifetime_secs: u64,

    pub bucket_granularity: TimeGranularity,
    pub max_batch_size: usize,

    pub foyer_memory_bytes: usize,
    pub foyer_disk_bytes: usize,
    pub foyer_disk_path: String,
}

/// A durable `BatchStore` implementation backed by SlateDB + Foyer hybrid cache.
///
/// Notes:
/// - `put/load/delete` are "best-effort" (trait has no Result); errors are swallowed.
/// - Checkpoint/restore are fallible and async, returning `anyhow::Result`.
pub struct RemoteBatchStore {
    cfg: RemoteBatchStoreConfig,
    object_store: Arc<dyn ObjectStore>,
    db_path_current: RwLock<String>,
    db: RwLock<Arc<Db>>,
}

impl std::fmt::Debug for RemoteBatchStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBatchStore")
            .field("cfg", &self.cfg)
            .field("db_path_current", &self.db_path_current.read())
            .finish_non_exhaustive()
    }
}

impl RemoteBatchStore {
    pub async fn open(
        cfg: RemoteBatchStoreConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> anyhow::Result<Self> {
        let cache = HybridCacheBuilder::new()
            .with_name("volga-slatedb-foyer-hybrid")
            .memory(cfg.foyer_memory_bytes)
            .with_weighter(|_, v: &CachedEntry| v.size())
            .storage(Engine::large())
            .with_device_options(
                DirectFsDeviceOptions::new(&cfg.foyer_disk_path).with_capacity(cfg.foyer_disk_bytes),
            )
            .build()
            .await?;

        let cache = Arc::new(FoyerHybridCache::new_with_cache(cache));

        let mut settings = Settings::default();
        // Avoid double disk caching: keep SlateDB's own object-store cache disabled.
        settings.object_store_cache_options.root_folder = None;

        let db = Db::builder(cfg.db_path.clone(), object_store.clone())
            .with_settings(settings)
            .with_memory_cache(cache)
            .build()
            .await?;

        Ok(Self {
            cfg,
            object_store,
            db_path_current: RwLock::new(String::new()), // set below
            db: RwLock::new(Arc::new(db)),
        }
        .with_current_db_path())
    }

    fn with_current_db_path(self) -> Self {
        *self.db_path_current.write() = self.cfg.db_path.clone();
        self
    }

    fn key_bytes(task_id: &TaskId, batch_id: BatchId) -> Vec<u8> {
        // Stable namespacing: task_id / batch_id.
        // Keep it ASCII for easy debugging in object stores.
        format!("{}/{}", task_id.as_ref(), batch_id.to_string()).into_bytes()
    }

    fn write_opts_low_latency() -> WriteOptions {
        WriteOptions { await_durable: false }
    }

    fn restored_clone_path(base: &str, checkpoint_uuid: &str) -> String {
        // Ensure clone path differs from parent path even if caller uses same base path.
        // We keep it deterministic so restores are repeatable.
        let base = base.trim_end_matches('/');
        format!("{}/restore-{}", base, checkpoint_uuid)
    }

    fn record_batch_to_ipc_bytes(batch: &RecordBatch) -> Vec<u8> {
        let mut arrow_buffer = Vec::new();
        let mut writer = arrow::ipc::writer::FileWriter::try_new(
            std::io::Cursor::new(&mut arrow_buffer),
            batch.schema().as_ref(),
        )
        .expect("ipc writer");
        writer.write(batch).expect("ipc write");
        writer.finish().expect("ipc finish");
        arrow_buffer
    }

    fn record_batch_from_ipc_bytes(bytes: &[u8]) -> RecordBatch {
        let mut reader =
            arrow::ipc::reader::FileReader::try_new(std::io::Cursor::new(bytes), None).expect("ipc reader");
        match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => panic!("Failed to read record batch: {}", e),
            None => panic!("No record batch in IPC bytes"),
        }
    }
}

impl BatchStore for RemoteBatchStore {
    fn bucket_granularity(&self) -> TimeGranularity {
        self.cfg.bucket_granularity
    }

    fn max_batch_size(&self) -> usize {
        self.cfg.max_batch_size
    }

    fn partition_records(
        &self,
        batch: &RecordBatch,
        partition_key: &Key,
        ts_column_index: usize,
    ) -> Vec<(Timestamp, RecordBatch)> {
        let out: Vec<(BatchId, RecordBatch)> = InMemBatchStore::time_partition_batch(
            batch,
            partition_key,
            ts_column_index,
            self.cfg.bucket_granularity,
            self.cfg.max_batch_size,
        );
        out.into_iter().map(|(id, b)| (id.time_bucket(), b)).collect()
    }

    fn load_batch<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, Option<RecordBatch>> {
        Box::pin(async move {
            let key = Self::key_bytes(&task_id, batch_id);
            let db = self.db.read().clone();
            match db.get(&key).await {
                Ok(Some(bytes)) => Some(Self::record_batch_from_ipc_bytes(bytes.as_ref())),
                Ok(None) => None,
                Err(_) => None,
            }
        })
    }

    fn remove_batch<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, ()> {
        Box::pin(async move {
            let key = Self::key_bytes(&task_id, batch_id);
            let db = self.db.read().clone();
            let _ = db.delete_with_options(&key, &Self::write_opts_low_latency()).await;
        })
    }

    fn put_batch_with_id<'a>(
        &'a self,
        task_id: TaskId,
        batch_id: BatchId,
        batch: RecordBatch,
        _partition_key: &'a Key,
    ) -> BoxFut<'a, ()> {
        Box::pin(async move {
            let key = Self::key_bytes(&task_id, batch_id);
            let bytes = Self::record_batch_to_ipc_bytes(&batch);
            let db = self.db.read().clone();
            let _ = db
                .put_with_options(&key, bytes, &slatedb::config::PutOptions::default(), &Self::write_opts_low_latency())
                .await;
        })
    }

    fn await_persisted<'a>(&'a self) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let db = self.db.read().clone();
            db.flush().await.map_err(Into::into)
        })
    }

    fn to_checkpoint<'a>(
        &'a self,
        task_id: TaskId,
    ) -> BoxFut<'a, anyhow::Result<BatchStoreCheckpoint>> {
        Box::pin(async move {
            // Ensure all prior writes are flushed before checkpointing.
            self.await_persisted().await?;

            let lifetime = Duration::from_secs(self.cfg.checkpoint_lifetime_secs);
            let db = self.db.read().clone();
            let res = db
                .create_checkpoint(
                    CheckpointScope::All,
                    &CheckpointOptions {
                        lifetime: Some(lifetime),
                        ..CheckpointOptions::default()
                    },
                )
                .await?;

            let token = RemoteCheckpointToken {
                parent_db_path: self.db_path_current.read().clone(),
                checkpoint_uuid: res.id.to_string(),
                manifest_id: res.manifest_id,
                lifetime_secs: Some(self.cfg.checkpoint_lifetime_secs),
            };

            // Task id currently does not affect the checkpoint token itself, but keep the parameter
            // to match the trait and allow future per-task DB sharding.
            let _ = task_id;

            Ok(BatchStoreCheckpoint::Remote(token))
        })
    }

    fn apply_checkpoint<'a>(
        &'a self,
        _task_id: TaskId,
        cp: BatchStoreCheckpoint,
    ) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let token = match cp {
                BatchStoreCheckpoint::Remote(t) => t,
                BatchStoreCheckpoint::InMem(_) => anyhow::bail!("cannot apply in-mem checkpoint to RemoteBatchStore"),
            };

            let checkpoint_uuid = uuid::Uuid::parse_str(&token.checkpoint_uuid)?;
            let clone_path = Self::restored_clone_path(&self.cfg.db_path, &token.checkpoint_uuid);

            // Create clone DB from checkpoint, then open it and swap our handle.
            let admin = AdminBuilder::new(Path::from(clone_path.clone()), self.object_store.clone()).build();
            admin
                .create_clone(Path::from(token.parent_db_path.clone()), Some(checkpoint_uuid))
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;

            let mut settings = Settings::default();
            settings.object_store_cache_options.root_folder = None;

            // Rebuild cache for the restored instance (simple approach; optimize later if needed).
            let cache = HybridCacheBuilder::new()
                .with_name("volga-slatedb-foyer-hybrid")
                .memory(self.cfg.foyer_memory_bytes)
                .with_weighter(|_, v: &CachedEntry| v.size())
                .storage(Engine::large())
                .with_device_options(
                    DirectFsDeviceOptions::new(&self.cfg.foyer_disk_path).with_capacity(self.cfg.foyer_disk_bytes),
                )
                .build()
                .await?;
            let cache = Arc::new(FoyerHybridCache::new_with_cache(cache));

            let db = Db::builder(Path::from(clone_path.clone()), self.object_store.clone())
                .with_settings(settings)
                .with_memory_cache(cache)
                .build()
                .await?;

            *self.db.write() = Arc::new(db);
            *self.db_path_current.write() = clone_path;

            Ok(())
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use arrow::array::{Int64Array, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};

    use slatedb::object_store::memory::InMemory;

    fn make_batch(n: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new("timestamp", DataType::Timestamp(TimeUnit::Millisecond, None), false),
            Field::new("value", DataType::Int64, false),
            Field::new("key", DataType::Utf8, false),
        ]));

        let ts = TimestampMillisecondArray::from_iter_values((0..n as i64).map(|v| 1000 + v));
        let value = Int64Array::from_iter_values((0..n as i64).map(|v| v));
        let key = StringArray::from_iter_values((0..n).map(|_| "A"));

        RecordBatch::try_new(
            schema,
            vec![Arc::new(ts), Arc::new(value), Arc::new(key)],
        )
        .unwrap()
    }

    fn make_key(partition: &str) -> Key {
        let schema = Arc::new(Schema::new(vec![Field::new("partition", DataType::Utf8, false)]));
        let arr = StringArray::from(vec![partition]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(arr)]).expect("key batch");
        Key::new(batch).expect("key")
    }

    fn tmp_under_target(name: &str) -> String {
        let base = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("target");
        let path = base.join(format!("remote-store-test-{}-{}", name, rand::random::<u64>()));
        std::fs::create_dir_all(&path).unwrap();
        path.to_string_lossy().to_string()
    }

    #[tokio::test]
    async fn put_load_delete_roundtrip() -> anyhow::Result<()> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let cfg = RemoteBatchStoreConfig {
            db_path: "db_put_load_delete_roundtrip".to_string(),
            checkpoint_lifetime_secs: 60,
            bucket_granularity: TimeGranularity::Seconds(1),
            max_batch_size: 1024,
            foyer_memory_bytes: 1024 * 1024,
            foyer_disk_bytes: 16 * 1024 * 1024,
            foyer_disk_path: tmp_under_target("foyer"),
        };

        let store = RemoteBatchStore::open(cfg, object_store).await?;
        let task_id: TaskId = Arc::<str>::from("t1");
        let key = make_key("A");
        let batch_id = BatchId::random();

        let batch = make_batch(16);
        store
            .put_batch_with_id(task_id.clone(), batch_id, batch.clone(), &key)
            .await;

        store.await_persisted().await?;

        let loaded = store.load_batch(task_id.clone(), batch_id, &key).await;
        assert!(loaded.is_some());
        assert_eq!(loaded.as_ref().unwrap().num_rows(), batch.num_rows());

        store.remove_batch(task_id.clone(), batch_id, &key).await;
        store.await_persisted().await?;

        let loaded2 = store.load_batch(task_id, batch_id, &key).await;
        assert!(loaded2.is_none());
        Ok(())
    }

    #[tokio::test]
    async fn checkpoint_token_restore_roundtrip() -> anyhow::Result<()> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let foyer_path = tmp_under_target("foyer_cp");

        // 1) Create parent store, write one batch, checkpoint.
        let parent_db_path = format!("db_parent_{}", rand::random::<u64>());
        let parent_cfg = RemoteBatchStoreConfig {
            db_path: parent_db_path.clone(),
            checkpoint_lifetime_secs: 60,
            bucket_granularity: TimeGranularity::Seconds(1),
            max_batch_size: 1024,
            foyer_memory_bytes: 1024 * 1024,
            foyer_disk_bytes: 16 * 1024 * 1024,
            foyer_disk_path: foyer_path.clone(),
        };
        let parent = RemoteBatchStore::open(parent_cfg, object_store.clone()).await?;
        let task_id: TaskId = Arc::<str>::from("t1");
        let key = make_key("A");
        let batch_id = BatchId::random();
        let batch = make_batch(16);
        parent
            .put_batch_with_id(task_id.clone(), batch_id, batch.clone(), &key)
            .await;
        parent.await_persisted().await?;

        let cp = parent.to_checkpoint(task_id.clone()).await?;

        // 2) Open a new store at a different path and restore via clone.
        let restore_db_path = format!("db_restore_{}", rand::random::<u64>());
        let restore_cfg = RemoteBatchStoreConfig {
            db_path: restore_db_path,
            checkpoint_lifetime_secs: 60,
            bucket_granularity: TimeGranularity::Seconds(1),
            max_batch_size: 1024,
            foyer_memory_bytes: 1024 * 1024,
            foyer_disk_bytes: 16 * 1024 * 1024,
            foyer_disk_path: foyer_path,
        };
        let restored = RemoteBatchStore::open(restore_cfg, object_store).await?;
        restored.apply_checkpoint(task_id.clone(), cp).await?;

        let loaded = restored.load_batch(task_id, batch_id, &key).await;
        assert!(loaded.is_some());
        assert_eq!(loaded.as_ref().unwrap().num_rows(), batch.num_rows());
        Ok(())
    }

    #[tokio::test]
    async fn task_namespacing_avoids_collisions() -> anyhow::Result<()> {
        let object_store: Arc<dyn ObjectStore> = Arc::new(InMemory::new());

        let cfg = RemoteBatchStoreConfig {
            db_path: format!("db_task_namespacing_{}", rand::random::<u64>()),
            checkpoint_lifetime_secs: 60,
            bucket_granularity: TimeGranularity::Seconds(1),
            max_batch_size: 1024,
            foyer_memory_bytes: 1024 * 1024,
            foyer_disk_bytes: 16 * 1024 * 1024,
            foyer_disk_path: tmp_under_target("foyer_ns"),
        };

        let store = RemoteBatchStore::open(cfg, object_store).await?;
        let key = make_key("A");
        let batch_id = BatchId::random();
        let b1 = make_batch(8);
        let b2 = make_batch(12);

        let t1: TaskId = Arc::<str>::from("t1");
        let t2: TaskId = Arc::<str>::from("t2");

        store.put_batch_with_id(t1.clone(), batch_id, b1.clone(), &key).await;
        store.put_batch_with_id(t2.clone(), batch_id, b2.clone(), &key).await;
        store.await_persisted().await?;

        let l1 = store.load_batch(t1, batch_id, &key).await.unwrap();
        let l2 = store.load_batch(t2, batch_id, &key).await.unwrap();
        assert_eq!(l1.num_rows(), b1.num_rows());
        assert_eq!(l2.num_rows(), b2.num_rows());
        Ok(())
    }
}

