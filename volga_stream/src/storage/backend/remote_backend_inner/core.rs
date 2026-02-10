use std::sync::Arc;

use parking_lot::RwLock;

use slatedb::config::{Settings, WriteOptions};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::CachedEntry;
use slatedb::object_store::ObjectStore;
use slatedb::Db;

use foyer::{DirectFsDeviceOptions, Engine, HybridCacheBuilder};

use super::RemoteBackendInnerConfig;

pub struct RemoteBackendInner {
    pub(crate) cfg: RemoteBackendInnerConfig,
    pub(crate) object_store: Arc<dyn ObjectStore>,
    pub(crate) db_path_current: RwLock<String>,
    pub(crate) db: RwLock<Arc<Db>>,
}

impl std::fmt::Debug for RemoteBackendInner {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RemoteBackendInner")
            .field("cfg", &self.cfg)
            .field("db_path_current", &self.db_path_current.read())
            .finish_non_exhaustive()
    }
}

impl RemoteBackendInner {
    pub async fn open(cfg: RemoteBackendInnerConfig, object_store: Arc<dyn ObjectStore>) -> anyhow::Result<Self> {
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
        settings.object_store_cache_options.root_folder = None;

        let db = Db::builder(cfg.db_path.clone(), object_store.clone())
            .with_settings(settings)
            .with_memory_cache(cache)
            .build()
            .await?;

        Ok(Self {
            db_path_current: RwLock::new(cfg.db_path.clone()),
            cfg,
            object_store,
            db: RwLock::new(Arc::new(db)),
        })
    }

    pub(crate) fn write_opts_low_latency() -> WriteOptions {
        WriteOptions { await_durable: false }
    }

    pub(crate) fn restored_clone_path(base: &str, checkpoint_uuid: &str) -> String {
        let base = base.trim_end_matches('/');
        format!("{}/restore-{}", base, checkpoint_uuid)
    }
}

