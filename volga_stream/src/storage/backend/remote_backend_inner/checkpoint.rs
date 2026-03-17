use std::sync::Arc;
use std::time::Duration;

use crate::storage::batch::{BoxFut, RemoteCheckpointToken};

use slatedb::admin::AdminBuilder;
use slatedb::config::{CheckpointOptions, CheckpointScope, Settings};
use slatedb::db_cache::foyer_hybrid::FoyerHybridCache;
use slatedb::db_cache::CachedEntry;
use slatedb::object_store::path::Path;
use slatedb::Db;

use foyer::{DirectFsDeviceOptions, Engine, HybridCacheBuilder};

use super::RemoteBackendInner;

impl RemoteBackendInner {
    pub fn await_persisted<'a>(&'a self) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let db = self.db.read().clone();
            db.flush().await.map_err(Into::into)
        })
    }

    pub fn to_checkpoint<'a>(
        &'a self,
        task_id: crate::runtime::TaskId,
    ) -> BoxFut<'a, anyhow::Result<RemoteCheckpointToken>> {
        Box::pin(async move {
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
            let _ = task_id;
            Ok(RemoteCheckpointToken {
                parent_db_path: self.db_path_current.read().clone(),
                checkpoint_uuid: res.id.to_string(),
                manifest_id: res.manifest_id,
                lifetime_secs: Some(self.cfg.checkpoint_lifetime_secs),
            })
        })
    }

    pub fn apply_checkpoint<'a>(&'a self, token: RemoteCheckpointToken) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let checkpoint_uuid = uuid::Uuid::parse_str(&token.checkpoint_uuid)?;
            let clone_path = Self::restored_clone_path(&self.cfg.db_path, &token.checkpoint_uuid);

            let admin = AdminBuilder::new(Path::from(clone_path.clone()), self.object_store.clone()).build();
            admin
                .create_clone(Path::from(token.parent_db_path.clone()), Some(checkpoint_uuid))
                .await
                .map_err(|e| anyhow::anyhow!(e.to_string()))?;

            let mut settings = Settings::default();
            settings.object_store_cache_options.root_folder = None;

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

