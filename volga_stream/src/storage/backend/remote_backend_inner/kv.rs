use crate::storage::batch::BoxFut;

use super::RemoteBackendInner;

impl RemoteBackendInner {
    pub fn get_raw_bytes<'a>(&'a self, key: Vec<u8>) -> BoxFut<'a, anyhow::Result<Option<Vec<u8>>>> {
        Box::pin(async move {
            let db = self.db.read().clone();
            match db.get(&key).await {
                Ok(Some(bytes)) => Ok(Some(bytes.as_ref().to_vec())),
                Ok(None) => Ok(None),
                Err(e) => Err(anyhow::anyhow!(e.to_string())),
            }
        })
    }

    pub fn put_raw_bytes<'a>(&'a self, key: Vec<u8>, value: Vec<u8>) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let db = self.db.read().clone();
            db.put_with_options(
                &key,
                value,
                &slatedb::config::PutOptions::default(),
                &Self::write_opts_low_latency(),
            )
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!(e.to_string()))
        })
    }

    pub fn delete_raw_bytes<'a>(&'a self, key: Vec<u8>) -> BoxFut<'a, anyhow::Result<()>> {
        Box::pin(async move {
            let db = self.db.read().clone();
            db.delete_with_options(&key, &Self::write_opts_low_latency())
                .await
                .map(|_| ())
                .map_err(|e| anyhow::anyhow!(e.to_string()))
        })
    }
}

